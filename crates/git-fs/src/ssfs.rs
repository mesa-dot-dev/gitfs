//! Module responsible for managing the filesystem registry.
//!
//! The terminology here is unconventional -- in this context, a "registry" refers to what is
//! normally called the filesystem.

//! TODO(markovejnovic): A large part of these Arcs should be avoided, but the implementation is
//! very rushed. We could squeeze out a lot more performance out of this cache.

use std::{
    ffi::{OsStr, OsString},
    future::Future,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicU32, AtomicU64, Ordering},
    },
};

use fuser::FUSE_ROOT_ID;
use rustc_hash::FxHashMap;
use tokio::sync::Notify;
use tracing::{debug, error, trace};

/// Each filesystem `INode` is identified by a unique inode number (`INo`).
pub type INo = u32;

/// Each path component is represented as an `OsString`.
pub type Path = OsString;

// A lightweight view into a path component.
pub type PathView = OsStr;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum INodeKind {
    File,
    Directory,
}

/// Represents the children state of a directory inode.
#[derive(Debug, Clone)]
pub enum DirChildren {
    /// This inode is not a directory (i.e., it's a file).
    NotADirectory,
    /// This directory's children haven't been fetched yet.
    Unpopulated,
    /// This directory's children are known.
    Populated(Arc<FxHashMap<Path, INo>>),
}

/// Each node on the filesystem tree is represented by an `INode`.
#[derive(Debug)]
pub struct INode {
    /// The unique inode number for this node.
    pub ino: INo,

    /// The parent inode number for this node.
    pub parent: INo,

    /// The name of this node (path component within its parent directory).
    pub name: Path,

    /// The children of this node.
    pub children: DirChildren,

    /// The size of this node in bytes.
    pub size: u64,
}

impl INode {
    /// Returns the kind of this `INode` (file or directory).
    pub fn kind(&self) -> INodeKind {
        match &self.children {
            DirChildren::NotADirectory => INodeKind::File,
            DirChildren::Unpopulated | DirChildren::Populated(_) => INodeKind::Directory,
        }
    }

    /// Returns a lightweight handle containing the copyable metadata of this `INode`.
    pub fn handle(&self) -> INodeHandle {
        INodeHandle {
            ino: self.ino,
            parent: self.parent,
            kind: self.kind(),
            size: self.size,
        }
    }
}

/// A lightweight, copyable handle to an `INode`'s metadata.
///
/// This avoids cloning the potentially large children map when only scalar metadata is needed.
#[derive(Debug, Clone, Copy)]
pub struct INodeHandle {
    pub ino: INo,
    pub parent: INo,
    pub kind: INodeKind,
    pub size: u64,
}

/// The result of resolving an inode number in the filesystem.
#[derive(Debug)]
pub enum SsfsResolutionError {
    DoesNotExist,
    EntryIsNotDirectory,
    EntryIsNotFile,
    IoError,
}

#[derive(Debug)]
pub enum GetINodeError {
    DoesNotExist,
}

impl From<GetINodeError> for SsfsResolutionError {
    fn from(value: GetINodeError) -> Self {
        match value {
            GetINodeError::DoesNotExist => Self::DoesNotExist,
        }
    }
}

pub enum SsfsOk<T> {
    Resolved(T),
    Future(Pin<Box<dyn Future<Output = SsfsResolvedResult<T>> + Send>>),
}

pub type SsfsResult<T> = Result<SsfsOk<T>, SsfsResolutionError>;
pub type SsfsResolvedResult<T> = Result<T, SsfsResolutionError>;

// --- Backend trait and types ---

/// A directory entry returned by the backend.
#[derive(Debug, Clone)]
pub struct SsfsDirEntry {
    pub name: OsString,
    pub kind: INodeKind,
    pub size: u64,
}

/// Errors that can occur when fetching from the backend.
#[derive(Debug)]
pub enum SsfsBackendError {
    NotFound,
    #[expect(dead_code)]
    Io(Box<dyn std::error::Error + Send + Sync>),
}

/// Trait for the backend that provides directory listings and file content.
pub trait SsfsBackend: Send + Sync + 'static {
    fn readdir(
        &self,
        path: &str,
    ) -> impl Future<Output = Result<Vec<SsfsDirEntry>, SsfsBackendError>> + Send;

    fn read_file(
        &self,
        path: &str,
    ) -> impl Future<Output = Result<Vec<u8>, SsfsBackendError>> + Send;
}

// --- Caching Backend Wrapper ---

/// Configuration for the file content cache.
#[derive(Debug, Clone, Copy)]
pub struct FileCacheConfig {
    /// Minimum file size in bytes to cache. Files smaller than this are not cached.
    pub min_file_size: u64,
    /// Maximum total size in bytes for the cache. When exceeded, old entries are evicted.
    pub max_cache_size: u64,
}

impl Default for FileCacheConfig {
    fn default() -> Self {
        Self {
            // Only cache files >= 4KB (small files aren't worth caching overhead)
            min_file_size: 4 * 1024,
            // Default max cache size: 256MB
            max_cache_size: 256 * 1024 * 1024,
        }
    }
}

/// A cached file entry with access tracking for LRU eviction.
#[derive(Debug)]
struct CachedFile {
    /// The file contents.
    data: Arc<Vec<u8>>,
    /// Monotonic counter value when this entry was last accessed.
    last_access: AtomicU64,
}

impl CachedFile {
    fn new(data: Vec<u8>, access_counter: u64) -> Self {
        Self {
            data: Arc::new(data),
            last_access: AtomicU64::new(access_counter),
        }
    }

    fn touch(&self, access_counter: u64) {
        self.last_access.store(access_counter, Ordering::Relaxed);
    }

    fn last_access(&self) -> u64 {
        self.last_access.load(Ordering::Relaxed)
    }
}

/// A caching wrapper around an `SsfsBackend` that provides LRU file content caching.
///
/// Files larger than `config.min_file_size` are cached in memory. When the cache
/// exceeds `config.max_cache_size`, older entries are evicted using LRU.
pub struct CachingBackend<B: SsfsBackend> {
    /// The wrapped backend.
    inner: Arc<B>,

    /// Cache for file contents, keyed by path.
    file_cache: Arc<scc::HashMap<String, CachedFile>>,

    /// Current total size of cached file contents in bytes.
    file_cache_size: Arc<AtomicU64>,

    /// Monotonically increasing counter for LRU access tracking.
    access_counter: Arc<AtomicU64>,

    /// Configuration for the file cache.
    config: FileCacheConfig,
}

impl<B: SsfsBackend> CachingBackend<B> {
    /// Creates a new caching backend wrapping the given inner backend.
    pub fn new(inner: Arc<B>) -> Self {
        Self::with_config(inner, FileCacheConfig::default())
    }

    /// Creates a new caching backend with custom configuration.
    pub fn with_config(inner: Arc<B>, config: FileCacheConfig) -> Self {
        Self {
            inner,
            file_cache: Arc::new(scc::HashMap::new()),
            file_cache_size: Arc::new(AtomicU64::new(0)),
            access_counter: Arc::new(AtomicU64::new(0)),
            config,
        }
    }

    /// Returns current file cache statistics (total bytes cached, entry count).
    #[cfg_attr(not(test), expect(dead_code))]
    pub fn cache_stats(&self) -> (u64, usize) {
        let size = self.file_cache_size.load(Ordering::Relaxed);
        let count = self.file_cache.len();
        (size, count)
    }
}

impl<B: SsfsBackend> SsfsBackend for CachingBackend<B> {
    fn readdir(
        &self,
        path: &str,
    ) -> impl Future<Output = Result<Vec<SsfsDirEntry>, SsfsBackendError>> + Send {
        self.inner.readdir(path)
    }

    fn read_file(
        &self,
        path: &str,
    ) -> impl Future<Output = Result<Vec<u8>, SsfsBackendError>> + Send {
        let path_owned = path.to_owned();
        let inner = Arc::clone(&self.inner);
        let file_cache = Arc::clone(&self.file_cache);
        let file_cache_size = Arc::clone(&self.file_cache_size);
        let access_counter = Arc::clone(&self.access_counter);
        let config = self.config;

        async move {
            // Check cache first.
            let access = access_counter.fetch_add(1, Ordering::Relaxed);
            if let Some(cached) = file_cache
                .read_async(&path_owned, |_, entry| {
                    entry.touch(access);
                    Arc::clone(&entry.data)
                })
                .await
            {
                debug!(path = %path_owned, size = cached.len(), "cache hit");
                return Ok((*cached).clone());
            }

            debug!(path = %path_owned, "cache miss, fetching from backend");

            let result = inner.read_file(&path_owned).await;

            if let Ok(ref data) = result {
                Self::maybe_cache_file(
                    &path_owned,
                    data,
                    &file_cache,
                    &file_cache_size,
                    &access_counter,
                    config,
                )
                .await;
            }

            result
        }
    }
}

impl<B: SsfsBackend> CachingBackend<B> {
    /// Cache a file if it meets the size threshold.
    async fn maybe_cache_file(
        path: &str,
        data: &[u8],
        file_cache: &scc::HashMap<String, CachedFile>,
        file_cache_size: &AtomicU64,
        access_counter: &AtomicU64,
        config: FileCacheConfig,
    ) {
        let data_len = data.len() as u64;

        if data_len < config.min_file_size {
            trace!(
                path,
                size = data_len,
                min = config.min_file_size,
                "file too small to cache"
            );
            return;
        }

        let access = access_counter.fetch_add(1, Ordering::Relaxed);

        // Check if we need to evict before inserting.
        let current_size = file_cache_size.load(Ordering::Relaxed);
        if current_size + data_len > config.max_cache_size {
            Self::evict_lru_entries(file_cache, file_cache_size, config.max_cache_size, data_len)
                .await;
        }

        // Insert into cache.
        let cached = CachedFile::new(data.to_vec(), access);
        if file_cache
            .insert_async(path.to_owned(), cached)
            .await
            .is_ok()
        {
            file_cache_size.fetch_add(data_len, Ordering::Relaxed);
            debug!(path, size = data_len, "cached file");
        }
    }

    /// Evict LRU entries from the file cache until there's room for `needed_space` bytes.
    async fn evict_lru_entries(
        file_cache: &scc::HashMap<String, CachedFile>,
        file_cache_size: &AtomicU64,
        max_cache_size: u64,
        needed_space: u64,
    ) {
        let target_size = max_cache_size.saturating_sub(needed_space);
        let mut current_size = file_cache_size.load(Ordering::Relaxed);

        // Collect all entries with their access times for LRU sorting.
        let mut entries: Vec<(String, u64, u64)> = Vec::new();
        file_cache
            .iter_async(|path, entry| {
                entries.push((path.clone(), entry.last_access(), entry.data.len() as u64));
                true // continue iterating
            })
            .await;

        // Sort by last_access (oldest first = lowest counter value).
        entries.sort_by_key(|(_, last_access, _)| *last_access);

        // Evict until we're under target.
        for (path, _, size) in entries {
            if current_size <= target_size {
                break;
            }

            if file_cache.remove_async(&path).await.is_some() {
                current_size = file_cache_size.fetch_sub(size, Ordering::Relaxed) - size;
                debug!(path, size, "evicted file from cache");
            }
        }
    }
}

/// TODO(markovejnovic): In the future, we'll have to figure out how ssfs will serialize to disk.
pub struct SsFs<B: SsfsBackend> {
    /// Mapping from inode numbers to `INode`s.
    nodes: Arc<scc::HashMap<INo, INode>>,

    /// Holds notifications for pending updates to inodes.
    ///
    /// The key is the inode number that the update is associated with. For directories, this is
    /// the inode number of the directory being updated, and for files, this is the inode number of
    /// the parent directory.
    ///
    /// Waiters are notified when the update completes.
    pending_updates: Arc<scc::HashMap<INo, Arc<Notify>>>,

    /// Atomic counter for allocating new inode numbers.
    next_ino: Arc<AtomicU32>,

    /// The backend for fetching directory contents and file data.
    backend: Arc<B>,

    /// Handle to the tokio runtime for spawning async tasks.
    rt_handle: tokio::runtime::Handle,
}

impl<B: SsfsBackend> SsFs<B> {
    #[expect(clippy::cast_possible_truncation)] // FUSE_ROOT_ID is always 1
    pub const ROOT_INO: INo = FUSE_ROOT_ID as u32;

    /// Creates a new filesystem with the given backend and runtime handle.
    pub fn new(backend: Arc<B>, rt_handle: tokio::runtime::Handle) -> Self {
        let nodes = Arc::new(scc::HashMap::new());
        let root = INode {
            ino: Self::ROOT_INO,
            parent: Self::ROOT_INO,
            name: OsString::new(),
            children: DirChildren::Unpopulated,
            size: 0,
        };
        drop(nodes.insert_sync(Self::ROOT_INO, root));

        let s = Self {
            nodes,
            pending_updates: Arc::new(scc::HashMap::new()),
            next_ino: Arc::new(AtomicU32::new(Self::ROOT_INO + 1)),
            backend,
            rt_handle,
        };

        // Eagerly prefetch the root directory so it's likely cached before the first FUSE call.
        drop(s.initiate_readdir(Self::ROOT_INO));

        s
    }

    /// Reconstruct the path string for a given inode by walking up the parent chain.
    /// Returns the `/`-separated path relative to the root. Root itself returns `""`.
    fn abspath(&self, ino: INo) -> Option<String> {
        if ino == Self::ROOT_INO {
            return Some(String::new());
        }

        let mut components = Vec::new();
        let mut current = ino;

        loop {
            if current == Self::ROOT_INO {
                break;
            }

            let (name, parent) = self
                .nodes
                .read_sync(&current, |_, inode| (inode.name.clone(), inode.parent))?;

            components.push(name.to_string_lossy().into_owned());
            current = parent;
        }

        components.reverse();
        Some(components.join("/"))
    }

    /// Read the contents of a directory. Returns a list of (ino, kind, name) tuples.
    ///
    /// If the directory is unpopulated, initiates a backend fetch.
    pub fn readdir(&self, ino: INo) -> SsfsResult<Vec<(INo, INodeKind, Path)>> {
        let state = self
            .nodes
            .read_sync(&ino, |_, inode| (inode.kind(), inode.children.clone()));

        let Some((kind, children)) = state else {
            return Err(SsfsResolutionError::DoesNotExist);
        };

        if kind != INodeKind::Directory {
            return Err(SsfsResolutionError::EntryIsNotDirectory);
        }

        match children {
            DirChildren::NotADirectory => Err(SsfsResolutionError::EntryIsNotDirectory),
            DirChildren::Populated(map) => {
                // Fast path: children are already known.
                debug!(
                    ino,
                    count = map.len(),
                    "readdir: cache hit (already populated)"
                );
                let entries: Vec<(INo, INodeKind, Path)> = map
                    .iter()
                    .filter_map(|(name, &child_ino)| {
                        self.nodes.read_sync(&child_ino, |_, child| {
                            (child_ino, child.kind(), name.clone())
                        })
                    })
                    .collect();
                Ok(SsfsOk::Resolved(entries))
            }
            DirChildren::Unpopulated => {
                // Need to fetch from backend.
                debug!(
                    ino,
                    "readdir: cache miss (unpopulated), fetching from backend"
                );
                self.initiate_readdir(ino)
            }
        }
    }

    /// Initiate a backend readdir fetch. Uses `pending_updates` for deduplication so that
    /// concurrent callers share a single in-flight request.
    fn initiate_readdir(&self, ino: INo) -> SsfsResult<Vec<(INo, INodeKind, Path)>> {
        // Check if there's already a pending fetch for this directory.
        let existing = self.pending_updates.read_sync(&ino, |_, v| Arc::clone(v));

        if let Some(existing_notify) = existing {
            // Someone else is already fetching. Wait on their notification.
            debug!(
                ino,
                "initiate_readdir: joining existing in-flight fetch (dedup)"
            );
            let nodes = Arc::clone(&self.nodes);
            let fut = async move {
                existing_notify.notified().await;
                Self::collect_children(&nodes, ino)
            };
            return Ok(SsfsOk::Future(Box::pin(fut)));
        }

        // No pending fetch yet. Try to insert our own.
        let notify = Arc::new(Notify::new());
        match self.pending_updates.insert_sync(ino, Arc::clone(&notify)) {
            Ok(()) => {
                // We successfully claimed the fetch. Spawn the backend task.
            }
            Err((_key, _val)) => {
                // Someone else raced us. Read their notify and wait.
                debug!(
                    ino,
                    "initiate_readdir: lost insert race, joining existing fetch"
                );
                let existing = self.pending_updates.read_sync(&ino, |_, v| Arc::clone(v));
                if let Some(existing_notify) = existing {
                    let nodes = Arc::clone(&self.nodes);
                    let fut = async move {
                        existing_notify.notified().await;
                        Self::collect_children(&nodes, ino)
                    };
                    return Ok(SsfsOk::Future(Box::pin(fut)));
                }
                // The other fetch completed between our failed insert and this read.
                // The directory should now be populated.
                let nodes = Arc::clone(&self.nodes);
                return Ok(SsfsOk::Future(Box::pin(async move {
                    Self::collect_children(&nodes, ino)
                })));
            }
        }

        // Spawn the fetch task.
        let nodes = Arc::clone(&self.nodes);
        let backend = Arc::clone(&self.backend);
        let next_ino = Arc::clone(&self.next_ino);
        let pending = Arc::clone(&self.pending_updates);
        let task_notify = Arc::clone(&notify);

        let Some(path) = self.abspath(ino) else {
            return Err(SsfsResolutionError::DoesNotExist);
        };

        debug!(ino, path = %path, "initiate_readdir: spawning backend fetch");

        self.rt_handle.spawn(async move {
            let result = backend.readdir(&path).await;

            match result {
                Ok(entries) => {
                    debug!(ino, path = %path, count = entries.len(), "backend fetch complete");
                    let mut children_map = FxHashMap::default();

                    for entry in entries {
                        let child_ino = next_ino.fetch_add(1, Ordering::Relaxed);
                        let child = INode {
                            ino: child_ino,
                            parent: ino,
                            name: entry.name.clone(),
                            children: match entry.kind {
                                INodeKind::Directory => DirChildren::Unpopulated,
                                INodeKind::File => DirChildren::NotADirectory,
                            },
                            size: entry.size,
                        };
                        drop(nodes.insert_async(child_ino, child).await);
                        children_map.insert(entry.name, child_ino);
                    }

                    // Update the parent's children to Populated.
                    nodes
                        .update_async(&ino, |_, inode| {
                            inode.children = DirChildren::Populated(Arc::new(children_map));
                        })
                        .await;
                }
                Err(ref e) => {
                    error!(ino, ?e, "backend fetch failed");
                    // Leave directory as Unpopulated on error. Waiters will detect this
                    // and return IoError.
                }
            }

            // Remove from pending and notify all waiters.
            drop(pending.remove_async(&ino).await);
            task_notify.notify_waiters();
        });

        // Return a future that waits for our own fetch to complete.
        let nodes = Arc::clone(&self.nodes);
        let fut = async move {
            notify.notified().await;
            Self::collect_children(&nodes, ino)
        };
        Ok(SsfsOk::Future(Box::pin(fut)))
    }

    /// Collect children entries from a (presumably now-populated) directory.
    /// Returns `IoError` if the directory is still unpopulated after a fetch attempt.
    fn collect_children(
        nodes: &scc::HashMap<INo, INode>,
        ino: INo,
    ) -> SsfsResolvedResult<Vec<(INo, INodeKind, Path)>> {
        let state = nodes.read_sync(&ino, |_, inode| inode.children.clone());
        match state {
            Some(DirChildren::Populated(map)) => {
                let entries: Vec<(INo, INodeKind, Path)> = map
                    .iter()
                    .filter_map(|(name, &child_ino)| {
                        nodes.read_sync(&child_ino, |_, child| {
                            (child_ino, child.kind(), name.clone())
                        })
                    })
                    .collect();
                Ok(entries)
            }
            _ => Err(SsfsResolutionError::IoError),
        }
    }

    /// Prefetch subdirectories of a populated directory in the background.
    ///
    /// For each child that is an unpopulated directory, kicks off a background fetch via
    /// `initiate_readdir`. This is fire-and-forget: failures are silently ignored since
    /// this is only a speculative prefetch.
    pub fn prefetch_subdirectories(&self, ino: INo) {
        let children = self
            .nodes
            .read_sync(&ino, |_, inode| inode.children.clone());
        let Some(DirChildren::Populated(map)) = children else {
            return;
        };

        let mut prefetch_count = 0u32;
        for (_, &child_ino) in map.iter() {
            let is_unpopulated_dir = self
                .nodes
                .read_sync(&child_ino, |_, inode| {
                    matches!(inode.children, DirChildren::Unpopulated)
                })
                .unwrap_or(false);

            if is_unpopulated_dir {
                drop(self.initiate_readdir(child_ino));
                prefetch_count += 1;
            }
        }

        debug!(
            ino,
            prefetch_count,
            total_children = map.len(),
            "prefetch_subdirectories"
        );
    }

    /// Query the filesystem for a child node given its parent inode number and name.
    pub fn lookup(&self, parent: INo, path: &PathView) -> SsfsResult<INodeHandle> {
        match self.get_inode(parent).map_err(SsfsResolutionError::from)? {
            SsfsOk::Resolved(parent_handle) => {
                if parent_handle.kind != INodeKind::Directory {
                    return Err(SsfsResolutionError::EntryIsNotDirectory);
                }

                let children_state = self.nodes.read_sync(&parent, |_, n| n.children.clone());

                match children_state {
                    Some(DirChildren::Populated(map)) => {
                        debug!(parent, ?path, "lookup: cache hit, direct child lookup");
                        match map.get(path).copied() {
                            Some(child_ino) => {
                                self.get_inode(child_ino).map_err(SsfsResolutionError::from)
                            }
                            None => Err(SsfsResolutionError::DoesNotExist),
                        }
                    }
                    Some(DirChildren::Unpopulated) => {
                        debug!(
                            parent,
                            ?path,
                            "lookup: children unpopulated, fetching from backend"
                        );
                        match self.initiate_readdir(parent)? {
                            SsfsOk::Resolved(_) => {
                                // Children now populated inline — look up the child.
                                let maybe_child_ino =
                                    self.nodes
                                        .read_sync(&parent, |_, n| match &n.children {
                                            DirChildren::Populated(m) => m.get(path).copied(),
                                            DirChildren::NotADirectory
                                            | DirChildren::Unpopulated => None,
                                        })
                                        .flatten();
                                match maybe_child_ino {
                                    Some(child_ino) => {
                                        self.get_inode(child_ino).map_err(SsfsResolutionError::from)
                                    }
                                    None => Err(SsfsResolutionError::DoesNotExist),
                                }
                            }
                            SsfsOk::Future(readdir_fut) => {
                                let nodes = Arc::clone(&self.nodes);
                                let path = path.to_owned();
                                Ok(SsfsOk::Future(Box::pin(async move {
                                    readdir_fut.await?;
                                    let maybe_child_ino = nodes
                                        .read_sync(&parent, |_, n| match &n.children {
                                            DirChildren::Populated(m) => m.get(&path).copied(),
                                            DirChildren::NotADirectory
                                            | DirChildren::Unpopulated => None,
                                        })
                                        .flatten();
                                    match maybe_child_ino {
                                        Some(child_ino) => nodes
                                            .read_sync(&child_ino, |_, inode| inode.handle())
                                            .ok_or(SsfsResolutionError::DoesNotExist),
                                        None => Err(SsfsResolutionError::DoesNotExist),
                                    }
                                })))
                            }
                        }
                    }
                    Some(DirChildren::NotADirectory) => {
                        Err(SsfsResolutionError::EntryIsNotDirectory)
                    }
                    None => Err(SsfsResolutionError::DoesNotExist),
                }
            }
            SsfsOk::Future(fut) => {
                // TODO(markovejnovic): This Arc gives me the ick, I couldn't figure out a way to
                // write this without it though. It does seem that multiple futures may need to
                // await this same nodes map, so maybe it's unavoidable.
                let nodes = Arc::clone(&self.nodes);
                let path = path.to_owned();
                let fut = async move {
                    let parent_handle = fut.await?;
                    if parent_handle.kind != INodeKind::Directory {
                        return Err(SsfsResolutionError::EntryIsNotDirectory);
                    }

                    // After the parent resolves, look up the child.
                    let maybe_child_ino = nodes
                        .read_sync(&parent_handle.ino, |_, parent_inode| {
                            match &parent_inode.children {
                                DirChildren::Populated(map) => map.get(&path).copied(),
                                DirChildren::NotADirectory | DirChildren::Unpopulated => None,
                            }
                        })
                        .flatten();

                    match maybe_child_ino {
                        Some(child_ino) => nodes
                            .read_sync(&child_ino, |_, inode| inode.handle())
                            .ok_or(SsfsResolutionError::DoesNotExist),
                        None => Err(SsfsResolutionError::DoesNotExist),
                    }
                };

                Ok(SsfsOk::Future(Box::pin(fut)))
            }
        }
    }

    /// Retrieve an inode handle by its inode number. If the inode has a pending update, returns a
    /// future that resolves once the update completes.
    pub fn get_inode(&self, ino: INo) -> Result<SsfsOk<INodeHandle>, GetINodeError> {
        // Check if there is a pending update for this inode.
        let maybe_notify = self.pending_updates.read_sync(&ino, |_, v| Arc::clone(v));

        if let Some(notify) = maybe_notify {
            let nodes = Arc::clone(&self.nodes);
            let fut = async move {
                notify.notified().await;
                nodes
                    .read_sync(&ino, |_, inode| inode.handle())
                    .ok_or(SsfsResolutionError::DoesNotExist)
            };

            return Ok(SsfsOk::Future(Box::pin(fut)));
        }

        // There are no pending updates for this inode, so we can return immediately.
        let maybe_handle = self.nodes.read_sync(&ino, |_, inode| inode.handle());
        match maybe_handle {
            Some(handle) => Ok(SsfsOk::Resolved(handle)),
            None => Err(GetINodeError::DoesNotExist),
        }
    }

    /// Read the contents of a file by its inode number.
    ///
    /// Verifies the inode exists and is a file, resolves the path, and delegates to the backend.
    pub fn read(&self, ino: INo) -> SsfsResult<Vec<u8>> {
        let Some(handle) = self.nodes.read_sync(&ino, |_, inode| inode.handle()) else {
            return Err(SsfsResolutionError::DoesNotExist);
        };

        if handle.kind != INodeKind::File {
            return Err(SsfsResolutionError::EntryIsNotFile);
        }

        let Some(path) = self.abspath(ino) else {
            return Err(SsfsResolutionError::DoesNotExist);
        };

        debug!(ino, path = %path, "read: fetching from backend");

        let backend = Arc::clone(&self.backend);
        Ok(SsfsOk::Future(Box::pin(async move {
            backend.read_file(&path).await.map_err(|e| match e {
                SsfsBackendError::NotFound => SsfsResolutionError::DoesNotExist,
                SsfsBackendError::Io(_) => SsfsResolutionError::IoError,
            })
        })))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A mock backend for testing.
    struct MockBackend {
        files: std::sync::Mutex<std::collections::HashMap<String, Vec<u8>>>,
    }

    impl MockBackend {
        fn new() -> Self {
            Self {
                files: std::sync::Mutex::new(std::collections::HashMap::new()),
            }
        }

        fn add_file(&self, path: &str, content: Vec<u8>) {
            self.files.lock().unwrap().insert(path.to_owned(), content);
        }
    }

    impl SsfsBackend for MockBackend {
        fn readdir(
            &self,
            path: &str,
        ) -> impl Future<Output = Result<Vec<SsfsDirEntry>, SsfsBackendError>> + Send {
            let files = self.files.lock().unwrap().clone();
            async move {
                // Return files as if they're all in the root directory
                if path.is_empty() {
                    let entries: Vec<SsfsDirEntry> = files
                        .iter()
                        .map(|(name, content)| SsfsDirEntry {
                            name: OsString::from(name),
                            kind: INodeKind::File,
                            size: content.len() as u64,
                        })
                        .collect();
                    Ok(entries)
                } else {
                    Ok(vec![])
                }
            }
        }

        fn read_file(
            &self,
            path: &str,
        ) -> impl Future<Output = Result<Vec<u8>, SsfsBackendError>> + Send {
            let files = self.files.lock().unwrap().clone();
            let path = path.to_owned();
            async move { files.get(&path).cloned().ok_or(SsfsBackendError::NotFound) }
        }
    }

    #[test]
    fn test_file_cache_config_default() {
        let config = FileCacheConfig::default();
        assert_eq!(config.min_file_size, 4 * 1024);
        assert_eq!(config.max_cache_size, 256 * 1024 * 1024);
    }

    #[test]
    fn test_file_cache_small_file_not_cached() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let mock = Arc::new(MockBackend::new());

        // Add a small file (under threshold)
        let small_content = vec![0u8; 100]; // 100 bytes < 4KB threshold
        mock.add_file("small.txt", small_content.clone());

        let config = FileCacheConfig {
            min_file_size: 4 * 1024,
            max_cache_size: 256 * 1024 * 1024,
        };
        let caching_backend = Arc::new(CachingBackend::with_config(mock, config));
        let ssfs = SsFs::new(Arc::clone(&caching_backend), rt.handle().clone());

        // First, populate the directory
        rt.block_on(async {
            if let Ok(SsfsOk::Future(fut)) =
                ssfs.readdir(SsFs::<CachingBackend<MockBackend>>::ROOT_INO)
            {
                drop(fut.await);
            }
        });

        // Look up the file
        let file_ino = rt.block_on(async {
            match ssfs.lookup(
                SsFs::<CachingBackend<MockBackend>>::ROOT_INO,
                OsStr::new("small.txt"),
            ) {
                Ok(SsfsOk::Resolved(handle)) => Some(handle.ino),
                Ok(SsfsOk::Future(fut)) => fut.await.ok().map(|h| h.ino),
                Err(_) => None,
            }
        });

        let file_ino = file_ino.expect("File should exist");

        // Read the file
        rt.block_on(async {
            match ssfs.read(file_ino) {
                Ok(SsfsOk::Future(fut)) => {
                    let data = fut.await.unwrap();
                    assert_eq!(data, small_content);
                }
                _ => panic!("Expected future"),
            }
        });

        // Check cache stats - small file should NOT be cached
        let (cache_size, cache_count) = caching_backend.cache_stats();
        assert_eq!(cache_size, 0, "Small file should not be cached");
        assert_eq!(cache_count, 0, "Cache should be empty");
    }

    #[test]
    fn test_file_cache_large_file_cached() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let mock = Arc::new(MockBackend::new());

        // Add a large file (over threshold)
        let large_content = vec![42u8; 8 * 1024]; // 8KB > 4KB threshold
        mock.add_file("large.txt", large_content.clone());

        let config = FileCacheConfig {
            min_file_size: 4 * 1024,
            max_cache_size: 256 * 1024 * 1024,
        };
        let caching_backend = Arc::new(CachingBackend::with_config(mock, config));
        let ssfs = SsFs::new(Arc::clone(&caching_backend), rt.handle().clone());

        // First, populate the directory
        rt.block_on(async {
            if let Ok(SsfsOk::Future(fut)) =
                ssfs.readdir(SsFs::<CachingBackend<MockBackend>>::ROOT_INO)
            {
                drop(fut.await);
            }
        });

        // Look up the file
        let file_ino = rt.block_on(async {
            match ssfs.lookup(
                SsFs::<CachingBackend<MockBackend>>::ROOT_INO,
                OsStr::new("large.txt"),
            ) {
                Ok(SsfsOk::Resolved(handle)) => Some(handle.ino),
                Ok(SsfsOk::Future(fut)) => fut.await.ok().map(|h| h.ino),
                Err(_) => None,
            }
        });

        let file_ino = file_ino.expect("File should exist");

        // Read the file
        rt.block_on(async {
            match ssfs.read(file_ino) {
                Ok(SsfsOk::Future(fut)) => {
                    let data = fut.await.unwrap();
                    assert_eq!(data, large_content);
                }
                _ => panic!("Expected future"),
            }
        });

        // Check cache stats - large file should be cached
        let (cache_size, cache_count) = caching_backend.cache_stats();
        assert_eq!(cache_size, 8 * 1024, "Large file should be cached");
        assert_eq!(cache_count, 1, "Cache should have one entry");
    }

    #[test]
    fn test_file_cache_eviction() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let mock = Arc::new(MockBackend::new());

        // Add multiple files
        let file1_content = vec![1u8; 5 * 1024]; // 5KB
        let file2_content = vec![2u8; 5 * 1024]; // 5KB
        let file3_content = vec![3u8; 5 * 1024]; // 5KB
        mock.add_file("file1.txt", file1_content.clone());
        mock.add_file("file2.txt", file2_content.clone());
        mock.add_file("file3.txt", file3_content.clone());

        // Configure small cache limit (10KB) so that only 2 files fit
        let config = FileCacheConfig {
            min_file_size: 1024,       // 1KB threshold
            max_cache_size: 10 * 1024, // 10KB max
        };
        let caching_backend = Arc::new(CachingBackend::with_config(mock, config));
        let ssfs = SsFs::new(Arc::clone(&caching_backend), rt.handle().clone());

        // Populate directory
        rt.block_on(async {
            if let Ok(SsfsOk::Future(fut)) =
                ssfs.readdir(SsFs::<CachingBackend<MockBackend>>::ROOT_INO)
            {
                drop(fut.await);
            }
        });

        // Look up all files
        let lookup_file = |name: &str| {
            rt.block_on(async {
                match ssfs.lookup(
                    SsFs::<CachingBackend<MockBackend>>::ROOT_INO,
                    OsStr::new(name),
                ) {
                    Ok(SsfsOk::Resolved(handle)) => Some(handle.ino),
                    Ok(SsfsOk::Future(fut)) => fut.await.ok().map(|h| h.ino),
                    Err(_) => None,
                }
            })
            .expect("File should exist")
        };

        let ino1 = lookup_file("file1.txt");
        let ino2 = lookup_file("file2.txt");
        let ino3 = lookup_file("file3.txt");

        // Read files in order
        let read_file = |ino: INo| {
            rt.block_on(async {
                match ssfs.read(ino) {
                    Ok(SsfsOk::Future(fut)) => fut.await.unwrap(),
                    Ok(SsfsOk::Resolved(data)) => data,
                    Err(e) => panic!("Read error: {e:?}"),
                }
            })
        };

        // Read file1 first (oldest)
        assert_eq!(read_file(ino1), file1_content);
        let (size1, count1) = caching_backend.cache_stats();
        assert_eq!(size1, 5 * 1024);
        assert_eq!(count1, 1);

        // Read file2 (file1 is now oldest)
        assert_eq!(read_file(ino2), file2_content);
        let (size2, count2) = caching_backend.cache_stats();
        assert_eq!(size2, 10 * 1024);
        assert_eq!(count2, 2);

        // Read file3 - should trigger eviction of file1 (oldest)
        assert_eq!(read_file(ino3), file3_content);
        let (size3, count3) = caching_backend.cache_stats();
        // Cache should have evicted file1 to make room for file3
        assert!(
            size3 <= 10 * 1024,
            "Cache should not exceed max size: {size3}"
        );
        assert!(count3 <= 2, "Cache should have at most 2 entries: {count3}");
    }
}
