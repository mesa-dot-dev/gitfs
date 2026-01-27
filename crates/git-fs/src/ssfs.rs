//! Module responsible for managing the filesystem registry.
//!
//! The terminology here is unconventional -- in this context, a "registry" refers to what is
//! normally called the filesystem.

//! TODO(markovejnovic): A large part of these Arcs should be avoided, but the implementation is
//! very rushed. We could squeeze out a lot more performance out of this cache.
//!
//! TODO(markovejnovic): This cache grows unbounded. There's no eviction policy, so it will grow to
//! absurdly large sizes, especially for large repositories. We need to implement some kind of LRU
//! or TTL-based eviction policy.

use std::{ffi::{OsStr, OsString}, pin::Pin, sync::{atomic::{AtomicU32, Ordering}, Arc}};

use fuser::FUSE_ROOT_ID;
use rustc_hash::FxHashMap;
use tokio::sync::Notify;
use tracing::debug;

/// Each filesystem INode is identified by a unique inode number (INo).
pub type INo = u32;

/// Each path component is represented as an OsString.
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

/// Each node on the filesystem tree is represented by an INode.
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

    /// The UNIX permissions for this node.
    ///
    /// TODO(markovejnovic): Could and should be a bitfield.
    pub permissions: u16,

    /// The size of this node in bytes.
    pub size: u64,
}

impl INode {
    /// Returns the kind of this INode (file or directory).
    pub fn kind(&self) -> INodeKind {
        match &self.children {
            DirChildren::NotADirectory => INodeKind::File,
            DirChildren::Unpopulated | DirChildren::Populated(_) => INodeKind::Directory,
        }
    }

    /// Returns a lightweight handle containing the copyable metadata of this INode.
    pub fn handle(&self) -> INodeHandle {
        INodeHandle {
            ino: self.ino,
            parent: self.parent,
            kind: self.kind(),
            permissions: self.permissions,
            size: self.size,
        }
    }
}

/// A lightweight, copyable handle to an INode's metadata.
///
/// This avoids cloning the potentially large children map when only scalar metadata is needed.
#[derive(Debug, Clone, Copy)]
pub struct INodeHandle {
    pub ino: INo,
    pub parent: INo,
    pub kind: INodeKind,
    pub permissions: u16,
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

impl Into<SsfsResolutionError> for GetINodeError {
    fn into(self) -> SsfsResolutionError {
        match self {
            GetINodeError::DoesNotExist => SsfsResolutionError::DoesNotExist,
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

/// TODO(markovejnovic): In the future, we'll have to figure out how ssfs will serialize to disk.
pub struct SsFs<B: SsfsBackend> {
    /// Mapping from inode numbers to INodes.
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

    /// The backend for fetching directory contents.
    backend: Arc<B>,

    /// Handle to the tokio runtime for spawning async tasks.
    rt_handle: tokio::runtime::Handle,
}

impl<B: SsfsBackend> SsFs<B> {
    pub const ROOT_INO: INo = FUSE_ROOT_ID as u32;

    /// Creates a new filesystem with the given backend and runtime handle.
    pub fn new(backend: Arc<B>, rt_handle: tokio::runtime::Handle) -> Self {
        let nodes = Arc::new(scc::HashMap::new());
        let root = INode {
            ino: Self::ROOT_INO,
            parent: Self::ROOT_INO,
            name: OsString::new(),
            children: DirChildren::Unpopulated,
            permissions: 0o755,
            size: 0,
        };
        let _ = nodes.insert_sync(Self::ROOT_INO, root);

        let s = Self {
            nodes,
            pending_updates: Arc::new(scc::HashMap::new()),
            next_ino: Arc::new(AtomicU32::new(Self::ROOT_INO + 1)),
            backend,
            rt_handle,
        };

        // Eagerly prefetch the root directory so it's likely cached before the first FUSE call.
        let _ = s.initiate_readdir(Self::ROOT_INO);

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

            let (name, parent) = self.nodes.read_sync(&current, |_, inode| {
                (inode.name.clone(), inode.parent)
            })?;

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
        let state = self.nodes.read_sync(&ino, |_, inode| {
            (inode.kind(), inode.children.clone())
        });

        let (kind, children) = match state {
            Some(s) => s,
            None => return Err(SsfsResolutionError::DoesNotExist),
        };

        if kind != INodeKind::Directory {
            return Err(SsfsResolutionError::EntryIsNotDirectory);
        }

        match children {
            DirChildren::NotADirectory => Err(SsfsResolutionError::EntryIsNotDirectory),
            DirChildren::Populated(map) => {
                // Fast path: children are already known.
                debug!(ino, count = map.len(), "readdir: cache hit (already populated)");
                let entries: Vec<(INo, INodeKind, Path)> = map.iter().filter_map(|(name, &child_ino)| {
                    self.nodes.read_sync(&child_ino, |_, child| {
                        (child_ino, child.kind(), name.clone())
                    })
                }).collect();
                Ok(SsfsOk::Resolved(entries))
            },
            DirChildren::Unpopulated => {
                // Need to fetch from backend.
                debug!(ino, "readdir: cache miss (unpopulated), fetching from backend");
                self.initiate_readdir(ino)
            },
        }
    }

    /// Initiate a backend readdir fetch. Uses pending_updates for deduplication so that
    /// concurrent callers share a single in-flight request.
    fn initiate_readdir(&self, ino: INo) -> SsfsResult<Vec<(INo, INodeKind, Path)>> {
        // Check if there's already a pending fetch for this directory.
        let existing = self.pending_updates.read_sync(&ino, |_, v| Arc::clone(v));

        if let Some(existing_notify) = existing {
            // Someone else is already fetching. Wait on their notification.
            debug!(ino, "initiate_readdir: joining existing in-flight fetch (dedup)");
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
            },
            Err((_key, _val)) => {
                // Someone else raced us. Read their notify and wait.
                debug!(ino, "initiate_readdir: lost insert race, joining existing fetch");
                let existing = self.pending_updates.read_sync(&ino, |_, v| Arc::clone(v));
                match existing {
                    Some(existing_notify) => {
                        let nodes = Arc::clone(&self.nodes);
                        let fut = async move {
                            existing_notify.notified().await;
                            Self::collect_children(&nodes, ino)
                        };
                        return Ok(SsfsOk::Future(Box::pin(fut)));
                    },
                    None => {
                        // The other fetch completed between our failed insert and this read.
                        // The directory should now be populated.
                        let nodes = Arc::clone(&self.nodes);
                        return Ok(SsfsOk::Future(Box::pin(async move {
                            Self::collect_children(&nodes, ino)
                        })));
                    },
                }
            },
        }

        // Spawn the fetch task.
        let nodes = Arc::clone(&self.nodes);
        let backend = Arc::clone(&self.backend);
        let next_ino = Arc::clone(&self.next_ino);
        let pending = Arc::clone(&self.pending_updates);
        let task_notify = Arc::clone(&notify);

        let path = match self.abspath(ino) {
            Some(p) => p,
            None => return Err(SsfsResolutionError::DoesNotExist),
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
                            permissions: match entry.kind {
                                INodeKind::Directory => 0o755,
                                INodeKind::File => 0o444,
                            },
                            size: entry.size,
                        };
                        let _ = nodes.insert_async(child_ino, child).await;
                        children_map.insert(entry.name, child_ino);
                    }

                    // Update the parent's children to Populated.
                    nodes.update_async(&ino, |_, inode| {
                        inode.children = DirChildren::Populated(Arc::new(children_map));
                    }).await;
                },
                Err(ref e) => {
                    debug!(ino, ?e, "backend fetch failed");
                    // Leave directory as Unpopulated on error. Waiters will detect this
                    // and return IoError.
                },
            }

            // Remove from pending and notify all waiters.
            let _ = pending.remove_async(&ino).await;
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
    /// Returns IoError if the directory is still unpopulated after a fetch attempt.
    fn collect_children(
        nodes: &scc::HashMap<INo, INode>,
        ino: INo,
    ) -> SsfsResolvedResult<Vec<(INo, INodeKind, Path)>> {
        let state = nodes.read_sync(&ino, |_, inode| inode.children.clone());
        match state {
            Some(DirChildren::Populated(map)) => {
                let entries: Vec<(INo, INodeKind, Path)> = map.iter().filter_map(|(name, &child_ino)| {
                    nodes.read_sync(&child_ino, |_, child| {
                        (child_ino, child.kind(), name.clone())
                    })
                }).collect();
                Ok(entries)
            },
            _ => Err(SsfsResolutionError::IoError),
        }
    }

    /// Prefetch subdirectories of a populated directory in the background.
    ///
    /// For each child that is an unpopulated directory, kicks off a background fetch via
    /// `initiate_readdir`. This is fire-and-forget: failures are silently ignored since
    /// this is only a speculative prefetch.
    pub fn prefetch_subdirectories(&self, ino: INo) {
        let children = self.nodes.read_sync(&ino, |_, inode| inode.children.clone());
        let map = match children {
            Some(DirChildren::Populated(map)) => map,
            _ => return,
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
                let _ = self.initiate_readdir(child_ino);
                prefetch_count += 1;
            }
        }

        debug!(ino, prefetch_count, total_children = map.len(), "prefetch_subdirectories");
    }

    pub fn get_abspath(&self, ino: INo) -> Result<SsfsOk<Path>, GetINodeError> {
        unimplemented!();
    }

    /// Query the filesystem for a child node given its parent inode number and name.
    pub fn lookup(&self, parent: INo, path: &PathView) -> SsfsResult<INodeHandle> {
        match self.get_inode(parent).map_err(|e| e.into())? {
            SsfsOk::Resolved(parent_handle) => {
                if parent_handle.kind != INodeKind::Directory {
                    return Err(SsfsResolutionError::EntryIsNotDirectory);
                }

                let children_state = self.nodes.read_sync(&parent, |_, n| n.children.clone());

                match children_state {
                    Some(DirChildren::Populated(map)) => {
                        debug!(parent, ?path, "lookup: cache hit, direct child lookup");
                        match map.get(path).copied() {
                            Some(child_ino) => self.get_inode(child_ino).map_err(|e| e.into()),
                            None => Err(SsfsResolutionError::DoesNotExist),
                        }
                    },
                    Some(DirChildren::Unpopulated) => {
                        debug!(parent, ?path, "lookup: children unpopulated, fetching from backend");
                        match self.initiate_readdir(parent)? {
                            SsfsOk::Resolved(_) => {
                                // Children now populated inline — look up the child.
                                let maybe_child_ino = self.nodes.read_sync(&parent, |_, n| {
                                    match &n.children {
                                        DirChildren::Populated(m) => m.get(path).copied(),
                                        _ => None,
                                    }
                                }).flatten();
                                match maybe_child_ino {
                                    Some(child_ino) => self.get_inode(child_ino).map_err(|e| e.into()),
                                    None => Err(SsfsResolutionError::DoesNotExist),
                                }
                            },
                            SsfsOk::Future(readdir_fut) => {
                                let nodes = Arc::clone(&self.nodes);
                                let path = path.to_owned();
                                Ok(SsfsOk::Future(Box::pin(async move {
                                    readdir_fut.await?;
                                    let maybe_child_ino = nodes.read_sync(&parent, |_, n| {
                                        match &n.children {
                                            DirChildren::Populated(m) => m.get(&path).copied(),
                                            _ => None,
                                        }
                                    }).flatten();
                                    match maybe_child_ino {
                                        Some(child_ino) => {
                                            nodes.read_sync(&child_ino, |_, inode| inode.handle())
                                                .ok_or(SsfsResolutionError::DoesNotExist)
                                        },
                                        None => Err(SsfsResolutionError::DoesNotExist),
                                    }
                                })))
                            },
                        }
                    },
                    Some(DirChildren::NotADirectory) => Err(SsfsResolutionError::EntryIsNotDirectory),
                    None => Err(SsfsResolutionError::DoesNotExist),
                }
            },
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
                    let maybe_child_ino = nodes.read_sync(&parent_handle.ino, |_, parent_inode| {
                        match &parent_inode.children {
                            DirChildren::Populated(map) => map.get(&path).copied(),
                            _ => None,
                        }
                    }).flatten();

                    match maybe_child_ino {
                        Some(child_ino) => {
                            nodes.read_sync(&child_ino, |_, inode| inode.handle())
                                .ok_or(SsfsResolutionError::DoesNotExist)
                        },
                        None => Err(SsfsResolutionError::DoesNotExist),
                    }
                };

                Ok(SsfsOk::Future(Box::pin(fut)))
            },
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
                Ok(nodes.read_sync(&ino, |_, inode| inode.handle())
                    .expect("INode should exist after pending update"))
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
    /// No caching -- always fetches from the backend.
    pub fn read(&self, ino: INo) -> SsfsResult<Vec<u8>> {
        let handle = self.nodes.read_sync(&ino, |_, inode| inode.handle());
        let handle = match handle {
            Some(h) => h,
            None => return Err(SsfsResolutionError::DoesNotExist),
        };

        if handle.kind != INodeKind::File {
            return Err(SsfsResolutionError::EntryIsNotFile);
        }

        let path = match self.abspath(ino) {
            Some(p) => p,
            None => return Err(SsfsResolutionError::DoesNotExist),
        };

        let backend = Arc::clone(&self.backend);
        Ok(SsfsOk::Future(Box::pin(async move {
            backend.read_file(&path).await.map_err(|e| match e {
                SsfsBackendError::NotFound => SsfsResolutionError::DoesNotExist,
                SsfsBackendError::Io(_) => SsfsResolutionError::IoError,
            })
        })))
    }
}
