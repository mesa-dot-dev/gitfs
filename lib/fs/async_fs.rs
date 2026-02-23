//! Async `INode` Table which supports concurrent access and modification.

use std::ffi::{OsStr, OsString};
use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;
use tokio::sync::Semaphore;

use crate::cache::async_backed::FutureBackedCache;
use crate::drop_ward::StatelessDrop;
use crate::fs::{
    AsyncFsStats, DirEntry, FileHandle, INode, INodeType, InodeAddr, LoadedAddr, OpenFlags,
    dcache::DCache,
};

/// The concrete type of the lookup cache used by [`AsyncFs`].
///
/// Keyed by `(parent_addr, child_name)`, valued by the resolved `INode`.
/// Exposed as a type alias so [`InodeForget`] can include it in its
/// `StatelessDrop` context without repeating the full generic signature.
pub type LookupCache = FutureBackedCache<(InodeAddr, Arc<OsStr>), INode>;

type LookupKey = (InodeAddr, Arc<OsStr>);

/// A reverse-index entry: the lookup-cache key plus the child inode addr
/// that the key resolved to. Storing the child addr allows [`evict_addr`]
/// to clean both the parent and child sides of the reverse index without
/// needing to read the (already-removed) cache value.
type ReverseEntry = (LookupKey, InodeAddr);

/// Wraps a [`LookupCache`] with a reverse index for O(k) eviction.
///
/// The reverse index maps each `InodeAddr` to the set of lookup-cache
/// keys that reference it (either as parent or as child). This avoids
/// the O(N) `retain_sync` scan that would otherwise be required when
/// evicting a single inode.
///
/// Unlike [`DCache`](super::dcache::DCache)'s 1:1 `child_to_parent`
/// reverse index (where each child has exactly one parent), the lookup
/// cache maps one inode address to *multiple* cache keys (because an
/// inode can appear as both a parent and a child in different entries).
/// Hence we use `Vec<ReverseEntry>` rather than a flat map.
pub struct IndexedLookupCache {
    cache: LookupCache,
    /// addr → set of `(key, child_addr)` pairs where `addr` appears as
    /// parent or child.
    reverse: scc::HashMap<InodeAddr, Vec<ReverseEntry>>,
}

impl Default for IndexedLookupCache {
    fn default() -> Self {
        Self {
            cache: LookupCache::default(),
            reverse: scc::HashMap::new(),
        }
    }
}

impl IndexedLookupCache {
    /// Delegate to the inner cache's `get_or_try_init`, then record the
    /// result in the reverse index.
    pub async fn get_or_try_init<F, Fut>(
        &self,
        key: LookupKey,
        factory: F,
    ) -> Result<INode, std::io::Error>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<INode, std::io::Error>> + Send + 'static,
    {
        let child = self.cache.get_or_try_init(key.clone(), factory).await?;
        self.index_entry(&key, child.addr);
        Ok(child)
    }

    /// Remove a single key from the cache and its reverse-index entries.
    pub fn remove_sync(&self, key: &LookupKey) {
        if self.cache.remove_sync(key) {
            self.deindex_key(key);
        }
    }

    /// Remove all lookup-cache entries referencing `addr` (as parent or child).
    ///
    /// O(k) where k is the number of entries referencing `addr`, vs the
    /// previous O(N) scan over the entire cache.
    pub fn evict_addr(&self, addr: InodeAddr) {
        let entries = self
            .reverse
            .remove_sync(&addr)
            .map(|(_, entries)| entries)
            .unwrap_or_default();

        for (key, child_addr) in &entries {
            self.cache.remove_sync(key);
            let (parent_addr, _) = key;
            // Clean the *other* side(s) of the reverse index.
            // We removed `addr`'s Vec already; now prune the key from
            // whichever other addrs it was indexed under.
            if *parent_addr != addr {
                self.reverse.update_sync(parent_addr, |_, v| {
                    v.retain(|(k, _)| k != key);
                });
            }
            if *child_addr != addr && *child_addr != *parent_addr {
                self.reverse.update_sync(child_addr, |_, v| {
                    v.retain(|(k, _)| k != key);
                });
            }
        }
    }

    /// Record a lookup entry in the reverse index for both parent and child addrs.
    ///
    /// Deduplicates: if the key is already present in the `Vec` for a given
    /// addr, the push is skipped. This prevents unbounded growth when the
    /// same key is looked up repeatedly (cache hits still call this method
    /// because the `FutureBackedCache` joiner path returns without
    /// distinguishing hits from misses).
    fn index_entry(&self, key: &LookupKey, child_addr: InodeAddr) {
        let entry = (key.clone(), child_addr);
        let (parent_addr, _) = key;
        // Index under parent addr (deduplicated).
        self.reverse
            .entry_sync(*parent_addr)
            .or_default()
            .get_mut()
            .dedup_push(&entry);
        // Index under child addr (if different from parent, deduplicated).
        if child_addr != *parent_addr {
            self.reverse
                .entry_sync(child_addr)
                .or_default()
                .get_mut()
                .dedup_push(&entry);
        }
    }

    /// Returns the number of entries in the reverse-index `Vec` for `addr`.
    ///
    /// Intended for testing only — verifies that the reverse index stays
    /// bounded and does not accumulate duplicates.
    #[doc(hidden)]
    #[must_use]
    pub fn reverse_entry_count(&self, addr: InodeAddr) -> usize {
        self.reverse
            .read_sync(&addr, |_, entries| entries.len())
            .unwrap_or(0)
    }

    /// Remove a single key's entries from the reverse index.
    ///
    /// Cleans the parent side. The child side cannot be cleaned here because
    /// the cache value (which held the child addr) has already been removed.
    /// This is acceptable: orphaned child-side entries are harmless (they
    /// reference a key that no longer exists in the cache) and are cleaned
    /// up when the child addr is eventually evicted via [`evict_addr`].
    fn deindex_key(&self, key: &LookupKey) {
        let (parent_addr, _) = key;
        self.reverse.update_sync(parent_addr, |_, entries| {
            entries.retain(|(k, _)| k != key);
        });
    }
}

/// Extension trait for `Vec` to push only if the element is not already present.
trait DedupPush<T: PartialEq> {
    fn dedup_push(&mut self, item: &T)
    where
        T: Clone;
}

impl<T: PartialEq + Clone> DedupPush<T> for Vec<T> {
    fn dedup_push(&mut self, item: &T)
    where
        T: Clone,
    {
        if !self.contains(item) {
            self.push(item.clone());
        }
    }
}

/// A reader for an open file, returned by [`FsDataProvider::open`].
///
/// Implementors provide the actual data for read operations. The FUSE
/// adapter calls [`close`](Self::close) to release resources explicitly.
pub trait FileReader: Send + Sync + 'static {
    /// Read up to `size` bytes starting at byte `offset`.
    fn read(
        &self,
        offset: u64,
        size: u32,
    ) -> impl Future<Output = Result<Bytes, std::io::Error>> + Send;

    /// Release any resources held by this reader.
    ///
    /// Called explicitly by the FUSE adapter during `release`. Implementations
    /// that hold inner file handles should release them here. The default
    /// implementation is a no-op.
    fn close(&self) -> impl Future<Output = Result<(), std::io::Error>> + Send {
        async { Ok(()) }
    }
}

/// A data provider for [`AsyncFs`] that fetches inode data on cache misses.
pub trait FsDataProvider: Clone + Send + Sync + 'static {
    /// The reader type returned by [`open`](Self::open).
    type Reader: FileReader;

    /// Look up a child inode by name within the given parent directory.
    fn lookup(
        &self,
        parent: INode,
        name: &OsStr,
    ) -> impl Future<Output = Result<INode, std::io::Error>> + Send;

    /// List all children of a directory.
    ///
    /// Called by [`AsyncFs::readdir`] on a cache miss. The returned
    /// children are inserted into the directory cache and inode table
    /// so subsequent reads are served from cache.
    fn readdir(
        &self,
        parent: INode,
    ) -> impl Future<Output = Result<Vec<(OsString, INode)>, std::io::Error>> + Send;

    /// Open a file and return a reader for subsequent read calls.
    fn open(
        &self,
        inode: INode,
        flags: OpenFlags,
    ) -> impl Future<Output = Result<Self::Reader, std::io::Error>> + Send;

    /// Clean up provider-internal state for an evicted inode.
    ///
    /// The `DropWard`/`InodeForget` system automatically removes inodes from
    /// the shared `inode_table` when the FUSE refcount reaches zero, but data
    /// providers often maintain auxiliary structures (path maps, bridge maps)
    /// that also need cleanup. This method is that extension point.
    ///
    /// Never called directly -- [`InodeForget::delete`] invokes it
    /// automatically when the refcount drops to zero.
    fn forget(&self, _addr: InodeAddr) {}
}

/// Zero-sized cleanup tag for inode eviction.
///
/// The [`StatelessDrop`] implementations on this type evict inodes from the
/// inode table and, when a data provider is present, delegate to
/// [`FsDataProvider::forget`] so the provider can clean up its own auxiliary
/// structures (path maps, bridge maps, etc.).
pub struct InodeForget;

/// Evicts the inode from the table only. Used when no data provider is available.
impl StatelessDrop<Arc<FutureBackedCache<InodeAddr, INode>>, InodeAddr> for InodeForget {
    fn delete(inode_table: &Arc<FutureBackedCache<InodeAddr, INode>>, addr: &InodeAddr) {
        inode_table.remove_sync(addr);
    }
}

/// Evicts the inode from the table, directory cache, and lookup cache, then
/// delegates to [`FsDataProvider::forget`] so the provider can clean up its
/// own auxiliary state.
///
/// The lookup cache cleanup removes all entries referencing the forgotten
/// inode (as parent or child) via the [`IndexedLookupCache`]'s reverse
/// index, ensuring O(k) eviction instead of O(N) full-cache scan.
impl<DP: FsDataProvider>
    StatelessDrop<
        (
            Arc<FutureBackedCache<InodeAddr, INode>>,
            Arc<DCache>,
            Arc<IndexedLookupCache>,
            DP,
        ),
        InodeAddr,
    > for InodeForget
{
    fn delete(
        ctx: &(
            Arc<FutureBackedCache<InodeAddr, INode>>,
            Arc<DCache>,
            Arc<IndexedLookupCache>,
            DP,
        ),
        key: &InodeAddr,
    ) {
        let addr = *key;
        ctx.0.remove_sync(key);
        ctx.1.evict(LoadedAddr::new_unchecked(addr));
        ctx.2.evict_addr(addr);
        ctx.3.forget(addr);
    }
}

/// A looked-up inode returned by [`AsyncFs::lookup`].
///
/// Each `ResolvedINode` returned by lookup represents one reference that
/// the FUSE kernel holds. The caller must balance it by decrementing the
/// [`InodeLifecycle`] ward when the kernel sends `forget`.
#[derive(Debug, Clone, Copy)]
pub struct ResolvedINode {
    /// The resolved inode data.
    pub inode: INode,
}

/// An open file that provides read access.
///
/// Returned by [`AsyncFs::open`]. The caller owns this handle and uses
/// [`read`](Self::read) to fetch data. Dropping the handle releases
/// the underlying reader when the last `Arc` clone is gone.
#[derive(Debug, Clone)]
pub struct OpenFile<R> {
    /// The raw file handle number, suitable for returning to the FUSE kernel.
    pub fh: FileHandle,
    /// The reader backing this open file.
    pub reader: Arc<R>,
}

impl<R: FileReader> OpenFile<R> {
    /// Read up to `size` bytes starting at byte `offset`.
    pub async fn read(&self, offset: u64, size: u32) -> Result<Bytes, std::io::Error> {
        self.reader.read(offset, size).await
    }
}

/// Co-located inode table and reference-count ward.
///
/// When `dec` reaches zero for a key, [`InodeForget::delete`] synchronously
/// removes that inode from the table.
pub struct InodeLifecycle {
    table: Arc<FutureBackedCache<InodeAddr, INode>>,
    ward: crate::drop_ward::DropWard<
        Arc<FutureBackedCache<InodeAddr, INode>>,
        InodeAddr,
        InodeForget,
    >,
}

impl InodeLifecycle {
    /// Create a new lifecycle managing the given inode table.
    pub fn from_table(table: Arc<FutureBackedCache<InodeAddr, INode>>) -> Self {
        let ward = crate::drop_ward::DropWard::new(Arc::clone(&table));
        Self { table, ward }
    }

    /// Increment the reference count for an inode address.
    pub fn inc(&mut self, addr: InodeAddr) -> usize {
        self.ward.inc(addr)
    }

    /// Decrement the reference count for an inode address.
    ///
    /// When the count reaches zero, the inode is automatically evicted
    /// from the table via [`InodeForget::delete`].
    pub fn dec(&mut self, addr: &InodeAddr) -> Option<usize> {
        self.ward.dec(addr)
    }

    /// Decrement the reference count by `count`.
    ///
    /// When the count reaches zero, the inode is automatically evicted.
    pub fn dec_count(&mut self, addr: &InodeAddr, count: usize) -> Option<usize> {
        self.ward.dec_count(addr, count)
    }

    /// Read-only access to the underlying inode table.
    #[must_use]
    pub fn table(&self) -> &FutureBackedCache<InodeAddr, INode> {
        &self.table
    }
}

/// RAII guard that calls [`DCache::abort_populate`] on drop unless defused.
///
/// Prevents the populate flag from getting stuck in `IN_PROGRESS` if the
/// populating future is cancelled (e.g. by a FUSE interrupt or `select!`).
struct PopulateGuard<'a> {
    dcache: &'a DCache,
    parent: LoadedAddr,
    armed: bool,
}

impl<'a> PopulateGuard<'a> {
    fn new(dcache: &'a DCache, parent: LoadedAddr) -> Self {
        Self {
            dcache,
            parent,
            armed: true,
        }
    }

    /// Defuse the guard after a successful `finish_populate`.
    fn defuse(&mut self) {
        self.armed = false;
    }
}

impl Drop for PopulateGuard<'_> {
    /// Fires when the populating future is cancelled before [`defuse`](Self::defuse)
    /// is called, resetting the dcache populate flag from `IN_PROGRESS` back to
    /// `UNCLAIMED` so a subsequent `readdir` can retry. This is a normal
    /// occurrence under FUSE interrupts or `tokio::select!` cancellation —
    /// not an error.
    fn drop(&mut self) {
        if self.armed {
            self.dcache.abort_populate(self.parent);
        }
    }
}

/// Background-populate a single child directory into the caches.
///
/// Uses the same CAS gate as `readdir` so duplicate work is impossible.
/// Errors are silently ignored — prefetch is best-effort.
async fn prefetch_dir<DP: FsDataProvider>(
    dir_addr: LoadedAddr,
    directory_cache: Arc<DCache>,
    inode_table: Arc<FutureBackedCache<InodeAddr, INode>>,
    data_provider: DP,
) {
    use crate::fs::dcache::PopulateStatus;

    let claim_gen = match directory_cache.try_claim_populate(dir_addr) {
        PopulateStatus::Claimed(claim_gen) => claim_gen,
        PopulateStatus::InProgress | PopulateStatus::Done => return,
    };

    let mut guard = PopulateGuard::new(&directory_cache, dir_addr);

    let Some(dir_inode) = inode_table.get(&dir_addr.addr()).await else {
        return;
    };

    let Ok(children) = data_provider.readdir(dir_inode).await else {
        return;
    };

    for (name, child_inode) in children {
        let is_dir = child_inode.itype == INodeType::Directory;
        inode_table
            .get_or_init(child_inode.addr, || async move { child_inode })
            .await;
        directory_cache.insert(
            dir_addr,
            name,
            LoadedAddr::new_unchecked(child_inode.addr),
            is_dir,
        );
    }
    directory_cache.finish_populate(dir_addr, claim_gen);
    guard.defuse();
}

/// Maximum number of concurrent prefetch tasks spawned per [`AsyncFs`] instance.
///
/// Prevents thundering-herd API calls when a parent directory contains many
/// subdirectories (e.g. `node_modules`). Each `readdir` that discovers child
/// directories spawns at most this many concurrent prefetch tasks; additional
/// children wait for a permit.
const MAX_PREFETCH_CONCURRENCY: usize = 8;

/// An asynchronous filesystem cache mapping `InodeAddr` to `INode`.
///
/// Uses two [`FutureBackedCache`] layers:
/// - `inode_table` stores resolved inodes by address, used by [`loaded_inode`](Self::loaded_inode).
/// - `lookup_cache` stores lookup results by `(parent_addr, name)`, ensuring `dp.lookup()` is only
///   called on a true cache miss (not already cached or in-flight).
///
/// The [`DCache`] sits in front as a synchronous fast path mapping `(parent, name)` to child addr.
///
/// **Known limitation:** Both `inode_table` and `lookup_cache` grow monotonically — entries are
/// only removed when FUSE sends `forget`, which may never happen for long-lived mounts or
/// recursive traversals (e.g. `find`, `tree`). Under sustained traversal the memory footprint
/// grows without bound. Adding LRU or TTL-based eviction to these caches is a planned
/// improvement.
pub struct AsyncFs<DP: FsDataProvider> {
    /// Canonical addr -> `INode` map. Used by `loaded_inode()` to retrieve inodes by address.
    inode_table: Arc<FutureBackedCache<InodeAddr, INode>>,

    /// Deduplicating lookup cache keyed by `(parent_addr, child_name)`. The factory is
    /// `dp.lookup()`, so the data provider is only called on a true cache miss.
    ///
    /// Uses [`IndexedLookupCache`] with a reverse index for O(k) eviction
    /// instead of O(N) full-cache scans. Wrapped in `Arc` so that
    /// [`InodeForget`] can include it in its `StatelessDrop` context.
    lookup_cache: Arc<IndexedLookupCache>,

    /// Directory entry cache, mapping `(parent, name)` to child inode address.
    directory_cache: Arc<DCache>,

    /// The data provider used to fetch inode data on cache misses.
    data_provider: DP,

    /// Monotonically increasing file handle counter. Starts at 1 (0 is reserved).
    next_fh: AtomicU64,

    /// Bounds the number of concurrent background prefetch tasks.
    prefetch_semaphore: Arc<Semaphore>,
}

impl<DP: FsDataProvider> AsyncFs<DP> {
    /// Create a new `AsyncFs`, seeding the root inode into the table.
    pub async fn new(
        data_provider: DP,
        root: INode,
        inode_table: Arc<FutureBackedCache<InodeAddr, INode>>,
    ) -> Self {
        inode_table
            .get_or_init(root.addr, || async move { root })
            .await;

        Self {
            inode_table,
            lookup_cache: Arc::new(IndexedLookupCache::default()),
            directory_cache: Arc::new(DCache::new()),
            data_provider,
            next_fh: AtomicU64::new(1),
            prefetch_semaphore: Arc::new(Semaphore::new(MAX_PREFETCH_CONCURRENCY)),
        }
    }

    /// Create a new `AsyncFs`, assuming the root inode is already in the table.
    ///
    /// The caller must ensure the root inode has already been inserted into
    /// `inode_table` (e.g. via [`FutureBackedCache::insert_sync`]).
    #[must_use]
    pub fn new_preseeded(
        data_provider: DP,
        inode_table: Arc<FutureBackedCache<InodeAddr, INode>>,
    ) -> Self {
        Self {
            inode_table,
            lookup_cache: Arc::new(IndexedLookupCache::default()),
            directory_cache: Arc::new(DCache::new()),
            data_provider,
            next_fh: AtomicU64::new(1),
            prefetch_semaphore: Arc::new(Semaphore::new(MAX_PREFETCH_CONCURRENCY)),
        }
    }

    /// Spawn background tasks to prefetch each child directory of `parent`.
    ///
    /// Concurrency is bounded by [`MAX_PREFETCH_CONCURRENCY`] via a shared
    /// semaphore, preventing thundering-herd API calls when a parent
    /// directory contains many subdirectories.
    fn spawn_prefetch_children(&self, parent: LoadedAddr) {
        let child_dirs = self.directory_cache.child_dir_addrs(parent);
        for child_addr in child_dirs {
            let sem = Arc::clone(&self.prefetch_semaphore);
            let dcache = Arc::clone(&self.directory_cache);
            let table = Arc::clone(&self.inode_table);
            let dp = self.data_provider.clone();
            tokio::spawn(async move {
                let Ok(_permit) = sem.acquire().await else {
                    return;
                };
                prefetch_dir(child_addr, dcache, table, dp).await;
            });
        }
    }

    /// Returns a clone of the directory cache handle.
    ///
    /// Used by the FUSE adapter to pass the cache into the [`DropWard`]
    /// context so that [`InodeForget`] can evict stale entries when the
    /// kernel forgets an inode.
    #[must_use]
    pub fn directory_cache(&self) -> Arc<DCache> {
        Arc::clone(&self.directory_cache)
    }

    /// Returns a clone of the lookup cache handle.
    ///
    /// Used by the FUSE adapter to pass the cache into the [`DropWard`]
    /// context so that [`InodeForget`] can clean up stale
    /// `(parent, name) → INode` entries when the kernel forgets an inode.
    #[must_use]
    pub fn lookup_cache(&self) -> Arc<IndexedLookupCache> {
        Arc::clone(&self.lookup_cache)
    }

    /// Get the total number of inodes currently stored in the inode table.
    #[must_use]
    pub fn inode_count(&self) -> usize {
        self.inode_table.len()
    }

    /// Return filesystem statistics.
    ///
    /// Reports the current inode count from the cache. Block-related
    /// fields default to values appropriate for a virtual read-only
    /// filesystem (4 KiB blocks, no free space).
    #[must_use]
    pub fn statfs(&self) -> AsyncFsStats {
        AsyncFsStats {
            block_size: 4096,
            total_blocks: 0,
            free_blocks: 0,
            available_blocks: 0,
            total_inodes: self.inode_count() as u64,
            free_inodes: 0,
            max_filename_length: 255,
        }
    }

    /// Asynchronously look up an inode by name within a parent directory.
    ///
    /// Resolution order:
    /// 1. Directory cache (synchronous fast path)
    /// 2. Lookup cache (`get_or_try_init` — calls `dp.lookup()` only on a true miss)
    /// 3. On success, populates inode table and directory cache
    pub async fn lookup(
        &self,
        parent: LoadedAddr,
        name: &OsStr,
    ) -> Result<ResolvedINode, std::io::Error> {
        let parent_ino = self.loaded_inode(parent).await?;
        debug_assert!(
            matches!(parent_ino.itype, INodeType::Directory),
            "parent inode should be a directory"
        );

        if let Some(dentry) = self.directory_cache.lookup(parent, name) {
            if let Some(inode) = self.inode_table.get(&dentry.ino.addr()).await {
                return Ok(ResolvedINode { inode });
            }
            // Inode was evicted (e.g. by forget). Evict the stale lookup_cache
            // entry so the slow path calls dp.lookup() fresh.
            self.lookup_cache
                .remove_sync(&(parent.addr(), Arc::from(name)));
        }

        // Note: get_or_try_init deduplicates successful lookups but NOT
        // failures. Under transient API errors, concurrent lookups for
        // the same (parent, name) may each independently call dp.lookup().
        // This is acceptable: the cost of a redundant API call on error is
        // low compared to the complexity of error-channel deduplication.
        let name_arc: Arc<OsStr> = Arc::from(name);
        let lookup_key = (parent.addr(), Arc::clone(&name_arc));
        let dp = self.data_provider.clone();

        let child = self
            .lookup_cache
            .get_or_try_init(lookup_key, || {
                let name_for_dp = Arc::clone(&name_arc);
                async move { dp.lookup(parent_ino, &name_for_dp).await }
            })
            .await?;

        self.inode_table
            .get_or_init(child.addr, || async move { child })
            .await;

        self.directory_cache.insert(
            parent,
            name_arc.as_ref().to_os_string(),
            LoadedAddr::new_unchecked(child.addr),
            matches!(child.itype, INodeType::Directory),
        );

        Ok(ResolvedINode { inode: child })
    }

    /// Retrieve an inode that is expected to already be loaded.
    ///
    /// If the inode is currently in-flight (being loaded by another caller), this awaits
    /// completion. Returns an error if the inode is not in the table at all.
    pub async fn loaded_inode(&self, addr: LoadedAddr) -> Result<INode, std::io::Error> {
        self.inode_table.get(&addr.addr()).await.ok_or_else(|| {
            tracing::error!(
                inode = ?addr.addr(),
                "inode not found in table — this is a programming bug"
            );
            std::io::Error::from_raw_os_error(libc::ENOENT)
        })
    }

    /// Return the attributes of the inode at `addr`.
    ///
    /// This is the getattr entry point for the filesystem. Returns the
    /// cached [`INode`] directly — callers at the FUSE boundary are
    /// responsible for converting to `fuser::FileAttr`.
    pub async fn getattr(&self, addr: LoadedAddr) -> Result<INode, std::io::Error> {
        self.loaded_inode(addr).await
    }

    /// Open a file for reading.
    ///
    /// Validates the inode is not a directory, delegates to the data provider
    /// to create a [`FileReader`], and returns an [`OpenFile`] that the caller
    /// owns. Reads go through [`OpenFile::read`].
    pub async fn open(
        &self,
        addr: LoadedAddr,
        flags: OpenFlags,
    ) -> Result<OpenFile<DP::Reader>, std::io::Error> {
        let inode = self.loaded_inode(addr).await?;
        if inode.itype == INodeType::Directory {
            return Err(std::io::Error::from_raw_os_error(libc::EISDIR));
        }
        let reader = self.data_provider.open(inode, flags).await?;
        let fh = self.next_fh.fetch_add(1, Ordering::Relaxed);
        Ok(OpenFile {
            fh,
            reader: Arc::new(reader),
        })
    }

    /// Evict an inode from the inode table and notify the data provider.
    ///
    /// Called by the composite layer when propagating `forget` to a child
    /// filesystem. Removes the inode from the table and calls
    /// [`FsDataProvider::forget`] so the provider can clean up auxiliary
    /// structures (path maps, etc.).
    pub fn evict(&self, addr: InodeAddr) {
        self.inode_table.remove_sync(&addr);
        self.directory_cache.evict(LoadedAddr::new_unchecked(addr));
        self.lookup_cache.evict_addr(addr);
        self.data_provider.forget(addr);
    }

    /// Iterate directory entries for `parent`, starting from `offset`.
    ///
    /// On the first call for a given parent, fetches the directory listing
    /// from the data provider and populates the directory cache and inode
    /// table. Subsequent calls serve entries directly from cache.
    ///
    /// Entries are yielded in name-sorted order. For each entry, `filler` is
    /// called with the [`DirEntry`] and the next offset value. If `filler`
    /// returns `true` (indicating the caller's buffer is full), iteration
    /// stops early.
    ///
    pub async fn readdir(
        &self,
        parent: LoadedAddr,
        offset: u64,
        mut filler: impl FnMut(DirEntry<'_>, u64) -> bool,
    ) -> Result<(), std::io::Error> {
        use crate::fs::dcache::PopulateStatus;

        let parent_inode = self.loaded_inode(parent).await?;
        if parent_inode.itype != INodeType::Directory {
            return Err(std::io::Error::from_raw_os_error(libc::ENOTDIR));
        }

        // Populate the directory cache on first readdir for this parent.
        // Uses a three-state CAS gate to prevent duplicate dp.readdir() calls.
        loop {
            match self.directory_cache.try_claim_populate(parent) {
                PopulateStatus::Claimed(claim_gen) => {
                    // RAII guard: if this future is cancelled between Claimed
                    // and finish_populate, automatically abort so other waiters
                    // can retry instead of hanging forever.
                    let mut guard = PopulateGuard::new(&self.directory_cache, parent);

                    let children = self.data_provider.readdir(parent_inode).await?;
                    for (name, child_inode) in children {
                        self.inode_table
                            .get_or_init(child_inode.addr, || async move { child_inode })
                            .await;
                        self.directory_cache.insert(
                            parent,
                            name,
                            LoadedAddr::new_unchecked(child_inode.addr),
                            child_inode.itype == INodeType::Directory,
                        );
                    }
                    self.directory_cache.finish_populate(parent, claim_gen);
                    guard.defuse();
                    self.spawn_prefetch_children(parent);
                    break;
                }
                PopulateStatus::InProgress => {
                    self.directory_cache.wait_populated(parent).await;
                    // Re-check: the populator may have aborted.
                }
                PopulateStatus::Done => break,
            }
        }

        #[expect(
            clippy::cast_possible_truncation,
            reason = "offset fits in usize on supported 64-bit platforms"
        )]
        let skip = offset as usize;

        // Collect only entries at or past `offset`, avoiding clones for
        // entries that will be skipped during paginated readdir.
        let mut entries: Vec<(OsString, LoadedAddr)> = Vec::new();
        let mut idx = 0usize;
        self.directory_cache.readdir(parent, |name, dvalue| {
            if idx >= skip {
                entries.push((name.to_os_string(), dvalue.ino));
            }
            idx += 1;
        });

        for (i, (name, child_addr)) in entries.iter().enumerate() {
            let Some(inode) = self.inode_table.get(&child_addr.addr()).await else {
                // Inode was evicted between readdir collection and iteration
                // (e.g. by a concurrent forget). Skip the stale entry.
                tracing::debug!(addr = ?child_addr.addr(), name = ?name, "inode evicted during readdir, skipping");
                continue;
            };
            let next_offset = (skip + i + 1) as u64;
            if filler(DirEntry { name, inode }, next_offset) {
                break;
            }
        }

        Ok(())
    }
}
