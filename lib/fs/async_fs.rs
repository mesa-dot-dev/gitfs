//! Async `INode` Table which supports concurrent access and modification.

use std::ffi::{OsStr, OsString};
use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;
use tokio::sync::{Mutex, Semaphore};

use crate::cache::async_backed::FutureBackedCache;
use crate::drop_ward::StatelessDrop;
use crate::fs::{
    AsyncFsStats, DirEntry, FileHandle, INode, INodeType, InodeAddr, LoadedAddr, OpenFlags,
    dcache::DCache, indexed_lookup_cache::IndexedLookupCache,
};

/// The concrete type of the lookup cache used by [`AsyncFs`].
///
/// Keyed by `(parent_addr, child_name)`, valued by the resolved `INode`.
/// Exposed as a type alias so [`InodeForget`] can include it in its
/// `StatelessDrop` context without repeating the full generic signature.
pub type LookupCache = FutureBackedCache<(InodeAddr, Arc<OsStr>), INode>;

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

    /// Sync the full file content to the remote backend.
    ///
    /// Called by `AsyncFs::write` with `offset = 0` and the complete merged
    /// file content after the overlay has been updated. The `offset` parameter
    /// is reserved for future partial-write support but is currently always 0.
    ///
    /// The default implementation returns `EROFS` (read-only filesystem).
    /// Providers that support writes should override this.
    fn write(
        &self,
        _inode: INode,
        _offset: u64,
        _data: Bytes,
    ) -> impl Future<Output = Result<u32, std::io::Error>> + Send {
        async { Err(std::io::Error::from_raw_os_error(libc::EROFS)) }
    }

    /// Update file attributes (size, timestamps).
    ///
    /// The default implementation returns `EROFS` (read-only filesystem).
    /// Providers that support attribute changes should override this.
    fn setattr(
        &self,
        _inode: INode,
        _size: Option<u64>,
        _atime: Option<std::time::SystemTime>,
        _mtime: Option<std::time::SystemTime>,
    ) -> impl Future<Output = Result<INode, std::io::Error>> + Send {
        async { Err(std::io::Error::from_raw_os_error(libc::EROFS)) }
    }

    /// Create a new file in the given parent directory.
    ///
    /// Returns the inode for the newly created file. The default
    /// implementation returns `EROFS` (read-only filesystem).
    fn create(
        &self,
        _parent: INode,
        _name: &OsStr,
        _mode: u32,
    ) -> impl Future<Output = Result<INode, std::io::Error>> + Send {
        async { Err(std::io::Error::from_raw_os_error(libc::EROFS)) }
    }

    /// Remove a file from the given parent directory.
    ///
    /// The default implementation returns `EROFS` (read-only filesystem).
    /// Providers that support deletion should override this.
    fn unlink(
        &self,
        _parent: INode,
        _name: &OsStr,
    ) -> impl Future<Output = Result<(), std::io::Error>> + Send {
        async { Err(std::io::Error::from_raw_os_error(libc::EROFS)) }
    }

    /// Returns the shared directory cache, if the provider owns one.
    ///
    /// Providers that need to share their [`DCache`] with the wrapping
    /// [`AsyncFs`] (e.g. for ignore-rule checking) override this to return
    /// their `Arc<DCache>`. The default returns `None`, causing
    /// [`AsyncFs`] to create its own cache.
    fn dcache(&self) -> Option<Arc<DCache>> {
        None
    }
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

/// Context for inode eviction when a data provider is available.
///
/// Groups the shared caches and provider into a named struct so that
/// [`InodeForget::delete`] reads clearly instead of using positional
/// tuple fields (`ctx.0`, `ctx.1`, etc.).
pub struct ForgetContext<DP: FsDataProvider> {
    /// Canonical addr → `INode` map.
    pub inode_table: Arc<FutureBackedCache<InodeAddr, INode>>,
    /// Directory entry cache.
    pub dcache: Arc<DCache>,
    /// Reverse-indexed lookup cache.
    pub lookup_cache: Arc<IndexedLookupCache>,
    /// The data provider for provider-specific cleanup.
    pub provider: DP,
    /// Write overlay — inodes present here must not be evicted.
    pub write_overlay: Arc<FutureBackedCache<InodeAddr, Bytes>>,
    /// Inodes removed via `unlink` — on forget, these get full cleanup
    /// including `write_overlay` removal.
    pub unlinked_inodes: Arc<scc::HashSet<InodeAddr>>,
}

/// Evicts the inode from the table, directory cache, and lookup cache, then
/// delegates to [`FsDataProvider::forget`] so the provider can clean up its
/// own auxiliary state.
///
/// Inodes that have locally-written data (present in `write_overlay`) are
/// skipped — they must persist for the lifetime of the mount.
///
/// The lookup cache cleanup removes all entries referencing the forgotten
/// inode (as parent or child) via the [`IndexedLookupCache`]'s reverse
/// index, ensuring O(k) eviction instead of O(N) full-cache scan.
impl<DP: FsDataProvider> StatelessDrop<ForgetContext<DP>, InodeAddr> for InodeForget {
    fn delete(ctx: &ForgetContext<DP>, key: &InodeAddr) {
        let is_unlinked = ctx.unlinked_inodes.remove_sync(key).is_some();
        if is_unlinked {
            // Unlinked inode: FUSE refcount hit zero, safe to fully clean up.
            ctx.write_overlay.remove_sync(key);
            ctx.inode_table.remove_sync(key);
            ctx.dcache.evict(LoadedAddr::new_unchecked(*key));
            ctx.lookup_cache.evict_addr(*key);
            ctx.provider.forget(*key);
            return;
        }
        if ctx.write_overlay.contains_sync(key) {
            return;
        }
        let addr = *key;
        ctx.inode_table.remove_sync(key);
        ctx.dcache.evict(LoadedAddr::new_unchecked(addr));
        ctx.lookup_cache.evict_addr(addr);
        ctx.provider.forget(addr);
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

/// A file reader that checks the write overlay first, falling back to the
/// underlying provider reader.
pub struct OverlayReader<R: FileReader> {
    /// The inode address this reader is for.
    pub addr: InodeAddr,
    write_overlay: Arc<FutureBackedCache<InodeAddr, Bytes>>,
    inner: R,
}

impl<R: FileReader> std::fmt::Debug for OverlayReader<R> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OverlayReader")
            .field("addr", &self.addr)
            .finish_non_exhaustive()
    }
}

impl<R: FileReader> FileReader for OverlayReader<R> {
    #[expect(
        clippy::cast_possible_truncation,
        reason = "offset and size fit in usize on supported 64-bit platforms"
    )]
    async fn read(&self, offset: u64, size: u32) -> Result<Bytes, std::io::Error> {
        if let Some(data) = self.write_overlay.get(&self.addr).await {
            let start = (offset as usize).min(data.len());
            let end = (start + size as usize).min(data.len());
            return Ok(data.slice(start..end));
        }
        self.inner.read(offset, size).await
    }

    async fn close(&self) -> Result<(), std::io::Error> {
        self.inner.close().await
    }
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
    token: u64,
    armed: bool,
}

impl<'a> PopulateGuard<'a> {
    fn new(dcache: &'a DCache, parent: LoadedAddr, token: u64) -> Self {
        Self {
            dcache,
            parent,
            token,
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
            self.dcache.abort_populate(self.parent, self.token);
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

    let receipt = match directory_cache.try_claim_populate(dir_addr) {
        PopulateStatus::Claimed(receipt) => receipt,
        PopulateStatus::InProgress | PopulateStatus::Done => return,
    };

    let mut guard = PopulateGuard::new(&directory_cache, dir_addr, receipt.token);

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
    directory_cache.finish_populate(dir_addr, receipt);
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

    /// Overlay cache for locally-written file data.
    ///
    /// Keyed by inode address, valued by the full file content. Written
    /// entries are never evicted by `InodeForget` — they persist for the
    /// lifetime of the mount.
    write_overlay: Arc<FutureBackedCache<InodeAddr, Bytes>>,

    /// Per-inode write locks serializing the non-atomic read-modify-write
    /// on `write_overlay` in [`write`](Self::write) and
    /// [`setattr`](Self::setattr).
    write_locks: Arc<scc::HashMap<InodeAddr, Arc<Mutex<()>>>>,

    /// Inodes that have been unlinked but may still have open file handles.
    /// The `InodeForget` cleanup path checks this set and performs full
    /// eviction (`inode_table` + `write_overlay` removal) when the FUSE
    /// refcount drops to zero.
    unlinked_inodes: Arc<scc::HashSet<InodeAddr>>,
}

impl<DP: FsDataProvider> AsyncFs<DP> {
    /// Create a new `AsyncFs`, seeding the root inode into the table.
    pub async fn new(
        data_provider: DP,
        root: INode,
        inode_table: Arc<FutureBackedCache<InodeAddr, INode>>,
        directory_cache: Arc<DCache>,
    ) -> Self {
        inode_table
            .get_or_init(root.addr, || async move { root })
            .await;

        Self {
            inode_table,
            lookup_cache: Arc::new(IndexedLookupCache::default()),
            directory_cache,
            data_provider,
            next_fh: AtomicU64::new(1),
            prefetch_semaphore: Arc::new(Semaphore::new(MAX_PREFETCH_CONCURRENCY)),
            write_overlay: Arc::new(FutureBackedCache::default()),
            write_locks: Arc::new(scc::HashMap::new()),
            unlinked_inodes: Arc::new(scc::HashSet::new()),
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
        directory_cache: Arc<DCache>,
    ) -> Self {
        Self {
            inode_table,
            lookup_cache: Arc::new(IndexedLookupCache::default()),
            directory_cache,
            data_provider,
            next_fh: AtomicU64::new(1),
            prefetch_semaphore: Arc::new(Semaphore::new(MAX_PREFETCH_CONCURRENCY)),
            write_overlay: Arc::new(FutureBackedCache::default()),
            write_locks: Arc::new(scc::HashMap::new()),
            unlinked_inodes: Arc::new(scc::HashSet::new()),
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
    /// 1. Directory cache hit — returns immediately.
    /// 2. Directory cache miss in a fully-populated directory — returns `ENOENT`
    ///    immediately without hitting the remote data provider.
    /// 3. Lookup cache (`get_or_try_init` — calls `dp.lookup()` only on a true miss)
    /// 4. On success, populates inode table and directory cache
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
        } else if self.directory_cache.is_populated(parent) {
            // The directory has been fully populated (e.g. by readdir) and this
            // name was not found — the file does not exist. Short-circuit
            // without hitting the remote data provider.
            return Err(std::io::Error::from_raw_os_error(libc::ENOENT));
        }

        // Note: get_or_try_init deduplicates successful lookups but NOT
        // failures. Under transient API errors, concurrent lookups for
        // the same (parent, name) may each independently call dp.lookup().
        // This is acceptable: the cost of a redundant API call on error is
        // low compared to the complexity of error-channel deduplication.
        // NOTE(MES-853): Arc::from(name) allocates on every lookup call.
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
    /// to create a [`FileReader`], and returns an [`OpenFile`] wrapping an
    /// [`OverlayReader`] that checks the write overlay first, falling back
    /// to the provider reader.
    pub async fn open(
        &self,
        addr: LoadedAddr,
        flags: OpenFlags,
    ) -> Result<OpenFile<OverlayReader<DP::Reader>>, std::io::Error> {
        let inode = self.loaded_inode(addr).await?;
        if inode.itype == INodeType::Directory {
            return Err(std::io::Error::from_raw_os_error(libc::EISDIR));
        }
        let reader = self.data_provider.open(inode, flags).await?;
        let overlay_reader = OverlayReader {
            addr: addr.addr(),
            write_overlay: Arc::clone(&self.write_overlay),
            inner: reader,
        };
        let fh = self.next_fh.fetch_add(1, Ordering::Relaxed);
        Ok(OpenFile {
            fh,
            reader: Arc::new(overlay_reader),
        })
    }

    /// Acquire (or lazily create) the per-inode write lock.
    async fn inode_write_lock(&self, addr: InodeAddr) -> Arc<Mutex<()>> {
        let entry = self
            .write_locks
            .entry_async(addr)
            .await
            .or_insert_with(|| Arc::new(Mutex::new(())));
        Arc::clone(entry.get())
    }

    /// Write data to a file at the given offset.
    ///
    /// Stores the result in the write overlay cache. The overlay takes
    /// precedence over the data provider on subsequent reads. Also calls
    /// [`FsDataProvider::write`] so the provider can log or forward as needed.
    pub async fn write(
        &self,
        addr: LoadedAddr,
        offset: u64,
        data: Bytes,
    ) -> Result<u32, std::io::Error> {
        let inode = self.loaded_inode(addr).await?;
        if inode.itype == INodeType::Directory {
            return Err(std::io::Error::from_raw_os_error(libc::EISDIR));
        }

        #[expect(
            clippy::cast_possible_truncation,
            reason = "data.len() fits in u32 (FUSE writes are at most 128 KiB)"
        )]
        let bytes_written = data.len() as u32;

        // Acquire per-inode lock to serialize the read-modify-write on the overlay.
        let lock = self.inode_write_lock(addr.addr()).await;
        let guard = lock.lock().await;

        // Merge with existing overlay content, or seed from provider on
        // first write so pre-existing data is not lost.
        //
        // TODO(MES-829, MES-851): this copies the entire file on every write,
        // making writes O(file_size). Switch to Arc<Mutex<Vec<u8>>> or
        // BytesMut to mutate in place.
        let existing = self.write_overlay.get(&addr.addr()).await;
        let mut buf = if let Some(data) = existing {
            data.to_vec()
        } else {
            let read_size: u32 = inode.size.try_into().unwrap_or_else(|_| {
                tracing::error!(
                    addr = addr.addr(),
                    size = inode.size,
                    "file size exceeds u32::MAX, read will be truncated"
                );
                u32::MAX
            });
            let reader = self.data_provider.open(inode, OpenFlags::RDONLY).await?;
            let data = reader.read(0, read_size).await?;
            data.to_vec()
        };

        #[expect(
            clippy::cast_possible_truncation,
            reason = "offset fits in usize on supported 64-bit platforms"
        )]
        let offset_usize = offset as usize;
        if offset_usize > buf.len() {
            buf.resize(offset_usize, 0);
        }
        let end = offset_usize + data.len();
        if end > buf.len() {
            buf.resize(end, 0);
        }
        buf[offset_usize..end].copy_from_slice(&data);

        let new_content = Bytes::from(buf);
        let provider_content = Bytes::clone(&new_content);
        let new_size = new_content.len() as u64;
        self.write_overlay.insert_sync(addr.addr(), new_content);

        // Update inode size in the table.
        let mut updated = inode;
        updated.size = new_size;
        updated.last_modified_at = std::time::SystemTime::now();
        self.inode_table.insert_sync(addr.addr(), updated);

        // Notify the data provider with the full content while still
        // holding the per-inode write lock. This ensures MPSC ordering
        // is consistent with overlay ordering — a concurrent
        // setattr(truncate) cannot enqueue between our overlay update
        // and provider notification. For MPSC-based providers (e.g.
        // MesRepoProvider) this is non-blocking (just a channel send).
        // NOTE(MES-846): each FUSE write chunk queues a full-file upload;
        // the consumer should coalesce consecutive writes to the same path.
        if let Err(e) = self.data_provider.write(updated, 0, provider_content).await {
            tracing::warn!(
                addr = updated.addr,
                %e,
                "data provider write failed (overlay is authoritative)"
            );
        }

        drop(guard);

        Ok(bytes_written)
    }

    /// Create a new empty file without opening it.
    ///
    /// Performs all the metadata work of [`create`](Self::create) — provider
    /// call, inode table insert, directory cache insert, overlay init — but
    /// skips the `open()` step. Used by [`CompositeFs`] to avoid a
    /// redundant double-open.
    pub async fn create_metadata(
        &self,
        parent: LoadedAddr,
        name: &OsStr,
        mode: u32,
    ) -> Result<INode, std::io::Error> {
        let parent_inode = self.loaded_inode(parent).await?;
        if parent_inode.itype != INodeType::Directory {
            return Err(std::io::Error::from_raw_os_error(libc::ENOTDIR));
        }

        // Ask the data provider to create the inode metadata.
        let child = self.data_provider.create(parent_inode, name, mode).await?;

        // Insert into inode table.
        self.inode_table
            .get_or_init(child.addr, || async move { child })
            .await;

        // Insert into directory cache.
        self.directory_cache.insert(
            parent,
            name.to_os_string(),
            LoadedAddr::new_unchecked(child.addr),
            false, // not a directory
        );

        // Initialize empty overlay entry so reads return empty, not 404.
        self.write_overlay.insert_sync(child.addr, Bytes::new());

        Ok(child)
    }

    /// Create a new empty file in the given parent directory.
    ///
    /// Allocates a new inode, inserts it into the inode table and directory
    /// cache, initializes an empty write overlay entry, and returns the
    /// inode along with an open file handle.
    pub async fn create(
        &self,
        parent: LoadedAddr,
        name: &OsStr,
        mode: u32,
    ) -> Result<(INode, OpenFile<OverlayReader<DP::Reader>>), std::io::Error> {
        let child = self.create_metadata(parent, name, mode).await?;

        // Open the file for the caller.
        let open_file = self
            .open(LoadedAddr::new_unchecked(child.addr), OpenFlags::RDWR)
            .await?;

        Ok((child, open_file))
    }

    /// Remove a file from a directory.
    ///
    /// Removes the entry from the directory cache and lookup cache.
    /// The `inode_table` and `write_overlay` entries are intentionally kept
    /// so that open file handles remain valid (POSIX). Full cleanup
    /// happens in [`InodeForget::delete`] when the FUSE refcount drops
    /// to zero. Calls [`FsDataProvider::unlink`] as a spawned
    /// fire-and-forget task so the FUSE response is not blocked.
    pub async fn unlink(&self, parent: LoadedAddr, name: &OsStr) -> Result<(), std::io::Error> {
        let parent_inode = self.loaded_inode(parent).await?;
        if parent_inode.itype != INodeType::Directory {
            return Err(std::io::Error::from_raw_os_error(libc::ENOTDIR));
        }

        // Check is_dir before removing from dcache to avoid a window where
        // the directory entry is temporarily missing from readdir results.
        let (child_addr, is_dir) = if let Some(entry) = self.directory_cache.lookup(parent, name) {
            (entry.ino.addr(), entry.is_dir)
        } else {
            // Dcache miss — fall back to provider lookup.
            let child = self.data_provider.lookup(parent_inode, name).await?;
            (child.addr, child.itype == INodeType::Directory)
        };

        // POSIX: unlink() must return EISDIR for directories.
        if is_dir {
            return Err(std::io::Error::from_raw_os_error(libc::EISDIR));
        }

        // Safe to remove now — we've confirmed it's not a directory.
        self.directory_cache.remove_child(parent, name);

        // Remove from lookup cache so a subsequent lookup calls the
        // provider fresh instead of returning the deleted inode.
        self.lookup_cache
            .remove_sync(&(parent.addr(), Arc::from(name)));

        // Mark as unlinked. The inode_table and write_overlay entries are
        // intentionally kept so that open file handles remain valid (POSIX).
        // InodeForget::delete will perform full cleanup when the FUSE
        // refcount drops to zero.
        let _ = self.unlinked_inodes.insert_sync(child_addr);

        // Notify the data provider (fire-and-forget). Spawned so the
        // FUSE response is not blocked by remote round-trips.
        let dp = self.data_provider.clone();
        let name_owned = name.to_os_string();
        tokio::spawn(async move {
            if let Err(e) = dp.unlink(parent_inode, &name_owned).await {
                tracing::warn!(
                    parent = parent_inode.addr,
                    ?name_owned,
                    %e,
                    "data provider unlink failed (local state is authoritative)"
                );
            }
        });

        Ok(())
    }

    /// Update file attributes (size, timestamps).
    ///
    /// Currently supports:
    /// - `size`: Truncate or extend the file (updates write overlay).
    /// - `mtime`: Update the last-modified timestamp.
    /// - `atime`: Accepted but stored as mtime (we don't track atime separately).
    ///
    /// Returns the updated inode.
    pub async fn setattr(
        &self,
        addr: LoadedAddr,
        size: Option<u64>,
        atime: Option<std::time::SystemTime>,
        mtime: Option<std::time::SystemTime>,
    ) -> Result<INode, std::io::Error> {
        let mut inode = self.loaded_inode(addr).await?;

        // When truncating, hold the per-inode write lock for the entire
        // operation — overlay mutation through provider sync. This prevents
        // a concurrent write() from interleaving and sending stale content
        // to the remote (TOCTOU).
        let write_lock = match size {
            Some(_) => Some(self.inode_write_lock(addr.addr()).await),
            None => None,
        };
        let _write_guard = match write_lock {
            Some(ref lock) => Some(lock.lock().await),
            None => None,
        };

        let truncated_content = if let Some(new_size) = size {
            #[expect(
                clippy::cast_possible_truncation,
                reason = "new_size fits in usize on supported 64-bit platforms"
            )]
            let new_len = new_size as usize;

            let existing = self.write_overlay.get(&addr.addr()).await;

            // When truncating to zero, skip reading the provider entirely —
            // the result is always an empty buffer regardless of content.
            // For other sizes, only read min(new_size, old_size) bytes from
            // the provider rather than the full file.
            let mut buf = if new_size == 0 {
                Vec::new()
            } else if let Some(data) = existing {
                data.to_vec()
            } else {
                let bytes_to_read = new_size.min(inode.size);
                let read_size: u32 = bytes_to_read.try_into().unwrap_or_else(|_| {
                    tracing::error!(
                        addr = addr.addr(),
                        size = bytes_to_read,
                        "file size exceeds u32::MAX, read will be truncated"
                    );
                    u32::MAX
                });
                let reader = self.data_provider.open(inode, OpenFlags::RDONLY).await?;
                let data = reader.read(0, read_size).await?;
                data.to_vec()
            };

            buf.resize(new_len, 0);
            let content = Bytes::from(buf);
            self.write_overlay
                .insert_sync(addr.addr(), Bytes::clone(&content));
            inode.size = new_size;
            Some(content)
        } else {
            None
        };

        if let Some(t) = mtime {
            inode.last_modified_at = t;
        } else if let Some(t) = atime {
            inode.last_modified_at = t;
        } else if size.is_some() {
            // Implicit mtime update: truncation modifies the file.
            inode.last_modified_at = std::time::SystemTime::now();
        }
        self.inode_table.insert_sync(addr.addr(), inode);

        // Notify the data provider (fire-and-forget — the overlay is
        // authoritative). Log failures so they are observable.
        if let Err(e) = self.data_provider.setattr(inode, size, atime, mtime).await {
            tracing::debug!(
                addr = addr.addr(),
                %e,
                "data provider setattr failed (overlay is authoritative)"
            );
        }

        // Sync truncated content to remote when size changed. Runs under
        // the per-inode write lock to prevent TOCTOU with concurrent write().
        // Skip when content is empty — the file already exists on remote
        // (via a background create task) and writing zero bytes would only
        // contend on the per-inode lock for no benefit.
        if let Some(content) = truncated_content
            && !content.is_empty()
            && let Err(e) = self.data_provider.write(inode, 0, content).await
        {
            tracing::debug!(
                addr = inode.addr,
                %e,
                "data provider write failed (overlay is authoritative)"
            );
        }

        Ok(inode)
    }

    /// Returns a clone of the write overlay handle.
    ///
    /// Used by the FUSE adapter to pass into `ForgetContext` so that
    /// `InodeForget` can check whether an inode has locally-written data.
    #[must_use]
    pub fn write_overlay(&self) -> Arc<FutureBackedCache<InodeAddr, Bytes>> {
        Arc::clone(&self.write_overlay)
    }

    /// Shared reference to the set of unlinked inodes.
    #[must_use]
    pub fn unlinked_inodes(&self) -> Arc<scc::HashSet<InodeAddr>> {
        Arc::clone(&self.unlinked_inodes)
    }

    /// Set ignore rules for a directory in the directory cache.
    pub fn set_ignore_rules(&self, parent: LoadedAddr, filename: &str, content: &str) {
        self.directory_cache
            .set_ignore_rules(parent, filename, content);
    }

    /// Clear ignore rules for a specific file in a directory.
    pub fn clear_ignore_rules(&self, parent: LoadedAddr, filename: &str) {
        self.directory_cache.clear_ignore_rules(parent, filename);
    }

    /// Check whether a child inode is ignored by any ancestor's ignore rules.
    #[must_use]
    pub fn is_ignored(&self, child_ino: LoadedAddr) -> bool {
        self.directory_cache.is_ignored(child_ino)
    }

    /// Check whether a child with the given name would be ignored.
    #[must_use]
    pub fn is_name_ignored(&self, parent_ino: LoadedAddr, name: &OsStr, is_dir: bool) -> bool {
        self.directory_cache
            .is_name_ignored(parent_ino, name, is_dir)
    }

    /// Returns the parent inode of a child, if known.
    #[must_use]
    pub fn parent_of(&self, child: LoadedAddr) -> Option<LoadedAddr> {
        self.directory_cache.parent_of(child)
    }

    /// Evict an inode from the inode table and notify the data provider.
    ///
    /// Called by the composite layer when propagating `forget` to a child
    /// filesystem. Removes the inode from the table and calls
    /// [`FsDataProvider::forget`] so the provider can clean up auxiliary
    /// structures (path maps, etc.).
    ///
    /// Inodes that have locally-written data (present in `write_overlay`) are
    /// skipped — they must persist for the lifetime of the mount, mirroring
    /// the guard in [`InodeForget::delete`].
    pub fn evict(&self, addr: InodeAddr) {
        if self.unlinked_inodes.remove_sync(&addr).is_some() {
            // Unlinked inode: full cleanup including write overlay.
            self.write_overlay.remove_sync(&addr);
            self.inode_table.remove_sync(&addr);
            self.directory_cache.evict(LoadedAddr::new_unchecked(addr));
            self.lookup_cache.evict_addr(addr);
            self.data_provider.forget(addr);
            return;
        }
        if self.write_overlay.contains_sync(&addr) {
            return;
        }
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
                PopulateStatus::Claimed(receipt) => {
                    // RAII guard: if this future is cancelled between Claimed
                    // and finish_populate, automatically abort so other waiters
                    // can retry instead of hanging forever.
                    let mut guard =
                        PopulateGuard::new(&self.directory_cache, parent, receipt.token);

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
                    self.directory_cache.finish_populate(parent, receipt);
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
