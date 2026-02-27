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

    /// Write data to a file at the given offset.
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

    /// Overlay cache for locally-written file data.
    ///
    /// Keyed by inode address, valued by the full file content. Written
    /// entries are never evicted by `InodeForget` — they persist for the
    /// lifetime of the mount.
    write_overlay: Arc<FutureBackedCache<InodeAddr, Bytes>>,
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
            write_overlay: Arc::new(FutureBackedCache::default()),
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
            write_overlay: Arc::new(FutureBackedCache::default()),
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

        // Merge with existing overlay content, if any.
        let existing = self.write_overlay.get(&addr.addr()).await;
        let mut buf = existing.map_or_else(Vec::new, |b| b.to_vec());

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
        let new_size = new_content.len() as u64;
        self.write_overlay.insert_sync(addr.addr(), new_content);

        // Update inode size in the table.
        let mut updated = inode;
        updated.size = new_size;
        updated.last_modified_at = std::time::SystemTime::now();
        self.inode_table.insert_sync(addr.addr(), updated);

        // Notify the data provider (for logging / future forwarding).
        drop(self.data_provider.write(inode, offset, data).await);

        Ok(bytes_written)
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

        // Open the file for the caller.
        let open_file = self
            .open(LoadedAddr::new_unchecked(child.addr), OpenFlags::RDWR)
            .await?;

        Ok((child, open_file))
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

        if let Some(new_size) = size {
            let existing = self.write_overlay.get(&addr.addr()).await;
            let mut buf = existing.map_or_else(Vec::new, |b| b.to_vec());

            #[expect(
                clippy::cast_possible_truncation,
                reason = "new_size fits in usize on supported 64-bit platforms"
            )]
            let new_len = new_size as usize;
            buf.resize(new_len, 0);
            self.write_overlay
                .insert_sync(addr.addr(), Bytes::from(buf));
            inode.size = new_size;
        }

        if let Some(t) = mtime {
            inode.last_modified_at = t;
        } else if let Some(t) = atime {
            inode.last_modified_at = t;
        }

        inode.last_modified_at = std::time::SystemTime::now();
        self.inode_table.insert_sync(addr.addr(), inode);

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
