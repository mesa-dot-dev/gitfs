//! Async `INode` Table which supports concurrent access and modification.

use std::ffi::{OsStr, OsString};
use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;

use crate::cache::async_backed::FutureBackedCache;
use crate::drop_ward::StatelessDrop;
use crate::fs::{
    AsyncFsStats, DirEntry, FileHandle, INode, INodeType, InodeAddr, LoadedAddr, OpenFlags,
    dcache::DCache,
};

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

    /// Called when the kernel forgets an inode (refcount reaches zero).
    ///
    /// Implementations should clean up any internal mappings for the given
    /// address (e.g. bridge maps, path maps). The default is a no-op.
    fn forget(&self, _addr: InodeAddr) {}
}

/// Zero-sized tag whose [`StatelessDrop`] implementation automatically evicts
/// an inode from the inode table when its reference count reaches zero.
pub struct InodeForget;

impl<'a> StatelessDrop<&'a FutureBackedCache<InodeAddr, INode>, InodeAddr> for InodeForget {
    fn delete(inode_table: &&'a FutureBackedCache<InodeAddr, INode>, addr: &InodeAddr) {
        inode_table.remove_sync(addr);
    }
}

impl<'a, DP: FsDataProvider> StatelessDrop<(&'a FutureBackedCache<InodeAddr, INode>, DP), InodeAddr>
    for InodeForget
{
    fn delete(ctx: &(&'a FutureBackedCache<InodeAddr, INode>, DP), key: &InodeAddr) {
        ctx.0.remove_sync(key);
        ctx.1.forget(*key);
    }
}

/// A looked-up inode whose lifetime must be managed by the caller.
///
/// Each `TrackedINode` returned by [`AsyncFs::lookup`] represents one
/// reference that the FUSE kernel holds. The caller must balance it by
/// decrementing the [`InodeLifecycle`] ward when the kernel sends `forget`.
#[derive(Debug, Clone, Copy)]
pub struct TrackedINode {
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

mod inode_lifecycle_impl {
    #![allow(clippy::future_not_send, clippy::mem_forget)]
    use ouroboros::self_referencing;

    use crate::cache::async_backed::FutureBackedCache;
    use crate::drop_ward::DropWard;
    use crate::fs::InodeAddr;

    use super::{INode, InodeForget};

    /// Co-located inode table and reference-count ward.
    ///
    /// The ward borrows the table directly (no `Arc`) via `ouroboros`.
    /// When `dec` reaches zero for a key, [`InodeForget::delete`] synchronously
    /// removes that inode from the table.
    #[self_referencing]
    pub struct InodeLifecycle {
        pub(super) table: FutureBackedCache<InodeAddr, INode>,
        #[borrows(table)]
        #[not_covariant]
        pub(super) ward:
            DropWard<&'this FutureBackedCache<InodeAddr, INode>, InodeAddr, InodeForget>,
    }

    impl InodeLifecycle {
        /// Create a new lifecycle managing the given inode table.
        pub fn from_table(table: FutureBackedCache<InodeAddr, INode>) -> Self {
            Self::new(table, |tbl| DropWard::new(tbl))
        }
    }
}

pub use inode_lifecycle_impl::InodeLifecycle;

impl InodeLifecycle {
    /// Increment the reference count for an inode address.
    pub fn inc(&mut self, addr: InodeAddr) -> usize {
        self.with_ward_mut(|ward| ward.inc(addr))
    }

    /// Decrement the reference count for an inode address.
    ///
    /// When the count reaches zero, the inode is automatically evicted
    /// from the table via [`InodeForget::delete`].
    pub fn dec(&mut self, addr: &InodeAddr) -> Option<usize> {
        self.with_ward_mut(|ward| ward.dec(addr))
    }

    /// Decrement the reference count by `count`.
    ///
    /// When the count reaches zero, the inode is automatically evicted.
    pub fn dec_count(&mut self, addr: &InodeAddr, count: usize) -> Option<usize> {
        self.with_ward_mut(|ward| ward.dec_count(addr, count))
    }

    /// Read-only access to the underlying inode table.
    #[must_use]
    pub fn table(&self) -> &FutureBackedCache<InodeAddr, INode> {
        self.borrow_table()
    }
}

/// An asynchronous filesystem cache mapping `InodeAddr` to `INode`.
///
/// Uses two [`FutureBackedCache`] layers:
/// - `inode_table` stores resolved inodes by address, used by [`loaded_inode`](Self::loaded_inode).
/// - `lookup_cache` stores lookup results by `(parent_addr, name)`, ensuring `dp.lookup()` is only
///   called on a true cache miss (not already cached or in-flight).
///
/// The [`DCache`] sits in front as a synchronous fast path mapping `(parent, name)` to child addr.
pub struct AsyncFs<'tbl, DP: FsDataProvider> {
    /// Canonical addr -> `INode` map. Used by `loaded_inode()` to retrieve inodes by address.
    inode_table: &'tbl FutureBackedCache<InodeAddr, INode>,

    /// Deduplicating lookup cache keyed by `(parent_addr, child_name)`. The factory is
    /// `dp.lookup()`, so the data provider is only called on a true cache miss.
    lookup_cache: FutureBackedCache<(InodeAddr, OsString), INode>,

    /// Directory entry cache, mapping `(parent, name)` to child inode address.
    directory_cache: DCache,

    /// The data provider used to fetch inode data on cache misses.
    data_provider: DP,

    /// Monotonically increasing file handle counter. Starts at 1 (0 is reserved).
    next_fh: AtomicU64,
}

impl<'tbl, DP: FsDataProvider> AsyncFs<'tbl, DP> {
    /// Create a new `AsyncFs`, seeding the root inode into the table.
    pub async fn new(
        data_provider: DP,
        root: INode,
        inode_table: &'tbl FutureBackedCache<InodeAddr, INode>,
    ) -> Self {
        inode_table
            .get_or_init(root.addr, || async move { root })
            .await;

        Self {
            inode_table,
            lookup_cache: FutureBackedCache::default(),
            directory_cache: DCache::new(),
            data_provider,
            next_fh: AtomicU64::new(1),
        }
    }

    /// Create a new `AsyncFs`, assuming the root inode is already in the table.
    ///
    /// This synchronous constructor is needed for ouroboros builders where
    /// async is unavailable. The caller must ensure the root inode has already
    /// been inserted into `inode_table` (e.g. via [`FutureBackedCache::insert_sync`]).
    #[must_use]
    pub fn new_preseeded(
        data_provider: DP,
        inode_table: &'tbl FutureBackedCache<InodeAddr, INode>,
    ) -> Self {
        Self {
            inode_table,
            lookup_cache: FutureBackedCache::default(),
            directory_cache: DCache::new(),
            data_provider,
            next_fh: AtomicU64::new(1),
        }
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
    ) -> Result<TrackedINode, std::io::Error> {
        let parent_ino = self.loaded_inode(parent).await?;
        debug_assert!(
            matches!(parent_ino.itype, INodeType::Directory),
            "parent inode should be a directory"
        );

        if let Some(dentry) = self.directory_cache.lookup(parent, name)
            && let Some(inode) = self.inode_table.get(&dentry.ino.0).await
        {
            return Ok(TrackedINode { inode });
        }
        // Inode was evicted from the table — fall through to the slow path.

        let name_owned = name.to_os_string();
        let lookup_key = (parent.0, name_owned.clone());
        let dp = self.data_provider.clone();

        let child = self
            .lookup_cache
            .get_or_try_init(lookup_key, || {
                let name_for_dp = name_owned.clone();
                async move { dp.lookup(parent_ino, &name_for_dp).await }
            })
            .await?;

        self.inode_table
            .get_or_init(child.addr, || async move { child })
            .await;

        self.directory_cache
            .insert(
                parent,
                name_owned,
                LoadedAddr(child.addr),
                matches!(child.itype, INodeType::Directory),
            )
            .await;

        Ok(TrackedINode { inode: child })
    }

    /// Retrieve an inode that is expected to already be loaded.
    ///
    /// If the inode is currently in-flight (being loaded by another caller), this awaits
    /// completion. Returns an error if the inode is not in the table at all.
    pub async fn loaded_inode(&self, addr: LoadedAddr) -> Result<INode, std::io::Error> {
        self.inode_table.get(&addr.0).await.ok_or_else(|| {
            tracing::error!(
                inode = ?addr.0,
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
    /// TODO(MES-746): Implement `opendir` and `releasedir` to snapshot directory contents and
    ///                avoid racing with `lookup`/`createfile`.
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
                PopulateStatus::Claimed => {
                    match self.data_provider.readdir(parent_inode).await {
                        Ok(children) => {
                            for (name, child_inode) in children {
                                self.inode_table
                                    .get_or_init(child_inode.addr, || async move { child_inode })
                                    .await;
                                self.directory_cache
                                    .insert(
                                        parent,
                                        name,
                                        LoadedAddr(child_inode.addr),
                                        child_inode.itype == INodeType::Directory,
                                    )
                                    .await;
                            }
                            self.directory_cache.finish_populate(parent);
                        }
                        Err(e) => {
                            self.directory_cache.abort_populate(parent);
                            return Err(e);
                        }
                    }
                    break;
                }
                PopulateStatus::InProgress => {
                    self.directory_cache.wait_populated(parent).await;
                    // Re-check: the populator may have aborted.
                }
                PopulateStatus::Done => break,
            }
        }

        let mut children = self.directory_cache.readdir(parent).await;
        children.sort_unstable_by(|(a, _), (b, _)| a.cmp(b));

        #[expect(
            clippy::cast_possible_truncation,
            reason = "offset fits in usize on supported 64-bit platforms"
        )]
        for (i, (name, dvalue)) in children.iter().enumerate().skip(offset as usize) {
            let inode = self.loaded_inode(dvalue.ino).await?;
            let next_offset = (i + 1) as u64;
            if filler(DirEntry { name, inode }, next_offset) {
                break;
            }
        }

        Ok(())
    }
}
