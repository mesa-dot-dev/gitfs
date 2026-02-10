//! Mescloud-specific inode control block, helpers, and directory cache wrapper.

use std::ffi::OsStr;
use std::time::SystemTime;

use crate::fs::icache::{AsyncICache, IcbLike, IcbResolver, InodeFactory};
use crate::fs::r#trait::{
    CommonFileAttr, FileAttr, FileHandle, FilesystemStats, Inode, Permissions,
};

/// Inode control block for mescloud filesystem layers.
pub struct InodeControlBlock {
    pub parent: Option<Inode>,
    pub rc: u64,
    pub path: std::path::PathBuf,
    /// Cached file attributes from the last lookup.
    pub attr: Option<FileAttr>,
}

impl IcbLike for InodeControlBlock {
    fn new_root(path: std::path::PathBuf) -> Self {
        Self {
            rc: 1,
            parent: None,
            path,
            attr: None,
        }
    }

    fn rc(&self) -> u64 {
        self.rc
    }

    fn rc_mut(&mut self) -> &mut u64 {
        &mut self.rc
    }

    fn needs_resolve(&self) -> bool {
        self.attr.is_none()
    }
}

/// Calculate the number of blocks needed for a given size.
pub fn blocks_of_size(block_size: u32, size: u64) -> u64 {
    size.div_ceil(u64::from(block_size))
}

/// Free function -- usable by both MescloudICache and resolvers.
pub fn make_common_file_attr(
    ino: Inode,
    perm: u16,
    atime: SystemTime,
    mtime: SystemTime,
    fs_owner: (u32, u32),
    block_size: u32,
) -> CommonFileAttr {
    CommonFileAttr {
        ino,
        atime,
        mtime,
        ctime: SystemTime::UNIX_EPOCH,
        crtime: SystemTime::UNIX_EPOCH,
        perm: Permissions::from_bits_truncate(perm),
        nlink: 1,
        uid: fs_owner.0,
        gid: fs_owner.1,
        blksize: block_size,
    }
}

/// Mescloud-specific directory cache wrapper over AsyncICache.
pub struct MescloudICache<R: IcbResolver<Icb = InodeControlBlock>> {
    inner: AsyncICache<R>,
    inode_factory: InodeFactory,
    fs_owner: (u32, u32),
    block_size: u32,
}

impl<R: IcbResolver<Icb = InodeControlBlock>> MescloudICache<R> {
    /// Create a new `MescloudICache`. Initializes root ICB (rc=1), caches root dir attr.
    pub fn new(resolver: R, root_ino: Inode, fs_owner: (u32, u32), block_size: u32) -> Self {
        let cache = Self {
            inner: AsyncICache::new(resolver, root_ino, "/"),
            inode_factory: InodeFactory::new(root_ino + 1),
            fs_owner,
            block_size,
        };

        // Set root directory attr synchronously during initialization
        let now = SystemTime::now();
        let root_attr = FileAttr::Directory {
            common: make_common_file_attr(root_ino, 0o755, now, now, fs_owner, block_size),
        };
        cache.inner.get_icb_mut_sync(root_ino, |icb| {
            icb.attr = Some(root_attr);
        });

        cache
    }

    // -- Delegated from AsyncICache (async) --

    pub async fn contains(&self, ino: Inode) -> bool {
        self.inner.contains(ino).await
    }

    pub async fn get_icb<T>(&self, ino: Inode, f: impl FnOnce(&InodeControlBlock) -> T) -> Option<T> {
        self.inner.get_icb(ino, f).await
    }

    pub async fn get_icb_mut<T>(
        &self,
        ino: Inode,
        f: impl FnOnce(&mut InodeControlBlock) -> T,
    ) -> Option<T> {
        self.inner.get_icb_mut(ino, f).await
    }

    pub async fn insert_icb(&self, ino: Inode, icb: InodeControlBlock) {
        self.inner.insert_icb(ino, icb).await
    }

    pub async fn entry_or_insert_icb<T>(
        &self,
        ino: Inode,
        factory: impl FnOnce() -> InodeControlBlock,
        then: impl FnOnce(&mut InodeControlBlock) -> T,
    ) -> T {
        self.inner.entry_or_insert_icb(ino, factory, then).await
    }

    pub async fn inc_rc(&self, ino: Inode) -> u64 {
        self.inner.inc_rc(ino).await
    }

    pub async fn forget(&self, ino: Inode, nlookups: u64) -> Option<InodeControlBlock> {
        self.inner.forget(ino, nlookups).await
    }

    pub async fn get_or_resolve<T>(
        &self,
        ino: Inode,
        then: impl FnOnce(&InodeControlBlock) -> T,
    ) -> Result<T, R::Error> {
        self.inner.get_or_resolve(ino, then).await
    }

    // -- Delegated (sync) --

    pub fn allocate_fh(&self) -> FileHandle {
        self.inner.allocate_fh()
    }

    pub fn for_each(&self, f: impl FnMut(&Inode, &InodeControlBlock)) {
        self.inner.for_each(f)
    }

    pub fn inode_count(&self) -> usize {
        self.inner.inode_count()
    }

    // -- Domain-specific --

    /// Allocate a new inode number.
    pub fn allocate_inode(&self) -> Inode {
        self.inode_factory.allocate()
    }

    pub async fn get_attr(&self, ino: Inode) -> Option<FileAttr> {
        self.inner.get_icb(ino, |icb| icb.attr).await.flatten()
    }

    pub async fn cache_attr(&self, ino: Inode, attr: FileAttr) {
        self.inner
            .get_icb_mut(ino, |icb| {
                icb.attr = Some(attr);
            })
            .await;
    }

    pub fn fs_owner(&self) -> (u32, u32) {
        self.fs_owner
    }

    pub fn block_size(&self) -> u32 {
        self.block_size
    }

    pub fn statfs(&self) -> FilesystemStats {
        FilesystemStats {
            block_size: self.block_size,
            fragment_size: u64::from(self.block_size),
            total_blocks: 0,
            free_blocks: 0,
            available_blocks: 0,
            total_inodes: self.inner.inode_count() as u64,
            free_inodes: 0,
            available_inodes: 0,
            filesystem_id: 0,
            mount_flags: 0,
            max_filename_length: 255,
        }
    }

    /// Find an existing child by (parent, name) or allocate a new inode.
    /// If new, inserts a stub ICB (parent+path set, attr=None, rc=0).
    /// Does NOT bump rc. Returns the inode number.
    pub async fn ensure_child_ino(&self, parent: Inode, name: &OsStr) -> Inode {
        // Search for existing child by parent + name
        let mut existing_ino = None;
        self.inner.for_each(|&ino, icb| {
            if icb.parent == Some(parent) && icb.path.as_os_str() == name {
                existing_ino = Some(ino);
            }
        });

        if let Some(ino) = existing_ino {
            return ino;
        }

        // Allocate new inode and insert stub
        let ino = self.inode_factory.allocate();
        self.inner
            .insert_icb(
                ino,
                InodeControlBlock {
                    rc: 0,
                    path: name.into(),
                    parent: Some(parent),
                    attr: None,
                },
            )
            .await;
        ino
    }

    /// Direct access to the inner async cache for resolvers that need it.
    pub fn inner(&self) -> &AsyncICache<R> {
        &self.inner
    }
}
