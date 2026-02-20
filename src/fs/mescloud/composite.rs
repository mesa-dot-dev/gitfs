use std::collections::HashMap;
use std::ffi::OsStr;
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;
use git_fs::cache::async_backed::FutureBackedCache;
use git_fs::fs::dcache::DCache;
use git_fs::fs::{
    AsyncFsStats, FileHandle, INode, INodeType, InodeAddr, InodePerms, LoadedAddr, OpenFlags,
};
use rustc_hash::FxHashMap;
use tracing::{instrument, trace};

use super::common::{
    ChildFs, FsDirEntry, GetAttrError, LookupError, OpenError, ReadDirError, ReadError,
    ReleaseError,
};

/// Bidirectional inode mapping between outer (composite) and inner (child) address spaces.
///
/// Convention: **outer = left, inner = right**.
pub(super) struct InodeBridge {
    map: bimap::BiMap<InodeAddr, InodeAddr>,
}

impl InodeBridge {
    pub fn new() -> Self {
        Self {
            map: bimap::BiMap::new(),
        }
    }

    pub fn insert(&mut self, outer: InodeAddr, inner: InodeAddr) {
        self.map.insert(outer, inner);
    }

    pub fn forward(&self, outer: InodeAddr) -> Option<InodeAddr> {
        self.map.get_by_left(&outer).copied()
    }

    #[expect(dead_code, reason = "will be needed by future callers")]
    pub fn backward(&self, inner: InodeAddr) -> Option<InodeAddr> {
        self.map.get_by_right(&inner).copied()
    }

    /// Look up inner->outer, or allocate a new outer address if unmapped.
    pub fn backward_or_insert(
        &mut self,
        inner: InodeAddr,
        allocate: impl FnOnce() -> InodeAddr,
    ) -> InodeAddr {
        if let Some(&outer) = self.map.get_by_right(&inner) {
            outer
        } else {
            let outer = allocate();
            self.map.insert(outer, inner);
            outer
        }
    }

    pub fn remove_by_outer(&mut self, outer: InodeAddr) {
        self.map.remove_by_left(&outer);
    }

    #[expect(dead_code, reason = "will be needed by future callers")]
    pub fn get_inner(&self, outer: InodeAddr) -> Option<&InodeAddr> {
        self.map.get_by_left(&outer)
    }
}

pub(super) struct ChildSlot<Inner> {
    pub inner: Inner,
    pub bridge: InodeBridge,
}

/// Tracks an open file: which child slot owns it and the inner fh.
struct OpenFileEntry {
    slot_idx: usize,
    inner_ino: InodeAddr,
    inner_fh: FileHandle,
}

pub(super) struct CompositeFs<Inner> {
    pub(super) inode_table: FutureBackedCache<InodeAddr, INode>,
    pub(super) directory_cache: DCache,
    next_ino: AtomicU64,
    next_fh: AtomicU64,
    refcounts: FxHashMap<InodeAddr, u64>,
    pub(super) readdir_buf: Vec<FsDirEntry>,
    open_files: HashMap<FileHandle, OpenFileEntry>,
    pub(super) child_inodes: HashMap<InodeAddr, usize>,
    pub(super) inode_to_slot: HashMap<InodeAddr, usize>,
    pub(super) slots: Vec<ChildSlot<Inner>>,
    fs_owner: (u32, u32),
    block_size: u32,
}

impl<Inner> CompositeFs<Inner> {
    pub const ROOT_INO: InodeAddr = 1;

    pub fn new(fs_owner: (u32, u32), block_size: u32) -> Self {
        let inode_table = FutureBackedCache::default();
        let now = std::time::SystemTime::now();
        let root = INode {
            addr: Self::ROOT_INO,
            permissions: InodePerms::from_bits_truncate(0o755),
            uid: fs_owner.0,
            gid: fs_owner.1,
            create_time: now,
            last_modified_at: now,
            parent: None,
            size: 0,
            itype: INodeType::Directory,
        };
        inode_table.insert_sync(Self::ROOT_INO, root);

        let mut refcounts = FxHashMap::default();
        refcounts.insert(Self::ROOT_INO, 1);

        Self {
            inode_table,
            directory_cache: DCache::new(),
            next_ino: AtomicU64::new(Self::ROOT_INO + 1),
            next_fh: AtomicU64::new(1),
            refcounts,
            readdir_buf: Vec::new(),
            open_files: HashMap::new(),
            child_inodes: HashMap::new(),
            inode_to_slot: HashMap::new(),
            slots: Vec::new(),
            fs_owner,
            block_size,
        }
    }

    pub fn allocate_inode(&self) -> InodeAddr {
        self.next_ino.fetch_add(1, Ordering::Relaxed)
    }

    pub fn fs_owner(&self) -> (u32, u32) {
        self.fs_owner
    }

    #[expect(dead_code, reason = "available for future use")]
    pub fn block_size(&self) -> u32 {
        self.block_size
    }

    pub fn add_child(&mut self, inner: Inner, child_root_ino: InodeAddr) -> InodeAddr {
        self.add_child_with_parent(inner, child_root_ino, Self::ROOT_INO)
    }

    pub fn cache_inode(&self, inode: INode) {
        self.inode_table.insert_sync(inode.addr, inode);
    }

    /// Insert the inode into the table and initialise its refcount to zero.
    ///
    /// The caller is responsible for bumping the refcount via [`inc_rc`](Self::inc_rc).
    pub fn cache_inode_and_init_rc(&mut self, inode: INode) {
        let addr = inode.addr;
        self.inode_table.insert_sync(addr, inode);
        self.refcounts.entry(addr).or_insert(0);
    }

    pub fn inc_rc(&mut self, addr: InodeAddr) -> Option<u64> {
        let rc = self.refcounts.get_mut(&addr)?;
        *rc += 1;
        Some(*rc)
    }

    pub fn slot_for_inode(&self, ino: InodeAddr) -> Option<usize> {
        self.inode_to_slot.get(&ino).copied()
    }

    /// Like [`add_child`](Self::add_child) but sets a custom parent inode
    /// instead of always using `ROOT_INO`.
    pub fn add_child_with_parent(
        &mut self,
        inner: Inner,
        child_root_ino: InodeAddr,
        parent_ino: InodeAddr,
    ) -> InodeAddr {
        let outer_ino = self.allocate_inode();
        let now = std::time::SystemTime::now();
        let inode = INode {
            addr: outer_ino,
            permissions: InodePerms::from_bits_truncate(0o755),
            uid: self.fs_owner.0,
            gid: self.fs_owner.1,
            create_time: now,
            last_modified_at: now,
            parent: Some(parent_ino),
            size: 0,
            itype: INodeType::Directory,
        };
        self.inode_table.insert_sync(outer_ino, inode);

        let mut bridge = InodeBridge::new();
        bridge.insert(outer_ino, child_root_ino);

        let idx = self.slots.len();
        self.slots.push(ChildSlot { inner, bridge });
        self.child_inodes.insert(outer_ino, idx);
        self.inode_to_slot.insert(outer_ino, idx);

        outer_ino
    }
}

impl<Inner: ChildFs> CompositeFs<Inner> {
    #[instrument(name = "CompositeFs::delegated_lookup", skip(self, name))]
    pub async fn delegated_lookup(
        &mut self,
        parent: InodeAddr,
        name: &OsStr,
    ) -> Result<INode, LookupError> {
        // Fast path: DCache hit + inode still in table
        if let Some(dentry) = self.directory_cache.lookup(LoadedAddr(parent), name)
            && let Some(inode) = self.inode_table.get(&dentry.ino.0).await
        {
            *self.refcounts.entry(inode.addr).or_insert(0) += 1;
            return Ok(inode);
        }

        // Slow path: delegate to child
        let idx = self
            .inode_to_slot
            .get(&parent)
            .copied()
            .ok_or(LookupError::InodeNotFound)?;
        let inner_parent = self.slots[idx]
            .bridge
            .forward(parent)
            .ok_or(LookupError::InodeNotFound)?;
        let inner_inode = self.slots[idx].inner.lookup(inner_parent, name).await?;

        let next_ino = &self.next_ino;
        let outer_ino = self.slots[idx]
            .bridge
            .backward_or_insert(inner_inode.addr, || {
                next_ino.fetch_add(1, Ordering::Relaxed)
            });
        self.inode_to_slot.insert(outer_ino, idx);

        let remapped = INode {
            addr: outer_ino,
            ..inner_inode
        };
        self.inode_table
            .get_or_init(outer_ino, || async move { remapped })
            .await;

        let is_dir = matches!(inner_inode.itype, INodeType::Directory);
        self.directory_cache
            .insert(
                LoadedAddr(parent),
                name.to_os_string(),
                LoadedAddr(outer_ino),
                is_dir,
            )
            .await;

        *self.refcounts.entry(outer_ino).or_insert(0) += 1;
        let rc = self.refcounts[&outer_ino];
        trace!(
            outer_ino,
            inner_ino = inner_inode.addr,
            rc,
            "lookup: resolved via delegation"
        );

        Ok(remapped)
    }

    #[instrument(name = "CompositeFs::delegated_readdir", skip(self))]
    pub async fn delegated_readdir(
        &mut self,
        ino: InodeAddr,
    ) -> Result<&[FsDirEntry], ReadDirError> {
        let idx = self
            .inode_to_slot
            .get(&ino)
            .copied()
            .ok_or(ReadDirError::InodeNotFound)?;

        if !self.directory_cache.is_populated(LoadedAddr(ino)) {
            let inner_ino = self.slots[idx]
                .bridge
                .forward(ino)
                .ok_or(ReadDirError::InodeNotFound)?;
            let inner_entries = self.slots[idx].inner.readdir(inner_ino).await?;

            for (name, child_inode) in &inner_entries {
                let next_ino = &self.next_ino;
                let outer_child = self.slots[idx]
                    .bridge
                    .backward_or_insert(child_inode.addr, || {
                        next_ino.fetch_add(1, Ordering::Relaxed)
                    });
                self.inode_to_slot.insert(outer_child, idx);

                let remapped = INode {
                    addr: outer_child,
                    ..*child_inode
                };
                self.inode_table
                    .get_or_init(outer_child, || async move { remapped })
                    .await;

                let is_dir = matches!(child_inode.itype, INodeType::Directory);
                self.directory_cache
                    .insert(
                        LoadedAddr(ino),
                        name.clone(),
                        LoadedAddr(outer_child),
                        is_dir,
                    )
                    .await;
            }

            self.directory_cache.mark_populated(LoadedAddr(ino));
        }

        let mut children = self.directory_cache.readdir(LoadedAddr(ino)).await;
        children.sort_unstable_by(|(a, _), (b, _)| a.cmp(b));

        let mut entries = Vec::with_capacity(children.len());
        for (name, dvalue) in &children {
            if let Some(inode) = self.inode_table.get(&dvalue.ino.0).await {
                entries.push(FsDirEntry {
                    ino: inode.addr,
                    name: name.clone(),
                });
            }
        }

        self.readdir_buf = entries;
        Ok(&self.readdir_buf)
    }

    #[instrument(name = "CompositeFs::delegated_getattr", skip(self))]
    pub async fn delegated_getattr(&self, ino: InodeAddr) -> Result<INode, GetAttrError> {
        self.inode_table
            .get(&ino)
            .await
            .ok_or(GetAttrError::InodeNotFound)
    }

    #[expect(dead_code, reason = "will be needed by future callers")]
    #[must_use]
    pub fn delegated_statfs(&self) -> AsyncFsStats {
        AsyncFsStats {
            block_size: self.block_size,
            total_blocks: 0,
            free_blocks: 0,
            available_blocks: 0,
            total_inodes: self.inode_table.len() as u64,
            free_inodes: 0,
            max_filename_length: 255,
        }
    }

    #[instrument(name = "CompositeFs::delegated_open", skip(self))]
    pub async fn delegated_open(
        &mut self,
        ino: InodeAddr,
        flags: OpenFlags,
    ) -> Result<FileHandle, OpenError> {
        let idx = self
            .inode_to_slot
            .get(&ino)
            .copied()
            .ok_or(OpenError::InodeNotFound)?;
        let inner_ino = self.slots[idx]
            .bridge
            .forward(ino)
            .ok_or(OpenError::InodeNotFound)?;
        let inner_fh = self.slots[idx].inner.open(inner_ino, flags).await?;

        let outer_fh = self.next_fh.fetch_add(1, Ordering::Relaxed);
        self.open_files.insert(
            outer_fh,
            OpenFileEntry {
                slot_idx: idx,
                inner_ino,
                inner_fh,
            },
        );

        trace!(ino, outer_fh, inner_fh, "open: assigned fh");
        Ok(outer_fh)
    }

    #[instrument(name = "CompositeFs::delegated_read", skip(self))]
    pub async fn delegated_read(
        &mut self,
        fh: FileHandle,
        offset: u64,
        size: u32,
    ) -> Result<Bytes, ReadError> {
        let entry = self.open_files.get(&fh).ok_or(ReadError::FileNotOpen)?;
        let slot_idx = entry.slot_idx;
        let inner_ino = entry.inner_ino;
        let inner_fh = entry.inner_fh;
        self.slots[slot_idx]
            .inner
            .read(inner_ino, inner_fh, offset, size)
            .await
    }

    #[instrument(name = "CompositeFs::delegated_release", skip(self))]
    pub async fn delegated_release(&mut self, fh: FileHandle) -> Result<(), ReleaseError> {
        let entry = self
            .open_files
            .remove(&fh)
            .ok_or(ReleaseError::FileNotOpen)?;
        let result = self.slots[entry.slot_idx]
            .inner
            .release(entry.inner_ino, entry.inner_fh)
            .await;
        trace!(fh, "release: cleaned up fh mapping");
        result
    }

    /// Returns `true` if the inode was evicted.
    ///
    /// The composite only manages its own refcounts and inode table.
    /// Inner filesystem inodes are managed by the inner FS itself through
    /// its own lifecycle; the composite does not propagate forget to children.
    #[expect(dead_code, reason = "will be needed by future callers")]
    #[must_use]
    #[instrument(name = "CompositeFs::delegated_forget", skip(self))]
    pub fn delegated_forget(&mut self, ino: InodeAddr, nlookups: u64) -> bool {
        let slot_idx = self.inode_to_slot.get(&ino).copied();

        if let Some(rc) = self.refcounts.get_mut(&ino) {
            *rc = rc.saturating_sub(nlookups);
            if *rc > 0 {
                return false;
            }
            self.refcounts.remove(&ino);
        } else {
            return false;
        }

        self.inode_table.remove_sync(&ino);
        self.child_inodes.remove(&ino);
        self.inode_to_slot.remove(&ino);
        if let Some(idx) = slot_idx {
            self.slots[idx].bridge.remove_by_outer(ino);
        }

        true
    }
}
