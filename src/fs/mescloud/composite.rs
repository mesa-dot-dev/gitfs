use std::collections::HashMap;
use std::ffi::OsStr;

use bytes::Bytes;
use tracing::{instrument, trace, warn};

use crate::fs::icache::bridge::HashMapBridge;
use crate::fs::icache::{FileTable, IcbResolver};
use crate::fs::r#trait::{
    DirEntry, FileAttr, FileHandle, FilesystemStats, Fs, Inode, LockOwner, OpenFile, OpenFlags,
};

use super::common::{
    GetAttrError, InodeCachePeek, LookupError, OpenError, ReadDirError, ReadError, ReleaseError,
};
use super::icache::{InodeControlBlock, MescloudICache};

/// A child filesystem slot: inner filesystem + bidirectional inode/fh bridge.
pub(super) struct ChildSlot<Inner> {
    pub inner: Inner,
    pub bridge: HashMapBridge,
}

/// Layered filesystem that presents multiple child filesystems under a single
/// inode namespace.
///
/// `MesaCloud`'s filesystem is a hierarchy of compositions:
///
/// ```text
/// MesaFS  (CompositeFs<_, OrgFs>)
///  └─ OrgFs  (CompositeFs<_, RepoFs>)
///      └─ RepoFs  (leaf — backed by git)
/// ```
///
/// Each child filesystem numbers its inodes starting from 1, so the composite
/// maintains a bidirectional inode/file-handle bridge per child (see
/// [`ChildSlot`]) to translate between the outer namespace visible to FUSE and
/// each child's internal namespace.
pub(super) struct CompositeFs<R, Inner>
where
    R: IcbResolver<Icb = InodeControlBlock>,
{
    pub icache: MescloudICache<R>,
    pub file_table: FileTable,
    pub readdir_buf: Vec<DirEntry>,
    /// Maps outer inode to index into `slots` for child-root inodes.
    pub child_inodes: HashMap<Inode, usize>,
    /// Maps every translated outer inode to its owning slot index.
    pub inode_to_slot: HashMap<Inode, usize>,
    pub slots: Vec<ChildSlot<Inner>>,
}

impl<R, Inner> CompositeFs<R, Inner>
where
    R: IcbResolver<Icb = InodeControlBlock>,
    Inner: Fs<
            LookupError = LookupError,
            GetAttrError = GetAttrError,
            OpenError = OpenError,
            ReadError = ReadError,
            ReaddirError = ReadDirError,
            ReleaseError = ReleaseError,
        > + InodeCachePeek
        + Send
        + Sync,
{
    /// Look up which child slot owns an inode via direct map.
    #[instrument(name = "CompositeFs::slot_for_inode", skip(self))]
    pub fn slot_for_inode(&self, ino: Inode) -> Option<usize> {
        self.inode_to_slot.get(&ino).copied()
    }

    /// Allocate an outer file handle and map it through the bridge.
    #[must_use]
    pub fn alloc_fh(&mut self, slot_idx: usize, inner_fh: FileHandle) -> FileHandle {
        let fh = self.file_table.allocate();
        self.slots[slot_idx].bridge.insert_fh(fh, inner_fh);
        fh
    }

    /// Translate an inner inode to an outer inode, allocating if needed.
    /// Also inserts a stub ICB into the outer icache when the inode is new.
    #[instrument(name = "CompositeFs::translate_inner_ino", skip(self, name))]
    pub async fn translate_inner_ino(
        &mut self,
        slot_idx: usize,
        inner_ino: Inode,
        parent_outer_ino: Inode,
        name: &OsStr,
    ) -> Inode {
        let outer_ino = self.slots[slot_idx]
            .bridge
            .backward_or_insert_inode(inner_ino, || self.icache.allocate_inode());
        self.inode_to_slot.insert(outer_ino, slot_idx);
        self.icache
            .entry_or_insert_icb(
                outer_ino,
                || InodeControlBlock {
                    rc: 0,
                    path: name.into(),
                    parent: Some(parent_outer_ino),
                    attr: None,
                    children: None,
                },
                |_| {},
            )
            .await;
        outer_ino
    }

    /// Get cached file attributes for an inode.
    #[instrument(name = "CompositeFs::delegated_getattr", skip(self))]
    pub async fn delegated_getattr(&self, ino: Inode) -> Result<FileAttr, GetAttrError> {
        self.icache.get_attr(ino).await.ok_or_else(|| {
            warn!(ino, "getattr on unknown inode");
            GetAttrError::InodeNotFound
        })
    }

    /// Find slot, forward inode, delegate to inner, allocate outer file handle.
    #[instrument(name = "CompositeFs::delegated_open", skip(self))]
    pub async fn delegated_open(
        &mut self,
        ino: Inode,
        flags: OpenFlags,
    ) -> Result<OpenFile, OpenError> {
        let idx = self.slot_for_inode(ino).ok_or_else(|| {
            warn!(ino, "open on inode not belonging to any child");
            OpenError::InodeNotFound
        })?;
        let inner_ino = self.slots[idx]
            .bridge
            .forward_or_insert_inode(ino, || unreachable!("open: ino should be mapped"));
        let inner_open = self.slots[idx].inner.open(inner_ino, flags).await?;
        let outer_fh = self.alloc_fh(idx, inner_open.handle);
        trace!(
            ino,
            outer_fh,
            inner_fh = inner_open.handle,
            "open: assigned file handle"
        );
        Ok(OpenFile {
            handle: outer_fh,
            options: inner_open.options,
        })
    }

    /// Find slot, forward inode and file handle, delegate read to inner.
    #[expect(clippy::too_many_arguments, reason = "mirrors fuser read API")]
    #[instrument(name = "CompositeFs::delegated_read", skip(self))]
    pub async fn delegated_read(
        &mut self,
        ino: Inode,
        fh: FileHandle,
        offset: u64,
        size: u32,
        flags: OpenFlags,
        lock_owner: Option<LockOwner>,
    ) -> Result<Bytes, ReadError> {
        let idx = self.slot_for_inode(ino).ok_or_else(|| {
            warn!(ino, "read on inode not belonging to any child");
            ReadError::InodeNotFound
        })?;
        let inner_ino = self.slots[idx]
            .bridge
            .forward_or_insert_inode(ino, || unreachable!("read: ino should be mapped"));
        let inner_fh = self.slots[idx].bridge.fh_forward(fh).ok_or_else(|| {
            warn!(fh, "read: no fh mapping found");
            ReadError::FileNotOpen
        })?;
        self.slots[idx]
            .inner
            .read(inner_ino, inner_fh, offset, size, flags, lock_owner)
            .await
    }

    /// Find slot, forward inode and file handle, delegate release to inner,
    /// then clean up the file handle mapping.
    #[instrument(name = "CompositeFs::delegated_release", skip(self))]
    pub async fn delegated_release(
        &mut self,
        ino: Inode,
        fh: FileHandle,
        flags: OpenFlags,
        flush: bool,
    ) -> Result<(), ReleaseError> {
        let idx = self.slot_for_inode(ino).ok_or_else(|| {
            warn!(ino, "release on inode not belonging to any child");
            ReleaseError::FileNotOpen
        })?;
        let inner_ino = self.slots[idx]
            .bridge
            .forward_or_insert_inode(ino, || unreachable!("release: ino should be mapped"));
        let inner_fh = self.slots[idx].bridge.fh_forward(fh).ok_or_else(|| {
            warn!(fh, "release: no fh mapping found");
            ReleaseError::FileNotOpen
        })?;
        let result = self.slots[idx]
            .inner
            .release(inner_ino, inner_fh, flags, flush)
            .await;
        self.slots[idx].bridge.remove_fh_by_left(fh);
        trace!(ino, fh, "release: cleaned up fh mapping");
        result
    }

    /// Propagate forget to the inner filesystem, evict from icache, and clean
    /// up bridge mappings. Returns `true` if the inode was evicted.
    ///
    /// Child-root inodes (those in `child_inodes`) do NOT propagate forget to
    /// the inner filesystem: the inner root's `rc=1` is an initialization
    /// invariant unrelated to outer FUSE lookup counts. Propagating would
    /// evict the inner root, breaking all subsequent operations on that child.
    #[must_use]
    #[instrument(name = "CompositeFs::delegated_forget", skip(self))]
    pub async fn delegated_forget(&mut self, ino: Inode, nlookups: u64) -> bool {
        let slot_idx = self.slot_for_inode(ino);
        let is_child_root = self.child_inodes.contains_key(&ino);
        if !is_child_root
            && let Some(idx) = slot_idx
            && let Some(&inner_ino) = self.slots[idx].bridge.inode_map_get_by_left(ino)
        {
            self.slots[idx].inner.forget(inner_ino, nlookups).await;
        }
        if self.icache.forget(ino, nlookups).await.is_some() {
            self.child_inodes.remove(&ino);
            self.inode_to_slot.remove(&ino);
            if let Some(idx) = slot_idx {
                self.slots[idx].bridge.remove_inode_by_left(ino);
            }
            true
        } else {
            false
        }
    }

    /// Return filesystem statistics from the icache.
    #[must_use]
    pub fn delegated_statfs(&self) -> FilesystemStats {
        self.icache.statfs()
    }

    /// Delegation branch for lookup when the parent is owned by a child slot.
    #[instrument(name = "CompositeFs::delegated_lookup", skip(self, name))]
    pub async fn delegated_lookup(
        &mut self,
        parent: Inode,
        name: &OsStr,
    ) -> Result<FileAttr, LookupError> {
        let idx = self
            .slot_for_inode(parent)
            .ok_or(LookupError::InodeNotFound)?;
        let inner_parent = self.slots[idx]
            .bridge
            .forward_or_insert_inode(parent, || unreachable!("lookup: parent should be mapped"));
        let inner_attr = self.slots[idx].inner.lookup(inner_parent, name).await?;
        let inner_ino = inner_attr.common().ino;
        let outer_ino = self.translate_inner_ino(idx, inner_ino, parent, name).await;
        let outer_attr = self.slots[idx].bridge.attr_backward(inner_attr);
        self.icache.cache_attr(outer_ino, outer_attr).await;
        // None means the entry was concurrently evicted; fail the lookup so
        // the kernel doesn't hold a ref the cache no longer tracks.
        let rc = self
            .icache
            .inc_rc(outer_ino)
            .await
            .ok_or(LookupError::InodeNotFound)?;
        trace!(outer_ino, inner_ino, rc, "lookup: resolved via delegation");
        Ok(outer_attr)
    }

    /// Delegation branch for readdir when the inode is owned by a child slot.
    #[instrument(name = "CompositeFs::delegated_readdir", skip(self))]
    pub async fn delegated_readdir(&mut self, ino: Inode) -> Result<&[DirEntry], ReadDirError> {
        let idx = self
            .slot_for_inode(ino)
            .ok_or(ReadDirError::InodeNotFound)?;
        let inner_ino = self.slots[idx]
            .bridge
            .forward_or_insert_inode(ino, || unreachable!("readdir: ino should be mapped"));
        let inner_entries = self.slots[idx].inner.readdir(inner_ino).await?;
        let inner_entries: Vec<DirEntry> = inner_entries.to_vec();
        let evicted = self.icache.evict_zero_rc_children(ino).await;
        for evicted_ino in evicted {
            if let Some(slot) = self.inode_to_slot.remove(&evicted_ino) {
                self.slots[slot].bridge.remove_inode_by_left(evicted_ino);
            }
            self.child_inodes.remove(&evicted_ino);
        }
        let mut outer_entries = Vec::with_capacity(inner_entries.len());
        for entry in &inner_entries {
            let outer_child_ino = self
                .translate_inner_ino(idx, entry.ino, ino, &entry.name)
                .await;
            if let Some(inner_attr) = self.slots[idx].inner.peek_attr(entry.ino).await {
                let outer_attr = self.slots[idx].bridge.attr_backward(inner_attr);
                self.icache.cache_attr(outer_child_ino, outer_attr).await;
            }
            outer_entries.push(DirEntry {
                ino: outer_child_ino,
                name: entry.name.clone(),
                kind: entry.kind,
            });
        }
        self.readdir_buf = outer_entries;
        Ok(&self.readdir_buf)
    }
}
