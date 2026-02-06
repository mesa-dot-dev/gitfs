//! Reusable directory cache for mescloud filesystem implementations.

use std::collections::HashMap;
use std::ffi::OsStr;
use std::time::SystemTime;

use tracing::{trace, warn};

use crate::fs::r#trait::{
    CommonFileAttr, DirEntryType, FileAttr, FileHandle, FilesystemStats, Inode, Permissions,
};

use super::common::InodeControlBlock;

/// Monotonically increasing inode allocator.
pub(super) struct InodeFactory {
    next_inode: Inode,
}

impl InodeFactory {
    fn new(start: Inode) -> Self {
        Self { next_inode: start }
    }

    fn allocate(&mut self) -> Inode {
        let ino = self.next_inode;
        self.next_inode += 1;
        ino
    }
}

/// Shared directory cache state.
///
/// Owns inode allocation, reference counting, attribute caching, and file handle allocation.
/// Designed for composition — each filesystem struct embeds a `DCache` field.
pub(super) struct DCache {
    inode_table: HashMap<Inode, InodeControlBlock>,
    inode_factory: InodeFactory,
    next_fh: FileHandle,
    fs_owner: (u32, u32),
    block_size: u32,
}

impl DCache {
    /// Create a new DCache. Initializes root ICB (rc=1), caches root dir attr.
    pub fn new(root_ino: Inode, fs_owner: (u32, u32), block_size: u32) -> Self {
        let now = SystemTime::now();

        let mut inode_table = HashMap::new();
        inode_table.insert(
            root_ino,
            InodeControlBlock {
                rc: 1,
                parent: None,
                path: "/".into(),
                children: None,
                attr: None,
            },
        );

        let mut dcache = Self {
            inode_table,
            inode_factory: InodeFactory::new(root_ino + 1),
            next_fh: 1,
            fs_owner,
            block_size,
        };

        let root_attr = FileAttr::Directory {
            common: dcache.make_common_file_attr(root_ino, 0o755, now, now),
        };
        dcache.cache_attr(root_ino, root_attr);
        dcache
    }

    // ── Inode allocation ────────────────────────────────────────────────

    /// Allocate a new inode number.
    pub fn allocate_inode(&mut self) -> Inode {
        self.inode_factory.allocate()
    }

    /// Allocate a file handle (increments `next_fh` and returns the old value).
    pub fn allocate_fh(&mut self) -> FileHandle {
        let fh = self.next_fh;
        self.next_fh += 1;
        fh
    }

    // ── ICB access ──────────────────────────────────────────────────────

    pub fn get_icb(&self, ino: Inode) -> Option<&InodeControlBlock> {
        self.inode_table.get(&ino)
    }

    pub fn get_icb_mut(&mut self, ino: Inode) -> Option<&mut InodeControlBlock> {
        self.inode_table.get_mut(&ino)
    }

    pub fn contains(&self, ino: Inode) -> bool {
        self.inode_table.contains_key(&ino)
    }

    /// Insert an ICB directly (for ensure_org_inode / ensure_repo_inode patterns).
    pub fn insert_icb(&mut self, ino: Inode, icb: InodeControlBlock) {
        self.inode_table.insert(ino, icb);
    }

    /// Insert an ICB only if absent (for translate_*_ino_to_* patterns).
    /// Returns a mutable reference to the (possibly pre-existing) ICB.
    pub fn entry_or_insert_icb(
        &mut self,
        ino: Inode,
        icb: impl FnOnce() -> InodeControlBlock,
    ) -> &mut InodeControlBlock {
        self.inode_table.entry(ino).or_insert_with(icb)
    }

    pub fn inode_count(&self) -> usize {
        self.inode_table.len()
    }

    // ── Attr caching ────────────────────────────────────────────────────

    pub fn get_attr(&self, ino: Inode) -> Option<FileAttr> {
        self.inode_table.get(&ino).and_then(|icb| icb.attr)
    }

    pub fn cache_attr(&mut self, ino: Inode, attr: FileAttr) {
        if let Some(icb) = self.inode_table.get_mut(&ino) {
            icb.attr = Some(attr);
        }
    }

    // ── Reference counting ──────────────────────────────────────────────

    /// Increment rc. Panics (via unwrap) if inode doesn't exist.
    pub fn inc_rc(&mut self, ino: Inode) -> u64 {
        let icb = self
            .inode_table
            .get_mut(&ino)
            .unwrap_or_else(|| unreachable!("inc_rc: inode {ino} not in table"));
        icb.rc += 1;
        icb.rc
    }

    /// Decrement rc by `nlookups`. Returns `Some(evicted_icb)` if the inode was evicted.
    pub fn forget(&mut self, ino: Inode, nlookups: u64) -> Option<InodeControlBlock> {
        match self.inode_table.entry(ino) {
            std::collections::hash_map::Entry::Occupied(mut entry) => {
                if entry.get().rc <= nlookups {
                    trace!(ino, "evicting inode");
                    Some(entry.remove())
                } else {
                    entry.get_mut().rc -= nlookups;
                    trace!(ino, new_rc = entry.get().rc, "decremented rc");
                    None
                }
            }
            std::collections::hash_map::Entry::Vacant(_) => {
                warn!(ino, "forget on unknown inode");
                None
            }
        }
    }

    // ── Child inode management ──────────────────────────────────────────

    /// Ensure a child inode exists under `parent` with the given `name` and `kind`.
    /// Reuses existing inode if present. Does NOT bump rc.
    pub fn ensure_child_inode(
        &mut self,
        parent: Inode,
        name: &OsStr,
        kind: DirEntryType,
    ) -> (Inode, FileAttr) {
        // Check existing child by parent + name.
        if let Some((&existing_ino, _)) = self
            .inode_table
            .iter()
            .find(|&(&_ino, icb)| icb.parent == Some(parent) && icb.path.as_os_str() == name)
        {
            if let Some(attr) = self.inode_table.get(&existing_ino).and_then(|icb| icb.attr) {
                return (existing_ino, attr);
            }

            warn!(ino = existing_ino, parent, name = ?name, ?kind,
                "ensure_child_inode: attr missing on existing inode, rebuilding");
            let attr = self.make_attr_for_kind(existing_ino, kind);
            self.cache_attr(existing_ino, attr);
            return (existing_ino, attr);
        }

        let ino = self.inode_factory.allocate();
        self.inode_table.insert(
            ino,
            InodeControlBlock {
                rc: 0,
                path: name.into(),
                parent: Some(parent),
                children: None,
                attr: None,
            },
        );

        let attr = self.make_attr_for_kind(ino, kind);
        self.cache_attr(ino, attr);
        (ino, attr)
    }

    // ── Attr construction ───────────────────────────────────────────────

    pub fn make_common_file_attr(
        &self,
        ino: Inode,
        perm: u16,
        atime: SystemTime,
        mtime: SystemTime,
    ) -> CommonFileAttr {
        CommonFileAttr {
            ino,
            atime,
            mtime,
            ctime: SystemTime::UNIX_EPOCH,
            crtime: SystemTime::UNIX_EPOCH,
            perm: Permissions::from_bits_truncate(perm),
            nlink: 1,
            uid: self.fs_owner.0,
            gid: self.fs_owner.1,
            blksize: self.block_size,
        }
    }

    fn make_attr_for_kind(&self, ino: Inode, kind: DirEntryType) -> FileAttr {
        let now = SystemTime::now();
        match kind {
            DirEntryType::Directory => FileAttr::Directory {
                common: self.make_common_file_attr(ino, 0o755, now, now),
            },
            DirEntryType::RegularFile
            | DirEntryType::Symlink
            | DirEntryType::CharDevice
            | DirEntryType::BlockDevice
            | DirEntryType::NamedPipe
            | DirEntryType::Socket => FileAttr::RegularFile {
                common: self.make_common_file_attr(ino, 0o644, now, now),
                size: 0,
                blocks: 0,
            },
        }
    }

    // ── Filesystem stats ────────────────────────────────────────────────

    pub fn fs_owner(&self) -> (u32, u32) {
        self.fs_owner
    }

    pub fn statfs(&self) -> FilesystemStats {
        FilesystemStats {
            block_size: self.block_size,
            fragment_size: u64::from(self.block_size),
            total_blocks: 0,
            free_blocks: 0,
            available_blocks: 0,
            total_inodes: self.inode_table.len() as u64,
            free_inodes: 0,
            available_inodes: 0,
            filesystem_id: 0,
            mount_flags: 0,
            max_filename_length: 255,
        }
    }
}

pub fn blocks_of_size(block_size: u32, size: u64) -> u64 {
    size.div_ceil(u64::from(block_size))
}
