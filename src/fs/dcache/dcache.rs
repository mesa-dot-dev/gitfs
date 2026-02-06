//! Mescloud-specific directory cache wrapper.
//!
//! Composes [`DCache<InodeControlBlock>`] with inode allocation, attribute
//! caching, and filesystem-owner metadata.

use std::ffi::OsStr;
use std::time::SystemTime;

use tracing::warn;

use crate::fs::r#trait::{
    CommonFileAttr, DirEntryType, FileAttr, FilesystemStats, Inode, Permissions,
};
use crate::fs::mescloud::dcache::InodeControlBlock;

use super::DCache;

// ── InodeFactory ────────────────────────────────────────────────────────

/// Monotonically increasing inode allocator.
struct InodeFactory {
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

// ── MescloudDCache ──────────────────────────────────────────────────────

/// Mescloud-specific directory cache.
///
/// Wraps [`DCache<InodeControlBlock>`] and adds inode allocation, attribute
/// caching, `ensure_child_inode`, and filesystem metadata.
pub struct MescloudDCache {
    inner: DCache<InodeControlBlock>,
    inode_factory: InodeFactory,
    fs_owner: (u32, u32),
    block_size: u32,
}

impl std::ops::Deref for MescloudDCache {
    type Target = DCache<InodeControlBlock>;
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl std::ops::DerefMut for MescloudDCache {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

impl MescloudDCache {
    /// Create a new `MescloudDCache`. Initializes root ICB (rc=1), caches root dir attr.
    pub fn new(root_ino: Inode, fs_owner: (u32, u32), block_size: u32) -> Self {
        let mut dcache = Self {
            inner: DCache::new(root_ino, "/"),
            inode_factory: InodeFactory::new(root_ino + 1),
            fs_owner,
            block_size,
        };

        let now = SystemTime::now();
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

    // ── Attr caching ────────────────────────────────────────────────────

    pub fn get_attr(&self, ino: Inode) -> Option<FileAttr> {
        self.inner.get_icb(ino).and_then(|icb| icb.attr)
    }

    pub fn cache_attr(&mut self, ino: Inode, attr: FileAttr) {
        if let Some(icb) = self.inner.get_icb_mut(ino) {
            icb.attr = Some(attr);
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
        let existing = self
            .inner
            .iter()
            .find(|&(&_ino, icb)| icb.parent == Some(parent) && icb.path.as_os_str() == name)
            .map(|(&ino, _)| ino);

        if let Some(existing_ino) = existing {
            if let Some(attr) = self.inner.get_icb(existing_ino).and_then(|icb| icb.attr) {
                return (existing_ino, attr);
            }

            warn!(ino = existing_ino, parent, name = ?name, ?kind,
                "ensure_child_inode: attr missing on existing inode, rebuilding");
            let attr = self.make_attr_for_kind(existing_ino, kind);
            self.cache_attr(existing_ino, attr);
            return (existing_ino, attr);
        }

        let ino = self.inode_factory.allocate();
        self.inner.insert_icb(
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
            total_inodes: self.inner.inode_count() as u64,
            free_inodes: 0,
            available_inodes: 0,
            filesystem_id: 0,
            mount_flags: 0,
            max_filename_length: 255,
        }
    }
}
