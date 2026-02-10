//! Mescloud-specific inode control block, helpers, and directory cache wrapper.

use std::ffi::OsStr;
use std::time::SystemTime;

use tracing::warn;

use crate::fs::icache::{ICache, IcbLike, InodeFactory};
use crate::fs::r#trait::{
    CommonFileAttr, DirEntry, DirEntryType, FileAttr, FilesystemStats, Inode, Permissions,
};

/// Inode control block for mescloud filesystem layers (`MesaFS`, `OrgFs`, `RepoFs`).
pub struct InodeControlBlock {
    /// The root inode doesn't have a parent.
    pub parent: Option<Inode>,
    pub rc: u64,
    pub path: std::path::PathBuf,
    pub children: Option<Vec<DirEntry>>,
    /// Cached file attributes from the last lookup.
    pub attr: Option<FileAttr>,
}

impl IcbLike for InodeControlBlock {
    fn new_root(path: std::path::PathBuf) -> Self {
        Self {
            rc: 1,
            parent: None,
            path,
            children: None,
            attr: None,
        }
    }

    fn rc(&self) -> u64 {
        self.rc
    }

    fn rc_mut(&mut self) -> &mut u64 {
        &mut self.rc
    }
}

/// Calculate the number of blocks needed for a given size.
pub fn blocks_of_size(block_size: u32, size: u64) -> u64 {
    size.div_ceil(u64::from(block_size))
}

/// Mescloud-specific directory cache.
///
/// Wraps [`ICache<InodeControlBlock>`] and adds inode allocation, attribute
/// caching, `ensure_child_inode`, and filesystem metadata.
pub struct MescloudICache {
    inner: ICache<InodeControlBlock>,
    inode_factory: InodeFactory,
    fs_owner: (u32, u32),
    block_size: u32,
}

impl std::ops::Deref for MescloudICache {
    type Target = ICache<InodeControlBlock>;
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl std::ops::DerefMut for MescloudICache {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

impl MescloudICache {
    /// Create a new `MescloudICache`. Initializes root ICB (rc=1), caches root dir attr.
    pub fn new(root_ino: Inode, fs_owner: (u32, u32), block_size: u32) -> Self {
        let mut icache = Self {
            inner: ICache::new(root_ino, "/"),
            inode_factory: InodeFactory::new(root_ino + 1),
            fs_owner,
            block_size,
        };

        let now = SystemTime::now();
        let root_attr = FileAttr::Directory {
            common: icache.make_common_file_attr(root_ino, 0o755, now, now),
        };
        icache.cache_attr(root_ino, root_attr);
        icache
    }

    /// Allocate a new inode number.
    pub fn allocate_inode(&mut self) -> Inode {
        self.inode_factory.allocate()
    }

    pub fn get_attr(&self, ino: Inode) -> Option<FileAttr> {
        self.inner.get_icb(ino).and_then(|icb| icb.attr)
    }

    pub fn cache_attr(&mut self, ino: Inode, attr: FileAttr) {
        if let Some(icb) = self.inner.get_icb_mut(ino) {
            icb.attr = Some(attr);
        }
    }

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
