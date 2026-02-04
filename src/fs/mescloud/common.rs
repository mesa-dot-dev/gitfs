//! Shared types and helpers used by both `MesaFS` and `RepoFs`.

use std::{collections::HashMap, ffi::OsStr, time::SystemTime};

use thiserror::Error;
use tracing::warn;

use crate::fs::r#trait::{CommonFileAttr, DirEntry, DirEntryType, FileAttr, Inode, Permissions};

pub(super) struct InodeFactory {
    next_inode: Inode,
}

impl InodeFactory {
    pub(super) fn new(start: Inode) -> Self {
        Self { next_inode: start }
    }

    pub(super) fn allocate(&mut self) -> Inode {
        let ino = self.next_inode;
        self.next_inode += 1;
        ino
    }
}

pub(super) struct InodeControlBlock {
    /// The root inode doesn't have a parent.
    pub parent: Option<Inode>,
    pub rc: u64,
    pub path: std::path::PathBuf,
    pub children: Option<Vec<DirEntry>>,
    /// Cached file attributes from the last lookup.
    pub attr: Option<FileAttr>,
}

pub(super) fn blocks_of_size(block_size: u32, size: u64) -> u64 {
    size.div_ceil(u64::from(block_size))
}

pub(super) fn make_common_file_attr(
    fs_owner: (u32, u32),
    block_size: u32,
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
        uid: fs_owner.0,
        gid: fs_owner.1,
        blksize: block_size,
    }
}

pub(super) fn cache_attr(
    inode_table: &mut HashMap<Inode, InodeControlBlock>,
    ino: Inode,
    attr: FileAttr,
) {
    if let Some(icb) = inode_table.get_mut(&ino) {
        icb.attr = Some(attr);
    }
}

/// Ensure a child inode exists under `parent` with the given `name` and `kind`.
///
/// Reuses an existing inode if one already exists for this parent+name pair.
/// Does NOT bump rc — callers that create kernel-visible references must bump rc themselves.
#[expect(
    clippy::too_many_arguments,
    reason = "inode creation requires all these contextual parameters"
)]
pub(super) fn ensure_child_inode(
    inode_table: &mut HashMap<Inode, InodeControlBlock>,
    inode_factory: &mut InodeFactory,
    fs_owner: (u32, u32),
    block_size: u32,
    parent: Inode,
    name: &OsStr,
    kind: DirEntryType,
) -> (Inode, FileAttr) {
    // Check if an inode already exists for this child under this parent.
    if let Some((&existing_ino, _)) = inode_table
        .iter()
        .find(|&(&_ino, icb)| icb.parent == Some(parent) && icb.path.as_os_str() == name)
    {
        if let Some(attr) = inode_table.get(&existing_ino).and_then(|icb| icb.attr) {
            return (existing_ino, attr);
        }

        // Attr missing, rebuild from kind.
        warn!(ino = existing_ino, parent, name = ?name, ?kind, "ensure_child_inode: attr missing on existing inode, rebuilding");
        let now = SystemTime::now();
        let attr = match kind {
            DirEntryType::Directory => FileAttr::Directory {
                common: make_common_file_attr(fs_owner, block_size, existing_ino, 0o755, now, now),
            },
            DirEntryType::RegularFile
            | DirEntryType::Symlink
            | DirEntryType::CharDevice
            | DirEntryType::BlockDevice
            | DirEntryType::NamedPipe
            | DirEntryType::Socket => FileAttr::RegularFile {
                common: make_common_file_attr(fs_owner, block_size, existing_ino, 0o644, now, now),
                size: 0,
                blocks: 0,
            },
        };
        cache_attr(inode_table, existing_ino, attr);
        return (existing_ino, attr);
    }

    // No existing inode — allocate without bumping rc.
    let ino = inode_factory.allocate();
    let now = SystemTime::now();
    inode_table.insert(
        ino,
        InodeControlBlock {
            rc: 0,
            path: name.into(),
            parent: Some(parent),
            children: None,
            attr: None,
        },
    );

    let attr = match kind {
        DirEntryType::Directory => FileAttr::Directory {
            common: make_common_file_attr(fs_owner, block_size, ino, 0o755, now, now),
        },
        DirEntryType::RegularFile
        | DirEntryType::Symlink
        | DirEntryType::CharDevice
        | DirEntryType::BlockDevice
        | DirEntryType::NamedPipe
        | DirEntryType::Socket => FileAttr::RegularFile {
            common: make_common_file_attr(fs_owner, block_size, ino, 0o644, now, now),
            size: 0,
            blocks: 0,
        },
    };
    cache_attr(inode_table, ino, attr);
    (ino, attr)
}

// ── Error types ──────────────────────────────────────────────────────────────

#[derive(Debug, Error)]
pub enum LookupError {
    #[error("inode not found")]
    InodeNotFound,

    #[error("file does not exist")]
    FileDoesNotExist,

    #[error("remote mesa error: {0}")]
    RemoteMesaError(#[from] mesa_dev::error::MesaError),
}

impl From<LookupError> for i32 {
    fn from(e: LookupError) -> Self {
        match e {
            LookupError::InodeNotFound | LookupError::FileDoesNotExist => libc::ENOENT,
            LookupError::RemoteMesaError(_) => libc::EIO,
        }
    }
}

#[derive(Debug, Error)]
pub enum GetAttrError {
    #[error("inode not found")]
    InodeNotFound,
}

impl From<GetAttrError> for i32 {
    fn from(e: GetAttrError) -> Self {
        match e {
            GetAttrError::InodeNotFound => libc::ENOENT,
        }
    }
}

#[derive(Debug, Error)]
pub enum OpenError {
    #[error("inode not found")]
    InodeNotFound,
}

impl From<OpenError> for i32 {
    fn from(e: OpenError) -> Self {
        match e {
            OpenError::InodeNotFound => libc::ENOENT,
        }
    }
}

#[derive(Debug, Error)]
pub enum ReadError {
    #[error("file not open")]
    FileNotOpen,

    #[error("inode not found")]
    InodeNotFound,

    #[error("remote mesa error: {0}")]
    RemoteMesaError(#[from] mesa_dev::error::MesaError),

    #[error("content is not a file")]
    NotAFile,

    #[error("base64 decode error: {0}")]
    Base64Decode(#[from] base64::DecodeError),
}

impl From<ReadError> for i32 {
    fn from(e: ReadError) -> Self {
        match e {
            ReadError::FileNotOpen => libc::EBADF,
            ReadError::InodeNotFound => libc::ENOENT,
            ReadError::RemoteMesaError(_) | ReadError::Base64Decode(_) => libc::EIO,
            ReadError::NotAFile => libc::EISDIR,
        }
    }
}

#[derive(Debug, Error)]
pub enum ReadDirError {
    #[error("inode not found")]
    InodeNotFound,

    #[error("remote mesa error: {0}")]
    RemoteMesaError(#[from] mesa_dev::error::MesaError),

    #[error("inode is not a directory")]
    NotADirectory,

    #[error("operation not permitted")]
    NotPermitted,
}

impl From<ReadDirError> for i32 {
    fn from(e: ReadDirError) -> Self {
        match e {
            ReadDirError::InodeNotFound => libc::ENOENT,
            ReadDirError::RemoteMesaError(_) => libc::EIO,
            ReadDirError::NotADirectory => libc::ENOTDIR,
            ReadDirError::NotPermitted => libc::EPERM,
        }
    }
}

#[derive(Debug, Error)]
pub enum ReleaseError {
    #[error("file not open")]
    FileNotOpen,
}

impl From<ReleaseError> for i32 {
    fn from(e: ReleaseError) -> Self {
        match e {
            ReleaseError::FileNotOpen => libc::EBADF,
        }
    }
}
