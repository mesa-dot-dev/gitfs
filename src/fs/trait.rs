//! Generic trait for implementing filesystems.
//!
//! Note that this is a slightly cleaner interface than directly using fuser. The whole point of
//! this is to abstract away fuser-specific details.
use async_trait::async_trait;
use std::{
    ffi::{OsStr, OsString},
    time::{Duration, SystemTime},
};
use tracing::error;

use bitflags::bitflags;
use bytes::Bytes;

/// Type representing an inode.
pub type Inode = u64;

pub type FileHandle = u64;

/// An opaque lock owner identifier provided by the kernel.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct LockOwner(pub u64);

bitflags! {
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
    pub struct Permissions: u16 {
        // Other
        const OTHER_EXECUTE = 1 << 0;
        const OTHER_WRITE   = 1 << 1;
        const OTHER_READ    = 1 << 2;

        // Group
        const GROUP_EXECUTE = 1 << 3;
        const GROUP_WRITE   = 1 << 4;
        const GROUP_READ    = 1 << 5;

        // Owner
        const OWNER_EXECUTE = 1 << 6;
        const OWNER_WRITE   = 1 << 7;
        const OWNER_READ    = 1 << 8;

        // Special bits
        const STICKY        = 1 << 9;
        const SETGID        = 1 << 10;
        const SETUID        = 1 << 11;

        const OTHER_RWX = Self::OTHER_READ.bits()
            | Self::OTHER_WRITE.bits()
            | Self::OTHER_EXECUTE.bits();
        const GROUP_RWX = Self::GROUP_READ.bits()
            | Self::GROUP_WRITE.bits()
            | Self::GROUP_EXECUTE.bits();
        const OWNER_RWX = Self::OWNER_READ.bits()
            | Self::OWNER_WRITE.bits()
            | Self::OWNER_EXECUTE.bits();
    }
}

bitflags! {
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
    pub struct OpenFlags: i32 {
        // Access modes (mutually exclusive)
        const RDONLY = libc::O_RDONLY;
        const WRONLY = libc::O_WRONLY;
        const RDWR = libc::O_RDWR;

        // Creation/status flags
        const APPEND = libc::O_APPEND;
        const TRUNC = libc::O_TRUNC;
        const CREAT = libc::O_CREAT;
        const EXCL = libc::O_EXCL;

        // Behavior flags
        const NONBLOCK = libc::O_NONBLOCK;
        const SYNC = libc::O_SYNC;
        const DSYNC = libc::O_DSYNC;
        const NOFOLLOW = libc::O_NOFOLLOW;
        const CLOEXEC = libc::O_CLOEXEC;
        const DIRECTORY = libc::O_DIRECTORY;

        #[cfg(target_os = "linux")]
        const NOATIME = libc::O_NOATIME;
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct CommonFileAttr {
    pub ino: Inode,
    pub atime: SystemTime,
    pub mtime: SystemTime,
    pub ctime: SystemTime,
    pub crtime: SystemTime,
    pub perm: Permissions,
    pub nlink: u32,
    pub uid: u32,
    pub gid: u32,
    pub blksize: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum FileAttr {
    RegularFile {
        common: CommonFileAttr,
        size: u64,
        blocks: u64,
    },
    Directory {
        common: CommonFileAttr,
    },
    Symlink {
        common: CommonFileAttr,
        size: u64,
    },
    CharDevice {
        common: CommonFileAttr,
        rdev: u64,
    },
    BlockDevice {
        common: CommonFileAttr,
        rdev: u64,
    },
    NamedPipe {
        common: CommonFileAttr,
    },
    Socket {
        common: CommonFileAttr,
    },
}

impl FileAttr {
    pub fn common(&self) -> &CommonFileAttr {
        match self {
            Self::RegularFile { common, .. }
            | Self::Directory { common }
            | Self::Symlink { common, .. }
            | Self::CharDevice { common, .. }
            | Self::BlockDevice { common, .. }
            | Self::NamedPipe { common }
            | Self::Socket { common } => common,
        }
    }
}

bitflags! {
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
    pub (crate) struct FileOpenOptions: u32 {
        const DIRECT_IO = 1 << 0;
        const KEEP_CACHE = 1 << 1;
        const NONSEEKABLE = 1 << 2;
        const STREAM = 1 << 4;
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct OpenFile {
    pub handle: FileHandle,
    pub options: FileOpenOptions,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DirEntryType {
    RegularFile,
    Directory,
    Symlink,
    CharDevice,
    BlockDevice,
    NamedPipe,
    Socket,
}

impl TryFrom<std::fs::Metadata> for FileAttr {
    type Error = ();

    #[expect(
        clippy::cast_possible_truncation,
        reason = "metadata mode/nlink/blksize narrowing is intentional"
    )]
    #[expect(
        clippy::cast_sign_loss,
        reason = "nsecs from MetadataExt is always in [0, 999_999_999]"
    )]
    fn try_from(meta: std::fs::Metadata) -> Result<Self, Self::Error> {
        use std::os::unix::fs::FileTypeExt as _;
        use std::os::unix::fs::MetadataExt as _;

        fn to_systime(secs: i64, nsecs: i64) -> SystemTime {
            if secs >= 0 {
                std::time::UNIX_EPOCH + Duration::new(secs.cast_unsigned(), nsecs as u32)
            } else {
                // nsecs is always in [0, 999_999_999] from MetadataExt.
                // For negative secs, subtract whole seconds then add back nsecs.
                std::time::UNIX_EPOCH - Duration::from_secs((-secs).cast_unsigned())
                    + Duration::from_nanos(nsecs.cast_unsigned())
            }
        }

        let common_attr = CommonFileAttr {
            ino: meta.ino(),
            atime: to_systime(meta.atime(), meta.atime_nsec()),
            mtime: to_systime(meta.mtime(), meta.mtime_nsec()),
            ctime: to_systime(meta.ctime(), meta.ctime_nsec()),
            crtime: to_systime(0, 0), // Not available in std::fs::Metadata
            perm: Permissions::from_bits_truncate(meta.mode() as u16),
            nlink: meta.nlink() as u32,
            uid: meta.uid(),
            gid: meta.gid(),
            blksize: meta.blksize() as u32,
        };

        let ft = meta.file_type();
        if ft.is_file() {
            Ok(Self::RegularFile {
                common: common_attr,
                size: meta.len(),
                blocks: meta.blocks(),
            })
        } else if ft.is_dir() {
            Ok(Self::Directory {
                common: common_attr,
            })
        } else if ft.is_symlink() {
            Ok(Self::Symlink {
                common: common_attr,
                size: meta.len(),
            })
        } else if ft.is_char_device() {
            Ok(Self::CharDevice {
                common: common_attr,
                rdev: meta.rdev(),
            })
        } else if ft.is_block_device() {
            Ok(Self::BlockDevice {
                common: common_attr,
                rdev: meta.rdev(),
            })
        } else if ft.is_fifo() {
            Ok(Self::NamedPipe {
                common: common_attr,
            })
        } else if ft.is_socket() {
            Ok(Self::Socket {
                common: common_attr,
            })
        } else {
            debug_assert!(
                false,
                "Unknown file type encountered in FileAttr conversion"
            );
            Err(())
        }
    }
}

impl From<FileAttr> for DirEntryType {
    fn from(attr: FileAttr) -> Self {
        match attr {
            FileAttr::RegularFile { .. } => Self::RegularFile,
            FileAttr::Directory { .. } => Self::Directory,
            FileAttr::Symlink { .. } => Self::Symlink,
            FileAttr::CharDevice { .. } => Self::CharDevice,
            FileAttr::BlockDevice { .. } => Self::BlockDevice,
            FileAttr::NamedPipe { .. } => Self::NamedPipe,
            FileAttr::Socket { .. } => Self::Socket,
        }
    }
}

impl TryFrom<std::fs::FileType> for DirEntryType {
    type Error = ();

    fn try_from(ft: std::fs::FileType) -> Result<Self, ()> {
        use std::os::unix::fs::FileTypeExt as _;

        if ft.is_file() {
            Ok(Self::RegularFile)
        } else if ft.is_dir() {
            Ok(Self::Directory)
        } else if ft.is_symlink() {
            Ok(Self::Symlink)
        } else if ft.is_char_device() {
            Ok(Self::CharDevice)
        } else if ft.is_block_device() {
            Ok(Self::BlockDevice)
        } else if ft.is_fifo() {
            Ok(Self::NamedPipe)
        } else if ft.is_socket() {
            Ok(Self::Socket)
        } else {
            debug_assert!(
                false,
                "Unknown file type encountered in DirEntryType conversion"
            );
            error!(ft = ?ft, "Unknown file type encountered in DirEntryType conversion");
            Err(())
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct DirEntry {
    pub ino: Inode,
    // TODO(markovejnovic): This OsString is hella expensive
    pub name: OsString,
    pub kind: DirEntryType,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct FilesystemStats {
    pub block_size: u32,
    pub fragment_size: u64,
    pub total_blocks: u64,
    pub free_blocks: u64,
    pub available_blocks: u64,
    pub total_inodes: u64,
    pub free_inodes: u64,
    pub available_inodes: u64,
    pub filesystem_id: u64,
    pub mount_flags: u32,
    pub max_filename_length: u32,
}

#[async_trait]
pub trait Fs {
    type LookupError: std::error::Error;
    type GetAttrError: std::error::Error;
    type OpenError: std::error::Error;
    type ReadError: std::error::Error;
    type ReaddirError: std::error::Error;
    type ReleaseError: std::error::Error;

    /// For each lookup call made by the kernel, it expects the dcache to be updated with the
    /// returned `FileAttr`.
    async fn lookup(&mut self, parent: Inode, name: &OsStr) -> Result<FileAttr, Self::LookupError>;

    /// Can be called in two contexts -- the file is not open (in which case `fh` is `None`),
    /// or the file is open (in which case `fh` is `Some`).
    async fn getattr(
        &mut self,
        ino: Inode,
        fh: Option<FileHandle>,
    ) -> Result<FileAttr, Self::GetAttrError>;

    /// Read the contents of a directory.
    async fn readdir(&mut self, ino: Inode) -> Result<&[DirEntry], Self::ReaddirError>;

    /// Open a file for reading.
    async fn open(&mut self, ino: Inode, flags: OpenFlags) -> Result<OpenFile, Self::OpenError>;

    /// Read data from an open file.
    #[expect(clippy::too_many_arguments, reason = "mirrors fuser read API")]
    async fn read(
        &mut self,
        ino: Inode,
        fh: FileHandle,
        offset: u64,
        size: u32,
        flags: OpenFlags,
        lock_owner: Option<LockOwner>,
    ) -> Result<Bytes, Self::ReadError>;

    /// Called when the kernel closes a file handle.
    async fn release(
        &mut self,
        ino: Inode,
        fh: FileHandle,
        flags: OpenFlags,
        flush: bool,
    ) -> Result<(), Self::ReleaseError>;

    /// Called when the kernel is done with an inode.
    async fn forget(&mut self, ino: Inode, nlookups: u64);

    /// Get filesystem statistics.
    async fn statfs(&mut self) -> Result<FilesystemStats, std::io::Error>;
}
