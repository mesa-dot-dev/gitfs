//! Useful filesystem generalizations.
//!
//! # Cache invalidation
//!
//! The current implementation caches directory listings and inode data
//! indefinitely once populated. Staleness is mitigated only by a short
//! FUSE entry/attr TTL (currently 1 second in `FuserAdapter`).
//!
//! The intended long-term strategy is to use FUSE kernel notifications
//! (`notify_inval_inode` / `notify_inval_entry`) to proactively invalidate
//! specific entries when the backing data changes. This would allow a
//! much higher TTL while still reflecting changes promptly. The key
//! changes needed:
//!
//! 1. `DCache` needs a `remove` or `invalidate` method to reset a
//!    parent's `PopulateStatus` back to `UNCLAIMED`.
//! 2. `FuserAdapter` needs access to the `fuser::Session` handle to
//!    send `notify_inval_entry` notifications.
//! 3. Data providers need a way to signal when their backing data changes
//!    (e.g. webhook, polling, or subscription).

/// Async filesystem cache with concurrent inode management.
pub mod async_fs;
/// Lock-free bidirectional inode address mapping.
pub mod bridge;
/// Generic composite filesystem types.
pub mod composite;
/// Directory entry cache for fast parent-child lookups.
pub mod dcache;
/// FUSE adapter: maps [`fuser::Filesystem`] callbacks to [`async_fs::AsyncFs`].
pub mod fuser;
/// Reverse-indexed lookup cache for O(k) inode eviction.
pub mod indexed_lookup_cache;

pub use async_fs::{InodeForget, InodeLifecycle, LookupCache, OpenFile, ResolvedINode};
pub use indexed_lookup_cache::IndexedLookupCache;

use std::ffi::OsStr;
use std::time::SystemTime;

use bitflags::bitflags;

/// Type representing an inode identifier.
pub type InodeAddr = u64;

/// The conventional root inode address used by all filesystem layers.
///
/// Both [`CompositeFs`](composite::CompositeFs) and data providers use this
/// value as the root address. Monotonic inode counters start at `ROOT_INO + 1`.
pub const ROOT_INO: InodeAddr = 1;

/// Represents an inode address that has been loaded into the inode table.
///
/// This newtype wrapper distinguishes inode addresses that are known to exist
/// in the [`async_fs::AsyncFs`] inode table from raw [`InodeAddr`] values.
///
/// The inner field is private to prevent unchecked construction. Code within
/// the crate may use [`LoadedAddr::new_unchecked`] at trusted boundaries
/// (e.g. after inserting into the inode table, or at the FUSE adapter boundary
/// where the kernel provides addresses it previously received from us).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct LoadedAddr(InodeAddr);

impl LoadedAddr {
    /// Construct a `LoadedAddr` without validating that the address exists in
    /// the inode table.
    ///
    /// # Safety contract (logical, not `unsafe`)
    ///
    /// The caller must ensure one of:
    /// - The address was previously inserted into an inode table, **or**
    /// - The address originates from the FUSE kernel (which only knows
    ///   addresses we previously returned to it).
    #[doc(hidden)]
    #[must_use]
    pub fn new_unchecked(addr: InodeAddr) -> Self {
        Self(addr)
    }

    /// Return the raw inode address.
    #[must_use]
    pub fn addr(self) -> InodeAddr {
        self.0
    }
}

/// Type representing a file handle.
pub type FileHandle = u64;

bitflags! {
    /// Permission bits for an inode, similar to Unix file permissions.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
    pub struct InodePerms: u16 {
        /// Other: execute permission.
        const OTHER_EXECUTE = 1 << 0;
        /// Other: write permission.
        const OTHER_WRITE   = 1 << 1;
        /// Other: read permission.
        const OTHER_READ    = 1 << 2;

        /// Group: execute permission.
        const GROUP_EXECUTE = 1 << 3;
        /// Group: write permission.
        const GROUP_WRITE   = 1 << 4;
        /// Group: read permission.
        const GROUP_READ    = 1 << 5;

        /// Owner: execute permission.
        const OWNER_EXECUTE = 1 << 6;
        /// Owner: write permission.
        const OWNER_WRITE   = 1 << 7;
        /// Owner: read permission.
        const OWNER_READ    = 1 << 8;

        /// Sticky bit.
        const STICKY        = 1 << 9;
        /// Set-group-ID bit.
        const SETGID        = 1 << 10;
        /// Set-user-ID bit.
        const SETUID        = 1 << 11;

        /// Other: read, write, and execute.
        const OTHER_RWX = Self::OTHER_READ.bits()
            | Self::OTHER_WRITE.bits()
            | Self::OTHER_EXECUTE.bits();
        /// Group: read, write, and execute.
        const GROUP_RWX = Self::GROUP_READ.bits()
            | Self::GROUP_WRITE.bits()
            | Self::GROUP_EXECUTE.bits();
        /// Owner: read, write, and execute.
        const OWNER_RWX = Self::OWNER_READ.bits()
            | Self::OWNER_WRITE.bits()
            | Self::OWNER_EXECUTE.bits();
    }
}

bitflags! {
    /// Flags for opening a file, similar to Unix open(2) flags.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
    pub struct OpenFlags: i32 {
        /// Open for reading only.
        const RDONLY = libc::O_RDONLY;
        /// Open for writing only.
        const WRONLY = libc::O_WRONLY;
        /// Open for reading and writing.
        const RDWR = libc::O_RDWR;

        /// Append on each write.
        const APPEND = libc::O_APPEND;
        /// Truncate to zero length.
        const TRUNC = libc::O_TRUNC;
        /// Create file if it does not exist.
        const CREAT = libc::O_CREAT;
        /// Error if file already exists (with `CREAT`).
        const EXCL = libc::O_EXCL;

        /// Non-blocking mode.
        const NONBLOCK = libc::O_NONBLOCK;
        /// Synchronous writes.
        const SYNC = libc::O_SYNC;
        /// Synchronous data integrity writes.
        const DSYNC = libc::O_DSYNC;
        /// Do not follow symlinks.
        const NOFOLLOW = libc::O_NOFOLLOW;
        /// Set close-on-exec.
        const CLOEXEC = libc::O_CLOEXEC;
        /// Fail if not a directory.
        const DIRECTORY = libc::O_DIRECTORY;

        /// Do not update access time (Linux only).
        #[cfg(target_os = "linux")]
        const NOATIME = libc::O_NOATIME;
    }
}

/// The type of an inode entry in the filesystem.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum INodeType {
    /// A regular file.
    File,
    /// A directory.
    Directory,
    /// A symbolic link.
    Symlink,
}

/// Representation of an inode.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct INode {
    /// The address of this inode, which serves as its unique identifier.
    pub addr: InodeAddr,
    /// The permissions associated with this inode, represented as a bitfield.
    pub permissions: InodePerms,
    /// The user ID of the owner of this inode.
    pub uid: u32,
    /// The group ID of the owner of this inode.
    pub gid: u32,
    /// The time this inode was created at.
    pub create_time: SystemTime,
    /// The time this inode was last modified at.
    pub last_modified_at: SystemTime,
    /// The parent inode address, if any. This is `None` for the root inode.
    pub parent: Option<InodeAddr>,
    /// The size of the file represented by this inode, in bytes.
    pub size: u64,
    /// Additional information about the type of this inode (e.g., file vs directory).
    pub itype: INodeType,
}

impl INode {
    /// Check if this inode is the root inode (i.e., has no parent).
    #[must_use]
    pub fn is_root(&self) -> bool {
        self.parent.is_none()
    }
}

/// A directory entry yielded by [`async_fs::AsyncFs::readdir`].
///
/// Borrows the entry name from the directory cache's iteration buffer.
#[derive(Debug, Clone, Copy)]
pub struct DirEntry<'a> {
    /// The name of this entry within its parent directory.
    pub name: &'a OsStr,
    /// The full inode data for this entry.
    pub inode: INode,
}

/// Filesystem statistics returned by [`async_fs::AsyncFs::statfs`].
///
/// Block-related sizes are in units of `block_size` bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct AsyncFsStats {
    /// Filesystem block size (bytes).
    pub block_size: u32,
    /// Total number of data blocks.
    pub total_blocks: u64,
    /// Number of free blocks.
    pub free_blocks: u64,
    /// Number of blocks available to unprivileged users.
    pub available_blocks: u64,
    /// Total number of file nodes (inodes).
    pub total_inodes: u64,
    /// Number of free file nodes.
    pub free_inodes: u64,
    /// Maximum filename length (bytes).
    pub max_filename_length: u32,
}
