//! FUSE adapter: maps [`fuser::Filesystem`] callbacks to [`AsyncFs`](super::async_fs::AsyncFs).

use std::collections::HashMap;
use std::ffi::OsStr;
use std::sync::Arc;

use super::async_fs::{FileReader as _, FsDataProvider};
use super::{FileHandle, INode, INodeType, InodeAddr, LoadedAddr, OpenFlags};
use crate::cache::async_backed::FutureBackedCache;
use tracing::{debug, error, instrument};

/// Convert an I/O error to the corresponding errno value for FUSE replies.
#[expect(
    clippy::wildcard_enum_match_arm,
    reason = "ErrorKind is non_exhaustive; EIO is the safe default"
)]
fn io_to_errno(e: &std::io::Error) -> i32 {
    e.raw_os_error().unwrap_or_else(|| match e.kind() {
        std::io::ErrorKind::NotFound => libc::ENOENT,
        std::io::ErrorKind::PermissionDenied => libc::EACCES,
        std::io::ErrorKind::AlreadyExists => libc::EEXIST,
        _ => libc::EIO,
    })
}

/// Trait abstracting the `.error(errno)` method common to all fuser reply types.
trait FuseReply {
    fn error(self, errno: i32);
}

macro_rules! impl_fuse_reply {
    ($($ty:ty),* $(,)?) => {
        $(impl FuseReply for $ty {
            fn error(self, errno: i32) {
                // Calls the inherent fuser method (not this trait method).
                self.error(errno);
            }
        })*
    };
}

// ReplyEmpty and ReplyStatfs are excluded: release and statfs
// do not follow the block_on -> fuse_reply pattern.
impl_fuse_reply!(
    fuser::ReplyEntry,
    fuser::ReplyAttr,
    fuser::ReplyDirectory,
    fuser::ReplyOpen,
    fuser::ReplyData,
);

/// Extension trait on `Result<T, std::io::Error>` for FUSE reply handling.
///
/// Centralizes the error-logging + errno-reply path so each FUSE callback
/// only has to express its success path.
trait FuseResultExt<T> {
    fn fuse_reply<R: FuseReply>(self, reply: R, on_ok: impl FnOnce(T, R));
}

impl<T> FuseResultExt<T> for Result<T, std::io::Error> {
    fn fuse_reply<R: FuseReply>(self, reply: R, on_ok: impl FnOnce(T, R)) {
        match self {
            Ok(val) => on_ok(val, reply),
            Err(e) => {
                debug!(error = %e, "replying error");
                reply.error(io_to_errno(&e));
            }
        }
    }
}

mod inner {
    #![allow(clippy::future_not_send, clippy::mem_forget)]

    use ouroboros::self_referencing;

    use crate::cache::async_backed::FutureBackedCache;
    use crate::drop_ward::DropWard;
    use crate::fs::async_fs::{AsyncFs, FsDataProvider, InodeForget};
    use crate::fs::{INode, InodeAddr};

    /// Self-referential struct holding the inode table, refcount ward, and `AsyncFs`.
    ///
    /// Both `ward` and `fs` borrow from `table`. The ward manages inode
    /// refcounts; the fs serves lookup/readdir/open/read operations.
    #[self_referencing]
    pub(super) struct FuseBridgeInner<DP: FsDataProvider> {
        table: FutureBackedCache<InodeAddr, INode>,
        #[borrows(table)]
        #[not_covariant]
        ward: DropWard<&'this FutureBackedCache<InodeAddr, INode>, InodeAddr, InodeForget>,
        #[borrows(table)]
        #[covariant]
        fs: AsyncFs<'this, DP>,
    }

    impl<DP: FsDataProvider> FuseBridgeInner<DP> {
        pub(super) fn create(table: FutureBackedCache<InodeAddr, INode>, provider: DP) -> Self {
            FuseBridgeInnerBuilder {
                table,
                ward_builder: |tbl| DropWard::new(tbl),
                fs_builder: |tbl| AsyncFs::new_preseeded(provider, tbl),
            }
            .build()
        }

        pub(super) fn get_fs(&self) -> &AsyncFs<'_, DP> {
            self.borrow_fs()
        }

        pub(super) fn ward_inc(&mut self, addr: InodeAddr) -> usize {
            self.with_ward_mut(|ward| ward.inc(addr))
        }

        pub(super) fn ward_dec_count(&mut self, addr: InodeAddr, count: usize) -> Option<usize> {
            self.with_ward_mut(|ward| ward.dec_count(&addr, count))
        }
    }
}

use inner::FuseBridgeInner;

/// Convert an `INode` to the fuser-specific `FileAttr`.
fn inode_to_fuser_attr(inode: &INode, block_size: u32) -> fuser::FileAttr {
    fuser::FileAttr {
        ino: inode.addr,
        size: inode.size,
        blocks: inode.size.div_ceil(512),
        atime: inode.last_modified_at,
        mtime: inode.last_modified_at,
        ctime: inode.last_modified_at,
        crtime: inode.create_time,
        kind: inode_type_to_fuser(inode.itype),
        perm: inode.permissions.bits(),
        nlink: 1,
        uid: inode.uid,
        gid: inode.gid,
        rdev: 0,
        blksize: block_size,
        flags: 0,
    }
}

#[expect(
    clippy::wildcard_enum_match_arm,
    reason = "INodeType is non_exhaustive; File is the safe default"
)]
fn inode_type_to_fuser(itype: INodeType) -> fuser::FileType {
    match itype {
        INodeType::Directory => fuser::FileType::Directory,
        INodeType::Symlink => fuser::FileType::Symlink,
        _ => fuser::FileType::RegularFile,
    }
}

const BLOCK_SIZE: u32 = 4096;

/// Bridges a generic [`FsDataProvider`] to the [`fuser::Filesystem`] trait.
///
/// Owns a self-referential inode table + ward + [`AsyncFs`](super::async_fs::AsyncFs),
/// plus an open-file map and a tokio runtime handle for blocking on async ops.
pub struct FuserAdapter<DP: FsDataProvider> {
    inner: FuseBridgeInner<DP>,
    open_files: HashMap<FileHandle, Arc<DP::Reader>>,
    runtime: tokio::runtime::Handle,
}

impl<DP: FsDataProvider> FuserAdapter<DP> {
    // TODO(markovejnovic): This low TTL is really not ideal. It slows us down a lot, since the
    // kernel has to ask us for every single lookup all the time.
    //
    // I think a better implementation is to implement
    //
    // notify_inval_inode(ino, offset, len)
    // notify_inval_entry(parent_ino, name)
    //
    // These two functions can be used to invalidate specific entries in the kernel cache when we
    // know they have changed. This would allow us to set a much higher TTL here.
    const SHAMEFUL_TTL: std::time::Duration = std::time::Duration::from_secs(1);

    /// Create a new adapter from a pre-seeded inode table and data provider.
    ///
    /// The `table` must already have the root inode inserted.
    pub fn new(
        table: FutureBackedCache<InodeAddr, INode>,
        provider: DP,
        runtime: tokio::runtime::Handle,
    ) -> Self {
        Self {
            inner: FuseBridgeInner::create(table, provider),
            open_files: HashMap::new(),
            runtime,
        }
    }
}

impl<DP: FsDataProvider> fuser::Filesystem for FuserAdapter<DP> {
    #[instrument(name = "FuserAdapter::lookup", skip(self, _req, reply))]
    fn lookup(
        &mut self,
        _req: &fuser::Request<'_>,
        parent: u64,
        name: &OsStr,
        reply: fuser::ReplyEntry,
    ) {
        self.runtime
            .block_on(async {
                let tracked = self.inner.get_fs().lookup(LoadedAddr(parent), name).await?;
                self.inner.ward_inc(tracked.inode.addr);
                Ok::<_, std::io::Error>(tracked.inode)
            })
            .fuse_reply(reply, |inode, reply| {
                let f_attr = inode_to_fuser_attr(&inode, BLOCK_SIZE);
                debug!(?f_attr, "replying...");
                reply.entry(&Self::SHAMEFUL_TTL, &f_attr, 0);
            });
    }

    #[instrument(name = "FuserAdapter::getattr", skip(self, _req, _fh, reply))]
    fn getattr(
        &mut self,
        _req: &fuser::Request<'_>,
        ino: u64,
        _fh: Option<u64>,
        reply: fuser::ReplyAttr,
    ) {
        self.runtime
            .block_on(async { self.inner.get_fs().getattr(LoadedAddr(ino)).await })
            .fuse_reply(reply, |inode, reply| {
                let attr = inode_to_fuser_attr(&inode, BLOCK_SIZE);
                debug!(?attr, "replying...");
                reply.attr(&Self::SHAMEFUL_TTL, &attr);
            });
    }

    #[instrument(name = "FuserAdapter::readdir", skip(self, _req, _fh, offset, reply))]
    fn readdir(
        &mut self,
        _req: &fuser::Request<'_>,
        ino: u64,
        _fh: u64,
        offset: i64,
        mut reply: fuser::ReplyDirectory,
    ) {
        let offset_u64 = offset.cast_unsigned();
        let result = self.runtime.block_on(async {
            let mut entries = Vec::new();
            self.inner
                .get_fs()
                .readdir(LoadedAddr(ino), offset_u64, |de, _next_offset| {
                    entries.push((de.inode.addr, de.name.to_os_string(), de.inode.itype));
                    false
                })
                .await?;
            Ok::<_, std::io::Error>(entries)
        });

        let entries = match result {
            Ok(entries) => entries,
            Err(e) => {
                debug!(error = %e, "replying error");
                reply.error(io_to_errno(&e));
                return;
            }
        };

        #[expect(
            clippy::cast_possible_truncation,
            reason = "offset fits in usize on supported 64-bit platforms"
        )]
        for (i, (entry_ino, entry_name, entry_itype)) in entries.iter().enumerate() {
            let kind = inode_type_to_fuser(*entry_itype);
            let abs_idx = offset_u64 as usize + i + 1;
            let Ok(idx): Result<i64, _> = abs_idx.try_into() else {
                error!("Directory entry index {} too large for fuser", abs_idx);
                reply.error(libc::EIO);
                return;
            };

            debug!(?entry_name, ino = entry_ino, "adding entry to reply...");
            if reply.add(*entry_ino, idx, kind, entry_name) {
                debug!("buffer full for now, stopping readdir");
                break;
            }
        }

        debug!("finalizing reply...");
        reply.ok();
    }

    #[instrument(name = "FuserAdapter::open", skip(self, _req, flags, reply))]
    fn open(&mut self, _req: &fuser::Request<'_>, ino: u64, flags: i32, reply: fuser::ReplyOpen) {
        let flags = OpenFlags::from_bits_truncate(flags);
        self.runtime
            .block_on(async {
                let open_file = self.inner.get_fs().open(LoadedAddr(ino), flags).await?;
                let fh = open_file.fh;
                self.open_files.insert(fh, Arc::clone(&open_file.reader));
                Ok::<_, std::io::Error>(fh)
            })
            .fuse_reply(reply, |fh, reply| {
                debug!(handle = fh, "replying...");
                reply.opened(fh, 0);
            });
    }

    #[instrument(
        name = "FuserAdapter::read",
        skip(self, _req, _ino, fh, offset, size, _flags, _lock_owner, reply)
    )]
    fn read(
        &mut self,
        _req: &fuser::Request<'_>,
        _ino: u64,
        fh: u64,
        offset: i64,
        size: u32,
        _flags: i32,
        _lock_owner: Option<u64>,
        reply: fuser::ReplyData,
    ) {
        let result = self.runtime.block_on(async {
            let reader = self
                .open_files
                .get(&fh)
                .ok_or_else(|| std::io::Error::from_raw_os_error(libc::EBADF))?;
            reader.read(offset.cast_unsigned(), size).await
        });
        match result {
            Ok(data) => {
                debug!(read_bytes = data.len(), "replying...");
                reply.data(&data);
            }
            Err(e) => {
                debug!(error = %e, "replying error");
                reply.error(io_to_errno(&e));
            }
        }
    }

    #[instrument(
        name = "FuserAdapter::release",
        skip(self, _req, _ino, fh, _flags, _lock_owner, _flush, reply)
    )]
    fn release(
        &mut self,
        _req: &fuser::Request<'_>,
        _ino: u64,
        fh: u64,
        _flags: i32,
        _lock_owner: Option<u64>,
        _flush: bool,
        reply: fuser::ReplyEmpty,
    ) {
        if let Some(reader) = self.open_files.remove(&fh) {
            if let Err(e) = self.runtime.block_on(reader.close()) {
                debug!(error = %e, "reader close reported error");
            }
            debug!("replying ok");
            reply.ok();
        } else {
            debug!("file handle not open, replying error");
            reply.error(libc::EBADF);
        }
    }

    #[expect(
        clippy::cast_possible_truncation,
        reason = "nlookups fits in usize on supported 64-bit platforms"
    )]
    #[instrument(name = "FuserAdapter::forget", skip(self, _req, nlookup))]
    fn forget(&mut self, _req: &fuser::Request<'_>, ino: u64, nlookup: u64) {
        self.inner.ward_dec_count(ino, nlookup as usize);
    }

    #[instrument(name = "FuserAdapter::statfs", skip(self, _req, _ino, reply))]
    fn statfs(&mut self, _req: &fuser::Request<'_>, _ino: u64, reply: fuser::ReplyStatfs) {
        let stats = self.inner.get_fs().statfs();
        debug!(?stats, "replying...");
        reply.statfs(
            stats.total_blocks,
            stats.free_blocks,
            stats.available_blocks,
            stats.total_inodes,
            stats.free_inodes,
            stats.block_size,
            stats.max_filename_length,
            0,
        );
    }
}
