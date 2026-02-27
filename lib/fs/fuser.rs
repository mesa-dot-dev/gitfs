//! FUSE adapter: maps [`fuser::Filesystem`] callbacks to [`AsyncFs`](super::async_fs::AsyncFs).

use std::collections::HashMap;
use std::ffi::{OsStr, OsString};
use std::sync::Arc;

use bytes::Bytes;

use super::async_fs::{FileReader as _, FsDataProvider, OverlayReader};
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
    fuser::ReplyWrite,
    fuser::ReplyCreate,
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

type FuseWard<DP> = crate::drop_ward::DropWard<
    super::async_fs::ForgetContext<DP>,
    InodeAddr,
    super::async_fs::InodeForget,
>;

struct FuseBridgeInner<DP: FsDataProvider> {
    ward: FuseWard<DP>,
    fs: super::async_fs::AsyncFs<DP>,
}

impl<DP: FsDataProvider> FuseBridgeInner<DP> {
    fn create(table: FutureBackedCache<InodeAddr, INode>, provider: DP) -> Self {
        let table = Arc::new(table);
        let fs = super::async_fs::AsyncFs::new_preseeded(provider.clone(), Arc::clone(&table));
        let ctx = super::async_fs::ForgetContext {
            inode_table: table,
            dcache: fs.directory_cache(),
            lookup_cache: fs.lookup_cache(),
            provider,
            write_overlay: fs.write_overlay(),
        };
        let ward = crate::drop_ward::DropWard::new(ctx);
        Self { ward, fs }
    }

    fn get_fs(&self) -> &super::async_fs::AsyncFs<DP> {
        &self.fs
    }

    fn ward_inc(&mut self, addr: InodeAddr) -> usize {
        self.ward.inc(addr)
    }

    fn ward_dec_count(&mut self, addr: InodeAddr, count: usize) -> Option<usize> {
        self.ward.dec_count(&addr, count)
    }
}

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

/// Convert a fuser `TimeOrNow` to a `SystemTime`.
fn resolve_fuser_time(t: fuser::TimeOrNow) -> std::time::SystemTime {
    match t {
        fuser::TimeOrNow::SpecificTime(t) => t,
        fuser::TimeOrNow::Now => std::time::SystemTime::now(),
    }
}

const BLOCK_SIZE: u32 = 4096;

/// Snapshot of a directory listing created by `opendir`.
enum DirSnapshot {
    /// Directory handle allocated but not yet populated. Carries the inode
    /// address of the directory so `readdir` can assert consistency with
    /// the `ino` parameter that fuser provides.
    Pending(InodeAddr),
    /// Fully materialized directory listing.
    Ready(Vec<(InodeAddr, OsString, INodeType)>),
}

/// Bridges a generic [`FsDataProvider`] to the [`fuser::Filesystem`] trait.
///
/// Owns a self-referential inode table + ward + [`AsyncFs`](super::async_fs::AsyncFs),
/// plus an open-file map, a directory-handle map, and a tokio runtime handle
/// for blocking on async ops.
pub struct FuserAdapter<DP: FsDataProvider> {
    inner: FuseBridgeInner<DP>,
    open_files: HashMap<FileHandle, Arc<OverlayReader<DP::Reader>>>,
    dir_handles: HashMap<FileHandle, DirSnapshot>,
    next_dir_fh: u64,
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
            dir_handles: HashMap::new(),
            next_dir_fh: 1,
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
                let tracked = self
                    .inner
                    .get_fs()
                    .lookup(LoadedAddr::new_unchecked(parent), name)
                    .await?;
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
            .block_on(async {
                self.inner
                    .get_fs()
                    .getattr(LoadedAddr::new_unchecked(ino))
                    .await
            })
            .fuse_reply(reply, |inode, reply| {
                let attr = inode_to_fuser_attr(&inode, BLOCK_SIZE);
                debug!(?attr, "replying...");
                reply.attr(&Self::SHAMEFUL_TTL, &attr);
            });
    }

    #[instrument(
        name = "FuserAdapter::setattr",
        skip(
            self, _req, _mode, _uid, _gid, size, atime, mtime, _ctime, _fh, _crtime, _chgtime,
            _bkuptime, _flags, reply
        )
    )]
    fn setattr(
        &mut self,
        _req: &fuser::Request<'_>,
        ino: u64,
        _mode: Option<u32>,
        _uid: Option<u32>,
        _gid: Option<u32>,
        size: Option<u64>,
        atime: Option<fuser::TimeOrNow>,
        mtime: Option<fuser::TimeOrNow>,
        _ctime: Option<std::time::SystemTime>,
        _fh: Option<u64>,
        _crtime: Option<std::time::SystemTime>,
        _chgtime: Option<std::time::SystemTime>,
        _bkuptime: Option<std::time::SystemTime>,
        _flags: Option<u32>,
        reply: fuser::ReplyAttr,
    ) {
        let new_atime = atime.map(resolve_fuser_time);
        let new_mtime = mtime.map(resolve_fuser_time);
        self.runtime
            .block_on(async {
                self.inner
                    .get_fs()
                    .setattr(
                        LoadedAddr::new_unchecked(ino),
                        size,
                        new_atime,
                        new_mtime,
                    )
                    .await
            })
            .fuse_reply(reply, |inode, reply| {
                let attr = inode_to_fuser_attr(&inode, BLOCK_SIZE);
                debug!(?attr, "replying...");
                reply.attr(&Self::SHAMEFUL_TTL, &attr);
            });
    }

    #[instrument(name = "FuserAdapter::opendir", skip(self, _req, _flags, reply))]
    fn opendir(
        &mut self,
        _req: &fuser::Request<'_>,
        ino: u64,
        _flags: i32,
        reply: fuser::ReplyOpen,
    ) {
        let fh = self.next_dir_fh;
        self.next_dir_fh += 1;
        self.dir_handles.insert(fh, DirSnapshot::Pending(ino));
        debug!(handle = fh, "replying...");
        reply.opened(fh, 0);
    }

    #[instrument(name = "FuserAdapter::readdir", skip(self, _req, fh, offset, reply))]
    fn readdir(
        &mut self,
        _req: &fuser::Request<'_>,
        ino: u64,
        fh: u64,
        offset: i64,
        reply: fuser::ReplyDirectory,
    ) {
        let offset_u64 = offset.cast_unsigned();

        // Lazily populate the snapshot on the first readdir call for this handle.
        let needs_populate = match self.dir_handles.get(&fh) {
            Some(DirSnapshot::Pending(stored_ino)) => {
                debug_assert_eq!(
                    *stored_ino, ino,
                    "readdir ino mismatch: opendir recorded {stored_ino} but readdir received {ino}"
                );
                true
            }
            None => true,
            Some(DirSnapshot::Ready(_)) => false,
        };

        let snapshot = if needs_populate {
            let result = self.runtime.block_on(async {
                let mut entries = Vec::new();
                self.inner
                    .get_fs()
                    .readdir(LoadedAddr::new_unchecked(ino), 0, |de, _next_offset| {
                        entries.push((de.inode.addr, de.name.to_os_string(), de.inode.itype));
                        false
                    })
                    .await?;
                Ok::<_, std::io::Error>(entries)
            });
            match result {
                Ok(entries) => {
                    self.dir_handles.insert(fh, DirSnapshot::Ready(entries));
                }
                Err(e) => {
                    debug!(error = %e, "replying error");
                    reply.error(io_to_errno(&e));
                    return;
                }
            }
            match self.dir_handles.get(&fh) {
                Some(DirSnapshot::Ready(entries)) => entries,
                _ => unreachable!("just inserted Ready"),
            }
        } else {
            match self.dir_handles.get(&fh) {
                Some(DirSnapshot::Ready(entries)) => entries,
                _ => unreachable!("checked above"),
            }
        };

        #[expect(
            clippy::cast_possible_truncation,
            reason = "offset fits in usize on supported 64-bit platforms"
        )]
        let skip = offset_u64 as usize;
        let mut reply = reply;

        for (i, (entry_ino, entry_name, entry_itype)) in snapshot.iter().enumerate().skip(skip) {
            let kind = inode_type_to_fuser(*entry_itype);
            let abs_idx = i + 1;
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

    #[instrument(
        name = "FuserAdapter::releasedir",
        skip(self, _req, _ino, fh, _flags, reply)
    )]
    fn releasedir(
        &mut self,
        _req: &fuser::Request<'_>,
        _ino: u64,
        fh: u64,
        _flags: i32,
        reply: fuser::ReplyEmpty,
    ) {
        self.dir_handles.remove(&fh);
        debug!("replying ok");
        reply.ok();
    }

    #[instrument(name = "FuserAdapter::open", skip(self, _req, flags, reply))]
    fn open(&mut self, _req: &fuser::Request<'_>, ino: u64, flags: i32, reply: fuser::ReplyOpen) {
        let flags = OpenFlags::from_bits_truncate(flags);
        self.runtime
            .block_on(async {
                let open_file = self
                    .inner
                    .get_fs()
                    .open(LoadedAddr::new_unchecked(ino), flags)
                    .await?;
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
        self.runtime
            .block_on(async {
                let reader = self
                    .open_files
                    .get(&fh)
                    .ok_or_else(|| std::io::Error::from_raw_os_error(libc::EBADF))?;
                reader.read(offset.cast_unsigned(), size).await
            })
            .fuse_reply(reply, |data, reply| {
                debug!(read_bytes = data.len(), "replying...");
                reply.data(&data);
            });
    }

    #[instrument(
        name = "FuserAdapter::write",
        skip(
            self,
            _req,
            _ino,
            fh,
            offset,
            data,
            _write_flags,
            _flags,
            _lock_owner,
            reply
        )
    )]
    fn write(
        &mut self,
        _req: &fuser::Request<'_>,
        _ino: u64,
        fh: u64,
        offset: i64,
        data: &[u8],
        _write_flags: u32,
        _flags: i32,
        _lock_owner: Option<u64>,
        reply: fuser::ReplyWrite,
    ) {
        let Some(reader) = self.open_files.get(&fh) else {
            reply.error(libc::EBADF);
            return;
        };
        let addr = reader.addr;
        let data = Bytes::from(data.to_vec());

        self.runtime
            .block_on(async {
                self.inner
                    .get_fs()
                    .write(
                        LoadedAddr::new_unchecked(addr),
                        offset.cast_unsigned(),
                        data,
                    )
                    .await
            })
            .fuse_reply(reply, |written, reply| {
                debug!(bytes_written = written, "replying...");
                reply.written(written);
            });
    }

    #[instrument(
        name = "FuserAdapter::create",
        skip(self, _req, name, _mode, _umask, flags, reply)
    )]
    fn create(
        &mut self,
        _req: &fuser::Request<'_>,
        parent: u64,
        name: &OsStr,
        _mode: u32,
        _umask: u32,
        flags: i32,
        reply: fuser::ReplyCreate,
    ) {
        let _ = flags; // flags are passed to open internally by AsyncFs::create
        self.runtime
            .block_on(async {
                let (inode, open_file) = self
                    .inner
                    .get_fs()
                    .create(LoadedAddr::new_unchecked(parent), name, 0o666)
                    .await?;
                self.inner.ward_inc(inode.addr);
                let fh = open_file.fh;
                self.open_files.insert(fh, Arc::clone(&open_file.reader));
                Ok::<_, std::io::Error>((inode, fh))
            })
            .fuse_reply(reply, |(inode, fh), reply| {
                let f_attr = inode_to_fuser_attr(&inode, BLOCK_SIZE);
                debug!(?f_attr, handle = fh, "replying...");
                reply.created(&Self::SHAMEFUL_TTL, &f_attr, 0, fh, 0);
            });
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
