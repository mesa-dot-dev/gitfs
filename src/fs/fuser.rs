use std::ffi::OsStr;
use std::sync::Arc;

use crate::fs::r#trait::{CommonFileAttr, DirEntryType, FileAttr, Fs, LockOwner, OpenFlags};
use tracing::{debug, error, instrument};

impl From<FileAttr> for fuser::FileAttr {
    fn from(val: FileAttr) -> Self {
        fn common_to_fuser(common: CommonFileAttr) -> fuser::FileAttr {
            fuser::FileAttr {
                ino: common.ino,
                size: 0,
                blocks: 0,
                atime: common.atime,
                mtime: common.mtime,
                ctime: common.ctime,
                crtime: common.crtime,
                kind: fuser::FileType::RegularFile,
                perm: common.perm.bits(),
                nlink: common.nlink,
                uid: common.uid,
                gid: common.gid,
                rdev: 0,
                blksize: common.blksize,
                flags: 0,
            }
        }

        match val {
            FileAttr::RegularFile {
                common,
                size,
                blocks,
            } => {
                let mut attr = common_to_fuser(common);
                attr.size = size;
                attr.blocks = blocks;
                attr.kind = fuser::FileType::RegularFile;
                attr
            }
            FileAttr::Directory { common } => {
                let mut attr = common_to_fuser(common);
                attr.kind = fuser::FileType::Directory;
                attr
            }
            FileAttr::Symlink { common, size } => {
                let mut attr = common_to_fuser(common);
                attr.size = size;
                attr.kind = fuser::FileType::Symlink;
                attr
            }
            FileAttr::CharDevice { common, rdev } => {
                let mut attr = common_to_fuser(common);
                debug_assert!(u32::try_from(rdev).is_ok(), "rdev value {rdev} too large");
                attr.rdev = rdev
                    .try_into()
                    .map_err(|_| {
                        error!("rdev value {rdev} too large for fuser::FileAttr");
                    })
                    .unwrap_or(0);
                attr.kind = fuser::FileType::CharDevice;
                attr
            }
            FileAttr::BlockDevice { common, rdev } => {
                let mut attr = common_to_fuser(common);
                debug_assert!(u32::try_from(rdev).is_ok(), "rdev value {rdev} too large");
                attr.rdev = rdev
                    .try_into()
                    .map_err(|_| {
                        error!("rdev value {rdev} too large for fuser::FileAttr");
                    })
                    .unwrap_or(0);
                attr.kind = fuser::FileType::BlockDevice;
                attr
            }
            FileAttr::NamedPipe { common } => {
                let mut attr = common_to_fuser(common);
                attr.kind = fuser::FileType::NamedPipe;
                attr
            }
            FileAttr::Socket { common } => {
                let mut attr = common_to_fuser(common);
                attr.kind = fuser::FileType::Socket;
                attr
            }
        }
    }
}

impl From<DirEntryType> for fuser::FileType {
    fn from(val: DirEntryType) -> Self {
        match val {
            DirEntryType::RegularFile => Self::RegularFile,
            DirEntryType::Directory => Self::Directory,
            DirEntryType::Symlink => Self::Symlink,
            DirEntryType::CharDevice => Self::CharDevice,
            DirEntryType::BlockDevice => Self::BlockDevice,
            DirEntryType::NamedPipe => Self::NamedPipe,
            DirEntryType::Socket => Self::Socket,
        }
    }
}

impl From<i32> for OpenFlags {
    fn from(val: i32) -> Self {
        Self::from_bits_truncate(val)
    }
}

pub struct FuserAdapter<F: Fs>
where
    F::LookupError: Into<i32>,
    F::GetAttrError: Into<i32>,
    F::OpenError: Into<i32>,
    F::ReadError: Into<i32>,
    F::ReaddirError: Into<i32>,
    F::ReleaseError: Into<i32>,
{
    fs: Arc<F>,
    runtime: tokio::runtime::Handle,
}

impl<F: Fs> FuserAdapter<F>
where
    F::LookupError: Into<i32>,
    F::GetAttrError: Into<i32>,
    F::OpenError: Into<i32>,
    F::ReadError: Into<i32>,
    F::ReaddirError: Into<i32>,
    F::ReleaseError: Into<i32>,
{
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

    pub fn new(fs: F, runtime: tokio::runtime::Handle) -> Self {
        Self {
            fs: Arc::new(fs),
            runtime,
        }
    }
}

impl<F: Fs> fuser::Filesystem for FuserAdapter<F>
where
    F::LookupError: Into<i32>,
    F::GetAttrError: Into<i32>,
    F::OpenError: Into<i32>,
    F::ReadError: Into<i32>,
    F::ReaddirError: Into<i32>,
    F::ReleaseError: Into<i32>,
{
    fn init(
        &mut self,
        _req: &fuser::Request<'_>,
        _config: &mut fuser::KernelConfig,
    ) -> Result<(), libc::c_int> {
        self.runtime.block_on(self.fs.init());
        Ok(())
    }

    #[instrument(name = "FuserAdapter::lookup", skip(self, _req, reply))]
    fn lookup(
        &mut self,
        _req: &fuser::Request<'_>,
        parent: u64,
        name: &OsStr,
        reply: fuser::ReplyEntry,
    ) {
        let fs = Arc::clone(&self.fs);
        let name = name.to_owned();
        let ttl = Self::SHAMEFUL_TTL;
        self.runtime.spawn(async move {
            match fs.lookup(parent, &name).await {
                Ok(attr) => {
                    let f_attr: fuser::FileAttr = attr.into();
                    debug!(?f_attr, "replying...");
                    reply.entry(&ttl, &f_attr, 0);
                }
                Err(e) => {
                    debug!(error = %e, "replying error");
                    reply.error(e.into());
                }
            }
        });
    }

    #[instrument(name = "FuserAdapter::getattr", skip(self, _req, fh, reply))]
    fn getattr(
        &mut self,
        _req: &fuser::Request<'_>,
        ino: u64,
        fh: Option<u64>,
        reply: fuser::ReplyAttr,
    ) {
        let fs = Arc::clone(&self.fs);
        let ttl = Self::SHAMEFUL_TTL;
        self.runtime.spawn(async move {
            match fs.getattr(ino, fh).await {
                Ok(attr) => {
                    debug!(?attr, "replying...");
                    reply.attr(&ttl, &attr.into());
                }
                Err(e) => {
                    debug!(error = %e, "replying error");
                    reply.error(e.into());
                }
            }
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
        let fs = Arc::clone(&self.fs);
        self.runtime.spawn(async move {
            let entries = match fs.readdir(ino).await {
                Ok(entries) => entries,
                Err(e) => {
                    debug!(error = %e, "replying error");
                    reply.error(e.into());
                    return;
                }
            };

            #[expect(
                clippy::cast_possible_truncation,
                reason = "fuser offset is i64 but always non-negative"
            )]
            for (i, entry) in entries
                .iter()
                .enumerate()
                .skip(offset.cast_unsigned() as usize)
            {
                let kind: fuser::FileType = entry.kind.into();
                let Ok(idx): Result<i64, _> = (i + 1).try_into() else {
                    error!("Directory entry index {} too large for fuser", i + 1);
                    reply.error(libc::EIO);
                    return;
                };

                debug!(?entry, "adding entry to reply...");
                if reply.add(entry.ino, idx, kind, &entry.name) {
                    debug!("buffer full for now, stopping readdir");
                    break;
                }
            }

            debug!("finalizing reply...");
            reply.ok();
        });
    }

    #[instrument(name = "FuserAdapter::open", skip(self, _req, flags, reply))]
    fn open(&mut self, _req: &fuser::Request<'_>, ino: u64, flags: i32, reply: fuser::ReplyOpen) {
        let fs = Arc::clone(&self.fs);
        let flags: OpenFlags = flags.into();
        self.runtime.spawn(async move {
            match fs.open(ino, flags).await {
                Ok(open_file) => {
                    debug!(handle = open_file.handle, "replying...");
                    reply.opened(open_file.handle, 0);
                }
                Err(e) => {
                    debug!(error = %e, "replying error");
                    reply.error(e.into());
                }
            }
        });
    }

    #[instrument(
        name = "FuserAdapter::read",
        skip(self, _req, fh, offset, size, flags, lock_owner, reply)
    )]
    fn read(
        &mut self,
        _req: &fuser::Request<'_>,
        ino: u64,
        fh: u64,
        offset: i64,
        size: u32,
        flags: i32,
        lock_owner: Option<u64>,
        reply: fuser::ReplyData,
    ) {
        let fs = Arc::clone(&self.fs);
        let flags: OpenFlags = flags.into();
        let lock_owner = lock_owner.map(LockOwner);
        self.runtime.spawn(async move {
            match fs
                .read(ino, fh, offset.cast_unsigned(), size, flags, lock_owner)
                .await
            {
                Ok(data) => {
                    debug!(read_bytes = data.len(), "replying...");
                    reply.data(&data);
                }
                Err(e) => {
                    debug!(error = %e, "replying error");
                    reply.error(e.into());
                }
            }
        });
    }

    #[instrument(name = "FuserAdapter::release", skip(self, _req, _lock_owner, reply))]
    fn release(
        &mut self,
        _req: &fuser::Request<'_>,
        ino: u64,
        fh: u64,
        flags: i32,
        _lock_owner: Option<u64>,
        flush: bool,
        reply: fuser::ReplyEmpty,
    ) {
        let fs = Arc::clone(&self.fs);
        let flags: OpenFlags = flags.into();
        self.runtime.spawn(async move {
            match fs.release(ino, fh, flags, flush).await {
                Ok(()) => {
                    debug!("replying ok");
                    reply.ok();
                }
                Err(e) => {
                    debug!(error = %e, "replying error");
                    reply.error(e.into());
                }
            }
        });
    }

    #[instrument(name = "FuserAdapter::forget", skip(self, _req, nlookup))]
    fn forget(&mut self, _req: &fuser::Request<'_>, ino: u64, nlookup: u64) {
        let fs = Arc::clone(&self.fs);
        self.runtime.spawn(async move {
            fs.forget(ino, nlookup).await;
        });
    }

    #[instrument(name = "FuserAdapter::statfs", skip(self, _req, _ino, reply))]
    fn statfs(&mut self, _req: &fuser::Request<'_>, _ino: u64, reply: fuser::ReplyStatfs) {
        let fs = Arc::clone(&self.fs);
        self.runtime.spawn(async move {
            match fs.statfs().await {
                Ok(statvfs) => {
                    debug!(?statvfs, "replying...");
                    reply.statfs(
                        statvfs.total_blocks,
                        statvfs.free_blocks,
                        statvfs.available_blocks,
                        statvfs.total_inodes,
                        statvfs.free_inodes,
                        statvfs.block_size,
                        statvfs.max_filename_length,
                        0,
                    );
                }
                Err(e) => {
                    debug!(error = %e, "replying error");
                    reply.error(e.raw_os_error().unwrap_or(libc::EIO));
                }
            }
        });
    }
}
