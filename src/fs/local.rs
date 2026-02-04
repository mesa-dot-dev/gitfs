//! An implementation of a filesystem that directly overlays the host filesystem.
use bytes::Bytes;
use nix::sys::statvfs::statvfs;
use std::{
    collections::{HashMap, hash_map::Entry},
    path::PathBuf,
};
use thiserror::Error;
use tokio::io::{AsyncReadExt as _, AsyncSeekExt as _};

use std::ffi::OsStr;
use tracing::warn;

use crate::fs::r#trait::{
    DirEntry, FileAttr, FileHandle, FileOpenOptions, FilesystemStats, Fs, Inode, LockOwner,
    OpenFile, OpenFlags,
};

#[derive(Debug, Error)]
pub enum LookupError {
    #[error("inode not found")]
    InodeNotFound,
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("invalid file type")]
    InvalidFileType,
}

impl From<LookupError> for i32 {
    fn from(e: LookupError) -> Self {
        match e {
            LookupError::InodeNotFound => libc::ENOENT,
            LookupError::Io(ref io_err) => io_err.raw_os_error().unwrap_or(libc::EIO),
            LookupError::InvalidFileType => libc::EINVAL,
        }
    }
}

#[derive(Debug, Error)]
pub enum GetAttrError {
    #[error("inode not found")]
    InodeNotFound,

    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("invalid file type")]
    InvalidFileType,
}

impl From<GetAttrError> for i32 {
    fn from(e: GetAttrError) -> Self {
        match e {
            GetAttrError::InodeNotFound => libc::ENOENT,
            GetAttrError::Io(ref io_err) => io_err.raw_os_error().unwrap_or(libc::EIO),
            GetAttrError::InvalidFileType => libc::EINVAL,
        }
    }
}

#[derive(Debug, Error)]
pub enum OpenError {
    #[error("inode not found")]
    InodeNotFound,

    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
}

impl From<OpenError> for i32 {
    fn from(e: OpenError) -> Self {
        match e {
            OpenError::InodeNotFound => libc::ENOENT,
            OpenError::Io(ref io_err) => io_err.raw_os_error().unwrap_or(libc::EIO),
        }
    }
}

#[derive(Debug, Error)]
pub enum ReadError {
    #[error("inode not found")]
    InodeNotFound,

    #[error("file not open")]
    FileNotOpen,

    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
}

impl From<ReadError> for i32 {
    fn from(e: ReadError) -> Self {
        match e {
            ReadError::InodeNotFound => libc::ENOENT,
            ReadError::FileNotOpen => libc::EBADF,
            ReadError::Io(ref io_err) => io_err.raw_os_error().unwrap_or(libc::EIO),
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

#[derive(Debug, Error)]
pub enum ReadDirError {
    #[error("inode not found")]
    InodeNotFound,

    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("invalid file type")]
    InvalidFileType,
}

impl From<ReadDirError> for i32 {
    fn from(e: ReadDirError) -> Self {
        match e {
            ReadDirError::InodeNotFound => libc::ENOENT,
            ReadDirError::Io(ref io_err) => io_err.raw_os_error().unwrap_or(libc::EIO),
            ReadDirError::InvalidFileType => libc::EINVAL,
        }
    }
}

struct InodeControlBlock {
    pub rc: u64,
    pub path: PathBuf,
    pub children: Option<Vec<DirEntry>>,
}

pub struct LocalFs {
    inode_table: HashMap<Inode, InodeControlBlock>,
    open_files: HashMap<FileHandle, tokio::fs::File>,
    next_fh: FileHandle,
}

impl LocalFs {
    #[expect(dead_code, reason = "alternative filesystem implementation")]
    pub fn new(abs_path: impl Into<PathBuf>) -> Self {
        let mut inode_table = HashMap::new();
        inode_table.insert(
            1,
            InodeControlBlock {
                rc: 1,
                path: abs_path.into(),
                children: None,
            },
        );

        Self {
            inode_table,
            open_files: HashMap::new(),
            next_fh: 1,
        }
    }

    fn abspath(&self) -> &PathBuf {
        &self
            .inode_table
            .get(&1)
            .unwrap_or_else(|| unreachable!("root inode 1 must always exist in inode_table"))
            .path
    }

    async fn parse_tokio_dirent(
        dir_entry: &tokio::fs::DirEntry,
    ) -> Result<DirEntry, tokio::io::Error> {
        return Ok(DirEntry {
            ino: dir_entry.ino(),
            name: dir_entry.file_name(),
            kind: dir_entry.file_type().await?.try_into().map_err(|()| {
                tokio::io::Error::new(
                    tokio::io::ErrorKind::InvalidData,
                    "invalid file type in directory entry",
                )
            })?,
        });
    }
}

#[async_trait::async_trait]
impl Fs for LocalFs {
    type LookupError = LookupError;
    type GetAttrError = GetAttrError;
    type OpenError = OpenError;
    type ReadError = ReadError;
    type ReaddirError = ReadDirError;
    type ReleaseError = ReleaseError;

    async fn lookup(&mut self, parent: Inode, name: &OsStr) -> Result<FileAttr, LookupError> {
        debug_assert!(
            self.inode_table.contains_key(&parent),
            "parent inode {parent} not in inode_table"
        );
        let parent_icb = self.inode_table.get(&parent).ok_or_else(|| {
            warn!(
                "Lookup called on unknown parent inode {}. This is a programming bug",
                parent
            );
            LookupError::InodeNotFound
        })?;

        let child_path = parent_icb.path.join(name);
        let meta = tokio::fs::metadata(&child_path)
            .await
            .map_err(LookupError::Io)?;

        let file_attr = FileAttr::try_from(meta).map_err(|()| LookupError::InvalidFileType);
        debug_assert!(file_attr.is_ok(), "FileAttr conversion failed unexpectedly");
        let file_attr = file_attr?;

        let map_entry =
            self.inode_table
                .entry(file_attr.common().ino)
                .or_insert(InodeControlBlock {
                    rc: 0,
                    path: child_path,
                    children: None,
                });
        map_entry.rc += 1;

        Ok(file_attr)
    }

    async fn getattr(
        &mut self,
        ino: Inode,
        fh: Option<FileHandle>,
    ) -> Result<FileAttr, GetAttrError> {
        if let Some(fh) = fh {
            // The file was already opened, we can just call fstat.
            debug_assert!(
                self.open_files.contains_key(&fh),
                "file handle {fh} not in open_files"
            );
            let file = self.open_files.get(&fh).ok_or_else(|| {
                warn!(
                    "GetAttr called on unknown file handle {}. This is a programming bug",
                    fh
                );
                GetAttrError::InodeNotFound
            })?;

            let meta = file.metadata().await.map_err(GetAttrError::Io)?;
            let file_attr = FileAttr::try_from(meta).map_err(|()| GetAttrError::InvalidFileType);
            debug_assert!(file_attr.is_ok(), "FileAttr conversion failed unexpectedly");

            Ok(file_attr?)
        } else {
            // No open path, so we have to do a painful stat on the path.
            debug_assert!(
                self.inode_table.contains_key(&ino),
                "inode {ino} not in inode_table"
            );
            let icb = self.inode_table.get(&ino).ok_or_else(|| {
                warn!(
                    "GetAttr called on unknown inode {}. This is a programming bug",
                    ino
                );
                GetAttrError::InodeNotFound
            })?;

            let meta = tokio::fs::metadata(&icb.path)
                .await
                .map_err(GetAttrError::Io)?;
            let file_attr = FileAttr::try_from(meta).map_err(|()| GetAttrError::InvalidFileType);
            debug_assert!(file_attr.is_ok(), "FileAttr conversion failed unexpectedly");

            Ok(file_attr?)
        }
    }

    async fn readdir(&mut self, ino: Inode) -> Result<&[DirEntry], ReadDirError> {
        debug_assert!(
            self.inode_table.contains_key(&ino),
            "inode {ino} not in inode_table"
        );

        let inode_cb = self.inode_table.get_mut(&ino).ok_or_else(|| {
            warn!(
                parent = ino,
                "Readdir of unknown parent inode. Programming bug"
            );
            ReadDirError::InodeNotFound
        })?;

        let mut read_dir = tokio::fs::read_dir(&inode_cb.path)
            .await
            .map_err(ReadDirError::Io)?;

        // Note that we HAVE to re-read all entries here, since there's really no way for us to
        // know whether another process has modified the underlying directory, without our consent.
        //
        // TODO(markovejnovic): If we can guarantee that only our process has access to the
        // underlying directory, we can avoid re-loading the entries every time.
        //
        // Two mechanisms appear to exist: namespace mount and/or file permissions.
        //
        // However, both of these mechanisms take time to develop and we don't have time.
        let mut entries: Vec<DirEntry> = Vec::new();
        while let Some(dir_entry) = read_dir.next_entry().await.map_err(ReadDirError::Io)? {
            entries.push(Self::parse_tokio_dirent(&dir_entry).await?);
        }

        let inode_cb = self.inode_table.get_mut(&ino).ok_or_else(|| {
            warn!(parent = ino, "inode disappeared. TOCTOU programming bug");
            ReadDirError::InodeNotFound
        })?;

        Ok(inode_cb.children.insert(entries))
    }

    async fn open(&mut self, ino: Inode, flags: OpenFlags) -> Result<OpenFile, OpenError> {
        debug_assert!(
            self.inode_table.contains_key(&ino),
            "inode {ino} not in inode_table"
        );
        let icb = self.inode_table.get(&ino).ok_or_else(|| {
            warn!(
                "Open called on unknown inode {}. This is a programming bug",
                ino
            );
            OpenError::InodeNotFound
        })?;

        // TODO(markovejnovic): Not all flags are supported here. We could do better.
        let file = tokio::fs::OpenOptions::new()
            .read(true)
            .write(flags.contains(OpenFlags::RDWR) || flags.contains(OpenFlags::WRONLY))
            .append(flags.contains(OpenFlags::APPEND))
            .truncate(flags.contains(OpenFlags::TRUNC))
            .create(flags.contains(OpenFlags::CREAT))
            .open(&icb.path)
            .await
            .map_err(OpenError::Io)?;

        // Generate a new file handle.
        let fh = self.next_fh;
        self.next_fh += 1;
        self.open_files.insert(fh, file);

        Ok(OpenFile {
            handle: fh,
            // TODO(markovejnovic): Might be interesting to set some of these options.
            options: FileOpenOptions::empty(),
        })
    }

    async fn read(
        &mut self,
        ino: Inode,
        fh: FileHandle,
        offset: u64,
        size: u32,
        _flags: OpenFlags,
        _lock_owner: Option<LockOwner>,
    ) -> Result<Bytes, ReadError> {
        // TODO(markovejnovic): Respect flags and lock_owner.
        debug_assert!(
            self.inode_table.contains_key(&ino),
            "inode {ino} not in inode_table"
        );
        debug_assert!(
            self.open_files.contains_key(&fh),
            "file handle {fh} not in open_files"
        );

        let file: &mut tokio::fs::File = self.open_files.get_mut(&fh).ok_or_else(|| {
            warn!(
                "Read called on unknown file handle {}. This is a programming bug",
                fh
            );
            ReadError::FileNotOpen
        })?;

        let mut buffer = vec![0u8; size as usize];
        file.seek(std::io::SeekFrom::Start(offset))
            .await
            .map_err(ReadError::Io)?;
        let nbytes = file.read(&mut buffer).await.map_err(ReadError::Io)?;

        buffer.truncate(nbytes);
        Ok(Bytes::from(buffer))
    }

    async fn release(
        &mut self,
        _ino: Inode,
        fh: FileHandle,
        _flags: OpenFlags,
        _flush: bool,
    ) -> Result<(), ReleaseError> {
        self.open_files.remove(&fh).ok_or_else(|| {
            warn!(
                "Release called on unknown file handle {}. Programming bug",
                fh
            );
            ReleaseError::FileNotOpen
        })?;
        Ok(())
    }

    async fn forget(&mut self, ino: Inode, nlookups: u64) {
        debug_assert!(
            self.inode_table.contains_key(&ino),
            "inode {ino} not in inode_table"
        );

        match self.inode_table.entry(ino) {
            Entry::Occupied(mut entry) => {
                if entry.get().rc <= nlookups {
                    entry.remove();
                } else {
                    entry.get_mut().rc -= nlookups;
                }
            }
            Entry::Vacant(_) => {
                warn!(
                    "Forget called on unknown inode {}. This is a programming bug",
                    ino
                );
            }
        }
    }

    async fn statfs(&mut self) -> Result<FilesystemStats, std::io::Error> {
        let stat = statvfs(self.abspath().as_path())?;

        Ok(FilesystemStats {
            block_size: stat.block_size().try_into().map_err(|_| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "block size too large to fit into u32",
                )
            })?,
            fragment_size: stat.fragment_size(),
            #[allow(clippy::allow_attributes)]
            #[allow(clippy::useless_conversion)]
            total_blocks: u64::from(stat.blocks()),
            #[allow(clippy::allow_attributes)]
            #[allow(clippy::useless_conversion)]
            free_blocks: u64::from(stat.blocks_free()),
            #[allow(clippy::allow_attributes)]
            #[allow(clippy::useless_conversion)]
            available_blocks: u64::from(stat.blocks_available()),
            total_inodes: self.inode_table.len() as u64,
            #[allow(clippy::allow_attributes)]
            #[allow(clippy::useless_conversion)]
            free_inodes: u64::from(stat.files_free()),
            #[allow(clippy::allow_attributes)]
            #[allow(clippy::useless_conversion)]
            available_inodes: u64::from(stat.files_available()),
            filesystem_id: 0,
            mount_flags: 0,
            #[expect(
                clippy::cast_possible_truncation,
                reason = "max filename length always fits in u32"
            )]
            max_filename_length: stat.name_max() as u32,
        })
    }
}
