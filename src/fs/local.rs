//! An implementation of a filesystem that directly overlays the host filesystem.
#![allow(dead_code, reason = "LocalFs consumed by WriteThroughFs in Tasks 4-6")]

use std::collections::HashMap;
use std::ffi::OsStr;
use std::future::Future;
use std::path::{Path, PathBuf};

use async_trait::async_trait;
use bytes::Bytes;
use nix::sys::statvfs::statvfs;
use thiserror::Error;
use tokio::io::{AsyncReadExt as _, AsyncSeekExt as _};
use tracing::warn;

use crate::fs::icache::{AsyncICache, FileTable, IcbLike, IcbResolver};
use crate::fs::r#trait::{
    DirEntry, FileAttr, FileHandle, FileOpenOptions, FilesystemStats, Fs, FsCacheProvider, Inode,
    LockOwner, OpenFile, OpenFlags,
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

#[derive(Clone)]
struct InodeControlBlock {
    rc: u64,
    path: PathBuf,
    children: Option<Vec<DirEntry>>,
}

impl IcbLike for InodeControlBlock {
    fn new_root(path: PathBuf) -> Self {
        Self {
            rc: 1,
            path,
            children: None,
        }
    }

    fn rc(&self) -> u64 {
        self.rc
    }

    fn rc_mut(&mut self) -> &mut u64 {
        &mut self.rc
    }

    fn needs_resolve(&self) -> bool {
        false
    }
}

struct LocalResolver;

impl IcbResolver for LocalResolver {
    type Icb = InodeControlBlock;
    type Error = std::convert::Infallible;

    #[expect(
        clippy::manual_async_fn,
        reason = "must match IcbResolver trait signature"
    )]
    fn resolve(
        &self,
        _ino: Inode,
        _stub: Option<InodeControlBlock>,
        _cache: &AsyncICache<Self>,
    ) -> impl Future<Output = Result<InodeControlBlock, Self::Error>> + Send {
        async { unreachable!("LocalResolver should never be called") }
    }
}

pub struct LocalFs {
    root_path: PathBuf,
    icache: AsyncICache<LocalResolver>,
    file_table: FileTable,
    open_files: HashMap<FileHandle, tokio::fs::File>,
    readdir_buf: Vec<DirEntry>,
}

impl LocalFs {
    #[must_use]
    pub fn new(abs_path: impl Into<PathBuf>) -> Self {
        let root_path: PathBuf = abs_path.into();
        Self {
            icache: AsyncICache::new(LocalResolver, 1, &root_path),
            root_path,
            file_table: FileTable::new(),
            open_files: HashMap::new(),
            readdir_buf: Vec::new(),
        }
    }

    pub fn root_path(&self) -> &Path {
        &self.root_path
    }

    async fn parse_tokio_dirent(
        dir_entry: &tokio::fs::DirEntry,
    ) -> Result<DirEntry, tokio::io::Error> {
        Ok(DirEntry {
            ino: dir_entry.ino(),
            name: dir_entry.file_name(),
            kind: dir_entry.file_type().await?.try_into().map_err(|()| {
                tokio::io::Error::new(
                    tokio::io::ErrorKind::InvalidData,
                    "invalid file type in directory entry",
                )
            })?,
        })
    }
}

#[async_trait]
impl Fs for LocalFs {
    type LookupError = LookupError;
    type GetAttrError = GetAttrError;
    type OpenError = OpenError;
    type ReadError = ReadError;
    type ReaddirError = ReadDirError;
    type ReleaseError = ReleaseError;

    async fn lookup(&mut self, parent: Inode, name: &OsStr) -> Result<FileAttr, LookupError> {
        debug_assert!(
            self.icache.contains(parent),
            "parent inode {parent} not in inode_table"
        );
        let parent_path = self.icache.get_icb(parent, |icb| icb.path.clone()).await;
        let parent_path = parent_path.ok_or_else(|| {
            warn!(
                "Lookup called on unknown parent inode {}. This is a programming bug",
                parent
            );
            LookupError::InodeNotFound
        })?;

        let child_path = parent_path.join(name);
        let meta = tokio::fs::metadata(&child_path)
            .await
            .map_err(LookupError::Io)?;

        let file_attr = FileAttr::try_from(meta).map_err(|()| LookupError::InvalidFileType);
        debug_assert!(file_attr.is_ok(), "FileAttr conversion failed unexpectedly");
        let file_attr = file_attr?;

        self.icache
            .entry_or_insert_icb(
                file_attr.common().ino,
                || InodeControlBlock {
                    rc: 0,
                    path: child_path,
                    children: None,
                },
                |icb| {
                    *icb.rc_mut() += 1;
                },
            )
            .await;

        Ok(file_attr)
    }

    async fn getattr(
        &mut self,
        ino: Inode,
        fh: Option<FileHandle>,
    ) -> Result<FileAttr, GetAttrError> {
        if let Some(fh) = fh {
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
            debug_assert!(self.icache.contains(ino), "inode {ino} not in inode_table");
            let path = self.icache.get_icb(ino, |icb| icb.path.clone()).await;
            let path = path.ok_or_else(|| {
                warn!(
                    "GetAttr called on unknown inode {}. This is a programming bug",
                    ino
                );
                GetAttrError::InodeNotFound
            })?;

            let meta = tokio::fs::metadata(&path).await.map_err(GetAttrError::Io)?;
            let file_attr = FileAttr::try_from(meta).map_err(|()| GetAttrError::InvalidFileType);
            debug_assert!(file_attr.is_ok(), "FileAttr conversion failed unexpectedly");

            Ok(file_attr?)
        }
    }

    async fn readdir(&mut self, ino: Inode) -> Result<&[DirEntry], ReadDirError> {
        debug_assert!(self.icache.contains(ino), "inode {ino} not in inode_table");

        let path = self.icache.get_icb(ino, |icb| icb.path.clone()).await;
        let path = path.ok_or_else(|| {
            warn!(
                parent = ino,
                "Readdir of unknown parent inode. Programming bug"
            );
            ReadDirError::InodeNotFound
        })?;

        let mut read_dir = tokio::fs::read_dir(&path).await.map_err(ReadDirError::Io)?;

        let mut entries: Vec<DirEntry> = Vec::new();
        while let Some(dir_entry) = read_dir.next_entry().await.map_err(ReadDirError::Io)? {
            entries.push(Self::parse_tokio_dirent(&dir_entry).await?);
        }

        self.readdir_buf = entries;
        Ok(&self.readdir_buf)
    }

    async fn open(&mut self, ino: Inode, flags: OpenFlags) -> Result<OpenFile, OpenError> {
        debug_assert!(self.icache.contains(ino), "inode {ino} not in inode_table");
        let path = self.icache.get_icb(ino, |icb| icb.path.clone()).await;
        let path = path.ok_or_else(|| {
            warn!(
                "Open called on unknown inode {}. This is a programming bug",
                ino
            );
            OpenError::InodeNotFound
        })?;

        let file = tokio::fs::OpenOptions::new()
            .read(true)
            .write(flags.contains(OpenFlags::RDWR) || flags.contains(OpenFlags::WRONLY))
            .append(flags.contains(OpenFlags::APPEND))
            .truncate(flags.contains(OpenFlags::TRUNC))
            .create(flags.contains(OpenFlags::CREAT))
            .open(&path)
            .await
            .map_err(OpenError::Io)?;

        let fh = self.file_table.allocate();
        self.open_files.insert(fh, file);

        Ok(OpenFile {
            handle: fh,
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
        debug_assert!(self.icache.contains(ino), "inode {ino} not in inode_table");
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
        debug_assert!(self.icache.contains(ino), "inode {ino} not in inode_table");

        self.icache.forget(ino, nlookups).await;
    }

    async fn statfs(&mut self) -> Result<FilesystemStats, std::io::Error> {
        let stat = statvfs(self.root_path.as_path())?;

        Ok(FilesystemStats {
            block_size: stat.block_size().try_into().map_err(|_| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "block size too large to fit into u32",
                )
            })?,
            fragment_size: stat.fragment_size(),
            #[expect(clippy::allow_attributes, reason = "platform-dependent lint")]
            #[allow(clippy::useless_conversion)]
            total_blocks: u64::from(stat.blocks()),
            #[expect(clippy::allow_attributes, reason = "platform-dependent lint")]
            #[allow(clippy::useless_conversion)]
            free_blocks: u64::from(stat.blocks_free()),
            #[expect(clippy::allow_attributes, reason = "platform-dependent lint")]
            #[allow(clippy::useless_conversion)]
            available_blocks: u64::from(stat.blocks_available()),
            #[expect(clippy::allow_attributes, reason = "platform-dependent lint")]
            #[allow(clippy::cast_possible_truncation)]
            total_inodes: self.icache.inode_count() as u64,
            #[expect(clippy::allow_attributes, reason = "platform-dependent lint")]
            #[allow(clippy::useless_conversion)]
            free_inodes: u64::from(stat.files_free()),
            #[expect(clippy::allow_attributes, reason = "platform-dependent lint")]
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

#[async_trait]
impl FsCacheProvider for LocalFs {
    type CacheError = std::io::Error;

    async fn populate_file(&self, path: &Path, content: &[u8]) -> Result<(), Self::CacheError> {
        let full_path = self.root_path.join(path);
        if let Some(parent) = full_path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        tokio::fs::write(&full_path, content).await
    }

    async fn read_cached_file(
        &self,
        path: &Path,
        offset: u64,
        size: u32,
    ) -> Result<Option<Bytes>, Self::CacheError> {
        let full_path = self.root_path.join(path);
        let mut file = match tokio::fs::File::open(&full_path).await {
            Ok(f) => f,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(e),
        };
        file.seek(std::io::SeekFrom::Start(offset)).await?;
        let mut buf = vec![0u8; size as usize];
        let n = file.read(&mut buf).await?;
        buf.truncate(n);
        Ok(Some(Bytes::from(buf)))
    }

    async fn is_cached(&self, path: &Path) -> bool {
        let full_path = self.root_path.join(path);
        tokio::fs::try_exists(&full_path).await.unwrap_or(false)
    }

    async fn invalidate(&self, path: &Path) -> Result<(), Self::CacheError> {
        let full_path = self.root_path.join(path);
        match tokio::fs::metadata(&full_path).await {
            Ok(meta) => {
                if meta.is_dir() {
                    tokio::fs::remove_dir_all(&full_path).await
                } else {
                    tokio::fs::remove_file(&full_path).await
                }
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_dir() -> PathBuf {
        let dir = std::env::temp_dir().join(format!("localfs-test-{}", rand::random::<u64>()));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[tokio::test]
    async fn populate_file_creates_file_with_content() {
        let root = temp_dir();
        let fs = LocalFs::new(&root);
        let path = Path::new("subdir/hello.txt");

        fs.populate_file(path, b"hello world").await.unwrap();

        let full = root.join(path);
        let content = tokio::fs::read_to_string(&full).await.unwrap();
        assert_eq!(content, "hello world");
    }

    #[tokio::test]
    async fn read_cached_file_returns_content() {
        let root = temp_dir();
        let fs = LocalFs::new(&root);
        let path = Path::new("data.bin");

        let data = b"abcdefghij";
        fs.populate_file(path, data).await.unwrap();

        let result = fs.read_cached_file(path, 2, 5).await.unwrap();
        assert_eq!(result, Some(Bytes::from_static(b"cdefg")));
    }

    #[tokio::test]
    async fn read_cached_file_returns_none_for_missing() {
        let root = temp_dir();
        let fs = LocalFs::new(&root);

        let result = fs
            .read_cached_file(Path::new("nope.txt"), 0, 100)
            .await
            .unwrap();
        assert_eq!(result, None);
    }

    #[tokio::test]
    async fn is_cached_returns_true_for_existing_file() {
        let root = temp_dir();
        let fs = LocalFs::new(&root);
        let path = Path::new("exists.txt");

        fs.populate_file(path, b"yes").await.unwrap();

        assert!(fs.is_cached(path).await);
        assert!(!fs.is_cached(Path::new("nope.txt")).await);
    }

    #[tokio::test]
    async fn invalidate_removes_file() {
        let root = temp_dir();
        let fs = LocalFs::new(&root);
        let path = Path::new("remove_me.txt");

        fs.populate_file(path, b"gone").await.unwrap();
        assert!(fs.is_cached(path).await);

        fs.invalidate(path).await.unwrap();
        assert!(!fs.is_cached(path).await);
    }

    #[tokio::test]
    async fn invalidate_removes_directory_tree() {
        let root = temp_dir();
        let fs = LocalFs::new(&root);

        fs.populate_file(Path::new("dir/a.txt"), b"a")
            .await
            .unwrap();
        fs.populate_file(Path::new("dir/sub/b.txt"), b"b")
            .await
            .unwrap();

        assert!(fs.is_cached(Path::new("dir")).await);

        fs.invalidate(Path::new("dir")).await.unwrap();
        assert!(!fs.is_cached(Path::new("dir")).await);
    }

    #[tokio::test]
    async fn invalidate_missing_path_is_ok() {
        let root = temp_dir();
        let fs = LocalFs::new(&root);

        let result = fs.invalidate(Path::new("nonexistent")).await;
        assert!(result.is_ok());
    }
}
