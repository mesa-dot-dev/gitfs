//! Write-through caching filesystem layer.
//!
//! `WriteThroughFs<F, B>` wraps a backend filesystem `B` and mirrors data
//! to a local cache `F` via the `FsCacheProvider` trait. Reads check the
//! cache first; on miss the backend is called and the result is cached in
//! the background via `tokio::spawn`.
#![allow(dead_code, reason = "consumed by WriteThroughFs integration")]

use std::collections::HashMap;
use std::ffi::OsStr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use bytes::Bytes;
use tracing::{instrument, warn};

use crate::fs::cache_tracker::CacheTracker;
use crate::fs::r#trait::{
    DirEntry, FileAttr, FileHandle, FilesystemStats, Fs, FsCacheProvider, Inode, LockOwner,
    OpenFile, OpenFlags,
};

/// Write-through caching layer that mirrors backend data to a local cache.
///
/// Generic parameters:
/// - `F`: Local cache filesystem implementing both `FsCacheProvider` and `Fs`
/// - `B`: Backend (remote) filesystem
pub struct WriteThroughFs<F, B>
where
    F: FsCacheProvider + Fs + Send + Sync + 'static,
    B: Fs,
{
    front: Arc<F>,
    back: B,
    tracker: Arc<CacheTracker>,
    /// Maps backend inodes to their cache-relative paths.
    inode_paths: HashMap<Inode, PathBuf>,
    readdir_buf: Vec<DirEntry>,
    stale_after: Duration,
    last_eviction: Instant,
    eviction_interval: Duration,
}

impl<F, B> WriteThroughFs<F, B>
where
    F: FsCacheProvider + Fs + Send + Sync + 'static,
    B: Fs,
{
    /// Create a new write-through caching layer.
    ///
    /// - `front`: local cache implementing `FsCacheProvider + Fs`
    /// - `back`: backend filesystem (e.g., remote)
    /// - `stale_after`: maximum age before a cached entry is considered stale
    /// - `eviction_interval`: how often to run stale-entry eviction
    #[must_use]
    pub fn new(front: F, back: B, stale_after: Duration, eviction_interval: Duration) -> Self {
        Self {
            front: Arc::new(front),
            back,
            tracker: Arc::new(CacheTracker::new()),
            inode_paths: HashMap::new(),
            readdir_buf: Vec::new(),
            stale_after,
            last_eviction: Instant::now(),
            eviction_interval,
        }
    }

    /// Run stale-entry eviction if enough time has elapsed since the last run.
    fn maybe_evict(&mut self) {
        if self.last_eviction.elapsed() < self.eviction_interval {
            return;
        }
        self.last_eviction = Instant::now();

        let front = Arc::clone(&self.front);
        let tracker = Arc::clone(&self.tracker);
        let stale_after = self.stale_after;

        tokio::spawn(async move {
            let stale = tracker.stale_entries(stale_after).await;
            for path in stale {
                if let Err(e) = front.invalidate(&path).await {
                    warn!(?e, ?path, "failed to invalidate stale cache entry");
                }
                tracker.untrack(&path).await;
            }
        });
    }
}

#[async_trait]
impl<F, B> Fs for WriteThroughFs<F, B>
where
    F: FsCacheProvider + Fs + Send + Sync + 'static,
    B: Fs + Send,
    B::LookupError: Send,
    B::GetAttrError: Send,
    B::OpenError: Send,
    B::ReadError: Send,
    B::ReaddirError: Send,
    B::ReleaseError: Send,
{
    type LookupError = B::LookupError;
    type GetAttrError = B::GetAttrError;
    type OpenError = B::OpenError;
    type ReadError = B::ReadError;
    type ReaddirError = B::ReaddirError;
    type ReleaseError = B::ReleaseError;

    #[instrument(name = "WriteThroughFs::lookup", skip(self, name))]
    async fn lookup(&mut self, parent: Inode, name: &OsStr) -> Result<FileAttr, Self::LookupError> {
        self.maybe_evict();

        let attr = self.back.lookup(parent, name).await?;
        let ino = attr.common().ino;

        // Record path for cache operations
        let parent_path = self.inode_paths.get(&parent).cloned().unwrap_or_default();
        let child_path = parent_path.join(name);
        self.inode_paths.insert(ino, child_path);

        Ok(attr)
    }

    #[instrument(name = "WriteThroughFs::getattr", skip(self))]
    async fn getattr(
        &mut self,
        ino: Inode,
        fh: Option<FileHandle>,
    ) -> Result<FileAttr, Self::GetAttrError> {
        self.back.getattr(ino, fh).await
    }

    #[instrument(name = "WriteThroughFs::readdir", skip(self))]
    async fn readdir(&mut self, ino: Inode) -> Result<&[DirEntry], Self::ReaddirError> {
        // Stub -- caching implemented in Task 6
        let entries = self.back.readdir(ino).await?.to_vec();
        self.readdir_buf = entries;
        Ok(&self.readdir_buf)
    }

    #[instrument(name = "WriteThroughFs::open", skip(self))]
    async fn open(&mut self, ino: Inode, flags: OpenFlags) -> Result<OpenFile, Self::OpenError> {
        self.back.open(ino, flags).await
    }

    #[instrument(name = "WriteThroughFs::read", skip(self))]
    async fn read(
        &mut self,
        ino: Inode,
        fh: FileHandle,
        offset: u64,
        size: u32,
        flags: OpenFlags,
        lock_owner: Option<LockOwner>,
    ) -> Result<Bytes, Self::ReadError> {
        // Stub -- caching implemented in Task 5
        self.back
            .read(ino, fh, offset, size, flags, lock_owner)
            .await
    }

    #[instrument(name = "WriteThroughFs::release", skip(self))]
    async fn release(
        &mut self,
        ino: Inode,
        fh: FileHandle,
        flags: OpenFlags,
        flush: bool,
    ) -> Result<(), Self::ReleaseError> {
        self.back.release(ino, fh, flags, flush).await
    }

    #[instrument(name = "WriteThroughFs::forget", skip(self))]
    async fn forget(&mut self, ino: Inode, nlookups: u64) {
        // Evict cached data for this inode
        if let Some(path) = self.inode_paths.remove(&ino) {
            let front = Arc::clone(&self.front);
            let tracker = Arc::clone(&self.tracker);
            tokio::spawn(async move {
                if let Err(e) = front.invalidate(&path).await {
                    warn!(?e, ?path, "failed to invalidate on forget");
                }
                tracker.untrack(&path).await;
            });
        }
        self.back.forget(ino, nlookups).await;
    }

    async fn statfs(&mut self) -> Result<FilesystemStats, std::io::Error> {
        self.back.statfs().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fs::r#trait::{CommonFileAttr, Permissions};
    use std::path::Path;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicU64, Ordering};

    // -- Mock Backend Fs --

    #[derive(Debug, thiserror::Error)]
    #[error("mock error")]
    pub struct MockError;

    impl From<MockError> for i32 {
        fn from(_: MockError) -> Self {
            libc::EIO
        }
    }

    struct MockBackend {
        lookup_results: HashMap<(Inode, String), FileAttr>,
        getattr_results: HashMap<Inode, FileAttr>,
        readdir_results: HashMap<Inode, Vec<DirEntry>>,
        read_results: HashMap<Inode, Bytes>,
        lookup_count: AtomicU64,
        getattr_count: AtomicU64,
        _readdir_count: AtomicU64,
        _read_count: AtomicU64,
        readdir_buf: Vec<DirEntry>,
    }

    impl MockBackend {
        fn new() -> Self {
            Self {
                lookup_results: HashMap::new(),
                getattr_results: HashMap::new(),
                readdir_results: HashMap::new(),
                read_results: HashMap::new(),
                lookup_count: AtomicU64::new(0),
                getattr_count: AtomicU64::new(0),
                _readdir_count: AtomicU64::new(0),
                _read_count: AtomicU64::new(0),
                readdir_buf: Vec::new(),
            }
        }
    }

    #[async_trait]
    impl Fs for MockBackend {
        type LookupError = MockError;
        type GetAttrError = MockError;
        type OpenError = MockError;
        type ReadError = MockError;
        type ReaddirError = MockError;
        type ReleaseError = MockError;

        async fn lookup(&mut self, parent: Inode, name: &OsStr) -> Result<FileAttr, MockError> {
            self.lookup_count.fetch_add(1, Ordering::Relaxed);
            let key = (parent, name.to_string_lossy().into_owned());
            self.lookup_results.get(&key).copied().ok_or(MockError)
        }

        async fn getattr(
            &mut self,
            ino: Inode,
            _fh: Option<FileHandle>,
        ) -> Result<FileAttr, MockError> {
            self.getattr_count.fetch_add(1, Ordering::Relaxed);
            self.getattr_results.get(&ino).copied().ok_or(MockError)
        }

        async fn readdir(&mut self, ino: Inode) -> Result<&[DirEntry], MockError> {
            self._readdir_count.fetch_add(1, Ordering::Relaxed);
            let entries = self.readdir_results.get(&ino).ok_or(MockError)?;
            self.readdir_buf = entries.clone();
            Ok(&self.readdir_buf)
        }

        async fn open(&mut self, _ino: Inode, _flags: OpenFlags) -> Result<OpenFile, MockError> {
            Err(MockError)
        }

        async fn read(
            &mut self,
            ino: Inode,
            _fh: FileHandle,
            offset: u64,
            size: u32,
            _flags: OpenFlags,
            _lock_owner: Option<LockOwner>,
        ) -> Result<Bytes, MockError> {
            self._read_count.fetch_add(1, Ordering::Relaxed);
            let data = self.read_results.get(&ino).ok_or(MockError)?;
            let start = usize::try_from(offset)
                .unwrap_or(usize::MAX)
                .min(data.len());
            let end = start
                .saturating_add(usize::try_from(size).unwrap_or(usize::MAX))
                .min(data.len());
            Ok(data.slice(start..end))
        }

        async fn release(
            &mut self,
            _ino: Inode,
            _fh: FileHandle,
            _flags: OpenFlags,
            _flush: bool,
        ) -> Result<(), MockError> {
            Ok(())
        }

        async fn forget(&mut self, _ino: Inode, _nlookups: u64) {}

        async fn statfs(&mut self) -> Result<FilesystemStats, std::io::Error> {
            Ok(FilesystemStats {
                block_size: 4096,
                fragment_size: 4096,
                total_blocks: 0,
                free_blocks: 0,
                available_blocks: 0,
                total_inodes: 0,
                free_inodes: 0,
                available_inodes: 0,
                filesystem_id: 0,
                mount_flags: 0,
                max_filename_length: 255,
            })
        }
    }

    // -- Mock FsCacheProvider --

    struct MockCache {
        files: Mutex<HashMap<PathBuf, Vec<u8>>>,
    }

    impl MockCache {
        fn new() -> Self {
            Self {
                files: Mutex::new(HashMap::new()),
            }
        }
    }

    #[derive(Debug, thiserror::Error)]
    #[error("mock cache error")]
    pub struct MockCacheError;

    impl From<MockCacheError> for i32 {
        fn from(_: MockCacheError) -> Self {
            libc::EIO
        }
    }

    // Minimal Fs impl on MockCache (required by the bound, but not used by WriteThroughFs)
    #[async_trait]
    impl Fs for MockCache {
        type LookupError = MockCacheError;
        type GetAttrError = MockCacheError;
        type OpenError = MockCacheError;
        type ReadError = MockCacheError;
        type ReaddirError = MockCacheError;
        type ReleaseError = MockCacheError;

        async fn lookup(&mut self, _: Inode, _: &OsStr) -> Result<FileAttr, MockCacheError> {
            Err(MockCacheError)
        }
        async fn getattr(
            &mut self,
            _: Inode,
            _: Option<FileHandle>,
        ) -> Result<FileAttr, MockCacheError> {
            Err(MockCacheError)
        }
        async fn readdir(&mut self, _: Inode) -> Result<&[DirEntry], MockCacheError> {
            Err(MockCacheError)
        }
        async fn open(&mut self, _: Inode, _: OpenFlags) -> Result<OpenFile, MockCacheError> {
            Err(MockCacheError)
        }
        async fn read(
            &mut self,
            _: Inode,
            _: FileHandle,
            _: u64,
            _: u32,
            _: OpenFlags,
            _: Option<LockOwner>,
        ) -> Result<Bytes, MockCacheError> {
            Err(MockCacheError)
        }
        async fn release(
            &mut self,
            _: Inode,
            _: FileHandle,
            _: OpenFlags,
            _: bool,
        ) -> Result<(), MockCacheError> {
            Ok(())
        }
        async fn forget(&mut self, _: Inode, _: u64) {}
        async fn statfs(&mut self) -> Result<FilesystemStats, std::io::Error> {
            Err(std::io::Error::new(std::io::ErrorKind::Unsupported, "mock"))
        }
    }

    #[async_trait]
    impl FsCacheProvider for MockCache {
        type CacheError = std::io::Error;

        async fn populate_file(&self, path: &Path, content: &[u8]) -> Result<(), std::io::Error> {
            self.files
                .lock()
                .expect("mock lock")
                .insert(path.to_path_buf(), content.to_vec());
            Ok(())
        }

        async fn read_cached_file(
            &self,
            path: &Path,
            offset: u64,
            size: u32,
        ) -> Result<Option<Bytes>, std::io::Error> {
            let files = self.files.lock().expect("mock lock");
            match files.get(path) {
                Some(data) => {
                    let start = usize::try_from(offset)
                        .unwrap_or(usize::MAX)
                        .min(data.len());
                    let end = start
                        .saturating_add(usize::try_from(size).unwrap_or(usize::MAX))
                        .min(data.len());
                    Ok(Some(Bytes::copy_from_slice(&data[start..end])))
                }
                None => Ok(None),
            }
        }

        async fn is_cached(&self, path: &Path) -> bool {
            self.files.lock().expect("mock lock").contains_key(path)
        }

        async fn invalidate(&self, path: &Path) -> Result<(), std::io::Error> {
            self.files.lock().expect("mock lock").remove(path);
            Ok(())
        }
    }

    // -- Helpers --

    fn dummy_file_attr(ino: Inode) -> FileAttr {
        FileAttr::RegularFile {
            common: CommonFileAttr {
                ino,
                atime: std::time::SystemTime::UNIX_EPOCH,
                mtime: std::time::SystemTime::UNIX_EPOCH,
                ctime: std::time::SystemTime::UNIX_EPOCH,
                crtime: std::time::SystemTime::UNIX_EPOCH,
                perm: Permissions::from_bits_truncate(0o644),
                nlink: 1,
                uid: 0,
                gid: 0,
                blksize: 4096,
            },
            size: 100,
            blocks: 1,
        }
    }

    fn dummy_dir_attr(ino: Inode) -> FileAttr {
        FileAttr::Directory {
            common: CommonFileAttr {
                ino,
                atime: std::time::SystemTime::UNIX_EPOCH,
                mtime: std::time::SystemTime::UNIX_EPOCH,
                ctime: std::time::SystemTime::UNIX_EPOCH,
                crtime: std::time::SystemTime::UNIX_EPOCH,
                perm: Permissions::from_bits_truncate(0o755),
                nlink: 2,
                uid: 0,
                gid: 0,
                blksize: 4096,
            },
        }
    }

    fn make_wt(mock: MockBackend) -> WriteThroughFs<MockCache, MockBackend> {
        WriteThroughFs::new(
            MockCache::new(),
            mock,
            Duration::from_secs(60),
            Duration::from_secs(30),
        )
    }

    // -- Tests --

    #[tokio::test]
    async fn lookup_delegates_to_backend_on_miss() {
        let mut mock = MockBackend::new();
        mock.lookup_results
            .insert((1, "file.txt".into()), dummy_file_attr(10));
        let mut wt = make_wt(mock);

        let attr = wt.lookup(1, OsStr::new("file.txt")).await.unwrap();
        assert_eq!(attr, dummy_file_attr(10));
        assert_eq!(wt.back.lookup_count.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn lookup_records_inode_path() {
        let mut mock = MockBackend::new();
        mock.lookup_results
            .insert((1, "dir".into()), dummy_dir_attr(2));
        mock.lookup_results
            .insert((2, "file.txt".into()), dummy_file_attr(10));
        let mut wt = make_wt(mock);

        // Root inode 1 maps to empty path
        wt.inode_paths.insert(1, PathBuf::new());
        wt.lookup(1, OsStr::new("dir")).await.unwrap();
        wt.lookup(2, OsStr::new("file.txt")).await.unwrap();

        assert_eq!(wt.inode_paths.get(&2), Some(&PathBuf::from("dir")));
        assert_eq!(
            wt.inode_paths.get(&10),
            Some(&PathBuf::from("dir/file.txt"))
        );
    }

    #[tokio::test]
    async fn getattr_delegates_to_backend() {
        let mut mock = MockBackend::new();
        mock.getattr_results.insert(10, dummy_file_attr(10));
        let mut wt = make_wt(mock);

        let attr = wt.getattr(10, None).await.unwrap();
        assert_eq!(attr, dummy_file_attr(10));
        assert_eq!(wt.back.getattr_count.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn forget_evicts_inode_path() {
        let mut mock = MockBackend::new();
        mock.lookup_results
            .insert((1, "file.txt".into()), dummy_file_attr(10));
        let mut wt = make_wt(mock);
        wt.inode_paths.insert(1, PathBuf::new());

        wt.lookup(1, OsStr::new("file.txt")).await.unwrap();
        assert!(
            wt.inode_paths.contains_key(&10),
            "inode 10 should be tracked after lookup"
        );

        wt.forget(10, 1).await;
        assert!(
            !wt.inode_paths.contains_key(&10),
            "inode 10 should be evicted after forget"
        );
    }
}
