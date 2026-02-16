//! File-based MT-safe, async-safe data cache.

use std::{
    fmt::Debug,
    hash::Hash,
    path::{Path, PathBuf},
    sync::{Arc, atomic::AtomicUsize},
};

use tracing::error;

use crate::{
    cache::{
        eviction::lru::{Deleter, LruEvictionTracker},
        traits::{AsyncReadableCache, AsyncWritableCache},
    },
    io,
};
use thiserror::Error;

use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

/// Drop guard that deletes an unregistered cache file if the insert task is cancelled or fails
/// between file creation and map registration. Defused once the file is registered in the map.
struct FileGuard {
    path: Option<PathBuf>,
}

impl Drop for FileGuard {
    fn drop(&mut self) {
        if let Some(path) = self.path.take()
            && let Err(e) = std::fs::remove_file(&path)
            && e.kind() != std::io::ErrorKind::NotFound
        {
            error!(error = ?e, path = ?path, "failed to clean up orphaned cache file");
        }
    }
}

struct FileCacheShared<K: Eq + Hash> {
    root_path: PathBuf,
    map: scc::HashMap<K, CacheMapEntry>,
    size_bytes: AtomicUsize,
}

impl<K: Eq + Hash + Send + Sync> FileCacheShared<K> {
    fn size(&self) -> usize {
        self.size_bytes.load(std::sync::atomic::Ordering::Relaxed)
    }

    fn path_for(&self, fid: usize) -> PathBuf {
        self.root_path.join(fid.to_string())
    }

    /// Given an entry, remove it from the disk. Note this does not update the cache size tracker.
    ///
    /// Gracefully handles the case where the file doesn't exist, which can happen if the file was
    /// deleted after we read the file ID from the map, but before we tried to delete
    /// the file.
    async fn delete_entry_from_disk_async(&self, entry: &CacheMapEntry) -> std::io::Result<()> {
        match tokio::fs::remove_file(self.path_for(entry.fid)).await {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e),
        }
    }
}

#[derive(Debug, Clone)]
struct DeleterCtx {
    fid: usize,
}

#[derive(Clone)]
struct LruEntryDeleter<K: Eq + Hash> {
    shared: Arc<FileCacheShared<K>>,
}

impl<K: Eq + Hash> LruEntryDeleter<K> {
    fn new(shared: Arc<FileCacheShared<K>>) -> Self {
        Self { shared }
    }
}

impl<K: Copy + Eq + Hash + Sync + Send + 'static> Deleter<K, DeleterCtx> for LruEntryDeleter<K> {
    /// The LRU cache will call this method when it wants to evict keys.
    async fn delete(&mut self, key: K, ctx: DeleterCtx) {
        if let Some(entry) = self
            .shared
            .map
            .remove_if_async(&key, |m| m.fid == ctx.fid)
            .await
        {
            if let Err(e) = self.shared.delete_entry_from_disk_async(&entry.1).await {
                error!(error = ?e, "failed to delete evicted cache entry from disk");
            }

            self.shared
                .size_bytes
                .fetch_sub(entry.1.size_bytes, std::sync::atomic::Ordering::Relaxed);
        }
    }
}

/// Error thrown during construction of a `FileCache` which describes why the given root path is
/// invalid.
#[derive(Debug, Error)]
pub enum InvalidRootPathError {
    /// The root path exists but isn't a directory.
    #[error("Root path is not a directory: {0}")]
    NotADirectory(PathBuf),

    /// The root path exists and is a directory, but it isn't empty and the data in it appears to
    /// come from a different source than this application.
    #[error("Root path appears to contain data stemming from sources different to this app: {0}")]
    RootPathUnsafeCache(PathBuf),

    /// An IO error occurred while trying to access the root path.
    #[error("IO error while accessing root path: {0}")]
    Io(#[from] std::io::Error),
}

/// Error thrown during insertion into the cache.
#[derive(Debug, Error)]
pub enum CacheWriteError {
    /// An IO error occurred while trying to write the new value to disk.
    #[error("IO error while inserting into cache: {0}")]
    Io(#[from] std::io::Error),
}

#[derive(Debug, Clone, Copy)]
struct CacheMapEntry {
    /// A unique ID representing the name of the file on disk where the value for this cache entry
    /// is stored. This is just an integer that is used to generate the file path for the cache
    /// entry.
    fid: usize,

    /// The size of the value for this cache entry in bytes. This is used to keep track of the
    /// total size of the cache, and to evict entries when necessary.
    size_bytes: usize,
}

/// A general-purpose file-backed cache. This cache is designed to store arbitrary byte values on
/// disk, and to retrieve them later using a key.
///
/// This cache is considered thread-safe and async-safe.
pub struct FileCache<K: Eq + Hash> {
    shared: Arc<FileCacheShared<K>>,

    /// Generates unique file paths for new cache entries. This is just a counter that increments
    /// for each new file, and the file ID is the value of the counter at the time of creation.
    file_generator: AtomicUsize,

    /// The maximum size of the cache in bytes. This is used to determine when to evict entries
    /// from the cache. This is a soft hint, rather than a hard limit, and the cache may
    /// temporarily exceed this size during insertions, but it will try to evict entries as soon as
    /// possible to get back under this limit.
    max_size_bytes: usize,

    /// LRU eviction tracker. This is used to track the least recently used keys in the cache, and
    /// to evict keys when necessary.
    lru_tracker: LruEvictionTracker<K, DeleterCtx>,
}

impl<K: Eq + Hash + Send + Sync + Copy + 'static> FileCache<K> {
    // How many cache entries to evict at most in a single batch.
    //
    // Not really sure how to determine this number.
    const LRU_EVICTION_MAX_BATCH_SIZE: u32 = 8;

    // The maximum number of messages that can be buffered between this file cache and the LRU
    // eviction tracker. If this is too small, then the file cache will be blocked waiting for the
    // eviction tracker to catch up.
    const MAX_LRU_TRACKER_CHANNEL_SIZE: usize = 256;

    // Dangerous: Changing this constant may cause the program to treat existing cache directories
    // as invalid, and thus provide a worse user experience. Do not change this unless you have a
    // very good reason to do so. Changing this will break backwards compatibility with existing
    // cache directories.
    const GITFS_MARKER_FILE: &'static str = ".gitfs_cache";

    // The total maximum number of times we will try to read a "deleted" file before giving up and
    // treating it as a hard error.
    //
    // See usage for reasoning on why this is necessary.
    const MAX_READ_RETRY_COUNT: usize = 8;

    /// Try to create a new file cache at the given path.
    ///
    ///
    /// # Args
    ///
    /// * `file_path` - If the path exists, it must either be an empty directory, or a directory
    ///   which was previously used as a cache for this program.
    /// * `max_size_bytes` - The maximum size of the cache in bytes. This is used to determine when
    ///   to evict entries from the cache. This is a soft hint, rather than a hard limit, and the
    ///   cache may temporarily exceed this size during insertions, but it will try to evict entries
    ///   as soon as possible to get back under this limit.
    pub async fn new(
        file_path: &Path,
        max_size_bytes: usize,
    ) -> Result<Self, InvalidRootPathError> {
        let mut pbuf = match tokio::fs::canonicalize(file_path).await {
            Ok(mut p) => {
                if !tokio::fs::metadata(&p).await?.is_dir() {
                    return Err(InvalidRootPathError::NotADirectory(p));
                }

                let mut entries = tokio::fs::read_dir(&p).await?;
                let is_empty = entries.next_entry().await?.is_none();

                p.push(Self::GITFS_MARKER_FILE);
                let marker_exists = tokio::fs::try_exists(&p).await?;
                p.pop();

                if !(is_empty || marker_exists) {
                    return Err(InvalidRootPathError::RootPathUnsafeCache(p));
                }

                io::remove_dir_contents(&p).await?;

                Ok(p)
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                tokio::fs::create_dir_all(file_path).await?;
                tokio::fs::canonicalize(file_path).await
            }
            Err(e) => return Err(e.into()),
        }?;

        // Create marker file so that subsequent restarts of this application gracefully handle the
        // existing cache directory.
        pbuf.push(Self::GITFS_MARKER_FILE);
        tokio::fs::OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&pbuf)
            .await?;
        pbuf.pop();

        let shared = Arc::new(FileCacheShared {
            root_path: pbuf,
            map: scc::HashMap::new(),
            size_bytes: AtomicUsize::new(0),
        });

        Ok(Self {
            shared: Arc::clone(&shared),
            file_generator: AtomicUsize::new(0),
            max_size_bytes,
            lru_tracker: LruEvictionTracker::spawn(
                LruEntryDeleter::new(shared),
                Self::MAX_LRU_TRACKER_CHANNEL_SIZE,
            ),
        })
    }

    async fn create_file(&self, path: &Path) -> Result<tokio::fs::File, std::io::Error> {
        tokio::fs::OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(path)
            .await
    }
}

impl<K: Eq + Hash> Drop for FileCache<K> {
    fn drop(&mut self) {
        // Surprisingly, it's safe to delete files here.
        //
        // Note that the LRU tracker is running at the time of Drop. It, consequently, may end up
        // calling the deleter, but that's fine, since the deleter ignores this problem gracefully.
        //
        // It is 100% safe to do this here, since it's guaranteed no concurrent task is borrowing
        // the FileCache at this point.
        if let Err(e) = std::fs::remove_dir_all(&self.shared.root_path) {
            error!(error = ?e, "failed to delete cache directory on drop");
        }
    }
}

impl<K: Eq + Hash + Copy + Send + Sync + Debug + 'static> AsyncReadableCache<K, Vec<u8>>
    for FileCache<K>
{
    async fn get(&self, key: &K) -> Option<Vec<u8>> {
        for _ in 0..Self::MAX_READ_RETRY_COUNT {
            // Grab the best-guess file ID for the given key. If this fails, then the key is not in
            // the cache, and we can return `None`.
            let entry = *(self.shared.map.get_async(key).await?);

            // The next thing we need to do is try to open the file. This may fail for a plethora
            // of reasons.
            //
            // TODO(markovejnovic): path_for allocates on heap, could be on stack.
            let mut file = match tokio::fs::File::open(self.shared.path_for(entry.fid)).await {
                Ok(file) => file,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                    // The file was not found.
                    // This may happen for two reasons:
                    //
                    // There is a bug in the code.
                    //
                    // The file was id was deleted after we read the file ID from the map on line:
                    //
                    //   let fid: usize = *(self.map.get_async(key).await?);
                    //
                    // The file can get deleted as part of the insert procedure, or the deletion
                    // procedure, so we need to try to re-read the file.
                    continue;
                }
                Err(e) => {
                    error!(error = ?e, key = ?key, "IO error while opening file for cache key");

                    // This MIGHT be recoverable. The IO error may very well be transient, so we'll
                    // just say we don't have this value and then pray that the user will perform
                    // an .insert
                    return None;
                }
            };

            let mut buf = Vec::new();
            file.read_to_end(&mut buf)
                .await
                .inspect_err(|e| {
                    error!(error = ?e, key = ?key, "IO error while reading file for cache key");
                })
                .ok()?;
            self.lru_tracker.access(*key).await;
            return Some(buf);
        }

        error!(key = ?key, attempt_count = Self::MAX_READ_RETRY_COUNT, "could not find file.");

        // Again, might be recoverable if the user calls an .insert later on.
        None
    }

    async fn contains(&self, key: &K) -> bool {
        self.shared.map.contains_async(key).await
    }
}

impl<K: Eq + Hash + Copy + Send + Sync + 'static + Debug>
    AsyncWritableCache<K, Vec<u8>, CacheWriteError> for FileCache<K>
{
    async fn insert(&self, key: &K, value: Vec<u8>) -> Result<(), CacheWriteError> {
        // Inserts are tricky. The first thing we'll do is find a new file handle for the new
        // value.
        let new_entry = CacheMapEntry {
            fid: self
                .file_generator
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed),
            size_bytes: value.len(),
        };

        while self.shared.size() + new_entry.size_bytes > self.max_size_bytes {
            if self.shared.size() == 0 {
                // This means that the new entry is larger than the entire cache size limit. In
                // this case, we have no choice but to just insert the entry and let the cache
                // temporarily exceed the size limit. We will try to evict entries as soon as
                // possible, but in the meantime, we need to at least insert this entry.
                break;
            }

            // TODO(markovejnovic): This whole spinlock situation sounded better in my head, but
            // realistically, I think I could've gotten away with just a plain Notify. It is what
            // it is now.

            // The cache doesn't have any space for this new entry, so we need to evict some
            // entries before we can insert this new entry.
            // Before we can do that, we need to make sure there are no other pending culls. If we
            // let multiple culls happen at the same time, we risk draining a lot more entries from
            // the cache than necessary, which results in a regression.
            if self.lru_tracker.have_pending_culls() {
                // If there are any pending culls, then we need to just wait.
                // TODO(markovejnovic): This could be nicer and maybe starve the CPU less. Chances
                // are we actually need to yield to the LRU tracker task.
                tokio::task::yield_now().await;
                continue;
            }

            // There are no culls in progress, but the cache is still too full for this new entry,
            // which means we need to evict some entries.
            if !self.lru_tracker.try_cull(Self::LRU_EVICTION_MAX_BATCH_SIZE) {
                tokio::task::yield_now().await;
            }
        }

        let path = self.shared.path_for(new_entry.fid);
        let mut new_file = self.create_file(&path).await?;
        let mut guard = FileGuard { path: Some(path) };
        new_file.write_all(&value).await?;

        let mut size_delta: isize = new_entry.size_bytes.cast_signed();

        // Register the file in the map. After this point, the map owns the file and the guard
        // must not delete it.
        let old_entry = self.shared.map.upsert_async(*key, new_entry).await;
        guard.path = None;

        if let Some(old_entry) = old_entry {
            // If there was an old file ID, we need to delete the old file and notify the LRU
            // tracker that the key was accessed.
            self.lru_tracker.access(*key).await;
            size_delta -= old_entry.size_bytes.cast_signed();

            // TODO(markovejnovic): Could stack allocate the path.
            // Note that the file already may be deleted at this point -- the LRU tracker deleter
            // may have already deleted this file, so if the file doesn't exist, that's already
            // fine with us.
            if let Err(e) = self.shared.delete_entry_from_disk_async(&old_entry).await {
                error!(error = ?e, key = ?key, "failed to delete old cache entry from disk");
            }
        } else {
            self.lru_tracker
                .insert(*key, DeleterCtx { fid: new_entry.fid })
                .await;
        }

        if size_delta > 0 {
            self.shared.size_bytes.fetch_add(
                size_delta.cast_unsigned(),
                std::sync::atomic::Ordering::Relaxed,
            );
        } else if size_delta < 0 {
            self.shared.size_bytes.fetch_sub(
                size_delta.unsigned_abs(),
                std::sync::atomic::Ordering::Relaxed,
            );
        }

        // Epic, we did it.
        Ok(())
    }
}
