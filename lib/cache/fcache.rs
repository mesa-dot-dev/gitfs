//! File-based MT-safe, async-safe data cache.

use std::{
    fmt::Debug,
    hash::Hash,
    path::{Path, PathBuf},
};

use std::sync::{
    Arc,
    atomic::{self, AtomicU64, AtomicUsize},
};

use tracing::error;

use crate::{
    cache::{
        eviction::lru::{Deleter, LruEvictionTracker, Versioned},
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
        self.size_bytes.load(atomic::Ordering::Relaxed)
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

#[derive(Debug, Clone, Copy)]
struct DeleterCtx {
    fid: usize,
    version: u64,
}

impl Versioned for DeleterCtx {
    fn version(&self) -> u64 {
        self.version
    }
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
                .fetch_sub(entry.1.size_bytes, atomic::Ordering::Relaxed);
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

    /// Global monotonic counter for per-key versioning. Incremented under the scc bucket lock
    /// to guarantee that versions are strictly monotonically increasing per key.
    version_counter: AtomicU64,
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

    // The maximum number of eviction loop iterations before giving up and proceeding with the
    // insert. Prevents livelock when the LRU worker's ordered map is empty (e.g., Upserted
    // messages haven't been delivered yet) and try_cull succeeds but evicts nothing.
    const MAX_EVICTION_ATTEMPTS: u32 = 4;

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
            version_counter: AtomicU64::new(0),
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
            self.lru_tracker.access(*key);
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
        let new_fid = self.file_generator.fetch_add(1, atomic::Ordering::Relaxed);
        let new_size = value.len();

        let mut eviction_attempts = 0u32;
        while self.shared.size() + new_size > self.max_size_bytes {
            if self.shared.size() == 0 {
                // The new entry is larger than the entire cache size limit. Insert it anyway
                // and let the cache temporarily exceed the limit.
                break;
            }

            if eviction_attempts >= Self::MAX_EVICTION_ATTEMPTS {
                // We've tried enough times. The cache is over budget but max_size_bytes is a
                // soft hint -- proceed with the insert and let future inserts drive further
                // eviction.
                break;
            }
            eviction_attempts += 1;

            if self.lru_tracker.have_pending_culls() {
                tokio::task::yield_now().await;
                continue;
            }

            if !self.lru_tracker.try_cull(Self::LRU_EVICTION_MAX_BATCH_SIZE) {
                tokio::task::yield_now().await;
            }
        }

        // Write file to disk. The guard is constructed *before* the `.await` so that if
        // this future is cancelled while `create_file` is in flight, the drop guard still
        // cleans up the (potentially created) file.
        let path = self.shared.path_for(new_fid);
        let mut guard = FileGuard {
            path: Some(path.clone()),
        };
        let mut new_file = self.create_file(&path).await?;
        new_file.write_all(&value).await?;

        // Use entry_async to lock the bucket, then allocate version under the lock.
        // This ensures version monotonicity per key matches the actual mutation order.
        // Size accounting is also done under the lock to prevent transient underflow
        // when concurrent inserts to the same key interleave their deltas.
        let (old_entry, new_version) = match self.shared.map.entry_async(*key).await {
            scc::hash_map::Entry::Occupied(mut o) => {
                let old = *o.get();
                let v = self.version_counter.fetch_add(1, atomic::Ordering::Relaxed);
                *o.get_mut() = CacheMapEntry {
                    fid: new_fid,
                    size_bytes: new_size,
                };

                let size_delta: isize = new_size.cast_signed() - old.size_bytes.cast_signed();
                if size_delta > 0 {
                    self.shared
                        .size_bytes
                        .fetch_add(size_delta.cast_unsigned(), atomic::Ordering::Relaxed);
                } else if size_delta < 0 {
                    self.shared
                        .size_bytes
                        .fetch_sub(size_delta.unsigned_abs(), atomic::Ordering::Relaxed);
                }

                (Some(old), v)
            }
            scc::hash_map::Entry::Vacant(vacant) => {
                let v = self.version_counter.fetch_add(1, atomic::Ordering::Relaxed);
                vacant.insert_entry(CacheMapEntry {
                    fid: new_fid,
                    size_bytes: new_size,
                });

                self.shared
                    .size_bytes
                    .fetch_add(new_size, atomic::Ordering::Relaxed);

                (None, v)
            }
        };
        // Bucket lock released here.
        guard.path = None;

        // LRU notification (sync via try_send, non-cancellable).
        self.lru_tracker.upsert(
            *key,
            DeleterCtx {
                fid: new_fid,
                version: new_version,
            },
        );

        // Deferrable: delete old file (safe to cancel — file is orphaned).
        if let Some(old_entry) = old_entry {
            drop(self.shared.delete_entry_from_disk_async(&old_entry).await);
        }

        Ok(())
    }
}
