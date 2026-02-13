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

use tokio::io::{AsyncReadExt, AsyncWriteExt};

struct LruEntryDeleter<K: Eq + Hash> {
    file_cache: Arc<FileCache<K>>,
}

impl<K: Eq + Hash> LruEntryDeleter<K> {
    fn new(file_cache: Arc<FileCache<K>>) -> Self {
        Self { file_cache }
    }
}

impl<K: Copy + Eq + Hash + Sync + Send + 'static> Deleter<K> for LruEntryDeleter<K> {
    /// The LRU cache will call this method when it wants to evict keys.
    fn delete<'a>(&mut self, keys: impl Iterator<Item = &'a K>)
    where
        K: 'a,
    {
        for key in keys {
            // Deleting keys should be atomic from scc::HashMap, which means that all subsequent
            // lookups on the same key will fail, and thus we'll be a-ok.
            if let Some(entry) = self.file_cache.map.remove_sync(key) {
                // On the other hand, deleting the file may fail for a variety of reasons, but
                // that's all mostly managed by the delete_entry_from_disk_sync method, which
                // gracefully handles the case where the file is already deleted.
                self.file_cache.delete_entry_from_disk_sync(&entry.1);
            }
        }
    }
}

/// Error thrown during construction of a FileCache which describes why the given root path is
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
    /// The root path for the cache. This is the directory where all cache files will be stored.
    root_path: PathBuf,

    /// The main map of the cache. This maps keys to file IDs, which are just integers that
    /// correspond to file paths on disk. The file ID is used to generate the file path for the
    /// cache entry.
    map: scc::HashMap<K, CacheMapEntry>,

    /// Generates unique file paths for new cache entries. This is just a counter that increments
    /// for each new file, and the file ID is the value of the counter at the time of creation.
    file_generator: AtomicUsize,

    /// Rough estimate of the current size of the cache in bytes. Not exact, but should be close
    /// enough for eviction purposes.
    size_bytes: AtomicUsize,

    /// The maximum size of the cache in bytes. This is used to determine when to evict entries
    /// from the cache. This is a soft hint, rather than a hard limit, and the cache may
    /// temporarily exceed this size during insertions, but it will try to evict entries as soon as
    /// possible to get back under this limit.
    max_size_bytes: usize,

    /// LRU eviction tracker. This is used to track the least recently used keys in the cache, and
    /// to evict keys when necessary.
    lru_tracker: LruEvictionTracker<K>,
}

impl<K: Eq + Hash + Send + Copy + 'static> FileCache<K> {
    // How many cache entries to evict at most in a single batch.
    //
    // Not really sure how to determine this number.
    const LRU_EVICTION_MAX_BATCH_SIZE: usize = 32;

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
    ///                 which was previously used as a cache for this program.
    /// * `max_size_bytes` - The maximum size of the cache in bytes. This is used to determine when
    ///                      to evict entries from the cache. This is a soft hint, rather than a
    ///                      hard limit, and the cache may temporarily exceed this size during
    ///                      insertions, but it will try to evict entries as soon as possible to
    ///                      get back under this limit.
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
            .write(true)
            .open(&pbuf)
            .await?;
        pbuf.pop();

        Ok(Self {
            root_path: pbuf,
            map: scc::HashMap::new(),
            file_generator: AtomicUsize::new(0),
            size_bytes: AtomicUsize::new(0),
            max_size_bytes,
            lru_tracker: LruEvictionTracker::spawn(
                LruEntryDeleter::new(),
                Self::LRU_EVICTION_MAX_BATCH_SIZE,
            ),
        })
    }

    /// Retrieve the correct path for the given file ID.
    fn path_for(&self, fid: usize) -> PathBuf {
        self.root_path.join(fid.to_string())
    }

    async fn create_file(&self, path: &Path) -> Result<tokio::fs::File, std::io::Error> {
        tokio::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .open(path)
            .await
    }

    /// Given an entry, remove it from the disk and update the size of the cache accordingly.
    ///
    /// Gracefully handles the case where the file doesn't exist, which can happen if the file was
    /// deleted after we read the file ID from the map, but before we tried to delete
    /// the file.
    async fn delete_entry_from_disk_async(&self, entry: &CacheMapEntry) -> std::io::Result<()> {
        match tokio::fs::remove_file(self.path_for(entry.fid)).await {
            Ok(()) => {
                self.size_bytes
                    .fetch_sub(entry.size_bytes, std::sync::atomic::Ordering::Relaxed);
                Ok(())
            }
            // Note that there's a race condition between the deleter and the mutator in
            // AsyncWritableCache. Either one of these operations atomically grabs the file ID (so
            // accessing the file ID is safe), but then they race to delete the file.
            //
            // Consequently, it's quite possible that the file is already deleted by the time we
            // try to delete it, and that's already fine with us, so we just treat that case as a
            // successful deletion.
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e),
        }
    }

    /// Given an entry, remove it from the disk and update the size of the cache accordingly.
    ///
    /// Gracefully handles the case where the file doesn't exist, which can happen if the file was
    /// deleted after we read the file ID from the map, but before we tried to delete
    /// the file.
    fn delete_entry_from_disk_sync(&self, entry: &CacheMapEntry) -> std::io::Result<()> {
        match std::fs::remove_file(self.path_for(entry.fid)) {
            Ok(()) => {
                self.size_bytes
                    .fetch_sub(entry.size_bytes, std::sync::atomic::Ordering::Relaxed);
                Ok(())
            }
            // Note that there's a race condition between the deleter and the mutator in
            // AsyncWritableCache. Either one of these operations atomically grabs the file ID (so
            // accessing the file ID is safe), but then they race to delete the file.
            //
            // Consequently, it's quite possible that the file is already deleted by the time we
            // try to delete it, and that's already fine with us, so we just treat that case as a
            // successful deletion.
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e),
        }
    }
}

impl<K: Eq + Hash + Copy + Send + Debug + 'static> AsyncReadableCache<K, Vec<u8>> for FileCache<K> {
    async fn get(&self, key: &K) -> Option<Vec<u8>> {
        for _ in 0..Self::MAX_READ_RETRY_COUNT {
            // Grab the best-guess file ID for the given key. If this fails, then the key is not in
            // the cache, and we can return `None`.
            let entry = *(self.map.get_async(key).await?);

            // The next thing we need to do is try to open the file. This may fail for a plethora
            // of reasons.
            //
            // TODO(markovejnovic): path_for allocates on heap, could be on stack.
            let mut file = match tokio::fs::File::open(self.path_for(entry.fid)).await {
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
        self.map.contains_async(key).await
    }
}

impl<K: Eq + Hash + Copy + Send + 'static> AsyncWritableCache<K, Vec<u8>, CacheWriteError>
    for FileCache<K>
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

        if self.size_bytes.load(std::sync::atomic::Ordering::Relaxed) + new_entry.size_bytes
            > self.max_size_bytes
        {
            // We need to evict some entries before we can insert this new entry. Let's just evict
            // a batch of entries.
            self.lru_tracker
                .cull(Self::LRU_EVICTION_MAX_BATCH_SIZE)
                .await;
        }

        let mut new_file = self.create_file(&self.path_for(new_entry.fid)).await?;
        new_file.write_all(&value).await?;

        self.size_bytes
            .fetch_add(new_entry.size_bytes, std::sync::atomic::Ordering::Relaxed);

        // Now we insert the new file ID into the map, and get the old file ID if it exists.
        if let Some(old_entry) = self.map.upsert_async(*key, new_entry).await {
            // If there was an old file ID, we need to delete the old file and notify the LRU
            // tracker that the key was accessed.
            self.lru_tracker.access(*key).await;

            // TODO(markovejnovic): Could stack allocate the path.
            // Note that the file already may be deleted at this point -- the LRU tracker deleter
            // may have already deleted this file, so if the file doesn't exist, that's already
            // fine with us.
            self.delete_entry_from_disk_async(&old_entry).await?;
        } else {
            self.lru_tracker.insert(*key).await;
        }

        // Epic, we did it.
        Ok(())
    }
}
