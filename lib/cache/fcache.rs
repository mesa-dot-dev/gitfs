//! File-based cache.

use std::{
    hash::Hash,
    path::{Path, PathBuf},
    sync::atomic::AtomicUsize,
};

use crate::{cache::traits::AsyncReadableCache, io};
use thiserror::Error;

use tokio::io::AsyncReadExt;

#[derive(Debug, Error)]
pub enum InvalidRootPathError {
    #[error("Root path is not a directory: {0}")]
    NotADirectory(PathBuf),

    #[error("Root path appears to contain data stemming from sources different to this app: {0}")]
    RootPathUnsafeCache(PathBuf),

    #[error("IO error while accessing root path: {0}")]
    Io(#[from] std::io::Error),
}

pub struct FileCache<K: Eq + Hash> {
    root_path: PathBuf,
    map: scc::HashMap<K, usize>,
    file_generator: AtomicUsize,
}

impl<K: Eq + Hash> FileCache<K> {
    // Dangerous: Changing this constant may cause the program to treat existing cache directories
    // as invalid, and thus provide a worse user experience. Do not change this unless you have a
    // very good reason to do so. Changing this will break backwards compatibility with existing
    // cache directories.
    const GITFS_MARKER_FILE: &'static str = ".gitfs_cache";

    /// Try to create a new file cache at the given path.
    ///
    /// If the path exists, it must either be an empty directory, or a directory which was
    /// previously used as a cache for this program.
    pub async fn new(file_path: &Path) -> Result<Self, InvalidRootPathError> {
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
        })
    }
}

impl<K: Eq + Hash> AsyncReadableCache<K, Vec<u8>> for FileCache<K> {
    async fn get(&self, key: &K) -> Option<Vec<u8>> {
        let mut file = {
            let entry = self.map.get_async(key).await?;
            let path = self.root_path.join(entry.get().to_string());
            tokio::fs::File::open(&path).await.ok()?
        };

        let mut buf = Vec::new();
        file.read_to_end(&mut buf).await.ok()?;
        Some(buf)
    }

    async fn contains(&self, key: &K) -> bool {
        self.map.contains_async(key).await
    }
}
