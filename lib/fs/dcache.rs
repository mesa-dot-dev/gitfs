use std::ffi::{OsStr, OsString};

use crate::fs::LoadedAddr;

/// Cached metadata for a directory entry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DValue {
    /// Inode address of this entry.
    pub ino: LoadedAddr,
    /// Whether this entry is itself a directory.
    pub is_dir: bool,
}

/// In-memory directory entry cache mapping `(parent, name)` to child metadata.
///
/// Backed by [`scc::HashMap`] for atomic upsert on insert. The `readdir`
/// implementation scans the entire map and filters by parent — this is O(n)
/// over the cache size rather than O(log n + k) with an ordered index, but
/// guarantees that `insert` never creates a window where an entry is absent.
#[derive(Default)]
pub struct DCache {
    cache: scc::HashMap<(LoadedAddr, OsString), DValue>,
}

impl DCache {
    /// Creates an empty directory cache.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Looks up a single child entry by parent inode and name.
    #[must_use]
    pub fn lookup(&self, parent_ino: LoadedAddr, name: &OsStr) -> Option<DValue> {
        let key = (parent_ino, name.to_os_string());
        self.cache.read_sync(&key, |_, v| v.clone())
    }

    /// Atomically inserts or overwrites a child entry in the cache.
    pub async fn insert(
        &self,
        parent_ino: LoadedAddr,
        name: OsString,
        ino: LoadedAddr,
        is_dir: bool,
    ) {
        let key = (parent_ino, name);
        let value = DValue { ino, is_dir };
        self.cache.upsert_async(key, value).await;
    }

    /// Returns all cached children of `parent_ino` as `(name, value)` pairs.
    pub async fn readdir(&self, parent_ino: LoadedAddr) -> Vec<(OsString, DValue)> {
        let mut entries = Vec::new();
        self.cache
            .iter_async(|key, value| {
                if key.0 == parent_ino {
                    entries.push((key.1.clone(), value.clone()));
                }
                true
            })
            .await;
        entries
    }
}
