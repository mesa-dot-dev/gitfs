use std::ffi::{OsStr, OsString};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use crate::fs::LoadedAddr;

/// Cached metadata for a directory entry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DValue {
    /// Inode address of this entry.
    pub ino: LoadedAddr,
    /// Whether this entry is itself a directory.
    pub is_dir: bool,
}

/// Per-parent directory state holding child entries and a population flag.
struct DirState {
    children: scc::HashMap<OsString, DValue>,
    populated: AtomicBool,
}

impl DirState {
    fn new() -> Self {
        Self {
            children: scc::HashMap::new(),
            populated: AtomicBool::new(false),
        }
    }
}

/// In-memory directory entry cache with per-parent child maps.
///
/// Each parent directory gets its own [`DirState`] containing a
/// [`scc::HashMap`] of child entries and an [`AtomicBool`] population flag.
/// This makes `readdir` O(k) in the number of children rather than O(n)
/// over the entire cache.
pub struct DCache {
    dirs: scc::HashMap<LoadedAddr, Arc<DirState>>,
}

impl Default for DCache {
    fn default() -> Self {
        Self::new()
    }
}

impl DCache {
    /// Creates an empty directory cache.
    #[must_use]
    pub fn new() -> Self {
        Self {
            dirs: scc::HashMap::new(),
        }
    }

    /// Returns the [`DirState`] for `parent_ino`, creating one if absent.
    fn dir_state(&self, parent_ino: LoadedAddr) -> Arc<DirState> {
        if let Some(entry) = self.dirs.read_sync(&parent_ino, |_, v| Arc::clone(v)) {
            return entry;
        }
        let state = Arc::new(DirState::new());
        match self.dirs.entry_sync(parent_ino) {
            scc::hash_map::Entry::Occupied(occ) => Arc::clone(occ.get()),
            scc::hash_map::Entry::Vacant(vac) => {
                let cloned = Arc::clone(&state);
                vac.insert_entry(state);
                cloned
            }
        }
    }

    /// Looks up a single child entry by parent inode and name.
    #[must_use]
    pub fn lookup(&self, parent_ino: LoadedAddr, name: &OsStr) -> Option<DValue> {
        let state = self.dirs.read_sync(&parent_ino, |_, v| Arc::clone(v))?;
        state
            .children
            .read_sync(&name.to_os_string(), |_, v| v.clone())
    }

    /// Atomically inserts or overwrites a child entry in the cache.
    pub async fn insert(
        &self,
        parent_ino: LoadedAddr,
        name: OsString,
        ino: LoadedAddr,
        is_dir: bool,
    ) {
        let state = self.dir_state(parent_ino);
        let value = DValue { ino, is_dir };
        state.children.upsert_async(name, value).await;
    }

    /// Returns all cached children of `parent_ino` as `(name, value)` pairs.
    pub async fn readdir(&self, parent_ino: LoadedAddr) -> Vec<(OsString, DValue)> {
        let Some(state) = self.dirs.read_sync(&parent_ino, |_, v| Arc::clone(v)) else {
            return Vec::new();
        };
        let mut entries = Vec::new();
        state
            .children
            .iter_async(|k, v| {
                entries.push((k.clone(), v.clone()));
                true
            })
            .await;
        entries
    }

    /// Returns `true` if the directory at `parent_ino` has been fully populated.
    #[must_use]
    pub fn is_populated(&self, parent_ino: LoadedAddr) -> bool {
        self.dirs
            .read_sync(&parent_ino, |_, v| v.populated.load(Ordering::Acquire))
            .unwrap_or(false)
    }

    /// Marks the directory at `parent_ino` as fully populated.
    pub fn mark_populated(&self, parent_ino: LoadedAddr) {
        let state = self.dir_state(parent_ino);
        state.populated.store(true, Ordering::Release);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ffi::OsString;

    #[tokio::test]
    async fn lookup_returns_none_for_missing_entry() {
        let cache = DCache::new();
        assert!(cache.lookup(LoadedAddr(1), OsStr::new("foo")).is_none());
    }

    #[tokio::test]
    async fn insert_then_lookup() {
        let cache = DCache::new();
        cache
            .insert(LoadedAddr(1), OsString::from("foo"), LoadedAddr(10), false)
            .await;
        let dv = cache.lookup(LoadedAddr(1), OsStr::new("foo"));
        assert!(dv.is_some(), "entry should be present after insert");
        let dv = dv.expect("checked above");
        assert_eq!(dv.ino, LoadedAddr(10));
        assert!(!dv.is_dir);
    }

    #[tokio::test]
    async fn readdir_returns_only_children_of_parent() {
        let cache = DCache::new();
        cache
            .insert(LoadedAddr(1), OsString::from("a"), LoadedAddr(10), false)
            .await;
        cache
            .insert(LoadedAddr(1), OsString::from("b"), LoadedAddr(11), true)
            .await;
        cache
            .insert(LoadedAddr(2), OsString::from("c"), LoadedAddr(12), false)
            .await;
        let children = cache.readdir(LoadedAddr(1)).await;
        assert_eq!(children.len(), 2);
        let names: Vec<_> = children.iter().map(|(n, _)| n.clone()).collect();
        assert!(names.contains(&OsString::from("a")));
        assert!(names.contains(&OsString::from("b")));
    }

    #[tokio::test]
    async fn readdir_empty_parent_returns_empty() {
        let cache = DCache::new();
        let children = cache.readdir(LoadedAddr(1)).await;
        assert!(children.is_empty());
    }

    #[tokio::test]
    async fn is_populated_false_by_default() {
        let cache = DCache::new();
        assert!(!cache.is_populated(LoadedAddr(1)));
    }

    #[tokio::test]
    async fn mark_populated_then_check() {
        let cache = DCache::new();
        cache.mark_populated(LoadedAddr(1));
        assert!(cache.is_populated(LoadedAddr(1)));
    }

    #[tokio::test]
    async fn insert_does_not_mark_populated() {
        let cache = DCache::new();
        cache
            .insert(LoadedAddr(1), OsString::from("foo"), LoadedAddr(10), false)
            .await;
        assert!(
            !cache.is_populated(LoadedAddr(1)),
            "insert alone should not mark a directory as populated"
        );
    }

    #[tokio::test]
    async fn upsert_overwrites_existing_entry() {
        let cache = DCache::new();
        cache
            .insert(LoadedAddr(1), OsString::from("foo"), LoadedAddr(10), false)
            .await;
        cache
            .insert(LoadedAddr(1), OsString::from("foo"), LoadedAddr(20), true)
            .await;
        let dv = cache.lookup(LoadedAddr(1), OsStr::new("foo"));
        assert!(dv.is_some(), "entry should still be present after upsert");
        let dv = dv.expect("checked above");
        assert_eq!(dv.ino, LoadedAddr(20));
        assert!(dv.is_dir);
    }
}
