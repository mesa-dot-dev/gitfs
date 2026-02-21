use std::ffi::{OsStr, OsString};
use std::sync::Arc;
use std::sync::atomic::{AtomicU8, Ordering};

use crate::fs::LoadedAddr;

/// Cached metadata for a directory entry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DValue {
    /// Inode address of this entry.
    pub ino: LoadedAddr,
    /// Whether this entry is itself a directory.
    pub is_dir: bool,
}

/// Population states for a directory.
const POPULATE_UNCLAIMED: u8 = 0;
const POPULATE_IN_PROGRESS: u8 = 1;
const POPULATE_DONE: u8 = 2;

/// Result of attempting to claim a directory for population.
pub enum PopulateStatus {
    /// This caller won the race and should populate the directory.
    Claimed,
    /// Another caller is currently populating; wait and re-check.
    InProgress,
    /// The directory is already fully populated.
    Done,
}

/// Per-parent directory state holding child entries and a population flag.
struct DirState {
    children: scc::HashMap<OsString, DValue>,
    populated: AtomicU8,
}

impl DirState {
    fn new() -> Self {
        Self {
            children: scc::HashMap::new(),
            populated: AtomicU8::new(POPULATE_UNCLAIMED),
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
        state.children.read_sync(name, |_, v| v.clone())
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

    /// Atomically try to claim a directory for population.
    ///
    /// Uses `compare_exchange` on the three-state flag:
    /// - `UNCLAIMED → IN_PROGRESS`: returns `Claimed` (caller should populate)
    /// - Already `IN_PROGRESS`: returns `InProgress` (caller should wait)
    /// - Already `DONE`: returns `Done` (nothing to do)
    pub fn try_claim_populate(&self, parent_ino: LoadedAddr) -> PopulateStatus {
        let state = self.dir_state(parent_ino);
        match state.populated.compare_exchange(
            POPULATE_UNCLAIMED,
            POPULATE_IN_PROGRESS,
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            Ok(_) => PopulateStatus::Claimed,
            Err(POPULATE_IN_PROGRESS) => PopulateStatus::InProgress,
            Err(_) => PopulateStatus::Done,
        }
    }

    /// Mark a directory as fully populated after successful population.
    pub fn finish_populate(&self, parent_ino: LoadedAddr) {
        let state = self.dir_state(parent_ino);
        state.populated.store(POPULATE_DONE, Ordering::Release);
    }

    /// Abort a population attempt, resetting back to unclaimed so another
    /// caller can retry.
    pub fn abort_populate(&self, parent_ino: LoadedAddr) {
        let state = self.dir_state(parent_ino);
        state.populated.store(POPULATE_UNCLAIMED, Ordering::Release);
    }

    /// Wait until a directory is no longer in the `InProgress` state.
    pub async fn wait_populated(&self, parent_ino: LoadedAddr) {
        loop {
            let current = self
                .dirs
                .read_sync(&parent_ino, |_, v| v.populated.load(Ordering::Acquire))
                .unwrap_or(POPULATE_UNCLAIMED);
            if current != POPULATE_IN_PROGRESS {
                return;
            }
            tokio::task::yield_now().await;
        }
    }
}
