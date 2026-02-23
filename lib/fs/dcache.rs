use std::collections::BTreeMap;
use std::ffi::{OsStr, OsString};
use std::sync::atomic::{AtomicU8, AtomicU64, Ordering};
use std::sync::{Arc, RwLock};

use tokio::sync::Notify;

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
    /// Carries the generation at claim time so [`DCache::finish_populate`]
    /// can detect whether an eviction invalidated the populate.
    Claimed(u64),
    /// Another caller is currently populating; wait and re-check.
    InProgress,
    /// The directory is already fully populated.
    Done,
}

/// Per-parent directory state holding child entries and a population flag.
struct DirState {
    /// Child entries, guarded by `std::sync::RwLock` (NOT `tokio::sync::RwLock`).
    ///
    /// This is intentional: all lock acquisitions are scoped and synchronous,
    /// never held across `.await` points. `std::sync::RwLock` has lower
    /// overhead in the uncontended case. Do NOT introduce `.await` calls
    /// while holding a guard — this would block the tokio worker thread.
    children: RwLock<BTreeMap<OsString, DValue>>,
    populated: AtomicU8,
    /// Monotonically increasing counter bumped by each [`DCache::evict`] call.
    /// Allows [`DCache::finish_populate`] to detect that an eviction occurred
    /// while a populate was in flight.
    generation: AtomicU64,
    /// Wakes waiters when `populated` transitions out of `IN_PROGRESS`.
    notify: Notify,
}

impl DirState {
    fn new() -> Self {
        Self {
            children: RwLock::new(BTreeMap::new()),
            populated: AtomicU8::new(POPULATE_UNCLAIMED),
            generation: AtomicU64::new(0),
            notify: Notify::new(),
        }
    }
}

/// In-memory directory entry cache with per-parent child maps.
///
/// Each parent directory gets its own [`DirState`] containing a
/// [`BTreeMap`] of child entries (kept in sorted order) and an [`AtomicU8`]
/// population flag. This makes `readdir` O(k) in the number of children
/// with zero sorting overhead.
pub struct DCache {
    dirs: scc::HashMap<LoadedAddr, Arc<DirState>>,
    /// Reverse index: child inode -> parent inode, for O(1) parent discovery
    /// during eviction.
    child_to_parent: scc::HashMap<LoadedAddr, LoadedAddr>,
    /// Reverse index: child inode -> entry name, for O(log n) removal from
    /// the parent's `BTreeMap` during eviction (instead of O(n) `retain`).
    child_to_name: scc::HashMap<LoadedAddr, OsString>,
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
            child_to_parent: scc::HashMap::new(),
            child_to_name: scc::HashMap::new(),
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
        let children = state
            .children
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        children.get(name).cloned()
    }

    /// Atomically inserts or overwrites a child entry in the cache.
    ///
    /// Handles two kinds of stale-entry cleanup before the insert:
    ///
    /// - **Cross-parent move:** the child was previously cached under a
    ///   different parent. The old entry is removed and the old parent's
    ///   populate status is reset to `UNCLAIMED`.
    /// - **Same-parent rename:** the child was previously cached under this
    ///   parent with a different name. The old name entry is removed so that
    ///   `readdir` does not return two entries for the same inode.
    pub fn insert(&self, parent_ino: LoadedAddr, name: OsString, ino: LoadedAddr, is_dir: bool) {
        self.cleanup_stale_entry(parent_ino, &name, ino);

        let state = self.dir_state(parent_ino);
        let value = DValue { ino, is_dir };
        let mut children = state
            .children
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(old) = children.insert(name.clone(), value)
            && old.ino != ino
        {
            self.child_to_name.remove_sync(&old.ino);
            self.child_to_parent.remove_sync(&old.ino);
        }
        self.child_to_name.upsert_sync(ino, name);
        self.child_to_parent.upsert_sync(ino, parent_ino);
    }

    /// Remove a stale cache entry for `ino` if it moved to a new parent or
    /// was renamed within the same parent.
    ///
    /// For cross-parent moves the old parent's write lock is acquired and
    /// released before `insert` takes the new parent's write lock, avoiding
    /// deadlock from simultaneous two-lock holds.
    ///
    /// For same-parent renames the same lock is acquired sequentially (not
    /// nested). The brief window between the two acquisitions is acceptable:
    /// during initial population `readdir` is blocked by `IN_PROGRESS`, and
    /// after population a concurrent `readdir` would at worst momentarily
    /// miss the entry — it will reappear on the next call.
    ///
    /// # Concurrent same-inode inserts
    ///
    /// Two concurrent `insert` calls for the same `ino` with different names
    /// under the same parent can orphan an entry: both read the same stale
    /// `old_name`, the first removes it, the second's guard no-ops, and both
    /// inserts proceed — leaving two name→ino mappings with only the last
    /// writer's name in the reverse index. This is not reachable in practice
    /// because [`AsyncFs`](super::async_fs::AsyncFs) deduplicates per-inode
    /// operations through [`FutureBackedCache`](crate::cache::async_backed::FutureBackedCache),
    /// but callers that bypass that layer must serialize inserts per inode.
    fn cleanup_stale_entry(&self, new_parent: LoadedAddr, new_name: &OsStr, ino: LoadedAddr) {
        let Some(old_parent) = self.child_to_parent.read_sync(&ino, |_, &v| v) else {
            return;
        };
        let Some(old_name) = self.child_to_name.read_sync(&ino, |_, v| v.clone()) else {
            return;
        };
        if old_parent == new_parent && old_name.as_os_str() == new_name {
            return;
        }
        let Some(old_state) = self.dirs.read_sync(&old_parent, |_, v| Arc::clone(v)) else {
            return;
        };
        let mut old_children = old_state
            .children
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        // Guard: only remove if the entry still maps to this inode.
        // A concurrent insert may have reused the name for a different child.
        if old_children.get(&old_name).is_some_and(|dv| dv.ino == ino) {
            old_children.remove(&old_name);
        }
        drop(old_children);
        // Only reset populate status for cross-parent moves. A same-parent
        // rename does not invalidate the directory listing — the data
        // provider returned the new name, so the cache is being corrected,
        // not going stale.
        if old_parent != new_parent {
            old_state.generation.fetch_add(1, Ordering::Release);
            let _ = old_state.populated.compare_exchange(
                POPULATE_DONE,
                POPULATE_UNCLAIMED,
                Ordering::AcqRel,
                Ordering::Relaxed,
            );
            old_state.notify.notify_waiters();
        }
    }

    /// Iterate all cached children of `parent_ino` in name-sorted order.
    ///
    /// Calls `f` for each `(name, value)` pair while holding the read lock.
    /// Callers decide what to collect, avoiding unnecessary allocations for
    /// entries that will be skipped (e.g. by offset-based pagination).
    pub fn readdir(&self, parent_ino: LoadedAddr, mut f: impl FnMut(&OsStr, &DValue)) {
        let Some(state) = self.dirs.read_sync(&parent_ino, |_, v| Arc::clone(v)) else {
            return;
        };
        let children = state
            .children
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        for (name, value) in children.iter() {
            f(name, value);
        }
    }

    /// Returns the [`LoadedAddr`] of every child that is itself a directory.
    ///
    /// Used by the prefetch logic to discover which subdirectories to
    /// background-populate after a `readdir` completes.
    #[must_use]
    pub fn child_dir_addrs(&self, parent_ino: LoadedAddr) -> Vec<LoadedAddr> {
        let Some(state) = self.dirs.read_sync(&parent_ino, |_, v| Arc::clone(v)) else {
            return Vec::new();
        };
        let children = state
            .children
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        children
            .values()
            .filter(|dv| dv.is_dir)
            .map(|dv| dv.ino)
            .collect()
    }

    /// Removes a single child entry from the cache.
    ///
    /// Returns the removed [`DValue`] if it was present, or `None` if the
    /// parent or child did not exist.
    pub fn remove_child(&self, parent_ino: LoadedAddr, name: &OsStr) -> Option<DValue> {
        let state = self.dirs.read_sync(&parent_ino, |_, v| Arc::clone(v))?;
        let mut children = state
            .children
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let removed = children.remove(name);
        if let Some(ref dv) = removed {
            self.child_to_parent.remove_sync(&dv.ino);
            self.child_to_name.remove_sync(&dv.ino);
        }
        removed
    }

    /// Removes the entire [`DirState`] for `parent_ino`, resetting its
    /// population status so the next `readdir` will re-fetch from the
    /// data provider.
    ///
    /// Returns `true` if an entry was removed.
    pub fn remove_parent(&self, parent_ino: LoadedAddr) -> bool {
        if let Some((_, state)) = self.dirs.remove_sync(&parent_ino) {
            let children = state
                .children
                .read()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            for dv in children.values() {
                self.child_to_parent.remove_sync(&dv.ino);
                self.child_to_name.remove_sync(&dv.ino);
            }
            true
        } else {
            false
        }
    }

    /// Evict a child inode from the cache by its address.
    ///
    /// Looks up the parent via the reverse index, removes the child entry
    /// from that parent's children map, and resets the parent's populate
    /// flag to `UNCLAIMED` so the next `readdir` re-fetches from the
    /// data provider.
    ///
    /// The reset uses `compare_exchange(DONE -> UNCLAIMED)` rather than a
    /// blind store to avoid a race with an in-flight populate: if a
    /// concurrent `readdir` is mid-populate (`IN_PROGRESS`), a blind store
    /// of `UNCLAIMED` would be overwritten by the populator's final `DONE`
    /// store, leaving the cache in a stale-but-marked-done state.
    ///
    /// # Ordering with concurrent `insert`
    ///
    /// The reverse-index removal and children-map removal are performed
    /// while holding the parent's `children` write lock. This serializes
    /// with `insert` (which also holds the write lock while updating the
    /// reverse indices), preventing a race where a concurrent `insert`
    /// for the same child inode could clobber freshly removed reverse-index
    /// entries between `child_to_parent.remove_sync` and the write lock
    /// acquisition.
    pub fn evict(&self, child_ino: LoadedAddr) {
        // Read the parent without removing — we need the write lock first
        // to serialize with concurrent `insert`.
        let Some(parent_ino) = self.child_to_parent.read_sync(&child_ino, |_, &v| v) else {
            return;
        };
        let Some(state) = self.dirs.read_sync(&parent_ino, |_, v| Arc::clone(v)) else {
            // Parent dir was removed; clean up reverse indices.
            self.child_to_parent.remove_sync(&child_ino);
            self.child_to_name.remove_sync(&child_ino);
            return;
        };
        let mut children = state
            .children
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        // Re-check that child_to_parent still points to this parent.
        // A concurrent `insert` may have re-parented the child while we
        // were waiting for the write lock.
        let still_ours = self
            .child_to_parent
            .read_sync(&child_ino, |_, &v| v == parent_ino)
            .unwrap_or(false);
        if !still_ours {
            // The child was re-parented by a concurrent insert.
            // Nothing to do — the new parent owns the reverse indices.
            return;
        }
        // Now atomically remove reverse indices and children entry.
        self.child_to_parent.remove_sync(&child_ino);
        if let Some((_, name)) = self.child_to_name.remove_sync(&child_ino) {
            children.remove(&name);
        }
        drop(children);
        // Bump generation so any in-flight populate knows its data is stale.
        state.generation.fetch_add(1, Ordering::Release);
        // Reset DONE -> UNCLAIMED so the next readdir re-fetches.
        let _ = state.populated.compare_exchange(
            POPULATE_DONE,
            POPULATE_UNCLAIMED,
            Ordering::AcqRel,
            Ordering::Relaxed,
        );
        state.notify.notify_waiters();
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
            Ok(_) => {
                let claim_gen = state.generation.load(Ordering::Acquire);
                PopulateStatus::Claimed(claim_gen)
            }
            Err(POPULATE_IN_PROGRESS) => PopulateStatus::InProgress,
            Err(_) => PopulateStatus::Done,
        }
    }

    /// Mark a directory as fully populated after successful population.
    ///
    /// `claimed_gen` is the generation returned by [`try_claim_populate`]. If
    /// an [`evict`](Self::evict) bumped the generation since then, the data
    /// is stale so the flag is reset to `UNCLAIMED` instead of `DONE`.
    pub fn finish_populate(&self, parent_ino: LoadedAddr, claimed_gen: u64) {
        let state = self.dir_state(parent_ino);
        let current_gen = state.generation.load(Ordering::Acquire);
        if current_gen == claimed_gen {
            state.populated.store(POPULATE_DONE, Ordering::Release);
        } else {
            state.populated.store(POPULATE_UNCLAIMED, Ordering::Release);
        }
        state.notify.notify_waiters();
    }

    /// Abort a population attempt, resetting back to unclaimed so another
    /// caller can retry.
    pub fn abort_populate(&self, parent_ino: LoadedAddr) {
        let state = self.dir_state(parent_ino);
        state.populated.store(POPULATE_UNCLAIMED, Ordering::Release);
        state.notify.notify_waiters();
    }

    /// Wait until a directory is no longer in the `InProgress` state.
    ///
    /// Uses [`Notify`] to sleep efficiently instead of spinning.
    ///
    /// The `Notified` future is pinned and `enable()`d before checking the
    /// flag so that the waiter is registered with the `Notify` *before* the
    /// state check. Without this, a `notify_waiters()` firing between
    /// `notified()` and the first poll would be lost (since
    /// `notify_waiters` does not store a permit), causing a permanent hang.
    pub async fn wait_populated(&self, parent_ino: LoadedAddr) {
        let state = self.dir_state(parent_ino);
        loop {
            let mut notified = std::pin::pin!(state.notify.notified());
            notified.as_mut().enable();
            let current = state.populated.load(Ordering::Acquire);
            if current != POPULATE_IN_PROGRESS {
                return;
            }
            // SAFETY(cancel): re-entering the loop re-creates the Notified
            // future, so spurious wakeups just re-check the flag.
            notified.await;
        }
    }
}
