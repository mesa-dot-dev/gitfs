//! Concurrent cache size and staleness tracker.
//!
//! [`CacheTracker`] records which paths are cached, their estimated sizes, and
//! when they were inserted. It is safe to use from many async tasks
//! simultaneously because every method takes `&self`.
#![allow(dead_code, reason = "consumed by WriteThroughFs in Tasks 4-6")]

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use scc::HashMap as ConcurrentHashMap;

struct CacheEntry {
    inserted_at: Instant,
    estimated_size: u64,
}

/// Tracks cached file paths, their estimated sizes, and insertion times.
///
/// All methods take `&self`, making the tracker safe for concurrent use from
/// multiple async tasks without external synchronisation.
pub struct CacheTracker {
    entries: ConcurrentHashMap<PathBuf, CacheEntry>,
    total_bytes: AtomicU64,
}

impl CacheTracker {
    /// Create a new, empty tracker.
    #[must_use]
    pub fn new() -> Self {
        Self {
            entries: ConcurrentHashMap::new(),
            total_bytes: AtomicU64::new(0),
        }
    }

    /// Record a cached path with its estimated size.
    ///
    /// If the path was already tracked, its entry is replaced and
    /// `total_bytes` is adjusted to reflect the new size.
    pub async fn track(&self, path: PathBuf, size: u64) {
        use scc::hash_map::Entry;

        match self.entries.entry_async(path).await {
            Entry::Vacant(vac) => {
                vac.insert_entry(CacheEntry {
                    inserted_at: Instant::now(),
                    estimated_size: size,
                });
                self.total_bytes.fetch_add(size, Ordering::Relaxed);
            }
            Entry::Occupied(mut occ) => {
                let old_size = occ.get().estimated_size;
                occ.get_mut().estimated_size = size;
                occ.get_mut().inserted_at = Instant::now();

                // Adjust total: subtract old, add new.
                if old_size <= size {
                    self.total_bytes
                        .fetch_add(size - old_size, Ordering::Relaxed);
                } else {
                    self.total_bytes
                        .fetch_sub(old_size - size, Ordering::Relaxed);
                }
            }
        }
    }

    /// Remove a path from tracking, reducing `total_bytes` accordingly.
    ///
    /// If the path is not tracked this is a no-op.
    pub async fn untrack(&self, path: &Path) {
        if let Some((_, entry)) = self.entries.remove_async(path).await {
            self.total_bytes
                .fetch_sub(entry.estimated_size, Ordering::Relaxed);
        }
    }

    /// Total estimated bytes across all tracked entries.
    #[must_use]
    pub fn estimated_size(&self) -> u64 {
        self.total_bytes.load(Ordering::Relaxed)
    }

    /// Number of tracked entries.
    #[must_use]
    pub fn entry_count(&self) -> usize {
        self.entries.len()
    }

    /// Collect paths whose cache entry is older than `max_age`.
    pub async fn stale_entries(&self, max_age: Duration) -> Vec<PathBuf> {
        let Some(cutoff) = Instant::now().checked_sub(max_age) else {
            // max_age exceeds process uptime -- nothing can be that old.
            return Vec::new();
        };
        let mut stale = Vec::new();

        self.entries
            .iter_async(|key, value| {
                if value.inserted_at < cutoff {
                    stale.push(key.clone());
                }
                true
            })
            .await;

        stale
    }
}

impl Default for CacheTracker {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn new_tracker_has_zero_size() {
        let tracker = CacheTracker::new();
        assert_eq!(tracker.estimated_size(), 0, "fresh tracker should be empty");
        assert_eq!(
            tracker.entry_count(),
            0,
            "fresh tracker should have no entries"
        );
    }

    #[tokio::test]
    async fn track_increases_estimated_size() {
        let tracker = CacheTracker::new();
        tracker.track(PathBuf::from("/a"), 100).await;
        tracker.track(PathBuf::from("/b"), 250).await;

        assert_eq!(
            tracker.estimated_size(),
            350,
            "total should be sum of tracked sizes"
        );
        assert_eq!(tracker.entry_count(), 2, "should have two entries");
    }

    #[tokio::test]
    async fn track_same_path_replaces_size() {
        let tracker = CacheTracker::new();
        tracker.track(PathBuf::from("/a"), 100).await;
        tracker.track(PathBuf::from("/a"), 60).await;

        assert_eq!(
            tracker.estimated_size(),
            60,
            "size should reflect replacement"
        );
        assert_eq!(
            tracker.entry_count(),
            1,
            "duplicate path should not create second entry"
        );
    }

    #[tokio::test]
    async fn untrack_decreases_size() {
        let tracker = CacheTracker::new();
        tracker.track(PathBuf::from("/a"), 100).await;
        tracker.track(PathBuf::from("/b"), 200).await;
        tracker.untrack(Path::new("/a")).await;

        assert_eq!(tracker.estimated_size(), 200, "only /b should remain");
        assert_eq!(
            tracker.entry_count(),
            1,
            "should have one entry after untrack"
        );
    }

    #[tokio::test]
    async fn untrack_missing_path_is_noop() {
        let tracker = CacheTracker::new();
        tracker.track(PathBuf::from("/a"), 100).await;
        tracker.untrack(Path::new("/nonexistent")).await;

        assert_eq!(tracker.estimated_size(), 100, "size should be unchanged");
        assert_eq!(tracker.entry_count(), 1, "entry count should be unchanged");
    }

    #[tokio::test]
    async fn stale_entries_returns_old_paths() {
        let tracker = CacheTracker::new();
        tracker.track(PathBuf::from("/old"), 50).await;

        // Wait long enough for the entry to become stale.
        tokio::time::sleep(Duration::from_millis(50)).await;

        tracker.track(PathBuf::from("/fresh"), 50).await;

        let stale = tracker.stale_entries(Duration::from_millis(25)).await;
        assert!(
            stale.contains(&PathBuf::from("/old")),
            "/old should be stale"
        );
        assert!(
            !stale.contains(&PathBuf::from("/fresh")),
            "/fresh should not be stale"
        );
    }

    #[tokio::test]
    async fn stale_entries_returns_empty_when_all_fresh() {
        let tracker = CacheTracker::new();
        tracker.track(PathBuf::from("/a"), 10).await;
        tracker.track(PathBuf::from("/b"), 20).await;

        let stale = tracker.stale_entries(Duration::from_secs(60)).await;
        assert!(
            stale.is_empty(),
            "no entries should be stale with a 60s threshold"
        );
    }

    #[tokio::test]
    async fn entry_count_tracks_insertions_and_removals() {
        let tracker = CacheTracker::new();
        assert_eq!(tracker.entry_count(), 0, "starts at zero");

        tracker.track(PathBuf::from("/a"), 1).await;
        assert_eq!(tracker.entry_count(), 1, "one after first insert");

        tracker.track(PathBuf::from("/b"), 2).await;
        assert_eq!(tracker.entry_count(), 2, "two after second insert");

        tracker.untrack(Path::new("/a")).await;
        assert_eq!(tracker.entry_count(), 1, "back to one after removal");

        tracker.untrack(Path::new("/b")).await;
        assert_eq!(tracker.entry_count(), 0, "zero after removing all");
    }
}
