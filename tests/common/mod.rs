#![allow(dead_code, missing_docs, clippy::unwrap_used)]

pub mod async_fs_mocks;

use std::sync::{Arc, Mutex};
use std::time::Duration;

use git_fs::cache::eviction::lru::{Deleter, LruEvictionTracker, Versioned};

/// Minimal versioned context for LRU tests.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MockCtx {
    pub version: u64,
}

impl Versioned for MockCtx {
    fn version(&self) -> u64 {
        self.version
    }
}

/// A mock deleter that records every (key, ctx) pair it receives.
#[derive(Clone)]
pub struct MockDeleter {
    pub deleted: Arc<Mutex<Vec<(u64, MockCtx)>>>,
}

impl MockDeleter {
    pub fn new() -> Self {
        Self {
            deleted: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// Returns just the keys that were deleted, in deletion order.
    pub fn deleted_keys(&self) -> Vec<u64> {
        self.deleted
            .lock()
            .unwrap()
            .iter()
            .map(|(k, _)| *k)
            .collect()
    }
}

impl Deleter<u64, MockCtx> for MockDeleter {
    async fn delete(&mut self, key: u64, ctx: MockCtx) {
        self.deleted.lock().unwrap().push((key, ctx));
    }
}

/// Poll `have_pending_culls()` until it returns false, or panic after timeout.
pub async fn wait_for_culls(tracker: &LruEvictionTracker<u64, MockCtx>) {
    for _ in 0..200 {
        if !tracker.have_pending_culls() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    panic!("culls did not complete within 1 second");
}
