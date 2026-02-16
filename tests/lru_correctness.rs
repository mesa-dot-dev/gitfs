#![allow(clippy::unwrap_used, clippy::similar_names, missing_docs)]

mod common;

use common::{MockCtx, MockDeleter, wait_for_culls};
use git_fs::cache::eviction::lru::LruEvictionTracker;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn evicts_least_recently_inserted() {
    let deleter = MockDeleter::new();
    let tracker = LruEvictionTracker::spawn(deleter.clone(), 64);

    // Insert keys 1, 2, 3 in order. Key 1 is the oldest.
    tracker.upsert(1, MockCtx { version: 1 });
    tracker.upsert(2, MockCtx { version: 2 });
    tracker.upsert(3, MockCtx { version: 3 });

    // Let the worker process the upsert messages.
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    // Evict 1 key — should be key 1 (least recently inserted).
    assert!(tracker.try_cull(1), "try_cull should succeed");
    wait_for_culls(&tracker).await;

    let deleted = deleter.deleted_keys();
    assert_eq!(deleted, vec![1], "key 1 should be evicted first");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn access_moves_key_to_back() {
    let deleter = MockDeleter::new();
    let tracker = LruEvictionTracker::spawn(deleter.clone(), 64);

    tracker.upsert(1, MockCtx { version: 1 });
    tracker.upsert(2, MockCtx { version: 2 });
    tracker.upsert(3, MockCtx { version: 3 });

    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    // Access key 1, moving it to the back of the LRU queue.
    tracker.access(1);

    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    // Evict 1 key — should now be key 2 (key 1 was refreshed).
    assert!(tracker.try_cull(1));
    wait_for_culls(&tracker).await;

    let deleted = deleter.deleted_keys();
    assert_eq!(deleted, vec![2], "key 2 should be evicted since key 1 was accessed");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn try_cull_returns_true_on_success() {
    let deleter = MockDeleter::new();
    let tracker = LruEvictionTracker::spawn(deleter, 64);

    tracker.upsert(1, MockCtx { version: 1 });
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let result = tracker.try_cull(1);
    assert!(result, "try_cull should return true when channel has space");
}
