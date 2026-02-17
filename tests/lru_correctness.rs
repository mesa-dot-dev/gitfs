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
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

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

    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    // Access key 1, moving it to the back of the LRU queue.
    tracker.access(1);

    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    // Evict 1 key — should now be key 2 (key 1 was refreshed).
    assert!(tracker.try_cull(1));
    wait_for_culls(&tracker).await;

    let deleted = deleter.deleted_keys();
    assert_eq!(
        deleted,
        vec![2],
        "key 2 should be evicted since key 1 was accessed"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn try_cull_returns_true_on_success() {
    let deleter = MockDeleter::new();
    let tracker = LruEvictionTracker::spawn(deleter, 64);

    tracker.upsert(1, MockCtx { version: 1 });
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let result = tracker.try_cull(1);
    assert!(result, "try_cull should return true when channel has space");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn have_pending_culls_reflects_state() {
    let deleter = MockDeleter::new();
    let tracker = LruEvictionTracker::spawn(deleter, 64);

    // No culls requested yet.
    assert!(!tracker.have_pending_culls());

    tracker.upsert(1, MockCtx { version: 1 });
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    // Request a cull.
    let _ = tracker.try_cull(1);

    // Immediately after try_cull, pending work should be true
    // (unless the worker already processed it, which is unlikely but possible).
    // Wait for completion and verify it clears.
    wait_for_culls(&tracker).await;
    assert!(!tracker.have_pending_culls());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stale_version_ignored() {
    let deleter = MockDeleter::new();
    let tracker = LruEvictionTracker::spawn(deleter.clone(), 64);

    // Upsert key 1 with version 5, then send a stale version 3.
    tracker.upsert(1, MockCtx { version: 5 });
    tracker.upsert(1, MockCtx { version: 3 });

    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    // Evict — the context should carry version 5 (stale version 3 was dropped).
    assert!(tracker.try_cull(1));
    wait_for_culls(&tracker).await;

    let deleted = deleter.deleted.lock().unwrap();
    assert_eq!(deleted.len(), 1);
    assert_eq!(
        deleted[0].1.version, 5,
        "should carry version 5, not stale version 3"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn evict_more_than_available() {
    let deleter = MockDeleter::new();
    let tracker = LruEvictionTracker::spawn(deleter.clone(), 64);

    tracker.upsert(1, MockCtx { version: 1 });
    tracker.upsert(2, MockCtx { version: 2 });

    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    // Request eviction of 100 keys but only 2 exist.
    assert!(tracker.try_cull(100));
    wait_for_culls(&tracker).await;

    let deleted = deleter.deleted_keys();
    assert_eq!(deleted.len(), 2, "should only evict the 2 available keys");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn multiple_eviction_rounds() {
    let deleter = MockDeleter::new();
    let tracker = LruEvictionTracker::spawn(deleter.clone(), 64);

    for i in 1u64..=6 {
        tracker.upsert(i, MockCtx { version: i });
    }

    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    // Round 1: evict 2
    assert!(tracker.try_cull(2));
    wait_for_culls(&tracker).await;

    // Round 2: evict 2 more
    assert!(tracker.try_cull(2));
    wait_for_culls(&tracker).await;

    let deleted = deleter.deleted_keys();
    assert_eq!(
        deleted.len(),
        4,
        "should have evicted 4 total across 2 rounds"
    );
    // First round evicts keys 1, 2; second round evicts keys 3, 4.
    // Deletion tasks within a round may complete in any order, so sort each batch.
    let mut round1: Vec<_> = deleted[..2].to_vec();
    round1.sort_unstable();
    let mut round2: Vec<_> = deleted[2..].to_vec();
    round2.sort_unstable();
    assert_eq!(round1, vec![1, 2]);
    assert_eq!(round2, vec![3, 4]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn try_cull_returns_false_when_channel_full() {
    let deleter = MockDeleter::new();
    // Channel size 1 — fills immediately.
    let tracker = LruEvictionTracker::spawn(deleter, 1);

    tracker.upsert(1, MockCtx { version: 1 });
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    // First cull takes the only channel slot.
    assert!(tracker.try_cull(1), "first try_cull should succeed");

    // Second cull should fail — channel is full.
    assert!(
        !tracker.try_cull(1),
        "second try_cull should fail when channel is full"
    );

    // After the worker drains, pending culls should eventually clear.
    wait_for_culls(&tracker).await;
    assert!(!tracker.have_pending_culls());
}
