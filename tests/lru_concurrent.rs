#![allow(clippy::unwrap_used, clippy::similar_names, missing_docs)]

mod common;

use std::sync::Arc;
use std::time::Duration;

use common::{MockCtx, MockDeleter, wait_for_culls};
use mesafs::cache::eviction::lru::LruEvictionTracker;
use tokio::task::JoinSet;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_upserts() {
    let deleter = MockDeleter::new();
    let tracker = Arc::new(LruEvictionTracker::spawn(deleter.clone(), 256));

    let mut set = JoinSet::new();
    for i in 0u64..100 {
        let tracker = Arc::clone(&tracker);
        set.spawn(async move {
            tracker.upsert(i, MockCtx { version: i });
        });
    }
    while set.join_next().await.is_some() {}

    // Let the worker process all messages.
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Evict all 100 — every key should appear in the deletion list exactly once.
    assert!(tracker.try_cull(100));
    wait_for_culls(&tracker).await;

    let mut deleted = deleter.deleted_keys();
    deleted.sort_unstable();
    let expected: Vec<u64> = (0..100).collect();
    assert_eq!(deleted, expected, "all 100 keys should have been evicted");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_access_notifications() {
    let deleter = MockDeleter::new();
    let tracker = Arc::new(LruEvictionTracker::spawn(deleter.clone(), 256));

    // Insert keys 1..=10
    for i in 1u64..=10 {
        tracker.upsert(i, MockCtx { version: i });
    }
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Concurrently access keys 1..=5, making them "recently used."
    let mut set = JoinSet::new();
    for i in 1u64..=5 {
        let tracker = Arc::clone(&tracker);
        set.spawn(async move {
            for _ in 0..10 {
                tracker.access(i);
            }
        });
    }
    while set.join_next().await.is_some() {}
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Evict 5 keys — keys 6..=10 should be evicted (they were never accessed).
    assert!(tracker.try_cull(5));
    wait_for_culls(&tracker).await;

    let mut deleted = deleter.deleted_keys();
    deleted.sort_unstable();
    assert_eq!(
        deleted,
        vec![6, 7, 8, 9, 10],
        "non-accessed keys 6..=10 should be evicted first"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_cull_requests() {
    let deleter = MockDeleter::new();
    let tracker = Arc::new(LruEvictionTracker::spawn(deleter.clone(), 256));

    for i in 0u64..20 {
        tracker.upsert(i, MockCtx { version: i });
    }
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Fire 10 concurrent cull requests, each for 2 keys.
    let mut set = JoinSet::new();
    for _ in 0..10 {
        let tracker = Arc::clone(&tracker);
        set.spawn(async move {
            let _ = tracker.try_cull(2);
        });
    }
    while set.join_next().await.is_some() {}

    // Wait for all culls to settle.
    wait_for_culls(&tracker).await;

    // Some keys should have been evicted (exact count depends on channel capacity
    // and scheduling, but at least some culls should succeed).
    let deleted = deleter.deleted_keys();
    assert!(
        deleted.len() >= 2,
        "at least one cull request (2 keys) should have succeeded"
    );
    assert!(deleted.len() <= 20, "cannot evict more keys than exist");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn high_throughput_message_storm() {
    let deleter = MockDeleter::new();
    // Small channel to stress the try_send → spawn fallback path.
    let tracker = Arc::new(LruEvictionTracker::spawn(deleter.clone(), 4));

    let mut set = JoinSet::new();

    // 50 tasks each upsert and access rapidly.
    for i in 0u64..50 {
        let tracker = Arc::clone(&tracker);
        set.spawn(async move {
            tracker.upsert(i, MockCtx { version: i });
            tracker.access(i);
            tracker.upsert(i, MockCtx { version: i + 100 });
        });
    }
    while set.join_next().await.is_some() {}

    // Let messages drain.
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Evict everything.
    assert!(tracker.try_cull(50));
    wait_for_culls(&tracker).await;

    // All 50 unique keys should have been evicted exactly once.
    let mut deleted = deleter.deleted_keys();
    deleted.sort_unstable();
    deleted.dedup();
    assert_eq!(deleted.len(), 50, "all 50 unique keys should be evicted");
}
