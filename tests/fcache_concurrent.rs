#![allow(clippy::unwrap_used, clippy::expect_used, missing_docs)]

use std::sync::Arc;

use git_fs::cache::fcache::FileCache;
use git_fs::cache::traits::{AsyncReadableCache as _, AsyncWritableCache as _};
use tokio::task::JoinSet;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_inserts_different_keys() {
    let tmp = tempfile::tempdir().unwrap();
    let cache = Arc::new(
        FileCache::<u64>::new(tmp.path(), 1024 * 1024)
            .await
            .unwrap(),
    );

    let mut set = JoinSet::new();
    for i in 0u64..100 {
        let cache = Arc::clone(&cache);
        set.spawn(async move {
            cache
                .insert(&i, format!("value-{i}").into_bytes())
                .await
                .unwrap();
        });
    }
    while set.join_next().await.is_some() {}

    // Every key should be present with the correct value.
    for i in 0u64..100 {
        let val = cache.get(&i).await;
        assert_eq!(
            val.as_deref(),
            Some(format!("value-{i}").as_bytes()),
            "key {i} missing or has wrong value"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_inserts_same_key() {
    let tmp = tempfile::tempdir().unwrap();
    let cache = Arc::new(
        FileCache::<u64>::new(tmp.path(), 1024 * 1024)
            .await
            .unwrap(),
    );

    let mut set = JoinSet::new();
    for i in 0u64..50 {
        let cache = Arc::clone(&cache);
        set.spawn(async move {
            cache
                .insert(&0, format!("value-{i}").into_bytes())
                .await
                .unwrap();
        });
    }
    while set.join_next().await.is_some() {}

    // The final value must be one of the inserted values (last writer wins).
    let val = cache.get(&0).await;
    assert!(val.is_some(), "key 0 should still be present");

    let val_str = String::from_utf8(val.unwrap()).unwrap();
    assert!(
        val_str.starts_with("value-"),
        "value should be one of the inserted values, got: {val_str}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_reads_during_writes() {
    let tmp = tempfile::tempdir().unwrap();
    let cache = Arc::new(
        FileCache::<u64>::new(tmp.path(), 1024 * 1024)
            .await
            .unwrap(),
    );

    // Pre-populate with initial value.
    cache.insert(&0, b"initial".to_vec()).await.unwrap();

    let mut set = JoinSet::new();

    // Spawn 20 writers overwriting key 0.
    for i in 0u64..20 {
        let cache = Arc::clone(&cache);
        set.spawn(async move {
            cache
                .insert(&0, format!("write-{i}").into_bytes())
                .await
                .unwrap();
        });
    }

    // Spawn 20 readers -- each get() should return None or valid UTF-8.
    for _ in 0..20 {
        let cache = Arc::clone(&cache);
        set.spawn(async move {
            if let Some(val) = cache.get(&0).await {
                String::from_utf8(val)
                    .expect("concurrent read returned corrupted (non-UTF-8) data");
            }
        });
    }

    while set.join_next().await.is_some() {}
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_inserts_with_eviction() {
    // Small cache: 200 bytes. Sequential warmup fills the LRU worker's map,
    // then concurrent inserts trigger eviction.
    let tmp = tempfile::tempdir().unwrap();
    let cache = Arc::new(FileCache::<u64>::new(tmp.path(), 200).await.unwrap());

    // Warmup: insert 10 entries sequentially so the LRU worker registers them
    // in its ordered map. This ensures eviction has candidates to evict.
    for i in 0u64..10 {
        cache.insert(&i, vec![b'x'; 20]).await.unwrap();
    }
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    // Concurrent phase: 40 more inserts push the cache well over the 200-byte
    // limit and trigger eviction against the warmed-up LRU map.
    let mut set = JoinSet::new();
    for i in 10u64..50 {
        let cache = Arc::clone(&cache);
        set.spawn(async move {
            cache.insert(&i, vec![b'x'; 20]).await.unwrap();
        });
    }
    while set.join_next().await.is_some() {}

    // Give eviction time to settle.
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    // Not all keys can fit (50 x 20 = 1000 > 200). Some must have been evicted.
    let mut present = 0usize;
    for i in 0u64..50 {
        if cache.get(&i).await.is_some() {
            present += 1;
        }
    }

    assert!(
        present < 50,
        "expected some evictions under pressure, but all 50 entries survived"
    );
    assert!(present > 0, "at least some entries should still be present");
}
