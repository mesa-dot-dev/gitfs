#![allow(clippy::unwrap_used, missing_docs)]

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use git_fs::cache::async_backed::FutureBackedCache;

#[tokio::test]
async fn try_init_ok_caches_value() {
    let cache = FutureBackedCache::<u64, String>::default();
    let result: Result<String, &str> = cache
        .get_or_try_init(1, || async { Ok("hello".to_owned()) })
        .await;
    assert_eq!(result.unwrap(), "hello", "should return Ok value");

    // Value should now be cached (get returns it without factory)
    let cached = cache.get(&1).await;
    assert_eq!(cached.unwrap(), "hello", "value should be in cache");
}

#[tokio::test]
async fn try_init_err_does_not_cache() {
    let cache = FutureBackedCache::<u64, String>::default();
    let result: Result<String, &str> = cache.get_or_try_init(1, || async { Err("boom") }).await;
    assert_eq!(result.unwrap_err(), "boom", "should return the error");

    // Cache should be empty — error was not stored
    assert!(cache.is_empty(), "cache should have no entries after error");
    assert!(cache.get(&1).await.is_none(), "key should not exist");
}

#[tokio::test]
async fn try_init_err_then_retry_ok() {
    let cache = FutureBackedCache::<u64, String>::default();

    // First call: factory fails
    let r1: Result<String, &str> = cache.get_or_try_init(1, || async { Err("fail") }).await;
    assert!(r1.is_err(), "first call should fail");

    // Second call: factory succeeds
    let r2: Result<String, &str> = cache
        .get_or_try_init(1, || async { Ok("recovered".to_owned()) })
        .await;
    assert_eq!(r2.unwrap(), "recovered", "retry should succeed");

    // Value should now be cached
    let cached = cache.get(&1).await;
    assert_eq!(cached.unwrap(), "recovered");
}

#[tokio::test]
async fn try_init_returns_value_cached_by_init() {
    let cache = FutureBackedCache::<u64, String>::default();

    // Populate via infallible get_or_init
    cache
        .get_or_init(1, || async { "from_init".to_owned() })
        .await;

    // get_or_try_init should return the cached value without running factory
    let result: Result<String, &str> = cache
        .get_or_try_init(1, || async { panic!("factory should not run") })
        .await;
    assert_eq!(result.unwrap(), "from_init");
}

#[tokio::test]
async fn panic_in_factory_is_recovered() {
    let cache = Arc::new(FutureBackedCache::<u64, String>::default());
    let call_count = Arc::new(AtomicUsize::new(0));

    // Spawn a task whose factory panics. tokio::spawn catches the panic.
    let cache2 = Arc::clone(&cache);
    let call_count2 = Arc::clone(&call_count);
    let handle = tokio::spawn(async move {
        cache2
            .get_or_init(1, || {
                call_count2.fetch_add(1, Ordering::Relaxed);
                async { panic!("boom") }
            })
            .await
    });
    // The spawned task panics internally; JoinHandle returns Err.
    assert!(handle.await.is_err(), "task should have panicked");

    // The key should NOT be permanently bricked. A new caller should succeed.
    let v = cache
        .get_or_init(1, || {
            call_count.fetch_add(1, Ordering::Relaxed);
            async { "recovered".to_owned() }
        })
        .await;
    assert_eq!(v, "recovered", "should recover after panic");
    assert_eq!(
        call_count.load(Ordering::Relaxed),
        2,
        "factory called twice"
    );
}
