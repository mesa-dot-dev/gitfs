#![allow(clippy::unwrap_used, missing_docs)]

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use tokio::sync::oneshot;

use mesafs::cache::async_backed::FutureBackedCache;

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

/// With 3+ joiners the dedup property becomes observable: under the old
/// broken code each joiner would run its own factory after the owner fails
/// (4 total calls for 1 owner + 3 joiners). With the loop-based retry only
/// one joiner wins the `Vacant` race, so we expect exactly 2 calls
/// (A's fail + one winner's success).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn try_init_retry_after_joined_failure_deduplicates() {
    let cache = Arc::new(FutureBackedCache::<u64, String>::default());
    let call_count = Arc::new(AtomicUsize::new(0));

    // Channel to control timing of Task A's factory.
    let (release_tx, release_rx) = oneshot::channel::<()>();

    // Task A: starts a failing InFlight, held until we release.
    let cache_a = Arc::clone(&cache);
    let count_a = Arc::clone(&call_count);
    let task_a = tokio::spawn(async move {
        let result: Result<String, String> = cache_a
            .get_or_try_init(1, || {
                count_a.fetch_add(1, Ordering::Relaxed);
                async move {
                    let _ = release_rx.await;
                    Err("task_a_fail".to_owned())
                }
            })
            .await;
        result
    });

    // Give Task A time to register the InFlight slot.
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    // Spawn 3 joiners that all join A's InFlight. After A fails, exactly
    // one should win the Vacant race and run its factory; the others join
    // the new InFlight.
    let mut joiner_handles = Vec::new();
    for _ in 0..3 {
        let cache_j = Arc::clone(&cache);
        let count_j = Arc::clone(&call_count);
        joiner_handles.push(tokio::spawn(async move {
            let result: Result<String, String> = cache_j
                .get_or_try_init(1, || {
                    count_j.fetch_add(1, Ordering::Relaxed);
                    async move { Ok("joiner_ok".to_owned()) }
                })
                .await;
            result
        }));
    }

    // Give joiners time to join the InFlight.
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    // Release A's factory → it fails.
    release_tx.send(()).unwrap();

    let result_a = task_a.await.unwrap();
    assert!(result_a.is_err(), "task A should fail");

    for handle in joiner_handles {
        let result = handle.await.unwrap();
        assert_eq!(result.unwrap(), "joiner_ok", "every joiner should succeed");
    }

    // Factory should have been called exactly 2 times: A's fail + one
    // joiner winning the Vacant race. The other 2 joiners piggyback on
    // the winner's InFlight via Shared, so their factories are never called.
    assert_eq!(
        call_count.load(Ordering::Relaxed),
        2,
        "factory should be called exactly twice (A's fail + one joiner's success), \
         not 4 (which would indicate each joiner ran its own factory)"
    );
}
