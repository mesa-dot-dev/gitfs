#![allow(clippy::unwrap_used, clippy::expect_used, missing_docs)]

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use git_fs::drop_ward::{DropWard, StatelessDrop};

/// Context that tracks how many times `delete` has been called.
struct DeleteCounter {
    call_count: AtomicUsize,
}

/// Tag type whose `delete` panics on the first call and succeeds on all
/// subsequent calls. Simulates a transient failure in cleanup.
struct PanicOnFirstDelete;

impl StatelessDrop<Arc<DeleteCounter>, u64> for PanicOnFirstDelete {
    fn delete(ctx: &Arc<DeleteCounter>, _key: &u64) {
        let prev = ctx.call_count.fetch_add(1, Ordering::SeqCst);
        assert!(prev != 0, "transient delete failure");
    }
}

/// When `dec_count(key, N)` triggers a panicking `delete` (N > 1), a
/// subsequent `dec(key, 1)` must still reach count 0 and retry the
/// cleanup — not silently leave the entry stuck at count N−1 forever.
#[test]
fn dec_count_panic_allows_retry_on_subsequent_dec() {
    let ctx = Arc::new(DeleteCounter {
        call_count: AtomicUsize::new(0),
    });
    let mut ward: DropWard<Arc<DeleteCounter>, u64, PanicOnFirstDelete> =
        DropWard::new(Arc::clone(&ctx));

    ward.inc(42);
    ward.inc(42);
    ward.inc(42); // count = 3

    // dec_count(42, 3) should trigger delete, which panics on first call.
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| ward.dec_count(&42, 3)));
    assert!(result.is_err(), "first delete should panic");
    assert_eq!(
        ctx.call_count.load(Ordering::SeqCst),
        1,
        "delete should have been called once (and panicked)"
    );

    // Now a single dec(42, 1) should reach zero and retry delete successfully.
    // BUG before fix: count stays at 3 (never updated), dec gives 2, no retry.
    let remaining = ward.dec(&42);
    assert_eq!(
        remaining,
        Some(0),
        "retry dec should reach zero and call delete again"
    );
    assert_eq!(
        ctx.call_count.load(Ordering::SeqCst),
        2,
        "delete should have been called a second time (successful retry)"
    );
}
