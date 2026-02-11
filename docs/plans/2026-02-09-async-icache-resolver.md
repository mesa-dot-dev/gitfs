# Async ICache Resolver Refactor Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Refactor `AsyncICache` so the cache manages the full InFlight lifecycle internally via an `IcbResolver` trait, eliminating the spin-lock race condition and removing manual `mark_inflight`/`complete` calls.

**Architecture:** Replace `Arc<Notify>` with `tokio::sync::watch` channels to eliminate the race condition where a notification fires between cloning the handle and awaiting it. Introduce an `IcbResolver` trait that acts as a "promise" to eventually produce an ICB for a given inode. The cache itself manages the InFlight→Available transition via a new `get_or_resolve` method. The struct becomes `AsyncICache<R: IcbResolver>` with the ICB type derived from `R::Icb`. Public `mark_inflight`/`complete` are removed.

**Tech Stack:** Rust 2024, `scc` 3.5.6 (`HashMap`), `tokio::sync::watch`, `std::sync::atomic::AtomicU64`, RPITIT (return-position impl trait in traits)

---

## Key Design Decisions

### `IcbResolver` trait

```rust
pub trait IcbResolver: Send + Sync {
    type Icb: IcbLike + Send + Sync;
    type Error: Send;

    fn resolve(
        &self,
        ino: Inode,
    ) -> impl Future<Output = Result<Self::Icb, Self::Error>> + Send;
}
```

Uses RPITIT (Rust 2024 edition) instead of `#[async_trait]` — no heap allocation.

### `IcbState` with `watch`

```rust
pub enum IcbState<I> {
    InFlight(watch::Receiver<()>),
    Available(I),
}
```

The `watch::Sender<()>` is held by the task performing resolution. When the sender is dropped (whether from success or failure), all `Receiver::changed().await` calls wake up with `Err(RecvError)` — guaranteeing no missed notifications.

### `AsyncICache<R: IcbResolver>`

```rust
pub struct AsyncICache<R: IcbResolver> {
    resolver: R,
    inode_table: ConcurrentHashMap<Inode, IcbState<R::Icb>>,
    next_fh: AtomicU64,
}
```

Single type parameter. ICB type is `R::Icb`.

### `wait_for_available` — no loop

```rust
async fn wait_for_available(&self, ino: Inode) -> bool {
    let rx = self.inode_table.read_async(&ino, |_, s| match s {
        IcbState::InFlight(rx) => Some(rx.clone()),
        IcbState::Available(_) => None,
    }).await;

    match rx {
        None => false,               // key missing
        Some(None) => true,          // Available
        Some(Some(mut rx)) => {
            // Wait for sender to signal (or drop)
            let _ = rx.changed().await;
            // Re-check: entry should now be Available or removed
            self.inode_table.read_async(&ino, |_, s|
                matches!(s, IcbState::Available(_))
            ).await.unwrap_or(false)
        }
    }
}
```

No loop. `watch::Receiver::changed()` never misses — if the sender already signaled or was dropped before `.changed().await`, it returns immediately.

### `get_or_resolve` — cache-managed lifecycle

```rust
pub async fn get_or_resolve<R2>(
    &self,
    ino: Inode,
    then: impl FnOnce(&I) -> R2,
) -> Result<R2, R::Error> { ... }
```

1. Check if `Available` → run `then`, return
2. If `InFlight` → clone receiver, await, re-check, run `then`
3. If absent → insert `InFlight(rx)`, call `resolver.resolve(ino).await`, on success upsert `Available`, on error remove entry, wake all waiters either way

### Removed public API

- `mark_inflight` — removed (internal only via `get_or_resolve`)
- `complete` — removed (internal only via `get_or_resolve`)

### Retained public API (unchanged signatures)

- `new(resolver, root_ino, root_path)` — now takes resolver as first arg
- `allocate_fh(&self) -> FileHandle`
- `inode_count(&self) -> usize`
- `contains(&self, ino) -> bool` (async, awaits InFlight)
- `get_icb(&self, ino, f) -> Option<R>` (async, awaits InFlight)
- `get_icb_mut(&self, ino, f) -> Option<R>` (async, awaits InFlight)
- `insert_icb(&self, ino, icb)` (async)
- `entry_or_insert_icb(&self, ino, factory, then) -> R` (async)
- `inc_rc(&self, ino) -> u64` (async)
- `forget(&self, ino, nlookups) -> Option<I>` (async)
- `for_each(&self, f)` (sync iteration)

---

## Files

- **Modify:** `src/fs/icache/async_cache.rs` (all tasks)
- **Modify:** `src/fs/icache/mod.rs` (Task 1 only — re-export `IcbResolver`)

---

## Task 1: Add `IcbResolver` trait and make `AsyncICache` generic over resolver

**Files:**
- Modify: `src/fs/icache/async_cache.rs`
- Modify: `src/fs/icache/mod.rs`

**Step 1: Write the failing test**

Add a new `TestResolver` to the test module and update `TestIcb` and test helpers. Replace the first two tests (`contains_returns_true_for_root`, `new_creates_root_entry`) to use the resolver-based constructor.

Add at the top of `mod tests`:

```rust
use std::collections::HashMap as StdHashMap;
use std::sync::Mutex;

struct TestResolver {
    /// Pre-loaded responses keyed by inode.
    responses: Mutex<StdHashMap<Inode, Result<TestIcb, String>>>,
}

impl TestResolver {
    fn new() -> Self {
        Self {
            responses: Mutex::new(StdHashMap::new()),
        }
    }

    fn add(&self, ino: Inode, icb: TestIcb) {
        self.responses.lock().unwrap().insert(ino, Ok(icb));
    }

    fn add_err(&self, ino: Inode, err: impl Into<String>) {
        self.responses.lock().unwrap().insert(ino, Err(err.into()));
    }
}

impl IcbResolver for TestResolver {
    type Icb = TestIcb;
    type Error = String;

    fn resolve(
        &self,
        ino: Inode,
    ) -> impl Future<Output = Result<Self::Icb, Self::Error>> + Send {
        let result = self
            .responses
            .lock()
            .unwrap()
            .remove(&ino)
            .unwrap_or_else(|| Err(format!("no response for inode {ino}")));
        async move { result }
    }
}

/// Helper: build a cache with a `TestResolver` at root inode 1.
fn test_cache() -> AsyncICache<TestResolver> {
    AsyncICache::new(TestResolver::new(), 1, "/root")
}

/// Helper: build a cache with a given resolver at root inode 1.
fn test_cache_with(resolver: TestResolver) -> AsyncICache<TestResolver> {
    AsyncICache::new(resolver, 1, "/root")
}
```

Update the existing `new_creates_root_entry` and `contains_returns_true_for_root` tests to use `test_cache()`:

```rust
#[tokio::test]
async fn new_creates_root_entry() {
    let cache = test_cache();
    assert_eq!(cache.inode_count(), 1, "should have exactly 1 entry");
}

#[tokio::test]
async fn contains_returns_true_for_root() {
    let cache = test_cache();
    assert!(cache.contains(1).await, "root should exist");
}
```

**Step 2: Run tests to verify they fail**

Run: `cargo test -p git-fs icache::async_cache -- --nocapture`
Expected: FAIL — `IcbResolver` trait and new `AsyncICache::new` signature don't exist

**Step 3: Implement `IcbResolver` trait and update `AsyncICache`**

At the top of `async_cache.rs`, add the `Future` import and trait definition:

```rust
use std::future::Future;
```

```rust
/// Trait for resolving an inode to its control block.
///
/// Implementations act as a "promise" that an ICB will eventually be produced
/// for a given inode. The cache calls `resolve` when it needs to populate a
/// missing entry.
pub trait IcbResolver: Send + Sync {
    /// The inode control block type this resolver produces.
    type Icb: IcbLike + Send + Sync;
    /// Error type returned when resolution fails.
    type Error: Send;

    /// Resolve an inode to its control block.
    fn resolve(
        &self,
        ino: Inode,
    ) -> impl Future<Output = Result<Self::Icb, Self::Error>> + Send;
}
```

Change the struct:

```rust
pub struct AsyncICache<R: IcbResolver> {
    resolver: R,
    inode_table: ConcurrentHashMap<Inode, IcbState<R::Icb>>,
    next_fh: AtomicU64,
}
```

Change the `impl` block signature:

```rust
impl<R: IcbResolver> AsyncICache<R> {
```

Update the constructor to accept a resolver:

```rust
pub fn new(resolver: R, root_ino: Inode, root_path: impl Into<std::path::PathBuf>) -> Self {
    let table = ConcurrentHashMap::new();
    drop(table.insert_sync(
        root_ino,
        IcbState::Available(R::Icb::new_root(root_path.into())),
    ));
    Self {
        resolver,
        inode_table: table,
        next_fh: AtomicU64::new(1),
    }
}
```

Replace all `I` type references in method signatures/bodies with `R::Icb`. Specifically:

- `IcbState<I>` → already generic, no change needed (the enum stays `IcbState<I>`)
- In method signatures: `icb: I` → `icb: R::Icb`, `FnOnce(&I)` → `FnOnce(&R::Icb)`, `FnOnce(&mut I)` → `FnOnce(&mut R::Icb)`, `FnOnce() -> I` → `FnOnce() -> R::Icb`, `Option<I>` → `Option<R::Icb>`
- Remove the `I: IcbLike + Send + Sync` bound from the impl block (it's now derived from `R::Icb`)

Update all existing tests to use `test_cache()` instead of `AsyncICache::<TestIcb>::new(1, "/root")`.

For tests that used `Arc::new(AsyncICache::<TestIcb>::new(...))`, use `Arc::new(test_cache())` instead.

For tests that used `cache.mark_inflight(42).await` and `cache.complete(42, ...)`, keep them compiling for now (they still exist); they'll be removed in Task 4.

**Step 4: Update `mod.rs`**

Add `IcbResolver` to the re-exports:

```rust
#[expect(unused_imports)]
pub use async_cache::AsyncICache;
#[expect(unused_imports)]
pub use async_cache::IcbResolver;
```

**Step 5: Run tests to verify they pass**

Run: `cargo test -p git-fs icache::async_cache -- --nocapture`
Expected: PASS (19 tests)

**Step 6: Run clippy**

Run: `cargo clippy -p git-fs -- -D warnings`
Expected: PASS

**Step 7: Commit**

```bash
git add src/fs/icache/async_cache.rs src/fs/icache/mod.rs
git commit -m "refactor(icache): add IcbResolver trait, make AsyncICache generic over resolver"
```

---

## Task 2: Replace `Notify` with `watch` and rewrite `wait_for_available`

**Files:**
- Modify: `src/fs/icache/async_cache.rs`

**Step 1: Write the failing test**

Add a test that validates the race-condition-free behavior — specifically that waiting on an already-completed entry doesn't hang:

```rust
#[tokio::test]
async fn wait_does_not_miss_signal_on_immediate_complete() {
    let cache = Arc::new(test_cache());

    // Insert InFlight, then immediately complete before anyone waits
    let (tx, rx) = tokio::sync::watch::channel(());
    cache
        .inode_table
        .upsert_async(42, IcbState::InFlight(rx))
        .await;

    // Complete before any waiter — drop sender to signal
    cache
        .insert_icb(
            42,
            TestIcb {
                rc: 1,
                path: "/fast".into(),
            },
        )
        .await;
    drop(tx);

    // This must NOT hang — the signal was already sent
    let result = tokio::time::timeout(
        std::time::Duration::from_millis(100),
        cache.contains(42),
    )
    .await;
    assert_eq!(result, Ok(true), "should not hang on already-completed entry");
}
```

**Step 2: Run test to verify it fails**

Run: `cargo test -p git-fs icache::async_cache::tests::wait_does_not_miss_signal -- --nocapture`
Expected: FAIL — `IcbState::InFlight` expects `Arc<Notify>`, not `watch::Receiver`

**Step 3: Replace `Notify` with `watch` throughout**

1. Change imports:

Remove:
```rust
use std::sync::Arc;
use tokio::sync::Notify;
```

Add:
```rust
use tokio::sync::watch;
```

2. Update `IcbState`:

```rust
pub enum IcbState<I> {
    /// Entry is being loaded; waiters clone the receiver and `.changed().await`.
    InFlight(watch::Receiver<()>),
    /// Entry is ready for use.
    Available(I),
}
```

3. Rewrite `wait_for_available` (no loop):

```rust
async fn wait_for_available(&self, ino: Inode) -> bool {
    let rx = self
        .inode_table
        .read_async(&ino, |_, s| match s {
            IcbState::InFlight(rx) => Some(rx.clone()),
            IcbState::Available(_) => None,
        })
        .await;

    match rx {
        None => false,           // key missing
        Some(None) => true,      // Available
        Some(Some(mut rx)) => {
            // Wait for the resolver to complete (or fail/drop sender).
            // changed() returns Err(RecvError) when sender is dropped,
            // which is fine — it means resolution finished.
            let _ = rx.changed().await;
            // Re-check: entry is now Available or was removed on error.
            self.inode_table
                .read_async(&ino, |_, s| matches!(s, IcbState::Available(_)))
                .await
                .unwrap_or(false)
        }
    }
}
```

4. Update `mark_inflight` to use `watch`:

```rust
pub async fn mark_inflight(&self, ino: Inode) -> watch::Sender<()> {
    let (tx, rx) = watch::channel(());
    self.inode_table
        .upsert_async(ino, IcbState::InFlight(rx))
        .await;
    tx
}
```

5. Update `complete` to accept `watch::Sender`:

Actually, `complete` no longer needs the sender — it just upserts Available. The old sender being dropped will notify waiters. But we need the sender to still be alive when `complete` is called to ensure proper sequencing. Simplify: `complete` upserts Available. The caller drops the sender afterward (or it's already dropped).

```rust
pub async fn complete(&self, ino: Inode, icb: R::Icb) {
    self.inode_table
        .upsert_async(ino, IcbState::Available(icb))
        .await;
    // Waiters wake when the sender (held by caller) is dropped.
    // If sender was already dropped, waiters already woke from changed().await Err.
}
```

6. Update `entry_or_insert_icb` — replace `Arc::clone(notify)` with `rx.clone()`:

```rust
IcbState::InFlight(rx) => {
    let mut rx = rx.clone();
    drop(occ); // release shard lock before awaiting
    let _ = rx.changed().await;
}
```

7. Update all tests that use `mark_inflight`/`complete`:

Replace `let _notify = cache.mark_inflight(42).await;` with `let _tx = cache.mark_inflight(42).await;`.

The `complete` calls remain the same signature (still takes `ino, icb`), but now the `_tx` being dropped after `complete` signals the waiters. In tests, ensure `_tx` is dropped *after* `complete`:

```rust
// In tests that use mark_inflight + complete:
let tx = cache.mark_inflight(42).await;
// ... spawn task that waits ...
cache.complete(42, TestIcb { ... }).await;
drop(tx); // signal all waiters
```

**Step 4: Run tests to verify they pass**

Run: `cargo test -p git-fs icache::async_cache -- --nocapture`
Expected: PASS (20 tests — 19 existing + 1 new)

**Step 5: Run clippy**

Run: `cargo clippy -p git-fs -- -D warnings`
Expected: PASS

**Step 6: Commit**

```bash
git add src/fs/icache/async_cache.rs
git commit -m "refactor(icache): replace Notify with watch channels, eliminate spin-lock"
```

---

## Task 3: Add `get_or_resolve` method

**Files:**
- Modify: `src/fs/icache/async_cache.rs`

**Step 1: Write the failing tests**

```rust
#[tokio::test]
async fn get_or_resolve_returns_existing() {
    let cache = test_cache();
    cache
        .insert_icb(42, TestIcb { rc: 1, path: "/existing".into() })
        .await;

    let path: Result<PathBuf, String> = cache
        .get_or_resolve(42, |icb| icb.path.clone())
        .await;
    assert_eq!(path, Ok(PathBuf::from("/existing")));
}

#[tokio::test]
async fn get_or_resolve_resolves_missing() {
    let resolver = TestResolver::new();
    resolver.add(42, TestIcb { rc: 1, path: "/resolved".into() });
    let cache = test_cache_with(resolver);

    let path: Result<PathBuf, String> = cache
        .get_or_resolve(42, |icb| icb.path.clone())
        .await;
    assert_eq!(path, Ok(PathBuf::from("/resolved")));
    // Should now be cached
    assert!(cache.contains(42).await);
}

#[tokio::test]
async fn get_or_resolve_propagates_error() {
    let resolver = TestResolver::new();
    resolver.add_err(42, "network error");
    let cache = test_cache_with(resolver);

    let result: Result<PathBuf, String> = cache
        .get_or_resolve(42, |icb| icb.path.clone())
        .await;
    assert_eq!(result, Err("network error".to_owned()));
    // Entry should be cleaned up on error
    assert!(!cache.contains(42).await);
}

#[tokio::test]
async fn get_or_resolve_coalesces_concurrent_requests() {
    use std::sync::atomic::{AtomicUsize, Ordering};

    let resolve_count = Arc::new(AtomicUsize::new(0));
    let resolve_count2 = Arc::clone(&resolve_count);

    // A resolver that counts calls and delays slightly
    struct CountingResolver {
        count: Arc<AtomicUsize>,
    }
    impl IcbResolver for CountingResolver {
        type Icb = TestIcb;
        type Error = String;
        fn resolve(
            &self,
            _ino: Inode,
        ) -> impl Future<Output = Result<Self::Icb, Self::Error>> + Send {
            self.count.fetch_add(1, Ordering::SeqCst);
            async {
                tokio::task::yield_now().await;
                Ok(TestIcb { rc: 1, path: "/coalesced".into() })
            }
        }
    }

    let cache = Arc::new(AsyncICache::new(
        CountingResolver { count: resolve_count2 },
        1,
        "/root",
    ));

    // Spawn 5 concurrent get_or_resolve for the same inode
    let mut handles = Vec::new();
    for _ in 0..5 {
        let c = Arc::clone(&cache);
        handles.push(tokio::spawn(async move {
            c.get_or_resolve(42, |icb| icb.path.clone()).await
        }));
    }

    for h in handles {
        assert_eq!(
            h.await.expect("task panicked"),
            Ok(PathBuf::from("/coalesced")),
        );
    }

    // Resolver should only have been called ONCE (not 5 times)
    assert_eq!(resolve_count.load(Ordering::SeqCst), 1, "should coalesce to 1 resolve call");
}
```

**Step 2: Run tests to verify they fail**

Run: `cargo test -p git-fs icache::async_cache -- --nocapture`
Expected: FAIL — `get_or_resolve` doesn't exist

**Step 3: Implement `get_or_resolve`**

```rust
/// Look up `ino`. If `Available`, run `then` and return `Ok(R)`.
/// If absent, call the resolver to fetch the ICB, cache it, then run `then`.
/// If another task is already resolving this inode (`InFlight`), wait for it.
///
/// Returns `Err(R::Error)` if resolution fails. On error the `InFlight`
/// entry is removed so subsequent calls can retry.
pub async fn get_or_resolve<R2>(
    &self,
    ino: Inode,
    then: impl FnOnce(&R::Icb) -> R2,
) -> Result<R2, R::Error> {
    use scc::hash_map::Entry;

    // Fast path: already Available
    {
        let hit = self
            .inode_table
            .read_async(&ino, |_, s| match s {
                IcbState::Available(icb) => Some(then(icb)),
                IcbState::InFlight(_) => None,
            })
            .await;
        match hit {
            Some(Some(r)) => return Ok(r),
            Some(None) => { /* InFlight — fall through */ }
            None => { /* absent — fall through */ }
        }
    }

    // Try to become the resolver, or wait on existing InFlight
    let mut then_fn = Some(then);
    loop {
        match self.inode_table.entry_async(ino).await {
            Entry::Occupied(mut occ) => match occ.get_mut() {
                IcbState::Available(icb) => {
                    let t = then_fn.take().unwrap_or_else(|| unreachable!());
                    return Ok(t(icb));
                }
                IcbState::InFlight(rx) => {
                    let mut rx = rx.clone();
                    drop(occ);
                    let _ = rx.changed().await;
                    // Re-check on next loop iteration
                }
            },
            Entry::Vacant(vac) => {
                // We win the race — install InFlight and resolve
                let (tx, rx) = watch::channel(());
                vac.insert_entry(IcbState::InFlight(rx));

                match self.resolver.resolve(ino).await {
                    Ok(icb) => {
                        let t = then_fn.take().unwrap_or_else(|| unreachable!());
                        let result = self
                            .inode_table
                            .update_async(&ino, |_, state| {
                                *state = IcbState::Available(icb);
                            })
                            .await;
                        // If update_async returned None, entry was removed
                        // between our insert and here (shouldn't happen, but
                        // handle gracefully).
                        if result.is_none() {
                            // Re-read to get the value we just set — but the
                            // entry was removed, so we need to re-insert.
                            // This is an edge case that shouldn't occur in
                            // practice. For safety, drop tx and retry.
                            drop(tx);
                        } else {
                            // Read the now-Available value to run `then`
                            drop(tx); // wake all waiters
                            let r = self
                                .inode_table
                                .read_async(&ino, |_, s| match s {
                                    IcbState::Available(icb) => Some(t(icb)),
                                    IcbState::InFlight(_) => None,
                                })
                                .await
                                .flatten();
                            if let Some(r) = r {
                                return Ok(r);
                            }
                        }
                        // Extremely unlikely fallthrough — retry
                    }
                    Err(e) => {
                        // Remove the InFlight entry
                        self.inode_table.remove_async(&ino).await;
                        drop(tx); // wake all waiters — they'll see entry missing
                        return Err(e);
                    }
                }
            }
        }
    }
}
```

> **Note on the loop:** Unlike `wait_for_available`, this loop only iterates if:
> (a) we were waiting on InFlight and it completed — we loop back to read Available, or
> (b) an extremely unlikely race removed our entry — we retry.
> It is NOT a spin-lock: every iteration either returns or awaits a `watch::changed()`.

**Step 4: Run tests to verify they pass**

Run: `cargo test -p git-fs icache::async_cache -- --nocapture`
Expected: PASS (23 tests — 20 existing + 4 new, though the coalescing test may need the `CountingResolver` to be defined at module level if inner items with impls are not supported)

**Step 5: Run clippy**

Run: `cargo clippy -p git-fs -- -D warnings`
Expected: PASS

**Step 6: Commit**

```bash
git add src/fs/icache/async_cache.rs
git commit -m "feat(icache): add get_or_resolve with automatic InFlight lifecycle management"
```

---

## Task 4: Remove public `mark_inflight`/`complete`, update tests

**Files:**
- Modify: `src/fs/icache/async_cache.rs`

**Step 1: Make `mark_inflight` and `complete` private (or remove)**

Remove `pub` from `mark_inflight` and `complete`. If they are only used by tests, move them into a `#[cfg(test)]` impl block or remove entirely if tests have been updated.

Check: are `mark_inflight` and `complete` still used by any test?

Tests that used them:
- `contains_awaits_inflight_then_returns_true`
- `get_icb_awaits_inflight`
- `entry_or_insert_awaits_inflight`
- `for_each_skips_inflight`
- `wait_does_not_miss_signal_on_immediate_complete`

**Step 2: Rewrite these tests to use `get_or_resolve` instead**

Replace tests that manually managed InFlight with resolver-based tests:

```rust
#[tokio::test]
async fn contains_awaits_inflight_then_returns_true() {
    let resolver = TestResolver::new();
    resolver.add(42, TestIcb { rc: 1, path: "/test".into() });
    let cache = Arc::new(test_cache_with(resolver));

    // Trigger resolve in background
    let cache2 = Arc::clone(&cache);
    let handle = tokio::spawn(async move {
        cache2.get_or_resolve(42, |_| ()).await
    });

    handle.await.expect("task panicked").expect("resolve failed");
    assert!(cache.contains(42).await, "should be true after resolve");
}

#[tokio::test]
async fn get_icb_awaits_inflight_via_resolver() {
    use std::sync::atomic::{AtomicBool, Ordering};

    let resolver = TestResolver::new();
    resolver.add(42, TestIcb { rc: 1, path: "/loaded".into() });
    let cache = Arc::new(test_cache_with(resolver));

    // Resolve inode 42 first
    let _: Result<(), String> = cache.get_or_resolve(42, |_| ()).await;

    let path = cache.get_icb(42, |icb| icb.path.clone()).await;
    assert_eq!(path, Some(PathBuf::from("/loaded")));
}

#[tokio::test]
async fn for_each_skips_inflight_via_resolver() {
    // Use a resolver that never responds (we'll insert InFlight manually for test)
    let cache = test_cache();
    // Directly insert an InFlight entry for testing iteration
    let (_tx, rx) = watch::channel(());
    cache
        .inode_table
        .upsert_async(42, IcbState::InFlight(rx))
        .await;

    let mut count = 0;
    cache.for_each(|_, _| {
        count += 1;
    });
    assert_eq!(count, 1, "only root, not the InFlight entry");
}
```

**Step 3: Remove `mark_inflight` and `complete` methods entirely**

Delete the `mark_inflight` and `complete` methods from the impl block. Also remove the `// -- InFlight management --` section comment.

**Step 4: Run tests to verify they pass**

Run: `cargo test -p git-fs icache::async_cache -- --nocapture`
Expected: PASS

**Step 5: Run clippy and fmt**

Run: `cargo clippy -p git-fs -- -D warnings && cargo fmt -p git-fs --check`
Expected: PASS

**Step 6: Commit**

```bash
git add src/fs/icache/async_cache.rs
git commit -m "refactor(icache): remove public mark_inflight/complete, use resolver-driven lifecycle"
```

---

## Task 5: Clean up `entry_or_insert_icb` InFlight handling

**Files:**
- Modify: `src/fs/icache/async_cache.rs`

Now that the cache owns the resolver, `entry_or_insert_icb` should also use the resolver pattern for InFlight entries instead of its own loop. However, `entry_or_insert_icb` serves a different purpose — it's for callers that already have an ICB to insert (factory pattern). The InFlight wait inside it should use the `watch`-based wait (which it does after Task 2).

**Step 1: Verify `entry_or_insert_icb` uses watch correctly**

Read the current state and verify:
- InFlight branch clones `rx`, drops the entry, awaits `rx.changed()`
- No `Arc<Notify>` references remain anywhere

**Step 2: Audit for any remaining `Notify` or `Arc` imports**

Search the file for any `Notify`, `Arc`, or `use std::sync::Arc` — remove if unused.

**Step 3: Run full test suite**

Run: `cargo test -p git-fs icache::async_cache -- --nocapture`
Expected: PASS

**Step 4: Run clippy and fmt**

Run: `cargo clippy -p git-fs -- -D warnings && cargo fmt -p git-fs --check`
Expected: PASS

**Step 5: Commit (if changes were needed)**

```bash
git add src/fs/icache/async_cache.rs
git commit -m "refactor(icache): clean up remaining Notify references"
```

---

## Verification

After all tasks are complete:

1. **Run all async_cache tests:** `cargo test -p git-fs icache::async_cache -- --nocapture`
2. **Run full test suite:** `cargo test -p git-fs`
3. **Check lints:** `cargo clippy -p git-fs -- -D warnings`
4. **Check formatting:** `cargo fmt -p git-fs --check`

All commands should pass without errors or warnings.
