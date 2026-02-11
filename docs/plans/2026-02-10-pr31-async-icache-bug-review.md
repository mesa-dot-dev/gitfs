# PR #31 Async ICache Bug Review

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Fix bugs identified in the async icache PR before merge.

**Architecture:** The PR replaces a synchronous `HashMap`-based `ICache` with `AsyncICache` built on `scc::HashMap` + `tokio::sync::watch` channels, introduces `CompositeFs` for shared FUSE delegation, and adds `IcbResolver` trait for async inode resolution.

**Tech Stack:** Rust, tokio, scc::HashMap, FUSE (fuser crate)

---

## Summary of Findings

| # | Severity | Location | Description |
|---|----------|----------|-------------|
| 1 | Critical | `async_cache.rs:289-322` | `upsert_async` after lock drop can resurrect evicted entries |
| 2 | High | `async_cache.rs:133-160` | TOCTOU between `wait_for_available` and read in `get_icb`/`get_icb_mut` |
| 3 | High | `async_cache.rs:96-120` | `wait_for_available` doesn't loop on InFlight→Available→InFlight |
| 4 | High | `repo.rs:58` | `RepoResolver::resolve` panics on missing stub instead of returning error |
| 5 | Medium | `mod.rs:141-153`, `org.rs:200-215` | `inode_role` falls back to Root in release builds, hiding misrouted ops |
| 6 | Medium | `org.rs:126-144`, `mod.rs:179-196` | `ensure_owner_inode`/`ensure_org_inode` attr-missing path doesn't verify ICB exists |
| 7 | Medium | `async_cache.rs:444-451` | `for_each` uses `iter_sync` from async context (scc docs warn against this) |
| 8 | Medium | `Cargo.toml:47` | `reqwest-blocking-client` feature may deadlock in async context |
| 9 | Low | `repo.rs:268` | Redundant `cache_attr` after `get_or_resolve` in `RepoFs::lookup` |
| 10 | Low | `composite.rs:220-222` | `delegated_forget` iterates ALL slots instead of targeted removal |
| 11 | Low | `mod.rs:3` | Commented-out `local` module left behind |

Notes on findings NOT included above:
- `ensure_child_ino` TOCTOU (duplicate inodes) — currently safe due to `&mut self` serialization on `Fs` trait. Worth a comment but not a live bug.
- `evict_zero_rc_children` non-atomic scan-then-forget — safe because `nlookups=0` makes the forget conditional on rc still being 0.
- `unreachable!()` in `forward_or_insert_inode` closures — safe due to `&mut self`, same reasoning.
- `delegated_forget` unconditionally propagating to inner FS — pre-existing design, correct by FUSE protocol invariant (inner/outer rc move in lockstep).
- `needs_resolve()` has no TTL for mutable refs — design concern for future, not a bug for fixed-ref mounts.
- `ensure_child_ino` O(n) scans — performance concern, not a correctness bug.
- `insert_icb` infinite loop on repeated resolution failures — liveness concern, unlikely in practice.

---

### Task 1: Fix Critical — `upsert_async` resurrects evicted entries

**Files:**
- Modify: `src/fs/icache/async_cache.rs:289-322` (stub resolution path)
- Modify: `src/fs/icache/async_cache.rs:330-349` (vacant path)

**Problem:** In `get_or_resolve`, the code acquires the entry lock via `entry_async`, replaces `Available(stub)` with `InFlight(rx)`, then **drops the lock** before calling the resolver. After resolution, it writes back with `upsert_async`. Between the drop and the upsert, a concurrent `forget()` can evict the entry. `upsert_async` then **re-inserts** a dead inode — one the kernel has already forgotten. This is a reference count leak that persists until unmount.

The same bug exists on the error path (lines 311-319): if `fallback.rc() > 0`, `upsert_async` restores the stub, but the entry may have been evicted during resolution.

**Step 1: Write the failing test**

Add to the test module in `async_cache.rs`:

```rust
#[tokio::test]
async fn get_or_resolve_does_not_resurrect_evicted_entry() {
    // Resolver that takes long enough for a concurrent forget to run
    struct SlowResolver;
    impl IcbResolver for SlowResolver {
        type Icb = StubIcb;
        async fn resolve(
            &self,
            _ino: Inode,
            stub: Option<Self::Icb>,
            _cache: &AsyncICache<Self>,
        ) -> Result<Self::Icb, LookupError> {
            tokio::time::sleep(Duration::from_millis(50)).await;
            Ok(StubIcb::new_resolved())
        }
    }

    let cache = AsyncICache::new(SlowResolver, 1);
    // Insert a stub that needs resolution, with rc=1
    cache.insert_icb(2, StubIcb::new_stub()).await;
    cache.inc_rc(2);

    let resolve_handle = tokio::spawn({
        let cache_ref = &cache; // won't work directly — use Arc
        async move {
            cache_ref.get_or_resolve(2, |icb| icb.clone()).await
        }
    });

    // Wait for resolution to start, then forget
    tokio::time::sleep(Duration::from_millis(10)).await;
    cache.forget(2, 1).await; // rc drops to 0, entry evicted

    // Resolution completes — should NOT resurrect the entry
    let _ = resolve_handle.await;
    assert!(!cache.contains(2), "evicted entry was resurrected");
}
```

Note: this test will need to be adapted to the actual test infrastructure (Arc wrapping, proper StubIcb types). The key invariant being tested is: if an entry is evicted during resolution, the resolved value must not be re-inserted.

**Step 2: Run test to verify it fails**

Run: `cargo test --quiet -p git-fs get_or_resolve_does_not_resurrect`
Expected: FAIL — entry is resurrected because `upsert_async` unconditionally inserts.

**Step 3: Implement the fix**

Replace `upsert_async` with `entry_async` + conditional insert:

```rust
// After resolver returns Ok(icb):
// Instead of:
//   self.inode_table.upsert_async(ino, IcbState::Available(icb)).await;
//
// Use:
match self.inode_table.entry_async(ino).await {
    Entry::Occupied(mut occ) => {
        // Entry still exists (InFlight from our resolution) — update it
        *occ.get_mut() = IcbState::Available(icb);
    }
    Entry::Vacant(_) => {
        // Entry was evicted during resolution — do NOT resurrect
        // The kernel has already forgotten this inode.
        tracing::debug!(ino, "resolved inode was evicted during resolution, dropping result");
    }
}
```

Apply the same pattern to:
1. The error fallback path (lines 311-319) where `upsert_async` restores the stub
2. The vacant path (lines 340-345) where `upsert_async` stores the first resolution

**Step 4: Run test to verify it passes**

Run: `cargo test --quiet -p git-fs get_or_resolve_does_not_resurrect`
Expected: PASS

**Step 5: Run full test suite**

Run: `cargo fmt --all && cargo clippy --all-targets --all-features -- -D warnings && cargo test --quiet`
Expected: All pass

**Step 6: Commit**

```bash
git add src/fs/icache/async_cache.rs
git commit -m "fix(icache): prevent upsert_async from resurrecting evicted entries"
```

---

### Task 2: Fix High — `wait_for_available` should loop on re-encountering InFlight

**Files:**
- Modify: `src/fs/icache/async_cache.rs:96-120`

**Problem:** `wait_for_available` waits once on the watch channel. After waking, it re-reads the entry. If the entry transitioned InFlight→Available→InFlight (another resolution cycle started), the re-read finds InFlight and returns `false` — callers interpret this as "inode does not exist" when it actually does.

**Step 1: Write the failing test**

```rust
#[tokio::test]
async fn wait_for_available_retries_on_re_inflight() {
    // A resolver that resolves quickly with a stub that still needs_resolve,
    // causing a second InFlight cycle when get_or_resolve is called again
    // ... (test setup that causes InFlight→Available(stub)→InFlight→Available(resolved))
    // Assert that get_icb returns Some, not None.
}
```

**Step 2: Run test to verify it fails**

Run: `cargo test --quiet -p git-fs wait_for_available_retries`
Expected: FAIL

**Step 3: Implement the fix**

Wrap `wait_for_available` in a loop:

```rust
async fn wait_for_available(&self, ino: Inode) -> bool {
    loop {
        let rx = self
            .inode_table
            .read_async(&ino, |_, s| match s {
                IcbState::InFlight(rx) => Some(rx.clone()),
                IcbState::Available(_) => None,
            })
            .await;

        match rx {
            None => return false,        // key missing
            Some(None) => return true,   // Available
            Some(Some(mut rx)) => {
                // Wait for this InFlight to resolve
                let _ = rx.changed().await;
                // Loop back to re-check — entry might be InFlight again
                // from a new resolution cycle, or might be removed
                continue;
            }
        }
    }
}
```

Also update `get_icb` and `get_icb_mut` to retry when they encounter InFlight after `wait_for_available`:

```rust
pub async fn get_icb<T>(&self, ino: Inode, f: impl Fn(&R::Icb) -> T) -> Option<T> {
    loop {
        if !self.wait_for_available(ino).await {
            return None;
        }
        let result = self
            .inode_table
            .read_async(&ino, |_, state| match state {
                IcbState::Available(icb) => Some(f(icb)),
                IcbState::InFlight(_) => None, // retry
            })
            .await;
        match result {
            Some(Some(val)) => return Some(val),
            Some(None) => continue, // was InFlight, retry
            None => return None,     // key missing
        }
    }
}
```

**Step 4: Run test to verify it passes**

Run: `cargo test --quiet -p git-fs wait_for_available_retries`
Expected: PASS

**Step 5: Run full test suite**

Run: `cargo fmt --all && cargo clippy --all-targets --all-features -- -D warnings && cargo test --quiet`
Expected: All pass

**Step 6: Commit**

```bash
git add src/fs/icache/async_cache.rs
git commit -m "fix(icache): loop in wait_for_available and get_icb on re-encountering InFlight"
```

---

### Task 3: Fix High — `RepoResolver::resolve` panics on missing stub

**Files:**
- Modify: `src/fs/mescloud/repo.rs:58`

**Problem:** `RepoResolver::resolve` uses `unreachable!()` when `stub` is `None`. If any code path ever calls `get_or_resolve` for an inode that was never inserted as a stub, the process panics. This should return an error, not crash.

**Step 1: Implement the fix**

```rust
// Replace:
let stub = stub.unwrap_or_else(|| unreachable!("RepoResolver requires a stub ICB"));

// With:
let stub = stub.ok_or(LookupError::InodeNotFound)?;
```

**Step 2: Run full test suite**

Run: `cargo fmt --all && cargo clippy --all-targets --all-features -- -D warnings && cargo test --quiet`
Expected: All pass

**Step 3: Commit**

```bash
git add src/fs/mescloud/repo.rs
git commit -m "fix(repo): return error instead of panicking on missing stub in RepoResolver"
```

---

### Task 4: Fix Medium — `inode_role` falls back to Root in release builds

**Files:**
- Modify: `src/fs/mescloud/mod.rs:141-153` (MesaFS)
- Modify: `src/fs/mescloud/org.rs:200-215` (OrgFs)

**Problem:** When an inode can't be classified, `debug_assert!` fires (stripped in release) and the code falls back to `InodeRole::Root`. In release builds, an unclassifiable inode silently gets the root role — `readdir` on it returns the top-level listing, `lookup` tries to match org/repo names. This can happen if the kernel caches an inode past the 1-second TTL and calls getattr/readdir after the cache has forgotten it.

**Step 1: Implement the fix**

Change `inode_role` to return `Option<InodeRole>`:

```rust
fn inode_role(&self, ino: Inode) -> Option<InodeRole> {
    if ino == Self::ROOT_NODE_INO {
        return Some(InodeRole::Root);
    }
    if self.composite.child_inodes.contains_key(&ino) {
        return Some(InodeRole::OrgOwned);
    }
    if self.composite.slot_for_inode(ino).is_some() {
        return Some(InodeRole::OrgOwned);
    }
    None
}
```

Update all callers to handle `None` by returning `ENOENT`:

```rust
let role = self.inode_role(ino).ok_or(LookupError::InodeNotFound)?;
```

**Step 2: Run full test suite**

Run: `cargo fmt --all && cargo clippy --all-targets --all-features -- -D warnings && cargo test --quiet`
Expected: All pass

**Step 3: Commit**

```bash
git add src/fs/mescloud/mod.rs src/fs/mescloud/org.rs
git commit -m "fix(mescloud): return ENOENT for unclassifiable inodes instead of falling back to Root"
```

---

### Task 5: Fix Medium — `ensure_owner_inode`/`ensure_org_inode` attr-missing path doesn't verify ICB exists

**Files:**
- Modify: `src/fs/mescloud/org.rs:126-144`
- Modify: `src/fs/mescloud/mod.rs:179-196`

**Problem:** When the inode exists in the tracking map (`owner_inodes`/`child_inodes`) but the attr is missing from the icache, the code rebuilds the attr and calls `cache_attr`. But if the ICB was evicted entirely, `cache_attr` (which calls `get_icb_mut`) returns `None` and silently does nothing. The caller receives a stale attr with an inode number that the icache doesn't track, leading to subsequent `getattr` failures.

**Step 1: Implement the fix**

When the attr is missing AND `cache_attr` effectively no-ops, clean up the stale tracking entry and fall through to the allocation path:

```rust
// In ensure_owner_inode:
for (&ino, existing_owner) in &self.owner_inodes {
    if existing_owner == owner {
        if let Some(attr) = self.composite.icache.get_attr(ino).await {
            return (ino, attr);
        }
        // ICB may have been evicted — check if it still exists
        if self.composite.icache.contains(ino) {
            let now = SystemTime::now();
            let attr = FileAttr::Directory { /* ... */ };
            self.composite.icache.cache_attr(ino, attr).await;
            return (ino, attr);
        }
        // ICB was evicted — fall through to allocate a new one
        break;
    }
}
// ... allocation path (also remove the stale entry from owner_inodes)
```

Note: the `for` loop borrows `self.owner_inodes` immutably, so the stale entry removal must happen after the loop. Use a separate `stale_ino` variable.

**Step 2: Run full test suite**

Run: `cargo fmt --all && cargo clippy --all-targets --all-features -- -D warnings && cargo test --quiet`
Expected: All pass

**Step 3: Commit**

```bash
git add src/fs/mescloud/org.rs src/fs/mescloud/mod.rs
git commit -m "fix(mescloud): handle evicted ICBs in ensure_owner_inode/ensure_org_inode"
```

---

### Task 6: Fix Medium — `for_each` uses `iter_sync` from async context

**Files:**
- Modify: `src/fs/icache/async_cache.rs:444-451`

**Problem:** The `for_each` method uses `scc::HashMap::scan` (synchronous shard locks) from within `async fn` callers. The scc docs warn against mixing sync and async operations. On a single-threaded tokio runtime this could deadlock; on multi-threaded it causes contention.

**Step 1: Implement the fix**

Replace `for_each` with an async-safe alternative. Since `scc::HashMap` provides `scan_async`, use that:

```rust
pub async fn for_each(&self, mut f: impl FnMut(&Inode, &R::Icb)) {
    self.inode_table
        .scan_async(|k, v| {
            if let IcbState::Available(icb) = v {
                f(k, icb);
            }
        })
        .await;
}
```

Check if `scan_async` is available in the version of `scc` being used. If not, document the requirement for a multi-threaded runtime in a comment.

**Step 2: Run full test suite**

Run: `cargo fmt --all && cargo clippy --all-targets --all-features -- -D warnings && cargo test --quiet`
Expected: All pass

**Step 3: Commit**

```bash
git add src/fs/icache/async_cache.rs
git commit -m "fix(icache): use scan_async instead of iter_sync for for_each"
```

---

### Task 7: Fix Medium — `reqwest-blocking-client` may deadlock in async context

**Files:**
- Modify: `Cargo.toml:47`

**Problem:** The `reqwest-blocking-client` feature uses blocking HTTP, which can panic or deadlock inside a tokio runtime. The batch span exporter runs its own thread so it's likely fine, but the async client is safer.

**Step 1: Implement the fix**

```toml
# Replace:
opentelemetry-otlp = { version = "0.29", features = ["http-proto", "trace", "reqwest-blocking-client"], optional = true }

# With:
opentelemetry-otlp = { version = "0.29", features = ["http-proto", "trace", "reqwest-client"], optional = true }
```

**Step 2: Verify it compiles**

Run: `cargo check --features __otlp_export`
Expected: Compiles without errors. If the OTLP exporter builder API differs for async reqwest, adjust the builder code in `trc.rs` accordingly.

**Step 3: Run full test suite**

Run: `cargo fmt --all && cargo clippy --all-targets --all-features -- -D warnings && cargo test --quiet`
Expected: All pass

**Step 4: Commit**

```bash
git add Cargo.toml Cargo.lock src/trc.rs
git commit -m "fix(deps): use async reqwest client for OTLP export to avoid blocking in tokio"
```

---

### Task 8: Fix Low — Redundant `cache_attr` after `get_or_resolve` in `RepoFs::lookup`

**Files:**
- Modify: `src/fs/mescloud/repo.rs` (lookup method, around line 268)

**Problem:** `get_or_resolve` already stores the resolved ICB (including attr) via `upsert_async`. The subsequent `cache_attr` reads the ICB back and writes the same attr — two unnecessary shard lock acquisitions.

**Step 1: Implement the fix**

Remove the redundant `cache_attr` call:

```rust
// In RepoFs::lookup, remove this line:
self.icache.cache_attr(ino, attr).await;
```

**Step 2: Run full test suite**

Run: `cargo fmt --all && cargo clippy --all-targets --all-features -- -D warnings && cargo test --quiet`
Expected: All pass

**Step 3: Commit**

```bash
git add src/fs/mescloud/repo.rs
git commit -m "fix(repo): remove redundant cache_attr after get_or_resolve in lookup"
```

---

### Task 9: Fix Low — `delegated_forget` iterates ALL slots

**Files:**
- Modify: `src/fs/mescloud/composite.rs:220-222`

**Problem:** `delegated_forget` already identifies the correct slot via `slot_for_inode` (line 212) but then iterates all slots to remove the inode from bridges. This is wasteful.

**Step 1: Implement the fix**

```rust
// Replace:
for slot in &mut self.slots {
    slot.bridge.remove_inode_by_left(ino);
}

// With:
if let Some(idx) = self.slot_for_inode(ino) {
    self.slots[idx].bridge.remove_inode_by_left(ino);
}
```

Wait — looking at the code again, `slot_for_inode` is already called at line 212 and the result is used for forwarding. The removal should use that same index. Check that `inode_to_slot` has already been updated before this point; if `inode_to_slot.remove` happens at line 219 before the bridge cleanup, we need to capture the index earlier.

**Step 2: Run full test suite**

Run: `cargo fmt --all && cargo clippy --all-targets --all-features -- -D warnings && cargo test --quiet`
Expected: All pass

**Step 3: Commit**

```bash
git add src/fs/mescloud/composite.rs
git commit -m "fix(composite): target single slot in delegated_forget instead of iterating all"
```

---

## Findings NOT requiring fixes (acknowledged, documented)

These are design concerns or latent issues that are safe under current architecture (`&mut self` serialization on `Fs` trait):

1. **`ensure_child_ino` TOCTOU** — safe due to `&mut self`. Add a `// SAFETY:` comment documenting why.
2. **`unreachable!()` in `forward_or_insert_inode`** — safe due to `&mut self`. Would become bugs if `Fs` changes to `&self`.
3. **`delegated_forget` unconditional propagation to inner FS** — correct by FUSE protocol invariant (inner/outer rc move in lockstep). Pre-existing design.
4. **`needs_resolve()` no TTL for mutable refs** — design concern for future. Not a bug for current fixed-ref mounts.
5. **O(n) linear scans in `ensure_child_ino` and `evict_zero_rc_children`** — performance concern for large repos. Worth optimizing with a parent→children index but not a correctness bug.
6. **`insert_icb` infinite loop on repeated failures** — liveness concern. In practice, resolution should eventually succeed.
7. **Commented-out `local` module** — cleanup task, not a bug.
8. **OTLP only in Ugly mode** — appears intentional. Confirm with author.
9. **`readdir` caches `size: 0` placeholder attrs** — standard FUSE pattern. Subsequent `lookup` triggers real resolution.
10. **`OrgResolver`/`MesaResolver` return `children: Some(vec![])`** — intentional. Org/Mesa layers manage children via `readdir`, not the resolver.
