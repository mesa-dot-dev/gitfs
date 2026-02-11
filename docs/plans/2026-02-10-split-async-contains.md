# Split AsyncICache::contains into Sync Methods

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Replace the async `contains()` method with two non-async variants to eliminate debug/release control flow divergence caused by awaiting InFlight entries in debug_asserts.

**Architecture:** The async `contains()` currently awaits InFlight entries (potentially blocking on network I/O), which changes control flow in debug vs release builds. We split it into `contains()` (sync, key exists in any state) and `contains_resolved()` (sync, key is Available). The internal `wait_for_available()` remains for other methods that genuinely need to await.

**Tech Stack:** Rust, scc::HashMap (has `read_sync` and `contains_sync`), tokio

---

### Task 1: Add sync `contains` and `contains_resolved` to `AsyncICache`

**Files:**
- Modify: `src/fs/icache/async_cache.rs:119-122`

**Step 1: Add the two new methods**

Add these methods to the `impl<R: IcbResolver> AsyncICache<R>` block, replacing the existing `pub async fn contains`:

```rust
    /// Check whether `ino` has an entry in the table (either `InFlight` or `Available`).
    ///
    /// This is a non-blocking, synchronous check. It does **not** wait for
    /// `InFlight` entries to resolve.
    pub fn contains(&self, ino: Inode) -> bool {
        self.inode_table.contains_sync(&ino)
    }

    /// Check whether `ino` is fully resolved (`Available`).
    ///
    /// Returns `false` if the entry is missing **or** still `InFlight`.
    /// This is a non-blocking, synchronous check.
    pub fn contains_resolved(&self, ino: Inode) -> bool {
        self.inode_table
            .read_sync(&ino, |_, s| matches!(s, IcbState::Available(_)))
            .unwrap_or(false)
    }
```

**Step 2: Run verify**

Run: `cargo fmt --all && cargo clippy --all-targets --all-features -- -D warnings && cargo test --quiet`

This will fail because tests still call `cache.contains(42).await` on the now non-async method. That's expected — we fix the tests in Task 4.

**Step 3: Commit**

```bash
git add src/fs/icache/async_cache.rs
git commit -m "feat: add sync contains() and contains_resolved() to AsyncICache"
```

---

### Task 2: Update `MescloudICache` delegation

**Files:**
- Modify: `src/fs/mescloud/icache.rs:101-105`

**Step 1: Replace the async delegation with sync delegations**

Replace:
```rust
    pub async fn contains(&self, ino: Inode) -> bool {
        self.inner.contains(ino).await
    }
```

With:
```rust
    pub fn contains(&self, ino: Inode) -> bool {
        self.inner.contains(ino)
    }

    pub fn contains_resolved(&self, ino: Inode) -> bool {
        self.inner.contains_resolved(ino)
    }
```

**Step 2: Run verify**

Run: `cargo fmt --all && cargo clippy --all-targets --all-features -- -D warnings && cargo test --quiet`

This will still fail from tests — expected.

**Step 3: Commit**

```bash
git add src/fs/mescloud/icache.rs
git commit -m "feat: add sync contains/contains_resolved to MescloudICache"
```

---

### Task 3: Replace `debug_assert!` usages in `repo.rs`

**Files:**
- Modify: `src/fs/mescloud/repo.rs` (lines 228-231, 260-263, 352, 450-453)

**Step 1: Update the three debug_asserts**

In `lookup` (line 228-231), replace:
```rust
        debug_assert!(
            self.icache.contains(parent).await,
            "lookup: parent inode {parent} not in inode table"
        );
```
With:
```rust
        debug_assert!(
            self.icache.contains(parent),
            "lookup: parent inode {parent} not in inode table"
        );
```

In `readdir` (line 260-263), replace:
```rust
        debug_assert!(
            self.icache.contains(ino).await,
            "readdir: inode {ino} not in inode table"
        );
```
With:
```rust
        debug_assert!(
            self.icache.contains(ino),
            "readdir: inode {ino} not in inode table"
        );
```

In `forget` (line 450-453), replace:
```rust
        debug_assert!(
            self.icache.contains(ino).await,
            "forget: inode {ino} not in inode table"
        );
```
With:
```rust
        debug_assert!(
            self.icache.contains(ino),
            "forget: inode {ino} not in inode table"
        );
```

**Step 2: Update the `open` guard**

In `open` (line 352), replace:
```rust
        if !self.icache.contains(ino).await {
```
With:
```rust
        if !self.icache.contains(ino) {
```

The `open` method doesn't read ICB data — it only allocates a file handle. Subsequent `read` calls go through `path_of_inode` → `get_icb` which properly awaits InFlight entries. FUSE guarantees the inode was previously looked up, so it must be in the table (InFlight or Available).

**Step 3: Run verify**

Run: `cargo fmt --all && cargo clippy --all-targets --all-features -- -D warnings && cargo test --quiet`

Still expect test failures from `async_cache.rs` tests.

**Step 4: Commit**

```bash
git add src/fs/mescloud/repo.rs
git commit -m "fix: use sync contains() in debug_asserts and open guard"
```

---

### Task 4: Update `async_cache.rs` tests

**Files:**
- Modify: `src/fs/icache/async_cache.rs` (test module, lines 460+)

**Step 1: Update tests that used `contains(...).await`**

The following test assertions need their `.await` removed since `contains()` is now sync:

`contains_returns_true_for_root` (line 463):
```rust
assert!(cache.contains(1), "root should exist");
```

`contains_returns_false_for_missing` (line 469):
```rust
assert!(!cache.contains(999), "missing inode should not exist");
```

`contains_after_resolver_completes` (line 493):
```rust
assert!(cache.contains(42), "should be true after resolve");
```

`insert_icb_adds_entry` (line 564):
```rust
assert!(cache.contains(42), "inserted entry should exist");
```

`forget_evicts_when_rc_drops_to_zero` (line 700):
```rust
assert!(!cache.contains(42), "evicted entry should be gone");
```

`wait_does_not_miss_signal_on_immediate_complete` (lines 795-801): This test exercises the awaiting behavior of the old `contains`. It should now test `contains_resolved` or be refactored. Replace:
```rust
        let result =
            tokio::time::timeout(std::time::Duration::from_millis(100), cache.contains(42)).await;
        assert_eq!(
            result,
            Ok(true),
            "should not hang on already-completed entry"
        );
```
With:
```rust
        assert!(cache.contains(42), "entry should exist in table");
        assert!(
            cache.contains_resolved(42),
            "should be resolved after insert_icb overwrote InFlight"
        );
```

`get_or_resolve_resolves_missing` (line 840):
```rust
assert!(cache.contains(42));
```

`get_or_resolve_propagates_error` (line 853):
```rust
assert!(!cache.contains(42));
```

**Step 2: Add dedicated tests for `contains_resolved`**

Add after the existing `contains_returns_false_for_missing` test:

```rust
    #[tokio::test]
    async fn contains_resolved_returns_true_for_root() {
        let cache = test_cache();
        assert!(cache.contains_resolved(1), "root should be resolved");
    }

    #[tokio::test]
    async fn contains_resolved_returns_false_for_missing() {
        let cache = test_cache();
        assert!(
            !cache.contains_resolved(999),
            "missing inode should not be resolved"
        );
    }

    #[tokio::test]
    async fn contains_resolved_returns_false_for_inflight() {
        let cache = test_cache();
        let (_tx, rx) = watch::channel(());
        cache
            .inode_table
            .upsert_async(42, IcbState::InFlight(rx))
            .await;
        assert!(cache.contains(42), "InFlight entry should exist");
        assert!(
            !cache.contains_resolved(42),
            "InFlight entry should not be resolved"
        );
    }
```

**Step 3: Run verify**

Run: `cargo fmt --all && cargo clippy --all-targets --all-features -- -D warnings && cargo test --quiet`

Expected: ALL PASS

**Step 4: Commit**

```bash
git add src/fs/icache/async_cache.rs
git commit -m "test: update tests for sync contains/contains_resolved"
```
