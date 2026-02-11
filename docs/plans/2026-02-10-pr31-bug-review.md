# PR #31 Bug Review Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:dispatching-parallel-agents to execute this review plan.

**Goal:** Find bugs in PR #31 (MES-710: Add an async icache) by reviewing every changed file, every domain of concern, and every critical code path.

**Architecture:** Three-phase review: (1) per-file structural review, (2) cross-cutting domain review, (3) end-to-end code path review. Each phase uses parallel subagents.

**Tech Stack:** Rust, tokio, scc::HashMap (lock-free concurrent map), FUSE (via fuser crate), watch channels for InFlight signaling.

---

## Phase 1: Per-File Review (parallel agents)

Each agent reads one file (final state on branch) and the diff, looking for bugs.

### Agent 1.1: `async_cache.rs` (1210 lines — highest risk)
**File:** `src/fs/icache/async_cache.rs`
**Focus:** Race conditions in InFlight/Available state machine, deadlocks, lost wakeups, `unreachable!` panics, correctness of `forget`/`inc_rc`/`get_or_resolve`.

### Agent 1.2: `composite.rs` (294 lines)
**File:** `src/fs/mescloud/composite.rs`
**Focus:** Inode/FH translation correctness, bridge cleanup on forget, `readdir_buf` lifetime, `unreachable!` in `forward_or_insert_inode` closures.

### Agent 1.3: `icache.rs` (mescloud wrapper)
**File:** `src/fs/mescloud/icache.rs`
**Focus:** `needs_resolve()` logic, `evict_zero_rc_children` correctness (iterating while modifying), `ensure_child_ino` O(n) scan, `cache_attr` silent failure.

### Agent 1.4: `mod.rs` (MesaFS)
**File:** `src/fs/mescloud/mod.rs`
**Focus:** `MesaResolver` always returning `Infallible`, `inode_role` fallback to `Root`, `ensure_org_inode` bridge reset, removed `debug_assert!`s.

### Agent 1.5: `org.rs` (OrgFs)
**File:** `src/fs/mescloud/org.rs`
**Focus:** `OrgResolver`, `register_repo_slot` orphaned slot handling, `owner_inodes` cleanup on forget, github special casing.

### Agent 1.6: `repo.rs` (RepoFs)
**File:** `src/fs/mescloud/repo.rs`
**Focus:** `RepoResolver::resolve` with `unreachable!` on missing stub, `build_repo_path` infinite loop potential, `readdir` calling `get_or_resolve` then caching attr again, `path_of_inode` duplication.

### Agent 1.7: Small files (grouped)
**Files:** `src/fs/icache/file_table.rs`, `src/fs/icache/inode_factory.rs`, `src/fs/icache/mod.rs`, `src/fs/mescloud/common.rs`
**Focus:** Atomic ordering correctness (`Relaxed` for monotonic counters), `IcbLike` requiring `Clone`, error conversion completeness.

### Agent 1.8: `trc.rs` + `fuser.rs`
**Files:** `src/trc.rs`, `src/fs/fuser.rs`
**Focus:** OTLP shutdown ordering, feature flag correctness, instrument name changes.

## Phase 2: Domain Review (parallel agents)

### Agent 2.1: Concurrency & Race Conditions
**Scope:** All files using `AsyncICache`, `scc::HashMap`, `watch` channels
**Focus:** TOCTOU between `wait_for_available` and subsequent `update_async`/`read_async`, ABA problems in InFlight→Available→InFlight transitions, `for_each` + concurrent mutation, deadlock scenarios with nested shard locks.

### Agent 2.2: FUSE Ref-Counting Correctness
**Scope:** `inc_rc`, `forget`, `lookup` across all filesystem layers
**Focus:** Every `lookup` must `inc_rc` exactly once, every `forget` must propagate to inner FS, `inc_rc` returning `None` must fail the lookup (not silently proceed), ref-count leaks when errors occur after `inc_rc`.

### Agent 2.3: Error Recovery & Cleanup
**Scope:** `get_or_resolve` error paths, `InFlight` cleanup on resolver failure
**Focus:** Is the `InFlight` entry always removed/restored on error? Does the `watch::Sender` always get dropped? What happens to waiters when resolution fails? Are there resource leaks?

### Agent 2.4: CompositeFs Bridge Consistency
**Scope:** `composite.rs`, `mod.rs`, `org.rs` — all bridge operations
**Focus:** Are `child_inodes`, `inode_to_slot`, and bridge maps kept in sync? Does `delegated_forget` clean up all three? What about `readdir_buf` aliasing?

## Phase 3: Code Path Review (parallel agents)

### Agent 3.1: Lookup Path (FUSE → MesaFS → OrgFs → RepoFs)
**Trace:** `FuserAdapter::lookup` → `MesaFS::lookup` → `CompositeFs::delegated_lookup` → `OrgFs::lookup` → `CompositeFs::delegated_lookup` → `RepoFs::lookup` → `RepoResolver::resolve`
**Focus:** Are inodes properly translated at each boundary? Is `inc_rc` called exactly once at each layer? What happens if the inner lookup succeeds but `inc_rc` returns `None`?

### Agent 3.2: Readdir Path
**Trace:** `FuserAdapter::readdir` → `MesaFS::readdir` → `CompositeFs::delegated_readdir` → `OrgFs::readdir` → `CompositeFs::delegated_readdir` → `RepoFs::readdir`
**Focus:** `readdir_buf` ownership and aliasing, `evict_zero_rc_children` TOCTOU with concurrent lookups, `translate_inner_ino` creating stubs that may conflict with concurrent resolvers.

### Agent 3.3: Forget/Eviction Path
**Trace:** `FuserAdapter::forget` → `MesaFS::forget` → `CompositeFs::delegated_forget` → `OrgFs::forget` → `CompositeFs::delegated_forget` → `RepoFs::forget`
**Focus:** Does forget propagate correctly through all layers? Is the bridge cleaned up before or after the inner forget? Can forget race with a concurrent lookup that's incrementing rc?
