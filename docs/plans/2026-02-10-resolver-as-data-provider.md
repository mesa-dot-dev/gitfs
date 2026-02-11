# Consolidate Attr Creation Into Resolvers

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Eliminate manual `FileAttr` construction in `ensure_*` methods by delegating attr creation to the resolver via `get_or_resolve`, making the resolver the single source of truth for attribute data.

**Architecture:** The async icache has a resolver-based state machine (`InFlight`/`Available`) where `get_or_resolve` is the canonical path for populating ICBs. Currently, `ensure_owner_inode`, `ensure_repo_inode`, and `ensure_org_inode` bypass this by manually constructing `FileAttr::Directory` and calling `cache_attr`. This refactoring makes them insert stubs (attr=None) and then call `get_or_resolve`, which triggers the resolver to produce the attr. The resolvers (`OrgResolver`, `MesaResolver`) already contain this exact logic.

**Tech Stack:** Rust, tokio, scc::HashMap

---

## Context

### The Problem

Three `ensure_*` methods duplicate attr construction that their resolvers already handle:

| Method | File | Resolver |
|---|---|---|
| `ensure_owner_inode` | `src/fs/mescloud/org.rs:152` | `OrgResolver` |
| `ensure_repo_inode` | `src/fs/mescloud/org.rs:276` | `OrgResolver` |
| `ensure_org_inode` | `src/fs/mescloud/mod.rs:181` | `MesaResolver` |

Each method manually constructs `FileAttr::Directory { common: make_common_file_attr(...) }` in **two places** (existing-entry-missing-attr fallback + new-entry creation), then calls `cache_attr`. The resolvers do the exact same construction. This is ~6 duplicated attr-construction sites.

### The Fix

Replace manual construction with the resolver flow:
1. `insert_icb(stub)` — creates entry with `attr: None`
2. `get_or_resolve(ino, |icb| icb.attr)` — resolver populates the attr

The "existing entry with missing attr" defensive fallback also becomes unnecessary since `get_or_resolve` handles stubs (where `needs_resolve()` returns true) by calling the resolver.

### What Stays The Same

- `insert_icb` remains for stub creation (it's the correct way to seed the inode table with parent/path before resolution)
- `cache_attr` remains for cross-layer attr propagation (MesaFS ← OrgFs ← RepoFs via bridges) — this is NOT resolver data
- `ensure_child_ino` in `MescloudICache` stays unchanged (it creates stubs for the repo layer, resolved later by `RepoResolver`)
- `entry_or_insert_icb` stays unchanged (used by `translate_*` methods for bridge-level ICB mirroring)

---

### Task 1: Change `OrgResolver::Error` to `Infallible`

`OrgResolver::resolve` always returns `Ok(...)` — it synthesizes directory attrs from local data with no I/O. The error type `LookupError` is misleading. Changing to `Infallible` makes the guarantee explicit and aligns with `MesaResolver` which already uses `Infallible`.

**Files:**
- Modify: `src/fs/mescloud/org.rs:35-69` (OrgResolver impl)

**Step 1: Update the OrgResolver impl**

Change the error type and return type:

```rust
impl IcbResolver for OrgResolver {
    type Icb = InodeControlBlock;
    type Error = std::convert::Infallible;

    fn resolve(
        &self,
        ino: Inode,
        stub: Option<InodeControlBlock>,
        _cache: &AsyncICache<Self>,
    ) -> impl Future<Output = Result<InodeControlBlock, std::convert::Infallible>> + Send
    where
        Self: Sized,
    {
        // ... body unchanged ...
    }
}
```

**Step 2: Verify**

Run: `cargo fmt --all && cargo clippy --all-targets --all-features -- -D warnings && cargo test --quiet`

**Step 3: Commit**

```bash
git add src/fs/mescloud/org.rs
git commit -m "refactor: change OrgResolver::Error to Infallible"
```

---

### Task 2: Refactor `ensure_owner_inode` to use resolver

**Files:**
- Modify: `src/fs/mescloud/org.rs:152-202`

**Step 1: Replace the method body**

The new structure: find-or-create the inode, then resolve through the canonical path.

```rust
async fn ensure_owner_inode(&mut self, owner: &str) -> (Inode, FileAttr) {
    let existing_ino = self
        .owner_inodes
        .iter()
        .find_map(|(&ino, existing_owner)| (existing_owner == owner).then_some(ino));

    let ino = if let Some(ino) = existing_ino {
        ino
    } else {
        let ino = self.icache.allocate_inode();
        self.icache
            .insert_icb(
                ino,
                InodeControlBlock {
                    rc: 0,
                    path: owner.into(),
                    parent: Some(Self::ROOT_INO),
                    attr: None,
                },
            )
            .await;
        self.owner_inodes.insert(ino, owner.to_owned());
        ino
    };

    let attr = self
        .icache
        .get_or_resolve(ino, |icb| icb.attr.expect("resolved ICB must have attr"))
        .await
        .unwrap(); // OrgResolver is infallible
    (ino, attr)
}
```

Key changes:
- Removed all manual `FileAttr::Directory` construction (was in 2 places)
- Removed the "attr missing → rebuild" defensive fallback — `get_or_resolve` handles this via the resolver
- Removed `use std::time::SystemTime` usage in this method (resolver handles it)
- Single `get_or_resolve` call covers both "existing with attr" (fast path) and "existing without attr" / "newly created stub" (resolver path)

**Step 2: Verify**

Run: `cargo fmt --all && cargo clippy --all-targets --all-features -- -D warnings && cargo test --quiet`

**Step 3: Commit**

```bash
git add src/fs/mescloud/org.rs
git commit -m "refactor: ensure_owner_inode delegates attr creation to OrgResolver"
```

---

### Task 3: Refactor `ensure_repo_inode` to use resolver

**Files:**
- Modify: `src/fs/mescloud/org.rs:276-361`

**Step 1: Replace the method body**

Same pattern. Side-effects (RepoFs creation, bridge setup) happen in the "allocate new" branch before resolution.

```rust
async fn ensure_repo_inode(
    &mut self,
    repo_name: &str,
    display_name: &str,
    default_branch: &str,
    parent_ino: Inode,
) -> (Inode, FileAttr) {
    let repos = &self.repos;
    let existing_ino = self
        .repo_inodes
        .iter()
        .find_map(|(&ino, &idx)| (repos[idx].repo.repo_name() == repo_name).then_some(ino));

    let ino = if let Some(ino) = existing_ino {
        let rc = self.icache.get_icb(ino, |icb| icb.rc).await.unwrap_or(0);
        trace!(ino, repo = repo_name, rc, "ensure_repo_inode: reusing");
        ino
    } else {
        let ino = self.icache.allocate_inode();
        trace!(ino, repo = repo_name, "ensure_repo_inode: allocated new inode");

        self.icache
            .insert_icb(
                ino,
                InodeControlBlock {
                    rc: 0,
                    path: display_name.into(),
                    parent: Some(parent_ino),
                    attr: None,
                },
            )
            .await;

        let repo = RepoFs::new(
            self.client.clone(),
            self.name.clone(),
            repo_name.to_owned(),
            default_branch.to_owned(),
            self.icache.fs_owner(),
        );

        let mut bridge = HashMapBridge::new();
        bridge.insert_inode(ino, RepoFs::ROOT_INO);

        let idx = self.repos.len();
        self.repos.push(RepoSlot { repo, bridge });
        self.repo_inodes.insert(ino, idx);
        ino
    };

    let attr = self
        .icache
        .get_or_resolve(ino, |icb| icb.attr.expect("resolved ICB must have attr"))
        .await
        .unwrap(); // OrgResolver is infallible
    (ino, attr)
}
```

Key changes:
- Removed all manual `FileAttr::Directory` construction (was in 2 places)
- Removed the "attr missing → rebuilding" warn/fallback path
- Side-effects (RepoFs, bridge, repo_inodes) preserved in the else branch

**Step 2: Verify**

Run: `cargo fmt --all && cargo clippy --all-targets --all-features -- -D warnings && cargo test --quiet`

**Step 3: Commit**

```bash
git add src/fs/mescloud/org.rs
git commit -m "refactor: ensure_repo_inode delegates attr creation to OrgResolver"
```

---

### Task 4: Refactor `ensure_org_inode` to use resolver

**Files:**
- Modify: `src/fs/mescloud/mod.rs:181-253`

**Step 1: Replace the method body**

```rust
async fn ensure_org_inode(&mut self, org_idx: usize) -> (Inode, FileAttr) {
    let existing_ino = self
        .org_inodes
        .iter()
        .find(|&(_, &idx)| idx == org_idx)
        .map(|(&ino, _)| ino);

    let ino = if let Some(ino) = existing_ino {
        let rc = self
            .icache
            .get_icb(ino, |icb| icb.rc)
            .await
            .unwrap_or(0);
        trace!(ino, org_idx, rc, "ensure_org_inode: reusing existing inode");
        ino
    } else {
        let org_name = self.org_slots[org_idx].org.name().to_owned();
        let ino = self.icache.allocate_inode();
        trace!(ino, org_idx, org = %org_name, "ensure_org_inode: allocated new inode");

        self.icache
            .insert_icb(
                ino,
                InodeControlBlock {
                    rc: 0,
                    path: org_name.as_str().into(),
                    parent: Some(Self::ROOT_NODE_INO),
                    attr: None,
                },
            )
            .await;

        self.org_inodes.insert(ino, org_idx);
        self.org_slots[org_idx]
            .bridge
            .insert_inode(ino, OrgFs::ROOT_INO);
        ino
    };

    let attr = self
        .icache
        .get_or_resolve(ino, |icb| icb.attr.expect("resolved ICB must have attr"))
        .await
        .unwrap(); // MesaResolver is infallible
    (ino, attr)
}
```

Key changes:
- Removed all manual `FileAttr::Directory` construction (was in 2 places)
- Removed the "attr missing → rebuilding" warn/fallback path
- Side-effects (org_inodes, bridge seeding) preserved

**Step 2: Remove unused `SystemTime` import if no longer needed**

Check if `SystemTime` is still used in `mod.rs`. It is used in `MesaResolver::resolve` and `MescloudICache::new`, so it stays.

**Step 3: Verify**

Run: `cargo fmt --all && cargo clippy --all-targets --all-features -- -D warnings && cargo test --quiet`

**Step 4: Commit**

```bash
git add src/fs/mescloud/mod.rs
git commit -m "refactor: ensure_org_inode delegates attr creation to MesaResolver"
```

---

### Task 5: Remove unused imports

**Files:**
- Modify: `src/fs/mescloud/org.rs` (check for unused `SystemTime`, `mescloud_icache` usage)
- Modify: `src/fs/mescloud/mod.rs` (same check)

**Step 1: Check and remove unused imports**

After the refactoring, check whether `SystemTime` and `mescloud_icache::make_common_file_attr` are still used in each file outside of the resolver. The resolvers still use them, so they likely stay. But verify with clippy.

**Step 2: Verify**

Run: `cargo fmt --all && cargo clippy --all-targets --all-features -- -D warnings && cargo test --quiet`

**Step 3: Commit (if changes needed)**

```bash
git add src/fs/mescloud/org.rs src/fs/mescloud/mod.rs
git commit -m "chore: remove unused imports after resolver refactoring"
```
