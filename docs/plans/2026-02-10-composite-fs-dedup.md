# CompositeFs Deduplication Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Eliminate duplicated delegation logic between `MesaFS` (mod.rs) and `OrgFs` (org.rs) by extracting a shared `CompositeFs<R, Inner>` struct.

**Architecture:** Both `MesaFS` and `OrgFs` implement the same "compositing filesystem" pattern: they own a set of child filesystem instances, each with a `HashMapBridge` for inode/fh translation. The delegation code for `open`, `read`, `release`, `forget`, `getattr`, `statfs`, and the inner branches of `lookup`/`readdir` is nearly identical. We extract this into a generic `CompositeFs<R, Inner>` struct that holds the shared state and implements all delegation methods, then refactor both types to embed it.

**Tech Stack:** Rust, async_trait, tokio

---

## Feasibility Analysis

### Why not a blanket impl?

The user's original proposal was:

```rust
trait Subtrait { ... }
impl<T: Subtrait> Fs for T { ... }
```

This is **technically possible** but has significant trade-offs:

1. **Borrow checker friction:** The subtrait needs accessor methods like `composite_mut(&mut self) -> &mut CompositeFs<...>`. All delegation goes through this single `&mut self` borrow, which prevents the split-borrow patterns the current code relies on (e.g., accessing `self.slots[idx].bridge` and `self.icache` simultaneously). Workable, but requires pre-allocating inodes before closure calls and restructuring some APIs.

2. **Verbose subtrait definition:** The subtrait needs ~6 methods (`composite()`, `composite_mut()`, `delegation_target()`, `handle_root_lookup()`, `handle_root_readdir()`, `on_forget_cleanup()`), each implemented on both types. The net LOC savings vs thin wrappers is modest.

3. **Indirection cost:** Readers must understand both the `MescloudFs` subtrait and the blanket impl to follow any `Fs` method. With the composition approach, each `Fs` method is a clear 1-2 line delegation.

### Recommended approach: CompositeFs (composition)

Extract a `CompositeFs<R, Inner>` struct that owns the shared state and implements all delegation methods. `MesaFS` and `OrgFs` embed it and write thin `impl Fs` wrappers. This:

- Eliminates ~200 lines of duplicated delegation logic
- Avoids borrow checker complications (direct field access within CompositeFs)
- Keeps `impl Fs` on each type readable (1-line delegations + custom root logic)
- Is a standard Rust composition pattern

---

## Task 1: Create `ChildSlot` and `CompositeFs` structs

**Files:**
- Create: `src/fs/mescloud/composite.rs`
- Modify: `src/fs/mescloud/mod.rs` (add `mod composite;`)

**Step 1: Write the `ChildSlot` and `CompositeFs` types**

Create `src/fs/mescloud/composite.rs`:

```rust
use std::collections::HashMap;
use std::ffi::OsStr;
use std::time::SystemTime;

use bytes::Bytes;
use tracing::{trace, warn};

use crate::fs::icache::bridge::HashMapBridge;
use crate::fs::icache::{FileTable, IcbResolver};
use crate::fs::r#trait::{
    DirEntry, DirEntryType, FileAttr, FileHandle, FilesystemStats, Fs, Inode, LockOwner, OpenFile,
    OpenFlags,
};

use super::common::InodeControlBlock;
use super::common::{GetAttrError, LookupError, OpenError, ReadDirError, ReadError, ReleaseError};
use super::icache as mescloud_icache;
use super::icache::MescloudICache;

/// A child filesystem slot: inner filesystem + bidirectional inode/fh bridge.
pub(super) struct ChildSlot<Inner> {
    pub inner: Inner,
    pub bridge: HashMapBridge,
}

/// Generic compositing filesystem that delegates to child `Inner` filesystems.
///
/// Holds the shared infrastructure (icache, file table, readdir buffer, child
/// slots) and implements all the delegation methods that `MesaFS` and `OrgFs`
/// previously duplicated.
pub(super) struct CompositeFs<R, Inner>
where
    R: IcbResolver<Icb = InodeControlBlock>,
{
    pub icache: MescloudICache<R>,
    pub file_table: FileTable,
    pub readdir_buf: Vec<DirEntry>,
    /// Maps outer inode → index into `slots` for child-root inodes.
    pub child_inodes: HashMap<Inode, usize>,
    pub slots: Vec<ChildSlot<Inner>>,
}
```

**Step 2: Verify it compiles**

Run: `cargo fmt --all && cargo clippy --all-targets --all-features -- -D warnings && cargo test --quiet`
Expected: PASS (new file is just types, no usage yet)

**Step 3: Commit**

```bash
git add src/fs/mescloud/composite.rs src/fs/mescloud/mod.rs
git commit -m "refactor: add CompositeFs and ChildSlot structs for shared delegation"
```

---

## Task 2: Create `InodeCachePeek` trait

Both `OrgFs` and `RepoFs` expose `inode_table_get_attr()` for parent layers to cache attrs during readdir. Extract this into a trait so `CompositeFs` can call it generically.

**Files:**
- Modify: `src/fs/mescloud/common.rs` (add trait definition)
- Modify: `src/fs/mescloud/org.rs` (implement trait, remove ad-hoc method)
- Modify: `src/fs/mescloud/repo.rs` (implement trait, remove ad-hoc method)

**Step 1: Add the trait to common.rs**

```rust
/// Allows a parent compositor to peek at cached attrs from a child filesystem.
#[async_trait::async_trait]
pub(super) trait InodeCachePeek {
    async fn peek_attr(&self, ino: Inode) -> Option<FileAttr>;
}
```

**Step 2: Implement on OrgFs and RepoFs**

Replace `pub(crate) async fn inode_table_get_attr` on both types with:

```rust
#[async_trait::async_trait]
impl InodeCachePeek for OrgFs {
    async fn peek_attr(&self, ino: Inode) -> Option<FileAttr> {
        self.icache.get_attr(ino).await
    }
}
```

(Same for RepoFs.)

**Step 3: Update call sites in mod.rs and org.rs**

Replace `.inode_table_get_attr(...)` calls with `.peek_attr(...)`.

**Step 4: Verify**

Run: `cargo fmt --all && cargo clippy --all-targets --all-features -- -D warnings && cargo test --quiet`
Expected: PASS

**Step 5: Commit**

```bash
git add src/fs/mescloud/common.rs src/fs/mescloud/org.rs src/fs/mescloud/repo.rs src/fs/mescloud/mod.rs
git commit -m "refactor: extract InodeCachePeek trait from inode_table_get_attr"
```

---

## Task 3: Implement delegation methods on CompositeFs

**Files:**
- Modify: `src/fs/mescloud/composite.rs`

Add all the shared delegation methods. These are the methods that were duplicated between `MesaFS` and `OrgFs`.

**Step 1: Add helper methods**

```rust
impl<R, Inner> CompositeFs<R, Inner>
where
    R: IcbResolver<Icb = InodeControlBlock>,
    Inner: Fs<
        LookupError = LookupError,
        GetAttrError = GetAttrError,
        OpenError = OpenError,
        ReadError = ReadError,
        ReaddirError = ReadDirError,
        ReleaseError = ReleaseError,
    > + InodeCachePeek + Send,
{
    /// Find the child slot that owns `ino` by walking the parent chain.
    pub async fn slot_for_inode(&self, ino: Inode) -> Option<usize> {
        if let Some(&idx) = self.child_inodes.get(&ino) {
            return Some(idx);
        }
        let mut current = ino;
        loop {
            let parent = self
                .icache
                .get_icb(current, |icb| icb.parent)
                .await
                .flatten()?;
            if let Some(&idx) = self.child_inodes.get(&parent) {
                return Some(idx);
            }
            current = parent;
        }
    }

    /// Allocate an outer file handle and map it through the bridge.
    pub fn alloc_fh(&mut self, slot_idx: usize, inner_fh: FileHandle) -> FileHandle {
        let fh = self.file_table.allocate();
        self.slots[slot_idx].bridge.insert_fh(fh, inner_fh);
        fh
    }

    /// Translate an inner inode to an outer inode, allocating if needed.
    /// Also inserts a stub ICB into the outer icache.
    pub async fn translate_inner_ino(
        &mut self,
        slot_idx: usize,
        inner_ino: Inode,
        parent_outer_ino: Inode,
        name: &OsStr,
    ) -> Inode {
        let outer_ino = self.slots[slot_idx]
            .bridge
            .backward_or_insert_inode(inner_ino, || self.icache.allocate_inode());
        self.icache
            .entry_or_insert_icb(
                outer_ino,
                || InodeControlBlock {
                    rc: 0,
                    path: name.into(),
                    parent: Some(parent_outer_ino),
                    attr: None,
                    children: None,
                },
                |_| {},
            )
            .await;
        outer_ino
    }
}
```

**Step 2: Add delegation methods**

```rust
    // -- Fs delegation methods --

    pub async fn delegated_getattr(
        &self,
        ino: Inode,
    ) -> Result<FileAttr, GetAttrError> {
        self.icache.get_attr(ino).await.ok_or_else(|| {
            warn!(ino, "getattr on unknown inode");
            GetAttrError::InodeNotFound
        })
    }

    pub async fn delegated_open(
        &mut self,
        ino: Inode,
        flags: OpenFlags,
    ) -> Result<OpenFile, OpenError> {
        let idx = self.slot_for_inode(ino).await.ok_or_else(|| {
            warn!(ino, "open on inode not belonging to any child");
            OpenError::InodeNotFound
        })?;
        let inner_ino = self.slots[idx]
            .bridge
            .forward_or_insert_inode(ino, || unreachable!("open: ino should be mapped"));
        let inner_open = self.slots[idx].inner.open(inner_ino, flags).await?;
        let outer_fh = self.alloc_fh(idx, inner_open.handle);
        trace!(ino, outer_fh, inner_fh = inner_open.handle, "open: assigned file handle");
        Ok(OpenFile {
            handle: outer_fh,
            options: inner_open.options,
        })
    }

    pub async fn delegated_read(
        &mut self,
        ino: Inode,
        fh: FileHandle,
        offset: u64,
        size: u32,
        flags: OpenFlags,
        lock_owner: Option<LockOwner>,
    ) -> Result<Bytes, ReadError> {
        let idx = self.slot_for_inode(ino).await.ok_or_else(|| {
            warn!(ino, "read on inode not belonging to any child");
            ReadError::InodeNotFound
        })?;
        let inner_ino = self.slots[idx]
            .bridge
            .forward_or_insert_inode(ino, || unreachable!("read: ino should be mapped"));
        let inner_fh = self.slots[idx].bridge.fh_forward(fh).ok_or_else(|| {
            warn!(fh, "read: no fh mapping found");
            ReadError::FileNotOpen
        })?;
        self.slots[idx]
            .inner
            .read(inner_ino, inner_fh, offset, size, flags, lock_owner)
            .await
    }

    pub async fn delegated_release(
        &mut self,
        ino: Inode,
        fh: FileHandle,
        flags: OpenFlags,
        flush: bool,
    ) -> Result<(), ReleaseError> {
        let idx = self.slot_for_inode(ino).await.ok_or_else(|| {
            warn!(ino, "release on inode not belonging to any child");
            ReleaseError::FileNotOpen
        })?;
        let inner_ino = self.slots[idx]
            .bridge
            .forward_or_insert_inode(ino, || unreachable!("release: ino should be mapped"));
        let inner_fh = self.slots[idx].bridge.fh_forward(fh).ok_or_else(|| {
            warn!(fh, "release: no fh mapping found");
            ReleaseError::FileNotOpen
        })?;
        let result = self.slots[idx]
            .inner
            .release(inner_ino, inner_fh, flags, flush)
            .await;
        self.slots[idx].bridge.remove_fh_by_left(fh);
        trace!(ino, fh, "release: cleaned up fh mapping");
        result
    }

    /// Returns `true` if the inode was evicted (rc dropped to zero).
    pub async fn delegated_forget(&mut self, ino: Inode, nlookups: u64) -> bool {
        // Propagate forget to inner if applicable.
        if let Some(idx) = self.slot_for_inode(ino).await {
            if let Some(&inner_ino) = self.slots[idx].bridge.inode_map_get_by_left(ino) {
                self.slots[idx].inner.forget(inner_ino, nlookups).await;
            }
        }
        if self.icache.forget(ino, nlookups).await.is_some() {
            self.child_inodes.remove(&ino);
            for slot in &mut self.slots {
                slot.bridge.remove_inode_by_left(ino);
            }
            true
        } else {
            false
        }
    }

    pub fn delegated_statfs(&self) -> FilesystemStats {
        self.icache.statfs()
    }

    /// Delegation branch for lookup (when parent is owned by a child slot).
    pub async fn delegated_lookup(
        &mut self,
        parent: Inode,
        name: &OsStr,
    ) -> Result<FileAttr, LookupError> {
        let idx = self.slot_for_inode(parent).await.ok_or(LookupError::InodeNotFound)?;
        let inner_parent = self.slots[idx]
            .bridge
            .forward_or_insert_inode(parent, || unreachable!("lookup: parent should be mapped"));
        let inner_attr = self.slots[idx].inner.lookup(inner_parent, name).await?;
        let inner_ino = inner_attr.common().ino;
        let outer_ino = self.translate_inner_ino(idx, inner_ino, parent, name).await;
        let outer_attr = self.slots[idx].bridge.attr_backward(inner_attr);
        self.icache.cache_attr(outer_ino, outer_attr).await;
        let rc = self.icache.inc_rc(outer_ino).await;
        trace!(outer_ino, inner_ino, rc, "lookup: resolved via delegation");
        Ok(outer_attr)
    }

    /// Delegation branch for readdir (when ino is owned by a child slot).
    pub async fn delegated_readdir(
        &mut self,
        ino: Inode,
    ) -> Result<&[DirEntry], ReadDirError> {
        let idx = self.slot_for_inode(ino).await.ok_or(ReadDirError::InodeNotFound)?;
        let inner_ino = self.slots[idx]
            .bridge
            .forward_or_insert_inode(ino, || unreachable!("readdir: ino should be mapped"));
        let inner_entries = self.slots[idx].inner.readdir(inner_ino).await?;
        let inner_entries: Vec<DirEntry> = inner_entries.to_vec();

        let mut outer_entries = Vec::with_capacity(inner_entries.len());
        for entry in &inner_entries {
            let outer_child_ino = self
                .translate_inner_ino(idx, entry.ino, ino, &entry.name)
                .await;
            if let Some(inner_attr) = self.slots[idx].inner.peek_attr(entry.ino).await {
                let outer_attr = self.slots[idx].bridge.attr_backward(inner_attr);
                self.icache.cache_attr(outer_child_ino, outer_attr).await;
            }
            outer_entries.push(DirEntry {
                ino: outer_child_ino,
                name: entry.name.clone(),
                kind: entry.kind,
            });
        }
        self.readdir_buf = outer_entries;
        Ok(&self.readdir_buf)
    }
```

**Step 3: Verify**

Run: `cargo fmt --all && cargo clippy --all-targets --all-features -- -D warnings && cargo test --quiet`
Expected: PASS (methods exist but aren't called yet)

**Step 4: Commit**

```bash
git add src/fs/mescloud/composite.rs
git commit -m "refactor: implement delegation methods on CompositeFs"
```

---

## Task 4: Refactor MesaFS to use CompositeFs

**Files:**
- Modify: `src/fs/mescloud/mod.rs`

**Step 1: Replace MesaFS fields with CompositeFs**

Replace:
```rust
pub struct MesaFS {
    icache: MescloudICache<MesaResolver>,
    file_table: FileTable,
    readdir_buf: Vec<DirEntry>,
    org_inodes: HashMap<Inode, usize>,
    org_slots: Vec<OrgSlot>,
}
```

With:
```rust
pub struct MesaFS {
    composite: CompositeFs<MesaResolver, OrgFs>,
}
```

Remove the `OrgSlot` struct (replaced by `ChildSlot<OrgFs>`).

**Step 2: Update `MesaFS::new`**

Replace field initialization with `CompositeFs` construction:
```rust
pub fn new(orgs: impl Iterator<Item = OrgConfig>, fs_owner: (u32, u32)) -> Self {
    let resolver = MesaResolver { fs_owner, block_size: Self::BLOCK_SIZE };
    Self {
        composite: CompositeFs {
            icache: MescloudICache::new(resolver, Self::ROOT_NODE_INO, fs_owner, Self::BLOCK_SIZE),
            file_table: FileTable::new(),
            readdir_buf: Vec::new(),
            child_inodes: HashMap::new(),
            slots: orgs.map(|org_conf| {
                let client = MesaClient::builder()
                    .with_api_key(org_conf.api_key.expose_secret())
                    .with_base_path(MESA_API_BASE_URL)
                    .build();
                let org = OrgFs::new(org_conf.name, client, fs_owner);
                ChildSlot { inner: org, bridge: HashMapBridge::new() }
            }).collect(),
        },
    }
}
```

**Step 3: Update helper methods**

- `inode_role`: access `self.composite.child_inodes` instead of `self.org_inodes`
- `org_slot_for_inode`: replace with `self.composite.slot_for_inode(ino)`
- `ensure_org_inode`: access `self.composite.icache`, `self.composite.slots[idx]`, `self.composite.child_inodes`
- `alloc_fh`: remove (use `self.composite.alloc_fh()`)
- `translate_org_ino_to_mesa`: remove (use `self.composite.translate_inner_ino()`)

**Step 4: Update `impl Fs for MesaFS`**

Replace delegation methods with one-line forwards:
```rust
#[async_trait::async_trait]
impl Fs for MesaFS {
    type LookupError = LookupError;
    type GetAttrError = GetAttrError;
    type OpenError = OpenError;
    type ReadError = ReadError;
    type ReaddirError = ReadDirError;
    type ReleaseError = ReleaseError;

    async fn lookup(&mut self, parent: Inode, name: &OsStr) -> Result<FileAttr, LookupError> {
        if parent == Self::ROOT_NODE_INO {
            // Root children are orgs — custom logic stays here.
            let org_name = name.to_str().ok_or(LookupError::InodeNotFound)?;
            let org_idx = self.composite.slots.iter()
                .position(|s| s.inner.name() == org_name)
                .ok_or(LookupError::InodeNotFound)?;
            let (ino, attr) = self.ensure_org_inode(org_idx).await;
            self.composite.icache.inc_rc(ino).await;
            Ok(attr)
        } else {
            self.composite.delegated_lookup(parent, name).await
        }
    }

    async fn getattr(&mut self, ino: Inode, _fh: Option<FileHandle>) -> Result<FileAttr, GetAttrError> {
        self.composite.delegated_getattr(ino).await
    }

    async fn readdir(&mut self, ino: Inode) -> Result<&[DirEntry], ReadDirError> {
        if ino == Self::ROOT_NODE_INO {
            // Root readdir lists orgs — custom logic stays here.
            // ... (keep existing root readdir logic, using self.composite.*)
        } else {
            self.composite.delegated_readdir(ino).await
        }
    }

    async fn open(&mut self, ino: Inode, flags: OpenFlags) -> Result<OpenFile, OpenError> {
        self.composite.delegated_open(ino, flags).await
    }

    async fn read(&mut self, ino: Inode, fh: FileHandle, offset: u64, size: u32, flags: OpenFlags, lock_owner: Option<LockOwner>) -> Result<Bytes, ReadError> {
        self.composite.delegated_read(ino, fh, offset, size, flags, lock_owner).await
    }

    async fn release(&mut self, ino: Inode, fh: FileHandle, flags: OpenFlags, flush: bool) -> Result<(), ReleaseError> {
        self.composite.delegated_release(ino, fh, flags, flush).await
    }

    async fn forget(&mut self, ino: Inode, nlookups: u64) {
        self.composite.delegated_forget(ino, nlookups).await;
    }

    async fn statfs(&mut self) -> Result<FilesystemStats, std::io::Error> {
        Ok(self.composite.delegated_statfs())
    }
}
```

**Step 5: Verify**

Run: `cargo fmt --all && cargo clippy --all-targets --all-features -- -D warnings && cargo test --quiet`
Expected: PASS (all 36 tests)

**Step 6: Commit**

```bash
git add src/fs/mescloud/mod.rs
git commit -m "refactor: MesaFS now delegates to CompositeFs"
```

---

## Task 5: Refactor OrgFs to use CompositeFs

**Files:**
- Modify: `src/fs/mescloud/org.rs`

**Step 1: Replace OrgFs fields with CompositeFs**

Replace:
```rust
pub struct OrgFs {
    name: String,
    client: MesaClient,
    icache: MescloudICache<OrgResolver>,
    file_table: FileTable,
    readdir_buf: Vec<DirEntry>,
    repo_inodes: HashMap<Inode, usize>,
    owner_inodes: HashMap<Inode, String>,
    repos: Vec<RepoSlot>,
}
```

With:
```rust
pub struct OrgFs {
    name: String,
    client: MesaClient,
    composite: CompositeFs<OrgResolver, RepoFs>,
    /// Maps org-level owner-dir inodes → owner name (github only).
    owner_inodes: HashMap<Inode, String>,
}
```

Remove the `RepoSlot` struct (replaced by `ChildSlot<RepoFs>`).

**Step 2: Update `OrgFs::new`, helper methods, and `impl Fs`**

Same pattern as Task 4:
- `new`: build `CompositeFs` instead of individual fields
- `inode_role`: check `self.owner_inodes` and `self.composite.child_inodes`
- `repo_slot_for_inode`: replace with `self.composite.slot_for_inode(ino)`
- `ensure_repo_inode`: use `self.composite.icache.*` and `self.composite.slots`
- `alloc_fh`: remove (use `self.composite.alloc_fh()`)
- `translate_repo_ino_to_org`: remove (use `self.composite.translate_inner_ino()`)
- Delegation Fs methods: one-line forwards to `self.composite.*`
- Root/OwnerDir branches: keep custom logic, using `self.composite.*` for icache access

**Step 3: Update `impl InodeCachePeek for OrgFs`**

```rust
#[async_trait::async_trait]
impl InodeCachePeek for OrgFs {
    async fn peek_attr(&self, ino: Inode) -> Option<FileAttr> {
        self.composite.icache.get_attr(ino).await
    }
}
```

**Step 4: Handle `forget` cleanup for `owner_inodes`**

```rust
async fn forget(&mut self, ino: Inode, nlookups: u64) {
    let evicted = self.composite.delegated_forget(ino, nlookups).await;
    if evicted {
        self.owner_inodes.remove(&ino);
    }
}
```

**Step 5: Verify**

Run: `cargo fmt --all && cargo clippy --all-targets --all-features -- -D warnings && cargo test --quiet`
Expected: PASS (all 36 tests)

**Step 6: Commit**

```bash
git add src/fs/mescloud/org.rs
git commit -m "refactor: OrgFs now delegates to CompositeFs"
```

---

## Task 6: Remove code separators

Per project conventions, remove the `// ------` section separators from `mod.rs` and `org.rs` while we're in these files.

**Step 1: Remove separators**

Delete all lines matching `// -----------` in `mod.rs` and `org.rs`.

**Step 2: Verify**

Run: `cargo fmt --all && cargo clippy --all-targets --all-features -- -D warnings && cargo test --quiet`
Expected: PASS

**Step 3: Commit**

```bash
git add src/fs/mescloud/mod.rs src/fs/mescloud/org.rs
git commit -m "chore: remove section separator comments per project conventions"
```

---

## Summary of changes

| File | Change |
|------|--------|
| `src/fs/mescloud/composite.rs` | **NEW** — `ChildSlot<Inner>`, `CompositeFs<R, Inner>` with all delegation methods |
| `src/fs/mescloud/common.rs` | Add `InodeCachePeek` trait |
| `src/fs/mescloud/mod.rs` | Replace `OrgSlot` + duplicated fields/methods with `CompositeFs`, thin `impl Fs` wrappers |
| `src/fs/mescloud/org.rs` | Replace `RepoSlot` + duplicated fields/methods with `CompositeFs`, thin `impl Fs` wrappers |
| `src/fs/mescloud/repo.rs` | Implement `InodeCachePeek`, remove `inode_table_get_attr` |

**Estimated net LOC change:** Remove ~150-200 lines of duplicated delegation logic, add ~120 lines of `CompositeFs` (shared once). Net reduction ~30-80 lines with much less duplication.
