# FileTable: Extract File Handle Management from ICache

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Extract file handle allocation into a dedicated `FileTable` type so that icaches are no longer responsible for file handle management.

**Architecture:** Currently both `ICache` (sync) and `AsyncICache` (async) embed a monotonic file handle counter (`next_fh`). This couples inode caching with file handle allocation — two unrelated concerns. We introduce `FileTable`, a standalone atomic counter (mirroring `InodeFactory` for inodes), owned directly by each filesystem (`MesaFS`, `OrgFs`, `RepoFs`) rather than by the icache layer.

**Tech Stack:** Rust, `std::sync::atomic::AtomicU64`

---

### Task 1: Create `FileTable` type

**Files:**
- Create: `src/fs/icache/file_table.rs`
- Modify: `src/fs/icache/mod.rs`

**Step 1: Write the test**

Add to `src/fs/icache/file_table.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn allocate_returns_monotonic_handles() {
        let ft = FileTable::new();
        assert_eq!(ft.allocate(), 1);
        assert_eq!(ft.allocate(), 2);
        assert_eq!(ft.allocate(), 3);
    }
}
```

**Step 2: Write the implementation**

Create `src/fs/icache/file_table.rs`:

```rust
use std::sync::atomic::{AtomicU64, Ordering};

use crate::fs::r#trait::FileHandle;

/// Monotonically increasing file handle allocator.
pub struct FileTable {
    next_fh: AtomicU64,
}

impl FileTable {
    pub fn new() -> Self {
        Self {
            next_fh: AtomicU64::new(1),
        }
    }

    pub fn allocate(&self) -> FileHandle {
        self.next_fh.fetch_add(1, Ordering::Relaxed)
    }
}
```

**Step 3: Register the module and export**

In `src/fs/icache/mod.rs`, add:
- `mod file_table;` (private module, like `inode_factory`)
- `pub use file_table::FileTable;`

**Step 4: Run tests to verify**

Run: `cargo fmt --all && cargo clippy --all-targets --all-features -- -D warnings && cargo test --quiet`
Expected: PASS — new type compiles and test passes.

**Step 5: Commit**

```bash
git add src/fs/icache/file_table.rs src/fs/icache/mod.rs
git commit -m "feat: add FileTable type for file handle allocation"
```

---

### Task 2: Remove `allocate_fh` from `AsyncICache`

**Files:**
- Modify: `src/fs/icache/async_cache.rs`

**Step 1: Remove the `next_fh` field from the struct**

Remove from `AsyncICache`:
```rust
next_fh: AtomicU64,
```

**Step 2: Remove `allocate_fh` method**

Remove:
```rust
pub fn allocate_fh(&self) -> FileHandle {
    self.next_fh.fetch_add(1, Ordering::Relaxed)
}
```

**Step 3: Remove `next_fh` initialization from `new()`**

Remove from `AsyncICache::new()`:
```rust
next_fh: AtomicU64::new(1),
```

**Step 4: Remove the `allocate_fh_increments` test**

Remove the entire test block.

**Step 5: Clean up unused imports**

Remove `AtomicU64` from `use std::sync::atomic::{AtomicU64, Ordering};` if only used in test code. Remove `FileHandle` from `use crate::fs::r#trait::{FileHandle, Inode};` if no longer used.

**Do NOT run verify yet** — Tasks 2–5 must all land together.

---

### Task 3: Remove `allocate_fh` from `ICache` (sync)

**Files:**
- Modify: `src/fs/icache/cache.rs`

**Step 1: Remove the `next_fh` field**

Remove from `ICache`:
```rust
next_fh: FileHandle,
```

**Step 2: Remove `allocate_fh` method**

Remove:
```rust
pub fn allocate_fh(&mut self) -> FileHandle {
    let fh = self.next_fh;
    self.next_fh += 1;
    fh
}
```

**Step 3: Remove `next_fh` from constructor**

Remove from `ICache::new()`:
```rust
next_fh: 1,
```

**Step 4: Clean up unused imports**

Remove `FileHandle` from `use crate::fs::r#trait::{FileHandle, Inode};` if no longer needed.

**Do NOT run verify yet.**

---

### Task 4: Remove `allocate_fh` from `MescloudICache`

**Files:**
- Modify: `src/fs/mescloud/icache.rs`

**Step 1: Remove the `allocate_fh` delegation**

Remove entirely:
```rust
pub fn allocate_fh(&self) -> FileHandle {
    self.inner.allocate_fh()
}
```

**Step 2: Clean up unused imports**

Remove `FileHandle` from the `use crate::fs::r#trait::` import if no longer used in this file.

**Do NOT run verify yet.**

---

### Task 5: Add `FileTable` to each filesystem

**Files:**
- Modify: `src/fs/mescloud/mod.rs` (`MesaFS`)
- Modify: `src/fs/mescloud/org.rs` (`OrgFs`)
- Modify: `src/fs/mescloud/repo.rs` (`RepoFs`)

**Step 1: `MesaFS` — add `FileTable` field**

Add import:
```rust
use crate::fs::icache::FileTable;
```

Add field to `MesaFS`:
```rust
file_table: FileTable,
```

Initialize in `MesaFS::new()`:
```rust
file_table: FileTable::new(),
```

Change `alloc_fh` to use the file table:
```rust
fn alloc_fh(&mut self, slot_idx: usize, org_fh: FileHandle) -> FileHandle {
    let fh = self.file_table.allocate();
    self.org_slots[slot_idx].bridge.insert_fh(fh, org_fh);
    fh
}
```

**Step 2: `OrgFs` — add `FileTable` field**

Add import:
```rust
use crate::fs::icache::FileTable;
```

Add field to `OrgFs`:
```rust
file_table: FileTable,
```

Initialize in `OrgFs::new()`:
```rust
file_table: FileTable::new(),
```

Change `alloc_fh` to use the file table:
```rust
fn alloc_fh(&mut self, slot_idx: usize, repo_fh: FileHandle) -> FileHandle {
    let fh = self.file_table.allocate();
    self.repos[slot_idx].bridge.insert_fh(fh, repo_fh);
    fh
}
```

**Step 3: `RepoFs` — add `FileTable` field**

Add import:
```rust
use crate::fs::icache::FileTable;
```

Add field to `RepoFs`:
```rust
file_table: FileTable,
```

Initialize in `RepoFs::new()`:
```rust
file_table: FileTable::new(),
```

In `open()`, change:
```rust
let fh = self.icache.allocate_fh();
```
to:
```rust
let fh = self.file_table.allocate();
```

**Step 4: Run the full verify**

Run: `cargo fmt --all && cargo clippy --all-targets --all-features -- -D warnings && cargo test --quiet`
Expected: All PASS.

**Step 5: Commit**

```bash
git add src/fs/icache/async_cache.rs src/fs/icache/cache.rs src/fs/mescloud/icache.rs src/fs/mescloud/mod.rs src/fs/mescloud/org.rs src/fs/mescloud/repo.rs
git commit -m "refactor: move file handle allocation from icaches to FileTable on each filesystem"
```
