# Decouple DCache and Mescloud InodeControlBlock

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Separate the generic `MescloudDCache` implementation from the mescloud-specific `InodeControlBlock` struct, so that `src/fs/dcache/` contains only generic cache machinery and `src/fs/mescloud/` owns its own ICB definition.

**Architecture:** Currently `src/fs/dcache/mescloud.rs` conflates two concerns: (1) the `MescloudDCache` wrapper (inode allocation, attr caching, statfs) and (2) the `InodeControlBlock` data structure specific to mescloud filesystems. We will split this file in two: `src/fs/dcache/dcache.rs` gets the `MescloudDCache` (renamed to just keep the module name generic), and `src/fs/mescloud/dcache.rs` gets the `InodeControlBlock`. We also rename `MescloudDCache` to a more generic name since the wrapper is not truly mescloud-specific.

**Tech Stack:** Rust, no new dependencies.

---

## Analysis: What Lives Where

### Currently in `src/fs/dcache/mescloud.rs` (246 lines):
1. **`InodeControlBlock`** (lines 19-47) - Mescloud-specific ICB with `parent`, `rc`, `path`, `children`, `attr` fields + `IcbLike` impl
2. **`InodeFactory`** (lines 52-67) - Monotonically increasing inode allocator (private helper)
3. **`MescloudDCache`** (lines 75-241) - Wraps `DCache<InodeControlBlock>` with inode allocation, attr caching, child inode management, attr construction, statfs
4. **`blocks_of_size`** (line 243) - Utility function used in `repo.rs`

### Consumers of `InodeControlBlock`:
- `src/fs/mescloud/common.rs:5` - `pub(super) use crate::fs::dcache::mescloud::InodeControlBlock`
- `src/fs/mescloud/mod.rs:17` - `use common::InodeControlBlock` (constructs ICB literals)
- `src/fs/mescloud/org.rs:11` - `use super::common::InodeControlBlock` (constructs ICB literals)
- `src/fs/dcache/mescloud.rs` itself (constructs ICB in `ensure_child_inode`)

### Key insight:
`MescloudDCache` directly constructs `InodeControlBlock` literals inside `ensure_child_inode()` (line 164). This creates a hard coupling. To decouple, `MescloudDCache` must become generic over its ICB type (using `IcbLike`), or `ensure_child_inode` must be moved/changed. Since `ensure_child_inode` needs `parent`, `attr`, and `children` fields that go beyond `IcbLike`, the cleanest approach is:

1. Move `InodeControlBlock` to `src/fs/mescloud/dcache.rs`
2. Keep `MescloudDCache` in `src/fs/dcache/` but rename the file to reflect it's a higher-level cache wrapper
3. Add a factory method to `IcbLike` so `MescloudDCache` can construct ICBs generically, OR keep `MescloudDCache` typed to `InodeControlBlock` but import it from the new location

**Chosen approach:** The simplest correct refactor is:
- Move `InodeControlBlock` + its `IcbLike` impl to `src/fs/mescloud/dcache.rs`
- Keep `MescloudDCache` in `src/fs/dcache/` (rename file from `mescloud.rs` to `dcache.rs`) but make it import `InodeControlBlock` from `src/fs/mescloud::dcache`
- This creates a circular dependency problem: `dcache` depends on `mescloud` and `mescloud` depends on `dcache`

**Revised approach:** To avoid circular deps, we must make `MescloudDCache` generic. Extend `IcbLike` with the additional capabilities that `ensure_child_inode` and attr methods need:

- Add `parent(&self) -> Option<Inode>` and `set_parent(&mut self, parent: Option<Inode>)` to `IcbLike`
- Add `path(&self) -> &Path` to `IcbLike`
- Add `attr(&self) -> Option<FileAttr>` and `set_attr(&mut self, attr: Option<FileAttr>)` to `IcbLike`
- Add `children(&self) -> Option<&[DirEntry]>` and `children_mut` to `IcbLike`
- Add a new constructor `fn new_child(parent: Inode, path: PathBuf) -> Self` to `IcbLike`

This is over-engineering. Let's reconsider.

**Final approach (simplest):**
1. Rename `src/fs/dcache/mescloud.rs` to `src/fs/dcache/dcache.rs` - keep `MescloudDCache`, `InodeFactory`, `blocks_of_size` here
2. Create `src/fs/mescloud/dcache.rs` - move `InodeControlBlock` + `IcbLike` impl here
3. `MescloudDCache` in `src/fs/dcache/dcache.rs` imports `InodeControlBlock` from `crate::fs::mescloud::dcache`
4. **Circular dependency check:** `src/fs/dcache/dcache.rs` imports from `crate::fs::mescloud::dcache` and `src/fs/mescloud/*` imports from `crate::fs::dcache`. In Rust, cross-module imports within the same crate are fine as long as there are no circular `mod` declarations. Since both `dcache` and `mescloud` are siblings under `src/fs/mod.rs`, this works.

---

## Additional Readability Opportunities Found

1. **`src/fs/mescloud/common.rs`** - The `pub(super) use crate::fs::dcache::mescloud::InodeControlBlock` re-export (line 5) should change to import from the new location (`super::dcache::InodeControlBlock`).

2. **`blocks_of_size` function** - Currently lives in `src/fs/dcache/mescloud.rs` (line 243) but is only used by `src/fs/mescloud/repo.rs`. It's a mescloud concern, not a generic dcache concern. Move it to `src/fs/mescloud/dcache.rs` alongside `InodeControlBlock`.

---

## Tasks

### Task 1: Create `src/fs/mescloud/dcache.rs` with `InodeControlBlock`

**Files:**
- Create: `src/fs/mescloud/dcache.rs`
- Modify: `src/fs/mescloud/mod.rs` (add `pub mod dcache;` declaration)

**Step 1: Create `src/fs/mescloud/dcache.rs`**

```rust
//! Mescloud-specific inode control block and helpers.

use crate::fs::dcache::IcbLike;
use crate::fs::r#trait::{DirEntry, Inode};

/// Inode control block for mescloud filesystem layers (MesaFS, OrgFs, RepoFs).
pub struct InodeControlBlock {
    /// The root inode doesn't have a parent.
    pub parent: Option<Inode>,
    pub rc: u64,
    pub path: std::path::PathBuf,
    pub children: Option<Vec<DirEntry>>,
    /// Cached file attributes from the last lookup.
    pub attr: Option<crate::fs::r#trait::FileAttr>,
}

impl IcbLike for InodeControlBlock {
    fn new_root(path: std::path::PathBuf) -> Self {
        Self {
            rc: 1,
            parent: None,
            path,
            children: None,
            attr: None,
        }
    }

    fn rc(&self) -> u64 {
        self.rc
    }

    fn rc_mut(&mut self) -> &mut u64 {
        &mut self.rc
    }
}

/// Calculate the number of blocks needed for a given size.
pub fn blocks_of_size(block_size: u32, size: u64) -> u64 {
    size.div_ceil(u64::from(block_size))
}
```

**Step 2: Add module declaration in `src/fs/mescloud/mod.rs`**

Add `pub mod dcache;` after the existing module declarations (after line 25: `pub mod repo;`). The new line:

```rust
pub mod dcache;
```

**Step 3: Verify it compiles**

Run: `cargo check 2>&1 | head -30`
Expected: Compiles (new module exists but isn't consumed yet; existing code still uses old paths)

**Step 4: Commit**

```bash
git add src/fs/mescloud/dcache.rs src/fs/mescloud/mod.rs
git commit -m "Add mescloud/dcache.rs with InodeControlBlock and blocks_of_size"
```

---

### Task 2: Rename `src/fs/dcache/mescloud.rs` to `src/fs/dcache/dcache.rs` and update imports

**Files:**
- Rename: `src/fs/dcache/mescloud.rs` -> `src/fs/dcache/dcache.rs`
- Modify: `src/fs/dcache/dcache.rs` (remove `InodeControlBlock`, `IcbLike` impl, and `blocks_of_size`; import `InodeControlBlock` from new location)
- Modify: `src/fs/dcache/mod.rs` (change `pub mod mescloud;` to `pub mod dcache;`, update re-exports)

**Step 1: Rename the file**

```bash
git mv src/fs/dcache/mescloud.rs src/fs/dcache/dcache.rs
```

**Step 2: Update `src/fs/dcache/mod.rs`**

Replace the full contents with:

```rust
//! Generic directory cache and inode management primitives.

mod dcache;
pub mod bridge;
mod table;

pub use dcache::MescloudDCache;
pub use table::DCache;

/// Common interface for inode control block types usable with `DCache`.
pub trait IcbLike {
    /// Create an ICB with rc=1, the given path, and no children.
    fn new_root(path: std::path::PathBuf) -> Self;
    fn rc(&self) -> u64;
    fn rc_mut(&mut self) -> &mut u64;
}
```

Note: `pub mod mescloud` becomes `mod dcache` (private, since consumers access `MescloudDCache` via the re-export).

**Step 3: Update `src/fs/dcache/dcache.rs`**

Remove the `InodeControlBlock` struct (lines 19-27), its `IcbLike` impl (lines 29-47), and the `blocks_of_size` function (lines 243-245).

Replace the import `use super::{DCache, IcbLike};` with:

```rust
use super::{DCache, IcbLike};
use crate::fs::mescloud::dcache::InodeControlBlock;
```

The `use crate::fs::r#trait::...` import line should drop `Permissions` only if it was exclusively used by `InodeControlBlock`. Check: `Permissions` is still used by `make_common_file_attr` (line 193), so keep it.

The file should now contain only: `InodeFactory`, `MescloudDCache`, and their impls. No `InodeControlBlock`, no `blocks_of_size`.

**Step 4: Verify it compiles**

Run: `cargo check 2>&1 | head -30`
Expected: Compiles successfully

**Step 5: Commit**

```bash
git add src/fs/dcache/
git commit -m "Rename dcache/mescloud.rs to dcache/dcache.rs, import ICB from mescloud"
```

---

### Task 3: Update `src/fs/mescloud/common.rs` import path

**Files:**
- Modify: `src/fs/mescloud/common.rs`

**Step 1: Update the import**

Change line 5 from:
```rust
pub(super) use crate::fs::dcache::mescloud::InodeControlBlock;
```
to:
```rust
pub(super) use super::dcache::InodeControlBlock;
```

**Step 2: Verify it compiles**

Run: `cargo check 2>&1 | head -30`
Expected: Compiles successfully

**Step 3: Commit**

```bash
git add src/fs/mescloud/common.rs
git commit -m "Update InodeControlBlock import in common.rs to use mescloud::dcache"
```

---

### Task 4: Update `src/fs/mescloud/repo.rs` import path

**Files:**
- Modify: `src/fs/mescloud/repo.rs`

**Step 1: Update the import**

Change line 17 from:
```rust
use crate::fs::dcache::mescloud::{self as mescloud_dcache, MescloudDCache};
```
to:
```rust
use crate::fs::dcache::MescloudDCache;
use super::dcache as mescloud_dcache;
```

This keeps the `mescloud_dcache::blocks_of_size` call on line 138 working since `blocks_of_size` now lives in `src/fs/mescloud/dcache.rs`.

**Step 2: Verify it compiles**

Run: `cargo check 2>&1 | head -30`
Expected: Compiles successfully

**Step 3: Commit**

```bash
git add src/fs/mescloud/repo.rs
git commit -m "Update repo.rs imports to use mescloud::dcache for blocks_of_size"
```

---

### Task 5: Final verification and cleanup

**Step 1: Full build check**

Run: `cargo check 2>&1`
Expected: No errors, no warnings related to our changes

**Step 2: Verify no remaining references to old path**

Search for `dcache::mescloud::` across the codebase. Should find zero results (all references now go through the re-export or the new path).

Run: `grep -r "dcache::mescloud" src/`
Expected: No output

**Step 3: Verify file structure matches goal**

```
src/fs/dcache/
  mod.rs          - re-exports DCache, MescloudDCache, IcbLike trait
  dcache.rs       - MescloudDCache, InodeFactory (imports InodeControlBlock from mescloud)
  table.rs        - generic DCache<I: IcbLike>
  bridge.rs       - HashMapBridge

src/fs/mescloud/
  mod.rs          - MesaFS (top-level container)
  dcache.rs       - InodeControlBlock, blocks_of_size  <-- NEW
  common.rs       - error types, re-exports InodeControlBlock
  org.rs          - OrgFs (single org)
  repo.rs         - RepoFs (single repo)
```

**Step 4: Commit (if any cleanup was needed)**

```bash
git add -A
git commit -m "Final cleanup: verify decoupled dcache and mescloud ICB"
```
