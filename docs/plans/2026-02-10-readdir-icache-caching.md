# RepoFs readdir icache caching Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Make `RepoFs::readdir` read directory listings from the icache (via the resolver) instead of calling the mesa API on every invocation.

**Architecture:** Add a `children` field to `InodeControlBlock` storing `Option<Vec<(String, DirEntryType)>>`. The `RepoResolver` populates this field when resolving directory inodes (it already calls the content API). `readdir` then calls `get_or_resolve` on the icache, which transparently invokes the resolver on cache miss. `needs_resolve()` is updated to return `true` for directory ICBs that lack children, ensuring directories get fully resolved on first access. For a fixed ref, directory contents are immutable, making this cache always valid.

**Tech Stack:** Rust, tokio, scc (concurrent HashMap), mesa_dev SDK

---

### Task 1: Add `children` field to `InodeControlBlock`

**Files:**
- Modify: `src/fs/mescloud/icache.rs:1-39`

**Step 1: Write a failing test for `needs_resolve()` on directory ICBs**

Add a `#[cfg(test)]` module at the bottom of `src/fs/mescloud/icache.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::fs::r#trait::DirEntryType;

    fn dummy_dir_attr(ino: Inode) -> FileAttr {
        let now = std::time::SystemTime::now();
        FileAttr::Directory {
            common: make_common_file_attr(ino, 0o755, now, now, (0, 0), 4096),
        }
    }

    fn dummy_file_attr(ino: Inode) -> FileAttr {
        let now = std::time::SystemTime::now();
        FileAttr::RegularFile {
            common: make_common_file_attr(ino, 0o644, now, now, (0, 0), 4096),
            size: 100,
            blocks: 1,
        }
    }

    #[test]
    fn needs_resolve_stub_returns_true() {
        let icb = InodeControlBlock {
            parent: Some(1),
            rc: 0,
            path: "stub".into(),
            attr: None,
            children: None,
        };
        assert!(icb.needs_resolve());
    }

    #[test]
    fn needs_resolve_file_with_attr_returns_false() {
        let icb = InodeControlBlock {
            parent: Some(1),
            rc: 1,
            path: "file.txt".into(),
            attr: Some(dummy_file_attr(2)),
            children: None,
        };
        assert!(!icb.needs_resolve());
    }

    #[test]
    fn needs_resolve_dir_without_children_returns_true() {
        let icb = InodeControlBlock {
            parent: Some(1),
            rc: 1,
            path: "dir".into(),
            attr: Some(dummy_dir_attr(3)),
            children: None,
        };
        assert!(icb.needs_resolve());
    }

    #[test]
    fn needs_resolve_dir_with_children_returns_false() {
        let icb = InodeControlBlock {
            parent: Some(1),
            rc: 1,
            path: "dir".into(),
            attr: Some(dummy_dir_attr(3)),
            children: Some(vec![
                ("README.md".to_owned(), DirEntryType::RegularFile),
            ]),
        };
        assert!(!icb.needs_resolve());
    }

    #[test]
    fn needs_resolve_dir_with_empty_children_returns_false() {
        let icb = InodeControlBlock {
            parent: Some(1),
            rc: 1,
            path: "empty-dir".into(),
            attr: Some(dummy_dir_attr(4)),
            children: Some(vec![]),
        };
        assert!(!icb.needs_resolve());
    }
}
```

**Step 2: Run test to verify it fails**

Run: `cargo test --quiet -p git-fs --lib mescloud::icache::tests`
Expected: FAIL — `InodeControlBlock` doesn't have `children` field yet.

**Step 3: Add `children` field and update `needs_resolve()`**

In `src/fs/mescloud/icache.rs`, add the import for `DirEntryType`:

```rust
use crate::fs::r#trait::{CommonFileAttr, DirEntryType, FileAttr, FilesystemStats, Inode, Permissions};
```

Update the struct:

```rust
pub struct InodeControlBlock {
    pub parent: Option<Inode>,
    pub rc: u64,
    pub path: std::path::PathBuf,
    /// Cached file attributes from the last lookup.
    pub attr: Option<FileAttr>,
    /// Cached directory children from the resolver (directories only).
    pub children: Option<Vec<(String, DirEntryType)>>,
}
```

Update `new_root`:

```rust
fn new_root(path: std::path::PathBuf) -> Self {
    Self {
        rc: 1,
        parent: None,
        path,
        attr: None,
        children: None,
    }
}
```

Update `needs_resolve`:

```rust
fn needs_resolve(&self) -> bool {
    match self.attr {
        None => true,
        Some(FileAttr::Directory { .. }) => self.children.is_none(),
        Some(_) => false,
    }
}
```

**Step 4: Fix all `InodeControlBlock` construction sites**

Every place that creates an `InodeControlBlock` literal must add `children: None` (or `children: Some(...)` where appropriate). These are all in `src/fs/mescloud/`:

1. **`src/fs/mescloud/icache.rs:231`** — `ensure_child_ino` stub:
   ```rust
   InodeControlBlock {
       rc: 0,
       path: name.into(),
       parent: Some(parent),
       attr: None,
       children: None,
   }
   ```

2. **`src/fs/mescloud/mod.rs:62`** — `MesaResolver::resolve` stub fallback:
   ```rust
   let stub = stub.unwrap_or_else(|| InodeControlBlock {
       parent: None,
       path: "/".into(),
       rc: 0,
       attr: None,
       children: None,
   });
   ```

3. **`src/fs/mescloud/mod.rs:74`** — `MesaResolver::resolve` return (directories — set `children: Some(vec![])`):
   ```rust
   Ok(InodeControlBlock {
       attr: Some(attr),
       children: Some(vec![]),
       ..stub
   })
   ```

4. **`src/fs/mescloud/mod.rs:227`** — `MesaFS::ensure_org_inode` insert:
   ```rust
   InodeControlBlock {
       rc: 0,
       path: org_name.as_str().into(),
       parent: Some(Self::ROOT_NODE_INO),
       attr: None,
       children: None,
   }
   ```

5. **`src/fs/mescloud/mod.rs:280`** — `MesaFS::translate_org_ino_to_mesa` factory:
   ```rust
   InodeControlBlock {
       rc: 0,
       path: name.into(),
       parent: Some(parent_mesa_ino),
       attr: None,
       children: None,
   }
   ```

6. **`src/fs/mescloud/org.rs:51`** — `OrgResolver::resolve` stub fallback:
   ```rust
   let stub = stub.unwrap_or_else(|| InodeControlBlock {
       parent: None,
       path: "/".into(),
       rc: 0,
       attr: None,
       children: None,
   });
   ```

7. **`src/fs/mescloud/org.rs:63`** — `OrgResolver::resolve` return (directories — set `children: Some(vec![])`):
   ```rust
   Ok(InodeControlBlock {
       attr: Some(attr),
       children: Some(vec![]),
       ..stub
   })
   ```

8. **`src/fs/mescloud/org.rs:179-188`** — `OrgFs::ensure_owner_inode` insert:
   ```rust
   InodeControlBlock {
       rc: 0,
       path: owner.into(),
       parent: Some(Self::ROOT_INO),
       attr: None,
       children: None,
   }
   ```

9. **`src/fs/mescloud/org.rs:325-334`** — `OrgFs::ensure_repo_inode` insert:
   ```rust
   InodeControlBlock {
       rc: 0,
       path: display_name.into(),
       parent: Some(parent_ino),
       attr: None,
       children: None,
   }
   ```

10. **`src/fs/mescloud/org.rs:411`** — `OrgFs::translate_repo_ino_to_org` factory:
    ```rust
    InodeControlBlock {
        rc: 0,
        path: name.into(),
        parent: Some(parent_org_ino),
        attr: None,
        children: None,
    }
    ```

11. **`src/fs/mescloud/repo.rs:99`** — `RepoResolver::resolve` return (will be updated in Task 2 — for now, add `children: None`):
    ```rust
    Ok(InodeControlBlock {
        parent: stub.parent,
        path: stub.path,
        rc: stub.rc,
        attr: Some(attr),
        children: None,
    })
    ```

**Step 5: Run tests to verify they pass**

Run: `cargo fmt --all && cargo clippy --all-targets --all-features -- -D warnings && cargo test --quiet`
Expected: All tests PASS, including the new `needs_resolve` tests.

**Step 6: Commit**

```bash
git add src/fs/mescloud/icache.rs src/fs/mescloud/mod.rs src/fs/mescloud/org.rs src/fs/mescloud/repo.rs
git commit -m "feat: add children field to InodeControlBlock for directory caching"
```

---

### Task 2: Populate `children` in `RepoResolver`

**Files:**
- Modify: `src/fs/mescloud/repo.rs:37-107` (the `RepoResolver::resolve` impl)

**Step 1: Update `RepoResolver::resolve` to populate `children` for directories**

In `src/fs/mescloud/repo.rs`, inside the `resolve` async block, after building `attr`, extract children from `Content::Dir`:

```rust
async move {
    let stub = stub.unwrap_or_else(|| unreachable!("RepoResolver requires a stub ICB"));
    let file_path = build_repo_path(stub.parent, &stub.path, cache, RepoFs::ROOT_INO).await;

    let content = client
        .org(&org_name)
        .repos()
        .at(&repo_name)
        .content()
        .get(Some(ref_.as_str()), file_path.as_deref(), None)
        .await
        .map_err(MesaApiError::from)?;

    let now = SystemTime::now();
    let attr = match &content {
        Content::File(f) => {
            let size = f.size.to_u64().unwrap_or(0);
            FileAttr::RegularFile {
                common: mescloud_icache::make_common_file_attr(
                    ino, 0o644, now, now, fs_owner, block_size,
                ),
                size,
                blocks: mescloud_icache::blocks_of_size(block_size, size),
            }
        }
        Content::Symlink(s) => {
            let size = s.size.to_u64().unwrap_or(0);
            FileAttr::RegularFile {
                common: mescloud_icache::make_common_file_attr(
                    ino, 0o644, now, now, fs_owner, block_size,
                ),
                size,
                blocks: mescloud_icache::blocks_of_size(block_size, size),
            }
        }
        Content::Dir(_) => FileAttr::Directory {
            common: mescloud_icache::make_common_file_attr(
                ino, 0o755, now, now, fs_owner, block_size,
            ),
        },
    };

    let children = match content {
        Content::Dir(d) => Some(
            d.entries
                .into_iter()
                .filter_map(|e| {
                    let (name, kind) = match e {
                        MesaDirEntry::File(f) => (f.name?, DirEntryType::RegularFile),
                        // TODO(MES-712): return DirEntryType::Symlink once readlink is wired up.
                        MesaDirEntry::Symlink(s) => (s.name?, DirEntryType::RegularFile),
                        MesaDirEntry::Dir(d) => (d.name?, DirEntryType::Directory),
                    };
                    Some((name, kind))
                })
                .collect(),
        ),
        Content::File(_) | Content::Symlink(_) => None,
    };

    Ok(InodeControlBlock {
        parent: stub.parent,
        path: stub.path,
        rc: stub.rc,
        attr: Some(attr),
        children,
    })
}
```

Note: The `match &content` (borrow) for `attr` must come before `match content` (move) for `children`. The existing code already borrows for `attr`, so this change only adds a second `match` that consumes `content`.

**Step 2: Run tests to verify they pass**

Run: `cargo fmt --all && cargo clippy --all-targets --all-features -- -D warnings && cargo test --quiet`
Expected: PASS. This is a purely additive change — the resolver now populates `children` but nothing reads it yet.

**Step 3: Commit**

```bash
git add src/fs/mescloud/repo.rs
git commit -m "feat: populate children in RepoResolver for directory inodes"
```

---

### Task 3: Add `From<LookupError> for ReadDirError` conversion

**Files:**
- Modify: `src/fs/mescloud/common.rs:125-149`

**Step 1: Write a failing test for the conversion**

Add tests at the bottom of `src/fs/mescloud/common.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lookup_inode_not_found_converts_to_readdir_inode_not_found() {
        let err: ReadDirError = LookupError::InodeNotFound.into();
        assert!(matches!(err, ReadDirError::InodeNotFound));
    }

    #[test]
    fn lookup_file_does_not_exist_converts_to_readdir_inode_not_found() {
        let err: ReadDirError = LookupError::FileDoesNotExist.into();
        assert!(matches!(err, ReadDirError::InodeNotFound));
    }

    #[test]
    fn lookup_remote_error_converts_to_readdir_remote_error() {
        let api_err = MesaApiError::Response {
            status: 500,
            body: "test".to_owned(),
        };
        let err: ReadDirError = LookupError::RemoteMesaError(api_err).into();
        assert!(matches!(err, ReadDirError::RemoteMesaError(_)));
    }
}
```

**Step 2: Run test to verify it fails**

Run: `cargo test --quiet -p git-fs --lib mescloud::common::tests`
Expected: FAIL — `From<LookupError> for ReadDirError` not implemented.

**Step 3: Add the `From` impl**

In `src/fs/mescloud/common.rs`, add after the `ReadDirError` definition (before `impl From<ReadDirError> for i32`):

```rust
impl From<LookupError> for ReadDirError {
    fn from(e: LookupError) -> Self {
        match e {
            LookupError::RemoteMesaError(api) => Self::RemoteMesaError(api),
            LookupError::InodeNotFound | LookupError::FileDoesNotExist => Self::InodeNotFound,
        }
    }
}
```

**Step 4: Run tests to verify they pass**

Run: `cargo fmt --all && cargo clippy --all-targets --all-features -- -D warnings && cargo test --quiet`
Expected: PASS.

**Step 5: Commit**

```bash
git add src/fs/mescloud/common.rs
git commit -m "feat: add From<LookupError> for ReadDirError conversion"
```

---

### Task 4: Rewrite `RepoFs::readdir` to use icache

**Files:**
- Modify: `src/fs/mescloud/repo.rs:260-350` (the `readdir` impl)

**Step 1: Replace the direct API call with `get_or_resolve`**

Replace the entire `readdir` method body in `src/fs/mescloud/repo.rs`:

```rust
#[instrument(skip(self), fields(repo = %self.repo_name))]
async fn readdir(&mut self, ino: Inode) -> Result<&[DirEntry], ReadDirError> {
    debug_assert!(
        self.icache.contains(ino),
        "readdir: inode {ino} not in inode table"
    );
    debug_assert!(
        matches!(
            self.icache.get_attr(ino).await,
            Some(FileAttr::Directory { .. }) | None
        ),
        "readdir: inode {ino} has non-directory cached attr"
    );

    let children = self
        .icache
        .get_or_resolve(ino, |icb| icb.children.clone())
        .await?
        .ok_or(ReadDirError::NotADirectory)?;

    trace!(ino, count = children.len(), "readdir: resolved directory listing from icache");

    let mut entries = Vec::with_capacity(children.len());
    for (name, kind) in &children {
        let child_ino = self.icache.ensure_child_ino(ino, OsStr::new(name)).await;
        let now = SystemTime::now();
        let attr = match kind {
            DirEntryType::Directory => FileAttr::Directory {
                common: mescloud_icache::make_common_file_attr(
                    child_ino,
                    0o755,
                    now,
                    now,
                    self.icache.fs_owner(),
                    self.icache.block_size(),
                ),
            },
            DirEntryType::RegularFile
            | DirEntryType::Symlink
            | DirEntryType::CharDevice
            | DirEntryType::BlockDevice
            | DirEntryType::NamedPipe
            | DirEntryType::Socket => FileAttr::RegularFile {
                common: mescloud_icache::make_common_file_attr(
                    child_ino,
                    0o644,
                    now,
                    now,
                    self.icache.fs_owner(),
                    self.icache.block_size(),
                ),
                size: 0,
                blocks: 0,
            },
        };
        self.icache.cache_attr(child_ino, attr).await;
        entries.push(DirEntry {
            ino: child_ino,
            name: name.clone().into(),
            kind: *kind,
        });
    }

    self.readdir_buf = entries;
    Ok(&self.readdir_buf)
}
```

Key differences from original:
- No `self.path_of_inode(ino)` call
- No `self.client.org(...).repos().at(...).content().get(...)` call
- Instead: `self.icache.get_or_resolve(ino, |icb| icb.children.clone())` reads cached children (resolver fetches on miss)
- Error type uses `?` with `From<LookupError> for ReadDirError` (from Task 3)
- The child inode allocation and attr caching loop is identical

**Step 2: Run full verification**

Run: `cargo fmt --all && cargo clippy --all-targets --all-features -- -D warnings && cargo test --quiet`
Expected: PASS. Clippy may warn about unused imports — address in next step.

**Step 3: Clean up unused imports if any**

After the change, check whether any imports in `repo.rs` are now unused. The `readdir` method no longer uses:
- `self.client` in readdir (still used by `read`)
- `self.path_of_inode` in readdir (still used by `read`)

All imports should still be needed since `read` uses `Content` and the resolver uses `MesaDirEntry`. Verify with clippy output.

**Step 4: Run full verification again**

Run: `cargo fmt --all && cargo clippy --all-targets --all-features -- -D warnings && cargo test --quiet`
Expected: PASS with no warnings.

**Step 5: Commit**

```bash
git add src/fs/mescloud/repo.rs
git commit -m "feat: readdir reads from icache instead of querying API directly"
```
