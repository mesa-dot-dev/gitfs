# mesa-dev 0.1.1 → 1.8.0 Migration Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Fix all compilation errors caused by upgrading mesa-dev from 0.1.1 to 1.8.0.

**Architecture:** The mesa-dev crate restructured its API from flat module access (`mesa_dev::Mesa`, `mesa_dev::models::*`, `mesa_dev::error::*`) to a hierarchical client pattern (`MesaClient::builder()...build()`, `client.org().repos().at().content()`) with OpenAPI-generated model types. We need to update 4 files in `src/fs/mescloud/` to use the new imports, builder pattern, navigation API, and model types.

**Tech Stack:** Rust, mesa-dev 1.8.0, mesa_dev_oapi 1.8.0, thiserror, futures

---

## API Change Summary

| Old (0.1.1) | New (1.8.0) |
|---|---|
| `mesa_dev::Mesa` | `mesa_dev::MesaClient` |
| `mesa_dev::error::MesaError` | `mesa_dev::low_level::apis::Error<T>` (generic per endpoint) |
| `mesa_dev::models::Repo` | `mesa_dev::models::GetByOrgRepos200ResponseReposInner` (list) / `mesa_dev::models::PostByOrgRepos201Response` (get) |
| `mesa_dev::models::Content` | `mesa_dev::low_level::content::Content` |
| `mesa_dev::models::Content::File { size, content, .. }` | `Content::File(f)` where `f.size: f64`, `f.content: Option<String>` |
| `mesa_dev::models::Content::Dir { entries, .. }` | `Content::Dir(d)` where `d.entries: Vec<DirEntry>` |
| `mesa_dev::models::DirEntryType::{File, Dir}` | `mesa_dev::low_level::content::DirEntry::{File(_), Symlink(_), Dir(_)}` |
| `Mesa::builder(api_key).base_url(url).build()` | `MesaClient::builder().with_api_key(key).with_base_path(url).build()` |
| `client.content(org, repo).get(path, ref_)` | `client.org(org).repos().at(repo).content().get(ref_, path, depth)` |
| `client.repos(org).get(repo)` | `client.org(org).repos().at(repo).get()` |
| `client.repos(org).list_all()` | `client.org(org).repos().list(None)` |
| `Repo.default_branch: String` | `repo.default_branch: Option<String>` |
| `Repo.name: String` | `repo.name: Option<String>` |
| `Repo.status: Option<RepoStatus>` | Field removed (no sync status on model) |

---

### Task 1: Fix error types in `common.rs`

**Files:**
- Modify: `src/fs/mescloud/common.rs`

The old `mesa_dev::error::MesaError` no longer exists. The new error type `mesa_dev::low_level::apis::Error<T>` is generic (different `T` per endpoint), so we can't use `#[from]`. Change to storing a `String` and use `.map_err()` at call sites.

**Step 1: Update the three error enums**

Replace all three occurrences of:
```rust
#[error("remote mesa error: {0}")]
RemoteMesaError(#[from] mesa_dev::error::MesaError),
```

With:
```rust
#[error("remote mesa error: {0}")]
RemoteMesaError(String),
```

This affects `LookupError` (line 16), `ReadError` (line 65), and `ReadDirError` (line 91).

**Step 2: Verify the file is self-consistent**

No further changes needed in `common.rs` — the `From<...> for i32` impls still work since they match on the variant name, not the inner type.

---

### Task 2: Fix client import and builder in `mod.rs`

**Files:**
- Modify: `src/fs/mescloud/mod.rs`

**Step 1: Update the import**

Change line 5 from:
```rust
use mesa_dev::Mesa as MesaClient;
```
To:
```rust
use mesa_dev::MesaClient;
```

**Step 2: Update the builder call**

Change the client construction in `MesaFS::new()` (lines 70-72) from:
```rust
let client = MesaClient::builder(org_conf.api_key.expose_secret())
    .base_url(MESA_API_BASE_URL)
    .build();
```
To:
```rust
let client = MesaClient::builder()
    .with_api_key(org_conf.api_key.expose_secret())
    .with_base_path(MESA_API_BASE_URL)
    .build();
```

---

### Task 3: Fix `org.rs` — imports, types, and API calls

**Files:**
- Modify: `src/fs/mescloud/org.rs`

**Step 1: Update the import**

Change line 7 from:
```rust
use mesa_dev::Mesa as MesaClient;
```
To:
```rust
use mesa_dev::MesaClient;
```

**Step 2: Update `wait_for_sync` method**

The old `Repo` model had a `status` field; the new one does not. Change the method signature and body (lines 282-293).

From:
```rust
async fn wait_for_sync(
    &self,
    repo_name: &str,
) -> Result<mesa_dev::models::Repo, mesa_dev::error::MesaError> {
    let mut repo = self.client.repos(&self.name).get(repo_name).await?;
    while repo.status.is_some() {
        trace!(repo = repo_name, "repo is syncing, waiting...");
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        repo = self.client.repos(&self.name).get(repo_name).await?;
    }
    Ok(repo)
}
```

To:
```rust
async fn wait_for_sync(
    &self,
    repo_name: &str,
) -> Result<mesa_dev::models::PostByOrgRepos201Response, String> {
    self.client
        .org(&self.name)
        .repos()
        .at(repo_name)
        .get()
        .await
        .map_err(|e| e.to_string())
}
```

**Step 3: Update `lookup` — OrgRoot branch**

In lines 375-381, the `?` on `wait_for_sync` now returns a `String` error. Update to map into `LookupError`:

From:
```rust
let repo = self.wait_for_sync(name_str).await?;

let (ino, attr) = self.ensure_repo_inode(
    name_str,
    name_str,
    &repo.default_branch,
    Self::ROOT_INO,
);
```

To:
```rust
let repo = self
    .wait_for_sync(name_str)
    .await
    .map_err(LookupError::RemoteMesaError)?;

let default_branch = repo.default_branch.as_deref().unwrap_or("main");
let (ino, attr) = self.ensure_repo_inode(
    name_str,
    name_str,
    default_branch,
    Self::ROOT_INO,
);
```

**Step 4: Update `lookup` — OwnerDir branch**

Similarly update lines 408-411:

From:
```rust
let repo = self.wait_for_sync(&encoded).await?;

let (ino, attr) =
    self.ensure_repo_inode(&encoded, repo_name_str, &repo.default_branch, parent);
```

To:
```rust
let repo = self
    .wait_for_sync(&encoded)
    .await
    .map_err(LookupError::RemoteMesaError)?;

let default_branch = repo.default_branch.as_deref().unwrap_or("main");
let (ino, attr) =
    self.ensure_repo_inode(&encoded, repo_name_str, default_branch, parent);
```

**Step 5: Update `readdir` — OrgRoot branch**

Change the repo listing (lines 467-478) from:
```rust
let repos: Vec<mesa_dev::models::Repo> = self
    .client
    .repos(&self.name)
    .list_all()
    .try_collect()
    .await?;

let repo_infos: Vec<(String, String)> = repos
    .into_iter()
    .filter(|r| r.status.is_none()) // skip repos still syncing
    .map(|r| (r.name, r.default_branch))
    .collect();
```

To:
```rust
let repos: Vec<mesa_dev::models::GetByOrgRepos200ResponseReposInner> = self
    .client
    .org(&self.name)
    .repos()
    .list(None)
    .try_collect()
    .await
    .map_err(|e| ReadDirError::RemoteMesaError(e.to_string()))?;

let repo_infos: Vec<(String, String)> = repos
    .into_iter()
    .filter_map(|r| {
        let name = r.name?;
        let branch = r.default_branch.unwrap_or_else(|| "main".to_owned());
        Some((name, branch))
    })
    .collect();
```

---

### Task 4: Fix `repo.rs` — imports, content API, and pattern matching

**Files:**
- Modify: `src/fs/mescloud/repo.rs`

**Step 1: Update imports**

Change line 9 from:
```rust
use mesa_dev::Mesa as MesaClient;
```
To:
```rust
use mesa_dev::MesaClient;
use mesa_dev::low_level::content::{Content, DirEntry as MesaDirEntry};
```

**Step 2: Update `lookup` — content API call**

Change lines 121-125 from:
```rust
let content = self
    .client
    .content(&self.org_name, &self.repo_name)
    .get(file_path.as_deref(), Some(self.ref_.as_str()))
    .await?;
```

To:
```rust
let content = self
    .client
    .org(&self.org_name)
    .repos()
    .at(&self.repo_name)
    .content()
    .get(Some(self.ref_.as_str()), file_path.as_deref(), None)
    .await
    .map_err(|e| LookupError::RemoteMesaError(e.to_string()))?;
```

Note: parameter order changed from `(path, ref)` to `(ref, path, depth)`.

**Step 3: Update `lookup` — Content pattern matching**

Change lines 127-144 from:
```rust
let kind = match &content {
    mesa_dev::models::Content::File { .. } => DirEntryType::RegularFile,
    mesa_dev::models::Content::Dir { .. } => DirEntryType::Directory,
};

let (ino, _) = self.icache.ensure_child_inode(parent, name, kind);

let now = SystemTime::now();
let attr = match content {
    mesa_dev::models::Content::File { size, .. } => FileAttr::RegularFile {
        common: self.icache.make_common_file_attr(ino, 0o644, now, now),
        size,
        blocks: mescloud_icache::blocks_of_size(Self::BLOCK_SIZE, size),
    },
    mesa_dev::models::Content::Dir { .. } => FileAttr::Directory {
        common: self.icache.make_common_file_attr(ino, 0o755, now, now),
    },
};
```

To:
```rust
let kind = match &content {
    Content::File(_) | Content::Symlink(_) => DirEntryType::RegularFile,
    Content::Dir(_) => DirEntryType::Directory,
};

let (ino, _) = self.icache.ensure_child_inode(parent, name, kind);

let now = SystemTime::now();
let attr = match &content {
    Content::File(f) | Content::Symlink(f) => {
        #[expect(clippy::cast_sign_loss, clippy::cast_possible_truncation)]
        let size = f.size as u64;
        FileAttr::RegularFile {
            common: self.icache.make_common_file_attr(ino, 0o644, now, now),
            size,
            blocks: mescloud_icache::blocks_of_size(Self::BLOCK_SIZE, size),
        }
    }
    Content::Dir(_) => FileAttr::Directory {
        common: self.icache.make_common_file_attr(ino, 0o755, now, now),
    },
};
```

**Step 4: Update `readdir` — content API call**

Change lines 180-184 from:
```rust
let content = self
    .client
    .content(&self.org_name, &self.repo_name)
    .get(file_path.as_deref(), Some(self.ref_.as_str()))
    .await?;
```

To:
```rust
let content = self
    .client
    .org(&self.org_name)
    .repos()
    .at(&self.repo_name)
    .content()
    .get(Some(self.ref_.as_str()), file_path.as_deref(), None)
    .await
    .map_err(|e| ReadDirError::RemoteMesaError(e.to_string()))?;
```

**Step 5: Update `readdir` — Content + DirEntry pattern matching**

Change lines 186-200 from:
```rust
let mesa_entries = match content {
    mesa_dev::models::Content::Dir { entries, .. } => entries,
    mesa_dev::models::Content::File { .. } => return Err(ReadDirError::NotADirectory),
};

let collected: Vec<_> = mesa_entries
    .into_iter()
    .map(|e| {
        let kind = match e.entry_type {
            mesa_dev::models::DirEntryType::File => DirEntryType::RegularFile,
            mesa_dev::models::DirEntryType::Dir => DirEntryType::Directory,
        };
        (e.name, kind)
    })
    .collect();
```

To:
```rust
let mesa_entries = match content {
    Content::Dir(d) => d.entries,
    Content::File(_) | Content::Symlink(_) => return Err(ReadDirError::NotADirectory),
};

let collected: Vec<(String, DirEntryType)> = mesa_entries
    .into_iter()
    .filter_map(|e| {
        let (name, kind) = match e {
            MesaDirEntry::File(f) => (f.name?, DirEntryType::RegularFile),
            MesaDirEntry::Symlink(s) => (s.name?, DirEntryType::RegularFile),
            MesaDirEntry::Dir(d) => (d.name?, DirEntryType::Directory),
        };
        Some((name, kind))
    })
    .collect();
```

The explicit `Vec<(String, DirEntryType)>` annotation resolves the E0282 type inference error on `OsStr::new(name)` at line 206.

**Step 6: Update `read` — content API call and pattern matching**

Change lines 271-280 from:
```rust
let content = self
    .client
    .content(&self.org_name, &self.repo_name)
    .get(file_path.as_deref(), Some(self.ref_.as_str()))
    .await?;

let encoded_content = match content {
    mesa_dev::models::Content::File { content, .. } => content,
    mesa_dev::models::Content::Dir { .. } => return Err(ReadError::NotAFile),
};
```

To:
```rust
let content = self
    .client
    .org(&self.org_name)
    .repos()
    .at(&self.repo_name)
    .content()
    .get(Some(self.ref_.as_str()), file_path.as_deref(), None)
    .await
    .map_err(|e| ReadError::RemoteMesaError(e.to_string()))?;

let encoded_content = match content {
    Content::File(f) | Content::Symlink(f) => {
        f.content.unwrap_or_default()
    }
    Content::Dir(_) => return Err(ReadError::NotAFile),
};
```

---

### Task 5: Build and verify

**Step 1: Run cargo build**

Run: `cargo build`
Expected: Successful compilation with no errors.

**Step 2: Fix any remaining warnings or errors**

If there are clippy warnings about `wildcard_enum_match_arm` or other lints, address them. The project has strict clippy settings (`clippy::all = "deny"`, `clippy::pedantic = "warn"`).

---

### Task 6: Commit

**Step 1: Stage and commit**

```bash
git add src/fs/mescloud/common.rs src/fs/mescloud/mod.rs src/fs/mescloud/org.rs src/fs/mescloud/repo.rs
git commit -m "Migrate mesa-dev from 0.1.1 to 1.8.0

Update all API call sites to use the new hierarchical client
pattern (client.org().repos().at().content()), new Content/DirEntry
enums from mesa_dev::low_level::content, and string-based error
wrapping since the error type is now generic per endpoint."
```
