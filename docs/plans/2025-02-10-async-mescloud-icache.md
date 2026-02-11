# Async Mescloud ICache Migration

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Rewrite `src/fs/mescloud/icache.rs` to use `AsyncICache`, with resolvers that fetch from the mesa backend.

**Architecture:** `MescloudICache` becomes generic over `R: IcbResolver`. Each filesystem layer provides its own resolver: `RepoResolver` fetches file/directory metadata from the mesa content API, `OrgResolver` validates repos via the repo API, and `MesaResolver` creates static org directory entries. The `IcbLike` trait gains a `needs_resolve()` method so the cache can distinguish "stub" entries (parent+path known, attr not yet fetched) from fully resolved entries. `get_or_resolve` handles both missing entries and stubs that need resolution.

**Tech Stack:** Rust, `scc::HashMap` (already in Cargo.toml), `tokio::sync::watch`, `async_trait`, `mesa_dev::MesaClient`

---

## Key Design Decisions

### Stub + Resolve pattern

When `lookup(parent, name)` is called, the flow is:
1. `ensure_child_ino(parent, name)` — find existing child or allocate a new inode; insert a **stub** ICB (`parent` + `path` set, `attr: None`) if the entry is new.
2. `get_or_resolve(ino, |icb| icb.attr)` — if the stub's `needs_resolve()` returns true (attr is None), the cache transitions the entry to `InFlight`, calls the resolver, and transitions to `Available` once the resolver returns a fully populated ICB with attr.
3. Concurrent callers for the same inode coalesce: they see `InFlight` and wait.

This lets the resolver access the stub's `parent` and `path` fields to build the API path, without needing a separate context parameter.

### Resolver receives `stub: Option<Self::Icb>` + `cache: &AsyncICache<Self>`

The resolver signature is:
```rust
fn resolve(&self, ino: Inode, stub: Option<Self::Icb>, cache: &AsyncICache<Self>) -> Future<Result<Self::Icb, Self::Error>>
```

- `stub`: `Some(icb)` when upgrading a stub entry, `None` when creating from scratch.
- `cache`: lets the resolver walk the parent chain to build paths (e.g., `cache.get_icb(parent, |icb| icb.path.clone())`).

### `get_or_resolve` handles stubs

The existing `get_or_resolve` only resolves **missing** entries. We extend it to also resolve **Available entries where `icb.needs_resolve()` is true**:

| Current state | Action |
|---|---|
| Available + `!needs_resolve()` | Return immediately (fast path) |
| Available + `needs_resolve()` | Extract stub → InFlight → resolve → Available |
| InFlight | Wait for resolution |
| Vacant | InFlight → resolve → Available |

### `readdir` bypasses the resolver

`readdir` makes a single API call that returns all children. It inserts fully-populated ICBs directly via `insert_icb` (not via the resolver). This is a batch optimization — the resolver is for per-inode resolution.

### `MescloudICache<R>` is generic

```rust
pub struct MescloudICache<R: IcbResolver<Icb = InodeControlBlock>> {
    inner: AsyncICache<R>,
    inode_factory: InodeFactory,
    fs_owner: (u32, u32),
    block_size: u32,
}
```

Each FS layer instantiates with its own resolver:
- `RepoFs` → `MescloudICache<RepoResolver>` — resolver calls mesa content API
- `OrgFs` → `MescloudICache<OrgResolver>` — resolver validates repos + creates directory attrs
- `MesaFS` → `MescloudICache<MesaResolver>` — resolver creates static directory attrs

### `readdir_buf` replaces `children`

The `Fs::readdir` trait returns `&[DirEntry]` borrowed from `&mut self`. The async cache's closure-based API can't return references that outlive the closure. Each FS struct gets a `readdir_buf: Vec<DirEntry>` field. The `children` field on `InodeControlBlock` is removed.

### `make_common_file_attr` becomes a free function

Currently a method on `MescloudICache` (uses `self.fs_owner`, `self.block_size`). Becomes a free function so resolvers can call it too.

---

## Task 1: Extend IcbLike trait with `needs_resolve`

**Files:**
- Modify: `src/fs/icache/mod.rs`

**Step 1: Add `needs_resolve` to IcbLike**

```rust
pub trait IcbLike {
    fn new_root(path: std::path::PathBuf) -> Self;
    fn rc(&self) -> u64;
    fn rc_mut(&mut self) -> &mut u64;
    /// Returns true if this entry needs resolution (e.g., attr not yet fetched).
    fn needs_resolve(&self) -> bool;
}
```

**Step 2: Update existing IcbLike implementations**

In `src/fs/local.rs`, the `InodeControlBlock` for local FS:
```rust
fn needs_resolve(&self) -> bool {
    false // local FS entries are always fully resolved
}
```

In `src/fs/mescloud/icache.rs`:
```rust
fn needs_resolve(&self) -> bool {
    self.attr.is_none()
}
```

In `src/fs/icache/async_cache.rs` tests, the `TestIcb`:
```rust
fn needs_resolve(&self) -> bool {
    false
}
```

**Step 3: Verify compilation**

Run: `cargo check -p git-fs`

**Step 4: Commit**

```bash
git add src/fs/icache/mod.rs src/fs/local.rs src/fs/mescloud/icache.rs src/fs/icache/async_cache.rs
git commit -m "feat(icache): add needs_resolve to IcbLike trait"
```

---

## Task 2: Modify IcbResolver trait and AsyncICache

**Files:**
- Modify: `src/fs/icache/async_cache.rs`
- Modify: `src/fs/icache/mod.rs`

**Step 1: Update IcbResolver trait**

In `async_cache.rs`, change the resolver to receive stub data and cache reference:

```rust
pub trait IcbResolver: Send + Sync {
    type Icb: IcbLike + Send + Sync;
    type Error: Send;

    /// Resolve an inode to a fully-populated control block.
    ///
    /// - `stub`: `Some(icb)` if upgrading an existing stub entry, `None` if creating
    ///   from scratch. The stub typically has `parent` and `path` set but `attr` missing.
    /// - `cache`: reference to the cache, useful for walking parent chains to build paths.
    fn resolve(
        &self,
        ino: Inode,
        stub: Option<Self::Icb>,
        cache: &AsyncICache<Self>,
    ) -> impl Future<Output = Result<Self::Icb, Self::Error>> + Send
    where
        Self: Sized;
}
```

**Step 2: Update `get_or_resolve` to handle stubs**

Rewrite `get_or_resolve` in `AsyncICache<R>`:

```rust
pub async fn get_or_resolve<T>(
    &self,
    ino: Inode,
    then: impl FnOnce(&R::Icb) -> T,
) -> Result<T, R::Error> {
    use scc::hash_map::Entry;

    let mut then_fn = Some(then);

    // Fast path: Available and fully resolved
    {
        let hit = self
            .inode_table
            .read_async(&ino, |_, s| match s {
                IcbState::Available(icb) if !icb.needs_resolve() => {
                    let t = then_fn.take().unwrap_or_else(|| unreachable!());
                    Some(t(icb))
                }
                _ => None,
            })
            .await;
        if let Some(Some(r)) = hit {
            return Ok(r);
        }
    }

    // Slow path: missing, InFlight, or stub needing resolution
    loop {
        match self.inode_table.entry_async(ino).await {
            Entry::Occupied(mut occ) => match occ.get_mut() {
                IcbState::Available(icb) if !icb.needs_resolve() => {
                    let t = then_fn.take().unwrap_or_else(|| unreachable!());
                    return Ok(t(icb));
                }
                IcbState::Available(_) => {
                    // Stub needing resolution — extract stub, replace with InFlight
                    let (tx, rx) = watch::channel(());
                    let old = std::mem::replace(occ.get_mut(), IcbState::InFlight(rx));
                    let stub = match old {
                        IcbState::Available(icb) => icb,
                        _ => unreachable!(),
                    };
                    drop(occ); // release shard lock before awaiting

                    match self.resolver.resolve(ino, Some(stub), self).await {
                        Ok(icb) => {
                            let t = then_fn.take().unwrap_or_else(|| unreachable!());
                            let result = t(&icb);
                            self.inode_table
                                .upsert_async(ino, IcbState::Available(icb))
                                .await;
                            drop(tx);
                            return Ok(result);
                        }
                        Err(e) => {
                            self.inode_table.remove_async(&ino).await;
                            drop(tx);
                            return Err(e);
                        }
                    }
                }
                IcbState::InFlight(rx) => {
                    let mut rx = rx.clone();
                    drop(occ);
                    let _ = rx.changed().await;
                }
            },
            Entry::Vacant(vac) => {
                let (tx, rx) = watch::channel(());
                vac.insert_entry(IcbState::InFlight(rx));

                match self.resolver.resolve(ino, None, self).await {
                    Ok(icb) => {
                        let t = then_fn.take().unwrap_or_else(|| unreachable!());
                        let result = t(&icb);
                        self.inode_table
                            .upsert_async(ino, IcbState::Available(icb))
                            .await;
                        drop(tx);
                        return Ok(result);
                    }
                    Err(e) => {
                        self.inode_table.remove_async(&ino).await;
                        drop(tx);
                        return Err(e);
                    }
                }
            }
        }
    }
}
```

**Step 3: Add `get_icb_mut_sync` for initialization**

Add after `for_each`:

```rust
/// Synchronous mutable access to an `Available` entry.
/// Does **not** wait for `InFlight`. Intended for initialization.
pub fn get_icb_mut_sync<T>(&self, ino: Inode, f: impl FnOnce(&mut R::Icb) -> T) -> Option<T> {
    self.inode_table
        .update(&ino, |_, state| match state {
            IcbState::Available(icb) => Some(f(icb)),
            IcbState::InFlight(_) => None,
        })
        .flatten()
}
```

**Step 4: Update existing tests**

All test resolvers need the new signature. Update `TestResolver`:

```rust
impl IcbResolver for TestResolver {
    type Icb = TestIcb;
    type Error = String;

    fn resolve(
        &self,
        ino: Inode,
        _stub: Option<Self::Icb>,
        _cache: &AsyncICache<Self>,
    ) -> impl Future<Output = Result<Self::Icb, Self::Error>> + Send {
        let result = self.responses.lock().expect("test mutex").remove(&ino)
            .unwrap_or_else(|| Err(format!("no response for inode {ino}")));
        async move { result }
    }
}
```

Similarly update `CountingResolver`. Also update `TestIcb` to implement `needs_resolve`:

```rust
impl IcbLike for TestIcb {
    // ... existing methods ...
    fn needs_resolve(&self) -> bool {
        false
    }
}
```

**Step 5: Add test for stub resolution**

```rust
#[tokio::test]
async fn get_or_resolve_resolves_stubs() {
    let resolver = TestResolver::new();
    resolver.add(42, TestIcb { rc: 1, path: "/resolved".into() });
    let cache = test_cache_with(resolver);

    // Insert a stub that needs_resolve
    // We need a TestIcb variant that returns true for needs_resolve...
    // For this test, use a NeedsResolveIcb or modify TestIcb.
    // Simplest: make TestIcb.needs_resolve configurable.
}
```

Note: to properly test stub resolution, `TestIcb` needs a way to signal `needs_resolve() == true`. Add an optional field:

```rust
#[derive(Debug, Clone, PartialEq)]
struct TestIcb {
    rc: u64,
    path: PathBuf,
    resolved: bool, // defaults to true in existing tests
}

impl IcbLike for TestIcb {
    fn needs_resolve(&self) -> bool {
        !self.resolved
    }
    // ...
}
```

Then test:
```rust
#[tokio::test]
async fn get_or_resolve_resolves_stub_entry() {
    let resolver = TestResolver::new();
    resolver.add(42, TestIcb { rc: 1, path: "/resolved".into(), resolved: true });
    let cache = test_cache_with(resolver);

    // Insert unresolved stub
    cache.insert_icb(42, TestIcb { rc: 0, path: "/stub".into(), resolved: false }).await;

    // get_or_resolve should trigger resolution because needs_resolve() == true
    let path: Result<PathBuf, String> = cache.get_or_resolve(42, |icb| icb.path.clone()).await;
    assert_eq!(path, Ok(PathBuf::from("/resolved")));
}
```

**Step 6: Run tests**

Run: `cargo test -p git-fs --lib fs::icache::async_cache::tests`
Expected: PASS

**Step 7: Update icache/mod.rs exports**

Remove `#[cfg_attr(not(test), expect(dead_code))]` and `#[expect(unused_imports)]` annotations.

**Step 8: Commit**

```bash
git add src/fs/icache/async_cache.rs src/fs/icache/mod.rs
git commit -m "feat(icache): extend IcbResolver with stub+cache params, handle stubs in get_or_resolve"
```

---

## Task 3: Make InodeFactory atomic

**Files:**
- Modify: `src/fs/icache/inode_factory.rs`

**Step 1: Rewrite with AtomicU64**

```rust
use std::sync::atomic::{AtomicU64, Ordering};
use crate::fs::r#trait::Inode;

pub struct InodeFactory {
    next_inode: AtomicU64,
}

impl InodeFactory {
    pub fn new(start: Inode) -> Self {
        Self { next_inode: AtomicU64::new(start) }
    }

    pub fn allocate(&self) -> Inode {
        self.next_inode.fetch_add(1, Ordering::Relaxed)
    }
}
```

**Step 2: Add tests, verify, commit**

```bash
git add src/fs/icache/inode_factory.rs
git commit -m "feat(icache): make InodeFactory atomic"
```

---

## Task 4: Rewrite MescloudICache as generic wrapper

**Files:**
- Modify: `src/fs/mescloud/icache.rs`

This is the core rewrite. `MescloudICache` becomes `MescloudICache<R>` generic over the resolver.

**Step 1: Define the new MescloudICache**

```rust
use std::ffi::OsStr;
use std::time::SystemTime;

use tracing::warn;

use crate::fs::icache::{AsyncICache, IcbLike, IcbResolver, InodeFactory};
use crate::fs::r#trait::{
    CommonFileAttr, DirEntryType, FileAttr, FileHandle, FilesystemStats, Inode, Permissions,
};

pub struct InodeControlBlock {
    pub parent: Option<Inode>,
    pub rc: u64,
    pub path: std::path::PathBuf,
    pub attr: Option<FileAttr>,
}

impl IcbLike for InodeControlBlock {
    fn new_root(path: std::path::PathBuf) -> Self {
        Self { rc: 1, parent: None, path, attr: None }
    }
    fn rc(&self) -> u64 { self.rc }
    fn rc_mut(&mut self) -> &mut u64 { &mut self.rc }
    fn needs_resolve(&self) -> bool { self.attr.is_none() }
}

/// Free function — usable by both MescloudICache and resolvers.
pub fn make_common_file_attr(
    ino: Inode, perm: u16, atime: SystemTime, mtime: SystemTime,
    fs_owner: (u32, u32), block_size: u32,
) -> CommonFileAttr {
    CommonFileAttr {
        ino, atime, mtime,
        ctime: SystemTime::UNIX_EPOCH,
        crtime: SystemTime::UNIX_EPOCH,
        perm: Permissions::from_bits_truncate(perm),
        nlink: 1,
        uid: fs_owner.0,
        gid: fs_owner.1,
        blksize: block_size,
    }
}

pub fn blocks_of_size(block_size: u32, size: u64) -> u64 {
    size.div_ceil(u64::from(block_size))
}

pub struct MescloudICache<R: IcbResolver<Icb = InodeControlBlock>> {
    inner: AsyncICache<R>,
    inode_factory: InodeFactory,
    fs_owner: (u32, u32),
    block_size: u32,
}
```

**Step 2: Implement methods**

Key methods (all `&self`):

```rust
impl<R: IcbResolver<Icb = InodeControlBlock>> MescloudICache<R> {
    pub fn new(resolver: R, root_ino: Inode, fs_owner: (u32, u32), block_size: u32) -> Self { ... }

    // Delegated from AsyncICache (async):
    pub async fn contains(&self, ino: Inode) -> bool { ... }
    pub async fn get_icb<T>(&self, ino: Inode, f: impl FnOnce(&InodeControlBlock) -> T) -> Option<T> { ... }
    pub async fn get_icb_mut<T>(&self, ino: Inode, f: impl FnOnce(&mut InodeControlBlock) -> T) -> Option<T> { ... }
    pub async fn insert_icb(&self, ino: Inode, icb: InodeControlBlock) { ... }
    pub async fn entry_or_insert_icb<T>(&self, ino: Inode, factory: impl FnOnce() -> InodeControlBlock, then: impl FnOnce(&mut InodeControlBlock) -> T) -> T { ... }
    pub async fn inc_rc(&self, ino: Inode) -> u64 { ... }
    pub async fn forget(&self, ino: Inode, nlookups: u64) -> Option<InodeControlBlock> { ... }
    pub async fn get_or_resolve<T>(&self, ino: Inode, then: impl FnOnce(&InodeControlBlock) -> T) -> Result<T, R::Error> { ... }

    // Delegated (sync):
    pub fn allocate_fh(&self) -> FileHandle { ... }
    pub fn for_each(&self, f: impl FnMut(&Inode, &InodeControlBlock)) { ... }
    pub fn inode_count(&self) -> usize { ... }

    // Domain-specific:
    pub fn allocate_inode(&self) -> Inode { ... }
    pub async fn get_attr(&self, ino: Inode) -> Option<FileAttr> { ... }
    pub async fn cache_attr(&self, ino: Inode, attr: FileAttr) { ... }
    pub fn fs_owner(&self) -> (u32, u32) { ... }
    pub fn block_size(&self) -> u32 { ... }
    pub fn statfs(&self) -> FilesystemStats { ... }

    /// Find an existing child inode by (parent, name), or allocate a new one.
    /// If the entry is new, inserts a stub ICB (parent+path set, attr=None).
    pub async fn ensure_child_ino(&self, parent: Inode, name: &OsStr) -> Inode { ... }
}
```

Notable changes from old `MescloudICache`:
- `new()` takes a `resolver: R` parameter
- `make_common_file_attr` is now a free function (exported from module)
- `ensure_child_inode` is split: `ensure_child_ino` (finds/allocates + inserts stub) + `get_or_resolve` (resolves via resolver)
- `children` field removed from `InodeControlBlock`
- Constructor uses `get_icb_mut_sync` to set root attr

**Step 3: Write tests for MescloudICache**

Create tests using a `TestResolver` that creates simple directory/file ICBs:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    struct TestMescloudResolver {
        fs_owner: (u32, u32),
        block_size: u32,
    }

    impl IcbResolver for TestMescloudResolver {
        type Icb = InodeControlBlock;
        type Error = String;

        fn resolve(
            &self, ino: Inode, stub: Option<InodeControlBlock>,
            _cache: &AsyncICache<Self>,
        ) -> impl Future<Output = Result<InodeControlBlock, String>> + Send {
            let fs_owner = self.fs_owner;
            let block_size = self.block_size;
            async move {
                let stub = stub.ok_or("no stub")?;
                let now = SystemTime::now();
                let attr = FileAttr::Directory {
                    common: make_common_file_attr(ino, 0o755, now, now, fs_owner, block_size),
                };
                Ok(InodeControlBlock { attr: Some(attr), ..stub })
            }
        }
    }

    // Tests: new_creates_root_with_attr, ensure_child_ino_allocates,
    // get_or_resolve_populates_attr, etc.
}
```

**Step 4: Verify, commit**

```bash
git add src/fs/mescloud/icache.rs
git commit -m "feat(mescloud): rewrite MescloudICache as generic over IcbResolver"
```

---

## Task 5: Implement RepoResolver + update RepoFs

**Files:**
- Modify: `src/fs/mescloud/repo.rs`

**Step 1: Define RepoResolver**

```rust
use super::icache::{blocks_of_size, make_common_file_attr, InodeControlBlock, MescloudICache};
use crate::fs::icache::{AsyncICache, IcbLike, IcbResolver};

pub(super) struct RepoResolver {
    client: MesaClient,
    org_name: String,
    repo_name: String,
    ref_: String,
    fs_owner: (u32, u32),
    block_size: u32,
}

impl IcbResolver for RepoResolver {
    type Icb = InodeControlBlock;
    type Error = LookupError;

    fn resolve(
        &self,
        ino: Inode,
        stub: Option<InodeControlBlock>,
        cache: &AsyncICache<Self>,
    ) -> impl Future<Output = Result<InodeControlBlock, LookupError>> + Send
    where
        Self: Sized,
    {
        // Move data needed by the async block
        let client = self.client.clone();
        let org_name = self.org_name.clone();
        let repo_name = self.repo_name.clone();
        let ref_ = self.ref_.clone();
        let fs_owner = self.fs_owner;
        let block_size = self.block_size;

        async move {
            let stub = stub.expect("RepoResolver requires a stub ICB with parent+path");
            let parent = stub.parent.expect("non-root inodes have parents");

            // Build repo-relative path by walking parent chain
            let file_path = build_path_from_cache(parent, &stub.path, cache).await;

            // Fetch from mesa content API
            let content = client
                .org(&org_name).repos().at(&repo_name).content()
                .get(Some(ref_.as_str()), file_path.as_deref(), None)
                .await
                .map_err(MesaApiError::from)?;

            let now = std::time::SystemTime::now();
            let attr = match &content {
                Content::File(f) => {
                    let size = f.size.to_u64().unwrap_or(0);
                    FileAttr::RegularFile {
                        common: make_common_file_attr(ino, 0o644, now, now, fs_owner, block_size),
                        size,
                        blocks: blocks_of_size(block_size, size),
                    }
                }
                Content::Symlink(s) => {
                    let size = s.size.to_u64().unwrap_or(0);
                    FileAttr::RegularFile {
                        common: make_common_file_attr(ino, 0o644, now, now, fs_owner, block_size),
                        size,
                        blocks: blocks_of_size(block_size, size),
                    }
                }
                Content::Dir(_) => FileAttr::Directory {
                    common: make_common_file_attr(ino, 0o755, now, now, fs_owner, block_size),
                },
            };

            Ok(InodeControlBlock {
                parent: stub.parent,
                path: stub.path,
                rc: stub.rc,
                attr: Some(attr),
            })
        }
    }
}

/// Walk the parent chain in the cache to build the repo-relative path.
async fn build_path_from_cache(
    parent: Inode,
    name: &std::path::Path,
    cache: &AsyncICache<RepoResolver>,
) -> Option<String> {
    use std::path::PathBuf;

    let mut components = vec![name.to_path_buf()];
    let mut current = parent;
    while current != RepoFs::ROOT_INO {
        let (path, next_parent) = cache
            .get_icb(current, |icb| (icb.path.clone(), icb.parent))
            .await?;
        components.push(path);
        current = next_parent?;
    }
    components.reverse();
    let joined: PathBuf = components.iter().collect();
    joined.to_str().map(String::from)
}
```

**Step 2: Update RepoFs struct**

```rust
pub struct RepoFs {
    icache: MescloudICache<RepoResolver>,
    readdir_buf: Vec<DirEntry>,
    open_files: HashMap<FileHandle, Inode>,
}
```

Constructor creates the resolver and passes it to `MescloudICache::new()`. The `client`, `org_name`, `repo_name`, `ref_` move into the resolver. `path_of_inode` and `path_of_child` are removed (path building is now in the resolver + `build_path_from_cache`).

**Step 3: Update `lookup` to use get_or_resolve**

```rust
async fn lookup(&mut self, parent: Inode, name: &OsStr) -> Result<FileAttr, LookupError> {
    let ino = self.icache.ensure_child_ino(parent, name).await;
    let attr = self.icache.get_or_resolve(ino, |icb| {
        icb.attr.expect("resolver should populate attr")
    }).await?;
    self.icache.inc_rc(ino).await;
    Ok(attr)
}
```

**Step 4: Update `readdir`**

Readdir still calls the API directly (batch operation). For each child, uses `ensure_child_ino` + `insert_icb` with full attr. Uses `readdir_buf` for return.

Note: readdir needs the path for the API call. Since `path_of_inode` was removed, add a helper method on `MescloudICache` or use `build_path_from_cache` directly. Actually, `path_of_inode` should stay on `RepoFs` (or become a method that uses the icache). Keep it as an async method that walks the parent chain.

**Step 5: Update `read`**

`read` still calls the API directly (data transfer, not metadata caching). Needs `path_of_inode` for the path.

**Step 6: Update remaining Fs methods**

- `getattr`: `self.icache.get_attr(ino).await.ok_or(...)` (unchanged pattern)
- `open`: `self.icache.contains(ino).await`, `self.icache.allocate_fh()`
- `forget`: `self.icache.forget(ino, nlookups).await`
- `statfs`: `self.icache.statfs()`

**Step 7: Verify compilation + commit**

```bash
git add src/fs/mescloud/repo.rs
git commit -m "feat(mescloud): implement RepoResolver, update RepoFs to use async icache"
```

---

## Task 6: Implement OrgResolver + update OrgFs

**Files:**
- Modify: `src/fs/mescloud/org.rs`

**Step 1: Define OrgResolver**

```rust
pub(super) struct OrgResolver {
    client: MesaClient,
    org_name: String,
    fs_owner: (u32, u32),
    block_size: u32,
}

impl IcbResolver for OrgResolver {
    type Icb = InodeControlBlock;
    type Error = LookupError;

    fn resolve(
        &self, ino: Inode, stub: Option<InodeControlBlock>,
        _cache: &AsyncICache<Self>,
    ) -> impl Future<Output = Result<InodeControlBlock, LookupError>> + Send {
        let client = self.client.clone();
        let org_name = self.org_name.clone();
        let fs_owner = self.fs_owner;
        let block_size = self.block_size;

        async move {
            let stub = stub.expect("OrgResolver requires stub");

            // Determine if this is a repo or owner dir.
            // For now, all org-level inodes are directories.
            // Repo validation is done by the caller before get_or_resolve.
            let now = SystemTime::now();
            let attr = FileAttr::Directory {
                common: make_common_file_attr(ino, 0o755, now, now, fs_owner, block_size),
            };

            Ok(InodeControlBlock { attr: Some(attr), ..stub })
        }
    }
}
```

Note: The OrgResolver creates directory ICBs. Repo validation (the `wait_for_sync` API call) stays in `OrgFs::lookup` as a pre-check before `get_or_resolve`. This keeps the resolver simple and the validation/orchestration logic (creating `RepoFs`, bridge mappings) in `OrgFs`.

**Step 2: Update OrgFs struct**

```rust
pub struct OrgFs {
    name: String,
    client: MesaClient,
    icache: MescloudICache<OrgResolver>,
    readdir_buf: Vec<DirEntry>,
    repo_inodes: HashMap<Inode, usize>,
    owner_inodes: HashMap<Inode, String>,
    repos: Vec<RepoSlot>,
}
```

**Step 3: Update helper methods (async)**

- `repo_slot_for_inode` → async (walks parent chain via `get_icb(...).await`)
- `inode_role` → async (calls `repo_slot_for_inode`)
- `ensure_owner_inode` → async (calls icache methods with `.await`)
- `ensure_repo_inode` → async
- `translate_repo_ino_to_org` → async
- `inode_table_get_attr` → async

**Step 4: Update Fs trait implementations**

Same patterns as Task 5: add `.await` to icache calls, use `readdir_buf`, update `inode_role(...).await`, etc.

**Step 5: Verify compilation + commit**

```bash
git add src/fs/mescloud/org.rs
git commit -m "feat(mescloud): implement OrgResolver, update OrgFs to use async icache"
```

---

## Task 7: Implement MesaResolver + update MesaFS

**Files:**
- Modify: `src/fs/mescloud/mod.rs`

**Step 1: Define MesaResolver**

```rust
pub(super) struct MesaResolver {
    fs_owner: (u32, u32),
    block_size: u32,
}

impl IcbResolver for MesaResolver {
    type Icb = InodeControlBlock;
    type Error = std::convert::Infallible;

    fn resolve(
        &self, ino: Inode, stub: Option<InodeControlBlock>,
        _cache: &AsyncICache<Self>,
    ) -> impl Future<Output = Result<InodeControlBlock, Infallible>> + Send {
        let fs_owner = self.fs_owner;
        let block_size = self.block_size;
        async move {
            let stub = stub.unwrap_or_else(|| InodeControlBlock {
                parent: None, path: "/".into(), rc: 0, attr: None,
            });
            let now = SystemTime::now();
            let attr = FileAttr::Directory {
                common: make_common_file_attr(ino, 0o755, now, now, fs_owner, block_size),
            };
            Ok(InodeControlBlock { attr: Some(attr), ..stub })
        }
    }
}
```

**Step 2: Update MesaFS struct**

```rust
pub struct MesaFS {
    icache: MescloudICache<MesaResolver>,
    readdir_buf: Vec<DirEntry>,
    org_inodes: HashMap<Inode, usize>,
    org_slots: Vec<OrgSlot>,
}
```

**Step 3: Update helper methods + Fs implementations**

Same patterns: make helper methods async, add `.await`, use `readdir_buf`, update `inode_role(ino).await`, etc.

**Step 4: Verify full compilation + commit**

```bash
git add src/fs/mescloud/mod.rs
git commit -m "feat(mescloud): implement MesaResolver, update MesaFS to use async icache"
```

---

## Task 8: Update common.rs + clean up

**Files:**
- Modify: `src/fs/mescloud/common.rs`
- Modify: `src/fs/icache/mod.rs`

**Step 1: Update InodeControlBlock re-export**

The `children` field was removed. Verify `common.rs` still compiles with the new ICB structure.

**Step 2: Clean up icache/mod.rs exports**

Remove dead-code annotations, ensure `AsyncICache`, `IcbResolver`, `IcbLike` are all exported cleanly.

**Step 3: Run full test suite**

Run: `cargo test -p git-fs`
Run: `cargo clippy -p git-fs`

**Step 4: Final commit**

```bash
git add -A
git commit -m "chore: clean up async icache migration"
```

---

## Summary of files changed

| File | Change |
|---|---|
| `src/fs/icache/mod.rs` | Add `needs_resolve()` to `IcbLike`, remove dead-code annotations |
| `src/fs/icache/async_cache.rs` | Update `IcbResolver` trait (stub + cache params), extend `get_or_resolve` for stubs, add `get_icb_mut_sync` |
| `src/fs/icache/inode_factory.rs` | Make atomic (`AtomicU64`, `&self`) |
| `src/fs/mescloud/icache.rs` | Full rewrite: generic `MescloudICache<R>`, `make_common_file_attr` free fn, `ensure_child_ino`, remove `children` from ICB |
| `src/fs/mescloud/repo.rs` | Add `RepoResolver` (fetches from mesa content API), update all methods to async icache |
| `src/fs/mescloud/org.rs` | Add `OrgResolver` (creates directory attrs), update all methods to async icache |
| `src/fs/mescloud/mod.rs` | Add `MesaResolver` (creates static directory attrs), update all methods to async icache |
| `src/fs/mescloud/common.rs` | Update ICB re-export (no `children`) |
| `src/fs/local.rs` | Add `needs_resolve()` to local `InodeControlBlock` |
