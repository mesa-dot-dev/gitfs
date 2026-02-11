//! Mescloud-specific inode control block, helpers, and directory cache wrapper.

use std::collections::HashSet;
use std::ffi::{OsStr, OsString};
use std::sync::Arc;
use std::time::SystemTime;

use scc::HashMap as ConcurrentHashMap;

use crate::fs::icache::{AsyncICache, IcbLike, IcbResolver, InodeFactory};
use crate::fs::r#trait::{
    CommonFileAttr, DirEntryType, FileAttr, FilesystemStats, Inode, Permissions,
};

/// Inode control block for mescloud filesystem layers.
#[derive(Clone)]
pub struct InodeControlBlock {
    pub parent: Option<Inode>,
    pub rc: u64,
    pub path: std::path::PathBuf,
    /// Cached file attributes from the last lookup.
    pub attr: Option<FileAttr>,
    /// Cached directory children from the resolver (directories only).
    pub children: Option<Vec<(String, DirEntryType)>>,
}

impl IcbLike for InodeControlBlock {
    fn new_root(path: std::path::PathBuf) -> Self {
        Self {
            rc: 1,
            parent: None,
            path,
            attr: None,
            children: None,
        }
    }

    fn rc(&self) -> u64 {
        self.rc
    }

    fn rc_mut(&mut self) -> &mut u64 {
        &mut self.rc
    }

    fn needs_resolve(&self) -> bool {
        match self.attr {
            None => true,
            Some(FileAttr::Directory { .. }) => self.children.is_none(),
            Some(_) => false,
        }
    }
}

/// Calculate the number of blocks needed for a given size.
pub fn blocks_of_size(block_size: u32, size: u64) -> u64 {
    size.div_ceil(u64::from(block_size))
}

/// Free function -- usable by both `MescloudICache` and resolvers.
pub fn make_common_file_attr(
    ino: Inode,
    perm: u16,
    atime: SystemTime,
    mtime: SystemTime,
    fs_owner: (u32, u32),
    block_size: u32,
) -> CommonFileAttr {
    CommonFileAttr {
        ino,
        atime,
        mtime,
        ctime: SystemTime::UNIX_EPOCH,
        crtime: SystemTime::UNIX_EPOCH,
        perm: Permissions::from_bits_truncate(perm),
        nlink: 1,
        uid: fs_owner.0,
        gid: fs_owner.1,
        blksize: block_size,
    }
}

struct MescloudICacheInner<R: IcbResolver<Icb = InodeControlBlock>> {
    cache: AsyncICache<R>,
    inode_factory: InodeFactory,
    /// O(1) lookup from (parent inode, child name) to child inode.
    /// Maintained alongside the inode table to avoid linear scans in
    /// `ensure_child_ino` and to provide atomicity under concurrent access.
    child_index: ConcurrentHashMap<(Inode, OsString), Inode>,
    fs_owner: (u32, u32),
    block_size: u32,
}

/// Mescloud-specific directory cache wrapper over `AsyncICache`.
///
/// Cheaply cloneable via `Arc` so background tasks (e.g. prefetch) can share
/// access to the cache and its domain-specific helpers.
pub struct MescloudICache<R: IcbResolver<Icb = InodeControlBlock>> {
    inner: Arc<MescloudICacheInner<R>>,
}

impl<R: IcbResolver<Icb = InodeControlBlock>> Clone for MescloudICache<R> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<R: IcbResolver<Icb = InodeControlBlock>> MescloudICache<R> {
    /// Create a new `MescloudICache`. Initializes root ICB (rc=1), caches root dir attr.
    pub fn new(resolver: R, root_ino: Inode, fs_owner: (u32, u32), block_size: u32) -> Self {
        let cache = Self {
            inner: Arc::new(MescloudICacheInner {
                cache: AsyncICache::new(resolver, root_ino, "/"),
                inode_factory: InodeFactory::new(root_ino + 1),
                child_index: ConcurrentHashMap::new(),
                fs_owner,
                block_size,
            }),
        };

        // Set root directory attr synchronously during initialization
        let now = SystemTime::now();
        let root_attr = FileAttr::Directory {
            common: make_common_file_attr(root_ino, 0o755, now, now, fs_owner, block_size),
        };
        cache.inner.cache.get_icb_mut_sync(root_ino, |icb| {
            icb.attr = Some(root_attr);
        });

        cache
    }

    // -- Delegated from AsyncICache (async) --

    pub fn contains(&self, ino: Inode) -> bool {
        self.inner.cache.contains(ino)
    }

    pub async fn get_icb<T>(
        &self,
        ino: Inode,
        // `Sync` required: see comment on `AsyncICache::get_icb`.
        f: impl Fn(&InodeControlBlock) -> T + Send + Sync,
    ) -> Option<T> {
        self.inner.cache.get_icb(ino, f).await
    }

    pub async fn insert_icb(&self, ino: Inode, icb: InodeControlBlock) {
        self.inner.cache.insert_icb(ino, icb).await;
    }

    pub async fn entry_or_insert_icb<T>(
        &self,
        ino: Inode,
        factory: impl FnOnce() -> InodeControlBlock,
        then: impl FnOnce(&mut InodeControlBlock) -> T,
    ) -> T {
        self.inner
            .cache
            .entry_or_insert_icb(ino, factory, then)
            .await
    }

    pub async fn inc_rc(&self, ino: Inode) -> Option<u64> {
        self.inner.cache.inc_rc(ino).await
    }

    pub async fn forget(&self, ino: Inode, nlookups: u64) -> Option<InodeControlBlock> {
        let evicted = self.inner.cache.forget(ino, nlookups).await;
        if let Some(ref icb) = evicted
            && let Some(parent) = icb.parent
        {
            self.inner
                .child_index
                .remove_async(&(parent, icb.path.as_os_str().to_os_string()))
                .await;
        }
        evicted
    }

    pub async fn get_or_resolve<T>(
        &self,
        ino: Inode,
        then: impl FnOnce(&InodeControlBlock) -> T,
    ) -> Result<T, R::Error> {
        self.inner.cache.get_or_resolve(ino, then).await
    }

    pub fn spawn_prefetch(&self, inodes: impl IntoIterator<Item = Inode>)
    where
        R: 'static,
        R::Error: 'static,
    {
        self.inner.cache.spawn_prefetch(inodes);
    }

    // -- Domain-specific --

    /// Allocate a new inode number.
    pub fn allocate_inode(&self) -> Inode {
        self.inner.inode_factory.allocate()
    }

    pub async fn get_attr(&self, ino: Inode) -> Option<FileAttr> {
        self.inner
            .cache
            .get_icb(ino, |icb| icb.attr)
            .await
            .flatten()
    }

    pub async fn cache_attr(&self, ino: Inode, attr: FileAttr) {
        self.inner
            .cache
            .get_icb_mut(ino, |icb| {
                icb.attr = Some(attr);
            })
            .await;
    }

    pub fn fs_owner(&self) -> (u32, u32) {
        self.inner.fs_owner
    }

    pub fn block_size(&self) -> u32 {
        self.inner.block_size
    }

    pub fn statfs(&self) -> FilesystemStats {
        FilesystemStats {
            block_size: self.inner.block_size,
            fragment_size: u64::from(self.inner.block_size),
            total_blocks: 0,
            free_blocks: 0,
            available_blocks: 0,
            total_inodes: self.inner.cache.inode_count() as u64,
            free_inodes: 0,
            available_inodes: 0,
            filesystem_id: 0,
            mount_flags: 0,
            max_filename_length: 255,
        }
    }

    /// Evict all `Available` children of `parent` that have `rc == 0`.
    /// Returns the list of evicted inode numbers so callers can clean up
    /// associated state (e.g., bridge mappings, slot tracking).
    #[cfg(test)]
    pub async fn evict_zero_rc_children(&self, parent: Inode) -> Vec<Inode> {
        let mut to_evict = Vec::new();
        self.inner
            .cache
            .for_each(|&ino, icb| {
                if icb.rc == 0 && icb.parent == Some(parent) {
                    to_evict.push((ino, icb.path.as_os_str().to_os_string()));
                }
            })
            .await;
        let mut evicted = Vec::new();
        for (ino, name) in to_evict {
            if self.inner.cache.forget(ino, 0).await.is_some() {
                self.inner.child_index.remove_async(&(parent, name)).await;
                evicted.push(ino);
            }
        }
        evicted
    }

    /// Evict rc=0 children of `parent` that are NOT in `current_names`.
    /// Preserves prefetched children that are still part of the directory listing.
    /// Returns the list of evicted inode numbers so callers can clean up
    /// associated state (e.g., bridge mappings, slot tracking).
    pub async fn evict_stale_children(
        &self,
        parent: Inode,
        current_names: &HashSet<OsString>,
    ) -> Vec<Inode> {
        let mut to_evict = Vec::new();
        self.inner
            .cache
            .for_each(|&ino, icb| {
                if icb.rc == 0
                    && icb.parent == Some(parent)
                    && !current_names.contains(icb.path.as_os_str())
                {
                    to_evict.push((ino, icb.path.as_os_str().to_os_string()));
                }
            })
            .await;
        let mut evicted = Vec::new();
        for (ino, name) in to_evict {
            if self.inner.cache.forget(ino, 0).await.is_some() {
                self.inner.child_index.remove_async(&(parent, name)).await;
                evicted.push(ino);
            }
        }
        evicted
    }

    /// Find an existing child by (parent, name) or allocate a new inode.
    /// If new, inserts a stub ICB (parent+path set, attr=None, children=None, rc=0).
    /// Does NOT bump rc. Returns the inode number.
    ///
    /// Thread-safe: uses `child_index` with `entry_async` for atomic
    /// check-and-insert, so concurrent callers for the same (parent, name)
    /// always receive the same inode.
    pub async fn ensure_child_ino(&self, parent: Inode, name: &OsStr) -> Inode {
        use scc::hash_map::Entry;

        let key = (parent, name.to_os_string());
        match self.inner.child_index.entry_async(key).await {
            Entry::Occupied(occ) => *occ.get(),
            Entry::Vacant(vac) => {
                let ino = self.inner.inode_factory.allocate();
                vac.insert_entry(ino);
                self.inner
                    .cache
                    .insert_icb(
                        ino,
                        InodeControlBlock {
                            rc: 0,
                            path: name.into(),
                            parent: Some(parent),
                            attr: None,
                            children: None,
                        },
                    )
                    .await;
                ino
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::future::Future;

    use super::*;
    use crate::fs::icache::async_cache::AsyncICache;
    use crate::fs::r#trait::DirEntryType;

    fn dummy_dir_attr(ino: Inode) -> FileAttr {
        let now = SystemTime::now();
        FileAttr::Directory {
            common: make_common_file_attr(ino, 0o755, now, now, (0, 0), 4096),
        }
    }

    fn dummy_file_attr(ino: Inode) -> FileAttr {
        let now = SystemTime::now();
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
            children: Some(vec![("README.md".to_owned(), DirEntryType::RegularFile)]),
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

    struct NoOpResolver;

    impl IcbResolver for NoOpResolver {
        type Icb = InodeControlBlock;
        type Error = std::convert::Infallible;

        #[expect(
            clippy::manual_async_fn,
            reason = "must match IcbResolver trait signature"
        )]
        fn resolve(
            &self,
            _ino: Inode,
            _stub: Option<InodeControlBlock>,
            _cache: &AsyncICache<Self>,
        ) -> impl Future<Output = Result<InodeControlBlock, Self::Error>> + Send {
            async { unreachable!("NoOpResolver should not be called") }
        }
    }

    fn test_mescloud_cache() -> MescloudICache<NoOpResolver> {
        MescloudICache::new(NoOpResolver, 1, (0, 0), 4096)
    }

    #[tokio::test]
    async fn ensure_child_ino_is_idempotent() {
        let cache = test_mescloud_cache();
        let ino1 = cache.ensure_child_ino(1, OsStr::new("foo")).await;
        let ino2 = cache.ensure_child_ino(1, OsStr::new("foo")).await;
        assert_eq!(ino1, ino2, "same (parent, name) must return same inode");
    }

    #[tokio::test]
    async fn ensure_child_ino_concurrent_same_child() {
        let cache = Arc::new(test_mescloud_cache());
        let mut handles = Vec::new();
        for _ in 0..10 {
            let c = Arc::clone(&cache);
            handles.push(tokio::spawn(async move {
                c.ensure_child_ino(1, OsStr::new("bar")).await
            }));
        }
        let inodes: Vec<Inode> = futures::future::join_all(handles)
            .await
            .into_iter()
            .map(|r| r.unwrap_or_else(|e| panic!("task panicked: {e}")))
            .collect();
        assert!(
            inodes.iter().all(|&i| i == inodes[0]),
            "all concurrent calls must return the same inode: {inodes:?}"
        );
    }

    #[tokio::test]
    async fn evict_zero_rc_children_removes_stubs() {
        let cache = test_mescloud_cache();

        // Insert stubs as children of root (ino=1) with rc=0
        cache
            .insert_icb(
                10,
                InodeControlBlock {
                    rc: 0,
                    path: "child_a".into(),
                    parent: Some(1),
                    attr: None,
                    children: None,
                },
            )
            .await;
        cache
            .insert_icb(
                11,
                InodeControlBlock {
                    rc: 0,
                    path: "child_b".into(),
                    parent: Some(1),
                    attr: None,
                    children: None,
                },
            )
            .await;

        // Insert a child with rc > 0 — should survive
        cache
            .insert_icb(
                12,
                InodeControlBlock {
                    rc: 1,
                    path: "active".into(),
                    parent: Some(1),
                    attr: None,
                    children: None,
                },
            )
            .await;

        // Insert a stub under a different parent — should survive
        cache
            .insert_icb(
                20,
                InodeControlBlock {
                    rc: 0,
                    path: "other".into(),
                    parent: Some(12),
                    attr: None,
                    children: None,
                },
            )
            .await;

        let evicted = cache.evict_zero_rc_children(1).await;
        assert_eq!(evicted.len(), 2, "should evict 2 zero-rc children of root");

        assert!(!cache.contains(10), "child_a should be evicted");
        assert!(!cache.contains(11), "child_b should be evicted");
        assert!(cache.contains(12), "active child should survive");
        assert!(
            cache.contains(20),
            "child of different parent should survive"
        );
    }

    #[tokio::test]
    async fn evict_cleans_child_index() {
        let cache = test_mescloud_cache();
        let ino1 = cache.ensure_child_ino(1, OsStr::new("temp")).await;
        let evicted = cache.evict_zero_rc_children(1).await;
        assert!(evicted.contains(&ino1));
        let ino2 = cache.ensure_child_ino(1, OsStr::new("temp")).await;
        assert_ne!(
            ino1, ino2,
            "after eviction, a new inode should be allocated"
        );
    }

    #[test]
    #[expect(
        clippy::redundant_clone,
        reason = "test exists to verify Clone is implemented"
    )]
    fn mescloud_icache_is_clone() {
        let cache = test_mescloud_cache();
        let _clone = cache.clone();
    }

    #[tokio::test]
    async fn forget_cleans_child_index() {
        let cache = test_mescloud_cache();
        let ino = cache.ensure_child_ino(1, OsStr::new("ephemeral")).await;
        let evicted = cache.forget(ino, 0).await;
        assert!(evicted.is_some());
        let ino2 = cache.ensure_child_ino(1, OsStr::new("ephemeral")).await;
        assert_ne!(ino, ino2, "child_index should have been cleaned up");
    }

    #[tokio::test]
    #[expect(
        clippy::similar_names,
        reason = "bar_ino/baz_ino distinction is intentional"
    )]
    async fn evict_stale_children_preserves_current() {
        use std::collections::HashSet;

        let cache = test_mescloud_cache();

        // Insert children of root via ensure_child_ino (populates child_index)
        let foo_ino = cache.ensure_child_ino(1, OsStr::new("foo")).await;
        let bar_ino = cache.ensure_child_ino(1, OsStr::new("bar")).await;
        let baz_ino = cache.ensure_child_ino(1, OsStr::new("baz")).await;

        // "foo" and "baz" are in the current listing; "bar" is stale
        let current: HashSet<OsString> =
            ["foo", "baz"].iter().copied().map(OsString::from).collect();
        let evicted = cache.evict_stale_children(1, &current).await;

        assert_eq!(evicted, vec![bar_ino], "only stale 'bar' should be evicted");
        assert!(cache.contains(foo_ino), "current child 'foo' preserved");
        assert!(!cache.contains(bar_ino), "stale child 'bar' evicted");
        assert!(cache.contains(baz_ino), "current child 'baz' preserved");
    }
}
