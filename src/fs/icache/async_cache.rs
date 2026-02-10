//! Async inode cache with InFlight/Available state machine.

use std::future::Future;

use scc::HashMap as ConcurrentHashMap;
use tokio::sync::watch;

use tracing::{trace, warn};

use crate::fs::r#trait::Inode;

use super::IcbLike;

/// State of an entry in the async inode cache.
pub enum IcbState<I> {
    /// Entry is being loaded; waiters clone the receiver and `.changed().await`.
    ///
    /// The channel carries `()` rather than the resolved value because the map
    /// is the single source of truth: ICBs are mutated in-place (rc, attrs) so
    /// a snapshot in the channel would immediately go stale. Sender-drop also
    /// gives us implicit, leak-proof signalling on both success and error paths.
    InFlight(watch::Receiver<()>),
    /// Entry is ready for use.
    Available(I),
}

/// Trait for resolving an inode to its control block.
///
/// Implementations act as a "promise" that an ICB will eventually be produced
/// for a given inode. The cache calls `resolve` when it needs to populate a
/// missing entry.
pub trait IcbResolver: Send + Sync {
    /// The inode control block type this resolver produces.
    type Icb: IcbLike + Send + Sync;
    /// Error type returned when resolution fails.
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

/// Async, concurrency-safe inode cache.
///
/// All methods take `&self` — internal synchronization is provided by
/// `scc::HashMap` (sharded lock-free map).
pub struct AsyncICache<R: IcbResolver> {
    resolver: R,
    inode_table: ConcurrentHashMap<Inode, IcbState<R::Icb>>,
}

impl<R: IcbResolver> AsyncICache<R> {
    /// Create a new cache with a root ICB at `root_ino` (rc = 1).
    pub fn new(resolver: R, root_ino: Inode, root_path: impl Into<std::path::PathBuf>) -> Self {
        let table = ConcurrentHashMap::new();
        // insert_sync is infallible for a fresh map
        drop(table.insert_sync(
            root_ino,
            IcbState::Available(R::Icb::new_root(root_path.into())),
        ));
        Self {
            resolver,
            inode_table: table,
        }
    }

    /// Number of entries (`InFlight` + `Available`) in the table.
    pub fn inode_count(&self) -> usize {
        self.inode_table.len()
    }

    /// Wait until `ino` is `Available`.
    /// Returns `true` if the entry exists and is Available,
    /// `false` if the entry does not exist.
    async fn wait_for_available(&self, ino: Inode) -> bool {
        let rx = self
            .inode_table
            .read_async(&ino, |_, s| match s {
                IcbState::InFlight(rx) => Some(rx.clone()),
                IcbState::Available(_) => None,
            })
            .await;

        match rx {
            None => false,      // key missing
            Some(None) => true, // Available
            Some(Some(mut rx)) => {
                // Wait for the resolver to complete (or fail/drop sender).
                // changed() returns Err(RecvError) when sender is dropped,
                // which is fine — it means resolution finished.
                let _ = rx.changed().await;
                // Re-check: entry is now Available or was removed on error.
                self.inode_table
                    .read_async(&ino, |_, s| matches!(s, IcbState::Available(_)))
                    .await
                    .unwrap_or(false)
            }
        }
    }

    /// Check whether `ino` exists. **Awaits** if the entry is `InFlight`.
    pub async fn contains(&self, ino: Inode) -> bool {
        self.wait_for_available(ino).await
    }

    /// Read an ICB via closure. **Awaits** if `InFlight`.
    /// Returns `None` if `ino` doesn't exist.
    pub async fn get_icb<T>(&self, ino: Inode, f: impl FnOnce(&R::Icb) -> T) -> Option<T> {
        if !self.wait_for_available(ino).await {
            return None;
        }
        self.inode_table
            .read_async(&ino, |_, state| match state {
                IcbState::Available(icb) => Some(f(icb)),
                IcbState::InFlight(_) => None,
            })
            .await
            .flatten()
    }

    /// Mutate an ICB via closure. **Awaits** if `InFlight`.
    /// Returns `None` if `ino` doesn't exist.
    pub async fn get_icb_mut<T>(&self, ino: Inode, f: impl FnOnce(&mut R::Icb) -> T) -> Option<T> {
        if !self.wait_for_available(ino).await {
            return None;
        }
        self.inode_table
            .update_async(&ino, |_, state| match state {
                IcbState::Available(icb) => Some(f(icb)),
                IcbState::InFlight(_) => None,
            })
            .await
            .flatten()
    }

    /// Insert an ICB directly as `Available` (overwrites any existing entry).
    pub async fn insert_icb(&self, ino: Inode, icb: R::Icb) {
        self.inode_table
            .upsert_async(ino, IcbState::Available(icb))
            .await;
    }

    /// Get-or-insert pattern. If `ino` exists (awaits `InFlight`), runs `then`
    /// on it. If absent, calls `factory` to create, inserts, then runs `then`.
    ///
    /// Both `factory` and `then` are `FnOnce` — wrapped in `Option` internally
    /// to satisfy the borrow checker across the await-loop.
    pub async fn entry_or_insert_icb<T>(
        &self,
        ino: Inode,
        factory: impl FnOnce() -> R::Icb,
        then: impl FnOnce(&mut R::Icb) -> T,
    ) -> T {
        use scc::hash_map::Entry;
        let mut factory = Some(factory);
        let mut then_fn = Some(then);

        loop {
            match self.inode_table.entry_async(ino).await {
                Entry::Occupied(mut occ) => match occ.get_mut() {
                    IcbState::Available(icb) => {
                        let t = then_fn.take().unwrap_or_else(|| unreachable!());
                        return t(icb);
                    }
                    IcbState::InFlight(rx) => {
                        let mut rx = rx.clone();
                        drop(occ); // release shard lock before awaiting
                        let _ = rx.changed().await;
                    }
                },
                Entry::Vacant(vac) => {
                    let f = factory.take().unwrap_or_else(|| unreachable!());
                    let mut occ = vac.insert_entry(IcbState::Available(f()));
                    if let IcbState::Available(icb) = occ.get_mut() {
                        let t = then_fn.take().unwrap_or_else(|| unreachable!());
                        return t(icb);
                    }
                    unreachable!("just inserted Available");
                }
            }
        }
    }

    /// Look up `ino`. If `Available` and fully resolved, run `then` and return
    /// `Ok(T)`. If `Available` but `needs_resolve()` is true (stub), extract
    /// the stub, resolve it, cache the result, then run `then`. If absent, call
    /// the resolver to fetch the ICB, cache it, then run `then`. If another task
    /// is already resolving this inode (`InFlight`), wait for it.
    ///
    /// Returns `Err(R::Error)` if resolution fails. On error the `InFlight`
    /// entry is removed so subsequent calls can retry.
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
                    IcbState::InFlight(_) | IcbState::Available(_) => None,
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
                        let IcbState::Available(stub) = old else {
                            unreachable!()
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

    /// Increment rc. **Awaits** `InFlight`. Panics if inode is missing.
    pub async fn inc_rc(&self, ino: Inode) -> u64 {
        self.wait_for_available(ino).await;
        self.inode_table
            .update_async(&ino, |_, state| match state {
                IcbState::Available(icb) => {
                    *icb.rc_mut() += 1;
                    icb.rc()
                }
                IcbState::InFlight(_) => unreachable!("inc_rc after wait_for_available"),
            })
            .await
            .unwrap_or_else(|| unreachable!("inc_rc: inode {ino} not in table"))
    }

    /// Decrement rc by `nlookups`. If rc drops to zero, evicts and returns
    /// the ICB. **Awaits** `InFlight` entries.
    pub async fn forget(&self, ino: Inode, nlookups: u64) -> Option<R::Icb> {
        if !self.wait_for_available(ino).await {
            warn!(ino, "forget on unknown inode");
            return None;
        }

        // Atomically remove if rc <= nlookups
        let removed = self
            .inode_table
            .remove_if_async(
                &ino,
                |state| matches!(state, IcbState::Available(icb) if icb.rc() <= nlookups),
            )
            .await;

        if let Some((_, IcbState::Available(icb))) = removed {
            trace!(ino, "evicting inode");
            return Some(icb);
        }

        // Entry survives — decrement rc
        self.inode_table
            .update_async(&ino, |_, state| {
                if let IcbState::Available(icb) = state {
                    *icb.rc_mut() -= nlookups;
                    trace!(ino, new_rc = icb.rc(), "decremented rc");
                }
            })
            .await;

        None
    }

    /// Synchronous mutable access to an `Available` entry.
    /// Does **not** wait for `InFlight`. Intended for initialization.
    pub fn get_icb_mut_sync<T>(&self, ino: Inode, f: impl FnOnce(&mut R::Icb) -> T) -> Option<T> {
        self.inode_table
            .update_sync(&ino, |_, state| match state {
                IcbState::Available(icb) => Some(f(icb)),
                IcbState::InFlight(_) => None,
            })
            .flatten()
    }

    /// Iterate over all `Available` entries (skips `InFlight`).
    pub fn for_each(&self, mut f: impl FnMut(&Inode, &R::Icb)) {
        self.inode_table.iter_sync(|ino, state| {
            if let IcbState::Available(icb) = state {
                f(ino, icb);
            }
            true // continue iteration
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap as StdHashMap;
    use std::path::PathBuf;
    use std::sync::atomic::Ordering;
    use std::sync::{Arc, Mutex};

    #[derive(Debug, Clone, PartialEq)]
    struct TestIcb {
        rc: u64,
        path: PathBuf,
        resolved: bool,
    }

    impl IcbLike for TestIcb {
        fn new_root(path: PathBuf) -> Self {
            Self {
                rc: 1,
                path,
                resolved: true,
            }
        }
        fn rc(&self) -> u64 {
            self.rc
        }
        fn rc_mut(&mut self) -> &mut u64 {
            &mut self.rc
        }
        fn needs_resolve(&self) -> bool {
            !self.resolved
        }
    }

    struct TestResolver {
        responses: Mutex<StdHashMap<Inode, Result<TestIcb, String>>>,
    }

    impl TestResolver {
        fn new() -> Self {
            Self {
                responses: Mutex::new(StdHashMap::new()),
            }
        }

        fn add(&self, ino: Inode, icb: TestIcb) {
            self.responses
                .lock()
                .expect("test mutex")
                .insert(ino, Ok(icb));
        }

        fn add_err(&self, ino: Inode, err: impl Into<String>) {
            self.responses
                .lock()
                .expect("test mutex")
                .insert(ino, Err(err.into()));
        }
    }

    impl IcbResolver for TestResolver {
        type Icb = TestIcb;
        type Error = String;

        fn resolve(
            &self,
            ino: Inode,
            _stub: Option<Self::Icb>,
            _cache: &AsyncICache<Self>,
        ) -> impl Future<Output = Result<Self::Icb, Self::Error>> + Send {
            let result = self
                .responses
                .lock()
                .expect("test mutex")
                .remove(&ino)
                .unwrap_or_else(|| Err(format!("no response for inode {ino}")));
            async move { result }
        }
    }

    fn test_cache() -> AsyncICache<TestResolver> {
        AsyncICache::new(TestResolver::new(), 1, "/root")
    }

    fn test_cache_with(resolver: TestResolver) -> AsyncICache<TestResolver> {
        AsyncICache::new(resolver, 1, "/root")
    }

    #[tokio::test]
    async fn contains_returns_true_for_root() {
        let cache = test_cache();
        assert!(cache.contains(1).await, "root should exist");
    }

    #[tokio::test]
    async fn contains_returns_false_for_missing() {
        let cache = test_cache();
        assert!(!cache.contains(999).await, "missing inode should not exist");
    }

    #[tokio::test]
    async fn contains_after_resolver_completes() {
        let resolver = TestResolver::new();
        resolver.add(
            42,
            TestIcb {
                rc: 1,
                path: "/test".into(),
                resolved: true,
            },
        );
        let cache = Arc::new(test_cache_with(resolver));

        // Trigger resolve in background
        let cache2 = Arc::clone(&cache);
        let handle = tokio::spawn(async move { cache2.get_or_resolve(42, |_| ()).await });

        handle
            .await
            .expect("task panicked")
            .expect("resolve failed");
        assert!(cache.contains(42).await, "should be true after resolve");
    }

    #[tokio::test]
    async fn new_creates_root_entry() {
        let cache = test_cache();
        assert_eq!(cache.inode_count(), 1, "should have exactly 1 entry");
    }

    #[tokio::test]
    async fn get_icb_returns_value() {
        let cache = test_cache();
        let path = cache.get_icb(1, |icb| icb.path.clone()).await;
        assert_eq!(path, Some(PathBuf::from("/root")));
    }

    #[tokio::test]
    async fn get_icb_returns_none_for_missing() {
        let cache = test_cache();
        let result = cache.get_icb(999, IcbLike::rc).await;
        assert_eq!(result, None, "missing inode should return None");
    }

    #[tokio::test]
    async fn get_icb_mut_modifies_value() {
        let cache = test_cache();
        cache
            .get_icb_mut(1, |icb| {
                *icb.rc_mut() += 10;
            })
            .await;
        let rc = cache.get_icb(1, IcbLike::rc).await;
        assert_eq!(rc, Some(11), "root starts at rc=1, +10 = 11");
    }

    #[tokio::test]
    async fn get_icb_after_resolver_completes() {
        let resolver = TestResolver::new();
        resolver.add(
            42,
            TestIcb {
                rc: 1,
                path: "/loaded".into(),
                resolved: true,
            },
        );
        let cache = test_cache_with(resolver);

        // Resolve inode 42
        cache
            .get_or_resolve(42, |_| ())
            .await
            .expect("resolve failed");

        let path = cache.get_icb(42, |icb| icb.path.clone()).await;
        assert_eq!(path, Some(PathBuf::from("/loaded")));
    }

    #[tokio::test]
    async fn insert_icb_adds_entry() {
        let cache = test_cache();
        cache
            .insert_icb(
                42,
                TestIcb {
                    rc: 1,
                    path: "/foo".into(),
                    resolved: true,
                },
            )
            .await;
        assert!(cache.contains(42).await, "inserted entry should exist");
        assert_eq!(cache.inode_count(), 2, "root + inserted = 2");
    }

    #[tokio::test]
    async fn entry_or_insert_creates_new() {
        let cache = test_cache();
        let rc = cache
            .entry_or_insert_icb(
                42,
                || TestIcb {
                    rc: 0,
                    path: "/new".into(),
                    resolved: true,
                },
                |icb| {
                    *icb.rc_mut() += 1;
                    icb.rc()
                },
            )
            .await;
        assert_eq!(rc, 1, "factory creates rc=0, then +1 = 1");
    }

    #[tokio::test]
    async fn entry_or_insert_returns_existing() {
        let cache = test_cache();
        cache
            .insert_icb(
                42,
                TestIcb {
                    rc: 5,
                    path: "/existing".into(),
                    resolved: true,
                },
            )
            .await;

        let rc = cache
            .entry_or_insert_icb(
                42,
                || panic!("factory should not be called"),
                |icb| icb.rc(),
            )
            .await;
        assert_eq!(rc, 5, "existing entry rc should be 5");
    }

    #[tokio::test]
    async fn entry_or_insert_after_resolver_completes() {
        let resolver = TestResolver::new();
        resolver.add(
            42,
            TestIcb {
                rc: 1,
                path: "/resolved".into(),
                resolved: true,
            },
        );
        let cache = Arc::new(test_cache_with(resolver));

        // Start resolve in background
        let cache2 = Arc::clone(&cache);
        let resolve_handle = tokio::spawn(async move { cache2.get_or_resolve(42, |_| ()).await });

        // Wait for resolve to finish
        resolve_handle
            .await
            .expect("task panicked")
            .expect("resolve failed");

        // Now entry_or_insert should find the existing entry
        let rc = cache
            .entry_or_insert_icb(
                42,
                || panic!("factory should not be called"),
                |icb| icb.rc(),
            )
            .await;
        assert_eq!(rc, 1, "should find the resolved entry");
    }

    #[tokio::test]
    async fn inc_rc_increments() {
        let cache = test_cache();
        cache
            .insert_icb(
                42,
                TestIcb {
                    rc: 1,
                    path: "/a".into(),
                    resolved: true,
                },
            )
            .await;
        let new_rc = cache.inc_rc(42).await;
        assert_eq!(new_rc, 2, "rc 1 + 1 = 2");
    }

    #[tokio::test]
    async fn forget_decrements_rc() {
        let cache = test_cache();
        cache
            .insert_icb(
                42,
                TestIcb {
                    rc: 5,
                    path: "/a".into(),
                    resolved: true,
                },
            )
            .await;

        let evicted = cache.forget(42, 2).await;
        assert!(evicted.is_none(), "rc 5 - 2 = 3, should not evict");

        let rc = cache.get_icb(42, IcbLike::rc).await;
        assert_eq!(rc, Some(3), "rc should be 3 after forget(2)");
    }

    #[tokio::test]
    async fn forget_evicts_when_rc_drops_to_zero() {
        let cache = test_cache();
        cache
            .insert_icb(
                42,
                TestIcb {
                    rc: 3,
                    path: "/a".into(),
                    resolved: true,
                },
            )
            .await;

        let evicted = cache.forget(42, 3).await;
        assert!(evicted.is_some(), "rc 3 - 3 = 0, should evict");
        assert!(!cache.contains(42).await, "evicted entry should be gone");
        assert_eq!(cache.inode_count(), 1, "only root remains");
    }

    #[tokio::test]
    async fn forget_unknown_inode_returns_none() {
        let cache = test_cache();
        let evicted = cache.forget(999, 1).await;
        assert!(evicted.is_none(), "unknown inode should return None");
    }

    #[tokio::test]
    async fn for_each_iterates_available_entries() {
        let cache = test_cache();
        cache
            .insert_icb(
                2,
                TestIcb {
                    rc: 1,
                    path: "/a".into(),
                    resolved: true,
                },
            )
            .await;
        cache
            .insert_icb(
                3,
                TestIcb {
                    rc: 1,
                    path: "/b".into(),
                    resolved: true,
                },
            )
            .await;

        let mut seen = std::collections::HashSet::new();
        cache.for_each(|ino, _icb| {
            seen.insert(*ino);
        });
        assert_eq!(seen.len(), 3, "should see all 3 entries");
        assert!(seen.contains(&1), "should contain root");
        assert!(seen.contains(&2), "should contain inode 2");
        assert!(seen.contains(&3), "should contain inode 3");
    }

    #[tokio::test]
    async fn for_each_skips_inflight() {
        let cache = test_cache();
        // Directly insert an InFlight entry for testing iteration
        let (_tx, rx) = watch::channel(());
        cache
            .inode_table
            .upsert_async(42, IcbState::InFlight(rx))
            .await;

        let mut count = 0;
        cache.for_each(|_, _| {
            count += 1;
        });
        assert_eq!(count, 1, "only root, not the InFlight entry");
    }

    #[tokio::test]
    async fn wait_does_not_miss_signal_on_immediate_complete() {
        let cache = Arc::new(test_cache());

        // Insert InFlight manually, then immediately complete before anyone waits
        let (tx, rx) = watch::channel(());
        cache
            .inode_table
            .upsert_async(42, IcbState::InFlight(rx))
            .await;

        // Complete before any waiter
        cache
            .insert_icb(
                42,
                TestIcb {
                    rc: 1,
                    path: "/fast".into(),
                    resolved: true,
                },
            )
            .await;
        drop(tx);

        // This must NOT hang
        let result =
            tokio::time::timeout(std::time::Duration::from_millis(100), cache.contains(42)).await;
        assert_eq!(
            result,
            Ok(true),
            "should not hang on already-completed entry"
        );
    }

    // -- get_or_resolve tests --

    #[tokio::test]
    async fn get_or_resolve_returns_existing() {
        let cache = test_cache();
        cache
            .insert_icb(
                42,
                TestIcb {
                    rc: 1,
                    path: "/existing".into(),
                    resolved: true,
                },
            )
            .await;

        let path: Result<PathBuf, String> = cache.get_or_resolve(42, |icb| icb.path.clone()).await;
        assert_eq!(path, Ok(PathBuf::from("/existing")));
    }

    #[tokio::test]
    async fn get_or_resolve_resolves_missing() {
        let resolver = TestResolver::new();
        resolver.add(
            42,
            TestIcb {
                rc: 1,
                path: "/resolved".into(),
                resolved: true,
            },
        );
        let cache = test_cache_with(resolver);

        let path: Result<PathBuf, String> = cache.get_or_resolve(42, |icb| icb.path.clone()).await;
        assert_eq!(path, Ok(PathBuf::from("/resolved")));
        // Should now be cached
        assert!(cache.contains(42).await);
    }

    #[tokio::test]
    async fn get_or_resolve_propagates_error() {
        let resolver = TestResolver::new();
        resolver.add_err(42, "network error");
        let cache = test_cache_with(resolver);

        let result: Result<PathBuf, String> =
            cache.get_or_resolve(42, |icb| icb.path.clone()).await;
        assert_eq!(result, Err("network error".to_owned()));
        // Entry should be cleaned up on error
        assert!(!cache.contains(42).await);
    }

    struct CountingResolver {
        count: Arc<std::sync::atomic::AtomicUsize>,
    }

    impl IcbResolver for CountingResolver {
        type Icb = TestIcb;
        type Error = String;

        fn resolve(
            &self,
            _ino: Inode,
            _stub: Option<Self::Icb>,
            _cache: &AsyncICache<Self>,
        ) -> impl Future<Output = Result<Self::Icb, Self::Error>> + Send {
            self.count.fetch_add(1, Ordering::SeqCst);
            async {
                tokio::task::yield_now().await;
                Ok(TestIcb {
                    rc: 1,
                    path: "/coalesced".into(),
                    resolved: true,
                })
            }
        }
    }

    #[tokio::test]
    async fn get_or_resolve_coalesces_concurrent_requests() {
        use std::sync::atomic::AtomicUsize;

        let resolve_count = Arc::new(AtomicUsize::new(0));

        let cache = Arc::new(AsyncICache::new(
            CountingResolver {
                count: Arc::clone(&resolve_count),
            },
            1,
            "/root",
        ));

        let mut handles = Vec::new();
        for _ in 0..5 {
            let c = Arc::clone(&cache);
            handles.push(tokio::spawn(async move {
                c.get_or_resolve(42, |icb| icb.path.clone()).await
            }));
        }

        for h in handles {
            assert_eq!(
                h.await.expect("task panicked"),
                Ok(PathBuf::from("/coalesced")),
            );
        }

        // Resolver should only have been called ONCE (not 5 times)
        assert_eq!(
            resolve_count.load(Ordering::SeqCst),
            1,
            "should coalesce to 1 resolve call"
        );
    }

    #[tokio::test]
    async fn get_or_resolve_resolves_stub_entry() {
        let resolver = TestResolver::new();
        resolver.add(
            42,
            TestIcb {
                rc: 1,
                path: "/resolved".into(),
                resolved: true,
            },
        );
        let cache = test_cache_with(resolver);

        // Insert unresolved stub
        cache
            .insert_icb(
                42,
                TestIcb {
                    rc: 0,
                    path: "/stub".into(),
                    resolved: false,
                },
            )
            .await;

        // get_or_resolve should trigger resolution because needs_resolve() == true
        let path: Result<PathBuf, String> = cache.get_or_resolve(42, |icb| icb.path.clone()).await;
        assert_eq!(path, Ok(PathBuf::from("/resolved")));
    }
}
