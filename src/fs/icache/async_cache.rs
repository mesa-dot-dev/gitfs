//! Async inode cache with InFlight/Available state machine.

use std::future::Future;
use std::sync::Arc;

use futures::future::join_all;
use scc::HashMap as ConcurrentHashMap;
use tokio::sync::watch;

use tracing::{instrument, trace, warn};

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

impl<I> IcbState<I> {
    /// Consume `self`, returning the inner value if `Available`, or `None` if `InFlight`.
    fn into_available(self) -> Option<I> {
        match self {
            Self::Available(inner) => Some(inner),
            Self::InFlight(_) => None,
        }
    }
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

/// Shared interior of [`AsyncICache`], behind an `Arc` so the cache can be
/// cheaply cloned for fire-and-forget background work (e.g. `spawn_prefetch`).
struct AsyncICacheInner<R: IcbResolver> {
    resolver: R,
    inode_table: ConcurrentHashMap<Inode, IcbState<R::Icb>>,
}

/// Async, concurrency-safe inode cache.
///
/// All methods take `&self` — internal synchronization is provided by
/// `scc::HashMap` (sharded lock-free map). The cache is cheaply cloneable
/// (via `Arc`) so that prefetch work can be spawned in the background.
pub struct AsyncICache<R: IcbResolver> {
    inner: Arc<AsyncICacheInner<R>>,
}

impl<R: IcbResolver> Clone for AsyncICache<R> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
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
            inner: Arc::new(AsyncICacheInner {
                resolver,
                inode_table: table,
            }),
        }
    }

    /// Number of entries (`InFlight` + `Available`) in the table.
    pub fn inode_count(&self) -> usize {
        self.inner.inode_table.len()
    }

    /// Wait until `ino` is `Available`.
    /// Returns `true` if the entry exists and is Available,
    /// `false` if the entry does not exist.
    #[instrument(name = "AsyncICache::wait_for_available", skip(self))]
    async fn wait_for_available(&self, ino: Inode) -> bool {
        loop {
            let rx = self
                .inner
                .inode_table
                .read_async(&ino, |_, s| match s {
                    IcbState::InFlight(rx) => Some(rx.clone()),
                    IcbState::Available(_) => None,
                })
                .await;

            match rx {
                None => return false,      // key missing
                Some(None) => return true, // Available
                Some(Some(mut rx)) => {
                    // Wait for the resolver to complete (or fail/drop sender).
                    // changed() returns Err(RecvError) when sender is dropped,
                    // which is fine — it means resolution finished.
                    let _ = rx.changed().await;
                    // Loop back — the entry might be InFlight again if another
                    // resolution cycle started between our wakeup and re-read.
                }
            }
        }
    }

    /// Check whether `ino` has an entry in the table (either `InFlight` or `Available`).
    ///
    /// This is a non-blocking, synchronous check. It does **not** wait for
    /// `InFlight` entries to resolve.
    pub fn contains(&self, ino: Inode) -> bool {
        self.inner.inode_table.contains_sync(&ino)
    }

    /// Read an ICB via closure. **Awaits** if `InFlight`.
    /// Returns `None` if `ino` doesn't exist.
    #[instrument(name = "AsyncICache::get_icb", skip(self, f))]
    // `Sync` is required because `f` is held across `.await` points in the
    // loop body; for the resulting future to be `Send`, the captured closure
    // must be `Sync` (clippy::future_not_send).
    pub async fn get_icb<T>(
        &self,
        ino: Inode,
        f: impl Fn(&R::Icb) -> T + Send + Sync,
    ) -> Option<T> {
        loop {
            if !self.wait_for_available(ino).await {
                return None;
            }
            let result = self
                .inner
                .inode_table
                .read_async(&ino, |_, state| match state {
                    IcbState::Available(icb) => Some(f(icb)),
                    IcbState::InFlight(_) => None,
                })
                .await;
            match result {
                Some(Some(val)) => return Some(val),
                Some(None) => {}     // was InFlight, retry
                None => return None, // key missing
            }
        }
    }

    /// Mutate an ICB via closure. **Awaits** if `InFlight`.
    /// Returns `None` if `ino` doesn't exist.
    #[instrument(name = "AsyncICache::get_icb_mut", skip(self, f))]
    pub async fn get_icb_mut<T>(
        &self,
        ino: Inode,
        mut f: impl FnMut(&mut R::Icb) -> T + Send,
    ) -> Option<T> {
        loop {
            if !self.wait_for_available(ino).await {
                return None;
            }
            let result = self
                .inner
                .inode_table
                .update_async(&ino, |_, state| match state {
                    IcbState::Available(icb) => Some(f(icb)),
                    IcbState::InFlight(_) => None,
                })
                .await;
            match result {
                Some(Some(val)) => return Some(val),
                Some(None) => {}     // was InFlight, retry
                None => return None, // key missing
            }
        }
    }

    /// Insert an ICB directly as `Available`. If the entry is currently
    /// `InFlight`, waits for resolution before overwriting.
    #[instrument(name = "AsyncICache::insert_icb", skip(self, icb))]
    pub async fn insert_icb(&self, ino: Inode, icb: R::Icb) {
        use scc::hash_map::Entry;
        let mut icb = Some(icb);
        loop {
            match self.inner.inode_table.entry_async(ino).await {
                Entry::Vacant(vac) => {
                    let val = icb
                        .take()
                        .unwrap_or_else(|| unreachable!("icb consumed more than once"));
                    vac.insert_entry(IcbState::Available(val));
                    return;
                }
                Entry::Occupied(mut occ) => match occ.get_mut() {
                    IcbState::InFlight(rx) => {
                        let mut rx = rx.clone();
                        drop(occ);
                        let _ = rx.changed().await;
                    }
                    IcbState::Available(_) => {
                        let val = icb
                            .take()
                            .unwrap_or_else(|| unreachable!("icb consumed more than once"));
                        *occ.get_mut() = IcbState::Available(val);
                        return;
                    }
                },
            }
        }
    }

    /// Get-or-insert pattern. If `ino` exists (awaits `InFlight`), runs `then`
    /// on it. If absent, calls `factory` to create, inserts, then runs `then`.
    ///
    /// Both `factory` and `then` are `FnOnce` — wrapped in `Option` internally
    /// to satisfy the borrow checker across the await-loop.
    #[instrument(name = "AsyncICache::entry_or_insert_icb", skip(self, factory, then))]
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
            match self.inner.inode_table.entry_async(ino).await {
                Entry::Occupied(mut occ) => match occ.get_mut() {
                    IcbState::Available(icb) => {
                        let t = then_fn
                            .take()
                            .unwrap_or_else(|| unreachable!("then_fn consumed more than once"));
                        return t(icb);
                    }
                    IcbState::InFlight(rx) => {
                        let mut rx = rx.clone();
                        drop(occ); // release shard lock before awaiting
                        let _ = rx.changed().await;
                    }
                },
                Entry::Vacant(vac) => {
                    let f = factory
                        .take()
                        .unwrap_or_else(|| unreachable!("factory consumed more than once"));
                    let t = then_fn
                        .take()
                        .unwrap_or_else(|| unreachable!("then_fn consumed more than once"));
                    let mut icb = f();
                    let result = t(&mut icb);
                    vac.insert_entry(IcbState::Available(icb));
                    return result;
                }
            }
        }
    }

    /// Write an ICB back to the table only if the entry still exists.
    ///
    /// If the entry was evicted (vacant) during resolution, the result is
    /// silently dropped — this prevents resurrecting entries that a concurrent
    /// `forget` has already removed.
    async fn write_back_if_present(&self, ino: Inode, icb: R::Icb) {
        use scc::hash_map::Entry;
        match self.inner.inode_table.entry_async(ino).await {
            Entry::Occupied(mut occ) => {
                *occ.get_mut() = IcbState::Available(icb);
            }
            Entry::Vacant(_) => {
                tracing::debug!(
                    ino,
                    "resolved inode was evicted during resolution, dropping result"
                );
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
    #[instrument(name = "AsyncICache::get_or_resolve", skip(self, then))]
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
                .inner
                .inode_table
                .read_async(&ino, |_, s| match s {
                    IcbState::Available(icb) if !icb.needs_resolve() => {
                        let t = then_fn
                            .take()
                            .unwrap_or_else(|| unreachable!("then_fn consumed more than once"));
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
            match self.inner.inode_table.entry_async(ino).await {
                Entry::Occupied(mut occ) => match occ.get_mut() {
                    IcbState::Available(icb) if !icb.needs_resolve() => {
                        let t = then_fn
                            .take()
                            .unwrap_or_else(|| unreachable!("then_fn consumed more than once"));
                        return Ok(t(icb));
                    }
                    IcbState::Available(_) => {
                        // Stub needing resolution — extract stub, replace with InFlight
                        let (tx, rx) = watch::channel(());
                        let old = std::mem::replace(occ.get_mut(), IcbState::InFlight(rx));
                        let stub = old.into_available().unwrap_or_else(|| {
                            unreachable!("matched Available arm, replaced value must be Available")
                        });
                        let fallback = stub.clone();
                        drop(occ); // release shard lock before awaiting

                        match self.inner.resolver.resolve(ino, Some(stub), self).await {
                            Ok(icb) => {
                                let t = then_fn.take().unwrap_or_else(|| {
                                    unreachable!("then_fn consumed more than once")
                                });
                                let result = t(&icb);
                                self.write_back_if_present(ino, icb).await;
                                drop(tx);
                                return Ok(result);
                            }
                            Err(e) => {
                                if fallback.rc() > 0 {
                                    self.write_back_if_present(ino, fallback).await;
                                } else {
                                    self.inner.inode_table.remove_async(&ino).await;
                                }
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

                    match self.inner.resolver.resolve(ino, None, self).await {
                        Ok(icb) => {
                            let t = then_fn
                                .take()
                                .unwrap_or_else(|| unreachable!("then_fn consumed more than once"));
                            let result = t(&icb);
                            self.write_back_if_present(ino, icb).await;
                            drop(tx);
                            return Ok(result);
                        }
                        Err(e) => {
                            self.inner.inode_table.remove_async(&ino).await;
                            drop(tx);
                            return Err(e);
                        }
                    }
                }
            }
        }
    }

    /// Increment rc. **Awaits** `InFlight`.
    ///
    /// Returns `None` if the inode does not exist or was evicted concurrently.
    /// This can happen when a concurrent `forget` removes the entry between the
    /// caller's insert/cache and this `inc_rc` call, or when a concurrent
    /// `get_or_resolve` swaps the entry to `InFlight` and the entry is then
    /// evicted on resolution failure. Callers in FUSE `lookup` paths should
    /// treat `None` as a lookup failure to avoid ref-count leaks (the kernel
    /// would hold a reference the cache no longer tracks).
    #[instrument(name = "AsyncICache::inc_rc", skip(self))]
    pub async fn inc_rc(&self, ino: Inode) -> Option<u64> {
        loop {
            if !self.wait_for_available(ino).await {
                warn!(ino, "inc_rc: inode not in table");
                return None;
            }
            let result = self
                .inner
                .inode_table
                .update_async(&ino, |_, state| match state {
                    IcbState::Available(icb) => {
                        *icb.rc_mut() += 1;
                        Some(icb.rc())
                    }
                    IcbState::InFlight(_) => None,
                })
                .await
                .flatten();

            match result {
                Some(rc) => return Some(rc),
                None => {
                    // Entry was concurrently replaced with InFlight or evicted.
                    if !self.contains(ino) {
                        warn!(ino, "inc_rc: inode evicted concurrently");
                        return None;
                    }
                    // Entry exists but became InFlight — retry.
                }
            }
        }
    }

    /// Decrement rc by `nlookups`. If rc drops to zero, evicts and returns
    /// the ICB. **Awaits** `InFlight` entries.
    #[instrument(name = "AsyncICache::forget", skip(self))]
    pub async fn forget(&self, ino: Inode, nlookups: u64) -> Option<R::Icb> {
        use scc::hash_map::Entry;

        loop {
            match self.inner.inode_table.entry_async(ino).await {
                Entry::Occupied(mut occ) => match occ.get_mut() {
                    IcbState::Available(icb) => {
                        if icb.rc() <= nlookups {
                            trace!(ino, "evicting inode");
                            let (_, state) = occ.remove_entry();
                            return state.into_available();
                        }
                        *icb.rc_mut() -= nlookups;
                        trace!(ino, new_rc = icb.rc(), "decremented rc");
                        return None;
                    }
                    IcbState::InFlight(rx) => {
                        let mut rx = rx.clone();
                        drop(occ);
                        let _ = rx.changed().await;
                    }
                },
                Entry::Vacant(_) => {
                    warn!(ino, "forget on unknown inode");
                    return None;
                }
            }
        }
    }

    /// Synchronous mutable access to an `Available` entry.
    /// Does **not** wait for `InFlight`. Intended for initialization.
    pub fn get_icb_mut_sync<T>(&self, ino: Inode, f: impl FnOnce(&mut R::Icb) -> T) -> Option<T> {
        self.inner
            .inode_table
            .update_sync(&ino, |_, state| match state {
                IcbState::Available(icb) => Some(f(icb)),
                IcbState::InFlight(_) => None,
            })
            .flatten()
    }

    /// Concurrently resolve multiple inodes, best-effort.
    ///
    /// Each inode is resolved via [`get_or_resolve`] which handles
    /// `InFlight`-coalescing and stub-upgrade logic. Errors are silently
    /// ignored because prefetch is a performance optimization: a failure
    /// simply means the subsequent access will pay the full API latency.
    pub async fn prefetch(&self, inodes: impl IntoIterator<Item = Inode>) {
        let futs: Vec<_> = inodes
            .into_iter()
            .map(|ino| async move {
                drop(self.get_or_resolve(ino, |_| ()).await);
            })
            .collect();
        join_all(futs).await;
    }

    /// Fire-and-forget variant of [`prefetch`](Self::prefetch).
    ///
    /// Spawns the prefetch work on the tokio runtime and returns immediately,
    /// so the caller is never blocked waiting for API responses.
    pub fn spawn_prefetch(&self, inodes: impl IntoIterator<Item = Inode>)
    where
        R: 'static,
        R::Icb: 'static,
        R::Error: 'static,
    {
        let inodes: Vec<_> = inodes.into_iter().collect();
        if inodes.is_empty() {
            return;
        }
        let cache = self.clone();
        tokio::spawn(async move {
            cache.prefetch(inodes).await;
        });
    }

    /// Iterate over all `Available` entries (skips `InFlight`).
    /// Async-safe iteration using `iter_async` to avoid contention on single-threaded runtimes.
    pub async fn for_each(&self, mut f: impl FnMut(&Inode, &R::Icb)) {
        self.inner
            .inode_table
            .iter_async(|ino, state| {
                if let IcbState::Available(icb) = state {
                    f(ino, icb);
                }
                true // continue iteration
            })
            .await;
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
        assert!(cache.contains(1), "root should exist");
    }

    #[tokio::test]
    async fn contains_returns_false_for_missing() {
        let cache = test_cache();
        assert!(!cache.contains(999), "missing inode should not exist");
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
        assert!(cache.contains(42), "should be true after resolve");
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
        assert!(cache.contains(42), "inserted entry should exist");
        assert_eq!(cache.inode_count(), 2, "root + inserted = 2");
    }

    #[tokio::test]
    async fn insert_icb_does_not_clobber_inflight() {
        let cache = Arc::new(test_cache());
        let (tx, rx) = watch::channel(());
        cache
            .inner
            .inode_table
            .upsert_async(42, IcbState::InFlight(rx))
            .await;

        // Spawn insert_icb in background — should wait for InFlight to resolve
        let cache2 = Arc::clone(&cache);
        let handle = tokio::spawn(async move {
            cache2
                .insert_icb(
                    42,
                    TestIcb {
                        rc: 5,
                        path: "/inserted".into(),
                        resolved: true,
                    },
                )
                .await;
        });

        // Give insert_icb time to start waiting
        tokio::task::yield_now().await;

        // Complete the InFlight from the resolver side (write directly)
        cache
            .inner
            .inode_table
            .upsert_async(
                42,
                IcbState::Available(TestIcb {
                    rc: 1,
                    path: "/resolved".into(),
                    resolved: true,
                }),
            )
            .await;
        drop(tx); // signal watchers

        handle.await.expect("task panicked");

        // After insert_icb completes, it should have overwritten the resolved value
        let path = cache.get_icb(42, |icb| icb.path.clone()).await;
        assert_eq!(path, Some(PathBuf::from("/inserted")));
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
        assert_eq!(new_rc, Some(2), "rc 1 + 1 = 2");
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
        assert!(!cache.contains(42), "evicted entry should be gone");
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
        cache
            .for_each(|ino, _icb| {
                seen.insert(*ino);
            })
            .await;
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
            .inner
            .inode_table
            .upsert_async(42, IcbState::InFlight(rx))
            .await;

        let mut count = 0;
        cache
            .for_each(|_, _| {
                count += 1;
            })
            .await;
        assert_eq!(count, 1, "only root, not the InFlight entry");
    }

    #[tokio::test]
    async fn wait_does_not_miss_signal_on_immediate_complete() {
        let cache = Arc::new(test_cache());

        // Insert InFlight manually, then immediately complete before anyone waits
        let (tx, rx) = watch::channel(());
        cache
            .inner
            .inode_table
            .upsert_async(42, IcbState::InFlight(rx))
            .await;

        // Complete before any waiter (simulate resolver by writing directly)
        cache
            .inner
            .inode_table
            .upsert_async(
                42,
                IcbState::Available(TestIcb {
                    rc: 1,
                    path: "/fast".into(),
                    resolved: true,
                }),
            )
            .await;
        drop(tx);

        assert!(cache.contains(42), "entry should exist in table");
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
        assert!(cache.contains(42));
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
        assert!(!cache.contains(42));
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

    #[test]
    fn icb_state_into_available_returns_inner() {
        let state = IcbState::Available(TestIcb {
            rc: 1,
            path: "/test".into(),
            resolved: true,
        });
        assert!(state.into_available().is_some());
    }

    #[test]
    fn icb_state_into_available_returns_none_for_inflight() {
        let (_tx, rx) = watch::channel(());
        let state: IcbState<TestIcb> = IcbState::InFlight(rx);
        assert!(state.into_available().is_none());
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

    #[tokio::test]
    async fn forget_handles_inflight_entry() {
        let cache = Arc::new(test_cache());
        let (tx, rx) = watch::channel(());
        cache
            .inner
            .inode_table
            .upsert_async(42, IcbState::InFlight(rx))
            .await;

        let cache2 = Arc::clone(&cache);
        let handle = tokio::spawn(async move { cache2.forget(42, 1).await });

        // Give forget time to start waiting
        tokio::task::yield_now().await;

        // Simulate resolver completing (write directly to inode_table)
        cache
            .inner
            .inode_table
            .upsert_async(
                42,
                IcbState::Available(TestIcb {
                    rc: 3,
                    path: "/inflight".into(),
                    resolved: true,
                }),
            )
            .await;
        drop(tx);

        let evicted = handle.await.expect("task panicked");
        assert!(evicted.is_none(), "rc=3 - 1 = 2, should not evict");

        let rc = cache.get_icb(42, IcbLike::rc).await;
        assert_eq!(rc, Some(2), "rc should be 2 after forget(1) on rc=3");
    }

    #[tokio::test]
    async fn get_or_resolve_error_preserves_stub_with_nonzero_rc() {
        let resolver = TestResolver::new();
        resolver.add_err(42, "resolve failed");
        let cache = test_cache_with(resolver);

        // Insert a stub with rc=2 (simulates a looked-up entry needing resolution)
        cache
            .insert_icb(
                42,
                TestIcb {
                    rc: 2,
                    path: "/stub".into(),
                    resolved: false,
                },
            )
            .await;

        // get_or_resolve should fail
        let result: Result<PathBuf, String> =
            cache.get_or_resolve(42, |icb| icb.path.clone()).await;
        assert!(result.is_err(), "should propagate resolver error");

        // The stub should be preserved since rc > 0
        assert!(cache.contains(42), "entry with rc=2 should survive error");
        let rc = cache.get_icb(42, IcbLike::rc).await;
        assert_eq!(rc, Some(2), "rc should be preserved");
    }

    #[tokio::test]
    async fn inc_rc_missing_inode_returns_none() {
        let cache = test_cache();
        assert_eq!(cache.inc_rc(999).await, None);
    }

    #[tokio::test]
    async fn inc_rc_waits_for_inflight() {
        let cache = Arc::new(test_cache());
        let (tx, rx) = watch::channel(());
        cache
            .inner
            .inode_table
            .upsert_async(42, IcbState::InFlight(rx))
            .await;

        let cache2 = Arc::clone(&cache);
        let handle = tokio::spawn(async move { cache2.inc_rc(42).await });

        // Simulate resolver completing by writing directly to inode_table
        cache
            .inner
            .inode_table
            .upsert_async(
                42,
                IcbState::Available(TestIcb {
                    rc: 1,
                    path: "/a".into(),
                    resolved: true,
                }),
            )
            .await;
        drop(tx);

        let result = handle
            .await
            .unwrap_or_else(|e| panic!("task panicked: {e}"));
        assert_eq!(
            result,
            Some(2),
            "waited for Available, then incremented 1 -> 2"
        );
    }

    #[tokio::test]
    async fn inc_rc_returns_none_after_concurrent_eviction() {
        let cache = Arc::new(test_cache());
        let (tx, rx) = watch::channel(());
        cache
            .inner
            .inode_table
            .upsert_async(42, IcbState::InFlight(rx))
            .await;

        let cache2 = Arc::clone(&cache);
        let handle = tokio::spawn(async move { cache2.inc_rc(42).await });

        // Evict instead of completing
        cache.inner.inode_table.remove_async(&42).await;
        drop(tx);

        let result = handle
            .await
            .unwrap_or_else(|e| panic!("task panicked: {e}"));
        assert_eq!(result, None, "evicted entry should return None");
    }

    /// Resolver that pauses mid-resolution via a `Notify`, allowing the test
    /// to interleave a `forget` while the resolve future is suspended.
    struct SlowResolver {
        /// Signalled by the resolver once it has started (so the test knows
        /// resolution is in progress).
        started: Arc<tokio::sync::Notify>,
        /// The resolver waits on this before returning (the test signals it
        /// after calling `forget`).
        proceed: Arc<tokio::sync::Notify>,
    }

    impl IcbResolver for SlowResolver {
        type Icb = TestIcb;
        type Error = String;

        fn resolve(
            &self,
            _ino: Inode,
            _stub: Option<Self::Icb>,
            _cache: &AsyncICache<Self>,
        ) -> impl Future<Output = Result<Self::Icb, Self::Error>> + Send {
            let started = Arc::clone(&self.started);
            let proceed = Arc::clone(&self.proceed);
            async move {
                started.notify_one();
                proceed.notified().await;
                Ok(TestIcb {
                    rc: 1,
                    path: "/slow-resolved".into(),
                    resolved: true,
                })
            }
        }
    }

    /// Regression test: `get_icb` must survive the entry cycling back to
    /// `InFlight` between when `wait_for_available` returns and when
    /// `read_async` runs. The loop in `get_icb` should retry and eventually
    /// return the final resolved value.
    #[tokio::test]
    async fn wait_for_available_retries_on_re_inflight() {
        let cache = Arc::new(test_cache());
        let ino: Inode = 42;

        // Phase 1: insert an InFlight entry.
        let (tx1, rx1) = watch::channel(());
        cache
            .inner
            .inode_table
            .upsert_async(ino, IcbState::InFlight(rx1))
            .await;

        // Spawn get_icb — it will wait for InFlight to resolve.
        let cache_get = Arc::clone(&cache);
        let get_handle =
            tokio::spawn(async move { cache_get.get_icb(ino, |icb| icb.path.clone()).await });

        // Give get_icb time to start waiting on the watch channel.
        tokio::task::yield_now().await;

        // Phase 1 complete: transition to Available briefly, then immediately
        // back to InFlight (simulates get_or_resolve finding a stub and
        // re-entering InFlight for a second resolution).
        let (tx2, rx2) = watch::channel(());
        cache
            .inner
            .inode_table
            .upsert_async(ino, IcbState::InFlight(rx2))
            .await;
        // Signal phase-1 watchers so get_icb wakes up; it will re-read the
        // entry and find InFlight again, then loop back to wait.
        drop(tx1);

        // Give get_icb time to re-enter the wait loop.
        tokio::task::yield_now().await;

        // Phase 2 complete: write the final resolved value.
        cache
            .inner
            .inode_table
            .upsert_async(
                ino,
                IcbState::Available(TestIcb {
                    rc: 1,
                    path: "/fully-resolved".into(),
                    resolved: true,
                }),
            )
            .await;
        drop(tx2);

        // get_icb should return the final resolved value (not None).
        let result = get_handle.await.expect("get_icb task panicked");
        assert_eq!(
            result,
            Some(PathBuf::from("/fully-resolved")),
            "get_icb must survive re-InFlight and return the final resolved value"
        );
    }

    /// Regression test: an entry evicted by `forget` during an in-progress
    /// `get_or_resolve` must NOT be resurrected when resolution completes.
    #[tokio::test]
    async fn get_or_resolve_does_not_resurrect_evicted_entry() {
        let started = Arc::new(tokio::sync::Notify::new());
        let proceed = Arc::new(tokio::sync::Notify::new());

        let cache = Arc::new(AsyncICache::new(
            SlowResolver {
                started: Arc::clone(&started),
                proceed: Arc::clone(&proceed),
            },
            1,
            "/root",
        ));

        let ino: Inode = 42;

        // Insert a stub with rc=1 (simulates a looked-up, unresolved entry).
        cache
            .insert_icb(
                ino,
                TestIcb {
                    rc: 1,
                    path: "/stub".into(),
                    resolved: false,
                },
            )
            .await;

        // Spawn get_or_resolve which will trigger slow resolution.
        let cache2 = Arc::clone(&cache);
        let resolve_handle =
            tokio::spawn(async move { cache2.get_or_resolve(ino, |icb| icb.path.clone()).await });

        // Wait until the resolver has started (entry is now InFlight).
        started.notified().await;

        // Evict the entry while resolution is in progress.
        // forget waits for InFlight, so we need to complete resolution for
        // forget to proceed. Instead, remove the InFlight entry directly to
        // simulate a concurrent eviction (e.g., by another path that already
        // removed the entry).
        cache.inner.inode_table.remove_async(&ino).await;

        // Let the resolver finish.
        proceed.notify_one();

        // Wait for get_or_resolve to complete.
        drop(resolve_handle.await.expect("task panicked"));

        // The entry must NOT have been resurrected by write_back_if_present.
        assert!(
            !cache.contains(ino),
            "evicted entry must not be resurrected after resolution completes"
        );
    }

    #[tokio::test]
    async fn prefetch_resolves_multiple_inodes() {
        let resolver = TestResolver::new();
        for ino in [10, 11, 12] {
            resolver.add(
                ino,
                TestIcb {
                    rc: 0,
                    path: format!("/dir{ino}").into(),
                    resolved: true,
                },
            );
        }
        let cache = test_cache_with(resolver);

        for ino in [10, 11, 12] {
            cache
                .insert_icb(
                    ino,
                    TestIcb {
                        rc: 0,
                        path: format!("/dir{ino}").into(),
                        resolved: false,
                    },
                )
                .await;
        }

        cache.prefetch([10, 11, 12]).await;

        for ino in [10, 11, 12] {
            let is_resolved = cache.get_icb(ino, |icb| icb.resolved).await;
            assert_eq!(
                is_resolved,
                Some(true),
                "inode {ino} should be resolved after prefetch"
            );
        }
    }

    #[tokio::test]
    async fn prefetch_skips_already_resolved() {
        let cache = test_cache();
        // Root (ino=1) is already resolved -- prefetch should be a no-op
        cache.prefetch([1]).await;
        let path = cache.get_icb(1, |icb| icb.path.clone()).await;
        assert_eq!(path, Some(PathBuf::from("/root")));
    }

    #[tokio::test]
    async fn prefetch_ignores_errors() {
        let resolver = TestResolver::new();
        resolver.add(
            10,
            TestIcb {
                rc: 0,
                path: "/ok".into(),
                resolved: true,
            },
        );
        resolver.add_err(11, "network error");
        let cache = test_cache_with(resolver);

        for (ino, path) in [(10, "/stub10"), (11, "/stub11")] {
            cache
                .insert_icb(
                    ino,
                    TestIcb {
                        rc: 0,
                        path: path.into(),
                        resolved: false,
                    },
                )
                .await;
        }

        cache.prefetch([10, 11]).await;

        let is_resolved = cache.get_icb(10, |icb| icb.resolved).await;
        assert_eq!(is_resolved, Some(true), "ino 10 should be resolved");
        // ino 11 failed with rc=0 -> removed by get_or_resolve error path
        assert!(
            !cache.contains(11),
            "ino 11 should be removed after error with rc=0"
        );
    }

    #[tokio::test]
    async fn prefetch_does_not_modify_rc() {
        let resolver = TestResolver::new();
        resolver.add(
            10,
            TestIcb {
                rc: 0,
                path: "/dir".into(),
                resolved: true,
            },
        );
        let cache = test_cache_with(resolver);

        cache
            .insert_icb(
                10,
                TestIcb {
                    rc: 0,
                    path: "/stub".into(),
                    resolved: false,
                },
            )
            .await;

        cache.prefetch([10]).await;

        let rc = cache.get_icb(10, IcbLike::rc).await;
        assert_eq!(rc, Some(0), "prefetch must not modify rc");
    }

    #[tokio::test]
    async fn prefetch_empty_is_noop() {
        let cache = test_cache();
        cache.prefetch(std::iter::empty()).await;
        assert_eq!(cache.inode_count(), 1, "empty prefetch changes nothing");
    }
}
