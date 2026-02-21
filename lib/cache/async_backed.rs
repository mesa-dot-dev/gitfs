//! Concurrent deduplication cache for async computations.
//!
//! Given a key and an async factory, ensures the factory runs at most once per key. Subsequent
//! callers for the same key await the already-in-flight computation via a [`Shared`] future,
//! avoiding the race conditions inherent in `Notify`-based signalling.
//!
//! Note that this cache does not support automatic eviction.

use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::{fmt::Debug, future::Future, hash::Hash, pin::Pin};

use futures::FutureExt as _;
use futures::future::Shared;

type SharedFut<V> = Shared<Pin<Box<dyn Future<Output = Option<V>> + Send>>>;

/// Two-state slot: `InFlight` while a factory future is running, then promoted to `Ready` once
/// the future completes.
///
/// The `InFlight` variant holds a generation counter and a `Shared<..., Output = Option<V>>`
/// where `None` signals that the factory panicked (caught by `catch_unwind`). On `None`, callers
/// remove the entry only if the generation matches, avoiding destruction of a valid re-inserted
/// entry.
enum Slot<V: Clone + Send + 'static> {
    InFlight(u64, SharedFut<V>),
    Ready(V),
}

/// Deduplicating async cache.
///
/// If [`get_or_init`](Self::get_or_init) is called concurrently for the same key, only one
/// invocation of the factory runs. All callers receive a clone of the result.
pub struct FutureBackedCache<K, V: Clone + Send + 'static> {
    map: scc::HashMap<K, Slot<V>>,
    next_gen: AtomicU64,
}

impl<K, V> Default for FutureBackedCache<K, V>
where
    K: Eq + Hash,
    V: Clone + Send + 'static,
{
    fn default() -> Self {
        Self {
            map: scc::HashMap::default(),
            next_gen: AtomicU64::new(0),
        }
    }
}

impl<K, V> FutureBackedCache<K, V>
where
    K: Eq + Hash + Debug + Clone + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
{
    /// Get the cached value for `key`, or initialize it by running `factory`.
    ///
    /// If another caller is already computing the value for this key, this awaits the in-flight
    /// computation instead of spawning a duplicate. If the factory panics, the entry is removed
    /// and the next caller retries with a fresh factory invocation.
    ///
    /// # Panics
    ///
    /// Panics if this caller joins an in-flight factory that itself panicked (i.e. the caller
    /// lost the race to insert a fresh entry after the poisoned slot was removed).
    pub async fn get_or_init<F, Fut>(&self, key: K, factory: F) -> V
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = V> + Send + 'static,
    {
        // Fast path: value already cached.
        let existing = self
            .map
            .read_async(&key, |_, slot| match slot {
                Slot::Ready(v) => Ok(v.clone()),
                Slot::InFlight(generation, shared) => Err((*generation, shared.clone())),
            })
            .await;

        match existing {
            Some(Ok(v)) => return v,
            Some(Err((generation, shared))) => {
                if let Some(v) = self.await_shared(&key, generation, shared).await {
                    return v;
                }
                // Factory panicked; entry removed. Fall through to re-insert below.
            }
            None => {}
        }

        // Slow path: use entry_async for atomic check-and-insert.
        let (generation, shared) = match self.map.entry_async(key.clone()).await {
            scc::hash_map::Entry::Occupied(occ) => match occ.get() {
                Slot::Ready(v) => return v.clone(),
                Slot::InFlight(g, shared) => (*g, shared.clone()),
            },
            scc::hash_map::Entry::Vacant(vac) => {
                let generation = self.next_gen.fetch_add(1, Ordering::Relaxed);
                let shared = Self::make_shared(factory);
                let ret = shared.clone();
                vac.insert_entry(Slot::InFlight(generation, shared));
                (generation, ret)
            }
        };

        if let Some(v) = self.await_shared(&key, generation, shared).await {
            return v;
        }

        panic!("FutureBackedCache: joined an in-flight factory that panicked for key {key:?}");
    }

    /// Like [`get_or_init`](Self::get_or_init), but for fallible factories.
    ///
    /// If the factory returns `Ok(v)`, the value is cached and returned. If it returns `Err(e)`,
    /// **nothing is cached** and the error is propagated to the caller.
    ///
    /// Concurrent callers for the same key are deduplicated: only one factory invocation runs,
    /// and joiners await its shared result. If the factory fails, the poisoned `InFlight` entry
    /// is removed and joiners fall through to run their own factory (non-deduplicated retry).
    ///
    /// # Panics
    ///
    /// Panics if the factory panics (caught internally via `catch_unwind`).
    pub async fn get_or_try_init<F, Fut, E>(&self, key: K, factory: F) -> Result<V, E>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<V, E>> + Send + 'static,
        E: Send + 'static,
    {
        // Fast path: value already cached or in-flight.
        let existing = self
            .map
            .read_async(&key, |_, slot| match slot {
                Slot::Ready(v) => Ok(v.clone()),
                Slot::InFlight(generation, shared) => Err((*generation, shared.clone())),
            })
            .await;

        match existing {
            Some(Ok(v)) => return Ok(v),
            Some(Err((generation, shared))) => {
                if let Some(v) = self.await_shared(&key, generation, shared).await {
                    return Ok(v);
                }
                // In-flight failed; fall through to slow path.
            }
            None => {}
        }

        // Slow path: claim a slot or join an existing in-flight computation.
        // The error side-channel lets the owner retrieve the `Err(e)` from the
        // shared future (which only produces `Option<V>`).
        let error_cell: Arc<std::sync::Mutex<Option<E>>> = Arc::new(std::sync::Mutex::new(None));

        match self.map.entry_async(key.clone()).await {
            scc::hash_map::Entry::Occupied(occ) => match occ.get() {
                Slot::Ready(v) => Ok(v.clone()),
                Slot::InFlight(g, shared) => {
                    let (generation, shared) = (*g, shared.clone());
                    drop(occ);
                    if let Some(v) = self.await_shared(&key, generation, shared).await {
                        return Ok(v);
                    }
                    // In-flight failed. We still have `factory` — run it ourselves.
                    let val = factory().await?;
                    match self.map.entry_async(key).await {
                        scc::hash_map::Entry::Occupied(occ) => match occ.get() {
                            Slot::Ready(v) => Ok(v.clone()),
                            Slot::InFlight(..) => Ok(val),
                        },
                        scc::hash_map::Entry::Vacant(vac) => {
                            vac.insert_entry(Slot::Ready(val.clone()));
                            Ok(val)
                        }
                    }
                }
            },
            scc::hash_map::Entry::Vacant(vac) => {
                let generation = self.next_gen.fetch_add(1, Ordering::Relaxed);
                let shared = Self::make_shared_fallible(factory, Arc::clone(&error_cell));
                let ret = shared.clone();
                vac.insert_entry(Slot::InFlight(generation, shared));

                if let Some(v) = self.await_shared(&key, generation, ret).await {
                    return Ok(v);
                }
                // Our factory returned `Err` — retrieve it from the side channel.
                let captured = error_cell
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .take();
                match captured {
                    Some(e) => Err(e),
                    None => panic!(
                        "FutureBackedCache: factory for key {key:?} resolved to None \
                         but no error was captured (factory panicked)"
                    ),
                }
            }
        }
    }

    /// Get the cached value for `key` if it exists.
    ///
    /// - If the value is `Ready`, returns `Some(v)` immediately.
    /// - If the value is `InFlight`, awaits the in-flight computation and returns `Some(v)`.
    /// - If the key is absent, returns `None`.
    /// - If the in-flight factory panicked, returns `None` (and removes the poisoned entry).
    pub async fn get(&self, key: &K) -> Option<V> {
        let existing = self
            .map
            .read_async(key, |_, slot| match slot {
                Slot::Ready(v) => Ok(v.clone()),
                Slot::InFlight(generation, shared) => Err((*generation, shared.clone())),
            })
            .await;

        match existing {
            Some(Ok(v)) => Some(v),
            Some(Err((generation, shared))) => self.await_shared(key, generation, shared).await,
            None => None,
        }
    }

    /// Await a `Shared` future, handle promotion to `Ready`, and handle panic recovery.
    ///
    /// The `observed_gen` parameter is the generation of the `InFlight` slot that was read.
    /// On panic recovery, only the entry with this exact generation is removed, preventing
    /// destruction of a valid entry re-inserted by a recovered thread.
    ///
    /// Returns `Some(v)` on success. Returns `None` if the factory panicked, after removing
    /// the poisoned entry from the map.
    async fn await_shared(&self, key: &K, observed_gen: u64, shared: SharedFut<V>) -> Option<V> {
        let mut guard = PromoteGuard {
            map: &self.map,
            key,
            observed_gen,
            value: None,
        };

        let result = shared.await;

        if let Some(v) = result {
            guard.value = Some(v.clone());

            self.map
                .update_async(key, |_, slot| {
                    if matches!(slot, Slot::InFlight(g, _) if *g == observed_gen) {
                        *slot = Slot::Ready(v.clone());
                    }
                })
                .await;

            guard.value = None;
            Some(v)
        } else {
            // Factory panicked. Remove the poisoned InFlight entry so the next caller
            // can retry — but only if the generation matches our observation.
            drop(self.map.remove_if_sync(
                key,
                |slot| matches!(slot, Slot::InFlight(g, _) if *g == observed_gen),
            ));
            None
        }
    }

    /// Wrap a factory future in `catch_unwind`, producing a `Shared` with `Output = Option<V>`.
    fn make_shared<F, Fut>(factory: F) -> SharedFut<V>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = V> + Send + 'static,
    {
        let fut = AssertUnwindSafe(factory()).catch_unwind();
        let boxed: Pin<Box<dyn Future<Output = Option<V>> + Send>> =
            Box::pin(async move { fut.await.ok() });
        boxed.shared()
    }

    /// Like [`make_shared`](Self::make_shared), but for fallible factories.
    ///
    /// On `Ok(v)`, the shared future resolves to `Some(v)`. On `Err(e)`, the
    /// error is captured in `error_cell` and the future resolves to `None`.
    fn make_shared_fallible<F, Fut, E>(
        factory: F,
        error_cell: Arc<std::sync::Mutex<Option<E>>>,
    ) -> SharedFut<V>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<V, E>> + Send + 'static,
        E: Send + 'static,
    {
        let fut = AssertUnwindSafe(factory()).catch_unwind();
        let boxed: Pin<Box<dyn Future<Output = Option<V>> + Send>> = Box::pin(async move {
            match fut.await {
                Ok(Ok(v)) => Some(v),
                Ok(Err(e)) => {
                    *error_cell
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(e);
                    None
                }
                Err(_panic) => None,
            }
        });
        boxed.shared()
    }

    /// Returns the number of entries in the cache (both `Ready` and `InFlight`).
    #[must_use]
    pub fn len(&self) -> usize {
        self.map.len()
    }

    /// Returns `true` if the cache contains no entries.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    /// Synchronously insert a value, overwriting any existing entry.
    ///
    /// Suitable for seeding the cache before async operations begin (e.g.
    /// inside an ouroboros builder where async is unavailable).
    pub fn insert_sync(&self, key: K, value: V) {
        drop(self.map.insert_sync(key, Slot::Ready(value)));
    }

    /// Synchronously remove the entry for `key`, returning `true` if it was present.
    ///
    /// Suitable for use in contexts where async is not available (e.g. inside
    /// [`StatelessDrop::delete`](crate::drop_ward::StatelessDrop::delete)).
    pub fn remove_sync(&self, key: &K) -> bool {
        self.map.remove_sync(key).is_some()
    }
}

/// Drop guard that synchronously promotes an `InFlight` entry to `Ready` if the caller
/// is cancelled between `shared.await` completing and the async promotion running.
///
/// Set `value = None` to defuse after successful promotion.
struct PromoteGuard<'a, K, V>
where
    K: Eq + Hash,
    V: Clone + Send + Sync + 'static,
{
    map: &'a scc::HashMap<K, Slot<V>>,
    key: &'a K,
    observed_gen: u64,
    value: Option<V>,
}

impl<K, V> Drop for PromoteGuard<'_, K, V>
where
    K: Eq + Hash,
    V: Clone + Send + Sync + 'static,
{
    fn drop(&mut self) {
        if let Some(v) = self.value.take() {
            let generation = self.observed_gen;
            self.map.update_sync(self.key, |_, slot| {
                if matches!(slot, Slot::InFlight(g, _) if *g == generation) {
                    *slot = Slot::Ready(v);
                }
            });
        }
    }
}
