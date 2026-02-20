//! Concurrent deduplication cache for async computations.
//!
//! Given a key and an async factory, ensures the factory runs at most once per key. Subsequent
//! callers for the same key await the already-in-flight computation via a [`Shared`] future,
//! avoiding the race conditions inherent in `Notify`-based signalling.
//!
//! Note that this cache does not support automatic eviction.

use std::panic::AssertUnwindSafe;
use std::{fmt::Debug, future::Future, hash::Hash, pin::Pin};

use futures::FutureExt as _;
use futures::future::Shared;

type SharedFut<V> = Shared<Pin<Box<dyn Future<Output = Option<V>> + Send>>>;

/// Two-state slot: `InFlight` while a factory future is running, then promoted to `Ready` once
/// the future completes.
///
/// The `InFlight` variant holds a `Shared<..., Output = Option<V>>` where `None` signals that the
/// factory panicked (caught by `catch_unwind`). On `None`, callers remove the entry and retry.
enum Slot<V: Clone + Send + 'static> {
    InFlight(SharedFut<V>),
    Ready(V),
}

/// Deduplicating async cache.
///
/// If [`get_or_init`](Self::get_or_init) is called concurrently for the same key, only one
/// invocation of the factory runs. All callers receive a clone of the result.
pub struct FutureBackedCache<K, V: Clone + Send + 'static> {
    map: scc::HashMap<K, Slot<V>>,
}

impl<K, V> Default for FutureBackedCache<K, V>
where
    K: Eq + Hash,
    V: Clone + Send + 'static,
{
    fn default() -> Self {
        Self {
            map: scc::HashMap::default(),
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
                Slot::InFlight(shared) => Err(shared.clone()),
            })
            .await;

        match existing {
            Some(Ok(v)) => return v,
            Some(Err(shared)) => {
                if let Some(v) = self.await_shared(&key, shared).await {
                    return v;
                }
                // Factory panicked; entry removed. Fall through to re-insert below.
            }
            None => {}
        }

        // Slow path: use entry_async for atomic check-and-insert.
        let shared = match self.map.entry_async(key.clone()).await {
            scc::hash_map::Entry::Occupied(occ) => match occ.get() {
                Slot::Ready(v) => return v.clone(),
                Slot::InFlight(shared) => shared.clone(),
            },
            scc::hash_map::Entry::Vacant(vac) => {
                let shared = Self::make_shared(factory);
                let ret = shared.clone();
                vac.insert_entry(Slot::InFlight(shared));
                ret
            }
        };

        if let Some(v) = self.await_shared(&key, shared).await {
            return v;
        }

        panic!("FutureBackedCache: joined an in-flight factory that panicked for key {key:?}");
    }

    /// Like [`get_or_init`](Self::get_or_init), but for fallible factories.
    ///
    /// If the factory returns `Ok(v)`, the value is cached and returned. If it returns `Err(e)`,
    /// **nothing is cached** and the error is propagated to the caller.
    ///
    /// Unlike `get_or_init`, concurrent callers are **not** deduplicated — each caller that
    /// finds the key absent will invoke the factory independently. However, if a value was
    /// previously cached (by either `get_or_init` or a successful `get_or_try_init`), it is
    /// returned immediately without calling the factory.
    pub async fn get_or_try_init<F, Fut, E>(&self, key: K, factory: F) -> Result<V, E>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<V, E>> + Send + 'static,
    {
        // Fast path: value already cached or in-flight from an infallible init.
        let existing = self
            .map
            .read_async(&key, |_, slot| match slot {
                Slot::Ready(v) => Ok(v.clone()),
                Slot::InFlight(shared) => Err(shared.clone()),
            })
            .await;

        match existing {
            Some(Ok(v)) => return Ok(v),
            Some(Err(shared)) => {
                if let Some(v) = self.await_shared(&key, shared).await {
                    return Ok(v);
                }
                // Factory panicked; entry was removed. Fall through to run our own factory.
            }
            None => {}
        }

        // Run the fallible factory (not deduplicated).
        let val = factory().await?;

        // Attempt to cache. If another caller raced us and already inserted,
        // return the existing value and discard ours.
        match self.map.entry_async(key).await {
            scc::hash_map::Entry::Occupied(occ) => match occ.get() {
                Slot::Ready(v) => Ok(v.clone()),
                Slot::InFlight(shared) => Ok(self
                    .await_shared(occ.key(), shared.clone())
                    .await
                    .unwrap_or(val)),
            },
            scc::hash_map::Entry::Vacant(vac) => {
                vac.insert_entry(Slot::Ready(val.clone()));
                Ok(val)
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
                Slot::InFlight(shared) => Err(shared.clone()),
            })
            .await;

        match existing {
            Some(Ok(v)) => Some(v),
            Some(Err(shared)) => self.await_shared(key, shared).await,
            None => None,
        }
    }

    /// Await a `Shared` future, handle promotion to `Ready`, and handle panic recovery.
    ///
    /// Returns `Some(v)` on success. Returns `None` if the factory panicked, after removing
    /// the poisoned entry from the map.
    async fn await_shared(&self, key: &K, shared: SharedFut<V>) -> Option<V> {
        let mut guard = PromoteGuard {
            map: &self.map,
            key,
            value: None,
        };

        let result = shared.await;

        if let Some(v) = result {
            guard.value = Some(v.clone());

            self.map
                .update_async(key, |_, slot| {
                    if matches!(slot, Slot::InFlight(_)) {
                        *slot = Slot::Ready(v.clone());
                    }
                })
                .await;

            guard.value = None;
            Some(v)
        } else {
            // Factory panicked. Remove the poisoned InFlight entry so the next caller
            // can retry.
            drop(
                self.map
                    .remove_if_sync(key, |slot| matches!(slot, Slot::InFlight(_))),
            );
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
    value: Option<V>,
}

impl<K, V> Drop for PromoteGuard<'_, K, V>
where
    K: Eq + Hash,
    V: Clone + Send + Sync + 'static,
{
    fn drop(&mut self) {
        if let Some(v) = self.value.take() {
            self.map.update_sync(self.key, |_, slot| {
                if matches!(slot, Slot::InFlight(_)) {
                    *slot = Slot::Ready(v);
                }
            });
        }
    }
}
