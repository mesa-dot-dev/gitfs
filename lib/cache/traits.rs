use std::{future::Future, hash::Hash, pin::Pin};

/// A readable cache is a cache that can be read from, but not necessarily written to.
///
/// This trait is designed to be a generic trait for any cache. When using a cache as a generic
/// parameter, this is the correct trait to use.
///
/// You should avoid manually implementing this trait, and prefer implementing the
/// `SyncReadableCache` or `AsyncReadableCache` traits instead, which will automatically implement
/// this trait for you.
pub trait ReadableCache<K, V>
where
    K: Eq + Hash,
    V: Clone,
{
    /// The future type returned by the `get` and `contains` methods. This is used to allow
    /// zero-cost synchronous and asynchronous implementations of the `ReadableCache` trait.
    type Fut<'a, T: 'a>: Future<Output = T> + 'a
    where
        Self: 'a,
        K: 'a,
        V: 'a;

    /// Fetch the value associated with the given key from the cache. If the key is not present in
    /// the cache, return `None`.
    fn get<'a>(&'a self, key: &'a K) -> Self::Fut<'a, Option<V>>
    where
        V: 'a;

    /// Check if the cache contains the given key.
    fn contains<'a>(&'a self, key: &'a K) -> Self::Fut<'a, bool>
    where
        V: 'a;
}

/// Convenience trait for implementing `ReadableCache` with synchronous cache accesses.
///
/// Avoid using this cache as a generic parameter. `ReadableCache` is the correct trait to use for
/// generic parameters, and will support both synchronous and asynchronous implementations of the
/// cache.
pub trait SyncReadableCache<K, V>
where
    K: Eq + Hash,
    V: Clone,
{
    /// See `ReadableCache::get`.
    fn get(&self, key: &K) -> Option<V>;

    /// See `ReadableCache::contains`.
    fn contains(&self, key: &K) -> bool;
}

/// Convenience trait for implementing `ReadableCache` with asynchronous cache accesses.
///
/// Avoid using this cache as a generic parameter. `ReadableCache` is the correct trait to use for
/// generic parameters, and will support both synchronous and asynchronous implementations of the
/// cache.
#[expect(async_fn_in_trait)]
pub trait AsyncReadableCache<K, V>
where
    K: Eq + Hash,
    V: Clone,
{
    /// See `ReadableCache::get`.
    async fn get(&self, key: &K) -> Option<V>;

    /// See `ReadableCache::contains`.
    async fn contains(&self, key: &K) -> bool;
}

impl<K: Eq + Hash, V: Clone, T: AsyncReadableCache<K, V>> !SyncReadableCache<K, V> for T {}

impl<K: Eq + Hash, V: Clone, T: AsyncReadableCache<K, V>> ReadableCache<K, V> for T {
    type Fut<'a, O: 'a>
        = Pin<Box<dyn Future<Output = O> + 'a>>
    where
        Self: 'a,
        K: 'a,
        V: 'a;

    fn get<'a>(&'a self, key: &'a K) -> Self::Fut<'a, Option<V>>
    where
        V: 'a,
    {
        Box::pin(AsyncReadableCache::get(self, key))
    }

    fn contains<'a>(&'a self, key: &'a K) -> Self::Fut<'a, bool>
    where
        V: 'a,
    {
        Box::pin(AsyncReadableCache::contains(self, key))
    }
}

impl<K: Eq + Hash, V: Clone, T: SyncReadableCache<K, V>> ReadableCache<K, V> for T {
    type Fut<'a, O: 'a>
        = std::future::Ready<O>
    where
        Self: 'a,
        K: 'a,
        V: 'a;

    fn get<'a>(&'a self, key: &'a K) -> Self::Fut<'a, Option<V>>
    where
        V: 'a,
    {
        std::future::ready(SyncReadableCache::get(self, key))
    }

    fn contains<'a>(&'a self, key: &'a K) -> Self::Fut<'a, bool>
    where
        V: 'a,
    {
        std::future::ready(SyncReadableCache::contains(self, key))
    }
}

/// A writable cache is a cache that can be written to, but not necessarily read from.
///
/// Note that this trait also does not require the cache support manual eviction of entries. Many
/// caches only support automatic eviction (such as LRU eviction), and this trait is designed to
/// support those caches as well.
///
/// This trait is designed to be a generic trait for any cache. When using a cache as a generic
/// parameter, this is the correct trait to use.
///
/// You should avoid manually implementing this trait, and prefer implementing the
/// `SyncWritableCache` or `AsyncWritableCache` traits instead, which will automatically implement
/// this trait for you.
pub trait WritableCache<K, V, E>
where
    K: Eq + Hash,
    V: Clone,
    E: std::error::Error,
{
    /// The future type returned by the `insert` method. This is used to allow zero-cost
    /// synchronous and asynchronous implementations of the `WritableCache` trait.
    type Fut<'a, T: 'a>: Future<Output = T> + 'a
    where
        Self: 'a,
        K: 'a,
        V: 'a,
        E: 'a;

    /// Insert the given value into the cache with the given key. If the key was already present in
    /// the cache, return the old value. Otherwise, return `None`.
    fn insert<'a>(&'a self, key: &'a K, value: V) -> Self::Fut<'a, Result<(), E>>
    where
        V: 'a;
}

/// Convenience trait for implementing `WritableCache` with synchronous cache accesses.
///
/// Avoid using this cache as a generic parameter. `WritableCache` is the correct trait to use for
/// generic parameters, and will support both synchronous and asynchronous implementations of the
/// cache.
pub trait SyncWritableCache<K, V, E>
where
    K: Eq + Hash,
    V: Clone,
    E: std::error::Error,
{
    /// Insert the given value into the cache with the given key. If the key was already present in
    /// the cache, return the old value. Otherwise, return `None`.
    fn insert(&self, key: &K, value: V) -> Result<(), E>;
}

/// Convenience trait for implementing `WritableCache` with asynchronous cache accesses.
///
/// Avoid using this cache as a generic parameter. `WritableCache` is the correct trait to use for
/// generic parameters, and will support both synchronous and asynchronous implementations of the
/// cache.
#[expect(async_fn_in_trait)]
pub trait AsyncWritableCache<K, V, E>
where
    K: Eq + Hash,
    V: Clone,
    E: std::error::Error,
{
    /// Insert the given value into the cache with the given key. If the key was already present in
    /// the cache, return the old value. Otherwise, return `None`.
    async fn insert(&self, key: &K, value: V) -> Result<(), E>;
}

impl<K: Eq + Hash, V: Clone, E: std::error::Error, T: AsyncWritableCache<K, V, E>>
    !SyncWritableCache<K, V, E> for T
{
}

impl<K: Eq + Hash, V: Clone, E: std::error::Error, T: AsyncWritableCache<K, V, E>>
    WritableCache<K, V, E> for T
{
    type Fut<'a, O: 'a>
        = Pin<Box<dyn Future<Output = O> + 'a>>
    where
        Self: 'a,
        K: 'a,
        V: 'a,
        E: 'a;

    fn insert<'a>(&'a self, key: &'a K, value: V) -> Self::Fut<'a, Result<(), E>>
    where
        V: 'a,
        E: 'a,
    {
        Box::pin(AsyncWritableCache::insert(self, key, value))
    }
}

impl<K: Eq + Hash, V: Clone, E: std::error::Error, T: SyncWritableCache<K, V, E>>
    WritableCache<K, V, E> for T
{
    type Fut<'a, O: 'a>
        = std::future::Ready<O>
    where
        Self: 'a,
        K: 'a,
        V: 'a,
        E: 'a;

    fn insert<'a>(&'a self, key: &'a K, value: V) -> Self::Fut<'a, Result<(), E>>
    where
        V: 'a,
        E: 'a,
    {
        std::future::ready(SyncWritableCache::insert(self, key, value))
    }
}
