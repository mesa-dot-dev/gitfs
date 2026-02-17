use std::hash::Hash;

/// A readable cache that can be read from asynchronously.
#[expect(async_fn_in_trait)]
pub trait AsyncReadableCache<K, V>
where
    K: Eq + Hash,
    V: Clone,
{
    /// Fetch the value associated with the given key from the cache. If the key is not present in
    /// the cache, return `None`.
    async fn get(&self, key: &K) -> Option<V>;

    /// Check if the cache contains the given key.
    async fn contains(&self, key: &K) -> bool;
}

/// A writable cache that can be written to asynchronously.
///
/// Note that this trait does not require the cache to support manual eviction of entries. Many
/// caches only support automatic eviction (such as LRU eviction), and this trait is designed to
/// support those caches as well.
#[expect(async_fn_in_trait)]
pub trait AsyncWritableCache<K, V, E>
where
    K: Eq + Hash,
    V: Clone,
    E: std::error::Error,
{
    /// Insert the given value into the cache with the given key.
    async fn insert(&self, key: &K, value: V) -> Result<(), E>;
}
