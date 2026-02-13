use std::{future::Future, hash::Hash, pin::Pin};

pub trait ReadableCache<K, V>
where
    K: Eq + Hash,
    V: Clone,
{
    type Fut<'a, T: 'a>: Future<Output = T> + 'a
    where
        Self: 'a,
        K: 'a,
        V: 'a;

    fn get<'a>(&'a self, key: &'a K) -> Self::Fut<'a, Option<V>>
    where
        V: 'a;
    fn contains<'a>(&'a self, key: &'a K) -> Self::Fut<'a, bool>
    where
        V: 'a;
}

pub trait SyncReadableCache<K, V>
where
    K: Eq + Hash,
    V: Clone,
{
    fn get(&self, key: &K) -> Option<V>;
    fn contains(&self, key: &K) -> bool;
}

pub trait AsyncReadableCache<K, V>
where
    K: Eq + Hash,
    V: Clone,
{
    async fn get(&self, key: &K) -> Option<V>;
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
