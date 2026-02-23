//! [`IndexedLookupCache`]: a reverse-indexed wrapper around [`LookupCache`]
//! for O(k) eviction of lookup-cache entries by inode address.

use std::ffi::OsStr;
use std::future::Future;
use std::sync::Arc;

use super::async_fs::LookupCache;
use super::{INode, InodeAddr};

type LookupKey = (InodeAddr, Arc<OsStr>);

/// A reverse-index entry: the lookup-cache key plus the child inode addr
/// that the key resolved to. Storing the child addr allows [`IndexedLookupCache::evict_addr`]
/// to clean both the parent and child sides of the reverse index without
/// needing to read the (already-removed) cache value.
type ReverseEntry = (LookupKey, InodeAddr);

/// Wraps a [`LookupCache`] with a reverse index for O(k) eviction.
///
/// The reverse index maps each `InodeAddr` to the set of lookup-cache
/// keys that reference it (either as parent or as child). This avoids
/// the O(N) `retain_sync` scan that would otherwise be required when
/// evicting a single inode.
///
/// Unlike [`DCache`](super::dcache::DCache)'s 1:1 `child_to_parent`
/// reverse index (where each child has exactly one parent), the lookup
/// cache maps one inode address to *multiple* cache keys (because an
/// inode can appear as both a parent and a child in different entries).
/// Hence we use `Vec<ReverseEntry>` rather than a flat map.
pub struct IndexedLookupCache {
    cache: LookupCache,
    /// addr → set of `(key, child_addr)` pairs where `addr` appears as
    /// parent or child.
    reverse: scc::HashMap<InodeAddr, Vec<ReverseEntry>>,
}

impl Default for IndexedLookupCache {
    fn default() -> Self {
        Self {
            cache: LookupCache::default(),
            reverse: scc::HashMap::new(),
        }
    }
}

impl IndexedLookupCache {
    /// Delegate to the inner cache's `get_or_try_init`, then record the
    /// result in the reverse index.
    pub async fn get_or_try_init<F, Fut>(
        &self,
        key: LookupKey,
        factory: F,
    ) -> Result<INode, std::io::Error>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<INode, std::io::Error>> + Send + 'static,
    {
        let child = self.cache.get_or_try_init(key.clone(), factory).await?;
        self.index_entry(&key, child.addr);
        Ok(child)
    }

    /// Remove a single key from the cache and its reverse-index entries.
    pub fn remove_sync(&self, key: &LookupKey) {
        if self.cache.remove_sync(key) {
            self.deindex_key(key);
        }
    }

    /// Remove all lookup-cache entries referencing `addr` (as parent or child).
    ///
    /// O(k) where k is the number of entries referencing `addr`, vs the
    /// previous O(N) scan over the entire cache.
    pub fn evict_addr(&self, addr: InodeAddr) {
        let entries = self
            .reverse
            .remove_sync(&addr)
            .map(|(_, entries)| entries)
            .unwrap_or_default();

        for (key, child_addr) in &entries {
            let (parent_addr, _) = key;
            // When evicting by parent (parent_addr == addr), all lookups
            // under that parent are invalid — remove unconditionally.
            // When evicting by child, only remove if the cache still maps
            // to the old child; a concurrent re-lookup may have replaced
            // the value with a new child under the same key.
            if *parent_addr == addr {
                self.cache.remove_sync(key);
            } else {
                self.cache
                    .remove_if_ready_sync(key, |inode| inode.addr == addr);
            }
            // Clean the *other* side(s) of the reverse index.
            // We removed `addr`'s Vec already; now prune the key from
            // whichever other addrs it was indexed under.
            //
            // The retain predicate matches on both key and child_addr to
            // avoid spuriously removing a freshly-indexed entry for a
            // *different* child that reuses the same LookupKey.
            if *parent_addr != addr {
                self.reverse.update_sync(parent_addr, |_, v| {
                    v.retain(|(k, ca)| !(k == key && *ca == *child_addr));
                });
            }
            if *child_addr != addr && *child_addr != *parent_addr {
                self.reverse.update_sync(child_addr, |_, v| {
                    v.retain(|(k, ca)| !(k == key && *ca == *child_addr));
                });
            }
        }
    }

    /// Record a lookup entry in the reverse index for both parent and child addrs.
    ///
    /// Deduplicates: if the key is already present in the `Vec` for a given
    /// addr, the push is skipped. This prevents unbounded growth when the
    /// same key is looked up repeatedly (cache hits still call this method
    /// because the `FutureBackedCache` joiner path returns without
    /// distinguishing hits from misses).
    fn index_entry(&self, key: &LookupKey, child_addr: InodeAddr) {
        let entry = (key.clone(), child_addr);
        let (parent_addr, _) = key;
        // Index under parent addr (deduplicated).
        self.reverse
            .entry_sync(*parent_addr)
            .or_default()
            .get_mut()
            .dedup_push(&entry);
        // Index under child addr (if different from parent, deduplicated).
        if child_addr != *parent_addr {
            self.reverse
                .entry_sync(child_addr)
                .or_default()
                .get_mut()
                .dedup_push(&entry);
        }
    }

    /// Returns the number of entries in the reverse-index `Vec` for `addr`.
    ///
    /// Intended for testing only — verifies that the reverse index stays
    /// bounded and does not accumulate duplicates.
    #[doc(hidden)]
    #[must_use]
    pub fn reverse_entry_count(&self, addr: InodeAddr) -> usize {
        self.reverse
            .read_sync(&addr, |_, entries| entries.len())
            .unwrap_or(0)
    }

    /// Remove a single key's entries from the reverse index.
    ///
    /// Cleans the parent side. The child side cannot be cleaned here because
    /// the cache value (which held the child addr) has already been removed.
    /// This is acceptable: orphaned child-side entries are harmless (they
    /// reference a key that no longer exists in the cache) and are cleaned
    /// up when the child addr is eventually evicted via [`evict_addr`].
    fn deindex_key(&self, key: &LookupKey) {
        let (parent_addr, _) = key;
        self.reverse.update_sync(parent_addr, |_, entries| {
            entries.retain(|(k, _)| k != key);
        });
    }
}

/// Extension trait for `Vec` to push only if the element is not already present.
trait DedupPush<T: PartialEq + Clone> {
    fn dedup_push(&mut self, item: &T);
}

impl<T: PartialEq + Clone> DedupPush<T> for Vec<T> {
    fn dedup_push(&mut self, item: &T) {
        if !self.contains(item) {
            self.push(item.clone());
        }
    }
}
