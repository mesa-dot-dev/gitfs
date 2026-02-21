//! Bidirectional inode address mapping.
//!
//! [`ConcurrentBridge`] maps between "outer" (composite) and "inner" (child)
//! inode address spaces using two [`scc::HashMap`]s guarded by a coordination
//! lock for cross-map atomicity.

use std::sync::Mutex;

use crate::fs::InodeAddr;

/// Bidirectional inode mapping between outer (composite) and inner (child) address spaces.
///
/// Uses two concurrent `scc::HashMap`s for lock-free reads. Mutations that
/// touch both maps are serialized by a `Mutex<()>` to prevent cross-map
/// inconsistencies (e.g. a concurrent `remove_by_outer` between the two
/// `insert_sync` calls in `insert` could leave orphaned entries).
pub struct ConcurrentBridge {
    /// outer -> inner
    fwd: scc::HashMap<InodeAddr, InodeAddr>,
    /// inner -> outer
    bwd: scc::HashMap<InodeAddr, InodeAddr>,
    /// Serializes mutations that touch both maps.
    mu: Mutex<()>,
}

impl ConcurrentBridge {
    /// Creates an empty bridge.
    #[must_use]
    pub fn new() -> Self {
        Self {
            fwd: scc::HashMap::new(),
            bwd: scc::HashMap::new(),
            mu: Mutex::new(()),
        }
    }

    /// Insert a mapping from outer to inner.
    ///
    /// Serialized with other mutations via the coordination lock.
    pub fn insert(&self, outer: InodeAddr, inner: InodeAddr) {
        let _guard = self
            .mu
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let _ = self.fwd.insert_sync(outer, inner);
        let _ = self.bwd.insert_sync(inner, outer);
    }

    /// Resolve outer -> inner.
    ///
    /// This read is **not** serialized with mutations. A concurrent [`insert`]
    /// may have completed the forward entry but not yet the backward entry (or
    /// vice versa for [`remove_by_outer`]). Callers must tolerate stale or
    /// transiently-missing results. Use [`backward_or_insert`] when
    /// cross-map consistency is required.
    ///
    /// [`insert`]: Self::insert
    /// [`remove_by_outer`]: Self::remove_by_outer
    /// [`backward_or_insert`]: Self::backward_or_insert
    #[must_use]
    pub fn forward(&self, outer: InodeAddr) -> Option<InodeAddr> {
        self.fwd.read_sync(&outer, |_, &v| v)
    }

    /// Resolve inner -> outer.
    ///
    /// This read is **not** serialized with mutations. See [`forward`] for
    /// the consistency caveats. Use [`backward_or_insert`] when cross-map
    /// consistency is required.
    ///
    /// [`forward`]: Self::forward
    /// [`backward_or_insert`]: Self::backward_or_insert
    #[must_use]
    pub fn backward(&self, inner: InodeAddr) -> Option<InodeAddr> {
        self.bwd.read_sync(&inner, |_, &v| v)
    }

    /// Look up inner -> outer, or allocate a new outer address if unmapped.
    ///
    /// Serialized with other mutations via the coordination lock.
    #[must_use]
    pub fn backward_or_insert(
        &self,
        inner: InodeAddr,
        allocate: impl FnOnce() -> InodeAddr,
    ) -> InodeAddr {
        let _guard = self
            .mu
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        match self.bwd.entry_sync(inner) {
            scc::hash_map::Entry::Occupied(occ) => *occ.get(),
            scc::hash_map::Entry::Vacant(vac) => {
                let outer = allocate();
                vac.insert_entry(outer);
                let _ = self.fwd.insert_sync(outer, inner);
                outer
            }
        }
    }

    /// Remove the mapping for the given outer address.
    ///
    /// Returns `true` if the bridge is empty after the removal — the caller
    /// can use this to garbage-collect the owning slot. The emptiness check
    /// is performed under the coordination lock so there is no TOCTOU gap
    /// with the removal itself.
    pub fn remove_by_outer(&self, outer: InodeAddr) -> bool {
        let _guard = self
            .mu
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some((_, inner)) = self.fwd.remove_sync(&outer) {
            self.bwd.remove_sync(&inner);
        }
        self.fwd.is_empty()
    }

    /// Returns `true` if the bridge contains no mappings.
    ///
    /// Reads are not serialized with mutations. The result is a
    /// snapshot that may be immediately stale. Use under the
    /// coordination lock or an external guard when consistency
    /// with mutations is required.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.fwd.is_empty()
    }
}

impl Default for ConcurrentBridge {
    fn default() -> Self {
        Self::new()
    }
}
