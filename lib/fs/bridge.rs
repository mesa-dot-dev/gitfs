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
    #[must_use]
    pub fn forward(&self, outer: InodeAddr) -> Option<InodeAddr> {
        self.fwd.read_sync(&outer, |_, &v| v)
    }

    /// Resolve inner -> outer.
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
    /// Serialized with other mutations via the coordination lock.
    pub fn remove_by_outer(&self, outer: InodeAddr) {
        let _guard = self
            .mu
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some((_, inner)) = self.fwd.remove_sync(&outer) {
            self.bwd.remove_sync(&inner);
        }
    }
}

impl Default for ConcurrentBridge {
    fn default() -> Self {
        Self::new()
    }
}
