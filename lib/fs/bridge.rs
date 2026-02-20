//! Lock-free bidirectional inode address mapping.
//!
//! [`ConcurrentBridge`] maps between "outer" (composite) and "inner" (child)
//! inode address spaces using two [`scc::HashMap`]s.

use crate::fs::InodeAddr;

/// Bidirectional inode mapping between outer (composite) and inner (child) address spaces.
///
/// Uses two lock-free `scc::HashMap`s. Insertion order: forward map first,
/// then backward map, so any observer that discovers an outer addr via
/// `backward` can immediately resolve it via `forward`.
pub struct ConcurrentBridge {
    /// outer -> inner
    fwd: scc::HashMap<InodeAddr, InodeAddr>,
    /// inner -> outer
    bwd: scc::HashMap<InodeAddr, InodeAddr>,
}

impl ConcurrentBridge {
    /// Creates an empty bridge.
    #[must_use]
    pub fn new() -> Self {
        Self {
            fwd: scc::HashMap::new(),
            bwd: scc::HashMap::new(),
        }
    }

    /// Insert a mapping from outer to inner.
    ///
    /// Inserts into the forward map first (see module docs for ordering rationale).
    pub fn insert(&self, outer: InodeAddr, inner: InodeAddr) {
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
    pub fn backward_or_insert(
        &self,
        inner: InodeAddr,
        allocate: impl FnOnce() -> InodeAddr,
    ) -> InodeAddr {
        if let Some(outer) = self.backward(inner) {
            return outer;
        }
        let outer = allocate();
        self.insert(outer, inner);
        outer
    }

    /// Remove the mapping for the given outer address.
    pub fn remove_by_outer(&self, outer: InodeAddr) {
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
