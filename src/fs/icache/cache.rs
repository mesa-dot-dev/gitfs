//! Generic inode table with reference counting.

use std::collections::HashMap;

use tracing::{trace, warn};

use crate::fs::r#trait::Inode;

use super::IcbLike;

/// Generic directory cache.
///
/// Owns an inode table. Provides reference counting and ICB lookup/insertion.
pub struct ICache<I: IcbLike> {
    inode_table: HashMap<Inode, I>,
}

impl<I: IcbLike> ICache<I> {
    /// Create a new `ICache` with a root ICB at `root_ino` (rc=1).
    pub fn new(root_ino: Inode, root_path: impl Into<std::path::PathBuf>) -> Self {
        let mut inode_table = HashMap::new();
        inode_table.insert(root_ino, I::new_root(root_path.into()));
        Self { inode_table }
    }

    pub fn get_icb(&self, ino: Inode) -> Option<&I> {
        self.inode_table.get(&ino)
    }

    pub fn get_icb_mut(&mut self, ino: Inode) -> Option<&mut I> {
        self.inode_table.get_mut(&ino)
    }

    pub fn contains(&self, ino: Inode) -> bool {
        self.inode_table.contains_key(&ino)
    }

    /// Insert an ICB only if absent.
    /// Returns a mutable reference to the (possibly pre-existing) ICB.
    pub fn entry_or_insert_icb(&mut self, ino: Inode, f: impl FnOnce() -> I) -> &mut I {
        self.inode_table.entry(ino).or_insert_with(f)
    }

    /// Number of inodes in the table.
    pub fn inode_count(&self) -> usize {
        self.inode_table.len()
    }

    /// Decrement rc by `nlookups`. Returns `Some(evicted_icb)` if the inode was evicted.
    pub fn forget(&mut self, ino: Inode, nlookups: u64) -> Option<I> {
        match self.inode_table.entry(ino) {
            std::collections::hash_map::Entry::Occupied(mut entry) => {
                if entry.get().rc() <= nlookups {
                    trace!(ino, "evicting inode");
                    Some(entry.remove())
                } else {
                    *entry.get_mut().rc_mut() -= nlookups;
                    trace!(ino, new_rc = entry.get().rc(), "decremented rc");
                    None
                }
            }
            std::collections::hash_map::Entry::Vacant(_) => {
                warn!(ino, "forget on unknown inode");
                None
            }
        }
    }
}
