//! Generic inode table with reference counting and file handle allocation.

use std::collections::HashMap;

use tracing::{trace, warn};

use crate::fs::r#trait::{FileHandle, Inode};

use super::IcbLike;

/// Generic directory cache.
///
/// Owns an inode table and a file handle counter. Provides reference counting,
/// ICB lookup/insertion, and file handle allocation.
pub struct ICache<I: IcbLike> {
    inode_table: HashMap<Inode, I>,
    next_fh: FileHandle,
}

impl<I: IcbLike> ICache<I> {
    /// Create a new `ICache` with a root ICB at `root_ino` (rc=1).
    pub fn new(root_ino: Inode, root_path: impl Into<std::path::PathBuf>) -> Self {
        let mut inode_table = HashMap::new();
        inode_table.insert(root_ino, I::new_root(root_path.into()));
        Self {
            inode_table,
            next_fh: 1,
        }
    }

    /// Allocate a file handle (increments `next_fh` and returns the old value).
    pub fn allocate_fh(&mut self) -> FileHandle {
        let fh = self.next_fh;
        self.next_fh += 1;
        fh
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

    /// Insert an ICB directly.
    pub fn insert_icb(&mut self, ino: Inode, icb: I) {
        self.inode_table.insert(ino, icb);
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

    /// Increment rc. Panics (via unwrap) if inode doesn't exist.
    pub fn inc_rc(&mut self, ino: Inode) -> u64 {
        let icb = self
            .inode_table
            .get_mut(&ino)
            .unwrap_or_else(|| unreachable!("inc_rc: inode {ino} not in table"));
        *icb.rc_mut() += 1;
        icb.rc()
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

    pub fn iter(&self) -> impl Iterator<Item = (&Inode, &I)> {
        self.inode_table.iter()
    }
}
