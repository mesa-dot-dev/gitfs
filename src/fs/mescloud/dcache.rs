//! Mescloud-specific inode control block and helpers.

use crate::fs::dcache::IcbLike;
use crate::fs::r#trait::{DirEntry, FileAttr, Inode};

/// Inode control block for mescloud filesystem layers (MesaFS, OrgFs, RepoFs).
pub struct InodeControlBlock {
    /// The root inode doesn't have a parent.
    pub parent: Option<Inode>,
    pub rc: u64,
    pub path: std::path::PathBuf,
    pub children: Option<Vec<DirEntry>>,
    /// Cached file attributes from the last lookup.
    pub attr: Option<FileAttr>,
}

impl IcbLike for InodeControlBlock {
    fn new_root(path: std::path::PathBuf) -> Self {
        Self {
            rc: 1,
            parent: None,
            path,
            children: None,
            attr: None,
        }
    }

    fn rc(&self) -> u64 {
        self.rc
    }

    fn rc_mut(&mut self) -> &mut u64 {
        &mut self.rc
    }
}

/// Calculate the number of blocks needed for a given size.
pub fn blocks_of_size(block_size: u32, size: u64) -> u64 {
    size.div_ceil(u64::from(block_size))
}
