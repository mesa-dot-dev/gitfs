use std::collections::HashMap;

use crate::fs::icache::bridge::HashMapBridge;
use crate::fs::icache::{FileTable, IcbResolver};
use crate::fs::r#trait::{DirEntry, Inode};

use super::icache::{InodeControlBlock, MescloudICache};

/// A child filesystem slot: inner filesystem + bidirectional inode/fh bridge.
#[expect(
    dead_code,
    reason = "will be used when MesaFS/OrgFs are refactored to use CompositeFs"
)]
pub(super) struct ChildSlot<Inner> {
    pub inner: Inner,
    pub bridge: HashMapBridge,
}

/// Generic compositing filesystem that delegates to child `Inner` filesystems.
///
/// Holds the shared infrastructure (icache, file table, readdir buffer, child
/// slots) and implements all the delegation methods that `MesaFS` and `OrgFs`
/// previously duplicated.
#[expect(
    dead_code,
    reason = "will be used when MesaFS/OrgFs are refactored to use CompositeFs"
)]
pub(super) struct CompositeFs<R, Inner>
where
    R: IcbResolver<Icb = InodeControlBlock>,
{
    pub icache: MescloudICache<R>,
    pub file_table: FileTable,
    pub readdir_buf: Vec<DirEntry>,
    /// Maps outer inode to index into `slots` for child-root inodes.
    pub child_inodes: HashMap<Inode, usize>,
    pub slots: Vec<ChildSlot<Inner>>,
}
