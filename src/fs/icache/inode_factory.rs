use crate::fs::r#trait::Inode;
use std::sync::atomic::{AtomicU64, Ordering};

/// Monotonically increasing inode allocator.
pub struct InodeFactory {
    next_inode: AtomicU64,
}

impl InodeFactory {
    pub fn new(start: Inode) -> Self {
        Self {
            next_inode: AtomicU64::new(start),
        }
    }

    pub fn allocate(&self) -> Inode {
        self.next_inode.fetch_add(1, Ordering::Relaxed)
    }
}
