use std::sync::atomic::{AtomicU64, Ordering};

use crate::fs::r#trait::FileHandle;

/// Monotonically increasing file handle allocator.
#[must_use]
pub struct FileTable {
    next_fh: AtomicU64,
}

impl FileTable {
    pub fn new() -> Self {
        Self {
            next_fh: AtomicU64::new(1),
        }
    }

    #[must_use]
    pub fn allocate(&self) -> FileHandle {
        self.next_fh.fetch_add(1, Ordering::Relaxed)
    }
}
