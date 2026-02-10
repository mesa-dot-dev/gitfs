use std::sync::atomic::{AtomicU64, Ordering};

use crate::fs::r#trait::FileHandle;

/// Monotonically increasing file handle allocator.
pub struct FileTable {
    next_fh: AtomicU64,
}

impl FileTable {
    pub fn new() -> Self {
        Self {
            next_fh: AtomicU64::new(1),
        }
    }

    pub fn allocate(&self) -> FileHandle {
        self.next_fh.fetch_add(1, Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn allocate_returns_monotonic_handles() {
        let ft = FileTable::new();
        assert_eq!(ft.allocate(), 1);
        assert_eq!(ft.allocate(), 2);
        assert_eq!(ft.allocate(), 3);
    }
}
