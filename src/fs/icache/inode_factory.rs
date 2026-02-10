use crate::fs::r#trait::Inode;

/// Monotonically increasing inode allocator.
pub struct InodeFactory {
    next_inode: Inode,
}

impl InodeFactory {
    pub fn new(start: Inode) -> Self {
        Self { next_inode: start }
    }

    pub fn allocate(&mut self) -> Inode {
        let ino = self.next_inode;
        self.next_inode += 1;
        ino
    }
}
