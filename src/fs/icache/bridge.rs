use crate::fs::r#trait::{FileAttr, FileHandle, Inode};

/// Bidirectional bridge for both inodes and file handles between two Fs layers.
///
/// Convention: **left = outer (caller), right = inner (callee)**.
/// `forward(left)` → right, `backward(right)` → left.
pub struct HashMapBridge {
    inode_map: bimap::BiMap<Inode, Inode>,
    fh_map: bimap::BiMap<FileHandle, FileHandle>,
}

impl HashMapBridge {
    pub fn new() -> Self {
        Self {
            inode_map: bimap::BiMap::new(),
            fh_map: bimap::BiMap::new(),
        }
    }

    // ── Inode methods ────────────────────────────────────────────────────

    pub fn insert_inode(&mut self, left: Inode, right: Inode) {
        self.inode_map.insert(left, right);
    }

    /// Look up right→left, or allocate a new left inode if unmapped.
    pub fn backward_or_insert_inode(
        &mut self,
        right: Inode,
        allocate: impl FnOnce() -> Inode,
    ) -> Inode {
        if let Some(&left) = self.inode_map.get_by_right(&right) {
            left
        } else {
            let left = allocate();
            self.inode_map.insert(left, right);
            left
        }
    }

    /// Look up left→right, or allocate a new right inode if unmapped.
    pub fn forward_or_insert_inode(
        &mut self,
        left: Inode,
        allocate: impl FnOnce() -> Inode,
    ) -> Inode {
        if let Some(&right) = self.inode_map.get_by_left(&left) {
            right
        } else {
            let right = allocate();
            self.inode_map.insert(left, right);
            right
        }
    }

    /// Remove an inode mapping by its left (outer) key.
    pub fn remove_inode_by_left(&mut self, left: Inode) {
        self.inode_map.remove_by_left(&left);
    }

    /// Look up left→right directly.
    pub fn inode_map_get_by_left(&self, left: Inode) -> Option<&Inode> {
        self.inode_map.get_by_left(&left)
    }

    /// Rewrite the `ino` field in a [`FileAttr`] from right (inner) to left (outer) namespace.
    pub fn attr_backward(&self, attr: FileAttr) -> FileAttr {
        let backward = |ino: Inode| -> Inode {
            if let Some(&left) = self.inode_map.get_by_right(&ino) {
                left
            } else {
                tracing::warn!(
                    inner_ino = ino,
                    "attr_backward: no bridge mapping, using raw inner inode"
                );
                ino
            }
        };
        rewrite_attr_ino(attr, backward)
    }

    // ── File handle methods ──────────────────────────────────────────────

    pub fn insert_fh(&mut self, left: FileHandle, right: FileHandle) {
        self.fh_map.insert(left, right);
    }

    pub fn fh_forward(&self, left: FileHandle) -> Option<FileHandle> {
        self.fh_map.get_by_left(&left).copied()
    }

    /// Remove a file handle mapping by its left (outer) key.
    pub fn remove_fh_by_left(&mut self, left: FileHandle) {
        self.fh_map.remove_by_left(&left);
    }
}

/// Rewrite the `ino` field in a [`FileAttr`] using the given translation function.
fn rewrite_attr_ino(attr: FileAttr, translate: impl Fn(Inode) -> Inode) -> FileAttr {
    match attr {
        FileAttr::RegularFile {
            mut common,
            size,
            blocks,
        } => {
            common.ino = translate(common.ino);
            FileAttr::RegularFile {
                common,
                size,
                blocks,
            }
        }
        FileAttr::Directory { mut common } => {
            common.ino = translate(common.ino);
            FileAttr::Directory { common }
        }
        FileAttr::Symlink { mut common, size } => {
            common.ino = translate(common.ino);
            FileAttr::Symlink { common, size }
        }
        FileAttr::CharDevice { mut common, rdev } => {
            common.ino = translate(common.ino);
            FileAttr::CharDevice { common, rdev }
        }
        FileAttr::BlockDevice { mut common, rdev } => {
            common.ino = translate(common.ino);
            FileAttr::BlockDevice { common, rdev }
        }
        FileAttr::NamedPipe { mut common } => {
            common.ino = translate(common.ino);
            FileAttr::NamedPipe { common }
        }
        FileAttr::Socket { mut common } => {
            common.ino = translate(common.ino);
            FileAttr::Socket { common }
        }
    }
}
