use std::sync::RwLock;

use crate::fs::r#trait::{FileAttr, FileHandle, Inode};

/// Bidirectional bridge for both inodes and file handles between two Fs layers.
///
/// Convention: **left = outer (caller), right = inner (callee)**.
/// `forward(left)` → right, `backward(right)` → left.
///
/// All methods take `&self` — interior mutability is provided by `RwLock`.
pub struct HashMapBridge {
    inode_map: RwLock<bimap::BiMap<Inode, Inode>>,
    fh_map: RwLock<bimap::BiMap<FileHandle, FileHandle>>,
}

#[expect(
    clippy::expect_used,
    reason = "RwLock poisoning is unrecoverable; panicking is the correct behavior"
)]
impl HashMapBridge {
    pub fn new() -> Self {
        Self {
            inode_map: RwLock::new(bimap::BiMap::new()),
            fh_map: RwLock::new(bimap::BiMap::new()),
        }
    }

    pub fn insert_inode(&self, left: Inode, right: Inode) {
        self.inode_map
            .write()
            .expect("poisoned")
            .insert(left, right);
    }

    /// Look up right→left, or allocate a new left inode if unmapped.
    pub fn backward_or_insert_inode(
        &self,
        right: Inode,
        allocate: impl FnOnce() -> Inode,
    ) -> Inode {
        // Fast path: read-only check.
        if let Some(&left) = self
            .inode_map
            .read()
            .expect("poisoned")
            .get_by_right(&right)
        {
            return left;
        }
        // Slow path: acquire write lock and double-check.
        let mut map = self.inode_map.write().expect("poisoned");
        if let Some(&left) = map.get_by_right(&right) {
            return left;
        }
        let left = allocate();
        map.insert(left, right);
        left
    }

    /// Look up left→right, or allocate a new right inode if unmapped.
    pub fn forward_or_insert_inode(&self, left: Inode, allocate: impl FnOnce() -> Inode) -> Inode {
        // Fast path: read-only check.
        if let Some(&right) = self.inode_map.read().expect("poisoned").get_by_left(&left) {
            return right;
        }
        // Slow path: acquire write lock and double-check.
        let mut map = self.inode_map.write().expect("poisoned");
        if let Some(&right) = map.get_by_left(&left) {
            return right;
        }
        let right = allocate();
        map.insert(left, right);
        right
    }

    /// Remove an inode mapping by its left (outer) key.
    pub fn remove_inode_by_left(&self, left: Inode) {
        self.inode_map
            .write()
            .expect("poisoned")
            .remove_by_left(&left);
    }

    /// Look up left→right directly.
    pub fn inode_map_get_by_left(&self, left: Inode) -> Option<Inode> {
        self.inode_map
            .read()
            .expect("poisoned")
            .get_by_left(&left)
            .copied()
    }

    /// Rewrite the `ino` field in a [`FileAttr`] from right (inner) to left (outer) namespace.
    pub fn attr_backward(&self, attr: FileAttr) -> FileAttr {
        let map = self.inode_map.read().expect("poisoned");
        let backward = |ino: Inode| -> Inode {
            if let Some(&left) = map.get_by_right(&ino) {
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

    pub fn insert_fh(&self, left: FileHandle, right: FileHandle) {
        self.fh_map.write().expect("poisoned").insert(left, right);
    }

    pub fn fh_forward(&self, left: FileHandle) -> Option<FileHandle> {
        self.fh_map
            .read()
            .expect("poisoned")
            .get_by_left(&left)
            .copied()
    }

    /// Remove a file handle mapping by its left (outer) key.
    pub fn remove_fh_by_left(&self, left: FileHandle) {
        self.fh_map.write().expect("poisoned").remove_by_left(&left);
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
