//! Generic directory cache and inode management primitives.

pub mod bridge;
mod cache;
mod inode_factory;

pub use cache::ICache;
pub use inode_factory::InodeFactory;

/// Common interface for inode control block types usable with `ICache`.
pub trait IcbLike {
    /// Create an ICB with rc=1, the given path, and no children.
    fn new_root(path: std::path::PathBuf) -> Self;
    fn rc(&self) -> u64;
    fn rc_mut(&mut self) -> &mut u64;
}
