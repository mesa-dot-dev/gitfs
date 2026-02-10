//! Generic directory cache and inode management primitives.

pub mod async_cache;
pub mod bridge;
mod file_table;
mod inode_factory;

pub use async_cache::AsyncICache;
pub use async_cache::IcbResolver;
pub use file_table::FileTable;
pub use inode_factory::InodeFactory;

/// Common interface for inode control block types usable with `ICache`.
pub trait IcbLike {
    /// Create an ICB with rc=1, the given path, and no children.
    fn new_root(path: std::path::PathBuf) -> Self;
    fn rc(&self) -> u64;
    fn rc_mut(&mut self) -> &mut u64;
    /// Returns true if this entry needs resolution (e.g., attr not yet fetched).
    fn needs_resolve(&self) -> bool;
}
