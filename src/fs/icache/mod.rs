//! Generic directory cache and inode management primitives.

#[cfg_attr(not(test), expect(dead_code))]
pub mod async_cache;
pub mod bridge;
mod cache;
mod inode_factory;

#[expect(unused_imports)]
pub use async_cache::AsyncICache;
#[expect(unused_imports)]
pub use async_cache::IcbResolver;
pub use cache::ICache;
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
