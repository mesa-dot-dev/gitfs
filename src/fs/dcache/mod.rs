//! Generic directory cache and inode management primitives.

mod dcache;
pub mod bridge;
mod table;

pub use dcache::MescloudDCache;
pub use table::DCache;

/// Common interface for inode control block types usable with `DCache`.
pub trait IcbLike {
    /// Create an ICB with rc=1, the given path, and no children.
    fn new_root(path: std::path::PathBuf) -> Self;
    fn rc(&self) -> u64;
    fn rc_mut(&mut self) -> &mut u64;
}
