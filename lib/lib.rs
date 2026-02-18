//! git-fs shared library.

/// Caching primitives for git-fs.
pub mod cache;
pub mod drop_ward;
/// Filesystem abstractions and caching layers.
pub mod fs;
pub mod io;
