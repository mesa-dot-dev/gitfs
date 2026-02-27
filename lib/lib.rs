//! git-fs shared library.

/// Caching primitives for git-fs.
pub mod cache;
pub mod drop_ward;
/// Filesystem abstractions and caching layers.
pub mod fs;
/// Gitignore-aware path matching.
pub mod ignore_tracker;
pub mod io;
