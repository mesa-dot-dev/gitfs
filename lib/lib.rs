//! git-fs shared library.

#![feature(negative_impls)]
#![feature(with_negative_coherence)]
#![feature(fn_traits)]

/// Caching primitives for git-fs.
pub mod cache;
pub mod io;
