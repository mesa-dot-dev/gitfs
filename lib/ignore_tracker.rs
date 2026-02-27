use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};

use ignore::gitignore::{Gitignore, GitignoreBuilder};

/// Errors that can occur when observing a new ignore file.
#[derive(Debug, thiserror::Error)]
pub enum ObserveError {
    /// The ignore file could not be parsed.
    #[error("failed to parse ignore file at {path}: {source}")]
    Parse {
        /// Path to the ignore file that failed to parse.
        path: PathBuf,
        /// Underlying parse error from the `ignore` crate.
        source: ignore::Error,
    },
    /// The rebuilt gitignore matcher failed to compile.
    #[error("failed to build gitignore matcher: {source}")]
    Build {
        /// Underlying build error from the `ignore` crate.
        #[source]
        source: ignore::Error,
    },
}

/// Holds the mutable state behind `RwLock`.
struct IgnoreTrackerInner {
    /// Compiled gitignore matcher built from all observed ignore files.
    matcher: Gitignore,
    /// Paths of all ignore files observed so far, kept to rebuild the matcher
    /// when a new file is added (the `ignore` crate's `Gitignore` is immutable
    /// once built).
    observed_files: Vec<PathBuf>,
}

/// Thread-safe tracker for `.gitignore` rules.
///
/// Wraps [`ignore::gitignore::Gitignore`] behind a [`std::sync::RwLock`] so
/// that reads (`is_abspath_ignored`) can proceed concurrently while writes
/// (`observe_ignorefile`) serialize.
///
/// # Concurrency
///
/// Uses `std::sync::RwLock` (NOT `tokio::sync::RwLock`).  All lock
/// acquisitions are scoped and synchronous — never held across `.await`
/// points.  `std::sync::RwLock` has lower overhead in the uncontended case.
/// Do NOT introduce `.await` calls while holding a guard.
#[derive(Clone)]
pub struct IgnoreTracker {
    root: PathBuf,
    inner: Arc<RwLock<IgnoreTrackerInner>>,
}

impl IgnoreTracker {
    /// Create a new ignore tracker rooted at the given directory.
    ///
    /// The root is the top-level directory against which all paths are
    /// resolved (typically the repository root).
    #[must_use]
    pub fn new(root: PathBuf) -> Self {
        let matcher = Gitignore::empty();
        Self {
            root,
            inner: Arc::new(RwLock::new(IgnoreTrackerInner {
                matcher,
                observed_files: Vec::new(),
            })),
        }
    }

    /// Test whether the given absolute path is ignored.
    ///
    /// The path must be absolute (or at least relative to the tracker root).
    /// Returns `true` if matched by any observed gitignore pattern, `false`
    /// otherwise.
    #[must_use]
    pub fn is_abspath_ignored(&self, path: &Path) -> bool {
        let inner = self
            .inner
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let is_dir = path.is_dir();
        inner
            .matcher
            .matched_path_or_any_parents(path, is_dir)
            .is_ignore()
    }

    /// Observe a new gitignore file and incorporate its rules.
    ///
    /// Rebuilds the internal matcher from all previously observed files plus
    /// the new one. The `path` should point to a `.gitignore` file on disk.
    ///
    /// # Errors
    ///
    /// Returns [`ObserveError::Parse`] if the file cannot be read/parsed, or
    /// [`ObserveError::Build`] if the rebuilt matcher fails to compile.
    pub fn observe_ignorefile(&self, path: &Path) -> Result<(), ObserveError> {
        let mut inner = self
            .inner
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);

        // Rebuild from scratch: GitignoreBuilder produces an immutable Gitignore,
        // so we replay all previously observed files plus the new one.
        let mut builder = GitignoreBuilder::new(&self.root);

        // Replay existing files.
        for existing in &inner.observed_files {
            if let Some(err) = builder.add(existing) {
                tracing::warn!(?existing, %err, "re-adding previously observed ignore file produced a warning");
            }
        }

        // Add the new file.
        if let Some(err) = builder.add(path) {
            return Err(ObserveError::Parse {
                path: path.to_path_buf(),
                source: err,
            });
        }

        let matcher = builder
            .build()
            .map_err(|e| ObserveError::Build { source: e })?;

        inner.matcher = matcher;
        inner.observed_files.push(path.to_path_buf());

        Ok(())
    }
}
