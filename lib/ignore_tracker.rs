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
        let is_dir = path.is_dir();
        let inner = self
            .inner
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        inner
            .matcher
            .matched_path_or_any_parents(path, is_dir)
            .is_ignore()
    }

    /// Observe a gitignore file and incorporate its rules.
    ///
    /// If the file has not been observed before, it is added to the tracked
    /// set.  If it has already been observed, its content is reloaded from
    /// disk (picking up any changes).  The internal matcher is rebuilt in
    /// either case.
    ///
    /// # Errors
    ///
    /// Returns [`ObserveError::Parse`] if a newly observed file cannot be
    /// read, or [`ObserveError::Build`] if the rebuilt matcher fails to compile.
    pub fn observe_ignorefile(&self, path: &Path) -> Result<(), ObserveError> {
        let mut inner = self
            .inner
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);

        let is_new = !inner.observed_files.iter().any(|p| p == path);

        if is_new {
            // Validate the file is readable before committing to track it.
            let mut probe = GitignoreBuilder::new(&self.root);
            if let Some(err) = probe.add(path) {
                return Err(ObserveError::Parse {
                    path: path.to_path_buf(),
                    source: err,
                });
            }
            inner.observed_files.push(path.to_path_buf());
        }

        Self::rebuild_matcher(&self.root, &mut inner)
    }

    /// Forget a previously observed gitignore file and remove its rules.
    ///
    /// If the file was not previously observed, this is a no-op.  Otherwise,
    /// the file is removed from the tracked set and the matcher is rebuilt
    /// from the remaining files.
    pub fn forget_ignorefile(&self, path: &Path) {
        let mut inner = self
            .inner
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);

        let before = inner.observed_files.len();
        inner.observed_files.retain(|p| p != path);

        if inner.observed_files.len() == before {
            return;
        }

        if let Err(e) = Self::rebuild_matcher(&self.root, &mut inner) {
            tracing::warn!(%e, "rebuild after forget_ignorefile failed");
        }
    }

    /// Rebuild the matcher from all files currently in `observed_files`.
    ///
    /// Must be called while holding the write lock (caller passes `&mut inner`).
    fn rebuild_matcher(root: &Path, inner: &mut IgnoreTrackerInner) -> Result<(), ObserveError> {
        let mut builder = GitignoreBuilder::new(root);

        for existing in &inner.observed_files {
            if let Some(err) = builder.add(existing) {
                tracing::warn!(?existing, %err, "re-adding observed ignore file produced a warning");
            }
        }

        let matcher = builder
            .build()
            .map_err(|e| ObserveError::Build { source: e })?;

        inner.matcher = matcher;
        Ok(())
    }
}
