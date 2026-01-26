//! Diff models.

use serde::Deserialize;

/// Summary statistics for a diff.
#[derive(Debug, Clone, Deserialize)]
pub struct DiffStats {
    /// Number of files changed.
    pub files: u64,
    /// Number of lines added.
    pub additions: u64,
    /// Number of lines deleted.
    pub deletions: u64,
    /// Total number of changes.
    pub changes: u64,
}

/// A single file within a diff.
#[derive(Debug, Clone, Deserialize)]
pub struct DiffFile {
    /// File path.
    pub path: String,
    /// Change status (e.g. `"added"`, `"modified"`, `"deleted"`).
    pub status: String,
    /// Previous path if the file was renamed.
    pub old_path: Option<String>,
    /// Size in bytes.
    pub bytes: Option<u64>,
    /// Whether the file ends without a newline.
    pub is_eof: Option<bool>,
    /// Raw diff output for this file.
    pub raw: Option<String>,
}

/// A diff between two refs.
#[derive(Debug, Clone, Deserialize)]
pub struct Diff {
    /// Base ref.
    pub base: String,
    /// Head ref.
    pub head: String,
    /// Diff statistics.
    pub stats: DiffStats,
    /// Files changed.
    pub files: Vec<DiffFile>,
    /// Number of files filtered out (if any limit was applied).
    pub filtered_files: u64,
}
