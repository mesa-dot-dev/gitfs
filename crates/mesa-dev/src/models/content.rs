//! Content models.

use serde::Deserialize;

/// Repository content — either a file or a directory.
#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum Content {
    /// A file entry.
    File {
        /// File name.
        name: String,
        /// File path.
        path: String,
        /// Object SHA.
        sha: String,
        /// File size in bytes.
        size: u64,
        /// Content encoding (e.g. `"base64"`).
        encoding: String,
        /// Encoded file content.
        content: String,
    },
    /// A directory entry.
    Dir {
        /// Directory name.
        name: String,
        /// Directory path.
        path: String,
        /// Object SHA.
        sha: String,
        /// Directory entries.
        entries: Vec<DirEntry>,
        /// Cursor for the next page of entries.
        next_cursor: Option<String>,
        /// Whether more entries are available.
        has_more: bool,
    },
}

#[derive(Debug, Clone, Deserialize)]
pub enum DirEntryType {
    #[serde(rename = "file")]
    File,
    #[serde(rename = "dir")]
    Dir,
}

/// An entry within a directory listing.
#[derive(Debug, Clone, Deserialize)]
pub struct DirEntry {
    /// Type of entry (`"file"` or `"dir"`).
    #[serde(rename = "type")]
    pub entry_type: DirEntryType,
    /// Entry name.
    pub name: String,
    /// Entry path.
    pub path: String,
    /// Object SHA.
    pub sha: String,
    /// Size in bytes (files only).
    pub size: Option<u64>,
}
