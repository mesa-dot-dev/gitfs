//! Commit models.

use serde::{Deserialize, Serialize};

/// A commit author or committer.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Author {
    /// Author name.
    pub name: String,
    /// Author email.
    pub email: String,
    /// Optional date string.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub date: Option<String>,
}

/// The action to perform on a file in a commit.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum CommitFileAction {
    /// Create or update the file.
    Upsert,
    /// Delete the file.
    Delete,
}

/// Encoding used for file content in a commit.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum CommitEncoding {
    /// Base64-encoded content.
    Base64,
    /// UTF-8 text content.
    Utf8,
}

/// A file change within a commit.
#[derive(Debug, Clone, Serialize)]
pub struct CommitFile {
    /// The action to perform.
    pub action: CommitFileAction,
    /// File path.
    pub path: String,
    /// Content encoding.
    pub encoding: CommitEncoding,
    /// File content (for upsert).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub content: Option<String>,
}

/// Request body for creating a commit.
#[derive(Debug, Clone, Serialize)]
pub struct CreateCommitRequest {
    /// Target branch name.
    pub branch: String,
    /// Commit message.
    pub message: String,
    /// Commit author.
    pub author: Author,
    /// Files to include in the commit.
    pub files: Vec<CommitFile>,
    /// Optional base SHA for optimistic concurrency.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub base_sha: Option<String>,
}

/// Summary of a commit in a list response.
#[derive(Debug, Clone, Deserialize)]
pub struct CommitSummary {
    /// Commit SHA.
    pub sha: String,
    /// Branch the commit was made on.
    pub branch: String,
    /// Commit message.
    pub message: String,
}

/// Full commit details.
#[derive(Debug, Clone, Deserialize)]
pub struct Commit {
    /// Commit SHA.
    pub sha: String,
    /// Commit message.
    pub message: String,
    /// Commit author.
    pub author: Author,
    /// Commit committer.
    pub committer: Author,
}

/// Paginated list of commits.
#[derive(Debug, Clone, Deserialize)]
pub struct ListCommitsResponse {
    /// The commits in this page.
    pub commits: Vec<CommitSummary>,
    /// Cursor for the next page, if more results exist.
    pub next_cursor: Option<String>,
    /// Whether more results are available.
    pub has_more: bool,
}
