//! Repository models.

use serde::{Deserialize, Serialize};

/// A repository.
#[derive(Debug, Clone, Deserialize)]
pub struct Repo {
    /// Unique repository identifier.
    pub id: String,
    /// Organization that owns this repository.
    pub org: String,
    /// Repository name.
    pub name: String,
    /// Name of the default branch.
    pub default_branch: String,
    /// Size in bytes.
    pub size_bytes: u64,
    /// Timestamp of the last push, if any.
    pub last_push_at: Option<String>,
    /// Creation timestamp.
    pub created_at: String,
}

/// Request body for creating a repository.
#[derive(Debug, Clone, Serialize)]
pub struct CreateRepoRequest {
    /// Repository name.
    pub name: String,
    /// Optional default branch name (defaults to `"main"` on the server).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub default_branch: Option<String>,
}

/// Request body for renaming a repository.
#[derive(Debug, Clone, Serialize)]
pub struct RenameRepoRequest {
    /// New repository name.
    pub name: String,
}

/// Paginated list of repositories.
#[derive(Debug, Clone, Deserialize)]
pub struct ListReposResponse {
    /// The repositories in this page.
    pub repos: Vec<Repo>,
    /// Cursor for the next page, if more results exist.
    pub next_cursor: Option<String>,
    /// Whether more results are available.
    pub has_more: bool,
}
