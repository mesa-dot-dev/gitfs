//! Branch models.

use serde::{Deserialize, Serialize};

/// A branch in a repository.
#[derive(Debug, Clone, Deserialize)]
pub struct Branch {
    /// Branch name.
    pub name: String,
    /// SHA of the branch head commit.
    pub head_sha: String,
    /// Whether this is the default branch.
    pub is_default: bool,
}

/// Request body for creating a branch.
#[derive(Debug, Clone, Serialize)]
pub struct CreateBranchRequest {
    /// Name for the new branch.
    pub name: String,
    /// SHA or branch name to create from.
    pub from: String,
}

/// Paginated list of branches.
#[derive(Debug, Clone, Deserialize)]
pub struct ListBranchesResponse {
    /// The branches in this page.
    pub branches: Vec<Branch>,
    /// Cursor for the next page, if more results exist.
    pub next_cursor: Option<String>,
    /// Whether more results are available.
    pub has_more: bool,
}
