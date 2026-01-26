//! API model types.

mod admin;
mod branch;
mod commit;
mod common;
mod content;
mod diff;
pub mod pagination;
mod repo;

pub use admin::{ApiKey, ApiKeyCreated, ApiKeyScope, CreateApiKeyRequest, ListApiKeysResponse};
pub use branch::{Branch, CreateBranchRequest, ListBranchesResponse};
pub use commit::{
    Author, Commit, CommitFile, CommitFileAction, CommitSummary, CreateCommitRequest,
    ListCommitsResponse,
};
pub use common::SuccessResponse;
pub use content::{Content, DirEntry};
pub use diff::{Diff, DiffFile, DiffStats};
pub use pagination::{Paginated, PaginationParams};
pub use repo::{CreateRepoRequest, ListReposResponse, RenameRepoRequest, Repo};

// ── Paginated trait implementations ──

impl Paginated for ListReposResponse {
    type Item = Repo;

    fn items(self) -> Vec<Self::Item> {
        self.repos
    }

    fn next_cursor(&self) -> Option<&str> {
        self.next_cursor.as_deref()
    }

    fn has_more(&self) -> bool {
        self.has_more
    }
}

impl Paginated for ListBranchesResponse {
    type Item = Branch;

    fn items(self) -> Vec<Self::Item> {
        self.branches
    }

    fn next_cursor(&self) -> Option<&str> {
        self.next_cursor.as_deref()
    }

    fn has_more(&self) -> bool {
        self.has_more
    }
}

impl Paginated for ListCommitsResponse {
    type Item = CommitSummary;

    fn items(self) -> Vec<Self::Item> {
        self.commits
    }

    fn next_cursor(&self) -> Option<&str> {
        self.next_cursor.as_deref()
    }

    fn has_more(&self) -> bool {
        self.has_more
    }
}
