//! Commit resource.

use http::Method;

use crate::client::MesaClient;
use crate::error::MesaError;
use crate::http_client::HttpClient;
use crate::models::{
    Commit, CommitSummary, CreateCommitRequest, ListCommitsResponse, PaginationParams,
};
use crate::pagination::PageStream;

/// Query parameters for listing commits.
#[derive(Debug, Clone, Default)]
pub struct ListCommitsParams {
    /// Pagination parameters.
    pub pagination: PaginationParams,
    /// Optional ref (branch name or SHA) to list commits from.
    pub ref_: Option<String>,
}

/// Operations on commits within a repository.
pub struct CommitsResource<'c, C: HttpClient> {
    client: &'c MesaClient<C>,
    org: String,
    repo: String,
}

impl<'c, C: HttpClient> CommitsResource<'c, C> {
    pub(crate) fn new(client: &'c MesaClient<C>, org: String, repo: String) -> Self {
        Self { client, org, repo }
    }

    /// Create a new commit.
    pub async fn create(
        &self,
        req: &CreateCommitRequest,
    ) -> Result<CommitSummary, MesaError> {
        let path = format!("/{}/{}/commits", self.org, self.repo);
        self.client.request(Method::POST, &path, &[], Some(req)).await
    }

    /// List commits with pagination and optional ref filter.
    pub async fn list(
        &self,
        params: &ListCommitsParams,
    ) -> Result<ListCommitsResponse, MesaError> {
        let path = format!("/{}/{}/commits", self.org, self.repo);
        let mut query: Vec<(&str, String)> = Vec::new();
        if let Some(ref cursor) = params.pagination.cursor {
            query.push(("cursor", cursor.clone()));
        }
        if let Some(limit) = params.pagination.limit {
            query.push(("limit", limit.to_string()));
        }
        if let Some(ref r) = params.ref_ {
            query.push(("ref", r.clone()));
        }
        let query_refs: Vec<(&str, &str)> =
            query.iter().map(|(k, v)| (*k, v.as_str())).collect();
        self.client
            .request(Method::GET, &path, &query_refs, None::<&()>)
            .await
    }

    /// Return a [`PageStream`] that iterates over all commits.
    ///
    /// Optionally filter by ref (branch name or SHA).
    pub fn list_all(&self, ref_: Option<&str>) -> PageStream<C, ListCommitsResponse> {
        let path = format!("/{}/{}/commits", self.org, self.repo);
        let extra = match ref_ {
            Some(r) => vec![("ref".to_owned(), r.to_owned())],
            None => Vec::new(),
        };
        PageStream::new(self.client.inner.clone(), path, extra)
    }

    /// Get a single commit by SHA.
    pub async fn get(&self, sha: &str) -> Result<Commit, MesaError> {
        let path = format!("/{}/{}/commits/{sha}", self.org, self.repo);
        self.client
            .request(Method::GET, &path, &[], None::<&()>)
            .await
    }
}
