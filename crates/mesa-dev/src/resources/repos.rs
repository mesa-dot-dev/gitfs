//! Repository resource.

use http::Method;

use crate::client::MesaClient;
use crate::error::MesaError;
use crate::http_client::HttpClient;
use crate::models::{
    CreateRepoRequest, ListReposResponse, PaginationParams, RenameRepoRequest, Repo,
    SuccessResponse,
};
use crate::pagination::PageStream;

/// Operations on repositories within an organization.
pub struct ReposResource<'c, C: HttpClient> {
    client: &'c MesaClient<C>,
    org: String,
}

impl<'c, C: HttpClient> ReposResource<'c, C> {
    pub(crate) fn new(client: &'c MesaClient<C>, org: String) -> Self {
        Self { client, org }
    }

    /// Create a new repository.
    pub async fn create(&self, req: &CreateRepoRequest) -> Result<Repo, MesaError> {
        let path = format!("/{}/repos", self.org);
        self.client.request(Method::POST, &path, &[], Some(req)).await
    }

    /// List repositories with pagination parameters.
    pub async fn list(
        &self,
        params: &PaginationParams,
    ) -> Result<ListReposResponse, MesaError> {
        let path = format!("/{}/repos", self.org);
        let mut query = Vec::new();
        if let Some(ref cursor) = params.cursor {
            query.push(("cursor", cursor.as_str()));
        }
        if let Some(limit) = params.limit {
            let limit_str = limit.to_string();
            // Need to own the string for the borrow to work.
            return self
                .client
                .request::<ListReposResponse>(
                    Method::GET,
                    &path,
                    &[&query[..], &[("limit", &limit_str)]].concat(),
                    None::<&()>,
                )
                .await;
        }
        self.client
            .request(Method::GET, &path, &query, None::<&()>)
            .await
    }

    /// Return a [`PageStream`] that iterates over all repositories.
    pub fn list_all(&self) -> PageStream<C, ListReposResponse> {
        let path = format!("/{}/repos", self.org);
        PageStream::new(self.client.inner.clone(), path, Vec::new())
    }

    /// Get a single repository by name.
    pub async fn get(&self, repo: &str) -> Result<Repo, MesaError> {
        let path = format!("/{}/{repo}", self.org);
        self.client
            .request(Method::GET, &path, &[], None::<&()>)
            .await
    }

    /// Rename a repository.
    pub async fn rename(
        &self,
        repo: &str,
        req: &RenameRepoRequest,
    ) -> Result<Repo, MesaError> {
        let path = format!("/{}/{repo}", self.org);
        self.client
            .request(Method::PATCH, &path, &[], Some(req))
            .await
    }

    /// Delete a repository.
    pub async fn delete(&self, repo: &str) -> Result<SuccessResponse, MesaError> {
        let path = format!("/{}/{repo}", self.org);
        self.client
            .request(Method::DELETE, &path, &[], None::<&()>)
            .await
    }
}
