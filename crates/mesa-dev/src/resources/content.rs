//! Content resource.

use http::Method;

use crate::client::MesaClient;
use crate::error::MesaError;
use crate::http_client::HttpClient;
use crate::models::Content;

/// Operations on repository content.
pub struct ContentResource<'c, C: HttpClient> {
    client: &'c MesaClient<C>,
    org: String,
    repo: String,
}

impl<'c, C: HttpClient> ContentResource<'c, C> {
    pub(crate) fn new(client: &'c MesaClient<C>, org: String, repo: String) -> Self {
        Self { client, org, repo }
    }

    /// Get file or directory content.
    ///
    /// - `path`: file or directory path within the repo (optional, defaults to root).
    /// - `ref_`: branch name or commit SHA (optional, defaults to the default branch).
    pub async fn get(&self, path: Option<&str>, ref_: Option<&str>) -> Result<Content, MesaError> {
        let url_path = format!("/{}/{}/content", self.org, self.repo);
        let mut query = Vec::new();
        if let Some(p) = path {
            query.push(("path", p));
        }
        if let Some(r) = ref_ {
            query.push(("ref", r));
        }
        self.client
            .request(Method::GET, &url_path, &query, None::<&()>)
            .await
    }
}
