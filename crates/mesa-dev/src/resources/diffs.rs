//! Diff resource.

use http::Method;

use crate::client::MesaClient;
use crate::error::MesaError;
use crate::http_client::HttpClient;
use crate::models::Diff;

/// Operations on diffs within a repository.
pub struct DiffsResource<'c, C: HttpClient> {
    client: &'c MesaClient<C>,
    org: String,
    repo: String,
}

impl<'c, C: HttpClient> DiffsResource<'c, C> {
    pub(crate) fn new(client: &'c MesaClient<C>, org: String, repo: String) -> Self {
        Self { client, org, repo }
    }

    /// Get the diff between two refs.
    pub async fn get(&self, base: &str, head: &str) -> Result<Diff, MesaError> {
        let path = format!("/{}/{}/diff", self.org, self.repo);
        self.client
            .request(
                Method::GET,
                &path,
                &[("base", base), ("head", head)],
                None::<&()>,
            )
            .await
    }
}
