//! Admin / API key resource.

use http::Method;

use crate::client::MesaClient;
use crate::error::MesaError;
use crate::http_client::HttpClient;
use crate::models::{
    ApiKey, ApiKeyCreated, CreateApiKeyRequest, ListApiKeysResponse, SuccessResponse,
};

/// Operations on API keys for an organization.
pub struct AdminResource<'c, C: HttpClient> {
    client: &'c MesaClient<C>,
    org: String,
}

impl<'c, C: HttpClient> AdminResource<'c, C> {
    pub(crate) fn new(client: &'c MesaClient<C>, org: String) -> Self {
        Self { client, org }
    }

    /// Create a new API key.
    pub async fn create_api_key(
        &self,
        req: &CreateApiKeyRequest,
    ) -> Result<ApiKeyCreated, MesaError> {
        let path = format!("/{}/api-keys", self.org);
        self.client
            .request(Method::POST, &path, &[], Some(req))
            .await
    }

    /// List all API keys.
    pub async fn list_api_keys(&self) -> Result<Vec<ApiKey>, MesaError> {
        let path = format!("/{}/api-keys", self.org);
        let resp: ListApiKeysResponse = self
            .client
            .request(Method::GET, &path, &[], None::<&()>)
            .await?;
        Ok(resp.api_keys)
    }

    /// Revoke an API key by ID.
    pub async fn revoke_api_key(&self, id: &str) -> Result<SuccessResponse, MesaError> {
        let path = format!("/{}/api-keys/{id}", self.org);
        self.client
            .request(Method::DELETE, &path, &[], None::<&()>)
            .await
    }
}
