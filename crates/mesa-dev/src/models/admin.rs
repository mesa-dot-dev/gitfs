//! Admin / API key models.

use serde::{Deserialize, Serialize};

/// A permission scope for an API key.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ApiKeyScope {
    /// Read git data.
    #[serde(rename = "git:read")]
    GitRead,
    /// Write git data.
    #[serde(rename = "git:write")]
    GitWrite,
    /// Read repository metadata.
    #[serde(rename = "repo:read")]
    RepoRead,
    /// Create repositories.
    #[serde(rename = "repo:create")]
    RepoCreate,
    /// Delete repositories.
    #[serde(rename = "repo:delete")]
    RepoDelete,
    /// Full admin access.
    #[serde(rename = "admin")]
    Admin,
}

/// Request body for creating an API key.
#[derive(Debug, Clone, Serialize)]
pub struct CreateApiKeyRequest {
    /// Human-readable name for the key.
    pub name: String,
    /// Permission scopes.
    pub scopes: Vec<ApiKeyScope>,
}

/// Response returned when an API key is created (includes the secret).
#[derive(Debug, Clone, Deserialize)]
pub struct ApiKeyCreated {
    /// Key identifier.
    pub id: String,
    /// The secret key value (only returned at creation time).
    pub key: String,
    /// Human-readable name.
    pub name: Option<String>,
    /// Permission scopes.
    pub scopes: Vec<ApiKeyScope>,
    /// Creation timestamp.
    pub created_at: String,
}

/// An existing API key (without the secret).
#[derive(Debug, Clone, Deserialize)]
pub struct ApiKey {
    /// Key identifier.
    pub id: String,
    /// Human-readable name.
    pub name: Option<String>,
    /// Permission scopes.
    pub scopes: Vec<ApiKeyScope>,
    /// Last usage timestamp.
    pub last_used_at: Option<String>,
    /// Expiration timestamp.
    pub expires_at: Option<String>,
    /// Revocation timestamp.
    pub revoked_at: Option<String>,
    /// Creation timestamp.
    pub created_at: String,
}

/// Response listing API keys.
#[derive(Debug, Clone, Deserialize)]
pub struct ListApiKeysResponse {
    /// The API keys.
    pub api_keys: Vec<ApiKey>,
}
