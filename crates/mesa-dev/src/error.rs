//! Error types for the Mesa SDK.

use http::StatusCode;
use serde::Deserialize;
use std::fmt;

/// Top-level error type for the Mesa SDK.
#[derive(Debug, thiserror::Error)]
pub enum MesaError {
    /// An error returned by the Mesa API.
    #[error("API error {status}: [{code}] {message}")]
    Api {
        /// HTTP status code.
        status: StatusCode,
        /// Structured error code.
        code: ApiErrorCode,
        /// Human-readable error message.
        message: String,
        /// Additional error details.
        details: serde_json::Value,
    },
    /// An error from the underlying HTTP client.
    #[error("HTTP client error: {0}")]
    HttpClient(#[from] HttpClientError),
    /// A serialization or deserialization error.
    #[error("Serialization error: {0}")]
    Serialization(#[from] serde_json::Error),
    /// All retry attempts have been exhausted.
    #[error("Request failed after {attempts} attempts: {last_error}")]
    RetriesExhausted {
        /// Number of attempts made.
        attempts: u32,
        /// The last error encountered.
        last_error: Box<Self>,
    },
}

impl MesaError {
    /// Returns `true` if this error is retryable (429 or 5xx).
    #[must_use]
    pub fn is_retryable(&self) -> bool {
        match self {
            Self::Api { status, .. } => status.as_u16() == 429 || status.is_server_error(),
            Self::HttpClient(HttpClientError::Timeout | HttpClientError::Connection(_)) => true,
            Self::HttpClient(HttpClientError::Other(_))
            | Self::Serialization(_)
            | Self::RetriesExhausted { .. } => false,
        }
    }

    /// Returns the HTTP status code, if this is an API error.
    #[must_use]
    pub fn status(&self) -> Option<StatusCode> {
        match self {
            Self::Api { status, .. } => Some(*status),
            Self::HttpClient(_) | Self::Serialization(_) | Self::RetriesExhausted { .. } => None,
        }
    }
}

/// Structured error code returned by the Mesa API.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ApiErrorCode {
    /// 400 Bad Request.
    BadRequest,
    /// 401 Unauthorized.
    Unauthorized,
    /// 403 Forbidden.
    Forbidden,
    /// 404 Not Found.
    NotFound,
    /// 406 Not Acceptable.
    NotAcceptable,
    /// 409 Conflict.
    Conflict,
    /// 500 Internal Server Error.
    InternalServerError,
    /// An unrecognized error code.
    Unknown(String),
}

impl ApiErrorCode {
    /// Parse an error code string from the API.
    #[must_use]
    pub fn from_code(s: &str) -> Self {
        match s {
            "bad_request" => Self::BadRequest,
            "unauthorized" => Self::Unauthorized,
            "forbidden" => Self::Forbidden,
            "not_found" => Self::NotFound,
            "not_acceptable" => Self::NotAcceptable,
            "conflict" => Self::Conflict,
            "internal_server_error" => Self::InternalServerError,
            other => Self::Unknown(other.to_owned()),
        }
    }
}

impl fmt::Display for ApiErrorCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::BadRequest => f.write_str("bad_request"),
            Self::Unauthorized => f.write_str("unauthorized"),
            Self::Forbidden => f.write_str("forbidden"),
            Self::NotFound => f.write_str("not_found"),
            Self::NotAcceptable => f.write_str("not_acceptable"),
            Self::Conflict => f.write_str("conflict"),
            Self::InternalServerError => f.write_str("internal_server_error"),
            Self::Unknown(code) => f.write_str(code),
        }
    }
}

/// Errors originating from the HTTP transport layer.
#[derive(Debug, thiserror::Error)]
pub enum HttpClientError {
    /// The request timed out.
    #[error("Request timed out")]
    Timeout,
    /// A connection error occurred.
    #[error("Connection error: {0}")]
    Connection(String),
    /// Any other transport error.
    #[error("{0}")]
    Other(#[from] Box<dyn std::error::Error + Send + Sync>),
}

// ── Internal deserialization structs for API error responses ──

/// The top-level JSON body returned on API errors.
#[derive(Debug, Deserialize)]
pub(crate) struct ApiErrorResponse {
    pub error: ApiErrorBody,
}

/// The nested error object inside an API error response.
#[derive(Debug, Deserialize)]
pub(crate) struct ApiErrorBody {
    pub code: String,
    #[serde(default)]
    pub message: String,
    #[serde(default = "default_details")]
    pub details: serde_json::Value,
}

fn default_details() -> serde_json::Value {
    serde_json::Value::Null
}
