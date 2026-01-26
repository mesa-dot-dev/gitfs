//! HTTP client abstraction for pluggable backends.

use bytes::Bytes;
use http::{HeaderMap, Method, StatusCode};
use std::future::Future;

use crate::error::HttpClientError;

/// An HTTP request to be sent by an [`HttpClient`] implementation.
#[derive(Debug, Clone)]
pub struct HttpRequest {
    /// The HTTP method.
    pub method: Method,
    /// The fully-qualified URL.
    pub url: String,
    /// Request headers.
    pub headers: HeaderMap,
    /// Optional request body.
    pub body: Option<Bytes>,
}

/// An HTTP response returned by an [`HttpClient`] implementation.
#[derive(Debug)]
pub struct HttpResponse {
    /// The HTTP status code.
    pub status: StatusCode,
    /// Response headers.
    pub headers: HeaderMap,
    /// Response body bytes.
    pub body: Bytes,
}

/// Trait for pluggable HTTP client backends.
///
/// Uses Rust edition 2024's native `impl Future` in traits (RPITIT) —
/// no `async-trait` macro required.
pub trait HttpClient: Send + Sync {
    /// Send an HTTP request and return the response.
    fn send(
        &self,
        request: HttpRequest,
    ) -> impl Future<Output = Result<HttpResponse, HttpClientError>> + Send;
}
