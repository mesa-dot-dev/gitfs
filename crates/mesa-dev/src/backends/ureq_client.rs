//! Ureq-based HTTP client backend (blocking).
//!
//! Since ureq is a synchronous HTTP client, the async [`HttpClient::send`]
//! implementation will block the calling thread during the request.
//! This is acceptable for use cases that don't require a fully async runtime.

use std::io::Read as _;
use std::time::Duration;

use bytes::Bytes;

use crate::error::HttpClientError;
use crate::http_client::{HttpClient, HttpRequest, HttpResponse};

/// An [`HttpClient`] implementation backed by [`ureq`] (blocking).
#[derive(Debug)]
pub struct UreqClient {
    agent: ureq::Agent,
}

impl UreqClient {
    /// Create a new `UreqClient` with the given timeout.
    pub fn new(timeout: Duration) -> Self {
        let agent = ureq::Agent::config_builder()
            .timeout_global(Some(timeout))
            // We handle HTTP status codes ourselves for proper error parsing.
            .http_status_as_error(false)
            .build()
            .new_agent();
        Self { agent }
    }
}

impl HttpClient for UreqClient {
    async fn send(&self, request: HttpRequest) -> Result<HttpResponse, HttpClientError> {
        // Build an http::Request for ureq's `agent.run()`.
        let mut builder = http::Request::builder()
            .method(request.method)
            .uri(&request.url);

        for (name, value) in &request.headers {
            builder = builder.header(name, value);
        }

        let result = if let Some(body) = request.body {
            let req = builder
                .body(body.to_vec())
                .map_err(|e| HttpClientError::Other(Box::new(e)))?;
            self.agent.run(req)
        } else {
            let req = builder
                .body(())
                .map_err(|e| HttpClientError::Other(Box::new(e)))?;
            self.agent.run(req)
        };

        match result {
            Ok(resp) => convert_response(resp),
            Err(ureq::Error::Timeout(_)) => Err(HttpClientError::Timeout),
            Err(ureq::Error::HostNotFound) => {
                Err(HttpClientError::Connection("host not found".to_owned()))
            }
            Err(ureq::Error::Io(e)) => {
                Err(HttpClientError::Connection(e.to_string()))
            }
            Err(e) => Err(HttpClientError::Other(Box::new(e))),
        }
    }
}

/// Convert a ureq `http::Response<Body>` into our [`HttpResponse`].
fn convert_response(
    response: http::Response<ureq::Body>,
) -> Result<HttpResponse, HttpClientError> {
    let (parts, body) = response.into_parts();

    let mut body_bytes = Vec::new();
    body.into_reader()
        .read_to_end(&mut body_bytes)
        .map_err(|e| HttpClientError::Connection(e.to_string()))?;

    Ok(HttpResponse {
        status: parts.status,
        headers: parts.headers,
        body: Bytes::from(body_bytes),
    })
}
