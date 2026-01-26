//! Mesa API client with retry logic.

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use http::{HeaderMap, HeaderValue, Method, StatusCode};
use serde::de::DeserializeOwned;
use serde::Serialize;

use crate::error::{ApiErrorCode, ApiErrorResponse, HttpClientError, MesaError};
use crate::http_client::{HttpClient, HttpRequest, HttpResponse};

/// Default base URL for the Mesa API.
const DEFAULT_BASE_URL: &str = "https://depot.mesa.dev/api/v1";

/// Default request timeout.
const DEFAULT_TIMEOUT: Duration = Duration::from_secs(30);

/// Default maximum number of retry attempts.
const DEFAULT_MAX_RETRIES: u32 = 3;

/// Default initial backoff duration for retries.
const DEFAULT_INITIAL_BACKOFF: Duration = Duration::from_millis(500);

/// Default maximum backoff duration.
const DEFAULT_MAX_BACKOFF: Duration = Duration::from_secs(30);

/// Client configuration.
#[derive(Debug, Clone)]
pub struct ClientConfig {
    /// Base URL for the Mesa API.
    pub base_url: String,
    /// API key for authentication.
    pub api_key: String,
    /// Request timeout (passed to the HTTP backend).
    pub timeout: Duration,
    /// Maximum number of retry attempts.
    pub max_retries: u32,
    /// Initial backoff duration for retries.
    pub initial_backoff: Duration,
    /// Maximum backoff duration.
    pub max_backoff: Duration,
    /// Default headers included in every request.
    pub default_headers: HeaderMap,
}

/// Builder for constructing a [`MesaClient`].
#[derive(Debug)]
pub struct ClientBuilder {
    config: ClientConfig,
}

impl ClientBuilder {
    /// Create a new builder with the given API key.
    pub fn new(api_key: impl Into<String>) -> Self {
        Self {
            config: ClientConfig {
                base_url: DEFAULT_BASE_URL.to_owned(),
                api_key: api_key.into(),
                timeout: DEFAULT_TIMEOUT,
                max_retries: DEFAULT_MAX_RETRIES,
                initial_backoff: DEFAULT_INITIAL_BACKOFF,
                max_backoff: DEFAULT_MAX_BACKOFF,
                default_headers: HeaderMap::new(),
            },
        }
    }

    /// Set the base URL.
    #[must_use]
    pub fn base_url(mut self, url: impl Into<String>) -> Self {
        self.config.base_url = url.into();
        self
    }

    /// Set the request timeout.
    #[must_use]
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.config.timeout = timeout;
        self
    }

    /// Set the maximum number of retries.
    #[must_use]
    pub fn max_retries(mut self, n: u32) -> Self {
        self.config.max_retries = n;
        self
    }

    /// Set the initial backoff duration.
    #[must_use]
    pub fn initial_backoff(mut self, d: Duration) -> Self {
        self.config.initial_backoff = d;
        self
    }

    /// Set the maximum backoff duration.
    #[must_use]
    pub fn max_backoff(mut self, d: Duration) -> Self {
        self.config.max_backoff = d;
        self
    }

    /// Add a default header to every request.
    #[must_use]
    pub fn default_header(mut self, name: http::HeaderName, value: HeaderValue) -> Self {
        self.config.default_headers.insert(name, value);
        self
    }

    /// Build a [`MesaClient`] with the default reqwest backend.
    #[cfg(feature = "reqwest-client")]
    #[must_use]
    pub fn build(self) -> MesaClient<crate::backends::ReqwestClient> {
        let http = crate::backends::ReqwestClient::new(self.config.timeout);
        self.build_with(http)
    }

    /// Build a [`MesaClient`] with a custom HTTP client backend.
    #[must_use]
    pub fn build_with<C: HttpClient>(self, http_client: C) -> MesaClient<C> {
        MesaClient {
            inner: Arc::new(ClientInner {
                config: self.config,
                http: http_client,
            }),
        }
    }
}

/// The Mesa API client, generic over an HTTP backend.
///
/// Clone is cheap — the inner state is shared via `Arc`.
#[derive(Debug, Clone)]
pub struct MesaClient<C: HttpClient> {
    pub(crate) inner: Arc<ClientInner<C>>,
}

/// Internal shared state for the client.
#[derive(Debug)]
#[expect(unreachable_pub)]
pub struct ClientInner<C: HttpClient> {
    pub(crate) config: ClientConfig,
    pub(crate) http: C,
}

impl<C: HttpClient> ClientInner<C> {
    /// Send an API request, deserializing the response as JSON.
    pub(crate) async fn request<T: DeserializeOwned>(
        &self,
        method: Method,
        path: &str,
        query: &[(&str, &str)],
        body: Option<Bytes>,
    ) -> Result<T, MesaError> {
        let url = build_url(&self.config.base_url, path, query);
        let response = self.send_with_retry(method, &url, body).await?;
        serde_json::from_slice(&response.body).map_err(MesaError::from)
    }

    /// Send a request with retry logic (exponential backoff + jitter).
    async fn send_with_retry(
        &self,
        method: Method,
        url: &str,
        body: Option<Bytes>,
    ) -> Result<HttpResponse, MesaError> {
        let max_attempts = self.config.max_retries + 1;
        let mut last_error: Option<MesaError> = None;

        for attempt in 0..max_attempts {
            if attempt > 0 {
                if let Some(ref err) = last_error
                    && !err.is_retryable()
                {
                    break;
                }

                let backoff = compute_backoff(
                    attempt,
                    self.config.initial_backoff,
                    self.config.max_backoff,
                );
                std::thread::sleep(backoff);
            }

            let request = self.build_request(method.clone(), url, body.clone());
            match self.http.send(request).await {
                Ok(response) if response.status.is_success() => return Ok(response),
                Ok(response) => {
                    let err = parse_api_error(response.status, &response.body);
                    last_error = Some(err);
                }
                Err(http_err) => {
                    last_error = Some(MesaError::HttpClient(http_err));
                }
            }
        }

        match last_error {
            Some(err) if max_attempts > 1 && err.is_retryable() => {
                Err(MesaError::RetriesExhausted {
                    attempts: max_attempts,
                    last_error: Box::new(err),
                })
            }
            Some(err) => Err(err),
            None => Err(MesaError::HttpClient(HttpClientError::Connection(
                "no attempts made".to_owned(),
            ))),
        }
    }

    /// Build an [`HttpRequest`] with the configured headers.
    fn build_request(&self, method: Method, url: &str, body: Option<Bytes>) -> HttpRequest {
        let mut headers = self.config.default_headers.clone();

        if let Ok(auth) =
            HeaderValue::from_str(&format!("Bearer {}", self.config.api_key))
        {
            headers.insert(http::header::AUTHORIZATION, auth);
        }

        if body.is_some() {
            headers.insert(
                http::header::CONTENT_TYPE,
                HeaderValue::from_static("application/json"),
            );
        }

        headers.insert(
            http::header::ACCEPT,
            HeaderValue::from_static("application/json"),
        );

        HttpRequest {
            method,
            url: url.to_owned(),
            headers,
            body,
        }
    }
}

/// Convenience type alias when using the default reqwest backend.
#[cfg(feature = "reqwest-client")]
pub type Mesa = MesaClient<crate::backends::ReqwestClient>;

#[cfg(feature = "reqwest-client")]
impl Mesa {
    /// Create a new client with the default reqwest backend.
    pub fn new(api_key: impl Into<String>) -> Self {
        ClientBuilder::new(api_key).build()
    }
}

impl<C: HttpClient> MesaClient<C> {
    /// Create a new builder for configuring a client.
    pub fn builder(api_key: impl Into<String>) -> ClientBuilder {
        ClientBuilder::new(api_key)
    }

    /// Send an API request with a JSON body, deserializing the response.
    pub(crate) async fn request<T: DeserializeOwned>(
        &self,
        method: Method,
        path: &str,
        query: &[(&str, &str)],
        body: Option<&(impl Serialize + Sync)>,
    ) -> Result<T, MesaError> {
        let json_body = match body {
            Some(b) => Some(Bytes::from(serde_json::to_vec(b)?)),
            None => None,
        };
        self.inner.request(method, path, query, json_body).await
    }

    // ── Resource namespace accessors ──

    /// Access repository operations for the given organization.
    #[must_use]
    pub fn repos(&self, org: &str) -> crate::resources::ReposResource<'_, C> {
        crate::resources::ReposResource::new(self, org.to_owned())
    }

    /// Access branch operations for the given repository.
    #[must_use]
    pub fn branches(
        &self,
        org: &str,
        repo: &str,
    ) -> crate::resources::BranchesResource<'_, C> {
        crate::resources::BranchesResource::new(self, org.to_owned(), repo.to_owned())
    }

    /// Access commit operations for the given repository.
    #[must_use]
    pub fn commits(
        &self,
        org: &str,
        repo: &str,
    ) -> crate::resources::CommitsResource<'_, C> {
        crate::resources::CommitsResource::new(self, org.to_owned(), repo.to_owned())
    }

    /// Access content operations for the given repository.
    #[must_use]
    pub fn content(
        &self,
        org: &str,
        repo: &str,
    ) -> crate::resources::ContentResource<'_, C> {
        crate::resources::ContentResource::new(self, org.to_owned(), repo.to_owned())
    }

    /// Access diff operations for the given repository.
    #[must_use]
    pub fn diffs(
        &self,
        org: &str,
        repo: &str,
    ) -> crate::resources::DiffsResource<'_, C> {
        crate::resources::DiffsResource::new(self, org.to_owned(), repo.to_owned())
    }

    /// Access admin operations (API keys) for the given organization.
    #[must_use]
    pub fn admin(&self, org: &str) -> crate::resources::AdminResource<'_, C> {
        crate::resources::AdminResource::new(self, org.to_owned())
    }
}

/// Build a full URL from base, path, and query parameters.
fn build_url(base: &str, path: &str, query: &[(&str, &str)]) -> String {
    let mut url = format!("{base}{path}");
    if !query.is_empty() {
        url.push('?');
        for (i, (key, value)) in query.iter().enumerate() {
            if i > 0 {
                url.push('&');
            }
            url.push_str(key);
            url.push('=');
            url.push_str(&url_encode(value));
        }
    }
    url
}

/// Minimal percent-encoding for query parameter values.
fn url_encode(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for byte in s.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(byte as char);
            }
            _ => {
                out.push('%');
                // Format each byte as two uppercase hex digits.
                let high = byte >> 4;
                let low = byte & 0x0F;
                out.push(hex_digit(high));
                out.push(hex_digit(low));
            }
        }
    }
    out
}

/// Convert a nibble (0–15) to a hex character.
const fn hex_digit(nibble: u8) -> char {
    match nibble {
        0..=9 => (b'0' + nibble) as char,
        _ => (b'A' + nibble - 10) as char,
    }
}

/// Compute exponential backoff with jitter for the given attempt number.
#[expect(clippy::cast_possible_truncation)] // millis fits in u64 for any reasonable backoff
fn compute_backoff(attempt: u32, initial: Duration, max: Duration) -> Duration {
    let base = initial.saturating_mul(1 << attempt.min(16));
    let capped = base.min(max);
    // Simple jitter: use between 50% and 100% of capped value.
    let millis = capped.as_millis() as u64;
    let jitter_millis = millis / 2 + simple_random_u64() % (millis / 2 + 1);
    Duration::from_millis(jitter_millis)
}

/// Produce a simple pseudo-random u64 using the current time.
/// Not cryptographically secure — only used for retry jitter.
#[expect(clippy::cast_possible_truncation)] // intentional wrapping for jitter
fn simple_random_u64() -> u64 {
    use std::time::SystemTime;
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map_or(0, |d| d.as_nanos() as u64)
}

/// Parse an API error response body into a [`MesaError`].
#[expect(unreachable_pub)]
pub fn parse_api_error(status: StatusCode, body: &[u8]) -> MesaError {
    match serde_json::from_slice::<ApiErrorResponse>(body) {
        Ok(resp) => MesaError::Api {
            status,
            code: ApiErrorCode::from_code(&resp.error.code),
            message: resp.error.message,
            details: resp.error.details,
        },
        Err(_) => MesaError::Api {
            status,
            code: ApiErrorCode::Unknown(status.to_string()),
            message: String::from_utf8_lossy(body).into_owned(),
            details: serde_json::Value::Null,
        },
    }
}
