//! Rust SDK for the [mesa.dev](https://mesa.dev) API.
//!
//! # Quick Start
//!
//! With the default `reqwest-client` feature enabled:
//!
//! ```rust,no_run
//! use mesa_dev::{Mesa, models::CreateRepoRequest};
//!
//! # async fn run() -> Result<(), mesa_dev::MesaError> {
//! let client = Mesa::new("my-api-key");
//!
//! // Create a repository
//! let repo = client
//!     .repos("my-org")
//!     .create(&CreateRepoRequest {
//!         name: "my-repo".to_owned(),
//!         default_branch: None,
//!     })
//!     .await?;
//!
//! // List all branches
//! let branches = client
//!     .branches("my-org", "my-repo")
//!     .list_all()
//!     .collect()
//!     .await?;
//! # Ok(())
//! # }
//! ```
//!
//! # Custom HTTP Client
//!
//! Implement the [`HttpClient`] trait to bring your own backend:
//!
//! ```rust
//! use mesa_dev::{ClientBuilder, HttpClient, HttpRequest, HttpResponse, HttpClientError};
//!
//! struct MyClient;
//!
//! impl HttpClient for MyClient {
//!     async fn send(
//!         &self,
//!         _request: HttpRequest,
//!     ) -> Result<HttpResponse, HttpClientError> {
//!         todo!()
//!     }
//! }
//!
//! let client = ClientBuilder::new("my-api-key").build_with(MyClient);
//! ```

pub mod backends;
mod client;
pub mod error;
mod http_client;
pub mod models;
mod pagination;
pub mod resources;

// ── Core client types ──
pub use client::{ClientBuilder, ClientConfig, MesaClient};
#[cfg(feature = "reqwest-client")]
pub use client::Mesa;

// ── Error types ──
pub use error::{ApiErrorCode, HttpClientError, MesaError};

// ── HTTP abstraction ──
pub use http_client::{HttpClient, HttpRequest, HttpResponse};

// ── Pagination ──
pub use pagination::PageStream;

// ── Backend re-exports ──
#[cfg(feature = "reqwest-client")]
pub use backends::ReqwestClient;
#[cfg(feature = "ureq-client")]
pub use backends::UreqClient;
