//! HTTP client backend implementations.

#[cfg(feature = "reqwest-client")]
mod reqwest_client;
#[cfg(feature = "reqwest-client")]
pub use reqwest_client::ReqwestClient;

#[cfg(feature = "ureq-client")]
mod ureq_client;
#[cfg(feature = "ureq-client")]
pub use ureq_client::UreqClient;
