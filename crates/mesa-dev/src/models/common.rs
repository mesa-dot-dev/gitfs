//! Common response types.

use serde::Deserialize;

/// A simple success response.
#[derive(Debug, Clone, Deserialize)]
pub struct SuccessResponse {
    /// Whether the operation succeeded.
    pub success: bool,
}
