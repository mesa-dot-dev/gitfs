//! Pagination types.

use serde::Serialize;

/// Query parameters for paginated endpoints.
#[derive(Debug, Clone, Default, Serialize)]
pub struct PaginationParams {
    /// Cursor for the next page.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cursor: Option<String>,
    /// Maximum number of results per page.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub limit: Option<u32>,
}

/// Trait for paginated response types.
pub trait Paginated {
    /// The individual item type within a page.
    type Item;

    /// Returns the items from this page.
    fn items(self) -> Vec<Self::Item>;

    /// Returns the cursor for the next page, if any.
    fn next_cursor(&self) -> Option<&str>;

    /// Returns whether more pages are available.
    fn has_more(&self) -> bool;
}
