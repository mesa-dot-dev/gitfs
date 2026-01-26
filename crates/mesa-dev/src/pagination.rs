//! Cursor-based pagination support.

use std::collections::VecDeque;
use std::sync::Arc;

use http::Method;
use serde::de::DeserializeOwned;

use crate::client::ClientInner;
use crate::error::MesaError;
use crate::http_client::HttpClient;
use crate::models::Paginated;

/// An async page stream that lazily fetches pages from a paginated endpoint.
///
/// Owns all its state (via `Arc`) so there are no lifetime parameters.
pub struct PageStream<C: HttpClient, Page: Paginated + DeserializeOwned> {
    inner: Arc<ClientInner<C>>,
    path: String,
    extra_query: Vec<(String, String)>,
    cursor: Option<String>,
    buffer: VecDeque<Page::Item>,
    done: bool,
}

impl<C: HttpClient, Page: Paginated + DeserializeOwned> PageStream<C, Page> {
    /// Create a new page stream.
    pub(crate) fn new(
        inner: Arc<ClientInner<C>>,
        path: String,
        extra_query: Vec<(String, String)>,
    ) -> Self {
        Self {
            inner,
            path,
            extra_query,
            cursor: None,
            buffer: VecDeque::new(),
            done: false,
        }
    }

    /// Fetch the next individual item, requesting new pages as needed.
    ///
    /// Returns `Ok(None)` when all pages have been exhausted.
    pub async fn next(&mut self) -> Result<Option<Page::Item>, MesaError> {
        if let Some(item) = self.buffer.pop_front() {
            return Ok(Some(item));
        }

        if self.done {
            return Ok(None);
        }

        if let Some(page) = self.fetch_page().await? {
            let has_more = page.has_more();
            self.cursor = page.next_cursor().map(ToOwned::to_owned);
            self.buffer = VecDeque::from(page.items());
            self.done = !has_more || self.cursor.is_none();
            Ok(self.buffer.pop_front())
        } else {
            self.done = true;
            Ok(None)
        }
    }

    /// Collect all remaining items into a `Vec`.
    pub async fn collect(mut self) -> Result<Vec<Page::Item>, MesaError> {
        let mut all = Vec::new();
        while let Some(item) = self.next().await? {
            all.push(item);
        }
        Ok(all)
    }

    /// Fetch the next full page.
    ///
    /// Returns `Ok(None)` when all pages have been exhausted.
    pub async fn next_page(&mut self) -> Result<Option<Page>, MesaError> {
        if self.done {
            return Ok(None);
        }
        let page = self.fetch_page().await?;
        if let Some(ref p) = page {
            let has_more = p.has_more();
            self.cursor = p.next_cursor().map(ToOwned::to_owned);
            self.done = !has_more || self.cursor.is_none();
        } else {
            self.done = true;
        }
        Ok(page)
    }

    /// Internal: fetch a single page from the API.
    async fn fetch_page(&self) -> Result<Option<Page>, MesaError> {
        let mut query: Vec<(&str, &str)> = self
            .extra_query
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect();

        if let Some(ref cursor) = self.cursor {
            query.push(("cursor", cursor));
        }

        let page: Page = self
            .inner
            .request(Method::GET, &self.path, &query, None)
            .await?;

        Ok(Some(page))
    }
}
