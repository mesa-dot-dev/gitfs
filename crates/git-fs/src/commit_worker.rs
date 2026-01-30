//! Background worker for processing commit requests.
//!
//! This module handles asynchronous commits to the remote repository via the Mesa API.

use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use mesa_dev::Mesa;
use mesa_dev::models::{Author, CommitEncoding, CommitFile, CommitFileAction, CreateCommitRequest};
use tokio::sync::mpsc;
use tracing::{error, info};

/// A request to create a commit, sent to the background worker.
pub enum CommitRequest {
    /// Create a new file with the given content.
    Create {
        /// Path to the file to create.
        path: String,
        /// Content of the file.
        content: Vec<u8>,
    },
    /// Update an existing file with new content.
    Update {
        /// Path to the file to update.
        path: String,
        /// New content of the file.
        content: Vec<u8>,
    },
    /// Delete a file.
    Delete {
        /// Path to the file to delete.
        path: String,
    },
}

/// Configuration for the commit worker.
pub struct CommitWorkerConfig {
    /// Mesa API client.
    pub mesa: Mesa,
    /// Repository organization/owner.
    pub org: String,
    /// Repository name.
    pub repo: String,
    /// Branch to commit to.
    pub branch: String,
    /// Author information for commits.
    pub author: Author,
}

/// Spawns a background task that processes commit requests from the given receiver.
///
/// The task will run until the channel is closed (all senders are dropped).
pub fn spawn_commit_worker(
    rt: &tokio::runtime::Runtime,
    config: CommitWorkerConfig,
    mut commit_rx: mpsc::UnboundedReceiver<CommitRequest>,
) {
    let mesa = config.mesa;
    let org = config.org;
    let repo = config.repo;
    let branch = config.branch;
    let author = config.author;
    rt.spawn(async move {
        while let Some(request) = commit_rx.recv().await {
            let (message, files) = match request {
                CommitRequest::Create { path, content } => {
                    // Use "." for empty files to work around Mesa API bug with empty content
                    let content_bytes = if content.is_empty() {
                        b".".as_slice()
                    } else {
                        &content
                    };
                    (
                        format!("Create {path}"),
                        vec![CommitFile {
                            action: CommitFileAction::Upsert,
                            path,
                            encoding: CommitEncoding::Base64,
                            content: Some(BASE64.encode(content_bytes)),
                        }],
                    )
                }
                CommitRequest::Update { path, content } => {
                    // Use "." for empty files to work around Mesa API bug with empty content
                    let content_bytes = if content.is_empty() {
                        b".".as_slice()
                    } else {
                        &content
                    };
                    (
                        format!("Update {path}"),
                        vec![CommitFile {
                            action: CommitFileAction::Upsert,
                            path,
                            encoding: CommitEncoding::Base64,
                            content: Some(BASE64.encode(content_bytes)),
                        }],
                    )
                }
                CommitRequest::Delete { path } => (
                    format!("Delete {path}"),
                    vec![CommitFile {
                        action: CommitFileAction::Delete,
                        path,
                        encoding: CommitEncoding::Base64,
                        content: None,
                    }],
                ),
            };

            let create_commit_request = CreateCommitRequest {
                branch: branch.clone(),
                message: message.clone(),
                author: author.clone(),
                files,
                base_sha: None,
            };

            info!("about to commit the following: {:?}", create_commit_request);

            let result = mesa
                .commits(&org, &repo)
                .create(&create_commit_request)
                .await;

            match result {
                Ok(_) => info!(message = %message, "commit pushed"),
                Err(e) => error!(message = %message, error = %e, "commit failed"),
            }
        }
    });
}
