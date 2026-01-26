//! Resource namespaces for the Mesa API.

mod admin;
mod branches;
mod commits;
mod content;
mod diffs;
mod repos;

pub use admin::AdminResource;
pub use branches::BranchesResource;
pub use commits::{CommitsResource, ListCommitsParams};
pub use content::ContentResource;
pub use diffs::DiffsResource;
pub use repos::ReposResource;
