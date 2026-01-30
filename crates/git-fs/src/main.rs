//! Mount a GitHub repository as a filesystem, without ever cloning.
use std::path::PathBuf;
use std::process::Command;

use clap::Parser;
use fuser::MountOption;
use tracing::error;
use tracing_subscriber::{EnvFilter, fmt};

mod commit_worker;
mod domain;
mod mesafuse;
mod ssfs;
mod util;

use mesafuse::MesaFS;

use crate::domain::GhRepoInfo;

#[derive(Parser)]
#[command(version, author = "Marko Vejnovic")]
struct Args {
    /// Github Repo URL, eg. rust-lang/rust
    repo: GhRepoInfo,

    /// The path to the mount point.
    mount_point: PathBuf,

    /// The Mesa API key. Can also be provided via the `MESA_API_KEY` environment variable.
    #[arg(long, env = "MESA_API_KEY")]
    mesa_api_key: String,

    /// The git reference (branch, tag, commit SHA) to mount. If not provided, defaults to the
    /// repository's default branch.
    #[arg(long)]
    r#ref: Option<String>,

    /// Enable write mode. When enabled, file modifications are immediately committed to the
    /// remote repository. Requires git config user.name and user.email to be set.
    #[arg(long)]
    writable: bool,
}

/// Author information for commits.
#[derive(Debug, Clone)]
pub struct Author {
    /// Author name (from git config user.name).
    pub name: String,
    /// Author email (from git config user.email).
    pub email: String,
}

/// Read a git config value.
fn git_config_get(key: &str) -> Option<String> {
    let output = Command::new("git")
        .args(["config", "--get", key])
        .output()
        .ok()?;

    output
        .status
        .success()
        .then(|| String::from_utf8_lossy(&output.stdout).trim().to_owned())
}

/// Read author from git config, returning an error message if not found.
fn get_author_from_git_config() -> Result<Author, String> {
    let name = git_config_get("user.name").ok_or(
        "git config user.name is not set. Please run: git config --global user.name \"Your Name\"",
    )?;
    let email = git_config_get("user.email")
        .ok_or("git config user.email is not set. Please run: git config --global user.email \"your@email.com\"")?;
    Ok(Author { name, email })
}

fn main() {
    let args = Args::parse();
    fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .with_span_events(fmt::format::FmtSpan::EXIT)
        .init();

    // Read author from git config if writable mode is enabled
    let author = if args.writable {
        match get_author_from_git_config() {
            Ok(author) => Some(author),
            Err(msg) => {
                error!("{msg}");
                std::process::exit(1);
            }
        }
    } else {
        None
    };

    let mut options = vec![
        MountOption::AutoUnmount,
        MountOption::FSName("mesafs".to_owned()),
    ];

    if !args.writable {
        options.push(MountOption::RO);
    }

    let mesa_fs = MesaFS::new(&args.mesa_api_key, args.repo, args.r#ref.as_deref(), author);
    if let Err(e) = fuser::mount2(mesa_fs, &args.mount_point, &options) {
        error!("Failed to mount filesystem: {e}");
    }
}
