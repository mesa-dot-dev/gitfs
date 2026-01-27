//! Mount a GitHub repository as a filesystem, without ever cloning.
use std::path::PathBuf;

use clap::Parser;
use fuser::MountOption;
use tracing::error;
use tracing_subscriber::{EnvFilter, fmt};

mod ssfs;
mod util;
mod domain;
mod mesafuse;

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
}

fn main() {
    let args = Args::parse();
    fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .with_span_events(fmt::format::FmtSpan::EXIT)
        .init();

    let options = vec![
        MountOption::RO,
        MountOption::AutoUnmount,
        MountOption::FSName("mesafs".to_string())
    ];

    let mesa_fs = MesaFS::new(&args.mesa_api_key, args.repo, args.r#ref.as_deref());
    if let Err(e) = fuser::mount2(mesa_fs, &args.mount_point, &options) {
        error!("Failed to mount filesystem: {e}");
    }
}
