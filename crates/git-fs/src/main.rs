//! Mount a GitHub repository as a filesystem, without ever cloning.
use std::path::{Path, PathBuf};
use std::process::Command;

use clap::Parser;
use fuser::MountOption;
use tracing::{error, info, warn};
use tracing_subscriber::{EnvFilter, fmt};

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
        MountOption::FSName("mesafs".to_owned()),
    ];

    let mesa_fs = MesaFS::new(&args.mesa_api_key, args.repo, args.r#ref.as_deref());

    // Use spawn_mount2 to get a BackgroundSession that can be properly cleaned up.
    // When the session is dropped, the filesystem is unmounted.
    let session = match fuser::spawn_mount2(mesa_fs, &args.mount_point, &options) {
        Ok(session) => session,
        Err(e) => {
            error!("Failed to mount filesystem: {e}");
            return;
        }
    };

    info!(
        "Mounted at {:?}. Press Ctrl+C to unmount.",
        args.mount_point
    );

    // Wait for CTRL+C signal.
    let (tx, rx) = std::sync::mpsc::channel();
    if let Err(e) = ctrlc::set_handler(move || {
        let _ = tx.send(());
    }) {
        error!("Failed to set Ctrl+C handler: {e}");
        // Fall back to just joining the session thread
        session.join();
        return;
    }

    // Block until we receive the signal.
    let _ = rx.recv();
    info!("Received Ctrl+C, unmounting...");

    // Force unmount the filesystem. This handles the case where something
    // is still accessing the mount. The kernel will send DESTROY to our
    // FUSE handler, causing the background thread to exit.
    if force_unmount(&args.mount_point) {
        // Force unmount succeeded. Forget the session to prevent fuser's
        // destructor from trying to unmount again (which would fail with
        // "Invalid argument" since it's already unmounted).
        // The background thread has already exited due to DESTROY.
        #[expect(clippy::mem_forget)]
        std::mem::forget(session);
    } else {
        // Force unmount failed, let fuser try its normal cleanup.
        session.join();
    }

    info!("Unmounted successfully.");
}

/// Force unmount a FUSE filesystem. Uses platform-specific commands.
/// Returns true if unmount succeeded, false otherwise.
fn force_unmount(mount_point: &Path) -> bool {
    #[cfg(target_os = "macos")]
    let result = Command::new("umount").arg("-f").arg(mount_point).status();

    #[cfg(target_os = "linux")]
    let result = Command::new("fusermount")
        .arg("-uz") // lazy unmount
        .arg(mount_point)
        .status();

    #[cfg(not(any(target_os = "macos", target_os = "linux")))]
    let result: Result<std::process::ExitStatus, std::io::Error> = Err(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "unsupported platform",
    ));

    match result {
        Ok(status) if status.success() => {
            info!("Force unmount succeeded");
            true
        }
        Ok(status) => {
            warn!("Force unmount exited with status: {status}");
            false
        }
        Err(e) => {
            warn!("Force unmount failed: {e}");
            false
        }
    }
}
