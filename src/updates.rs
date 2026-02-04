//! Checks whether the running binary is the latest released version.

use tracing::{error, info};

/// The git SHA baked in at compile time by `vergen-gitcl`.
const BUILD_SHA: &str = env!("VERGEN_GIT_SHA");

/// The crate version from `Cargo.toml`.
const PKG_VERSION: &str = env!("CARGO_PKG_VERSION");

/// Check GitHub for the latest stable release and warn if this binary is outdated.
///
/// This function never fails the application — it logs errors and returns.
pub fn check_for_updates() {
    let short_sha = &BUILD_SHA[..7.min(BUILD_SHA.len())];
    let running_version = format!("{PKG_VERSION}+{short_sha}");

    let releases = match self_update::backends::github::ReleaseList::configure()
        .repo_owner("mesa-dot-dev")
        .repo_name("gitfs")
        .build()
    {
        Ok(list) => match list.fetch() {
            Ok(releases) => releases,
            Err(e) => {
                info!("Could not check for updates: {e}");
                return;
            }
        },
        Err(e) => {
            info!("Could not configure update check: {e}");
            return;
        }
    };

    // Find the stable release (tagged "latest" on GitHub).
    let Some(stable) = releases.iter().find(|r| r.version == "latest") else {
        info!("No stable release found on GitHub.");
        return;
    };

    // Release name format: "git-fs 0.1.1-alpha+172e35d"
    let latest_version = stable.name.strip_prefix("git-fs ").unwrap_or(&stable.name);

    // TODO(ME-672): This check is technically wrong, since it checks for equality, but some users
    // may be running canary/dev, which may be more recent. We should ideally parse semver and
    // compare properly.
    if running_version == latest_version {
        info!("You are running the latest version ({running_version}).");
    } else {
        error!(
            "You are running git-fs {running_version}, \
             but the latest release is {latest_version}. \
             Please update: https://github.com/mesa-dot-dev/gitfs/releases"
        );
    }
}
