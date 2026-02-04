//! Checks whether the running binary is the latest released version.

use tracing::{error, info};

/// The git SHA baked in at compile time by `vergen-gitcl`.
const BUILD_SHA: &str = env!("VERGEN_GIT_SHA");

/// Check GitHub for the latest release and warn if this binary is outdated.
///
/// This function never fails the application — it logs errors and returns.
pub fn check_for_updates() {
    let short_sha = &BUILD_SHA[..7.min(BUILD_SHA.len())];

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

    let Some(latest) = releases.first() else {
        info!("No releases found on GitHub.");
        return;
    };

    // Release tags are "canary-{short_sha}". Extract the SHA suffix.
    let latest_sha = latest
        .version
        .strip_prefix("canary-")
        .unwrap_or(&latest.version);

    if short_sha == latest_sha {
        info!("You are running the latest version ({short_sha}).");
    } else {
        error!(
            "You are running git-fs built from commit {short_sha}, \
             but the latest release is from commit {latest_sha}. \
             Please update: https://github.com/mesa-dot-dev/gitfs/releases"
        );
    }
}
