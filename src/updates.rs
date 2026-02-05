//! Checks whether the running binary is the latest released version.

use tracing::{debug, error, warn};

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
        .repo_name("git-fs")
        .build()
    {
        Ok(list) => match list.fetch() {
            Ok(releases) => releases,
            Err(e) => {
                error!("Could not check for updates: {e}");
                return;
            }
        },
        Err(e) => {
            error!("Could not configure update check: {e}");
            return;
        }
    };

    // Find the stable release (tagged "latest" on GitHub).
    let Some(stable) = releases.iter().find(|r| r.version == "latest") else {
        error!("No stable release found on GitHub.");
        return;
    };

    let latest_version = extract_version_from_release_name(&stable.name);

    let (Some(running_ver), Some(latest_ver)) = (
        semver::Version::parse(&running_version).ok(),
        semver::Version::parse(latest_version).ok(),
    ) else {
        error!(
            version = running_version,
            latest_version = latest_version,
            "Could not parse version strings for comparison. If you are seeing this, it's a \
                bug. Please report it on https://github.com/mesa-dot-dev/git-fs/issues."
        );
        return;
    };

    match running_ver.cmp_precedence(&latest_ver) {
        std::cmp::Ordering::Equal => {
            debug!(version = running_version, "Using latest version.");
        }
        std::cmp::Ordering::Greater => {
            warn!(
                version = running_version,
                "git-fs is ahead of the latest stable release. \
                 This version is considered unstable."
            );
        }
        std::cmp::Ordering::Less => {
            error!(
                version = running_version,
                latest_version = latest_version,
                "You are running git-fs {running_version}, \
                 but the latest release is {latest_version}. \
                 Please update: https://github.com/mesa-dot-dev/git-fs/releases"
            );
        }
    }
}

/// Extract the semver version string from a GitHub release name.
///
/// Strips the `"git-fs "` prefix and optional `" (canary)"` suffix.
/// If the prefix is not present, returns the full name unchanged.
fn extract_version_from_release_name(name: &str) -> &str {
    let without_prefix = name.strip_prefix("git-fs ").unwrap_or(name);
    without_prefix
        .strip_suffix(" (canary)")
        .unwrap_or(without_prefix)
}
