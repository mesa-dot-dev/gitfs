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
        .repo_name("git-fs")
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

    let latest_version = extract_version_from_release_name(&stable.name);

    match compare_versions(&running_version, latest_version) {
        Some(UpdateStatus::UpToDate) => {
            info!("You are running the latest version ({running_version}).");
        }
        Some(UpdateStatus::AheadOfLatest) => {
            info!(
                "You are running git-fs {running_version}, \
                 which is ahead of the latest stable release ({latest_version}). \
                 This is expected for canary or development builds."
            );
        }
        Some(UpdateStatus::UpdateAvailable) => {
            error!(
                "You are running git-fs {running_version}, \
                 but the latest release is {latest_version}. \
                 Please update: https://github.com/mesa-dot-dev/git-fs/releases"
            );
        }
        None => {
            info!(
                "Could not parse version strings for comparison \
                 (running={running_version}, latest={latest_version})."
            );
        }
    }
}

/// Result of comparing the running binary version against the latest release.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UpdateStatus {
    /// The running version matches the latest stable release.
    UpToDate,
    /// A newer stable release is available.
    UpdateAvailable,
    /// The running version is newer than the latest release (canary or dev build).
    AheadOfLatest,
}

/// Parse two semver strings and determine the update status.
///
/// Returns `None` if either string fails to parse as a valid semver version.
/// Build metadata (the `+...` portion) is ignored by the semver specification,
/// meaning two versions differing only in build metadata are considered equal.
fn compare_versions(running: &str, latest: &str) -> Option<UpdateStatus> {
    let running_ver = semver::Version::parse(running).ok()?;
    let latest_ver = semver::Version::parse(latest).ok()?;

    Some(match running_ver.cmp_precedence(&latest_ver) {
        std::cmp::Ordering::Equal => UpdateStatus::UpToDate,
        std::cmp::Ordering::Less => UpdateStatus::UpdateAvailable,
        std::cmp::Ordering::Greater => UpdateStatus::AheadOfLatest,
    })
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn equal_versions_are_up_to_date() {
        assert_eq!(
            compare_versions("0.1.1-alpha.1+ab1cd23", "0.1.1-alpha.1+ab1cd23"),
            Some(UpdateStatus::UpToDate),
        );
    }

    #[test]
    fn same_version_different_sha_is_up_to_date() {
        assert_eq!(
            compare_versions("0.1.1-alpha.1+ab1cd23", "0.1.1-alpha.1+ff9900a"),
            Some(UpdateStatus::UpToDate),
        );
    }

    #[test]
    fn running_older_than_latest_is_update_available() {
        assert_eq!(
            compare_versions("0.1.0+ab1cd23", "0.1.1+ff9900a"),
            Some(UpdateStatus::UpdateAvailable),
        );
    }

    #[test]
    fn running_newer_than_latest_is_ahead() {
        assert_eq!(
            compare_versions("0.2.0+ab1cd23", "0.1.1+ff9900a"),
            Some(UpdateStatus::AheadOfLatest),
        );
    }

    #[test]
    fn prerelease_is_less_than_release() {
        assert_eq!(
            compare_versions("0.1.1-alpha.1+ab1cd23", "0.1.1+ff9900a"),
            Some(UpdateStatus::UpdateAvailable),
        );
    }

    #[test]
    fn release_is_greater_than_prerelease() {
        assert_eq!(
            compare_versions("0.1.1+ab1cd23", "0.1.1-alpha.1+ff9900a"),
            Some(UpdateStatus::AheadOfLatest),
        );
    }

    #[test]
    fn prerelease_ordering_works() {
        assert_eq!(
            compare_versions("0.1.1-alpha.1+ab1cd23", "0.1.1-alpha.2+ff9900a"),
            Some(UpdateStatus::UpdateAvailable),
        );
    }

    #[test]
    fn invalid_running_version_returns_none() {
        assert_eq!(compare_versions("not-a-version", "0.1.1+ff9900a"), None);
    }

    #[test]
    fn invalid_latest_version_returns_none() {
        assert_eq!(
            compare_versions("0.1.1+ab1cd23", "also-not-a-version"),
            None
        );
    }

    #[test]
    fn versions_without_build_metadata_work() {
        assert_eq!(
            compare_versions("0.1.1", "0.1.1"),
            Some(UpdateStatus::UpToDate),
        );
    }

    #[test]
    fn strips_git_fs_prefix() {
        assert_eq!(
            extract_version_from_release_name("git-fs 0.1.1-alpha.1+ab1cd23"),
            "0.1.1-alpha.1+ab1cd23",
        );
    }

    #[test]
    fn strips_canary_suffix() {
        assert_eq!(
            extract_version_from_release_name("git-fs 0.1.1-alpha.1+ab1cd23 (canary)"),
            "0.1.1-alpha.1+ab1cd23",
        );
    }

    #[test]
    fn handles_name_without_prefix() {
        assert_eq!(
            extract_version_from_release_name("0.1.1-alpha.1+ab1cd23"),
            "0.1.1-alpha.1+ab1cd23",
        );
    }

    #[test]
    fn handles_name_with_only_canary_suffix_no_prefix() {
        assert_eq!(
            extract_version_from_release_name("0.1.1-alpha.1+ab1cd23 (canary)"),
            "0.1.1-alpha.1+ab1cd23",
        );
    }
}
