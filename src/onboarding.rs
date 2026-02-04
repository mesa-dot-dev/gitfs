//! Interactive onboarding wizard for first-time configuration.

use std::fmt::Write as _;
use std::path::{Path, PathBuf};

use inquire::{Confirm, Password, Text, validator::Validation};
use secrecy::{ExposeSecret as _, SecretString};

use crate::app_config::{Config, OrganizationConfig};

/// Error type for onboarding wizard failures.
#[derive(Debug, thiserror::Error)]
pub enum OnboardingError {
    /// The user cancelled the wizard.
    #[error("Setup cancelled.")]
    Cancelled,

    /// An error occurred while prompting the user.
    #[error("Prompt error: {0}")]
    Prompt(#[from] inquire::InquireError),
}

/// Result of the onboarding wizard.
pub struct OnboardingResult {
    /// The constructed configuration.
    pub config: Config,
    /// Organization names and their real (unmasked) API keys, for writing to disk.
    pub org_keys: Vec<(String, SecretString)>,
}

/// Runs the interactive onboarding wizard.
///
/// Returns the config and real API keys (since Config serialization masks them).
///
/// # Errors
///
/// Returns `OnboardingError::Cancelled` if the user presses Ctrl+C.
/// Returns `OnboardingError::Prompt` for other inquire errors.
#[expect(clippy::print_stdout)]
pub fn run_wizard() -> Result<OnboardingResult, OnboardingError> {
    println!("Welcome to git-fs! Let's set up your configuration.\n");

    let defaults = Config::default();

    // 1. Mount point
    let mount_point_str = Text::new("Where should git-fs mount the filesystem?")
        .with_default(&defaults.mount_point.display().to_string())
        .prompt()
        .map_err(map_inquire_error)?;
    let mount_point = PathBuf::from(mount_point_str);

    // 2. Organizations (loop)
    let mut org_keys: Vec<(String, SecretString)> = Vec::new();

    loop {
        let add_org = Confirm::new("Would you like to add an organization?")
            .with_default(false)
            .with_help_message("Organizations connect git-fs to Mesa, GitLab, or other providers.")
            .prompt()
            .map_err(map_inquire_error)?;

        if !add_org {
            break;
        }

        let org_name = Text::new("Organization name:")
            .with_help_message("e.g. 'mesa', 'my-gitlab'")
            .with_validator(|input: &str| {
                if input.is_empty() {
                    return Ok(Validation::Invalid(
                        "Organization name cannot be empty.".into(),
                    ));
                }
                if input.contains(['.', '[', ']', '"', '\'', ' ']) {
                    return Ok(Validation::Invalid(
                        "Organization name cannot contain '.', '[', ']', '\"', '\\'', or spaces."
                            .into(),
                    ));
                }
                Ok(Validation::Valid)
            })
            .prompt()
            .map_err(map_inquire_error)?;

        let api_key = Password::new("API key:")
            .without_confirmation()
            .prompt()
            .map_err(map_inquire_error)?;

        org_keys.push((org_name, SecretString::from(api_key)));
    }

    // Build config
    let mut config = Config {
        mount_point,
        ..defaults
    };

    for (name, key) in &org_keys {
        config.organizations.insert(
            name.clone(),
            OrganizationConfig {
                api_key: key.clone(),
            },
        );
    }

    Ok(OnboardingResult { config, org_keys })
}

/// Writes a config to disk, properly handling API key serialization.
///
/// The normal `Config` serialization masks API keys with `"****"` and strips
/// well-known orgs. This function writes the base config, then appends the
/// real organization API keys.
///
/// Sets file permissions to 0600 (owner read/write only) since the file
/// contains API keys.
///
/// # Errors
///
/// Returns an error if serialization, directory creation, or file writing fails.
pub fn write_config_to_path(
    path: &Path,
    config: &Config,
    org_keys: &[(String, SecretString)],
) -> Result<(), std::io::Error> {
    let mut content = toml::to_string_pretty(config)
        .map_err(|e| std::io::Error::other(format!("Failed to serialize config: {e}")))?;

    // Append real API keys for user-added orgs
    for (org_name, api_key) in org_keys {
        content.push('\n');
        let _ = writeln!(content, "[organizations.{org_name}]");
        let _ = writeln!(content, "api-key = \"{}\"", api_key.expose_secret());
    }

    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }

    std::fs::write(path, content)?;

    // Set file permissions to 0600
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))?;
    }

    Ok(())
}

/// Maps inquire cancellation/interruption errors to `OnboardingError::Cancelled`.
fn map_inquire_error(e: inquire::InquireError) -> OnboardingError {
    match e {
        inquire::InquireError::OperationCanceled | inquire::InquireError::OperationInterrupted => {
            OnboardingError::Cancelled
        }
        other @ (inquire::InquireError::NotTTY
        | inquire::InquireError::InvalidConfiguration(_)
        | inquire::InquireError::IO(_)
        | inquire::InquireError::Custom(_)) => OnboardingError::Prompt(other),
    }
}
