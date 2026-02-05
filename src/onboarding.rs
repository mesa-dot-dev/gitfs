//! Interactive onboarding wizard for first-time configuration.

use std::path::PathBuf;

use inquire::{Confirm, Password, Text, validator::Validation};
use secrecy::SecretString;

use crate::app_config::{Config, OrganizationConfig};

/// Error type for onboarding wizard failures.
#[derive(Debug, thiserror::Error)]
pub enum OnboardingError {
    /// An error occurred while prompting the user.
    #[error("Prompt error: {0}")]
    Prompt(#[from] inquire::InquireError),
}

/// Runs the interactive onboarding wizard.
///
/// Returns the config and real API keys (since Config serialization masks them).
///
/// # Errors
///
/// Returns `OnboardingError::Prompt` on inquire errors.
pub fn run_wizard() -> Result<Config, OnboardingError> {
    println!("Welcome to git-fs! Let's set up your configuration.\n");

    let defaults = Config::default();

    // 1. Mount point
    let mount_point_str = Text::new("Where should git-fs mount the filesystem?")
        .with_default(&defaults.mount_point.display().to_string())
        .prompt()
        ?;
    let mount_point = PathBuf::from(mount_point_str);

    // 2. Organizations (loop)
    let mut org_keys: Vec<(String, SecretString)> = Vec::new();

    loop {
        let add_org = Confirm::new("Would you like to add an organization?")
            .with_default(false)
            .with_help_message("Organizations connect git-fs to Mesa, GitLab, or other providers.")
            .prompt()
            ?;

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
            ?;

        let api_key = Password::new("API key:")
            .without_confirmation()
            .prompt()
            ?;

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

    Ok(config)
}
