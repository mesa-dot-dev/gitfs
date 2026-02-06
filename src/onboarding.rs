//! Interactive onboarding wizard for first-time configuration.

use std::io::IsTerminal as _;
use std::path::PathBuf;

use inquire::{Confirm, Password, Text, validator::Validation};
use secrecy::SecretString;

use crate::app_config::{Config, ExpandedPathBuf, OrganizationConfig};

const WELCOME_MESSAGE: &str = "
    \x1b[32m@@@@\x1b[0m      Welcome to \x1b[1mgit-fs\x1b[0m! Let's get you started!
    \x1b[32m@@@@ @@@\x1b[0m
    \x1b[32m@@@@ @@@\x1b[0m  \x1b[1mgit-fs\x1b[0m allows you to mount all of GitHub on your local filesystem.
\x1b[32m@@@ @@@@@@@@\x1b[0m  It works by mirroring GitHub repositories to fast caches hosted on \x1b]8;;https://mesa.dev\x1b\\\x1b[1mmesa.dev\x1b[0m\x1b]8;;\x1b\\.
 \x1b[32m@@@@@@@@@@\x1b[0m
  \x1b[32m@@@@@@@@\x1b[0m
    \x1b[32m@@@@\x1b[0m      Mesa ships a fast on-demand VCS, optimized for agentic AI.
    \x1b[32m@@@@\x1b[0m      You do not need to use Mesa products to use \x1b[1mgit-fs\x1b[0m, but if you
    \x1b[32m@@@@\x1b[0m      like \x1b[1mgit-fs\x1b[0m, we're sure you're going to love \x1b]8;;https://mesa.dev\x1b\\\x1b[1mmesa.dev\x1b[0m\x1b]8;;\x1b\\.
";

/// Error type for onboarding wizard failures.
#[derive(Debug, thiserror::Error)]
pub enum OnboardingError {
    /// An error occurred while prompting the user.
    #[error("Prompt error: {0}")]
    Prompt(#[from] inquire::InquireError),

    #[error("Terminal is not interactive")]
    NonInteractive,
}

/// Runs the interactive onboarding wizard.
///
/// Returns the config and real API keys (since Config serialization masks them).
///
/// # Errors
///
/// Returns `OnboardingError::Prompt` on inquire errors.
pub fn run_wizard() -> Result<Config, OnboardingError> {
    if !(std::io::stdin().is_terminal() && std::io::stdout().is_terminal()) {
        return Err(OnboardingError::NonInteractive);
    }

    println!("{WELCOME_MESSAGE}");

    let defaults = Config::default();

    let mount_point_str = Text::new("Where should git-fs mount the filesystem?")
        .with_default(&defaults.mount_point.display().to_string())
        .prompt()?;
    let mount_point = ExpandedPathBuf::new(PathBuf::from(
        shellexpand::tilde(&mount_point_str).into_owned(),
    ));

    let mut org_keys: Vec<(String, SecretString)> = Vec::new();
    loop {
        let prompt_msg = if org_keys.is_empty() {
            "Would you like to add Mesa organizations?"
        } else {
            "Would you like to add another Mesa organization?"
        };
        let add_org = Confirm::new(prompt_msg)
            .with_default(false)
            .with_help_message(
                "If all you're trying to do is mount public GitHub repos, you can skip this step.",
            )
            .prompt()?;

        if !add_org {
            break;
        }

        let org_name = Text::new("Organization name:")
            .with_help_message("e.g. 'tyrell-corp', 'globex'")
            .with_validator(|input: &str| {
                if input.is_empty() {
                    return Ok(Validation::Invalid(
                        "Organization name cannot be empty.".into(),
                    ));
                }
                // TODO(markovejnovic): I don't know if this is correct.
                if input.contains(['.', '[', ']', '"', '\'', ' ']) {
                    return Ok(Validation::Invalid(
                        "Organization name cannot contain '.', '[', ']', '\"', '\\'', or spaces."
                            .into(),
                    ));
                }
                Ok(Validation::Valid)
            })
            .prompt()?;

        let api_key = Password::new("API key:").without_confirmation().prompt()?;

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
