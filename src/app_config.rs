//! Module for application configuration settings.
//!
//! User configurations may be specified in a configuration file.

use bytesize::ByteSize;
use secrecy::SecretString;
use tracing::{debug, info};

use std::{
    collections::HashMap,
    path::{Path, PathBuf},
};

use figment::{
    Figment,
    providers::{Env, Format as _, Toml},
};
use serde::{Deserialize, Serialize};

fn mesa_runtime_dir() -> Option<PathBuf> {
    let runtime_dir = dirs::runtime_dir();
    if let Some(path) = runtime_dir {
        return Some(path.join("git-fs"));
    }

    let home_dir = dirs::home_dir();
    if let Some(path) = home_dir {
        return Some(path.join(".local").join("share").join("git-fs"));
    }

    None
}

fn default_pid_file() -> PathBuf {
    mesa_runtime_dir().map_or_else(
        || PathBuf::from("/var/run/git-fs.pid"),
        |rd| rd.join("git-fs.pid"),
    )
}

fn default_mount_point() -> PathBuf {
    mesa_runtime_dir().map_or_else(|| PathBuf::from("/tmp/git-fs/mnt"), |rd| rd.join("mnt"))
}

fn current_uid() -> u32 {
    nix::unistd::Uid::current().as_raw()
}

fn current_gid() -> u32 {
    nix::unistd::Gid::current().as_raw()
}

/// The cache configuration.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct CacheConfig {
    /// The maximum size of the cache in bytes.
    pub max_size: Option<ByteSize>,

    /// The path to the cache directory.
    pub path: PathBuf,
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            max_size: None,
            path: mesa_runtime_dir()
                .map_or_else(|| PathBuf::from("/tmp/git-fs/cache"), |rd| rd.join("cache")),
        }
    }
}

/// Well-known organizations and their default API keys.
///
/// Note that these are publicly exposed and we are comfortable with that, as these keys are
/// equivalent to read-only access tokens.
const WELL_KNOWN_ORGS: [(&str, &str); 1] = [("github", "dp_live_uAgKRbhVNcDiUZXVyTQbBIaJEerhSwQh")];

fn default_organizations() -> HashMap<String, OrganizationConfig> {
    WELL_KNOWN_ORGS.iter().fold(
        HashMap::<String, OrganizationConfig>::new(),
        |mut acc, (name, api_key)| {
            acc.insert(
                name.to_string(),
                OrganizationConfig {
                    api_key: SecretString::from(api_key.to_string()),
                },
            );
            acc
        },
    )
}

/// Deserializes the organizations map, then ensures the default GitHub entry
/// is always present (without overwriting a user-provided one).
fn deserialize_organizations_with_defaults<'de, D>(
    deserializer: D,
) -> Result<HashMap<String, OrganizationConfig>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    Ok(WELL_KNOWN_ORGS.iter().fold(
        HashMap::<String, OrganizationConfig>::deserialize(deserializer)?,
        |mut acc, (name, api_key)| {
            acc.insert(
                name.to_string(),
                OrganizationConfig {
                    api_key: SecretString::from(api_key.to_string()),
                },
            );
            acc
        },
    ))
}

/// Serializes the organizations map, excluding well-known organizations so they
/// don't get written to the config file.
fn serialize_organizations_without_well_known<S>(
    organizations: &HashMap<String, OrganizationConfig>,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    use serde::ser::SerializeMap as _;

    let well_known_names: std::collections::HashSet<&str> =
        WELL_KNOWN_ORGS.iter().map(|(name, _)| *name).collect();

    let filtered: Vec<_> = organizations
        .iter()
        .filter(|(key, _)| !well_known_names.contains(key.as_str()))
        .collect();

    let mut map = serializer.serialize_map(Some(filtered.len()))?;
    for (key, value) in filtered {
        map.serialize_entry(key, value)?;
    }
    map.end()
}

/// The configuration block on a per organization basis.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct OrganizationConfig {
    /// The API key to use for this organization.
    #[serde(serialize_with = "serialize_api_key")]
    pub api_key: SecretString,
}

fn serialize_api_key<S>(_api_key: &SecretString, serializer: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    serializer.serialize_str("****")
}

/// Daemon configuration.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct DaemonConfig {
    /// The path to the PID file for the daemon. Uses /var/run/git-fs.pid if not specified.
    #[serde(default = "default_pid_file")]
    pub pid_file: PathBuf,
}

impl Default for DaemonConfig {
    fn default() -> Self {
        Self {
            pid_file: default_pid_file(),
        }
    }
}

/// Application configuration structure.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct Config {
    #[serde(
        default = "default_organizations",
        deserialize_with = "deserialize_organizations_with_defaults",
        serialize_with = "serialize_organizations_without_well_known"
    )]
    pub organizations: HashMap<String, OrganizationConfig>,

    #[serde(default)]
    pub cache: CacheConfig,

    #[serde(default)]
    pub daemon: DaemonConfig,

    /// The mount point for the filesystem.
    #[serde(default = "default_mount_point")]
    pub mount_point: PathBuf,

    /// The user to mount the filesystem as. If not specified, runs as the current user.
    #[serde(default = "current_uid")]
    pub uid: u32,

    /// The group to mount the filesystem as. If not specified, runs as the current group.
    #[serde(default = "current_gid")]
    pub gid: u32,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            organizations: default_organizations(),
            cache: CacheConfig::default(),
            daemon: DaemonConfig::default(),
            mount_point: default_mount_point(),
            uid: current_uid(),
            gid: current_gid(),
        }
    }
}

impl Config {
    /// Validate the correctness of the configuration.
    ///
    /// Returns:
    /// - `Ok(())` if the configuration is valid.
    /// - `Err(Vec<String>)` containing a list of validation error messages if the configuration
    pub fn validate(&self) -> Result<(), Vec<String>> {
        let mut errors = Vec::new();

        if self.daemon.pid_file.parent().is_none() {
            errors.push(format!(
                "PID file path '{}' has no parent directory.",
                self.daemon.pid_file.display()
            ));
        }

        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors)
        }
    }
}

// Defines a trait for providing configuration paths.
pub trait ConfigPathProviderTrait {
    /// Returns a vector of OS strings representing configuration file paths.
    ///
    /// This method should return all relevant configuration file paths for the specific OS. The
    /// order should be of ascending priority.
    ///
    /// The most-specific configuration file should be last in the returned vector.
    ///
    /// Configurations will be merged, with later configurations overriding earlier ones.
    fn get_config_paths() -> impl IntoIterator<Item = impl AsRef<Path>>;

    /// Returns the path to be used for creating a new configuration file.
    fn get_creation_config_path<P: AsRef<Path>>(all_paths: impl Iterator<Item = P>) -> Option<P> {
        all_paths.last()
    }

    /// Loads the configuration from the available configuration files.
    fn load_config(external_config_path: Option<&Path>) -> Result<Config, Box<figment::Error>> {
        let figment = Figment::new();

        let paths: Vec<_> = Self::get_config_paths().into_iter().collect();
        let mut figment = paths.iter().fold(figment, |figment: Figment, path| {
            if path.as_ref().exists() {
                debug!(path = ?path.as_ref(), "Found configuration file.");
                figment.merge(Toml::file(path))
            } else {
                figment
            }
        });

        if let Some(path) = external_config_path {
            figment = figment.merge(Toml::file(path));
        }

        figment
            .merge(Env::prefixed("GIT_FS_"))
            .extract()
            .map_err(Box::new)
    }

    /// Loads the configuration or creates a default one if none exists.
    fn load_or_create(external_config_path: Option<&Path>) -> Result<Config, Box<figment::Error>> {
        // Figment doesn't have a way to tell us whether a file existed or not, so the best we can
        // do is try to load the config (hot-path), and if that fails, check exactly why it failed.
        // We manually have to go through the paths to see if any of them exist. If none do, we can
        // safely create a new config.
        match Self::load_config(external_config_path) {
            Ok(config) => {
                debug!("Loaded configuration successfully.");
                Ok(config)
            }
            Err(e) => {
                let config_paths: Vec<_> = Self::get_config_paths().into_iter().collect();
                let any_path_exists = config_paths.iter().any(|path| path.as_ref().exists());

                if any_path_exists {
                    // We found SOME config file, it just seems malformed.
                    Err(e)
                } else {
                    // We now need to find a place to write the default config to.
                    match Self::get_creation_config_path(config_paths.iter()) {
                        None => {
                            // The user doesn't have some of the directories where we could write the
                            // config. That just means we can't create a config file.
                            Err(e)
                        }
                        Some(path) => {
                            // The path is valid and we can create a default config file there.
                            info!(path = ?path.as_ref(), "Creating default config...");
                            Self::create_default_in_path(path.as_ref())
                        }
                    }
                }
            }
        }
    }

    fn create_default_in_path(path: &Path) -> Result<Config, Box<figment::Error>> {
        let config = Config::default();

        // TODO(markovejnovic): This currently doesn't write out potentially useful comments to the
        // toml file. There's a `documented` crate thay may be useful for this.
        let content = toml::to_string_pretty(&config).map_err(|e| {
            Box::new(figment::Error::from(format!(
                "Failed to serialize default config: {e}"
            )))
        })?;

        std::fs::create_dir_all(path.parent().unwrap_or(path)).map_err(|e| {
            Box::new(figment::Error::from(format!(
                "Failed to create config directory '{}': {e}",
                path.display()
            )))
        })?;
        std::fs::write(path, content).map_err(|e| {
            Box::new(figment::Error::from(format!(
                "Failed to write default config file: {e}"
            )))
        })?;

        Ok(config)
    }
}

/// Searches for paths according to the following priority:
///
/// - `$XDG_CONFIG_HOME/git-fs/config.toml`
/// - `$HOME/.config/git-fs/config.toml`
/// - `/etc/git-fs/config.toml`
pub struct ConfigPathProvider;

impl ConfigPathProviderTrait for ConfigPathProvider {
    fn get_config_paths() -> impl IntoIterator<Item = impl AsRef<Path>> {
        let xdg_config_home = dirs::config_dir().map(|p| p.join("git-fs").join("config.toml"));
        let config_home =
            dirs::home_dir().map(|p| p.join(".config").join("git-fs").join("config.toml"));
        let etc_config = Some(PathBuf::from("/etc/git-fs/config.toml"));

        [xdg_config_home, config_home, etc_config]
            .into_iter()
            .rev()
            .flatten()
            .collect::<Vec<_>>()
    }
}
