//! Module for application configuration settings.
//!
//! User configurations may be specified in a configuration file.

use bytesize::ByteSize;
use secrecy::{ExposeSecret as _, SecretString};
use thiserror::Error;
use tracing::{debug, info};

use std::{
    collections::HashMap,
    io::IsTerminal as _,
    path::{Path, PathBuf},
};

use serde::{Deserialize, Serialize};

use crate::{
    onboarding::{self, OnboardingError},
    term,
};

/// A `PathBuf` that automatically expands `~` to the user's home directory
/// during deserialization. This ensures that any path loaded from configuration
/// is already resolved.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(transparent)]
pub struct ExpandedPathBuf(PathBuf);

impl<'de> Deserialize<'de> for ExpandedPathBuf {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let raw = String::deserialize(deserializer)?;
        let expanded = shellexpand::tilde(&raw);
        Ok(Self(PathBuf::from(expanded.into_owned())))
    }
}

impl ExpandedPathBuf {
    /// Creates a new `ExpandedPathBuf` from any path, without expansion.
    /// Use this for programmatically-constructed paths that are already absolute.
    pub fn new(path: PathBuf) -> Self {
        Self(path)
    }
}

impl std::ops::Deref for ExpandedPathBuf {
    type Target = Path;

    fn deref(&self) -> &Path {
        &self.0
    }
}

impl AsRef<Path> for ExpandedPathBuf {
    fn as_ref(&self) -> &Path {
        &self.0
    }
}

impl std::fmt::Display for ExpandedPathBuf {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.display().fmt(f)
    }
}

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

fn default_pid_file() -> ExpandedPathBuf {
    ExpandedPathBuf::new(mesa_runtime_dir().map_or_else(
        || PathBuf::from("/var/run/git-fs.pid"),
        |rd| rd.join("git-fs.pid"),
    ))
}

fn default_mount_point() -> ExpandedPathBuf {
    ExpandedPathBuf::new(
        mesa_runtime_dir().map_or_else(|| PathBuf::from("/tmp/git-fs/mnt"), |rd| rd.join("mnt")),
    )
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
    pub path: ExpandedPathBuf,
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            max_size: None,
            path: ExpandedPathBuf::new(
                mesa_runtime_dir()
                    .map_or_else(|| PathBuf::from("/tmp/git-fs/cache"), |rd| rd.join("cache")),
            ),
        }
    }
}

/// Where daemon logs should be written.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum LogTarget {
    /// Write logs to stdout (default for foreground mode).
    #[default]
    Stdout,
    /// Write logs to stderr.
    Stderr,
    /// Write logs to a file at the given path.
    File(PathBuf),
}

impl LogTarget {
    /// Opens the log file for the daemon, if this target is [`LogTarget::File`].
    ///
    /// Returns `None` for stdout/stderr targets (the daemonize crate's default
    /// sends these to `/dev/null`; the tracing subscriber handles them instead).
    pub fn open_log_file(&self) -> Result<Option<std::fs::File>, std::io::Error> {
        match self {
            Self::Stdout | Self::Stderr => Ok(None),
            Self::File(path) => {
                if let Some(parent) = path.parent() {
                    std::fs::create_dir_all(parent)?;
                }
                let file = std::fs::OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(path)?;
                Ok(Some(file))
            }
        }
    }
}

/// Controls whether ANSI color codes are included in log output.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ColorMode {
    /// Auto-detect: enable color for terminals, disable for files and pipes.
    #[default]
    Auto,
    /// Always include ANSI color codes.
    Always,
    /// Never include ANSI color codes.
    Never,
}

/// Logging configuration for the daemon.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case", default)]
pub struct LogConfig {
    /// Where to write log output. Defaults to stdout.
    pub target: LogTarget,

    /// Whether to include ANSI color in log output.
    /// "auto" detects based on whether the target is a terminal.
    pub color: ColorMode,
}

impl LogConfig {
    /// Determines whether ANSI color codes should be used for the configured target.
    ///
    /// Rules:
    /// - `Always` -> true
    /// - `Never` -> false
    /// - `Auto` -> true only if the target is stdout/stderr AND it's a terminal (TTY)
    pub fn should_use_color(&self) -> bool {
        match self.color {
            ColorMode::Always => true,
            ColorMode::Never => false,
            ColorMode::Auto => match self.target {
                LogTarget::Stdout => term::should_use_color(&std::io::stdout()),
                LogTarget::Stderr => term::should_use_color(&std::io::stderr()),
                LogTarget::File(_) => false,
            },
        }
    }
}

trait WithApiKey {
    fn with_api_key(key: SecretString) -> Self;
}

impl WithApiKey for OrganizationConfig {
    fn with_api_key(key: SecretString) -> Self {
        Self { api_key: key }
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
fn deserialize_organizations_with_defaults<'de, D, K, V>(
    deserializer: D,
) -> Result<HashMap<K, V>, D::Error>
where
    D: serde::Deserializer<'de>,
    K: Deserialize<'de> + Eq + std::hash::Hash + From<String>,
    V: Deserialize<'de> + WithApiKey,
{
    let mut map = HashMap::<K, V>::deserialize(deserializer)?;
    for (name, api_key) in &WELL_KNOWN_ORGS {
        map.entry(K::from((*name).to_owned()))
            .or_insert_with(|| V::with_api_key(SecretString::from((*api_key).to_owned())));
    }
    Ok(map)
}

/// Serializes the organizations map, excluding well-known organizations so they
/// don't get written to the config file.
fn serialize_organizations_without_well_known<S, K, V>(
    organizations: &HashMap<K, V>,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
    K: AsRef<str> + Serialize + Eq + std::hash::Hash,
    V: Serialize,
{
    use serde::ser::SerializeMap as _;

    let well_known_names: std::collections::HashSet<&str> =
        WELL_KNOWN_ORGS.iter().map(|(name, _)| *name).collect();

    let filtered: Vec<_> = organizations
        .iter()
        .filter(|(key, _)| !well_known_names.contains(key.as_ref()))
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

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
struct DangerousOrganizationConfig<'a> {
    /// The API key to use for this organization.
    pub api_key: &'a str,
}

impl<'a> From<&'a OrganizationConfig> for DangerousOrganizationConfig<'a> {
    fn from(org: &'a OrganizationConfig) -> Self {
        Self {
            api_key: org.api_key.expose_secret(),
        }
    }
}

/// Daemon configuration.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct DaemonConfig {
    /// The path to the PID file for the daemon. Uses /var/run/git-fs.pid if not specified.
    #[serde(default = "default_pid_file")]
    pub pid_file: ExpandedPathBuf,

    /// Logging configuration.
    #[serde(default)]
    pub log: LogConfig,
}

impl Default for DaemonConfig {
    fn default() -> Self {
        Self {
            pid_file: default_pid_file(),
            log: LogConfig::default(),
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
    pub mount_point: ExpandedPathBuf,

    /// The user to mount the filesystem as. If not specified, runs as the current user.
    #[serde(default = "current_uid")]
    pub uid: u32,

    /// The group to mount the filesystem as. If not specified, runs as the current group.
    #[serde(default = "current_gid")]
    pub gid: u32,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "kebab-case")]
struct DangerousConfig<'a> {
    pub organizations: HashMap<&'a str, DangerousOrganizationConfig<'a>>,
    pub cache: &'a CacheConfig,
    pub daemon: &'a DaemonConfig,
    pub mount_point: &'a Path,
    pub uid: u32,
    pub gid: u32,
}

impl<'a> From<&'a Config> for DangerousConfig<'a> {
    fn from(config: &'a Config) -> Self {
        let well_known_names: std::collections::HashSet<&str> =
            WELL_KNOWN_ORGS.iter().map(|(name, _)| *name).collect();

        Self {
            organizations: config
                .organizations
                .iter()
                .filter(|(k, _)| !well_known_names.contains(k.as_str()))
                .map(|(k, v)| (k.as_str(), DangerousOrganizationConfig::from(v)))
                .collect(),
            cache: &config.cache,
            daemon: &config.daemon,
            mount_point: &config.mount_point,
            uid: config.uid,
            gid: config.gid,
        }
    }
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

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("Configuration validation errors: {0:?}")]
    ValidationErrors(Vec<String>),

    #[error("Failed to onboard: {0}")]
    OnboardingError(OnboardingError),

    #[error("Serialization error: {0}")]
    SerializationError(#[from] toml::ser::Error),

    #[error("Deserialization error: {0}")]
    DeserializationError(#[from] toml::de::Error),

    #[error("I/O error: {0}")]
    IoError(#[from] std::io::Error),

    #[error("Config parent directory does not exist.")]
    NoParentDir,

    #[error("No suitable configuration path found.")]
    NoSuitableConfigPath,
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

        if let LogTarget::File(ref path) = self.daemon.log.target {
            if path.as_os_str().is_empty() {
                errors.push("Log file path must not be empty.".to_owned());
            } else if path.parent().is_none() {
                errors.push(format!(
                    "Log file path '{}' has no parent directory.",
                    path.display()
                ));
            }
        }

        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors)
        }
    }

    /// Returns config file paths in descending priority order.
    /// On macOS, skips `dirs::config_dir()` (resolves to ~/Library/Application Support/).
    fn config_search_paths() -> Vec<PathBuf> {
        let mut paths = Vec::new();

        #[cfg(not(target_os = "macos"))]
        if let Some(xdg) = dirs::config_dir() {
            paths.push(xdg.join("git-fs").join("config.toml"));
        }

        if let Some(home) = dirs::home_dir() {
            paths.push(home.join(".config").join("git-fs").join("config.toml"));
        }

        paths.push(PathBuf::from("/etc/git-fs/config.toml"));

        paths
    }

    /// Loads config from a single TOML file.
    fn load_from_file(path: &Path) -> Result<Self, ConfigError> {
        debug!(path = ?path, "Loading configuration file.");
        let content = std::fs::read_to_string(path)?;
        let config: Self = toml::from_str(&content)?;
        Ok(config)
    }

    /// Loads configuration from the first found config file, or the external path if given.
    pub fn load(
        external_config_path: Option<&Path>,
    ) -> Option<Result<(Self, PathBuf), ConfigError>> {
        if let Some(path) = external_config_path {
            return Some(Self::load_from_file(path).map(|cfg| (cfg, path.to_path_buf())));
        }

        let search_paths = Self::config_search_paths();
        if let Some(path) = search_paths.iter().find(|p| p.exists()) {
            Some(Self::load_from_file(path).map(|cfg| (cfg, path.clone())))
        } else {
            info!(tried = ?search_paths, "No configuration file found.");
            None
        }
    }

    /// Loads config or creates a default if none exists.
    /// Errors if a config file exists but is malformed.
    pub fn load_or_create(external_config_path: Option<&Path>) -> Result<Self, ConfigError> {
        if let Some(res) = Self::load(external_config_path) {
            let (config, path) = res?;
            if let Err(validation_errors) = config.validate() {
                return Err(ConfigError::ValidationErrors(validation_errors));
            }
            info!(path = %path.display(), "Loaded config file.");
            return Ok(config);
        }

        // No config exists — create default at highest-priority path
        let creation_path = Self::config_search_paths()
            .into_iter()
            .next()
            .ok_or(ConfigError::NoSuitableConfigPath)?;

        let config = onboarding::run_wizard().map_err(ConfigError::OnboardingError)?;
        config.dangerously_write_to_disk(&creation_path)?;
        info!(path = ?creation_path.display(), "Created configuration file.");
        Ok(config)
    }

    fn dangerously_write_to_disk(&self, path: &Path) -> Result<(), ConfigError> {
        let dangerous_config = DangerousConfig::from(self);
        let toml_str = toml::to_string_pretty(&dangerous_config)?;
        std::fs::create_dir_all(path.parent().ok_or(ConfigError::NoParentDir)?)?;
        std::fs::write(path, toml_str)?;
        Ok(())
    }
}
