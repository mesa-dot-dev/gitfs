//! Tracing configuration and initialization.

use tracing_indicatif::IndicatifLayer;
use tracing_subscriber::{
    EnvFilter,
    fmt::format::FmtSpan,
    layer::SubscriberExt,
    util::{SubscriberInitExt, TryInitError},
};

struct FgConfig {
    no_spin: bool,
}

impl FgConfig {
    fn is_ugly(&self) -> bool {
        self.no_spin
    }

    pub fn pretty() -> Self {
        Self { no_spin: false }
    }

    pub fn ugly() -> Self {
        Self { no_spin: true }
    }
}

impl Default for FgConfig {
    fn default() -> Self {
        Self { no_spin: false }
    }
}

enum TrcMode {
    Foreground(FgConfig),
    Daemon,
}

pub struct Trc {
    mode: TrcMode,
    env_filter: EnvFilter,
}

impl Default for Trc {
    fn default() -> Self {
        let maybe_env_filter =
            EnvFilter::try_from_env("GIT_FS_LOG").or_else(|_| EnvFilter::try_from_default_env());

        match maybe_env_filter {
            Ok(env_filter) => Self {
                // If the user provided an env_filter, they probably not what they're doing and
                // don't want any fancy formatting, spinners or bullshit like that. So we default
                // to the ugly mode.
                mode: TrcMode::Foreground(FgConfig::ugly()),
                env_filter,
            },
            Err(_) => Self {
                // If the user didn't provide an env_filter, we assume they just want a nice
                // out-of-the-box experience, and default to pretty mode with an info level filter.
                mode: TrcMode::Foreground(FgConfig::pretty()),
                env_filter: EnvFilter::new("info"),
            },
        }
    }
}

impl Trc {
    pub fn init(self) -> Result<(), TryInitError> {
        match &self.mode {
            TrcMode::Daemon => self.init_ugly_mode(),
            TrcMode::Foreground(fg_config) => {
                if fg_config.is_ugly() {
                    self.init_ugly_mode()
                } else {
                    self.init_pretty_mode()
                }
            }
        }
    }

    fn init_ugly_mode(self) -> Result<(), TryInitError> {
        // "Ugly mode" is the plain, verbose, rust logging mode.
        tracing_subscriber::fmt()
            .with_env_filter(self.env_filter)
            .with_span_events(FmtSpan::ENTER | FmtSpan::CLOSE)
            .init();

        Ok(())
    }

    fn init_pretty_mode(self) -> Result<(), TryInitError> {
        // "Pretty mode" is the more user-friendly, compact, and colorful mode. We add a bunch of
        // spinners and interactive elements to keep the user entertained.
        let indicatif_layer = IndicatifLayer::new();
        tracing_subscriber::registry()
            .with(self.env_filter)
            .with(
                tracing_subscriber::fmt::layer()
                    .with_writer(indicatif_layer.get_stderr_writer())
                    .with_target(false)
                    .without_time()
                    .compact(),
            )
            .with(indicatif_layer)
            .try_init()?;

        Ok(())
    }
}
