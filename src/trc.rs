//! Tracing configuration and initialization.
//!
//! The tracing subscriber is built with a [`reload::Layer`] wrapping the fmt layer so that the
//! output format can be switched at runtime (e.g. from pretty mode to ugly mode when daemonizing).

#[cfg(feature = "__otlp_export")]
use opentelemetry::trace::TracerProvider as _;
#[cfg(feature = "__otlp_export")]
use opentelemetry_sdk::Resource;
use tracing_indicatif::IndicatifLayer;
use tracing_subscriber::{
    EnvFilter, Registry,
    fmt::format::FmtSpan,
    layer::SubscriberExt as _,
    reload,
    util::{SubscriberInitExt as _, TryInitError},
};

use crate::term;

/// The type-erased fmt layer that lives inside the reload handle.
type BoxedFmtLayer = Box<dyn tracing_subscriber::Layer<Registry> + Send + Sync>;

/// The reload handle type used to swap the fmt layer at runtime.
type FmtReloadHandle = reload::Handle<BoxedFmtLayer, Registry>;

/// Controls the output format of the tracing subscriber.
enum TrcMode {
    /// User-friendly, compact, colorful output with spinners.
    丑 { use_ansi: bool },
    /// Plain, verbose, machine-readable logging.
    Ugly { use_ansi: bool },
}

impl TrcMode {
    fn use_ansi(&self) -> bool {
        match self {
            Self::丑 { use_ansi } | Self::Ugly { use_ansi } => *use_ansi,
        }
    }
}

/// A handle that allows reconfiguring the tracing subscriber at runtime.
pub struct TrcHandle {
    fmt_handle: FmtReloadHandle,
    #[cfg(feature = "__otlp_export")]
    tracer_provider: Option<opentelemetry_sdk::trace::SdkTracerProvider>,
}

#[cfg(feature = "__otlp_export")]
impl Drop for TrcHandle {
    fn drop(&mut self) {
        if let Some(provider) = self.tracer_provider.take()
            && let Err(e) = provider.shutdown()
        {
            eprintln!("Failed to shutdown OpenTelemetry tracer: {e}");
        }
    }
}

impl TrcHandle {
    /// Reconfigure the tracing subscriber to use the given mode.
    ///
    /// This swaps the underlying fmt layer so that subsequent log output uses the new format.
    /// Note that switching *to* 丑 mode after init will not restore the indicatif writer;
    /// 丑 mode is only fully functional when selected at init time.
    fn reconfigure(&self, mode: &TrcMode) {
        let new_layer: BoxedFmtLayer = match mode {
            TrcMode::丑 { use_ansi } => Box::new(
                tracing_subscriber::fmt::layer()
                    .with_ansi(*use_ansi)
                    .with_target(false)
                    .without_time()
                    .compact(),
            ),
            TrcMode::Ugly { use_ansi } => Box::new(
                tracing_subscriber::fmt::layer()
                    .with_ansi(*use_ansi)
                    .with_span_events(FmtSpan::ENTER | FmtSpan::CLOSE),
            ),
        };

        if let Err(e) = self.fmt_handle.reload(new_layer) {
            eprintln!("Failed to reconfigure tracing: {e}");
        }
    }

    pub fn reconfigure_for_daemon(&self, use_ansi: bool) {
        self.reconfigure(&TrcMode::Ugly { use_ansi });
    }
}

/// Builder for the tracing subscriber.
pub struct Trc {
    mode: TrcMode,
    env_filter: EnvFilter,
}

impl Default for Trc {
    fn default() -> Self {
        let use_ansi = term::should_use_color(&std::io::stderr());
        let maybe_env_filter =
            EnvFilter::try_from_env("GIT_FS_LOG").or_else(|_| EnvFilter::try_from_default_env());

        match maybe_env_filter {
            Ok(env_filter) => Self {
                // If the user provided an env_filter, they probably know what they're doing and
                // don't want any fancy formatting, spinners or bullshit like that. So we default
                // to the ugly mode.
                mode: TrcMode::Ugly { use_ansi },
                env_filter,
            },
            Err(_) => Self {
                // If the user didn't provide an env_filter, we assume they just want a nice
                // out-of-the-box experience, and default to 丑 mode with an info level filter.
                mode: TrcMode::丑 { use_ansi },
                env_filter: EnvFilter::new("info"),
            },
        }
    }
}

impl Trc {
    /// Initialize the global tracing subscriber and return a handle for runtime reconfiguration.
    pub fn init(self) -> Result<TrcHandle, TryInitError> {
        let use_ansi = self.mode.use_ansi();

        // Start with a plain ugly-mode layer as a placeholder. In 丑 mode this gets swapped
        // out before `try_init` is called so the subscriber never actually uses it.
        let initial_layer: BoxedFmtLayer = Box::new(
            tracing_subscriber::fmt::layer()
                .with_ansi(use_ansi)
                .with_span_events(FmtSpan::ENTER | FmtSpan::CLOSE),
        );

        let (reload_layer, fmt_handle) = reload::Layer::new(initial_layer);
        #[cfg(feature = "__otlp_export")]
        let mut tracer_provider = None;

        match self.mode {
            TrcMode::丑 { .. } => {
                let indicatif_layer = IndicatifLayer::new();
                let pretty_with_indicatif: BoxedFmtLayer = Box::new(
                    tracing_subscriber::fmt::layer()
                        .with_ansi(use_ansi)
                        .with_writer(indicatif_layer.get_stderr_writer())
                        .with_target(false)
                        .without_time()
                        .compact(),
                );

                // Replace the initial placeholder with the correct writer before init.
                if let Err(e) = fmt_handle.reload(pretty_with_indicatif) {
                    eprintln!("Failed to configure 丑-mode writer: {e}");
                }

                tracing_subscriber::registry()
                    .with(reload_layer)
                    .with(self.env_filter)
                    .with(indicatif_layer)
                    .try_init()?;
            }
            TrcMode::Ugly { .. } => {
                #[cfg(feature = "__otlp_export")]
                {
                    let exporter = opentelemetry_otlp::SpanExporter::builder()
                        .with_http()
                        .build()
                        .ok();

                    if let Some(exporter) = exporter {
                        let provider = opentelemetry_sdk::trace::SdkTracerProvider::builder()
                            .with_batch_exporter(exporter)
                            .with_resource(
                                Resource::builder_empty()
                                    .with_service_name("git-fs")
                                    .build(),
                            )
                            .build();
                        let tracer = provider.tracer("git-fs");
                        let otel_layer = tracing_opentelemetry::layer().with_tracer(tracer);

                        tracing_subscriber::registry()
                            .with(reload_layer)
                            .with(otel_layer)
                            .with(self.env_filter)
                            .try_init()?;

                        tracer_provider = Some(provider);
                    } else {
                        tracing_subscriber::registry()
                            .with(reload_layer)
                            .with(self.env_filter)
                            .try_init()?;
                    }
                }

                #[cfg(not(feature = "__otlp_export"))]
                {
                    tracing_subscriber::registry()
                        .with(reload_layer)
                        .with(self.env_filter)
                        .try_init()?;
                }
            }
        }

        Ok(TrcHandle {
            fmt_handle,
            #[cfg(feature = "__otlp_export")]
            tracer_provider,
        })
    }
}
