//! Tracing configuration and initialization.
//!
//! The tracing subscriber is built with a [`reload::Layer`] wrapping the fmt layer so that the
//! output format can be switched at runtime (e.g. from pretty mode to ugly mode when daemonizing).

use opentelemetry::trace::TracerProvider as _;
use opentelemetry_otlp::WithExportConfig as _;
use opentelemetry_sdk::Resource;
use tracing_indicatif::IndicatifLayer;
use tracing_subscriber::{
    EnvFilter, Registry,
    fmt::format::FmtSpan,
    layer::SubscriberExt as _,
    reload,
    util::{SubscriberInitExt as _, TryInitError},
};

use crate::app_config::TelemetryConfig;
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
    tracer_provider: Option<opentelemetry_sdk::trace::SdkTracerProvider>,
}

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
    otlp_endpoints: Vec<String>,
}

impl Default for Trc {
    fn default() -> Self {
        let use_ansi = term::should_use_color(&std::io::stderr());
        let maybe_env_filter =
            EnvFilter::try_from_env("GIT_FS_LOG").or_else(|_| EnvFilter::try_from_default_env());

        match maybe_env_filter {
            Ok(env_filter) => {
                // If the user provided an env filter, they probably know what they're doing
                // and don't want any fancy formatting or spinners. Default to ugly mode.
                Self {
                    mode: TrcMode::Ugly { use_ansi },
                    env_filter,
                    otlp_endpoints: Vec::new(),
                }
            }
            Err(_) => {
                // No env filter provided — give the user a nice out-of-the-box experience
                // with compact formatting and progress spinners.
                Self {
                    mode: TrcMode::丑 { use_ansi },
                    env_filter: EnvFilter::new("info"),
                    otlp_endpoints: Vec::new(),
                }
            }
        }
    }
}

impl Trc {
    /// Configure OTLP telemetry endpoints from the application config.
    #[must_use]
    pub fn with_telemetry(mut self, telemetry: &TelemetryConfig) -> Self {
        self.otlp_endpoints = telemetry.endpoints();
        self
    }

    /// Build the OpenTelemetry tracer provider if any OTLP endpoints are configured.
    fn build_otel_provider(&self) -> Option<opentelemetry_sdk::trace::SdkTracerProvider> {
        if self.otlp_endpoints.is_empty() {
            return None;
        }

        let resource = Resource::builder()
            .with_service_name("git-fs")
            .with_attribute(opentelemetry::KeyValue::new(
                "service.version",
                env!("CARGO_PKG_VERSION"),
            ))
            .build();
        let mut builder =
            opentelemetry_sdk::trace::SdkTracerProvider::builder().with_resource(resource);

        let mut has_exporter = false;
        for endpoint in &self.otlp_endpoints {
            match opentelemetry_otlp::SpanExporter::builder()
                .with_http()
                .with_endpoint(endpoint)
                .build()
            {
                Ok(exporter) => {
                    builder = builder.with_batch_exporter(exporter);
                    has_exporter = true;
                }
                Err(e) => {
                    eprintln!("Failed to create OTLP exporter for {endpoint}: {e}");
                }
            }
        }

        has_exporter.then(|| builder.build())
    }

    /// Initialize the global tracing subscriber and return a handle for runtime reconfiguration.
    pub fn init(self) -> Result<TrcHandle, TryInitError> {
        let use_ansi = self.mode.use_ansi();

        let initial_layer: BoxedFmtLayer = Box::new(
            tracing_subscriber::fmt::layer()
                .with_ansi(use_ansi)
                .with_span_events(FmtSpan::ENTER | FmtSpan::CLOSE),
        );

        let (reload_layer, fmt_handle) = reload::Layer::new(initial_layer);
        let provider = self.build_otel_provider();
        if provider.is_some() {
            opentelemetry::global::set_text_map_propagator(
                opentelemetry_sdk::propagation::TraceContextPropagator::new(),
            );
        }

        match self.mode {
            TrcMode::丑 { .. } => {
                let indicatif_layer = IndicatifLayer::new().with_max_progress_bars(24, None);
                let pretty_with_indicatif: BoxedFmtLayer = Box::new(
                    tracing_subscriber::fmt::layer()
                        .with_ansi(use_ansi)
                        .with_writer(indicatif_layer.get_stderr_writer())
                        .with_target(false)
                        .without_time()
                        .compact(),
                );

                if let Err(e) = fmt_handle.reload(pretty_with_indicatif) {
                    eprintln!("Failed to configure 丑-mode writer: {e}");
                }

                let otel_layer = provider
                    .as_ref()
                    .map(|p| tracing_opentelemetry::layer().with_tracer(p.tracer("git-fs")));

                tracing_subscriber::registry()
                    .with(reload_layer)
                    .with(otel_layer)
                    .with(self.env_filter)
                    .with(indicatif_layer)
                    .try_init()?;
            }
            TrcMode::Ugly { .. } => {
                let otel_layer = provider
                    .as_ref()
                    .map(|p| tracing_opentelemetry::layer().with_tracer(p.tracer("git-fs")));

                tracing_subscriber::registry()
                    .with(reload_layer)
                    .with(otel_layer)
                    .with(self.env_filter)
                    .try_init()?;
            }
        }

        Ok(TrcHandle {
            fmt_handle,
            tracer_provider: provider,
        })
    }
}
