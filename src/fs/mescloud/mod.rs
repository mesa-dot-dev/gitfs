use mesa_dev::MesaClient;
use opentelemetry::propagation::Injector;
use tracing_opentelemetry::OpenTelemetrySpanExt as _;

#[cfg(feature = "staging")]
const MESA_API_BASE_URL: &str = "https://staging.depot.mesa.dev/api/v1";
#[cfg(not(feature = "staging"))]
const MESA_API_BASE_URL: &str = "https://depot.mesa.dev/api/v1";

mod common;
pub mod repo;
pub mod roots;

struct HeaderInjector<'a>(&'a mut reqwest::header::HeaderMap);

impl Injector for HeaderInjector<'_> {
    fn set(&mut self, key: &str, value: String) {
        if let (Ok(name), Ok(val)) = (
            reqwest::header::HeaderName::from_bytes(key.as_bytes()),
            reqwest::header::HeaderValue::from_str(&value),
        ) {
            self.0.insert(name, val);
        }
    }
}

/// Middleware that injects W3C `traceparent`/`tracestate` headers from the
/// current `tracing` span into every outgoing HTTP request.
struct OtelPropagationMiddleware;

#[async_trait::async_trait]
impl reqwest_middleware::Middleware for OtelPropagationMiddleware {
    async fn handle(
        &self,
        mut req: reqwest::Request,
        extensions: &mut http::Extensions,
        next: reqwest_middleware::Next<'_>,
    ) -> reqwest_middleware::Result<reqwest::Response> {
        let cx = tracing::Span::current().context();
        opentelemetry::global::get_text_map_propagator(|propagator| {
            propagator.inject_context(&cx, &mut HeaderInjector(req.headers_mut()));
        });
        tracing::debug!(
            traceparent = req.headers().get("traceparent").and_then(|v| v.to_str().ok()),
            url = %req.url(),
            "outgoing request"
        );
        next.run(req, extensions).await
    }
}

pub fn build_mesa_client(api_key: &str) -> MesaClient {
    let client = reqwest_middleware::ClientBuilder::new(reqwest::Client::new())
        .with(OtelPropagationMiddleware)
        .build();
    MesaClient::builder()
        .with_api_key(api_key)
        .with_base_path(MESA_API_BASE_URL)
        .with_client(client)
        .build()
}
