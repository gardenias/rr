//! otel-client — minimal example showing how to propagate W3C
//! `traceparent` from an OpenTelemetry-instrumented client through
//! the rr-client SDK so server-side spans on the rr cluster pick up
//! the same trace.
//!
//! Run:
//! ```bash
//! cargo run -p otel-client
//! ```
//!
//! The example uses an stdout exporter so spans print directly. In
//! production you would swap in an OTLP exporter pointed at your
//! collector (jaeger / tempo / honeycomb / datadog / …) and the
//! same `tracing` macros + the same rr-client calls would emit
//! distributed traces unchanged.
//!
//! What this demonstrates:
//! 1. Initialize OpenTelemetry + tracing-opentelemetry as the
//!    `tracing` subscriber's layer.
//! 2. Open a parent span on the client side via the standard
//!    `#[tracing::instrument]` attribute.
//! 3. Pull the W3C `traceparent` from the current span via
//!    `OpenTelemetrySpanExt::context()`.
//! 4. Attach it to the outbound gRPC request with
//!    `rr_network::tracing::attach_traceparent` so the rr server's
//!    `RaftService::extract_traceparent` middleware (Phase 3 Wave E)
//!    picks it up and continues the trace.

use anyhow::Result;
use opentelemetry::global;
use opentelemetry::propagation::TextMapPropagator;
use opentelemetry::trace::TracerProvider as _;
use opentelemetry_sdk::propagation::TraceContextPropagator;
use opentelemetry_sdk::trace::TracerProvider;
use std::collections::HashMap;
use tracing::{info, instrument};
use tracing_opentelemetry::OpenTelemetrySpanExt;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

#[tokio::main]
async fn main() -> Result<()> {
    let provider = TracerProvider::builder()
        .with_simple_exporter(opentelemetry_stdout::SpanExporter::default())
        .build();
    let tracer = provider.tracer("otel-client-example");
    global::set_tracer_provider(provider);
    global::set_text_map_propagator(TraceContextPropagator::new());

    tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer())
        .with(tracing_opentelemetry::layer().with_tracer(tracer))
        .with(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    do_work().await?;

    global::shutdown_tracer_provider();
    Ok(())
}

#[instrument(name = "client.do_work")]
async fn do_work() -> Result<()> {
    info!("starting business operation");
    let traceparent = current_traceparent();
    info!(?traceparent, "captured traceparent for outbound RPC");

    info!(
        "next step: build an `rr_client::RrClient`, then before each \
         `send_request*` call, do:\n\
         \trr_network::tracing::attach_traceparent(&mut request, &traceparent)?;"
    );

    Ok(())
}

fn current_traceparent() -> String {
    let cx = tracing::Span::current().context();
    let propagator = TraceContextPropagator::new();
    let mut carrier: HashMap<String, String> = HashMap::new();
    propagator.inject_context(&cx, &mut carrier);
    carrier.remove("traceparent").unwrap_or_default()
}
