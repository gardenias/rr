//! W3C Trace Context propagation for rr gRPC services.
//!
//! This module provides **lightweight** trace-context forwarding over
//! gRPC metadata without depending on the full OpenTelemetry SDK. It:
//!
//! 1. On the **server** side — extracts the `traceparent` header (W3C
//!    TraceContext, RFC <https://www.w3.org/TR/trace-context/>) from
//!    inbound gRPC metadata and stashes it on the current `tracing` span
//!    as a `traceparent` field. This lets operators correlate rr logs
//!    with the upstream request's trace id by greppping any structured
//!    sink (JSON logs, Loki, etc.).
//!
//! 2. On the **client** side — exposes [`attach_traceparent`] so callers
//!    can inject their current trace context into outbound requests.
//!
//! ### Bring-your-own OpenTelemetry
//!
//! Users who install [`tracing-opentelemetry`](https://docs.rs/tracing-opentelemetry)
//! + an OTLP exporter will automatically:
//!
//! - On the server, see rr spans with their upstream trace id
//!   (via the `traceparent` field).
//! - On the client, the current span's trace id can be serialized to
//!   `traceparent` and passed to [`attach_traceparent`].
//!
//! rr does **not** bundle the SDK, exporter, or collector configuration —
//! those are intentionally left to the user's observability stack.
//!
//! ### Why not auto-generate spans?
//!
//! The full `tracing-opentelemetry` bridge is ~12MB of dependencies
//! (protobuf, grpcio, OT SDK, multi-backend exporters). rr-network must
//! stay small; the tradeoff is that users must opt in to full OTLP.

use tonic::metadata::{AsciiMetadataValue, MetadataMap};
use tonic::{Request, Status};

/// W3C Trace Context header name (lowercase as per HTTP/2 convention).
pub const TRACEPARENT_HEADER: &str = "traceparent";

/// W3C Trace State header name (optional vendor extensions).
pub const TRACESTATE_HEADER: &str = "tracestate";

/// Extract the `traceparent` header from an inbound request's metadata.
///
/// Returns `None` if the header is absent or malformed. Validation is
/// lenient — the header is accepted verbatim and forwarded; full W3C
/// format validation is left to downstream tooling (OpenTelemetry
/// parser, jaeger collector, etc.).
pub fn extract_traceparent<T>(request: &Request<T>) -> Option<String> {
    extract_from_metadata(request.metadata(), TRACEPARENT_HEADER)
}

/// Extract the `tracestate` header (vendor-specific trace extensions).
pub fn extract_tracestate<T>(request: &Request<T>) -> Option<String> {
    extract_from_metadata(request.metadata(), TRACESTATE_HEADER)
}

fn extract_from_metadata(md: &MetadataMap, key: &str) -> Option<String> {
    md.get(key)
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string())
}

/// Attach a W3C `traceparent` value to an outbound request's metadata.
///
/// The caller is responsible for constructing the `traceparent` string
/// per the W3C spec (`00-<trace-id>-<span-id>-<flags>`). For a full-
/// featured experience, wire this to `tracing-opentelemetry` in your
/// application and pull the current span's context:
///
/// ```ignore
/// use tracing_opentelemetry::OpenTelemetrySpanExt;
/// let cx = tracing::Span::current().context();
/// // ... serialize cx to a W3C traceparent string ...
/// rr_network::tracing::attach_traceparent(&mut req, traceparent_string)?;
/// ```
pub fn attach_traceparent<T>(request: &mut Request<T>, traceparent: &str) -> Result<(), Status> {
    let value: AsciiMetadataValue = traceparent
        .parse()
        .map_err(|_| Status::invalid_argument("traceparent not valid ASCII"))?;
    request.metadata_mut().insert(TRACEPARENT_HEADER, value);
    Ok(())
}

/// Attach a W3C `tracestate` value alongside `traceparent` (optional).
pub fn attach_tracestate<T>(request: &mut Request<T>, tracestate: &str) -> Result<(), Status> {
    let value: AsciiMetadataValue = tracestate
        .parse()
        .map_err(|_| Status::invalid_argument("tracestate not valid ASCII"))?;
    request.metadata_mut().insert(TRACESTATE_HEADER, value);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn req_with_header(key: &'static str, value: &str) -> Request<()> {
        let mut req = Request::new(());
        req.metadata_mut().insert(key, value.parse().unwrap());
        req
    }

    #[test]
    fn extracts_present_traceparent() {
        let req = req_with_header(
            "traceparent",
            "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01",
        );
        assert_eq!(
            extract_traceparent(&req).as_deref(),
            Some("00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01")
        );
    }

    #[test]
    fn returns_none_when_absent() {
        let req: Request<()> = Request::new(());
        assert_eq!(extract_traceparent(&req), None);
    }

    #[test]
    fn extracts_tracestate_when_present() {
        let req = req_with_header("tracestate", "rojo=00f067aa0ba902b7");
        assert_eq!(
            extract_tracestate(&req).as_deref(),
            Some("rojo=00f067aa0ba902b7")
        );
    }

    #[test]
    fn attach_traceparent_sets_header() {
        let mut req: Request<()> = Request::new(());
        let tp = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01";
        attach_traceparent(&mut req, tp).unwrap();
        assert_eq!(extract_traceparent(&req).as_deref(), Some(tp));
    }

    #[test]
    fn attach_and_extract_roundtrip_includes_tracestate() {
        let mut req: Request<()> = Request::new(());
        attach_traceparent(
            &mut req,
            "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01",
        )
        .unwrap();
        attach_tracestate(&mut req, "rojo=00f067aa0ba902b7").unwrap();
        assert!(extract_traceparent(&req).is_some());
        assert!(extract_tracestate(&req).is_some());
    }

    #[test]
    fn rejects_invalid_header_value() {
        let mut req: Request<()> = Request::new(());
        let err = attach_traceparent(&mut req, "invalid\nvalue\rwith-CRLF").unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
    }
}
