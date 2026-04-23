//! HTTP `/health`, `/ready`, and `/metrics` endpoints for rr-server.
//!
//! When [`NodeConfig::with_metrics_port`](crate::config::NodeConfig::with_metrics_port)
//! is set, `RrNode` spawns a minimal hyper server on that port serving:
//!
//! - `GET /health` — liveness probe. Returns 200 iff the process is up.
//! - `GET /ready`  — readiness probe. Returns 200 iff the node finished
//!                   `start()` successfully; 503 otherwise (e.g. during
//!                   startup or after `shutdown()`).
//! - `GET /metrics` — Prometheus scrape endpoint (text format).
//!
//! K8s `readinessProbe` / `livenessProbe` should use HTTP on this port
//! rather than TCP on the gRPC port, because TCP liveness on gRPC doesn't
//! distinguish "server accepting connections" from "cluster can make
//! progress".

use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use http_body_util::Full;
use hyper::body::Bytes;
use hyper::{Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use metrics_exporter_prometheus::PrometheusHandle;
use tokio::net::TcpListener;
use tokio::sync::watch;
use tracing::{info, warn};

use rr_core::error::{ConfigError, RrError};

/// Readiness signal — flipped to `true` when `RrNode::start()` completes,
/// back to `false` on `shutdown()`. Shared (via `Arc`) between the node
/// and the HTTP server task.
#[derive(Clone, Debug)]
pub struct ReadinessFlag(Arc<AtomicBool>);

impl ReadinessFlag {
    pub fn new() -> Self {
        Self(Arc::new(AtomicBool::new(false)))
    }

    pub fn set(&self, ready: bool) {
        self.0.store(ready, Ordering::Release);
    }

    pub fn is_ready(&self) -> bool {
        self.0.load(Ordering::Acquire)
    }
}

impl Default for ReadinessFlag {
    fn default() -> Self {
        Self::new()
    }
}

/// Spawn the HTTP observability server. Returns a `JoinHandle` the caller
/// must own and, on shutdown, wake via `shutdown_rx`.
pub async fn spawn_http_server(
    addr: SocketAddr,
    prom_handle: PrometheusHandle,
    readiness: ReadinessFlag,
    mut shutdown_rx: watch::Receiver<bool>,
) -> Result<tokio::task::JoinHandle<()>, RrError> {
    let listener = TcpListener::bind(addr).await.map_err(|e| {
        RrError::Config(ConfigError::InvalidConfig(format!(
            "failed to bind HTTP observability port {addr}: {e}"
        )))
    })?;
    info!(%addr, "HTTP observability server listening (/health /ready /metrics)");

    let handle = tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = shutdown_rx.changed() => {
                    info!(%addr, "HTTP observability server shutting down");
                    break;
                }
                accept = listener.accept() => {
                    let (stream, _peer) = match accept {
                        Ok(pair) => pair,
                        Err(e) => {
                            warn!(%addr, "accept error: {e}");
                            continue;
                        }
                    };
                    let io = TokioIo::new(stream);
                    let ph = prom_handle.clone();
                    let rf = readiness.clone();
                    tokio::spawn(async move {
                        let service = hyper::service::service_fn(move |req| {
                            handle_request(req, ph.clone(), rf.clone())
                        });
                        if let Err(err) = hyper::server::conn::http1::Builder::new()
                            .serve_connection(io, service)
                            .await
                        {
                            // Don't spam on normal client disconnects.
                            if !is_benign_hyper_error(&err) {
                                warn!("hyper connection error: {err}");
                            }
                        }
                    });
                }
            }
        }
    });
    Ok(handle)
}

async fn handle_request(
    req: Request<hyper::body::Incoming>,
    prom_handle: PrometheusHandle,
    readiness: ReadinessFlag,
) -> Result<Response<Full<Bytes>>, std::convert::Infallible> {
    let resp = match (req.method(), req.uri().path()) {
        (&hyper::Method::GET, "/health") => text_response(StatusCode::OK, "ok\n"),
        (&hyper::Method::GET, "/ready") => {
            if readiness.is_ready() {
                text_response(StatusCode::OK, "ready\n")
            } else {
                text_response(StatusCode::SERVICE_UNAVAILABLE, "starting\n")
            }
        }
        (&hyper::Method::GET, "/metrics") => {
            let body = prom_handle.render();
            Response::builder()
                .status(StatusCode::OK)
                .header(
                    hyper::header::CONTENT_TYPE,
                    "text/plain; version=0.0.4; charset=utf-8",
                )
                .body(Full::new(Bytes::from(body)))
                .expect("valid response")
        }
        _ => text_response(StatusCode::NOT_FOUND, "not found\n"),
    };
    Ok(resp)
}

fn text_response(status: StatusCode, body: &str) -> Response<Full<Bytes>> {
    Response::builder()
        .status(status)
        .header(hyper::header::CONTENT_TYPE, "text/plain; charset=utf-8")
        .body(Full::new(Bytes::from(body.to_string())))
        .expect("valid response")
}

fn is_benign_hyper_error(err: &hyper::Error) -> bool {
    let s = err.to_string();
    s.contains("connection closed") || s.contains("Broken pipe") || s.contains("reset by peer")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn readiness_flag_default_is_not_ready() {
        let r = ReadinessFlag::new();
        assert!(!r.is_ready());
    }

    #[test]
    fn readiness_flag_roundtrip() {
        let r = ReadinessFlag::new();
        r.set(true);
        assert!(r.is_ready());
        r.set(false);
        assert!(!r.is_ready());
    }

    #[test]
    fn readiness_flag_clones_share_state() {
        let r1 = ReadinessFlag::new();
        let r2 = r1.clone();
        r1.set(true);
        assert!(r2.is_ready());
    }
}
