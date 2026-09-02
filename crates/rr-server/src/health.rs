//! HTTP `/health`, `/ready`, and `/metrics` endpoints for rr-server.
//!
//! When [`NodeConfig::with_metrics_port`](crate::config::NodeConfig::with_metrics_port)
//! is set, `RrNode` spawns a minimal hyper server on that port serving:
//!
//! - `GET /health` — liveness probe. Returns 200 iff the process is up.
//! - `GET /ready`  — readiness probe. Returns 200 iff the node finished
//!   `start()` successfully; 503 otherwise (e.g. during startup or after
//!   `shutdown()`). Body is JSON:
//!
//!   ```json
//!   {"ready": true,  "reason": "ready",    "since_unix_secs": 1714128000}
//!   {"ready": false, "reason": "starting", "since_unix_secs": 1714127990}
//!   ```
//!
//!   The `reason` field is free-form and operator-readable
//!   (`"starting"`, `"ready"`, `"shutting_down"`, `"degraded:<detail>"`,
//!   etc.). Tools that only care about the boolean should consume the
//!   HTTP status code; tools that want richer state should parse the
//!   body. K8s readinessProbe falls into the former bucket — the status
//!   code alone gates traffic.
//!
//! - `GET /metrics` — Prometheus scrape endpoint (text format).
//!
//! K8s `readinessProbe` / `livenessProbe` should use HTTP on this port
//! rather than TCP on the gRPC port, because TCP liveness on gRPC doesn't
//! distinguish "server accepting connections" from "cluster can make
//! progress".

use std::net::SocketAddr;
use std::sync::{Arc, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};

use http_body_util::Full;
use hyper::body::Bytes;
use hyper::{Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use metrics_exporter_prometheus::PrometheusHandle;
use serde::Serialize;
use tokio::net::TcpListener;
use tokio::sync::watch;
use tracing::{info, warn};

use rr_core::error::{ConfigError, RrError};

/// Readiness signal with a free-form `reason` and the wall-clock time
/// the current state was last set. Flipped by [`Self::set`] (boolean
/// shorthand) or [`Self::set_with_reason`] (full transition).
/// Shared (via `Arc`) between the node and the HTTP server task.
///
/// Internally a `std::sync::RwLock` — the read/write is microsecond-scale
/// and not a hot path (only touched on startup, shutdown, and on each
/// `/ready` HTTP request).
#[derive(Clone, Debug)]
pub struct ReadinessFlag(Arc<RwLock<ReadinessState>>);

#[derive(Clone, Debug, Serialize)]
struct ReadinessState {
    ready: bool,
    reason: String,
    since_unix_secs: u64,
}

impl ReadinessFlag {
    pub fn new() -> Self {
        Self(Arc::new(RwLock::new(ReadinessState {
            ready: false,
            reason: "starting".to_string(),
            since_unix_secs: now_unix_secs(),
        })))
    }

    /// Boolean shorthand: flip readiness, default the reason to
    /// `"ready"` (when becoming ready) or `"shutting_down"` (when
    /// becoming not-ready). Preserves backwards compatibility with
    /// the v0.2 API.
    pub fn set(&self, ready: bool) {
        let reason = if ready { "ready" } else { "shutting_down" };
        self.set_with_reason(ready, reason);
    }

    /// Full state transition with a free-form reason. Use this for
    /// richer signals like `"degraded:lost_quorum"` or
    /// `"draining_for_decommission"` that operators query via
    /// `GET /ready`.
    pub fn set_with_reason(&self, ready: bool, reason: impl Into<String>) {
        if let Ok(mut guard) = self.0.write() {
            guard.ready = ready;
            guard.reason = reason.into();
            guard.since_unix_secs = now_unix_secs();
        }
    }

    /// Synchronous boolean read. Cheap, safe in any context.
    pub fn is_ready(&self) -> bool {
        self.0.read().map(|g| g.ready).unwrap_or(false)
    }

    fn snapshot(&self) -> ReadinessState {
        self.0
            .read()
            .map(|g| g.clone())
            .unwrap_or_else(|_| ReadinessState {
                ready: false,
                reason: "lock_poisoned".to_string(),
                since_unix_secs: 0,
            })
    }
}

impl Default for ReadinessFlag {
    fn default() -> Self {
        Self::new()
    }
}

fn now_unix_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// Spawn the HTTP observability server. Returns a `JoinHandle` the caller
/// must own and, on shutdown, wake via `shutdown_rx`.
pub async fn spawn_http_server(
    addr: SocketAddr,
    prom_handle: PrometheusHandle,
    readiness: ReadinessFlag,
    shutdown_rx: watch::Receiver<bool>,
) -> Result<tokio::task::JoinHandle<()>, RrError> {
    let listener = TcpListener::bind(addr).await.map_err(|e| {
        RrError::Config(ConfigError::InvalidConfig(format!(
            "failed to bind HTTP observability port {addr}: {e}"
        )))
    })?;
    spawn_http_server_inner(listener, prom_handle, readiness, shutdown_rx).await
}

async fn spawn_http_server_inner(
    listener: TcpListener,
    prom_handle: PrometheusHandle,
    readiness: ReadinessFlag,
    mut shutdown_rx: watch::Receiver<bool>,
) -> Result<tokio::task::JoinHandle<()>, RrError> {
    let addr = listener.local_addr().map_err(|e| {
        RrError::Config(ConfigError::InvalidConfig(format!(
            "failed to get local addr: {e}"
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
        (&hyper::Method::GET, "/ready") => ready_response(&readiness),
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

fn ready_response(readiness: &ReadinessFlag) -> Response<Full<Bytes>> {
    let state = readiness.snapshot();
    let status = if state.ready {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };
    let body = serde_json::to_vec(&state).unwrap_or_else(|_| {
        br#"{"ready":false,"reason":"json_encode_failed","since_unix_secs":0}"#.to_vec()
    });
    Response::builder()
        .status(status)
        .header(
            hyper::header::CONTENT_TYPE,
            "application/json; charset=utf-8",
        )
        .body(Full::new(Bytes::from(body)))
        .expect("valid response")
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

    #[test]
    fn readiness_default_state_is_starting() {
        let r = ReadinessFlag::new();
        let snap = r.snapshot();
        assert!(!snap.ready);
        assert_eq!(snap.reason, "starting");
    }

    #[test]
    fn readiness_set_true_updates_reason_to_ready() {
        let r = ReadinessFlag::new();
        r.set(true);
        let snap = r.snapshot();
        assert!(snap.ready);
        assert_eq!(snap.reason, "ready");
    }

    #[test]
    fn readiness_set_false_updates_reason_to_shutting_down() {
        let r = ReadinessFlag::new();
        r.set(true);
        r.set(false);
        let snap = r.snapshot();
        assert!(!snap.ready);
        assert_eq!(snap.reason, "shutting_down");
    }

    #[test]
    fn readiness_set_with_reason_carries_custom_reason() {
        let r = ReadinessFlag::new();
        r.set_with_reason(false, "degraded:lost_quorum");
        let snap = r.snapshot();
        assert!(!snap.ready);
        assert_eq!(snap.reason, "degraded:lost_quorum");
    }

    #[test]
    fn readiness_snapshot_serializes_to_documented_json_shape() {
        let r = ReadinessFlag::new();
        r.set_with_reason(true, "ready");
        let snap = r.snapshot();
        let json = serde_json::to_value(&snap).unwrap();
        assert_eq!(json["ready"], serde_json::Value::Bool(true));
        assert_eq!(json["reason"], "ready");
        assert!(json.get("since_unix_secs").is_some());
    }

    #[test]
    fn ready_response_returns_503_when_not_ready() {
        let r = ReadinessFlag::new();
        let resp = ready_response(&r);
        assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(
            resp.headers()
                .get(hyper::header::CONTENT_TYPE)
                .unwrap()
                .to_str()
                .unwrap(),
            "application/json; charset=utf-8"
        );
    }

    #[test]
    fn ready_response_returns_200_when_ready() {
        let r = ReadinessFlag::new();
        r.set(true);
        let resp = ready_response(&r);
        assert_eq!(resp.status(), StatusCode::OK);
    }

    use std::time::Duration;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpStream;

    async fn http_get(port: u16, path: &str) -> (u16, String, String) {
        let mut stream = TcpStream::connect(format!("127.0.0.1:{port}"))
            .await
            .unwrap();
        let request =
            format!("GET {path} HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\nConnection: close\r\n\r\n");
        stream.write_all(request.as_bytes()).await.unwrap();

        let mut header_buf = Vec::new();
        let mut byte = [0u8; 1];
        loop {
            let n = stream.read(&mut byte).await.unwrap();
            assert_eq!(n, 1);
            header_buf.push(byte[0]);
            let len = header_buf.len();
            if len >= 4 && &header_buf[len - 4..] == b"\r\n\r\n" {
                break;
            }
        }

        let head = String::from_utf8_lossy(&header_buf);
        let status_line = head.lines().next().unwrap_or("");
        let status_code: u16 = status_line
            .split_whitespace()
            .nth(1)
            .unwrap_or("0")
            .parse()
            .unwrap_or(0);

        let content_length = head
            .lines()
            .find_map(|line| {
                let lower = line.to_lowercase();
                lower
                    .strip_prefix("content-length:")
                    .and_then(|v| v.trim().parse::<usize>().ok())
            })
            .unwrap_or(0);

        let mut body_buf = vec![0u8; content_length];
        if content_length > 0 {
            stream.read_exact(&mut body_buf).await.unwrap();
        }

        (
            status_code,
            head.to_string(),
            String::from_utf8_lossy(&body_buf).to_string(),
        )
    }

    fn test_prom_handle() -> PrometheusHandle {
        let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
        let handle = recorder.handle();
        let _ = metrics::set_global_recorder(recorder);
        handle
    }

    #[tokio::test]
    async fn http_health_returns_200_ok() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();

        let readiness = ReadinessFlag::new();
        let (shutdown_tx, shutdown_rx) = watch::channel(false);

        let _handle = spawn_http_server_inner(listener, test_prom_handle(), readiness, shutdown_rx)
            .await
            .unwrap();

        let (status, _headers, body) = http_get(port, "/health").await;
        assert_eq!(status, 200);
        assert_eq!(body, "ok\n");

        let _ = shutdown_tx.send(true);
    }

    #[tokio::test]
    async fn http_ready_not_ready_returns_503() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();

        let readiness = ReadinessFlag::new();
        let (shutdown_tx, shutdown_rx) = watch::channel(false);

        let _handle = spawn_http_server_inner(listener, test_prom_handle(), readiness, shutdown_rx)
            .await
            .unwrap();

        let (status, _headers, body) = http_get(port, "/ready").await;
        assert_eq!(status, 503);

        let json: serde_json::Value = serde_json::from_str(&body).unwrap();
        assert_eq!(json["ready"], serde_json::Value::Bool(false));

        let _ = shutdown_tx.send(true);
    }

    #[tokio::test]
    async fn http_ready_when_ready_returns_200() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();

        let readiness = ReadinessFlag::new();
        readiness.set(true);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);

        let _handle = spawn_http_server_inner(listener, test_prom_handle(), readiness, shutdown_rx)
            .await
            .unwrap();

        let (status, _headers, body) = http_get(port, "/ready").await;
        assert_eq!(status, 200);

        let json: serde_json::Value = serde_json::from_str(&body).unwrap();
        assert_eq!(json["ready"], serde_json::Value::Bool(true));

        let _ = shutdown_tx.send(true);
    }

    #[tokio::test]
    async fn http_ready_body_has_correct_json_shape() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();

        let readiness = ReadinessFlag::new();
        let (shutdown_tx, shutdown_rx) = watch::channel(false);

        let _handle = spawn_http_server_inner(listener, test_prom_handle(), readiness, shutdown_rx)
            .await
            .unwrap();

        let (_status, _headers, body) = http_get(port, "/ready").await;
        let json: serde_json::Value = serde_json::from_str(&body).unwrap();

        assert!(json.get("ready").is_some());
        assert!(json.get("reason").is_some());
        assert!(json.get("since_unix_secs").is_some());

        let _ = shutdown_tx.send(true);
    }

    #[tokio::test]
    async fn http_metrics_returns_200_with_text_plain() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();

        let readiness = ReadinessFlag::new();
        let (shutdown_tx, shutdown_rx) = watch::channel(false);

        let _handle = spawn_http_server_inner(listener, test_prom_handle(), readiness, shutdown_rx)
            .await
            .unwrap();

        let (status, headers, _body) = http_get(port, "/metrics").await;
        assert_eq!(status, 200);
        assert!(
            headers.to_lowercase().contains("content-type: text/plain"),
            "expected text/plain content-type, got headers:\n{headers}"
        );

        let _ = shutdown_tx.send(true);
    }

    #[tokio::test]
    async fn http_unknown_path_returns_404() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();

        let readiness = ReadinessFlag::new();
        let (shutdown_tx, shutdown_rx) = watch::channel(false);

        let _handle = spawn_http_server_inner(listener, test_prom_handle(), readiness, shutdown_rx)
            .await
            .unwrap();

        let (status, _headers, body) = http_get(port, "/xyz").await;
        assert_eq!(status, 404);
        assert_eq!(body, "not found\n");

        let _ = shutdown_tx.send(true);
    }

    #[tokio::test]
    async fn http_server_stops_on_shutdown() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();

        let readiness = ReadinessFlag::new();
        let (shutdown_tx, shutdown_rx) = watch::channel(false);

        let handle = spawn_http_server_inner(listener, test_prom_handle(), readiness, shutdown_rx)
            .await
            .unwrap();

        let (status, _, _) = http_get(port, "/health").await;
        assert_eq!(status, 200);

        let _ = shutdown_tx.send(true);

        let result = tokio::time::timeout(Duration::from_secs(2), handle).await;
        assert!(result.is_ok(), "server should stop within 2 seconds");
    }
}
