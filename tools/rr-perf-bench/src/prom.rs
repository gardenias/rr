//! Server-side window capture via Prometheus (rr-prod-perf plan §5.2 item 6).
//!
//! When `--prom-url` is set, a scenario's JSON report embeds a server-side
//! summary over the SAME wall-clock window the client measured — the
//! cross-check that lets a report attribute a client-side spike to a server
//! segment (proposal / apply / replication RTT / lag) instead of guessing
//! (gap G7). The client-side HDRs remain the primary source; this summary is
//! coarse by design (whole-scenario window, histograms aggregated to p50/p99).
//!
//! Queries are instant queries at the window end with `[<window>s]` ranges
//! against the raw histogram/counter families (the `deploy/observability`
//! recording rules stay untouched). Prometheus is queried over HTTP via
//! `curl` — present in the perf image and on every dev host — so the crate
//! gains no HTTP dependency (same subprocess pattern as `cmd_stdout`).
//!
//! Capture failures NEVER fail a run: every field degrades to `None` and the
//! first error lands in `note` (a bench result without Prom is still a valid
//! result; Tier-1 clusters may have no Prometheus at all).

use std::process::Command;
use std::time::Duration;

use serde::Serialize;

/// Server-side rollup of one measurement window. Units: rates in ops/s,
/// latencies in microseconds, lag in log entries. A field is `null`/absent
/// when its query returned no data (idle metric, short window) or failed.
#[derive(Serialize, Default, Debug, Clone)]
pub struct ServerSideWindow {
    pub window_start_unix_secs: u64,
    pub window_end_unix_secs: u64,
    /// sum(rate(rr_server_request_duration_seconds_count[win]))
    #[serde(skip_serializing_if = "Option::is_none")]
    pub server_request_rate: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub server_p50_us: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub server_p99_us: Option<f64>,
    /// Client-write path: quorum replication + apply.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub proposal_p50_us: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub proposal_p99_us: Option<f64>,
    /// State-machine apply segment (incl. idempotency layer).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub apply_p50_us: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub apply_p99_us: Option<f64>,
    /// Leader→follower AppendEntries RTT (kind="append" only; heartbeats
    /// excluded — they would dominate the histogram at low write rates).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub append_rtt_p50_us: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub append_rtt_p99_us: Option<f64>,
    /// Max follower lag in log entries observed anywhere in the window.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub replication_lag_max_entries: Option<f64>,
    /// Ingestor path (W6): Kafka-ingested commands committed per second
    /// (rr_ingestor_commands_committed_total).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ingestor_batch_commit_rate: Option<f64>,
    /// First capture error, if any (Prometheus unreachable, curl missing…).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub note: Option<String>,
}

/// One fixed query: report field + PromQL template (`{win}` = window range).
const QUERIES: &[(&str, &str)] = &[
    (
        "server_request_rate",
        "sum(rate(rr_server_request_duration_seconds_count[{win}]))",
    ),
    (
        "server_p50_us",
        "histogram_quantile(0.50, sum by (le) (increase(rr_server_request_duration_seconds_bucket[{win}])))",
    ),
    (
        "server_p99_us",
        "histogram_quantile(0.99, sum by (le) (increase(rr_server_request_duration_seconds_bucket[{win}])))",
    ),
    (
        "proposal_p50_us",
        "histogram_quantile(0.50, sum by (le) (increase(rr_proposal_duration_seconds_bucket[{win}])))",
    ),
    (
        "proposal_p99_us",
        "histogram_quantile(0.99, sum by (le) (increase(rr_proposal_duration_seconds_bucket[{win}])))",
    ),
    (
        "apply_p50_us",
        "histogram_quantile(0.50, sum by (le) (increase(rr_raft_apply_duration_seconds_bucket[{win}])))",
    ),
    (
        "apply_p99_us",
        "histogram_quantile(0.99, sum by (le) (increase(rr_raft_apply_duration_seconds_bucket[{win}])))",
    ),
    (
        "append_rtt_p50_us",
        "histogram_quantile(0.50, sum by (le) (increase(rr_raft_append_entries_rtt_seconds_bucket{kind=\"append\"}[{win}])))",
    ),
    (
        "append_rtt_p99_us",
        "histogram_quantile(0.99, sum by (le) (increase(rr_raft_append_entries_rtt_seconds_bucket{kind=\"append\"}[{win}])))",
    ),
    (
        "replication_lag_max_entries",
        "max(max_over_time(rr_raft_replication_lag[{win}]))",
    ),
    (
        "ingestor_batch_commit_rate",
        "sum(rate(rr_ingestor_commands_committed_total[{win}]))",
    ),
];

/// Capture the server-side window `[start_unix, end_unix]` from the
/// Prometheus at `prom_url` (base URL, e.g. `http://prometheus:9090`).
pub fn capture(prom_url: &str, start_unix: u64, end_unix: u64) -> ServerSideWindow {
    let mut out = ServerSideWindow {
        window_start_unix_secs: start_unix,
        window_end_unix_secs: end_unix,
        ..Default::default()
    };
    if end_unix <= start_unix {
        out.note = Some("empty window (end <= start)".to_string());
        return out;
    }
    let win = format!("{}s", end_unix - start_unix);
    for (field, template) in QUERIES {
        let query = template.replace("{win}", &win);
        match query_scalar(prom_url, &query, end_unix) {
            Ok(value) => set_field(&mut out, field, value),
            Err(e) => {
                if out.note.is_none() {
                    out.note = Some(format!("{field}: {e}"));
                }
            }
        }
    }
    out
}

fn set_field(w: &mut ServerSideWindow, field: &str, value: Option<f64>) {
    // Latency fields are seconds from histogram_quantile → microseconds.
    let scaled = value.map(|v| if field.ends_with("_us") { v * 1e6 } else { v });
    match field {
        "server_request_rate" => w.server_request_rate = scaled,
        "server_p50_us" => w.server_p50_us = scaled,
        "server_p99_us" => w.server_p99_us = scaled,
        "proposal_p50_us" => w.proposal_p50_us = scaled,
        "proposal_p99_us" => w.proposal_p99_us = scaled,
        "apply_p50_us" => w.apply_p50_us = scaled,
        "apply_p99_us" => w.apply_p99_us = scaled,
        "append_rtt_p50_us" => w.append_rtt_p50_us = scaled,
        "append_rtt_p99_us" => w.append_rtt_p99_us = scaled,
        "replication_lag_max_entries" => w.replication_lag_max_entries = scaled,
        "ingestor_batch_commit_rate" => w.ingestor_batch_commit_rate = scaled,
        _ => unreachable!("QUERIES and set_field are kept in sync"),
    }
}

/// One instant query at `time_unix`. Returns `Ok(None)` for an empty vector
/// (no data in the window — not an error).
fn query_scalar(prom_url: &str, query: &str, time_unix: u64) -> Result<Option<f64>, String> {
    let url = format!("{}/api/v1/query", prom_url.trim_end_matches('/'));
    let output = Command::new("curl")
        .args([
            "-fsS",
            "--max-time",
            "10",
            "-G",
            "--data-urlencode",
            &format!("query={query}"),
            "--data-urlencode",
            &format!("time={time_unix}"),
            &url,
        ])
        .output()
        .map_err(|e| format!("curl spawn failed: {e}"))?;
    if !output.status.success() {
        return Err(format!(
            "curl exit {}: {}",
            output.status,
            String::from_utf8_lossy(&output.stderr).trim()
        ));
    }
    parse_vector_scalar(&output.stdout)
}

/// Parse a Prometheus vector response: first element's value, or None for an
/// empty result set. Errors on malformed JSON / non-success status.
fn parse_vector_scalar(body: &[u8]) -> Result<Option<f64>, String> {
    let v: serde_json::Value =
        serde_json::from_slice(body).map_err(|e| format!("bad JSON: {e}"))?;
    if v["status"].as_str() != Some("success") {
        return Err(format!(
            "prometheus status={:?}: {:?}",
            v["status"].as_str(),
            v["error"].as_str().unwrap_or("<no error field>")
        ));
    }
    let result = v["data"]["result"]
        .as_array()
        .ok_or("missing data.result")?;
    let Some(first) = result.first() else {
        return Ok(None);
    };
    let raw = first["value"][1].as_str().ok_or("missing value[1]")?;
    let parsed: f64 = raw.parse().map_err(|e| format!("bad value {raw:?}: {e}"))?;
    if parsed.is_nan() {
        return Ok(None); // histogram_quantile over zero-count buckets → NaN
    }
    Ok(Some(parsed))
}

/// Wall-clock helper: current unix seconds (for scenario window stamping).
pub fn now_unix_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

/// Sleep budget guard so a hung curl can't stall a run (defence in depth on
/// top of curl's own --max-time).
#[allow(dead_code)]
pub const CAPTURE_TIMEOUT: Duration = Duration::from_secs(15);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_vector_scalar() {
        let body = br#"{"status":"success","data":{"resultType":"vector","result":[{"metric":{},"value":[1757500000,"12.34"]}]}}"#;
        assert_eq!(parse_vector_scalar(body).unwrap(), Some(12.34));
    }

    #[test]
    fn empty_result_is_none_not_error() {
        let body = br#"{"status":"success","data":{"resultType":"vector","result":[]}}"#;
        assert_eq!(parse_vector_scalar(body).unwrap(), None);
    }

    #[test]
    fn nan_is_none() {
        let body = br#"{"status":"success","data":{"resultType":"vector","result":[{"metric":{},"value":[1757500000,"NaN"]}]}}"#;
        assert_eq!(parse_vector_scalar(body).unwrap(), None);
    }

    #[test]
    fn prometheus_error_is_an_error() {
        let body = br#"{"status":"error","errorType":"bad_data","error":"invalid parameter"}"#;
        assert!(parse_vector_scalar(body).is_err());
    }

    #[test]
    fn malformed_json_is_an_error() {
        assert!(parse_vector_scalar(b"not json").is_err());
    }
}
