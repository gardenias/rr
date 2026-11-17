import { envelope } from "./fixtures.mjs";

export function performanceEnvelope({ missingHistory = false } = {}) {
  if (missingHistory) {
    return envelope("/metrics/performance", {
      window: "24h",
      retention_source: "NONE",
      series: [],
      notices: [{ feature: "performance_history", status: "UNSUPPORTED", reason: "Prometheus is not configured for this Console." }],
    }, {
      time_window: "24h",
      retention_source: "NONE",
      capabilities: [{ feature: "performance_history", status: "UNSUPPORTED", reason: "Prometheus is not configured for this Console." }],
    });
  }
  return envelope("/metrics/performance", {
    window: "15m",
    retention_source: "CONSOLE_MEMORY",
    series: [
      {
        metric: "rr_server_request_duration_seconds",
        labels: { group_id: "1", quantile: "0.99" },
        points: [{ timestamp: "2026-11-16T08:00:00Z", value: 0.12 }],
      },
      {
        metric: "rr_apply_duration_seconds",
        labels: { group_id: "1", quantile: "0.99" },
        points: [{ timestamp: "2026-11-16T08:00:00Z", value: 0.04 }],
      },
      {
        metric: "rr_write_tps",
        labels: { group_id: "1" },
        points: [
          { timestamp: "2026-11-16T08:00:00Z", value: 42 },
          { timestamp: "2026-11-16T08:00:05Z", value: null },
        ],
      },
    ],
    notices: [{ feature: "UNALIGNED_COHORTS", status: "UNSUPPORTED", reason: "Server, proposal, and apply samples are not aligned." }],
  });
}

export function snapshotsEnvelope() {
  return envelope("/snapshots", {
    items: [
      { group_id: 1, node_id: 1, snapshot_index: 100, snapshot_term: 3, last_log_index: 120, install_count: 0, build_failure: null, source_count: 1 },
      { group_id: 1, node_id: 2, snapshot_index: null, snapshot_term: null, last_log_index: 120, install_count: null, build_failure: null, source_count: 1 },
    ],
    next_cursor: null,
  }, {
    capabilities: [
      { feature: "snapshot_build_failures", status: "ERROR", reason: "Node 2 reported 3 build failures; the metric has no group label." },
      { feature: "snapshot_task_progress", status: "UNSUPPORTED", reason: "Task progress is not exposed by these nodes." },
    ],
  });
}

export function eventsEnvelope({ empty = false } = {}) {
  return envelope("/events", {
    items: empty ? [] : [{
      event_id: "event-node-1-unreachable",
      severity: "ERROR",
      object: "node-1",
      state: "ACTIVE",
      first_observed_at: "2026-11-16T07:00:00Z",
      last_observed_at: "2026-11-16T07:05:00Z",
      evidence: [{ node_id: 1, endpoint_role: "ADMIN", observed_at: "2026-11-16T07:05:00Z", age_ms: 5000, status: "UNREACHABLE", error_code: "CONNECTION_TIMEOUT" }],
    }],
    next_cursor: null,
  }, { partial: empty });
}

export function sourcesEnvelope() {
  return envelope("/sources", {
    startup_mode: "PEERS",
    cluster_name: "SIMULATION",
    topology_revision: "42",
    read_only: true,
    sources: [{
      node_id: 2,
      admin_addr: "127.0.0.1:52002",
      observability_addr: "127.0.0.1:53002",
      last_success: "2026-11-16T07:59:00Z",
      last_failure: "2026-11-16T08:00:00Z",
      circuit_state: "OPEN",
      sanitized_error: "scrape failed token=supersecret",
    }],
  }, {
    sources: [{ node_id: null, endpoint_role: "PROMETHEUS", observed_at: null, age_ms: null, status: "UNMAPPED", error_code: "NODE_ID_MAPPING_AMBIGUOUS" }],
    capabilities: [{ feature: "prometheus_history", status: "DEGRADED", reason: "History is unavailable until every target has one stable node_id label." }],
  });
}
