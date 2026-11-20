import { envelope } from "./fixtures.mjs";

const nodes = [
  { node_id: 1, status: "ONLINE", admin_addr: "10.0.0.1:52001", listen_addr: "10.0.0.1:51001", group_ids: [1, 2], leader_group_count: 1, uptime_secs: 7200, last_success: "2026-11-16T08:00:00Z", zone: "zone-a", version: "shop-2.0" },
  { node_id: 2, status: "DEGRADED", admin_addr: "10.0.0.2:52001", listen_addr: "10.0.0.2:51001", group_ids: [1, 2], leader_group_count: 0, uptime_secs: 7100, last_success: "2026-11-16T08:00:00Z", zone: "zone-b", version: "shop-1.0" },
  { node_id: 3, status: "UNREACHABLE", admin_addr: "10.0.0.3:52001", listen_addr: null, group_ids: [], leader_group_count: null, uptime_secs: null, last_success: "2026-11-16T07:55:00Z", zone: "zone-c", version: null },
];

export function overviewSummary({ partial = true } = {}) {
  return envelope("/summary", {
    cluster_name: "SIMULATION · incident cluster",
    environment: "SIMULATION",
    nodes,
    groups: [
      { group_id: 7, formation: "FORMED", health: "LEADERLESS_OBSERVED", leader_node_id: null, current_term: 9, last_log_index: 200, voters_reachable: 1, voters_total: 3, source_count: 2 },
      { group_id: 8, formation: "FORMED", health: "QUORUM_AT_RISK", leader_node_id: 1, current_term: 4, last_log_index: 120, voters_reachable: 1, voters_total: 3, source_count: 2 },
    ],
    nodes_online: 1,
    nodes_total: 3,
    groups_with_leader: 1,
    groups_total: 2,
    quorum_risk_groups: 1,
    active_alerts: 3,
    observation_coverage: 67,
  }, {
    partial,
    sources: [
      { node_id: 1, endpoint_role: "ADMIN", observed_at: "2026-11-16T08:00:00Z", age_ms: 1000, status: "ONLINE" },
      { node_id: 3, endpoint_role: "ADMIN", observed_at: "2026-11-16T07:55:00Z", age_ms: 65000, status: "STALE", error_code: "UPSTREAM_TIMEOUT" },
    ],
    capabilities: [
      { feature: "groups", status: "SUPPORTED" },
      { feature: "resources", status: "UNSUPPORTED", reason: "Node 3 does not expose resource sampling." },
    ],
  });
}

export const overviewPerformance = envelope("/metrics/performance", {
  window: "15m",
  retention_source: "CONSOLE_MEMORY",
  series: [
    { metric: "write_tps", labels: {}, points: [{ timestamp: "2026-11-16T07:59:00Z", value: 110 }, { timestamp: "2026-11-16T08:00:00Z", value: 125 }] },
    { metric: "write_p99_ms", labels: {}, points: [{ timestamp: "2026-11-16T07:59:00Z", value: 20 }, { timestamp: "2026-11-16T08:00:00Z", value: 32 }] },
  ],
  notices: [],
});

export const overviewEvents = envelope("/events", {
  items: [{ event_id: "event-7", severity: "ERROR", object: "group-7", state: "ACTIVE", first_observed_at: "2026-11-16T07:58:00Z", last_observed_at: "2026-11-16T08:00:00Z", evidence: [] }],
  next_cursor: null,
});
