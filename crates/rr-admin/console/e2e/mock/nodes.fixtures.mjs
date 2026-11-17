import { envelope } from "./fixtures.mjs";

const build = {
  app_version: "shop-2.0",
  rr_version: "0.2.0",
  git_commit: "bbb2222",
  dirty: false,
  profile: "release",
  config_fingerprint: "cfg-node-1",
  boot_id: "boot-current",
  process_start_time: "2026-11-16T07:00:00Z",
  admin_protocol: { major: 1, minor: 1 },
  raft_protocol: { major: 1, minor: 1, min_peer_minor: 1 },
};

const resources = {
  cpu_usage_ratio: 0.42,
  cpu_cores: 8,
  cpu_limit_cores: 2,
  memory_used_bytes: 536870912,
  memory_total_bytes: 1073741824,
  memory_limit_bytes: 1073741824,
  network_rx_bytes_per_sec: 2048,
  network_tx_bytes_per_sec: 1024,
  filesystem_used_bytes: 1073741824,
  filesystem_total_bytes: 10737418240,
  scope: "cgroup",
  sampled_at: "2026-11-16T08:00:00Z",
};

export const nodeOne = {
  node_id: 1,
  status: "ONLINE",
  admin_addr: "127.0.0.1:52001",
  listen_addr: "127.0.0.1:51001",
  observability_addr: "127.0.0.1:53001",
  group_ids: [1, 2],
  leader_group_count: 1,
  uptime_secs: 3600,
  last_success: "2026-11-16T08:00:00Z",
  zone: "zone-a",
  region: "local",
  rack: "rack-a",
  version: "shop-2.0",
  ready: true,
  liveness: true,
  boot_id: "boot-current",
  process_start_time: "2026-11-16T07:00:00Z",
  config_fingerprint: "cfg-node-1",
  config: { storage: "rocksdb", listen_addr: "127.0.0.1:51001", admin_addr: "127.0.0.1:52001", admin_token: "must-not-render" },
  resources,
  build,
};

export const nodesPage = envelope("/nodes", {
  items: [
    nodeOne,
    { node_id: 2, status: "DEGRADED", admin_addr: "127.0.0.1:52002", listen_addr: "127.0.0.1:51002", group_ids: [2], leader_group_count: 0, uptime_secs: 3500, last_success: "2026-11-16T08:00:00Z", zone: "zone-b", version: "shop-1.0", resources: null, build: null },
    { node_id: 3, status: "UNREACHABLE", admin_addr: "127.0.0.1:52003", listen_addr: null, group_ids: [], leader_group_count: null, uptime_secs: null, last_success: "2026-11-16T07:55:00Z", zone: "zone-c", version: null, resources: null, build: null },
  ],
  next_cursor: null,
}, {
  capabilities: [{ feature: "resources", status: "DEGRADED", reason: "Nodes 2 and 3 do not provide current resource samples." }],
});

export function nodeDetail(nodeId) {
  if (nodeId !== 1) return envelope(`/nodes/${nodeId}`, { node: nodesPage.data.items[nodeId - 1], groups: [] }, { capabilities: [{ feature: "node_detail", status: "UNSUPPORTED", reason: "This legacy node exposes only summary status." }] });
  return envelope("/nodes/1", {
    node: nodeOne,
    groups: [
      { group_id: 1, health: "HEALTHY", leader_node_id: 1, current_term: 4, last_log_index: 120, source_count: 3, observations: [{ node_id: 1, group_id: 1, is_leader: true, current_term: 4, last_log_index: 120, commit_index: 118, applied_index: 117, matched_index: 120, lag_entries: 0, leader_node_id: 1, observed_at: "2026-11-16T08:00:00Z" }] },
      { group_id: 2, health: "DEGRADED", leader_node_id: 2, current_term: 8, last_log_index: 90, source_count: 2, observations: [{ node_id: 1, group_id: 2, is_leader: false, current_term: 8, last_log_index: 88, commit_index: 90, applied_index: 87, matched_index: null, lag_entries: null, leader_node_id: 2, observed_at: "2026-11-16T08:00:00Z" }] },
    ],
  });
}

export function bootPage(cursor) {
  if (cursor === "page-2") return envelope("/nodes/1/boots", {
    items: [{ node_id: 1, boot_id: "boot-previous", started_at: "2026-11-16T05:00:00Z", ended_at: "2026-11-16T06:00:00Z", shutdown_state: "ABNORMAL_OR_UNCERTAIN", persistence: "VOLATILE", app_version: "shop-1.0", rr_version: "0.1.0", git_commit: "aaa1111" }],
    next_cursor: null,
  });
  return envelope("/nodes/1/boots", {
    items: [{ node_id: 1, boot_id: "boot-current", started_at: "2026-11-16T07:00:00Z", ended_at: null, shutdown_state: "RUNNING", persistence: "VOLATILE", app_version: "shop-2.0", rr_version: "0.2.0", git_commit: "bbb2222" }],
    next_cursor: "page-2",
  });
}

export const nodePerformance = envelope("/metrics/performance", {
  window: "15m",
  retention_source: "CONSOLE_MEMORY",
  series: [{ metric: "rr_node_cpu_usage_ratio", labels: { node_id: "1", scope: "cgroup" }, points: [{ timestamp: "2026-11-16T07:59:00Z", value: 0.4 }, { timestamp: "2026-11-16T08:00:00Z", value: 0.42 }] }],
  notices: [],
});
