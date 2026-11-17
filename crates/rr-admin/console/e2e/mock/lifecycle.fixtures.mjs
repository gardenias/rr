import { envelope } from "./fixtures.mjs";

const current = [
  { node_id: 1, status: "ONLINE", uptime_secs: 3600, process_start_time: "2026-11-16T07:00:00Z", boot_id: "boot-current", ready_phase: "READY", app_version: "shop-2.0", rr_version: "0.2.0", git_commit: "bbb2222", config_hash: "cfg-b", observed_at: "2026-11-16T08:00:00Z" },
  { node_id: 2, status: "UNREACHABLE", uptime_secs: null, process_start_time: null, boot_id: null, ready_phase: "UNKNOWN", app_version: null, rr_version: null, git_commit: null, config_hash: null, observed_at: "2026-11-16T07:55:00Z" },
];

const restarts = [
  { node_id: 1, boot_id: "boot-current", started_at: "2026-11-16T07:00:00Z", ended_at: null, shutdown_state: "RUNNING", persistence: "VOLATILE", app_version: "shop-2.0", rr_version: "0.2.0", git_commit: "bbb2222", record_source: "LOCAL_JOURNAL" },
  { node_id: 1, boot_id: "boot-previous", started_at: "2026-11-16T05:00:00Z", ended_at: "2026-11-16T06:00:00Z", shutdown_state: "ABNORMAL_OR_UNCERTAIN", persistence: "VOLATILE", app_version: "shop-1.0", rr_version: "0.1.0", git_commit: "aaa1111", record_source: "LOCAL_JOURNAL" },
];

const versions = [
  { ...restarts[0], config_hash: "cfg-b", dirty: true, admin_protocol: { major: 1, minor: 1 }, raft_protocol: { major: 1, minor: 1, min_peer_minor: 1 }, compatibility: { status: "INCOMPATIBLE", reason: "Raft minor does not satisfy node 2 min_peer_minor", incompatible_node_ids: [2] } },
  { ...restarts[1], node_id: 2, config_hash: "cfg-a", dirty: false, admin_protocol: { major: 1, minor: 0 }, raft_protocol: { major: 1, minor: 0, min_peer_minor: 0 }, compatibility: { status: "UNKNOWN", reason: "Node 2 is unreachable" } },
];

export function lifecycleResponse(view, cursor) {
  const items = view === "restarts" ? restarts : view === "versions" ? versions : current;
  return envelope("/lifecycle", { view, items, next_cursor: view === "restarts" && !cursor ? "older" : null }, {
    partial: true,
    capabilities: view === "current" ? [] : [{ feature: "persistent_history", status: "DEGRADED", reason: "Node 1 uses volatile boot history; node 2 is unreachable." }],
  });
}
