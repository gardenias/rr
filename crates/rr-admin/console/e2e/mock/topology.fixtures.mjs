import { envelope } from "./fixtures.mjs";

const generatedAt = "2026-11-16T08:00:00Z";
const degradedMeta = {
  generated_at: generatedAt,
  partial: true,
  sources: [{ node_id: 3, endpoint_role: "ADMIN", observed_at: generatedAt, age_ms: 65_000, status: "STALE", error_code: "ADMIN_TIMEOUT" }],
};

export const nodesTopologyResponse = envelope("/topology", {
  view: "nodes",
  revision: "42",
  nodes: [
    { node_id: 1, status: "ONLINE", region: "simulation", zone: "zone-a", rack: "rack-1", version: "0.2.0-simulation", group_ids: [1, 2], resources: { cpu_usage_ratio: 0.42 } },
    { node_id: 2, status: "DEGRADED", region: "simulation", zone: "zone-a", rack: "rack-2", version: "0.2.0-simulation", group_ids: [1, 2] },
    { node_id: 3, status: "UNREACHABLE", region: "simulation", zone: "zone-b", rack: "rack-3", version: null, group_ids: [1, 2] },
  ],
  edges: [{ source_node_id: 1, target_node_id: 2, group_count: 2, observed_at: generatedAt }],
}, degradedMeta);

export const raftTopologyResponse = envelope("/topology", {
  view: "raft",
  revision: "42",
  groups: [
    {
      group_id: 1,
      health: "HEALTHY",
      leader_node_id: 1,
      current_term: 8,
      observations: [
        { node_id: 1, is_leader: true, current_term: 8, last_log_index: 120, observed_at: generatedAt },
        { node_id: 2, is_leader: false, current_term: 8, last_log_index: 118, matched_index: 117, lag_entries: 3, append_rtt_p99_ms: 12, source_node_id: 1, observed_at: generatedAt },
        { node_id: 3, is_leader: false, current_term: 8, last_log_index: 110, observed_at: generatedAt },
      ],
    },
    { group_id: 2, health: "SYNCING", leader_node_id: 2, current_term: 9, observations: [{ node_id: 2, is_leader: true, current_term: 9 }] },
    { group_id: 3, health: "CONFLICTING_VIEWS", leader_node_id: 1, current_term: 9, observations: [{ node_id: 1, is_leader: true, current_term: 9 }] },
    { group_id: 4, health: "LEADERLESS_OBSERVED", leader_node_id: null, current_term: 10, observations: [{ node_id: 2, is_leader: false, current_term: 10 }] },
    {
      group_id: 99,
      health: "DEGRADED",
      leader_node_id: 1,
      current_term: 12,
      observations: Array.from({ length: 41 }, (_, index) => ({ node_id: index + 1, is_leader: index === 0, current_term: 12, last_log_index: 200 - index })),
    },
  ],
}, degradedMeta);

export const routesTopologyResponse = envelope("/topology", {
  view: "routes",
  revision: "42",
  routes: [
    { partition_id: 10, group_id: 1, replica_node_ids: [1, 2, 3], leader_node_id: 1, revision: "42", stale: false, observed_at: generatedAt },
    { partition_id: 11, group_id: 2, replica_node_ids: [2, 3], leader_node_id: null, revision: "41", stale: true },
    { partition_id: 12, group_id: null, replica_node_ids: [], route_not_found: true },
  ],
}, degradedMeta);
