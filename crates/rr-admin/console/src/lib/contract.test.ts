import { makeMeta } from "@/test/fixtures";
import type {
  ConsoleEnvelope,
  ReplicaData,
  TopologyData,
} from "@/lib/contract";

describe("BFF-compatible topology and replica contracts", () => {
  it("accepts neutral control-plane metadata without a cluster ID", () => {
    const meta = makeMeta({
      cluster_id: null,
      retention_source: "NONE",
      sources: [],
      capabilities: [],
    });

    expect(meta.cluster_id).toBeNull();
  });

  it("accepts named topology arrays and both replica response shapes", () => {
    const meta = makeMeta();
    const responses = [
      {
        meta,
        data: { view: "nodes", nodes: [], edges: [] },
      } satisfies ConsoleEnvelope<TopologyData>,
      {
        meta,
        data: { view: "raft", groups: [] },
      } satisfies ConsoleEnvelope<TopologyData>,
      {
        meta,
        data: { view: "routes", routes: [] },
      } satisfies ConsoleEnvelope<TopologyData>,
      {
        meta,
        data: {
          group_id: 7,
          replicas: [{
            node_id: 2,
            group_id: 7,
            is_leader: false,
            current_term: 4,
            last_log_index: 99,
            leader_node_id: 1,
            observed_at: "2026-11-16T08:00:00Z",
            voters: [{ node_id: 1, addr: "127.0.0.1:52001" }],
            learners: [],
            last_snapshot_index: 80,
            last_snapshot_term: 3,
          }],
        },
      } satisfies ConsoleEnvelope<ReplicaData>,
      {
        meta,
        data: {
          items: [{
            node_id: 2,
            server_state: "Follower",
            role: "FOLLOWER",
            term: 8,
            last_index: 120,
            commit_index: 119,
            applied_index: 118,
            matched_index: 110,
            lag_entries: 5,
            source_node_id: 1,
          }],
          next_cursor: null,
        },
      } satisfies ConsoleEnvelope<ReplicaData>,
    ];

    expect(responses).toHaveLength(5);
  });
});
