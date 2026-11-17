import { raftReplicaRows, topologyEdges, topologyRows } from "@/components/console/topology/model";

describe("topology payload normalization", () => {
  it("reads the actual legacy nodes, Raft, routes, and edge payloads", () => {
    expect(topologyRows({ view: "nodes", nodes: [{ node_id: 1 }], edges: [] }, "nodes")).toEqual([{ node_id: 1 }]);
    expect(topologyRows({ view: "raft", groups: [{ group_id: 2 }] }, "raft")).toEqual([{ group_id: 2 }]);
    expect(topologyRows({ view: "routes", routes: [{ partition_id: 9 }] }, "routes")).toEqual([{ partition_id: 9 }]);
    expect(topologyEdges({ edges: [{ source_node_id: 1, target_node_id: 2 }] })).toEqual([{ source_node_id: 1, target_node_id: 2 }]);
  });

  it("retains the Task 4 items fallback", () => {
    expect(topologyRows({ view: "nodes", items: [{ node_id: 7 }] }, "nodes")).toEqual([{ node_id: 7 }]);
  });

  it("normalizes legacy group observations and leader direction without inventing replication metrics", () => {
    const rows = raftReplicaRows({
      group_id: 1,
      leader_node_id: 1,
      observations: [
        { node_id: 1, is_leader: true, current_term: 4, last_log_index: 20, observed_at: "2026-11-16T08:00:00Z" },
        { node_id: 2, is_leader: false, current_term: 4, last_log_index: 18, observed_at: "2026-11-16T08:00:01Z" },
      ],
    });

    expect(rows).toHaveLength(2);
    expect(rows[0]).toMatchObject({ node_id: 1, role: "LEADER", term: 4, last_index: 20 });
    expect(rows[1]).toMatchObject({ node_id: 2, role: "FOLLOWER", matched_index: null, lag_entries: null, append_rtt_p99_ms: null });
  });
});
