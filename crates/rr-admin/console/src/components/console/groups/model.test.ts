import { normalizeReplicaRows } from "@/components/console/groups/model";

describe("normalizeReplicaRows", () => {
  it("accepts the legacy BFF replica envelope and preserves absent optional evidence", () => {
    const rows = normalizeReplicaRows({
      group_id: 7,
      replicas: [
        {
          node_id: 2,
          group_id: 7,
          is_leader: false,
          current_term: 4,
          last_log_index: 99,
          leader_node_id: 1,
          observed_at: "2026-11-16T08:00:00Z",
          voters: [{ node_id: 1 }, { node_id: 2 }],
          learners: [{ node_id: 3 }],
          last_snapshot_index: 80,
          last_snapshot_term: 3,
        },
      ],
    });

    expect(rows).toHaveLength(1);
    expect(rows[0]).toMatchObject({
      node_id: 2,
      role: "FOLLOWER",
      term: 4,
      last_index: 99,
      snapshot_index: 80,
      source_node_id: 2,
      matched_index: null,
      lag_entries: null,
      append_rtt_p99_ms: null,
    });
  });

  it("accepts the Task 4 cursor page and leader-predicated replica fields", () => {
    const rows = normalizeReplicaRows({
      items: [
        {
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
        },
      ],
      next_cursor: null,
    });

    expect(rows[0]).toMatchObject({ matched_index: 110, lag_entries: 5, source_node_id: 1 });
  });
});
