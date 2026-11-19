import { render, screen } from "@testing-library/react";
import { ReplicaMatrix } from "@/components/console/groups/replica-matrix";
import type { ReplicaRow } from "@/components/console/groups/model";

const replicas: ReplicaRow[] = [
  {
    node_id: 1,
    server_state: "Leader",
    role: "LEADER",
    term: 8,
    last_index: 1_200,
    commit_index: 1_190,
    applied_index: 1_180,
    matched_index: 1_200,
    lag_entries: 0,
    heartbeat_age_ms: 1_200,
    heartbeat_rtt_p99_ms: 4,
    append_rtt_p99_ms: 8,
    snapshot_index: 1_100,
    source_node_id: 1,
    observed_at: "2026-11-16T08:00:00Z",
    age_ms: 1_000,
  },
  {
    node_id: 2,
    server_state: "Follower",
    role: "FOLLOWER",
    term: 8,
    last_index: 1_183,
    commit_index: 1_190,
    applied_index: 1_175,
    matched_index: 1_173,
    lag_entries: 17,
    heartbeat_age_ms: null,
    heartbeat_rtt_p99_ms: null,
    append_rtt_p99_ms: 12.5,
    snapshot_index: 1_100,
    source_node_id: 1,
    observed_at: "2026-11-16T08:00:00Z",
    age_ms: 1_000,
  },
  {
    node_id: 3,
    server_state: "Follower",
    role: "FOLLOWER",
    term: 8,
    last_index: 1_170,
    commit_index: null,
    applied_index: null,
    matched_index: 999,
    lag_entries: 0,
    heartbeat_age_ms: null,
    heartbeat_rtt_p99_ms: null,
    append_rtt_p99_ms: null,
    snapshot_index: null,
    source_node_id: 3,
    observed_at: "2026-11-16T08:00:01Z",
    age_ms: 2_000,
  },
];

describe("ReplicaMatrix", () => {
  it("keeps leader-only matched and lag evidence attached to its source", () => {
    render(<ReplicaMatrix replicas={replicas} leaderNodeId={1} />);

    expect(screen.getByRole("table", { name: "Group replica evidence" })).toBeVisible();
    expect(screen.getByText("1,173")).toBeVisible();
    expect(screen.getByText("17")).toBeVisible();
    expect(screen.getAllByText("Non-leader source; no data")).toHaveLength(2);
    expect(screen.queryByText("999")).not.toBeInTheDocument();
    expect(screen.getByText("12.5 ms")).toBeVisible();
    expect(screen.getByText("1.2s")).toBeVisible();
  });

  it("uses an unavailable marker for absent RTT and snapshot evidence", () => {
    render(<ReplicaMatrix replicas={replicas} leaderNodeId={1} connectionId="cluster-a" />);

    expect(screen.getAllByText("—").length).toBeGreaterThanOrEqual(5);
    expect(screen.queryByText("0 ms")).not.toBeInTheDocument();
    expect(screen.getByText("Source node 3 · 2s old")).toBeVisible();
    expect(screen.getByRole("link", { name: "Node 1" })).toHaveAttribute(
      "href",
      "/nodes?window=15m&filter=node-1&connection=cluster-a",
    );
  });
});
