import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { RaftTopology } from "@/components/console/topology/raft-topology";

const payload = {
  view: "raft",
  groups: [
    {
      group_id: 1,
      health: "HEALTHY",
      leader_node_id: 1,
      current_term: 8,
      observations: [
        { node_id: 1, is_leader: true, current_term: 8, last_log_index: 120, observed_at: "2026-11-16T08:00:00Z" },
        { node_id: 2, is_leader: false, current_term: 8, last_log_index: 118, matched_index: 117, lag_entries: 3, append_rtt_p99_ms: 12, source_node_id: 1, observed_at: "2026-11-16T08:00:00Z" },
      ],
    },
    {
      group_id: 2,
      health: "SYNCING",
      leader_node_id: 3,
      current_term: 9,
      observations: [{ node_id: 3, is_leader: true, current_term: 9 }],
    },
  ],
};

describe("RaftTopology", () => {
  it("draws only the selected group and exposes replication evidence on a keyboard-selectable edge", async () => {
    const user = userEvent.setup();
    render(<RaftTopology data={payload} selectedGroupId={1} onSelectGroup={vi.fn()} timeWindow="15m" />);

    expect(screen.getByRole("group", { name: "Raft topology graph for group 1" })).toBeVisible();
    expect(screen.queryByRole("group", { name: "Raft topology graph for group 2" })).not.toBeInTheDocument();
    const edge = screen.getByRole("button", { name: "Connection Group 1 replication to Node 2" });
    edge.focus();
    await user.keyboard("{Enter}");
    expect(screen.getByText(/Lag 3 entries/, { selector: "p" })).toBeVisible();
    expect(screen.getByText(/Append RTT P99 12 ms/, { selector: "p" })).toBeVisible();

    await user.click(screen.getByRole("tab", { name: "Data table" }));
    expect(screen.getByRole("table", { name: "Raft topology for group 1" })).toBeVisible();
    expect(screen.getByRole("link", { name: "Inspect group 1" })).toHaveAttribute(
      "href",
      "/groups?window=15m&group_id=1",
    );
  });

  it("uses the large-group table fallback", () => {
    const observations = Array.from({ length: 41 }, (_, index) => ({ node_id: index + 1, is_leader: index === 0 }));
    render(
      <RaftTopology
        data={{ view: "raft", groups: [{ group_id: 7, health: "DEGRADED", leader_node_id: 1, observations }] }}
        selectedGroupId={7}
        onSelectGroup={vi.fn()}
        timeWindow="15m"
      />,
    );

    expect(screen.getByText(/41 replicas exceed the 40-replica graph budget/)).toBeVisible();
    expect(screen.getByRole("table", { name: "Raft topology for group 7" })).toBeVisible();
    expect(screen.queryByRole("group", { name: "Raft topology graph for group 7" })).not.toBeInTheDocument();
  });
});
