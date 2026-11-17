import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { GroupTable } from "@/components/console/groups/group-table";
import type { GroupListItem } from "@/components/console/groups/model";

const groups: GroupListItem[] = [
  {
    group_id: 1,
    formation: "FORMED",
    health: "HEALTHY",
    leader_node_id: 1,
    current_term: 8,
    voters_reachable: 2,
    voters_total: 3,
    learners: 1,
    max_lag_entries: 17,
    max_append_rtt_p99_ms: 23.5,
    last_log_index: 1_200,
    snapshot_index: 1_100,
    tps: 42.5,
    source_count: 3,
  },
  {
    group_id: 2,
    formation: "FORMED",
    health: "LEADERLESS_OBSERVED",
    leader_node_id: null,
    current_term: 9,
    voters_reachable: null,
    voters_total: 3,
    last_log_index: null,
    source_count: 2,
  },
];

describe("GroupTable", () => {
  it("shows the complete group evidence columns without turning missing values into zero", () => {
    render(<GroupTable groups={groups} onSelect={vi.fn()} />);

    expect(screen.getByRole("table", { name: "Raft groups" })).toBeVisible();
    expect(screen.getByRole("columnheader", { name: /Max append RTT P99/ })).toBeVisible();
    expect(screen.getByText("23.5 ms")).toBeVisible();
    expect(screen.getByText("42.5/s")).toBeVisible();
    expect(screen.getByText("2 / 3")).toBeVisible();
    expect(screen.getByText("No leader observed")).toBeVisible();
    expect(screen.getByRole("status", { name: "Health: Leaderless observed" })).toBeVisible();
    expect(screen.getAllByText("—").length).toBeGreaterThanOrEqual(4);
  });

  it("opens group evidence from an explicitly labelled control", async () => {
    const user = userEvent.setup();
    const onSelect = vi.fn();
    render(<GroupTable groups={groups} onSelect={onSelect} />);

    await user.click(screen.getByRole("button", { name: "Inspect group 1" }));
    expect(onSelect).toHaveBeenCalledWith(1);
  });
});
