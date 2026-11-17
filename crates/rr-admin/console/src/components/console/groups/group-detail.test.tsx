import { render, screen } from "@testing-library/react";
import { GroupEvidenceSummary } from "@/components/console/groups/group-detail";
import type { GroupDetailItem } from "@/components/console/groups/model";

const detail: GroupDetailItem = {
  group_id: 1,
  health: "QUORUM_AT_RISK",
  leader_node_id: 1,
  current_term: 8,
  last_log_index: 120,
  source_count: 2,
  voters_reachable: 1,
  voters_total: 3,
  membership_revision: "19",
  last_consistent_sample_at: "2026-11-16T08:00:00Z",
};

describe("GroupEvidenceSummary", () => {
  it("keeps configured membership, leader observation, and Console quorum perspective separate", () => {
    render(<GroupEvidenceSummary group={detail} />);

    expect(screen.getByText("Configured membership")).toBeVisible();
    expect(screen.getByText("3 voters")).toBeVisible();
    expect(screen.getByText("Console view: 1 / 3 voters reachable")).toBeVisible();
    expect(screen.getByText("Leader observation")).toBeVisible();
    expect(screen.getByText("Node 1 · term 8")).toBeVisible();
    expect(screen.getByText("Membership revision 19")).toBeVisible();
  });

  it("describes syncing as non-atomic evidence rather than a conflict verdict", () => {
    render(<GroupEvidenceSummary group={{ ...detail, health: "SYNCING" }} />);

    expect(screen.getByText(/Different terms or membership revisions/)).toBeVisible();
    expect(screen.queryByText(/Same-term leader conflict/)).not.toBeInTheDocument();
  });
});
