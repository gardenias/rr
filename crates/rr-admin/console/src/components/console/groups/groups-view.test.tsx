import { act, render, screen } from "@testing-library/react";
import { RefreshProvider } from "@/lib/refresh-context";
import { makeMeta } from "@/test/fixtures";
import type { GroupDetailItem, GroupListItem } from "@/components/console/groups/model";

const mocks = vi.hoisted(() => ({
  resource: vi.fn(),
  setFilter: vi.fn(),
  group: vi.fn(),
  replicas: vi.fn(),
}));

vi.mock("@/lib/use-console-resource", () => ({
  useConsoleResource: mocks.resource,
}));
vi.mock("@/lib/use-route-query-state", () => ({
  useRouteQueryState: () => ({
    state: { window: "15m", filters: { filter: "1", group_id: "1" } },
    setFilter: mocks.setFilter,
  }),
}));
vi.mock("@/lib/api", async (importOriginal) => {
  const actual = await importOriginal<typeof import("@/lib/api")>();
  return {
    ...actual,
    consoleApi: { ...actual.consoleApi, group: mocks.group, replicas: mocks.replicas },
  };
});

import { GroupsView } from "@/components/console/groups/groups-view";

const item: GroupListItem = {
  group_id: 1,
  health: "HEALTHY",
  leader_node_id: 1,
  current_term: 8,
  last_log_index: 120,
  source_count: 2,
  voters_reachable: 2,
  voters_total: 3,
};
const detail: GroupDetailItem = {
  ...item,
  membership_revision: "7",
  last_consistent_sample_at: "2026-11-16T08:00:00Z",
};

describe("GroupsView", () => {
  beforeEach(() => {
    mocks.resource.mockReturnValue({
      data: { items: [item], next_cursor: null },
      meta: makeMeta({ partial: true }),
      error: null,
      loading: false,
      refresh: vi.fn(),
    });
    mocks.group.mockResolvedValue({ data: detail, meta: makeMeta() });
    mocks.replicas.mockResolvedValue({
      data: {
        group_id: 1,
        replicas: [{
          node_id: 1,
          is_leader: true,
          current_term: 8,
          last_log_index: 120,
          observed_at: "2026-11-16T08:00:00Z",
          last_snapshot_index: 100,
        }],
      },
      meta: makeMeta(),
    });
  });

  it("loads a deep-linked group and normalizes the actual legacy replica payload", async () => {
    render(<RefreshProvider><GroupsView /></RefreshProvider>);
    await act(async () => {
      await Promise.resolve();
      await Promise.resolve();
    });

    expect(screen.getByRole("table", { name: "Raft groups", hidden: true })).toBeInTheDocument();
    expect(screen.getByText("Partial")).toBeInTheDocument();
    expect(screen.getByRole("dialog", { name: "Group 1 evidence" })).toBeVisible();
    expect(screen.getByRole("table", { name: "Group replica evidence" })).toBeVisible();
    expect(screen.getByText("Membership revision 7")).toBeVisible();
    expect(mocks.group).toHaveBeenCalledWith(1, expect.any(AbortSignal));
    expect(mocks.replicas).toHaveBeenCalledWith(1, expect.any(AbortSignal));
  });
});
