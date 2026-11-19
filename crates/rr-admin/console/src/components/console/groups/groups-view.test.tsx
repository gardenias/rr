import { act, fireEvent, render, screen } from "@testing-library/react";
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
const routeState = vi.hoisted(() => ({ connection: "cluster-a", groupId: "1" }));
vi.mock("@/lib/use-route-query-state", () => ({
  useRouteQueryState: () => ({
    state: { window: "15m", connection: routeState.connection, filters: { filter: "1", group_id: routeState.groupId } },
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
    routeState.connection = "cluster-a";
    routeState.groupId = "1";
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
    render(<RefreshProvider connectionId="cluster-a"><GroupsView /></RefreshProvider>);
    await act(async () => {
      await Promise.resolve();
      await Promise.resolve();
    });

    expect(screen.getByRole("table", { name: "Raft groups", hidden: true })).toBeInTheDocument();
    expect(screen.getByText("Partial")).toBeInTheDocument();
    expect(screen.getByRole("dialog", { name: "Group 1 evidence" })).toBeVisible();
    expect(screen.getByRole("table", { name: "Group replica evidence" })).toBeVisible();
    expect(screen.getByText("Membership revision 7")).toBeVisible();
    expect(mocks.group).toHaveBeenCalledWith("cluster-a", 1, expect.any(AbortSignal));
    expect(mocks.replicas).toHaveBeenCalledWith("cluster-a", 1, expect.any(AbortSignal));
  });

  it("resets group cursor history when the connection changes", () => {
    routeState.groupId = "";
    mocks.resource.mockReturnValue({
      data: { items: [item], next_cursor: "groups-page-2" },
      meta: makeMeta(),
      error: null,
      loading: false,
      refresh: vi.fn(),
    });
    const view = render(<RefreshProvider connectionId="cluster-a"><GroupsView /></RefreshProvider>);
    fireEvent.click(screen.getByRole("button", { name: "Next page" }));
    expect(mocks.resource).toHaveBeenLastCalledWith("/groups", expect.objectContaining({ cursor: "groups-page-2" }));

    routeState.connection = "cluster-b";
    view.rerender(<RefreshProvider connectionId="cluster-b"><GroupsView /></RefreshProvider>);
    expect(mocks.resource).toHaveBeenLastCalledWith("/groups", expect.objectContaining({ cursor: null }));
    expect(screen.getByRole("button", { name: "Previous page" })).toBeDisabled();
  });
});
