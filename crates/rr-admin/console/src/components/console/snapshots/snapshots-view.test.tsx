import { render, screen } from "@testing-library/react";
import { makeMeta } from "@/test/fixtures";
import type { SnapshotItem } from "@/lib/contract";
import { SnapshotsView } from "@/components/console/snapshots/snapshots-view";

const resourceMock = vi.hoisted(() => vi.fn());
vi.mock("@/lib/use-console-resource", () => ({ useConsoleResource: resourceMock }));
vi.mock("@/lib/use-route-query-state", () => ({
  useRouteQueryState: () => ({
    state: { window: "15m", filters: {} },
    setWindow: vi.fn(),
    setFilter: vi.fn(),
  }),
}));

function snapshot(overrides: Partial<SnapshotItem> = {}): SnapshotItem {
  return {
    group_id: 1,
    node_id: 1,
    snapshot_index: 100,
    snapshot_term: 3,
    last_log_index: 120,
    install_count: 0,
    build_failure: null,
    source_count: 1,
    ...overrides,
  };
}

describe("SnapshotsView", () => {
  it("shows observed snapshot distance and an explicit no-snapshot state", () => {
    resourceMock.mockReturnValue({
      data: { items: [snapshot(), snapshot({ node_id: 2, snapshot_index: null, snapshot_term: null, install_count: null })], next_cursor: null },
      meta: makeMeta(),
      error: null,
      loading: false,
      refresh: vi.fn(),
    });
    render(<SnapshotsView />);

    expect(screen.getByRole("heading", { level: 1, name: "Snapshots & storage" })).toBeVisible();
    expect(screen.getByText("20 entries")).toBeVisible();
    expect(screen.getByText("No snapshot observed yet")).toBeVisible();
    expect(screen.getAllByText("UNSUPPORTED").length).toBeGreaterThan(0);
    expect(screen.queryByText("0%")).not.toBeInTheDocument();
  });

  it("keeps node-only build failures out of group cells", () => {
    resourceMock.mockReturnValue({
      data: { items: [snapshot()], next_cursor: null },
      meta: makeMeta({
        capabilities: [{
          feature: "snapshot_build_failures",
          status: "ERROR",
          reason: "Node 2 reported 3 build failures; the metric has no group label.",
        }],
      }),
      error: null,
      loading: false,
      refresh: vi.fn(),
    });
    render(<SnapshotsView />);

    expect(screen.getByRole("heading", { name: "Node-level build failures" })).toBeVisible();
    expect(screen.getByText(/Node 2 reported 3 build failures/)).toBeVisible();
    const table = screen.getByRole("table", { name: "Snapshots by group and node" });
    expect(table).toHaveTextContent("UNSUPPORTED");
    expect(table).not.toHaveTextContent("3 build failures");
  });

  it("reads the legacy last_snapshot field names without inventing counters", () => {
    const legacy = {
      group_id: 1,
      node_id: 1,
      last_snapshot_index: 100,
      last_snapshot_term: 3,
      last_log_index: 120,
      observed_at: "2026-11-16T08:00:00Z",
    } as unknown as SnapshotItem;
    resourceMock.mockReturnValue({
      data: { items: [legacy], next_cursor: null },
      meta: makeMeta(),
      error: null,
      loading: false,
      refresh: vi.fn(),
    });
    render(<SnapshotsView />);

    const table = screen.getByRole("table", { name: "Snapshots by group and node" });
    expect(table).toHaveTextContent("100 / 3");
    expect(table).toHaveTextContent("20 entries");
    expect(table).not.toHaveTextContent("No snapshot observed yet");
    expect(table).toHaveTextContent("—");
  });

  it("does not invent storage details when only snapshot indexes are available", () => {
    resourceMock.mockReturnValue({
      data: { items: [], next_cursor: null },
      meta: makeMeta({
        capabilities: [{
          feature: "snapshot_storage_metrics",
          status: "UNSUPPORTED",
          reason: "Transfer, per-group disk usage, and task progress are not exposed by these nodes.",
        }],
      }),
      error: null,
      loading: false,
      refresh: vi.fn(),
    });
    render(<SnapshotsView />);

    expect(screen.getByText("No snapshot observations for this filter")).toBeVisible();
    expect(screen.getByText(/Transfer, per-group disk usage, and task progress/)).toBeVisible();
    expect(screen.queryByText(/transferring/i)).not.toBeInTheDocument();
  });
});
