import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { makeMeta } from "@/test/fixtures";

const mocks = vi.hoisted(() => ({
  resource: vi.fn(),
  setFilter: vi.fn(),
}));

vi.mock("@/lib/use-console-resource", () => ({ useConsoleResource: mocks.resource }));
vi.mock("@/lib/use-route-query-state", () => ({
  useRouteQueryState: () => ({ state: { window: "15m", filters: { view: "nodes" } }, setFilter: mocks.setFilter }),
}));

import { TopologyView } from "@/components/console/topology/topology-view";

describe("TopologyView", () => {
  beforeEach(() => {
    mocks.resource.mockReturnValue({
      data: { view: "nodes", nodes: [{ node_id: 1, status: "ONLINE" }], edges: [] },
      meta: makeMeta({ partial: true }),
      error: null,
      loading: false,
      refresh: vi.fn(),
    });
  });

  it("loads the requested projection and supports keyboard view tabs", async () => {
    const user = userEvent.setup();
    render(<TopologyView />);

    expect(screen.getByRole("heading", { level: 1, name: "Topology" })).toBeVisible();
    expect(screen.getByText("Observation contract connected")).toBeVisible();
    expect(screen.getByText("Partial")).toBeVisible();
    expect(mocks.resource).toHaveBeenCalledWith("/topology", { view: "nodes" });
    await user.click(screen.getByRole("tab", { name: "Nodes" }));
    await user.keyboard("{ArrowRight}");
    expect(mocks.setFilter).toHaveBeenCalledWith("view", "raft");
  });
});
