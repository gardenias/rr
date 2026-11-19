import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { NodesTopology } from "@/components/console/topology/nodes-topology";

const payload = {
  view: "nodes",
  nodes: [
    { node_id: 1, status: "ONLINE", zone: "zone-a", rack: "rack-1", version: "0.2.0", group_ids: [1, 2] },
    { node_id: 2, status: "UNREACHABLE", zone: "zone-b", rack: "rack-2", version: "0.2.0", group_ids: [1] },
  ],
  edges: [
    { source_node_id: 1, target_node_id: 2, group_count: 2, observed_at: "2026-11-16T08:00:00Z" },
  ],
};

describe("NodesTopology", () => {
  it("switches between an accessible graph and equivalent node and edge tables with the keyboard", async () => {
    const user = userEvent.setup();
    render(<NodesTopology data={payload} timeWindow="15m" />);

    expect(screen.getByRole("group", { name: "Node topology graph" })).toBeVisible();
    await user.click(screen.getByRole("tab", { name: "Graph" }));
    await user.keyboard("{ArrowRight}");
    expect(screen.getByRole("tab", { name: "Data table" })).toHaveFocus();
    expect(screen.getByRole("table", { name: "Node topology" })).toBeVisible();
    expect(screen.getByRole("table", { name: "Observed AppendEntries relationships" })).toBeVisible();
    expect(screen.getByText("Near 5m AppendEntries · 2 groups")).toBeVisible();
    expect(screen.getByRole("status", { name: "Health: Unreachable" })).toBeVisible();
  });

  it("selects graph nodes with the keyboard and exposes a node deep link", async () => {
    const user = userEvent.setup();
    render(<NodesTopology data={payload} timeWindow="1h" connectionId="cluster-a" />);

    const node = screen.getByRole("button", { name: "Node 1ONLINE · zone-a" });
    node.focus();
    await user.keyboard("{Enter}");
    expect(screen.getByRole("link", { name: "Open Node 1 details" })).toHaveAttribute(
      "href",
      "/nodes?window=1h&filter=node-1&connection=cluster-a",
    );
    expect(screen.getByRole("button", { name: "Zoom in" })).toBeVisible();
    expect(screen.getByRole("button", { name: "Fit topology" })).toBeVisible();
  });

  it("falls back to the equivalent table instead of rendering an unbounded graph", () => {
    const nodes = Array.from({ length: 41 }, (_, index) => ({ node_id: index + 1, status: "ONLINE", zone: "zone-a" }));
    render(<NodesTopology data={{ view: "nodes", nodes, edges: [] }} timeWindow="15m" />);

    expect(screen.getByText(/41 nodes exceed the 40-node graph budget/)).toBeVisible();
    expect(screen.getByRole("table", { name: "Node topology" })).toBeVisible();
    expect(screen.queryByRole("group", { name: "Node topology graph" })).not.toBeInTheDocument();
    expect(screen.getByRole("tab", { name: "Graph" })).toBeDisabled();
  });
});
