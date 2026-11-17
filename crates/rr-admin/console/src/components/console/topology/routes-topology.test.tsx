import { render, screen } from "@testing-library/react";
import { RoutesTopology } from "@/components/console/topology/routes-topology";

const payload = {
  view: "routes",
  revision: "42",
  routes: [
    { partition_id: 10, group_id: 1, replica_node_ids: [1, 2, 3], leader_node_id: 1, revision: "42", stale: false },
    { partition_id: 11, group_id: 2, replica_node_ids: [2, 3], leader_node_id: null, revision: "41", stale: true },
    { partition_id: 12, group_id: null, replica_node_ids: [], route_not_found: true },
  ],
};

describe("RoutesTopology", () => {
  it("shows expected routes, actual leaders, revisions, and degraded route states", () => {
    render(<RoutesTopology data={payload} timeWindow="15m" />);

    expect(screen.getByRole("table", { name: "Partition and route topology" })).toBeVisible();
    expect(screen.getByText("Stale route")).toBeVisible();
    expect(screen.getByText("Route not found")).toBeVisible();
    expect(screen.getByText("Revision 41")).toBeVisible();
    expect(screen.getByRole("link", { name: "Group 1" })).toHaveAttribute(
      "href",
      "/groups?window=15m&group_id=1",
    );
    expect(screen.getByRole("link", { name: "Partition 10" })).toHaveAttribute(
      "href",
      "/topology?window=15m&view=routes&filter=partition-10",
    );
  });

  it("filters by the related group deep link without dropping missing routes", () => {
    render(<RoutesTopology data={payload} timeWindow="15m" filter="group-2" />);

    expect(screen.getByRole("link", { name: "Partition 11" })).toBeVisible();
    expect(screen.queryByRole("link", { name: "Partition 10" })).not.toBeInTheDocument();
  });
});
