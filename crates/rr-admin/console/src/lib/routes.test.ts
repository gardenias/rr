import { clusterRoutes, routeByKey } from "@/lib/routes";

describe("console route contract", () => {
  it("registers exactly the nine P0 views with typed API resources", () => {
    expect(clusterRoutes.map((route) => route.href)).toEqual([
      "/overview",
      "/nodes",
      "/topology",
      "/groups",
      "/performance",
      "/snapshots",
      "/lifecycle",
      "/events",
      "/sources",
    ]);
    expect(clusterRoutes.every((route) => route.resource.startsWith("/"))).toBe(true);
    expect(clusterRoutes.map((route) => route.href)).not.toContain("/actions");
  });

  it("keeps each placeholder tied to its route-specific endpoint", () => {
    expect(routeByKey.performance.resource).toBe("/metrics/performance");
    expect(routeByKey.topology.resource).toBe("/topology");
    expect(routeByKey.lifecycle.resource).toBe("/lifecycle");
  });
});
