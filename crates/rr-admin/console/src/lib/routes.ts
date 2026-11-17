import type { ConsoleResourcePath } from "@/lib/contract";

export const clusterRoutes = [
  { key: "overview", href: "/overview", title: "Overview", section: "Cluster", resource: "/summary", description: "Cluster health, pressure, and the evidence that needs attention first." },
  { key: "nodes", href: "/nodes", title: "Nodes", section: "Cluster", resource: "/nodes", description: "Node reachability, resources, versions, and local runtime evidence." },
  { key: "topology", href: "/topology", title: "Topology", section: "Cluster", resource: "/topology", description: "Expected placement, observed Raft relationships, and partition routes." },
  { key: "groups", href: "/groups", title: "Raft groups", section: "Cluster", resource: "/groups", description: "Leadership, membership, replication, and per-replica observations." },
  { key: "performance", href: "/performance", title: "Performance", section: "Observe", resource: "/metrics/performance", description: "Throughput, histogram latency, replication, and backpressure evidence." },
  { key: "snapshots", href: "/snapshots", title: "Snapshots & storage", section: "Observe", resource: "/snapshots", description: "Snapshot indexes, observed transfers, and storage evidence." },
  { key: "lifecycle", href: "/lifecycle", title: "Lifecycle", section: "Observe", resource: "/lifecycle", description: "Boot history, shutdown evidence, and application and Rr versions." },
  { key: "events", href: "/events", title: "Events & alerts", section: "Observe", resource: "/events", description: "Observed changes and Console threshold alerts with their sources." },
  { key: "sources", href: "/sources", title: "Sources", section: "System", resource: "/sources", description: "Discovery, source capabilities, freshness, and collection errors." },
] as const satisfies ReadonlyArray<{
  key: string;
  href: string;
  title: string;
  section: "Cluster" | "Observe" | "System";
  resource: ConsoleResourcePath;
  description: string;
}>;

export type RouteKey = (typeof clusterRoutes)[number]["key"];
export type RouteDefinition = (typeof clusterRoutes)[number];

export const routeByKey = Object.fromEntries(
  clusterRoutes.map((route) => [route.key, route]),
) as Record<RouteKey, RouteDefinition>;
