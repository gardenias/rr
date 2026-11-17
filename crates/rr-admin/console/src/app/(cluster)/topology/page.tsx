import type { Metadata } from "next";
import { TopologyView } from "@/components/console/topology/topology-view";

export const metadata: Metadata = { title: "Topology" };
export default function TopologyPage() {
  return <TopologyView />;
}
