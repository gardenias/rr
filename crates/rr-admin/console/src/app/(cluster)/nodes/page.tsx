import type { Metadata } from "next";
import { NodesView } from "@/components/console/nodes/nodes-view";

export const metadata: Metadata = { title: "Nodes" };
export default function NodesPage() {
  return <NodesView />;
}
