import type { Metadata } from "next";
import { OverviewView } from "@/components/console/overview/overview-view";

export const metadata: Metadata = { title: "Overview" };
export default function OverviewPage() {
  return <OverviewView />;
}
