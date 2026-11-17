import type { Metadata } from "next";
import { SnapshotsView } from "@/components/console/snapshots/snapshots-view";

export const metadata: Metadata = { title: "Snapshots & storage" };
export default function SnapshotsPage() {
  return <SnapshotsView />;
}
