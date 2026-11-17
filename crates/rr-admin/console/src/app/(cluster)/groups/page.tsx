import type { Metadata } from "next";
import { GroupsView } from "@/components/console/groups/groups-view";

export const metadata: Metadata = { title: "Raft groups" };
export default function GroupsPage() {
  return <GroupsView />;
}
