import type { Metadata } from "next";
import { EventsView } from "@/components/console/events/events-view";

export const metadata: Metadata = { title: "Events & alerts" };
export default function EventsPage() {
  return <EventsView />;
}
