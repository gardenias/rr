import type { Metadata } from "next";
import { LifecycleView } from "@/components/console/lifecycle/lifecycle-view";

export const metadata: Metadata = { title: "Lifecycle" };
export default function LifecyclePage() {
  return <LifecycleView />;
}
