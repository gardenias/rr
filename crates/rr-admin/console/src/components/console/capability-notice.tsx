import { CircleAlert } from "lucide-react";
import type { Capability } from "@/lib/contract";

const labels = {
  SUPPORTED: "Supported",
  UNSUPPORTED: "Unsupported",
  ERROR: "Capability error",
  ON_DEMAND: "Available on demand",
  DEGRADED: "Degraded capability",
  PARTIAL: "Partially supported",
  UNTRACKED: "Untracked",
  UNKNOWN: "Capability unknown",
};

export function CapabilityNotice({ capability }: { capability: Capability }) {
  return (
    <aside className="capability-notice" data-status={capability.status.toLowerCase()}>
      <CircleAlert aria-hidden="true" size={16} />
      <div>
        <strong><span>{labels[capability.status]}</span> · {capability.feature.replaceAll("_", " ")}</strong>
        {capability.reason ? <p>{capability.reason}</p> : null}
      </div>
    </aside>
  );
}
