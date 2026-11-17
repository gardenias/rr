import {
  CircleAlert,
  CircleCheck,
  CircleHelp,
  CircleOff,
  RefreshCw,
  TriangleAlert,
} from "lucide-react";
import { Badge } from "@/components/ui/badge";
import type { HealthStatus } from "@/lib/contract";

const labels: Record<HealthStatus, string> = {
  ONLINE: "Online",
  DEGRADED: "Degraded",
  UNREACHABLE: "Unreachable",
  UNKNOWN: "Unknown",
  HEALTHY: "Healthy",
  LEADERLESS_OBSERVED: "Leaderless observed",
  QUORUM_AT_RISK: "Quorum at risk",
  CONFLICTING_VIEWS: "Conflicting views",
  SYNCING: "Syncing",
  FORMING: "Forming",
  UNTRACKED: "Untracked",
  STALE: "Stale",
  UNSUPPORTED: "Unsupported",
};

const tones = {
  ONLINE: "ok",
  HEALTHY: "ok",
  DEGRADED: "accent",
  QUORUM_AT_RISK: "accent",
  SYNCING: "accent",
  FORMING: "system",
  UNTRACKED: "system",
  UNREACHABLE: "bad",
  LEADERLESS_OBSERVED: "bad",
  CONFLICTING_VIEWS: "bad",
  STALE: "bad",
  UNSUPPORTED: "neutral",
  UNKNOWN: "neutral",
} as const;

export function HealthBadge({ status }: { status: HealthStatus }) {
  const Icon = ["ONLINE", "HEALTHY"].includes(status)
    ? CircleCheck
    : ["UNREACHABLE", "LEADERLESS_OBSERVED", "CONFLICTING_VIEWS"].includes(status)
      ? CircleOff
      : ["DEGRADED", "QUORUM_AT_RISK", "STALE"].includes(status)
        ? TriangleAlert
        : status === "SYNCING"
          ? RefreshCw
          : status === "UNKNOWN"
            ? CircleHelp
            : CircleAlert;
  const label = labels[status];
  return (
    <Badge tone={tones[status]} role="status" aria-label={`Health: ${label}`}>
      <Icon aria-hidden="true" size={12} />
      {label}
    </Badge>
  );
}
