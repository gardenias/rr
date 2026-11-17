import type { HealthStatus } from "@/lib/contract";
import { normalizeReplicaRows, type ReplicaRow } from "@/components/console/groups/model";

export type TopologyViewKind = "nodes" | "raft" | "routes";
export type TopologyRow = Record<string, unknown>;

export function asRecord(value: unknown): TopologyRow {
  return value != null && typeof value === "object" && !Array.isArray(value)
    ? value as TopologyRow
    : {};
}

export function rowNumber(row: TopologyRow, ...keys: string[]): number | null {
  for (const key of keys) {
    const value = row[key];
    if (typeof value === "number" && Number.isFinite(value)) return value;
  }
  return null;
}

export function rowString(row: TopologyRow, ...keys: string[]): string | null {
  for (const key of keys) {
    const value = row[key];
    if (typeof value === "string" && value) return value;
  }
  return null;
}

export function rowBoolean(row: TopologyRow, ...keys: string[]): boolean | null {
  for (const key of keys) {
    if (typeof row[key] === "boolean") return row[key] as boolean;
  }
  return null;
}

export function rowNumberList(row: TopologyRow, ...keys: string[]): number[] {
  for (const key of keys) {
    const value = row[key];
    if (!Array.isArray(value)) continue;
    return value.flatMap((entry) => {
      if (typeof entry === "number" && Number.isFinite(entry)) return [entry];
      const id = rowNumber(asRecord(entry), "node_id");
      return id == null ? [] : [id];
    });
  }
  return [];
}

export function healthStatus(value: unknown): HealthStatus {
  const statuses: HealthStatus[] = [
    "ONLINE", "DEGRADED", "UNREACHABLE", "UNKNOWN", "HEALTHY", "LEADERLESS_OBSERVED",
    "QUORUM_AT_RISK", "CONFLICTING_VIEWS", "SYNCING", "FORMING", "UNTRACKED", "STALE", "UNSUPPORTED",
  ];
  return statuses.includes(value as HealthStatus) ? value as HealthStatus : "UNKNOWN";
}

export function topologyRows(input: unknown, view: TopologyViewKind): TopologyRow[] {
  const data = asRecord(input);
  const key = view === "nodes" ? "nodes" : view === "raft" ? "groups" : "routes";
  const rows = Array.isArray(data[key]) ? data[key] : Array.isArray(data.items) ? data.items : [];
  return rows.map(asRecord);
}

export function topologyEdges(input: unknown): TopologyRow[] {
  const edges = asRecord(input).edges;
  return Array.isArray(edges) ? edges.map(asRecord) : [];
}

export function raftReplicaRows(input: unknown): ReplicaRow[] {
  const group = asRecord(input);
  const leaderId = rowNumber(group, "leader_node_id", "actual_leader_node_id");
  let candidates: unknown[] = [];
  if (Array.isArray(group.replicas)) candidates = group.replicas;
  else if (Array.isArray(group.observations)) candidates = group.observations;
  else {
    const voters = Array.isArray(group.voters) ? group.voters : [];
    const learners = Array.isArray(group.learners) ? group.learners : [];
    candidates = [
      ...voters.map((replica) => ({ ...asRecord(replica), node_id: typeof replica === "number" ? replica : rowNumber(asRecord(replica), "node_id") })),
      ...learners.map((replica) => ({ ...asRecord(replica), node_id: typeof replica === "number" ? replica : rowNumber(asRecord(replica), "node_id"), role: "LEARNER" })),
    ];
  }
  const withLeaderPredicate = candidates.map((candidate) => {
    const replica = asRecord(candidate);
    const nodeId = rowNumber(replica, "node_id");
    return {
      ...replica,
      is_leader: typeof replica.is_leader === "boolean" ? replica.is_leader : nodeId != null && nodeId === leaderId,
    };
  });
  return normalizeReplicaRows({ items: withLeaderPredicate });
}
