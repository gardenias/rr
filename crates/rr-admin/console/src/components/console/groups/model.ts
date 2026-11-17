import type { GroupItem, ReplicaEvidence } from "@/lib/contract";

export interface GroupObservation {
  node_id: number;
  group_id?: number;
  is_leader?: boolean;
  current_term?: number | null;
  last_log_index?: number | null;
  leader_node_id?: number | null;
  observed_at?: string | null;
  membership_revision?: string | null;
}

export interface GroupListItem extends GroupItem {
  learners?: number | null;
  max_lag_entries?: number | null;
  max_append_rtt_p99_ms?: number | null;
  snapshot_index?: number | null;
  tps?: number | null;
  observations?: GroupObservation[];
  conflicting_views?: GroupObservation[] | null;
}

export interface GroupDetailItem extends GroupListItem {
  membership_revision?: string | null;
  last_consistent_sample_at?: string | null;
}

export interface ReplicaRow {
  node_id: number;
  server_state: string | null;
  role: ReplicaEvidence["role"];
  term: number | null;
  last_index: number | null;
  commit_index: number | null;
  applied_index: number | null;
  matched_index: number | null;
  lag_entries: number | null;
  heartbeat_age_ms: number | null;
  heartbeat_rtt_p99_ms: number | null;
  append_rtt_p99_ms: number | null;
  snapshot_index: number | null;
  source_node_id: number;
  observed_at: string | null;
  age_ms: number | null;
}

function record(value: unknown): Record<string, unknown> {
  return value != null && typeof value === "object" && !Array.isArray(value)
    ? value as Record<string, unknown>
    : {};
}

function numberValue(value: unknown): number | null {
  return typeof value === "number" && Number.isFinite(value) ? value : null;
}

function stringValue(value: unknown): string | null {
  return typeof value === "string" && value ? value : null;
}

function firstNumber(row: Record<string, unknown>, ...keys: string[]): number | null {
  for (const key of keys) {
    const value = numberValue(row[key]);
    if (value != null) return value;
  }
  return null;
}

function memberIds(value: unknown): number[] {
  return Array.isArray(value)
    ? value.map((member) => {
      if (typeof member === "number") return member;
      return numberValue(record(member).node_id);
    }).filter((id): id is number => id != null)
    : [];
}

export function normalizeReplicaRows(input: unknown): ReplicaRow[] {
  const payload = record(input);
  const candidates = Array.isArray(payload.items)
    ? payload.items
    : Array.isArray(payload.replicas)
      ? payload.replicas
      : [];
  return candidates.flatMap((candidate) => {
    const row = record(candidate);
    const nodeId = firstNumber(row, "node_id");
    if (nodeId == null) return [];
    const source = record(row.source);
    const requestedRole = stringValue(row.role)?.toUpperCase();
    const role: ReplicaEvidence["role"] = requestedRole === "LEADER" || requestedRole === "FOLLOWER" || requestedRole === "LEARNER"
      ? requestedRole
      : row.is_leader === true
        ? "LEADER"
        : memberIds(row.learners).includes(nodeId)
          ? "LEARNER"
          : requestedRole === "UNKNOWN" ? "UNKNOWN" : "FOLLOWER";
    return [{
      node_id: nodeId,
      server_state: stringValue(row.server_state),
      role,
      term: firstNumber(row, "term", "current_term"),
      last_index: firstNumber(row, "last_index", "last_log_index"),
      commit_index: firstNumber(row, "commit_index"),
      applied_index: firstNumber(row, "applied_index"),
      matched_index: firstNumber(row, "matched_index"),
      lag_entries: firstNumber(row, "lag_entries"),
      heartbeat_age_ms: firstNumber(row, "heartbeat_age_ms"),
      heartbeat_rtt_p99_ms: firstNumber(row, "heartbeat_rtt_p99_ms", "heartbeat_rtt_ms"),
      append_rtt_p99_ms: firstNumber(row, "append_rtt_p99_ms", "append_rtt_ms"),
      snapshot_index: firstNumber(row, "snapshot_index", "last_snapshot_index"),
      source_node_id: firstNumber(row, "source_node_id") ?? firstNumber(source, "node_id") ?? nodeId,
      observed_at: stringValue(row.observed_at) ?? stringValue(source.observed_at),
      age_ms: firstNumber(row, "age_ms") ?? firstNumber(source, "age_ms"),
    }];
  });
}
