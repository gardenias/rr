import type { GroupItem, NodeDetail, NodeItem, ReplicaEvidence } from "@/lib/contract";
import { formatNumber } from "@/lib/format";

export type ResourceScope = "process" | "cgroup" | "host";

export interface NodeResourceSample {
  cpu_usage_ratio?: number | null;
  cpu_cores?: number | null;
  cpu_limit_cores?: number | null;
  memory_used_bytes?: number | null;
  memory_total_bytes?: number | null;
  memory_limit_bytes?: number | null;
  network_rx_bytes_per_sec?: number | null;
  network_tx_bytes_per_sec?: number | null;
  filesystem_used_bytes?: number | null;
  filesystem_total_bytes?: number | null;
  scope?: ResourceScope | null;
  cpu_scope?: ResourceScope | null;
  memory_scope?: ResourceScope | null;
  network_scope?: ResourceScope | null;
  filesystem_scope?: ResourceScope | null;
  sampled_at?: string | null;
}

export interface ProtocolVersion {
  major: number;
  minor: number;
  min_peer_minor?: number;
}

export interface VersionCompatibility {
  status: "COMPATIBLE" | "INCOMPATIBLE" | "UNKNOWN";
  reason?: string | null;
  incompatible_node_ids?: number[];
}

export interface NodeBuild {
  app_version?: string | null;
  rr_version?: string | null;
  git_commit?: string | null;
  dirty?: boolean | null;
  profile?: string | null;
  config_fingerprint?: string | null;
  boot_id?: string | null;
  process_start_time?: string | null;
  admin_protocol?: ProtocolVersion | null;
  raft_protocol?: ProtocolVersion | null;
  compatibility?: VersionCompatibility | null;
}

export interface FeatureNode extends NodeItem {
  boot_id?: string | null;
  region?: string | null;
  rack?: string | null;
  ready?: boolean | null;
  liveness?: boolean | null;
  process_start_time?: string | null;
  resources?: NodeResourceSample | null;
  build?: NodeBuild | null;
  config?: Record<string, unknown> | null;
}

export interface FeatureNodeDetail extends NodeDetail {
  process_start_time?: string | null;
  ready_phase?: string | null;
  resources?: NodeResourceSample | null;
  build?: NodeBuild | null;
  config?: Record<string, unknown> | null;
}

export interface NodeGroupObservation extends ReplicaEvidence {
  group_id: number;
  is_leader: boolean;
  current_term: number | null;
  last_log_index: number | null;
  leader_node_id: number | null;
  observed_at: string;
}

export interface NodeGroup extends GroupItem {
  observations?: NodeGroupObservation[];
}

export interface NodeDetailData {
  node: FeatureNode & FeatureNodeDetail;
  groups: NodeGroup[];
}

export interface NodeFilters {
  filter?: string;
  status?: string;
  zone?: string;
  version?: string;
  role?: string;
}

export function nodeVersion(node: FeatureNode): string | null {
  return node.build?.app_version ?? node.version ?? null;
}

export function filterNodes(nodes: FeatureNode[], filters: NodeFilters): FeatureNode[] {
  const text = filters.filter?.trim().toLowerCase();
  return nodes.filter((node) => {
    if (filters.status && node.status !== filters.status) return false;
    if (filters.zone && node.zone !== filters.zone) return false;
    if (filters.version && nodeVersion(node) !== filters.version) return false;
    if (filters.role === "leader" && !(node.leader_group_count != null && node.leader_group_count > 0)) return false;
    if (filters.role === "follower" && !(node.leader_group_count === 0 && node.group_ids.length > 0)) return false;
    if (filters.role === "learner") return false;
    if (!text) return true;
    return [
      `node ${node.node_id}`,
      node.admin_addr,
      node.listen_addr,
      node.observability_addr,
      node.zone,
      node.region,
      node.rack,
      nodeVersion(node),
      node.build?.rr_version,
      node.build?.git_commit,
      node.boot_id,
    ].some((value) => value?.toLowerCase().includes(text));
  });
}

export function safeConfigValue(value: unknown): string {
  return typeof value === "string" || typeof value === "number" || typeof value === "boolean"
    ? String(value)
    : "Unsupported value shape";
}

export function formatRatio(value: number | null | undefined): string {
  return value == null || !Number.isFinite(value) ? "—" : `${formatNumber(value * 100)}%`;
}
