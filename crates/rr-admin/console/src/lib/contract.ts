export type HealthStatus =
  | "ONLINE"
  | "DEGRADED"
  | "UNREACHABLE"
  | "UNKNOWN"
  | "HEALTHY"
  | "LEADERLESS_OBSERVED"
  | "QUORUM_AT_RISK"
  | "CONFLICTING_VIEWS"
  | "SYNCING"
  | "FORMING"
  | "UNTRACKED"
  | "STALE"
  | "UNSUPPORTED";

export type SourceStatus = "ONLINE" | "FRESH" | "STALE" | "UNREACHABLE" | "ERROR" | "UNSUPPORTED" | "UNMAPPED" | "UNKNOWN";
export type CapabilityStatus = "SUPPORTED" | "UNSUPPORTED" | "ERROR" | "ON_DEMAND" | "DEGRADED" | "UNKNOWN";

export interface SourceEvidence {
  node_id: number | null;
  endpoint_role: "ADMIN" | "OBSERVABILITY" | "METRICS" | "HEALTH" | "READY" | "PROMETHEUS" | "PROMETHEUS_HISTORY" | "DISCOVERY";
  observed_at: string | null;
  age_ms: number | null;
  status: SourceStatus;
  error_code?: string;
}

export interface Capability {
  feature: string;
  status: CapabilityStatus;
  reason?: string;
}

export interface ConsoleMeta {
  schema_version: string;
  correlation_id: string;
  cluster_id: string;
  generated_at: string;
  partial: boolean;
  time_window: string | null;
  retention_source: "CONSOLE_MEMORY" | "PROMETHEUS" | "NONE" | "UNKNOWN";
  sources: SourceEvidence[];
  capabilities: Capability[];
}

export interface ConsoleEnvelope<T> {
  meta: ConsoleMeta;
  data: T;
}

export interface LoginResponse {
  meta: ConsoleMeta;
  csrf_token: string | null;
  expires_at: string | null;
}

export interface CursorPage<T> {
  items: T[];
  next_cursor: string | null;
}

export interface SummaryData {
  nodes: NodeItem[];
  groups: GroupItem[];
  cluster_name?: string;
  environment?: string;
  nodes_online?: number | null;
  nodes_total?: number | null;
  groups_with_leader?: number | null;
  groups_total?: number | null;
  quorum_risk_groups?: number | null;
  active_alerts?: number | null;
  observation_coverage?: number | null;
}

export interface NodeItem {
  node_id: number;
  status: HealthStatus;
  admin_addr: string | null;
  listen_addr: string | null;
  group_ids: number[];
  leader_group_count: number | null;
  uptime_secs: number | null;
  last_success: string | null;
  observability_addr?: string | null;
  zone?: string | null;
  version?: string | null;
  last_contact?: string | null;
}

export interface NodeDetail extends NodeItem {
  boot_id?: string | null;
  process_started_at?: string | null;
  config_fingerprint?: string | null;
}

export interface BootRecord {
  boot_id: string;
  started_at: string;
  ended_at: string | null;
  shutdown_state: "GRACEFUL" | "ABNORMAL_OR_UNCERTAIN" | "RUNNING" | "UNKNOWN";
  persistence: "PERSISTENT" | "VOLATILE";
  app_version: string | null;
  rr_version: string | null;
  git_commit: string | null;
}

export interface TopologyNode extends NodeItem {
  region?: string | null;
  rack?: string | null;
  resources?: Record<string, unknown> | null;
  build?: Record<string, unknown> | null;
}

export interface TopologyEdge {
  source_node_id: number;
  target_node_id: number;
  group_count?: number | null;
  observed_at?: string | null;
}

export interface TopologyGroup extends GroupItem {
  observations?: Array<BffReplicaEvidence | ReplicaEvidence>;
  replicas?: Array<BffReplicaEvidence | ReplicaEvidence>;
}

export interface PartitionRoute {
  partition_id: number;
  group_id: number;
  replica_node_ids: number[];
  leader_node_id?: number | null;
  actual_leader_node_id?: number | null;
  revision?: string | null;
  stale?: boolean;
  route_not_found?: boolean;
  observed_at?: string | null;
}

interface TopologyCompatibility {
  revision?: string | null;
  items?: unknown[];
}

export type TopologyData = TopologyCompatibility & (
  | { view: "nodes"; nodes: TopologyNode[]; edges: TopologyEdge[] }
  | { view: "raft"; groups: TopologyGroup[] }
  | { view: "routes"; routes: PartitionRoute[] }
);

export interface GroupItem {
  group_id: number;
  health: HealthStatus;
  leader_node_id: number | null;
  current_term: number | null;
  last_log_index: number | null;
  source_count: number;
  formation?: string;
  voters_reachable?: number | null;
  voters_total?: number | null;
}

export interface GroupDetail extends GroupItem {
  membership_revision: string | null;
  last_consistent_sample_at: string | null;
}

export interface ReplicaEvidence {
  node_id: number;
  server_state: string | null;
  role: "LEADER" | "FOLLOWER" | "LEARNER" | "UNKNOWN";
  term: number | null;
  last_index: number | null;
  commit_index: number | null;
  applied_index: number | null;
  matched_index: number | null;
  lag_entries: number | null;
  source_node_id: number;
  group_id?: number;
  is_leader?: boolean;
  current_term?: number | null;
  last_log_index?: number | null;
  leader_node_id?: number | null;
  observed_at?: string | null;
  last_snapshot_index?: number | null;
  last_snapshot_term?: number | null;
}

export interface ReplicaMember {
  node_id: number;
  addr: string;
}

export interface BffReplicaEvidence {
  node_id: number;
  group_id: number;
  is_leader: boolean;
  current_term: number;
  last_log_index: number;
  leader_node_id: number | null;
  observed_at: string;
  voters: ReplicaMember[] | null;
  learners: ReplicaMember[] | null;
  last_snapshot_index: number | null;
  last_snapshot_term: number | null;
}

export interface BffReplicaData {
  group_id: number;
  replicas: BffReplicaEvidence[];
}

export type ReplicaData = CursorPage<ReplicaEvidence> | BffReplicaData;

export interface PerformanceData {
  window: string;
  retention_source: ConsoleMeta["retention_source"];
  series: Array<{
    metric: string;
    labels: Record<string, string>;
    points: Array<{ timestamp: string; value: number | null }>;
  }>;
  notices: Capability[];
}

export interface SnapshotItem {
  group_id: number;
  node_id: number;
  snapshot_index: number | null;
  snapshot_term: number | null;
  last_log_index: number | null;
  install_count: number | null;
  build_failure: null;
  source_count: number;
}

export interface LifecycleData {
  view: "current" | "restarts" | "versions";
  items: Array<NodeDetail | BootRecord>;
  next_cursor: string | null;
}

export interface EventItem {
  event_id: string;
  severity: "INFO" | "WARN" | "ERROR";
  object: string;
  state: "ACTIVE" | "RESOLVED";
  first_observed_at: string;
  last_observed_at: string;
  evidence: SourceEvidence[];
}

export interface SourcesData {
  startup_mode: "PEERS" | "DISCOVERY";
  cluster_name: string;
  topology_revision: string | null;
  read_only: true;
  prometheus: {
    status: "CONFIGURED" | "UNSUPPORTED";
    url: string | null;
    cluster_matcher?: string;
  };
  sources: Array<{
    node_id: number | null;
    admin_addr: string | null;
    observability_addr: string | null;
    last_success: string | null;
    last_failure: string | null;
    circuit_state: "CLOSED" | "OPEN" | "HALF_OPEN" | "UNKNOWN";
    sanitized_error: string | null;
  }>;
}

export interface ConsoleResponseMap {
  "/summary": SummaryData;
  "/nodes": CursorPage<NodeItem>;
  "/topology": TopologyData;
  "/groups": CursorPage<GroupItem>;
  "/metrics/performance": PerformanceData;
  "/snapshots": CursorPage<SnapshotItem>;
  "/lifecycle": LifecycleData;
  "/events": CursorPage<EventItem>;
  "/sources": SourcesData;
}

export type ConsoleResourcePath = keyof ConsoleResponseMap;
export type QueryValue = string | number | boolean | null | undefined;
export type ConsoleQuery = Record<string, QueryValue>;
