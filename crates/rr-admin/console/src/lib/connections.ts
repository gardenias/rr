import { deleteConsoleControl, requestConsoleControl } from "@/lib/api";

export interface SeedPeer {
  node_id: number;
  admin_addr: string;
}

export interface TlsProfile {
  ca: string | null;
  cert: string | null;
  key: string | null;
  domain: string | null;
}

export interface PrometheusProfile {
  url: string;
  cluster: string;
}

export type ConnectionStatus = "CONNECTING" | "CONNECTED" | "DEGRADED" | "UNREACHABLE" | "DISCONNECTED";
export type ConnectionSource = "PROFILE" | "STARTUP";
export type CredentialSource = "MEMORY" | "KEYCHAIN" | "MIXED";

export interface ConnectionSummary {
  id: string;
  name: string;
  environment: string | null;
  admin_seeds: SeedPeer[];
  discovery: string | null;
  cluster_name: string | null;
  admin_tls: TlsProfile;
  discovery_tls: TlsProfile;
  discovery_username: string | null;
  discovery_allow_insecure: boolean;
  prometheus: PrometheusProfile | null;
  poll_interval_secs: number;
  created_at: string;
  updated_at: string;
  observed_cluster_id: string | null;
  last_connected_at: string | null;
  source: ConnectionSource;
  status: ConnectionStatus;
  credentials_configured: boolean;
  credential_source: CredentialSource | null;
  last_error: string | null;
  node_count: number | null;
  group_count: number | null;
}

export interface ConnectionInput {
  name: string;
  environment?: string | null;
  admin_seeds?: SeedPeer[];
  discovery?: string | null;
  cluster_name?: string | null;
  admin_tls?: TlsProfile;
  discovery_tls?: TlsProfile;
  discovery_username?: string | null;
  discovery_allow_insecure?: boolean;
  prometheus?: PrometheusProfile | null;
  poll_interval_secs?: number;
  admin_token?: string | null;
  discovery_password?: string | null;
  remember?: boolean;
}

export interface ConnectionListData {
  items: ConnectionSummary[];
  keychain_available: boolean;
}

export interface ConnectionTestResult {
  tested: boolean;
}

function connectionPath(id: string, suffix = ""): string {
  return `/connections/${encodeURIComponent(id)}${suffix}`;
}

export const connectionsApi = {
  list: (signal?: AbortSignal) =>
    requestConsoleControl<ConnectionListData>("/connections", "GET", undefined, signal),
  get: (id: string, signal?: AbortSignal) =>
    requestConsoleControl<ConnectionSummary>(connectionPath(id), "GET", undefined, signal),
  create: (input: ConnectionInput, signal?: AbortSignal) =>
    requestConsoleControl<ConnectionSummary>("/connections", "POST", input, signal),
  testCandidate: (input: ConnectionInput, existingConnectionId?: string, signal?: AbortSignal) =>
    requestConsoleControl<ConnectionTestResult>("/connections/test", "POST", {
      ...input,
      ...(existingConnectionId ? { existing_connection_id: existingConnectionId } : {}),
    }, signal),
  update: (id: string, input: ConnectionInput, signal?: AbortSignal) =>
    requestConsoleControl<ConnectionSummary>(connectionPath(id), "PATCH", input, signal),
  test: (id: string, signal?: AbortSignal) =>
    requestConsoleControl<ConnectionTestResult>(connectionPath(id, "/test"), "POST", undefined, signal),
  connect: (id: string, signal?: AbortSignal) =>
    requestConsoleControl<ConnectionSummary>(connectionPath(id, "/connect"), "POST", undefined, signal),
  disconnect: (id: string, signal?: AbortSignal) =>
    requestConsoleControl<ConnectionSummary>(connectionPath(id, "/disconnect"), "POST", undefined, signal),
  remove: (id: string, signal?: AbortSignal): Promise<void> =>
    deleteConsoleControl(connectionPath(id), signal),
};
