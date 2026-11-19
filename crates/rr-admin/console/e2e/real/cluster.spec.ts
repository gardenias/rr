import { expect, test, type APIRequestContext, type Page } from "@playwright/test";
import { controlBaseUrl, realViewerToken, type ClusterKey } from "./harness";

interface ConsoleEnvelope<T> {
  data: T;
  meta: { partial: boolean; cluster_id?: string | null };
}

interface ResourceObservation {
  cpu_usage_ratio?: number | null;
  cpu_scope?: string | null;
  cpu_cores?: number | null;
  cpu_cores_scope?: string | null;
  memory_used_bytes?: number | null;
  memory_total_bytes?: number | null;
  memory_scope?: string | null;
  memory_used_scope?: string | null;
  memory_total_scope?: string | null;
}

interface NodeObservation {
  node_id: number;
  status: string;
  resources?: ResourceObservation | null;
  ready?: boolean | null;
  liveness?: boolean | null;
  ready_phase?: string | null;
  boot_id?: string;
  boot?: { current?: { boot_id?: string } | null } | null;
  build?: { boot_id?: string } | null;
}

interface GroupObservation {
  group_id: number;
  leader_node_id: number | null;
  reachable_voters?: number;
  voters_total?: number | null;
}

interface ReplicaObservation {
  node_id: number;
  commit_index: number | null;
  local_commit_index: number | null;
  cluster_commit_index: number | null;
  applied_index: number | null;
  matched_index: number | null;
  lag_entries: number | null;
  source_node_id: number | null;
  local_source_node_id: number | null;
  leader_node_id: number | null;
  leader_view: "LEADER" | "NOT_LEADER_SOURCE" | null;
  leader_replication: Array<{ node_id: number; matched_index: number | null; lag_entries: number | null }> | null;
  observed_at: string | null;
}

interface SummaryData {
  nodes: NodeObservation[];
  groups: GroupObservation[];
}

interface ProtocolVersion {
  major: number;
  minor: number;
  min_peer_minor: number;
}

interface Compatibility {
  status: "COMPATIBLE" | "INCOMPATIBLE" | "UNKNOWN";
  incompatible_node_ids: number[];
  reason?: string | null;
}

interface BootRecord {
  node_id?: number;
  boot_id: string;
  shutdown_state: string;
  persistence?: string;
  app_version?: string;
  rr_version?: string;
  git_commit?: string;
  dirty?: boolean;
  git_dirty?: boolean;
  current?: boolean;
  admin_protocol?: ProtocolVersion;
  raft_protocol?: ProtocolVersion;
  compatibility?: Compatibility;
  raft_incompatible_with?: number[];
}

interface EventRecord {
  event_id?: string;
  id?: string;
  object?: string;
  node_id?: number;
  kind?: string;
}

interface ConnectionSummary {
  id: string;
  name: string;
  status: string;
}

interface RealManifest {
  run_id: string;
  console: {
    args: string[];
    peers: null;
    discovery: null;
    state_dir: string;
    profile_store_path: string;
    profile_store_exists: boolean;
    profile_store_existed_at_start: boolean;
    starts: number;
  };
  clusters: Record<ClusterKey, {
    cluster_name: string;
    formation: "LEGACY_MANUAL_MEMBERSHIP";
    admin_seeds: Array<{ node_id: number; admin_addr: string }>;
    nodes: Array<{ node_id: number; admin_addr: string; observability_addr: string; data_dir: string; boot_journal_dir: string }>;
  }>;
}

const profileNames: Record<ClusterKey, string> = {
  a: "Real Cluster A",
  b: "Real Cluster B",
};
const connectionIds: Partial<Record<ClusterKey, string>> = {};
const connectionClusterIds = new Map<string, string>();
const topologyRequests: string[] = [];
let browserRequests: string[] = [];

function requiredConnectionId(key: ClusterKey) {
  const id = connectionIds[key];
  if (!id) throw new Error(`connection ${key} was not created through the UI`);
  return id;
}

function clusterUrl(path: string, connectionId: string) {
  const [pathname, search = ""] = path.split("?");
  const params = new URLSearchParams(search);
  params.set("connection", connectionId);
  return `${pathname}?${params.toString()}`;
}

function expectConnectionIdentity<T>(connectionId: string, envelope: ConsoleEnvelope<T>, description: string) {
  const expected = connectionClusterIds.get(connectionId);
  expect(expected, `missing expected cluster identity for ${description}`).toBeTruthy();
  expect(envelope.meta.cluster_id, `${description} must use its own real cluster identity`).toBe(expected);
}

async function consoleGet<T>(request: APIRequestContext, connectionId: string, path: string): Promise<ConsoleEnvelope<T>> {
  const scopedPath = `/api/v1/console/connections/${encodeURIComponent(connectionId)}${path}`;
  if (path.startsWith("/topology")) topologyRequests.push(scopedPath);
  const response = await request.get(scopedPath);
  expect(response.ok(), `GET connection ${connectionId} ${path} failed with HTTP ${response.status()}: ${await response.text()}`).toBe(true);
  const envelope = await response.json() as ConsoleEnvelope<T>;
  expectConnectionIdentity(connectionId, envelope, `GET ${path}`);
  return envelope;
}

async function control(request: APIRequestContext, key: ClusterKey, path: string) {
  const response = await request.post(`${controlBaseUrl}/clusters/${key}${path}`, { timeout: 60_000 });
  expect(response.ok(), `control POST cluster ${key} ${path} failed: ${await response.text()}`).toBe(true);
  return await response.json() as { node_id?: number };
}

async function harnessControl(request: APIRequestContext, path: "/console/restart") {
  const response = await request.post(`${controlBaseUrl}${path}`, { timeout: 60_000 });
  expect(response.ok(), `harness control POST ${path} failed: ${await response.text()}`).toBe(true);
  return await response.json();
}

async function realManifest(request: APIRequestContext) {
  const response = await request.get(`${controlBaseUrl}/manifest`);
  expect(response.ok(), `manifest GET failed: ${await response.text()}`).toBe(true);
  return await response.json() as RealManifest;
}

async function login(page: Page) {
  await page.goto("/login/");
  await page.getByLabel("Viewer credential").fill(realViewerToken);
  await page.getByRole("button", { name: "Sign in to Console" }).click();
  await expect(page).toHaveURL(/\/connections\/$/);
}

async function summary(request: APIRequestContext, connectionId: string) {
  return await consoleGet<SummaryData>(request, connectionId, "/summary?window=15m");
}

async function nodeDetail(request: APIRequestContext, connectionId: string, nodeId: number) {
  return await consoleGet<{ node: NodeObservation }>(request, connectionId, `/nodes/${nodeId}`);
}

function hasResourceValues(node: NodeObservation) {
  const resources = node.resources;
  if (!resources) return false;
  return resources.cpu_scope === "process"
    && resources.cpu_usage_ratio != null && resources.cpu_usage_ratio >= 0 && resources.cpu_usage_ratio <= 1
    && resources.cpu_cores_scope === "host" && resources.cpu_cores != null && resources.cpu_cores > 0
    && resources.memory_scope === "host" && resources.memory_used_scope === "host" && resources.memory_total_scope === "host"
    && resources.memory_used_bytes != null && resources.memory_used_bytes > 0
    && resources.memory_total_bytes != null && resources.memory_total_bytes >= resources.memory_used_bytes;
}

function expectProtocolEvidence(record: BootRecord) {
  expect(record.admin_protocol).toEqual({ major: 1, minor: 0, min_peer_minor: 0 });
  expect(record.raft_protocol).toEqual({ major: 1, minor: 0, min_peer_minor: 0 });
  expect(typeof record.dirty).toBe("boolean");
  expect(record.git_dirty).toBe(record.dirty);
  expect(record.compatibility?.status).toBe("COMPATIBLE");
  expect(record.compatibility?.incompatible_node_ids).toEqual([]);
  expect(record.raft_incompatible_with ?? []).toEqual([]);
}

async function instrumentSse(page: Page) {
  page.on("request", (request) => {
    const url = request.url();
    browserRequests.push(url);
    if (new URL(url).pathname.endsWith("/topology")) topologyRequests.push(url);
  });
  await page.addInitScript(() => {
    const state = {
      created: [] as string[],
      closed: [] as string[],
      events: [] as Array<{ url: string; type: string }>,
    };
    Object.defineProperty(window, "__realSse", { value: state });
    const NativeEventSource = window.EventSource;
    window.EventSource = class extends NativeEventSource {
      private wasClosed = false;

      constructor(url: string | URL, init?: EventSourceInit) {
        super(url, init);
        const value = String(url);
        state.created.push(value);
        for (const type of ["summary", "resync"]) {
          this.addEventListener(type, () => state.events.push({ url: value, type }));
        }
      }

      close() {
        if (!this.wasClosed) {
          this.wasClosed = true;
          state.closed.push(this.url);
        }
        super.close();
      }
    };
  });
}

async function sseState(page: Page) {
  return await page.evaluate(() => (window as unknown as {
    __realSse: { created: string[]; closed: string[]; events: Array<{ url: string; type: string }> };
  }).__realSse);
}

function deferred() {
  let release!: () => void;
  const promise = new Promise<void>((resolve) => {
    release = resolve;
  });
  return { promise, release };
}

async function createConnectionThroughUi(page: Page, key: ClusterKey) {
  const manifest = await realManifest(page.request);
  const cluster = manifest.clusters[key];
  await page.goto("/connections/new/");
  await page.getByLabel("Connection name").fill(profileNames[key]);
  await page.getByLabel("Environment").fill(`real-${key}`);
  for (const [index, seed] of cluster.admin_seeds.entries()) {
    if (index > 0) await page.getByRole("button", { name: "Add admin seed" }).click();
    await page.getByLabel(`Seed ${index + 1} node ID`).fill(String(seed.node_id));
    await page.getByLabel(`Seed ${index + 1} AdminService address`).fill(seed.admin_addr);
  }
  await page.getByLabel("Discovery cluster").fill(cluster.cluster_name);
  await page.getByText("Advanced settings", { exact: true }).click();
  await page.getByLabel("Poll interval (seconds)").fill("1");
  await page.getByRole("button", { name: "Test connection" }).click();
  await expect(page.getByText(/Connection test passed/)).toBeVisible({ timeout: 60_000 });

  const createdResponse = page.waitForResponse((response) => {
    const url = new URL(response.url());
    return response.request().method() === "POST" && url.pathname === "/api/v1/console/connections";
  });
  await page.getByRole("button", { name: "Save & connect" }).click();
  const response = await createdResponse;
  expect(response.status(), await response.text()).toBe(201);
  const created = await response.json() as ConsoleEnvelope<ConnectionSummary>;
  connectionIds[key] = created.data.id;
  connectionClusterIds.set(created.data.id, cluster.cluster_name);
  await expect(page).toHaveURL(new RegExp(`/overview/\\?connection=${created.data.id}$`), { timeout: 60_000 });
  await expect(page.getByRole("heading", { level: 1, name: "Overview" })).toBeVisible();
  await waitForHealthySummary(page.request, created.data.id);
  return created.data.id;
}

async function assertGroupEvidence(request: APIRequestContext, connectionId: string) {
  let group!: ConsoleEnvelope<GroupObservation>;
  let replicas!: ConsoleEnvelope<{ replicas: ReplicaObservation[] }>;
  await expect.poll(async () => {
    [group, replicas] = await Promise.all([
      consoleGet<GroupObservation>(request, connectionId, "/groups/1"),
      consoleGet<{ replicas: ReplicaObservation[] }>(request, connectionId, "/groups/1/replicas"),
    ]);
    const leader = replicas.data.replicas.find((replica) => replica.node_id === group.data.leader_node_id);
    return group.data.reachable_voters === 3 && group.data.voters_total === 3
      && replicas.data.replicas.length === 3 && leader?.leader_replication?.length === 3;
  }, { timeout: 60_000 }).toBe(true);
  expect(group.data.reachable_voters).toBe(3);
  expect(group.data.voters_total).toBe(3);
  expect(replicas.data.replicas).toHaveLength(3);
  const leaderId = group.data.leader_node_id!;
  const leader = replicas.data.replicas.find((replica) => replica.node_id === leaderId)!;
  expect(leader.leader_view).toBe("LEADER");
  expect(leader.source_node_id).toBe(leaderId);
  expect(leader.local_source_node_id).toBe(leaderId);
  expect(leader.commit_index).toBeGreaterThan(0);
  expect(leader.local_commit_index).toBeGreaterThan(0);
  expect(leader.cluster_commit_index).toBeGreaterThan(0);
  expect(leader.applied_index).toBeGreaterThan(0);
  expect(leader.leader_replication).toHaveLength(3);
  const selfEvidence = leader.leader_replication!.find((evidence) => evidence.node_id === leaderId)!;
  expect(selfEvidence.matched_index).not.toBeNull();
  expect(selfEvidence.lag_entries).not.toBeNull();
  expect(leader.matched_index).toBe(selfEvidence.matched_index);
  expect(leader.lag_entries).toBe(selfEvidence.lag_entries);
  for (const evidence of leader.leader_replication!) {
    expect(evidence.matched_index).not.toBeNull();
    expect(evidence.lag_entries).not.toBeNull();
  }
  for (const follower of replicas.data.replicas.filter((replica) => replica.node_id !== leaderId)) {
    expect(follower.leader_view).toBe("NOT_LEADER_SOURCE");
    expect(follower.leader_node_id).toBe(leaderId);
    expect(follower.local_source_node_id).toBe(follower.node_id);
    expect(follower.source_node_id).toBe(leaderId);
    expect(follower.commit_index).toBeGreaterThan(0);
    expect(follower.local_commit_index).toBeGreaterThan(0);
    expect(follower.cluster_commit_index).toBeGreaterThan(0);
    expect(follower.applied_index).toBeGreaterThan(0);
    expect(follower.matched_index).not.toBeNull();
    expect(follower.lag_entries).not.toBeNull();
    expect(follower.observed_at).toBe(leader.observed_at);
    expect(follower.leader_replication).toEqual([]);
  }
}

async function assertCompleteHealthyCluster(request: APIRequestContext, connectionId: string) {
  await waitForHealthySummary(request, connectionId);
  const current = await summary(request, connectionId);
  expect(current.data.nodes.map((node) => node.node_id).sort((a, b) => a - b)).toEqual([1, 2, 3]);
  expect(current.data.nodes.every((node) => node.status === "ONLINE")).toBe(true);
  const groups = current.data.groups.filter((group) => group.group_id === 1);
  expect(groups).toHaveLength(1);
  expect(groups[0]!.reachable_voters).toBe(3);
  expect(groups[0]!.voters_total).toBe(3);
  expect([1, 2, 3]).toContain(groups[0]!.leader_node_id);
  await assertGroupEvidence(request, connectionId);
  return current;
}

function isUnreachableForNode(event: EventRecord, nodeId: number) {
  return event.kind === "NODE_UNREACHABLE"
    && (event.node_id === nodeId || event.object === `node-${nodeId}`);
}

async function assertLifecycleEvidence(request: APIRequestContext, connectionId: string) {
  const currentLifecycle = await consoleGet<{ items: BootRecord[] }>(request, connectionId, "/lifecycle?view=current&page_size=50");
  expect(currentLifecycle.data.items).toHaveLength(3);
  for (const record of currentLifecycle.data.items) expectProtocolEvidence(record);
  const lifecycleVersions = await consoleGet<{ items: BootRecord[] }>(request, connectionId, "/lifecycle?view=versions&page_size=50");
  const currentVersions = lifecycleVersions.data.items.filter((record) => record.current);
  expect(currentVersions).toHaveLength(3);
  for (const record of currentVersions) expectProtocolEvidence(record);
}

async function waitForHealthySummary(request: APIRequestContext, connectionId: string) {
  await expect.poll(async () => {
    const current = await summary(request, connectionId);
    return current.data.nodes.map((node) => node.node_id).sort((a, b) => a - b);
  }, { timeout: 60_000 }).toEqual([1, 2, 3]);
  await expect.poll(async () => {
    const current = await summary(request, connectionId);
    return current.data.nodes.every((node) => node.status === "ONLINE") && current.data.groups.some((group) =>
      group.group_id === 1 && group.leader_node_id != null && group.reachable_voters === 3 && group.voters_total === 3,
    );
  }, { timeout: 60_000 }).toBe(true);
}

async function assertResourceFamilies(request: APIRequestContext, key: ClusterKey) {
  const connectionId = requiredConnectionId(key);
  const manifest = await realManifest(request);
  await waitForHealthySummary(request, connectionId);
  const currentSummary = await summary(request, connectionId);
  expect(currentSummary.meta.cluster_id).toBe(manifest.clusters[key].cluster_name);
  expect(currentSummary.meta.cluster_id).toBeTruthy();

  await expect.poll(async () => {
    const nodes = await consoleGet<{ items: NodeObservation[] }>(request, connectionId, "/nodes?page_size=50");
    return nodes.data.items.length === 3 && nodes.data.items.every(hasResourceValues);
  }, { timeout: 60_000 }).toBe(true);
  await expect.poll(async () => {
    const details = await Promise.all([1, 2, 3].map((id) => nodeDetail(request, connectionId, id)));
    return details.every(({ data }) => data.node.ready === true && data.node.liveness === true && data.node.ready_phase === "ready");
  }, { timeout: 60_000 }).toBe(true);
  const boots = await consoleGet<{ items: BootRecord[] }>(request, connectionId, "/nodes/1/boots?page_size=50");
  expect(boots.data.items.length).toBeGreaterThan(0);
  expect(boots.data.items[0]!.persistence).toBe("PERSISTENT");

  const groups = await consoleGet<{ items: GroupObservation[] }>(request, connectionId, "/groups?page_size=50");
  expect(groups.data.items.map((group) => group.group_id)).toContain(1);
  await assertGroupEvidence(request, connectionId);
  const topology = await consoleGet<Record<string, unknown>>(request, connectionId, "/topology?window=15m");
  expect(topology.data).toBeTruthy();
  expect(topology.meta.cluster_id).toBe(currentSummary.meta.cluster_id);

  await expect.poll(async () => {
    const snapshots = await consoleGet<{ items: Array<{ node_id: number; snapshot_index?: number | null; last_snapshot_index?: number | null }> }>(request, connectionId, "/snapshots?page_size=50");
    return snapshots.data.items.some((item) => (item.snapshot_index ?? item.last_snapshot_index ?? 0) > 0);
  }, { timeout: 60_000 }).toBe(true);
  await expect.poll(async () => {
    const performance = await consoleGet<{ series: unknown[] }>(request, connectionId, "/metrics/performance?window=15m");
    return performance.data.series.length;
  }, { timeout: 60_000 }).toBeGreaterThan(0);
  await assertLifecycleEvidence(request, connectionId);
  const events = await consoleGet<{ items: EventRecord[] }>(request, connectionId, "/events?page_size=50");
  expect(Array.isArray(events.data.items)).toBe(true);
  expect(events.meta.cluster_id).toBe(currentSummary.meta.cluster_id);
  const sources = await consoleGet<{
    mode?: string;
    sources: Array<{ node_id: number | null; admin_addr: string | null; observability_addr: string | null }>;
  }>(request, connectionId, "/sources");
  expect(sources.data.mode).toBe("EXPLICIT_ADMIN_SEEDS");
  const expectedNodes = manifest.clusters[key].nodes;
  const otherNodes = manifest.clusters[key === "a" ? "b" : "a"].nodes;
  expect(sources.data.sources).toHaveLength(3);
  expect(sources.data.sources.map((source) => source.admin_addr).sort()).toEqual(expectedNodes.map((node) => node.admin_addr).sort());
  expect(sources.data.sources.map((source) => source.observability_addr).sort()).toEqual(expectedNodes.map((node) => node.observability_addr).sort());
  const otherAddresses = new Set(otherNodes.flatMap((node) => [node.admin_addr, node.observability_addr]));
  for (const source of sources.data.sources) {
    expect(source.node_id).not.toBeNull();
    expect(otherAddresses.has(source.admin_addr!)).toBe(false);
    expect(otherAddresses.has(source.observability_addr!)).toBe(false);
  }
  return currentSummary;
}

function captureBrowserErrors(page: Page) {
  const errors: string[] = [];
  page.on("pageerror", (error) => errors.push(error.message));
  page.on("console", (message) => {
    if (message.type() === "error") errors.push(message.text());
  });
  return errors;
}

async function assertNineViews(page: Page, key: ClusterKey) {
  const connectionId = requiredConnectionId(key);
  const manifest = await realManifest(page.request);
  const clusterName = manifest.clusters[key].cluster_name;
  await page.goto(clusterUrl("/overview/", connectionId));
  await expect(page.getByRole("heading", { level: 1, name: "Overview" })).toBeVisible();
  await expect(page.locator(".cluster-identity strong")).toHaveText(clusterName);
  await expect(page.getByRole("table", { name: "Cluster Raft group summary" })).toContainText("Group 1");
  await expect(page.getByText("3 / 3", { exact: true }).first()).toBeVisible();
  await expect(page.getByText("Console data unavailable")).toHaveCount(0);

  await page.getByRole("link", { name: /Nodes online/ }).click();
  await expect(page).toHaveURL(/\/nodes\//);
  expect(new URL(page.url()).searchParams.get("connection")).toBe(connectionId);
  await expect(page.getByRole("heading", { level: 1, name: "Nodes" })).toBeVisible();
  await page.getByRole("button", { name: "Open node 1 details" }).click();
  const drawer = page.getByRole("dialog", { name: "Node 1" });
  await expect(drawer).toBeVisible();
  await expect(drawer.getByText("127.0.0.1:", { exact: false }).first()).toBeVisible();
  await drawer.getByRole("tab", { name: "Resources" }).click();
  await expect(drawer.getByText(/CPU|Memory/).first()).toBeVisible();
  await drawer.getByRole("tab", { name: "Restarts & versions" }).click();
  await expect(drawer.getByRole("table", { name: "Node 1 boot history" })).toContainText("PERSISTENT");
  await page.keyboard.press("Escape");

  await page.goto(clusterUrl("/overview/", connectionId));
  await page.getByRole("link", { name: "Group 1", exact: true }).click();
  await expect(page).toHaveURL(/\/groups\//);
  expect(new URL(page.url()).searchParams.get("group_id")).toBe("1");
  expect(new URL(page.url()).searchParams.get("connection")).toBe(connectionId);
  await expect(page.getByRole("dialog", { name: "Group 1 evidence" })).toBeVisible();
  await expect(page.getByRole("table", { name: "Group replica evidence" })).toContainText("Node 1");
  await page.getByRole("link", { name: "Group snapshots" }).click();
  await expect(page.getByRole("heading", { level: 1, name: "Snapshots & storage" })).toBeVisible();
  expect(new URL(page.url()).searchParams.get("connection")).toBe(connectionId);
  await expect(page.getByRole("table", { name: "Snapshots by group and node" })).toContainText("UNSUPPORTED");
  await expect(page.getByText("0%", { exact: true })).toHaveCount(0);

  await page.goto(clusterUrl("/topology/?view=nodes", connectionId));
  await expect(page.getByRole("heading", { level: 1, name: "Topology" })).toBeVisible();
  await expect(page.getByRole("group", { name: "Node topology graph" })).toBeVisible();
  await page.getByRole("tab", { name: "Data table", exact: true }).click();
  await expect(page.getByRole("table", { name: "Node topology" })).toContainText("Node 3");
  await expect(page.getByRole("table", { name: "Observed AppendEntries relationships" })).toBeVisible();
  await page.getByRole("tab", { name: "Raft", exact: true }).click();
  await expect(page.getByRole("combobox", { name: "Raft group" })).toHaveValue("1");
  await page.getByRole("tab", { name: "Partitions & routes" }).click();
  await expect(page.getByRole("table", { name: "Partition and route topology" })).toContainText("1");

  await page.goto(clusterUrl("/performance/", connectionId));
  await expect(page.getByRole("heading", { level: 1, name: "Performance" })).toBeVisible();
  await expect(page.getByRole("table", { name: "Performance hotspot observations" })).toBeVisible();
  await expect(page.getByText(/Console memory retention/).first()).toBeVisible();

  await page.goto(clusterUrl("/lifecycle/", connectionId));
  await expect(page.getByRole("heading", { level: 1, name: "Lifecycle" })).toBeVisible();
  await expect(page.getByRole("table", { name: "Current node lifecycle" })).toContainText("Node 3");
  await page.getByRole("tab", { name: "Versions" }).click();
  await expect(page.getByRole("table", { name: "Version history by boot" })).toContainText("console-e2e");

  await page.goto(clusterUrl("/events/", connectionId));
  await expect(page.getByRole("heading", { level: 1, name: "Events & alerts" })).toBeVisible();
  await expect(page.getByText(/observed events|observation coverage/i).first()).toBeVisible();

  await page.goto(clusterUrl("/sources/", connectionId));
  await expect(page.getByRole("heading", { level: 1, name: "Sources" })).toBeVisible();
  await expect(page.getByText("EXPLICIT_ADMIN_SEEDS", { exact: true })).toBeVisible();
  const sourceTable = page.getByRole("table", { name: "Node source health" });
  await expect(sourceTable).toContainText(manifest.clusters[key].admin_seeds[0]!.admin_addr);
  await expect(sourceTable).toContainText("Node 3");
  await expect(sourceTable).toContainText("127.0.0.1:");
  await page.getByRole("button", { name: "Reconcile now" }).click();
  await expect(page.getByText(/Reconciliation completed/)).toBeVisible();

  if (key === "a") {
    for (const width of [1440, 1280]) {
      await page.setViewportSize({ width, height: width === 1440 ? 900 : 800 });
      for (const route of ["overview", "nodes", "topology", "groups", "performance", "snapshots", "lifecycle", "events", "sources"]) {
        await page.goto(clusterUrl(`/${route}/`, connectionId));
        await expect(page.locator("h1")).toHaveCount(1);
        expect(await page.evaluate(() => document.documentElement.scrollWidth > window.innerWidth), `${route} page overflow at ${width}px`).toBe(false);
      }
    }
  }
}

test.describe.serial("real dual-cluster no-source Console", () => {
  test.beforeEach(async ({ page }) => {
    browserRequests = [];
    await instrumentSse(page);
    await login(page);
  });

  test.afterEach(async () => {
    const legacyRequests = browserRequests.filter((value) => {
      const path = new URL(value).pathname;
      return /^\/api\/v1\/console\/(summary|nodes|groups|topology|metrics|snapshots|lifecycle|events|sources|stream)/.test(path);
    });
    expect(legacyRequests, "the new UI must never use legacy unscoped data routes").toEqual([]);
  });

  test("starts empty without cluster sources and creates both real connections through the UI", async ({ page }) => {
    const before = await realManifest(page.request);
    expect(before.console.peers).toBeNull();
    expect(before.console.discovery).toBeNull();
    expect(before.console.profile_store_existed_at_start).toBe(false);
    expect(before.console.profile_store_path).toBe(`${before.console.state_dir}/connections.json`);
    expect(before.console.state_dir).toContain(before.run_id);
    expect(before.console.args).toContain("--keychain-disable");
    expect(before.console.args).not.toContain("--peers");
    expect(before.console.args).not.toContain("--discovery");
    expect(before.console.args).not.toContain("--poll-interval-secs");
    expect(before.clusters.a.cluster_name).not.toBe(before.clusters.b.cluster_name);
    expect(before.clusters.a.formation).toBe("LEGACY_MANUAL_MEMBERSHIP");
    expect(before.clusters.b.formation).toBe("LEGACY_MANUAL_MEMBERSHIP");
    expect(before.clusters.a.nodes.map((node) => node.node_id)).toEqual(before.clusters.b.nodes.map((node) => node.node_id));
    expect(before.clusters.a.admin_seeds.map((seed) => seed.admin_addr)).not.toEqual(before.clusters.b.admin_seeds.map((seed) => seed.admin_addr));

    await page.goto("/");
    await expect(page).toHaveURL(/\/connections\/$/);
    await expect(page.getByRole("heading", { name: "No connections yet" })).toBeVisible();
    const list = await page.request.get("/api/v1/console/connections");
    expect(list.ok()).toBe(true);
    const initialConnections = await list.json();
    expect(initialConnections.data.items).toEqual([]);
    expect(initialConnections.data.keychain_available).toBe(false);
    const legacy = await page.request.get("/api/v1/console/summary?window=15m");
    expect(legacy.status()).toBe(400);
    expect((await legacy.json()).error.code).toBe("CONNECTION_REQUIRED");

    const idA = await createConnectionThroughUi(page, "a");
    const idB = await createConnectionThroughUi(page, "b");
    expect(idA).not.toBe(idB);
    const after = await realManifest(page.request);
    expect(after.console.profile_store_exists).toBe(true);
    const summaryA = await summary(page.request, idA);
    const summaryB = await summary(page.request, idB);
    expect(summaryA.meta.cluster_id).toBeTruthy();
    expect(summaryB.meta.cluster_id).toBeTruthy();
    expect(summaryA.meta.cluster_id).not.toBe(summaryB.meta.cluster_id);
    expect(summaryA.meta.cluster_id).toBe(after.clusters.a.cluster_name);
    expect(summaryB.meta.cluster_id).toBe(after.clusters.b.cluster_name);
    expect(summaryA.data.nodes.map((node) => node.node_id)).toEqual(summaryB.data.nodes.map((node) => node.node_id));
    expect(summaryA.data.groups.map((group) => group.group_id)).toEqual(summaryB.data.groups.map((group) => group.group_id));
  });

  test("cold node topology collects leader replication evidence on its first request", async ({ page }) => {
    const idA = requiredConnectionId("a");
    expect(topologyRequests, "cold topology must precede every direct and browser topology request").toEqual([]);
    const firstResponse = page.waitForResponse((response) =>
      new URL(response.url()).pathname === `/api/v1/console/connections/${idA}/topology`,
    );
    await page.goto(clusterUrl("/topology/?view=nodes", idA));
    const response = await firstResponse;
    expect(response.ok()).toBe(true);
    expect(topologyRequests).toHaveLength(1);
    const topology = await response.json() as ConsoleEnvelope<{
      view: string;
      nodes: Array<{ node_id: number }>;
      edges: Array<{ source_node_id: number; target_node_id: number; group_count?: number | null; observed_at?: string | null }>;
    }>;
    expectConnectionIdentity(idA, topology, "cold topology response");
    expect(topology.data.view).toBe("nodes");
    expect(topology.data.nodes.map((node) => node.node_id).sort((a, b) => a - b)).toEqual([1, 2, 3]);
    expect(topology.data.edges.length).toBeGreaterThan(0);
    for (const edge of topology.data.edges) {
      expect([1, 2, 3]).toContain(edge.source_node_id);
      expect([1, 2, 3]).toContain(edge.target_node_id);
      expect(edge.source_node_id).not.toBe(edge.target_node_id);
      expect(edge.group_count).toBeGreaterThan(0);
      expect(edge.observed_at).toBeTruthy();
    }
    await page.getByRole("tab", { name: "Data table", exact: true }).click();
    const relationships = page.getByRole("table", { name: "Observed AppendEntries relationships" });
    await expect(relationships).toBeVisible();
    await expect(relationships.locator("tbody tr")).toHaveCount(topology.data.edges.length);
    await expect(relationships).toContainText("Near 5m AppendEntries");
    await assertGroupEvidence(page.request, idA);
  });

  test("keeps every resource family and all nine views scoped to the requested real cluster", async ({ page }) => {
    const errors = captureBrowserErrors(page);
    const summaryA = await assertResourceFamilies(page.request, "a");
    const summaryB = await assertResourceFamilies(page.request, "b");
    expect(summaryA.meta.cluster_id).not.toBe(summaryB.meta.cluster_id);
    await assertNineViews(page, "a");
    await assertNineViews(page, "b");
    expect(errors).toEqual([]);
  });

  test("cluster A leader failure and restart do not change cluster B evidence", async ({ page }) => {
    const errors = captureBrowserErrors(page);
    const idA = requiredConnectionId("a");
    const idB = requiredConnectionId("b");
    await waitForHealthySummary(page.request, idA);
    const beforeB = await assertCompleteHealthyCluster(page.request, idB);
    await assertGroupEvidence(page.request, idA);
    await assertLifecycleEvidence(page.request, idA);
    const before = await summary(page.request, idA);
    expect(before.data.groups.find((group) => group.group_id === 1)?.leader_node_id).not.toBeNull();
    const oldBootIds = new Map(before.data.nodes.map((node) => [
      node.node_id,
      node.boot?.current?.boot_id ?? node.boot_id ?? node.build?.boot_id,
    ]));

    const killed = await control(page.request, "a", "/leader/kill");
    const oldLeader = killed.node_id!;
    expect(oldLeader).toBeGreaterThan(0);
    const oldBootId = oldBootIds.get(oldLeader);
    expect(oldBootId).toBeTruthy();
    await expect.poll(async () => {
      const current = await summary(page.request, idA);
      const group = current.data.groups.find((candidate) => candidate.group_id === 1);
      const leader = group?.leader_node_id;
      const killedNode = current.data.nodes.find((node) => node.node_id === oldLeader);
      return current.meta.partial && leader != null && leader !== oldLeader && group?.reachable_voters === 2 && group.voters_total === 3 && ["STALE", "UNREACHABLE"].includes(killedNode?.status ?? "");
    }, { timeout: 60_000 }).toBe(true);

    let eventsA!: ConsoleEnvelope<{ items: EventRecord[] }>;
    await expect.poll(async () => {
      eventsA = await consoleGet<{ items: EventRecord[] }>(page.request, idA, "/events?state=ACTIVE&page_size=50");
      return eventsA.data.items.some((event) => isUnreachableForNode(event, oldLeader));
    }, { timeout: 30_000 }).toBe(true);
    expect(eventsA.data.items.length, "A must provide a non-empty real event candidate set").toBeGreaterThan(0);
    const eventsB = await consoleGet<{ items: EventRecord[] }>(page.request, idB, "/events?page_size=50");
    expect(eventsB.data.items.some((event) => isUnreachableForNode(event, oldLeader))).toBe(false);
    const currentB = await assertCompleteHealthyCluster(page.request, idB);
    expect(currentB.meta.cluster_id).toBe(beforeB.meta.cluster_id);

    await page.goto(clusterUrl("/overview/", idA));
    await expect(page.getByText("Partial", { exact: true }).first()).toBeVisible();
    await expect(page.getByText(/Unreachable|Stale/).first()).toBeVisible();
    await expect(page.getByRole("table", { name: "Cluster Raft group summary" })).not.toContainText(`Node ${oldLeader}`);
    await page.goto(clusterUrl("/events/?state=ACTIVE", idA));
    await expect(page.getByRole("table", { name: "Observed events and alerts" })).toContainText("NODE_UNREACHABLE");

    await control(page.request, "a", `/nodes/${oldLeader}/restart`);
    await waitForHealthySummary(page.request, idA);
    await waitForHealthySummary(page.request, idB);

    const activeEvents = await consoleGet<{ items: EventRecord[] }>(page.request, idA, "/events?state=ACTIVE");
    expect(activeEvents.data.items.some((event) => isUnreachableForNode(event, oldLeader))).toBe(false);
    const resolvedEvents = await consoleGet<{ items: EventRecord[] }>(page.request, idA, "/events?state=RESOLVED");
    expect(resolvedEvents.data.items.some((event) => isUnreachableForNode(event, oldLeader))).toBe(true);
    await expect.poll(async () => {
      const events = await consoleGet<{ items: Array<{ kind: string; node_id?: number | null }> }>(page.request, idA, "/events?state=RESOLVED");
      return events.data.items.some((event) => event.kind === "NODE_RESTARTED" && event.node_id === oldLeader);
    }, { timeout: 30_000 }).toBe(true);

    await expect.poll(async () => {
      const lifecycle = await consoleGet<{ items: BootRecord[] }>(page.request, idA, "/lifecycle?view=restarts&page_size=50");
      const boots = lifecycle.data.items.filter((boot) => boot.node_id === oldLeader);
      const oldBoot = boots.find((boot) => boot.boot_id === oldBootId);
      return boots.length >= 2 && oldBoot?.shutdown_state === "ABNORMAL_OR_UNCERTAIN" && boots.some((boot) => boot.boot_id !== oldBootId && boot.shutdown_state === "RUNNING" && boot.persistence === "PERSISTENT");
    }, { timeout: 60_000 }).toBe(true);

    const versions = await consoleGet<{ items: BootRecord[] }>(page.request, idA, "/lifecycle?view=versions&page_size=50");
    const killedNodeVersions = versions.data.items.filter((boot) => boot.node_id === oldLeader);
    expect(killedNodeVersions.length).toBeGreaterThanOrEqual(2);
    expect(killedNodeVersions.every((boot) => boot.app_version === "console-e2e" && Boolean(boot.rr_version) && Boolean(boot.git_commit))).toBe(true);
    const currentVersion = killedNodeVersions.find((boot) => boot.current)!;
    expectProtocolEvidence(currentVersion);
    const historicVersions = killedNodeVersions.filter((boot) => !boot.current);
    expect(historicVersions.length).toBeGreaterThanOrEqual(1);
    for (const record of historicVersions) {
      expect(record.admin_protocol).toEqual({ major: 1, minor: 0, min_peer_minor: 0 });
      expect(record.raft_protocol).toEqual({ major: 1, minor: 0, min_peer_minor: 0 });
      expect(typeof record.dirty).toBe("boolean");
      expect(record.compatibility?.status).toBe("UNKNOWN");
      expect(record.compatibility?.incompatible_node_ids).toEqual([]);
    }

    await page.goto(clusterUrl("/lifecycle/?view=restarts", idA));
    await expect(page.getByRole("table", { name: "Restart timeline data" })).toContainText(oldBootId!);
    await expect(page.getByText("Abnormal or uncertain").first()).toBeVisible();
    await expect(page.getByText("PERSISTENT").first()).toBeVisible();

    const degradedFollower = before.data.nodes.find((node) => node.node_id !== oldLeader)!.node_id;
    await control(page.request, "a", `/nodes/${degradedFollower}/readiness/degraded`);
    await expect.poll(async () => {
      const detail = await nodeDetail(page.request, idA, degradedFollower);
      return detail.data.node.status === "DEGRADED" && detail.data.node.ready === false && detail.data.node.liveness === true && detail.data.node.ready_phase === "degraded";
    }, { timeout: 30_000 }).toBe(true);
    await control(page.request, "a", `/nodes/${degradedFollower}/readiness/restore`);
    await expect.poll(async () => {
      const detail = await nodeDetail(page.request, idA, degradedFollower);
      return detail.data.node.status === "ONLINE" && detail.data.node.ready === true && detail.data.node.liveness === true && detail.data.node.ready_phase === "ready";
    }, { timeout: 30_000 }).toBe(true);
    const finalB = await assertCompleteHealthyCluster(page.request, idB);
    expect(finalB.meta.cluster_id).toBe(beforeB.meta.cluster_id);
    expect(errors).toEqual([]);
  });

  test("restarts the no-source Console without restoring runtimes, then reconnects both profiles through the UI", async ({ page }) => {
    const errors = captureBrowserErrors(page);
    const idA = requiredConnectionId("a");
    await waitForHealthySummary(page.request, idA);
    await page.goto(clusterUrl("/overview/", idA));
    await expect(page.getByText("Data plane live")).toBeVisible();
    await expect(page.getByRole("table", { name: "Cluster Raft group summary" })).toContainText("Group 1");
    expect(errors).toEqual([]);

    await harnessControl(page.request, "/console/restart");
    await expect(page.getByText("Reconnecting")).toBeVisible();
    await expect(page).toHaveURL(/\/login\/\?next=/, { timeout: 30_000 });
    await login(page);
    for (const key of ["a", "b"] as const) {
      const item = page.getByRole("article", { name: `${profileNames[key]} connection` });
      await expect(item.getByText("Disconnected", { exact: true })).toBeVisible();
    }
    expect((await realManifest(page.request)).console.starts).toBe(2);
    for (const key of ["a", "b"] as const) {
      const item = page.getByRole("article", { name: `${profileNames[key]} connection` });
      await item.getByRole("button", { name: `Connect ${profileNames[key]}` }).click();
      await expect(item.getByRole("link", { name: `Open ${profileNames[key]}` })).toBeVisible({ timeout: 60_000 });
    }
    await page.getByRole("article", { name: `${profileNames.a} connection` }).getByRole("link", { name: `Open ${profileNames.a}` }).click();
    await expect(page.getByText("Data plane live")).toBeVisible({ timeout: 30_000 });
    await expect(page.getByRole("table", { name: "Cluster Raft group summary" })).toContainText("Group 1");
    await expect.poll(async () => (await sseState(page)).events.some((event) => event.type === "summary" && event.url.includes(`/connections/${idA}/`))).toBe(true);
    const expectedRestartResourceError = /^Failed to load resource: (net::ERR_INCOMPLETE_CHUNKED_ENCODING|net::ERR_CONNECTION_REFUSED|the server responded with a status of 401 \(Unauthorized\))$/;
    expect(errors.filter((error) => !expectedRestartResourceError.test(error))).toEqual([]);
  });

  test("switches without old data, preserves refresh and bookmarks, and isolates two Pages", async ({ page, context }) => {
    const idA = requiredConnectionId("a");
    const idB = requiredConnectionId("b");
    const manifest = await realManifest(page.request);
    const nameA = manifest.clusters.a.cluster_name;
    const nameB = manifest.clusters.b.cluster_name;
    await page.goto(clusterUrl("/nodes/?window=1h&node_id=1&filter=old", idA));
    await expect(page.locator(".cluster-identity strong")).toHaveText(nameA);
    await expect(page.getByText("Data plane live")).toBeVisible();
    await page.getByRole("combobox", { name: "Active connection" }).selectOption(idB);
    await expect(page).toHaveURL(/\/nodes\//);
    await expect.poll(() => new URL(page.url()).searchParams.get("connection")).toBe(idB);
    expect(new URL(page.url()).searchParams.get("window")).toBe("1h");
    expect(new URL(page.url()).searchParams.get("node_id")).toBeNull();
    expect(new URL(page.url()).searchParams.get("filter")).toBeNull();
    await expect(page.locator(".cluster-identity strong")).toHaveText(nameB);
    await expect(page.locator(".cluster-identity strong")).not.toHaveText(nameA);
    await page.reload();
    await expect(page.locator(".cluster-identity strong")).toHaveText(nameB);
    expect(new URL(page.url()).searchParams.get("connection")).toBe(idB);

    const pageB = await context.newPage();
    await instrumentSse(pageB);
    await pageB.goto(clusterUrl("/overview/?window=15m", idB));
    await expect(pageB.locator(".cluster-identity strong")).toHaveText(nameB);
    await page.goto(clusterUrl("/overview/?window=15m", idA));
    await expect(page.locator(".cluster-identity strong")).toHaveText(nameA);
    await expect(page.getByText("Data plane live")).toBeVisible();
    await expect(pageB.getByText("Data plane live")).toBeVisible();
    expect(new URL(page.url()).searchParams.get("connection")).toBe(idA);
    expect(new URL(pageB.url()).searchParams.get("connection")).toBe(idB);
    await expect.poll(async () => (await sseState(page)).events.some((event) => event.type === "summary" && event.url.includes(`/connections/${idA}/`))).toBe(true);
    await expect.poll(async () => (await sseState(pageB)).events.some((event) => event.type === "summary" && event.url.includes(`/connections/${idB}/`))).toBe(true);
    expect((await sseState(page)).events.every((event) => event.url.includes(`/connections/${idA}/`))).toBe(true);
    expect((await sseState(pageB)).events.every((event) => event.url.includes(`/connections/${idB}/`))).toBe(true);
    await pageB.reload();
    await expect(pageB.locator(".cluster-identity strong")).toHaveText(nameB);
    expect(new URL(pageB.url()).searchParams.get("connection")).toBe(idB);
    await pageB.close();
  });

  test("late real A responses never render after switching to B and the old SSE closes", async ({ page }) => {
    const idA = requiredConnectionId("a");
    const idB = requiredConnectionId("b");
    const manifest = await realManifest(page.request);
    const nameB = manifest.clusters.b.cluster_name;
    const markerA = manifest.clusters.a.admin_seeds[0]!.admin_addr;
    const markerB = manifest.clusters.b.admin_seeds[0]!.admin_addr;
    const delayedA = deferred();
    const delayedAHandled = deferred();
    const delayedB = deferred();
    let holdA = false;
    let holdB = true;

    await page.route(`**/api/v1/console/connections/${idA}/sources`, async (route) => {
      const response = await route.fetch();
      if (holdA) {
        holdA = false;
        await delayedA.promise;
        try {
          await route.fulfill({ response });
        } catch {
          // Aborting the old browser request is the expected switch behavior.
        } finally {
          delayedAHandled.release();
        }
        return;
      }
      await route.fulfill({ response });
    });
    await page.route(`**/api/v1/console/connections/${idB}/sources`, async (route) => {
      const response = await route.fetch();
      if (holdB) {
        holdB = false;
        await delayedB.promise;
      }
      await route.fulfill({ response });
    });

    await page.goto(clusterUrl("/sources/?window=1h", idA));
    const main = page.locator("#console-main");
    await expect(main).toContainText(markerA);
    await expect(page.getByText("Data plane live")).toBeVisible();
    holdA = true;
    const pendingA = page.waitForRequest((request) => new URL(request.url()).pathname === `/api/v1/console/connections/${idA}/sources`);
    await page.getByRole("button", { name: "Reconcile now" }).click();
    const lateARequest = await pendingA;
    const lateASettled = Promise.race([
      page.waitForEvent("requestfinished", (request) => request === lateARequest),
      page.waitForEvent("requestfailed", (request) => request === lateARequest),
    ]);
    const pendingB = page.waitForRequest((request) => new URL(request.url()).pathname === `/api/v1/console/connections/${idB}/sources`);
    await page.getByRole("combobox", { name: "Active connection" }).selectOption(idB);
    await pendingB;
    expect(new URL(page.url()).searchParams.get("connection")).toBe(idB);
    expect(new URL(page.url()).searchParams.get("window")).toBe("1h");
    await expect(main).not.toContainText(markerA);
    delayedA.release();
    await delayedAHandled.promise;
    await lateASettled;
    await expect(main).not.toContainText(markerA);
    await expect(main).not.toContainText(markerB);
    delayedB.release();
    await expect(main).toContainText(markerB);
    await expect(main).not.toContainText(markerA);
    await expect(page.locator(".cluster-identity strong")).toHaveText(nameB);
    await expect.poll(async () => (await sseState(page)).closed.some((url) => url.includes(`/connections/${idA}/stream`))).toBe(true);
    expect((await sseState(page)).created.at(-1)).toContain(`/connections/${idB}/stream`);
  });

  test("disconnects and deletes A in another Page without affecting B, then removes both profiles", async ({ page, context }) => {
    const idA = requiredConnectionId("a");
    const idB = requiredConnectionId("b");
    const pageB = await context.newPage();
    await instrumentSse(pageB);
    await page.goto(clusterUrl("/overview/", idA));
    await pageB.goto(clusterUrl("/overview/", idB));
    await expect(page.getByText("Data plane live")).toBeVisible();
    await expect(pageB.getByText("Data plane live")).toBeVisible();
    await pageB.goto("/connections/");
    const itemA = pageB.getByRole("article", { name: `${profileNames.a} connection` });
    await itemA.getByRole("button", { name: `Disconnect ${profileNames.a}` }).click();
    await expect(itemA.getByText("Disconnected", { exact: true })).toBeVisible();
    await expect(page.getByText("Connection unavailable")).toBeVisible({ timeout: 30_000 });
    await pageB.goto(clusterUrl("/overview/", idB));
    await expect(pageB.getByText("Data plane live")).toBeVisible();
    await expect(pageB.getByRole("table", { name: "Cluster Raft group summary" })).toContainText("Group 1");
    await expect(pageB.locator(".cluster-identity strong")).toHaveText((await realManifest(page.request)).clusters.b.cluster_name);

    await pageB.goto("/connections/");
    await pageB.getByRole("article", { name: `${profileNames.a} connection` }).getByRole("button", { name: `Delete ${profileNames.a}` }).click();
    await pageB.getByRole("dialog", { name: `Delete ${profileNames.a}?` }).getByRole("button", { name: "Delete connection" }).click();
    await expect(pageB.getByRole("dialog", { name: `Delete ${profileNames.a}?` })).toHaveCount(0);
    await expect(pageB.getByRole("article", { name: `${profileNames.a} connection` })).toHaveCount(0);
    const deletedA = await pageB.request.get(`/api/v1/console/connections/${idA}`);
    const deletedABody = await deletedA.text();
    expect(deletedA.status(), deletedABody).toBe(404);
    expect(JSON.parse(deletedABody).error.code).toBe("CONNECTION_NOT_FOUND");
    await pageB.goto(clusterUrl("/overview/", idB));
    await expect(pageB.getByText("Data plane live")).toBeVisible();

    await pageB.goto("/connections/");
    const itemB = pageB.getByRole("article", { name: `${profileNames.b} connection` });
    await itemB.getByRole("button", { name: `Disconnect ${profileNames.b}` }).click();
    await expect(itemB.getByText("Disconnected", { exact: true })).toBeVisible();
    await itemB.getByRole("button", { name: `Delete ${profileNames.b}` }).click();
    await pageB.getByRole("dialog", { name: `Delete ${profileNames.b}?` }).getByRole("button", { name: "Delete connection" }).click();
    await expect(pageB.getByRole("heading", { name: "No connections yet" })).toBeVisible();
    const list = await pageB.request.get("/api/v1/console/connections");
    const initialList = (await list.json()).data;
    expect(initialList.items).toEqual([]);
    expect(initialList.keychain_available).toBe(false);
    expect((await realManifest(page.request)).console.profile_store_exists).toBe(true);
    await pageB.close();
  });
});
