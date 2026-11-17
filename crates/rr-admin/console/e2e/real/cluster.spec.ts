import { expect, test, type APIRequestContext, type Page } from "@playwright/test";
import { controlBaseUrl, realViewerToken } from "./harness";

interface ConsoleEnvelope<T> {
  data: T;
  meta: { partial: boolean };
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

async function consoleGet<T>(request: APIRequestContext, path: string): Promise<ConsoleEnvelope<T>> {
  const response = await request.get(`/api/v1/console${path}`);
  expect(response.ok(), `GET ${path} failed with HTTP ${response.status()}: ${await response.text()}`).toBe(true);
  return await response.json() as ConsoleEnvelope<T>;
}

async function control(request: APIRequestContext, path: string) {
  const response = await request.post(`${controlBaseUrl}${path}`, { timeout: 60_000 });
  expect(response.ok(), `control POST ${path} failed: ${await response.text()}`).toBe(true);
  return await response.json() as { node_id?: number };
}

async function login(page: Page) {
  await page.goto("/login/");
  await page.getByLabel("Viewer credential").fill(realViewerToken);
  await page.getByRole("button", { name: "Sign in to Console" }).click();
  await expect(page).toHaveURL(/\/overview\/$/);
}

async function summary(request: APIRequestContext) {
  return await consoleGet<SummaryData>(request, "/summary?window=15m");
}

async function nodeDetail(request: APIRequestContext, nodeId: number) {
  return await consoleGet<{ node: NodeObservation }>(request, `/nodes/${nodeId}`);
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
  await page.addInitScript(() => {
    const events: string[] = [];
    Object.defineProperty(window, "__realSseEvents", { value: events });
    const NativeEventSource = window.EventSource;
    window.EventSource = class extends NativeEventSource {
      constructor(url: string | URL, init?: EventSourceInit) {
        super(url, init);
        for (const type of ["summary", "resync"]) this.addEventListener(type, () => events.push(type));
      }
    };
  });
}

async function sseEvents(page: Page) {
  return await page.evaluate(() => (window as unknown as { __realSseEvents: string[] }).__realSseEvents);
}

async function assertGroupEvidence(request: APIRequestContext) {
  const group = await consoleGet<GroupObservation>(request, "/groups/1");
  expect(group.data.reachable_voters).toBe(3);
  expect(group.data.voters_total).toBe(3);
  const replicas = await consoleGet<{ replicas: ReplicaObservation[] }>(request, "/groups/1/replicas");
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

async function assertLifecycleEvidence(request: APIRequestContext) {
  const currentLifecycle = await consoleGet<{ items: BootRecord[] }>(request, "/lifecycle?view=current&page_size=50");
  expect(currentLifecycle.data.items).toHaveLength(3);
  for (const record of currentLifecycle.data.items) expectProtocolEvidence(record);
  const lifecycleVersions = await consoleGet<{ items: BootRecord[] }>(request, "/lifecycle?view=versions&page_size=50");
  expect(lifecycleVersions.data.items).toHaveLength(3);
  for (const record of lifecycleVersions.data.items) {
    expect(record.current).toBe(true);
    expectProtocolEvidence(record);
  }
}

async function waitForHealthySummary(request: APIRequestContext) {
  await expect.poll(async () => {
    const current = await summary(request);
    return current.data.nodes.map((node) => node.node_id).sort((a, b) => a - b);
  }, { timeout: 60_000 }).toEqual([1, 2, 3]);
  await expect.poll(async () => {
    const current = await summary(request);
    return current.data.nodes.every((node) => node.status === "ONLINE") && current.data.groups.some((group) => group.group_id === 1 && group.leader_node_id != null);
  }, { timeout: 60_000 }).toBe(true);
}

function captureBrowserErrors(page: Page) {
  const errors: string[] = [];
  page.on("pageerror", (error) => errors.push(error.message));
  page.on("console", (message) => {
    if (message.type() === "error") errors.push(message.text());
  });
  return errors;
}

test.describe.serial("real three-node RocksDB Console", () => {
  test.beforeEach(async ({ page }) => {
    await instrumentSse(page);
    await login(page);
  });

  test("cold node topology collects leader replication evidence on its first request", async ({ page }) => {
    await page.goto("/topology/?view=nodes");
    await page.getByRole("tab", { name: "Data table", exact: true }).click();
    await expect(page.getByRole("table", { name: "Observed AppendEntries relationships" })).toBeVisible();
  });

  test("discovers three nodes from two Admin seeds and renders all nine views with real workload and snapshots", async ({ page }) => {
    const errors = captureBrowserErrors(page);
    await waitForHealthySummary(page.request);

    await expect.poll(async () => {
      const nodes = await consoleGet<{ items: NodeObservation[] }>(page.request, "/nodes?page_size=50");
      return nodes.data.items.length === 3 && nodes.data.items.every(hasResourceValues);
    }, { timeout: 60_000 }).toBe(true);
    await expect.poll(async () => {
      const details = await Promise.all([1, 2, 3].map((id) => nodeDetail(page.request, id)));
      return details.every(({ data }) => data.node.ready === true && data.node.liveness === true && data.node.ready_phase === "ready");
    }, { timeout: 60_000 }).toBe(true);

    await expect.poll(async () => {
      const snapshots = await consoleGet<{ items: Array<{ node_id: number; snapshot_index?: number | null; last_snapshot_index?: number | null }> }>(page.request, "/snapshots?page_size=50");
      return snapshots.data.items.some((item) => (item.snapshot_index ?? item.last_snapshot_index ?? 0) > 0);
    }, { timeout: 60_000 }).toBe(true);
    await expect.poll(async () => {
      const performance = await consoleGet<{ series: unknown[] }>(page.request, "/metrics/performance?window=15m");
      return performance.data.series.length;
    }, { timeout: 60_000 }).toBeGreaterThan(0);

    await page.goto("/overview/");
    await expect(page.getByRole("heading", { level: 1, name: "Overview" })).toBeVisible();
    await expect(page.getByRole("table", { name: "Cluster Raft group summary" })).toContainText("Group 1");
    await expect(page.getByText("3 / 3", { exact: true }).first()).toBeVisible();
    await expect(page.getByText("Console data unavailable")).toHaveCount(0);

    await page.getByRole("link", { name: /Nodes online/ }).click();
    await expect(page).toHaveURL(/\/nodes\//);
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

    await page.goto("/overview/");
    await page.getByRole("link", { name: "Group 1", exact: true }).click();
    await expect(page).toHaveURL(/\/groups\/\?window=15m&group_id=1$/);
    await expect(page.getByRole("dialog", { name: "Group 1 evidence" })).toBeVisible();
    await expect(page.getByRole("table", { name: "Group replica evidence" })).toContainText("Node 1");
    await assertGroupEvidence(page.request);
    await page.getByRole("link", { name: "Group snapshots" }).click();
    await expect(page.getByRole("heading", { level: 1, name: "Snapshots & storage" })).toBeVisible();
    await expect(page.getByRole("table", { name: "Snapshots by group and node" })).toContainText("UNSUPPORTED");
    await expect(page.getByText("0%", { exact: true })).toHaveCount(0);

    await page.goto("/topology/?view=nodes");
    await expect(page.getByRole("heading", { level: 1, name: "Topology" })).toBeVisible();
    await expect(page.getByRole("group", { name: "Node topology graph" })).toBeVisible();
    await page.getByRole("tab", { name: "Data table", exact: true }).click();
    await expect(page.getByRole("table", { name: "Node topology" })).toContainText("Node 3");
    await expect(page.getByRole("table", { name: "Observed AppendEntries relationships" })).toBeVisible();
    await page.getByRole("tab", { name: "Raft", exact: true }).click();
    await expect(page.getByRole("combobox", { name: "Raft group" })).toHaveValue("1");
    await page.getByRole("tab", { name: "Partitions & routes" }).click();
    await expect(page.getByRole("table", { name: "Partition and route topology" })).toContainText("1");

    await page.goto("/performance/");
    await expect(page.getByRole("heading", { level: 1, name: "Performance" })).toBeVisible();
    await expect(page.getByRole("table", { name: "Performance hotspot observations" })).toBeVisible();
    await expect(page.getByText(/Console memory retention/).first()).toBeVisible();

    await page.goto("/lifecycle/");
    await expect(page.getByRole("heading", { level: 1, name: "Lifecycle" })).toBeVisible();
    await expect(page.getByRole("table", { name: "Current node lifecycle" })).toContainText("Node 3");
    await page.getByRole("tab", { name: "Versions" }).click();
    await expect(page.getByRole("table", { name: "Version history by boot" })).toContainText("console-e2e");
    await assertLifecycleEvidence(page.request);

    await page.goto("/events/");
    await expect(page.getByRole("heading", { level: 1, name: "Events & alerts" })).toBeVisible();
    await expect(page.getByText(/observed events|observation coverage/i).first()).toBeVisible();

    await page.goto("/sources/");
    await expect(page.getByRole("heading", { level: 1, name: "Sources" })).toBeVisible();
    await expect(page.getByText("EXPLICIT_ADMIN_SEEDS", { exact: true })).toBeVisible();
    const sourceTable = page.getByRole("table", { name: "Node source health" });
    await expect(sourceTable).toContainText("Node 3");
    await expect(sourceTable).toContainText("127.0.0.1:");
    await page.getByRole("button", { name: "Reconcile now" }).click();
    await expect(page.getByText(/Reconciliation completed/)).toBeVisible();

    for (const width of [1440, 1280]) {
      await page.setViewportSize({ width, height: width === 1440 ? 900 : 800 });
      for (const route of ["overview", "nodes", "topology", "groups", "performance", "snapshots", "lifecycle", "events", "sources"]) {
        await page.goto(`/${route}/`);
        await expect(page.locator("h1")).toHaveCount(1);
        expect(await page.evaluate(() => document.documentElement.scrollWidth > window.innerWidth), `${route} page overflow at ${width}px`).toBe(false);
      }
    }
    expect(errors).toEqual([]);
  });

  test("survives leader kill, elects a replacement, and persists abnormal boot history", async ({ page }) => {
    const errors = captureBrowserErrors(page);
    await waitForHealthySummary(page.request);
    await assertGroupEvidence(page.request);
    await assertLifecycleEvidence(page.request);
    const before = await summary(page.request);
    expect(before.data.groups.find((group) => group.group_id === 1)?.leader_node_id).not.toBeNull();
    const oldBootIds = new Map(before.data.nodes.map((node) => [
      node.node_id,
      node.boot?.current?.boot_id ?? node.boot_id ?? node.build?.boot_id,
    ]));

    const killed = await control(page.request, "/leader/kill");
    const oldLeader = killed.node_id!;
    expect(oldLeader).toBeGreaterThan(0);
    const oldBootId = oldBootIds.get(oldLeader);
    expect(oldBootId).toBeTruthy();
    await expect.poll(async () => {
      const current = await summary(page.request);
      const group = current.data.groups.find((candidate) => candidate.group_id === 1);
      const leader = group?.leader_node_id;
      const killedNode = current.data.nodes.find((node) => node.node_id === oldLeader);
      return current.meta.partial && leader != null && leader !== oldLeader && group?.reachable_voters === 2 && group.voters_total === 3 && ["STALE", "UNREACHABLE"].includes(killedNode?.status ?? "");
    }, { timeout: 60_000 }).toBe(true);

    await expect.poll(async () => {
      const events = await consoleGet<{ items: EventRecord[] }>(page.request, "/events?state=ACTIVE&page_size=50");
      return events.data.items.some((event) => event.kind === "NODE_UNREACHABLE" && (event.node_id === oldLeader || event.object === `node-${oldLeader}`));
    }, { timeout: 30_000 }).toBe(true);

    await page.goto("/overview/");
    await expect(page.getByText("Partial", { exact: true }).first()).toBeVisible();
    await expect(page.getByText(/Unreachable|Stale/).first()).toBeVisible();
    await expect(page.getByRole("table", { name: "Cluster Raft group summary" })).not.toContainText(`Node ${oldLeader}`);
    await page.goto("/events/?state=ACTIVE");
    await expect(page.getByRole("table", { name: "Observed events and alerts" })).toContainText("NODE_UNREACHABLE");

    await control(page.request, `/nodes/${oldLeader}/restart`);
    await waitForHealthySummary(page.request);

    const activeEvents = await consoleGet<{ items: Array<{ kind: string; node_id?: number | null }> }>(page.request, "/events?state=ACTIVE");
    expect(activeEvents.data.items.some((event) => event.kind === "NODE_UNREACHABLE" && event.node_id === oldLeader)).toBe(false);
    const resolvedEvents = await consoleGet<{ items: Array<{ kind: string; node_id?: number | null }> }>(page.request, "/events?state=RESOLVED");
    expect(resolvedEvents.data.items.some((event) => event.kind === "NODE_UNREACHABLE" && event.node_id === oldLeader)).toBe(true);
    await expect.poll(async () => {
      const events = await consoleGet<{ items: Array<{ kind: string; node_id?: number | null }> }>(page.request, "/events?state=RESOLVED");
      return events.data.items.some((event) => event.kind === "NODE_RESTARTED" && event.node_id === oldLeader);
    }, { timeout: 30_000 }).toBe(true);

    await expect.poll(async () => {
      const lifecycle = await consoleGet<{ items: BootRecord[] }>(page.request, "/lifecycle?view=restarts&page_size=50");
      const boots = lifecycle.data.items.filter((boot) => boot.node_id === oldLeader);
      const oldBoot = boots.find((boot) => boot.boot_id === oldBootId);
      return boots.length >= 2 && oldBoot?.shutdown_state === "ABNORMAL_OR_UNCERTAIN" && boots.some((boot) => boot.boot_id !== oldBootId && boot.shutdown_state === "RUNNING" && boot.persistence === "PERSISTENT");
    }, { timeout: 60_000 }).toBe(true);

    const versions = await consoleGet<{ items: BootRecord[] }>(page.request, "/lifecycle?view=versions&page_size=50");
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
      expect(record.git_dirty).toBe(record.dirty);
      expect(record.compatibility?.status).toBe("UNKNOWN");
      expect(record.compatibility?.incompatible_node_ids).toEqual([]);
    }

    await page.goto("/lifecycle/?view=restarts");
    await expect(page.getByRole("table", { name: "Restart timeline data" })).toContainText(oldBootId!);
    await expect(page.getByText("Abnormal or uncertain").first()).toBeVisible();
    await expect(page.getByText("PERSISTENT").first()).toBeVisible();

    const degradedFollower = before.data.nodes.find((node) => node.node_id !== oldLeader)!.node_id;
    await control(page.request, `/nodes/${degradedFollower}/readiness/degraded`);
    await expect.poll(async () => {
      const detail = await nodeDetail(page.request, degradedFollower);
      return detail.data.node.status === "DEGRADED" && detail.data.node.ready === false && detail.data.node.liveness === true && detail.data.node.ready_phase === "degraded";
    }, { timeout: 30_000 }).toBe(true);
    await control(page.request, `/nodes/${degradedFollower}/readiness/restore`);
    await expect.poll(async () => {
      const detail = await nodeDetail(page.request, degradedFollower);
      return detail.data.node.status === "ONLINE" && detail.data.node.ready === true && detail.data.node.liveness === true && detail.data.node.ready_phase === "ready";
    }, { timeout: 30_000 }).toBe(true);
    expect(errors).toEqual([]);
  });

  test("reconnects SSE after real network interruption", async ({ page }) => {
    const errors = captureBrowserErrors(page);
    await waitForHealthySummary(page.request);
    await page.goto("/overview/");
    await expect(page.getByText("Data plane live")).toBeVisible();
    await expect(page.getByRole("table", { name: "Cluster Raft group summary" })).toContainText("Group 1");
    expect(errors).toEqual([]);
    await control(page.request, "/console/restart");
    await expect(page.getByText("Reconnecting")).toBeVisible();
    await expect(page).toHaveURL(/\/login\/\?next=/, { timeout: 30_000 });
    await login(page);
    await page.goto("/overview/");
    await expect(page.getByText("Data plane live")).toBeVisible({ timeout: 30_000 });
    await expect(page.getByRole("table", { name: "Cluster Raft group summary" })).toContainText("Group 1");
    expect(await sseEvents(page)).toContain("summary");
  });
});
