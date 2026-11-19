import { execFile, execFileSync, spawn, type ChildProcess } from "node:child_process";
import { randomUUID } from "node:crypto";
import { accessSync, closeSync, constants, existsSync, mkdirSync, openSync, statSync, writeFileSync } from "node:fs";
import { mkdtemp } from "node:fs/promises";
import { createServer, type Server } from "node:http";
import { dirname, resolve } from "node:path";
import { promisify } from "node:util";

const execFileAsync = promisify(execFile);
const realDir = __dirname;
export const repositoryRoot = resolve(realDir, "../../../../..");
export const consoleRoot = resolve(repositoryRoot, "crates/rr-admin/console");
export const defaultArtifactsRoot = resolve(repositoryRoot, "target/console-e2e");
function reserveDefaultPorts() {
  const script = `
    const { createServer } = require("node:net");
    const servers = [createServer(), createServer()];
    Promise.all(servers.map((server) => new Promise((resolve, reject) => {
      server.once("error", reject);
      server.listen(0, "127.0.0.1", resolve);
    }))).then(() => {
      const ports = servers.map((server) => server.address().port);
      process.stdout.write(JSON.stringify(ports), () => servers.forEach((server) => server.close()));
    }).catch((error) => {
      console.error(error);
      process.exitCode = 1;
    });
  `;
  const output = execFileSync(process.execPath, ["-e", script], { encoding: "utf8", timeout: 10_000 });
  const ports = JSON.parse(output) as unknown;
  if (!Array.isArray(ports) || ports.length !== 2 || ports.some((port) => !Number.isInteger(port) || port <= 0) || new Set(ports).size !== 2) {
    throw new Error("failed to reserve distinct dynamic Console/control ports");
  }
  return ports as number[];
}

const defaultPorts = !process.env.RR_CONSOLE_E2E_BASE_URL || !process.env.RR_CONSOLE_E2E_CONTROL_URL
  ? reserveDefaultPorts()
  : [];
export const consoleBaseUrl = process.env.RR_CONSOLE_E2E_BASE_URL ?? `http://127.0.0.1:${defaultPorts[0]}`;
export const controlBaseUrl = process.env.RR_CONSOLE_E2E_CONTROL_URL ?? `http://127.0.0.1:${defaultPorts[1]}`;
process.env.RR_CONSOLE_E2E_BASE_URL = consoleBaseUrl;
process.env.RR_CONSOLE_E2E_CONTROL_URL = controlBaseUrl;
export const realViewerToken = process.env.RR_CONSOLE_E2E_TOKEN ?? "rr-real-e2e-viewer-token";

export type ClusterKey = "a" | "b";

const environmentAllowlist = [
  "PATH", "HOME", "TMPDIR", "TEMP", "TMP", "LANG", "LC_ALL", "RUST_BACKTRACE", "RUST_LOG",
  "SYSTEMROOT", "COMSPEC", "PATHEXT",
] as const;

export function isolatedProcessEnvironment(overrides: Record<string, string | undefined> = {}): NodeJS.ProcessEnv {
  const environment: NodeJS.ProcessEnv = { NODE_ENV: process.env.NODE_ENV };
  for (const name of environmentAllowlist) {
    const value = process.env[name];
    if (value !== undefined) environment[name] = value;
  }
  for (const [name, value] of Object.entries(overrides)) {
    if (value !== undefined) environment[name] = value;
  }
  return environment;
}

interface NodeLayout {
  id: number;
  clientPort: number;
  adminPort: number;
  metricsPort: number;
  proxyPort: number;
  dataDir: string;
  bootJournalDir: string;
}

interface RunningNode {
  layout: NodeLayout;
  process: ManagedProcess | null;
  starts: number;
}

interface ObservabilityProxy {
  server: Server;
  degraded: boolean;
}

interface AdminGroupStatus {
  is_leader?: boolean;
}

function sleep(milliseconds: number) {
  return new Promise((resolveSleep) => setTimeout(resolveSleep, milliseconds));
}

function requiredBinary(environmentName: string) {
  const configured = process.env[environmentName];
  if (!configured) throw new Error(`${environmentName} must point to an existing binary; the real E2E harness never builds or downloads one`);
  const path = resolve(configured);
  const info = statSync(path, { throwIfNoEntry: false });
  if (!info?.isFile()) throw new Error(`${environmentName} does not name a file: ${path}`);
  accessSync(path, constants.X_OK);
  return path;
}

function loopbackHttpUrl(value: string, environmentName: string) {
  const url = new URL(value);
  if (url.protocol !== "http:" || url.hostname !== "127.0.0.1" || !url.port || url.pathname !== "/" || url.search || url.hash) {
    throw new Error(`${environmentName} must be an http://127.0.0.1:PORT URL without a path, query, or fragment`);
  }
  return { url, port: Number(url.port) };
}

async function reservePort() {
  const { createServer: createNetServer } = await import("node:net");
  const server = createNetServer();
  await new Promise<void>((resolveListen, reject) => {
    server.once("error", reject);
    server.listen(0, "127.0.0.1", resolveListen);
  });
  const address = server.address();
  if (!address || typeof address === "string") throw new Error("failed to allocate a loopback TCP port");
  const { port } = address;
  await new Promise<void>((resolveClose, reject) => server.close((error) => error ? reject(error) : resolveClose()));
  return port;
}

async function consoleListenerPids(port: number) {
  const address = `127.0.0.1:${port}`;
  let stdout: string;
  try {
    const result = await execFileAsync("lsof", [
      "-nP", "-a", `-iTCP@${address}`, "-sTCP:LISTEN", "-t",
    ], { encoding: "utf8", timeout: 2_000, maxBuffer: 64 * 1024 });
    stdout = result.stdout;
  } catch (error) {
    const failure = error as { code?: number | string; stdout?: string; stderr?: string; message?: string };
    if (failure.code === 1 && !failure.stdout?.trim()) return [];
    throw new Error(`failed to inspect Console listener ownership at ${address}: ${failure.stderr?.trim() || failure.message || String(error)}`);
  }
  const lines = stdout.split(/\r?\n/).map((line) => line.trim()).filter(Boolean);
  if (lines.some((line) => !/^[1-9]\d*$/.test(line))) {
    throw new Error(`unexpected lsof listener output for ${address}: ${JSON.stringify(stdout)}`);
  }
  return [...new Set(lines.map(Number))];
}

class ManagedProcess {
  private exited = false;
  private readonly exitPromise: Promise<number | null>;

  constructor(readonly name: string, readonly child: ChildProcess, readonly logPath: string) {
    this.exitPromise = new Promise((resolveExit) => {
      child.once("error", () => {
        this.exited = true;
        resolveExit(null);
      });
      child.once("exit", (code) => {
        this.exited = true;
        resolveExit(code);
      });
    });
  }

  get pid() {
    return this.child.pid;
  }

  get running() {
    return !this.exited;
  }

  wait() {
    return this.exitPromise;
  }

  signal(signal: NodeJS.Signals) {
    if (this.exited) return;
    if (!this.pid) throw new Error(`${this.name} has no process ID`);
    try {
      process.kill(-this.pid, signal);
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code !== "ESRCH") throw error;
    }
  }

  async stop(signal: NodeJS.Signals = "SIGTERM", graceMilliseconds = 10_000) {
    if (this.exited) return;
    this.signal(signal);
    if (signal !== "SIGKILL") {
      let timer: ReturnType<typeof setTimeout> | undefined;
      const graceful = await Promise.race([
        this.exitPromise.then(() => true),
        new Promise<boolean>((resolveTimeout) => {
          timer = setTimeout(() => resolveTimeout(false), graceMilliseconds);
        }),
      ]);
      if (timer) clearTimeout(timer);
      if (!graceful) this.signal("SIGKILL");
    }
    await this.exitPromise;
  }
}

class ProcessManager {
  private readonly children = new Set<ManagedProcess>();
  private stopped = false;
  private readonly exitHandler = () => this.killAllSynchronously();
  private readonly interruptHandler = () => this.terminateFromSignal(130);
  private readonly terminateHandler = () => this.terminateFromSignal(143);

  constructor() {
    process.once("exit", this.exitHandler);
    process.once("SIGINT", this.interruptHandler);
    process.once("SIGTERM", this.terminateHandler);
  }

  spawn(name: string, binary: string, args: string[], logPath: string, environment: Record<string, string | undefined> = {}) {
    if (this.stopped) throw new Error(`process manager is stopped; refusing to spawn ${name}`);
    mkdirSync(dirname(logPath), { recursive: true });
    const output = openSync(logPath, "a");
    let child: ChildProcess;
    const childEnvironment = isolatedProcessEnvironment(environment);
    try {
      child = spawn(binary, args, {
        detached: true,
        env: childEnvironment,
        stdio: ["ignore", output, output],
      });
    } finally {
      closeSync(output);
    }
    const managed = new ManagedProcess(name, child, logPath);
    this.children.add(managed);
    void managed.wait().finally(() => this.children.delete(managed));
    return managed;
  }

  async stopAll() {
    this.stopped = true;
    process.removeListener("exit", this.exitHandler);
    process.removeListener("SIGINT", this.interruptHandler);
    process.removeListener("SIGTERM", this.terminateHandler);
    const running = [...this.children].reverse();
    await Promise.all(running.map(async (child) => {
      await child.stop().catch(() => child.stop("SIGKILL", 0));
    }));
    this.children.clear();
  }

  private terminateFromSignal(exitCode: number): never {
    this.killAllSynchronously();
    process.exit(exitCode);
  }

  private killAllSynchronously() {
    for (const child of this.children) {
      try {
        child.signal("SIGKILL");
      } catch {
        // The process may already have exited during parent shutdown.
      }
    }
  }
}

export class RealCluster {
  readonly nodes = new Map<number, RunningNode>();
  private readonly observabilityProxies = new Map<number, ObservabilityProxy>();
  private readonly processes = new ProcessManager();
  private readonly serverBinary = requiredBinary("RR_SERVER_BIN");
  private readonly adminBinary = requiredBinary("RR_ADMIN_BIN");
  private readonly perfBenchBinary = requiredBinary("RR_PERF_BENCH_BIN");
  private stopped = false;

  private constructor(readonly key: ClusterKey, readonly runDir: string, readonly clusterName: string) {}

  private ensureRunning() {
    if (this.stopped) throw new Error(`cluster ${this.key} is stopped`);
  }

  static async start(key: ClusterKey, runDir: string) {
    const cluster = new RealCluster(key, runDir, `console-e2e-${key}-${randomUUID()}`);
    try {
      await cluster.initialize();
      return cluster;
    } catch (error) {
      await cluster.stop();
      throw error;
    }
  }

  private async initialize() {
    mkdirSync(this.runDir, { recursive: true });
    const ports = await Promise.all(Array.from({ length: 12 }, () => reservePort()));
    if (new Set(ports).size !== ports.length) throw new Error("loopback port allocation returned a duplicate");
    for (const id of [1, 2, 3]) {
      const offset = (id - 1) * 3;
      const nodeRoot = resolve(this.runDir, `node-${id}`);
      const layout: NodeLayout = {
        id,
        clientPort: ports[offset]!,
        adminPort: ports[offset + 1]!,
        metricsPort: ports[offset + 2]!,
        proxyPort: ports[8 + id]!,
        dataDir: resolve(nodeRoot, "data"),
        bootJournalDir: resolve(nodeRoot, "boot-journal"),
      };
      mkdirSync(layout.dataDir, { recursive: true });
      mkdirSync(layout.bootJournalDir, { recursive: true });
      this.nodes.set(id, { layout, process: null, starts: 0 });
    }
    for (const node of this.nodes.values()) await this.startObservabilityProxy(node.layout);

    for (const id of [1, 2, 3]) this.startNode(id);
    await Promise.all([...this.nodes.values()].map((node) => this.waitForHttp(
      this.metricsUrl(node.layout, "/health"),
      `cluster ${this.key} node ${node.layout.id} health`,
      node.process!,
    )));
    await this.formCluster();
    await this.waitForLeader([1, 2, 3]);
    await this.runWorkload(3);
    await this.triggerSnapshot();
    this.writeManifest();
  }

  private node(id: number) {
    const node = this.nodes.get(id);
    if (!node) throw new Error(`unknown real E2E node ${id}`);
    return node;
  }

  private clientAddress(layout: NodeLayout) {
    return `127.0.0.1:${layout.clientPort}`;
  }

  private adminAddress(id: number) {
    const { adminPort } = this.node(id).layout;
    return `127.0.0.1:${adminPort}`;
  }

  private metricsUrl(layout: NodeLayout, path: string) {
    return `http://127.0.0.1:${layout.metricsPort}${path}`;
  }

  private async startObservabilityProxy(layout: NodeLayout) {
    const proxy: ObservabilityProxy = {
      degraded: false,
      server: createServer(async (request, response) => {
        const path = request.url ?? "/";
        if (proxy.degraded && path === "/ready") {
          response.writeHead(503, { "content-type": "application/json; charset=utf-8", "cache-control": "no-store" });
          response.end(JSON.stringify({ ready: false, phase: "degraded", detail: "real E2E readiness degradation" }));
          return;
        }
        try {
          const upstream = await fetch(this.metricsUrl(layout, path), { signal: AbortSignal.timeout(5_000) });
          const body = Buffer.from(await upstream.arrayBuffer());
          response.writeHead(upstream.status, {
            "content-type": upstream.headers.get("content-type") ?? "application/octet-stream",
            "cache-control": "no-store",
          });
          response.end(body);
        } catch {
          response.writeHead(502, { "cache-control": "no-store" });
          response.end();
        }
      }),
    };
    this.observabilityProxies.set(layout.id, proxy);
    await new Promise<void>((resolveListen, reject) => {
      proxy.server.once("error", reject);
      proxy.server.listen(layout.proxyPort, "127.0.0.1", resolveListen);
    });
  }

  private startNode(id: number) {
    this.ensureRunning();
    const node = this.node(id);
    if (node.process?.running) throw new Error(`cluster ${this.key} node ${id} is already running`);
    const { layout } = node;
    const clientAddress = this.clientAddress(layout);
    const adminAddress = `127.0.0.1:${layout.adminPort}`;
    const metricsAddress = `127.0.0.1:${layout.proxyPort}`;
    const clusterNodes = [...this.nodes.values()].map((candidate) => this.clientAddress(candidate.layout)).join(",");
    const args = [
      "serve",
      "--node-id", String(id),
      "--listen-addr", clientAddress,
      "--advertise-addr", clientAddress,
      "--admin-bind-host", "127.0.0.1",
      "--admin-port", String(layout.adminPort),
      "--admin-advertise-addr", adminAddress,
      "--metrics-bind-host", "127.0.0.1",
      "--metrics-port", String(layout.metricsPort),
      "--observability-advertise-addr", metricsAddress,
      "--environment", `console-e2e-${this.key}`,
      "--region", "local",
      "--zone", `zone-${id}`,
      "--rack", "rack-1",
      "--app-version", "console-e2e",
      "--cluster-name", this.clusterName,
      "--storage", "rocks",
      "--durability", "strict",
      "--data-dir", layout.dataDir,
      "--boot-journal-dir", layout.bootJournalDir,
      "--allow-insecure",
      "--log-level", "info",
    ];
    if (id === 1 && node.starts === 0) args.push("--bootstrap");
    else args.push("--peers", clusterNodes);
    node.starts += 1;
    node.process = this.processes.spawn(
      `cluster-${this.key}-node-${id}-boot-${node.starts}`,
      this.serverBinary,
      args,
      resolve(this.runDir, "logs", `node-${id}-boot-${node.starts}.log`),
    );
    this.writeManifest();
  }

  private async runAdmin(address: string, args: string[]) {
    try {
      const result = await execFileAsync(this.adminBinary, ["--addr", address, "--output", "json", ...args], {
        encoding: "utf8",
        timeout: 30_000,
        maxBuffer: 4 * 1024 * 1024,
        env: isolatedProcessEnvironment(),
      });
      return result.stdout;
    } catch (error) {
      const detail = error instanceof Error ? error.message : String(error);
      throw new Error(`rr-admin ${args.join(" ")} against ${address} failed: ${detail}`);
    }
  }

  private async findLeader(candidateIds: number[], excludedId?: number) {
    for (const id of candidateIds) {
      if (id === excludedId || !this.node(id).process?.running) continue;
      try {
        const output = await this.runAdmin(this.adminAddress(id), ["group", "status", "--group", "1"]);
        const status = JSON.parse(output) as AdminGroupStatus;
        if (status.is_leader === true) return id;
      } catch {
        // A stopped or unreachable candidate cannot report leadership.
      }
    }
    return null;
  }

  private async waitForLeader(candidateIds: number[], excludedId?: number) {
    const deadline = Date.now() + 45_000;
    while (Date.now() < deadline) {
      const leader = await this.findLeader(candidateIds, excludedId);
      if (leader != null) return leader;
      await sleep(250);
    }
    throw new Error(`timed out waiting for a group 1 leader among cluster ${this.key} nodes ${candidateIds.join(", ")}${excludedId ? ` excluding ${excludedId}` : ""}`);
  }

  private async waitForHttp(url: string, description: string, expectedProcess?: ManagedProcess) {
    const deadline = Date.now() + 45_000;
    let lastError: unknown;
    while (Date.now() < deadline) {
      if (expectedProcess && !expectedProcess.running) {
        throw new Error(`${description} process exited during readiness; see ${expectedProcess.logPath}`);
      }
      try {
        const response = await fetch(url, { signal: AbortSignal.timeout(2_000) });
        if (response.ok) return;
        lastError = new Error(`HTTP ${response.status}`);
      } catch (error) {
        lastError = error;
      }
      await sleep(200);
    }
    throw new Error(`timed out waiting for ${description} at ${url}: ${lastError instanceof Error ? lastError.message : String(lastError)}`);
  }

  private async formCluster() {
    await this.waitForLeader([1]);
    const leaderAdmin = this.adminAddress(1);
    for (const id of [2, 3]) {
      await this.runAdmin(leaderAdmin, [
        "group", "add-learner", "--group", "1", "--node-id", String(id), "--addr", this.clientAddress(this.node(id).layout),
      ]);
    }
    await this.runAdmin(leaderAdmin, ["group", "change-membership", "--group", "1", "--voters", "1,2,3"]);
  }

  async runWorkload(durationSeconds: number) {
    this.ensureRunning();
    if (!Number.isInteger(durationSeconds) || durationSeconds < 1 || durationSeconds > 60) {
      throw new Error("workload duration must be an integer from 1 to 60 seconds");
    }
    const reportPath = resolve(this.runDir, `workload-${randomUUID()}.json`);
    const logPath = resolve(this.runDir, "logs", `workload-${randomUUID()}.log`);
    const workload = this.processes.spawn(`workload-${this.key}`, this.perfBenchBinary, [
      "--scenario", "s1",
      "--cluster-addrs", [...this.nodes.values()].map((node) => this.clientAddress(node.layout)).join(","),
      "--warmup-secs", "1",
      "--duration-secs", String(durationSeconds),
      "--concurrency", "2",
      "--payload", "64",
      "--groups", "1",
      "--json-out", reportPath,
    ], logPath, { RR_SERVER_BIN: undefined, RR_ADMIN_BIN: undefined });
    let timer: ReturnType<typeof setTimeout> | undefined;
    const outcome = await Promise.race([
      workload.wait(),
      new Promise<"timeout">((resolveTimeout) => {
        timer = setTimeout(() => resolveTimeout("timeout"), (durationSeconds + 90) * 1_000);
      }),
    ]);
    if (timer) clearTimeout(timer);
    if (outcome === "timeout") {
      await workload.stop("SIGKILL");
      throw new Error(`cluster ${this.key} workload timed out; see ${logPath}`);
    }
    if (outcome !== 0) throw new Error(`cluster ${this.key} workload exited with status ${outcome}; see ${logPath}`);
    return reportPath;
  }

  async triggerSnapshot() {
    this.ensureRunning();
    const leader = await this.waitForLeader([1, 2, 3]);
    await this.runAdmin(this.adminAddress(leader), ["group", "snapshot", "--group", "1"]);
    return leader;
  }

  async killLeader() {
    this.ensureRunning();
    const leader = await this.waitForLeader([1, 2, 3]);
    const node = this.node(leader);
    if (!node.process) throw new Error(`leader node ${leader} has no managed process`);
    await node.process.stop("SIGKILL");
    this.writeManifest();
    return leader;
  }

  async restartNode(id: number) {
    this.ensureRunning();
    const node = this.node(id);
    if (node.process?.running) throw new Error(`refusing to restart running node ${id}`);
    this.startNode(id);
    await this.waitForHttp(this.metricsUrl(node.layout, "/health"), `restarted cluster ${this.key} node ${id} health`, node.process!);
    await this.waitForLeader([1, 2, 3]);
    this.writeManifest();
  }

  private setReadinessDegraded(id: number, degraded: boolean) {
    const proxy = this.observabilityProxies.get(id);
    if (!proxy) throw new Error(`unknown observability proxy ${id}`);
    proxy.degraded = degraded;
    return { node_id: id, degraded };
  }

  async handleControl(method: string, path: string): Promise<unknown> {
    this.ensureRunning();
    if (method === "GET" && path === "/health") return { ok: true, cluster: this.key };
    if (method === "POST" && path === "/leader/kill") return { node_id: await this.killLeader() };
    const readiness = path.match(/^\/nodes\/([123])\/readiness\/(degraded|restore)$/);
    if (method === "POST" && readiness) return this.setReadinessDegraded(Number(readiness[1]), readiness[2] === "degraded");
    const restart = path.match(/^\/nodes\/([123])\/restart$/);
    if (method === "POST" && restart) {
      const id = Number(restart[1]);
      await this.restartNode(id);
      return { node_id: id };
    }
    if (method === "POST" && path === "/groups/1/snapshot") return { node_id: await this.triggerSnapshot() };
    if (method === "POST" && path === "/workload") return { report: await this.runWorkload(3) };
    throw new Error(`unknown real E2E control action for cluster ${this.key}: ${method} ${path}`);
  }

  manifest() {
    return {
      cluster_key: this.key,
      cluster_name: this.clusterName,
      formation: "LEGACY_MANUAL_MEMBERSHIP",
      admin_seeds: [1, 2, 3].map((id) => ({ node_id: id, admin_addr: this.adminAddress(id) })),
      nodes: [...this.nodes.values()].map((node) => ({
        node_id: node.layout.id,
        client_addr: this.clientAddress(node.layout),
        admin_addr: this.adminAddress(node.layout.id),
        observability_addr: `127.0.0.1:${node.layout.proxyPort}`,
        upstream_metrics_addr: `127.0.0.1:${node.layout.metricsPort}`,
        data_dir: node.layout.dataDir,
        boot_journal_dir: node.layout.bootJournalDir,
        starts: node.starts,
        running: node.process?.running ?? false,
        pid: node.process?.pid ?? null,
      })),
    };
  }

  private writeManifest() {
    if (!this.runDir || this.nodes.size === 0) return;
    writeFileSync(resolve(this.runDir, "manifest.json"), `${JSON.stringify(this.manifest(), null, 2)}\n`, { mode: 0o600 });
  }

  private async stopObservabilityProxies() {
    await Promise.all([...this.observabilityProxies.values()].map(async (proxy) => {
      proxy.server.closeAllConnections();
      await new Promise<void>((resolveClose) => proxy.server.close(() => resolveClose()));
    }));
    this.observabilityProxies.clear();
  }

  async stop() {
    if (this.stopped) return;
    this.stopped = true;
    let firstError: unknown;
    const attempt = async (action: () => void | Promise<void>) => {
      try {
        await action();
      } catch (error) {
        firstError ??= error;
      }
    };
    await attempt(() => this.stopObservabilityProxies());
    await attempt(() => this.processes.stopAll());
    await attempt(() => this.writeManifest());
    if (firstError) throw firstError;
  }
}

export class RealHarness {
  private readonly processes = new ProcessManager();
  private readonly adminBinary = requiredBinary("RR_ADMIN_BIN");
  private readonly consoleAddress = loopbackHttpUrl(consoleBaseUrl, "RR_CONSOLE_E2E_BASE_URL");
  private readonly controlAddress = loopbackHttpUrl(controlBaseUrl, "RR_CONSOLE_E2E_CONTROL_URL");
  private readonly clusters = new Map<ClusterKey, RealCluster>();
  private readonly controlIdentity = randomUUID();
  private readonly controlActions = new Set<Promise<void>>();
  private controlServer: Server | null = null;
  private consoleProcess: ManagedProcess | null = null;
  private consoleStarts = 0;
  private runDir = "";
  private stateDir = "";
  private profileStoreExistedAtStart = false;
  private stopped = false;
  private stopPromise: Promise<void> | null = null;

  private ensureRunning() {
    if (this.stopped) throw new Error("real E2E harness is stopped");
  }

  static async start(harness = new RealHarness()) {
    try {
      await harness.initialize();
      return harness;
    } catch (error) {
      await harness.stop();
      throw error;
    }
  }

  private async initialize() {
    mkdirSync(defaultArtifactsRoot, { recursive: true });
    this.runDir = await mkdtemp(resolve(defaultArtifactsRoot, "real-dual-run-"));
    this.stateDir = resolve(this.runDir, "console-state");
    mkdirSync(this.stateDir, { recursive: true, mode: 0o700 });
    this.profileStoreExistedAtStart = existsSync(this.profileStorePath());
    const staticDir = resolve(consoleRoot, "out");
    if (!statSync(resolve(staticDir, "index.html"), { throwIfNoEntry: false })?.isFile()) {
      throw new Error(`Console static export is missing; run the frontend build before the real suite: ${staticDir}`);
    }

    for (const key of ["a", "b"] as const) {
      const cluster = await RealCluster.start(key, resolve(this.runDir, `cluster-${key}`));
      this.clusters.set(key, cluster);
    }
    await this.startConsole();
    await this.startControlServer();
    this.writeManifest();
  }

  private profileStorePath() {
    return resolve(this.stateDir, "connections.json");
  }

  private cluster(key: ClusterKey) {
    const cluster = this.clusters.get(key);
    if (!cluster) throw new Error(`unknown real E2E cluster ${key}`);
    return cluster;
  }

  private consoleArgs() {
    return [
      "console",
      "--listen", `127.0.0.1:${this.consoleAddress.port}`,
      "--console-token-env", "RR_CONSOLE_E2E_TOKEN",
      "--allow-insecure",
      "--keychain-disable",
      "--static-dir", resolve(consoleRoot, "out"),
      "--state-dir", this.stateDir,
    ];
  }

  private async startConsole() {
    this.ensureRunning();
    if (this.consoleProcess?.running) throw new Error("Console is already running");
    this.consoleStarts += 1;
    const managed = this.processes.spawn(
      `console-${this.consoleStarts}`,
      this.adminBinary,
      this.consoleArgs(),
      resolve(this.runDir, "logs", `console-${this.consoleStarts}.log`),
      {
        RR_CONSOLE_E2E_TOKEN: realViewerToken,
        RR_ADMIN_TOKEN: undefined,
        RR_CONSOLE_ETCD_PASSWORD: undefined,
      },
    );
    this.consoleProcess = managed;
    try {
      await this.waitForConsoleReady(managed);
    } catch (error) {
      let cleanupError: unknown;
      try {
        await managed.stop("SIGKILL");
      } catch (cause) {
        cleanupError = cause;
      }
      this.writeManifest();
      if (cleanupError) {
        throw new Error(`${error instanceof Error ? error.message : String(error)}; failed to stop spawned Console PID ${managed.pid}: ${cleanupError instanceof Error ? cleanupError.message : String(cleanupError)}`);
      }
      throw error;
    }
    this.writeManifest();
  }

  private async waitForConsoleReady(expectedProcess: ManagedProcess) {
    const deadline = Date.now() + 45_000;
    const expectedPid = expectedProcess.pid;
    const address = `127.0.0.1:${this.consoleAddress.port}`;
    if (!expectedPid) throw new Error(`spawned Console has no PID; see ${expectedProcess.logPath}`);
    let lastError: unknown;
    while (Date.now() < deadline) {
      const listenerPids = await consoleListenerPids(this.consoleAddress.port);
      const ownedBySpawnedProcess = listenerPids.length === 1 && listenerPids[0] === expectedPid;
      if (listenerPids.length > 0 && !ownedBySpawnedProcess) {
        throw new Error(`Console readiness ownership mismatch at ${address}: spawned PID ${expectedPid}, listener PID(s) ${listenerPids.join(",")}; refusing to contact the listener; see ${expectedProcess.logPath}`);
      }
      if (!expectedProcess.running) {
        throw new Error(`no-source Console PID ${expectedPid} exited during readiness; see ${expectedProcess.logPath}`);
      }
      if (!ownedBySpawnedProcess) {
        lastError = new Error(`waiting for spawned Console PID ${expectedPid} to own ${address}`);
        await sleep(200);
        continue;
      }
      try {
        const response = await fetch(`${consoleBaseUrl}/api/v1/console/connections`, { signal: AbortSignal.timeout(2_000) });
        const body = await response.json() as { meta?: { schema_version?: string }; error?: { code?: string } };
        if (response.status === 401 && body.meta?.schema_version === "console.v1" && body.error?.code === "UNAUTHENTICATED") {
          const confirmedPids = await consoleListenerPids(this.consoleAddress.port);
          if (confirmedPids.length === 1 && confirmedPids[0] === expectedPid && expectedProcess.running) return;
          lastError = new Error(`Console listener ownership changed during readiness at ${address}: spawned PID ${expectedPid}, listener PID(s) ${confirmedPids.join(",") || "none"}`);
        } else {
          lastError = new Error(`unexpected Console readiness response HTTP ${response.status}: ${JSON.stringify(body)}`);
        }
      } catch (error) {
        lastError = error;
      }
      await sleep(200);
    }
    throw new Error(`timed out waiting for spawned Console PID ${expectedPid} to own and serve ${address}: ${lastError instanceof Error ? lastError.message : String(lastError)}`);
  }

  async restartConsole() {
    this.ensureRunning();
    if (this.consoleProcess) await this.consoleProcess.stop();
    await sleep(1_500);
    this.ensureRunning();
    await this.startConsole();
  }

  private async waitForHttp(url: string, description: string, validate?: (response: Response) => Promise<void>) {
    const deadline = Date.now() + 45_000;
    let lastError: unknown;
    while (Date.now() < deadline) {
      try {
        const response = await fetch(url, { signal: AbortSignal.timeout(2_000) });
        if (response.ok) {
          if (!validate) return;
          try {
            await validate(response);
            return;
          } catch (error) {
            lastError = error;
          }
        } else {
          lastError = new Error(`HTTP ${response.status}`);
        }
      } catch (error) {
        lastError = error;
      }
      await sleep(200);
    }
    throw new Error(`timed out waiting for ${description} at ${url}: ${lastError instanceof Error ? lastError.message : String(lastError)}`);
  }

  private async startControlServer() {
    this.ensureRunning();
    this.controlServer = createServer((request, response) => {
      const action = Promise.resolve().then(async () => {
        try {
          const result = await this.handleControl(request.method ?? "GET", request.url ?? "/");
          response.writeHead(200, { "content-type": "application/json; charset=utf-8", "cache-control": "no-store" });
          response.end(JSON.stringify(result));
        } catch (error) {
          response.writeHead(500, { "content-type": "application/json; charset=utf-8", "cache-control": "no-store" });
          response.end(JSON.stringify({ error: error instanceof Error ? error.message : String(error) }));
        }
      });
      this.controlActions.add(action);
      void action.finally(() => this.controlActions.delete(action));
    });
    await new Promise<void>((resolveListen, reject) => {
      this.controlServer!.once("error", reject);
      this.controlServer!.listen(this.controlAddress.port, "127.0.0.1", resolveListen);
    });
    await this.waitForHttp(`${controlBaseUrl}/health`, "identified real E2E control server", async (response) => {
      const body = await response.json() as { ok?: boolean; control_id?: string; run_id?: string };
      if (body.ok !== true || body.control_id !== this.controlIdentity || body.run_id !== resolve(this.runDir).split("/").at(-1)) {
        throw new Error(`control identity mismatch: ${JSON.stringify(body)}`);
      }
    });
  }

  private async handleControl(method: string, path: string): Promise<unknown> {
    this.ensureRunning();
    if (method === "GET" && path === "/health") {
      return {
        ok: true,
        control_id: this.controlIdentity,
        run_id: resolve(this.runDir).split("/").at(-1),
        active_control_actions: this.controlActions.size,
      };
    }
    if (method === "GET" && path === "/manifest") return this.manifest();
    if (method === "POST" && path === "/console/restart") {
      await this.restartConsole();
      return { restarted: true };
    }
    const clusterAction = path.match(/^\/clusters\/(a|b)(\/.*)$/);
    if (clusterAction) return await this.cluster(clusterAction[1] as ClusterKey).handleControl(method, clusterAction[2]!);
    throw new Error(`unknown real E2E harness control action: ${method} ${path}`);
  }

  manifest() {
    return {
      run_id: resolve(this.runDir).split("/").at(-1),
      console_url: consoleBaseUrl,
      control_url: controlBaseUrl,
      control_identity: this.controlIdentity,
      in_flight_control_actions: this.controlActions.size,
      console: {
        args: this.consoleArgs(),
        peers: null,
        discovery: null,
        state_dir: this.stateDir,
        profile_store_path: this.profileStorePath(),
        profile_store_exists: existsSync(this.profileStorePath()),
        profile_store_existed_at_start: this.profileStoreExistedAtStart,
        starts: this.consoleStarts,
        pid: this.consoleProcess?.pid ?? null,
        running: this.consoleProcess?.running ?? false,
      },
      clusters: {
        a: this.clusters.get("a")?.manifest() ?? null,
        b: this.clusters.get("b")?.manifest() ?? null,
      },
    };
  }

  private writeManifest() {
    if (!this.runDir) return;
    writeFileSync(resolve(this.runDir, "manifest.json"), `${JSON.stringify(this.manifest(), null, 2)}\n`, { mode: 0o600 });
  }

  stop(): Promise<void> {
    if (!this.stopPromise) {
      this.stopped = true;
      this.stopPromise = this.performStop();
    }
    return this.stopPromise;
  }

  private async performStop() {
    let firstError: unknown;
    const attempt = async (action: () => void | Promise<void>) => {
      try {
        await action();
      } catch (error) {
        firstError ??= error;
      }
    };
    const pendingActions = [...this.controlActions];
    if (pendingActions.length) {
      const outcomes = await Promise.allSettled(pendingActions);
      for (const outcome of outcomes) {
        if (outcome.status === "rejected") firstError ??= outcome.reason;
      }
    }
    this.controlActions.clear();
    const controlServer = this.controlServer;
    this.controlServer = null;
    if (controlServer) {
      await attempt(async () => {
        controlServer.closeAllConnections();
        await new Promise<void>((resolveClose) => controlServer.close(() => resolveClose()));
      });
    }
    await attempt(() => this.processes.stopAll());
    for (const cluster of [...this.clusters.values()].reverse()) {
      await attempt(() => cluster.stop());
    }
    await attempt(() => this.writeManifest());
    if (firstError) throw firstError;
  }
}
