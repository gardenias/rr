import { execFile, spawn, spawnSync, type ChildProcess } from "node:child_process";
import { randomUUID } from "node:crypto";
import { accessSync, closeSync, constants, mkdirSync, openSync, statSync, writeFileSync } from "node:fs";
import { mkdtemp } from "node:fs/promises";
import { createServer, type Server } from "node:http";
import { dirname, resolve } from "node:path";
import { promisify } from "node:util";

const execFileAsync = promisify(execFile);
const realDir = __dirname;
export const repositoryRoot = resolve(realDir, "../../../../..");
export const consoleRoot = resolve(repositoryRoot, "crates/rr-admin/console");
export const defaultArtifactsRoot = resolve(repositoryRoot, "target/console-e2e");
export const consoleBaseUrl = process.env.RR_CONSOLE_E2E_BASE_URL ?? "http://127.0.0.1:4311";
export const controlBaseUrl = process.env.RR_CONSOLE_E2E_CONTROL_URL ?? "http://127.0.0.1:4312";
export const realViewerToken = process.env.RR_CONSOLE_E2E_TOKEN ?? "rr-real-e2e-viewer-token";
const etcdImage = "quay.io/coreos/etcd:v3.5.13";

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
  private readonly exitCleanups = new Set<() => void>();
  private readonly exitHandler = () => this.killAllSynchronously();
  private readonly interruptHandler = () => this.terminateFromSignal(130);
  private readonly terminateHandler = () => this.terminateFromSignal(143);

  constructor() {
    process.once("exit", this.exitHandler);
    process.once("SIGINT", this.interruptHandler);
    process.once("SIGTERM", this.terminateHandler);
  }

  spawn(name: string, binary: string, args: string[], logPath: string, environment: Record<string, string | undefined> = {}) {
    mkdirSync(dirname(logPath), { recursive: true });
    const output = openSync(logPath, "a");
    let child: ChildProcess;
    const childEnvironment = { ...process.env };
    for (const [name, value] of Object.entries(environment)) {
      if (value === undefined) delete childEnvironment[name];
      else childEnvironment[name] = value;
    }
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

  onExit(cleanup: () => void) {
    this.exitCleanups.add(cleanup);
  }

  async stopAll() {
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
    for (const cleanup of this.exitCleanups) {
      try {
        cleanup();
      } catch {
        // Best-effort cleanup must not prevent the remaining handlers.
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
  private readonly consoleAddress = loopbackHttpUrl(consoleBaseUrl, "RR_CONSOLE_E2E_BASE_URL");
  private readonly controlAddress = loopbackHttpUrl(controlBaseUrl, "RR_CONSOLE_E2E_CONTROL_URL");
  private controlServer: Server | null = null;
  private consoleProcess: ManagedProcess | null = null;
  private consoleStarts = 0;
  private runDir = "";
  private stopped = false;
  private clusterName = "";
  private etcdContainerName = "";
  private etcdClientPort = 0;

  static async start() {
    const cluster = new RealCluster();
    try {
      await cluster.initialize();
      return cluster;
    } catch (error) {
      await cluster.stop();
      throw error;
    }
  }

  private async initialize() {
    mkdirSync(defaultArtifactsRoot, { recursive: true });
    this.runDir = await mkdtemp(resolve(defaultArtifactsRoot, "real-run-"));
    const staticDir = resolve(consoleRoot, "out");
    if (!statSync(resolve(staticDir, "index.html"), { throwIfNoEntry: false })?.isFile()) {
      throw new Error(`Console static export is missing; run the frontend build before the real suite: ${staticDir}`);
    }

    const ports = await Promise.all(Array.from({ length: 14 }, () => reservePort()));
    if (new Set(ports).size !== ports.length) throw new Error("loopback port allocation returned a duplicate");
    this.etcdClientPort = ports[9]!;
    this.clusterName = `console-e2e-${randomUUID()}`;
    await this.startEtcd(ports[10]!);
    for (const id of [1, 2, 3]) {
      const offset = (id - 1) * 3;
      const nodeRoot = resolve(this.runDir, `node-${id}`);
      const layout: NodeLayout = {
        id,
        clientPort: ports[offset]!,
        adminPort: ports[offset + 1]!,
        metricsPort: ports[offset + 2]!,
        proxyPort: ports[10 + id]!,
        dataDir: resolve(nodeRoot, "data"),
        bootJournalDir: resolve(nodeRoot, "boot-journal"),
      };
      mkdirSync(layout.dataDir, { recursive: true });
      mkdirSync(layout.bootJournalDir, { recursive: true });
      this.nodes.set(id, { layout, process: null, starts: 0 });
    }
    for (const node of this.nodes.values()) await this.startObservabilityProxy(node.layout);

    for (const id of [1, 2, 3]) this.startNode(id);
    await Promise.all([...this.nodes.values()].map((node) => this.waitForHttp(this.metricsUrl(node.layout, "/health"), `node ${node.layout.id} health`)));
    await this.waitForLeader([1, 2, 3]);
    await this.startConsole();
    await this.runWorkload(3);
    await this.triggerSnapshot();
    await this.startControlServer();
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

  private async startEtcd(peerPort: number) {
    await execFileAsync("docker", ["image", "inspect", etcdImage, "--format", "{{.Id}}"], {
      encoding: "utf8",
      timeout: 10_000,
    });
    this.etcdContainerName = `rr-${this.clusterName}`;
    this.processes.onExit(() => {
      if (this.etcdContainerName) spawnSync("docker", ["rm", "-f", this.etcdContainerName], { stdio: "ignore" });
    });
    this.processes.spawn("etcd", "docker", [
      "run", "--pull", "never", "--rm", "--name", this.etcdContainerName,
      "-p", `127.0.0.1:${this.etcdClientPort}:2379`,
      "-p", `127.0.0.1:${peerPort}:2380`,
      etcdImage,
      "/usr/local/bin/etcd",
      "--name=console-e2e",
      "--data-dir=/tmp/console-e2e.etcd",
      "--listen-client-urls=http://0.0.0.0:2379",
      `--advertise-client-urls=http://127.0.0.1:${this.etcdClientPort}`,
      "--listen-peer-urls=http://0.0.0.0:2380",
      `--initial-advertise-peer-urls=http://127.0.0.1:${peerPort}`,
      `--initial-cluster=console-e2e=http://127.0.0.1:${peerPort}`,
      "--initial-cluster-state=new",
    ], resolve(this.runDir, "logs", "etcd.log"));
    await this.waitForHttp(`http://127.0.0.1:${this.etcdClientPort}/health`, "etcd health");
  }

  private startNode(id: number) {
    const node = this.node(id);
    if (node.process?.running) throw new Error(`node ${id} is already running`);
    const { layout } = node;
    const clientAddress = this.clientAddress(layout);
    const adminAddress = `127.0.0.1:${layout.adminPort}`;
    const metricsAddress = `127.0.0.1:${layout.proxyPort}`;
    const clusterNodes = [...this.nodes.values()].map((candidate) => `${candidate.layout.id}@${this.clientAddress(candidate.layout)}`).join(",");
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
      "--environment", "console-e2e",
      "--region", "local",
      "--zone", `zone-${id}`,
      "--rack", "rack-1",
      "--app-version", "console-e2e",
      "--cluster-name", this.clusterName,
      "--cluster-nodes", clusterNodes,
      "--formation", "genesis",
      "--etcd-endpoints", `http://127.0.0.1:${this.etcdClientPort}`,
      "--directory-etcd-endpoints", `http://127.0.0.1:${this.etcdClientPort}`,
      "--storage", "rocks",
      "--durability", "strict",
      "--data-dir", layout.dataDir,
      "--boot-journal-dir", layout.bootJournalDir,
      "--allow-insecure",
      "--log-level", "info",
    ];
    node.starts += 1;
    node.process = this.processes.spawn(`node-${id}-boot-${node.starts}`, this.serverBinary, args, resolve(this.runDir, "logs", `node-${id}-boot-${node.starts}.log`));
    this.writeManifest();
  }

  private async startConsole() {
    if (this.consoleProcess?.running) throw new Error("Console is already running");
    this.consoleStarts += 1;
    const seeds = [1, 2].map((id) => `${id}@${this.adminAddress(id)}`).join(",");
    this.consoleProcess = this.processes.spawn(
      `console-${this.consoleStarts}`,
      this.adminBinary,
      [
        "console",
        "--listen", `127.0.0.1:${this.consoleAddress.port}`,
        "--peers", seeds,
        "--console-token-env", "RR_CONSOLE_E2E_TOKEN",
        "--allow-insecure",
        "--poll-interval-secs", "5",
        "--static-dir", resolve(consoleRoot, "out"),
      ],
      resolve(this.runDir, "logs", `console-${this.consoleStarts}.log`),
      { RR_CONSOLE_E2E_TOKEN: realViewerToken },
    );
    await this.waitForHttp(`${consoleBaseUrl}/login/`, "rr-admin Console");
  }

  async restartConsole() {
    if (this.consoleProcess) await this.consoleProcess.stop();
    await sleep(1_500);
    await this.startConsole();
  }

  private async runAdmin(address: string, args: string[]) {
    try {
      const result = await execFileAsync(this.adminBinary, ["--addr", address, "--output", "json", ...args], {
        encoding: "utf8",
        timeout: 30_000,
        maxBuffer: 4 * 1024 * 1024,
        env: process.env,
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
    throw new Error(`timed out waiting for a group 1 leader among nodes ${candidateIds.join(", ")}${excludedId ? ` excluding ${excludedId}` : ""}`);
  }

  private async waitForHttp(url: string, description: string) {
    const deadline = Date.now() + 45_000;
    let lastError: unknown;
    while (Date.now() < deadline) {
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

  async runWorkload(durationSeconds: number) {
    if (!Number.isInteger(durationSeconds) || durationSeconds < 1 || durationSeconds > 60) {
      throw new Error("workload duration must be an integer from 1 to 60 seconds");
    }
    const reportPath = resolve(this.runDir, `workload-${randomUUID()}.json`);
    const logPath = resolve(this.runDir, "logs", `workload-${randomUUID()}.log`);
    const workload = this.processes.spawn("workload", this.perfBenchBinary, [
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
      throw new Error(`workload timed out; see ${logPath}`);
    }
    if (outcome !== 0) throw new Error(`workload exited with status ${outcome}; see ${logPath}`);
    return reportPath;
  }

  async triggerSnapshot() {
    const leader = await this.waitForLeader([1, 2, 3]);
    await this.runAdmin(this.adminAddress(leader), ["group", "snapshot", "--group", "1"]);
    return leader;
  }

  async killLeader() {
    const leader = await this.waitForLeader([1, 2, 3]);
    const node = this.node(leader);
    if (!node.process) throw new Error(`leader node ${leader} has no managed process`);
    await node.process.stop("SIGKILL");
    this.writeManifest();
    return leader;
  }

  async restartNode(id: number) {
    const node = this.node(id);
    if (node.process?.running) throw new Error(`refusing to restart running node ${id}`);
    this.startNode(id);
    await this.waitForHttp(this.metricsUrl(node.layout, "/health"), `restarted node ${id} health`);
    await this.waitForLeader([1, 2, 3]);
    this.writeManifest();
  }

  private async startControlServer() {
    this.controlServer = createServer((request, response) => {
      void this.handleControl(request.method ?? "GET", request.url ?? "/").then((result) => {
        response.writeHead(200, { "content-type": "application/json; charset=utf-8", "cache-control": "no-store" });
        response.end(JSON.stringify(result));
      }).catch((error: unknown) => {
        response.writeHead(500, { "content-type": "application/json; charset=utf-8", "cache-control": "no-store" });
        response.end(JSON.stringify({ error: error instanceof Error ? error.message : String(error) }));
      });
    });
    await new Promise<void>((resolveListen, reject) => {
      this.controlServer!.once("error", reject);
      this.controlServer!.listen(this.controlAddress.port, "127.0.0.1", resolveListen);
    });
  }

  private setReadinessDegraded(id: number, degraded: boolean) {
    const proxy = this.observabilityProxies.get(id);
    if (!proxy) throw new Error(`unknown observability proxy ${id}`);
    proxy.degraded = degraded;
    return { node_id: id, degraded };
  }

  private async handleControl(method: string, path: string) {
    if (method === "GET" && path === "/health") return { ok: true };
    if (method === "POST" && path === "/leader/kill") return { node_id: await this.killLeader() };
    if (method === "POST" && path === "/console/restart") {
      await this.restartConsole();
      return { restarted: true };
    }
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
    throw new Error(`unknown real E2E control action: ${method} ${path}`);
  }

  private writeManifest() {
    if (!this.runDir) return;
    const manifest = {
      run_id: resolve(this.runDir).split("/").at(-1),
      console_url: consoleBaseUrl,
      control_url: controlBaseUrl,
      cluster_name: this.clusterName,
      directory_endpoint: `http://127.0.0.1:${this.etcdClientPort}`,
      admin_seeds: [1, 2].map((id) => `${id}@${this.adminAddress(id)}`),
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
    writeFileSync(resolve(this.runDir, "manifest.json"), `${JSON.stringify(manifest, null, 2)}\n`, { mode: 0o600 });
  }

  private async stopEtcd() {
    if (!this.etcdContainerName) return;
    try {
      await execFileAsync("docker", ["rm", "-f", this.etcdContainerName], { encoding: "utf8", timeout: 15_000 });
    } catch {
      // docker run --rm may already have removed the container during graceful child shutdown.
    }
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
    await this.stopObservabilityProxies();
    if (this.controlServer) {
      this.controlServer.closeAllConnections();
      await new Promise<void>((resolveClose) => this.controlServer!.close(() => resolveClose()));
      this.controlServer = null;
    }
    await this.processes.stopAll();
    await this.stopEtcd();
    this.writeManifest();
  }
}
