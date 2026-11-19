import { expect, test } from "@playwright/test";
import { createServer } from "node:http";
import {
  consoleBaseUrl,
  controlBaseUrl,
  isolatedProcessEnvironment,
  RealHarness,
} from "./harness";

const poisonedEnvironment = [
  "RR_CONFIG",
  "RR_FORMATION",
  "RR_RAFT_TRANSPORT",
  "RR_RAFT_TCP_PORT",
  "RR_ETCD_CA",
  "RR_ETCD_CERT",
  "RR_ETCD_KEY",
  "RR_ETCD_DOMAIN",
  "RR_ETCD_USERNAME",
  "RR_ETCD_PASSWORD_ENV",
  "RR_GROUPS",
  "RR_TLS_CERT",
  "RR_ADMIN_TOKEN",
] as const;

let harness: RealHarness | null = null;
const previousEnvironment = new Map<string, string | undefined>();

test.describe.serial("real harness lifecycle", () => {
  test.beforeAll(async () => {
    for (const name of poisonedEnvironment) {
      previousEnvironment.set(name, process.env[name]);
      process.env[name] = name === "RR_RAFT_TRANSPORT" ? "tcp" : "poisoned";
    }
    harness = await RealHarness.start();
  });

  test.afterAll(async () => {
    if (harness) await harness.stop();
    for (const [name, value] of previousEnvironment) {
      if (value === undefined) delete process.env[name];
      else process.env[name] = value;
    }
  });

  test("uses dynamic identified endpoints and an RR-free child environment", async () => {
    const environment = isolatedProcessEnvironment({ RR_CONSOLE_E2E_TOKEN: "viewer" });
    for (const name of poisonedEnvironment) expect(environment).not.toHaveProperty(name);
    expect(environment.RR_CONSOLE_E2E_TOKEN).toBe("viewer");
    expect(environment.PATH).toBe(process.env.PATH);

    const consolePort = Number(new URL(consoleBaseUrl).port);
    const controlPort = Number(new URL(controlBaseUrl).port);
    expect(consolePort).toBeGreaterThan(0);
    expect(controlPort).toBeGreaterThan(0);
    expect(consolePort).not.toBe(controlPort);
    expect(process.env.RR_CONSOLE_E2E_BASE_URL).toBe(consoleBaseUrl);
    expect(process.env.RR_CONSOLE_E2E_CONTROL_URL).toBe(controlBaseUrl);

    const current = harness!.manifest();
    expect(current.console.running).toBe(true);
    expect(current.console.args).toContain("--keychain-disable");
    expect(current.console.args).toContain(`127.0.0.1:${consolePort}`);
    const healthResponse = await fetch(`${controlBaseUrl}/health`);
    expect(healthResponse.ok).toBe(true);
    const health = await healthResponse.json();
    expect(health).toMatchObject({
      ok: true,
      control_id: current.control_identity,
      run_id: current.run_id,
    });

    const consoleIdentity = await fetch(`${consoleBaseUrl}/api/v1/console/connections`);
    expect(consoleIdentity.status).toBe(401);
    expect((await consoleIdentity.json()).meta.schema_version).toBe("console.v1");
  });

  test("waits for an exceptional in-flight restart and finally stops every child", async () => {
    const restartRequest = fetch(`${controlBaseUrl}/console/restart`, { method: "POST" });
    await expect.poll(async () => {
      const response = await fetch(`${controlBaseUrl}/health`);
      if (!response.ok) return 0;
      return (await response.json()).active_control_actions ?? 0;
    }, { timeout: 10_000 }).toBeGreaterThan(1);

    const stopping = harness!.stop();
    await expect(harness!.restartConsole()).rejects.toThrow("stopped");
    const restartResponse = await restartRequest;
    expect(restartResponse.status).toBe(500);
    await stopping;
    await harness!.stop();

    const stopped = harness!.manifest();
    expect(stopped.in_flight_control_actions).toBe(0);
    expect(stopped.console.running).toBe(false);
    for (const cluster of Object.values(stopped.clusters)) {
      expect(cluster).not.toBeNull();
      expect(cluster!.nodes).toHaveLength(3);
      expect(cluster!.nodes.every((node) => !node.running)).toBe(true);
    }
  });

  test("rejects a foreign Console listener before HTTP and cleans only this run", async () => {
    const port = Number(new URL(consoleBaseUrl).port);
    let fakeRequests = 0;
    const fakeConsole = createServer((_request, response) => {
      fakeRequests += 1;
      response.writeHead(401, { "content-type": "application/json; charset=utf-8" });
      response.end(JSON.stringify({
        meta: { schema_version: "console.v1" },
        error: { code: "UNAUTHENTICATED" },
      }));
    });
    await new Promise<void>((resolveListen, reject) => {
      fakeConsole.once("error", reject);
      fakeConsole.listen(port, "127.0.0.1", resolveListen);
    });
    const candidate = new RealHarness();
    try {
      await expect(RealHarness.start(candidate)).rejects.toThrow(/ownership mismatch.*refusing to contact the listener/);
      expect(fakeRequests, "readiness must reject the foreign PID before any HTTP handshake").toBe(0);

      const stopped = candidate.manifest();
      expect(stopped.console.starts).toBe(1);
      expect(stopped.console.running).toBe(false);
      expect(stopped.in_flight_control_actions).toBe(0);
      for (const cluster of Object.values(stopped.clusters)) {
        expect(cluster).not.toBeNull();
        expect(cluster!.nodes).toHaveLength(3);
        expect(cluster!.nodes.every((node) => !node.running)).toBe(true);
      }

      const foreignResponse = await fetch(`${consoleBaseUrl}/api/v1/console/connections`);
      expect(foreignResponse.status).toBe(401);
      expect(fakeRequests).toBe(1);
    } finally {
      await candidate.stop();
      fakeConsole.closeAllConnections();
      await new Promise<void>((resolveClose) => fakeConsole.close(() => resolveClose()));
    }
  });
});
