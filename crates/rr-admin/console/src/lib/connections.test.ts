import { makeMeta } from "@/test/fixtures";
import type { ConsoleMeta } from "@/lib/contract";
import type { ConnectionInput, ConnectionSummary } from "@/lib/connections";

const neutralMeta: ConsoleMeta = makeMeta({
  cluster_id: null,
  retention_source: "NONE",
  sources: [],
  capabilities: [],
});

const summary: ConnectionSummary = {
  id: "cluster-a",
  name: "Cluster A",
  environment: "test",
  admin_seeds: [{ node_id: 1, admin_addr: "127.0.0.1:52001" }],
  discovery: null,
  cluster_name: "cluster-a",
  admin_tls: { ca: null, cert: null, key: null, domain: null },
  discovery_tls: { ca: null, cert: null, key: null, domain: null },
  discovery_username: null,
  discovery_allow_insecure: false,
  prometheus: null,
  poll_interval_secs: 5,
  created_at: "2026-11-18T08:00:00Z",
  updated_at: "2026-11-18T08:00:00Z",
  observed_cluster_id: "cluster-a",
  last_connected_at: "2026-11-18T08:01:00Z",
  source: "PROFILE",
  status: "CONNECTED",
  credentials_configured: true,
  credential_source: "MEMORY",
  last_error: null,
  node_count: 1,
  group_count: 2,
};

const input: ConnectionInput = {
  name: "Cluster A",
  environment: "test",
  admin_seeds: [{ node_id: 1, admin_addr: "127.0.0.1:52001" }],
  discovery: "etcd://127.0.0.1:2379",
  cluster_name: "cluster-a",
  admin_tls: { ca: "/tmp/ca.pem", cert: null, key: null, domain: "admin.test" },
  discovery_tls: { ca: null, cert: null, key: null, domain: null },
  discovery_username: "viewer",
  discovery_allow_insecure: false,
  prometheus: { url: "http://127.0.0.1:9090", cluster: "cluster-a" },
  poll_interval_secs: 5,
  admin_token: "admin-secret",
  discovery_password: "discovery-secret",
  remember: false,
};

function jsonResponse(data: unknown, status = 200): Response {
  return new Response(JSON.stringify({ meta: neutralMeta, data }), {
    status,
    headers: { "content-type": "application/json" },
  });
}

async function loadConnectionsApi() {
  vi.resetModules();
  return (await import("@/lib/connections")).connectionsApi;
}

describe("typed connections client", () => {
  afterEach(() => vi.unstubAllGlobals());

  it("lists and gets profiles through the connection management routes", async () => {
    const connectionsApi = await loadConnectionsApi();
    const fetchMock = vi.fn()
      .mockResolvedValueOnce(jsonResponse({ items: [summary], keychain_available: true }))
      .mockResolvedValueOnce(jsonResponse(summary));
    vi.stubGlobal("fetch", fetchMock);

    await expect(connectionsApi.list()).resolves.toMatchObject({
      data: { items: [summary], keychain_available: true },
    });
    await expect(connectionsApi.get("cluster-a")).resolves.toMatchObject({ data: summary });
    expect(fetchMock).toHaveBeenNthCalledWith(
      1,
      "/api/v1/console/connections",
      expect.objectContaining({ method: "GET", credentials: "same-origin", cache: "no-store" }),
    );
    expect(fetchMock).toHaveBeenNthCalledWith(
      2,
      "/api/v1/console/connections/cluster-a",
      expect.objectContaining({ method: "GET" }),
    );
  });

  it("sends nested mutation bodies with CSRF and accepts DELETE 204", async () => {
    const connectionsApi = await loadConnectionsApi();
    const fetchMock = vi.fn()
      .mockResolvedValueOnce(new Response(JSON.stringify({
        csrf_token: "connection-csrf",
        expires_at: "2026-11-18T08:15:00Z",
      }), { status: 200, headers: { "content-type": "application/json" } }))
      .mockResolvedValueOnce(jsonResponse(summary, 201))
      .mockResolvedValueOnce(jsonResponse({ tested: true }))
      .mockResolvedValueOnce(jsonResponse(summary))
      .mockResolvedValueOnce(jsonResponse({ tested: true }))
      .mockResolvedValueOnce(jsonResponse(summary))
      .mockResolvedValueOnce(jsonResponse(summary))
      .mockResolvedValueOnce(new Response(null, { status: 204 }));
    vi.stubGlobal("fetch", fetchMock);

    await connectionsApi.create(input);
    await connectionsApi.testCandidate(input);
    await connectionsApi.update("cluster-a", input);
    await connectionsApi.test("cluster-a");
    await connectionsApi.connect("cluster-a");
    await connectionsApi.disconnect("cluster-a");
    await expect(connectionsApi.remove("cluster-a")).resolves.toBeUndefined();

    expect(fetchMock).toHaveBeenCalledTimes(8);
    expect(fetchMock).toHaveBeenNthCalledWith(
      1,
      "/api/v1/console/auth/csrf",
      expect.objectContaining({ method: "GET", credentials: "same-origin" }),
    );
    const mutations = fetchMock.mock.calls.slice(1);
    expect(mutations.map(([url, init]) => [url, init?.method])).toEqual([
      ["/api/v1/console/connections", "POST"],
      ["/api/v1/console/connections/test", "POST"],
      ["/api/v1/console/connections/cluster-a", "PATCH"],
      ["/api/v1/console/connections/cluster-a/test", "POST"],
      ["/api/v1/console/connections/cluster-a/connect", "POST"],
      ["/api/v1/console/connections/cluster-a/disconnect", "POST"],
      ["/api/v1/console/connections/cluster-a", "DELETE"],
    ]);
    for (const [, init] of mutations) {
      expect(init).toEqual(expect.objectContaining({
        credentials: "same-origin",
        headers: expect.objectContaining({ "X-Console-CSRF": "connection-csrf" }),
      }));
    }
    expect(JSON.parse(String(mutations[0]?.[1]?.body))).toEqual(input);
    expect(JSON.parse(String(mutations[1]?.[1]?.body))).toEqual(input);
    expect(JSON.parse(String(mutations[2]?.[1]?.body))).toEqual(input);
    expect(mutations[0]?.[1]?.body).toContain('"admin_tls":{"ca":"/tmp/ca.pem"');
    expect(mutations[0]?.[1]?.body).toContain('"prometheus":{"url":"http://127.0.0.1:9090"');
  });

  it("passes an existing connection ID only for candidate secret inheritance", async () => {
    const connectionsApi = await loadConnectionsApi();
    const fetchMock = vi.fn()
      .mockResolvedValueOnce(new Response(JSON.stringify({ csrf_token: null }), {
        status: 200,
        headers: { "content-type": "application/json" },
      }))
      .mockResolvedValueOnce(jsonResponse({ tested: true }));
    vi.stubGlobal("fetch", fetchMock);

    await connectionsApi.testCandidate(input, "cluster-a");

    const body = JSON.parse(String(fetchMock.mock.calls[1]?.[1]?.body));
    expect(body).toEqual({ ...input, existing_connection_id: "cluster-a" });
  });

  it("returns unauthorized callers to the complete current URL", async () => {
    const connectionsApi = await loadConnectionsApi();
    window.history.replaceState({}, "", "/groups/?window=1h&group_id=3&connection=cluster-a");
    const assign = vi.fn();
    vi.stubGlobal("location", { ...window.location, assign });
    vi.stubGlobal("fetch", vi.fn().mockResolvedValue(new Response(JSON.stringify({
      meta: neutralMeta,
      error: { code: "UNAUTHENTICATED", message: "Viewer session required" },
    }), { status: 401, headers: { "content-type": "application/json" } })));

    await expect(connectionsApi.list()).rejects.toMatchObject({ status: 401 });
    expect(assign).toHaveBeenCalledWith(
      "/login/?next=%2Fgroups%2F%3Fwindow%3D1h%26group_id%3D3%26connection%3Dcluster-a",
    );
  });

  it("shares the login CSRF token instead of maintaining a second cache", async () => {
    vi.resetModules();
    const { consoleApi } = await import("@/lib/api");
    const { connectionsApi: client } = await import("@/lib/connections");
    const fetchMock = vi.fn()
      .mockResolvedValueOnce(new Response(JSON.stringify({
        meta: neutralMeta,
        csrf_token: "login-connection-csrf",
        expires_at: "2026-11-18T08:15:00Z",
      }), { status: 200, headers: { "content-type": "application/json" } }))
      .mockResolvedValueOnce(jsonResponse(summary, 201));
    vi.stubGlobal("fetch", fetchMock);

    await consoleApi.login("viewer-credential");
    await client.create(input);

    expect(fetchMock).toHaveBeenCalledTimes(2);
    expect(fetchMock).toHaveBeenNthCalledWith(
      2,
      "/api/v1/console/connections",
      expect.objectContaining({
        headers: expect.objectContaining({ "X-Console-CSRF": "login-connection-csrf" }),
      }),
    );
  });

  it("does not reuse a connection mutation CSRF token after logout", async () => {
    vi.resetModules();
    const { consoleApi } = await import("@/lib/api");
    const { connectionsApi: client } = await import("@/lib/connections");
    const fetchMock = vi.fn()
      .mockResolvedValueOnce(new Response(JSON.stringify({ csrf_token: "old-csrf" }), {
        status: 200,
        headers: { "content-type": "application/json" },
      }))
      .mockResolvedValueOnce(jsonResponse(summary, 201))
      .mockResolvedValueOnce(jsonResponse({ authenticated: false }))
      .mockResolvedValueOnce(new Response(JSON.stringify({ csrf_token: "new-csrf" }), {
        status: 200,
        headers: { "content-type": "application/json" },
      }))
      .mockResolvedValueOnce(jsonResponse(summary, 201));
    vi.stubGlobal("fetch", fetchMock);

    await client.create(input);
    await consoleApi.logout();
    await client.create(input);

    expect(fetchMock).toHaveBeenCalledTimes(5);
    expect(fetchMock).toHaveBeenNthCalledWith(
      4,
      "/api/v1/console/auth/csrf",
      expect.objectContaining({ method: "GET" }),
    );
    expect(fetchMock).toHaveBeenNthCalledWith(
      5,
      "/api/v1/console/connections",
      expect.objectContaining({
        headers: expect.objectContaining({ "X-Console-CSRF": "new-csrf" }),
      }),
    );
  });

  it("caches auth-disabled CSRF capability without sending a header", async () => {
    vi.resetModules();
    const { connectionsApi: client } = await import("@/lib/connections");
    const fetchMock = vi.fn()
      .mockResolvedValueOnce(new Response(JSON.stringify({ csrf_token: null, expires_at: null }), {
        status: 200,
        headers: { "content-type": "application/json" },
      }))
      .mockResolvedValueOnce(jsonResponse(summary, 201))
      .mockResolvedValueOnce(jsonResponse(summary));
    vi.stubGlobal("fetch", fetchMock);

    await client.create(input);
    await client.update("cluster-a", input);

    expect(fetchMock).toHaveBeenCalledTimes(3);
    for (const [, init] of fetchMock.mock.calls.slice(1)) {
      expect(init?.headers).not.toHaveProperty("X-Console-CSRF");
    }
  });

  it("redirects once without retrying when a mutation is unauthorized", async () => {
    vi.resetModules();
    const { connectionsApi: client } = await import("@/lib/connections");
    window.history.replaceState({}, "", "/nodes/?window=6h&connection=cluster-a");
    const assign = vi.fn();
    vi.stubGlobal("location", { ...window.location, assign });
    const fetchMock = vi.fn()
      .mockResolvedValueOnce(new Response(JSON.stringify({ csrf_token: "mutation-csrf" }), {
        status: 200,
        headers: { "content-type": "application/json" },
      }))
      .mockResolvedValueOnce(new Response(JSON.stringify({
        meta: neutralMeta,
        error: { code: "UNAUTHENTICATED", message: "Viewer session required" },
      }), { status: 401, headers: { "content-type": "application/json" } }));
    vi.stubGlobal("fetch", fetchMock);

    await expect(client.create(input)).rejects.toMatchObject({ status: 401 });
    expect(fetchMock).toHaveBeenCalledTimes(2);
    expect(assign).toHaveBeenCalledTimes(1);
    expect(assign).toHaveBeenCalledWith(
      "/login/?next=%2Fnodes%2F%3Fwindow%3D6h%26connection%3Dcluster-a",
    );
  });

  it("uses the same full-URL return when CSRF restoration is unauthorized", async () => {
    vi.resetModules();
    const { connectionsApi: client } = await import("@/lib/connections");
    window.history.replaceState({}, "", "/groups/?window=1h&connection=cluster-a");
    const assign = vi.fn();
    vi.stubGlobal("location", { ...window.location, assign });
    const fetchMock = vi.fn().mockResolvedValue(new Response(JSON.stringify({
      meta: neutralMeta,
      error: { code: "UNAUTHENTICATED", message: "Viewer session required" },
    }), { status: 401, headers: { "content-type": "application/json" } }));
    vi.stubGlobal("fetch", fetchMock);

    await expect(client.create(input)).rejects.toMatchObject({ status: 401 });
    expect(fetchMock).toHaveBeenCalledTimes(1);
    expect(assign).toHaveBeenCalledTimes(1);
    expect(assign).toHaveBeenCalledWith(
      "/login/?next=%2Fgroups%2F%3Fwindow%3D1h%26connection%3Dcluster-a",
    );
  });

  it("preserves secret keep/clear/replace inputs and encodes IDs as one path segment", async () => {
    const connectionsApi = await loadConnectionsApi();
    const fetchMock = vi.fn()
      .mockResolvedValueOnce(new Response(JSON.stringify({ csrf_token: null }), {
        status: 200,
        headers: { "content-type": "application/json" },
      }))
      .mockResolvedValueOnce(jsonResponse(summary, 201))
      .mockResolvedValueOnce(jsonResponse(summary));
    vi.stubGlobal("fetch", fetchMock);

    await connectionsApi.create({ name: "Tri-state", discovery_password: null });
    await connectionsApi.get("cluster/a+b");

    const body = JSON.parse(String(fetchMock.mock.calls[1]?.[1]?.body));
    expect(body).not.toHaveProperty("admin_token");
    expect(body).toHaveProperty("discovery_password", null);
    expect(fetchMock).toHaveBeenNthCalledWith(
      3,
      "/api/v1/console/connections/cluster%2Fa%2Bb",
      expect.objectContaining({ method: "GET" }),
    );
  });

  it("keeps typed sanitized errors for failed deletes", async () => {
    vi.resetModules();
    const { connectionsApi: client } = await import("@/lib/connections");
    const fetchMock = vi.fn()
      .mockResolvedValueOnce(new Response(JSON.stringify({ csrf_token: null }), {
        status: 200,
        headers: { "content-type": "application/json" },
      }))
      .mockResolvedValueOnce(new Response(JSON.stringify({
        meta: makeMeta({ correlation_id: "corr-delete" }),
        error: { code: "CONNECTION_STORE_FAILED", message: "token=secret store failed" },
      }), { status: 500, headers: { "content-type": "application/json" } }));
    vi.stubGlobal("fetch", fetchMock);

    const request = client.remove("cluster-a");
    await expect(request).rejects.toMatchObject({
      status: 500,
      code: "CONNECTION_STORE_FAILED",
      correlationId: "corr-delete",
    });
    await request.catch((error: Error) => {
      expect(error.message).not.toContain("secret");
    });
  });
});
