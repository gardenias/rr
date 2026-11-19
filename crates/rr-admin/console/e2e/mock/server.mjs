import { createServer } from "node:http";
import { randomUUID } from "node:crypto";
import { readFile, stat } from "node:fs/promises";
import { extname, resolve, sep } from "node:path";
import { fileURLToPath } from "node:url";
import { envelope, makeMeta, responseFor } from "./fixtures.mjs";

const root = fileURLToPath(new URL("../../out", import.meta.url));
const port = Number(process.env.PORT ?? 4310);
const viewerToken = process.env.MOCK_VIEWER_TOKEN ?? "mock-viewer-token";
const sessions = new Set();
const streams = new Set();
const streamIntervals = new Map();
const streamConnections = new Map();
const connections = new Map();
const connectionCredentials = new Map();
let connectionSequence = 0;
const mimeTypes = {
  ".css": "text/css; charset=utf-8",
  ".html": "text/html; charset=utf-8",
  ".js": "text/javascript; charset=utf-8",
  ".json": "application/json; charset=utf-8",
  ".svg": "image/svg+xml",
  ".woff2": "font/woff2",
};

function json(response, status, body, headers = {}) {
  response.writeHead(status, { "content-type": "application/json; charset=utf-8", "cache-control": "no-store", ...headers });
  response.end(JSON.stringify(body));
}

function apiError(response, pathname, status, code, message) {
  json(response, status, { meta: makeMeta(pathname), error: { code, message } });
}

async function readBody(request) {
  let body = "";
  for await (const chunk of request) {
    body += chunk;
    if (body.length > 16_384) throw new Error("Request body too large");
  }
  return JSON.parse(body || "{}");
}

function authenticated(request) {
  if (process.env.MOCK_REQUIRE_AUTH !== "1") return true;
  const cookie = request.headers.cookie ?? "";
  const match = cookie.match(/(?:^|;\s*)rr_console_session=([^;]+)/);
  return match ? sessions.has(match[1]) : false;
}

function cleanupStream(response) {
  const interval = streamIntervals.get(response);
  if (interval) clearInterval(interval);
  streamIntervals.delete(response);
  streamConnections.delete(response);
  streams.delete(response);
}

function disconnectStreams(connectionId = null) {
  for (const response of [...streams]) {
    if (connectionId && streamConnections.get(response) !== connectionId) continue;
    cleanupStream(response);
    response.end();
  }
}

function connectionEnvelope(pathname, data) {
  return envelope(pathname, data, {
    cluster_id: null,
    retention_source: "NONE",
    sources: [],
    capabilities: [],
  });
}

function connectionSummary(profile) {
  const credentials = connectionCredentials.get(profile.id) ?? { admin: false, discovery: false };
  const sources = [credentials.admin ? "MEMORY" : null, credentials.discovery ? "MEMORY" : null].filter(Boolean);
  return {
    ...profile,
    credentials_configured: sources.length > 0,
    credential_source: sources.length > 0 ? "MEMORY" : null,
  };
}

function normalizedConnectionInput(input) {
  return {
    name: String(input.name ?? "").trim(),
    environment: input.environment ?? null,
    admin_seeds: Array.isArray(input.admin_seeds) ? input.admin_seeds : [],
    discovery: input.discovery ?? null,
    cluster_name: input.cluster_name ?? null,
    admin_tls: input.admin_tls ?? { ca: null, cert: null, key: null, domain: null },
    discovery_tls: input.discovery_tls ?? { ca: null, cert: null, key: null, domain: null },
    discovery_username: input.discovery_username ?? null,
    discovery_allow_insecure: input.discovery_allow_insecure ?? false,
    prometheus: input.prometheus ?? null,
    poll_interval_secs: input.poll_interval_secs ?? 5,
  };
}

function updateCredentialCapabilities(id, input, create = false) {
  const current = create ? { admin: false, discovery: false } : connectionCredentials.get(id) ?? { admin: false, discovery: false };
  const next = { ...current };
  if (Object.hasOwn(input, "admin_token")) next.admin = typeof input.admin_token === "string" && input.admin_token.length > 0;
  if (Object.hasOwn(input, "discovery_password")) next.discovery = typeof input.discovery_password === "string" && input.discovery_password.length > 0;
  connectionCredentials.set(id, next);
}

function validHostPort(value) {
  if (!value || value.includes("://") || value.includes("@") || /\s/.test(value)) return false;
  const separator = value.lastIndexOf(":");
  if (separator < 1) return false;
  const host = value.slice(0, separator).replace(/^\[|\]$/g, "");
  const rawPort = value.slice(separator + 1);
  const port = Number(rawPort);
  return Boolean(host) && host !== "0.0.0.0" && host !== "::"
    && /^\d+$/.test(rawPort) && Number.isInteger(port) && port >= 1 && port <= 65535;
}

function validDiscoveryEndpoints(value) {
  const input = value.startsWith("etcd://") ? value.slice("etcd://".length) : "";
  if (!input) return false;
  const seen = new Set();
  for (const endpoint of input.split(",")) {
    const normalized = endpoint.trim();
    if (!validHostPort(normalized) || seen.has(normalized)) return false;
    seen.add(normalized);
  }
  return true;
}

function validTls(tls) {
  return Boolean(tls.cert) === Boolean(tls.key)
    && (!tls.cert || Boolean(tls.ca))
    && (!tls.domain || Boolean(tls.ca));
}

function validPrometheus(prometheus) {
  if (!prometheus) return true;
  try {
    const url = new URL(prometheus.url);
    const authority = prometheus.url.split("://")[1]?.split(/[/?#]/)[0] ?? "";
    return (url.protocol === "http:" || url.protocol === "https:") && Boolean(url.hostname)
      && !url.username && !url.password && !authority.includes("@") && !url.search && !url.hash;
  } catch {
    return false;
  }
}

function validConnectionProfile(profile) {
  const ids = new Set();
  const addresses = new Set();
  const seedsValid = profile.admin_seeds.every((seed) =>
    Number.isSafeInteger(seed.node_id) && seed.node_id > 0 && validHostPort(seed.admin_addr)
    && !ids.has(seed.node_id) && !addresses.has(seed.admin_addr)
    && (ids.add(seed.node_id), addresses.add(seed.admin_addr), true));
  if (!profile.name || (!profile.admin_seeds.length && !profile.discovery)
    || !Number.isInteger(profile.poll_interval_secs) || profile.poll_interval_secs < 1
    || profile.poll_interval_secs > 300 || !seedsValid || !validTls(profile.admin_tls)
    || !validTls(profile.discovery_tls) || !validPrometheus(profile.prometheus)) return false;
  if (!profile.discovery) return true;
  const hasCa = Boolean(profile.discovery_tls.ca);
  return validDiscoveryEndpoints(profile.discovery) && Boolean(profile.cluster_name)
    && hasCa !== profile.discovery_allow_insecure
    && (!profile.discovery_username || hasCa);
}

async function handleConnections(request, response, pathname) {
  if (pathname === "/connections" && request.method === "GET") {
    return json(response, 200, connectionEnvelope(pathname, {
      items: [...connections.values()].map(connectionSummary),
      keychain_available: process.env.MOCK_KEYCHAIN_AVAILABLE !== "0",
    }));
  }
  if (pathname === "/connections" && request.method === "POST") {
    const input = await readBody(request);
    const profile = normalizedConnectionInput(input);
    if (!validConnectionProfile(profile)) {
      return apiError(response, pathname, 400, "INVALID_CONNECTION_PROFILE", "Connection profile is invalid");
    }
    const now = new Date().toISOString();
    const id = `connection-${++connectionSequence}`;
    connections.set(id, {
      id,
      ...profile,
      created_at: now,
      updated_at: now,
      observed_cluster_id: null,
      last_connected_at: null,
      source: "PROFILE",
      status: "DISCONNECTED",
      last_error: null,
      node_count: null,
      group_count: null,
    });
    updateCredentialCapabilities(id, input, true);
    return json(response, 201, connectionEnvelope(pathname, connectionSummary(connections.get(id))));
  }
  if (pathname === "/connections/test" && request.method === "POST") {
    const input = await readBody(request);
    const profile = normalizedConnectionInput(input);
    if (!validConnectionProfile(profile)) {
      return apiError(response, pathname, 400, "INVALID_CONNECTION_PROFILE", "Connection profile is invalid");
    }
    if (input.existing_connection_id && !connections.has(input.existing_connection_id)) {
      return apiError(response, pathname, 404, "CONNECTION_NOT_FOUND", "Connection not found");
    }
    if (profile.discovery_username && !Object.hasOwn(input, "discovery_password")
      && !input.existing_connection_id) {
      return apiError(response, pathname, 409, "CREDENTIALS_REQUIRED", "Discovery credentials are required");
    }
    if (input.discovery === "etcd://test-failure:2379") {
      return apiError(response, pathname, 502, "CONNECTION_TEST_FAILED", "Connection test failed: discovery authentication was rejected");
    }
    return json(response, 200, connectionEnvelope(pathname, { tested: true }));
  }

  const match = /^\/connections\/([^/]+)(?:\/(test|connect|disconnect))?$/.exec(pathname);
  if (!match) return apiError(response, pathname, 404, "CONNECTION_NOT_FOUND", "Connection not found");
  const id = decodeURIComponent(match[1]);
  const action = match[2] ?? null;
  const existing = connections.get(id);
  if (!existing) return apiError(response, pathname, 404, "CONNECTION_NOT_FOUND", "Connection not found");

  if (!action && request.method === "GET") {
    return json(response, 200, connectionEnvelope(pathname, connectionSummary(existing)));
  }
  if (!action && request.method === "PATCH") {
    if (existing.status !== "DISCONNECTED") {
      return apiError(response, pathname, 409, "CONNECTION_ACTIVE", "Disconnect before editing this connection");
    }
    const input = await readBody(request);
    const profile = normalizedConnectionInput(input);
    if (!validConnectionProfile(profile)) {
      return apiError(response, pathname, 400, "INVALID_CONNECTION_PROFILE", "Connection profile is invalid");
    }
    const updated = { ...existing, ...profile, updated_at: new Date().toISOString() };
    connections.set(id, updated);
    updateCredentialCapabilities(id, input);
    return json(response, 200, connectionEnvelope(pathname, connectionSummary(updated)));
  }
  if (!action && request.method === "DELETE") {
    if (existing.name === "Delete failure") {
      return apiError(response, pathname, 500, "CONNECTION_STORE_FAILED", "Profile store is read-only");
    }
    connections.delete(id);
    connectionCredentials.delete(id);
    disconnectStreams(id);
    response.writeHead(204, { "cache-control": "no-store" });
    response.end();
    return;
  }
  if (action === "test" && request.method === "POST") {
    if (existing.discovery === "etcd://test-failure:2379") {
      return apiError(response, pathname, 502, "CONNECTION_TEST_FAILED", "Connection test failed: discovery authentication was rejected");
    }
    return json(response, 200, connectionEnvelope(pathname, { tested: true }));
  }
  if (action === "connect" && request.method === "POST") {
    if (existing.discovery === "etcd://connect-failure:2379") {
      const failed = { ...existing, status: "DISCONNECTED", last_error: "No admin endpoint responded", node_count: null, group_count: null };
      connections.set(id, failed);
      return apiError(response, pathname, 502, "CONNECTION_INITIALIZATION_FAILED", "No admin endpoint responded");
    }
    const connected = {
      ...existing,
      status: "CONNECTED",
      last_error: null,
      last_connected_at: new Date().toISOString(),
      observed_cluster_id: `observed-${id}`,
      node_count: 3,
      group_count: 2,
    };
    connections.set(id, connected);
    return json(response, 200, connectionEnvelope(pathname, connectionSummary(connected)));
  }
  if (action === "disconnect" && request.method === "POST") {
    disconnectStreams(id);
    const disconnected = { ...existing, status: "DISCONNECTED", node_count: null, group_count: null };
    connections.set(id, disconnected);
    return json(response, 200, connectionEnvelope(pathname, connectionSummary(disconnected)));
  }
  return apiError(response, pathname, 405, "METHOD_NOT_ALLOWED", "Method not allowed");
}

function scopedPayload(connection, resourcePath, searchParams) {
  const payload = responseFor(resourcePath, searchParams);
  if (!payload) return null;
  const scoped = structuredClone(payload);
  scoped.meta.cluster_id = connection.observed_cluster_id;
  if (resourcePath === "/summary") {
    scoped.data.cluster_name = connection.name;
    scoped.data.environment = connection.environment ?? "test";
  }
  return scoped;
}

function openScopedStream(request, response, connection) {
  response.writeHead(200, {
    "content-type": "text/event-stream",
    "cache-control": "no-cache",
    connection: "keep-alive",
    "x-accel-buffering": "no",
  });
  const sendSummary = () => {
    const summary = scopedPayload(connection, "/summary", new URLSearchParams({ window: "15m" }));
    response.write(`event: summary\ndata: ${JSON.stringify(summary)}\n\n`);
  };
  sendSummary();
  const interval = setInterval(() => {
    if (response.writableEnded || response.destroyed) return cleanupStream(response);
    sendSummary();
  }, 2_000);
  streams.add(response);
  streamIntervals.set(response, interval);
  streamConnections.set(response, connection.id);
  request.once("close", () => cleanupStream(response));
  response.once("close", () => cleanupStream(response));
  response.once("error", () => cleanupStream(response));
}

function resyncStreams(connectionId = null) {
  for (const response of streams) {
    if (connectionId && streamConnections.get(response) !== connectionId) continue;
    response.write(`event: resync\ndata: ${JSON.stringify({ reason: "LAGGED", skipped: 1 })}\n\n`);
  }
}

async function handleApi(request, response, url) {
  const pathname = url.pathname.slice("/api/v1/console".length);
  if (pathname === "/auth/login" && request.method === "POST") {
    try {
      const body = await readBody(request);
      if (body.token !== viewerToken) return apiError(response, pathname, 401, "UNAUTHENTICATED", "Sign-in failed");
      const session = randomUUID();
      sessions.add(session);
      return json(response, 200, {
        meta: makeMeta(pathname),
        csrf_token: randomUUID(),
        expires_at: new Date(Date.now() + 900_000).toISOString(),
      }, {
        "set-cookie": `rr_console_session=${session}; Path=/; HttpOnly; SameSite=Strict; Max-Age=900`,
      });
    } catch {
      return apiError(response, pathname, 400, "INVALID_REQUEST", "Invalid sign-in request");
    }
  }
  if (pathname === "/auth/logout" && request.method === "POST") {
    const cookie = request.headers.cookie ?? "";
    const match = cookie.match(/(?:^|;\s*)rr_console_session=([^;]+)/);
    if (match) sessions.delete(match[1]);
    return json(response, 200, envelope(pathname, { authenticated: false }), {
      "set-cookie": "rr_console_session=; Path=/; HttpOnly; SameSite=Strict; Max-Age=0",
    });
  }
  if (!authenticated(request)) return apiError(response, pathname, 401, "UNAUTHENTICATED", "Viewer session required");
  if (pathname === "/auth/csrf" && request.method === "GET") {
    return json(response, 200, { meta: makeMeta(pathname), csrf_token: randomUUID() });
  }
  const scopedMatch = /^\/connections\/([^/]+)\/(.+)$/.exec(pathname);
  if (scopedMatch && !["test", "connect", "disconnect"].includes(scopedMatch[2])) {
    const id = decodeURIComponent(scopedMatch[1]);
    const connection = connections.get(id);
    if (!connection) return apiError(response, pathname, 404, "CONNECTION_NOT_FOUND", "Connection not found");
    if (connection.status === "DISCONNECTED") {
      return apiError(response, pathname, 409, "CONNECTION_NOT_CONNECTED", "Connection is disconnected");
    }
    const resourcePath = `/${scopedMatch[2]}`;
    if (resourcePath === "/stream" && request.method === "GET") {
      openScopedStream(request, response, connection);
      return;
    }
    if (resourcePath === "/sources/reconcile" && request.method === "POST") {
      return json(response, 200, envelope(pathname, { accepted: true }));
    }
    if (request.method !== "GET") return apiError(response, pathname, 405, "METHOD_NOT_ALLOWED", "Method not allowed");
    const payload = scopedPayload(connection, resourcePath, url.searchParams);
    if (!payload) return apiError(response, pathname, 404, "NOT_FOUND", "Console resource not found");
    return json(response, 200, payload);
  }
  if (pathname === "/connections" || pathname.startsWith("/connections/")) {
    await handleConnections(request, response, pathname);
    return;
  }
  if (pathname === "/sources/reconcile" && request.method === "POST") {
    return apiError(response, pathname, 400, "CONNECTION_REQUIRED", "A connection ID is required");
  }
  if (pathname === "/stream") {
    return apiError(response, pathname, 400, "CONNECTION_REQUIRED", "A connection ID is required");
  }
  if (request.method !== "GET") return apiError(response, pathname, 405, "METHOD_NOT_ALLOWED", "Method not allowed");
  const payload = responseFor(pathname, url.searchParams);
  if (payload) return apiError(response, pathname, 400, "CONNECTION_REQUIRED", "A connection ID is required");
  return apiError(response, pathname, 404, "NOT_FOUND", "Console resource not found");
}

async function handleStatic(response, pathname) {
  let decoded;
  try {
    decoded = decodeURIComponent(pathname);
  } catch {
    response.writeHead(400).end();
    return;
  }
  const requested = resolve(root, `.${decoded}`);
  if (requested !== root && !requested.startsWith(`${root}${sep}`)) {
    response.writeHead(403).end();
    return;
  }
  const candidates = extname(requested)
    ? [requested]
    : [resolve(requested, "index.html"), `${requested}.html`, resolve(root, "index.html")];
  for (const candidate of candidates) {
    try {
      const info = await stat(candidate);
      if (!info.isFile()) continue;
      const content = await readFile(candidate);
      response.writeHead(200, {
        "content-type": mimeTypes[extname(candidate)] ?? "application/octet-stream",
        "cache-control": "no-cache",
      });
      response.end(content);
      return;
    } catch {
      continue;
    }
  }
  response.writeHead(404).end();
}

const server = createServer(async (request, response) => {
  const url = new URL(request.url ?? "/", `http://${request.headers.host ?? `127.0.0.1:${port}`}`);
  if (url.pathname === "/__mock/connections/seed" && request.method === "POST") {
    const input = await readBody(request);
    const id = String(input.id ?? "");
    const name = String(input.name ?? id);
    const status = ["CONNECTED", "UNREACHABLE", "DISCONNECTED"].includes(input.status) ? input.status : "CONNECTED";
    if (!/^[\w-]+$/.test(id) || !name) {
      response.writeHead(400).end();
      return;
    }
    disconnectStreams(id);
    const now = new Date().toISOString();
    connections.set(id, {
      id,
      name,
      environment: "test",
      admin_seeds: [{ node_id: 1, admin_addr: "127.0.0.1:52001" }],
      discovery: null,
      cluster_name: name,
      admin_tls: { ca: null, cert: null, key: null, domain: null },
      discovery_tls: { ca: null, cert: null, key: null, domain: null },
      discovery_username: null,
      discovery_allow_insecure: false,
      prometheus: null,
      poll_interval_secs: 5,
      created_at: now,
      updated_at: now,
      observed_cluster_id: `observed-${id}`,
      last_connected_at: now,
      source: input.source === "STARTUP" ? "STARTUP" : "PROFILE",
      status,
      last_error: status === "UNREACHABLE" ? "Admin endpoints are temporarily unreachable" : null,
      node_count: status === "DISCONNECTED" ? null : 3,
      group_count: status === "DISCONNECTED" ? null : 2,
    });
    response.writeHead(204).end();
    return;
  }
  if (url.pathname === "/__mock/connections/reset" && request.method === "POST") {
    disconnectStreams();
    connections.clear();
    connectionCredentials.clear();
    connectionSequence = 0;
    response.writeHead(204).end();
    return;
  }
  if (url.pathname === "/__mock/stream/disconnect" && request.method === "POST") {
    disconnectStreams(url.searchParams.get("connection"));
    response.writeHead(204).end();
    return;
  }
  if (url.pathname === "/__mock/stream/resync" && request.method === "POST") {
    resyncStreams(url.searchParams.get("connection"));
    response.writeHead(204).end();
    return;
  }
  if (url.pathname === "/__mock/stream/state" && request.method === "GET") {
    const byConnection = {};
    for (const response of streams) {
      const id = streamConnections.get(response);
      byConnection[id] = (byConnection[id] ?? 0) + 1;
    }
    return json(response, 200, { activeStreams: streams.size, activeIntervals: streamIntervals.size, byConnection });
  }
  if (url.pathname.startsWith("/api/v1/console/")) {
    await handleApi(request, response, url);
    return;
  }
  if (url.pathname.startsWith("/api/")) {
    apiError(response, url.pathname, 404, "NOT_FOUND", "API resource not found");
    return;
  }
  await handleStatic(response, url.pathname);
});

server.listen(port, "127.0.0.1");

for (const signal of ["SIGINT", "SIGTERM"]) {
  process.on(signal, () => {
    disconnectStreams();
    server.close(() => process.exit(0));
  });
}
