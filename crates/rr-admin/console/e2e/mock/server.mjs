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
  streams.delete(response);
}

function disconnectStreams() {
  for (const response of streams) {
    cleanupStream(response);
    response.end();
  }
}

function resyncStreams() {
  for (const response of streams) {
    response.write(`event: resync\ndata: ${JSON.stringify({ reason: "LAGGED", skipped: 1 })}\n\n`);
  }
}

async function handleApi(request, response, url) {
  const pathname = url.pathname.slice("/api/v1/console".length);
  if (pathname === "/stream") {
    if (!authenticated(request)) return apiError(response, pathname, 401, "UNAUTHENTICATED", "Viewer session required");
    response.writeHead(200, {
      "content-type": "text/event-stream",
      "cache-control": "no-cache",
      connection: "keep-alive",
      "x-accel-buffering": "no",
    });
    const summary = responseFor("/summary", new URLSearchParams({ window: "15m" }));
    response.write(`event: summary\ndata: ${JSON.stringify(summary)}\n\n`);
    const interval = setInterval(() => {
      if (response.writableEnded || response.destroyed) return cleanupStream(response);
      const update = responseFor("/summary", new URLSearchParams({ window: "15m" }));
      response.write(`event: summary\ndata: ${JSON.stringify(update)}\n\n`);
    }, 2_000);
    streams.add(response);
    streamIntervals.set(response, interval);
    request.once("close", () => cleanupStream(response));
    response.once("close", () => cleanupStream(response));
    response.once("error", () => cleanupStream(response));
    return;
  }
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
  if (pathname === "/sources/reconcile" && request.method === "POST") {
    return json(response, 200, envelope(pathname, { accepted: true }));
  }
  if (request.method !== "GET") return apiError(response, pathname, 405, "METHOD_NOT_ALLOWED", "Method not allowed");
  const payload = responseFor(pathname, url.searchParams);
  if (!payload) return apiError(response, pathname, 404, "NOT_FOUND", "Console resource not found");
  return json(response, 200, payload);
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
  if (url.pathname === "/__mock/stream/disconnect" && request.method === "POST") {
    disconnectStreams();
    response.writeHead(204).end();
    return;
  }
  if (url.pathname === "/__mock/stream/resync" && request.method === "POST") {
    resyncStreams();
    response.writeHead(204).end();
    return;
  }
  if (url.pathname === "/__mock/stream/state" && request.method === "GET") {
    return json(response, 200, { activeStreams: streams.size, activeIntervals: streamIntervals.size });
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
