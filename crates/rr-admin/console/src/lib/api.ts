import { loginRedirectFor } from "@/lib/auth";
import { sanitizeError } from "@/lib/format";
import { clampPageSize } from "@/lib/query-state";
import type {
  BootRecord,
  ConsoleEnvelope,
  ConsoleQuery,
  ConsoleResourcePath,
  ConsoleResponseMap,
  CursorPage,
  GroupDetail,
  LoginResponse,
  NodeDetail,
  ReplicaData,
} from "@/lib/contract";

export type {
  Capability,
  CapabilityStatus,
  ConsoleEnvelope,
  ConsoleMeta,
  ConsoleResourcePath,
  ConsoleResponseMap,
  HealthStatus,
  SourceEvidence,
  SourceStatus,
} from "@/lib/contract";

const API_ROOT = "/api/v1/console";
let csrfToken: string | null | undefined;

interface ErrorPayload {
  meta?: { correlation_id?: string };
  error?: { code?: string; message?: string };
}

export class ConsoleApiError extends Error {
  readonly status: number;
  readonly code: string;
  readonly correlationId: string | null;

  constructor(status: number, code: string, message: string, correlationId: string | null) {
    super(sanitizeError(message));
    this.name = "ConsoleApiError";
    this.status = status;
    this.code = code;
    this.correlationId = correlationId;
  }
}

export function buildConsoleUrl(path: string, query: ConsoleQuery = {}): string {
  const normalizedPath = path.startsWith("/") ? path : `/${path}`;
  const params = new URLSearchParams();
  for (const [key, rawValue] of Object.entries(query)) {
    if (rawValue == null || rawValue === "") continue;
    const value = key === "page_size" && typeof rawValue === "number"
      ? clampPageSize(rawValue)
      : rawValue;
    params.set(key, String(value));
  }
  const search = params.toString();
  return `${API_ROOT}${normalizedPath}${search ? `?${search}` : ""}`;
}

export function isTerminalConnectionError(error: unknown): error is ConsoleApiError {
  return error instanceof ConsoleApiError && [
    "CONNECTION_REQUIRED",
    "CONNECTION_NOT_FOUND",
    "CONNECTION_NOT_CONNECTED",
  ].includes(error.code);
}

function connectionResourcePath(connectionId: string, path: string): string {
  const id = connectionId.trim();
  if (!id) throw new ConsoleApiError(400, "CONNECTION_REQUIRED", "A connection ID is required", null);
  const normalizedPath = path.startsWith("/") ? path : `/${path}`;
  return `/connections/${encodeURIComponent(id)}${normalizedPath}`;
}

async function decodeError(response: Response): Promise<ConsoleApiError> {
  let payload: ErrorPayload = {};
  try {
    payload = (await response.json()) as ErrorPayload;
  } catch {
    payload = {};
  }
  return new ConsoleApiError(
    response.status,
    payload.error?.code ?? "HTTP_ERROR",
    payload.error?.message ?? `Console request failed with status ${response.status}`,
    payload.meta?.correlation_id ?? null,
  );
}

export async function getConsole<P extends ConsoleResourcePath>(
  connectionId: string,
  path: P,
  query: ConsoleQuery = {},
  signal?: AbortSignal,
): Promise<ConsoleEnvelope<ConsoleResponseMap[P]>> {
  return requestConsole<ConsoleResponseMap[P]>(connectionId, path, query, signal);
}

export async function getConsoleResource<T>(
  connectionId: string,
  path: string,
  query: ConsoleQuery = {},
  signal?: AbortSignal,
): Promise<ConsoleEnvelope<T>> {
  return requestConsole<T>(connectionId, path, query, signal);
}

async function requestConsole<T>(
  connectionId: string,
  path: string,
  query: ConsoleQuery,
  signal?: AbortSignal,
): Promise<ConsoleEnvelope<T>> {
  const response = await fetch(buildConsoleUrl(connectionResourcePath(connectionId, path), query), {
    method: "GET",
    headers: { Accept: "application/json" },
    credentials: "same-origin",
    cache: "no-store",
    signal,
  });
  if (!response.ok) throw await decodeError(response);
  return (await response.json()) as ConsoleEnvelope<T>;
}

export async function postConsole<T>(
  path: string,
  body?: Record<string, string>,
  signal?: AbortSignal,
  headers: Record<string, string> = {},
): Promise<T> {
  const response = await fetch(buildConsoleUrl(path), {
    method: "POST",
    headers: { Accept: "application/json", "Content-Type": "application/json", ...headers },
    credentials: "same-origin",
    cache: "no-store",
    body: body ? JSON.stringify(body) : undefined,
    signal,
  });
  if (!response.ok) throw await decodeError(response);
  return (await response.json()) as T;
}

async function reconcileHeaders(signal?: AbortSignal): Promise<Record<string, string>> {
  if (csrfToken === undefined) {
    const response = await fetch(buildConsoleUrl("/auth/csrf"), {
      method: "GET",
      headers: { Accept: "application/json" },
      credentials: "same-origin",
      cache: "no-store",
      signal,
    });
    if (!response.ok) throw await decodeError(response);
    const result = (await response.json()) as { csrf_token?: string | null; expires_at?: string | null };
    if (result.csrf_token === undefined || result.csrf_token === "") {
      throw new ConsoleApiError(0, "INVALID_CSRF_RESPONSE", "CSRF response did not include a token capability", null);
    }
    csrfToken = result.csrf_token;
  }
  return csrfToken ? { "X-Console-CSRF": csrfToken } : {};
}

function returnToLogin(): void {
  window.location.assign(loginRedirectFor(`${window.location.pathname}${window.location.search}`));
}

async function controlResponse(
  path: string,
  method: "GET" | "POST" | "PATCH" | "DELETE",
  body?: unknown,
  signal?: AbortSignal,
): Promise<Response> {
  const headers: Record<string, string> = { Accept: "application/json" };
  if (method !== "GET") {
    try {
      Object.assign(headers, await reconcileHeaders(signal));
    } catch (error) {
      if (error instanceof ConsoleApiError && error.status === 401) returnToLogin();
      throw error;
    }
  }
  if (body !== undefined) headers["Content-Type"] = "application/json";
  const response = await fetch(buildConsoleUrl(path), {
    method,
    headers,
    credentials: "same-origin",
    cache: "no-store",
    body: body === undefined ? undefined : JSON.stringify(body),
    signal,
  });
  if (response.status === 401) returnToLogin();
  if (!response.ok) throw await decodeError(response);
  return response;
}

export async function requestConsoleControl<T>(
  path: string,
  method: "GET" | "POST" | "PATCH",
  body?: unknown,
  signal?: AbortSignal,
): Promise<ConsoleEnvelope<T>> {
  const response = await controlResponse(path, method, body, signal);
  return (await response.json()) as ConsoleEnvelope<T>;
}

export async function deleteConsoleControl(path: string, signal?: AbortSignal): Promise<void> {
  await controlResponse(path, "DELETE", undefined, signal);
}

export const consoleApi = {
  summary: (connectionId: string, window = "15m", signal?: AbortSignal) =>
    getConsole(connectionId, "/summary", { window }, signal),
  node: (connectionId: string, nodeId: number, signal?: AbortSignal) =>
    getConsoleResource<NodeDetail>(connectionId, `/nodes/${nodeId}`, {}, signal),
  boots: (connectionId: string, nodeId: number, cursor?: string, pageSize = 50, signal?: AbortSignal) =>
    getConsoleResource<CursorPage<BootRecord>>(
      connectionId,
      `/nodes/${nodeId}/boots`,
      { cursor, page_size: pageSize },
      signal,
    ),
  group: (connectionId: string, groupId: number, signal?: AbortSignal) =>
    getConsoleResource<GroupDetail>(connectionId, `/groups/${groupId}`, {}, signal),
  replicas: (connectionId: string, groupId: number, signal?: AbortSignal) =>
    getConsoleResource<ReplicaData>(connectionId, `/groups/${groupId}/replicas`, {}, signal),
  reconcile: async (connectionId: string, signal?: AbortSignal) => postConsole<ConsoleEnvelope<{ accepted: boolean }>>(
    connectionResourcePath(connectionId, "/sources/reconcile"),
    {},
    signal,
    await reconcileHeaders(signal),
  ),
  login: async (token: string, signal?: AbortSignal) => {
    const result = await postConsole<LoginResponse>("/auth/login", { token }, signal);
    csrfToken = result.csrf_token;
    return result;
  },
  logout: async (signal?: AbortSignal) => {
    try {
      return await postConsole<ConsoleEnvelope<{ authenticated: false }>>("/auth/logout", {}, signal);
    } finally {
      csrfToken = undefined;
    }
  },
};

export function consoleStreamUrl(connectionId: string): string {
  return buildConsoleUrl(connectionResourcePath(connectionId, "/stream"));
}
