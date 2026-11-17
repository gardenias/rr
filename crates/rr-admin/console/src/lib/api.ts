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
  path: P,
  query: ConsoleQuery = {},
  signal?: AbortSignal,
): Promise<ConsoleEnvelope<ConsoleResponseMap[P]>> {
  return requestConsole<ConsoleResponseMap[P]>(path, query, signal);
}

export async function getConsoleResource<T>(
  path: string,
  query: ConsoleQuery = {},
  signal?: AbortSignal,
): Promise<ConsoleEnvelope<T>> {
  return requestConsole<T>(path, query, signal);
}

async function requestConsole<T>(
  path: string,
  query: ConsoleQuery,
  signal?: AbortSignal,
): Promise<ConsoleEnvelope<T>> {
  const response = await fetch(buildConsoleUrl(path, query), {
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

export const consoleApi = {
  summary: (window = "15m", signal?: AbortSignal) => getConsole("/summary", { window }, signal),
  node: (nodeId: number, signal?: AbortSignal) =>
    getConsoleResource<NodeDetail>(`/nodes/${nodeId}`, {}, signal),
  boots: (nodeId: number, cursor?: string, pageSize = 50, signal?: AbortSignal) =>
    getConsoleResource<CursorPage<BootRecord>>(
      `/nodes/${nodeId}/boots`,
      { cursor, page_size: pageSize },
      signal,
    ),
  group: (groupId: number, signal?: AbortSignal) =>
    getConsoleResource<GroupDetail>(`/groups/${groupId}`, {}, signal),
  replicas: (groupId: number, signal?: AbortSignal) =>
    getConsoleResource<ReplicaData>(`/groups/${groupId}/replicas`, {}, signal),
  reconcile: async (signal?: AbortSignal) => postConsole<ConsoleEnvelope<{ accepted: boolean }>>(
    "/sources/reconcile",
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

export function consoleStreamUrl(): string {
  return `${API_ROOT}/stream`;
}
