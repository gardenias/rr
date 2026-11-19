export const TIME_WINDOWS = ["15m", "1h", "6h", "24h"] as const;
export type TimeWindow = (typeof TIME_WINDOWS)[number];

const FILTER_KEYS = [
  "filter",
  "node_id",
  "group_id",
  "op",
  "source",
  "view",
  "severity",
  "state",
] as const;

export interface QueryState {
  window: TimeWindow;
  connection: string | null;
  filters: Partial<Record<(typeof FILTER_KEYS)[number], string>>;
}

export function parseQueryState(search: string): QueryState {
  const params = new URLSearchParams(search);
  const requestedWindow = params.get("window");
  const window = TIME_WINDOWS.includes(requestedWindow as TimeWindow)
    ? (requestedWindow as TimeWindow)
    : "15m";
  const connection = params.get("connection")?.trim() || null;
  const filters: QueryState["filters"] = {};
  for (const key of FILTER_KEYS) {
    const value = params.get(key)?.trim();
    if (value) filters[key] = value;
  }
  return { window, connection, filters };
}

export function serializeQueryState(state: QueryState): string {
  const params = new URLSearchParams({ window: state.window });
  for (const [key, rawValue] of Object.entries(state.filters)) {
    const value = rawValue?.trim();
    if (value) params.set(key, value);
  }
  if (state.connection) params.set("connection", state.connection);
  return params.toString();
}

export function hrefWithConnection(path: string, params: string | URLSearchParams): string {
  const search = serializeQueryState(parseQueryState(params.toString()));
  return `${path}${search ? `?${search}` : ""}`;
}

export function connectionDrilldownHref(
  path: string,
  window: TimeWindow,
  connectionId: string | null | undefined,
  filters: Record<string, string>,
): string {
  const params = new URLSearchParams({ window, ...filters });
  if (connectionId) params.set("connection", connectionId);
  return hrefWithConnection(path, params);
}

export function switchConnectionHref(path: string, currentSearch: string, id: string): string {
  const { window } = parseQueryState(currentSearch);
  const params = new URLSearchParams({ window, connection: id });
  return `${path}?${params.toString()}`;
}

export function clampPageSize(value: number | null | undefined): number {
  if (value == null || !Number.isFinite(value) || value < 1) return 50;
  return Math.min(Math.floor(value), 200);
}
