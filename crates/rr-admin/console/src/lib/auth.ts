import { parseQueryState, serializeQueryState } from "@/lib/query-state";

export function loginRedirectFor(candidate: string): string {
  return `/login/?next=${encodeURIComponent(safeReturnPath(candidate))}`;
}

const fallbackPath = "/connections/";
const consolePaths = new Set([
  "/connections/",
  "/connections/new/",
  "/overview/",
  "/nodes/",
  "/topology/",
  "/groups/",
  "/performance/",
  "/snapshots/",
  "/lifecycle/",
  "/events/",
  "/sources/",
]);

export function safeReturnPath(candidate: string | null | undefined): string {
  if (!candidate || !candidate.startsWith("/")) return fallbackPath;
  let decoded = candidate;
  for (let round = 0; round < 3; round += 1) {
    if (/[\\\u0000-\u001f\u007f]/.test(decoded) || decoded.startsWith("//")) return fallbackPath;
    let next: string;
    try {
      next = decodeURIComponent(decoded);
    } catch {
      return fallbackPath;
    }
    if (next === decoded) break;
    decoded = next;
  }
  if (/[\\\u0000-\u001f\u007f]/.test(decoded) || decoded.startsWith("//")) return fallbackPath;

  let url: URL;
  try {
    url = new URL(candidate, "http://console.local");
  } catch {
    return fallbackPath;
  }
  if (url.origin !== "http://console.local" || !consolePaths.has(url.pathname)) return fallbackPath;
  const search = url.search.slice(1);
  const originalKeys = new URLSearchParams(search);
  for (const key of originalKeys.keys()) {
    if (/token|secret|password|credential|authorization/i.test(key)) return fallbackPath;
  }
  if (url.pathname === "/connections/" || url.pathname === "/connections/new/") {
    const edit = url.pathname === "/connections/" ? originalKeys.get("edit")?.trim() : null;
    return `${url.pathname}${edit ? `?edit=${encodeURIComponent(edit)}` : ""}`;
  }
  const serialized = serializeQueryState(parseQueryState(search));
  return `${url.pathname}${serialized ? `?${serialized}` : ""}`;
}
