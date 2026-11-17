import type { ConsoleMeta } from "@/lib/api";

export function makeMeta(overrides: Partial<ConsoleMeta> = {}): ConsoleMeta {
  return {
    schema_version: "console.v1",
    correlation_id: "corr-test-1",
    cluster_id: "cluster-test",
    generated_at: "2026-11-16T08:00:00Z",
    partial: false,
    time_window: "15m",
    retention_source: "CONSOLE_MEMORY",
    sources: [
      {
        node_id: 1,
        endpoint_role: "ADMIN",
        observed_at: "2026-11-16T07:59:59Z",
        age_ms: 1_000,
        status: "ONLINE",
      },
    ],
    capabilities: [],
    ...overrides,
  };
}
