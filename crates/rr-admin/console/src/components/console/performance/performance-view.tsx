"use client";

import { ArrowUpRight } from "lucide-react";
import Link from "next/link";
import type { ColumnDef } from "@tanstack/react-table";
import { CapabilityNotice } from "@/components/console/capability-notice";
import { DenseTable } from "@/components/console/dense-table";
import { FreshnessBadge } from "@/components/console/freshness-badge";
import { MetricChart } from "@/components/console/metric-chart";
import { SourceEvidence } from "@/components/console/source-evidence";
import { EmptyState, ErrorState } from "@/components/console/states";
import { Button } from "@/components/ui/button";
import type { Capability, PerformanceData } from "@/lib/contract";
import { formatNumber } from "@/lib/format";
import { serializeQueryState, type TimeWindow } from "@/lib/query-state";
import { useConsoleResource } from "@/lib/use-console-resource";
import { useRouteQueryState } from "@/lib/use-route-query-state";

type MetricSeries = PerformanceData["series"][number];
type MetricCategory = "Throughput" | "Latency" | "Replication" | "Backpressure & errors";
type HotspotRow = {
  metric: string;
  scope: string;
  latest: number | null;
  observedAt: string;
};

function categoryFor(metric: string): MetricCategory {
  if (/latency|duration|p50|p95|p99|rtt/i.test(metric)) return "Latency";
  if (/replication|append|heartbeat|lag|channel/i.test(metric)) return "Replication";
  if (/pending|overflow|redirect|error|idempotency|wal|storage|stale|failure|deferred|output/i.test(metric)) {
    return "Backpressure & errors";
  }
  return "Throughput";
}

function seriesLabel(series: MetricSeries) {
  const labels = Object.entries(series.labels)
    .map(([key, value]) => `${key}=${value}`)
    .join(", ");
  return labels ? `${series.metric} · ${labels}` : series.metric;
}

function chartModel(series: MetricSeries[]) {
  const timestamps = [...new Set(series.flatMap((item) => item.points.map((point) => point.timestamp)))].sort();
  const data = timestamps.map((timestamp) => {
    const row: Record<string, string | number | null> = { timestamp };
    series.forEach((item, index) => {
      row[`series_${index}`] = item.points.find((point) => point.timestamp === timestamp)?.value ?? null;
    });
    return row;
  });
  return {
    data,
    series: series.map((item, index) => ({
      key: `series_${index}`,
      label: seriesLabel(item),
      unit: /seconds|duration|latency|rtt/i.test(item.metric) ? "s" : /tps|throughput/i.test(item.metric) ? "TPS" : undefined,
    })),
  };
}

function latestRows(series: MetricSeries[]): HotspotRow[] {
  return series.map((item) => {
    const latest = item.points.at(-1);
    return {
      metric: item.metric,
      scope: Object.entries(item.labels).map(([key, value]) => `${key}=${value}`).join(", ") || "cluster",
      latest: latest?.value ?? null,
      observedAt: latest?.timestamp ?? "—",
    };
  });
}

function retentionLabel(retention: PerformanceData["retention_source"]) {
  switch (retention) {
    case "CONSOLE_MEMORY":
      return "Console memory retention · this Console process only";
    case "PROMETHEUS":
      return "Prometheus retained history · cluster and node labels enforced";
    case "NONE":
      return "No retained history is available";
    default:
      return "Retention source unknown";
  }
}

const hotspotColumns: ColumnDef<HotspotRow>[] = [
  { accessorKey: "metric", header: "Metric" },
  { accessorKey: "scope", header: "Group / node / op" },
  {
    accessorKey: "latest",
    header: "Latest observed value",
    cell: ({ getValue }) => formatNumber(getValue<number | null>()),
  },
  { accessorKey: "observedAt", header: "Observed at" },
];

export function PerformanceView() {
  const { state, setWindow, setFilter } = useRouteQueryState();
  const { data, meta, error, loading, refresh } = useConsoleResource("/metrics/performance", {
    window: state.window,
    group_id: state.filters.group_id,
    node_id: state.filters.node_id,
    op: state.filters.op,
    source: state.filters.source,
  });
  const categories: MetricCategory[] = ["Throughput", "Latency", "Replication", "Backpressure & errors"];
  const sourcesHref = `/sources/?${serializeQueryState(state)}`;

  return (
    <div className="route-page">
      <div className="page-heading">
        <div>
          <div className="page-eyebrow">Observe / Read-only</div>
          <h1>Performance</h1>
          <p>Fixed-template throughput, latency, replication, and backpressure observations with explicit retention.</p>
        </div>
        <Button variant="secondary" size="small" onClick={refresh} disabled={loading}>Refresh observations</Button>
      </div>

      <section className="flex flex-wrap items-end gap-3 border border-(--line-1) bg-(--bg-1) p-4" aria-label="Performance filters">
        <label className="grid gap-1 text-xs text-(--tx-3)">
          Time window
          <select className="input w-32" value={state.window} onChange={(event) => setWindow(event.target.value as TimeWindow)}>
            {["15m", "1h", "6h", "24h"].map((window) => <option key={window} value={window}>{window}</option>)}
          </select>
        </label>
        <label className="grid gap-1 text-xs text-(--tx-3)">
          Retention
          <select className="input w-56" value={state.filters.source ?? ""} onChange={(event) => setFilter("source", event.target.value)}>
            <option value="">All available retention</option>
            <option value="CONSOLE_MEMORY">Live · Console memory</option>
            <option value="PROMETHEUS">History · Prometheus</option>
          </select>
        </label>
        <label className="grid gap-1 text-xs text-(--tx-3)">
          Group ID
          <input className="input w-32" inputMode="numeric" value={state.filters.group_id ?? ""} onChange={(event) => setFilter("group_id", event.target.value)} placeholder="All groups" />
        </label>
        <label className="grid gap-1 text-xs text-(--tx-3)">
          Node ID
          <input className="input w-32" inputMode="numeric" value={state.filters.node_id ?? ""} onChange={(event) => setFilter("node_id", event.target.value)} placeholder="All nodes" />
        </label>
        <label className="grid gap-1 text-xs text-(--tx-3)">
          Operation
          <input className="input w-40" value={state.filters.op ?? ""} onChange={(event) => setFilter("op", event.target.value)} placeholder="All operations" />
        </label>
      </section>

      {error ? <ErrorState title={data ? "Showing the last successful observations" : "Performance data unavailable"} message={error.message} correlationId={error.correlationId} recovery="Inspect Sources for metrics and Prometheus failures." /> : null}
      {loading && !data ? <div className="route-skeleton" role="status" aria-label="Loading Performance"><span /><span /><span /></div> : null}

      {data && meta ? (
        <>
          <section className="evidence-panel-meta border border-(--line-1)" aria-label="Performance observation status">
            <FreshnessBadge meta={meta} />
            <SourceEvidence meta={meta} />
            <span className="text-xs text-(--tx-3)">{retentionLabel(data.retention_source ?? meta.retention_source)}</span>
            <Link href={sourcesHref} className="evidence-link">Inspect sources <ArrowUpRight aria-hidden="true" size={12} /></Link>
          </section>

          <aside className="capability-notice" data-status="unsupported">
            <div>
              <strong>UNALIGNED_COHORTS</strong>
              <p>Server total, proposal, and apply samples are shown independently. Their outcomes and cohorts are not proven aligned, so no mean stacking, subtraction, or percentile composition is performed.</p>
            </div>
          </aside>
          {[...(data.notices ?? []), ...meta.capabilities]
            .filter((capability, index, all) => !capability.feature.includes("UNALIGNED_COHORTS") && all.findIndex((other) => other.feature === capability.feature && other.status === capability.status && other.reason === capability.reason) === index)
            .map((capability: Capability) => <CapabilityNotice key={`${capability.feature}-${capability.status}-${capability.reason}`} capability={capability} />)}

          {data.series.length === 0 ? (
            <EmptyState
              title="No performance observations for this window"
              description={`${retentionLabel(data.retention_source ?? meta.retention_source)}. Missing points are not replaced with zero or a flat line.`}
            />
          ) : (
            <>
              <div className="grid min-w-0 gap-4 xl:grid-cols-2">
                {categories.map((category) => {
                  const selected = data.series.filter((item) => categoryFor(item.metric) === category);
                  if (!selected.length) return null;
                  const chart = chartModel(selected);
                  return (
                    <div className="min-w-0" key={category}>
                      <MetricChart
                        title={category}
                        summary={`${selected.length} fixed-template series. Missing and reset-separated points remain unknown gaps.`}
                        data={chart.data}
                        xKey="timestamp"
                        series={chart.series}
                      />
                    </div>
                  );
                })}
              </div>
              <section className="grid min-w-0 gap-3" aria-labelledby="performance-hotspots">
                <div>
                  <h2 id="performance-hotspots">Latest values by metric and scope</h2>
                  <p className="mt-1 text-xs text-(--tx-3)">Rates, histogram quantiles, and reset handling are supplied by the BFF; the browser does not average node percentiles or recalculate counters.</p>
                </div>
                <DenseTable data={latestRows(data.series)} columns={hotspotColumns} caption="Performance hotspot observations" />
              </section>
            </>
          )}
        </>
      ) : null}
    </div>
  );
}
