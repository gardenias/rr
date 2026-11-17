"use client";

import { Activity, ArrowUpRight, CircleAlert, Server, ShieldAlert } from "lucide-react";
import Link from "next/link";
import { useSearchParams } from "next/navigation";
import type { ReactNode } from "react";
import { CapabilityNotice } from "@/components/console/capability-notice";
import { EmptyState, ErrorState } from "@/components/console/states";
import { FreshnessBadge } from "@/components/console/freshness-badge";
import { HealthBadge } from "@/components/console/health-badge";
import { MetricChart } from "@/components/console/metric-chart";
import { SourceEvidence } from "@/components/console/source-evidence";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import type { BootRecord, PerformanceData } from "@/lib/contract";
import { formatNumber, formatTimestamp } from "@/lib/format";
import { useConsoleResource } from "@/lib/use-console-resource";
import { useRouteQueryState } from "@/lib/use-route-query-state";
import { drilldownHref, incidentDrilldownHref, latestMetricValue, primaryIncident, summaryCounts } from "./model";

function SummaryCard({ href, label, value, detail, icon }: { href: string; label: string; value: string; detail: string; icon: ReactNode }) {
  return (
    <Link href={href} className="min-w-0 border border-(--line-1) bg-(--bg-1) p-4 transition-colors hover:border-(--acc-ring) hover:bg-(--acc-tint)">
      <div className="flex items-center justify-between gap-2 text-(--tx-3)"><span className="text-[11px] font-medium uppercase tracking-wider">{label}</span>{icon}</div>
      <p className="mt-3 font-mono text-2xl text-(--tx-1)">{value}</p>
      <p className="mt-1 truncate text-[11px] text-(--tx-3)" title={detail}>{detail}</p>
    </Link>
  );
}

function chartModel(data: PerformanceData | null) {
  const series = data?.series.filter((item) => item.metric === "write_tps" || item.metric === "write_p99_ms") ?? [];
  const timestamps = [...new Set(series.flatMap((item) => item.points.map((point) => point.timestamp)))].sort();
  const rows = timestamps.map((timestamp) => {
    const row: Record<string, string | number | null> = { timestamp: formatTimestamp(timestamp).replace(" UTC", "") };
    for (const item of series) row[item.metric] = item.points.find((point) => point.timestamp === timestamp)?.value ?? null;
    return row;
  });
  return {
    rows,
    series: series.map((item) => ({ key: item.metric, label: item.metric === "write_tps" ? "Write TPS" : "Write P99 (ms)", unit: item.metric === "write_p99_ms" ? "ms" : undefined })),
  };
}

export function OverviewView() {
  const { state } = useRouteQueryState();
  const searchParams = useSearchParams();
  const currentSearch = searchParams.toString();
  const summaryResource = useConsoleResource("/summary", { window: state.window, ...state.filters });
  const performanceResource = useConsoleResource("/metrics/performance", { window: state.window });
  const eventsResource = useConsoleResource("/events", { page_size: 10, state: "ACTIVE" });
  const lifecycleResource = useConsoleResource("/lifecycle", { view: "restarts", page_size: 5 });
  const { data, meta, error, loading, refresh } = summaryResource;
  const counts = data ? summaryCounts(data) : null;
  const incident = data && meta ? primaryIncident(data, meta) : null;
  const incidentHref = incident ? incidentDrilldownHref(incident.href, currentSearch) : null;
  const chart = chartModel(performanceResource.data);
  const recentBoots = (lifecycleResource.data?.items ?? []).filter((item): item is BootRecord & { node_id?: number } => "started_at" in item);

  return (
    <div className="route-page">
      <div className="page-heading">
        <div>
          <div className="page-eyebrow">Cluster / Read-only</div>
          <h1>Overview</h1>
          <p>Cluster health, pressure, and the evidence that needs attention first.</p>
        </div>
        <Button variant="secondary" size="small" onClick={refresh} disabled={loading}>Refresh observations</Button>
      </div>

      {error ? <ErrorState title={data ? "Showing the last successful observations" : "Console data unavailable"} message={error.message} correlationId={error.correlationId} recovery="Open Sources to inspect endpoint health and collection capabilities." /> : null}
      {loading && !data ? <div className="route-skeleton" role="status" aria-label="Loading Overview"><span /><span /><span /></div> : null}

      {data && meta ? (
        <>
          {incident && incidentHref ? (
            <section className="flex flex-col gap-3 border border-(--acc-ring) border-l-2 border-l-(--acc) bg-(--acc-tint) p-4 sm:flex-row sm:items-center sm:justify-between" role={incident.severity === "critical" ? "alert" : "status"}>
              <div className="flex gap-3"><CircleAlert className="mt-0.5 shrink-0 text-(--acc)" aria-hidden="true" size={18} /><div><h2>{incident.title}</h2><p className="mt-1 text-xs text-(--tx-2)">{incident.description}</p></div></div>
              <Link href={incidentHref} className="button button-secondary button-size-small shrink-0">{incident.href.includes("group_id=") ? `Inspect group ${incident.href.match(/group_id=(\d+)/)?.[1]}` : "Inspect evidence"}<ArrowUpRight aria-hidden="true" size={12} /></Link>
            </section>
          ) : (
            <section className="border border-(--line-1) bg-(--bg-1) p-4" aria-label="Observation coverage">
              <h2>Observation coverage {formatNumber(data.observation_coverage)}%</h2>
              <p className="mt-1 text-xs text-(--tx-3)">No higher-priority incident is present in this response. Coverage describes what the Console can observe, not absolute cluster health.</p>
            </section>
          )}

          <section className="grid grid-cols-1 gap-3 sm:grid-cols-2 lg:grid-cols-3 2xl:grid-cols-6" aria-label="Cluster summary">
            <SummaryCard href={drilldownHref("/nodes", currentSearch, {})} label="Nodes online" value={`${formatNumber(counts?.nodesOnline)} / ${formatNumber(counts?.nodesTotal)}`} detail="Admin and required sources fresh" icon={<Server aria-hidden="true" size={15} />} />
            <SummaryCard href={drilldownHref("/groups", currentSearch, {})} label="Groups with leader" value={`${formatNumber(counts?.groupsWithLeader)} / ${formatNumber(counts?.groupsTotal)}`} detail="Observed leadership, not desired state" icon={<Activity aria-hidden="true" size={15} />} />
            <SummaryCard href={drilldownHref("/groups", currentSearch, { state: "QUORUM_AT_RISK" })} label="Quorum risk" value={formatNumber(counts?.quorumRisk)} detail="Console voter reachability view" icon={<ShieldAlert aria-hidden="true" size={15} />} />
            <SummaryCard href={drilldownHref("/performance", currentSearch, { op: "write" })} label="Write TPS" value={formatNumber(latestMetricValue(performanceResource.data, "write_tps"))} detail={`${data.observation_coverage != null ? `${formatNumber(data.observation_coverage)}% coverage` : "Coverage unknown"} · ${performanceResource.data?.retention_source ?? "source unknown"}`} icon={<Activity aria-hidden="true" size={15} />} />
            <SummaryCard href={drilldownHref("/performance", currentSearch, { op: "write" })} label="Write P99" value={formatNumber(latestMetricValue(performanceResource.data, "write_p99_ms"))} detail="Milliseconds · aggregate histogram only" icon={<Activity aria-hidden="true" size={15} />} />
            <SummaryCard href={drilldownHref("/events", currentSearch, { state: "ACTIVE" })} label="Active alerts" value={formatNumber(data.active_alerts)} detail={`${formatNumber(data.observation_coverage)}% observation coverage`} icon={<CircleAlert aria-hidden="true" size={15} />} />
          </section>

          <section className="evidence-panel" aria-label="Overview observation status">
            <div className="evidence-panel-topline">
              <div className="evidence-panel-status"><Activity aria-hidden="true" size={17} /><div><h2>Observation contract connected</h2><p>Values retain their source and freshness. Unknown values are shown as unavailable rather than zero.</p></div></div>
              <Badge tone={meta.partial ? "accent" : "neutral"}>{meta.partial ? "PARTIAL" : "COMPLETE RESPONSE"}</Badge>
            </div>
            <div className="evidence-panel-meta"><FreshnessBadge meta={meta} /><SourceEvidence meta={meta} /><Link href={drilldownHref("/sources", currentSearch, {})} className="evidence-link">Inspect sources <ArrowUpRight aria-hidden="true" size={12} /></Link></div>
          </section>
          {meta.capabilities.map((capability) => <CapabilityNotice key={`${capability.feature}-${capability.status}`} capability={capability} />)}

          <div className="grid min-w-0 gap-4 xl:grid-cols-[3fr_2fr]">
            <section className="min-w-0 border border-(--line-1) bg-(--bg-1) p-4" aria-labelledby="overview-raft-title">
              <div className="flex items-center justify-between gap-3"><div><h2 id="overview-raft-title">Raft observations</h2><p className="mt-1 text-xs text-(--tx-3)">Compressed evidence; open a group for its full replica matrix.</p></div><Link href={drilldownHref("/topology", currentSearch, { view: "raft" })} className="evidence-link">Topology</Link></div>
              <div className="dense-table-scroll mt-4"><table className="dense-table" aria-label="Cluster Raft group summary"><thead><tr><th scope="col">Group</th><th scope="col">Observed health</th><th scope="col">Leader</th><th scope="col">Term</th><th scope="col">Voters reachable</th></tr></thead><tbody>
                {data.groups.map((group) => <tr key={group.group_id}><td><Link className="text-(--acc)" href={drilldownHref("/groups", currentSearch, { group_id: String(group.group_id) })}>Group {group.group_id}</Link></td><td><HealthBadge status={group.health} /></td><td>{group.leader_node_id == null ? "No leader observed" : `Node ${group.leader_node_id}`}</td><td className="text-right font-mono">{formatNumber(group.current_term)}</td><td className="text-right font-mono">{formatNumber(group.voters_reachable)} / {formatNumber(group.voters_total)}</td></tr>)}
              </tbody></table></div>
            </section>

            <section className="min-w-0 border border-(--line-1) bg-(--bg-1) p-4" aria-labelledby="overview-events-title">
              <div className="flex items-center justify-between"><h2 id="overview-events-title">Active events</h2><Link href={drilldownHref("/events", currentSearch, { state: "ACTIVE" })} className="evidence-link">All events</Link></div>
              {eventsResource.data?.items.length ? <ul className="mt-4 grid gap-3">{eventsResource.data.items.map((event) => <li key={event.event_id} className="border-l-2 border-(--acc) pl-3"><div className="flex flex-wrap items-center gap-2"><Badge tone={event.severity === "ERROR" ? "bad" : event.severity === "WARN" ? "accent" : "neutral"}>{event.severity}</Badge><code>{event.object}</code></div><p className="mt-1 text-xs text-(--tx-3)">Last observed {formatTimestamp(event.last_observed_at)}</p></li>)}</ul> : <p className="mt-4 text-xs text-(--tx-3)">No active events in this response. Observation coverage is {formatNumber(data.observation_coverage)}%.</p>}
            </section>
          </div>

          {chart.rows.length ? <MetricChart title="Throughput and write latency" summary="Console memory retention only; missing intervals are not backfilled. P99 comes from aggregated histogram data." data={chart.rows} xKey="timestamp" series={chart.series} /> : <EmptyState title="No retained performance points" description="The performance source returned no write throughput or P99 samples for this window. No flat line is inferred." />}

          <div className="grid min-w-0 gap-4 xl:grid-cols-2">
            <section className="min-w-0 border border-(--line-1) bg-(--bg-1) p-4"><h2>Nodes requiring attention</h2><div className="dense-table-scroll mt-4"><table className="dense-table" aria-label="Nodes requiring attention"><thead><tr><th scope="col">Node</th><th scope="col">Status</th><th scope="col">Zone</th><th scope="col">Last contact</th></tr></thead><tbody>
              {data.nodes.filter((node) => node.status !== "ONLINE").slice(0, 10).map((node) => <tr key={node.node_id}><td><Link className="text-(--acc)" href={drilldownHref("/nodes", currentSearch, { node_id: String(node.node_id) })}>Node {node.node_id}</Link></td><td><HealthBadge status={node.status} /></td><td>{node.zone ?? "—"}</td><td>{formatTimestamp(node.last_contact ?? node.last_success)}</td></tr>)}
            </tbody></table></div>{data.nodes.every((node) => node.status === "ONLINE") ? <p className="mt-3 text-xs text-(--tx-3)">All returned node observations are online; coverage is {formatNumber(data.observation_coverage)}%.</p> : null}</section>
            <section className="min-w-0 border border-(--line-1) bg-(--bg-1) p-4"><div className="flex items-center justify-between"><h2>Recent boots</h2><Link href={drilldownHref("/lifecycle", currentSearch, { view: "restarts" })} className="evidence-link">Lifecycle</Link></div>
              {recentBoots.length ? <ul className="mt-4 grid gap-3">{recentBoots.map((boot) => <li key={`${boot.node_id ?? "unknown"}-${boot.boot_id}`}><div className="flex flex-wrap gap-2"><strong>{boot.node_id == null ? "Node —" : `Node ${boot.node_id}`}</strong><code>{boot.boot_id}</code><Badge tone={boot.persistence === "VOLATILE" ? "accent" : "neutral"}>{boot.persistence}</Badge></div><p className="mt-1 text-xs text-(--tx-3)">{formatTimestamp(boot.started_at)} · App {boot.app_version ?? "—"} · Rr {boot.rr_version ?? "—"}</p></li>)}</ul> : <p className="mt-4 text-xs text-(--tx-3)">No boot history was returned. Open Lifecycle to distinguish unsupported history from an empty journal.</p>}
            </section>
          </div>
        </>
      ) : null}
    </div>
  );
}
