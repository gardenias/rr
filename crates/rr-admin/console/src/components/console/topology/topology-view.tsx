"use client";

import Link from "next/link";
import { ArrowUpRight, CheckCircle2 } from "lucide-react";
import type { KeyboardEvent } from "react";
import { CapabilityNotice } from "@/components/console/capability-notice";
import { FreshnessBadge } from "@/components/console/freshness-badge";
import { SourceEvidence } from "@/components/console/source-evidence";
import { ErrorState } from "@/components/console/states";
import { Button } from "@/components/ui/button";
import { useConsoleResource } from "@/lib/use-console-resource";
import { useRouteQueryState } from "@/lib/use-route-query-state";
import { hrefWithConnection, serializeQueryState } from "@/lib/query-state";
import { NodesTopology } from "@/components/console/topology/nodes-topology";
import { RaftTopology } from "@/components/console/topology/raft-topology";
import { RoutesTopology } from "@/components/console/topology/routes-topology";
import type { TopologyViewKind } from "@/components/console/topology/model";

const views: Array<{ id: TopologyViewKind; label: string }> = [
  { id: "nodes", label: "Nodes" },
  { id: "raft", label: "Raft" },
  { id: "routes", label: "Partitions & routes" },
];

export function TopologyView() {
  const { state, setFilter } = useRouteQueryState();
  const requestedView = state.filters.view;
  const view: TopologyViewKind = views.some((candidate) => candidate.id === requestedView)
    ? requestedView as TopologyViewKind
    : "nodes";
  const { data, meta, error, loading, refresh } = useConsoleResource("/topology", { view });
  const selectedGroupId = Number(state.filters.group_id);
  const onViewKeyDown = (event: KeyboardEvent<HTMLDivElement>) => {
    if (!["ArrowLeft", "ArrowRight", "Home", "End"].includes(event.key)) return;
    event.preventDefault();
    const index = views.findIndex((candidate) => candidate.id === view);
    const nextIndex = event.key === "Home" ? 0 : event.key === "End" ? views.length - 1 : event.key === "ArrowRight" ? (index + 1) % views.length : (index - 1 + views.length) % views.length;
    const next = views[nextIndex]!;
    setFilter("view", next.id);
    document.getElementById(`topology-${next.id}-tab`)?.focus();
  };
  return (
    <div className="route-page">
      <div className="page-heading">
        <div>
          <div className="page-eyebrow">Cluster / Read-only</div>
          <h1>Topology</h1>
          <p>Expected placement, observed Raft relationships, and partition routes with bounded graphs and equivalent tables.</p>
        </div>
        <Button variant="secondary" size="small" onClick={refresh} disabled={loading}>Refresh observations</Button>
      </div>
      <div className="flex gap-1 border-b border-white/10" role="tablist" aria-label="Topology views" onKeyDown={onViewKeyDown}>
        {views.map((candidate) => (
          <button
            key={candidate.id}
            id={`topology-${candidate.id}-tab`}
            type="button"
            role="tab"
            aria-selected={view === candidate.id}
            aria-controls="topology-view-panel"
            tabIndex={view === candidate.id ? 0 : -1}
            className={`border-b-2 px-3 py-2 text-xs ${view === candidate.id ? "border-orange-400 text-zinc-100" : "border-transparent text-zinc-400"}`}
            onClick={() => setFilter("view", candidate.id)}
          >{candidate.label}</button>
        ))}
      </div>

      {error ? <ErrorState title={data ? "Showing the last successful observations" : "Console data unavailable"} message={error.message} correlationId={error.correlationId} recovery="Open Sources to inspect endpoint health and collection capabilities." /> : null}
      {loading && !data ? <div className="route-skeleton" role="status" aria-label="Loading Topology"><span /><span /><span /></div> : null}
      {data && meta ? (
        <>
          <section className="evidence-panel" aria-label="Topology observation status">
            <div className="evidence-panel-topline">
              <div className="evidence-panel-status">
                <CheckCircle2 aria-hidden="true" size={17} />
                <div><h2>Observation contract connected</h2><p>Graphs visualize reported relationships only. Every graph has an equivalent table and a bounded rendering budget.</p></div>
              </div>
            </div>
            <div className="evidence-panel-meta">
              <FreshnessBadge meta={meta} />
              <SourceEvidence meta={meta} />
              <Link href={hrefWithConnection("/sources", serializeQueryState(state))} className="evidence-link">Inspect sources <ArrowUpRight aria-hidden="true" size={12} /></Link>
            </div>
          </section>
          {meta.capabilities.map((capability) => <CapabilityNotice key={`${capability.feature}-${capability.status}`} capability={capability} />)}
          <div id="topology-view-panel" role="tabpanel" aria-labelledby={`topology-${view}-tab`}>
            {view === "nodes" ? <NodesTopology data={data} timeWindow={state.window} connectionId={state.connection ?? undefined} /> : null}
            {view === "raft" ? <RaftTopology data={data} selectedGroupId={Number.isSafeInteger(selectedGroupId) && selectedGroupId > 0 ? selectedGroupId : undefined} onSelectGroup={(groupId) => setFilter("group_id", String(groupId))} timeWindow={state.window} connectionId={state.connection ?? undefined} /> : null}
            {view === "routes" ? <RoutesTopology data={data} timeWindow={state.window} filter={state.filters.filter} connectionId={state.connection ?? undefined} /> : null}
          </div>
        </>
      ) : null}
    </div>
  );
}
