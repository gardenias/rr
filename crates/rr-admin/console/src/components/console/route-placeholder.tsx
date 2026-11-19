"use client";

import { ArrowUpRight, CheckCircle2 } from "lucide-react";
import Link from "next/link";
import { CapabilityNotice } from "@/components/console/capability-notice";
import { FreshnessBadge } from "@/components/console/freshness-badge";
import { SourceEvidence } from "@/components/console/source-evidence";
import { EmptyState, ErrorState } from "@/components/console/states";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import type { RouteDefinition } from "@/lib/routes";
import { useConsoleResource } from "@/lib/use-console-resource";
import { useRouteQueryState } from "@/lib/use-route-query-state";
import { hrefWithConnection, serializeQueryState } from "@/lib/query-state";

export function RoutePlaceholder({ route }: { route: RouteDefinition }) {
  const { state } = useRouteQueryState();
  const query = {
    window: state.window,
    ...state.filters,
    page_size: 50,
    view: route.key === "topology" ? state.filters.view ?? "nodes" : route.key === "lifecycle" ? state.filters.view ?? "current" : state.filters.view,
  };
  const { data, meta, error, loading, refresh } = useConsoleResource(route.resource, query);
  const empty = data != null && "items" in data && Array.isArray(data.items) && data.items.length === 0;

  return (
    <div className="route-page">
      <div className="page-heading">
        <div>
          <div className="page-eyebrow">{route.section} / Read-only</div>
          <h1>{route.title}</h1>
          <p>{route.description}</p>
        </div>
        <Button variant="secondary" size="small" onClick={refresh} disabled={loading}>
          Refresh observations
        </Button>
      </div>

      {error ? (
        <ErrorState
          title={data ? "Showing the last successful observations" : "Console data unavailable"}
          message={error.message}
          correlationId={error.correlationId}
          recovery="Open Sources to inspect endpoint health and collection capabilities."
        />
      ) : null}

      {loading && !data ? (
        <div className="route-skeleton" role="status" aria-label={`Loading ${route.title}`}>
          <span /><span /><span />
        </div>
      ) : null}

      {data && meta ? (
        <>
          <section className="evidence-panel" aria-label={`${route.title} observation status`}>
            <div className="evidence-panel-topline">
              <div className="evidence-panel-status">
                <CheckCircle2 aria-hidden="true" size={17} />
                <div>
                  <h2>Observation contract connected</h2>
                  <p>Route-specific evidence panels are not yet available. No values are inferred from missing data.</p>
                </div>
              </div>
              <Badge tone="neutral"><code>{route.resource}</code></Badge>
            </div>
            <div className="evidence-panel-meta">
              <FreshnessBadge meta={meta} />
              <SourceEvidence meta={meta} />
              <Link href={hrefWithConnection("/sources", serializeQueryState(state))} className="evidence-link">Inspect sources <ArrowUpRight aria-hidden="true" size={12} /></Link>
            </div>
          </section>
          {empty ? (
            <EmptyState
              title="No observations returned"
              description="The request succeeded, but this view has no matching records. Missing observations are not evidence of health."
              coverage={route.key === "overview" && "observation_coverage" in data && typeof data.observation_coverage === "number" ? data.observation_coverage : undefined}
            />
          ) : null}
          {meta.capabilities.map((capability) => (
            <CapabilityNotice key={`${capability.feature}-${capability.status}`} capability={capability} />
          ))}
        </>
      ) : null}
    </div>
  );
}
