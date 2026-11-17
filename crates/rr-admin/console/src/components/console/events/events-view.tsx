"use client";

import { ArrowUpRight } from "lucide-react";
import Link from "next/link";
import { useState } from "react";
import type { ColumnDef } from "@tanstack/react-table";
import { CapabilityNotice } from "@/components/console/capability-notice";
import { DenseTable } from "@/components/console/dense-table";
import { FreshnessBadge } from "@/components/console/freshness-badge";
import { SourceEvidence } from "@/components/console/source-evidence";
import { EmptyState, ErrorState } from "@/components/console/states";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import type { EventItem } from "@/lib/contract";
import { formatDuration, formatTimestamp, sanitizeError } from "@/lib/format";
import { serializeQueryState } from "@/lib/query-state";
import { useConsoleResource } from "@/lib/use-console-resource";
import { useRouteQueryState } from "@/lib/use-route-query-state";

const PREFERENCES_KEY = "rr.console.event-preferences.v1";
type EventPreference = { acknowledged_at?: string; silenced_until?: string };
type EventPreferences = Record<string, EventPreference>;
type EventRow = EventItem & { kind?: string; message?: string };
type LegacyEvent = EventItem & {
  id?: string;
  kind?: string;
  node_id?: number | null;
  group_id?: number | null;
  message?: string;
};

function normalizeEvent(event: EventItem): EventRow {
  const legacy = event as LegacyEvent;
  return {
    ...event,
    event_id: event.event_id ?? legacy.id ?? `observed-${event.first_observed_at}-${legacy.node_id ?? legacy.group_id ?? "cluster"}`,
    object: event.object ?? (legacy.group_id != null ? `group-${legacy.group_id}` : legacy.node_id != null ? `node-${legacy.node_id}` : "cluster"),
    evidence: event.evidence ?? [],
    kind: legacy.kind,
    message: legacy.message,
  };
}

function readPreferences(): EventPreferences {
  try {
    const parsed: unknown = JSON.parse(window.localStorage.getItem(PREFERENCES_KEY) ?? "{}");
    if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) return {};
    return Object.fromEntries(Object.entries(parsed).flatMap(([eventId, value]) => {
      if (!value || typeof value !== "object" || Array.isArray(value)) return [];
      const preference = value as Record<string, unknown>;
      return [[eventId, {
        acknowledged_at: typeof preference.acknowledged_at === "string" ? preference.acknowledged_at : undefined,
        silenced_until: typeof preference.silenced_until === "string" ? preference.silenced_until : undefined,
      }]];
    }));
  } catch {
    return {};
  }
}

function observedDuration(event: EventItem) {
  const first = Date.parse(event.first_observed_at);
  const last = Date.parse(event.last_observed_at);
  return Number.isFinite(first) && Number.isFinite(last) && last >= first ? formatDuration((last - first) / 1_000) : "—";
}

export function EventsView() {
  const { state, setFilter } = useRouteQueryState();
  const [cursor, setCursor] = useState<string | undefined>();
  const [preferences, setPreferences] = useState<EventPreferences>(readPreferences);
  const [showSilenced, setShowSilenced] = useState(false);
  const [preferenceError, setPreferenceError] = useState<string | null>(null);
  const { data, meta, error, loading, refresh } = useConsoleResource("/events", {
    severity: state.filters.severity,
    state: state.filters.state,
    cursor,
    page_size: 50,
  });
  const now = meta ? Date.parse(meta.generated_at) : Number.NaN;
  const sourcesHref = `/sources/?${serializeQueryState(state)}`;
  const silencedUntil = (event: EventItem) => {
    const value = preferences[event.event_id]?.silenced_until;
    if (!value) return null;
    const timestamp = Date.parse(value);
    return Number.isFinite(timestamp) && timestamp > now ? value : null;
  };
  const observedEvents = data?.items.map(normalizeEvent) ?? [];
  const silencedCount = observedEvents.filter((event) => silencedUntil(event) != null).length;
  const visibleEvents = observedEvents.filter((event) => showSilenced || silencedUntil(event) == null);

  const updatePreference = (eventId: string, nextPreference: EventPreference) => {
    const next = { ...preferences, [eventId]: nextPreference };
    setPreferences(next);
    try {
      window.localStorage.setItem(PREFERENCES_KEY, JSON.stringify(next));
      setPreferenceError(null);
    } catch {
      setPreferenceError("This browser could not persist the preference. It applies only until this page is closed.");
    }
  };

  const columns: ColumnDef<EventRow>[] = [
    {
      id: "event",
      header: "Event / object",
      accessorFn: (event) => `${event.event_id} ${event.object}`,
      cell: ({ row }) => <div className="grid gap-0.5"><code className="text-xs">{row.original.event_id}</code><span>{row.original.object}</span>{row.original.kind ? <span>{row.original.kind}</span> : null}{row.original.message ? <span className="whitespace-normal text-(--tx-3)">{row.original.message}</span> : null}</div>,
    },
    {
      accessorKey: "severity",
      header: "Severity",
      cell: ({ row }) => <Badge tone={row.original.severity === "ERROR" ? "bad" : row.original.severity === "WARN" ? "accent" : "neutral"}>{row.original.severity}</Badge>,
    },
    { accessorKey: "state", header: "Observed state" },
    { accessorKey: "first_observed_at", header: "First observed", cell: ({ row }) => formatTimestamp(row.original.first_observed_at) },
    { accessorKey: "last_observed_at", header: "Last observed", cell: ({ row }) => formatTimestamp(row.original.last_observed_at) },
    { id: "duration", header: "Observed duration", accessorFn: observedDuration },
    {
      id: "evidence",
      header: "Evidence",
      accessorFn: (event) => `${event.evidence.length} sources`,
      cell: ({ row }) => (
        <details>
          <summary className="cursor-pointer">{row.original.evidence.length} sources</summary>
          <ul className="mt-2 grid gap-1 whitespace-normal text-xs">
            {row.original.evidence.map((source, index) => <li key={`${source.node_id}-${source.endpoint_role}-${index}`}>{source.node_id == null ? "Cluster" : `Node ${source.node_id}`} · {source.endpoint_role} · {source.status}{source.error_code ? ` · ${sanitizeError(source.error_code)}` : ""}</li>)}
          </ul>
        </details>
      ),
    },
    {
      id: "preference",
      header: "Local preference",
      accessorFn: (event) => silencedUntil(event) ? "Silenced locally" : preferences[event.event_id]?.acknowledged_at ? "Acknowledged locally" : "None",
      cell: ({ row }) => {
        const event = row.original;
        const silenced = silencedUntil(event);
        const acknowledged = preferences[event.event_id]?.acknowledged_at;
        return (
          <div className="flex items-center gap-2">
            {silenced ? <Badge tone="accent">Silenced locally</Badge> : acknowledged ? <Badge tone="neutral">Acknowledged locally</Badge> : null}
            <Button
              variant="ghost"
              size="small"
              disabled={Boolean(acknowledged)}
              aria-label={`Acknowledge ${event.event_id}`}
              onClick={() => updatePreference(event.event_id, { ...preferences[event.event_id], acknowledged_at: new Date().toISOString() })}
            >Acknowledge</Button>
            {silenced ? (
              <Button variant="ghost" size="small" aria-label={`Unsilence ${event.event_id}`} onClick={() => updatePreference(event.event_id, { ...preferences[event.event_id], silenced_until: undefined })}>Unsilence</Button>
            ) : (
              <Button variant="ghost" size="small" aria-label={`Silence 1h: ${event.event_id}`} onClick={() => updatePreference(event.event_id, { ...preferences[event.event_id], silenced_until: new Date(Date.now() + 3_600_000).toISOString() })}>Silence 1h</Button>
            )}
          </div>
        );
      },
    },
  ];

  return (
    <div className="route-page">
      <div className="page-heading">
        <div>
          <div className="page-eyebrow">Observe / Read-only</div>
          <h1>Events & alerts</h1>
          <p>Console-observed events and threshold alerts with source evidence. Acknowledgement and silence are browser-local preferences only.</p>
        </div>
        <Button variant="secondary" size="small" onClick={refresh} disabled={loading}>Refresh observations</Button>
      </div>

      <section className="flex flex-wrap items-end gap-3 border border-(--line-1) bg-(--bg-1) p-4" aria-label="Event filters">
        <label className="grid gap-1 text-xs text-(--tx-3)">
          Severity
          <select className="input w-40" value={state.filters.severity ?? ""} onChange={(event) => { setCursor(undefined); setFilter("severity", event.target.value); }}>
            <option value="">All severities</option><option value="INFO">INFO</option><option value="WARN">WARN</option><option value="ERROR">ERROR</option>
          </select>
        </label>
        <label className="grid gap-1 text-xs text-(--tx-3)">
          State
          <select className="input w-40" value={state.filters.state ?? ""} onChange={(event) => { setCursor(undefined); setFilter("state", event.target.value); }}>
            <option value="">All states</option><option value="ACTIVE">ACTIVE</option><option value="RESOLVED">RESOLVED</option>
          </select>
        </label>
      </section>

      {error ? <ErrorState title={data ? "Showing the last successful observations" : "Event data unavailable"} message={error.message} correlationId={error.correlationId} recovery="Inspect Sources before treating empty event results as healthy." /> : null}
      {preferenceError ? <p className="text-xs text-(--bad)" role="alert">{preferenceError}</p> : null}
      {loading && !data ? <div className="route-skeleton" role="status" aria-label="Loading Events and alerts"><span /><span /><span /></div> : null}

      {data && meta ? (
        <>
          <section className="evidence-panel-meta border border-(--line-1)" aria-label="Event observation status">
            <FreshnessBadge meta={meta} />
            <SourceEvidence meta={meta} />
            <Link href={sourcesHref} className="evidence-link">Inspect sources <ArrowUpRight aria-hidden="true" size={12} /></Link>
          </section>
          {meta.capabilities.map((capability) => <CapabilityNotice key={`${capability.feature}-${capability.status}-${capability.reason}`} capability={capability} />)}
          {silencedCount ? (
            <div className="flex flex-wrap items-center justify-between gap-3 border border-(--line-1) bg-(--bg-1) p-3" role="status">
              <span>{silencedCount} {silencedCount === 1 ? "event" : "events"} silenced by local preference</span>
              <Button variant="ghost" size="small" onClick={() => setShowSilenced((value) => !value)}>{showSilenced ? "Hide silenced events" : "Show silenced events"}</Button>
            </div>
          ) : null}
          {visibleEvents.length ? <DenseTable data={visibleEvents} columns={columns} caption="Observed events and alerts" /> : (
            <EmptyState
              title={silencedCount && !showSilenced ? "No unsilenced events" : "No matching observed events"}
              description={meta.partial ? "Observation is partial; this does not prove the cluster is healthy." : "No matching events were returned by the Console observation ring. Preferences do not delete event history."}
            />
          )}
          {data.next_cursor ? <Button variant="secondary" size="small" className="justify-self-start" onClick={() => setCursor(data.next_cursor ?? undefined)}>Next page</Button> : null}
        </>
      ) : null}
    </div>
  );
}
