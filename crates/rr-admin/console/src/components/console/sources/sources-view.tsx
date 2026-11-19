"use client";

import { useEffect, useRef, useState } from "react";
import Link from "next/link";
import type { ColumnDef } from "@tanstack/react-table";
import { CapabilityNotice } from "@/components/console/capability-notice";
import { DenseTable } from "@/components/console/dense-table";
import { FreshnessBadge } from "@/components/console/freshness-badge";
import { SourceEvidence } from "@/components/console/source-evidence";
import { EmptyState, ErrorState } from "@/components/console/states";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { consoleApi, ConsoleApiError } from "@/lib/api";
import { loginRedirectFor } from "@/lib/auth";
import type { SourcesData } from "@/lib/contract";
import { formatTimestamp, sanitizeError } from "@/lib/format";
import { useConsoleResource } from "@/lib/use-console-resource";
import { useRefresh } from "@/lib/refresh-context";

type SourceRow = SourcesData["sources"][number];
type LegacySourceRow = SourceRow & {
  last_attempt?: string | null;
  error?: { code?: string; message?: string } | null;
};
type LegacySourcesData = SourcesData & {
  mode?: string;
  console_version?: string;
  generation?: number;
};
type ReconcileFeedback = { message: string; reauthenticate: boolean } | null;

const columns: ColumnDef<SourceRow>[] = [
  { id: "node", header: "Node", accessorFn: (source) => source.node_id == null ? "Cluster" : `Node ${source.node_id}` },
  { id: "admin", header: "Admin address", accessorFn: (source) => source.admin_addr ?? "—" },
  { id: "observability", header: "Observability address", accessorFn: (source) => source.observability_addr ?? "—" },
  { id: "security", header: "TLS / mTLS", accessorFn: () => "Not reported" },
  { accessorKey: "circuit_state", header: "Circuit", cell: ({ row }) => <Badge tone={row.original.circuit_state === "OPEN" ? "bad" : row.original.circuit_state === "CLOSED" ? "ok" : "accent"}>{row.original.circuit_state}</Badge> },
  { id: "last_success", header: "Last success", accessorFn: (source) => formatTimestamp(source.last_success) },
  { id: "last_failure", header: "Last failure", accessorFn: (source) => formatTimestamp(source.last_failure) },
  { id: "error", header: "Sanitized error", accessorFn: (source) => source.sanitized_error ? sanitizeError(source.sanitized_error) : "—" },
];

export function SourcesView() {
  const { connectionId } = useRefresh();
  const { data, meta, error, loading, refresh } = useConsoleResource("/sources");
  const [pending, setPending] = useState(false);
  const [feedback, setFeedback] = useState<ReconcileFeedback>(null);
  const reconcileController = useRef<AbortController | null>(null);

  useEffect(() => () => {
    reconcileController.current?.abort();
  }, [connectionId]);

  const reconcile = async () => {
    if (!connectionId) return;
    const controller = new AbortController();
    reconcileController.current?.abort();
    reconcileController.current = controller;
    setPending(true);
    setFeedback(null);
    try {
      await consoleApi.reconcile(connectionId, controller.signal);
      if (controller.signal.aborted) return;
      setFeedback({ message: "Reconciliation completed. Source evidence has been refreshed.", reauthenticate: false });
      refresh();
    } catch (cause) {
      if (controller.signal.aborted) return;
      const apiError = cause instanceof ConsoleApiError ? cause : null;
      const reauthenticate = apiError?.status === 401 || apiError?.status === 403;
      setFeedback({
        message: reauthenticate
          ? `Reauthentication required. The viewer session or CSRF token is no longer valid.${apiError?.correlationId ? ` Correlation ID: ${apiError.correlationId}.` : ""}`
          : `Reconciliation failed: ${sanitizeError(apiError?.message ?? "The Console could not complete the request.")}`,
        reauthenticate,
      });
    } finally {
      if (reconcileController.current === controller) {
        reconcileController.current = null;
        setPending(false);
      }
    }
  };

  const prometheusSources = meta?.sources.filter((source) => source.endpoint_role === "PROMETHEUS_HISTORY" || source.endpoint_role === "PROMETHEUS") ?? [];
  const unmapped = prometheusSources.some((source) => source.status === "UNMAPPED");
  const retention = meta?.retention_source ?? "UNKNOWN";
  const prometheus = data?.prometheus;
  const legacyData = data as LegacySourcesData | null;
  const sourceRows = data?.sources.map((source) => {
    const legacy = source as LegacySourceRow;
    return {
      ...source,
      last_failure: source.last_failure ?? legacy.last_attempt ?? null,
      circuit_state: source.circuit_state ?? "UNKNOWN",
      sanitized_error: source.sanitized_error ?? (legacy.error ? `${legacy.error.code ?? "ERROR"}: ${legacy.error.message ?? "Source request failed"}` : null),
    } satisfies SourceRow;
  }) ?? [];

  return (
    <div className="route-page">
      <div className="page-heading">
        <div>
          <div className="page-eyebrow">System / Read-only</div>
          <h1>Sources</h1>
          <p>Inspect discovery, endpoint health, capabilities, and history retention without exposing credentials or sensitive configuration.</p>
        </div>
        <Button variant="secondary" size="small" onClick={() => void reconcile()} disabled={!connectionId || pending || loading}>{pending ? "Reconciling…" : "Reconcile now"}</Button>
      </div>

      {feedback ? (
        <div className={feedback.reauthenticate ? "error-state" : "border border-(--line-1) bg-(--bg-1) p-3"} role={feedback.reauthenticate ? "alert" : "status"}>
          <div>
            <p>{feedback.message}</p>
            {feedback.reauthenticate ? <Link className="button button-secondary button-size-small mt-3 inline-flex" href={loginRedirectFor(`${window.location.pathname}${window.location.search}`)}>Reauthenticate</Link> : null}
          </div>
        </div>
      ) : null}
      {error ? <ErrorState title={data ? "Showing the last successful observations" : "Source data unavailable"} message={error.message} correlationId={error.correlationId} recovery="Retry after checking Console connectivity. Sensitive upstream details remain in server logs." /> : null}
      {loading && !data ? <div className="route-skeleton" role="status" aria-label="Loading Sources"><span /><span /><span /></div> : null}

      {data && meta ? (
        <>
          <section className="evidence-panel-meta border border-(--line-1)" aria-label="Source observation status">
            <FreshnessBadge meta={meta} />
            <SourceEvidence meta={meta} />
            <Badge tone="system">Read-only</Badge>
          </section>

          <div className="grid gap-3 sm:grid-cols-2 xl:grid-cols-4">
            <section className="border border-(--line-1) bg-(--bg-1) p-4"><h2>Startup mode</h2><p className="mt-2 text-(--tx-1)">{data.startup_mode ?? legacyData?.mode ?? "UNKNOWN"}</p></section>
            <section className="border border-(--line-1) bg-(--bg-1) p-4"><h2>Cluster name</h2><p className="mt-2 text-(--tx-1)">{data.cluster_name ?? "Not reported"}</p></section>
            <section className="border border-(--line-1) bg-(--bg-1) p-4"><h2>Topology revision</h2><p className="mt-2 font-mono text-(--tx-1)">{data.topology_revision ?? "—"}</p></section>
            <section className="border border-(--line-1) bg-(--bg-1) p-4"><h2>Console version / generation</h2><p className="mt-2 text-xs text-(--tx-3)">{legacyData?.console_version ? `${legacyData.console_version} · generation ${legacyData.generation ?? "—"}` : legacyData?.generation != null ? `Generation ${legacyData.generation}` : `Observed at ${formatTimestamp(meta.generated_at)}`}</p></section>
          </div>

          {meta.capabilities.map((capability) => <CapabilityNotice key={`${capability.feature}-${capability.status}-${capability.reason}`} capability={capability} />)}

          <section className="grid min-w-0 gap-3" aria-labelledby="node-sources">
            <div><h2 id="node-sources">Node endpoints</h2><p className="mt-1 text-xs text-(--tx-3)">Addresses come from trusted seeds or descriptors. TLS/mTLS is not inferred when the response does not report it.</p></div>
            {sourceRows.length ? <DenseTable data={sourceRows} columns={columns} caption="Node source health" /> : <EmptyState title="No node sources reported" description="The Console has not returned a trusted node endpoint observation." />}
          </section>

          <section className="border border-(--line-1) bg-(--bg-1) p-4" aria-labelledby="history-source">
            <div className="flex flex-wrap items-center gap-2">
              <h2 id="history-source">Prometheus and history retention</h2>
              {unmapped ? <Badge tone="bad">UNMAPPED</Badge> : null}
            </div>
            <p className="mt-2 text-xs text-(--tx-3)">
              {retention === "CONSOLE_MEMORY" ? "Console memory retention · this Console process only" : retention === "PROMETHEUS" ? "Prometheus retained history · cluster and node labels enforced" : retention === "NONE" ? "No retained history is available" : "Retention source unknown"}
            </p>
            {prometheus?.url ? (
              <p className="mt-2 text-xs text-(--tx-3)">Endpoint <code>{prometheus.url}</code>{prometheus.cluster_matcher ? <> · cluster matcher <code>{prometheus.cluster_matcher}</code></> : null}</p>
            ) : <p className="mt-2 text-xs text-(--tx-3)">{prometheus?.status === "UNSUPPORTED" ? "Prometheus history is not configured." : "Prometheus endpoint not reported."}</p>}
            {unmapped ? <p className="mt-2 text-xs text-(--bad)">Prometheus history cannot be mapped safely. Missing or ambiguous node identities are never inferred from instance addresses.</p> : null}
            {prometheusSources.length ? (
              <ul className="mt-3 grid gap-2 text-xs">
                {prometheusSources.map((source, index) => <li key={`${source.status}-${index}`}>{source.status} · {source.error_code ? sanitizeError(source.error_code) : "No error code"} · observed {formatTimestamp(source.observed_at)}</li>)}
              </ul>
            ) : <p className="mt-2 text-xs text-(--tx-3)">No Prometheus source observation was returned.</p>}
          </section>
        </>
      ) : null}
    </div>
  );
}
