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
import { Button } from "@/components/ui/button";
import type { SnapshotItem } from "@/lib/contract";
import { formatNumber } from "@/lib/format";
import { serializeQueryState } from "@/lib/query-state";
import { useConsoleResource } from "@/lib/use-console-resource";
import { useRouteQueryState } from "@/lib/use-route-query-state";

type LegacySnapshotItem = SnapshotItem & {
  last_snapshot_index?: number | null;
  last_snapshot_term?: number | null;
};

function snapshotIndex(item: SnapshotItem) {
  return item.snapshot_index ?? (item as LegacySnapshotItem).last_snapshot_index ?? null;
}

function snapshotTerm(item: SnapshotItem) {
  return item.snapshot_term ?? (item as LegacySnapshotItem).last_snapshot_term ?? null;
}

function snapshotDistance(item: SnapshotItem) {
  const index = snapshotIndex(item);
  if (index == null || item.last_log_index == null) return null;
  return Math.max(0, item.last_log_index - index);
}

const columns: ColumnDef<SnapshotItem>[] = [
  { accessorKey: "group_id", header: "Group" },
  { accessorKey: "node_id", header: "Node" },
  {
    id: "snapshot",
    header: "Snapshot index / term",
    accessorFn: (item) => snapshotIndex(item) == null
      ? "No snapshot observed yet"
      : `${formatNumber(snapshotIndex(item))} / ${formatNumber(snapshotTerm(item))}`,
  },
  { accessorKey: "last_log_index", header: "Last log" },
  {
    id: "distance",
    header: "Log distance",
    accessorFn: (item) => {
      const distance = snapshotDistance(item);
      return distance == null ? null : `${formatNumber(distance)} entries`;
    },
  },
  { accessorKey: "install_count", header: "Observed installs" },
  { id: "build_failure", header: "Group build failure", accessorFn: () => "UNSUPPORTED" },
  { accessorKey: "source_count", header: "Sources" },
];

export function SnapshotsView() {
  const { state, setFilter } = useRouteQueryState();
  const [cursorState, setCursorState] = useState<{
    connectionId: string | null;
    value: string | undefined;
  }>({ connectionId: state.connection, value: undefined });
  const cursor = cursorState.connectionId === state.connection ? cursorState.value : undefined;
  const setCursor = (value: string | undefined) => setCursorState({ connectionId: state.connection, value });
  const { data, meta, error, loading, refresh } = useConsoleResource("/snapshots", {
    group_id: state.filters.group_id,
    node_id: state.filters.node_id,
    cursor,
    page_size: 50,
  });
  const buildFailureNotices = meta?.capabilities.filter((capability) => /build/i.test(capability.feature) && /fail/i.test(capability.feature)) ?? [];
  const groupsWithSnapshots = new Set(data?.items.filter((item) => snapshotIndex(item) != null).map((item) => item.group_id)).size;
  const sourcesHref = `/sources/?${serializeQueryState(state)}`;

  return (
    <div className="route-page">
      <div className="page-heading">
        <div>
          <div className="page-eyebrow">Observe / Read-only</div>
          <h1>Snapshots & storage</h1>
          <p>Observed snapshot indexes and storage evidence. Task progress and per-group failures are never inferred from legacy counters.</p>
        </div>
        <Button variant="secondary" size="small" onClick={refresh} disabled={loading}>Refresh observations</Button>
      </div>

      <section className="flex flex-wrap items-end gap-3 border border-(--line-1) bg-(--bg-1) p-4" aria-label="Snapshot filters">
        <label className="grid gap-1 text-xs text-(--tx-3)">
          Group ID
          <input className="input w-40" inputMode="numeric" value={state.filters.group_id ?? ""} onChange={(event) => { setCursor(undefined); setFilter("group_id", event.target.value); }} placeholder="All groups" />
        </label>
        <label className="grid gap-1 text-xs text-(--tx-3)">
          Node ID
          <input className="input w-40" inputMode="numeric" value={state.filters.node_id ?? ""} onChange={(event) => { setCursor(undefined); setFilter("node_id", event.target.value); }} placeholder="All nodes" />
        </label>
      </section>

      {error ? <ErrorState title={data ? "Showing the last successful observations" : "Snapshot data unavailable"} message={error.message} correlationId={error.correlationId} recovery="Inspect Sources for node collection and capability failures." /> : null}
      {loading && !data ? <div className="route-skeleton" role="status" aria-label="Loading Snapshots and storage"><span /><span /><span /></div> : null}

      {data && meta ? (
        <>
          <section className="evidence-panel-meta border border-(--line-1)" aria-label="Snapshot observation status">
            <FreshnessBadge meta={meta} />
            <SourceEvidence meta={meta} />
            <Link href={sourcesHref} className="evidence-link">Inspect sources <ArrowUpRight aria-hidden="true" size={12} /></Link>
          </section>

          <div className="grid gap-3 sm:grid-cols-2 xl:grid-cols-4">
            <section className="border border-(--line-1) bg-(--bg-1) p-4"><h2>Groups with snapshots</h2><p className="mt-2 font-mono text-xl text-(--tx-1)">{groupsWithSnapshots}</p></section>
            <section className="border border-(--line-1) bg-(--bg-1) p-4"><h2>Group × node observations</h2><p className="mt-2 font-mono text-xl text-(--tx-1)">{data.items.length}</p></section>
            <section className="border border-(--line-1) bg-(--bg-1) p-4"><h2>Install counters available</h2><p className="mt-2 font-mono text-xl text-(--tx-1)">{data.items.filter((item) => item.install_count != null).length}</p></section>
            <section className="border border-(--line-1) bg-(--bg-1) p-4"><h2>Node-level build failures</h2><p className="mt-2 text-xs text-(--tx-3)">{buildFailureNotices.length ? "Observed; see evidence below" : "No group attribution available"}</p></section>
          </div>

          {buildFailureNotices.map((capability) => (
            <aside className="capability-notice" data-status={capability.status.toLowerCase()} key={`${capability.feature}-${capability.reason}`}>
              <div><strong>Node-level build failures</strong><p>{capability.reason ?? "Build failures have no group label; Group × Node cells remain UNSUPPORTED."}</p></div>
            </aside>
          ))}
          {meta.capabilities.filter((capability) => !buildFailureNotices.includes(capability)).map((capability) => <CapabilityNotice key={`${capability.feature}-${capability.status}-${capability.reason}`} capability={capability} />)}

          {data.items.length ? (
            <DenseTable data={data.items} columns={columns} caption="Snapshots by group and node" />
          ) : (
            <EmptyState title="No snapshot observations for this filter" description="No snapshot index is inferred. If a group has a last log but no snapshot, it remains an observed no-snapshot state rather than 0% progress." />
          )}

          <section className="border border-(--line-1) bg-(--bg-1) p-4" aria-labelledby="storage-boundaries">
            <h2 id="storage-boundaries">Storage evidence boundaries</h2>
            <p className="mt-2 text-xs text-(--tx-3)">Send/receive bytes, transfer durations, RocksDB/WAL metrics, and node disk capacity are shown only when supplied by the Console contract. Per-group disk usage, build activity, transfer activity, and progress percentages are not reconstructed from indexes or the deprecated boolean.</p>
          </section>
          {data.next_cursor ? <Button variant="secondary" size="small" className="justify-self-start" onClick={() => setCursor(data.next_cursor ?? undefined)}>Next page</Button> : null}
        </>
      ) : null}
    </div>
  );
}
