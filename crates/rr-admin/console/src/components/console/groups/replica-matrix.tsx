"use client";

import { useMemo, type ReactNode } from "react";
import Link from "next/link";
import type { ColumnDef } from "@tanstack/react-table";
import { DenseTable } from "@/components/console/dense-table";
import { formatAge, formatNumber, formatTimestamp } from "@/lib/format";
import type { TimeWindow } from "@/lib/query-state";
import type { ReplicaRow } from "@/components/console/groups/model";

function number(value: number | null, suffix = "") {
  return value == null ? "—" : `${formatNumber(value)}${suffix}`;
}

export function ReplicaMatrix({
  replicas,
  leaderNodeId,
  timeWindow = "15m",
}: {
  replicas: ReplicaRow[];
  leaderNodeId: number | null;
  timeWindow?: TimeWindow;
}) {
  const columns = useMemo<ColumnDef<ReplicaRow>[]>(() => {
    const leaderOnly = (value: number | null, sourceNodeId: number, suffix = ""): ReactNode => {
      if (leaderNodeId == null || sourceNodeId !== leaderNodeId) {
        return <span title="Matched index and lag are reported by the leader">Non-leader source; no data</span>;
      }
      if (value != null) return number(value, suffix);
      return <span title="The leader did not report this value">—</span>;
    };
    return [
      {
        accessorKey: "node_id",
        header: "Node",
        cell: ({ getValue }) => {
          const nodeId = getValue<number>();
          return <Link className="text-orange-300 hover:underline" href={`/nodes/?window=${timeWindow}&filter=node-${nodeId}`}>Node {nodeId}</Link>;
        },
      },
      { accessorKey: "server_state", header: "ServerState", cell: ({ getValue }) => getValue<string | null>() ?? "—" },
      { accessorKey: "role", header: "Role", cell: ({ getValue }) => getValue<string>().toLowerCase().replace(/^./, (letter) => letter.toUpperCase()) },
      { accessorKey: "term", header: "Term", cell: ({ getValue }) => number(getValue<number | null>()) },
      { accessorKey: "last_index", header: "Last", cell: ({ getValue }) => number(getValue<number | null>()) },
      { accessorKey: "commit_index", header: "Commit", cell: ({ getValue }) => number(getValue<number | null>()) },
      { accessorKey: "applied_index", header: "Applied", cell: ({ getValue }) => number(getValue<number | null>()) },
      {
        id: "matched",
        header: "Matched",
        accessorFn: (row) => row.matched_index ?? -1,
        cell: ({ row }) => leaderOnly(row.original.matched_index, row.original.source_node_id),
      },
      {
        id: "lag",
        header: "Lag entries",
        accessorFn: (row) => row.lag_entries ?? -1,
        cell: ({ row }) => leaderOnly(row.original.lag_entries, row.original.source_node_id),
      },
      {
        accessorKey: "heartbeat_age_ms",
        header: "Heartbeat age",
        cell: ({ getValue }) => {
          const value = getValue<number | null>();
          return value == null ? "—" : `${formatNumber(value / 1_000)}s`;
        },
      },
      { accessorKey: "heartbeat_rtt_p99_ms", header: "Heartbeat RTT P99", cell: ({ getValue }) => number(getValue<number | null>(), " ms") },
      { accessorKey: "append_rtt_p99_ms", header: "Append RTT P99", cell: ({ getValue }) => number(getValue<number | null>(), " ms") },
      { accessorKey: "snapshot_index", header: "Snapshot", cell: ({ getValue }) => number(getValue<number | null>()) },
      {
        id: "source",
        header: "Source & freshness",
        accessorFn: (row) => row.age_ms,
        cell: ({ row }) => (
          <span title={row.original.observed_at ? `Observed ${formatTimestamp(row.original.observed_at)}` : "Observation time unavailable"}>
            Source node {row.original.source_node_id} · {formatAge(row.original.age_ms)}
          </span>
        ),
      },
    ];
  }, [leaderNodeId, timeWindow]);
  return <DenseTable data={replicas} columns={columns} caption="Group replica evidence" />;
}
