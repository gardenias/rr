"use client";

import { useMemo } from "react";
import type { ColumnDef } from "@tanstack/react-table";
import { DenseTable } from "@/components/console/dense-table";
import { HealthBadge } from "@/components/console/health-badge";
import { Button } from "@/components/ui/button";
import { formatLeader, formatNumber } from "@/lib/format";
import type { GroupListItem } from "@/components/console/groups/model";

function metric(value: number | null | undefined, suffix = "") {
  return value == null ? "—" : `${formatNumber(value)}${suffix}`;
}

export function GroupTable({
  groups,
  onSelect,
}: {
  groups: GroupListItem[];
  onSelect: (groupId: number) => void;
}) {
  const columns = useMemo<ColumnDef<GroupListItem>[]>(() => [
    {
      accessorKey: "group_id",
      header: "Group",
      cell: ({ row }) => (
        <Button
          variant="ghost"
          size="small"
          aria-label={`Inspect group ${row.original.group_id}`}
          onClick={() => onSelect(row.original.group_id)}
        >
          Group {row.original.group_id}
        </Button>
      ),
    },
    { accessorKey: "formation", header: "Formation", cell: ({ getValue }) => getValue<string>() ?? "—" },
    { accessorKey: "health", header: "Observed health", cell: ({ row }) => <HealthBadge status={row.original.health} /> },
    {
      id: "leader",
      header: "Leader",
      accessorFn: (row) => row.leader_node_id ?? 0,
      cell: ({ row }) => formatLeader(row.original.leader_node_id),
    },
    { accessorKey: "current_term", header: "Term", cell: ({ getValue }) => formatNumber(getValue<number | null>()) },
    {
      id: "voters",
      header: "Voters reachable / total",
      accessorFn: (row) => row.voters_reachable == null ? null : row.voters_reachable / (row.voters_total || 1),
      cell: ({ row }) => row.original.voters_reachable == null || row.original.voters_total == null
        ? "—"
        : `${formatNumber(row.original.voters_reachable)} / ${formatNumber(row.original.voters_total)}`,
    },
    { accessorKey: "learners", header: "Learners", cell: ({ getValue }) => formatNumber(getValue<number | null>()) },
    { accessorKey: "max_lag_entries", header: "Max lag entries", cell: ({ getValue }) => metric(getValue<number | null>()) },
    { accessorKey: "max_append_rtt_p99_ms", header: "Max append RTT P99", cell: ({ getValue }) => metric(getValue<number | null>(), " ms") },
    { accessorKey: "last_log_index", header: "Last log", cell: ({ getValue }) => formatNumber(getValue<number | null>()) },
    { accessorKey: "snapshot_index", header: "Snapshot index", cell: ({ getValue }) => formatNumber(getValue<number | null>()) },
    { accessorKey: "tps", header: "TPS", cell: ({ getValue }) => metric(getValue<number | null>(), "/s") },
    { accessorKey: "source_count", header: "Sources", cell: ({ getValue }) => formatNumber(getValue<number>()) },
  ], [onSelect]);
  return <DenseTable data={groups} columns={columns} caption="Raft groups" />;
}
