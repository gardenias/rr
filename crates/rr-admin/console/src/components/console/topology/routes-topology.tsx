"use client";

import { useMemo } from "react";
import Link from "next/link";
import type { ColumnDef } from "@tanstack/react-table";
import { DenseTable } from "@/components/console/dense-table";
import { EmptyState } from "@/components/console/states";
import { formatNumber, formatTimestamp } from "@/lib/format";
import type { TimeWindow } from "@/lib/query-state";
import { asRecord, rowBoolean, rowNumber, rowNumberList, rowString, topologyRows, type TopologyRow } from "@/components/console/topology/model";

export function RoutesTopology({
  data,
  timeWindow,
  filter,
}: {
  data: unknown;
  timeWindow: TimeWindow;
  filter?: string;
}) {
  const allRoutes = topologyRows(data, "routes");
  const routes = allRoutes.filter((route) => {
    if (!filter) return true;
    const partitionId = rowNumber(route, "partition_id");
    const groupId = rowNumber(route, "group_id");
    if (filter === `partition-${partitionId}` || filter === `group-${groupId}`) return true;
    return `${partitionId ?? ""} ${groupId ?? ""}`.includes(filter);
  });
  const columns = useMemo<ColumnDef<TopologyRow>[]>(() => [
    {
      id: "partition",
      header: "Partition",
      accessorFn: (row) => rowNumber(row, "partition_id") ?? 0,
      cell: ({ row }) => {
        const id = rowNumber(row.original, "partition_id");
        return id == null ? "—" : <Link className="text-orange-300" href={`/topology/?window=${timeWindow}&view=routes&filter=partition-${id}`}>Partition {id}</Link>;
      },
    },
    {
      id: "group",
      header: "Expected group",
      accessorFn: (row) => rowNumber(row, "group_id") ?? "—",
      cell: ({ row }) => {
        const id = rowNumber(row.original, "group_id");
        return id == null ? "—" : <Link className="text-orange-300" href={`/groups/?window=${timeWindow}&group_id=${id}`}>Group {id}</Link>;
      },
    },
    { id: "replicas", header: "Configured replicas", accessorFn: (row) => rowNumberList(row, "replica_node_ids", "replicas").map((id) => `Node ${id}`).join(", ") || "—" },
    { id: "leader", header: "Actual leader", accessorFn: (row) => rowNumber(row, "leader_node_id", "actual_leader_node_id") ?? "—", cell: ({ getValue }) => typeof getValue() === "number" ? `Node ${getValue<number>()}` : "—" },
    { id: "revision", header: "Configuration revision", accessorFn: (row) => rowString(row, "revision", "topology_revision") ?? rowString(asRecord(data), "revision", "topology_revision") ?? "—", cell: ({ getValue }) => `Revision ${getValue<string>()}` },
    {
      id: "status",
      header: "Route evidence",
      accessorFn: (row) => rowBoolean(row, "route_not_found") === true ? "Route not found" : rowBoolean(row, "stale", "stale_route") === true ? "Stale route" : "Observed route",
    },
    { id: "sample", header: "Sample", accessorFn: (row) => rowString(row, "observed_at", "sampled_at") ?? "—", cell: ({ getValue }) => formatTimestamp(getValue<string>()) },
  ], [data, timeWindow]);
  if (!routes.length) {
    return <EmptyState title={filter ? "No routes match this filter" : "No partition routes returned"} description="Missing route observations are not treated as an empty routing table or proof of data loss." />;
  }
  return (
    <section className="grid gap-3" aria-label="Partition and route evidence">
      <div className="flex flex-wrap items-center justify-between gap-3 text-xs text-zinc-400">
        <p>Expected partition routes and observed leaders remain separate. P0 is read-only; no migration controls are available.</p>
        <span>{formatNumber(routes.length)} of {formatNumber(allRoutes.length)} routes</span>
      </div>
      <DenseTable data={routes} columns={columns} caption="Partition and route topology" />
    </section>
  );
}
