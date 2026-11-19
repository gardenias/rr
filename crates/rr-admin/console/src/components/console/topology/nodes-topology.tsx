"use client";

import { useMemo } from "react";
import Link from "next/link";
import type { ColumnDef } from "@tanstack/react-table";
import { DenseTable } from "@/components/console/dense-table";
import { EmptyState } from "@/components/console/states";
import { HealthBadge } from "@/components/console/health-badge";
import { formatNumber, formatTimestamp } from "@/lib/format";
import { connectionDrilldownHref, type TimeWindow } from "@/lib/query-state";
import { ConsensusGraph, type ConsensusGraphEdge, type ConsensusGraphNode } from "@/components/console/topology/consensus-graph";
import { asRecord, healthStatus, rowNumber, rowNumberList, rowString, topologyEdges, topologyRows, type TopologyRow } from "@/components/console/topology/model";
import { RepresentationSwitch } from "@/components/console/topology/representation-switch";

function placement(row: TopologyRow) {
  return [rowString(row, "region"), rowString(row, "zone"), rowString(row, "rack")].filter(Boolean).join(" / ") || "Unlabeled placement";
}

function resourceSummary(row: TopologyRow) {
  const resources = asRecord(row.resources);
  const cpu = rowNumber(row, "cpu_usage_ratio") ?? rowNumber(resources, "cpu_usage_ratio");
  return cpu == null ? "Resources —" : `CPU ${formatNumber(cpu * 100)}%`;
}

export function NodesTopology({ data, timeWindow, connectionId }: { data: unknown; timeWindow: TimeWindow; connectionId?: string }) {
  const nodes = topologyRows(data, "nodes");
  const edges = topologyEdges(data);
  const nodeColumns = useMemo<ColumnDef<TopologyRow>[]>(() => [
    {
      id: "node",
      header: "Node",
      accessorFn: (row) => rowNumber(row, "node_id") ?? 0,
      cell: ({ row }) => {
        const id = rowNumber(row.original, "node_id");
        return id == null ? "—" : <Link className="text-orange-300" href={connectionDrilldownHref("/nodes/", timeWindow, connectionId, { filter: `node-${id}` })}>Node {id}</Link>;
      },
    },
    { id: "placement", header: "Region / zone / rack", accessorFn: placement },
    { id: "status", header: "Observation", accessorFn: (row) => healthStatus(row.status), cell: ({ row }) => <HealthBadge status={healthStatus(row.original.status)} /> },
    { id: "resources", header: "Resource hotspot", accessorFn: resourceSummary },
    { id: "version", header: "Version", accessorFn: (row) => rowString(row, "version") ?? "—" },
    { id: "groups", header: "Local groups", accessorFn: (row) => rowNumberList(row, "group_ids").join(", ") || "—" },
  ], [connectionId, timeWindow]);
  const edgeColumns = useMemo<ColumnDef<TopologyRow>[]>(() => [
    { id: "source", header: "From", accessorFn: (row) => rowNumber(row, "source_node_id", "from_node_id", "source") ?? "—", cell: ({ getValue }) => typeof getValue() === "number" ? `Node ${getValue<number>()}` : "—" },
    { id: "target", header: "To", accessorFn: (row) => rowNumber(row, "target_node_id", "to_node_id", "target") ?? "—", cell: ({ getValue }) => typeof getValue() === "number" ? `Node ${getValue<number>()}` : "—" },
    { id: "relationship", header: "Observed relationship", accessorFn: (row) => {
      const count = rowNumber(row, "group_count");
      return `Near 5m AppendEntries${count == null ? "" : ` · ${formatNumber(count)} groups`}`;
    } },
    { id: "sample", header: "Sample", accessorFn: (row) => rowString(row, "observed_at", "sampled_at") ?? "—", cell: ({ getValue }) => formatTimestamp(getValue<string>()) },
  ], []);
  if (!nodes.length) return <EmptyState title="No node topology returned" description="Node placement and communication relationships are unavailable. No network reachability is inferred." />;

  const placements = [...new Set(nodes.map(placement))];
  const perPlacement = new Map(placements.map((name) => [name, nodes.filter((node) => placement(node) === name)]));
  const graphNodes: ConsensusGraphNode[] = nodes.flatMap((row) => {
    const id = rowNumber(row, "node_id");
    if (id == null) return [];
    const place = placement(row);
    const groupIndex = placements.indexOf(place);
    const siblings = perPlacement.get(place) ?? [];
    const siblingIndex = siblings.indexOf(row);
    return [{
      id: String(id),
      label: `Node ${id}`,
      sublabel: `${healthStatus(row.status)} · ${rowString(row, "zone") ?? "zone —"}`,
      description: `Node ${id} · ${place} · ${healthStatus(row.status)} · ${resourceSummary(row)} · Version ${rowString(row, "version") ?? "—"}`,
      href: connectionDrilldownHref("/nodes/", timeWindow, connectionId, { filter: `node-${id}` }),
      x: 150 + groupIndex * 280,
      y: 75 + siblingIndex * 95,
    }];
  });
  const graphEdges: ConsensusGraphEdge[] = edges.flatMap((row, index) => {
    const source = rowNumber(row, "source_node_id", "from_node_id", "source");
    const target = rowNumber(row, "target_node_id", "to_node_id", "target");
    if (source == null || target == null) return [];
    const count = rowNumber(row, "group_count");
    const relationship = `Near 5m AppendEntries${count == null ? "" : ` · ${formatNumber(count)} groups`}`;
    return [{
      id: rowString(row, "edge_id") ?? `${source}-${target}-${index}`,
      source: String(source),
      target: String(target),
      label: `Node ${source} to Node ${target} · ${relationship}`,
      description: `Node ${source} → Node ${target} · ${relationship} · Sample ${formatTimestamp(rowString(row, "observed_at", "sampled_at"))}. This observed Raft relationship is not a physical-network assertion.`,
    }];
  });
  const graphHeight = Math.max(480, ...[...perPlacement.values()].map((group) => group.length * 95 + 100));
  const graph = (
    <div className="grid gap-3">
      <ConsensusGraph
        title="Node topology graph"
        description="Nodes are grouped by region, zone, and rack. Edges represent only observed AppendEntries relationships in the near-5m window; they are not physical-network claims."
        nodes={graphNodes}
        edges={graphEdges}
        width={Math.max(960, placements.length * 280 + 80)}
        height={graphHeight}
      />
      {!graphEdges.length ? <p className="text-xs text-zinc-400">No observed AppendEntries edges were reported. Absence of an edge does not prove network isolation.</p> : null}
    </div>
  );
  const table = (
    <div className="grid gap-4">
      <p className="text-xs text-zinc-400">Nodes are grouped by region, zone, and rack. Edges represent only observed AppendEntries relationships in the near-5m window; they are not physical-network claims.</p>
      <DenseTable data={nodes} columns={nodeColumns} caption="Node topology" />
      <section aria-label="Observed Raft communication">
        <h3 className="mb-2 text-xs font-medium text-zinc-300">Observed Raft communication</h3>
        {edges.length ? <DenseTable data={edges} columns={edgeColumns} caption="Observed AppendEntries relationships" /> : <p className="text-xs text-zinc-400">No observed AppendEntries edges were reported.</p>}
      </section>
    </div>
  );
  const largeDataReason = nodes.length > 40
    ? `${nodes.length} nodes exceed the 40-node graph budget`
    : edges.length > 80 ? `${edges.length} edges exceed the 80-edge graph budget` : null;
  return <RepresentationSwitch label="Node topology" graph={graph} table={table} largeDataReason={largeDataReason} />;
}
