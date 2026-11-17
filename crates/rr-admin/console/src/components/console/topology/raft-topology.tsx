"use client";

import { useMemo } from "react";
import Link from "next/link";
import type { ColumnDef } from "@tanstack/react-table";
import { DenseTable } from "@/components/console/dense-table";
import { EmptyState } from "@/components/console/states";
import { HealthBadge } from "@/components/console/health-badge";
import { formatNumber, formatTimestamp } from "@/lib/format";
import type { TimeWindow } from "@/lib/query-state";
import type { ReplicaRow } from "@/components/console/groups/model";
import { ConsensusGraph, type ConsensusGraphEdge, type ConsensusGraphNode } from "@/components/console/topology/consensus-graph";
import { healthStatus, raftReplicaRows, rowNumber, topologyRows } from "@/components/console/topology/model";
import { RepresentationSwitch } from "@/components/console/topology/representation-switch";

function metric(value: number | null, suffix = "") {
  return value == null ? "—" : `${formatNumber(value)}${suffix}`;
}

export function RaftTopology({
  data,
  selectedGroupId,
  onSelectGroup,
  timeWindow,
}: {
  data: unknown;
  selectedGroupId?: number;
  onSelectGroup: (groupId: number) => void;
  timeWindow: TimeWindow;
}) {
  const groups = topologyRows(data, "raft");
  const selected = groups.find((group) => rowNumber(group, "group_id") === selectedGroupId) ?? groups[0];
  const groupId = selected ? rowNumber(selected, "group_id") : null;
  const replicas = selected ? raftReplicaRows(selected) : [];
  const columns = useMemo<ColumnDef<ReplicaRow>[]>(() => [
    { accessorKey: "node_id", header: "Node", cell: ({ getValue }) => `Node ${getValue<number>()}` },
    { accessorKey: "role", header: "Role" },
    { accessorKey: "term", header: "Term", cell: ({ getValue }) => metric(getValue<number | null>()) },
    { accessorKey: "last_index", header: "Last log", cell: ({ getValue }) => metric(getValue<number | null>()) },
    { accessorKey: "matched_index", header: "Matched", cell: ({ getValue }) => metric(getValue<number | null>()) },
    { accessorKey: "lag_entries", header: "Lag entries", cell: ({ getValue }) => metric(getValue<number | null>()) },
    { accessorKey: "append_rtt_p99_ms", header: "Append RTT P99", cell: ({ getValue }) => metric(getValue<number | null>(), " ms") },
    { accessorKey: "observed_at", header: "Sample", cell: ({ getValue }) => formatTimestamp(getValue<string | null>()) },
  ], []);
  if (!selected || groupId == null) return <EmptyState title="No Raft topology returned" description="No group observations are available. No leader or membership state is inferred." />;

  const leaderId = rowNumber(selected, "leader_node_id", "actual_leader_node_id");
  const leader = replicas.find((replica) => replica.node_id === leaderId);
  const graphNodes: ConsensusGraphNode[] = replicas.map((replica, index) => {
    const isLeader = replica.node_id === leaderId;
    const followers = replicas.filter((candidate) => candidate.node_id !== leaderId);
    const followerIndex = followers.findIndex((candidate) => candidate.node_id === replica.node_id);
    return {
      id: String(replica.node_id),
      label: `Node ${replica.node_id}`,
      sublabel: `${replica.role} · term ${metric(replica.term)}`,
      description: `Node ${replica.node_id} · ${replica.role} · term ${metric(replica.term)} · last log ${metric(replica.last_index)} · sample ${formatTimestamp(replica.observed_at)}`,
      href: `/nodes/?window=${timeWindow}&filter=node-${replica.node_id}`,
      x: isLeader || leaderId == null ? 210 : 680,
      y: isLeader || leaderId == null ? 70 + index * 90 : 60 + followerIndex * Math.min(95, 340 / Math.max(1, followers.length - 1)),
    };
  });
  const graphEdges: ConsensusGraphEdge[] = leaderId == null ? [] : replicas.filter((replica) => replica.node_id !== leaderId).map((replica) => ({
    id: `${groupId}-${leaderId}-${replica.node_id}`,
    source: String(leaderId),
    target: String(replica.node_id),
    label: `Group ${groupId} replication to Node ${replica.node_id}`,
    description: `Group ${groupId} · Leader node ${leaderId} → Node ${replica.node_id} · Lag ${replica.lag_entries == null ? "—" : `${formatNumber(replica.lag_entries)} entries`} · Append RTT P99 ${metric(replica.append_rtt_p99_ms, " ms")} · Sample ${formatTimestamp(replica.observed_at)}`,
  }));
  const graph = (
    <div className="grid gap-3">
      <ConsensusGraph
        title={`Raft topology graph for group ${groupId}`}
        description={`Group ${groupId} replication direction. Only this selected group is drawn; line width carries no traffic meaning.`}
        nodes={graphNodes}
        edges={graphEdges}
        height={Math.max(440, replicas.length * 75 + 80)}
      />
      {leaderId == null ? <p className="text-xs text-zinc-400">No leader was observed, so no replication arrows are drawn.</p> : null}
      {healthStatus(selected.health) === "SYNCING" ? <p className="text-xs text-orange-300">Observations are syncing across terms or revisions. Non-atomic samples are not treated as a same-term conflict.</p> : null}
    </div>
  );
  const table = (
    <div className="grid gap-3">
      <div className="flex flex-wrap items-center justify-between gap-3">
        <HealthBadge status={healthStatus(selected.health)} />
        <Link className="text-xs text-orange-300" href={`/groups/?window=${timeWindow}&group_id=${groupId}`}>Inspect group {groupId}</Link>
      </div>
      <DenseTable data={replicas} columns={columns} caption={`Raft topology for group ${groupId}`} />
    </div>
  );
  const largeDataReason = replicas.length > 40 ? `${replicas.length} replicas exceed the 40-replica graph budget` : null;
  return (
    <div className="grid gap-4">
      <div className="flex flex-wrap items-end justify-between gap-3">
        <label className="grid gap-1 text-xs text-zinc-400">
          Raft group
          <select className="input min-w-56" aria-label="Raft group" value={groupId} onChange={(event) => onSelectGroup(Number(event.target.value))}>
            {groups.map((group) => {
              const id = rowNumber(group, "group_id");
              return id == null ? null : <option key={id} value={id}>Group {id} · {healthStatus(group.health)}</option>;
            })}
          </select>
        </label>
        <p className="text-xs text-zinc-400">{groups.length} groups available · one bounded graph at a time{leader ? ` · leader node ${leader.node_id}` : ""}</p>
      </div>
      <RepresentationSwitch label={`Raft topology for group ${groupId}`} graph={graph} table={table} largeDataReason={largeDataReason} />
    </div>
  );
}
