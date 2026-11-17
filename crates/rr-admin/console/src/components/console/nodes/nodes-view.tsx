"use client";

import type { CellContext, ColumnDef } from "@tanstack/react-table";
import { ArrowUpRight, Search } from "lucide-react";
import Link from "next/link";
import { usePathname, useRouter, useSearchParams } from "next/navigation";
import { useEffect, useRef } from "react";
import { CapabilityNotice } from "@/components/console/capability-notice";
import { DenseTable } from "@/components/console/dense-table";
import { EmptyState, ErrorState } from "@/components/console/states";
import { FreshnessBadge } from "@/components/console/freshness-badge";
import { HealthBadge } from "@/components/console/health-badge";
import { SourceEvidence } from "@/components/console/source-evidence";
import { Button } from "@/components/ui/button";
import { formatBytes, formatDuration, formatTimestamp } from "@/lib/format";
import { useConsoleResource } from "@/lib/use-console-resource";
import { filterNodes, formatRatio, nodeVersion, type FeatureNode, type NodeFilters } from "./model";
import { formatNetwork, isNodeTab, NodeDrawer } from "./node-drawer";

const filterKeys = ["filter", "status", "zone", "version", "role", "cursor"] as const;

function NodeDrawerCell({ row }: CellContext<FeatureNode, unknown>) {
  const router = useRouter();
  const pathname = usePathname();
  const searchParams = useSearchParams();
  const selectedNode = Number(searchParams.get("node_id")) || null;
  const requestedTab = searchParams.get("tab");
  const activeTab = isNodeTab(requestedTab) ? requestedTab : "overview";
  const updateQuery = (updates: Record<string, string | null>) => {
    const params = new URLSearchParams(searchParams.toString());
    if (!params.has("window")) params.set("window", "15m");
    for (const [key, value] of Object.entries(updates)) {
      if (value) params.set(key, value);
      else params.delete(key);
    }
    router.replace(`${pathname}?${params.toString()}`, { scroll: false });
  };

  return (
    <NodeDrawer
      node={row.original}
      open={selectedNode === row.original.node_id}
      activeTab={activeTab}
      onOpenChange={(open) => updateQuery({ node_id: open ? String(row.original.node_id) : null, tab: open ? activeTab : null })}
      onTabChange={(tab) => updateQuery({ tab })}
    />
  );
}

const nodeColumns: ColumnDef<FeatureNode>[] = [
  {
    id: "node",
    accessorKey: "node_id",
    header: "Node",
    cell: (context) => <NodeDrawerCell {...context} />,
  },
  {
    id: "address",
    accessorKey: "admin_addr",
    header: "Address / location",
    cell: ({ row }) => <div className="max-w-52 whitespace-normal"><code className="text-[11px]">{row.original.admin_addr ?? "—"}</code><p className="mt-1 text-[11px] text-(--tx-3)">{[row.original.region, row.original.zone, row.original.rack].filter(Boolean).join(" / ") || "Location —"}</p></div>,
  },
  { id: "status", accessorKey: "status", header: "Status", cell: ({ row }) => <HealthBadge status={row.original.status} /> },
  {
    id: "groups",
    accessorKey: "leader_group_count",
    header: "Leader / local groups",
    cell: ({ row }) => <span className="block text-right font-mono">{row.original.leader_group_count ?? "—"} / {row.original.group_ids.length}</span>,
  },
  {
    id: "cpu",
    header: "CPU",
    accessorFn: (node) => node.resources?.cpu_usage_ratio,
    cell: ({ row }) => <span className="block text-right font-mono" title={row.original.resources ? undefined : "No current resource sample"}>{formatRatio(row.original.resources?.cpu_usage_ratio)}</span>,
  },
  {
    id: "memory",
    header: "Memory",
    accessorFn: (node) => node.resources?.memory_used_bytes,
    cell: ({ row }) => <span className="block text-right font-mono">{formatBytes(row.original.resources?.memory_used_bytes)}<small className="block text-[10px] text-(--tx-4)">/ {formatBytes(row.original.resources?.memory_total_bytes)}</small></span>,
  },
  {
    id: "network",
    header: "Network RX / TX",
    accessorFn: (node) => node.resources?.network_rx_bytes_per_sec,
    cell: ({ row }) => <span className="block text-right font-mono text-[11px]">{formatNetwork(row.original.resources?.network_rx_bytes_per_sec, row.original.resources?.network_tx_bytes_per_sec)}</span>,
  },
  {
    id: "disk",
    header: "Disk",
    accessorFn: (node) => node.resources?.filesystem_used_bytes,
    cell: ({ row }) => <span className="block text-right font-mono text-[11px]">{formatBytes(row.original.resources?.filesystem_used_bytes)}<small className="block text-[10px] text-(--tx-4)">/ {formatBytes(row.original.resources?.filesystem_total_bytes)}</small></span>,
  },
  {
    id: "version",
    header: "App / Rr version",
    accessorFn: nodeVersion,
    cell: ({ row }) => <span>{nodeVersion(row.original) ?? "—"}<small className="block text-[10px] text-(--tx-4)">Rr {row.original.build?.rr_version ?? "—"}</small></span>,
  },
  { id: "uptime", accessorKey: "uptime_secs", header: "Uptime", cell: ({ row }) => <span className="block text-right font-mono">{formatDuration(row.original.uptime_secs)}</span> },
  { id: "contact", accessorKey: "last_success", header: "Last contact", cell: ({ row }) => formatTimestamp(row.original.last_contact ?? row.original.last_success) },
];

export function NodesView() {
  const router = useRouter();
  const pathname = usePathname();
  const searchParams = useSearchParams();
  const filters: NodeFilters = {
    filter: searchParams.get("filter") ?? undefined,
    status: searchParams.get("status") ?? undefined,
    zone: searchParams.get("zone") ?? undefined,
    version: searchParams.get("version") ?? undefined,
    role: searchParams.get("role") ?? undefined,
  };
  const cursor = searchParams.get("cursor") ?? undefined;
  const selectedNode = Number(searchParams.get("node_id")) || null;
  const requestedTab = searchParams.get("tab");
  const activeTab = isNodeTab(requestedTab) ? requestedTab : "overview";
  const { data, meta, error, loading, refresh } = useConsoleResource("/nodes", {
    filter: filters.filter,
    cursor,
    page_size: 50,
  });
  const nodes = (data?.items ?? []) as FeatureNode[];
  const visibleNodes = filterNodes(nodes, filters);
  const zones = [...new Set(nodes.map((node) => node.zone).filter((zone): zone is string => Boolean(zone)))].sort();
  const versions = [...new Set(nodes.map(nodeVersion).filter((version): version is string => Boolean(version)))].sort();
  const hasFilters = Object.values(filters).some(Boolean);
  const selectedOutsideFilters = selectedNode != null && !visibleNodes.some((node) => node.node_id === selectedNode)
    ? nodes.find((node) => node.node_id === selectedNode)
    : undefined;
  const previousSelectedNode = useRef(selectedNode);
  useEffect(() => {
    const previous = previousSelectedNode.current;
    previousSelectedNode.current = selectedNode;
    if (previous == null || selectedNode != null) return;
    const frame = requestAnimationFrame(() => {
      document.querySelector<HTMLButtonElement>(`[data-node-trigger="${previous}"]`)?.focus();
    });
    return () => cancelAnimationFrame(frame);
  }, [selectedNode]);

  const updateQuery = (updates: Record<string, string | null>) => {
    const params = new URLSearchParams(searchParams.toString());
    if (!params.has("window")) params.set("window", "15m");
    for (const [key, value] of Object.entries(updates)) {
      if (value) params.set(key, value);
      else params.delete(key);
    }
    router.replace(`${pathname}?${params.toString()}`, { scroll: false });
  };
  const updateFilter = (key: keyof NodeFilters, value: string) => updateQuery({ [key]: value || null, cursor: null, node_id: null, tab: null });
  const changeDrawer = (nodeId: number, open: boolean) => {
    updateQuery({ node_id: open ? String(nodeId) : null, tab: open ? activeTab : null });
  };
  const clearFilters = () => {
    const params = new URLSearchParams(searchParams.toString());
    for (const key of filterKeys) params.delete(key);
    params.delete("node_id");
    params.delete("tab");
    router.replace(`${pathname}?${params.toString()}`, { scroll: false });
  };

  return (
    <div className="route-page">
      <div className="page-heading">
        <div><div className="page-eyebrow">Cluster / Read-only</div><h1>Nodes</h1><p>Node reachability, resources, versions, and local runtime evidence.</p></div>
        <Button variant="secondary" size="small" onClick={refresh} disabled={loading}>Refresh observations</Button>
      </div>
      {error ? <ErrorState title={data ? "Showing the last successful observations" : "Console data unavailable"} message={error.message} correlationId={error.correlationId} recovery="Open Sources to inspect endpoint health and collection capabilities." /> : null}

      <section className="grid gap-3 border border-(--line-1) bg-(--bg-1) p-4" aria-label="Node filters">
        <div className="grid grid-cols-1 gap-3 sm:grid-cols-2 xl:grid-cols-5">
          <label className="text-[11px] text-(--tx-3)"><span className="mb-1 block">Search nodes</span><span className="relative block"><Search className="absolute top-2.5 left-2.5 text-(--tx-4)" aria-hidden="true" size={14} /><input className="input pl-8" value={filters.filter ?? ""} onChange={(event) => updateFilter("filter", event.target.value)} placeholder="Node, address, boot, commit" /></span></label>
          <label className="text-[11px] text-(--tx-3)"><span className="mb-1 block">Node status</span><select className="input" value={filters.status ?? ""} onChange={(event) => updateFilter("status", event.target.value)}><option value="">All statuses</option>{["ONLINE", "DEGRADED", "UNREACHABLE", "UNKNOWN"].map((status) => <option key={status}>{status}</option>)}</select></label>
          <label className="text-[11px] text-(--tx-3)"><span className="mb-1 block">Zone</span><select className="input" value={filters.zone ?? ""} onChange={(event) => updateFilter("zone", event.target.value)}><option value="">All zones</option>{zones.map((zone) => <option key={zone}>{zone}</option>)}</select></label>
          <label className="text-[11px] text-(--tx-3)"><span className="mb-1 block">Application version</span><select className="input" value={filters.version ?? ""} onChange={(event) => updateFilter("version", event.target.value)}><option value="">All versions</option>{versions.map((version) => <option key={version}>{version}</option>)}</select></label>
          <label className="text-[11px] text-(--tx-3)"><span className="mb-1 block">Raft role</span><select className="input" value={filters.role ?? ""} onChange={(event) => updateFilter("role", event.target.value)}><option value="">All roles</option><option value="leader">Has leader groups</option><option value="follower">No leader groups</option></select></label>
        </div>
        <div className="flex flex-wrap items-center justify-between gap-3"><p className="text-xs text-(--tx-3)" role="status">{visibleNodes.length} of {nodes.length} returned nodes match</p>{hasFilters ? <Button variant="ghost" size="small" onClick={clearFilters}>Clear filters</Button> : null}</div>
      </section>

      {loading && !data ? <div className="route-skeleton" role="status" aria-label="Loading Nodes"><span /><span /><span /></div> : null}
      {data && meta ? <>
        <div className="flex flex-wrap items-center gap-3 border border-(--line-1) bg-(--bg-1) px-4 py-2"><FreshnessBadge meta={meta} /><SourceEvidence meta={meta} /><Link href={`/sources?${searchParams.toString()}`} className="evidence-link">Inspect sources <ArrowUpRight aria-hidden="true" size={12} /></Link><span className="text-[11px] text-(--tx-4)">Observation contract connected</span></div>
        {meta.capabilities.map((capability) => <CapabilityNotice key={`${capability.feature}-${capability.status}`} capability={capability} />)}
        {visibleNodes.length ? <DenseTable data={visibleNodes} columns={nodeColumns} caption="Cluster nodes" /> : <EmptyState title="No nodes match this view" description="Adjust the filters or inspect Sources for discovery and collection failures. An empty result is not evidence of node health." />}
        <div className="flex items-center justify-between gap-3"><p className="text-xs text-(--tx-4)">Page size 50 · unavailable values remain —</p><div className="flex gap-2">{cursor ? <Button variant="secondary" size="small" onClick={() => updateQuery({ cursor: null, node_id: null, tab: null })}>First page</Button> : null}{data.next_cursor ? <Button variant="secondary" size="small" onClick={() => updateQuery({ cursor: data.next_cursor, node_id: null, tab: null })}>Next page</Button> : null}</div></div>
        {selectedOutsideFilters ? <div className="flex items-center gap-3 border border-(--line-1) bg-(--bg-1) p-3"><p className="text-xs text-(--tx-3)">The deep-linked node is outside the active filters.</p><NodeDrawer node={selectedOutsideFilters} open activeTab={activeTab} onOpenChange={(open) => changeDrawer(selectedOutsideFilters.node_id, open)} onTabChange={(tab) => updateQuery({ tab })} /></div> : null}
      </> : null}
    </div>
  );
}
