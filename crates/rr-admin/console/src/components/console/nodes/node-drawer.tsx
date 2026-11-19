"use client";

import { useEffect, useId, useState, type KeyboardEvent } from "react";
import Link from "next/link";
import { useSearchParams } from "next/navigation";
import { Drawer } from "@/components/ui/drawer";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { CapabilityNotice } from "@/components/console/capability-notice";
import { ErrorState } from "@/components/console/states";
import { HealthBadge } from "@/components/console/health-badge";
import { MetricChart } from "@/components/console/metric-chart";
import { SourceEvidence } from "@/components/console/source-evidence";
import { ConsoleApiError, getConsoleResource } from "@/lib/api";
import type { BootRecord, ConsoleEnvelope, ConsoleMeta, CursorPage } from "@/lib/contract";
import { formatBytes, formatDuration, formatNumber, formatTimestamp } from "@/lib/format";
import { useConsoleResource } from "@/lib/use-console-resource";
import { useRouteQueryState } from "@/lib/use-route-query-state";
import { useRefresh } from "@/lib/refresh-context";
import { shutdownLabel } from "@/components/console/lifecycle/boot-timeline";
import { NodeResources } from "./node-resources";
import { safeConfigValue, type FeatureNode, type NodeDetailData } from "./model";

const tabs = [
  ["overview", "Overview"],
  ["resources", "Resources"],
  ["raft", "Raft"],
  ["configuration", "Configuration"],
  ["history", "Restarts & versions"],
] as const;

type NodeTab = (typeof tabs)[number][0];

interface DetailState {
  data: NodeDetailData | null;
  meta: ConsoleMeta | null;
  error: ConsoleApiError | null;
  loading: boolean;
}

function useNodeDetail(nodeId: number) {
  const { connectionId } = useRefresh();
  const [reload, setReload] = useState(0);
  const [state, setState] = useState<DetailState>({ data: null, meta: null, error: null, loading: true });
  useEffect(() => {
    if (!connectionId) return;
    const owner = connectionId;
    const controller = new AbortController();
    const load = async () => {
      setState((current) => ({ ...current, loading: true, error: null }));
      try {
        const result: ConsoleEnvelope<NodeDetailData> = await getConsoleResource<NodeDetailData>(owner, `/nodes/${nodeId}`, {}, controller.signal);
        if (!controller.signal.aborted) setState({ data: result.data, meta: result.meta, error: null, loading: false });
      } catch (error) {
        if (controller.signal.aborted) return;
        setState((current) => ({
          ...current,
          loading: false,
          error: error instanceof ConsoleApiError ? error : new ConsoleApiError(0, "NETWORK_ERROR", "Node detail request failed", null),
        }));
      }
    };
    void load();
    return () => controller.abort();
  }, [connectionId, nodeId, reload]);
  return { ...state, refresh: () => setReload((value) => value + 1) };
}

function Field({ label, children }: { label: string; children: React.ReactNode }) {
  return <div className="min-w-0 border-t border-(--line-1) py-2 first:border-t-0"><dt className="text-[11px] text-(--tx-4)">{label}</dt><dd className="mt-1 break-words text-xs text-(--tx-2)">{children}</dd></div>;
}

function NodeOverview({ detail }: { detail: NodeDetailData }) {
  const node = detail.node;
  const build = node.build;
  return (
    <div className="grid gap-4">
      <div className="flex flex-wrap items-center gap-2"><HealthBadge status={node.status} /><Badge tone={node.ready ? "ok" : "neutral"}>Ready: {node.ready == null ? "—" : node.ready ? "true" : "false"}</Badge><Badge tone={node.liveness ? "ok" : "neutral"}>Liveness: {node.liveness == null ? "—" : node.liveness ? "true" : "false"}</Badge></div>
      <dl className="border border-(--line-1) bg-(--bg-1) px-3">
        <Field label="Node ID">{node.node_id}</Field>
        <Field label="Client address"><code>{node.listen_addr ?? "—"}</code></Field>
        <Field label="Admin address"><code>{node.admin_addr ?? "—"}</code></Field>
        <Field label="Observability address"><code>{node.observability_addr ?? "—"}</code></Field>
        <Field label="Location">{[node.region, node.zone, node.rack].filter(Boolean).join(" / ") || "—"}</Field>
        <Field label="Config fingerprint"><code>{node.config_fingerprint ?? build?.config_fingerprint ?? "—"}</code></Field>
        <Field label="Boot ID"><code>{node.boot_id ?? build?.boot_id ?? "—"}</code></Field>
        <Field label="Process started">{formatTimestamp(node.process_start_time ?? node.process_started_at ?? build?.process_start_time)}</Field>
        <Field label="Uptime">{formatDuration(node.uptime_secs)}</Field>
      </dl>
      <section><h3>Current build</h3><dl className="mt-2 border border-(--line-1) bg-(--bg-1) px-3"><Field label="Application version">{build?.app_version ?? node.version ?? "—"}</Field><Field label="Rr version">{build?.rr_version ?? "—"}</Field><Field label="Git commit"><code>{build?.git_commit ?? "—"}</code>{build?.dirty ? <Badge className="ml-2" tone="accent">Dirty build</Badge> : null}</Field><Field label="Build profile">{build?.profile ?? "—"}</Field></dl></section>
    </div>
  );
}

function ResourceTrends({ nodeId }: { nodeId: number }) {
  const { state } = useRouteQueryState();
  const { data } = useConsoleResource("/metrics/performance", { window: state.window, node_id: nodeId });
  const series = (data?.series ?? []).filter((item) => item.metric.startsWith("rr_node_"));
  if (!series.length) return <p className="text-xs text-(--tx-3)">No resource trend points are retained for this window. Current samples above remain available; no history is inferred.</p>;
  const timestamps = [...new Set(series.flatMap((item) => item.points.map((point) => point.timestamp)))].sort();
  const rows = timestamps.map((timestamp) => {
    const row: Record<string, string | number | null> = { timestamp: formatTimestamp(timestamp).replace(" UTC", "") };
    for (const item of series) row[item.metric] = item.points.find((point) => point.timestamp === timestamp)?.value ?? null;
    return row;
  });
  return <MetricChart title="Resource trends" summary={`${data?.retention_source === "CONSOLE_MEMORY" ? "Console memory retention" : "Reported retention source"} · raw node resource families`} data={rows} xKey="timestamp" series={series.map((item) => ({ key: item.metric, label: item.metric.replaceAll("rr_node_", "").replaceAll("_", " ") }))} />;
}

function NodeRaft({ detail }: { detail: NodeDetailData }) {
  const nodeId = detail.node.node_id;
  const searchParams = useSearchParams();
  const groupHref = (groupId: number) => {
    const params = new URLSearchParams(searchParams.toString());
    params.set("group_id", String(groupId));
    params.delete("node_id");
    params.delete("tab");
    return `/groups?${params.toString()}`;
  };
  if (!detail.groups.length) return <CapabilityNotice capability={{ feature: "Local Raft observations", status: "UNSUPPORTED", reason: "No local group observations were returned for this node." }} />;
  return (
    <div className="dense-table-scroll"><table className="dense-table" aria-label={`Node ${nodeId} Raft groups`}><thead><tr><th scope="col">Group</th><th scope="col">Role</th><th scope="col">Term</th><th scope="col">Last / commit / applied</th><th scope="col">Leader</th><th scope="col">Matched / lag</th><th scope="col">Observed</th></tr></thead><tbody>
      {detail.groups.map((group) => {
        const observation = group.observations?.find((item) => item.node_id === nodeId);
        const leader = observation?.is_leader ?? group.leader_node_id === nodeId;
        return <tr key={group.group_id}><td><Link className="text-(--acc)" href={groupHref(group.group_id)}>Group {group.group_id}</Link></td><td>{observation ? leader ? "LEADER" : "FOLLOWER" : "—"}</td><td>{formatNumber(observation?.current_term ?? group.current_term)}</td><td className="font-mono">{formatNumber(observation?.last_log_index ?? group.last_log_index)} / {formatNumber(observation?.commit_index)} / {formatNumber(observation?.applied_index)}</td><td>{observation?.leader_node_id == null ? "—" : `Node ${observation.leader_node_id}`}</td><td>{!leader && observation?.matched_index == null ? <span className="whitespace-normal text-[11px]">Non-leader source; matched and lag unavailable</span> : `${formatNumber(observation?.matched_index)} / ${formatNumber(observation?.lag_entries)}`}</td><td>{formatTimestamp(observation?.observed_at)}</td></tr>;
      })}
    </tbody></table></div>
  );
}

const allowedConfigKeys = new Set(["storage", "listen_addr", "admin_addr", "observability_addr", "region", "zone", "rack", "cluster_name", "journal_persistence"]);

function NodeConfiguration({ node }: { node: FeatureNode }) {
  const entries = Object.entries(node.config ?? {}).filter(([key]) => allowedConfigKeys.has(key));
  if (!entries.length) return <CapabilityNotice capability={{ feature: "Allowlisted configuration", status: "UNSUPPORTED", reason: "No non-sensitive configuration projection was returned. Arbitrary node keys are never rendered." }} />;
  return (
    <div className="grid gap-3"><p className="text-xs text-(--tx-3)">Only the documented allowlist is displayed. No cluster-default authority or seed baseline is inferred.</p><div className="dense-table-scroll"><table className="dense-table" aria-label="Allowlisted node configuration"><thead><tr><th scope="col">Field</th><th scope="col">Value</th><th scope="col">Built-in default diff</th></tr></thead><tbody>{entries.map(([key, value]) => <tr key={key}><td><code>{key}</code></td><td>{safeConfigValue(value)}</td><td>—</td></tr>)}</tbody></table></div></div>
  );
}

function NodeHistory({ node }: { node: FeatureNode }) {
  const { connectionId } = useRefresh();
  const [cursor, setCursor] = useState<string | undefined>();
  const [state, setState] = useState<{ data: CursorPage<BootRecord> | null; meta: ConsoleMeta | null; error: ConsoleApiError | null; loading: boolean }>({ data: null, meta: null, error: null, loading: true });
  useEffect(() => {
    if (!connectionId) return;
    const owner = connectionId;
    const controller = new AbortController();
    const load = async () => {
      setState((current) => ({ ...current, loading: true, error: null }));
      try {
        const result = await getConsoleResource<CursorPage<BootRecord>>(owner, `/nodes/${node.node_id}/boots`, { cursor, page_size: 50 }, controller.signal);
        if (!controller.signal.aborted) setState({ data: result.data, meta: result.meta, error: null, loading: false });
      } catch (error) {
        if (controller.signal.aborted) return;
        setState((current) => ({ ...current, loading: false, error: error instanceof ConsoleApiError ? error : new ConsoleApiError(0, "NETWORK_ERROR", "Boot history request failed", null) }));
      }
    };
    void load();
    return () => controller.abort();
  }, [connectionId, node.node_id, cursor]);
  return (
    <div className="grid gap-4">
      {state.error ? <ErrorState title="Boot history unavailable" message={state.error.message} correlationId={state.error.correlationId} /> : null}
      {state.loading && !state.data ? <p role="status">Loading boot records…</p> : null}
      {state.meta?.capabilities.map((capability) => <CapabilityNotice key={capability.feature} capability={capability} />)}
      {state.data && !state.data.items.length ? <CapabilityNotice capability={{ feature: "Boot history", status: "UNSUPPORTED", reason: "No boot records were returned. This can mean an unsupported node, an empty journal, or an unreachable source; inspect response capabilities and sources." }} /> : null}
      {state.data?.items.length ? <div className="dense-table-scroll"><table className="dense-table" aria-label={`Node ${node.node_id} boot history`}><thead><tr><th scope="col">Boot ID</th><th scope="col">Started</th><th scope="col">Shutdown</th><th scope="col">Persistence</th><th scope="col">App / Rr</th><th scope="col">Commit</th></tr></thead><tbody>{state.data.items.map((boot) => <tr key={boot.boot_id}><td><code>{boot.boot_id}</code></td><td>{formatTimestamp(boot.started_at)}</td><td>{shutdownLabel(boot.shutdown_state)}</td><td><Badge tone={boot.persistence === "VOLATILE" ? "accent" : "neutral"}>{boot.persistence}</Badge></td><td>{boot.app_version ?? "—"} / {boot.rr_version ?? "—"}</td><td><code>{boot.git_commit ?? "—"}</code></td></tr>)}</tbody></table></div> : null}
      <div className="flex gap-2">{cursor ? <Button variant="secondary" size="small" onClick={() => setCursor(undefined)}>Newest boots</Button> : null}{state.data?.next_cursor ? <Button variant="secondary" size="small" onClick={() => setCursor(state.data?.next_cursor ?? undefined)}>Older boots</Button> : null}</div>
    </div>
  );
}

function NodeDetailContent({ nodeId, activeTab, onTabChange }: { nodeId: number; activeTab: NodeTab; onTabChange: (tab: NodeTab) => void }) {
  const { data, meta, error, loading, refresh } = useNodeDetail(nodeId);
  const baseId = useId();
  const onKeys = (event: KeyboardEvent<HTMLDivElement>) => {
    if (!["ArrowLeft", "ArrowRight", "Home", "End"].includes(event.key)) return;
    event.preventDefault();
    const index = tabs.findIndex(([key]) => key === activeTab);
    const next = event.key === "Home" ? 0 : event.key === "End" ? tabs.length - 1 : event.key === "ArrowRight" ? (index + 1) % tabs.length : (index - 1 + tabs.length) % tabs.length;
    const tab = tabs[next]!;
    onTabChange(tab[0]);
    document.getElementById(`${baseId}-${tab[0]}`)?.focus();
  };
  return (
    <div className="grid gap-4">
      <div className="flex items-center justify-between gap-3">{meta ? <SourceEvidence meta={meta} /> : <span />}<Button variant="ghost" size="small" onClick={refresh} disabled={loading}>Refresh node</Button></div>
      {error ? <ErrorState title={data ? "Showing last node detail" : "Node detail unavailable"} message={error.message} correlationId={error.correlationId} /> : null}
      {loading && !data ? <p role="status">Loading node detail…</p> : null}
      {data ? <>
        <div role="tablist" aria-label={`Node ${nodeId} detail sections`} className="flex overflow-x-auto border-b border-(--line-2)" onKeyDown={onKeys}>
          {tabs.map(([key, label]) => <button key={key} id={`${baseId}-${key}`} type="button" role="tab" aria-selected={activeTab === key} aria-controls={`${baseId}-panel`} tabIndex={activeTab === key ? 0 : -1} onClick={() => onTabChange(key)} className="shrink-0 border-b-2 border-transparent px-3 py-2 text-[11px] text-(--tx-3) aria-selected:border-(--acc) aria-selected:text-(--tx-1)">{label}</button>)}
        </div>
        <div role="tabpanel" id={`${baseId}-panel`} aria-labelledby={`${baseId}-${activeTab}`} className="min-w-0">
          {activeTab === "overview" ? <NodeOverview detail={data} /> : null}
          {activeTab === "resources" ? <div className="grid gap-5"><NodeResources node={data.node} /><ResourceTrends nodeId={nodeId} /></div> : null}
          {activeTab === "raft" ? <NodeRaft detail={data} /> : null}
          {activeTab === "configuration" ? <NodeConfiguration node={data.node} /> : null}
          {activeTab === "history" ? <NodeHistory node={data.node} /> : null}
        </div>
      </> : null}
    </div>
  );
}

export function NodeDrawer({
  node,
  open,
  activeTab,
  onOpenChange,
  onTabChange,
}: {
  node: FeatureNode;
  open: boolean;
  activeTab: NodeTab;
  onOpenChange: (open: boolean) => void;
  onTabChange: (tab: NodeTab) => void;
}) {
  const { connectionId } = useRefresh();
  return (
    <Drawer
      trigger={<button type="button" data-node-trigger={node.node_id} className="cursor-pointer font-medium text-(--acc)" aria-label={`Open node ${node.node_id} details`}>Node {node.node_id}</button>}
      title={`Node ${node.node_id}`}
      description="Runtime, resource, Raft, configuration, and lifecycle evidence"
      open={open}
      onOpenChange={onOpenChange}
      width={560}
    >
      <NodeDetailContent key={`${connectionId ?? "none"}:${node.node_id}`} nodeId={node.node_id} activeTab={activeTab} onTabChange={onTabChange} />
    </Drawer>
  );
}

export function isNodeTab(value: string | null): value is NodeTab {
  return tabs.some(([key]) => key === value);
}

export function formatNetwork(rx: number | null | undefined, tx: number | null | undefined): string {
  return `↓ ${formatBytes(rx)}/s · ↑ ${formatBytes(tx)}/s`;
}
