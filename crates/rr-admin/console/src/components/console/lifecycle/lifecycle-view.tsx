"use client";

import { ArrowUpRight, Search } from "lucide-react";
import Link from "next/link";
import { usePathname, useRouter, useSearchParams } from "next/navigation";
import type { KeyboardEvent } from "react";
import { CapabilityNotice } from "@/components/console/capability-notice";
import { EmptyState, ErrorState } from "@/components/console/states";
import { FreshnessBadge } from "@/components/console/freshness-badge";
import { HealthBadge } from "@/components/console/health-badge";
import { SourceEvidence } from "@/components/console/source-evidence";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import type { NodeItem } from "@/lib/contract";
import { formatDuration, formatTimestamp } from "@/lib/format";
import { useConsoleResource } from "@/lib/use-console-resource";
import { BootTimeline, type TimelineBoot } from "./boot-timeline";
import { VersionMatrix, type VersionItem } from "./version-matrix";

const views = [["current", "Current"], ["restarts", "Restarts"], ["versions", "Versions"]] as const;
type LifecycleViewKind = (typeof views)[number][0];

interface CurrentLifecycleItem extends Partial<NodeItem> {
  node_id: number;
  process_start_time?: string | null;
  process_started_at?: string | null;
  boot_id?: string | null;
  ready_phase?: string | null;
  app_version?: string | null;
  rr_version?: string | null;
  git_commit?: string | null;
  config_hash?: string | null;
  config_fingerprint?: string | null;
  observed_at?: string | null;
}

function isView(value: string | null): value is LifecycleViewKind {
  return views.some(([key]) => key === value);
}

export function LifecycleView() {
  const router = useRouter();
  const pathname = usePathname();
  const searchParams = useSearchParams();
  const requestedView = searchParams.get("view");
  const view = isView(requestedView) ? requestedView : "current";
  const cursor = searchParams.get("cursor") ?? undefined;
  const filter = searchParams.get("filter")?.toLowerCase() ?? "";
  const { data, meta, error, loading, refresh } = useConsoleResource("/lifecycle", { view, cursor, page_size: 50 });
  const allItems = data?.items ?? [];
  const items = allItems.filter((item) => {
    if (!filter) return true;
    const candidate = item as Partial<TimelineBoot & VersionItem & CurrentLifecycleItem>;
    return [candidate.boot_id, candidate.git_commit, candidate.app_version, candidate.rr_version, candidate.node_id == null ? null : `node ${candidate.node_id}`]
      .some((value) => value?.toLowerCase().includes(filter));
  });

  const updateQuery = (updates: Record<string, string | null>) => {
    const params = new URLSearchParams(searchParams.toString());
    if (!params.has("window")) params.set("window", "15m");
    for (const [key, value] of Object.entries(updates)) {
      if (value) params.set(key, value);
      else params.delete(key);
    }
    router.replace(`${pathname}?${params.toString()}`, { scroll: false });
  };
  const onTabKeys = (event: KeyboardEvent<HTMLDivElement>) => {
    if (!["ArrowLeft", "ArrowRight", "Home", "End"].includes(event.key)) return;
    event.preventDefault();
    const index = views.findIndex(([key]) => key === view);
    const next = event.key === "Home" ? 0 : event.key === "End" ? views.length - 1 : event.key === "ArrowRight" ? (index + 1) % views.length : (index - 1 + views.length) % views.length;
    const nextView = views[next]![0];
    updateQuery({ view: nextView, cursor: null });
    document.getElementById(`lifecycle-tab-${nextView}`)?.focus();
  };

  return (
    <div className="route-page">
      <div className="page-heading">
        <div><div className="page-eyebrow">Observe / Read-only</div><h1>Lifecycle</h1><p>Boot history, shutdown evidence, and application and Rr versions.</p></div>
        <Button variant="secondary" size="small" onClick={refresh} disabled={loading}>Refresh observations</Button>
      </div>
      {error ? <ErrorState title={data ? "Showing the last successful observations" : "Lifecycle data unavailable"} message={error.message} correlationId={error.correlationId} recovery="Inspect Sources for node boot-journal and build-information capabilities." /> : null}

      <div className="flex flex-col gap-3 border border-(--line-1) bg-(--bg-1) p-3 sm:flex-row sm:items-end sm:justify-between">
        <div role="tablist" aria-label="Lifecycle views" className="flex overflow-x-auto border-b border-(--line-2)" onKeyDown={onTabKeys}>
          {views.map(([key, label]) => <button id={`lifecycle-tab-${key}`} key={key} type="button" role="tab" aria-selected={view === key} aria-controls="lifecycle-panel" tabIndex={view === key ? 0 : -1} onClick={() => updateQuery({ view: key, cursor: null })} className="shrink-0 border-b-2 border-transparent px-4 py-2 text-xs text-(--tx-3) aria-selected:border-(--acc) aria-selected:text-(--tx-1)">{label}</button>)}
        </div>
        <label className="block text-[11px] text-(--tx-3)"><span className="sr-only">Search lifecycle records</span><span className="relative block"><Search className="absolute top-2.5 left-2.5 text-(--tx-4)" aria-hidden="true" size={14} /><input className="input min-w-56 pl-8" value={searchParams.get("filter") ?? ""} onChange={(event) => updateQuery({ filter: event.target.value || null, cursor: null })} placeholder="Boot ID, commit, version, node" /></span></label>
      </div>

      {loading && !data ? <div className="route-skeleton" role="status" aria-label="Loading Lifecycle"><span /><span /><span /></div> : null}
      {data && meta ? <>
        <div className="flex flex-wrap items-center gap-3 border border-(--line-1) bg-(--bg-1) px-4 py-2"><FreshnessBadge meta={meta} /><SourceEvidence meta={meta} /><Link href={`/sources?${searchParams.toString()}`} className="evidence-link">Inspect sources <ArrowUpRight aria-hidden="true" size={12} /></Link><span className="text-[11px] text-(--tx-4)">Observation contract connected</span></div>
        {meta.capabilities.map((capability) => <CapabilityNotice key={`${capability.feature}-${capability.status}`} capability={capability} />)}
        <div id="lifecycle-panel" role="tabpanel" aria-labelledby={`lifecycle-tab-${view}`} className="grid min-w-0 gap-5">
          {view === "current" && items.length ? <section className="grid gap-3"><div><h2>Current node lifecycle</h2><p className="mt-1 text-xs text-(--tx-3)">Process start and ready phase are node evidence. An unreachable Console does not imply that the process stopped.</p></div><div className="dense-table-scroll"><table className="dense-table" aria-label="Current node lifecycle"><thead><tr><th scope="col">Node</th><th scope="col">Observation</th><th scope="col">Uptime</th><th scope="col">Process start</th><th scope="col">Boot ID</th><th scope="col">Ready phase</th><th scope="col">App / Rr</th><th scope="col">Config hash</th></tr></thead><tbody>{(items as unknown as CurrentLifecycleItem[]).map((item) => <tr key={item.node_id}><td><Link className="text-(--acc)" href={`/nodes?${new URLSearchParams({ window: searchParams.get("window") ?? "15m", node_id: String(item.node_id) }).toString()}`}>Node {item.node_id}</Link></td><td><HealthBadge status={item.status ?? "UNKNOWN"} /></td><td className="text-right font-mono">{formatDuration(item.uptime_secs)}</td><td>{formatTimestamp(item.process_start_time ?? item.process_started_at)}</td><td><code>{item.boot_id ?? "—"}</code></td><td><Badge tone={item.ready_phase === "READY" ? "ok" : "neutral"}>{item.ready_phase ?? "—"}</Badge></td><td>{item.app_version ?? "—"}<small className="block text-[10px] text-(--tx-4)">Rr {item.rr_version ?? "—"}</small></td><td><code>{item.config_hash ?? item.config_fingerprint ?? "—"}</code></td></tr>)}</tbody></table></div></section> : null}
          {view === "restarts" && items.length ? <BootTimeline items={items as unknown as TimelineBoot[]} partial={meta.partial} /> : null}
          {view === "versions" && items.length ? <><VersionMatrix items={items as unknown as VersionItem[]} /><section className="grid gap-3"><h2>Version history by boot</h2><div className="dense-table-scroll"><table className="dense-table" aria-label="Version history by boot"><thead><tr><th scope="col">Node</th><th scope="col">Boot ID</th><th scope="col">Started</th><th scope="col">Ended</th><th scope="col">App version</th><th scope="col">Rr version</th><th scope="col">Git commit</th></tr></thead><tbody>{(items as unknown as TimelineBoot[]).map((item) => <tr key={`${item.node_id}-${item.boot_id}`}><td>Node {item.node_id}</td><td><code>{item.boot_id}</code></td><td>{formatTimestamp(item.started_at)}</td><td>{formatTimestamp(item.ended_at)}</td><td>{item.app_version ?? "—"}</td><td>{item.rr_version ?? "—"}</td><td><code>{item.git_commit ?? "—"}</code></td></tr>)}</tbody></table></div><p className="text-xs text-(--tx-3)">Mixed versions describe the observed distribution. The Console does not declare a rolling upgrade complete.</p></section></> : null}
          {!items.length ? <EmptyState title={filter ? "No lifecycle records match the search" : "No lifecycle records returned"} description="Inspect the capability notices and sources to distinguish unsupported history, an empty journal, and unreachable nodes." /> : null}
        </div>
        <div className="flex items-center justify-between gap-3"><p className="text-xs text-(--tx-4)">Up to 50 records per page · node journals retain at most 100 boots</p><div className="flex gap-2">{cursor ? <Button variant="secondary" size="small" onClick={() => updateQuery({ cursor: null })}>First page</Button> : null}{data.next_cursor ? <Button variant="secondary" size="small" onClick={() => updateQuery({ cursor: data.next_cursor })}>Older restart records</Button> : null}</div></div>
      </> : null}
    </div>
  );
}
