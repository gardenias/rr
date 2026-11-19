"use client";

import { useCallback, useEffect, useState, type FormEvent } from "react";
import Link from "next/link";
import { ArrowUpRight, CheckCircle2 } from "lucide-react";
import { CapabilityNotice } from "@/components/console/capability-notice";
import { FreshnessBadge } from "@/components/console/freshness-badge";
import { SourceEvidence } from "@/components/console/source-evidence";
import { EmptyState, ErrorState } from "@/components/console/states";
import { Button } from "@/components/ui/button";
import { Drawer } from "@/components/ui/drawer";
import { ConsoleApiError, consoleApi, isTerminalConnectionError } from "@/lib/api";
import type { ConsoleMeta } from "@/lib/contract";
import { useConsoleResource } from "@/lib/use-console-resource";
import { useRefresh } from "@/lib/refresh-context";
import { refreshDelay } from "@/lib/refresh-policy";
import { useRouteQueryState } from "@/lib/use-route-query-state";
import { GroupEvidenceSummary } from "@/components/console/groups/group-detail";
import { GroupTable } from "@/components/console/groups/group-table";
import { normalizeReplicaRows, type GroupDetailItem, type GroupListItem, type ReplicaRow } from "@/components/console/groups/model";
import { ReplicaMatrix } from "@/components/console/groups/replica-matrix";
import { hrefWithConnection, serializeQueryState, type TimeWindow } from "@/lib/query-state";

interface DetailState {
  group: GroupDetailItem | null;
  replicas: ReplicaRow[];
  meta: ConsoleMeta | null;
  error: ConsoleApiError | null;
  replicaError: ConsoleApiError | null;
  loading: boolean;
}

function apiError(error: unknown): ConsoleApiError {
  return error instanceof ConsoleApiError
    ? error
    : new ConsoleApiError(0, "NETWORK_ERROR", error instanceof Error ? error.message : "Console request failed", null);
}

function GroupDetailLoader({ groupId, timeWindow }: { groupId: number; timeWindow: TimeWindow }) {
  const { connectionId, intervalMs, paused, recordRefresh } = useRefresh();
  const [reload, setReload] = useState(0);
  const [state, setState] = useState<DetailState>({
    group: null,
    replicas: [],
    meta: null,
    error: null,
    replicaError: null,
    loading: true,
  });

  useEffect(() => {
    if (!connectionId || paused) return;
    const owner = connectionId;
    const controller = new AbortController();
    let timer: ReturnType<typeof setInterval> | undefined;
    let stopped = false;
    let latestRequest = 0;
    const load = async (initial: boolean) => {
      if (stopped) return;
      const request = ++latestRequest;
      if (initial) setState((current) => ({ ...current, loading: true, error: null, replicaError: null }));
      const [groupResult, replicaResult] = await Promise.allSettled([
        consoleApi.group(owner, groupId, controller.signal),
        consoleApi.replicas(owner, groupId, controller.signal),
      ]);
      if (controller.signal.aborted || stopped || request !== latestRequest) return;
      const group = groupResult.status === "fulfilled" ? groupResult.value.data as GroupDetailItem : null;
      const meta = groupResult.status === "fulfilled"
        ? groupResult.value.meta
        : replicaResult.status === "fulfilled" ? replicaResult.value.meta : null;
      if (meta) recordRefresh(owner, meta.generated_at);
      const groupError = groupResult.status === "rejected" ? apiError(groupResult.reason) : null;
      const replicaError = replicaResult.status === "rejected" ? apiError(replicaResult.reason) : null;
      if (isTerminalConnectionError(groupError) || isTerminalConnectionError(replicaError)) {
        stopped = true;
        if (timer) clearInterval(timer);
      }
      setState({
        group,
        replicas: replicaResult.status === "fulfilled" ? normalizeReplicaRows(replicaResult.value.data) : [],
        meta,
        error: groupError,
        replicaError,
        loading: false,
      });
    };
    const schedule = () => {
      if (timer) clearInterval(timer);
      if (stopped) return;
      const delay = refreshDelay(intervalMs, document.hidden, false);
      if (delay) timer = setInterval(() => void load(false), delay);
    };
    void load(true);
    schedule();
    const onVisibilityChange = () => {
      if (stopped) return;
      if (!document.hidden) void load(false);
      schedule();
    };
    document.addEventListener("visibilitychange", onVisibilityChange);
    return () => {
      latestRequest += 1;
      controller.abort();
      if (timer) clearInterval(timer);
      document.removeEventListener("visibilitychange", onVisibilityChange);
    };
  }, [connectionId, groupId, intervalMs, paused, recordRefresh, reload]);

  const relatedHref = (path: string, updates: Record<string, string>) => {
    const params = new URLSearchParams({ window: timeWindow, ...updates });
    if (connectionId) params.set("connection", connectionId);
    return hrefWithConnection(path, params);
  };

  if (state.loading && !state.group) {
    return <div className="route-skeleton" role="status" aria-label={`Loading group ${groupId} evidence`}><span /><span /><span /></div>;
  }
  return (
    <div className="grid gap-5">
      {state.error ? (
        <ErrorState
          title={state.group ? "Showing the last successful group observation" : "Group evidence unavailable"}
          message={state.error.message}
          correlationId={state.error.correlationId}
          recovery="Retry the on-demand collection or inspect Sources for the affected node."
        />
      ) : null}
      {state.group ? <GroupEvidenceSummary group={state.group} /> : null}
      {state.meta ? (
        <div className="flex flex-wrap items-center gap-3 border-y border-white/10 py-2">
          <FreshnessBadge meta={state.meta} />
          <SourceEvidence meta={state.meta} />
          <Button variant="ghost" size="small" onClick={() => setReload((value) => value + 1)}>Refresh group</Button>
        </div>
      ) : null}
      {state.meta?.capabilities.map((capability) => (
        <CapabilityNotice key={`${capability.feature}-${capability.status}`} capability={capability} />
      ))}
      {state.replicaError ? (
        <ErrorState
          title="Replica evidence unavailable"
          message={state.replicaError.message}
          correlationId={state.replicaError.correlationId}
          recovery="The group summary is still usable. Inspect Sources before drawing replication conclusions."
        />
      ) : null}
      {state.replicas.length ? (
        <section className="grid gap-2" aria-label="Replica evidence">
          <div>
            <h3>Replica evidence matrix</h3>
            <p className="mt-1 text-xs text-zinc-400">Matched index and lag are shown only with their leader source predicate. Heartbeat and append RTT remain separate.</p>
          </div>
          <ReplicaMatrix replicas={state.replicas} leaderNodeId={state.group?.leader_node_id ?? null} timeWindow={timeWindow} connectionId={connectionId ?? undefined} />
        </section>
      ) : !state.replicaError && !state.loading ? (
        <EmptyState title="No replica observations" description="The group may be unsupported or no detailed source has responded. Missing replicas are not proof of membership loss." />
      ) : null}
      <nav className="flex flex-wrap gap-3 border-t border-white/10 pt-4 text-xs text-orange-300" aria-label="Related group views">
        <Link href={relatedHref("/topology/", { view: "routes", filter: `group-${groupId}` })}>Related partitions</Link>
        <Link href={relatedHref("/nodes/", { filter: `group-${groupId}` })}>Related nodes</Link>
        <Link href={relatedHref("/snapshots/", { group_id: String(groupId) })}>Group snapshots</Link>
        <Link href={relatedHref("/performance/", { group_id: String(groupId) })}>Group performance</Link>
      </nav>
    </div>
  );
}

export function GroupsView() {
  const { state: queryState, setFilter } = useRouteQueryState();
  const [pagination, setPagination] = useState<{
    connectionId: string | null;
    cursor: string | null;
    history: Array<string | null>;
  }>({ connectionId: queryState.connection, cursor: null, history: [] });
  const currentPagination = pagination.connectionId === queryState.connection
    ? pagination
    : { connectionId: queryState.connection, cursor: null, history: [] };
  const cursor = currentPagination.cursor;
  const cursorHistory = currentPagination.history;
  const { data, meta, error, loading, refresh } = useConsoleResource("/groups", {
    cursor,
    page_size: 50,
    filter: queryState.filters.filter,
  });
  const selectedGroupId = Number(queryState.filters.group_id);
  const validSelectedGroupId = Number.isSafeInteger(selectedGroupId) && selectedGroupId > 0 ? selectedGroupId : null;
  const onSelect = useCallback((groupId: number) => setFilter("group_id", String(groupId)), [setFilter]);
  const groups = (data?.items ?? []) as GroupListItem[];

  const applyFilter = (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    const form = new FormData(event.currentTarget);
    setPagination({ connectionId: queryState.connection, cursor: null, history: [] });
    setFilter("filter", String(form.get("filter") ?? ""));
  };
  const nextPage = () => {
    if (!data?.next_cursor) return;
    setPagination({
      connectionId: queryState.connection,
      cursor: data.next_cursor,
      history: [...cursorHistory, cursor],
    });
  };
  const previousPage = () => {
    setPagination({
      connectionId: queryState.connection,
      cursor: cursorHistory.at(-1) ?? null,
      history: cursorHistory.slice(0, -1),
    });
  };

  return (
    <div className="route-page">
      <div className="page-heading">
        <div>
          <div className="page-eyebrow">Cluster / Read-only</div>
          <h1>Raft groups</h1>
          <p>Leadership, membership, replication, and per-replica observations with their source predicates intact.</p>
        </div>
        <Button variant="secondary" size="small" onClick={refresh} disabled={loading}>Refresh observations</Button>
      </div>

      <form className="flex max-w-xl gap-2" onSubmit={applyFilter} role="search" aria-label="Filter Raft groups">
        <label className="sr-only" htmlFor="group-filter">Filter by group ID</label>
        <input id="group-filter" name="filter" className="input" defaultValue={queryState.filters.filter ?? ""} placeholder="Filter by group ID" />
        <Button type="submit" variant="secondary">Apply filter</Button>
      </form>

      {error ? (
        <ErrorState
          title={data ? "Showing the last successful observations" : "Console data unavailable"}
          message={error.message}
          correlationId={error.correlationId}
          recovery="Open Sources to inspect endpoint health and collection capabilities."
        />
      ) : null}
      {loading && !data ? <div className="route-skeleton" role="status" aria-label="Loading Raft groups"><span /><span /><span /></div> : null}

      {data && meta ? (
        <>
          <section className="evidence-panel" aria-label="Raft groups observation status">
            <div className="evidence-panel-topline">
              <div className="evidence-panel-status">
                <CheckCircle2 aria-hidden="true" size={17} />
                <div>
                  <h2>Observation contract connected</h2>
                  <p>Expected membership, runtime observations, and Console reachability remain separate. Missing evidence is never zero.</p>
                </div>
              </div>
            </div>
            <div className="evidence-panel-meta">
              <FreshnessBadge meta={meta} />
              <SourceEvidence meta={meta} />
              <Link href={hrefWithConnection("/sources", serializeQueryState(queryState))} className="evidence-link">Inspect sources <ArrowUpRight aria-hidden="true" size={12} /></Link>
            </div>
          </section>
          {meta.capabilities.map((capability) => (
            <CapabilityNotice key={`${capability.feature}-${capability.status}`} capability={capability} />
          ))}
          {groups.length ? <GroupTable groups={groups} onSelect={onSelect} /> : (
            <EmptyState title="No Raft groups returned" description="The request succeeded, but no groups match this view. Missing observations are not evidence of health." />
          )}
          <div className="flex items-center justify-between gap-3 text-xs text-zinc-400">
            <span>Up to 50 groups per page · {data.next_cursor ? "More observations available" : "End of this result set"}</span>
            <div className="flex gap-2">
              <Button variant="secondary" size="small" onClick={previousPage} disabled={!cursorHistory.length}>Previous page</Button>
              <Button variant="secondary" size="small" onClick={nextPage} disabled={!data.next_cursor}>Next page</Button>
            </div>
          </div>
        </>
      ) : null}

      {validSelectedGroupId ? (
        <Drawer
          trigger={<span className="sr-only" aria-hidden="true" />}
          title={`Group ${validSelectedGroupId} evidence`}
          description="On-demand membership, leadership, and replica observations. Polls are not atomic."
          open
          onOpenChange={(open) => {
            if (!open) setFilter("group_id", "");
          }}
        >
          <GroupDetailLoader key={`${queryState.connection ?? "none"}:${validSelectedGroupId}`} groupId={validSelectedGroupId} timeWindow={queryState.window} />
        </Drawer>
      ) : null}
    </div>
  );
}
