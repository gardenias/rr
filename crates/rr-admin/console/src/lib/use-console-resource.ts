"use client";

import { useEffect, useState } from "react";
import { ConsoleApiError, getConsole, isTerminalConnectionError } from "@/lib/api";
import { loginRedirectFor } from "@/lib/auth";
import type {
  ConsoleMeta,
  ConsoleQuery,
  ConsoleResourcePath,
  ConsoleResponseMap,
} from "@/lib/contract";
import { parseQueryState, serializeQueryState } from "@/lib/query-state";
import { useRefresh } from "@/lib/refresh-context";
import { refreshDelay } from "@/lib/refresh-policy";

interface ResourceState<T> {
  connectionId: string | null;
  data: T | null;
  meta: ConsoleMeta | null;
  error: ConsoleApiError | null;
  loading: boolean;
}

const emptyState: ResourceState<never> = {
  connectionId: null,
  data: null,
  meta: null,
  error: null,
  loading: false,
};

export function useConsoleResource<P extends ConsoleResourcePath>(
  path: P,
  query: ConsoleQuery = {},
) {
  const { connectionId, intervalMs, paused, recordRefresh } = useRefresh();
  const [reload, setReload] = useState(0);
  const [state, setState] = useState<ResourceState<ConsoleResponseMap[P]>>(emptyState);
  const queryKey = JSON.stringify(query);

  useEffect(() => {
    if (!connectionId || paused) return;
    const owner = connectionId;
    const controller = new AbortController();
    let timer: ReturnType<typeof setInterval> | undefined;
    let disposed = false;
    let stopped = false;
    let latestRequest = 0;

    const load = async (initial: boolean) => {
      if (disposed || stopped) return;
      const request = ++latestRequest;
      if (initial) {
        setState((current) => ({
          ...(current.connectionId === owner ? current : emptyState),
          connectionId: owner,
          loading: true,
          error: null,
        }));
      }
      try {
        const result = await getConsole(owner, path, JSON.parse(queryKey) as ConsoleQuery, controller.signal);
        if (disposed || stopped || request !== latestRequest || controller.signal.aborted) return;
        setState({ connectionId: owner, data: result.data, meta: result.meta, error: null, loading: false });
        recordRefresh(owner, result.meta.generated_at);
      } catch (error) {
        if (disposed || request !== latestRequest || controller.signal.aborted) return;
        const apiError = error instanceof ConsoleApiError
          ? error
          : new ConsoleApiError(0, "NETWORK_ERROR", error instanceof Error ? error.message : "Console request failed", null);
        setState((current) => ({
          ...(current.connectionId === owner ? current : emptyState),
          connectionId: owner,
          error: apiError,
          loading: false,
        }));
        if (isTerminalConnectionError(apiError)) {
          stopped = true;
          if (timer) clearInterval(timer);
        }
        if (apiError.status === 401) {
          const currentQuery = serializeQueryState(parseQueryState(window.location.search));
          window.location.assign(loginRedirectFor(`${window.location.pathname}${currentQuery ? `?${currentQuery}` : ""}`));
        }
      }
    };
    const schedule = () => {
      if (timer) clearInterval(timer);
      if (stopped || disposed) return;
      const delay = refreshDelay(intervalMs, document.hidden, false);
      if (delay) timer = setInterval(() => void load(false), delay);
    };
    void load(true);
    schedule();
    const onVisibilityChange = () => {
      if (stopped || disposed) return;
      if (!document.hidden) void load(false);
      schedule();
    };
    document.addEventListener("visibilitychange", onVisibilityChange);
    return () => {
      disposed = true;
      latestRequest += 1;
      controller.abort();
      if (timer) clearInterval(timer);
      document.removeEventListener("visibilitychange", onVisibilityChange);
    };
  }, [connectionId, intervalMs, paused, path, queryKey, recordRefresh, reload]);

  const current = state.connectionId === connectionId
    ? state
    : {
      ...emptyState,
      loading: connectionId !== null,
    } as ResourceState<ConsoleResponseMap[P]>;
  return {
    data: current.data,
    meta: current.meta,
    error: current.error,
    loading: current.loading,
    refresh: () => setReload((value) => value + 1),
  };
}
