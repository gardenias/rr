"use client";

import { useEffect, useState } from "react";
import { ConsoleApiError, getConsole } from "@/lib/api";
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
  data: T | null;
  meta: ConsoleMeta | null;
  error: ConsoleApiError | null;
  loading: boolean;
}

export function useConsoleResource<P extends ConsoleResourcePath>(
  path: P,
  query: ConsoleQuery = {},
) {
  const { intervalMs, paused, recordRefresh } = useRefresh();
  const [reload, setReload] = useState(0);
  const [state, setState] = useState<ResourceState<ConsoleResponseMap[P]>>({
    data: null,
    meta: null,
    error: null,
    loading: true,
  });
  const queryKey = JSON.stringify(query);

  useEffect(() => {
    if (paused) return;
    const controller = new AbortController();
    let timer: ReturnType<typeof setInterval> | undefined;
    const load = async (initial: boolean) => {
      if (initial) setState((current) => ({ ...current, loading: true, error: null }));
      try {
        const result = await getConsole(path, JSON.parse(queryKey) as ConsoleQuery, controller.signal);
        if (controller.signal.aborted) return;
        setState({ data: result.data, meta: result.meta, error: null, loading: false });
        recordRefresh(result.meta.generated_at);
      } catch (error) {
        if (controller.signal.aborted) return;
        const apiError = error instanceof ConsoleApiError
          ? error
          : new ConsoleApiError(0, "NETWORK_ERROR", error instanceof Error ? error.message : "Console request failed", null);
        setState((current) => ({ ...current, error: apiError, loading: false }));
        if (apiError.status === 401) {
          const currentQuery = serializeQueryState(parseQueryState(window.location.search));
          window.location.assign(loginRedirectFor(`${window.location.pathname}${currentQuery ? `?${currentQuery}` : ""}`));
        }
      }
    };
    const schedule = () => {
      if (timer) clearInterval(timer);
      const delay = refreshDelay(intervalMs, document.hidden, false);
      if (delay) timer = setInterval(() => void load(false), delay);
    };
    void load(true);
    schedule();
    const onVisibilityChange = () => {
      if (!document.hidden) void load(false);
      schedule();
    };
    document.addEventListener("visibilitychange", onVisibilityChange);
    return () => {
      controller.abort();
      if (timer) clearInterval(timer);
      document.removeEventListener("visibilitychange", onVisibilityChange);
    };
  }, [intervalMs, paused, path, queryKey, recordRefresh, reload]);

  return { ...state, refresh: () => setReload((value) => value + 1) };
}
