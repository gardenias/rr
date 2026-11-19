"use client";

import { useEffect, useRef, useState } from "react";
import { consoleStreamUrl, getConsole, isTerminalConnectionError } from "@/lib/api";
import { connectionsApi } from "@/lib/connections";
import { useRefresh } from "@/lib/refresh-context";
import { nextReconnectDelay } from "@/lib/refresh-policy";
import type { ConsoleEnvelope, SummaryData } from "@/lib/contract";

const CONNECTION_CLASSIFICATION_TIMEOUT_MS = 2_000;

export function useConsoleStream() {
  const { connectionId, paused, recordRefresh, recordSummary, setStreamState } = useRefresh();
  const [hidden, setHidden] = useState(false);
  const attempt = useRef(0);

  useEffect(() => {
    const onVisibility = () => setHidden(document.hidden);
    onVisibility();
    document.addEventListener("visibilitychange", onVisibility);
    return () => document.removeEventListener("visibilitychange", onVisibility);
  }, []);

  useEffect(() => {
    if (!connectionId) return;
    const owner = connectionId;
    if (paused || hidden) {
      setStreamState(owner, "PAUSED");
      return;
    }
    const controller = new AbortController();
    let source: EventSource | null = null;
    let reconnectTimer: ReturnType<typeof setTimeout> | null = null;
    let disposed = false;
    let stopped = false;
    let classifying = false;
    let classificationTimer: ReturnType<typeof setTimeout> | null = null;
    let latestResync = 0;
    attempt.current = 0;

    const stop = () => {
      if (disposed) return;
      stopped = true;
      latestResync += 1;
      source?.close();
      if (reconnectTimer) clearTimeout(reconnectTimer);
      reconnectTimer = null;
      setStreamState(owner, "STOPPED");
    };
    const armReconnect = () => {
      if (disposed || stopped || reconnectTimer) return;
      reconnectTimer = setTimeout(() => {
        reconnectTimer = null;
        attempt.current += 1;
        connect();
      }, nextReconnectDelay(attempt.current));
    };
    const recover = (eventSource: EventSource) => {
      if (disposed || stopped || source !== eventSource) return;
      latestResync += 1;
      eventSource.close();
      setStreamState(owner, "RECONNECTING");
      if (reconnectTimer || classifying) return;
      classifying = true;
      const classificationController = new AbortController();
      const abortClassification = () => classificationController.abort();
      let timedOut = false;
      controller.signal.addEventListener("abort", abortClassification, { once: true });
      classificationTimer = setTimeout(() => {
        classificationTimer = null;
        if (disposed || stopped || source !== eventSource) return;
        timedOut = true;
        classifying = false;
        classificationController.abort();
        armReconnect();
      }, CONNECTION_CLASSIFICATION_TIMEOUT_MS);
      void connectionsApi.get(owner, classificationController.signal)
        .then((envelope) => {
          if (timedOut || disposed || stopped || source !== eventSource) return;
          if (envelope.data.status === "DISCONNECTED") stop();
          else armReconnect();
        })
        .catch((error: unknown) => {
          if (timedOut || disposed || stopped || controller.signal.aborted) return;
          if (isTerminalConnectionError(error)) stop();
          else armReconnect();
        })
        .finally(() => {
          controller.signal.removeEventListener("abort", abortClassification);
          if (timedOut) return;
          classifying = false;
          if (classificationTimer) clearTimeout(classificationTimer);
          classificationTimer = null;
        });
    };
    const connect = () => {
      if (disposed || stopped) return;
      setStreamState(owner, attempt.current ? "RECONNECTING" : "CONNECTING");
      const eventSource = new EventSource(consoleStreamUrl(owner), { withCredentials: false });
      source = eventSource;
      let reconciling = false;
      eventSource.onopen = () => {
        if (disposed || stopped || source !== eventSource) return;
        attempt.current = 0;
        setStreamState(owner, "LIVE");
        recordRefresh(owner, new Date().toISOString());
      };
      eventSource.onmessage = () => {
        if (!disposed && !stopped && source === eventSource) recordRefresh(owner, new Date().toISOString());
      };
      eventSource.addEventListener("summary", (event: Event) => {
        if (disposed || stopped || source !== eventSource) return;
        try {
          const envelope = JSON.parse((event as MessageEvent<string>).data) as ConsoleEnvelope<SummaryData>;
          recordSummary(owner, envelope);
          recordRefresh(owner, envelope.meta.generated_at);
        } catch {
          return;
        }
      });
      eventSource.addEventListener("resync", () => {
        if (disposed || stopped || reconciling || source !== eventSource) return;
        reconciling = true;
        const resync = ++latestResync;
        setStreamState(owner, "RECONNECTING");
        void getConsole(owner, "/summary", { window: "15m" }, controller.signal)
          .then((envelope) => {
            if (disposed || stopped || resync !== latestResync || source !== eventSource) return;
            recordSummary(owner, envelope);
            recordRefresh(owner, envelope.meta.generated_at);
            if (!reconnectTimer) setStreamState(owner, "LIVE");
          })
          .catch((error: unknown) => {
            if (disposed || stopped || resync !== latestResync || controller.signal.aborted) return;
            if (isTerminalConnectionError(error)) stop();
            else recover(eventSource);
          })
          .finally(() => {
            reconciling = false;
          });
      });
      eventSource.onerror = () => recover(eventSource);
    };
    connect();
    return () => {
      disposed = true;
      latestResync += 1;
      controller.abort();
      source?.close();
      if (reconnectTimer) clearTimeout(reconnectTimer);
      if (classificationTimer) clearTimeout(classificationTimer);
    };
  }, [connectionId, hidden, paused, recordRefresh, recordSummary, setStreamState]);
}
