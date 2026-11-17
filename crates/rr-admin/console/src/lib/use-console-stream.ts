"use client";

import { useEffect, useRef, useState } from "react";
import { consoleStreamUrl, getConsole } from "@/lib/api";
import { useRefresh } from "@/lib/refresh-context";
import { nextReconnectDelay } from "@/lib/refresh-policy";
import type { ConsoleEnvelope, SummaryData } from "@/lib/contract";

export function useConsoleStream() {
  const { paused, recordRefresh, recordSummary, setStreamState } = useRefresh();
  const [hidden, setHidden] = useState(false);
  const attempt = useRef(0);

  useEffect(() => {
    const onVisibility = () => setHidden(document.hidden);
    onVisibility();
    document.addEventListener("visibilitychange", onVisibility);
    return () => document.removeEventListener("visibilitychange", onVisibility);
  }, []);

  useEffect(() => {
    if (paused || hidden) {
      setStreamState("PAUSED");
      return;
    }
    let source: EventSource | null = null;
    let reconnectTimer: ReturnType<typeof setTimeout> | null = null;
    let disposed = false;
    const connect = () => {
      if (disposed) return;
      setStreamState(attempt.current ? "RECONNECTING" : "CONNECTING");
      const eventSource = new EventSource(consoleStreamUrl(), { withCredentials: false });
      source = eventSource;
      let reconciling = false;
      const reconnect = () => {
        if (disposed || reconnectTimer) return;
        eventSource.close();
        setStreamState("RECONNECTING");
        reconnectTimer = setTimeout(() => {
          reconnectTimer = null;
          attempt.current += 1;
          connect();
        }, nextReconnectDelay(attempt.current));
      };
      eventSource.onopen = () => {
        attempt.current = 0;
        setStreamState("LIVE");
        recordRefresh(new Date().toISOString());
      };
      eventSource.onmessage = () => recordRefresh(new Date().toISOString());
      eventSource.addEventListener("summary", (event: Event) => {
        try {
          const envelope = JSON.parse((event as MessageEvent<string>).data) as ConsoleEnvelope<SummaryData>;
          recordSummary(envelope);
          recordRefresh(envelope.meta.generated_at);
        } catch {
          return;
        }
      });
      eventSource.addEventListener("resync", () => {
        if (disposed || reconciling) return;
        reconciling = true;
        setStreamState("RECONNECTING");
        void getConsole("/summary", { window: "15m" })
          .then((envelope) => {
            if (disposed) return;
            recordSummary(envelope);
            recordRefresh(envelope.meta.generated_at);
            if (!reconnectTimer && source === eventSource) setStreamState("LIVE");
          })
          .catch(reconnect)
          .finally(() => {
            reconciling = false;
          });
      });
      eventSource.onerror = reconnect;
    };
    connect();
    return () => {
      disposed = true;
      source?.close();
      if (reconnectTimer) clearTimeout(reconnectTimer);
    };
  }, [hidden, paused, recordRefresh, recordSummary, setStreamState]);
}
