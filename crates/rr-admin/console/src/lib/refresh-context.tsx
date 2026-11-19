"use client";

import {
  createContext,
  useCallback,
  useContext,
  useState,
  type Dispatch,
  type ReactNode,
  type SetStateAction,
} from "react";
import type { ConsoleEnvelope, SummaryData } from "@/lib/contract";

export type StreamState = "CONNECTING" | "LIVE" | "RECONNECTING" | "PAUSED" | "STOPPED";

interface RefreshContextValue {
  connectionId: string | null;
  intervalMs: number;
  setIntervalMs: Dispatch<SetStateAction<number>>;
  paused: boolean;
  setPaused: Dispatch<SetStateAction<boolean>>;
  lastUpdated: string | null;
  recordRefresh: (connectionId: string, value: string | null) => void;
  summary: ConsoleEnvelope<SummaryData> | null;
  recordSummary: (connectionId: string, envelope: ConsoleEnvelope<SummaryData>) => void;
  streamState: StreamState;
  setStreamState: (connectionId: string, state: StreamState) => void;
}

const RefreshContext = createContext<RefreshContextValue | null>(null);

export function RefreshProvider({ children, connectionId }: { children: ReactNode; connectionId: string | null }) {
  const [intervalMs, setIntervalMs] = useState(5_000);
  const [paused, setPaused] = useState(false);
  const [updatedState, setUpdatedState] = useState<{ connectionId: string; value: string | null } | null>(null);
  const [summaryState, setSummaryState] = useState<{
    connectionId: string;
    envelope: ConsoleEnvelope<SummaryData>;
  } | null>(null);
  const [streamState, updateStreamState] = useState<{ connectionId: string; state: StreamState } | null>(null);

  const recordRefresh = useCallback((owner: string, value: string | null) => {
    setUpdatedState({ connectionId: owner, value });
  }, []);
  const recordSummary = useCallback((owner: string, envelope: ConsoleEnvelope<SummaryData>) => {
    setSummaryState({ connectionId: owner, envelope });
  }, []);
  const setStreamState = useCallback((owner: string, state: StreamState) => {
    updateStreamState({ connectionId: owner, state });
  }, []);

  const lastUpdated = connectionId && updatedState?.connectionId === connectionId ? updatedState.value : null;
  const summary = connectionId && summaryState?.connectionId === connectionId ? summaryState.envelope : null;
  const currentStreamState = connectionId
    ? streamState?.connectionId === connectionId ? streamState.state : "CONNECTING"
    : "STOPPED";

  return (
    <RefreshContext.Provider value={{
      connectionId,
      intervalMs,
      setIntervalMs,
      paused,
      setPaused,
      lastUpdated,
      recordRefresh,
      summary,
      recordSummary,
      streamState: currentStreamState,
      setStreamState,
    }}>
      {children}
    </RefreshContext.Provider>
  );
}

export function useRefresh(): RefreshContextValue {
  const context = useContext(RefreshContext);
  if (!context) throw new Error("useRefresh must be used inside RefreshProvider");
  return context;
}
