"use client";

import { createContext, useContext, useState, type Dispatch, type ReactNode, type SetStateAction } from "react";
import type { ConsoleEnvelope, SummaryData } from "@/lib/contract";

export type StreamState = "CONNECTING" | "LIVE" | "RECONNECTING" | "PAUSED";

interface RefreshContextValue {
  intervalMs: number;
  setIntervalMs: Dispatch<SetStateAction<number>>;
  paused: boolean;
  setPaused: Dispatch<SetStateAction<boolean>>;
  lastUpdated: string | null;
  recordRefresh: Dispatch<SetStateAction<string | null>>;
  summary: ConsoleEnvelope<SummaryData> | null;
  recordSummary: Dispatch<SetStateAction<ConsoleEnvelope<SummaryData> | null>>;
  streamState: StreamState;
  setStreamState: Dispatch<SetStateAction<StreamState>>;
}

const RefreshContext = createContext<RefreshContextValue | null>(null);

export function RefreshProvider({ children }: { children: ReactNode }) {
  const [intervalMs, setIntervalMs] = useState(5_000);
  const [paused, setPaused] = useState(false);
  const [lastUpdated, recordRefresh] = useState<string | null>(null);
  const [summary, recordSummary] = useState<ConsoleEnvelope<SummaryData> | null>(null);
  const [streamState, setStreamState] = useState<StreamState>("CONNECTING");
  return (
    <RefreshContext.Provider value={{ intervalMs, setIntervalMs, paused, setPaused, lastUpdated, recordRefresh, summary, recordSummary, streamState, setStreamState }}>
      {children}
    </RefreshContext.Provider>
  );
}

export function useRefresh(): RefreshContextValue {
  const context = useContext(RefreshContext);
  if (!context) throw new Error("useRefresh must be used inside RefreshProvider");
  return context;
}
