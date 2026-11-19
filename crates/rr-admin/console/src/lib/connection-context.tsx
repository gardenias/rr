"use client";

import { useSearchParams } from "next/navigation";
import {
  createContext,
  useCallback,
  useContext,
  useEffect,
  useMemo,
  useRef,
  useState,
  type ReactNode,
} from "react";
import { ConsoleApiError } from "@/lib/api";
import { connectionsApi, type ConnectionSummary } from "@/lib/connections";

const RECENT_CONNECTION_KEY = "rr.console.recent-connection.v1";

interface ConnectionState {
  connections: ConnectionSummary[];
  keychainAvailable: boolean;
  loading: boolean;
  error: ConsoleApiError | null;
}

export interface ActiveConnectionContextValue extends ConnectionState {
  connectionId: string | null;
  activeConnection: ConnectionSummary | null;
  recentConnectionId: string | null;
  refresh: () => Promise<void>;
}

const ActiveConnectionContext = createContext<ActiveConnectionContextValue | null>(null);

export function ConnectionProvider({ children }: { children: ReactNode }) {
  const searchParams = useSearchParams();
  const requestedId = searchParams.get("connection")?.trim();
  const connectionId = requestedId || null;
  const requestGeneration = useRef(0);
  const [recentConnectionId, setRecentConnectionId] = useState<string | null>(null);
  const [state, setState] = useState<ConnectionState>({
    connections: [],
    keychainAvailable: false,
    loading: true,
    error: null,
  });

  const refresh = useCallback(async () => {
    const generation = ++requestGeneration.current;
    setState((current) => ({ ...current, loading: true, error: null }));
    try {
      const response = await connectionsApi.list();
      if (generation !== requestGeneration.current) return;
      setState({
        connections: response.data.items,
        keychainAvailable: response.data.keychain_available,
        loading: false,
        error: null,
      });
    } catch (error) {
      if (generation !== requestGeneration.current) return;
      setState((current) => ({
        ...current,
        loading: false,
        error: error instanceof ConsoleApiError
          ? error
          : new ConsoleApiError(
            0,
            "NETWORK_ERROR",
            error instanceof Error ? error.message : "Connection list request failed",
            null,
          ),
      }));
    }
  }, []);

  useEffect(() => {
    try {
      setRecentConnectionId(window.localStorage.getItem(RECENT_CONNECTION_KEY));
    } catch {
      setRecentConnectionId(null);
    }
    void refresh();
    return () => {
      requestGeneration.current += 1;
    };
  }, [refresh]);

  const activeConnection = useMemo(
    () => state.connections.find((connection) =>
      connection.id === connectionId && connection.status !== "DISCONNECTED"
    ) ?? null,
    [connectionId, state.connections],
  );

  useEffect(() => {
    if (!activeConnection) return;
    try {
      window.localStorage.setItem(RECENT_CONNECTION_KEY, activeConnection.id);
    } catch {
      // The in-memory recent hint remains available when browser storage is blocked.
    }
    setRecentConnectionId(activeConnection.id);
  }, [activeConnection]);

  const value = useMemo<ActiveConnectionContextValue>(() => ({
    ...state,
    connectionId,
    activeConnection,
    recentConnectionId,
    refresh,
  }), [activeConnection, connectionId, recentConnectionId, refresh, state]);

  return <ActiveConnectionContext.Provider value={value}>{children}</ActiveConnectionContext.Provider>;
}

export function useActiveConnection(): ActiveConnectionContextValue {
  const value = useContext(ActiveConnectionContext);
  if (!value) throw new Error("useActiveConnection must be used within ConnectionProvider");
  return value;
}
