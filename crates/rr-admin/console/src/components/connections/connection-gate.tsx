"use client";

import { useEffect, type ReactNode } from "react";
import { useActiveConnection } from "@/lib/connection-context";

export function ConnectionGate({ children }: { children: ReactNode }) {
  const { activeConnection, loading } = useActiveConnection();

  useEffect(() => {
    if (!loading && !activeConnection) window.location.replace("/connections/");
  }, [activeConnection, loading]);

  if (!activeConnection) {
    return <main className="login-page"><p role="status">{loading ? "Validating cluster connection…" : "Opening connection manager…"}</p></main>;
  }
  return children;
}
