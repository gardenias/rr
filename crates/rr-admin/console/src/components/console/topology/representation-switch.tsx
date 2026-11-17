"use client";

import { Component, useId, useState, useSyncExternalStore, type KeyboardEvent, type ReactNode } from "react";

function subscribeToSmallScreen(onChange: () => void) {
  if (typeof window === "undefined" || typeof window.matchMedia !== "function") return () => {};
  const media = window.matchMedia("(max-width: 767px)");
  media.addEventListener("change", onChange);
  return () => media.removeEventListener("change", onChange);
}

function smallScreenSnapshot() {
  return typeof window !== "undefined" && typeof window.matchMedia === "function" && window.matchMedia("(max-width: 767px)").matches;
}

class GraphErrorBoundary extends Component<{ onFailure: () => void; children: ReactNode }, { failed: boolean }> {
  state = { failed: false };

  static getDerivedStateFromError() {
    return { failed: true };
  }

  componentDidCatch() {
    this.props.onFailure();
  }

  render() {
    return this.state.failed ? <p role="alert">Graph rendering failed. The equivalent data table is now shown.</p> : this.props.children;
  }
}

export function RepresentationSwitch({
  label,
  graph,
  table,
  largeDataReason,
}: {
  label: string;
  graph: ReactNode;
  table: ReactNode;
  largeDataReason?: string | null;
}) {
  const [choice, setChoice] = useState<"graph" | "table" | null>(null);
  const smallScreen = useSyncExternalStore(subscribeToSmallScreen, smallScreenSnapshot, () => false);
  const view = choice ?? (largeDataReason || smallScreen ? "table" : "graph");
  const baseId = useId();
  const graphId = `${baseId}-graph`;
  const tableId = `${baseId}-table`;
  const choose = (next: "graph" | "table") => {
    if (next === "graph" && largeDataReason) return;
    setChoice(next);
    document.getElementById(next === "graph" ? graphId : tableId)?.focus();
  };
  const onKeyDown = (event: KeyboardEvent<HTMLDivElement>) => {
    if (!["ArrowLeft", "ArrowRight", "Home", "End"].includes(event.key)) return;
    event.preventDefault();
    if (event.key === "Home") choose("graph");
    else if (event.key === "End") choose("table");
    else choose(view === "graph" ? "table" : "graph");
  };
  return (
    <section className="metric-chart" aria-label={label}>
      <div className="metric-chart-header">
        <div role="tablist" aria-label={`${label} representation`} onKeyDown={onKeyDown}>
          <button
            id={graphId}
            type="button"
            role="tab"
            aria-selected={view === "graph"}
            aria-controls={`${baseId}-graph-panel`}
            tabIndex={view === "graph" ? 0 : -1}
            disabled={Boolean(largeDataReason)}
            onClick={() => choose("graph")}
          >Graph</button>
          <button
            id={tableId}
            type="button"
            role="tab"
            aria-selected={view === "table"}
            aria-controls={`${baseId}-table-panel`}
            tabIndex={view === "table" ? 0 : -1}
            onClick={() => choose("table")}
          >Data table</button>
        </div>
      </div>
      {largeDataReason ? <p className="mt-3 text-xs text-orange-300" role="status">Large topology: {largeDataReason}. Showing the equivalent bounded table.</p> : null}
      <div id={`${baseId}-${view}-panel`} role="tabpanel" aria-labelledby={view === "graph" ? graphId : tableId} className="mt-4">
        {view === "graph" ? <GraphErrorBoundary onFailure={() => setChoice("table")}>{graph}</GraphErrorBoundary> : table}
      </div>
    </section>
  );
}
