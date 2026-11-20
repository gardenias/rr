"use client";

import { ChevronDown, Database } from "lucide-react";
import { useCallback, useEffect, useId, useRef, useState } from "react";
import type { ConsoleMeta } from "@/lib/contract";
import { formatAge, formatTimestamp, sanitizeError } from "@/lib/format";

const endpointLabels = {
  ADMIN: "Admin",
  OBSERVABILITY: "Observability",
  METRICS: "Metrics",
  HEALTH: "Health",
  READY: "Ready",
  PROMETHEUS: "Prometheus",
  PROMETHEUS_HISTORY: "Prometheus history",
  DISCOVERY: "Discovery",
};

export function SourceEvidence({ meta }: { meta: ConsoleMeta }) {
  const [expanded, setExpanded] = useState(false);
  const [panelLeft, setPanelLeft] = useState(0);
  const rootRef = useRef<HTMLDivElement>(null);
  const panelId = useId();
  const count = meta.sources.length;
  const updatePanelPosition = useCallback(() => {
    if (!rootRef.current) return;
    const rootLeft = rootRef.current.getBoundingClientRect().left;
    const panelWidth = Math.min(460, window.innerWidth - 32);
    const maxLeft = Math.max(16, window.innerWidth - panelWidth - 16);
    setPanelLeft(Math.max(16, Math.min(rootLeft, maxLeft)) - rootLeft);
  }, []);
  useEffect(() => {
    if (!expanded) return;
    window.addEventListener("resize", updatePanelPosition);
    return () => window.removeEventListener("resize", updatePanelPosition);
  }, [expanded, updatePanelPosition]);
  const toggle = () => {
    if (!expanded) updatePanelPosition();
    setExpanded((value) => !value);
  };
  return (
    <div className="source-evidence" ref={rootRef}>
      <button
        type="button"
        className="source-evidence-trigger"
        aria-expanded={expanded}
        aria-controls={panelId}
        onClick={toggle}
      >
        <Database aria-hidden="true" size={13} />
        {count} {count === 1 ? "source" : "sources"}
        <ChevronDown aria-hidden="true" size={13} data-expanded={expanded} />
      </button>
      {expanded ? (
        <div id={panelId} className="source-evidence-panel" style={{ left: panelLeft }}>
          <dl className="evidence-meta">
            <div><dt>Generated</dt><dd>{formatTimestamp(meta.generated_at)}</dd></div>
            <div><dt>Correlation ID</dt><dd>{meta.correlation_id}</dd></div>
            <div><dt>Retention</dt><dd>{meta.retention_source.replaceAll("_", " ")}</dd></div>
          </dl>
          {count ? (
            <ul className="source-list">
              {meta.sources.map((source, index) => (
                <li key={`${source.node_id ?? "cluster"}-${source.endpoint_role}-${index}`}>
                  <div className="source-list-heading">
                    <strong>
                      {source.node_id == null ? "Cluster" : `Node ${source.node_id}`} · {endpointLabels[source.endpoint_role]}
                    </strong>
                    <span>{source.status} · {formatAge(source.age_ms)}</span>
                  </div>
                  <span>Observed {formatTimestamp(source.observed_at)}</span>
                  {source.error_code ? <code>{sanitizeError(source.error_code)}</code> : null}
                </li>
              ))}
            </ul>
          ) : <p>No source observations are available.</p>}
        </div>
      ) : null}
    </div>
  );
}
