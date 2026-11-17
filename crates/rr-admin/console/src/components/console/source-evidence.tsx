"use client";

import { ChevronDown, Database } from "lucide-react";
import { useId, useState } from "react";
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
  const panelId = useId();
  const count = meta.sources.length;
  return (
    <div className="source-evidence">
      <button
        type="button"
        className="source-evidence-trigger"
        aria-expanded={expanded}
        aria-controls={panelId}
        onClick={() => setExpanded((value) => !value)}
      >
        <Database aria-hidden="true" size={13} />
        {count} {count === 1 ? "source" : "sources"}
        <ChevronDown aria-hidden="true" size={13} data-expanded={expanded} />
      </button>
      {expanded ? (
        <div id={panelId} className="source-evidence-panel">
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
