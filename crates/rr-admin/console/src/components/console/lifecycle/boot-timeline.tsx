import { CircleAlert, CircleCheck, Play } from "lucide-react";
import type { BootRecord } from "@/lib/contract";
import { Badge } from "@/components/ui/badge";
import { formatDuration, formatTimestamp } from "@/lib/format";

export interface TimelineBoot extends BootRecord {
  node_id: number;
  record_source?: string | null;
  previous_app_version?: string | null;
  previous_rr_version?: string | null;
}

function bootDuration(boot: TimelineBoot): string {
  if (!boot.ended_at) return "Current";
  const duration = (new Date(boot.ended_at).getTime() - new Date(boot.started_at).getTime()) / 1000;
  return Number.isFinite(duration) && duration >= 0 ? formatDuration(duration) : "—";
}

export function shutdownLabel(state: BootRecord["shutdown_state"]): string {
  switch (state) {
    case "GRACEFUL": return "Graceful shutdown";
    case "ABNORMAL_OR_UNCERTAIN": return "Abnormal or uncertain";
    case "RUNNING": return "Running";
    case "UNKNOWN": return "Shutdown unknown";
  }
}

function ShutdownBadge({ state }: { state: BootRecord["shutdown_state"] }) {
  const tone = state === "GRACEFUL" || state === "RUNNING" ? "ok" : state === "ABNORMAL_OR_UNCERTAIN" ? "accent" : "neutral";
  return <Badge tone={tone}>{shutdownLabel(state)}</Badge>;
}

export function BootTimeline({ items, partial }: { items: TimelineBoot[]; partial: boolean }) {
  const grouped = Map.groupBy(items, (item) => item.node_id);
  return (
    <section aria-labelledby="restart-timeline-title" className="grid gap-4 border border-(--line-1) bg-(--bg-1) p-4">
      <div>
        <h2 id="restart-timeline-title">Restart timeline</h2>
        <p className="mt-1 text-xs text-(--tx-3)">Start and shutdown records are ordered per node. End state describes the record, never an inferred cause.</p>
      </div>
      {partial ? (
        <p className="border-l-2 border-(--acc) bg-(--acc-tint) p-3 text-xs text-(--tx-2)">
          Console observation gaps are not process crash evidence. Partial collection can hide intervals between records.
        </p>
      ) : null}
      <div className="grid gap-5">
        {[...grouped.entries()].map(([nodeId, boots]) => (
          <article key={nodeId} aria-label={`Node ${nodeId} restart timeline`}>
            <h3 className="mb-3">Node {nodeId}</h3>
            <ol className="grid gap-0">
              {boots.map((boot, index) => (
                <li key={boot.boot_id} className="relative grid grid-cols-[20px_1fr] gap-3 pb-5 last:pb-0">
                  {index < boots.length - 1 ? <span className="absolute top-5 bottom-0 left-[7px] w-px bg-(--line-2)" aria-hidden="true" /> : null}
                  <span className="mt-1 text-(--acc)">
                    {boot.shutdown_state === "RUNNING" ? <Play aria-hidden="true" size={15} /> : boot.shutdown_state === "GRACEFUL" ? <CircleCheck aria-hidden="true" size={15} /> : <CircleAlert aria-hidden="true" size={15} />}
                  </span>
                  <div className="min-w-0 border border-(--line-1) bg-(--bg-2) p-3">
                    <div className="flex flex-wrap items-center gap-2">
                      <code className="text-(--tx-1)">{boot.boot_id}</code>
                      <ShutdownBadge state={boot.shutdown_state} />
                      <Badge tone={boot.persistence === "VOLATILE" ? "accent" : "neutral"}>{boot.persistence}</Badge>
                    </div>
                    <p className="mt-2 text-xs text-(--tx-3)">Started {formatTimestamp(boot.started_at)}</p>
                    <p className="text-xs text-(--tx-3)">{boot.ended_at ? `Ended ${formatTimestamp(boot.ended_at)}` : "No shutdown record; this is the current boot"}</p>
                    <p className="mt-2 text-xs text-(--tx-3)">App {boot.app_version ?? "—"} · Rr {boot.rr_version ?? "—"} · <code>{boot.git_commit ?? "—"}</code></p>
                  </div>
                </li>
              ))}
            </ol>
          </article>
        ))}
      </div>
      <div className="text-xs">
        <h3 className="mb-3">Restart history</h3>
        <div className="dense-table-scroll">
          <table className="dense-table" aria-label="Restart timeline data">
            <thead><tr><th scope="col">Node</th><th scope="col">Boot ID</th><th scope="col">Started</th><th scope="col">Ended</th><th scope="col">Previous shutdown</th><th scope="col">Previous version</th><th scope="col">Current version</th><th scope="col">Duration</th><th scope="col">Persistence</th><th scope="col">Source</th></tr></thead>
            <tbody>{items.map((boot) => (
              <tr key={`${boot.node_id}-${boot.boot_id}`}>
                <td>Node {boot.node_id}</td><td><code>{boot.boot_id}</code></td><td>{formatTimestamp(boot.started_at)}</td><td>{formatTimestamp(boot.ended_at)}</td><td>{shutdownLabel(boot.shutdown_state)}</td><td>{boot.previous_app_version ?? "—"}<small className="block text-[10px] text-(--tx-4)">Rr {boot.previous_rr_version ?? "—"}</small></td><td>{boot.app_version ?? "—"}<small className="block text-[10px] text-(--tx-4)">Rr {boot.rr_version ?? "—"}</small></td><td>{bootDuration(boot)}</td><td>{boot.persistence}</td><td>{boot.record_source ?? "Node boot journal"}</td>
              </tr>
            ))}</tbody>
          </table>
        </div>
      </div>
    </section>
  );
}
