import { Copy, TriangleAlert } from "lucide-react";
import { Badge } from "@/components/ui/badge";
import type { NodeBuild, ProtocolVersion, VersionCompatibility } from "@/components/console/nodes/model";

export interface VersionItem extends NodeBuild {
  node_id: number;
  config_hash?: string | null;
  config_fingerprint?: string | null;
}

function protocol(admin?: ProtocolVersion | null, raft?: ProtocolVersion | null): string {
  if (!admin && !raft) return "—";
  const adminText = admin ? `Admin ${admin.major}.${admin.minor}` : "Admin —";
  const raftText = raft ? `Raft ${raft.major}.${raft.minor}${raft.min_peer_minor != null ? ` · min peer ${raft.min_peer_minor}` : ""}` : "Raft —";
  return `${adminText} · ${raftText}`;
}

function Compatibility({ value }: { value?: VersionCompatibility | null }) {
  if (!value) return <span title="No compatibility decision was supplied">—</span>;
  const label = value.status === "INCOMPATIBLE" ? "Incompatible" : value.status === "COMPATIBLE" ? "Compatible" : "UNKNOWN";
  return (
    <div className="max-w-80 whitespace-normal">
      <Badge tone={value.status === "INCOMPATIBLE" ? "bad" : value.status === "COMPATIBLE" ? "ok" : "neutral"}>
        {value.status === "INCOMPATIBLE" ? <TriangleAlert aria-hidden="true" size={12} /> : null}{label}
      </Badge>
      {value.reason ? <p className="mt-1 text-[11px] text-(--tx-3)">{value.reason}</p> : null}
    </div>
  );
}

async function copyCommit(commit: string) {
  await navigator.clipboard?.writeText(commit);
}

export function VersionMatrix({ items }: { items: VersionItem[] }) {
  const appVersions = new Set(items.map((item) => item.app_version).filter(Boolean));
  const rrVersions = new Set(items.map((item) => item.rr_version).filter(Boolean));
  return (
    <section aria-labelledby="version-matrix-title" className="grid gap-3">
      <div>
        <h2 id="version-matrix-title">Version matrix</h2>
        <p className="mt-1 text-xs text-(--tx-3)"><span>{appVersions.size} application versions · {rrVersions.size} Rr versions</span></p>
        <p className="mt-1 text-xs text-(--tx-3)">Compatibility is reported by the Console protocol rules, not inferred from version strings.</p>
      </div>
      <div className="dense-table-scroll">
        <table className="dense-table" aria-label="Node version matrix">
          <thead><tr><th scope="col">Node</th><th scope="col">App version</th><th scope="col">Rr version</th><th scope="col">Git commit</th><th scope="col">Protocol</th><th scope="col">Config hash</th><th scope="col">Build / compatibility</th></tr></thead>
          <tbody>{items.map((item, index) => (
            <tr key={`${item.node_id}-${item.git_commit ?? "unknown"}-${index}`}>
              <td>Node {item.node_id}</td>
              <td>{item.app_version ?? "—"}</td>
              <td>{item.rr_version ?? "—"}</td>
              <td>
                {item.git_commit ? <span className="inline-flex items-center gap-2"><code>{item.git_commit}</code><button type="button" className="icon-button" aria-label={`Copy commit ${item.git_commit}`} onClick={() => void copyCommit(item.git_commit!)}><Copy aria-hidden="true" size={12} /></button></span> : "—"}
              </td>
              <td>{protocol(item.admin_protocol, item.raft_protocol)}</td>
              <td><code>{item.config_hash ?? item.config_fingerprint ?? "—"}</code></td>
              <td>
                <div className="flex items-start gap-2 py-1">
                  {item.dirty ? <Badge tone="accent"><TriangleAlert aria-hidden="true" size={12} />Dirty build</Badge> : null}
                  <Compatibility value={item.compatibility} />
                </div>
              </td>
            </tr>
          ))}</tbody>
        </table>
      </div>
    </section>
  );
}
