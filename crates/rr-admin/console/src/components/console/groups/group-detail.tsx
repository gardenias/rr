import { HealthBadge } from "@/components/console/health-badge";
import { formatLeader, formatNumber, formatTimestamp } from "@/lib/format";
import type { GroupDetailItem } from "@/components/console/groups/model";

export function GroupEvidenceSummary({ group }: { group: GroupDetailItem }) {
  const observations = group.health === "CONFLICTING_VIEWS" && group.conflicting_views?.length
    ? group.conflicting_views
    : group.observations ?? [];
  return (
    <section className="grid gap-4" aria-label={`Group ${group.group_id} status evidence`}>
      <div className="flex flex-wrap items-center gap-2">
        <HealthBadge status={group.health} />
        <span className="text-xs text-zinc-400">Runtime observations are not an atomic poll.</span>
      </div>
      <dl className="grid grid-cols-1 gap-3 sm:grid-cols-2">
        <div className="rounded border border-white/10 bg-black/10 p-3">
          <dt className="text-xs text-zinc-400">Configured membership</dt>
          <dd className="mt-1 text-zinc-100">
            {group.voters_total == null ? "—" : `${formatNumber(group.voters_total)} voters`}
            {group.learners != null ? ` · ${formatNumber(group.learners)} learners` : ""}
          </dd>
          <dd className="mt-1 text-xs text-zinc-400">
            {group.membership_revision ? `Membership revision ${group.membership_revision}` : "Membership revision —"}
          </dd>
        </div>
        <div className="rounded border border-white/10 bg-black/10 p-3">
          <dt className="text-xs text-zinc-400">Leader observation</dt>
          <dd className="mt-1 text-zinc-100">{formatLeader(group.leader_node_id)} · term {formatNumber(group.current_term)}</dd>
          <dd className="mt-1 text-xs text-zinc-400">Last consistent sample {formatTimestamp(group.last_consistent_sample_at)}</dd>
        </div>
        <div className="rounded border border-white/10 bg-black/10 p-3 sm:col-span-2">
          <dt className="text-xs text-zinc-400">Quorum perspective</dt>
          <dd className="mt-1 text-zinc-100">
            {group.voters_reachable == null || group.voters_total == null
              ? "Console voter reachability —"
              : `Console view: ${formatNumber(group.voters_reachable)} / ${formatNumber(group.voters_total)} voters reachable`}
          </dd>
          <dd className="mt-1 text-xs text-zinc-400">Console reachability is not proof that the cluster has lost or retained quorum.</dd>
        </div>
      </dl>
      {group.health === "SYNCING" ? (
        <p className="rounded border border-orange-400/30 bg-orange-400/10 p-3 text-xs text-zinc-300">
          Different terms or membership revisions are being observed. Non-atomic samples may be transitioning; this is not a same-term conflict verdict.
        </p>
      ) : null}
      {group.health === "CONFLICTING_VIEWS" ? (
        <p className="rounded border border-rose-400/30 bg-rose-400/10 p-3 text-xs text-zinc-300">
          Same-term leader conflict reported by Console. Compare the timestamped source observations before diagnosing Raft divergence.
        </p>
      ) : null}
      {observations.length ? (
        <div>
          <h3 className="mb-2 text-xs font-medium text-zinc-300">Timestamped leader observations</h3>
          <ul className="grid gap-2 text-xs text-zinc-400">
            {observations.map((observation, index) => (
              <li key={`${observation.node_id}-${observation.observed_at ?? index}`} className="rounded border border-white/10 p-2">
                Node {observation.node_id} observed {formatLeader(observation.leader_node_id)} · term {formatNumber(observation.current_term)} · {formatTimestamp(observation.observed_at)}
              </li>
            ))}
          </ul>
        </div>
      ) : null}
    </section>
  );
}
