import type { ConsoleMeta, PerformanceData, SummaryData } from "@/lib/contract";

export function summaryCounts(summary: SummaryData) {
  return {
    nodesOnline: summary.nodes_online ?? summary.nodes.filter((node) => node.status === "ONLINE").length,
    nodesTotal: summary.nodes_total ?? summary.nodes.length,
    groupsWithLeader: summary.groups_with_leader ?? summary.groups.filter((group) => group.leader_node_id != null && group.leader_node_id !== 0).length,
    groupsTotal: summary.groups_total ?? summary.groups.length,
    quorumRisk: summary.quorum_risk_groups ?? summary.groups.filter((group) => group.health === "QUORUM_AT_RISK").length,
  };
}

export interface Incident {
  title: string;
  description: string;
  href: string;
  severity: "critical" | "warning";
}

export function primaryIncident(summary: SummaryData, meta: ConsoleMeta): Incident | null {
  const leaderless = summary.groups.find((group) => group.health === "LEADERLESS_OBSERVED");
  if (leaderless) {
    return {
      title: `Group ${leaderless.group_id} has no observed leader`,
      description: "Fresh reachable replicas do not currently report a leader. Inspect the observation evidence before acting.",
      href: `/groups?window=15m&group_id=${leaderless.group_id}`,
      severity: "critical",
    };
  }
  const conflict = summary.groups.find((group) => group.health === "CONFLICTING_VIEWS");
  if (conflict) {
    return {
      title: `Group ${conflict.group_id} has conflicting leader observations`,
      description: "The BFF found conflicting same-term evidence inside its comparison window.",
      href: `/groups?window=15m&group_id=${conflict.group_id}`,
      severity: "critical",
    };
  }
  const incompatible = meta.capabilities.find((capability) =>
    capability.status === "ERROR" && /compatib|protocol|version/i.test(capability.feature));
  if (incompatible) {
    return {
      title: "Replica protocol compatibility needs attention",
      description: incompatible.reason ?? "The Console reported a version compatibility error.",
      href: "/lifecycle?window=15m&view=versions",
      severity: "critical",
    };
  }
  const quorumRisk = summary.groups.find((group) => group.health === "QUORUM_AT_RISK");
  if (quorumRisk || (summary.quorum_risk_groups ?? 0) > 0) {
    return {
      title: quorumRisk
        ? `Group ${quorumRisk.group_id} is at quorum risk from the Console view`
        : `${summary.quorum_risk_groups} groups are at quorum risk from the Console view`,
      description: "Reachable voters are below a majority. This is a Console observation, not proof that quorum is lost.",
      href: quorumRisk ? `/groups?window=15m&group_id=${quorumRisk.group_id}` : "/groups?window=15m&state=QUORUM_AT_RISK",
      severity: "critical",
    };
  }
  if (meta.sources.some((source) => source.status === "STALE")) {
    return {
      title: "Some cluster evidence is stale",
      description: "Last-known values remain visible. Inspect source freshness before drawing conclusions.",
      href: "/sources?window=15m",
      severity: "warning",
    };
  }
  if (meta.partial) {
    return {
      title: "Cluster observations are partial",
      description: "Some sources did not contribute to this response. Missing observations are not evidence of health.",
      href: "/sources?window=15m",
      severity: "warning",
    };
  }
  return null;
}

export function latestMetricValue(data: PerformanceData | null, metric: string): number | null {
  const series = data?.series.find((item) => item.metric === metric && Object.keys(item.labels).length === 0);
  if (!series) return null;
  for (let index = series.points.length - 1; index >= 0; index -= 1) {
    const value = series.points[index]?.value;
    if (value != null && Number.isFinite(value)) return value;
  }
  return null;
}

export function incidentDrilldownHref(incidentHref: string, currentSearch: string): string {
  const url = new URL(incidentHref, "http://console.local");
  const updates = Object.fromEntries(url.searchParams.entries());
  delete updates.window;
  return drilldownHref(url.pathname, currentSearch, updates);
}

export function drilldownHref(
  path: string,
  currentSearch: string,
  updates: Record<string, string | null>,
): string {
  const params = new URLSearchParams(currentSearch);
  if (!params.has("window")) params.set("window", "15m");
  for (const [key, value] of Object.entries(updates)) {
    if (value) params.set(key, value);
    else params.delete(key);
  }
  return `${path}?${params.toString()}`;
}
