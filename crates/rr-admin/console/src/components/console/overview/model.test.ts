import { makeMeta } from "@/test/fixtures";
import type { PerformanceData, SummaryData } from "@/lib/contract";
import { drilldownHref, incidentDrilldownHref, latestMetricValue, primaryIncident, summaryCounts } from "./model";

const summary: SummaryData = {
  nodes: [],
  groups: [],
  nodes_online: 3,
  nodes_total: 3,
  groups_with_leader: 2,
  groups_total: 2,
  quorum_risk_groups: 0,
  active_alerts: 0,
  observation_coverage: 100,
};

describe("overview incident prioritization", () => {
  it("prioritizes an observed leaderless group over collection warnings", () => {
    const incident = primaryIncident({
      ...summary,
      groups: [{ group_id: 7, health: "LEADERLESS_OBSERVED", leader_node_id: null, current_term: 4, last_log_index: 20, source_count: 3 }],
    }, makeMeta({ partial: true }));

    expect(incident).toMatchObject({ title: "Group 7 has no observed leader", href: "/groups?window=15m&group_id=7" });
  });

  it("does not claim complete health when observations are partial", () => {
    expect(primaryIncident(summary, makeMeta())).toBeNull();
    expect(primaryIncident(summary, makeMeta({ partial: true }))).toMatchObject({
      title: "Cluster observations are partial",
      href: "/sources?window=15m",
    });
  });

  it("keeps aggregate quorum-risk evidence when the group page is absent", () => {
    const incident = primaryIncident({ ...summary, quorum_risk_groups: 2 }, makeMeta());
    expect(incident).toMatchObject({
      title: "2 groups are at quorum risk from the Console view",
      href: "/groups?window=15m&state=QUORUM_AT_RISK",
    });
  });

  it("surfaces stale evidence without converting it to a node crash", () => {
    const incident = primaryIncident(summary, makeMeta({
      sources: [{ node_id: 2, endpoint_role: "ADMIN", observed_at: null, age_ms: 60_000, status: "STALE" }],
    }));
    expect(incident?.title).toBe("Some cluster evidence is stale");
    expect(incident?.description).not.toMatch(/crash/i);
  });
});

describe("legacy summary fallbacks", () => {
  it("derives only exact counts from returned node and group arrays", () => {
    expect(summaryCounts({
      nodes: [
        { node_id: 1, status: "ONLINE", admin_addr: null, listen_addr: null, group_ids: [], leader_group_count: null, uptime_secs: null, last_success: null },
        { node_id: 2, status: "UNREACHABLE", admin_addr: null, listen_addr: null, group_ids: [], leader_group_count: null, uptime_secs: null, last_success: null },
      ],
      groups: [
        { group_id: 1, health: "HEALTHY", leader_node_id: 1, current_term: 1, last_log_index: 1, source_count: 1 },
        { group_id: 2, health: "QUORUM_AT_RISK", leader_node_id: null, current_term: 1, last_log_index: 1, source_count: 1 },
      ],
    })).toEqual({ nodesOnline: 1, nodesTotal: 2, groupsWithLeader: 1, groupsTotal: 2, quorumRisk: 1 });
  });
});

describe("overview metric and drilldown helpers", () => {
  it("uses the latest present cluster sample and never averages node quantiles", () => {
    const performance: PerformanceData = {
      window: "15m",
      retention_source: "CONSOLE_MEMORY",
      notices: [],
      series: [{
        metric: "write_p99_ms",
        labels: {},
        points: [
          { timestamp: "2026-11-16T08:00:00Z", value: 12 },
          { timestamp: "2026-11-16T08:00:05Z", value: null },
          { timestamp: "2026-11-16T08:00:10Z", value: 18 },
        ],
      }],
    };
    expect(latestMetricValue(performance, "write_p99_ms")).toBe(18);
    expect(latestMetricValue(performance, "write_tps")).toBeNull();
  });

  it("preserves shareable query state when changing drilldown filters", () => {
    expect(drilldownHref("/nodes", "window=1h&filter=zone-a&node_id=4", { node_id: null, status: "DEGRADED" }))
      .toBe("/nodes?window=1h&filter=zone-a&status=DEGRADED");
  });

  it("does not let a default incident window overwrite the active window", () => {
    expect(incidentDrilldownHref("/groups?window=15m&group_id=7", "window=1h&filter=zone-a"))
      .toBe("/groups?window=1h&filter=zone-a&group_id=7");
  });
});
