export function makeMeta(pathname, overrides = {}) {
  return {
    schema_version: "console.v1",
    correlation_id: `mock-${pathname.replaceAll("/", "-")}`,
    cluster_id: "mock-cluster-uuid",
    generated_at: new Date().toISOString(),
    partial: false,
    time_window: "15m",
    retention_source: "CONSOLE_MEMORY",
    sources: [
      {
        node_id: 1,
        endpoint_role: "ADMIN",
        observed_at: new Date(Date.now() - 1_000).toISOString(),
        age_ms: 1_000,
        status: "ONLINE",
      },
    ],
    capabilities: [],
    ...overrides,
  };
}

export function envelope(pathname, data, overrides) {
  return { meta: makeMeta(pathname, overrides), data };
}

export function responseFor(pathname, searchParams) {
  const window = searchParams.get("window") ?? "15m";
  switch (pathname) {
    case "/summary":
      return envelope(pathname, {
        nodes: [],
        groups: [],
        cluster_name: "SIMULATION · three-node cluster",
        environment: "SIMULATION",
        nodes_online: 3,
        nodes_total: 3,
        groups_with_leader: 2,
        groups_total: 2,
        quorum_risk_groups: 0,
        active_alerts: 2,
        observation_coverage: 100,
      }, { time_window: window });
    case "/nodes":
      return envelope(pathname, {
        items: [1, 2, 3].map((node_id) => ({
          node_id,
          status: "ONLINE",
          admin_addr: `127.0.0.1:${52000 + node_id}`,
          listen_addr: `127.0.0.1:${51000 + node_id}`,
          group_ids: [1, 2],
          leader_group_count: node_id === 1 ? 2 : 0,
          last_success: new Date().toISOString(),
          observability_addr: `127.0.0.1:${53000 + node_id}`,
          zone: `zone-${String.fromCharCode(96 + node_id)}`,
          version: "0.2.0-simulation",
          uptime_secs: 3600 + node_id,
          last_contact: new Date().toISOString(),
        })),
        next_cursor: null,
      });
    case "/topology": {
      const view = searchParams.get("view") ?? "nodes";
      if (view === "raft") {
        return envelope(pathname, {
          view,
          revision: "42",
          groups: [{ group_id: 1, health: "HEALTHY", leader_node_id: 1, current_term: 4, last_log_index: 120, source_count: 3, observations: [] }],
        });
      }
      if (view === "routes") {
        return envelope(pathname, {
          view,
          revision: "42",
          routes: [{ partition_id: 10, group_id: 1, replica_node_ids: [1, 2, 3] }],
        });
      }
      return envelope(pathname, {
        view: "nodes",
        revision: "42",
        nodes: [1, 2, 3].map((node_id) => ({
          node_id,
          status: "ONLINE",
          admin_addr: `127.0.0.1:${52000 + node_id}`,
          listen_addr: `127.0.0.1:${51000 + node_id}`,
          group_ids: [1, 2],
          leader_group_count: node_id === 1 ? 2 : 0,
          uptime_secs: 3600 + node_id,
          last_success: new Date().toISOString(),
        })),
        edges: [],
      });
    }
    case "/groups":
      return envelope(pathname, {
        items: [1, 2].map((group_id) => ({
          group_id,
          formation: "FORMED",
          health: "HEALTHY",
          leader_node_id: 1,
          current_term: 4,
          last_log_index: 120,
          voters_reachable: 3,
          voters_total: 3,
          source_count: 3,
        })),
        next_cursor: null,
      });
    case "/metrics/performance":
      return envelope(pathname, {
        window,
        retention_source: "CONSOLE_MEMORY",
        series: [],
        notices: [],
      });
    case "/snapshots":
      return envelope(pathname, {
        items: [{ group_id: 1, node_id: 1, snapshot_index: 100, snapshot_term: 3, last_log_index: 120, install_count: 0, build_failure: null, source_count: 1 }],
        next_cursor: null,
      }, {
        capabilities: [{ feature: "snapshot_task_progress", status: "UNSUPPORTED", reason: "P0 exposes observed snapshot indexes, not task progress." }],
      });
    case "/lifecycle":
      return envelope(pathname, {
        view: searchParams.get("view") ?? "current",
        items: [{ boot_id: "simulation-boot-1", started_at: "2026-11-16T07:00:00Z", ended_at: null, shutdown_state: "RUNNING", persistence: "PERSISTENT", app_version: "simulation", rr_version: "0.2.0", git_commit: "0000000" }],
        next_cursor: null,
      });
    case "/events":
      return envelope(pathname, {
        items: [{ event_id: "simulation-event-1", severity: "INFO", object: "group-1", state: "RESOLVED", first_observed_at: "2026-11-16T07:00:00Z", last_observed_at: "2026-11-16T07:01:00Z", evidence: [] }],
        next_cursor: null,
      });
    case "/sources":
      return envelope(pathname, {
        startup_mode: "PEERS",
        cluster_name: "SIMULATION",
        topology_revision: "42",
        read_only: true,
        sources: [1, 2, 3].map((node_id) => ({ node_id, admin_addr: `127.0.0.1:${52000 + node_id}`, observability_addr: `127.0.0.1:${53000 + node_id}`, last_success: new Date().toISOString(), last_failure: null, circuit_state: "CLOSED", sanitized_error: null })),
      });
    default:
      return null;
  }
}
