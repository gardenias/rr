import { render, screen } from "@testing-library/react";
import { NodeResources } from "./node-resources";
import type { FeatureNode } from "./model";

const node: FeatureNode = {
  node_id: 1,
  status: "ONLINE",
  admin_addr: "127.0.0.1:52001",
  listen_addr: null,
  group_ids: [],
  leader_group_count: 0,
  uptime_secs: 10,
  last_success: null,
  resources: {
    cpu_usage_ratio: 0,
    cpu_cores: 8,
    cpu_limit_cores: 2,
    memory_used_bytes: 0,
    memory_total_bytes: 1024,
    scope: "cgroup",
    sampled_at: "2026-11-16T08:00:00Z",
  },
};

describe("node resources", () => {
  it("renders zero measurements and their collection scope", () => {
    render(<NodeResources node={node} />);
    expect(screen.getByText("0%")).toBeVisible();
    expect(screen.getByText("0 B / 1 KiB")).toBeVisible();
    expect(screen.getAllByText("CGROUP").length).toBeGreaterThan(0);
  });

  it("does not turn unsupported resources into zero", () => {
    render(<NodeResources node={{ ...node, resources: null }} />);
    expect(screen.getByText(/Resource sampling unsupported/i)).toBeVisible();
    expect(screen.queryByText("0%")).not.toBeInTheDocument();
  });
});
