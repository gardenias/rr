import type { NodeItem } from "@/lib/contract";
import { filterNodes, formatRatio, nodeVersion, safeConfigValue, type FeatureNode } from "./model";

const nodes: FeatureNode[] = [
  {
    ...({ node_id: 1, status: "ONLINE", admin_addr: "10.0.0.1:52001", listen_addr: null, group_ids: [1], leader_group_count: 1, uptime_secs: 10, last_success: null } satisfies NodeItem),
    zone: "zone-a",
    build: { app_version: "shop-2.0", rr_version: "0.2.0", git_commit: "aaa", dirty: false },
  },
  {
    ...({ node_id: 2, status: "DEGRADED", admin_addr: "10.0.0.2:52001", listen_addr: null, group_ids: [2], leader_group_count: 0, uptime_secs: 20, last_success: null } satisfies NodeItem),
    zone: "zone-b",
    version: "shop-1.0",
  },
];

describe("node filters", () => {
  it("combines status, zone, role, version, and text filters", () => {
    expect(filterNodes(nodes, { status: "DEGRADED" }).map((node) => node.node_id)).toEqual([2]);
    expect(filterNodes(nodes, { zone: "zone-a" }).map((node) => node.node_id)).toEqual([1]);
    expect(filterNodes(nodes, { role: "leader" }).map((node) => node.node_id)).toEqual([1]);
    expect(filterNodes(nodes, { version: "shop-1.0" }).map((node) => node.node_id)).toEqual([2]);
    expect(filterNodes(nodes, { filter: "10.0.0.2" }).map((node) => node.node_id)).toEqual([2]);
    expect(filterNodes(nodes, { status: "ONLINE", zone: "zone-b" })).toEqual([]);
  });

  it("uses the application version and keeps Rr as separate build evidence", () => {
    expect(nodeVersion(nodes[0]!)).toBe("shop-2.0");
    expect(nodes[0]?.build?.rr_version).toBe("0.2.0");
  });
});

describe("allowlisted configuration values", () => {
  it("renders scalar values but never serializes nested credentials", () => {
    expect(safeConfigValue("rocksdb")).toBe("rocksdb");
    expect(safeConfigValue(3)).toBe("3");
    expect(safeConfigValue(false)).toBe("false");
    expect(safeConfigValue({ admin_token: "must-not-render" })).toBe("Unsupported value shape");
  });
});

describe("resource formatting", () => {
  it("distinguishes a real zero ratio from missing data", () => {
    expect(formatRatio(0)).toBe("0%");
    expect(formatRatio(0.725)).toBe("72.5%");
    expect(formatRatio(null)).toBe("—");
    expect(formatRatio(undefined)).toBe("—");
  });
});
