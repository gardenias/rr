import { render, screen, within } from "@testing-library/react";
import { VersionMatrix, type VersionItem } from "./version-matrix";

const versions: VersionItem[] = [
  {
    node_id: 1,
    app_version: "shop-2.0",
    rr_version: "0.2.0",
    git_commit: "bbb2222",
    config_hash: "cfg-b",
    dirty: true,
    admin_protocol: { major: 1, minor: 1 },
    raft_protocol: { major: 1, minor: 1, min_peer_minor: 1 },
    compatibility: { status: "INCOMPATIBLE", reason: "Raft minor does not satisfy node 2 min_peer_minor", incompatible_node_ids: [2] },
  },
  {
    node_id: 2,
    app_version: "shop-1.0",
    rr_version: "0.1.0",
    git_commit: "aaa1111",
    config_hash: "cfg-a",
    dirty: false,
    admin_protocol: { major: 1, minor: 0 },
    raft_protocol: { major: 1, minor: 0, min_peer_minor: 0 },
    compatibility: { status: "UNKNOWN", reason: "No peer comparison is available" },
  },
];

describe("version matrix", () => {
  it("shows authoritative incompatibility evidence and dirty-build warnings", () => {
    render(<VersionMatrix items={versions} />);
    const row = screen.getByRole("row", { name: /Node 1/ });
    expect(within(row).getByText("Dirty build")).toBeVisible();
    expect(within(row).getByText("Incompatible")).toBeVisible();
    expect(within(row).getByText(/node 2 min_peer_minor/i)).toBeVisible();
  });

  it("keeps application and framework versions in separate columns", () => {
    render(<VersionMatrix items={versions} />);
    expect(screen.getByRole("columnheader", { name: "App version" })).toBeVisible();
    expect(screen.getByRole("columnheader", { name: "Rr version" })).toBeVisible();
    expect(screen.getByText("2 application versions · 2 Rr versions")).toBeVisible();
  });
});
