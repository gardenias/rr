import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { makeMeta } from "@/test/fixtures";
import type { Capability } from "@/lib/contract";
import { CapabilityNotice } from "@/components/console/capability-notice";
import { FreshnessBadge } from "@/components/console/freshness-badge";
import { HealthBadge } from "@/components/console/health-badge";
import { SourceEvidence } from "@/components/console/source-evidence";

describe("evidence components", () => {
  it("uses text as well as color for health states", () => {
    render(<HealthBadge status="QUORUM_AT_RISK" />);
    expect(screen.getByText("Quorum at risk")).toBeVisible();
    expect(screen.getByRole("status")).toHaveAccessibleName(
      "Health: Quorum at risk",
    );
  });

  it("shows stale age and partial observation explicitly", () => {
    const meta = makeMeta({
      partial: true,
      sources: [
        {
          node_id: 2,
          endpoint_role: "METRICS",
          observed_at: "2026-11-16T07:58:55Z",
          age_ms: 65_000,
          status: "STALE",
          error_code: "SCRAPE_TIMEOUT",
        },
      ],
    });
    render(<FreshnessBadge meta={meta} />);
    expect(screen.getByText("Stale · 65s old")).toBeVisible();
    expect(screen.getByText("Partial")).toBeVisible();
  });

  it("exposes per-node evidence and errors on demand", async () => {
    const user = userEvent.setup();
    const meta = makeMeta({
      sources: [
        {
          node_id: 3,
          endpoint_role: "ADMIN",
          observed_at: "2026-11-16T07:59:58Z",
          age_ms: 2_000,
          status: "ERROR",
          error_code: "UNIMPLEMENTED",
        },
      ],
    });
    render(<SourceEvidence meta={meta} />);
    await user.click(screen.getByRole("button", { name: "1 source" }));
    expect(screen.getByText("Node 3 · Admin")).toBeVisible();
    expect(screen.getByText("UNIMPLEMENTED")).toBeVisible();
    expect(screen.getByText("corr-test-1")).toBeVisible();
  });

  it("labels health, readiness, and Prometheus history source roles", async () => {
    const user = userEvent.setup();
    const roles = ["HEALTH", "READY", "PROMETHEUS_HISTORY"] as const;
    const meta = makeMeta({
      sources: roles.map((endpoint_role, index) => ({
        node_id: index + 1,
        endpoint_role,
        observed_at: "2026-11-16T07:59:58Z",
        age_ms: 2_000,
        status: "ONLINE",
      })),
    });
    render(<SourceEvidence meta={meta} />);
    await user.click(screen.getByRole("button", { name: "3 sources" }));

    expect(screen.getByText("Node 1 · Health")).toBeVisible();
    expect(screen.getByText("Node 2 · Ready")).toBeVisible();
    expect(screen.getByText("Node 3 · Prometheus history")).toBeVisible();
  });

  it("uses unique disclosure relationships for multiple source panels", () => {
    render(<><SourceEvidence meta={makeMeta()} /><SourceEvidence meta={makeMeta()} /></>);
    const controls = screen.getAllByRole("button", { name: "1 source" }).map((button) => button.getAttribute("aria-controls"));
    expect(new Set(controls).size).toBe(2);
  });

  it.each([
    ["UNTRACKED", "Untracked"],
    ["PARTIAL", "Partially supported"],
  ])("labels the %s capability state", (status, label) => {
    render(<CapabilityNotice capability={{ feature: "cluster_identity", status: status as Capability["status"] }} />);
    expect(screen.getByText(label)).toBeVisible();
  });

  it("explains unsupported capabilities instead of rendering zero values", () => {
    render(
      <CapabilityNotice
        capability={{
          feature: "node_resources",
          status: "UNSUPPORTED",
          reason: "GetNodeDescriptor is not implemented by node 1",
        }}
      />,
    );
    expect(screen.getByText("Unsupported")).toBeVisible();
    expect(screen.getByText(/GetNodeDescriptor/)).toBeVisible();
  });
});
