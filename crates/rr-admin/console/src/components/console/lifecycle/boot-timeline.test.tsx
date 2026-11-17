import { render, screen } from "@testing-library/react";
import { BootTimeline, type TimelineBoot } from "./boot-timeline";

const boots: TimelineBoot[] = [
  {
    node_id: 2,
    boot_id: "boot-current",
    started_at: "2026-11-16T08:00:00Z",
    ended_at: null,
    shutdown_state: "RUNNING",
    persistence: "VOLATILE",
    app_version: "shop-2.0",
    rr_version: "0.2.0",
    git_commit: "bbb2222",
  },
  {
    node_id: 2,
    boot_id: "boot-previous",
    started_at: "2026-11-16T06:00:00Z",
    ended_at: "2026-11-16T07:00:00Z",
    shutdown_state: "ABNORMAL_OR_UNCERTAIN",
    persistence: "VOLATILE",
    app_version: "shop-1.0",
    rr_version: "0.1.0",
    git_commit: "aaa1111",
  },
];

describe("restart timeline", () => {
  it("keeps abnormal-or-uncertain shutdown evidence distinct from a crash", () => {
    render(<BootTimeline items={boots} partial={false} />);
    expect(screen.getAllByText("Abnormal or uncertain")).toHaveLength(2);
    expect(screen.queryByText(/crash/i)).not.toBeInTheDocument();
    expect(screen.getAllByText("boot-current")[0]).toBeVisible();
    expect(screen.getByRole("columnheader", { name: "Duration" })).toBeVisible();
    expect(screen.getByText("1h")).toBeVisible();
    expect(screen.getByRole("columnheader", { name: "Previous version" })).toBeVisible();
    expect(screen.getByRole("columnheader", { name: "Current version" })).toBeVisible();
  });

  it("labels volatile history and warns that console gaps are not process evidence", () => {
    render(<BootTimeline items={boots} partial={true} />);
    expect(screen.getAllByText("VOLATILE").length).toBeGreaterThan(0);
    expect(screen.getByText(/Console observation gaps are not process crash evidence/i)).toBeVisible();
    expect(screen.getByRole("table", { name: "Restart timeline data" })).toBeVisible();
  });
});
