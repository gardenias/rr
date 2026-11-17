import { render, screen, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { makeMeta } from "@/test/fixtures";
import type { PerformanceData } from "@/lib/contract";
import { PerformanceView } from "@/components/console/performance/performance-view";

const resourceMock = vi.hoisted(() => vi.fn());
vi.mock("@/lib/use-console-resource", () => ({ useConsoleResource: resourceMock }));
vi.mock("@/lib/use-route-query-state", () => ({
  useRouteQueryState: () => ({
    state: { window: "15m", filters: { group_id: "7" } },
    setWindow: vi.fn(),
    setFilter: vi.fn(),
  }),
}));

function performanceData(overrides: Partial<PerformanceData> = {}): PerformanceData {
  return {
    window: "15m",
    retention_source: "CONSOLE_MEMORY",
    series: [
      {
        metric: "rr_server_request_duration_seconds",
        labels: { group_id: "1", quantile: "0.99" },
        points: [{ timestamp: "2026-11-16T08:00:00Z", value: 0.12 }],
      },
      {
        metric: "rr_apply_duration_seconds",
        labels: { group_id: "1", quantile: "0.99" },
        points: [{ timestamp: "2026-11-16T08:00:00Z", value: 0.04 }],
      },
    ],
    notices: [
      {
        feature: "UNALIGNED_COHORTS",
        status: "UNSUPPORTED",
        reason: "Server, proposal, and apply samples do not share outcome and cohort labels.",
      },
    ],
    ...overrides,
  };
}

function renderPerformance(data: PerformanceData, meta = makeMeta()) {
  const refresh = vi.fn();
  resourceMock.mockReturnValue({ data, meta, error: null, loading: false, refresh });
  return render(<PerformanceView />);
}

describe("PerformanceView", () => {
  it("keeps latency cohorts separate and exposes their values in a table", async () => {
    const user = userEvent.setup();
    renderPerformance(performanceData());

    expect(screen.getByRole("heading", { level: 1, name: "Performance" })).toBeVisible();
    expect(screen.getByText("UNALIGNED_COHORTS", { exact: false })).toBeVisible();
    expect(screen.getByText(/Console memory retention · this Console process only/)).toBeVisible();
    const latency = screen.getByRole("region", { name: "Latency" });
    await user.click(within(latency).getByRole("tab", { name: "Data table" }));
    expect(within(latency).getByRole("table", { name: "Latency data" })).toHaveTextContent("0.12");
    expect(within(latency).getByRole("table", { name: "Latency data" })).toHaveTextContent("0.04");
    expect(screen.queryByText(/0\.08/)).not.toBeInTheDocument();
  });

  it("preserves reset gaps as unknown rather than charting zero", async () => {
    const user = userEvent.setup();
    renderPerformance(performanceData({
      series: [{
        metric: "rr_write_tps",
        labels: { group_id: "1" },
        points: [
          { timestamp: "2026-11-16T08:00:00Z", value: 42 },
          { timestamp: "2026-11-16T08:00:05Z", value: null },
        ],
      }],
    }));

    const throughput = screen.getByRole("region", { name: "Throughput" });
    await user.click(within(throughput).getByRole("tab", { name: "Data table" }));
    const table = within(throughput).getByRole("table", { name: "Throughput data" });
    expect(table).toHaveTextContent("42");
    expect(within(table).getByText("—")).toBeVisible();
    expect(table).not.toHaveTextContent("0 TPS");
  });

  it("preserves filters when drilling into source evidence", () => {
    renderPerformance(performanceData());

    expect(screen.getByRole("link", { name: "Inspect sources" })).toHaveAttribute("href", "/sources?window=15m&group_id=7");
  });

  it("degrades the legacy unsupported response without requiring target-only fields", () => {
    const legacy = {
      status: "UNSUPPORTED",
      series: [],
      filters: { window: "15m", group_id: null, node_id: null, op: null },
      reason: "legacy AdminService has no Prometheus performance projection",
    } as unknown as PerformanceData;
    renderPerformance(legacy, makeMeta({
      capabilities: [{ feature: "performance_metrics", status: "UNSUPPORTED", reason: "legacy AdminService has no Prometheus performance projection" }],
    }));

    expect(screen.getByText("No performance observations for this window")).toBeVisible();
    expect(screen.getByText(/legacy AdminService has no Prometheus performance projection/)).toBeVisible();
  });

  it("states when requested history is unavailable instead of backfilling", () => {
    renderPerformance(performanceData({
      retention_source: "NONE",
      series: [],
      notices: [{ feature: "performance_history", status: "UNSUPPORTED", reason: "Prometheus is not configured." }],
    }));

    expect(screen.getByText("No performance observations for this window")).toBeVisible();
    expect(screen.getByText(/No retained history is available\. Missing points/)).toBeVisible();
    expect(screen.getByText(/Prometheus is not configured/)).toBeVisible();
  });
});
