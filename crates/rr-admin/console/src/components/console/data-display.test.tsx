import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import type { ColumnDef } from "@tanstack/react-table";
import { DenseTable } from "@/components/console/dense-table";
import { MetricChart } from "@/components/console/metric-chart";

type Row = { node: string; lag: number | null };
const rows: Row[] = [
  { node: "node-2", lag: 8 },
  { node: "node-1", lag: null },
];
const columns: ColumnDef<Row>[] = [
  { accessorKey: "node", header: "Node" },
  { accessorKey: "lag", header: "Lag" },
];

describe("accessible data displays", () => {
  it("provides sortable dense tables with an explicit empty value", async () => {
    const user = userEvent.setup();
    render(<DenseTable data={rows} columns={columns} caption="Replica lag" />);
    expect(screen.getByRole("table", { name: "Replica lag" })).toBeVisible();
    expect(screen.getByText("—")).toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: "Sort by Node" }));
    expect(screen.getAllByRole("row")[1]).toHaveTextContent("node-1");
  });

  it("moves focus with arrow keys when changing representations", async () => {
    const user = userEvent.setup();
    render(
      <MetricChart
        title="Write latency"
        summary="Two observed points."
        data={[{ timestamp: "08:00", p99: 12 }]}
        xKey="timestamp"
        series={[{ key: "p99", label: "P99", unit: "ms" }]}
      />,
    );
    await user.click(screen.getByRole("tab", { name: "Chart" }));
    await user.keyboard("{ArrowRight}");
    expect(screen.getByRole("tab", { name: "Data table" })).toHaveFocus();
  });

  it("offers the chart data as an equivalent table", async () => {
    const user = userEvent.setup();
    render(
      <MetricChart
        title="Write latency"
        summary="Write P99 rose from 12 to 18 milliseconds."
        data={[
          { timestamp: "08:00", p99: 12 },
          { timestamp: "08:01", p99: 18 },
        ]}
        xKey="timestamp"
        series={[{ key: "p99", label: "P99", unit: "ms" }]}
      />,
    );
    expect(screen.getByText(/Write P99 rose/)).toBeVisible();
    await user.click(screen.getByRole("tab", { name: "Data table" }));
    expect(screen.getByRole("table", { name: "Write latency data" })).toBeVisible();
    expect(screen.getByText("18 ms")).toBeVisible();
  });
});
