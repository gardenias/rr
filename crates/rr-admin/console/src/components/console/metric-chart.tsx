"use client";

import { useState, type KeyboardEvent } from "react";
import {
  CartesianGrid,
  Line,
  LineChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";
import { formatNumber } from "@/lib/format";

interface MetricSeries {
  key: string;
  label: string;
  unit?: string;
}

interface MetricChartProps {
  title: string;
  summary: string;
  data: Array<Record<string, string | number | null>>;
  xKey: string;
  series: MetricSeries[];
}

export function MetricChart({ title, summary, data, xKey, series }: MetricChartProps) {
  const [view, setView] = useState<"chart" | "table">("chart");
  const tabId = `${title.replaceAll(/\s/g, "-").toLowerCase()}-chart-tab`;
  const tableTabId = `${title.replaceAll(/\s/g, "-").toLowerCase()}-table-tab`;
  const chooseView = (next: "chart" | "table") => {
    setView(next);
    document.getElementById(next === "chart" ? tabId : tableTabId)?.focus();
  };
  const onTabKeyDown = (event: KeyboardEvent<HTMLDivElement>) => {
    if (["ArrowLeft", "ArrowRight", "Home", "End"].includes(event.key)) {
      event.preventDefault();
      chooseView(event.key === "Home" ? "chart" : event.key === "End" ? "table" : view === "chart" ? "table" : "chart");
    }
  };
  return (
    <section className="metric-chart" aria-label={title}>
      <div className="metric-chart-header">
        <div>
          <h2>{title}</h2>
          <p>{summary}</p>
        </div>
        <div role="tablist" aria-label={`${title} representation`} onKeyDown={onTabKeyDown}>
          <button
            id={tabId}
            type="button"
            role="tab"
            aria-selected={view === "chart"}
            tabIndex={view === "chart" ? 0 : -1}
            onClick={() => setView("chart")}
          >
            Chart
          </button>
          <button
            id={tableTabId}
            type="button"
            role="tab"
            aria-selected={view === "table"}
            tabIndex={view === "table" ? 0 : -1}
            onClick={() => setView("table")}
          >
            Data table
          </button>
        </div>
      </div>
      {view === "chart" ? (
        <div className="chart-frame" role="tabpanel" aria-label={`${title} chart`}>
          <ResponsiveContainer width="100%" height={240}>
            <LineChart data={data} accessibilityLayer>
              <CartesianGrid stroke="var(--line-1)" vertical={false} />
              <XAxis dataKey={xKey} stroke="var(--tx-3)" fontSize={11} />
              <YAxis stroke="var(--tx-3)" fontSize={11} width={48} />
              <Tooltip contentStyle={{ background: "var(--bg-2)", border: "1px solid var(--line-2)" }} />
              {series.map((item, index) => (
                <Line
                  key={item.key}
                  type="monotone"
                  dataKey={item.key}
                  name={item.label}
                  stroke={index === 0 ? "var(--acc)" : "var(--tx-3)"}
                  strokeDasharray={index === 0 ? undefined : "4 4"}
                  dot={false}
                  isAnimationActive={false}
                />
              ))}
            </LineChart>
          </ResponsiveContainer>
        </div>
      ) : (
        <div className="dense-table-scroll" role="tabpanel">
          <table className="dense-table" aria-label={`${title} data`}>
            <thead><tr><th scope="col">{xKey}</th>{series.map((item) => <th scope="col" key={item.key}>{item.label}</th>)}</tr></thead>
            <tbody>
              {data.map((row, rowIndex) => (
                <tr key={rowIndex}>
                  <td>{row[xKey] ?? "—"}</td>
                  {series.map((item) => {
                    const value = row[item.key];
                    return <td key={item.key}>{typeof value === "number" ? `${formatNumber(value)}${item.unit ? ` ${item.unit}` : ""}` : value ?? "—"}</td>;
                  })}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </section>
  );
}
