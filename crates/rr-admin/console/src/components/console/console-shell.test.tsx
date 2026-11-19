import { fireEvent, render, screen } from "@testing-library/react";
import { useState } from "react";
import { ConsoleShell } from "@/components/console/console-shell";
import type { ConnectionSummary } from "@/lib/connections";
import { useRefresh } from "@/lib/refresh-context";
import { makeMeta } from "@/test/fixtures";

const mocks = vi.hoisted(() => ({
  push: vi.fn(),
  replace: vi.fn(),
  activeConnection: null as ConnectionSummary | null,
  connections: [] as ConnectionSummary[],
  search: "?window=1h&connection=cluster-a&node_id=9&cursor=old",
  resource: vi.fn(),
}));

vi.mock("next/navigation", () => ({
  usePathname: () => "/overview/",
  useRouter: () => ({ push: mocks.push, replace: mocks.replace }),
  useSearchParams: () => new URLSearchParams(mocks.search),
}));
vi.mock("@/lib/connection-context", () => ({
  useActiveConnection: () => ({
    connectionId: mocks.activeConnection?.id ?? null,
    activeConnection: mocks.activeConnection,
    connections: mocks.connections,
    keychainAvailable: true,
    loading: false,
    error: null,
    recentConnectionId: null,
    refresh: vi.fn(),
  }),
}));
vi.mock("@/lib/use-console-resource", () => ({
  useConsoleResource: mocks.resource,
}));
vi.mock("@/lib/use-console-stream", () => ({ useConsoleStream: () => undefined }));

function connection(id: string, name: string, status: ConnectionSummary["status"]): ConnectionSummary {
  return {
    id,
    name,
    environment: "test",
    admin_seeds: [],
    discovery: null,
    cluster_name: null,
    admin_tls: { ca: null, cert: null, key: null, domain: null },
    discovery_tls: { ca: null, cert: null, key: null, domain: null },
    discovery_username: null,
    discovery_allow_insecure: false,
    prometheus: null,
    poll_interval_secs: 5,
    created_at: "2026-11-19T08:00:00Z",
    updated_at: "2026-11-19T08:00:00Z",
    observed_cluster_id: null,
    last_connected_at: null,
    source: "PROFILE",
    status,
    last_error: null,
    node_count: null,
    group_count: null,
    credentials_configured: false,
    credential_source: null,
  };
}

function LocalStateProbe() {
  const [count, setCount] = useState(0);
  return <button type="button" onClick={() => setCount((value) => value + 1)}>Local count {count}</button>;
}

function SharedSummaryWriter() {
  const { connectionId, recordSummary } = useRefresh();
  if (!connectionId) return null;
  return <button type="button" onClick={() => recordSummary(connectionId, {
    meta: makeMeta(),
    data: {
      nodes: [],
      groups: [],
      cluster_name: "Streamed cluster",
      environment: "STREAM",
      nodes_online: 3,
      nodes_total: 3,
      groups_with_leader: 2,
      groups_total: 2,
      quorum_risk_groups: 0,
      active_alerts: 11,
      observation_coverage: 100,
    },
  })}>Publish streamed summary</button>;
}

describe("ConsoleShell connection switching", () => {
  beforeEach(() => {
    mocks.push.mockReset();
    mocks.replace.mockReset();
    mocks.resource.mockReset();
    mocks.resource.mockReturnValue({ data: null, meta: null, error: null, loading: false, refresh: vi.fn() });
    mocks.connections = [
      connection("cluster-a", "Cluster A", "CONNECTED"),
      connection("cluster-b", "Cluster B", "UNREACHABLE"),
      connection("cluster-c", "Disconnected C", "DISCONNECTED"),
    ];
    mocks.activeConnection = mocks.connections[0]!;
    mocks.search = "?window=1h&connection=cluster-a&node_id=9&cursor=old";
  });

  it("switches only the connection and observation window from the current main page", () => {
    render(<ConsoleShell><LocalStateProbe /></ConsoleShell>);
    const selector = screen.getByRole("combobox", { name: "Active connection" });
    expect(selector).toHaveValue("cluster-a");
    expect(screen.getByRole("option", { name: /Cluster B/ })).toHaveValue("cluster-b");
    expect(screen.queryByRole("option", { name: /Disconnected C/ })).not.toBeInTheDocument();

    fireEvent.change(selector, { target: { value: "cluster-b" } });
    expect(mocks.push).toHaveBeenCalledWith("/overview/?window=1h&connection=cluster-b");
    expect(mocks.push.mock.calls[0]?.[0]).not.toContain("node_id");
    expect(mocks.push.mock.calls[0]?.[0]).not.toContain("cursor");
  });

  it("keeps pause and interval while remounting connection-local page state", () => {
    const view = render(<ConsoleShell><LocalStateProbe /></ConsoleShell>);
    fireEvent.change(screen.getByRole("combobox", { name: "Refresh interval" }), { target: { value: "15000" } });
    fireEvent.click(screen.getByRole("button", { name: "Pause" }));
    fireEvent.click(screen.getByRole("button", { name: "Local count 0" }));
    expect(screen.getByRole("button", { name: "Local count 1" })).toBeVisible();

    mocks.activeConnection = mocks.connections[1]!;
    mocks.search = "?window=1h&connection=cluster-b";
    view.rerender(<ConsoleShell><LocalStateProbe /></ConsoleShell>);

    expect(screen.getByRole("combobox", { name: "Refresh interval" })).toHaveValue("15000");
    expect(screen.getByRole("button", { name: "Resume" })).toHaveAttribute("aria-pressed", "true");
    expect(screen.getByRole("button", { name: "Local count 0" })).toBeVisible();
  });

  it("renders the shared streamed summary without a duplicate Shell summary resource", () => {
    render(<ConsoleShell><SharedSummaryWriter /></ConsoleShell>);
    fireEvent.click(screen.getByRole("button", { name: "Publish streamed summary" }));

    expect(screen.getByRole("link", { name: "11 active alerts" })).toBeVisible();
    expect(screen.getByText("Streamed cluster", { selector: "strong" })).toBeVisible();
    expect(screen.getByText("STREAM", { exact: true })).toBeVisible();
    expect(mocks.resource).not.toHaveBeenCalledWith("/summary", expect.anything());
  });

  it("preserves the connection in brand, main navigation, and alert links", () => {
    render(<ConsoleShell><LocalStateProbe /></ConsoleShell>);
    const links = screen.getAllByRole("link").filter((link) =>
      ["/overview", "/nodes", "/topology", "/groups", "/performance", "/snapshots", "/lifecycle", "/events", "/sources"]
        .some((path) => link.getAttribute("href")?.startsWith(`${path}?`)),
    );
    expect(links.length).toBeGreaterThanOrEqual(10);
    for (const link of links) {
      expect(new URL(link.getAttribute("href")!, "http://console.local").searchParams.get("connection")).toBe("cluster-a");
    }
    expect(screen.getByRole("link", { name: "Manage connections" })).toHaveAttribute("href", "/connections");
  });
});
