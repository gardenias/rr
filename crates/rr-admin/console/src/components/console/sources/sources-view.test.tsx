import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { makeMeta } from "@/test/fixtures";
import type { SourcesData } from "@/lib/contract";
import { SourcesView } from "@/components/console/sources/sources-view";
import { RefreshProvider } from "@/lib/refresh-context";

const resourceMock = vi.hoisted(() => vi.fn());
const fetchMock = vi.hoisted(() => vi.fn());
vi.mock("@/lib/use-console-resource", () => ({ useConsoleResource: resourceMock }));

function sourcesData(): SourcesData {
  return {
    startup_mode: "PEERS",
    cluster_name: "SIMULATION",
    topology_revision: "42",
    read_only: true,
    prometheus: {
      status: "CONFIGURED",
      url: "http://prometheus:9090",
      cluster_matcher: "cluster_id=SIMULATION",
    },
    sources: [{
      node_id: 2,
      admin_addr: "127.0.0.1:52002",
      observability_addr: "127.0.0.1:53002",
      last_success: "2026-11-16T07:59:00Z",
      last_failure: "2026-11-16T08:00:00Z",
      circuit_state: "OPEN",
      sanitized_error: "scrape failed token=supersecret",
    }],
  };
}

function renderSources() {
  const refresh = vi.fn();
  resourceMock.mockReturnValue({
    data: sourcesData(),
    meta: makeMeta({
      retention_source: "CONSOLE_MEMORY",
      sources: [{
        node_id: null,
        endpoint_role: "PROMETHEUS_HISTORY",
        observed_at: null,
        age_ms: null,
        status: "UNMAPPED",
        error_code: "NODE_ID_MAPPING_AMBIGUOUS",
      }],
      capabilities: [{
        feature: "prometheus_history",
        status: "DEGRADED",
        reason: "History is unavailable until every target has one stable node_id label.",
      }],
    }),
    error: null,
    loading: false,
    refresh,
  });
  render(<RefreshProvider connectionId="cluster-a"><SourcesView /></RefreshProvider>);
  return { refresh };
}

describe("SourcesView", () => {
  beforeEach(() => {
    fetchMock.mockReset();
    vi.stubGlobal("fetch", fetchMock);
  });
  afterEach(() => vi.unstubAllGlobals());

  it("shows sanitized source failures and truthful Prometheus mapping degradation", () => {
    renderSources();

    expect(screen.getByRole("heading", { level: 1, name: "Sources" })).toBeVisible();
    expect(screen.getByText(/token=\[redacted\]/)).toBeVisible();
    expect(screen.queryByText(/supersecret/)).not.toBeInTheDocument();
    expect(screen.getByText("UNMAPPED")).toBeVisible();
    expect(screen.getByText(/stable node_id label/)).toBeVisible();
    expect(screen.getByText(/Console memory retention · this Console process only/)).toBeVisible();
    expect(screen.getByText("http://prometheus:9090")).toBeVisible();
    expect(screen.getByText("cluster_id=SIMULATION")).toBeVisible();
  });

  it("renders legacy source modes and sanitized nested errors", () => {
    const legacy = {
      mode: "EXPLICIT_ADMIN_SEEDS",
      read_only: true,
      console_version: "0.2.0",
      generation: 9,
      retained_observation_points: 12,
      sources: [{
        node_id: 3,
        admin_addr: "127.0.0.1:52003",
        observability_addr: null,
        last_success: "2026-11-16T07:59:00Z",
        last_attempt: "2026-11-16T08:00:00Z",
        error: { code: "UNAVAILABLE", message: "dial failed token=legacysecret" },
        capabilities: [],
      }],
      prometheus: { status: "UNSUPPORTED", url: null },
    } as unknown as SourcesData;
    resourceMock.mockReturnValue({ data: legacy, meta: makeMeta(), error: null, loading: false, refresh: vi.fn() });
    render(<RefreshProvider connectionId="cluster-a"><SourcesView /></RefreshProvider>);

    expect(screen.getByText("EXPLICIT_ADMIN_SEEDS")).toBeVisible();
    expect(screen.getByText("0.2.0 · generation 9")).toBeVisible();
    expect(screen.getByText(/UNAVAILABLE: dial failed token=\[redacted\]/)).toBeVisible();
    expect(screen.queryByText(/legacysecret/)).not.toBeInTheDocument();
    expect(screen.getByText("UNKNOWN")).toBeVisible();
  });

  it("refreshes source evidence after a successful reconcile", async () => {
    const user = userEvent.setup();
    const { refresh } = renderSources();
    fetchMock.mockImplementation((url: string) => Promise.resolve(new Response(
      url === "/api/v1/console/auth/csrf" ? JSON.stringify({ csrf_token: "test-csrf-token" }) : "{}",
      { status: 200, headers: { "Content-Type": "application/json" } },
    )));

    await user.click(screen.getByRole("button", { name: "Reconcile now" }));
    expect(await screen.findByText(/Reconciliation completed/)).toBeVisible();
    expect(refresh).toHaveBeenCalledOnce();
  });

  it("offers reauthentication when reconcile is forbidden", async () => {
    window.history.replaceState({}, "", "/sources/?window=1h&connection=cluster-a");
    const user = userEvent.setup();
    renderSources();
    fetchMock.mockResolvedValue(new Response(JSON.stringify({
      meta: { correlation_id: "corr-403" },
      error: { code: "FORBIDDEN", message: "CSRF validation failed" },
    }), { status: 403, headers: { "Content-Type": "application/json" } }));

    await user.click(screen.getByRole("button", { name: "Reconcile now" }));
    expect(await screen.findByText(/Reauthentication required/)).toBeVisible();
    expect(screen.getByRole("link", { name: "Reauthenticate" })).toHaveAttribute("href", expect.stringContaining("/login"));
    expect(decodeURIComponent(screen.getByRole("link", { name: "Reauthenticate" }).getAttribute("href")!)).toContain("connection=cluster-a");
    expect(screen.queryByText(/Reconciliation completed/)).not.toBeInTheDocument();
  });
});
