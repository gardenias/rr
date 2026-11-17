import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { makeMeta } from "@/test/fixtures";
import type { EventItem } from "@/lib/contract";
import { EventsView } from "@/components/console/events/events-view";

const resourceMock = vi.hoisted(() => vi.fn());
vi.mock("@/lib/use-console-resource", () => ({ useConsoleResource: resourceMock }));
vi.mock("@/lib/use-route-query-state", () => ({
  useRouteQueryState: () => ({
    state: { window: "15m", filters: {} },
    setWindow: vi.fn(),
    setFilter: vi.fn(),
  }),
}));

const event: EventItem = {
  event_id: "event-node-1-unreachable",
  severity: "ERROR",
  object: "node-1",
  state: "ACTIVE",
  first_observed_at: "2026-11-16T07:00:00Z",
  last_observed_at: "2026-11-16T07:05:00Z",
  evidence: [{
    node_id: 1,
    endpoint_role: "ADMIN",
    observed_at: "2026-11-16T07:05:00Z",
    age_ms: 5_000,
    status: "UNREACHABLE",
    error_code: "CONNECTION_TIMEOUT",
  }],
};

function renderEvents() {
  resourceMock.mockReturnValue({
    data: { items: [event], next_cursor: null },
    meta: makeMeta({ generated_at: new Date().toISOString() }),
    error: null,
    loading: false,
    refresh: vi.fn(),
  });
  return render(<EventsView />);
}

describe("EventsView", () => {
  beforeEach(() => window.localStorage.clear());

  it("persists acknowledgement as a local preference, not a cluster mutation", async () => {
    const user = userEvent.setup();
    const first = renderEvents();
    await user.click(screen.getByRole("button", { name: "Acknowledge event-node-1-unreachable" }));

    expect(screen.getByText("Acknowledged locally")).toBeVisible();
    expect(window.localStorage.getItem("rr.console.event-preferences.v1")).toContain(event.event_id);
    first.unmount();
    renderEvents();
    expect(screen.getByText("Acknowledged locally")).toBeVisible();
    expect(screen.getByRole("button", { name: "Acknowledge event-node-1-unreachable" })).toBeDisabled();
  });

  it("silences and restores an event without deleting its observation history", async () => {
    const user = userEvent.setup();
    renderEvents();
    await user.click(screen.getByRole("button", { name: "Silence 1h: event-node-1-unreachable" }));

    expect(screen.queryByRole("table", { name: "Observed events and alerts" })).not.toBeInTheDocument();
    expect(screen.getByText(/1 event silenced by local preference/)).toBeVisible();
    await user.click(screen.getByRole("button", { name: "Show silenced events" }));
    expect(screen.getByRole("table", { name: "Observed events and alerts" })).toHaveTextContent(event.event_id);
    await user.click(screen.getByRole("button", { name: "Unsilence event-node-1-unreachable" }));
    expect(screen.getByRole("table", { name: "Observed events and alerts" })).toHaveTextContent(event.event_id);
    expect(screen.queryByText(/1 event silenced by local preference/)).not.toBeInTheDocument();
  });

  it("normalizes legacy event identifiers and preserves their messages", async () => {
    const user = userEvent.setup();
    const legacy = {
      id: "legacy-event-7",
      severity: "WARN",
      kind: "NODE_UNREACHABLE",
      node_id: 2,
      group_id: null,
      state: "ACTIVE",
      first_observed_at: "2026-11-16T07:00:00Z",
      last_observed_at: "2026-11-16T07:01:00Z",
      message: "Admin observation timed out",
    } as unknown as EventItem;
    resourceMock.mockReturnValue({
      data: { items: [legacy], next_cursor: null },
      meta: makeMeta({ generated_at: new Date().toISOString() }),
      error: null,
      loading: false,
      refresh: vi.fn(),
    });
    render(<EventsView />);

    const table = screen.getByRole("table", { name: "Observed events and alerts" });
    expect(table).toHaveTextContent("legacy-event-7");
    expect(table).toHaveTextContent("NODE_UNREACHABLE");
    expect(table).toHaveTextContent("Admin observation timed out");
    expect(table).toHaveTextContent("node-2");
    await user.click(screen.getByRole("button", { name: "Acknowledge legacy-event-7" }));
    expect(window.localStorage.getItem("rr.console.event-preferences.v1")).toContain("legacy-event-7");
  });

  it("explains that empty results do not prove cluster health when evidence is partial", () => {
    resourceMock.mockReturnValue({
      data: { items: [], next_cursor: null },
      meta: makeMeta({ partial: true }),
      error: null,
      loading: false,
      refresh: vi.fn(),
    });
    render(<EventsView />);

    expect(screen.getByText("No matching observed events")).toBeVisible();
    expect(screen.getByText(/Observation is partial; this does not prove the cluster is healthy/)).toBeVisible();
  });
});
