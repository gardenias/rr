import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { makeMeta } from "@/test/fixtures";
import type { EventItem } from "@/lib/contract";
import { EventsView } from "@/components/console/events/events-view";

const resourceMock = vi.hoisted(() => vi.fn());
vi.mock("@/lib/use-console-resource", () => ({ useConsoleResource: resourceMock }));
const routeState = vi.hoisted(() => ({ connection: "cluster-a" }));
vi.mock("@/lib/use-route-query-state", () => ({
  useRouteQueryState: () => ({
    state: { window: "15m", connection: routeState.connection, filters: {} },
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
  beforeEach(() => {
    window.localStorage.clear();
    routeState.connection = "cluster-a";
  });

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

  it("partitions identical event IDs by connection", async () => {
    const user = userEvent.setup();
    const first = renderEvents();
    await user.click(screen.getByRole("button", { name: "Acknowledge event-node-1-unreachable" }));
    first.unmount();

    routeState.connection = "cluster-b";
    const second = renderEvents();
    expect(screen.queryByText("Acknowledged locally")).not.toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Acknowledge event-node-1-unreachable" })).toBeEnabled();
    await user.click(screen.getByRole("button", { name: "Acknowledge event-node-1-unreachable" }));
    second.unmount();

    routeState.connection = "cluster-a";
    renderEvents();
    expect(screen.getByText("Acknowledged locally")).toBeVisible();
    const stored = JSON.parse(window.localStorage.getItem("rr.console.event-preferences.v1") ?? "{}");
    expect(stored["cluster-a"][event.event_id].acknowledged_at).toEqual(expect.any(String));
    expect(stored["cluster-b"][event.event_id].acknowledged_at).toEqual(expect.any(String));
  });

  it("merges a preference write with changes stored after this view mounted", async () => {
    const user = userEvent.setup();
    renderEvents();
    window.localStorage.setItem("rr.console.event-preferences.v1", JSON.stringify({
      "cluster-a": {
        "external-event": { acknowledged_at: "2026-11-19T08:00:00Z" },
      },
      "cluster-b": {
        [event.event_id]: { silenced_until: "2026-11-19T10:00:00Z" },
      },
    }));

    await user.click(screen.getByRole("button", { name: "Acknowledge event-node-1-unreachable" }));
    const stored = JSON.parse(window.localStorage.getItem("rr.console.event-preferences.v1") ?? "{}");
    expect(stored["cluster-a"]["external-event"].acknowledged_at).toBe("2026-11-19T08:00:00Z");
    expect(stored["cluster-a"][event.event_id].acknowledged_at).toEqual(expect.any(String));
    expect(stored["cluster-b"][event.event_id].silenced_until).toBe("2026-11-19T10:00:00Z");
  });

  it("resets an events cursor when the connection changes", async () => {
    const user = userEvent.setup();
    resourceMock.mockReturnValue({
      data: { items: [event], next_cursor: "events-page-2" },
      meta: makeMeta(),
      error: null,
      loading: false,
      refresh: vi.fn(),
    });
    const view = render(<EventsView />);
    await user.click(screen.getByRole("button", { name: "Next page" }));
    expect(resourceMock).toHaveBeenLastCalledWith("/events", expect.objectContaining({ cursor: "events-page-2" }));

    routeState.connection = "cluster-b";
    view.rerender(<EventsView />);
    await waitFor(() => expect(resourceMock).toHaveBeenLastCalledWith("/events", expect.objectContaining({ cursor: undefined })));
  });
});
