import { act, render, screen, waitFor } from "@testing-library/react";
import { RefreshProvider, useRefresh } from "@/lib/refresh-context";
import { useConsoleStream } from "@/lib/use-console-stream";
import { makeMeta } from "@/test/fixtures";

class MockEventSource {
  static instances: MockEventSource[] = [];

  onopen: ((event: Event) => void) | null = null;
  onmessage: ((event: MessageEvent) => void) | null = null;
  onerror: ((event: Event) => void) | null = null;
  private listeners = new Map<string, Set<(event: MessageEvent) => void>>();

  constructor(readonly url: string) {
    MockEventSource.instances.push(this);
  }

  addEventListener(type: string, listener: (event: MessageEvent) => void) {
    const listeners = this.listeners.get(type) ?? new Set();
    listeners.add(listener);
    this.listeners.set(type, listeners);
  }

  removeEventListener(type: string, listener: (event: MessageEvent) => void) {
    this.listeners.get(type)?.delete(listener);
  }

  close() {}

  open() {
    this.onopen?.(new Event("open"));
  }

  emit(type: string, payload: unknown) {
    const event = new MessageEvent(type, { data: JSON.stringify(payload) });
    if (type === "message") this.onmessage?.(event);
    for (const listener of this.listeners.get(type) ?? []) listener(event);
  }
}

function summaryEnvelope(activeAlerts: number | null, generatedAt: string) {
  return {
    meta: makeMeta({ generated_at: generatedAt }),
    data: {
      nodes: [],
      groups: [],
      cluster_name: "cluster-test",
      environment: "test",
      nodes_online: 3,
      nodes_total: 3,
      groups_with_leader: 2,
      groups_total: 2,
      quorum_risk_groups: 0,
      active_alerts: activeAlerts,
      observation_coverage: 100,
    },
  };
}

function StreamedSummary() {
  useConsoleStream();
  const { summary } = useRefresh();
  return (
    <>
      <output aria-label="Active alerts">{summary?.data.active_alerts ?? "—"}</output>
      <output aria-label="Generated at">{summary?.meta.generated_at ?? ""}</output>
    </>
  );
}

describe("browser console stream", () => {
  beforeEach(() => {
    MockEventSource.instances = [];
    vi.stubGlobal("EventSource", MockEventSource);
  });

  afterEach(() => {
    vi.unstubAllGlobals();
  });

  it("applies a named summary envelope to the shared summary state", async () => {
    render(<RefreshProvider><StreamedSummary /></RefreshProvider>);
    expect(screen.getByLabelText("Active alerts")).toHaveTextContent("—");
    expect(MockEventSource.instances).toHaveLength(1);
    const source = MockEventSource.instances[0]!;
    act(() => source.open());

    act(() => source.emit("summary", summaryEnvelope(7, "2026-11-16T08:00:05Z")));

    await waitFor(() => expect(screen.getByLabelText("Active alerts")).toHaveTextContent("7"));
    expect(screen.getByLabelText("Generated at")).toHaveTextContent("2026-11-16T08:00:05Z");
  });

  it("reconciles a named resync with one full summary GET and no additional stream", async () => {
    const fetchMock = vi.fn().mockResolvedValue(new Response(
      JSON.stringify(summaryEnvelope(9, "2026-11-16T08:00:10Z")),
      { status: 200, headers: { "content-type": "application/json" } },
    ));
    vi.stubGlobal("fetch", fetchMock);
    render(<RefreshProvider><StreamedSummary /></RefreshProvider>);
    expect(screen.getByLabelText("Active alerts")).toHaveTextContent("—");
    expect(MockEventSource.instances).toHaveLength(1);
    const source = MockEventSource.instances[0]!;
    act(() => source.open());

    act(() => source.emit("resync", { reason: "LAGGED", skipped: 3 }));

    await waitFor(() => expect(fetchMock).toHaveBeenCalledTimes(1));
    expect(fetchMock).toHaveBeenNthCalledWith(
      1,
      "/api/v1/console/summary?window=15m",
      expect.objectContaining({ method: "GET", credentials: "same-origin" }),
    );
    await waitFor(() => expect(screen.getByLabelText("Active alerts")).toHaveTextContent("9"));
    expect(MockEventSource.instances).toHaveLength(1);
  });
});
