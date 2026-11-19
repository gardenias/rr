import { act, render, screen, waitFor } from "@testing-library/react";
import { RefreshProvider, useRefresh } from "@/lib/refresh-context";
import { useConsoleStream } from "@/lib/use-console-stream";
import { makeMeta } from "@/test/fixtures";

const mocks = vi.hoisted(() => ({ connectionGet: vi.fn() }));
vi.mock("@/lib/connections", async (importOriginal) => {
  const actual = await importOriginal<typeof import("@/lib/connections")>();
  return { ...actual, connectionsApi: { ...actual.connectionsApi, get: mocks.connectionGet } };
});

function deferred<T>() {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>((resolvePromise) => {
    resolve = resolvePromise;
  });
  return { promise, resolve };
}

class MockEventSource {
  static instances: MockEventSource[] = [];

  onopen: ((event: Event) => void) | null = null;
  onmessage: ((event: MessageEvent) => void) | null = null;
  onerror: ((event: Event) => void) | null = null;
  closed = false;
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

  close() {
    this.closed = true;
  }

  open() {
    this.onopen?.(new Event("open"));
  }

  error() {
    this.onerror?.(new Event("error"));
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
  const { summary, streamState } = useRefresh();
  return (
    <>
      <output aria-label="Active alerts">{summary?.data.active_alerts ?? "—"}</output>
      <output aria-label="Generated at">{summary?.meta.generated_at ?? ""}</output>
      <output aria-label="Stream state">{streamState}</output>
    </>
  );
}

function renderStream(connectionId: string | null) {
  return (
    <RefreshProvider connectionId={connectionId}>
      <StreamedSummary />
    </RefreshProvider>
  );
}

async function flushRequests() {
  await act(async () => {
    await Promise.resolve();
    await Promise.resolve();
    await Promise.resolve();
  });
}

describe("browser console stream", () => {
  beforeEach(() => {
    MockEventSource.instances = [];
    mocks.connectionGet.mockReset();
    mocks.connectionGet.mockResolvedValue({ data: { id: "cluster-a", status: "CONNECTED" } });
    vi.stubGlobal("EventSource", MockEventSource);
  });

  afterEach(() => {
    vi.unstubAllGlobals();
    vi.useRealTimers();
  });

  it("applies a named summary envelope to the shared summary state", async () => {
    render(renderStream("cluster-a"));
    expect(screen.getByLabelText("Active alerts")).toHaveTextContent("—");
    expect(MockEventSource.instances).toHaveLength(1);
    const source = MockEventSource.instances[0]!;
    expect(source.url).toBe("/api/v1/console/connections/cluster-a/stream");
    act(() => source.open());

    act(() => source.emit("summary", summaryEnvelope(7, "2026-11-16T08:00:05Z")));

    await waitFor(() => expect(screen.getByLabelText("Active alerts")).toHaveTextContent("7"));
    expect(screen.getByLabelText("Generated at")).toHaveTextContent("2026-11-16T08:00:05Z");
  });

  it("reconciles a named resync with one scoped summary GET and no additional stream", async () => {
    const fetchMock = vi.fn().mockResolvedValue(new Response(
      JSON.stringify(summaryEnvelope(9, "2026-11-16T08:00:10Z")),
      { status: 200, headers: { "content-type": "application/json" } },
    ));
    vi.stubGlobal("fetch", fetchMock);
    render(renderStream("cluster-a"));
    const source = MockEventSource.instances[0]!;
    act(() => source.open());

    act(() => source.emit("resync", { reason: "LAGGED", skipped: 3 }));

    await waitFor(() => expect(fetchMock).toHaveBeenCalledTimes(1));
    expect(fetchMock).toHaveBeenNthCalledWith(
      1,
      "/api/v1/console/connections/cluster-a/summary?window=15m",
      expect.objectContaining({ method: "GET", credentials: "same-origin", signal: expect.any(AbortSignal) }),
    );
    await waitFor(() => expect(screen.getByLabelText("Active alerts")).toHaveTextContent("9"));
    expect(MockEventSource.instances).toHaveLength(1);
  });

  it("closes A, clears its summary immediately, and ignores late A events after switching to B", async () => {
    const view = render(renderStream("cluster-a"));
    const sourceA = MockEventSource.instances[0]!;
    act(() => {
      sourceA.open();
      sourceA.emit("summary", summaryEnvelope(7, "2026-11-16T08:00:05Z"));
    });
    expect(screen.getByLabelText("Active alerts")).toHaveTextContent("7");

    view.rerender(renderStream("cluster-b"));
    expect(sourceA.closed).toBe(true);
    expect(screen.getByLabelText("Active alerts")).toHaveTextContent("—");
    expect(MockEventSource.instances).toHaveLength(2);
    const sourceB = MockEventSource.instances[1]!;
    expect(sourceB.url).toBe("/api/v1/console/connections/cluster-b/stream");

    act(() => sourceA.emit("summary", summaryEnvelope(99, "2026-11-16T08:00:59Z")));
    expect(screen.getByLabelText("Active alerts")).toHaveTextContent("—");
    act(() => sourceB.emit("summary", summaryEnvelope(4, "2026-11-16T08:01:00Z")));
    expect(screen.getByLabelText("Active alerts")).toHaveTextContent("4");
  });

  it("cancels A's pending reconnect when switching to B", async () => {
    vi.useFakeTimers();
    const view = render(renderStream("cluster-a"));
    const sourceA = MockEventSource.instances[0]!;
    act(() => sourceA.error());
    await flushRequests();
    expect(sourceA.closed).toBe(true);

    view.rerender(renderStream("cluster-b"));
    const sourceB = MockEventSource.instances[1]!;
    await act(async () => {
      vi.advanceTimersByTime(20_000);
      await Promise.resolve();
    });

    expect(MockEventSource.instances).toHaveLength(2);
    expect(sourceA.closed).toBe(true);
    expect(sourceB.url).toBe("/api/v1/console/connections/cluster-b/stream");
  });

  it("stops resync recovery after CONNECTION_NOT_CONNECTED", async () => {
    vi.useFakeTimers();
    const fetchMock = vi.fn().mockResolvedValue(new Response(JSON.stringify({
      meta: makeMeta(),
      error: { code: "CONNECTION_NOT_CONNECTED", message: "Connection is disconnected" },
    }), { status: 409, headers: { "content-type": "application/json" } }));
    vi.stubGlobal("fetch", fetchMock);
    render(renderStream("cluster-a"));
    const source = MockEventSource.instances[0]!;

    act(() => source.emit("resync", { reason: "LAGGED", skipped: 1 }));
    await flushRequests();
    expect(screen.getByLabelText("Stream state")).toHaveTextContent("STOPPED");
    expect(source.closed).toBe(true);

    await act(async () => {
      vi.advanceTimersByTime(20_000);
      await Promise.resolve();
    });
    expect(MockEventSource.instances).toHaveLength(1);
    expect(fetchMock).toHaveBeenCalledTimes(1);
  });

  it("aborts A resync and ignores its late summary after switching to B", async () => {
    const pending = deferred<Response>();
    const fetchMock = vi.fn().mockReturnValue(pending.promise);
    vi.stubGlobal("fetch", fetchMock);
    const view = render(renderStream("cluster-a"));
    const sourceA = MockEventSource.instances[0]!;
    act(() => sourceA.emit("resync", { reason: "LAGGED", skipped: 2 }));
    expect(fetchMock).toHaveBeenCalledTimes(1);
    const signal = fetchMock.mock.calls[0]?.[1]?.signal as AbortSignal;

    view.rerender(renderStream("cluster-b"));
    expect(signal.aborted).toBe(true);
    await act(async () => {
      pending.resolve(new Response(JSON.stringify(summaryEnvelope(88, "2026-11-16T08:02:00Z")), {
        status: 200,
        headers: { "content-type": "application/json" },
      }));
      await Promise.resolve();
    });
    expect(screen.getByLabelText("Active alerts")).toHaveTextContent("—");
    expect(sourceA.closed).toBe(true);
  });

  it("stops EventSource recovery when the control plane reports DISCONNECTED", async () => {
    vi.useFakeTimers();
    mocks.connectionGet.mockResolvedValue({ data: { id: "cluster-a", status: "DISCONNECTED" } });
    render(renderStream("cluster-a"));
    const source = MockEventSource.instances[0]!;
    act(() => source.error());
    await flushRequests();
    expect(screen.getByLabelText("Stream state")).toHaveTextContent("STOPPED");

    await act(async () => {
      vi.advanceTimersByTime(20_000);
      await Promise.resolve();
    });
    expect(MockEventSource.instances).toHaveLength(1);
    expect(source.closed).toBe(true);
  });

  it("uses the bounded reconnect delay while a connection is UNREACHABLE", async () => {
    vi.useFakeTimers();
    mocks.connectionGet.mockResolvedValue({ data: { id: "cluster-a", status: "UNREACHABLE" } });
    render(renderStream("cluster-a"));
    act(() => MockEventSource.instances[0]!.error());
    await flushRequests();
    expect(screen.getByLabelText("Stream state")).toHaveTextContent("RECONNECTING");

    await act(async () => {
      vi.advanceTimersByTime(999);
      await Promise.resolve();
    });
    expect(MockEventSource.instances).toHaveLength(1);
    await act(async () => {
      vi.advanceTimersByTime(1);
      await Promise.resolve();
    });
    expect(MockEventSource.instances).toHaveLength(2);
    expect(MockEventSource.instances[1]!.url).toBe("/api/v1/console/connections/cluster-a/stream");
  });

  it("invalidates a pending resync when source error starts recovery", async () => {
    const pendingResync = deferred<Response>();
    const pendingClassification = deferred<never>();
    const fetchMock = vi.fn().mockReturnValue(pendingResync.promise);
    mocks.connectionGet.mockReturnValue(pendingClassification.promise);
    vi.stubGlobal("fetch", fetchMock);
    render(renderStream("cluster-a"));
    const source = MockEventSource.instances[0]!;
    act(() => {
      source.open();
      source.emit("summary", summaryEnvelope(2, "2026-11-16T08:00:00Z"));
      source.emit("resync", { reason: "LAGGED", skipped: 1 });
      source.error();
    });
    expect(source.closed).toBe(true);
    expect(screen.getByLabelText("Stream state")).toHaveTextContent("RECONNECTING");

    await act(async () => {
      pendingResync.resolve(new Response(JSON.stringify(summaryEnvelope(99, "2026-11-16T08:03:00Z")), {
        status: 200,
        headers: { "content-type": "application/json" },
      }));
      await Promise.resolve();
    });
    expect(screen.getByLabelText("Stream state")).toHaveTextContent("RECONNECTING");
    expect(screen.getByLabelText("Active alerts")).toHaveTextContent("2");
  });

  it("times out a pending classification and still arms bounded reconnect", async () => {
    vi.useFakeTimers();
    const pendingClassification = deferred<never>();
    let classificationSignal: AbortSignal | undefined;
    mocks.connectionGet.mockImplementation((_id: string, signal: AbortSignal) => {
      classificationSignal = signal;
      return pendingClassification.promise;
    });
    render(renderStream("cluster-a"));
    act(() => MockEventSource.instances[0]!.error());
    await flushRequests();
    expect(classificationSignal?.aborted).toBe(false);

    await act(async () => {
      vi.advanceTimersByTime(2_000);
      await Promise.resolve();
    });
    expect(classificationSignal?.aborted).toBe(true);
    expect(screen.getByLabelText("Stream state")).toHaveTextContent("RECONNECTING");
    expect(MockEventSource.instances).toHaveLength(1);

    await act(async () => {
      vi.advanceTimersByTime(999);
      await Promise.resolve();
    });
    expect(MockEventSource.instances).toHaveLength(1);
    await act(async () => {
      vi.advanceTimersByTime(1);
      await Promise.resolve();
    });
    expect(MockEventSource.instances).toHaveLength(2);
    expect(MockEventSource.instances[1]!.url).toBe("/api/v1/console/connections/cluster-a/stream");
  });

  it("does not create a stream without an active connection", () => {
    render(renderStream(null));
    expect(MockEventSource.instances).toHaveLength(0);
    expect(screen.getByLabelText("Stream state")).toHaveTextContent("STOPPED");
  });
});
