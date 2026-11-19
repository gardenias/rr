import { act, fireEvent, render, screen } from "@testing-library/react";
import { RefreshProvider } from "@/lib/refresh-context";
import { useConsoleResource } from "@/lib/use-console-resource";
import { makeMeta } from "@/test/fixtures";

function deferred<T>() {
  let resolve!: (value: T) => void;
  let reject!: (reason: unknown) => void;
  const promise = new Promise<T>((resolvePromise, rejectPromise) => {
    resolve = resolvePromise;
    reject = rejectPromise;
  });
  return { promise, resolve, reject };
}

function nodesResponse(owner: string) {
  return new Response(JSON.stringify({
    meta: makeMeta({ generated_at: `2026-11-19T08:00:0${owner === "cluster-a" ? "0" : "1"}Z` }),
    data: { items: [], next_cursor: owner },
  }), { status: 200, headers: { "content-type": "application/json" } });
}

function ResourceProbe() {
  const { data, loading } = useConsoleResource("/nodes", { page_size: 50 });
  return <output aria-label="Resource owner">{loading ? "loading" : data?.next_cursor ?? "empty"}</output>;
}

function RefreshableProbe() {
  const { data, error, refresh } = useConsoleResource("/nodes", { page_size: 50 });
  return (
    <>
      <button type="button" onClick={refresh}>Reload resource</button>
      <output aria-label="Resource owner">{data?.next_cursor ?? "empty"}</output>
      <output aria-label="Resource error">{error?.code ?? ""}</output>
    </>
  );
}

function renderResource(connectionId: string | null) {
  return (
    <RefreshProvider connectionId={connectionId}>
      <ResourceProbe />
    </RefreshProvider>
  );
}

describe("connection-scoped console resources", () => {
  afterEach(() => {
    vi.unstubAllGlobals();
    vi.useRealTimers();
  });

  it("aborts the old request and ignores its late response after A switches to B", async () => {
    const first = deferred<Response>();
    const second = deferred<Response>();
    const signals: AbortSignal[] = [];
    const fetchMock = vi.fn().mockImplementation((_url: string, init: RequestInit) => {
      signals.push(init.signal as AbortSignal);
      return signals.length === 1 ? first.promise : second.promise;
    });
    vi.stubGlobal("fetch", fetchMock);

    const view = render(renderResource("cluster-a"));
    expect(fetchMock).toHaveBeenCalledWith(
      "/api/v1/console/connections/cluster-a/nodes?page_size=50",
      expect.objectContaining({ signal: signals[0] }),
    );

    view.rerender(renderResource("cluster-b"));
    expect(signals[0]?.aborted).toBe(true);
    expect(screen.getByLabelText("Resource owner")).toHaveTextContent("loading");

    await act(async () => {
      first.resolve(nodesResponse("cluster-a"));
      await Promise.resolve();
    });
    expect(screen.getByLabelText("Resource owner")).not.toHaveTextContent("cluster-a");

    await act(async () => {
      second.resolve(nodesResponse("cluster-b"));
      await Promise.resolve();
    });
    expect(screen.getByLabelText("Resource owner")).toHaveTextContent("cluster-b");
  });

  it("never renders last-good A data while B is loading", async () => {
    const pendingB = deferred<Response>();
    const fetchMock = vi.fn()
      .mockResolvedValueOnce(nodesResponse("cluster-a"))
      .mockReturnValueOnce(pendingB.promise);
    vi.stubGlobal("fetch", fetchMock);

    const view = render(renderResource("cluster-a"));
    expect(await screen.findByText("cluster-a")).toBeVisible();

    view.rerender(renderResource("cluster-b"));
    expect(screen.getByLabelText("Resource owner")).toHaveTextContent("loading");
    expect(screen.queryByText("cluster-a")).not.toBeInTheDocument();
  });

  it("does not issue requests without an active connection", () => {
    const fetchMock = vi.fn();
    vi.stubGlobal("fetch", fetchMock);
    render(renderResource(null));
    expect(fetchMock).not.toHaveBeenCalled();
    expect(screen.getByLabelText("Resource owner")).toHaveTextContent("empty");
  });

  it("retains last-good data for a transient error within the same connection", async () => {
    const fetchMock = vi.fn()
      .mockResolvedValueOnce(nodesResponse("cluster-a"))
      .mockResolvedValueOnce(new Response(JSON.stringify({
        meta: makeMeta(),
        error: { code: "UPSTREAM_UNAVAILABLE", message: "Admin endpoints are temporarily unreachable" },
      }), { status: 503, headers: { "content-type": "application/json" } }));
    vi.stubGlobal("fetch", fetchMock);
    render(<RefreshProvider connectionId="cluster-a"><RefreshableProbe /></RefreshProvider>);
    expect(await screen.findByText("cluster-a")).toBeVisible();

    fireEvent.click(screen.getByRole("button", { name: "Reload resource" }));
    expect(await screen.findByText("UPSTREAM_UNAVAILABLE")).toBeVisible();
    expect(screen.getByLabelText("Resource owner")).toHaveTextContent("cluster-a");
  });

  it.each(["CONNECTION_NOT_CONNECTED", "CONNECTION_NOT_FOUND"])("stops polling after %s", async (code) => {
    vi.useFakeTimers();
    const fetchMock = vi.fn().mockResolvedValue(new Response(JSON.stringify({
      meta: makeMeta(),
      error: { code, message: "Connection is unavailable for observation" },
    }), { status: 409, headers: { "content-type": "application/json" } }));
    vi.stubGlobal("fetch", fetchMock);

    render(renderResource("cluster-a"));
    await act(async () => {
      await Promise.resolve();
      await Promise.resolve();
    });
    expect(fetchMock).toHaveBeenCalledTimes(1);

    await act(async () => {
      vi.advanceTimersByTime(20_000);
      await Promise.resolve();
    });
    expect(fetchMock).toHaveBeenCalledTimes(1);
  });
});
