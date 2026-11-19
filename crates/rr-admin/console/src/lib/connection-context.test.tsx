import { act, renderHook, waitFor } from "@testing-library/react";
import type { PropsWithChildren } from "react";
import { makeMeta } from "@/test/fixtures";
import type { ConnectionStatus, ConnectionSummary } from "@/lib/connections";
import { ConnectionProvider, useActiveConnection } from "@/lib/connection-context";

const mocks = vi.hoisted(() => ({ search: { current: "" } }));
vi.mock("next/navigation", () => ({
  useSearchParams: () => new URLSearchParams(mocks.search.current),
}));

function profile(id: string, status: ConnectionStatus = "CONNECTED"): ConnectionSummary {
  return {
    id,
    name: id,
    environment: null,
    admin_seeds: [{ node_id: 1, admin_addr: "127.0.0.1:52001" }],
    discovery: null,
    cluster_name: id,
    admin_tls: { ca: null, cert: null, key: null, domain: null },
    discovery_tls: { ca: null, cert: null, key: null, domain: null },
    discovery_username: null,
    discovery_allow_insecure: false,
    prometheus: null,
    poll_interval_secs: 5,
    created_at: "2026-11-18T08:00:00Z",
    updated_at: "2026-11-18T08:00:00Z",
    observed_cluster_id: id,
    last_connected_at: null,
    source: "PROFILE",
    status,
    credentials_configured: true,
    credential_source: "MEMORY",
    last_error: null,
    node_count: status === "DISCONNECTED" ? null : 1,
    group_count: status === "DISCONNECTED" ? null : 1,
  };
}

function listResponse(items: ConnectionSummary[]): Response {
  return new Response(JSON.stringify({
    meta: makeMeta({ cluster_id: null, retention_source: "NONE", sources: [], capabilities: [] }),
    data: { items, keychain_available: true },
  }), { status: 200, headers: { "content-type": "application/json" } });
}

function deferred<T>() {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>((resolvePromise) => {
    resolve = resolvePromise;
  });
  return { promise, resolve };
}

function Wrapper({ children }: PropsWithChildren) {
  return <ConnectionProvider>{children}</ConnectionProvider>;
}

async function renderActive(search: string, responses: Response[]) {
  mocks.search.current = search;
  const fetchMock = vi.fn();
  for (const response of responses) fetchMock.mockResolvedValueOnce(response);
  vi.stubGlobal("fetch", fetchMock);
  const rendered = renderHook(() => useActiveConnection(), { wrapper: Wrapper });
  await waitFor(() => expect(rendered.result.current.loading).toBe(false));
  return { ...rendered, fetchMock };
}

describe("ConnectionProvider", () => {
  beforeEach(() => {
    localStorage.clear();
    mocks.search.current = "";
  });

  it("uses the URL connection instead of overriding it with the recent ID", async () => {
    localStorage.setItem("rr.console.recent-connection.v1", "cluster-b");
    const { result } = await renderActive("connection=cluster-a", [
      listResponse([profile("cluster-a"), profile("cluster-b")]),
    ]);

    expect(result.current.connectionId).toBe("cluster-a");
    expect(result.current.activeConnection?.id).toBe("cluster-a");
    expect(result.current.recentConnectionId).toBe("cluster-a");
  });

  it("does not select or connect the recent ID when the URL has no connection", async () => {
    localStorage.setItem("rr.console.recent-connection.v1", "cluster-a");
    const { result, fetchMock } = await renderActive("", [listResponse([profile("cluster-a")])]);

    expect(result.current.connectionId).toBeNull();
    expect(result.current.activeConnection).toBeNull();
    expect(result.current.recentConnectionId).toBe("cluster-a");
    expect(fetchMock).toHaveBeenCalledTimes(1);
    expect(fetchMock).toHaveBeenCalledWith(
      "/api/v1/console/connections",
      expect.objectContaining({ method: "GET" }),
    );
  });

  it.each(["unknown", "DISCONNECTED"] as const)("does not activate %s URL IDs", async (kind) => {
    const status = kind === "DISCONNECTED" ? "DISCONNECTED" : "CONNECTED";
    const { result, fetchMock } = await renderActive("connection=cluster-a", [
      listResponse(kind === "unknown" ? [profile("cluster-b")] : [profile("cluster-a", status)]),
    ]);

    expect(result.current.connectionId).toBe("cluster-a");
    expect(result.current.activeConnection).toBeNull();
    expect(fetchMock).toHaveBeenCalledTimes(1);
  });

  it("clears the active connection when a refresh shows that it was deleted", async () => {
    const { result } = await renderActive("connection=cluster-a", [
      listResponse([profile("cluster-a")]),
      listResponse([]),
    ]);
    expect(result.current.activeConnection?.id).toBe("cluster-a");

    await act(() => result.current.refresh());

    expect(result.current.activeConnection).toBeNull();
    expect(result.current.connectionId).toBe("cluster-a");
  });

  it.each(["CONNECTING", "DEGRADED", "UNREACHABLE"] as const)(
    "keeps an existing %s runtime active",
    async (status) => {
      const { result } = await renderActive("connection=cluster-a", [
        listResponse([profile("cluster-a", status)]),
      ]);

      expect(result.current.activeConnection?.status).toBe(status);
    },
  );

  it("does not change the URL selection when another tab updates the recent ID", async () => {
    const { result, fetchMock } = await renderActive("connection=cluster-a", [
      listResponse([profile("cluster-a"), profile("cluster-b")]),
    ]);

    act(() => {
      localStorage.setItem("rr.console.recent-connection.v1", "cluster-b");
      window.dispatchEvent(new StorageEvent("storage", {
        key: "rr.console.recent-connection.v1",
        newValue: "cluster-b",
      }));
    });

    expect(result.current.connectionId).toBe("cluster-a");
    expect(result.current.activeConnection?.id).toBe("cluster-a");
    expect(fetchMock).toHaveBeenCalledTimes(1);
  });

  it("keeps URL selection working when recent-ID storage is unavailable", async () => {
    vi.spyOn(Storage.prototype, "getItem").mockImplementation(() => {
      throw new DOMException("Storage disabled", "SecurityError");
    });

    const { result } = await renderActive("connection=cluster-a", [
      listResponse([profile("cluster-a")]),
    ]);

    expect(result.current.connectionId).toBe("cluster-a");
    expect(result.current.activeConnection?.id).toBe("cluster-a");
  });

  it("keeps the active connection when persisting its recent ID fails", async () => {
    vi.spyOn(Storage.prototype, "setItem").mockImplementation(() => {
      throw new DOMException("Storage disabled", "SecurityError");
    });

    const { result } = await renderActive("connection=cluster-a", [
      listResponse([profile("cluster-a")]),
    ]);

    expect(result.current.activeConnection?.id).toBe("cluster-a");
    expect(result.current.recentConnectionId).toBe("cluster-a");
  });

  it("keeps the current URL authoritative when the initial list arrives after A changes to B", async () => {
    const pending = deferred<Response>();
    const fetchMock = vi.fn().mockReturnValueOnce(pending.promise);
    vi.stubGlobal("fetch", fetchMock);
    mocks.search.current = "connection=cluster-a";
    const rendered = renderHook(() => useActiveConnection(), { wrapper: Wrapper });
    await waitFor(() => expect(fetchMock).toHaveBeenCalledTimes(1));

    mocks.search.current = "connection=cluster-b";
    rendered.rerender();
    expect(rendered.result.current.connectionId).toBe("cluster-b");
    expect(rendered.result.current.activeConnection).toBeNull();

    await act(async () => {
      pending.resolve(listResponse([profile("cluster-a")]));
      await pending.promise;
    });
    await waitFor(() => expect(rendered.result.current.loading).toBe(false));

    expect(rendered.result.current.connectionId).toBe("cluster-b");
    expect(rendered.result.current.activeConnection).toBeNull();
    expect(rendered.result.current.recentConnectionId).toBeNull();
    expect(rendered.result.current.connections.map(({ id }) => id)).toEqual(["cluster-a"]);
  });

  it("ignores an older refresh that completes after a newer refresh", async () => {
    const { result, fetchMock, rerender } = await renderActive("connection=cluster-a", [
      listResponse([profile("cluster-a"), profile("cluster-b")]),
    ]);
    mocks.search.current = "connection=cluster-b";
    rerender();
    expect(result.current.connectionId).toBe("cluster-b");
    expect(result.current.activeConnection?.id).toBe("cluster-b");

    const older = deferred<Response>();
    const newer = deferred<Response>();
    fetchMock.mockReturnValueOnce(older.promise).mockReturnValueOnce(newer.promise);
    let olderRefresh!: Promise<void>;
    let newerRefresh!: Promise<void>;
    act(() => {
      olderRefresh = result.current.refresh();
      newerRefresh = result.current.refresh();
    });
    expect(fetchMock).toHaveBeenCalledTimes(3);

    await act(async () => {
      newer.resolve(listResponse([profile("cluster-a")]));
      await newerRefresh;
    });
    expect(result.current.connectionId).toBe("cluster-b");
    expect(result.current.activeConnection).toBeNull();
    expect(result.current.connections.map(({ id }) => id)).toEqual(["cluster-a"]);

    await act(async () => {
      older.resolve(listResponse([profile("cluster-a"), profile("cluster-b")]));
      await olderRefresh;
    });
    expect(result.current.connectionId).toBe("cluster-b");
    expect(result.current.activeConnection).toBeNull();
    expect(result.current.connections.map(({ id }) => id)).toEqual(["cluster-a"]);
  });

});
