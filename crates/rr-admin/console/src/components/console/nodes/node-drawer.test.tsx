import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import { RefreshProvider } from "@/lib/refresh-context";
import { NodeDrawer } from "@/components/console/nodes/node-drawer";
import type { FeatureNode } from "@/components/console/nodes/model";
import { makeMeta } from "@/test/fixtures";

const node: FeatureNode = {
  node_id: 1,
  status: "ONLINE",
  admin_addr: "127.0.0.1:52001",
  listen_addr: "127.0.0.1:51001",
  group_ids: [],
  leader_group_count: 0,
  uptime_secs: 10,
  last_success: "2026-11-19T08:00:00Z",
};

function boot(id: string) {
  return {
    boot_id: id,
    started_at: "2026-11-19T08:00:00Z",
    ended_at: null,
    shutdown_state: "RUNNING",
    persistence: "PERSISTENT",
    app_version: "0.2.0",
    rr_version: "0.2.0",
    git_commit: "abc123",
  };
}

function renderDrawer(connectionId: string) {
  return (
    <RefreshProvider connectionId={connectionId}>
      <NodeDrawer node={node} open activeTab="history" onOpenChange={() => undefined} onTabChange={() => undefined} />
    </RefreshProvider>
  );
}

describe("NodeDrawer connection-local history", () => {
  afterEach(() => vi.unstubAllGlobals());

  it("aborts A detail and resets the boot cursor when switching to B", async () => {
    const requests: Array<{ url: string; signal: AbortSignal }> = [];
    const fetchMock = vi.fn().mockImplementation((url: string, init: RequestInit) => {
      requests.push({ url, signal: init.signal as AbortSignal });
      const pathname = new URL(url, "http://console.local").pathname;
      const search = new URL(url, "http://console.local").searchParams;
      const payload = pathname.endsWith("/boots")
        ? {
          items: [boot(search.get("cursor") ? "boot-previous" : "boot-current")],
          next_cursor: search.get("cursor") ? null : "boot-page-2",
        }
        : { node, groups: [] };
      return Promise.resolve(new Response(JSON.stringify({ meta: makeMeta(), data: payload }), {
        status: 200,
        headers: { "content-type": "application/json" },
      }));
    });
    vi.stubGlobal("fetch", fetchMock);

    const view = render(renderDrawer("cluster-a"));
    expect(await screen.findByText("boot-current")).toBeVisible();
    fireEvent.click(screen.getByRole("button", { name: "Older boots" }));
    expect(await screen.findByText("boot-previous")).toBeVisible();
    expect(requests.some(({ url }) => url.includes("cursor=boot-page-2"))).toBe(true);
    const detailA = requests.find(({ url }) => new URL(url, "http://console.local").pathname.endsWith("/nodes/1"))!;

    view.rerender(renderDrawer("cluster-b"));
    expect(detailA.signal.aborted).toBe(true);
    await waitFor(() => expect(requests.some(({ url }) =>
      url.startsWith("/api/v1/console/connections/cluster-b/nodes/1/boots?")
      && !url.includes("cursor="),
    )).toBe(true));
    expect(requests.some(({ url }) =>
      url.startsWith("/api/v1/console/connections/cluster-b/nodes/1/boots?")
      && url.includes("cursor=boot-page-2"),
    )).toBe(false);
  });
});
