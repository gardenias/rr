import { act, render, screen, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { makeMeta } from "@/test/fixtures";
import type { FeatureNode, NodeDetailData } from "./model";

const mocks = vi.hoisted(() => ({
  resource: vi.fn(),
  replace: vi.fn(),
  search: { current: "" },
}));

vi.mock("next/navigation", () => ({
  usePathname: () => "/nodes",
  useRouter: () => ({ replace: mocks.replace }),
  useSearchParams: () => new URLSearchParams(mocks.search.current),
}));
vi.mock("@/lib/use-console-resource", () => ({ useConsoleResource: mocks.resource }));

import { NodesView } from "./nodes-view";

const node: FeatureNode = {
  node_id: 1,
  status: "ONLINE",
  admin_addr: "127.0.0.1:52001",
  listen_addr: "127.0.0.1:51001",
  group_ids: [1],
  leader_group_count: 1,
  uptime_secs: 3600,
  last_success: "2026-11-16T08:00:00Z",
  version: "shop-2.0",
};

const detail: NodeDetailData = { node, groups: [] };

function resourceResult(items: FeatureNode[]) {
  return {
    data: { items, next_cursor: null },
    meta: makeMeta(),
    error: null,
    loading: false,
    refresh: vi.fn(),
  };
}

describe("NodesView drawer identity", () => {
  beforeEach(() => {
    mocks.search.current = "";
    mocks.replace.mockImplementation((href: string) => {
      mocks.search.current = href.split("?")[1] ?? "";
    });
    mocks.resource.mockReturnValue(resourceResult([node]));
    vi.stubGlobal("fetch", vi.fn().mockResolvedValue({
      ok: true,
      json: async () => ({ data: detail, meta: makeMeta() }),
    }));
  });

  afterEach(() => {
    vi.unstubAllGlobals();
  });

  it("preserves drawer DOM and local state across node data refreshes", async () => {
    const user = userEvent.setup();
    const view = render(<NodesView />);
    await user.click(screen.getByRole("button", { name: "Open node 1 details" }));
    await act(async () => {
      view.rerender(<NodesView />);
      await Promise.resolve();
    });

    const drawer = await screen.findByRole("dialog", { name: "Node 1" });
    const tablist = within(drawer).getByRole("tablist", { name: "Node 1 detail sections" });
    const panelId = within(drawer).getByRole("tabpanel").id;
    const sourceTrigger = within(drawer).getByRole("button", { name: "1 source" });
    await user.click(sourceTrigger);
    expect(sourceTrigger).toHaveAttribute("aria-expanded", "true");

    mocks.resource.mockReturnValue(resourceResult([{ ...node, version: "shop-2.1" }]));
    await act(async () => {
      view.rerender(<NodesView />);
      await Promise.resolve();
    });

    const refreshedDrawer = screen.getByRole("dialog", { name: "Node 1" });
    expect(within(refreshedDrawer).getByRole("tablist", { name: "Node 1 detail sections" })).toBe(tablist);
    expect(within(refreshedDrawer).getByRole("tabpanel")).toHaveAttribute("id", panelId);
    expect(within(refreshedDrawer).getByRole("button", { name: "1 source" })).toHaveAttribute("aria-expanded", "true");
  });
});
