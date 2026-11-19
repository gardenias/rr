import { expect, test, type Page } from "@playwright/test";
import { nodesTopologyResponse, raftTopologyResponse, routesTopologyResponse } from "./topology.fixtures.mjs";
import { CLUSTER_A, clusterUrl, seedConnection } from "./connection";

async function mockTopology(page: Page) {
  await page.route("**/api/v1/console/connections/cluster-a/topology*", async (route) => {
    const view = new URL(route.request().url()).searchParams.get("view") ?? "nodes";
    const json = view === "raft" ? raftTopologyResponse : view === "routes" ? routesTopologyResponse : nodesTopologyResponse;
    await route.fulfill({ json });
  });
}

test.beforeEach(async ({ page }) => {
  const response = await page.request.post("/api/v1/console/auth/login", {
    data: { token: "mock-viewer-token" },
  });
  expect(response.ok()).toBe(true);
  await seedConnection(page, CLUSTER_A);
  await mockTopology(page);
});

test("switches node topology to equivalent accessible tables without page overflow", async ({ page }) => {
  const consoleErrors: string[] = [];
  page.on("console", (message) => {
    if (message.type() === "error") consoleErrors.push(message.text());
  });
  page.on("pageerror", (error) => consoleErrors.push(error.message));
  await page.setViewportSize({ width: 1280, height: 800 });
  await page.goto(clusterUrl("/topology/?window=15m&view=nodes", CLUSTER_A));

  await expect(page.getByRole("group", { name: "Node topology graph" })).toBeVisible();
  await expect(page.getByText("Partial", { exact: true })).toBeVisible();
  await expect(page.getByText(/Stale ·/)).toBeVisible();
  await page.getByRole("tab", { name: "Graph", exact: true }).focus();
  await page.keyboard.press("ArrowRight");
  await expect(page.getByRole("table", { name: "Node topology" })).toBeVisible();
  await expect(page.getByRole("table", { name: "Observed AppendEntries relationships" })).toBeVisible();
  await expect(page.getByText("Near 5m AppendEntries · 2 groups")).toBeVisible();
  await expect(page.getByText(/not physical-network claims/)).toBeVisible();
  expect(await page.evaluate(() => document.documentElement.scrollWidth > window.innerWidth)).toBe(false);
  expect(consoleErrors).toEqual([]);
});

test("bounds Raft graphs to one selected group and falls back for a large group", async ({ page }) => {
  await page.goto(clusterUrl("/topology/?window=15m&view=raft&group_id=1", CLUSTER_A));
  await expect(page.getByRole("group", { name: "Raft topology graph for group 1" })).toBeVisible();
  const edge = page.getByRole("button", { name: "Connection Group 1 replication to Node 2" });
  await edge.focus();
  await page.keyboard.press("Enter");
  await expect(page.locator("p").filter({ hasText: /Lag 3 entries/ })).toBeVisible();
  await page.getByRole("tab", { name: "Data table", exact: true }).click();
  await expect(page.getByRole("table", { name: "Raft topology for group 1" })).toBeVisible();

  await page.getByRole("combobox", { name: "Raft group" }).selectOption("99");
  await expect(page).toHaveURL(/group_id=99/);
  await expect(page.getByText(/41 replicas exceed the 40-replica graph budget/)).toBeVisible();
  await expect(page.getByRole("table", { name: "Raft topology for group 99" })).toBeVisible();
  await expect(page.getByRole("group", { name: "Raft topology graph for group 99" })).toHaveCount(0);

  await page.getByRole("combobox", { name: "Raft group" }).selectOption("4");
  await page.getByRole("tab", { name: "Graph", exact: true }).click();
  await expect(page.getByText("No leader was observed, so no replication arrows are drawn.")).toBeVisible();
});

test("shows stale and missing routes with group and partition deep links", async ({ page }) => {
  await page.goto(clusterUrl("/topology/?window=15m&view=routes", CLUSTER_A));
  await expect(page.getByRole("table", { name: "Partition and route topology" })).toBeVisible();
  await expect(page.getByText("Stale route")).toBeVisible();
  await expect(page.getByText("Route not found")).toBeVisible();
  await expect(page.getByText("Revision 41")).toBeVisible();
  await expect(page.getByRole("link", { name: "Group 1" })).toHaveAttribute("href", /\/groups\/?\?window=15m&group_id=1&connection=cluster-a/);

  await page.goto(clusterUrl("/topology/?window=15m&view=routes&filter=group-2", CLUSTER_A));
  await expect(page.getByRole("link", { name: "Partition 11" })).toBeVisible();
  await expect(page.getByRole("link", { name: "Partition 10" })).toHaveCount(0);
});
