import { expect, test } from "@playwright/test";
import { bootPage, nodeDetail, nodePerformance, nodesPage } from "./nodes.fixtures.mjs";
import { CLUSTER_A, clusterUrl, seedConnection } from "./connection";

test.beforeEach(async ({ page }) => {
  const response = await page.request.post("/api/v1/console/auth/login", { data: { token: "mock-viewer-token" } });
  expect(response.ok()).toBe(true);
  await seedConnection(page, CLUSTER_A);
  await page.route("**/api/v1/console/connections/cluster-a/nodes?*", (route) => route.fulfill({ json: nodesPage }));
  await page.route(/\/api\/v1\/console\/connections\/cluster-a\/nodes\/\d+$/, (route) => {
    const nodeId = Number(new URL(route.request().url()).pathname.split("/").at(-1));
    return route.fulfill({ json: nodeDetail(nodeId) });
  });
  await page.route(/\/api\/v1\/console\/connections\/cluster-a\/nodes\/\d+\/boots/, (route) => {
    const cursor = new URL(route.request().url()).searchParams.get("cursor");
    return route.fulfill({ json: bootPage(cursor) });
  });
  await page.route("**/api/v1/console/connections/cluster-a/metrics/performance?*", (route) => route.fulfill({ json: nodePerformance }));
});

test("filters nodes and preserves the selected filters in the URL", async ({ page }) => {
  await page.goto(clusterUrl("/nodes/?window=1h", CLUSTER_A));
  await page.getByLabel("Node status").selectOption("UNREACHABLE");
  await expect(page).toHaveURL(/window=1h.*connection=cluster-a.*status=UNREACHABLE/);
  await expect(page.getByRole("button", { name: "Open node 3 details" })).toBeVisible();
  await expect(page.getByRole("button", { name: "Open node 1 details" })).toHaveCount(0);
  await page.getByRole("button", { name: "Clear filters" }).click();
  await expect(page.getByRole("button", { name: "Open node 1 details" })).toBeVisible();
});

test("node drawer exposes all five tabs, resource scope, safe config, and boot pagination", async ({ page }) => {
  await page.goto(clusterUrl("/nodes/", CLUSTER_A));
  await page.getByRole("button", { name: "Open node 1 details" }).click();
  const drawer = page.getByRole("dialog", { name: "Node 1" });
  await expect(drawer).toBeVisible();
  for (const tab of ["Overview", "Resources", "Raft", "Configuration", "Restarts & versions"]) {
    await expect(drawer.getByRole("tab", { name: tab })).toBeVisible();
  }
  await drawer.getByRole("tab", { name: "Resources" }).click();
  await expect(drawer.getByText("42%")).toBeVisible();
  await expect(drawer.getByText("CGROUP").first()).toBeVisible();
  await drawer.getByRole("tab", { name: "Raft" }).click();
  await expect(drawer.getByText("Non-leader source; matched and lag unavailable")).toBeVisible();
  await drawer.getByRole("tab", { name: "Configuration" }).click();
  await expect(drawer.getByText("rocksdb")).toBeVisible();
  await expect(drawer.getByText("must-not-render")).toHaveCount(0);
  await drawer.getByRole("tab", { name: "Restarts & versions" }).click();
  await expect(drawer.getByText("boot-current")).toBeVisible();
  await drawer.getByRole("button", { name: "Older boots" }).click();
  await expect(drawer.getByText("boot-previous")).toBeVisible();
  await expect(drawer.getByText("Abnormal or uncertain").first()).toBeVisible();
  await expect(drawer.getByText(/crash/i)).toHaveCount(0);
  await page.keyboard.press("Escape");
  await expect(drawer).toHaveCount(0);
  await expect(page.getByRole("button", { name: "Open node 1 details" })).toBeFocused();
});

test("deep-linked resources open on small screens without page overflow", async ({ page }) => {
  await page.setViewportSize({ width: 390, height: 844 });
  await page.goto(clusterUrl("/nodes/?window=15m&node_id=1&tab=resources", CLUSTER_A));
  await expect(page.getByRole("dialog", { name: "Node 1" }).getByRole("tab", { name: "Resources" })).toHaveAttribute("aria-selected", "true");
  expect(await page.evaluate(() => document.documentElement.scrollWidth > window.innerWidth)).toBe(false);
});
