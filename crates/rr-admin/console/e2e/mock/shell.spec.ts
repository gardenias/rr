import { expect, test } from "@playwright/test";
import { CLUSTER_A, clusterUrl, seedConnection } from "./connection";
import { envelope } from "./fixtures.mjs";

const routes = [
  ["overview", "Overview", "/api/v1/console/connections/cluster-a/summary"],
  ["nodes", "Nodes", "/api/v1/console/connections/cluster-a/nodes"],
  ["topology", "Topology", "/api/v1/console/connections/cluster-a/topology"],
  ["groups", "Raft groups", "/api/v1/console/connections/cluster-a/groups"],
  ["performance", "Performance", "/api/v1/console/connections/cluster-a/metrics/performance"],
  ["snapshots", "Snapshots & storage", "/api/v1/console/connections/cluster-a/snapshots"],
  ["lifecycle", "Lifecycle", "/api/v1/console/connections/cluster-a/lifecycle"],
  ["events", "Events & alerts", "/api/v1/console/connections/cluster-a/events"],
  ["sources", "Sources", "/api/v1/console/connections/cluster-a/sources"],
] as const;

test.beforeEach(async ({ page }) => {
  const response = await page.request.post("/api/v1/console/auth/login", {
    data: { token: "mock-viewer-token" },
  });
  expect(response.ok()).toBe(true);
  await seedConnection(page, CLUSTER_A);
});

for (const viewport of [
  { width: 1440, height: 900 },
  { width: 1280, height: 800 },
]) {
  test(`all nine exported routes work at ${viewport.width}×${viewport.height}`, async ({ page }) => {
    const consoleErrors: string[] = [];
    page.on("console", (message) => {
      if (message.type() === "error") consoleErrors.push(message.text());
    });
    page.on("pageerror", (error) => consoleErrors.push(error.message));
    await page.setViewportSize(viewport);

    for (const [route, title, resource] of routes) {
      const resourceReady = page.waitForResponse((response) =>
        new URL(response.url()).pathname === resource && response.ok(),
      );
      await page.goto(clusterUrl(`/${route}/`, CLUSTER_A));
      await resourceReady;
      await expect(page.getByRole("heading", { level: 1, name: title })).toBeVisible();
      await expect(page.getByText("Console data unavailable")).toHaveCount(0);
      await expect(page.locator("h1")).toHaveCount(1);
      const overflow = await page.evaluate(() => document.documentElement.scrollWidth > window.innerWidth);
      expect(overflow, `${route} overflows at ${viewport.width}px`).toBe(false);
    }
    expect(consoleErrors).toEqual([]);
  });
}

test("keyboard global search preserves a shareable filtered destination", async ({ page }) => {
  await page.goto(clusterUrl("/overview/", CLUSTER_A));
  await expect(page.locator(".cluster-identity strong")).toHaveText("Cluster A");
  await page.keyboard.press("Meta+K");
  await expect(page.getByRole("dialog", { name: "Search console" })).toBeVisible();
  await page.getByRole("textbox", { name: "Search query" }).fill("node-7");
  await page.getByRole("button", { name: "Find node “node-7”" }).click();
  await expect(page).toHaveURL(/\/nodes\/\?window=15m&filter=node-7&connection=cluster-a$/);
  await expect(page.getByRole("heading", { level: 1, name: "Nodes" })).toBeVisible();
});

test("pause stops polling and resume reconciles again", async ({ page }) => {
  const summaryRequests: string[] = [];
  page.on("request", (request) => {
    if (new URL(request.url()).pathname === "/api/v1/console/connections/cluster-a/summary") summaryRequests.push(request.url());
  });
  const initialSummary = page.waitForResponse((response) =>
    new URL(response.url()).pathname === "/api/v1/console/connections/cluster-a/summary" && response.ok(),
  );
  await page.goto(clusterUrl("/overview/", CLUSTER_A));
  await initialSummary;
  await expect(page.getByRole("heading", { level: 1, name: "Overview" })).toBeVisible();
  await page.getByRole("button", { name: "Pause", exact: true }).click();
  await expect(page.getByRole("button", { name: "Resume" })).toHaveAttribute("aria-pressed", "true");
  const pausedCount = summaryRequests.length;
  await page.waitForTimeout(5_500);
  expect(summaryRequests.length).toBe(pausedCount);
  await page.getByRole("button", { name: "Resume" }).click();
  await expect.poll(() => summaryRequests.length, { timeout: 7_000 }).toBeGreaterThan(pausedCount);
});

test("SSE reconnects after the mock stream drops", async ({ page }) => {
  await page.goto(clusterUrl("/overview/", CLUSTER_A));
  await expect(page.getByText("Data plane live")).toBeVisible();
  await page.request.post("/__mock/stream/disconnect");
  await expect(page.getByText("Reconnecting")).toBeVisible();
  await expect(page.getByText("Data plane live")).toBeVisible({ timeout: 10_000 });
  await page.goto("/login/");
  await expect.poll(async () => {
    const response = await page.request.get("/__mock/stream/state");
    return response.ok() ? await response.json() : null;
  }).toMatchObject({ activeStreams: 0, activeIntervals: 0 });
});

test("named SSE resync updates the visible Shell summary with one GET", async ({ page }) => {
  await page.goto(clusterUrl("/nodes/", CLUSTER_A));
  await expect(page.getByText("Data plane live")).toBeVisible();
  await expect(page.getByRole("link", { name: "2 active alerts" })).toBeVisible();
  await page.route("**/api/v1/console/connections/cluster-a/summary?*", (route) => route.fulfill({
    json: envelope("/summary", {
      nodes: [],
      groups: [],
      cluster_name: "Resync visible",
      environment: "RESYNC",
      nodes_online: 3,
      nodes_total: 3,
      groups_with_leader: 2,
      groups_total: 2,
      quorum_risk_groups: 0,
      active_alerts: 7,
      observation_coverage: 100,
    }),
  }));
  const summaryRequests: string[] = [];
  page.on("request", (request) => {
    if (new URL(request.url()).pathname === "/api/v1/console/connections/cluster-a/summary") summaryRequests.push(request.url());
  });
  const reconciliation = page.waitForResponse((response) =>
    new URL(response.url()).pathname === "/api/v1/console/connections/cluster-a/summary" && response.ok(),
    { timeout: 2_000 },
  );

  const resync = await page.request.post("/__mock/stream/resync");

  expect(resync.ok()).toBe(true);
  await reconciliation;
  await expect(page.getByRole("link", { name: "7 active alerts" })).toBeVisible();
  await expect(page.locator(".cluster-identity strong")).toHaveText("Resync visible");
  expect(summaryRequests).toHaveLength(1);
});

test("reduced motion disables shell transitions", async ({ page }) => {
  await page.emulateMedia({ reducedMotion: "reduce" });
  await page.goto(clusterUrl("/overview/", CLUSTER_A));
  const duration = await page.locator(".nav-link").first().evaluate((element) =>
    Number.parseFloat(getComputedStyle(element).transitionDuration),
  );
  expect(duration).toBeLessThanOrEqual(0.00001);
});
