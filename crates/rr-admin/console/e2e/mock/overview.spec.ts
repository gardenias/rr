import { expect, test } from "@playwright/test";
import { overviewEvents, overviewPerformance, overviewSummary } from "./overview.fixtures.mjs";
import { CLUSTER_A, clusterUrl, seedConnection } from "./connection";

test.beforeEach(async ({ page }) => {
  const response = await page.request.post("/api/v1/console/auth/login", { data: { token: "mock-viewer-token" } });
  expect(response.ok()).toBe(true);
  await seedConnection(page, CLUSTER_A);
  await page.route("**/api/v1/console/connections/cluster-a/summary?*", (route) => route.fulfill({ json: overviewSummary() }));
  await page.route("**/api/v1/console/connections/cluster-a/metrics/performance?*", (route) => route.fulfill({ json: overviewPerformance }));
  await page.route("**/api/v1/console/connections/cluster-a/events?*", (route) => route.fulfill({ json: overviewEvents }));
});

test("prioritizes leaderless evidence and preserves query state in drilldowns", async ({ page }) => {
  await page.goto(clusterUrl("/overview/?window=1h&filter=zone-a", CLUSTER_A));
  await expect(page.getByRole("heading", { level: 1, name: "Overview" })).toBeVisible();
  await expect(page.getByText("Group 7 has no observed leader")).toBeVisible();
  await expect(page.getByText("67% observation coverage", { exact: true })).toBeVisible();
  await expect(page.getByText("125", { exact: true })).toBeVisible();
  await expect(page.getByText("32", { exact: true })).toBeVisible();
  await page.getByRole("link", { name: "Inspect group 7" }).click();
  await expect(page).toHaveURL(/\/groups\/\?window=1h&filter=zone-a&connection=cluster-a&group_id=7$/);
});

test("keeps source evidence labels clear of the sidebar", async ({ page }) => {
  await page.goto(clusterUrl("/overview/", CLUSTER_A));
  const region = page.getByRole("region", { name: "Overview observation status" });
  await region.getByRole("button", { name: "2 sources" }).click();
  const panel = region.locator(".source-evidence-panel");
  const panelBox = await panel.boundingBox();
  const sidebarBox = await page.locator(".console-sidebar").boundingBox();
  expect(panelBox).not.toBeNull();
  expect(sidebarBox).not.toBeNull();
  expect(panelBox!.x).toBeGreaterThanOrEqual(sidebarBox!.x + sidebarBox!.width);
  const generated = panel.getByText("Generated", { exact: true });
  await expect(generated).toBeVisible();
  expect(await generated.evaluate((element) => {
    const rect = element.getBoundingClientRect();
    return document.elementFromPoint(rect.x + rect.width / 2, rect.y + rect.height / 2) === element;
  })).toBe(true);
});

test("keeps source evidence within a mobile viewport", async ({ page }) => {
  await page.setViewportSize({ width: 390, height: 844 });
  await page.goto(clusterUrl("/overview/", CLUSTER_A));
  const region = page.getByRole("region", { name: "Overview observation status" });
  await region.getByRole("button", { name: "2 sources" }).click();
  const panelBox = await region.locator(".source-evidence-panel").boundingBox();
  expect(panelBox).not.toBeNull();
  expect(panelBox!.x).toBeGreaterThanOrEqual(16);
  expect(panelBox!.x + panelBox!.width).toBeLessThanOrEqual(374);
  expect(await page.evaluate(() => document.documentElement.scrollWidth > innerWidth)).toBe(false);
});

test("repositions open source evidence after a viewport resize", async ({ page }) => {
  await page.goto(clusterUrl("/overview/", CLUSTER_A));
  const region = page.getByRole("region", { name: "Overview observation status" });
  await region.getByRole("button", { name: "2 sources" }).click();
  await page.setViewportSize({ width: 390, height: 844 });
  const panelBox = await region.locator(".source-evidence-panel").boundingBox();
  expect(panelBox).not.toBeNull();
  expect(panelBox!.x).toBeGreaterThanOrEqual(16);
  expect(panelBox!.x + panelBox!.width).toBeLessThanOrEqual(374);
});

test("omits supported capabilities from the overview attention feed", async ({ page }) => {
  await page.goto(clusterUrl("/overview/", CLUSTER_A));
  await expect(page.getByText(/Node 3 does not expose resource sampling/)).toBeVisible();
  await expect(page.getByText("Supported · groups", { exact: true })).toHaveCount(0);
});

test("keeps partial, stale, unsupported, and node failure evidence visible without overflow", async ({ page }) => {
  const errors: string[] = [];
  page.on("pageerror", (error) => errors.push(error.message));
  await page.setViewportSize({ width: 1280, height: 800 });
  await page.goto(clusterUrl("/overview/", CLUSTER_A));
  await expect(page.getByText("Partial", { exact: true }).first()).toBeVisible();
  await expect(page.getByText(/Stale/).first()).toBeVisible();
  await expect(page.getByText(/Node 3 does not expose resource sampling/)).toBeVisible();
  await expect(page.getByText("Unreachable", { exact: true }).first()).toBeVisible();
  expect(await page.evaluate(() => document.documentElement.scrollWidth > window.innerWidth)).toBe(false);
  expect(errors).toEqual([]);
});
