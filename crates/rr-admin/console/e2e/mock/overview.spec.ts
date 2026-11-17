import { expect, test } from "@playwright/test";
import { overviewEvents, overviewPerformance, overviewSummary } from "./overview.fixtures.mjs";

test.beforeEach(async ({ page }) => {
  const response = await page.request.post("/api/v1/console/auth/login", { data: { token: "mock-viewer-token" } });
  expect(response.ok()).toBe(true);
  await page.route("**/api/v1/console/summary?*", (route) => route.fulfill({ json: overviewSummary() }));
  await page.route("**/api/v1/console/metrics/performance?*", (route) => route.fulfill({ json: overviewPerformance }));
  await page.route("**/api/v1/console/events?*", (route) => route.fulfill({ json: overviewEvents }));
});

test("prioritizes leaderless evidence and preserves query state in drilldowns", async ({ page }) => {
  await page.goto("/overview/?window=1h&filter=zone-a");
  await expect(page.getByRole("heading", { level: 1, name: "Overview" })).toBeVisible();
  await expect(page.getByText("Group 7 has no observed leader")).toBeVisible();
  await expect(page.getByText("67% observation coverage", { exact: true })).toBeVisible();
  await expect(page.getByText("125", { exact: true })).toBeVisible();
  await expect(page.getByText("32", { exact: true })).toBeVisible();
  await page.getByRole("link", { name: "Inspect group 7" }).click();
  await expect(page).toHaveURL(/\/groups\/\?window=1h&filter=zone-a&group_id=7$/);
});

test("keeps partial, stale, unsupported, and node failure evidence visible without overflow", async ({ page }) => {
  const errors: string[] = [];
  page.on("pageerror", (error) => errors.push(error.message));
  await page.setViewportSize({ width: 1280, height: 800 });
  await page.goto("/overview/");
  await expect(page.getByText("Partial", { exact: true }).first()).toBeVisible();
  await expect(page.getByText(/Stale/).first()).toBeVisible();
  await expect(page.getByText(/Node 3 does not expose resource sampling/)).toBeVisible();
  await expect(page.getByText("Unreachable", { exact: true }).first()).toBeVisible();
  expect(await page.evaluate(() => document.documentElement.scrollWidth > window.innerWidth)).toBe(false);
  expect(errors).toEqual([]);
});
