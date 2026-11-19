import { expect, test } from "@playwright/test";
import { lifecycleResponse } from "./lifecycle.fixtures.mjs";
import { CLUSTER_A, clusterUrl, seedConnection } from "./connection";

test.beforeEach(async ({ page }) => {
  const response = await page.request.post("/api/v1/console/auth/login", { data: { token: "mock-viewer-token" } });
  expect(response.ok()).toBe(true);
  await seedConnection(page, CLUSTER_A);
  await page.route("**/api/v1/console/connections/cluster-a/lifecycle?*", (route) => {
    const url = new URL(route.request().url());
    return route.fulfill({ json: lifecycleResponse(url.searchParams.get("view") ?? "current", url.searchParams.get("cursor")) });
  });
});

test("shows current lifecycle evidence and volatile restart history without crash inference", async ({ page }) => {
  await page.goto(clusterUrl("/lifecycle/?window=1h", CLUSTER_A));
  await expect(page.getByRole("heading", { level: 1, name: "Lifecycle" })).toBeVisible();
  await expect(page.getByText("boot-current").first()).toBeVisible();
  await page.getByRole("tab", { name: "Restarts" }).click();
  await expect(page).toHaveURL(/window=1h.*connection=cluster-a.*view=restarts/);
  await expect(page.getByRole("table", { name: "Restart timeline data" })).toBeVisible();
  await expect(page.getByText("Abnormal or uncertain").first()).toBeVisible();
  await expect(page.getByText("VOLATILE").first()).toBeVisible();
  await expect(page.getByText(/Console observation gaps are not process crash evidence/i)).toBeVisible();
  await expect(page.getByText(/crash detected/i)).toHaveCount(0);
  await page.getByRole("button", { name: "Older restart records" }).click();
  await expect(page.getByRole("button", { name: "First page" })).toBeVisible();
});

test("renders version distribution, dirty builds, and authoritative incompatibility at 1280px", async ({ page }) => {
  await page.setViewportSize({ width: 1280, height: 800 });
  await page.goto(clusterUrl("/lifecycle/?view=versions", CLUSTER_A));
  await expect(page.getByText("2 application versions · 2 Rr versions")).toBeVisible();
  await expect(page.getByText("Dirty build")).toBeVisible();
  await expect(page.getByText("Incompatible")).toBeVisible();
  await expect(page.getByText(/node 2 min_peer_minor/i)).toBeVisible();
  await expect(page.getByText("UNKNOWN").first()).toBeVisible();
  expect(await page.evaluate(() => document.documentElement.scrollWidth > window.innerWidth)).toBe(false);
});
