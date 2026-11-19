import { expect, test } from "@playwright/test";
import { snapshotsEnvelope } from "./task7.fixtures.mjs";
import { CLUSTER_A, clusterUrl, seedConnection } from "./connection";

test.beforeEach(async ({ page }) => {
  const response = await page.request.post("/api/v1/console/auth/login", { data: { token: "mock-viewer-token" } });
  expect(response.ok()).toBe(true);
  await seedConnection(page, CLUSTER_A);
});

test("shows no-snapshot and node-only failure states without invented progress", async ({ page }) => {
  await page.route(/\/api\/v1\/console\/connections\/cluster-a\/snapshots(?:\?|$)/, (route) => route.fulfill({ json: snapshotsEnvelope() }));
  await page.goto(clusterUrl("/snapshots/", CLUSTER_A));

  const table = page.getByRole("table", { name: "Snapshots by group and node" });
  await expect(table).toBeVisible();
  await expect(table).toContainText("20 entries");
  await expect(table).toContainText("No snapshot observed yet");
  await expect(table).toContainText("UNSUPPORTED");
  await expect(page.getByText(/Node 2 reported 3 build failures/)).toBeVisible();
  await expect(page.getByText("0%", { exact: true })).toHaveCount(0);
  await page.getByLabel("Node ID").fill("2");
  await expect(page).toHaveURL(/node_id=2/);
});

test("keeps source drilldown query state", async ({ page }) => {
  await page.route(/\/api\/v1\/console\/connections\/cluster-a\/snapshots(?:\?|$)/, (route) => route.fulfill({ json: snapshotsEnvelope() }));
  await page.goto(clusterUrl("/snapshots/?window=1h&group_id=1", CLUSTER_A));

  await page.getByRole("link", { name: "Inspect sources" }).click();
  await expect(page).toHaveURL(/\/sources\/?\?window=1h&group_id=1&connection=cluster-a$/);
});
