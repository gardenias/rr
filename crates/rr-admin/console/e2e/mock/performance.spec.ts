import { expect, test } from "@playwright/test";
import { eventsEnvelope, performanceEnvelope, snapshotsEnvelope, sourcesEnvelope } from "./task7.fixtures.mjs";
import { CLUSTER_A, clusterUrl, seedConnection } from "./connection";

test.beforeEach(async ({ page }) => {
  const response = await page.request.post("/api/v1/console/auth/login", { data: { token: "mock-viewer-token" } });
  expect(response.ok()).toBe(true);
  await seedConnection(page, CLUSTER_A);
});

test("keeps cohorts and counter-reset gaps truthful with an accessible table and filter URL", async ({ page }) => {
  await page.route(/\/api\/v1\/console\/connections\/cluster-a\/metrics\/performance(?:\?|$)/, (route) => route.fulfill({ json: performanceEnvelope() }));
  await page.goto(clusterUrl("/performance/", CLUSTER_A));

  await expect(page.getByRole("heading", { level: 1, name: "Performance" })).toBeVisible();
  await expect(page.getByText("UNALIGNED_COHORTS", { exact: true })).toBeVisible();
  const latency = page.getByRole("region", { name: "Latency" });
  await latency.getByRole("tab", { name: "Data table" }).click();
  await expect(latency.getByRole("table", { name: "Latency data" })).toContainText("0.12");
  await expect(latency.getByRole("table", { name: "Latency data" })).toContainText("0.04");
  await expect(page.getByText("0.08", { exact: true })).toHaveCount(0);
  const throughput = page.getByRole("region", { name: "Throughput" });
  await throughput.getByRole("tab", { name: "Data table" }).click();
  await expect(throughput.getByRole("table", { name: "Throughput data" })).toContainText("—");

  await page.getByLabel("Group ID").fill("7");
  await expect(page).toHaveURL(/group_id=7/);
  await page.getByRole("link", { name: "Inspect sources" }).click();
  await expect(page).toHaveURL(/\/sources\/?\?window=15m&group_id=7&connection=cluster-a$/);
  await expect(page.getByRole("heading", { level: 1, name: "Sources" })).toBeVisible();
});

test("does not backfill missing Prometheus history", async ({ page }) => {
  await page.route(/\/api\/v1\/console\/connections\/cluster-a\/metrics\/performance(?:\?|$)/, (route) => route.fulfill({ json: performanceEnvelope({ missingHistory: true }) }));
  await page.goto(clusterUrl("/performance/?window=24h&source=PROMETHEUS", CLUSTER_A));

  await expect(page.getByText("No performance observations for this window")).toBeVisible();
  await expect(page.getByText(/No retained history is available\. Missing points/)).toBeVisible();
  await expect(page.getByRole("region", { name: "Latency" })).toHaveCount(0);
});

test("keeps all Task 7 routes responsive and free of browser errors", async ({ page }) => {
  const errors: string[] = [];
  page.on("console", (message) => { if (message.type() === "error") errors.push(message.text()); });
  page.on("pageerror", (error) => errors.push(error.message));
  await page.route(/\/api\/v1\/console\/connections\/cluster-a\/metrics\/performance(?:\?|$)/, (route) => route.fulfill({ json: performanceEnvelope() }));
  await page.route(/\/api\/v1\/console\/connections\/cluster-a\/snapshots(?:\?|$)/, (route) => route.fulfill({ json: snapshotsEnvelope() }));
  await page.route(/\/api\/v1\/console\/connections\/cluster-a\/events(?:\?|$)/, (route) => route.fulfill({ json: eventsEnvelope() }));
  await page.route(/\/api\/v1\/console\/connections\/cluster-a\/sources(?:\?|$)/, (route) => route.fulfill({ json: sourcesEnvelope() }));

  for (const viewport of [{ width: 1440, height: 900 }, { width: 1280, height: 800 }]) {
    await page.setViewportSize(viewport);
    for (const [route, title] of [["performance", "Performance"], ["snapshots", "Snapshots & storage"], ["events", "Events & alerts"], ["sources", "Sources"]] as const) {
      await page.goto(clusterUrl(`/${route}/`, CLUSTER_A));
      await expect(page.getByRole("heading", { level: 1, name: title })).toBeVisible();
      expect(await page.evaluate(() => document.documentElement.scrollWidth > window.innerWidth), `${route} overflows at ${viewport.width}px`).toBe(false);
    }
  }
  expect(errors).toEqual([]);
});
