import { expect, test, type Page } from "@playwright/test";
import { CLUSTER_A, CLUSTER_B, clusterUrl, seedDefaultClusters } from "./connection";
import { envelope } from "./fixtures.mjs";

function deferred() {
  let release!: () => void;
  const promise = new Promise<void>((resolve) => {
    release = resolve;
  });
  return { promise, release };
}

async function streamState(page: Page) {
  const response = await page.request.get("/__mock/stream/state?details=1");
  expect(response.ok()).toBe(true);
  return response.json();
}

function summaryMarker(observationCoverage: number) {
  return envelope("/summary", {
    nodes: [],
    groups: [],
    cluster_name: `HTTP ${observationCoverage}`,
    environment: "TEST",
    nodes_online: 3,
    nodes_total: 3,
    groups_with_leader: 2,
    groups_total: 2,
    quorum_risk_groups: 0,
    active_alerts: 0,
    observation_coverage: observationCoverage,
  });
}

test.beforeEach(async ({ page }) => {
  const response = await page.request.post("/api/v1/console/auth/login", {
    data: { token: "mock-viewer-token" },
  });
  expect(response.ok()).toBe(true);
  await seedDefaultClusters(page);
});

test("delayed A responses never render in B and switching closes A's SSE", async ({ page }) => {
  const delayedA = deferred();
  const delayedAHandled = deferred();
  const delayedB = deferred();
  let holdA = false;
  const holdB = true;
  await page.route(`**/api/v1/console/connections/${CLUSTER_A}/summary?*`, async (route) => {
    if (holdA) {
      holdA = false;
      await delayedA.promise;
      try {
        await route.fulfill({ json: summaryMarker(61) });
      } finally {
        delayedAHandled.release();
      }
      return;
    }
    await route.fulfill({ json: summaryMarker(61) });
  });
  await page.route(`**/api/v1/console/connections/${CLUSTER_B}/summary?*`, async (route) => {
    if (holdB) await delayedB.promise;
    await route.fulfill({ json: summaryMarker(92) });
  });

  await page.goto(clusterUrl("/overview/?window=1h&filter=old-search&node_id=9&cursor=old", CLUSTER_A));
  const main = page.locator("#console-main");
  await expect(main).toContainText("Observation coverage 61%");
  await expect(page.getByText("Data plane live")).toBeVisible();
  holdA = true;
  const pendingA = page.waitForRequest((request) =>
    new URL(request.url()).pathname === `/api/v1/console/connections/${CLUSTER_A}/summary`,
  );
  await page.getByRole("button", { name: "Refresh observations" }).click();
  const lateARequest = await pendingA;
  const lateASettled = Promise.race([
    page.waitForEvent("requestfinished", (request) => request === lateARequest),
    page.waitForEvent("requestfailed", (request) => request === lateARequest),
  ]);
  const pendingB = page.waitForRequest((request) =>
    new URL(request.url()).pathname === `/api/v1/console/connections/${CLUSTER_B}/summary`,
  );

  await page.getByRole("combobox", { name: "Active connection" }).selectOption(CLUSTER_B);
  await expect(page).toHaveURL(/\/overview\/\?window=1h&connection=cluster-b$/);
  await pendingB;
  await expect(main).not.toContainText("Observation coverage 61%");
  delayedA.release();
  await delayedAHandled.promise;
  await lateASettled;
  await expect(main).not.toContainText("Observation coverage 61%");
  await expect(main).not.toContainText("Observation coverage 92%");
  delayedB.release();
  await expect(main).toContainText("Observation coverage 92%");
  await expect(main).not.toContainText("Observation coverage 61%");
  await expect(page.getByText("Data plane live")).toBeVisible();
  await expect.poll(async () => {
    const state = await streamState(page);
    return [state.byConnection[CLUSTER_A] ?? 0, state.byConnection[CLUSTER_B] ?? 0];
  }).toEqual([0, 1]);
});

test("pause and refresh interval survive switching without starting B polling or SSE", async ({ page }) => {
  await page.goto(clusterUrl("/overview/", CLUSTER_A));
  await expect(page.locator(".cluster-identity strong")).toHaveText("Cluster A");
  await page.getByRole("combobox", { name: "Refresh interval" }).selectOption("15000");
  await page.getByRole("button", { name: "Pause", exact: true }).click();
  const bRequests: string[] = [];
  page.on("request", (request) => {
    if (request.url().includes(`/connections/${CLUSTER_B}/`)) bRequests.push(request.url());
  });

  await page.getByRole("combobox", { name: "Active connection" }).selectOption(CLUSTER_B);
  await expect(page.getByRole("button", { name: "Resume" })).toHaveAttribute("aria-pressed", "true");
  await expect(page.getByRole("combobox", { name: "Refresh interval" })).toHaveValue("15000");
  await page.waitForTimeout(500);
  expect(bRequests).toEqual([]);
  expect((await streamState(page)).byConnection[CLUSTER_B] ?? 0).toBe(0);

  await page.getByRole("button", { name: "Resume" }).click();
  await expect(page.locator(".cluster-identity strong")).toHaveText("Cluster B");
  await expect(page.getByText("Data plane live")).toBeVisible();
});

test("two pages retain independent connections and disconnect stops only A recovery", async ({ page, context }) => {
  const pageB = await context.newPage();
  await page.goto(clusterUrl("/overview/", CLUSTER_A));
  await pageB.goto(clusterUrl("/overview/", CLUSTER_B));
  await expect(page.locator(".cluster-identity strong")).toHaveText("Cluster A");
  await expect(pageB.locator(".cluster-identity strong")).toHaveText("Cluster B");
  await expect(page.getByText("Data plane live")).toBeVisible();
  await expect(pageB.getByText("Data plane live")).toBeVisible();

  await page.getByRole("link", { name: "Nodes", exact: true }).click();
  await expect(page).toHaveURL(/\/nodes\/\?.*connection=cluster-a/);
  await expect(pageB).toHaveURL(/\/overview\/\?connection=cluster-b$/);
  await expect.poll(async () => {
    const state = await streamState(page);
    return [state.byConnection[CLUSTER_A] ?? 0, state.byConnection[CLUSTER_B] ?? 0];
  }).toEqual([1, 1]);

  const disconnected = await pageB.request.post(`/api/v1/console/connections/${CLUSTER_A}/disconnect`);
  expect(disconnected.ok()).toBe(true);
  await expect(page.getByText("Connection unavailable")).toBeVisible();
  await page.waitForTimeout(1_500);
  const finalStreamState = await streamState(page);
  expect(finalStreamState.byConnection[CLUSTER_A] ?? 0).toBe(0);
  expect(finalStreamState.byConnection[CLUSTER_B]).toBe(1);
  await expect(pageB.locator(".cluster-identity strong")).toHaveText("Cluster B");
  await pageB.close();
});
