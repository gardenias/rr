import { expect, test } from "@playwright/test";
import { eventsEnvelope } from "./task7.fixtures.mjs";
import { CLUSTER_A, clusterUrl, CLUSTER_B, seedConnection } from "./connection";

test.beforeEach(async ({ page }) => {
  const response = await page.request.post("/api/v1/console/auth/login", { data: { token: "mock-viewer-token" } });
  expect(response.ok()).toBe(true);
  await seedConnection(page, CLUSTER_A);
});

test("persists acknowledgement and silence as reversible browser-local preferences", async ({ page }) => {
  await page.route(/\/api\/v1\/console\/connections\/cluster-a\/events(?:\?|$)/, (route) => route.fulfill({ json: eventsEnvelope() }));
  await page.goto(clusterUrl("/events/", CLUSTER_A));

  await page.getByRole("button", { name: "Acknowledge event-node-1-unreachable" }).click();
  await expect(page.getByText("Acknowledged locally")).toBeVisible();
  await page.reload();
  await expect(page.getByRole("button", { name: "Acknowledge event-node-1-unreachable" })).toBeDisabled();
  await page.getByRole("button", { name: "Silence 1h: event-node-1-unreachable" }).click();
  await expect(page.getByRole("table", { name: "Observed events and alerts" })).toHaveCount(0);
  await page.getByRole("button", { name: "Show silenced events" }).click();
  await expect(page.getByRole("table", { name: "Observed events and alerts" })).toContainText("event-node-1-unreachable");
  await page.getByRole("button", { name: "Unsilence event-node-1-unreachable" }).click();
  await expect(page.getByText(/1 event silenced by local preference/)).toHaveCount(0);

  await page.getByRole("combobox", { name: "State", exact: true }).selectOption("RESOLVED");
  await expect(page).toHaveURL(/state=RESOLVED/);
});

test("two loaded pages merge sequential A and B preference writes", async ({ page, context }) => {
  await seedConnection(page, CLUSTER_B);
  const pageB = await context.newPage();
  for (const current of [page, pageB]) {
    await current.route(/\/api\/v1\/console\/connections\/cluster-[ab]\/events(?:\?|$)/, (route) =>
      route.fulfill({ json: eventsEnvelope() }));
  }
  await Promise.all([
    page.goto(clusterUrl("/events/", CLUSTER_A)),
    pageB.goto(clusterUrl("/events/", CLUSTER_B)),
  ]);
  const acknowledgeA = page.getByRole("button", { name: "Acknowledge event-node-1-unreachable" });
  const acknowledgeB = pageB.getByRole("button", { name: "Acknowledge event-node-1-unreachable" });
  await expect(acknowledgeA).toBeEnabled();
  await expect(acknowledgeB).toBeEnabled();

  await acknowledgeA.click();
  await expect(page.getByText("Acknowledged locally")).toBeVisible();
  await acknowledgeB.click();
  await expect(pageB.getByText("Acknowledged locally")).toBeVisible();
  await Promise.all([page.reload(), pageB.reload()]);

  await expect(page.getByRole("button", { name: "Acknowledge event-node-1-unreachable" })).toBeDisabled();
  await expect(pageB.getByRole("button", { name: "Acknowledge event-node-1-unreachable" })).toBeDisabled();
  await pageB.close();
});

test("does not equate partial empty event results with health", async ({ page }) => {
  await page.route(/\/api\/v1\/console\/connections\/cluster-a\/events(?:\?|$)/, (route) => route.fulfill({ json: eventsEnvelope({ empty: true }) }));
  await page.goto(clusterUrl("/events/?severity=WARN", CLUSTER_A));

  await expect(page.getByText("No matching observed events")).toBeVisible();
  await expect(page.getByText(/Observation is partial; this does not prove the cluster is healthy/)).toBeVisible();
});
