import { expect, test } from "@playwright/test";
import { eventsEnvelope } from "./task7.fixtures.mjs";

test.beforeEach(async ({ page }) => {
  const response = await page.request.post("/api/v1/console/auth/login", { data: { token: "mock-viewer-token" } });
  expect(response.ok()).toBe(true);
});

test("persists acknowledgement and silence as reversible browser-local preferences", async ({ page }) => {
  await page.route(/\/api\/v1\/console\/events(?:\?|$)/, (route) => route.fulfill({ json: eventsEnvelope() }));
  await page.goto("/events/");

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

test("does not equate partial empty event results with health", async ({ page }) => {
  await page.route(/\/api\/v1\/console\/events(?:\?|$)/, (route) => route.fulfill({ json: eventsEnvelope({ empty: true }) }));
  await page.goto("/events/?severity=WARN");

  await expect(page.getByText("No matching observed events")).toBeVisible();
  await expect(page.getByText(/Observation is partial; this does not prove the cluster is healthy/)).toBeVisible();
});
