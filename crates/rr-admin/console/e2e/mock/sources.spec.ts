import { expect, test } from "@playwright/test";
import { sourcesEnvelope } from "./task7.fixtures.mjs";
import { CLUSTER_A, clusterUrl, seedConnection } from "./connection";

test.beforeEach(async ({ page }) => {
  const response = await page.request.post("/api/v1/console/auth/login", { data: { token: "mock-viewer-token" } });
  expect(response.ok()).toBe(true);
  await seedConnection(page, CLUSTER_A);
});

test("sanitizes source errors, explains UNMAPPED history, and confirms reconcile", async ({ page }) => {
  let sourceRequests = 0;
  await page.route(/\/api\/v1\/console\/connections\/cluster-a\/sources(?:\?|$)/, (route) => {
    sourceRequests += 1;
    return route.fulfill({ json: sourcesEnvelope() });
  });
  await page.goto(clusterUrl("/sources/", CLUSTER_A));

  await expect(page.getByText(/token=\[redacted\]/)).toBeVisible();
  await expect(page.getByText(/supersecret/)).toHaveCount(0);
  await expect(page.getByText("UNMAPPED", { exact: true })).toBeVisible();
  await expect(page.getByText(/stable node_id label/)).toBeVisible();
  await page.getByRole("button", { name: "Reconcile now" }).click();
  await expect(page.getByText(/Reconciliation completed/)).toBeVisible();
  await expect.poll(() => sourceRequests).toBeGreaterThan(1);
});

test("offers reauthentication after a forbidden reconcile", async ({ page }) => {
  await page.route(/\/api\/v1\/console\/connections\/cluster-a\/sources(?:\?|$)/, (route) => route.fulfill({ json: sourcesEnvelope() }));
  await page.route(/\/api\/v1\/console\/connections\/cluster-a\/sources\/reconcile$/, (route) => route.fulfill({
    status: 403,
    json: { meta: { correlation_id: "corr-403" }, error: { code: "FORBIDDEN", message: "CSRF validation failed" } },
  }));
  await page.goto(clusterUrl("/sources/", CLUSTER_A));

  await page.getByRole("button", { name: "Reconcile now" }).click();
  await expect(page.getByText(/Reauthentication required/)).toBeVisible();
  await expect(page.getByRole("link", { name: "Reauthenticate" })).toHaveAttribute("href", /\/login\/\?next=/);
});
