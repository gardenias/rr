import { expect, test, type Page } from "@playwright/test";
import { CLUSTER_A, clusterUrl, seedConnection } from "./connection";

const viewerToken = "mock-viewer-token";
const routes = ["overview", "nodes", "topology", "groups", "performance", "snapshots", "lifecycle", "events", "sources"];
const reconcilePath = "/api/v1/console/connections/cluster-a/sources/reconcile";

async function apiLogin(page: Page) {
  const response = await page.request.post("/api/v1/console/auth/login", { data: { token: viewerToken } });
  expect(response.ok()).toBe(true);
  await seedConnection(page, CLUSTER_A);
}

async function uiLogin(page: Page) {
  await page.goto("/login/");
  await page.getByLabel("Viewer credential").fill(viewerToken);
  await page.getByRole("button", { name: "Sign in to Console" }).click();
  await expect(page).toHaveURL(/\/connections\/$/);
  await seedConnection(page, CLUSTER_A);
}

test("viewer credentials never reach route URLs, HTML, persistent storage, cookies, or frontend logs", async ({ page, context }) => {
  const requestedUrls: string[] = [];
  const frontendLogs: string[] = [];
  page.on("request", (request) => requestedUrls.push(request.url()));
  page.on("console", (message) => frontendLogs.push(message.text()));
  page.on("pageerror", (error) => frontendLogs.push(error.message));

  await uiLogin(page);
  for (const route of routes) {
    await page.goto(clusterUrl(`/${route}/`, CLUSTER_A));
    await expect(page.locator("h1")).toHaveCount(1);
    expect(await page.content(), `${route} HTML contains the viewer credential`).not.toContain(viewerToken);
    const storage = await page.evaluate(() => ({
      local: Object.entries(localStorage),
      session: Object.entries(sessionStorage),
    }));
    expect(JSON.stringify(storage), `${route} browser storage contains the viewer credential`).not.toContain(viewerToken);
  }

  const cookies = await context.cookies();
  const session = cookies.find((cookie) => cookie.name === "rr_console_session");
  expect(session?.httpOnly).toBe(true);
  expect(session?.sameSite).toBe("Strict");
  expect(session?.value).not.toBe(viewerToken);
  expect(requestedUrls.join("\n")).not.toContain(viewerToken);
  expect(frontendLogs.join("\n")).not.toContain(viewerToken);
});

test("logout revokes the mock session and protected API access without retaining the credential", async ({ page, context }) => {
  await apiLogin(page);
  await page.goto(clusterUrl("/overview/", CLUSTER_A));
  await page.getByRole("button", { name: "Sign out" }).click();
  await expect(page).toHaveURL(/\/login\//);

  const cookies = await context.cookies();
  expect(cookies.find((cookie) => cookie.name === "rr_console_session")?.value ?? "").toBe("");
  const protectedResponse = await page.request.get("/api/v1/console/summary");
  expect(protectedResponse.status()).toBe(401);
  expect(await protectedResponse.text()).not.toContain(viewerToken);
  expect(await page.content()).not.toContain(viewerToken);
});

test("restores CSRF after a full page reload and sends one authenticated scoped reconcile", async ({ page }) => {
  const csrfRequests: string[] = [];
  const reconcileRequests: string[] = [];
  let reconcileCsrf: string | undefined;
  page.on("request", (request) => {
    const url = new URL(request.url());
    if (url.pathname === "/api/v1/console/auth/csrf") csrfRequests.push(request.method());
    if (url.pathname === reconcilePath) {
      reconcileRequests.push(request.method());
      reconcileCsrf = request.headers()["x-console-csrf"];
    }
  });

  await uiLogin(page);
  await page.goto(clusterUrl("/sources/", CLUSTER_A));
  await expect(page.getByRole("button", { name: "Reconcile now" })).toBeVisible();
  await page.reload();
  await page.getByRole("button", { name: "Reconcile now" }).click();
  await expect(page.getByText(/Reconciliation completed/)).toBeVisible();

  expect(csrfRequests).toEqual(["GET"]);
  expect(reconcileRequests).toEqual(["POST"]);
  expect(reconcileCsrf).toBeTruthy();
  const stored = await page.evaluate(() => JSON.stringify({
    local: Object.entries(localStorage),
    session: Object.entries(sessionStorage),
  }));
  expect(stored).not.toContain(reconcileCsrf);
});

test("caches the auth-disabled CSRF capability and omits the header on later scoped reconciles", async ({ page }) => {
  const csrfRequests: string[] = [];
  const reconcileHeaders: Array<string | undefined> = [];
  await page.route("**/api/v1/console/auth/csrf", (route) => route.fulfill({
    json: { meta: {}, csrf_token: null, expires_at: null },
  }));
  page.on("request", (request) => {
    const url = new URL(request.url());
    if (url.pathname === "/api/v1/console/auth/csrf") csrfRequests.push(request.method());
    if (url.pathname === reconcilePath) reconcileHeaders.push(request.headers()["x-console-csrf"]);
  });

  await uiLogin(page);
  await page.goto(clusterUrl("/sources/", CLUSTER_A));
  await page.reload();
  await page.getByRole("button", { name: "Reconcile now" }).click();
  await expect(page.getByText(/Reconciliation completed/)).toBeVisible();
  await page.getByRole("button", { name: "Reconcile now" }).click();
  await expect(page.getByText(/Reconciliation completed/)).toBeVisible();

  expect(csrfRequests).toEqual(["GET"]);
  expect(reconcileHeaders).toEqual([undefined, undefined]);
});
