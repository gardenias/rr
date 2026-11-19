import { expect, test, type Page } from "@playwright/test";
import { mkdir } from "node:fs/promises";
import { createRequire } from "node:module";
import { resolve } from "node:path";

const adminSecret = "admin-secret-not-for-storage";
const discoverySecret = "discovery-secret-not-for-storage";
const require = createRequire(__filename);
const nextRequire = createRequire(require.resolve("eslint-config-next"));
const axePath = createRequire(nextRequire.resolve("eslint-plugin-jsx-a11y")).resolve("axe-core");

async function audit(page: Page) {
  await page.addScriptTag({ path: axePath });
  return page.evaluate(async () => {
    const axe = (window as unknown as { axe: { run: (context: Document, options: unknown) => Promise<{ violations: unknown[] }> } }).axe;
    const result = await axe.run(document, { runOnly: { type: "tag", values: ["wcag2a", "wcag2aa", "wcag21a", "wcag21aa"] } });
    return result.violations;
  });
}

async function resetConnections(page: Page) {
  const response = await page.request.post("/__mock/connections/reset");
  expect(response.status()).toBe(204);
}

async function fillValidConnection(page: Page, name = "Production") {
  await page.getByLabel("Connection name").fill(name);
  await page.getByLabel("Environment").fill("prod");
  await page.getByLabel("Seed 1 node ID").fill("1");
  await page.getByLabel("Seed 1 AdminService address").fill("127.0.0.1:52001");
  await page.getByRole("button", { name: "Add admin seed" }).click();
  await page.getByLabel("Seed 2 node ID").fill("2");
  await page.getByLabel("Seed 2 AdminService address").fill("127.0.0.1:52002");
  await page.getByLabel("Discovery endpoints").fill("etcd://127.0.0.1:2379");
  await page.getByLabel("Discovery cluster").fill("production");
  await page.getByRole("checkbox", { name: "Allow insecure discovery transport" }).check();
  await page.getByLabel("Admin token", { exact: true }).fill(adminSecret);
  await page.getByLabel("Discovery password", { exact: true }).fill(discoverySecret);
}

function deferred() {
  let release!: () => void;
  const promise = new Promise<void>((resolve) => {
    release = resolve;
  });
  return { promise, release };
}

test.beforeEach(async ({ page }) => {
  const response = await page.request.post("/api/v1/console/auth/login", { data: { token: "mock-viewer-token" } });
  expect(response.ok()).toBe(true);
  await resetConnections(page);
});

test("root opens an empty manager and creates, tests, saves and connects a profile", async ({ page }) => {
  const summaryRequests: string[] = [];
  page.on("request", (request) => {
    const pathname = new URL(request.url()).pathname;
    if (pathname.endsWith("/summary")) summaryRequests.push(pathname);
  });

  await page.goto("/");
  await expect(page).toHaveURL(/\/connections\/$/);
  await expect(page.getByRole("heading", { name: "No connections yet" })).toBeVisible();
  await page.getByRole("link", { name: "Create first connection" }).click();
  await expect(page).toHaveURL(/\/connections\/new\/$/);
  await fillValidConnection(page);
  await page.getByRole("button", { name: "Test connection" }).click();
  await expect(page.getByText("Connection test passed.")).toBeVisible();
  await page.getByRole("button", { name: "Save & connect" }).click();

  await expect(page).toHaveURL(/\/overview\/\?connection=connection-1$/);
  await expect(page.getByRole("heading", { name: "Overview" })).toBeVisible();
  await expect(page.locator(".cluster-identity strong")).toHaveText("Production");
  expect(summaryRequests.length).toBeGreaterThan(0);
  expect(summaryRequests.every((path) => path.startsWith("/api/v1/console/connections/connection-1/"))).toBe(true);
});

test("opens a newly connected profile and a legacy startup runtime using only clicks", async ({ page }) => {
  const seeded = await page.request.post("/__mock/connections/seed", { data: {
    id: "startup-legacy",
    name: "Legacy startup",
    status: "CONNECTED",
    source: "STARTUP",
  } });
  expect(seeded.status()).toBe(204);

  await page.goto("/");
  await expect(page).toHaveURL(/\/connections\/$/);
  await page.getByRole("link", { name: "New connection" }).click();
  await fillValidConnection(page, "Click cluster");
  await page.getByRole("button", { name: "Save connection", exact: true }).click();
  const item = page.getByRole("article", { name: "Click cluster connection" });
  await expect(item.getByText("Disconnected", { exact: true })).toBeVisible();
  await item.getByRole("button", { name: "Connect Click cluster" }).click();
  await expect(item.getByText("Connected", { exact: true })).toBeVisible();
  await item.getByRole("link", { name: "Open Click cluster" }).click();
  await expect(page).toHaveURL(/\/overview\/\?connection=connection-1$/);
  await expect(page.getByRole("heading", { name: "Overview" })).toBeVisible();

  await page.getByRole("link", { name: "Manage connections" }).click();
  await expect(page).toHaveURL(/\/connections\/$/);
  await page.getByRole("link", { name: "Open Legacy startup" }).click();
  await expect(page).toHaveURL(/\/overview\/\?connection=startup-legacy$/);
  await expect(page.getByRole("heading", { name: "Overview" })).toBeVisible();
});

test("disconnects, edits with secret tri-state controls, and deletes a saved profile", async ({ page }) => {
  await page.goto("/connections/new/");
  await fillValidConnection(page);
  await page.getByRole("button", { name: "Save & connect" }).click();
  await expect(page).toHaveURL(/\/overview\//);
  await page.goto("/connections/");

  const item = page.getByRole("article", { name: "Production connection" });
  await expect(item.getByText("Connected", { exact: true })).toBeVisible();
  await expect(item.getByRole("button", { name: "Edit" })).toBeDisabled();
  await item.getByRole("button", { name: "Disconnect" }).click();
  await expect(item.getByText("Disconnected", { exact: true })).toBeVisible();
  await item.getByRole("button", { name: "Edit" }).click();
  const dialog = page.getByRole("dialog", { name: "Edit Production" });
  await expect(dialog.getByRole("radio", { name: "Keep current secret", exact: true }).first()).toBeChecked();
  await dialog.getByLabel("Connection name").fill("Production renamed");
  await dialog.getByRole("radiogroup", { name: "Admin token update" }).getByRole("radio", { name: "Clear secret" }).check();
  await dialog.getByRole("radiogroup", { name: "Discovery password update" }).getByRole("radio", { name: "Replace secret" }).check();
  await dialog.getByLabel("New discovery password").fill("replacement-secret");
  await dialog.getByRole("button", { name: "Save connection" }).click();
  await expect(dialog).toHaveCount(0);
  await expect(page.getByRole("article", { name: "Production renamed connection" })).toBeVisible();

  await page.getByRole("article", { name: "Production renamed connection" }).getByRole("button", { name: "Delete" }).click();
  await page.getByRole("dialog", { name: "Delete Production renamed?" }).getByRole("button", { name: "Delete connection" }).click();
  await expect(page.getByRole("heading", { name: "No connections yet" })).toBeVisible();
  await expect(page.getByRole("link", { name: "New connection", exact: true })).toBeFocused();
});

test("allows save after a failed test and preserves the profile when save-and-connect fails", async ({ page }) => {
  await page.goto("/connections/new/");
  await fillValidConnection(page, "Unreachable test");
  await page.getByLabel("Discovery endpoints").fill("etcd://test-failure:2379");
  await page.getByRole("button", { name: "Test connection" }).click();
  await expect(page.locator(".connection-operation-error")).toContainText("Connection test failed");
  await expect(page.locator(".connection-operation-error")).toContainText("Correlation ID:");
  await page.getByRole("button", { name: "Save connection" }).click();
  await expect(page).toHaveURL(/\/connections\/$/);
  await expect(page.getByRole("article", { name: "Unreachable test connection" })).toBeVisible();

  await page.goto("/connections/new/");
  await fillValidConnection(page, "Unavailable runtime");
  await page.getByLabel("Discovery endpoints").fill("etcd://connect-failure:2379");
  await page.getByRole("button", { name: "Save & connect" }).click();
  await expect(page.locator(".connection-operation-error")).toContainText("Connection saved, but connecting failed");
  await expect(page.locator(".connection-operation-error")).toContainText("Correlation ID:");
  await page.getByRole("link", { name: "Back to connections" }).click();
  await expect(page.getByRole("article", { name: "Unavailable runtime connection" })).toBeVisible();
  await expect(page.getByRole("article", { name: "Unavailable runtime connection" }).getByRole("button", { name: "Connect" })).toBeEnabled();
});

test("supports mobile layout and keyboard dialog dismissal without losing focus context", async ({ page }) => {
  await page.setViewportSize({ width: 320, height: 720 });
  await page.goto("/connections/new/");
  await fillValidConnection(page, "Mobile cluster");
  expect(await page.evaluate(() => document.documentElement.scrollWidth > window.innerWidth)).toBe(false);
  await page.getByRole("button", { name: "Save connection" }).click();
  await expect(page).toHaveURL(/\/connections\/$/);

  const edit = page.getByRole("button", { name: "Edit" });
  await edit.focus();
  await page.keyboard.press("Enter");
  const dialog = page.getByRole("dialog", { name: "Edit Mobile cluster" });
  await expect(dialog).toBeVisible();
  expect(await page.evaluate(() => document.documentElement.scrollWidth > window.innerWidth)).toBe(false);
  await page.keyboard.press("Escape");
  await expect(dialog).toHaveCount(0);
  await expect(edit).toBeFocused();

  const deleteButton = page.getByRole("button", { name: "Delete" });
  await deleteButton.focus();
  await page.keyboard.press("Enter");
  await expect(page.getByRole("dialog", { name: "Delete Mobile cluster?" })).toBeVisible();
  await page.keyboard.press("Escape");
  await expect(page.getByRole("dialog")).toHaveCount(0);
  await expect(deleteButton).toBeFocused();
});

test("keeps connection secrets out of URLs, browser storage and API responses", async ({ page }) => {
  const requestedUrls: string[] = [];
  const responseBodies: string[] = [];
  page.on("request", (request) => requestedUrls.push(request.url()));
  page.on("response", async (response) => {
    if (response.request().resourceType() === "fetch") responseBodies.push(await response.text().catch(() => ""));
  });

  await page.goto("/connections/new/");
  await fillValidConnection(page);
  await page.getByRole("button", { name: "Test connection" }).click();
  await expect(page.getByText("Connection test passed.")).toBeVisible();
  await page.getByRole("button", { name: "Save & connect" }).click();
  await expect(page).toHaveURL(/\/overview\/\?connection=connection-1$/);
  await page.goto("/connections/");
  const item = page.getByRole("article", { name: "Production connection" });
  await item.getByRole("button", { name: "Disconnect Production" }).click();
  await expect(item.getByText("Disconnected", { exact: true })).toBeVisible();
  await item.getByRole("button", { name: "Edit Production" }).click();
  const dialog = page.getByRole("dialog", { name: "Edit Production" });
  await dialog.getByRole("radiogroup", { name: "Admin token update" }).getByRole("radio", { name: "Replace secret" }).check();
  await dialog.getByLabel("New admin token").fill("replacement-secret");
  const updateResponse = page.waitForResponse((response) => response.request().method() === "PATCH" && response.url().endsWith("/connections/connection-1"));
  await dialog.getByRole("button", { name: "Save connection" }).click();
  expect((await updateResponse).ok()).toBe(true);
  await expect(dialog).toHaveCount(0);

  const storage = await page.evaluate(() => JSON.stringify({
    local: Object.entries(localStorage),
    session: Object.entries(sessionStorage),
  }));
  for (const secret of [adminSecret, discoverySecret, "replacement-secret"]) {
    expect(requestedUrls.join("\n")).not.toContain(secret);
    expect(responseBodies.join("\n")).not.toContain(secret);
    expect(storage).not.toContain(secret);
    expect(await page.content()).not.toContain(secret);
  }
});

test("redirects missing and disconnected URL connections to the manager without starting data requests", async ({ page }) => {
  const dataRequests: string[] = [];
  page.on("request", (request) => {
    const pathname = new URL(request.url()).pathname;
    if (pathname.endsWith("/summary") || pathname.endsWith("/stream")) dataRequests.push(pathname);
  });

  await page.goto("/overview/");
  await expect(page).toHaveURL(/\/connections\/$/);
  await page.goto("/overview/?connection=unknown");
  await expect(page).toHaveURL(/\/connections\/$/);
  await page.goto("/connections/new/");
  await fillValidConnection(page, "Disconnected cluster");
  await page.getByRole("button", { name: "Save connection" }).click();
  await expect(page).toHaveURL(/\/connections\/$/);
  await page.goto("/overview/?connection=connection-1");
  await expect(page).toHaveURL(/\/connections\/$/);
  expect(dataRequests).toEqual([]);
});


test("uses list DTO runtime counts without per-connection summary requests", async ({ page }) => {
  for (const name of ["Cluster A", "Cluster B"]) {
    const created = await page.request.post("/api/v1/console/connections", { data: {
      name,
      admin_seeds: [{ node_id: 1, admin_addr: "127.0.0.1:52001" }],
    } });
    expect(created.status()).toBe(201);
    const body = await created.json();
    const connected = await page.request.post(`/api/v1/console/connections/${body.data.id}/connect`);
    expect(connected.ok()).toBe(true);
  }
  const summaryRequests: string[] = [];
  page.on("request", (request) => {
    if (new URL(request.url()).pathname.endsWith("/summary")) summaryRequests.push(request.url());
  });

  await page.goto("/connections/");

  await expect(page.getByRole("article", { name: "Cluster A connection" })).toContainText("3 nodes / 2 groups");
  await expect(page.getByRole("article", { name: "Cluster B connection" })).toContainText("3 nodes / 2 groups");
  expect(summaryRequests).toEqual([]);
});

test("passes WCAG 2.1 AA axe checks for empty, new, and mobile edit states", async ({ page }) => {
  await page.emulateMedia({ reducedMotion: "reduce" });
  await page.goto("/connections/");
  await expect(page.getByRole("heading", { name: "No connections yet" })).toBeVisible();
  expect(await audit(page)).toEqual([]);

  await page.goto("/connections/new/");
  await expect(page.getByRole("heading", { name: "New connection" })).toBeVisible();
  expect(await audit(page)).toEqual([]);
  await fillValidConnection(page, "Accessible cluster");
  await page.getByRole("button", { name: "Save connection" }).click();
  await expect(page).toHaveURL(/\/connections\/$/);

  expect(await audit(page)).toEqual([]);
  await page.setViewportSize({ width: 320, height: 720 });
  expect(await page.evaluate(() => document.documentElement.scrollWidth > window.innerWidth)).toBe(false);
  await page.getByRole("button", { name: "Edit" }).click();
  await expect(page.getByRole("dialog", { name: "Edit Accessible cluster" })).toBeVisible();
  expect(await audit(page)).toEqual([]);
});

test("captures desktop and mobile visual evidence for connection management states", async ({ page }) => {
  const directory = resolve(process.cwd(), "../../../.superpowers/sdd/2026-11-18-multi-cluster-console/task-6-visual");
  await mkdir(directory, { recursive: true });
  await page.emulateMedia({ reducedMotion: "reduce" });
  await page.setViewportSize({ width: 1440, height: 900 });
  const capture = (name: string) => page.screenshot({ path: resolve(directory, `${name}.png`), fullPage: true, animations: "disabled" });

  await page.goto("/connections/");
  await expect(page.getByRole("heading", { name: "No connections yet" })).toBeVisible();
  await capture("01-desktop-empty");

  await page.goto("/connections/new/");
  await expect(page.getByRole("heading", { name: "New connection" })).toBeVisible();
  await capture("02-desktop-new");
  await page.getByRole("button", { name: "Save connection" }).click();
  await expect(page.getByText("Enter a connection name.")).toBeVisible();
  await capture("03-desktop-validation");

  await fillValidConnection(page);
  await page.getByRole("button", { name: "Test connection" }).click();
  await expect(page.getByText("Connection test passed.")).toBeVisible();
  await capture("04-desktop-test-success");
  await page.getByRole("button", { name: "Save & connect" }).click();
  await expect(page).toHaveURL(/\/overview\/\?connection=connection-1$/);
  await page.goto("/connections/");
  const connected = page.getByRole("article", { name: "Production connection" });
  await expect(connected.getByText("Connected", { exact: true })).toBeVisible();
  await capture("05-desktop-connected-list");

  await connected.getByRole("button", { name: "Disconnect" }).click();
  await expect(connected.getByText("Disconnected", { exact: true })).toBeVisible();
  await connected.getByRole("button", { name: "Edit" }).click();
  await expect(page.getByRole("dialog", { name: "Edit Production" })).toBeVisible();
  await capture("06-desktop-edit-dialog");
  await page.keyboard.press("Escape");
  await connected.getByRole("button", { name: "Delete" }).click();
  await expect(page.getByRole("dialog", { name: "Delete Production?" })).toBeVisible();
  await capture("07-desktop-delete-dialog");
  await page.keyboard.press("Escape");

  await page.goto("/connections/new/");
  await fillValidConnection(page, "Unavailable runtime");
  await page.getByLabel("Discovery endpoints").fill("etcd://connect-failure:2379");
  await page.getByRole("button", { name: "Save & connect" }).click();
  await expect(page.locator(".connection-operation-error")).toContainText("Connection saved, but connecting failed");
  await capture("08-desktop-connect-failure");

  await page.setViewportSize({ width: 320, height: 720 });
  await page.goto("/connections/new/");
  await expect(page.getByRole("heading", { name: "New connection" })).toBeVisible();
  await capture("09-mobile-new-320");
  await page.getByRole("button", { name: "Save connection" }).click();
  await expect(page.getByText("Enter a connection name.")).toBeVisible();
  await capture("10-mobile-validation-320");

  await page.goto("/connections/");
  await expect(page.getByRole("article", { name: "Production connection" })).toBeVisible();
  await expect(page.getByRole("article", { name: "Unavailable runtime connection" })).toBeVisible();
  expect(await page.evaluate(() => document.documentElement.scrollWidth > window.innerWidth)).toBe(false);
  await capture("11-mobile-list-320");
  const mobileProduction = page.getByRole("article", { name: "Production connection" });
  await mobileProduction.getByRole("button", { name: "Edit" }).click();
  await expect(page.getByRole("dialog", { name: "Edit Production" })).toBeVisible();
  await capture("12-mobile-edit-dialog-320");
  await page.keyboard.press("Escape");
  await mobileProduction.getByRole("button", { name: "Delete" }).click();
  await expect(page.getByRole("dialog", { name: "Delete Production?" })).toBeVisible();
  await capture("13-mobile-delete-dialog-320");
});


test("opens advanced settings and focuses an invalid poll interval", async ({ page }) => {
  await page.goto("/connections/new/");
  await fillValidConnection(page, "Advanced validation");
  const summary = page.getByText("Advanced settings");
  await summary.click();
  await page.getByLabel("Poll interval (seconds)").fill("0");
  await summary.click();
  await page.getByRole("button", { name: "Save connection" }).click();

  await expect(page.locator("details.connection-advanced")).toHaveAttribute("open", "");
  await expect(page.getByLabel("Poll interval (seconds)")).toBeFocused();
  await expect(page.getByText("Enter a whole number from 1 to 300 seconds.")).toBeVisible();
  await expect(page).toHaveURL(/\/connections\/new\/$/);
});

test("rejects duplicate seeds and focuses the second node ID", async ({ page }) => {
  await page.goto("/connections/new/");
  await fillValidConnection(page, "Duplicate seeds");
  await page.getByLabel("Seed 2 node ID").fill("1");
  await page.getByLabel("Seed 2 AdminService address").fill("127.0.0.1:52001");
  await page.getByRole("button", { name: "Save connection" }).click();

  await expect(page.getByText("Each admin seed must use a unique node ID.")).toBeVisible();
  await expect(page.getByText("Each admin seed must use a unique AdminService address.")).toBeVisible();
  await expect(page.getByLabel("Seed 2 node ID")).toBeFocused();
  await expect(page).toHaveURL(/\/connections\/new\/$/);
});

test("keeps delete failures and their correlation ID inside the dialog", async ({ page }) => {
  await page.goto("/connections/new/");
  await fillValidConnection(page, "Delete failure");
  await page.getByRole("button", { name: "Save connection" }).click();
  await expect(page).toHaveURL(/\/connections\/$/);
  await page.getByRole("button", { name: "Delete Delete failure" }).click();
  const dialog = page.getByRole("dialog", { name: "Delete Delete failure?" });
  await dialog.getByRole("button", { name: "Delete connection" }).click();

  await expect(dialog.getByRole("alert")).toContainText("Profile store is read-only");
  await expect(dialog.getByRole("alert")).toContainText("Correlation ID:");
  await expect(dialog).toBeVisible();
  await dialog.getByRole("button", { name: "Keep connection" }).click();
  await expect(page.getByRole("article", { name: "Delete failure connection" })).toBeVisible();
});

test("refreshes a deferred create after leaving without old Connect or navigation", async ({ page }) => {
  const pending = deferred();
  let connectRequests = 0;
  page.on("request", (request) => {
    if (request.method() === "POST" && new URL(request.url()).pathname.endsWith("/connect")) connectRequests += 1;
  });
  await page.route("**/api/v1/console/connections", async (route) => {
    await pending.promise;
    await route.continue();
  });
  await page.goto("/connections/new/");
  await fillValidConnection(page, "Deferred create");
  const createdRequest = page.waitForRequest((request) =>
    request.method() === "POST" && new URL(request.url()).pathname === "/api/v1/console/connections");
  await page.getByRole("button", { name: "Save & connect" }).click();
  await createdRequest;
  await page.getByRole("link", { name: "Back to connections" }).click();
  await expect(page).toHaveURL(/\/connections\/$/);

  pending.release();

  await expect(page.getByRole("article", { name: "Deferred create connection" })).toBeVisible();
  await expect(page).toHaveURL(/\/connections\/$/);
  expect(connectRequests).toBe(0);
});

test("refreshes a deferred A update without closing the newly opened B editor", async ({ page }) => {
  const ids: string[] = [];
  for (const name of ["Cluster A", "Cluster B"]) {
    const created = await page.request.post("/api/v1/console/connections", { data: {
      name,
      admin_seeds: [{ node_id: 1, admin_addr: "127.0.0.1:52001" }],
    } });
    expect(created.status()).toBe(201);
    ids.push((await created.json()).data.id);
  }
  const pending = deferred();
  await page.route(`**/api/v1/console/connections/${ids[0]}`, async (route) => {
    await pending.promise;
    await route.continue();
  });
  await page.goto(`/connections/?edit=${ids[0]}`);
  const editorA = page.getByRole("dialog", { name: "Edit Cluster A" });
  await editorA.getByLabel("Connection name").fill("Cluster A renamed");
  const updateRequest = page.waitForRequest((request) => request.method() === "PATCH");
  await editorA.getByRole("button", { name: "Save connection" }).click();
  await updateRequest;
  await page.keyboard.press("Escape");
  await page.getByRole("button", { name: "Edit Cluster B" }).click();
  const editorB = page.getByRole("dialog", { name: "Edit Cluster B" });
  await expect(editorB).toBeVisible();
  const updated = page.waitForResponse((response) => response.request().method() === "PATCH");
  const refreshed = page.waitForResponse((response) =>
    response.request().method() === "GET" && new URL(response.url()).pathname === "/api/v1/console/connections");

  pending.release();
  await updated;
  await refreshed;

  await expect(editorB).toBeVisible();
  await page.keyboard.press("Escape");
  await expect(page.getByRole("article", { name: "Cluster A renamed connection" })).toBeVisible();
});

test("keeps a deferred delete open through close attempts and restores focus after success", async ({ page }) => {
  const created = await page.request.post("/api/v1/console/connections", { data: {
    name: "Deferred delete",
    admin_seeds: [{ node_id: 1, admin_addr: "127.0.0.1:52001" }],
  } });
  expect(created.status()).toBe(201);
  const id = (await created.json()).data.id;
  const pending = deferred();
  await page.route(`**/api/v1/console/connections/${id}`, async (route) => {
    await pending.promise;
    await route.continue();
  });
  await page.goto("/connections/");
  await page.getByRole("button", { name: "Delete Deferred delete" }).click();
  const dialog = page.getByRole("dialog", { name: "Delete Deferred delete?" });
  const deleteRequest = page.waitForRequest((request) => request.method() === "DELETE");
  await dialog.getByRole("button", { name: "Delete connection" }).click();
  await deleteRequest;
  await dialog.getByRole("button", { name: "Close dialog" }).click();
  await expect(dialog).toBeVisible();
  await page.keyboard.press("Escape");
  await expect(dialog).toBeVisible();
  const keep = dialog.getByRole("button", { name: "Keep connection" });
  await expect(keep).toBeDisabled();
  await keep.evaluate((button: HTMLButtonElement) => button.click());
  await expect(dialog).toBeVisible();
  const deleted = page.waitForResponse((response) => response.request().method() === "DELETE");

  pending.release();
  await deleted;

  await expect(dialog).toHaveCount(0);
  await expect(page.getByRole("link", { name: "New connection", exact: true })).toBeFocused();
  await expect(page.getByRole("article", { name: "Deferred delete connection" })).toHaveCount(0);
});

test("uses the saved connection ID for Keep candidate tests without returning secrets", async ({ page }) => {
  await page.goto("/connections/new/");
  await fillValidConnection(page, "Keep candidate");
  await page.getByRole("button", { name: "Save connection" }).click();
  await expect(page).toHaveURL(/\/connections\/$/);
  await page.getByRole("button", { name: "Edit Keep candidate" }).click();
  const dialog = page.getByRole("dialog", { name: "Edit Keep candidate" });
  const candidateRequest = page.waitForRequest((request) =>
    request.method() === "POST" && new URL(request.url()).pathname === "/api/v1/console/connections/test");
  await dialog.getByRole("button", { name: "Test connection" }).click();

  const body = (await candidateRequest).postDataJSON();
  expect(body.existing_connection_id).toBe("connection-1");
  expect(body).not.toHaveProperty("admin_token");
  expect(body).not.toHaveProperty("discovery_password");
  await expect(dialog.getByText("Connection test passed. The candidate was not saved.")).toBeVisible();
  expect(JSON.stringify(body)).not.toContain(adminSecret);
  expect(JSON.stringify(body)).not.toContain(discoverySecret);
});

test("mock BFF rejects userinfo and invalid discovery transport without reflecting secrets", async ({ page }) => {
  const base = {
    name: "Unsafe endpoint",
    admin_seeds: [{ node_id: 1, admin_addr: "127.0.0.1:52001" }],
    discovery: null,
    cluster_name: "test",
    discovery_allow_insecure: false,
  };
  const invalid = [
    { ...base, admin_seeds: [{ node_id: 1, admin_addr: "user:endpoint-secret@127.0.0.1:52001" }] },
    {
      ...base,
      admin_seeds: [],
      discovery: "etcd://user:endpoint-secret@127.0.0.1:2379",
      discovery_allow_insecure: true,
    },
    { ...base, admin_seeds: [], discovery: "etcd://127.0.0.1:2379" },
  ];
  for (const input of invalid) {
    const response = await page.request.post("/api/v1/console/connections", { data: input });
    expect(response.status()).toBe(400);
    expect(await response.text()).not.toContain("endpoint-secret");
  }
  const list = await page.request.get("/api/v1/console/connections");
  expect(list.ok()).toBe(true);
  expect(await list.text()).not.toContain("endpoint-secret");
  expect((await list.json()).data.items).toEqual([]);
});

test.describe("coarse pointer targets", () => {
  test.use({ hasTouch: true, viewport: { width: 390, height: 844 } });

  test("keeps form, list, dialog and disclosure targets at least 44px", async ({ page }) => {
    await page.goto("/connections/new/");
    const formTargets = [
      page.getByRole("link", { name: "Back to connections" }),
      page.getByRole("button", { name: "Add admin seed" }),
      page.getByText("Advanced settings"),
      page.getByRole("button", { name: "Test connection" }),
      page.getByRole("button", { name: "Save connection" }),
      page.getByRole("button", { name: "Save & connect" }),
    ];
    for (const target of formTargets) {
      const box = await target.boundingBox();
      expect(box?.width).toBeGreaterThanOrEqual(44);
      expect(box?.height).toBeGreaterThanOrEqual(44);
    }

    const created = await page.request.post("/api/v1/console/connections", { data: {
      name: "Touch cluster",
      admin_seeds: [{ node_id: 1, admin_addr: "127.0.0.1:52001" }],
    } });
    expect(created.status()).toBe(201);
    await page.goto("/connections/");
    const item = page.getByRole("article", { name: "Touch cluster connection" });
    for (const action of ["Connect", "Test", "Edit", "Delete"]) {
      const box = await item.getByRole("button", { name: `${action} Touch cluster` }).boundingBox();
      expect(box?.width).toBeGreaterThanOrEqual(44);
      expect(box?.height).toBeGreaterThanOrEqual(44);
    }

    await item.getByRole("button", { name: "Edit Touch cluster" }).click();
    const dialog = page.getByRole("dialog", { name: "Edit Touch cluster" });
    const dialogTargets = [
      dialog.getByRole("button", { name: "Close dialog" }),
      dialog.getByText("Advanced settings"),
      ...await dialog.locator(".radio-field").all(),
    ];
    for (const target of dialogTargets) {
      const box = await target.boundingBox();
      expect(box?.width).toBeGreaterThanOrEqual(44);
      expect(box?.height).toBeGreaterThanOrEqual(44);
    }
    await page.keyboard.press("Escape");

    await item.getByRole("button", { name: "Delete Touch cluster" }).click();
    const deleteDialog = page.getByRole("dialog", { name: "Delete Touch cluster?" });
    const deleteClose = await deleteDialog.getByRole("button", { name: "Close dialog" }).boundingBox();
    expect(deleteClose?.width).toBeGreaterThanOrEqual(44);
    expect(deleteClose?.height).toBeGreaterThanOrEqual(44);
    await page.keyboard.press("Escape");

    await item.getByRole("button", { name: "Connect Touch cluster" }).click();
    await expect(item.getByText("Connected", { exact: true })).toBeVisible();
    const id = (await created.json()).data.id;
    await page.goto(`/connections/?edit=${id}`);
    const returnButton = page.getByRole("button", { name: "Return to connections" });
    const returnBox = await returnButton.boundingBox();
    expect(returnBox?.width).toBeGreaterThanOrEqual(44);
    expect(returnBox?.height).toBeGreaterThanOrEqual(44);
    await returnButton.click();
    await expect(page.getByRole("button", { name: "Disconnect Touch cluster" })).toBeFocused();

    await page.route("**/api/v1/console/connections", async (route) => {
      if (route.request().method() !== "GET") return route.continue();
      await route.fulfill({
        status: 500,
        contentType: "application/json",
        body: JSON.stringify({
          meta: { correlation_id: "corr-touch-retry" },
          error: { code: "CONNECTION_STORE_FAILED", message: "Profile store unavailable" },
        }),
      });
    });
    await page.goto("/connections/");
    const retry = page.getByRole("button", { name: "Retry loading connections" });
    const retryBox = await retry.boundingBox();
    expect(retryBox?.width).toBeGreaterThanOrEqual(44);
    expect(retryBox?.height).toBeGreaterThanOrEqual(44);
  });
});
