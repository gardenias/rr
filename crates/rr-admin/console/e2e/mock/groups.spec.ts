import { expect, test, type Page } from "@playwright/test";
import { groupDetailResponse, groupListResponse, replicaResponse } from "./groups.fixtures.mjs";

async function mockGroups(page: Page) {
  await page.route("**/api/v1/console/groups**", async (route) => {
    const pathname = new URL(route.request().url()).pathname;
    if (pathname === "/api/v1/console/groups") {
      await route.fulfill({ json: groupListResponse });
    } else if (pathname === "/api/v1/console/groups/1") {
      await route.fulfill({ json: groupDetailResponse });
    } else if (pathname === "/api/v1/console/groups/1/replicas") {
      await route.fulfill({ json: replicaResponse });
    } else {
      await route.continue();
    }
  });
}

test.beforeEach(async ({ page }) => {
  const response = await page.request.post("/api/v1/console/auth/login", {
    data: { token: "mock-viewer-token" },
  });
  expect(response.ok()).toBe(true);
  await mockGroups(page);
});

test("shows degraded group states and the complete leader-predicated replica matrix", async ({ page }) => {
  const consoleErrors: string[] = [];
  page.on("console", (message) => {
    if (message.type() === "error") consoleErrors.push(message.text());
  });
  page.on("pageerror", (error) => consoleErrors.push(error.message));

  await page.goto("/groups/?window=15m");
  await expect(page.getByRole("heading", { level: 1, name: "Raft groups" })).toBeVisible();
  await expect(page.getByRole("table", { name: "Raft groups" })).toBeVisible();
  await expect(page.getByText("Partial", { exact: true })).toBeVisible();
  await expect(page.getByText(/Stale ·/)).toBeVisible();
  await expect(page.getByText(/Node 3 does not implement/)).toBeVisible();
  await expect(page.getByRole("status", { name: "Health: Leaderless observed" })).toBeVisible();
  await expect(page.getByRole("status", { name: "Health: Syncing" })).toBeVisible();
  await expect(page.getByRole("status", { name: "Health: Conflicting views" })).toBeVisible();

  await page.getByRole("button", { name: "Inspect group 1", exact: true }).click();
  await expect(page.getByRole("dialog", { name: "Group 1 evidence" })).toBeVisible();
  await expect(page.getByText("Console view: 1 / 3 voters reachable")).toBeVisible();
  const matrix = page.getByRole("table", { name: "Group replica evidence" });
  await expect(matrix).toBeVisible();
  await expect(matrix.getByText("1,173", { exact: true })).toBeVisible();
  await expect(matrix.getByText("Non-leader source; no data", { exact: true })).toHaveCount(2);
  await expect(matrix.getByText("12.5 ms", { exact: true })).toBeVisible();
  await expect(matrix.getByText("Source node 3 · 2s old")).toBeVisible();

  await expect(page.getByRole("link", { name: "Related partitions" })).toHaveAttribute(
    "href",
    "/topology/?window=15m&view=routes&filter=group-1",
  );
  await expect(page.getByRole("link", { name: "Group performance" })).toHaveAttribute(
    "href",
    "/performance/?window=15m&group_id=1",
  );
  expect(consoleErrors).toEqual([]);
});

test("opens a shareable group deep link and closes back to the preserved list filter", async ({ page }) => {
  await page.goto("/groups/?window=1h&filter=1&group_id=1");
  await expect(page.getByRole("dialog", { name: "Group 1 evidence" })).toBeVisible();
  await expect(page.getByRole("table", { name: "Group replica evidence" })).toBeVisible();
  await page.getByRole("button", { name: "Close drawer" }).click();
  await expect(page.getByRole("dialog")).toHaveCount(0);
  await expect(page).toHaveURL(/\/groups\/\?window=1h&filter=1$/);
});
