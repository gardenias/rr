import { expect, test } from "@playwright/test";
import { CLUSTER_A, clusterUrl, seedConnection } from "./connection";

test("viewer login preserves the full connection return URL without browser credential storage", async ({ page, context }) => {
  const errors: string[] = [];
  page.on("console", (message) => {
    if (message.type() === "error") errors.push(message.text());
  });
  await seedConnection(page, CLUSTER_A);
  const returnPath = clusterUrl("/groups/?window=1h&group_id=3", CLUSTER_A);
  await page.goto(`/login/?next=${encodeURIComponent(returnPath)}`);
  await page.getByLabel("Viewer credential").fill("wrong-token");
  await page.getByRole("button", { name: "Sign in to Console" }).click();
  await expect(page.getByRole("alert").filter({ hasText: "Sign-in failed" })).toBeVisible();

  await page.getByLabel("Viewer credential").fill("mock-viewer-token");
  await page.getByRole("button", { name: "Sign in to Console" }).click();
  await expect(page).toHaveURL(/\/groups\/\?window=1h&group_id=3&connection=cluster-a$/);
  await expect(page.getByRole("dialog", { name: "Group 3 evidence" })).toBeVisible();
  await expect(page.getByRole("heading", { level: 1, name: "Raft groups" })).toHaveCount(0);
  await page.getByRole("button", { name: "Close drawer" }).click();
  await expect(page.getByRole("dialog")).toHaveCount(0);
  await expect(page.getByRole("heading", { level: 1, name: "Raft groups" })).toBeVisible();
  await expect(page).toHaveURL(/\/groups\/\?window=1h&connection=cluster-a$/);

  const cookies = await context.cookies();
  const session = cookies.find((cookie) => cookie.name === "rr_console_session");
  expect(session?.httpOnly).toBe(true);
  expect(session?.sameSite).toBe("Strict");
  const stored = await page.evaluate(() => ({
    local: Object.fromEntries(Object.entries(localStorage)),
    session: Object.fromEntries(Object.entries(sessionStorage)),
  }));
  expect(stored.local).toEqual({ "rr.console.recent-connection.v1": "cluster-a" });
  expect(stored.session).toEqual({});
  expect(await page.content()).not.toContain("mock-viewer-token");
  expect(errors.join("\n")).not.toContain("mock-viewer-token");
});
