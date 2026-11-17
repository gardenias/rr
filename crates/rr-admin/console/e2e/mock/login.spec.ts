import { expect, test } from "@playwright/test";

test("viewer login uses an HttpOnly session without browser credential storage", async ({ page, context }) => {
  const errors: string[] = [];
  page.on("console", (message) => {
    if (message.type() === "error") errors.push(message.text());
  });
  await page.goto("/login/?next=%2Fgroups%2F%3Fwindow%3D1h%26group_id%3D3");
  await page.getByLabel("Viewer credential").fill("wrong-token");
  await page.getByRole("button", { name: "Sign in to Console" }).click();
  await expect(page.getByRole("alert").filter({ hasText: "Sign-in failed" })).toBeVisible();

  await page.getByLabel("Viewer credential").fill("mock-viewer-token");
  await page.getByRole("button", { name: "Sign in to Console" }).click();
  await expect(page).toHaveURL(/\/groups\/\?window=1h&group_id=3$/);
  await expect(page.getByRole("dialog", { name: "Group 3 evidence" })).toBeVisible();
  await expect(page.getByRole("heading", { level: 1, name: "Raft groups" })).toHaveCount(0);
  await page.getByRole("button", { name: "Close drawer" }).click();
  await expect(page.getByRole("dialog")).toHaveCount(0);
  await expect(page.getByRole("heading", { level: 1, name: "Raft groups" })).toBeVisible();
  await expect(page).toHaveURL(/\/groups\/\?window=1h$/);

  const cookies = await context.cookies();
  const session = cookies.find((cookie) => cookie.name === "rr_console_session");
  expect(session?.httpOnly).toBe(true);
  expect(session?.sameSite).toBe("Strict");
  const stored = await page.evaluate(() => ({
    local: Object.keys(localStorage),
    session: Object.keys(sessionStorage),
  }));
  expect(stored).toEqual({ local: [], session: [] });
  expect(await page.content()).not.toContain("mock-viewer-token");
  expect(errors.join("\n")).not.toContain("mock-viewer-token");
});
