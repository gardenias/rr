import { expect, test, type Page } from "@playwright/test";
import { createRequire } from "node:module";

const routes = [
  ["overview", "Overview", "/api/v1/console/summary"],
  ["nodes", "Nodes", "/api/v1/console/nodes"],
  ["topology", "Topology", "/api/v1/console/topology"],
  ["groups", "Raft groups", "/api/v1/console/groups"],
  ["performance", "Performance", "/api/v1/console/metrics/performance"],
  ["snapshots", "Snapshots & storage", "/api/v1/console/snapshots"],
  ["lifecycle", "Lifecycle", "/api/v1/console/lifecycle"],
  ["events", "Events & alerts", "/api/v1/console/events"],
  ["sources", "Sources", "/api/v1/console/sources"],
] as const;

interface AxeViolation {
  id: string;
  impact: string | null;
  help: string;
  nodes: Array<{ target: string[]; failureSummary?: string }>;
}

const require = createRequire(__filename);
const nextRequire = createRequire(require.resolve("eslint-config-next"));
const a11yRequire = createRequire(nextRequire.resolve("eslint-plugin-jsx-a11y"));
const axePath = a11yRequire.resolve("axe-core");

async function audit(page: Page) {
  await page.addScriptTag({ path: axePath });
  return await page.evaluate(async () => {
    const axe = (window as unknown as {
      axe: {
        run: (context: Document, options: { runOnly: { type: string; values: string[] } }) => Promise<{ violations: AxeViolation[] }>;
      };
    }).axe;
    const result = await axe.run(document, {
      runOnly: { type: "tag", values: ["wcag2a", "wcag2aa", "wcag21a", "wcag21aa"] },
    });
    return result.violations;
  });
}

test.beforeEach(async ({ page }) => {
  await page.emulateMedia({ reducedMotion: "reduce" });
  const response = await page.request.post("/api/v1/console/auth/login", { data: { token: "mock-viewer-token" } });
  expect(response.ok()).toBe(true);
});

for (const [route, title, resource] of routes) {
  test(`${route} passes WCAG 2.1 AA axe checks and stays within both desktop viewports`, async ({ page }) => {
    for (const viewport of [{ width: 1440, height: 900 }, { width: 1280, height: 800 }]) {
      await page.setViewportSize(viewport);
      const ready = page.waitForResponse((response) => new URL(response.url()).pathname === resource && response.ok());
      await page.goto(`/${route}/`);
      await ready;
      await expect(page.getByRole("heading", { level: 1, name: title })).toBeVisible();
      await expect(page.locator(".route-skeleton")).toHaveCount(0);
      await page.evaluate(() => document.fonts.ready.then(() => undefined));
      expect(await audit(page), `${route} axe violations at ${viewport.width}px`).toEqual([]);
      expect(await page.evaluate(() => document.documentElement.scrollWidth > window.innerWidth), `${route} page overflow at ${viewport.width}px`).toBe(false);
    }
  });
}

test("node drawer traps keyboard focus, closes with Escape, and restores the trigger", async ({ page }) => {
  await page.goto("/nodes/");
  const trigger = page.getByRole("button", { name: "Open node 1 details" });
  await trigger.focus();
  await page.keyboard.press("Enter");
  const drawer = page.getByRole("dialog", { name: "Node 1" });
  await expect(drawer).toBeVisible();
  await expect(drawer.getByRole("button", { name: "Close drawer" })).toBeFocused();
  await page.keyboard.press("Shift+Tab");
  expect(await page.evaluate(() => document.activeElement?.closest("[role=dialog]") != null)).toBe(true);
  await page.keyboard.press("Escape");
  await expect(drawer).toHaveCount(0);
  await expect(trigger).toBeFocused();
});

test("reduced motion removes shell transitions and preserves keyboard skip navigation", async ({ page }) => {
  await page.goto("/overview/");
  const duration = await page.locator(".nav-link").first().evaluate((element) => Number.parseFloat(getComputedStyle(element).transitionDuration));
  expect(duration).toBeLessThanOrEqual(0.00001);
  await page.keyboard.press("Tab");
  const skip = page.getByRole("link", { name: "Skip to main content" });
  await expect(skip).toBeFocused();
  await page.keyboard.press("Enter");
  await expect(page.locator("#console-main")).toBeFocused();
});
