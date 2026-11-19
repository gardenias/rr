import { defineConfig } from "@playwright/test";
import { resolve } from "node:path";
import { consoleBaseUrl, defaultArtifactsRoot } from "./harness";

export default defineConfig({
  testDir: __dirname,
  testMatch: "cluster.spec.ts",
  globalSetup: resolve(__dirname, "global-setup.ts"),
  fullyParallel: false,
  workers: 1,
  timeout: 180_000,
  expect: { timeout: 20_000 },
  reporter: "list",
  outputDir: resolve(defaultArtifactsRoot, "playwright-results"),
  use: {
    baseURL: consoleBaseUrl,
    viewport: { width: 1440, height: 900 },
    actionTimeout: 20_000,
    navigationTimeout: 30_000,
    trace: "retain-on-failure",
    screenshot: "only-on-failure",
    video: "retain-on-failure",
  },
});
