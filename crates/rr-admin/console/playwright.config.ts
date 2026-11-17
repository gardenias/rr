import { defineConfig } from "@playwright/test";

export default defineConfig({
  testDir: "./e2e/mock",
  testMatch: "**/*.spec.ts",
  fullyParallel: false,
  workers: 2,
  reporter: "list",
  use: {
    baseURL: "http://127.0.0.1:4310",
    viewport: { width: 1440, height: 900 },
    trace: "retain-on-failure",
    screenshot: "only-on-failure",
  },
  webServer: {
    command: "node e2e/mock/server.mjs",
    url: "http://127.0.0.1:4310/login/",
    reuseExistingServer: false,
    timeout: 30_000,
    env: { MOCK_REQUIRE_AUTH: "1" },
  },
});
