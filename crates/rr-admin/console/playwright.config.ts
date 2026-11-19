import { defineConfig } from "@playwright/test";

const mockPort = process.env.RR_CONSOLE_MOCK_E2E_PORT ?? "4310";
const mockBaseUrl = `http://127.0.0.1:${mockPort}`;

export default defineConfig({
  testDir: "./e2e/mock",
  testMatch: "**/*.spec.ts",
  fullyParallel: false,
  workers: 1,
  reporter: "list",
  use: {
    baseURL: mockBaseUrl,
    viewport: { width: 1440, height: 900 },
    trace: "retain-on-failure",
    screenshot: "only-on-failure",
  },
  webServer: {
    command: "node e2e/mock/server.mjs",
    url: `${mockBaseUrl}/login/`,
    reuseExistingServer: false,
    timeout: 30_000,
    env: { MOCK_REQUIRE_AUTH: "1", PORT: mockPort },
  },
});
