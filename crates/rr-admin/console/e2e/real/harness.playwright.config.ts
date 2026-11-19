import { defineConfig } from "@playwright/test";
import { resolve } from "node:path";
import { defaultArtifactsRoot } from "./harness";

export default defineConfig({
  testDir: __dirname,
  testMatch: "harness.spec.ts",
  fullyParallel: false,
  workers: 1,
  timeout: 180_000,
  expect: { timeout: 20_000 },
  reporter: "list",
  outputDir: resolve(defaultArtifactsRoot, "harness-results"),
});
