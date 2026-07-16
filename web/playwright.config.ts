import { defineConfig, devices } from "@playwright/test";

import { readBrowserHarnessConfig } from "./scripts/browser-harness-config.mjs";

const harness = readBrowserHarnessConfig();

export default defineConfig({
  testDir: "./e2e",
  timeout: 30_000,
  expect: { timeout: 10_000 },
  fullyParallel: true,
  forbidOnly: true,
  retries: 0,
  reporter: [["line"]],
  use: {
    baseURL: harness.baseURL,
    screenshot: "only-on-failure",
    trace: "retain-on-failure",
  },
  projects: [
    {
      name: "chromium-desktop",
      use: { ...devices["Desktop Chrome"], viewport: { width: 1440, height: 900 } },
    },
    {
      name: "chromium-mobile",
      use: { ...devices["Desktop Chrome"], viewport: { width: 390, height: 844 } },
    },
  ],
  webServer: {
    command: "node scripts/build-browser-test.mjs && node scripts/prefix-preview-server.mjs",
    url: harness.configURL,
    timeout: 120_000,
    reuseExistingServer: !process.env.CI,
  },
});
