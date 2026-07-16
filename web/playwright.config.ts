import { defineConfig, devices } from "@playwright/test";

export default defineConfig({
  testDir: "./e2e",
  timeout: 30_000,
  expect: { timeout: 10_000 },
  fullyParallel: true,
  forbidOnly: true,
  retries: 0,
  reporter: [["line"]],
  use: {
    baseURL: "http://127.0.0.1:4173/dev",
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
    command: "VITE_E2E_ROUTE_BASE=/dev/ npm run build && node scripts/prefix-preview-server.mjs",
    url: "http://127.0.0.1:4173/dev/loom-frontend-config.json",
    timeout: 120_000,
    reuseExistingServer: !process.env.CI,
  },
});
