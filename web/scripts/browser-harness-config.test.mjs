import { describe, expect, it } from "vitest";

import {
  browserWebServerConfig,
  readBrowserHarnessConfig,
} from "./browser-harness-config.mjs";

describe("browser harness configuration", () => {
  it.each([
    ["/dev", "development"],
    ["/prod", "production"],
  ])("validates and derives %s routes", (routePrefix, runtimeEnvironment) => {
    expect(
      readBrowserHarnessConfig({
        LOOM_E2E_ORIGIN: "http://localhost:4817",
        LOOM_E2E_ROUTE_PREFIX: routePrefix,
      }),
    ).toMatchObject({
      origin: "http://localhost:4817",
      routePrefix,
      baseURL: `http://localhost:4817${routePrefix}`,
      apiBaseURL: `http://localhost:4817${routePrefix}/api`,
      configURL: `http://localhost:4817${routePrefix}/loom-frontend-config.json`,
      runtimeEnvironment,
    });
  });

  it.each([
    ["LOOM_E2E_ORIGIN", "https://127.0.0.1:4173"],
    ["LOOM_E2E_ORIGIN", "http://example.test:4173"],
    ["LOOM_E2E_ORIGIN", "http://user:secret@127.0.0.1:4173"],
    ["LOOM_E2E_ROUTE_PREFIX", "/staging"],
    ["LOOM_E2E_ROUTE_PREFIX", "/dev/"],
  ])("rejects invalid %s values", (name, value) => {
    expect(() => readBrowserHarnessConfig({ [name]: value })).toThrow();
  });

  it("never reuses an ambient server for browser evidence", () => {
    const harness = readBrowserHarnessConfig();
    expect(browserWebServerConfig(harness)).toEqual({
      command:
        "node scripts/build-browser-test.mjs && node scripts/prefix-preview-server.mjs",
      url: harness.configURL,
      timeout: 120_000,
      reuseExistingServer: false,
    });
  });
});
