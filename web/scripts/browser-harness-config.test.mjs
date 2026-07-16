import { describe, expect, it } from "vitest";

import { readBrowserHarnessConfig } from "./browser-harness-config.mjs";

describe("browser harness configuration", () => {
  it.each([
    ["/dev", "local"],
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
});
