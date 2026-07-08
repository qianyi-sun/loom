import { afterEach, describe, expect, it } from "vitest";

import { setFrontendConfigForTests } from "../../lib/frontendConfig";
import { currentServerOrigin } from "../../lib/serverOrigin";

describe("currentServerOrigin", () => {
  afterEach(() => setFrontendConfigForTests(null));

  it("includes the configured route path for path-prefixed deployments", () => {
    setFrontendConfigForTests({
      environment: "staging",
      environmentLabel: "Staging",
      routePath: "/dev",
      apiBase: "/dev",
      apiRouteBase: `${window.location.origin}/dev/api`,
    });

    expect(currentServerOrigin()).toBe(`${window.location.origin}/dev`);
  });

  it("includes the production route path for first-prod deployments", () => {
    setFrontendConfigForTests({
      environment: "prod",
      environmentLabel: "Production",
      routePath: "/prod",
      apiBase: "/prod",
      apiRouteBase: `${window.location.origin}/prod/api`,
    });

    expect(currentServerOrigin()).toBe(`${window.location.origin}/prod`);
  });

  it("keeps the bare origin for root-route deployments", () => {
    setFrontendConfigForTests({
      environment: "local",
      environmentLabel: "Local development",
      routePath: "",
      apiBase: "",
      apiRouteBase: `${window.location.origin}/api`,
    });

    expect(currentServerOrigin()).toBe(window.location.origin);
  });
});
