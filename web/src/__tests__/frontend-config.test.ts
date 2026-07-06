import { afterEach, describe, expect, it, vi } from "vitest";

import {
  getFrontendConfig,
  loadFrontendConfig,
  resolveFrontendConfig,
  setFrontendConfigForTests,
} from "../lib/frontendConfig";

describe("frontend runtime config", () => {
  afterEach(() => {
    setFrontendConfigForTests(null);
    vi.restoreAllMocks();
  });

  it("validates the production route and API prefix together", () => {
    const config = resolveFrontendConfig(
      {
        environment: "production",
        environmentLabel: "Production",
        routePath: "/prod",
        apiBase: "/prod",
      },
      new URL("https://yylx.world/prod/library"),
    );

    expect(config.apiBase).toBe("/prod");
    expect(config.apiRouteBase).toBe("https://yylx.world/prod/api");
    expect(config.environmentLabel).toBe("Production");
  });

  it("rejects a production config on the dev route", () => {
    expect(() =>
      resolveFrontendConfig(
        {
          environment: "production",
          environmentLabel: "Production",
          routePath: "/prod",
          apiBase: "/prod",
        },
        new URL("https://yylx.world/dev/monitor"),
      ),
    ).toThrow(/routePath .* does not match current route/);
  });

  it("rejects route and API base prefix mismatches", () => {
    expect(() =>
      resolveFrontendConfig(
        {
          environment: "development",
          environmentLabel: "Development / public beta",
          routePath: "/dev",
          apiBase: "/prod",
        },
        new URL("https://yylx.world/dev/monitor"),
      ),
    ).toThrow(/apiBase .* must match routePath/);
  });

  it("loads no-store runtime config before falling back to build env", async () => {
    vi.spyOn(global, "fetch").mockResolvedValue(
      new Response(
        JSON.stringify({
          environment: "development",
          environmentLabel: "Development / public beta",
          routePath: "/dev",
          apiBase: "/dev",
          apiRouteBase: "https://yylx.world/dev/api",
        }),
        {
          status: 200,
          headers: {
            "Content-Type": "application/json",
            "Cache-Control": "no-store",
          },
        },
      ),
    );
    window.history.replaceState(null, "", "/dev/settings");

    const config = await loadFrontendConfig();

    expect(global.fetch).toHaveBeenCalledWith(
      "/dev/loom-frontend-config.json",
      expect.objectContaining({ cache: "no-store" }),
    );
    expect(config.environmentLabel).toBe("Development / public beta");
    expect(getFrontendConfig().apiRouteBase).toBe("https://yylx.world/dev/api");
  });
});
