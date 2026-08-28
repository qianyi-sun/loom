import { afterEach, describe, expect, it, vi } from "vitest";

import {
  FrontendConfigLoadError,
  frontendHomePath,
  getFrontendConfig,
  loadFrontendConfig,
  resetFrontendConfigLoad,
  resolveFrontendConfig,
  setFrontendConfigForTests,
} from "../lib/frontendConfig";

describe("frontend runtime config", () => {
  afterEach(() => {
    window.history.replaceState(null, "", "/");
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

  it("rejects a staging config on the development route", () => {
    expect(() =>
      resolveFrontendConfig(
        {
          environment: "staging",
          environmentLabel: "Staging",
          routePath: "/dev",
          apiBase: "/dev",
        },
        new URL("https://yylx.world/dev/monitor"),
      ),
    ).toThrow(/staging frontend config does not use its canonical routePath/);
  });

  it("rejects route and API base prefix mismatches", () => {
    expect(() =>
      resolveFrontendConfig(
        {
          environment: "development",
          environmentLabel: "Development / staging",
          routePath: "/dev",
          apiBase: "/prod",
        },
        new URL("https://yylx.world/dev/monitor"),
      ),
    ).toThrow(/apiBase .* must match routePath/);
  });

  it("loads no-store runtime config before falling back to build env", async () => {
    vi.spyOn(globalThis, "fetch").mockResolvedValue(
      new Response(
        JSON.stringify({
          environment: "development",
          environmentLabel: "Development / staging",
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

    expect(globalThis.fetch).toHaveBeenCalledWith(
      "/dev/loom-frontend-config.json",
      expect.objectContaining({ cache: "no-store" }),
    );
    expect(config.environmentLabel).toBe("Development / staging");
    expect(getFrontendConfig().apiRouteBase).toBe("https://yylx.world/dev/api");
  });

  it("loads development config from a dedicated origin root", async () => {
    vi.spyOn(globalThis, "fetch").mockResolvedValue(
      new Response(
        JSON.stringify({
          environment: "development",
          environmentLabel: "Personal development",
          routePath: "",
          apiBase: "",
          apiRouteBase: "https://loom-service.dev.yylx.world/api",
        }),
        { status: 200, headers: { "Content-Type": "application/json" } },
      ),
    );
    window.history.replaceState(null, "", "/auth/setup");

    const config = await loadFrontendConfig();

    expect(globalThis.fetch).toHaveBeenCalledWith(
      "/loom-frontend-config.json",
      expect.objectContaining({ cache: "no-store" }),
    );
    expect(config).toMatchObject({
      environment: "development",
      routePath: "",
      apiRouteBase: "https://loom-service.dev.yylx.world/api",
    });
  });

  it("loads the exact isolated rehearsal runtime config", async () => {
    const rehearsalId = "5".repeat(24);
    const routePath = `/staging/rehearsal/${rehearsalId}`;
    vi.spyOn(globalThis, "fetch").mockResolvedValue(
      new Response(
        JSON.stringify({
          environment: "staging",
          environmentLabel: "Staging rehearsal",
          routePath,
          apiBase: routePath,
          apiRouteBase: `https://yylx.world${routePath}/api`,
        }),
        { status: 200 },
      ),
    );
    window.history.replaceState(null, "", `${routePath}/admin/access`);

    const config = await loadFrontendConfig();

    expect(globalThis.fetch).toHaveBeenCalledWith(
      `${routePath}/loom-frontend-config.json`,
      expect.objectContaining({ cache: "no-store" }),
    );
    expect(config.routePath).toBe(routePath);
    expect(config.apiRouteBase).toBe(`https://yylx.world${routePath}/api`);
  });

  it("deduplicates a runtime-config request within one startup attempt", async () => {
    window.history.replaceState(null, "", "/dev/settings");
    const fetchMock = vi.spyOn(globalThis, "fetch").mockResolvedValue(
      new Response(
        JSON.stringify({
          environment: "development",
          environmentLabel: "Development / staging",
          routePath: "/dev",
          apiBase: "/dev",
        }),
        { status: 200, headers: { "Content-Type": "application/json" } },
      ),
    );

    const first = loadFrontendConfig();
    const second = loadFrontendConfig();

    expect(first).toBe(second);
    await expect(Promise.all([first, second])).resolves.toHaveLength(2);
    expect(fetchMock).toHaveBeenCalledTimes(1);
  });

  it("fails closed on a prefixed HTTP error without reading its body", async () => {
    window.history.replaceState(null, "", "/prod/library");
    const response = new Response("upstream-body-with-loom_api_secretvalue123456", {
      status: 503,
      headers: { "Content-Type": "text/plain" },
    });
    const jsonSpy = vi.spyOn(response, "json");
    vi.spyOn(globalThis, "fetch").mockResolvedValue(response);

    const failure = await loadFrontendConfig().catch((error: unknown) => error);

    expect(failure).toBeInstanceOf(FrontendConfigLoadError);
    expect(failure).toMatchObject({ kind: "http", status: 503 });
    expect(String(failure)).not.toContain("upstream-body");
    expect(jsonSpy).not.toHaveBeenCalled();
  });

  it("classifies network and invalid payload failures without raw details", async () => {
    window.history.replaceState(null, "", "/dev/monitor");
    vi.spyOn(globalThis, "fetch").mockRejectedValueOnce(
      new Error("loom_api_abcdefghijklmnopqrstuvwxyz012345"),
    );

    const networkFailure = await loadFrontendConfig().catch(
      (error: unknown) => error,
    );
    expect(networkFailure).toMatchObject({ kind: "network" });
    expect(String(networkFailure)).not.toContain("loom_api_");

    resetFrontendConfigLoad();
    vi.mocked(globalThis.fetch).mockResolvedValueOnce(
      new Response("loom_reset_abcdefghijklmnopqrstuvwxyz012345", {
        status: 200,
        headers: { "Content-Type": "application/json" },
      }),
    );
    const invalidFailure = await loadFrontendConfig().catch(
      (error: unknown) => error,
    );
    expect(invalidFailure).toMatchObject({ kind: "invalid" });
    expect(String(invalidFailure)).not.toContain("loom_reset_");
  });

  it("keeps the missing-config fallback limited to the unprefixed dev server", async () => {
    window.history.replaceState(null, "", "/");
    vi.spyOn(globalThis, "fetch").mockRejectedValue(new Error("offline"));

    await expect(loadFrontendConfig()).resolves.toMatchObject({
      environment: "local",
      routePath: "",
    });
  });

  it("keeps recovery home links inside the active route prefix", () => {
    expect(frontendHomePath(new URL("https://loom.test/"))).toBe("/");
    expect(frontendHomePath(new URL("https://loom.test/dev/settings"))).toBe(
      "/dev/",
    );
    expect(frontendHomePath(new URL("https://loom.test/staging"))).toBe(
      "/staging/",
    );
    expect(
      frontendHomePath(new URL("https://loom.test/staging/library")),
    ).toBe("/staging/");
    expect(frontendHomePath(new URL("https://loom.test/prod/monitor"))).toBe(
      "/prod/",
    );
  });

  it("keeps recovery home links inside an exact rehearsal route prefix", () => {
    const rehearsalId = "a5".repeat(12);
    const routePath = `/staging/rehearsal/${rehearsalId}`;

    expect(frontendHomePath(new URL(`https://loom.test${routePath}`))).toBe(
      `${routePath}/`,
    );
    expect(
      frontendHomePath(new URL(`https://loom.test${routePath}/admin/access`)),
    ).toBe(`${routePath}/`);
  });

  it("fails closed for malformed rehearsal route near-misses", () => {
    const nearMisses = [
      "/staging/rehearsal",
      `/staging/rehearsal/${"a".repeat(23)}`,
      `/staging/rehearsal/${"A".repeat(24)}`,
      `/staging/rehearsal/${"a".repeat(25)}`,
    ];

    for (const pathname of nearMisses) {
      expect(frontendHomePath(new URL(`https://loom.test${pathname}`))).toBe(
        "/",
      );
    }
  });
});
