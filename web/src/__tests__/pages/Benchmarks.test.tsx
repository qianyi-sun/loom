import { screen } from "@testing-library/react";
import { beforeEach, describe, expect, it, vi } from "vitest";

import { setFrontendConfigForTests } from "../../lib/frontendConfig";
import Benchmarks from "../../pages/Benchmarks";
import { renderWithProviders } from "../../test-utils/renderWithProviders";

const BENCHMARKS_RESPONSE = {
  items: [
    {
      id: "aime-25",
      display_name: "AIME 2025",
      series: "aime",
      license_spdx: "MIT",
      upstream_kind: "huggingface",
      upstream_locator: "loom/aime-25",
      imported_at: "2026-06-20T12:00:00Z",
      task_count: 30,
      raw_task_count: 30,
      valid_task_config_count: 30,
      invalid_task_config_count: 0,
      readiness_state: "runnable",
      readiness_label: "Ready",
      readiness_message: "30 runnable tasks are registered.",
      selectable: true,
      blocker_reason: null,
    },
    {
      id: "swe-bench-verified",
      display_name: "SWE-Bench Verified",
      series: "swe-bench",
      license_spdx: "MIT",
      upstream_kind: "entrypoint",
      upstream_locator: "loom_benchmarks.swe_bench",
      imported_at: "2026-06-20T12:00:00Z",
      task_count: 0,
      raw_task_count: 0,
      valid_task_config_count: 0,
      invalid_task_config_count: 0,
      readiness_state: "blocked",
      readiness_label: "Needs publish",
      readiness_message: "Publish/register tasks before selecting this benchmark.",
      selectable: false,
      blocker_reason: "missing_tasks",
    },
  ],
  next_cursor: null,
};

function setupFetch(): ReturnType<typeof vi.spyOn> {
  return vi
    .spyOn(global, "fetch")
    .mockImplementation((input: RequestInfo | URL) => {
      const url = typeof input === "string" ? input : String(input);
      if (url.includes("/api/v1/benchmarks")) {
        return Promise.resolve(
          new Response(JSON.stringify(BENCHMARKS_RESPONSE), {
            status: 200,
            headers: { "Content-Type": "application/json" },
          }),
        );
      }
      return Promise.reject(new Error(`unexpected fetch ${url}`));
    });
}

describe("Benchmarks page", () => {
  beforeEach(() => {
    window.localStorage.clear();
    window.localStorage.setItem("loom_token", "test-token");
    setFrontendConfigForTests(null);
    vi.restoreAllMocks();
  });

  it("renders readiness context, hidden-route guidance, and safe CLI snippets", async () => {
    setFrontendConfigForTests({
      environment: "staging",
      environmentLabel: "Staging",
      routePath: "/dev",
      apiBase: "/dev",
      apiRouteBase: `${window.location.origin}/dev/api`,
    });
    setupFetch();
    renderWithProviders(<Benchmarks />, { route: "/benchmarks" });

    expect(await screen.findByText("AIME 2025")).toBeInTheDocument();
    expect(screen.getByText("SWE-Bench Verified")).toBeInTheDocument();
    expect(screen.getByText("Benchmark catalog guidance")).toBeInTheDocument();
    expect(screen.getByText(/hidden power-user view/i)).toBeInTheDocument();
    expect(screen.getByText("30 runnable tasks are registered.")).toBeInTheDocument();
    expect(
      screen.getByText("Publish/register tasks before selecting this benchmark."),
    ).toBeInTheDocument();

    const pageText = document.body.textContent ?? "";
    expect(pageText).toContain("loom datasets list --remote");
    expect(pageText).toContain(`--server-url ${window.location.origin}/dev`);
    expect(pageText).toContain("--token env:LOOM_API_TOKEN");
    expect(pageText).toContain('loom datasets audit --all --db-url "$LOOM_DB_URL"');
    expect(pageText).toContain("loom datasets sync-config");
    expect(pageText).not.toMatch(/\bsk-[A-Za-z0-9_-]+/i);
    expect(pageText).not.toMatch(/\bAuthorization:\s*Bearer\s+\S+/i);
    expect(pageText).not.toMatch(/X-Amz-Signature=/i);
  });

  it("requests hidden-route data with empty catalog rows included", async () => {
    const spy = setupFetch();
    renderWithProviders(<Benchmarks />, { route: "/benchmarks" });
    await screen.findByText("AIME 2025");

    const benchmarkCalls = spy.mock.calls.filter(([url]) =>
      String(url).includes("/api/v1/benchmarks"),
    );
    expect(benchmarkCalls.length).toBeGreaterThan(0);
    expect(
      benchmarkCalls.some(([url]) => String(url).includes("include_empty=true")),
    ).toBe(true);
  });
});
