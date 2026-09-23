import AxeBuilder from "@axe-core/playwright";
import { expect, test } from "./fixtures/guardedTest";

test.use({ contextOptions: { reducedMotion: "reduce" } });

const longName = `Research evaluation ${"long-unbroken-name-".repeat(14)}`;
const batch = {
  id: "quality-batch",
  name: longName,
  description: longName,
  team_id: "team-eai",
  owner_team: { id: "team-eai", name: "Research" },
  task_filter: {},
  trial_config: {},
  backend: "nebius",
  combinations: [],
  provider_connection_id: null,
  provider_model_id: null,
  state: "finished",
  result_status: "succeeded",
  visibility: "team",
  share_status: "shared",
  source_provenance: [],
  expected_trial_count: 1,
  created_by_token_prefix: "fixture",
  created_at: "2026-09-23T00:00:00Z",
  finished_at: "2026-09-23T00:01:00Z",
  trial_summary: { succeeded: 1 },
  aggregate_reward: 1,
  combination_summary: [],
  artifact_summary: {
    reports: 0,
    trajectories: 0,
    reusable_outputs: 0,
    logs_diagnostics: 0,
    raw_diagnostics: 0,
  },
  artifact_inventory: {
    reports: [],
    trajectories: [],
    reusable_outputs: [],
    logs_diagnostics: [],
    raw_diagnostics: [],
  },
};

function contrast(left: string, right: string): number {
  const luminance = (color: string) => {
    const rgb = color
      .match(/[\d.]+/g)!
      .slice(0, 3)
      .map(Number)
      .map((value) => {
        const channel = value / 255;
        return channel <= 0.04045 ? channel / 12.92 : ((channel + 0.055) / 1.055) ** 2.4;
      });
    return rgb[0] * 0.2126 + rgb[1] * 0.7152 + rgb[2] * 0.0722;
  };
  const values = [luminance(left), luminance(right)].sort((a, b) => a - b);
  return (values[1] + 0.05) / (values[0] + 0.05);
}

for (const scenario of ["runs", "run-detail", "provider-detail", "pipeline-artifacts"] as const) {
  test(`${scenario} retains long content and actions within the viewport`, async ({
    apiHarness,
    browserHarness,
    page,
  }, testInfo) => {
    const detail = scenario === "run-detail";
    const provider = scenario === "provider-detail";
    const pipeline = scenario === "pipeline-artifacts";
    const path = provider
      ? "/providers/quality-provider"
      : detail
        ? "/library/batches/quality-batch"
        : pipeline
          ? "/library?producer_kind=pipeline"
          : "/library";
    const apiPath = provider
      ? "/api/v1/provider-connections/quality-provider"
      : detail
        ? "/api/v1/run-library/batches/quality-batch"
        : pipeline
          ? "/api/v1/run-library/artifacts?producer_kind=pipeline"
          : "/api/v1/run-library/batches?limit=50";
    const body = provider
      ? {
          id: "quality-provider",
          name: longName,
          type: "openai-compatible",
          status: "valid",
          base_url: "https://models.example.test/v1",
          allowed_models: [longName],
          created_at: batch.created_at,
        }
      : detail
        ? batch
        : {
            items: pipeline
              ? [
                  {
                    id: "quality-artifact",
                    name: longName,
                    key: longName,
                    artifact_type: "metric_table",
                    size: 24,
                    producer_kind: "pipeline",
                    pipeline_recipe: "research@1",
                    pipeline_result: "succeeded",
                    share_status: "shared",
                    safety_state: "safe",
                    redaction_state: "not_required",
                  },
                ]
              : [batch],
            next_cursor: null,
          };
    await apiHarness.install({
      role: "user",
      overrides: [
        ...(detail
          ? [
              {
                name: "delivery export",
                method: "GET",
                path: "/api/v1/batches/quality-batch/delivery-export",
                response: { kind: "json" as const, status: 200, body: { status: "not_ready" } },
              },
            ]
          : []),
        { name: scenario, method: "GET", path: apiPath, response: { kind: "json", status: 200, body } },
      ],
    });
    await page.goto(`${browserHarness.baseURL}${path}`);
    await expect(page.locator("#root")).toHaveAttribute("data-loom-auth-settled", "true");
    await expect(page.getByText(longName, { exact: true }).first()).toBeVisible();
    await expect
      .poll(() => page.evaluate(() => document.documentElement.scrollWidth <= innerWidth))
      .toBe(true);
    for (const table of await page.getByRole("table").all()) {
      await expect(table).toHaveAccessibleName(/.+/);
    }
    if (scenario === "runs") {
      const search = page.getByRole("textbox", { name: "Search" });
      const style = await search.evaluate((el) => {
        const css = getComputedStyle(el);
        return { color: css.color, background: css.backgroundColor, border: css.borderTopColor };
      });
      expect(contrast(style.color, style.background)).toBeGreaterThanOrEqual(4.5);
      expect(contrast(style.border, style.background)).toBeGreaterThanOrEqual(3);
      expect(
        await page
          .getByRole("table", { name: "Runs", exact: true })
          .locator("tbody tr")
          .first()
          .evaluate((el) => el.getBoundingClientRect().height),
      ).toBeLessThan(500);
    }
    if (pipeline && testInfo.project.name === "chromium-mobile") {
      const scroll = page.getByRole("region", { name: "Pipeline artifacts scroll area" });
      await scroll.focus();
      await page.keyboard.press("ArrowRight");
      await expect.poll(() => scroll.evaluate((el) => el.scrollLeft)).toBeGreaterThan(0);
    }
    await expect
      .poll(() => page.locator(".animate-fade-in").evaluate((el) => Number(getComputedStyle(el).opacity)))
      .toBe(1);
    const result = await new AxeBuilder({ page }).analyze();
    expect(result.violations.filter((v) => v.impact === "serious" || v.impact === "critical"
      || v.id === "heading-order" || v.id === "target-size")).toEqual([]);
    await page.screenshot({ path: testInfo.outputPath(`${scenario}.png`), fullPage: true });
  });
}

test("Run Library exposes loading and error states without overflow", async ({
  apiHarness,
  browserHarness,
  page,
  failureSink,
}, testInfo) => {
  failureSink.expectDiagnostic({
    kind: "console",
    level: "error",
    message: "Failed to load resource: the server responded with a status of 503 (Service Unavailable)",
    count: 2,
  });
  await apiHarness.install({
    role: "user",
    overrides: [
      {
        name: "library unavailable",
        method: "GET",
        path: "/api/v1/run-library/batches?limit=50",
        count: 2,
        response: { kind: "json", status: 503, delayMs: 500, body: { detail: longName } },
      },
    ],
  });
  await page.goto(`${browserHarness.baseURL}/library`);
  await expect(page.getByText("Loading…", { exact: true })).toBeVisible();
  await page.screenshot({ path: testInfo.outputPath("loading.png"), fullPage: true });
  await expect(page.getByRole("alert")).toBeVisible();
  await expect.poll(() => page.evaluate(() => document.documentElement.scrollWidth <= innerWidth)).toBe(true);
  const result = await new AxeBuilder({ page }).analyze();
  expect(result.violations.filter((v) => v.impact === "serious" || v.impact === "critical")).toEqual([]);
  await page.screenshot({ path: testInfo.outputPath("error.png"), fullPage: true });
});
