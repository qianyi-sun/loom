import { readFileSync } from "node:fs";

import { expect, test } from "./fixtures/guardedTest";

const run = JSON.parse(
  readFileSync(new URL("./fixtures/pipeline-1000-stage.json", import.meta.url), "utf8"),
) as { id: string; stages: Array<Record<string, unknown>> };
const lastStage = run.stages.at(-1)!;

test("1000-stage Pipeline detail becomes interactive and keeps rows bounded", async ({
  apiHarness,
  browserHarness,
  page,
}) => {
  await apiHarness.install({
    role: "user",
    overrides: [
      {
        name: "1000-stage Pipeline detail",
        method: "GET",
        path: `/api/v1/pipeline-runs/${run.id}`,
        response: { kind: "json", status: 200, body: run },
      },
      {
        name: "terminal Pipeline event cursor",
        method: "GET",
        path: `/api/v1/pipeline-runs/${run.id}/events?after_seq=0&limit=500`,
        response: {
          kind: "json",
          status: 200,
          body: { events: [], next_after_seq: 0, terminal: true, retry_after_ms: null },
        },
      },
      {
        name: "selected Stage detail only",
        method: "GET",
        path: `/api/v1/pipeline-stage-runs/${String(lastStage.id)}`,
        response: {
          kind: "json",
          status: 200,
          body: {
            ...lastStage,
            pipeline_run_id: run.id,
            execution_spec_digest: `sha256:${"5".repeat(64)}`,
            input_bindings_digest: `sha256:${"6".repeat(64)}`,
            resource_profile_digest: `sha256:${"7".repeat(64)}`,
            request_renderer_digest: null,
            latest_checkpoint_artifact_id: null,
            artifacts: [],
          },
        },
      },
      {
        name: "selected Stage Attempt list only",
        method: "GET",
        path: `/api/v1/pipeline-stage-runs/${String(lastStage.id)}/attempts`,
        response: { kind: "json", status: 200, body: { items: [] } },
      },
    ],
  });

  await page.addInitScript(() => performance.mark("loom-pipeline-detail-navigation-start"));
  await page.goto(`${browserHarness.baseURL}/pipelines/${run.id}`);
  await expect(page.getByRole("heading", { name: "1000-stage performance fixture" })).toBeVisible();
  await expect(page.getByText("1000 StageRuns")).toBeVisible();
  await expect.poll(async () => page.evaluate(() => performance.getEntriesByName("loom-pipeline-detail-interactive").length)).toBe(1);
  const interactiveMs = await page.evaluate(() => {
    const start = performance.getEntriesByName("loom-pipeline-detail-navigation-start").at(-1);
    const interactive = performance.getEntriesByName("loom-pipeline-detail-interactive").at(-1);
    return start && interactive
      ? interactive.startTime - start.startTime
      : Number.POSITIVE_INFINITY;
  });
  expect(interactiveMs).toBeLessThan(2_000);

  const virtualRows = page.locator('[role="table"] [role="row"]');
  expect(await virtualRows.count()).toBeLessThanOrEqual(39);
  await page.getByLabel("Node key").fill("node-09");
  await expect(page.getByText("50 StageRuns")).toBeVisible();
  await page.getByLabel("Node key").fill("");

  await page.getByRole("button", { name: "Next" }).click();
  await page.getByRole("button", { name: "Next" }).click();
  await page.getByRole("button", { name: "Next" }).click();
  const openedAt = Date.now();
  const viewport = page.locator('[role="table"]');
  await viewport.evaluate((element) => {
    element.scrollTop = element.scrollHeight;
    element.dispatchEvent(new Event("scroll", { bubbles: true }));
  });
  const target = page.locator('[data-stage-row="249"]');
  await expect(target).toBeVisible();
  await target.dblclick();
  await expect(page.getByRole("heading", { name: "Attempts (0)" })).toBeVisible();
  expect(Date.now() - openedAt).toBeLessThanOrEqual(500);
});
