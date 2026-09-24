import { expect, test } from "./fixtures/guardedTest";

test.use({ contextOptions: { reducedMotion: "reduce" } });

test("Pipeline list preserves filtered first-middle-last pages through reload and detail return", async ({ apiHarness, browserHarness, page }) => {
  const runId = "list-run-100";
  const budget = { max_wall_seconds: 300, max_gpu_seconds: 0, max_provider_cost_usd: "1.00", max_artifact_bytes: 1024, max_stage_runs: 1, max_attempts_total: 1 };
  const rows = Array.from({ length: 101 }, (_, index) => ({
    id: `list-run-${index}`, display_name: `Pipeline result ${index}`, recipe: { name: "fixture", version: 1, digest: `sha256:${"a".repeat(64)}` }, state: "finished", result: "succeeded", completed_stage_runs: 0, total_stage_runs: 0, domain_outcomes: {}, budget: null, created_at: "2026-09-23T00:00:00Z", finished_at: "2026-09-23T00:01:00Z",
  }));
  await apiHarness.install({ role: "user", overrides: [
    ...[0, 50, 100].map((start) => ({ name: `Pipeline list page ${start / 50 + 1}`, method: "GET" as const, path: `/api/v1/pipeline-runs?state=finished&${start ? `cursor=list-${start}&` : ""}limit=50`, count: start === 100 ? 2 : 1, response: { kind: "json" as const, status: 200, body: { items: rows.slice(start, start + 50), next_cursor: start < 100 ? `list-${start + 50}` : null } } })),
    { name: "Pipeline filtered empty page", method: "GET", path: "/api/v1/pipeline-runs?state=finished&recipe=missing%401&limit=50", response: { kind: "json", status: 200, body: { items: [], next_cursor: null } } },
    { name: "selected Pipeline detail", method: "GET", path: `/api/v1/pipeline-runs/${runId}`, response: { kind: "json", status: 200, body: {
      ...rows[100], created_by_user_id: null, reason: null, graph_digest: `sha256:${"b".repeat(64)}`, control_binding_snapshots_digest: `sha256:${"c".repeat(64)}`, retry_of_pipeline_run_id: null, retry_from_stage_run_id: null, started_at: "2026-09-23T00:00:00Z", source_budget: budget,
      topology: [], progress: { total_stage_runs: 0, completed_stage_runs: 0, states: {}, domain_outcomes: {}, nodes: {} },
    } } },
    { name: "selected Pipeline stages", method: "GET", path: `/api/v1/pipeline-runs/${runId}/stages?limit=200`, response: { kind: "json", status: 200, body: { items: [], next_cursor: null } } },
    { name: "selected Pipeline artifacts", method: "GET", path: `/api/v1/pipeline-runs/${runId}/artifacts?limit=100`, response: { kind: "json", status: 200, body: { items: [], next_cursor: null } } },
    { name: "selected Pipeline terminal events", method: "GET", path: `/api/v1/pipeline-runs/${runId}/events?after_seq=0&limit=500`, response: { kind: "json", status: 200, body: { events: [], next_after_seq: 0, terminal: true, retry_after_ms: null } } },
  ] });
  await page.goto(`${browserHarness.baseURL}/pipelines?state=finished`);
  await expect(page.getByRole("link", { name: "Pipeline result 0", exact: true })).toBeVisible();
  await page.getByRole("button", { name: /next page/i }).click();
  await expect(page.getByRole("link", { name: "Pipeline result 50", exact: true })).toBeVisible();
  await page.getByRole("button", { name: /next page/i }).click();
  await expect(page.getByRole("link", { name: "Pipeline result 100", exact: true })).toBeVisible();
  await expect(page.getByRole("status").filter({ hasText: "Page 3, end of results" })).toBeVisible();
  const lastPage = page.url();
  await page.reload();
  await expect(page.getByRole("status").filter({ hasText: "Page 3, end of results" })).toBeVisible();
  await page.getByRole("link", { name: "Pipeline result 100", exact: true }).click();
  await expect(page.getByRole("heading", { name: "Pipeline result 100", exact: true })).toBeVisible();
  await page.getByRole("link", { name: "← Pipelines", exact: true }).click();
  await expect(page).toHaveURL(lastPage);
  await expect(page.getByRole("status").filter({ hasText: "Page 3, end of results" })).toBeVisible();
  await page.getByRole("textbox", { name: "Recipe", exact: true }).fill("missing@1");
  await page.getByRole("button", { name: "Apply", exact: true }).click();
  await expect(page.getByText("No pipeline runs", { exact: true })).toBeVisible();
  expect(new URL(page.url()).searchParams.has("cursor")).toBe(false);
  await expect(page.getByRole("table", { name: "Pipeline runs", exact: true })).toHaveCount(0);
});
