import { monitorSummary } from "./fixtures/api";
import { expect, test } from "./fixtures/guardedTest";

test("member can distinguish image preparation and shared-node scheduling", async ({
  apiHarness, browserHarness, page,
}, testInfo) => {
  const resources = { cpu_millis: 1000, memory_mib: 2048, storage_mib: 2048 };
  const progress = {
    trial_count: 3,
    stages: { image_preparation: 2, execution_wait: 0, starting: 0, running: 1, archiving: 0 },
    images: { image_count: 2, waiting_trials: 2, states: { running: 1, ready: 1 } },
  };
  const summary = {
    ...monitorSummary(), progress,
    service_execution: {
      activity: null,
      targets: [{
        target_id: "primary", pool_id: "nebius-cpu", environment: "development", region: "eu-north1",
        desired_state: "active", health_status: "healthy", policy: { max_nodes: 10 },
        observation: { is_fresh: true, observed_at: new Date().toISOString(), active_nodes: 1 },
        resource_profile: { immediate_executable_slots: 4 }, blockers: [], command_backlog: 0,
      }],
    },
  };
  const fixture = await apiHarness.install({
    role: "user",
    overrides: [
      { name: "native summary", method: "GET", path: "/api/v1/monitor/summary?view=trials",
        response: { kind: "json", status: 200, body: summary } },
      { name: "authorized node placement", method: "GET", path: "/api/v1/monitor/placement?target_id=primary&view=trials",
        response: { kind: "json", status: 200, body: {
          available: true, is_fresh: true, observed_at: new Date().toISOString(),
          build_concurrency_limit: 16, pending: [], pending_builds: 1, pending_executions: 0,
          nodes: [{ id: "1", label: "Node 1", ready: true, draining: false, deleting: false,
            unschedulable: false, allocatable: { cpu_millis: 4000, memory_mib: 8192, storage_mib: 32768 },
            requested: resources, build_pods: 1, execution_pods: 1,
            workloads: [{ kind: "build", trial_id: "build-trial", label: "example/task",
              state: "build", requests: resources, wait_message: null }] }],
        } } },
    ],
  });
  await page.goto(`${browserHarness.baseURL}/monitor?view=trials`);
  await expect(page.getByRole("heading", { name: "Task progress" })).toBeVisible();
  await expect(page.getByText("2 trials waiting for images.", { exact: false })).toBeVisible();
  expect(fixture.ledger.some((row) => row.path.includes("/monitor/placement"))).toBe(false);
  await page.getByText("Nodes, scheduling and capacity diagnostics", { exact: true }).click();
  await page.getByText("Shared nodes and scheduling", { exact: true }).click();
  await expect(page.getByRole("heading", { name: "Node 1 · Ready" })).toBeVisible();
  await expect(page.getByText("1 build Pods · 1 execution Pods")).toBeVisible();
  await expect(page.getByRole("link", { name: "example/task" })).toHaveAttribute(
    "href", `${browserHarness.routePrefix}/trials/build-trial`);
  await expect(page.getByRole("link", { name: "Preparing image 2" })).toHaveAttribute(
    "href", `${browserHarness.routePrefix}/monitor?view=trials&state=stage%3Aimage_preparation`);
  await page.screenshot({ path: testInfo.outputPath("nebius-monitor.png"), fullPage: true });
});
