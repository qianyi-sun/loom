import { expect, test } from "./fixtures/guardedTest";

test.use({ contextOptions: { reducedMotion: "reduce" } });

const trialId = "memory-probe";
const message = "The task-sandbox container exceeded its 7 GiB memory limit and was terminated by the system (OOMKilled). Subsequent communication or cleanup errors do not replace this cause. Periodic samples may miss the final memory peak.";
const resources = { cpu_millis: 1000, memory_mib: 7168, ephemeral_storage_mib: 2048 };

test("trial shows effective memory and authoritative OOM evidence", async ({
  apiHarness, browserHarness, page,
}, testInfo) => {
  await apiHarness.install({ role: "user", overrides: [
    { name: "failed trial", method: "GET", path: `/api/v1/trials/${trialId}`,
      response: { kind: "json", status: 200, body: {
        id: trialId, task_id: "local/memory-probe", team_id: "team-eai", state: "failed",
        agent_name: "terminus-2", model: { provider: "test", name: "test" },
        aggregate_reward: null, total_prompt_tokens: 0, total_completion_tokens: 0,
        llm_calls_count: 0, submitted_at: "2026-09-24T00:00:00Z", started_at: "2026-09-24T00:00:01Z",
        finished_at: "2026-09-24T00:00:10Z", attempt_count: 1,
        failure_reason: "oom_killed", failure_message: message,
        atif_ready: false, atif_url: "", trajectory_ready: false, trajectory_url: "",
        visibility: "team", share_status: "pending_scan", source_provenance: [], artifacts: [],
        diagnosis: {
          schema_version: "1", entity: { type: "trial", id: trialId }, summary: message,
          primary_cause: { reason_code: "trial.oom_killed", category: "resource", attribution: "resource_limit",
            confidence: "high", affected_trials: 1, affected_ratio: 1 },
          impact: "The trial did not complete.",
          evidence: ["Stage: agent; container: task-sandbox; incarnation: 0.",
            "Termination: 2026-09-24T00:00:10Z; exit code: 137; memory limit: 7168 MiB.",
            "Kernel termination evidence is independent of sampled peaks; missing peak data is not zero usage."],
          next_actions: [], reason_clusters: [],
        },
        materialization: {
          state: "unavailable", lifecycle_stage: "failed", compute_state: "failed", output_commit_state: "unavailable",
          canonical_ready: false, backend: "nebius", pool_id: "nebius-cpu", execution_state: "deleted",
          submitted_at: "2026-09-24T00:00:00Z", pod_scheduled_at: "2026-09-24T00:00:01Z",
          pod_started_at: "2026-09-24T00:00:01Z", pod_terminated_at: "2026-09-24T00:00:10Z",
          output_committed_at: null, source_bundle: null, attempts: 1, next_attempt_at: null,
          started_at: null, committed_at: null, error: null, trajectory_sha256: null, atif_sha256: null,
          source_cleanup_state: "complete", source_cleanup_attempts: 1, source_cleanup_error_message: null,
          source_retain_until: null, bundle: null,
          resource_allocation: { policy: "node-share-v1", baseline_slots: 16,
            declared_task: { ...resources, memory_mib: 4096 },
            pod_requests: { cpu_millis: 2200, memory_mib: 15360, ephemeral_storage_mib: 6144 },
            containers: [{ role: "task-sandbox", requests: resources, limits: resources }] },
        },
      } } },
    { name: "retained trajectory", method: "GET", path: `/api/v1/trials/${trialId}/stream?after_seq=-1`,
      response: { kind: "text", status: 200, contentType: "text/event-stream",
        body: 'event: complete\ndata: {}\n\n' } },
  ] });
  await page.goto(`${browserHarness.baseURL}/trials/${trialId}`);
  await expect(page.getByRole("region", { name: "Execution resource allocation" })).toContainText(
    "7 GiB memory reserved / 7 GiB limit",
  );
  await expect(page.getByText(message, { exact: true }).first()).toBeVisible();
  await expect(page.getByText("Kernel termination evidence is independent of sampled peaks; missing peak data is not zero usage.")).toBeVisible();
  await page.getByRole("region", { name: "Execution resource allocation" }).screenshot({ path: testInfo.outputPath("oom-allocation.png") });
});
