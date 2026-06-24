import { screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { Route, Routes } from "react-router-dom";
import { beforeEach, describe, expect, it, vi } from "vitest";

import type { components } from "../../api/schema";
import BatchDetail from "../../pages/BatchDetail";
import { renderWithProviders } from "../../test-utils/renderWithProviders";

const BATCH_ID = "be792550-fb63-40b4-b5a2-795adbf2cc9d";

type BatchBody = Record<string, unknown>;

const BATCH_BODY: BatchBody = {
  id: BATCH_ID,
  team_id: "team-1",
  owner_team: { id: "team-1", name: "Alpha Research" },
  name: "qwen2.5-litellm",
  description: null,
  task_filter: { subset_kind: "all", benchmark_ids: ["humaneval"] },
  trial_config: {},
  backend: "docker",
  combinations: [
    {
      label: "combo1",
      agent_name: "litellm",
      agent_model: {
        provider: "openai",
        name: "qwen2.5-coder-7b-instruct",
      },
      n_per_task: 1,
    },
  ],
  state: "submitted",
  result_status: null,
  failure_reason: null,
  failure_message: null,
  fanout_errors: [],
  rerun_of_batch_id: null,
  rerun_targets: [],
  visibility: "team",
  share_status: "pending_scan",
  source_provenance: [
    { kind: "cloned_batch_config", source_batch_id: "source-batch" },
  ],
  rerun_batches: [],
  rerunnable_failed_count: 0,
  effective_trial_summary: {},
  effective_result_status: null,
  effective_aggregate_reward: null,
  effective_total_prompt_tokens: 0,
  effective_total_completion_tokens: 0,
  effective_llm_calls_count: 0,
  created_at: "2026-06-19T20:23:00Z",
  finished_at: null,
  created_by_token_prefix: "test:web",
  expected_trial_count: 164,
  trial_summary: {},
  aggregate_reward: null,
  benchmark_summary: [],
  total_prompt_tokens: 0,
  total_completion_tokens: 0,
  llm_calls_count: 0,
};

const BATCH_DEBUG: components["schemas"]["DebugEvidence"] = {
  schema_version: "1",
  entity: { type: "batch", id: BATCH_ID, team_id: "team-1" },
  lifecycle: { state: "finished", terminal_status: "all_failed" },
  worker: { backend: "docker" },
  failure: {
    reason_code: "batch.fanout_submit_failed",
    reason: "fanout_submit_failed",
    category: "submit",
    attribution: "platform",
    message: "task local/mit-0 submit failed: HTTP 403",
  },
  provider: { llm_calls_count: 0, models: [] },
  task_selection: { expected_trial_count: 0 },
  next_actions: ["Inspect batch fan-out errors."],
};

const BATCH_DIAGNOSIS = {
  schema_version: "1",
  entity: { type: "batch", id: BATCH_ID },
  summary: (
    "The batch failed because most failed child trials hit provider gateway "
    + "errors before scoring."
  ),
  primary_cause: {
    reason_code: "trial.gateway_error",
    category: "gateway",
    attribution: "provider",
    confidence: "medium",
    affected_trials: 3,
    affected_ratio: 0.75,
  },
  impact: "The aggregate score is not reliable for model-quality comparison.",
  evidence: ["3/4 affected trial(s) matched trial.gateway_error"],
  next_actions: [
    {
      label: "Rerun failed trials after the provider path is healthy",
      kind: "web_action",
      action: "rerun_failed",
    },
  ],
  reason_clusters: [
    {
      reason_code: "trial.gateway_error",
      category: "gateway",
      attribution: "provider",
      count: 3,
      affected_ratio: 0.75,
      representative_trial_id: "trial-1",
      representative_task_id: "humaneval/0",
    },
  ],
};

function mockBatch(
  body: BatchBody = BATCH_BODY,
  rerunBody: Record<string, unknown> | null = null,
): ReturnType<typeof vi.spyOn> {
  return vi
    .spyOn(globalThis, "fetch")
    .mockImplementation((input: RequestInfo | URL, init?: RequestInit) => {
      const url = typeof input === "string" ? input : String(input);
      if (
        url.endsWith(`/api/v1/batches/${BATCH_ID}/rerun-failed`) &&
        init?.method === "POST" &&
        rerunBody
      ) {
        return Promise.resolve(
          new Response(JSON.stringify(rerunBody), {
            status: 201,
            headers: { "Content-Type": "application/json" },
          }),
        );
      }
      if (url.endsWith(`/api/v1/batches/${BATCH_ID}`)) {
        return Promise.resolve(
          new Response(JSON.stringify(body), {
            status: 200,
            headers: { "Content-Type": "application/json" },
          }),
        );
      }
      return Promise.reject(new Error(`unexpected fetch ${url}`));
    });
}

function renderBatchDetail(): void {
  renderWithProviders(
    <Routes>
      <Route path="/batches/:batchId" element={<BatchDetail />} />
    </Routes>,
    { route: `/batches/${BATCH_ID}` },
  );
}

describe("BatchDetail run plan", () => {
  beforeEach(() => {
    window.localStorage.clear();
    window.localStorage.setItem("loom_token", "test-token");
    vi.restoreAllMocks();
  });

  it("shows a human-readable run plan instead of raw payload fields", async () => {
    mockBatch();
    renderBatchDetail();

    expect(await screen.findByText(/Run plan/i)).toBeInTheDocument();
    expect(screen.getAllByText("Batch CLI").length).toBeGreaterThan(0);
    expect(screen.getByText(`loom eval batch show ${BATCH_ID}`)).toBeInTheDocument();
    expect(
      screen.getByText("HumanEval / all runnable tasks / 164 tasks"),
    ).toBeInTheDocument();
    expect(screen.getByText(/Shared trial settings/i)).toBeInTheDocument();
    expect(screen.getByText("Defaults only")).toBeInTheDocument();
    expect(screen.queryByText("task_filter")).not.toBeInTheDocument();
    expect(screen.queryByText("trial_config")).not.toBeInTheDocument();
  });

  it("shows owner team, share status, and provenance on the detail header", async () => {
    mockBatch();
    renderBatchDetail();

    expect(await screen.findByText("Owner team")).toBeInTheDocument();
    expect(screen.getByText("Alpha Research")).toBeInTheDocument();
    expect(screen.getByText("Visibility")).toBeInTheDocument();
    expect(screen.getByText("team / pending_scan")).toBeInTheDocument();
    expect(screen.getByText("Provenance")).toBeInTheDocument();
    expect(screen.getByText(/cloned batch config/i)).toBeInTheDocument();
  });

  it("shows structured debug evidence without exposing raw JSON by default", async () => {
    mockBatch({
      ...BATCH_BODY,
      state: "finished",
      result_status: "all_failed",
      failure_reason: "fanout_submit_failed",
      failure_message: "task local/mit-0 submit failed: HTTP 403",
      debug_evidence: BATCH_DEBUG,
      diagnosis: BATCH_DIAGNOSIS,
    });
    renderBatchDetail();

    expect(await screen.findByText("Diagnosis")).toBeInTheDocument();
    expect(screen.getByText(BATCH_DIAGNOSIS.summary)).toBeInTheDocument();
    expect(screen.getAllByText("trial.gateway_error").length).toBeGreaterThan(0);
    expect(
      screen.getByText("The aggregate score is not reliable for model-quality comparison."),
    ).toBeInTheDocument();
    expect(screen.getByText("Reason clusters")).toBeInTheDocument();
    expect(screen.getByText(/Rerun failed trials/i)).toBeInTheDocument();
    expect(await screen.findByText("Debug evidence")).toBeInTheDocument();
    expect(screen.getByText("batch.fanout_submit_failed")).toBeInTheDocument();
    expect(screen.getByText("platform")).toBeInTheDocument();
    expect(screen.getByText("Inspect batch fan-out errors.")).toBeInTheDocument();
    expect(screen.queryByText("debug_evidence")).not.toBeInTheDocument();
  });

  it("shows per-benchmark scores for multi-benchmark batches", async () => {
    mockBatch({
      ...BATCH_BODY,
      task_filter: {
        subset_kind: "all",
        benchmark_ids: ["humaneval", "mbpp"],
      },
      state: "finished",
      result_status: "partial_failed",
      expected_trial_count: 3,
      trial_summary: { succeeded: 2, failed: 1 },
      aggregate_reward: 0.5,
      benchmark_summary: [
        {
          benchmark_id: "humaneval",
          display_name: "HumanEval",
          metric_name: "score",
          expected_trial_count: 2,
          completed_trial_count: 2,
          platform_failed_count: 1,
          aggregate_reward: 0.5,
          trial_summary: {
            queued: 0,
            claimed: 0,
            running: 0,
            succeeded: 1,
            failed: 1,
            cancelled: 0,
          },
        },
        {
          benchmark_id: "mbpp",
          display_name: "MBPP",
          metric_name: "score",
          expected_trial_count: 1,
          completed_trial_count: 1,
          platform_failed_count: 0,
          aggregate_reward: 0.5,
          trial_summary: {
            queued: 0,
            claimed: 0,
            running: 0,
            succeeded: 1,
            failed: 0,
            cancelled: 0,
          },
        },
      ],
    });
    renderBatchDetail();

    expect(await screen.findByText("Benchmark results")).toBeInTheDocument();
    expect(screen.getByText("HumanEval")).toBeInTheDocument();
    expect(screen.getByText("MBPP")).toBeInTheDocument();
    expect(screen.getByText("2 / 2")).toBeInTheDocument();
    expect(screen.getByText("1 / 1")).toBeInTheDocument();
    expect(screen.getByText("1 failed")).toBeInTheDocument();
    expect(screen.getByText("0 failed")).toBeInTheDocument();
  });

  it("does not add benchmark-result clutter for single-benchmark batches", async () => {
    mockBatch({
      ...BATCH_BODY,
      benchmark_summary: [
        {
          benchmark_id: "humaneval",
          display_name: "HumanEval",
          metric_name: "score",
          expected_trial_count: 164,
          completed_trial_count: 0,
          platform_failed_count: 0,
          aggregate_reward: null,
          trial_summary: {
            queued: 0,
            claimed: 0,
            running: 0,
            succeeded: 0,
            failed: 0,
            cancelled: 0,
          },
        },
      ],
    });
    renderBatchDetail();

    expect(await screen.findByText(/Run plan/i)).toBeInTheDocument();
    expect(screen.queryByText("Benchmark results")).not.toBeInTheDocument();
  });

  it("keeps raw batch payload available in diagnostics", async () => {
    mockBatch();
    renderBatchDetail();

    const user = userEvent.setup();
    await user.click(await screen.findByText("Diagnostics"));

    await waitFor(() => {
      expect(screen.getByText("task_filter")).toBeInTheDocument();
      expect(screen.getByText("trial_config")).toBeInTheDocument();
    });
  });

  it("offers a rerun action when transient failed cases are available", async () => {
    const failedBatch = {
      ...BATCH_BODY,
      state: "finished",
      result_status: "partial_failed",
      trial_summary: { succeeded: 160, failed: 4 },
      rerunnable_failed_count: 4,
      effective_trial_summary: { succeeded: 160, failed: 4 },
      effective_result_status: "partial_failed",
    };
    const fetchMock = mockBatch(failedBatch, {
      batch_id: "rerun-batch-id",
      rerun_of_batch_id: BATCH_ID,
      expected_trial_count: 4,
      state: "submitted",
      rerun_target_count: 4,
    });
    renderBatchDetail();

    const user = userEvent.setup();
    await user.click(await screen.findByRole("button", { name: /Rerun failed cases/i }));

    await waitFor(() => {
      expect(fetchMock).toHaveBeenCalledWith(
        expect.stringContaining(`/api/v1/batches/${BATCH_ID}/rerun-failed`),
        expect.objectContaining({ method: "POST" }),
      );
    });
    expect(await screen.findByText(/Rerun queued/i)).toBeInTheDocument();
  });
});
