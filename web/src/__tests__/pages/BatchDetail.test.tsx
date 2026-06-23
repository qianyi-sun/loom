import { screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { Route, Routes } from "react-router-dom";
import { beforeEach, describe, expect, it, vi } from "vitest";

import BatchDetail from "../../pages/BatchDetail";
import { renderWithProviders } from "../../test-utils/renderWithProviders";

const BATCH_ID = "be792550-fb63-40b4-b5a2-795adbf2cc9d";

const BATCH_BODY = {
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
  effective_total_cost_usd: 0,
  created_at: "2026-06-19T20:23:00Z",
  finished_at: null,
  created_by_token_prefix: "test:web",
  expected_trial_count: 164,
  trial_summary: {},
  aggregate_reward: null,
  total_cost_usd: 0,
};

function mockBatch(
  body = BATCH_BODY,
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
