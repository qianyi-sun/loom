import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import { Route, Routes } from "react-router-dom";
import { vi } from "vitest";

import PipelineBudgetSummary from "../../components/pipelines/PipelineBudgetSummary";
import PipelineRunDetailPage from "../../pages/PipelineRunDetail";
import { renderWithProviders } from "../../test-utils/renderWithProviders";

const budgetRow = { limit: 100, reserved: 10, settled: 20, remaining: 70 };

const runDetail = {
  id: "run-1",
  created_by_user_id: "user-1",
  display_name: "Observed pipeline run",
  recipe: { name: "terminalgen-authoring", version: 1, digest: "sha256:recipe" },
  graph_digest: "sha256:graph",
  control_binding_snapshots_digest: "sha256:control",
  state: "running",
  result: null,
  reason: null,
  retry_of_pipeline_run_id: null,
  retry_from_stage_run_id: null,
  created_at: "2026-08-20T12:00:00Z",
  started_at: "2026-08-20T12:01:00Z",
  finished_at: null,
  source_budget: {
    max_wall_seconds: 3_600,
    max_gpu_seconds: 0,
    max_provider_cost_usd: "10.000000",
    max_artifact_bytes: 1_000_000,
    max_stage_runs: 10,
    max_attempts_total: 20,
  },
  budget: {
    max_wall_seconds: budgetRow,
    max_gpu_seconds: budgetRow,
    max_provider_cost_usd: budgetRow,
    max_artifact_bytes: budgetRow,
    max_stage_runs: budgetRow,
    max_attempts_total: budgetRow,
    wall_deadline_at: null,
    terminal_cause: null,
  },
  progress: {
    total_stage_runs: 2,
    completed_stage_runs: 1,
    states: { running: 1, succeeded: 1 },
    domain_outcomes: { accepted: 1 },
    nodes: {
      generate: {
        total_stage_runs: 2,
        completed_stage_runs: 1,
        states: { running: 1, succeeded: 1 },
        domain_outcomes: { accepted: 1 },
      },
    },
  },
  topology: [{
    node_key: "generate",
    node_kind: "container",
    topological_level: 0,
    upstream_node_keys: [],
  }],
  stages: [],
  artifacts: [],
};

const stage = {
  id: "stage-1",
  node_key: "generate",
  shard_key: "slot-1",
  node_kind: "container",
  topological_level: 0,
  upstream_node_keys: [],
  state: "running",
  domain_outcome: null,
  reason_code: null,
  attempt_count: 1,
  resource_profile_name: "authoring-cpu@1",
  resource_class: "cpu",
  retry_allowed: false,
  retry_ineligible_reason: "stage_not_failed",
};

function jsonResponse(value: unknown, status = 200): Response {
  return new Response(JSON.stringify(value), {
    status,
    headers: { "Content-Type": "application/json" },
  });
}

afterEach(() => {
  vi.unstubAllGlobals();
  vi.restoreAllMocks();
});

test("surfaces a budget ledger invariant instead of clamping", () => {
  const row = { limit: 10, reserved: 7, settled: 5, remaining: -2 };
  render(<PipelineBudgetSummary budget={{ max_wall_seconds: row, max_gpu_seconds: row, max_provider_cost_usd: row, max_artifact_bytes: row, max_stage_runs: row, max_attempts_total: row, wall_deadline_at: null, terminal_cause: "accounting_violation" }} />);
  expect(screen.getAllByText("budget ledger invariant")).toHaveLength(6);
});

test("renders six independent budget rows", () => {
  const row = { limit: 10, reserved: 2, settled: 3, remaining: 5 };
  render(<PipelineBudgetSummary budget={{ max_wall_seconds: row, max_gpu_seconds: row, max_provider_cost_usd: row, max_artifact_bytes: row, max_stage_runs: row, max_attempts_total: row, wall_deadline_at: null, terminal_cause: null }} />);
  expect(screen.getByText("max_wall_seconds")).toBeInTheDocument();
  expect(screen.getByText("max_attempts_total")).toBeInTheDocument();
});

test("renders durable progress from bounded StageRun and Artifact pages", async () => {
  const fetchMock = vi.fn(async (input: RequestInfo | URL) => {
    const url = String(input);
    if (url.includes("/auth/me")) {
      return jsonResponse({
        user: { id: "user-1", username: "owner", email: null, display_name: null, is_platform_admin: false },
        teams: [{ id: "team-1", name: "Team", role: "owner" }],
        current_team: { id: "team-1", name: "Team", role: "owner" },
        role: "owner",
        scopes: ["read:own", "submit"],
        is_platform_admin: false,
        csrf_token: "csrf",
      });
    }
    if (url.includes("/pipeline-runs/run-1/events")) {
      return jsonResponse({ events: [], next_after_seq: 0, terminal: true });
    }
    if (url.includes("/pipeline-runs/run-1/stages")) {
      return jsonResponse({ items: [stage], next_cursor: "next-stage-page" });
    }
    if (url.includes("/pipeline-runs/run-1/artifacts")) {
      return jsonResponse({
        items: [{
          id: "artifact-1",
          name: "runtime-task.tar.gz",
          artifact_type: "terminal_task_bundle",
          access_class: "team_runtime",
          stored_size_bytes: 512,
          content_sha256: "sha256:artifact",
        }],
        next_cursor: null,
      });
    }
    if (url.endsWith("/pipeline-runs/run-1")) return jsonResponse(runDetail);
    throw new Error(`unexpected request: ${url}`);
  });
  vi.stubGlobal("fetch", fetchMock);

  renderWithProviders(
    <Routes>
      <Route path="/pipelines/:runId" element={<PipelineRunDetailPage />} />
    </Routes>,
    { route: "/pipelines/run-1" },
  );

  expect(await screen.findByRole("heading", { name: "Observed pipeline run" })).toBeInTheDocument();
  expect(screen.getByText("1", { selector: "strong" }).closest("p")).toHaveTextContent("1 / 2 StageRuns terminal");
  expect(screen.getByText("accepted: 1")).toBeInTheDocument();
  expect(screen.getByText("slot-1")).toBeInTheDocument();
  expect(screen.getByRole("link", { name: "Download" })).toHaveAttribute(
    "href",
    "/api/v1/pipeline-artifacts/artifact-1/download",
  );
  expect(screen.getByRole("button", { name: "Cancel PipelineRun" })).toBeInTheDocument();

  fireEvent.click(screen.getByRole("button", { name: /generate, container, 2 shards/i }));
  await waitFor(() => {
    expect(fetchMock.mock.calls.some(([input]) => String(input).includes("node_key=generate"))).toBe(true);
  });
});
