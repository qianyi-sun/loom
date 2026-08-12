import { render, screen } from "@testing-library/react";

import PipelineBudgetSummary from "../../components/pipelines/PipelineBudgetSummary";

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
