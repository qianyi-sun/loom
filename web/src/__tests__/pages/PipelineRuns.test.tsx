import { fireEvent, screen, waitFor } from "@testing-library/react";
import { vi } from "vitest";
import userEvent from "@testing-library/user-event";
import { useNavigate } from "react-router-dom";

import PipelineRuns from "../../pages/PipelineRuns";
import { renderWithProviders } from "../../test-utils/renderWithProviders";

test("renders the exact server-ordered Pipeline list semantics", async () => {
  vi.stubGlobal("fetch", vi.fn(async (input: RequestInfo | URL) => {
    const url = String(input);
    if (url.includes("/auth/me")) return new Response(JSON.stringify({ user: { id: "u", username: "user", email: null, display_name: null, is_platform_admin: false }, teams: [{ id: "t", name: "Team", role: "member" }], current_team: { id: "t", name: "Team", role: "member" }, role: "member", scopes: ["read:own"], is_platform_admin: false, csrf_token: "csrf" }), { status: 200, headers: { "Content-Type": "application/json" } });
    return new Response(JSON.stringify({ items: [{ id: "00000000-0000-0000-0000-000000000001", display_name: "generic run", recipe: { name: "smoke", version: 1, digest: "sha256:x" }, state: "finished", result: "partial_failed", completed_stage_runs: 2, total_stage_runs: 2, domain_outcomes: { opaque_b: 1, opaque_a: 1 }, budget: { max_wall_seconds: { limit: 30, reserved: 0, settled: 10, remaining: 20 }, max_gpu_seconds: { limit: 60, reserved: 0, settled: 20, remaining: 40 }, max_provider_cost_usd: { limit: 1000000, reserved: 0, settled: 123456, remaining: 876544 }, max_artifact_bytes: { limit: 1024, reserved: 0, settled: 128, remaining: 896 }, max_stage_runs: { limit: 2, reserved: 0, settled: 2, remaining: 0 }, max_attempts_total: { limit: 3, reserved: 0, settled: 2, remaining: 1 }, wall_deadline_at: null, terminal_cause: null }, created_at: "2026-08-12T00:00:00Z", finished_at: "2026-08-12T00:01:00Z" }], next_cursor: null }), { status: 200, headers: { "Content-Type": "application/json" } });
  }));
  renderWithProviders(<PipelineRuns />, { route: "/pipelines" });
  expect(await screen.findByRole("link", { name: "generic run" })).toBeInTheDocument();
  expect(screen.getByText("Partially failed")).toBeInTheDocument();
  expect(screen.getByText(/opaque_a × 1, opaque_b × 1/)).toBeInTheDocument();
  expect(screen.getByText("$0.123456 / $1.000000")).toBeInTheDocument();
});

test("restores date drafts from the URL and Back, resetting pagination on apply", async () => {
  const user = userEvent.setup();
  const requests: URL[] = [];
  vi.stubGlobal("fetch", vi.fn(async (input: RequestInfo | URL) => {
    const url = new URL(String(input), "http://localhost");
    if (url.pathname.endsWith("/pipeline-runs")) requests.push(url);
    return new Response(JSON.stringify({ items: [], next_cursor: null }), { status: 200, headers: { "Content-Type": "application/json" } });
  }));
  function Back(): JSX.Element { const navigate = useNavigate(); return <button onClick={() => navigate(-1)}>Browser Back</button>; }
  renderWithProviders(<><PipelineRuns /><Back /></>, { route: "/pipelines?created_after=2026-09-01T00%3A00%3A00.000Z&created_before=2026-09-23T00%3A00%3A00.000Z&cursor=older" });
  expect(screen.getByLabelText("Created after")).toHaveValue("2026-09-01");
  expect(screen.getByLabelText("Created before")).toHaveValue("2026-09-23");
  await screen.findByText("No pipeline runs");
  fireEvent.change(screen.getByLabelText("Created after"), { target: { value: "2026-09-10" } });
  await user.click(screen.getByRole("button", { name: "Apply" }));
  await waitFor(() => expect(requests.at(-1)?.searchParams.get("created_after")).toBe("2026-09-10T00:00:00.000Z"));
  expect(requests.at(-1)?.searchParams.has("cursor")).toBe(false);
  expect(screen.queryByRole("table", { name: "Pipeline runs" })).not.toBeInTheDocument();
  expect(screen.queryByText("Page 1, end of results")).not.toBeInTheDocument();
  await user.click(screen.getByRole("button", { name: "Browser Back" }));
  expect(screen.getByLabelText("Created after")).toHaveValue("2026-09-01");
  expect(screen.getByLabelText("Created before")).toHaveValue("2026-09-23");
});
