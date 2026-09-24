import { act, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, expect, it, vi } from "vitest";
import { Suspense } from "react";
import { useLocation, useNavigate } from "react-router-dom";
import App from "../../App";
import { RootErrorBoundary } from "../../components/RootErrorBoundary";
import { jsonResponse } from "../../test-utils/fetchMock";
import { renderWithProviders } from "../../test-utils/renderWithProviders";

afterEach(() => vi.restoreAllMocks());

it("finds a second-page Trial from page one through the real App and keeps full search focus", async () => {
  const target = "7bbcd1ac-a62a-4f5b-b46e-0e44bbeaf5e3";
  const rows = ["11111111-1111-4111-8111-111111111111", target].map((id, i) => ({
    id, task_id: i ? "target-task" : "first-task", state: "succeeded", agent_name: "oracle",
    aggregate_reward: 1, total_prompt_tokens: 0, total_completion_tokens: 0, llm_calls_count: 0,
    submitted_at: "2026-09-23T00:00:00Z",
  }));
  const fetch = vi.spyOn(globalThis, "fetch").mockImplementation(async (input) => {
    const url = new URL(String(input), "http://localhost");
    if (url.pathname.endsWith("/auth/me")) return jsonResponse({
      user: { id: "member", username: "Member", display_name: "Member", email: "member@example.test", is_platform_admin: false },
      current_team: { id: "team", name: "Research", role: "member" }, teams: [{ id: "team", name: "Research", role: "member" }],
      role: "member", scopes: ["read:own"], is_platform_admin: false, csrf_token: "fixture",
    });
    if (url.pathname.endsWith("/monitor/summary")) return jsonResponse({ detail: "No capacity sample in this fixture" }, 503);
    if (url.pathname.endsWith("/trials")) {
      const query = url.searchParams.get("q");
      const filtered = query ? rows.filter((row) => row.id.includes(query) || row.task_id.includes(query)) : rows;
      const offset = url.searchParams.get("cursor") ? 1 : 0;
      return jsonResponse({ items: filtered.slice(offset, offset + 1), next_cursor: filtered.length > offset + 1 ? "page-two" : null });
    }
    return jsonResponse({ items: [], next_cursor: null });
  });
  const user = userEvent.setup();
  renderWithProviders(<RootErrorBoundary><App /></RootErrorBoundary>, { route: "/monitor?view=trials" });
  expect(await screen.findByText("first-task")).toBeInTheDocument();
  await user.click(screen.getByRole("button", { name: "next page" }));
  expect(await screen.findByText("target-task")).toBeInTheDocument();
  await user.click(screen.getByRole("button", { name: "previous page" }));
  expect(await screen.findByText("first-task")).toBeInTheDocument();
  const search = screen.getByRole("textbox", { name: "search" });
  await user.type(search, target);
  expect(search).toHaveFocus();
  expect(search).toHaveValue(target);
  expect(await screen.findByText("target-task")).toBeInTheDocument();
  expect(screen.queryByText("first-task")).not.toBeInTheDocument();
  await waitFor(() => {
    const urls = fetch.mock.calls.map(([input]) => new URL(String(input), "http://localhost"));
    expect(urls.some((url) => url.pathname.endsWith("/trials") && url.searchParams.get("q") === target && !url.searchParams.has("cursor"))).toBe(true);
  });
  expect(screen.getByText("Page 1, end of results.")).toBeInTheDocument();
});


it("keeps rapid characters while App URL transitions are suspended and hydrates history", async () => {
  let release!: () => void;
  let blocked = true;
  const commit = new Promise<void>((resolve) => { release = resolve; });
  function DelayedUrlCommit(): JSX.Element | null {
    const location = useLocation();
    if (blocked && location.search.includes("q=")) throw commit;
    return null;
  }
  function HistoryControls(): JSX.Element {
    const navigate = useNavigate();
    const location = useLocation();
    return <><output aria-label="test route">{location.search}</output><button onClick={() => navigate("/monitor?view=trials&q=external&state=failed")}>External search</button><button onClick={() => navigate(-1)}>History back</button><button onClick={() => navigate(1)}>History forward</button></>;
  }
  vi.spyOn(globalThis, "fetch").mockImplementation(async (input) => {
    const url = String(input);
    if (url.includes("/auth/me")) return jsonResponse({
      user: { id: "member", username: "Member", display_name: "Member", email: "member@example.test", is_platform_admin: false },
      current_team: { id: "team", name: "Research", role: "member" }, teams: [{ id: "team", name: "Research", role: "member" }],
      role: "member", scopes: ["read:own"], is_platform_admin: false, csrf_token: "fixture",
    });
    if (url.includes("/monitor/summary")) return jsonResponse({ detail: "No capacity sample in this fixture" }, 503);
    return jsonResponse({ items: [], next_cursor: null });
  });
  const user = userEvent.setup();
  renderWithProviders(<Suspense fallback={<p>Waiting for navigation</p>}><RootErrorBoundary><App /></RootErrorBoundary><DelayedUrlCommit /><HistoryControls /></Suspense>, { route: "/monitor?view=trials" });
  const search = await screen.findByRole("textbox", { name: "search" });
  await user.click(search);
  await user.keyboard("ab");
  expect(search).toHaveFocus();
  expect(search).toHaveValue("ab");
  await act(async () => { blocked = false; release(); await commit; });
  expect(search).toHaveValue("ab");
  expect(screen.getByLabelText("test route")).toHaveTextContent("?view=trials&q=ab");
  await user.click(screen.getByRole("button", { name: "External search" }));
  expect(search).toHaveValue("external");
  expect(screen.getByLabelText("filter by state")).toHaveValue("failed");
  await user.click(screen.getByRole("button", { name: "History back" }));
  expect(search).toHaveValue("ab");
  await user.click(screen.getByRole("button", { name: "History forward" }));
  expect(search).toHaveValue("external");
});
