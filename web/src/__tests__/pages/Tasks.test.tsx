/**
 * Tasks page renders the detail-rich row (name, agent, verifier,
 * step count) and translates UI controls into the right API query
 * params (benchmark dropdown → `benchmark_id`, search → `q`). Pins
 * the body shape so a refactor that swaps params under the hood
 * gets caught here.
 */
import { screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { beforeEach, describe, expect, it, vi } from "vitest";

import { Route, Routes, useLocation, useNavigate, Link } from "react-router-dom";

import Tasks from "../../pages/Tasks";
import type { FetchMock } from "../../test-utils/fetchMock";
import { renderWithProviders } from "../../test-utils/renderWithProviders";

vi.mock("../../components/SubmitTrialModal", () => ({
  SubmitTrialModal: ({ taskId, onClose }: { taskId: string; onClose: () => void }) => <div role="dialog" aria-label="Submit task"><p>{taskId}</p><button onClick={onClose}>Close task</button></div>,
}));
function RouteProbe() {
  const location = useLocation();
  const navigate = useNavigate();
  return <><div aria-label="Current URL">{location.pathname}{location.search}</div><Link to="/away">Leave list</Link><button onClick={() => navigate(-1)}>Browser back</button><Routes><Route path="/tasks" element={<Tasks />} /><Route path="/away" element={<p>Away</p>} /></Routes></>;
}

interface FetchSpyHandle {
  spy: FetchMock;
}

const TASKS_RESPONSE = {
  items: [
    {
      id: "humaneval/HumanEval/0",
      name: "Two-sum",
      description: "Return indices of two numbers that sum to a target.",
      agent_name: "oracle",
      verifier_name: "pytest",
      step_count: 2,
      benchmark_id: "humaneval",
      source: "local",
    },
  ],
  next_cursor: null,
};

const BENCHMARKS_RESPONSE = {
  items: [
    { id: "humaneval", display_name: "HumanEval" },
    { id: "mbpp", display_name: "MBPP" },
  ],
  next_cursor: null,
};

function setupFetch(): FetchSpyHandle {
  const spy = vi
    .spyOn(globalThis, "fetch")
    .mockImplementation((input: RequestInfo | URL) => {
      const url = typeof input === "string" ? input : String(input);
      if (url.includes("/api/v1/benchmarks")) {
        return Promise.resolve(
          new Response(JSON.stringify(BENCHMARKS_RESPONSE), {
            status: 200,
            headers: { "Content-Type": "application/json" },
          }),
        );
      }
      if (url.includes("/api/v1/tasks")) {
        return Promise.resolve(
          new Response(JSON.stringify(TASKS_RESPONSE), {
            status: 200,
            headers: { "Content-Type": "application/json" },
          }),
        );
      }
      return Promise.reject(new Error(`unexpected fetch ${url}`));
    });
  return { spy };
}

describe("Tasks page", () => {
  beforeEach(() => {
    window.localStorage.clear();
    window.localStorage.setItem("loom_token", "test-token");
    vi.restoreAllMocks();
  });

  it("renders detail-rich rows: name, agent, verifier, step count", async () => {
    setupFetch();
    renderWithProviders(<Tasks />);
    expect(await screen.findByText("Two-sum")).toBeInTheDocument();
    expect(
      screen.getByText(/Return indices of two numbers/),
    ).toBeInTheDocument();
    expect(screen.getByText(/agent: oracle/i)).toBeInTheDocument();
    expect(screen.getByText(/verifier: pytest/i)).toBeInTheDocument();
    expect(screen.getByText(/2 steps/i)).toBeInTheDocument();
    expect(screen.getByText(/benchmark: humaneval/i)).toBeInTheDocument();
    expect(screen.getByText("Task selection guide")).toBeInTheDocument();
    expect(screen.queryByText(/loom eval batch create/)).not.toBeInTheDocument();
  });

  it("benchmark dropdown is populated from /api/v1/benchmarks", async () => {
    setupFetch();
    renderWithProviders(<Tasks />);
    // Wait for the benchmarks query to resolve and re-render the
    // dropdown options.
    await screen.findByRole("option", { name: "HumanEval" });
    const dropdown = await screen.findByRole("combobox");
    const options = Array.from(
      dropdown.querySelectorAll("option"),
    ).map((o) => o.textContent);
    expect(options).toEqual(["All benchmarks", "HumanEval", "MBPP"]);
  });

  it("restores URL filters and page, then clears the cursor when typing a new filter", async () => {
    const { spy } = setupFetch();
    const user = userEvent.setup();
    renderWithProviders(<Tasks />, { route: "/tasks?benchmark=humaneval&q=two&cursor=page-two&cursor_history=%5Bnull%5D" });
    await screen.findByText("Two-sum");
    expect(screen.getByRole("combobox")).toHaveValue("humaneval");
    expect(screen.getByPlaceholderText(/humaneval\/0/)).toHaveValue("two");
    expect(spy.mock.calls.some(([url]) => String(url).includes("cursor=page-two"))).toBe(true);
    await user.type(screen.getByPlaceholderText(/humaneval\/0/), "s");
    await vi.waitFor(() => {
      const url = new URL(String(spy.mock.calls.filter(([url]) => String(url).includes("/api/v1/tasks")).at(-1)![0]), "http://localhost");
      expect(url.searchParams.get("q")).toBe("twos");
      expect(url.searchParams.has("cursor")).toBe(false);
    });
  });

  it("typing in search sends `q=…` to /api/v1/tasks", async () => {
    const { spy } = setupFetch();
    const user = userEvent.setup();
    renderWithProviders(<Tasks />);
    await screen.findByText("Two-sum");
    const input = screen.getByPlaceholderText(/humaneval\/0/);
    await user.type(input, "two");
    await vi.waitFor(() => {
      const taskCalls = spy.mock.calls.filter(([u]) =>
        String(u).includes("/api/v1/tasks") && String(u).includes("q=two"),
      );
      expect(taskCalls.length).toBeGreaterThan(0);
    });
  });

  it("picking a benchmark sends `benchmark_id=…`", async () => {
    const { spy } = setupFetch();
    const user = userEvent.setup();
    renderWithProviders(<Tasks />);
    await screen.findByText("Two-sum");
    const dropdown = await screen.findByRole("combobox");
    await user.selectOptions(dropdown, "humaneval");
    await vi.waitFor(() => {
      const taskCalls = spy.mock.calls.filter(([u]) =>
        String(u).includes("/api/v1/tasks") &&
        String(u).includes("benchmark_id=humaneval"),
      );
      expect(taskCalls.length).toBeGreaterThan(0);
    });
  });

  it("the dropped license filter is no longer in the URL", async () => {
    const { spy } = setupFetch();
    renderWithProviders(<Tasks />);
    await screen.findByText("Two-sum");
    const taskCalls = spy.mock.calls.filter(([u]) =>
      String(u).includes("/api/v1/tasks"),
    );
    expect(taskCalls.length).toBeGreaterThan(0);
    expect(
      taskCalls.some(([u]) => String(u).includes("license=")),
    ).toBe(false);
  });
  it("traverses first, middle, last and empty pages and restores the filtered page after task close and history return", async () => {
    const user = userEvent.setup();
    vi.spyOn(globalThis, "fetch").mockImplementation(async (input) => {
      const url = new URL(String(input), "http://localhost");
      if (url.pathname.endsWith("/benchmarks")) return new Response(JSON.stringify(BENCHMARKS_RESPONSE));
      const cursor = url.searchParams.get("cursor");
      const index = cursor === "middle" ? 2 : cursor === "last" ? 3 : 1;
      const empty = url.searchParams.get("q") === "missing";
      return new Response(JSON.stringify({ items: empty ? [] : [{ ...TASKS_RESPONSE.items[0], id: `task-${index}`, name: `Task page ${index}` }], next_cursor: empty || index === 3 ? null : index === 1 ? "middle" : "last" }));
    });
    renderWithProviders(<RouteProbe />, { route: "/tasks?benchmark=humaneval&q=task" });
    await screen.findByText("Task page 1");
    expect(screen.getByRole("button", { name: "previous page" })).toHaveAttribute("aria-disabled", "true");
    await user.click(screen.getByRole("button", { name: "next page" }));
    await screen.findByText("Task page 2");
    expect(screen.getByRole("status")).toHaveTextContent("Page 2, more results");
    expect(screen.getByLabelText("Current URL")).toHaveTextContent("cursor=middle");
    await user.click(screen.getByRole("button", { name: "Submit trial" }));
    expect(screen.getByRole("dialog")).toHaveTextContent("task-2");
    await user.click(screen.getByRole("button", { name: "Close task" }));
    expect(screen.getByText("Task page 2")).toBeInTheDocument();
    await user.click(screen.getByRole("link", { name: "Leave list" }));
    await user.click(screen.getByRole("button", { name: "Browser back" }));
    await screen.findByText("Task page 2");
    expect(screen.getByRole("combobox")).toHaveValue("humaneval");
    expect(screen.getByPlaceholderText(/humaneval\/0/)).toHaveValue("task");
    await user.click(screen.getByRole("button", { name: "next page" }));
    await screen.findByText("Task page 3");
    expect(screen.getByRole("status")).toHaveTextContent("Page 3, end of results");
    expect(screen.getByRole("button", { name: "next page" })).toHaveAttribute("aria-disabled", "true");
    await user.click(screen.getByRole("button", { name: "previous page" }));
    await screen.findByText("Task page 2");
    await user.clear(screen.getByPlaceholderText(/humaneval\/0/));
    await user.type(screen.getByPlaceholderText(/humaneval\/0/), "missing");
    await screen.findByText("No tasks match this filter.");
    await waitFor(() => expect(screen.getByRole("status")).toHaveTextContent("Page 1, end of results"));
    expect(screen.getByLabelText("Current URL")).not.toHaveTextContent("cursor=");
  });

});
