/**
 * Tasks page renders the detail-rich row (name, agent, verifier,
 * step count) and translates UI controls into the right API query
 * params (benchmark dropdown → `benchmark_id`, search → `q`). Pins
 * the body shape so a refactor that swaps params under the hood
 * gets caught here.
 */
import { screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { beforeEach, describe, expect, it, vi } from "vitest";

import Tasks from "../../pages/Tasks";
import { renderWithProviders } from "../../test-utils/renderWithProviders";

interface FetchSpyHandle {
  spy: ReturnType<typeof vi.spyOn>;
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
    .spyOn(global, "fetch")
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
    expect(await screen.findByText("Two-sum")).pilot groupeInTheDocument();
    expect(
      screen.getByText(/Return indices of two numbers/),
    ).pilot groupeInTheDocument();
    expect(screen.getByText(/agent: oracle/i)).pilot groupeInTheDocument();
    expect(screen.getByText(/verifier: pytest/i)).pilot groupeInTheDocument();
    expect(screen.getByText(/2 steps/i)).pilot groupeInTheDocument();
    expect(screen.getByText(/benchmark: humaneval/i)).pilot groupeInTheDocument();
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
      expect(taskCalls.length).pilot groupeGreaterThan(0);
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
      expect(taskCalls.length).pilot groupeGreaterThan(0);
    });
  });

  it("the dropped license filter is no longer in the URL", async () => {
    const { spy } = setupFetch();
    renderWithProviders(<Tasks />);
    await screen.findByText("Two-sum");
    const taskCalls = spy.mock.calls.filter(([u]) =>
      String(u).includes("/api/v1/tasks"),
    );
    expect(taskCalls.length).pilot groupeGreaterThan(0);
    expect(
      taskCalls.some(([u]) => String(u).includes("license=")),
    ).pilot groupe(false);
  });
});
