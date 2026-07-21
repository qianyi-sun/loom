import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { MemoryRouter, Route, Routes } from "react-router-dom";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import TaskSetDetail from "../../pages/TaskSetDetail";

const TASK_SET = {
  task_set_id: "task-set-1",
  status: "ready",
  status_reason: null,
  intents: ["evaluation"],
  manifest_intents: ["evaluation"],
  inferred_intents: [],
  capabilities: ["evaluation"],
  warnings: [],
  evaluation_ready: true,
  task_count: 2,
  error_summary: [
    { instance_index: 1, code: "invalid-task", message: "Task is invalid" },
  ],
  materialization_job_state: null,
};

function renderPage(initialPath = "/task-sets/task-set-1"): void {
  vi.stubGlobal(
    "fetch",
    vi.fn().mockImplementation(() =>
      Promise.resolve(
        new Response(JSON.stringify(TASK_SET), {
          status: 200,
          headers: { "Content-Type": "application/json" },
        }),
      ),
    ),
  );
  const queryClient = new QueryClient({
    defaultOptions: { queries: { retry: false } },
  });
  render(
    <QueryClientProvider client={queryClient}>
      <MemoryRouter future={{ v7_startTransition: true, v7_relativeSplatPath: true }} initialEntries={[initialPath]}>
        <Routes>
          <Route path="/task-sets/:id" element={<TaskSetDetail />} />
        </Routes>
      </MemoryRouter>
    </QueryClientProvider>,
  );
}

describe("TaskSetDetail tabs", () => {
  beforeEach(() => window.localStorage.setItem("loom_token", "t"));
  afterEach(() => vi.restoreAllMocks());

  it("links panels and supports keyboard activation without changing task-set behavior", async () => {
    const user = userEvent.setup();
    renderPage();

    expect(
      await screen.findByRole("heading", { name: "task-set-1" }),
    ).toBeInTheDocument();
    const overviewTab = screen.getByRole("tab", { name: "Overview" });
    const errorsTab = screen.getByRole("tab", { name: "Errors (1)" });
    expect(
      screen.getByRole("tablist", { name: "Task set sections" }),
    ).toContainElement(overviewTab);
    expect(overviewTab).toHaveAttribute("aria-selected", "true");
    expect(
      document.getElementById(
        errorsTab.getAttribute("aria-controls") ?? "missing",
      ),
    ).toHaveAttribute("aria-labelledby", errorsTab.id);

    overviewTab.focus();
    await user.keyboard("{ArrowRight}");
    expect(errorsTab).toHaveFocus();
    expect(errorsTab).toHaveAttribute("aria-selected", "true");
    expect(screen.getByRole("tabpanel")).toHaveTextContent("Task is invalid");

    await user.keyboard("{Home}");
    expect(overviewTab).toHaveAttribute("aria-selected", "true");
    expect(screen.getByText("Task Count")).toBeInTheDocument();
  });

  it("honors the existing URL-selected errors tab on first render", async () => {
    renderPage("/task-sets/task-set-1?tab=errors");

    await waitFor(() => {
      expect(screen.getByRole("tab", { name: "Errors (1)" })).toHaveAttribute(
        "aria-selected",
        "true",
      );
    });
    expect(screen.getByRole("tabpanel")).toHaveTextContent("Task is invalid");
  });
});
