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

function renderPage(
  initialPath = "/task-sets/task-set-1",
  fetchImpl?: (input: RequestInfo | URL, init?: RequestInit) => Promise<Response>,
): void {
  vi.stubGlobal(
    "fetch",
    vi.fn().mockImplementation(
      fetchImpl ??
        ((_input: RequestInfo | URL, init?: RequestInit) =>
          Promise.resolve(
            init?.method === "DELETE"
              ? new Response(null, { status: 204 })
              : new Response(JSON.stringify(TASK_SET), {
                  status: 200,
                  headers: { "Content-Type": "application/json" },
                }),
          )),
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
          <Route path="/task-sets" element={<h1>Task sets</h1>} />
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

  it("requires the exact TaskSet id and navigates only after success", async () => {
    const user = userEvent.setup();
    renderPage();

    await user.click(await screen.findByRole("button", { name: "Delete" }));
    expect(
      screen.getByRole("dialog", { name: "Delete task set" }),
    ).toBeInTheDocument();
    const confirm = screen.getByRole("button", { name: "Delete TaskSet" });
    const input = screen.getByLabelText("Type the TaskSet id to confirm");
    expect(confirm).toBeDisabled();
    await user.type(input, "Task-Set-1");
    expect(confirm).toBeDisabled();
    await user.clear(input);
    await user.type(input, "task-set-1");
    await user.click(confirm);

    await waitFor(() => {
      expect(globalThis.fetch).toHaveBeenCalledWith(
        "/api/v1/tasksets/task-set-1",
        expect.objectContaining({ method: "DELETE" }),
      );
      expect(
        screen.queryByRole("dialog", { name: "Delete task set" }),
      ).not.toBeInTheDocument();
    });
  });

  it("keeps the delete context open for a retryable failure", async () => {
    const user = userEvent.setup();
    renderPage(
      "/task-sets/task-set-1",
      (_input, init) =>
        Promise.resolve(
          init?.method === "DELETE"
            ? new Response(
                JSON.stringify({ detail: "conflict sk-proj-abcdefghijklmnopqrstuvwxyz" }),
                {
                  status: 409,
                  headers: { "Content-Type": "application/json" },
                },
              )
            : new Response(JSON.stringify(TASK_SET), {
                status: 200,
                headers: { "Content-Type": "application/json" },
              }),
        ),
    );

    await user.click(await screen.findByRole("button", { name: "Delete" }));
    await user.type(
      screen.getByLabelText("Type the TaskSet id to confirm"),
      "task-set-1",
    );
    await user.click(screen.getByRole("button", { name: "Delete TaskSet" }));

    expect(await screen.findByRole("alert")).toHaveTextContent("Error 409");
    expect(screen.getByRole("alert")).toHaveTextContent("[REDACTED]");
    expect(
      screen.getByLabelText("Type the TaskSet id to confirm"),
    ).toHaveValue("task-set-1");
  });
});
