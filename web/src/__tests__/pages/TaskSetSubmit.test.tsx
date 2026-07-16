import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { MemoryRouter, Route, Routes } from "react-router-dom";
import { afterEach, describe, expect, it, vi } from "vitest";

import TaskSetSubmit from "../../pages/TaskSetSubmit";
import { jsonResponse } from "../../test-utils/fetchMock";

function renderPage(): void {
  const queryClient = new QueryClient({
    defaultOptions: { mutations: { retry: false } },
  });
  render(
    <QueryClientProvider client={queryClient}>
      <MemoryRouter
        future={{ v7_startTransition: true, v7_relativeSplatPath: true }}
        initialEntries={["/task-sets/new"]}
      >
        <Routes>
          <Route path="/task-sets/new" element={<TaskSetSubmit />} />
          <Route path="/task-sets/:id" element={<p>Task set detail</p>} />
          <Route path="/task-sets" element={<p>Task set list</p>} />
        </Routes>
      </MemoryRouter>
    </QueryClientProvider>,
  );
}

describe("TaskSetSubmit", () => {
  afterEach(() => vi.restoreAllMocks());

  it("requires a manifest and supports cancel", async () => {
    const user = userEvent.setup();
    renderPage();
    await user.click(screen.getByRole("button", { name: "Submit Task Set" }));
    expect(screen.getByText("A manifest file is required.")).toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: "Cancel" }));
    expect(screen.getByText("Task set list")).toBeInTheDocument();
  });

  it("uploads required and optional files then navigates", async () => {
    const user = userEvent.setup();
    const fetchMock = vi.spyOn(globalThis, "fetch").mockResolvedValueOnce(
      jsonResponse({ task_set_id: "uploaded/set", status: "ready" }, 201),
    );
    renderPage();
    await user.upload(
      screen.getByLabelText("Manifest (required)"),
      new File(["kind: UserTaskSet"], "manifest.yaml", { type: "text/yaml" }),
    );
    await user.upload(
      screen.getByLabelText("Verifier (optional)"),
      new File(["pass"], "verify.py", { type: "text/x-python" }),
    );
    await user.upload(
      screen.getByLabelText("Transform (optional)"),
      new File(["pass"], "transform.py", { type: "text/x-python" }),
    );
    await user.click(screen.getByRole("button", { name: "Submit Task Set" }));

    expect(await screen.findByText("Task set detail")).toBeInTheDocument();
    const body = fetchMock.mock.calls[0]?.[1]?.body;
    expect(body).toBeInstanceOf(FormData);
    expect((body as FormData).get("manifest")).toBeInstanceOf(File);
    expect((body as FormData).get("verifier")).toBeInstanceOf(File);
    expect((body as FormData).get("transform")).toBeInstanceOf(File);
  });

  it("shows API details and the generic fallback", async () => {
    const user = userEvent.setup();
    vi.spyOn(globalThis, "fetch")
      .mockResolvedValueOnce(jsonResponse({ detail: "Manifest is invalid" }, 422))
      .mockRejectedValueOnce(new Error("offline"));
    renderPage();
    const manifest = screen.getByLabelText("Manifest (required)");
    await user.upload(manifest, new File(["bad"], "bad.yaml"));
    await user.click(screen.getByRole("button", { name: "Submit Task Set" }));
    expect(await screen.findByText("Manifest is invalid")).toBeInTheDocument();

    await user.click(screen.getByRole("button", { name: "Submit Task Set" }));
    await waitFor(() => {
      expect(screen.getByText("Submission failed. Please try again.")).toBeInTheDocument();
    });
  });
});
