import { screen } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";

import TaskSetsList from "../../pages/TaskSetsList";
import { jsonResponse } from "../../test-utils/fetchMock";
import { renderWithProviders } from "../../test-utils/renderWithProviders";

const baseItem = {
  task_set_id: "task-set-ready",
  display_name: "Ready evaluation set",
  status: "ready",
  status_reason: null,
  intents: ["evaluation", "trajectory_generation"],
  manifest_intents: ["evaluation"],
  inferred_intents: ["trajectory_generation"],
  capabilities: ["evaluation", "trajectory_generation"],
  warnings: [],
  evaluation_ready: true,
  task_count: 2,
  error_summary: [],
  materialization_job_state: null,
  created_at: "2026-07-16T00:00:00Z",
};

describe("TaskSetsList", () => {
  afterEach(() => vi.restoreAllMocks());

  it("renders empty and error states", async () => {
    vi.spyOn(globalThis, "fetch").mockResolvedValueOnce(jsonResponse({ items: [] }));
    const { unmount } = renderWithProviders(<TaskSetsList />);
    expect(await screen.findByText("No task sets yet")).toBeInTheDocument();
    unmount();

    vi.spyOn(globalThis, "fetch").mockResolvedValueOnce(jsonResponse({ detail: "bad" }, 500));
    renderWithProviders(<TaskSetsList />);
    expect(await screen.findByText("Could not load task sets.")).toBeInTheDocument();
  });

  it("renders every status and capability classification", async () => {
    const variants = [
      ["ready", true, ["evaluation", "trajectory_generation"], "both"],
      ["materializing", true, ["evaluation"], "evaluation-ready"],
      ["partial", false, ["trajectory_generation"], "trajectory-only"],
      ["failed", false, [], "trajectory-only"],
      ["deleted", false, [], "trajectory-only"],
      ["unknown", false, [], "trajectory-only"],
    ] as const;
    vi.spyOn(globalThis, "fetch").mockResolvedValueOnce(
      jsonResponse({
        items: variants.map(([status, evaluationReady, intents], index) => ({
          ...baseItem,
          task_set_id: `task-set-${index}`,
          display_name: `Task set ${index}`,
          status,
          evaluation_ready: evaluationReady,
          intents: [...intents],
        })),
      }),
    );

    renderWithProviders(<TaskSetsList />);
    expect(await screen.findByText("Task set 0")).toHaveAttribute("href", "/task-sets/task-set-0");
    for (const [, , , label] of variants) {
      expect(screen.getAllByText(label).length).toBeGreaterThan(0);
    }
    expect(screen.getByText("materializing")).toBeInTheDocument();
    expect(screen.getByText("partial")).toBeInTheDocument();
    expect(screen.getByText("failed")).toBeInTheDocument();
    expect(screen.getByText("deleted")).toBeInTheDocument();
    expect(screen.getByText("unknown")).toBeInTheDocument();
  });
});
