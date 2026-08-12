import { fireEvent, render, screen } from "@testing-library/react";
import { vi } from "vitest";

import type { PipelineStageRunSummary } from "../../api/client";
import PipelineStageList from "../../components/pipelines/PipelineStageList";

function stage(index: number, overrides: Partial<PipelineStageRunSummary> = {}): PipelineStageRunSummary {
  return {
    id: `stage-${index}`,
    node_key: `node-${String(index).padStart(3, "0")}`,
    shard_key: `shard-${index}`,
    node_kind: "container",
    topological_level: index,
    upstream_node_keys: [],
    state: "succeeded",
    domain_outcome: null,
    reason_code: null,
    attempt_count: 1,
    resource_profile_name: "cpu@1",
    resource_class: "cpu",
    retry_allowed: false,
    retry_ineligible_reason: "stage_not_failed",
    ...overrides,
  };
}

test("sorts, filters, opens, and resets the ordinary stage table", () => {
  const onOpen = vi.fn();
  const stages = [
    stage(2, { node_key: "zeta", shard_key: "b", state: "failed", domain_outcome: "bad", resource_class: "gpu", retry_allowed: true, retry_ineligible_reason: null }),
    stage(1, { node_key: "alpha", shard_key: "a", topological_level: 0, domain_outcome: "good" }),
  ];
  const { rerender } = render(<PipelineStageList stages={stages} selectedNodeKey={null} onOpen={onOpen} />);
  const rows = screen.getAllByRole("row");
  expect(rows[1]).toHaveTextContent("alpha");
  fireEvent.click(rows[1]);
  fireEvent.keyDown(rows[2], { key: "Enter" });
  expect(onOpen).toHaveBeenNthCalledWith(1, stages[1]);
  expect(onOpen).toHaveBeenNthCalledWith(2, stages[0]);

  fireEvent.change(screen.getByLabelText("State"), { target: { value: "failed" } });
  fireEvent.change(screen.getByLabelText("Domain outcome"), { target: { value: "bad" } });
  fireEvent.change(screen.getByLabelText("Resource class"), { target: { value: "gpu" } });
  expect(screen.getByText(/1 StageRuns/)).toBeInTheDocument();
  expect(screen.getByText("Eligible")).toBeInTheDocument();

  fireEvent.change(screen.getByLabelText("State"), { target: { value: "" } });
  fireEvent.change(screen.getByLabelText("Domain outcome"), { target: { value: "" } });
  fireEvent.change(screen.getByLabelText("Resource class"), { target: { value: "" } });
  rerender(<PipelineStageList stages={stages} selectedNodeKey="alpha" onOpen={onOpen} />);
  expect(screen.getByLabelText("Node key")).toHaveValue("alpha");
  expect(screen.getByText(/1 StageRuns/)).toBeInTheDocument();
});

test("virtualizes large pages, handles focus keys, scrolling, and pagination", () => {
  const onOpen = vi.fn();
  const stages = Array.from({ length: 260 }, (_, index) => stage(index, {
    node_key: "bulk",
    topological_level: 0,
    state: index === 0 ? "cancelled" : "queued",
    retry_ineligible_reason: index === 0 ? null : "stage_not_failed",
  }));
  render(<PipelineStageList stages={stages} selectedNodeKey={null} onOpen={onOpen} />);

  const table = screen.getByRole("table");
  expect(table).toHaveAttribute("aria-rowcount", "250");
  const first = screen.getByRole("row", { name: /bulk shard-0/i });
  fireEvent.keyDown(first, { key: "ArrowDown" });
  fireEvent.keyDown(first, { key: "ArrowUp" });
  fireEvent.keyDown(first, { key: "Enter" });
  fireEvent.doubleClick(first);
  expect(onOpen).toHaveBeenCalledTimes(2);
  fireEvent.scroll(table, { target: { scrollTop: 440 } });

  fireEvent.click(screen.getByRole("button", { name: "Next" }));
  expect(screen.getByText(/Page 2 of 2/)).toBeInTheDocument();
  expect(screen.getAllByRole("row")).toHaveLength(11);
  fireEvent.click(screen.getByRole("button", { name: "Previous" }));
  expect(screen.getByText(/Page 1 of 2/)).toBeInTheDocument();
});
