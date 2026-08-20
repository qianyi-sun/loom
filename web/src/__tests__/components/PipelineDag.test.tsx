import { fireEvent, render, screen } from "@testing-library/react";
import { vi } from "vitest";

import type { PipelineNodeTopology, PipelineRunDetail, PipelineStageRunSummary } from "../../api/client";
import PipelineDag from "../../components/pipelines/PipelineDag";

function stage(overrides: Partial<PipelineStageRunSummary>): PipelineStageRunSummary { return { id: crypto.randomUUID(), node_key: "prepare", shard_key: "one", node_kind: "container", topological_level: 0, upstream_node_keys: [], state: "succeeded", domain_outcome: "opaque", reason_code: null, attempt_count: 1, resource_profile_name: "cpu@1", resource_class: "cpu", retry_allowed: false, retry_ineligible_reason: "stage_not_failed", ...overrides }; }
function projection(stages: PipelineStageRunSummary[]): { topology: PipelineNodeTopology[]; progress: PipelineRunDetail["progress"] } {
  const topology = [...new Map(stages.map((item) => [item.node_key, { node_key: item.node_key, node_kind: item.node_kind, topological_level: item.topological_level, upstream_node_keys: item.upstream_node_keys }])).values()];
  const nodes: PipelineRunDetail["progress"]["nodes"] = {};
  for (const item of stages) {
    const node = nodes[item.node_key] ?? { total_stage_runs: 0, completed_stage_runs: 0, states: {}, domain_outcomes: {} };
    node.total_stage_runs += 1; node.completed_stage_runs += ["succeeded", "failed", "cancelled", "skipped"].includes(item.state) ? 1 : 0; node.states[item.state] = (node.states[item.state] ?? 0) + 1; nodes[item.node_key] = node;
  }
  return { topology, progress: { total_stage_runs: stages.length, completed_stage_runs: stages.length, states: {}, domain_outcomes: {}, nodes } };
}

test("groups shards, retains every count, and toggles filtering", () => {
  const onSelect = vi.fn(); const data = projection([stage({}), stage({ id: crypto.randomUUID(), shard_key: "two", state: "failed", domain_outcome: null })]); render(<PipelineDag {...data} selectedNodeKey={null} onSelectNode={onSelect} />);
  const node = screen.getByRole("button", { name: /prepare, container, 2 shards, Succeeded 1, Failed 1/i });
  fireEvent.click(node); expect(onSelect).toHaveBeenCalledWith("prepare");
});

test("renders gate shape and immutable upstream levels", () => {
  const data = projection([stage({}), stage({ node_key: "gate", node_kind: "gate", topological_level: 1, upstream_node_keys: ["prepare"], state: "skipped", attempt_count: 0, resource_profile_name: null, resource_class: "controller" })]); render(<PipelineDag {...data} selectedNodeKey={null} onSelectNode={vi.fn()} />);
  expect(screen.getByRole("button", { name: /gate, gate, 1 shards, Skipped 1/i })).toBeInTheDocument();
});

test("supports keyboard traversal within and between topology levels", () => {
  const data = projection([
    stage({ node_key: "alpha" }),
    stage({ node_key: "beta" }),
    stage({ node_key: "gamma", topological_level: 1, upstream_node_keys: ["alpha"] }),
  ]);
  render(
    <PipelineDag
      {...data}
      selectedNodeKey="alpha"
      onSelectNode={vi.fn()}
    />,
  );
  const alpha = screen.getByRole("button", { name: /alpha, container/i });
  const beta = screen.getByRole("button", { name: /beta, container/i });
  const gamma = screen.getByRole("button", { name: /gamma, container/i });

  alpha.focus();
  fireEvent.keyDown(alpha, { key: "ArrowRight" });
  expect(beta).toHaveFocus();
  fireEvent.keyDown(beta, { key: "Home" });
  expect(alpha).toHaveFocus();
  fireEvent.keyDown(alpha, { key: "End" });
  expect(beta).toHaveFocus();
  fireEvent.keyDown(beta, { key: "ArrowDown" });
  expect(gamma).toHaveFocus();
  fireEvent.keyDown(gamma, { key: "ArrowUp" });
  expect([alpha, beta]).toContain(document.activeElement);
  const focused = document.activeElement;
  fireEvent.keyDown(gamma, { key: "Escape" });
  expect(document.activeElement).toBe(focused);
});

test("clears an already selected node", () => {
  const onSelect = vi.fn();
  const data = projection([stage({})]);
  render(
    <PipelineDag {...data} selectedNodeKey="prepare" onSelectNode={onSelect} />,
  );
  fireEvent.click(screen.getByRole("button", { name: /prepare, container/i }));
  expect(onSelect).toHaveBeenCalledWith(null);
});
