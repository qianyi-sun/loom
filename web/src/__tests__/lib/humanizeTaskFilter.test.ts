import { describe, expect, it } from "vitest";

import { humanizeTaskFilter } from "../../lib/humanizeTaskFilter";

describe("humanizeTaskFilter", () => {
  it("summarizes all runnable tasks for benchmark ids", () => {
    const out = humanizeTaskFilter(
      { subset_kind: "all", benchmark_ids: ["humaneval"] },
      { matchedTaskCount: 164 },
    );

    expect(out.primary).toBe("HumanEval / all runnable tasks / 164 tasks");
    expect(out.details).toContain("Benchmark: humaneval");
  });

  it("summarizes seeded random subsets", () => {
    const out = humanizeTaskFilter(
      { subset_kind: "random_n", benchmark_ids: ["mbpp"], n: 25, seed: 7 },
      { matchedTaskCount: 25 },
    );

    expect(out.primary).toBe("MBPP / random 25 tasks / seed 7");
  });

  it("summarizes explicit ids without exposing raw JSON", () => {
    const out = humanizeTaskFilter(
      {
        subset_kind: "explicit",
        task_ids: ["humaneval/HumanEval/0", "humaneval/HumanEval/1"],
      },
      {},
    );

    expect(out.primary).toBe("2 explicit task IDs");
  });

  it("keeps unknown keys as diagnostics", () => {
    const out = humanizeTaskFilter(
      { subset_kind: "all", benchmark_id: "humaneval", unexpected: true },
      { matchedTaskCount: 164 },
    );

    expect(out.diagnostics).toContain("Unrecognized field: unexpected");
  });
});
