/// <reference types="node" />
import { describe, expect, it } from "vitest";
import { execFileSync, spawnSync } from "node:child_process";
import type { CreateBatchBody } from "../api";
import { batchExport } from "../pages/newBatch/exportBatch";
import { shellQuote } from "../lib/shellQuote";

const payload: CreateBatchBody = {
  team_id: "team-1", purpose: "trajectory_generation", name_suffix: "test's $(echo should-not-run)",
  task_filter: { task_set_ids: ["taskset"], benchmark_ids: ["benchmark"], tag_filters: { language: ["py", "C++"] }, subset_kind: "random_n", n: 4, seed: 72 },
  combinations: [
    { agent_name: "terminus-2", agent_version: "harbor-v2", agent_model: { provider: "openai", name: "model-1", source: "api" }, provider_connection_id: "provider-1", provider_model_id: "model-1", n_per_task: 3, label: "A's label" },
    { agent_name: "oracle", agent_model: null, n_per_task: 2, label: "Oracle" },
  ],
  trial_config: { skip_verifier: true, force_build: true, retry: { max_attempts: 2, retry_on: ["worker_crash"] } },
  budget_usd: 2.5, budget_policy: "soft", budget_confirmed: true,
};

describe("batch command export", () => {
  it("keeps the complete request and pins the current deployment", () => {
    const result = batchExport(payload, "/staging", "https://loom.example");
    expect(JSON.parse(result.json)).toEqual(payload);
    expect(result.cli).toContain("--server 'https://loom.example/staging'");
    expect(result.cli).toContain("--request-json @batch.json");
    expect(result.cli).toContain('--username "$LOOM_USERNAME" --password env:LOOM_PASSWORD');
    expect(result.cli).toContain("--team-id 'team-1'");
    expect(result.cli).not.toContain("LOOM_TOKEN");
    expect(result.api).toContain("LOOM_TOKEN");
    // Replace curl by printf; execute only argument handling, never a request.
    const args = execFileSync("/bin/sh", ["-c", result.api.replace(/^curl/, "printf '%s\\0'")], { encoding: "utf8", env: { LOOM_TOKEN: "test-placeholder" } }).split("\0");
    expect(args).toContain("https://loom.example/staging/api/v1/batches");
    expect(JSON.parse(args[args.indexOf("--data-raw") + 1])).toEqual(payload);
  });
  it("does not submit when login or team selection fails", () => {
    const result = batchExport(payload, "/staging", "https://loom.example");
    const execution = spawnSync("/bin/sh", ["-c", `loom() { printf '%s\\n' "$*"; return 1; }; ${result.cli}`], {
      encoding: "utf8", env: { LOOM_USERNAME: "test-user" },
    });
    expect(execution.status).toBe(1);
    expect(execution.stdout).toContain("auth login");
    expect(execution.stdout).not.toContain("eval batch create");
  });
  it("preserves shell metacharacters as literal data", () => {
    const raw = "single' double\" `command` $(echo bad)\\newline\nnext";
    expect(execFileSync("/bin/sh", ["-c", `printf '%s' ${shellQuote(raw)}`], { encoding: "utf8" })).toBe(raw);
  });
  it("keeps explicit tasks and the production prefix", () => {
    const body = { ...payload, purpose: "evaluation" as const, task_filter: { subset_kind: "explicit" as const, task_ids: ["a/0", "b/1"] } };
    const result = batchExport(body, "/prod", "https://loom.example");
    expect(JSON.parse(result.json)).toEqual(body);
    expect(result.api).toContain("https://loom.example/prod/api/v1/batches");
  });
});
