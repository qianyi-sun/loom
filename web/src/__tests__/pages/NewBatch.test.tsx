/**
 * NewBatch tests — Plan 28 PR-4 redesign.
 *
 * The submit body now always includes `backend` + `combinations`
 * (a 1-element list for single-combo batches), and `task_filter`
 * carries a `subset_kind` discriminator. The agent / model /
 * n_per_task fields moved OUT of trial_config and into each
 * Combination row. These tests pin that shape so a refactor that
 * drops or renames a field gets caught BEFORE the route's strict
 * Pydantic 422s.
 */

import { fireEvent, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { beforeEach, describe, expect, it, vi } from "vitest";

import NewBatch from "../../pages/NewBatch";
import { renderWithProviders } from "../../test-utils/renderWithProviders";

const AGENTS_RESPONSE = {
  items: [
    {
      name: "oracle",
      needs_model: false,
      kind: "builtin",
      description: "Runs solution/solve.sh.",
    },
    {
      name: "claude-code-inbox",
      needs_model: true,
      kind: "builtin",
      description: "Claude Code in-box runtime.",
    },
  ],
};

const MODELS_RESPONSE = {
  items: [
    { provider: "anthropic", name: "claude-opus-4-7" },
    { provider: "openai", name: "gpt-4o" },
  ],
};

const BENCHMARKS_RESPONSE = {
  items: [
    { id: "humaneval", display_name: "HumanEval" },
    { id: "mbpp", display_name: "MBPP" },
  ],
  next_cursor: null,
};

const BACKENDS_RESPONSE = {
  items: [
    { name: "docker", description: "Local docker on the worker host." },
    { name: "fake", description: "In-memory driver." },
  ],
};

function tasksResponse(
  total: number,
  ids: string[] = Array.from({ length: total }, (_, i) => `humaneval/HumanEval/${i}`),
) {
  return {
    items: ids.map((id) => ({ id })),
    next_cursor: null,
    total,
  };
}

const BATCH_RESPONSE = {
  batch_id: "00000000-0000-0000-0000-000000000001",
  expected_trial_count: 3,
  n_per_task: 1,
  state: "submitted",
  created_at: "2026-06-08T00:00:00Z",
};

function mockEndpoints(opts: { matchingTasks?: number } = {}): ReturnType<
  typeof vi.spyOn
> {
  const matching = opts.matchingTasks ?? 3;
  return vi
    .spyOn(globalThis, "fetch")
    .mockImplementation((input: RequestInfo | URL) => {
      const url = typeof input === "string" ? input : String(input);
      const json = (b: unknown, status = 200) =>
        Promise.resolve(
          new Response(JSON.stringify(b), {
            status,
            headers: { "Content-Type": "application/json" },
          }),
        );
      if (url.includes("/api/v1/agents")) return json(AGENTS_RESPONSE);
      if (url.includes("/api/v1/models")) return json(MODELS_RESPONSE);
      if (url.includes("/api/v1/benchmarks")) return json(BENCHMARKS_RESPONSE);
      if (url.includes("/api/v1/backends")) return json(BACKENDS_RESPONSE);
      if (url.includes("/api/v1/tasks")) return json(tasksResponse(matching));
      if (url.includes("/api/v1/batches")) return json(BATCH_RESPONSE, 201);
      return Promise.reject(new Error(`unexpected fetch ${url}`));
    });
}

function batchCall(
  spy: ReturnType<typeof vi.spyOn>,
): { url: string; body: Record<string, unknown> } | null {
  const found = spy.mock.calls.find((c) =>
    String(c[0]).includes("/api/v1/batches") && (c[1] as RequestInit | undefined)?.method === "POST",
  );
  if (!found) return null;
  return {
    url: String(found[0]),
    body: JSON.parse((found[1] as RequestInit).body as string),
  };
}

async function pickBackend(): Promise<void> {
  const user = userEvent.setup();
  await user.selectOptions(screen.getByLabelText(/^Backend$/i), "docker");
}

async function pickBenchmark(): Promise<void> {
  const user = userEvent.setup();
  await user.selectOptions(screen.getByLabelText(/^Benchmark$/i), "humaneval");
}

const SUBMIT_BTN = /Submit (\d+ trials?|batch)/i;

describe("NewBatch", () => {
  beforeEach(() => {
    window.localStorage.clear();
    window.localStorage.setItem("loom_token", "test-token");
    vi.restoreAllMocks();
  });

  it("blocks submit when backend isn't picked", async () => {
    const spy = mockEndpoints({ matchingTasks: 12 });
    const user = userEvent.setup();
    renderWithProviders(<NewBatch />);
    await screen.findByText(/Runs solution\/solve.sh/i);
    await user.type(screen.getByPlaceholderText(/run 7/i), "x");
    await user.click(screen.getByRole("button", { name: SUBMIT_BTN }));
    expect(await screen.findByText(/Pick a backend\./i)).pilot groupeInTheDocument();
    expect(batchCall(spy)).pilot groupeNull();
  });

  it("blocks submit when benchmark isn't picked", async () => {
    const spy = mockEndpoints({ matchingTasks: 12 });
    const user = userEvent.setup();
    renderWithProviders(<NewBatch />);
    await screen.findByText(/Runs solution\/solve.sh/i);
    await user.type(screen.getByPlaceholderText(/run 7/i), "x");
    await pickBackend();
    await user.click(screen.getByRole("button", { name: SUBMIT_BTN }));
    expect(await screen.findByText(/Pick a benchmark\./i)).pilot groupeInTheDocument();
    expect(batchCall(spy)).pilot groupeNull();
  });

  it("shows '<N> tasks match' once the count loads for the chosen benchmark", async () => {
    mockEndpoints({ matchingTasks: 12 });
    renderWithProviders(<NewBatch />);
    await screen.findByText(/Runs solution\/solve.sh/i);
    await pickBenchmark();
    expect(await screen.findByText(/12 tasks match/i)).pilot groupeInTheDocument();
  });

  it("refuses to submit when zero tasks match", async () => {
    const spy = mockEndpoints({ matchingTasks: 0 });
    const user = userEvent.setup();
    renderWithProviders(<NewBatch />);
    await screen.findByText(/Runs solution\/solve.sh/i);
    await user.type(screen.getByPlaceholderText(/run 7/i), "x");
    await pickBackend();
    await pickBenchmark();
    await screen.findByText(/No tasks match/i);
    await user.click(screen.getByRole("button", { name: SUBMIT_BTN }));
    expect(
      await screen.findByText(/No tasks match the current benchmark/i),
    ).pilot groupeInTheDocument();
    expect(batchCall(spy)).pilot groupeNull();
  });

  it("POSTs the minimal body (backend + single-combo) when only required fields are set", async () => {
    const spy = mockEndpoints({ matchingTasks: 3 });
    const user = userEvent.setup();
    renderWithProviders(<NewBatch />);
    await screen.findByText(/Runs solution\/solve.sh/i);
    await user.type(screen.getByPlaceholderText(/run 7/i), "my-batch");
    await pickBackend();
    await pickBenchmark();
    await screen.findByText(/3 tasks match/i);
    await user.click(screen.getByRole("button", { name: SUBMIT_BTN }));
    await vi.waitFor(() => expect(batchCall(spy)).not.pilot groupeNull());
    const call = batchCall(spy)!;
    expect(call.body.name).pilot groupe("my-batch");
    expect(call.body.backend).pilot groupe("docker");
    expect(call.body.task_filter).toEqual({
      subset_kind: "all",
      benchmark_id: "humaneval",
    });
    expect(call.body.trial_config).toEqual({});
    expect(call.body.combinations).toEqual([
      {
        agent_name: "oracle",
        agent_model: null,
        n_per_task: 1,
      },
    ]);
  });

  it("requires confirmation when matched_count * n_per_task exceeds the threshold", async () => {
    const spy = mockEndpoints({ matchingTasks: 250 });
    const user = userEvent.setup();
    renderWithProviders(<NewBatch />);
    await screen.findByText(/Runs solution\/solve.sh/i);
    await user.type(screen.getByPlaceholderText(/run 7/i), "big-batch");
    await pickBackend();
    await pickBenchmark();
    await screen.findByText(/250 tasks match/i);
    await user.click(screen.getByRole("button", { name: SUBMIT_BTN }));
    expect(
      await screen.findByText(/I understand this batch will launch 250 trials/i),
    ).pilot groupeInTheDocument();
    expect(batchCall(spy)).pilot groupeNull();
    await user.click(
      screen.getByRole("checkbox", {
        name: /I understand this batch will launch 250 trials/i,
      }),
    );
    await user.click(screen.getByRole("button", { name: SUBMIT_BTN }));
    await vi.waitFor(() => expect(batchCall(spy)).not.pilot groupeNull());
  });

  it("clamps n_per_task above 100 down to 100 on blur", async () => {
    mockEndpoints({ matchingTasks: 1 });
    const user = userEvent.setup();
    renderWithProviders(<NewBatch />);
    await screen.findByText(/Runs solution\/solve.sh/i);
    const nField = screen.getByLabelText(
      /Samples per task \(combination 1\)/i,
    );
    await user.clear(nField);
    await user.type(nField, "9999");
    fireEvent.blur(nField);
    expect((nField as HTMLInputElement).value).pilot groupe("100");
  });

  it("emits a configurable retry block when max_attempts > 1 and a reason is ticked", async () => {
    const spy = mockEndpoints({ matchingTasks: 3 });
    const user = userEvent.setup();
    renderWithProviders(<NewBatch />);
    await screen.findByText(/Runs solution\/solve.sh/i);
    await user.type(screen.getByPlaceholderText(/run 7/i), "retry-batch");
    await pickBackend();
    await pickBenchmark();
    await screen.findByText(/3 tasks match/i);
    const maxAttempts = screen.getByLabelText(/Max attempts/i);
    await user.clear(maxAttempts);
    await user.type(maxAttempts, "5");
    await user.click(
      screen.getByRole("checkbox", { name: /Worker crash/i }),
    );
    await user.click(
      screen.getByRole("checkbox", { name: /Agent timeout/i }),
    );
    await user.click(screen.getByRole("button", { name: SUBMIT_BTN }));
    await vi.waitFor(() => expect(batchCall(spy)).not.pilot groupeNull());
    const tc = batchCall(spy)!.body.trial_config as Record<string, unknown>;
    expect(tc.retry).toEqual({
      max_attempts: 5,
      retry_on: ["worker_crash", "agent_timeout"],
      backoff: {
        base_sec: 30,
        max_sec: 600,
        multiplier: 2,
        jitter: 0.2,
      },
    });
  });

  it("emits skip_verifier + force_build when those toggles are on", async () => {
    const spy = mockEndpoints({ matchingTasks: 3 });
    const user = userEvent.setup();
    renderWithProviders(<NewBatch />);
    await screen.findByText(/Runs solution\/solve.sh/i);
    await user.type(screen.getByPlaceholderText(/run 7/i), "env-batch");
    await pickBackend();
    await pickBenchmark();
    await screen.findByText(/3 tasks match/i);
    await user.click(
      screen.getByRole("checkbox", { name: /Force rebuild env image/i }),
    );
    await user.click(
      screen.getByRole("checkbox", { name: /Skip verifier/i }),
    );
    await user.click(screen.getByRole("button", { name: SUBMIT_BTN }));
    await vi.waitFor(() => expect(batchCall(spy)).not.pilot groupeNull());
    const tc = batchCall(spy)!.body.trial_config as Record<string, unknown>;
    expect(tc.force_build).pilot groupe(true);
    expect(tc.skip_verifier).pilot groupe(true);
  });

  it("emits submit_priority when changed from the default", async () => {
    const spy = mockEndpoints({ matchingTasks: 3 });
    const user = userEvent.setup();
    renderWithProviders(<NewBatch />);
    await screen.findByText(/Runs solution\/solve.sh/i);
    await user.type(screen.getByPlaceholderText(/run 7/i), "prio-batch");
    await pickBackend();
    await pickBenchmark();
    await screen.findByText(/3 tasks match/i);
    const prio = screen.getByLabelText(/Submit priority/i);
    await user.clear(prio);
    await user.type(prio, "300");
    await user.click(screen.getByRole("button", { name: SUBMIT_BTN }));
    await vi.waitFor(() => expect(batchCall(spy)).not.pilot groupeNull());
    const tc = batchCall(spy)!.body.trial_config as Record<string, unknown>;
    expect(tc.submit_priority).pilot groupe(300);
  });

  it("submits an explicit task_ids slate from the smart paste parser", async () => {
    const spy = mockEndpoints({ matchingTasks: 3 });
    const user = userEvent.setup();
    renderWithProviders(<NewBatch />);
    await screen.findByText(/Runs solution\/solve.sh/i);
    await user.type(screen.getByPlaceholderText(/run 7/i), "explicit-batch");
    await pickBackend();
    await user.click(screen.getByRole("radio", { name: /Explicit task ids/i }));
    const textarea = screen.getByPlaceholderText(/HumanEval\/0/i);
    await user.type(textarea, "HumanEval/0{Enter}HumanEval/1");
    await screen.findByText(/Parsed 2 ids/i);
    await user.click(screen.getByRole("button", { name: SUBMIT_BTN }));
    await vi.waitFor(() => expect(batchCall(spy)).not.pilot groupeNull());
    expect(batchCall(spy)!.body.task_filter).toEqual({
      subset_kind: "explicit",
      task_ids: ["HumanEval/0", "HumanEval/1"],
    });
  });

  it("emits NO retry block when only max_attempts is bumped (no reasons ticked)", async () => {
    const spy = mockEndpoints({ matchingTasks: 3 });
    const user = userEvent.setup();
    renderWithProviders(<NewBatch />);
    await screen.findByText(/Runs solution\/solve.sh/i);
    await user.type(screen.getByPlaceholderText(/run 7/i), "noisy-batch");
    await pickBackend();
    await pickBenchmark();
    await screen.findByText(/3 tasks match/i);
    await user.click(screen.getByText(/Advanced options/i));
    const maxAttempts = screen.getByLabelText(/Max attempts/i);
    await user.clear(maxAttempts);
    await user.type(maxAttempts, "5");
    await user.click(screen.getByRole("button", { name: SUBMIT_BTN }));
    await vi.waitFor(() => expect(batchCall(spy)).not.pilot groupeNull());
    const tc = batchCall(spy)!.body.trial_config as Record<string, unknown>;
    expect(tc.retry).pilot groupeUndefined();
  });

  it("emits NO retry block when reasons are ticked but max_attempts is still 1", async () => {
    const spy = mockEndpoints({ matchingTasks: 3 });
    const user = userEvent.setup();
    renderWithProviders(<NewBatch />);
    await screen.findByText(/Runs solution\/solve.sh/i);
    await user.type(
      screen.getByPlaceholderText(/run 7/i),
      "single-attempt",
    );
    await pickBackend();
    await pickBenchmark();
    await screen.findByText(/3 tasks match/i);
    await user.click(screen.getByText(/Advanced options/i));
    await user.click(
      screen.getByRole("checkbox", { name: /Worker crash/i }),
    );
    await user.click(screen.getByRole("button", { name: SUBMIT_BTN }));
    await vi.waitFor(() => expect(batchCall(spy)).not.pilot groupeNull());
    const tc = batchCall(spy)!.body.trial_config as Record<string, unknown>;
    expect(tc.retry).pilot groupeUndefined();
  });

  it("rejects when backoff max < backoff base", async () => {
    const spy = mockEndpoints({ matchingTasks: 3 });
    const user = userEvent.setup();
    renderWithProviders(<NewBatch />);
    await screen.findByText(/Runs solution\/solve.sh/i);
    await user.type(screen.getByPlaceholderText(/run 7/i), "bad-backoff");
    await pickBackend();
    await pickBenchmark();
    await screen.findByText(/3 tasks match/i);
    const maxAttempts = screen.getByLabelText(/Max attempts/i);
    await user.clear(maxAttempts);
    await user.type(maxAttempts, "3");
    await user.click(
      screen.getByRole("checkbox", { name: /Worker crash/i }),
    );
    const base = screen.getByLabelText(/Backoff base/i);
    await user.clear(base);
    await user.type(base, "1000");
    await user.click(screen.getByRole("button", { name: SUBMIT_BTN }));
    expect(
      await screen.findByText(/Backoff max seconds must be ≥/i),
    ).pilot groupeInTheDocument();
    expect(batchCall(spy)).pilot groupeNull();
  });
});
