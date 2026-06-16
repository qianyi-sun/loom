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
      supported_providers: ["*"],
      supported_model_sources: [],
    },
    {
      name: "claude-code-inbox",
      needs_model: true,
      kind: "builtin",
      description: "Claude Code in-box runtime.",
      supported_providers: ["anthropic"],
      supported_model_sources: ["api"],
    },
  ],
};

const MODELS_RESPONSE = {
  items: [
    { provider: "anthropic", name: "claude-opus-4-7" },
    { provider: "openai", name: "gpt-4o" },
  ],
};

const LOCAL_SERVERS_RESPONSE = { items: [] };

const BENCHMARKS_RESPONSE = {
  items: [
    {
      id: "humaneval",
      display_name: "HumanEval",
      task_count: 12,
      series: null,
    },
    {
      id: "mbpp",
      display_name: "MBPP",
      task_count: 3,
      series: null,
    },
    {
      id: "aime-22",
      display_name: "AIME (AIMO validation 2022–2024)",
      task_count: 90,
      series: "aime",
    },
    {
      id: "aime-25",
      display_name: "AIME 2025",
      task_count: 30,
      series: "aime",
    },
  ],
  next_cursor: null,
};

const BENCHMARK_TAGS_RESPONSES: Record<string, { items: { key: string; values: string[] }[] }> = {
  "aime-22": {
    items: [
      { key: "year", values: ["2022", "2023", "2024"] },
      { key: "exam", values: ["I", "II"] },
    ],
  },
  "aime-25": {
    items: [
      { key: "year", values: ["2025"] },
      { key: "exam", values: ["I", "II"] },
    ],
  },
  humaneval: { items: [] },
  mbpp: { items: [] },
};

const BACKENDS_RESPONSE = {
  items: [
    { name: "docker", description: "Local docker on the worker host.", available: true },
    { name: "fake", description: "In-memory driver.", available: true },
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

function mockEndpoints(opts: {
  matchingTasks?: number;
  /**
   * Override for `POST /api/v1/tasks/count`. When set, the count
   * endpoint returns this value regardless of body. Used by the
   * issue-#28 tests to simulate "tag filter narrows to zero".
   */
  tasksCount?: number;
} = {}): ReturnType<typeof vi.spyOn> {
  const matching = opts.matchingTasks ?? 12;
  const benchmarksResponse = {
    ...BENCHMARKS_RESPONSE,
    items: BENCHMARKS_RESPONSE.items.map((b) =>
      b.id === "humaneval" ? { ...b, task_count: matching } : b,
    ),
  };
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
      if (url.includes("/api/v1/local-servers")) return json(LOCAL_SERVERS_RESPONSE);
      const tagsMatch = url.match(/\/api\/v1\/benchmarks\/([^/]+)\/tags/);
      if (tagsMatch) {
        const id = decodeURIComponent(tagsMatch[1]);
        return json(BENCHMARK_TAGS_RESPONSES[id] ?? { items: [] });
      }
      if (url.includes("/api/v1/benchmarks")) return json(benchmarksResponse);
      if (url.includes("/api/v1/backends")) return json(BACKENDS_RESPONSE);
      // `/tasks/count` MUST be checked before `/tasks` since the
      // substring match would otherwise route both to the list stub.
      if (url.includes("/api/v1/tasks/count")) {
        return json({ count: opts.tasksCount ?? matching });
      }
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

async function pickBenchmark(id: string = "humaneval"): Promise<void> {
  const user = userEvent.setup();
  // The picker is a series-grouped checkbox list now — locate the row
  // by its accessible name and tick it.
  const cb = await screen.findByRole("checkbox", {
    name: new RegExp(`Select benchmark ${id}`, "i"),
  });
  await user.click(cb);
}

const SUBMIT_BTN = /Submit (\d+ trials?|batch)/i;

describe("NewBatch", () => {
  beforeEach(() => {
    window.localStorage.clear();
    window.localStorage.setItem("loom_token", "test-token");
    vi.restoreAllMocks();
  });

  it("defaults backend to docker once the catalog loads", async () => {
    mockEndpoints({ matchingTasks: 12 });
    renderWithProviders(<NewBatch />);
    await screen.findByText(/Runs solution\/solve.sh/i);
    const dropdown = (await screen.findByLabelText(
      "Backend",
    )) as HTMLSelectElement;
    expect(dropdown.value).toBe("docker");
  });

  it("blocks submit when no benchmark is picked", async () => {
    const spy = mockEndpoints({ matchingTasks: 12 });
    const user = userEvent.setup();
    renderWithProviders(<NewBatch />);
    await screen.findByText(/Runs solution\/solve.sh/i);
    await user.type(screen.getByPlaceholderText(/run 7/i), "x");
    await pickBackend();
    await user.click(screen.getByRole("button", { name: SUBMIT_BTN }));
    expect(
      await screen.findByText(/Pick at least one benchmark\./i),
    ).toBeInTheDocument();
    expect(batchCall(spy)).toBeNull();
  });

  it("shows '<N> tasks match' once the count loads for the chosen benchmark", async () => {
    mockEndpoints({ matchingTasks: 12 });
    renderWithProviders(<NewBatch />);
    await screen.findByText(/Runs solution\/solve.sh/i);
    await pickBenchmark();
    expect(
      await screen.findByText(/12 tasks match across 1 benchmark/i),
    ).toBeInTheDocument();
  });

  it("POSTs the minimal body (backend + single-combo) when only required fields are set", async () => {
    const spy = mockEndpoints({ matchingTasks: 12 });
    const user = userEvent.setup();
    renderWithProviders(<NewBatch />);
    await screen.findByText(/Runs solution\/solve.sh/i);
    await user.type(screen.getByPlaceholderText(/run 7/i), "my-batch");
    await pickBackend();
    await pickBenchmark();
    await screen.findByText(/12 tasks match across 1 benchmark/i);
    await user.click(screen.getByRole("button", { name: SUBMIT_BTN }));
    await vi.waitFor(() => expect(batchCall(spy)).not.toBeNull());
    const call = batchCall(spy)!;
    expect(call.body.name).toBe("my-batch");
    expect(call.body.backend).toBe("docker");
    expect(call.body.task_filter).toEqual({
      subset_kind: "all",
      benchmark_ids: ["humaneval"],
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
    await screen.findByText(/250 tasks match across 1 benchmark/i);
    await user.click(screen.getByRole("button", { name: SUBMIT_BTN }));
    expect(
      await screen.findByText(/I understand this batch will launch 250 trials/i),
    ).toBeInTheDocument();
    expect(batchCall(spy)).toBeNull();
    await user.click(
      screen.getByRole("checkbox", {
        name: /I understand this batch will launch 250 trials/i,
      }),
    );
    await user.click(screen.getByRole("button", { name: SUBMIT_BTN }));
    await vi.waitFor(() => expect(batchCall(spy)).not.toBeNull());
  });

  it("clamps n_per_task above 100 down to 100 on blur", async () => {
    mockEndpoints({ matchingTasks: 12 });
    const user = userEvent.setup();
    renderWithProviders(<NewBatch />);
    await screen.findByText(/Runs solution\/solve.sh/i);
    const nField = screen.getByLabelText(
      /Samples per task \(combination 1\)/i,
    );
    await user.clear(nField);
    await user.type(nField, "9999");
    fireEvent.blur(nField);
    expect((nField as HTMLInputElement).value).toBe("100");
  });

  it("emits a configurable retry block when max_attempts > 1 and a reason is ticked", async () => {
    const spy = mockEndpoints({ matchingTasks: 12 });
    const user = userEvent.setup();
    renderWithProviders(<NewBatch />);
    await screen.findByText(/Runs solution\/solve.sh/i);
    await user.type(screen.getByPlaceholderText(/run 7/i), "retry-batch");
    await pickBackend();
    await pickBenchmark();
    await screen.findByText(/12 tasks match across 1 benchmark/i);
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
    await vi.waitFor(() => expect(batchCall(spy)).not.toBeNull());
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
    const spy = mockEndpoints({ matchingTasks: 12 });
    const user = userEvent.setup();
    renderWithProviders(<NewBatch />);
    await screen.findByText(/Runs solution\/solve.sh/i);
    await user.type(screen.getByPlaceholderText(/run 7/i), "env-batch");
    await pickBackend();
    await pickBenchmark();
    await screen.findByText(/12 tasks match across 1 benchmark/i);
    await user.click(
      screen.getByRole("checkbox", { name: /Force rebuild env image/i }),
    );
    await user.click(
      screen.getByRole("checkbox", { name: /Skip verifier/i }),
    );
    await user.click(screen.getByRole("button", { name: SUBMIT_BTN }));
    await vi.waitFor(() => expect(batchCall(spy)).not.toBeNull());
    const tc = batchCall(spy)!.body.trial_config as Record<string, unknown>;
    expect(tc.force_build).toBe(true);
    expect(tc.skip_verifier).toBe(true);
  });

  it("emits submit_priority when changed from the default", async () => {
    const spy = mockEndpoints({ matchingTasks: 12 });
    const user = userEvent.setup();
    renderWithProviders(<NewBatch />);
    await screen.findByText(/Runs solution\/solve.sh/i);
    await user.type(screen.getByPlaceholderText(/run 7/i), "prio-batch");
    await pickBackend();
    await pickBenchmark();
    await screen.findByText(/12 tasks match across 1 benchmark/i);
    const prio = screen.getByLabelText(/Submit priority/i);
    await user.clear(prio);
    await user.type(prio, "300");
    await user.click(screen.getByRole("button", { name: SUBMIT_BTN }));
    await vi.waitFor(() => expect(batchCall(spy)).not.toBeNull());
    const tc = batchCall(spy)!.body.trial_config as Record<string, unknown>;
    expect(tc.submit_priority).toBe(300);
  });

  it("submits an explicit task_ids slate from the smart paste parser", async () => {
    const spy = mockEndpoints({ matchingTasks: 12 });
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
    await vi.waitFor(() => expect(batchCall(spy)).not.toBeNull());
    expect(batchCall(spy)!.body.task_filter).toEqual({
      subset_kind: "explicit",
      task_ids: ["HumanEval/0", "HumanEval/1"],
    });
  });

  it("emits NO retry block when only max_attempts is bumped (no reasons ticked)", async () => {
    const spy = mockEndpoints({ matchingTasks: 12 });
    const user = userEvent.setup();
    renderWithProviders(<NewBatch />);
    await screen.findByText(/Runs solution\/solve.sh/i);
    await user.type(screen.getByPlaceholderText(/run 7/i), "noisy-batch");
    await pickBackend();
    await pickBenchmark();
    await screen.findByText(/12 tasks match across 1 benchmark/i);
    await user.click(screen.getByText(/Advanced options/i));
    const maxAttempts = screen.getByLabelText(/Max attempts/i);
    await user.clear(maxAttempts);
    await user.type(maxAttempts, "5");
    await user.click(screen.getByRole("button", { name: SUBMIT_BTN }));
    await vi.waitFor(() => expect(batchCall(spy)).not.toBeNull());
    const tc = batchCall(spy)!.body.trial_config as Record<string, unknown>;
    expect(tc.retry).toBeUndefined();
  });

  it("emits NO retry block when reasons are ticked but max_attempts is still 1", async () => {
    const spy = mockEndpoints({ matchingTasks: 12 });
    const user = userEvent.setup();
    renderWithProviders(<NewBatch />);
    await screen.findByText(/Runs solution\/solve.sh/i);
    await user.type(
      screen.getByPlaceholderText(/run 7/i),
      "single-attempt",
    );
    await pickBackend();
    await pickBenchmark();
    await screen.findByText(/12 tasks match across 1 benchmark/i);
    await user.click(screen.getByText(/Advanced options/i));
    await user.click(
      screen.getByRole("checkbox", { name: /Worker crash/i }),
    );
    await user.click(screen.getByRole("button", { name: SUBMIT_BTN }));
    await vi.waitFor(() => expect(batchCall(spy)).not.toBeNull());
    const tc = batchCall(spy)!.body.trial_config as Record<string, unknown>;
    expect(tc.retry).toBeUndefined();
  });

  it("rejects when backoff max < backoff base", async () => {
    const spy = mockEndpoints({ matchingTasks: 12 });
    const user = userEvent.setup();
    renderWithProviders(<NewBatch />);
    await screen.findByText(/Runs solution\/solve.sh/i);
    await user.type(screen.getByPlaceholderText(/run 7/i), "bad-backoff");
    await pickBackend();
    await pickBenchmark();
    await screen.findByText(/12 tasks match across 1 benchmark/i);
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
    ).toBeInTheDocument();
    expect(batchCall(spy)).toBeNull();
  });

  // Series/tags catalog redesign (PR-3) — multi-benchmark group-select
  // and tag-filter card behavior.

  it("group-selects all benchmarks in a series with the series checkbox", async () => {
    const spy = mockEndpoints();
    const user = userEvent.setup();
    renderWithProviders(<NewBatch />);
    await screen.findByText(/Runs solution\/solve.sh/i);
    await user.type(screen.getByPlaceholderText(/run 7/i), "aime-batch");
    await pickBackend();
    await user.click(
      await screen.findByRole("checkbox", {
        name: /Select all in series aime/i,
      }),
    );
    // 90 (aime-22) + 30 (aime-25) = 120 across 2 benchmarks.
    await screen.findByText(/120 tasks match across 2 benchmarks/i);
    await user.click(screen.getByRole("button", { name: SUBMIT_BTN }));
    await vi.waitFor(() => expect(batchCall(spy)).not.toBeNull());
    expect(batchCall(spy)!.body.task_filter).toEqual({
      subset_kind: "all",
      benchmark_ids: ["aime-22", "aime-25"],
    });
  });

  it("emits tag_filters and surfaces the real count from /tasks/count", async () => {
    // Issue #28: with tag filters active the count summary now reflects
    // the exact filtered count (via POST /api/v1/tasks/count) instead
    // of the pre-filter upper bound.
    const spy = mockEndpoints({ tasksCount: 14 });
    const user = userEvent.setup();
    renderWithProviders(<NewBatch />);
    await screen.findByText(/Runs solution\/solve.sh/i);
    await user.type(screen.getByPlaceholderText(/run 7/i), "filtered");
    await pickBackend();
    await pickBenchmark("aime-22");
    await user.click(
      await screen.findByRole("button", { name: "2024", pressed: false }),
    );
    await user.click(
      await screen.findByRole("button", { name: "I", pressed: false }),
    );
    // Real count from the mocked /tasks/count endpoint.
    await screen.findByText(/14 tasks match the current benchmark \+ tag filters/i);
    await user.click(screen.getByRole("button", { name: SUBMIT_BTN }));
    await vi.waitFor(() => expect(batchCall(spy)).not.toBeNull());
    expect(batchCall(spy)!.body.task_filter).toEqual({
      subset_kind: "all",
      benchmark_ids: ["aime-22"],
      tag_filters: { exam: ["I"], year: ["2024"] },
    });
  });

  it("gates submit when tag filters narrow the slate to zero (issue #28)", async () => {
    // Previously the SPA let the user submit a tag-filtered batch
    // whose resolved set was empty, and the backend would 400 late.
    // Now /tasks/count returns 0 and the local gate fires with a
    // tag-filter-specific error message.
    const spy = mockEndpoints({ tasksCount: 0 });
    const user = userEvent.setup();
    renderWithProviders(<NewBatch />);
    await screen.findByText(/Runs solution\/solve.sh/i);
    await user.type(screen.getByPlaceholderText(/run 7/i), "narrowed-to-zero");
    await pickBackend();
    await pickBenchmark("aime-22");
    await user.click(
      await screen.findByRole("button", { name: "2024", pressed: false }),
    );
    // Wait for the zero-result count summary to land — proves
    // /tasks/count drove the message, not the local upper bound.
    await screen.findByText(/Tag filters narrow the slate to zero tasks/i);
    // Submitting should NOT POST a batch.
    await user.click(screen.getByRole("button", { name: SUBMIT_BTN }));
    // Give React time to re-render after the click; if a batch POST
    // was about to happen it'd be captured by the spy.
    await new Promise((resolve) => setTimeout(resolve, 50));
    expect(batchCall(spy)).toBeNull();
  });

  it("drops tag_filters whose key vanished after deselecting the only benchmark with that key", async () => {
    const spy = mockEndpoints();
    const user = userEvent.setup();
    renderWithProviders(<NewBatch />);
    await screen.findByText(/Runs solution\/solve.sh/i);
    await user.type(screen.getByPlaceholderText(/run 7/i), "swap-batch");
    await pickBackend();
    await pickBenchmark("aime-22");
    await user.click(
      await screen.findByRole("button", { name: "2024", pressed: false }),
    );
    // Now drop the aime benchmark and pick humaneval (no tags) — the
    // tag schema disappears, so the tag_filters payload must too.
    await user.click(
      screen.getByRole("checkbox", {
        name: /Select benchmark aime-22/i,
      }),
    );
    await pickBenchmark("humaneval");
    await user.click(screen.getByRole("button", { name: SUBMIT_BTN }));
    await vi.waitFor(() => expect(batchCall(spy)).not.toBeNull());
    expect(batchCall(spy)!.body.task_filter).toEqual({
      subset_kind: "all",
      benchmark_ids: ["humaneval"],
    });
  });
});
