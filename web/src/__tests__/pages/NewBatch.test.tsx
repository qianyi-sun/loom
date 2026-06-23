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
      name: "litellm",
      needs_model: true,
      kind: "builtin",
      description: "Multi-provider tool-loop agent.",
      supported_providers: ["*"],
      supported_model_sources: ["api", "local-server", "hf"],
    },
    {
      name: "claude-code",
      needs_model: true,
      kind: "builtin",
      description: "Claude Code in-box runtime.",
      supported_providers: ["anthropic"],
      supported_model_sources: ["api"],
    },
    {
      name: "opencode",
      needs_model: true,
      kind: "adapter",
      description: "Opencode CLI adapter.",
      supported_providers: ["*"],
      supported_model_sources: ["api", "local-server", "hf"],
      service_mode_ready: false,
      readiness_status: "unavailable",
      readiness_message: "agent opencode requires executable opencode",
      runtime_contract: {
        execution: "subprocess-adapter",
        capture: "stdout_jsonl",
        required_executables: ["opencode"],
        required_python_modules: [],
        required_packages: ["opencode-ai"],
        endpoint_dialect: "openai_chat",
        api_key_env: "OPENAI_API_KEY",
        base_url_env: "OPENAI_BASE_URL",
        model_name_template: "openai/{model_id}",
        sandbox_network: "gateway",
        install_hint: "Provision executable opencode before enabling agent opencode.",
      },
    },
  ],
};

const MODELS_RESPONSE = {
  items: [
    {
      provider: "anthropic",
      name: "claude-opus-4-7",
      source: "rate-card",
      agent_capable: true,
      recommended: true,
      visibility: "default",
      hidden_reason: null,
    },
    {
      provider: "openai",
      name: "deepseek-chat",
      provider_connection_id: "11111111-1111-4111-8111-111111111111",
      provider_connection_name: "Lab vLLM",
      provider_connection_type: "openai-compatible",
      source: "discovered",
      agent_capable: true,
      recommended: true,
      visibility: "default",
      hidden_reason: null,
      last_seen_at: "2026-06-16T00:00:00Z",
    },
  ],
};

const RAW_MODELS_RESPONSE = {
  items: [
    ...MODELS_RESPONSE.items,
    {
      provider: "openai",
      name: "amap-coordinate-convert",
      provider_connection_id: "11111111-1111-4111-8111-111111111111",
      provider_connection_name: "Lab vLLM",
      provider_connection_type: "openai-compatible",
      source: "discovered",
      agent_capable: false,
      recommended: false,
      visibility: "advanced",
      hidden_reason: "classifier-non-llm",
      last_seen_at: "2026-06-16T00:00:00Z",
    },
  ],
};

const PROVIDER_CONNECTIONS_RESPONSE = {
  items: [
    {
      id: "11111111-1111-4111-8111-111111111111",
      team_id: "22222222-2222-4222-8222-222222222222",
      name: "Lab vLLM",
      type: "openai-compatible",
      base_url: "https://vllm.lab.example/v1",
      upstream_host: "vllm.lab.example",
      resolved_egress_ips: ["203.0.113.10"],
      allowed_models: null,
      status: "valid",
      last_validated_at: "2026-06-16T00:00:00Z",
      last_validation_error: null,
      pricing_source: "tokens-only",
      pricing_data: null,
      rate_card_provider: "openai",
      created_by: "test:web",
      created_at: "2026-06-16T00:00:00Z",
      updated_at: "2026-06-16T00:00:00Z",
    },
  ],
};

const LOCAL_SERVERS_RESPONSE = { items: [] };

const BENCHMARKS_RESPONSE = {
  items: [
    {
      id: "humaneval",
      display_name: "HumanEval",
      task_count: 12,
      raw_task_count: 12,
      valid_task_config_count: 12,
      invalid_task_config_count: 0,
      readiness_state: "runnable",
      readiness_label: "Ready",
      readiness_message: "12 runnable tasks are registered.",
      selectable: true,
      series: null,
    },
    {
      id: "mbpp",
      display_name: "MBPP",
      task_count: 3,
      raw_task_count: 3,
      valid_task_config_count: 3,
      invalid_task_config_count: 0,
      readiness_state: "runnable",
      readiness_label: "Ready",
      readiness_message: "3 runnable tasks are registered.",
      selectable: true,
      series: null,
    },
    {
      id: "aime-22",
      display_name: "AIME (AIMO validation 2022–2024)",
      task_count: 90,
      raw_task_count: 90,
      valid_task_config_count: 90,
      invalid_task_config_count: 0,
      readiness_state: "runnable",
      readiness_label: "Ready",
      readiness_message: "90 runnable tasks are registered.",
      selectable: true,
      series: "aime",
    },
    {
      id: "aime-25",
      display_name: "AIME 2025",
      task_count: 30,
      raw_task_count: 30,
      valid_task_config_count: 30,
      invalid_task_config_count: 0,
      readiness_state: "runnable",
      readiness_label: "Ready",
      readiness_message: "30 runnable tasks are registered.",
      selectable: true,
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
  noBenchmarks?: boolean;
  emptyBenchmark?: boolean;
  legacyBenchmark?: boolean;
  licenseBlockedBenchmark?: boolean;
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
    items: opts.noBenchmarks
      ? []
      : BENCHMARKS_RESPONSE.items.map((b) =>
          b.id === "humaneval"
            ? {
                ...b,
                task_count: matching,
                raw_task_count: matching,
                valid_task_config_count: matching,
                readiness_message: `${matching} runnable tasks are registered.`,
              }
            : opts.legacyBenchmark && b.id === "mbpp"
              ? {
                  ...b,
                  task_count: 0,
                  raw_task_count: 3,
                  valid_task_config_count: 0,
                  invalid_task_config_count: 3,
                  readiness_state: "blocked",
                  readiness_label: "Needs republish",
                  readiness_message:
                    "3 task rows exist, but none have a valid TaskConfig. Re-publish/register this benchmark before selecting it.",
                  selectable: false,
                  blocker_reason: "manifest_legacy_missing_task_config",
                }
            : opts.licenseBlockedBenchmark && b.id === "mbpp"
              ? {
                  ...b,
                  task_count: 0,
                  raw_task_count: 3,
                  valid_task_config_count: 3,
                  invalid_task_config_count: 0,
                  license_allowed_task_count: 0,
                  license_blocked_task_count: 3,
                  blocked_licenses: ["CC-BY-NC-4.0"],
                  readiness_state: "blocked",
                  readiness_label: "License blocked",
                  readiness_message:
                    "3 runnable tasks blocked by team license policy: CC-BY-NC-4.0.",
                  selectable: false,
                  blocker_reason: "license_not_allowed",
                }
            : opts.emptyBenchmark && b.id === "mbpp"
              ? {
                  ...b,
                  task_count: 0,
                  raw_task_count: 0,
                  valid_task_config_count: 0,
                  invalid_task_config_count: 0,
                  readiness_state: "blocked",
                  readiness_label: "Needs publish",
                  readiness_message: "Publish/register tasks before selecting this benchmark.",
                  selectable: false,
                  blocker_reason: "manifest_missing",
                }
              : b,
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
      if (url.includes("/api/v1/provider-connections/") && url.endsWith("/models")) {
        return json({ model_id: "manual-vllm-checkpoint" }, 201);
      }
      if (url.includes("/api/v1/provider-connections")) {
        return json(PROVIDER_CONNECTIONS_RESPONSE);
      }
      if (url.includes("/api/v1/models?view=raw")) return json(RAW_MODELS_RESPONSE);
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

function manualModelCall(
  spy: ReturnType<typeof vi.spyOn>,
): { url: string; body: Record<string, unknown> } | null {
  const found = spy.mock.calls.find((c) =>
    String(c[0]).includes("/api/v1/provider-connections/") &&
    String(c[0]).endsWith("/models") &&
    (c[1] as RequestInit | undefined)?.method === "POST",
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

  it("labels launch controls with human-readable sections", async () => {
    mockEndpoints({ matchingTasks: 12 });
    renderWithProviders(<NewBatch />);
    await screen.findByText(/Runs solution\/solve.sh/i);

    expect(screen.getByText("Task selection")).toBeInTheDocument();
    expect(screen.getByText("Agent/model combinations")).toBeInTheDocument();
    expect(screen.getByText("Advanced trial settings")).toBeInTheDocument();
    expect(screen.getByText("CLI/API equivalent")).toBeInTheDocument();
    expect(screen.getByText(/loom eval batch create/)).toHaveTextContent(
      "--agent oracle",
    );
    expect(
      screen.getByText(
        /Shared settings applied to every trial unless a combination overrides them/i,
      ),
    ).toBeInTheDocument();
    expect(screen.queryByText("Which tasks")).not.toBeInTheDocument();
    expect(screen.queryByText("Combinations")).not.toBeInTheDocument();
    expect(screen.queryByText("Advanced options")).not.toBeInTheDocument();
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

  it("marks agents without service runtime as setup-needed", async () => {
    mockEndpoints({ matchingTasks: 12 });
    renderWithProviders(<NewBatch />);

    const unavailable = await screen.findByRole("option", {
      name: /opencode .*setup needed/i,
    });

    expect(unavailable).toBeDisabled();
    expect(unavailable).toHaveAttribute(
      "title",
      expect.stringContaining("executable opencode"),
    );
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

  it("explains why an empty benchmark is marked needs publish", async () => {
    mockEndpoints({ emptyBenchmark: true });
    renderWithProviders(<NewBatch />);
    await screen.findByText(/Runs solution\/solve.sh/i);

    const marker = await screen.findByText(/Needs publish/i);
    expect(marker.closest("label")).toHaveAttribute(
      "title",
      "Publish/register tasks before selecting this benchmark.",
    );
  });

  it("uses API readiness states to distinguish legacy rows from unpublished benchmarks", async () => {
    mockEndpoints({ legacyBenchmark: true });
    renderWithProviders(<NewBatch />);
    await screen.findByText(/Runs solution\/solve.sh/i);

    const legacy = await screen.findByRole("checkbox", {
      name: /Select benchmark mbpp/i,
    });
    expect(legacy).toBeDisabled();
    expect(legacy.closest("label")).toHaveTextContent(/Needs republish/i);
    expect(legacy.closest("label")).toHaveTextContent(/0\/3 runnable/i);
    expect(legacy.closest("label")).toHaveAttribute(
      "title",
      "3 task rows exist, but none have a valid TaskConfig. Re-publish/register this benchmark before selecting it.",
    );
  });

  it("visibly explains team-license blocked benchmarks", async () => {
    mockEndpoints({ licenseBlockedBenchmark: true });
    renderWithProviders(<NewBatch />);
    await screen.findByText(/Runs solution\/solve.sh/i);

    const blocked = await screen.findByRole("checkbox", {
      name: /Select benchmark mbpp/i,
    });
    expect(blocked).toBeDisabled();
    const row = blocked.closest("label");
    expect(row).toHaveTextContent(/License blocked/i);
    expect(row).toHaveTextContent(/0\/3 allowed/i);
    expect(row).toHaveTextContent(/CC-BY-NC-4.0/i);
  });

  it("shows deployment-facing guidance instead of an operator import command when no benchmarks are provisioned", async () => {
    mockEndpoints({ noBenchmarks: true });
    renderWithProviders(<NewBatch />);
    await screen.findByText(/Runs solution\/solve.sh/i);

    expect(
      await screen.findByText(/No runnable benchmarks are provisioned/i),
    ).toBeInTheDocument();
    expect(
      screen.getByText(/public beta catalog provisioning/i),
    ).toBeInTheDocument();
    expect(screen.queryByText(/loom_benchmark_tool import/i)).not.toBeInTheDocument();
    expect(
      screen.queryByText(/python -m loom_benchmark_tool/i),
    ).not.toBeInTheDocument();
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
    await user.click(screen.getByText(/Advanced trial settings/i));
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
    await user.click(screen.getByText(/Advanced trial settings/i));
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

  it("submits a discovered BYO provider connection model", async () => {
    const spy = mockEndpoints({ matchingTasks: 12 });
    const user = userEvent.setup();
    renderWithProviders(<NewBatch />);
    await screen.findByText(/Runs solution\/solve.sh/i);
    await user.type(screen.getByPlaceholderText(/run 7/i), "byo-batch");
    await pickBackend();
    await pickBenchmark();
    await user.selectOptions(screen.getByLabelText(/^Agent$/i), "litellm");
    await user.selectOptions(
      await screen.findByLabelText(/^Provider connection$/i),
      "11111111-1111-4111-8111-111111111111",
    );
    await user.selectOptions(
      await screen.findByLabelText(/^Model$/i),
      "openai|deepseek-chat|11111111-1111-4111-8111-111111111111",
    );
    expect(screen.queryByRole("option", {
      name: /amap-coordinate-convert/i,
    })).not.toBeInTheDocument();

    await user.click(screen.getByRole("button", { name: SUBMIT_BTN }));
    await vi.waitFor(() => expect(batchCall(spy)).not.toBeNull());
    const body = batchCall(spy)!.body;
    expect(body.provider_connection_id).toBe(
      "11111111-1111-4111-8111-111111111111",
    );
    expect(body.provider_model_id).toBe("deepseek-chat");
    expect(body.combinations).toEqual([
      {
        agent_name: "litellm",
        agent_model: {
          provider: "openai",
          name: "deepseek-chat",
          source: "api",
        },
        n_per_task: 1,
      },
    ]);
  });

  it("supports raw mode and manual BYO model ids", async () => {
    const spy = mockEndpoints({ matchingTasks: 12 });
    const user = userEvent.setup();
    renderWithProviders(<NewBatch />);
    await screen.findByText(/Runs solution\/solve.sh/i);
    await user.type(screen.getByPlaceholderText(/run 7/i), "manual-batch");
    await pickBackend();
    await pickBenchmark();
    await user.selectOptions(screen.getByLabelText(/^Agent$/i), "litellm");
    await user.selectOptions(
      await screen.findByLabelText(/^Provider connection$/i),
      "11111111-1111-4111-8111-111111111111",
    );

    await user.click(
      screen.getByRole("checkbox", {
        name: /Include hidden\/discovered models/i,
      }),
    );
    expect(
      await screen.findByRole("option", {
        name: /amap-coordinate-convert.*classifier-non-llm/i,
      }),
    ).toBeInTheDocument();

    await user.selectOptions(
      screen.getByLabelText(/^Model$/i),
      screen.getByRole("option", { name: /Ad-hoc model ID/i }),
    );
    await user.type(
      await screen.findByPlaceholderText("manual-vllm-checkpoint"),
      "manual-vllm-checkpoint",
    );
    await user.click(screen.getByRole("button", { name: SUBMIT_BTN }));
    await vi.waitFor(() => expect(batchCall(spy)).not.toBeNull());
    expect(manualModelCall(spy)?.body).toEqual({
      model_id: "manual-vllm-checkpoint",
    });
    const body = batchCall(spy)!.body;
    expect(body.provider_connection_id).toBe(
      "11111111-1111-4111-8111-111111111111",
    );
    expect(body.provider_model_id).toBe("manual-vllm-checkpoint");
  });
});
