import { screen, waitFor } from "@testing-library/react";
import { beforeEach, describe, expect, it, vi } from "vitest";

import Monitor from "../../pages/Monitor";
import { renderWithProviders } from "../../test-utils/renderWithProviders";

const monitorSummaryPayload = {
  scope: {
    view: "trials",
    team_id: "team-a",
    benchmark_id: "mbpp",
    agent: "litellm",
    model: "qwen",
    batch_id: null,
    state: "failed",
  },
  state_counts: {
    batches: { submitted: 1, running: 0, finished: 0, cancelled: 0 },
    trials: {
      queued: 1,
      claimed: 0,
      running: 2,
      succeeded: 4,
      failed: 1,
      cancelled: 0,
    },
  },
  queue: {
    queued: 1,
    claimed: 1,
    running: 2,
    waiting: 2,
    active_workers: 2,
    available_backends: ["docker", "fake"],
    has_default_backend: true,
    status: "waiting",
  },
  resources: {
    aggregate: {
      desired_slots: 18,
      pending_slots: 6,
      current_active_slots: 12,
      max_slots: 162,
      ceiling_slots: 162,
      active_workers: 2,
      draining_workers: 1,
      total_slots: 12,
      draining_slots: 2,
      occupied_slots: 3,
      free_slots: 9,
      running_tasks: 2,
      starting_tasks: 1,
      queued_tasks: 1,
    },
    pools: [
      {
        pool_name: "gb10-arm64",
        backend: "docker",
        cpu_arch: "arm64",
        autoscaler_environment: "production",
        autoscaler_actuator: "slurm",
        autoscaler_enabled: true,
        autoscaler_idle_since_at: "2026-06-27T12:00:00+00:00",
        autoscaler_idle_seconds: 601,
        desired_slots: 150,
        pending_slots: 0,
        current_active_slots: 10,
        max_slots: 150,
        ceiling_slots: 150,
        active_workers: 1,
        draining_workers: 1,
        total_slots: 10,
        draining_slots: 2,
        occupied_slots: 1,
        free_slots: 9,
        running_tasks: 1,
        starting_tasks: 0,
        queued_tasks: 1,
        last_autoscaler_decision: "request_drain",
        last_autoscaler_reason: "idle_excess_capacity",
        decision_reason: "idle_excess_capacity",
        last_autoscaler_blocked_reason: null,
        blocked_reason: null,
        last_autoscaler_error: null,
      },
      {
        pool_name: "public-beta-x86",
        backend: "docker",
        cpu_arch: "x86_64",
        autoscaler_environment: "production",
        autoscaler_actuator: "slurm",
        autoscaler_enabled: true,
        autoscaler_idle_since_at: null,
        autoscaler_idle_seconds: null,
        desired_slots: 6,
        pending_slots: 6,
        current_active_slots: 2,
        max_slots: 12,
        ceiling_slots: 12,
        active_workers: 1,
        draining_workers: 0,
        total_slots: 2,
        draining_slots: 0,
        occupied_slots: 2,
        free_slots: 0,
        running_tasks: 1,
        starting_tasks: 1,
        queued_tasks: 1,
        last_autoscaler_decision: "scale_up",
        last_autoscaler_reason: "queued_deficit",
        decision_reason: "queued_deficit",
        last_autoscaler_blocked_reason: "pending_cap",
        blocked_reason: "pending_cap",
        last_autoscaler_error: null,
      },
    ],
  },
};

function mockMonitorEndpoints(): ReturnType<typeof vi.spyOn> {
  return vi
    .spyOn(globalThis, "fetch")
    .mockImplementation((input: RequestInfo | URL) => {
      const url = typeof input === "string" ? input : String(input);
      if (url.includes("/api/v1/monitor/summary")) {
        return Promise.resolve(
          new Response(JSON.stringify(monitorSummaryPayload), {
            status: 200,
            headers: { "Content-Type": "application/json" },
          }),
        );
      }
      if (url.includes("/api/v1/batches")) {
        return Promise.resolve(
          new Response(
            JSON.stringify({
              items: [
                {
                  id: "batch-1",
                  name: "human-readable-batch",
                  state: "submitted",
                  expected_trial_count: 164,
                  created_at: "2026-06-19T20:23:00Z",
                  created_by_token_prefix: "test:web",
                  team_id: "team-a",
                  team_name: "EAI",
                  owner_team: { id: "team-a", name: "EAI" },
                  submitted_by_user: {
                    id: "user-ada",
                    username: "Ada",
                    team_id: "team-dev",
                    team_name: "Dev",
                  },
                },
              ],
              next_cursor: null,
            }),
            { status: 200, headers: { "Content-Type": "application/json" } },
          ),
        );
      }
      if (url.includes("/api/v1/trials")) {
        return Promise.resolve(
          new Response(JSON.stringify({ items: [], next_cursor: null }), {
            status: 200,
            headers: { "Content-Type": "application/json" },
          }),
        );
      }
      return Promise.reject(new Error(`unexpected fetch ${url}`));
    });
}

function mockFilteredMonitorEndpoints(): ReturnType<typeof vi.spyOn> {
  return vi
    .spyOn(globalThis, "fetch")
    .mockImplementation((input: RequestInfo | URL) => {
      const url = typeof input === "string" ? input : String(input);
      if (url.includes("/api/v1/monitor/summary")) {
        return Promise.resolve(
          new Response(JSON.stringify(monitorSummaryPayload), {
            status: 200,
            headers: { "Content-Type": "application/json" },
          }),
        );
      }
      if (url.includes("/api/v1/auth/me")) {
        return Promise.resolve(
          new Response(
            JSON.stringify({
              user: {
                id: "admin",
                email: "admin@example.com",
                display_name: "Admin",
                is_platform_admin: true,
              },
              teams: [],
              current_team: null,
              role: "platform_admin",
              scopes: ["admin:platform"],
              is_platform_admin: true,
              csrf_token: "csrf",
            }),
            { status: 200, headers: { "Content-Type": "application/json" } },
          ),
        );
      }
      if (url.includes("/api/v1/admin/teams")) {
        return Promise.resolve(
          new Response(
            JSON.stringify({
              items: [{ id: "team-a", name: "EAI" }],
            }),
            { status: 200, headers: { "Content-Type": "application/json" } },
          ),
        );
      }
      if (url.includes("/api/v1/trials")) {
        return Promise.resolve(
          new Response(
            JSON.stringify({
              items: [
                {
                  id: "trial-1",
                  task_id: "mbpp/1",
                  state: "failed",
                  agent_name: "litellm",
                  aggregate_reward: null,
                  total_prompt_tokens: 1,
                  total_completion_tokens: 2,
                  llm_calls_count: 1,
                  submitted_at: "2026-06-19T20:23:00Z",
                  team_id: "team-a",
                  team_name: "EAI",
                  owner_team: { id: "team-a", name: "EAI" },
                  submitted_by_user: {
                    id: "user-ada",
                    username: "Ada",
                    team_id: "team-dev",
                    team_name: "Dev",
                  },
                  model: { provider: "openai-compatible", name: "qwen" },
                },
              ],
              next_cursor: null,
            }),
            { status: 200, headers: { "Content-Type": "application/json" } },
          ),
        );
      }
      return Promise.reject(new Error(`unexpected fetch ${url}`));
    });
}

function mockFailureMonitorEndpoints(): ReturnType<typeof vi.spyOn> {
  return vi
    .spyOn(globalThis, "fetch")
    .mockImplementation((input: RequestInfo | URL) => {
      const url = typeof input === "string" ? input : String(input);
      if (url.includes("/api/v1/monitor/summary")) {
        return Promise.resolve(
          new Response(JSON.stringify(monitorSummaryPayload), {
            status: 200,
            headers: { "Content-Type": "application/json" },
          }),
        );
      }
      if (url.includes("/api/v1/auth/me")) {
        return Promise.resolve(
          new Response(
            JSON.stringify({
              user: {
                id: "admin",
                email: "admin@example.com",
                display_name: "Admin",
                is_platform_admin: true,
              },
              teams: [],
              current_team: null,
              role: "platform_admin",
              scopes: ["admin:platform"],
              is_platform_admin: true,
              csrf_token: "csrf",
            }),
            { status: 200, headers: { "Content-Type": "application/json" } },
          ),
        );
      }
      if (url.includes("/api/v1/admin/teams")) {
        return Promise.resolve(
          new Response(JSON.stringify({ items: [] }), {
            status: 200,
            headers: { "Content-Type": "application/json" },
          }),
        );
      }
      if (url.includes("/api/v1/trials")) {
        return Promise.resolve(
          new Response(
            JSON.stringify({
              items: [
                {
                  id: "trial-provider",
                  task_id: "mbpp/1",
                  state: "failed",
                  agent_name: "litellm",
                  aggregate_reward: null,
                  total_prompt_tokens: 1,
                  total_completion_tokens: 2,
                  llm_calls_count: 1,
                  submitted_at: "2026-06-19T20:23:00Z",
                  failure_reason: "gateway_error",
                  failure_message: "provider returned HTTP 502",
                  team_id: "team-a",
                  team_name: "EAI",
                  owner_team: { id: "team-a", name: "EAI" },
                  submitted_by_user: {
                    id: "user-ada",
                    username: "Ada",
                    team_id: "team-dev",
                    team_name: "Dev",
                  },
                  model: { provider: "openai-compatible", name: "qwen" },
                },
                {
                  id: "trial-sandbox",
                  task_id: "mbpp/2",
                  state: "failed",
                  agent_name: "litellm",
                  aggregate_reward: null,
                  total_prompt_tokens: 0,
                  total_completion_tokens: 0,
                  llm_calls_count: 0,
                  llm_evidence_status: "no_calls_invalid",
                  no_call: true,
                  submitted_at: "2026-06-19T20:24:00Z",
                  failure_reason: "task_image_build_failed",
                  failure_message: "Docker build failed",
                  team_id: "team-a",
                  team_name: "EAI",
                  owner_team: { id: "team-a", name: "EAI" },
                  submitted_by_user: {
                    id: "user-ada",
                    username: "Ada",
                    team_id: "team-dev",
                    team_name: "Dev",
                  },
                  model: { provider: "openai-compatible", name: "qwen" },
                },
              ],
              next_cursor: null,
            }),
            { status: 200, headers: { "Content-Type": "application/json" } },
          ),
        );
      }
      return Promise.reject(new Error(`unexpected fetch ${url}`));
    });
}

describe("Monitor human-readable labels", () => {
  beforeEach(() => {
    window.localStorage.clear();
    window.localStorage.setItem("loom_token", "test-token");
    vi.restoreAllMocks();
  });

  it("explains batch filters and planned trial counts without raw API wording", async () => {
    const fetchMock = mockMonitorEndpoints();
    renderWithProviders(<Monitor />, {
      route: "/monitor?view=batches&q=human-readable",
    });

    expect(await screen.findByText("human-readable-batch")).toBeInTheDocument();
    expect(screen.getByText("Ada / Dev")).toBeInTheDocument();
    expect(screen.getByText("Monitor quick actions")).toBeInTheDocument();
    expect(screen.getByText("loom eval batch show batch-1")).toBeInTheDocument();
    expect(screen.getByText("Planned trials")).toBeInTheDocument();
    expect(screen.queryByText("Expected")).not.toBeInTheDocument();
    expect(
      screen.getByPlaceholderText("Search batches by name or ID..."),
    ).toBeInTheDocument();
    expect(screen.getByLabelText("search")).toHaveValue("human-readable");
    expect(screen.getByRole("option", { name: "All states" })).toBeInTheDocument();
    expect(
      screen.getByRole("option", { name: "Submitted - waiting for scheduling" }),
    ).toBeInTheDocument();

    await waitFor(() => {
      const summaryRequest = fetchMock.mock.calls.find(([input]) =>
        String(input).includes("/api/v1/monitor/summary"),
      );
      expect(summaryRequest).toBeTruthy();
      const url = new URL(String(summaryRequest![0]), "http://localhost");
      expect(url.searchParams.get("view")).toBe("batches");
      expect(url.searchParams.get("q")).toBe("human-readable");
    });
  });

  it("hydrates trial filters from URL and sends shareable filter params", async () => {
    const fetchMock = mockFilteredMonitorEndpoints();
    renderWithProviders(<Monitor />, {
      route:
        "/monitor?view=trials&state=failed&q=ada&team_id=team-a&benchmark_id=mbpp&agent_name=litellm&model_provider=openai-compatible&model_name=qwen&provider_connection_id=11111111-1111-4111-8111-111111111111&provider_model_id=qwen",
    });

    expect(await screen.findByText("Ada / Dev")).toBeInTheDocument();
    expect(screen.getByLabelText("search")).toHaveValue("ada");
    expect(screen.getByLabelText("filter by state")).toHaveValue("failed");
    expect(screen.getByLabelText("filter by team")).toHaveValue("team-a");
    expect(screen.getByLabelText("filter by benchmark")).toHaveValue("mbpp");
    expect(screen.getByLabelText("filter by agent name")).toHaveValue("litellm");
    expect(screen.getByLabelText("filter by model provider")).toHaveValue("openai-compatible");
    expect(screen.getByLabelText("filter by model name")).toHaveValue("qwen");
    expect(screen.getByLabelText("filter by provider connection")).toHaveValue(
      "11111111-1111-4111-8111-111111111111",
    );
    expect(screen.getByLabelText("filter by provider model")).toHaveValue("qwen");

    await waitFor(() => {
      const trialRequest = fetchMock.mock.calls.find(([input]) =>
        String(input).includes("/api/v1/trials"),
      );
      expect(trialRequest).toBeTruthy();
      const url = new URL(String(trialRequest![0]), "http://localhost");
      expect(url.searchParams.get("state")).toBe("failed");
      expect(url.searchParams.get("team_id")).toBe("team-a");
      expect(url.searchParams.get("benchmark_id")).toBe("mbpp");
      expect(url.searchParams.get("agent_name")).toBe("litellm");
      expect(url.searchParams.get("model_provider")).toBe("openai-compatible");
      expect(url.searchParams.get("model_name")).toBe("qwen");
      expect(url.searchParams.get("provider_connection_id")).toBe(
        "11111111-1111-4111-8111-111111111111",
      );
      expect(url.searchParams.get("provider_model_id")).toBe("qwen");
    });
  });

  it("shows scoped monitor health and worker capacity from the URL filters", async () => {
    const fetchMock = mockFilteredMonitorEndpoints();
    renderWithProviders(<Monitor />, {
      route:
        "/monitor?view=trials&state=failed&q=mbpp&team_id=team-a&benchmark_id=mbpp&agent_name=litellm&model_provider=openai-compatible&model_name=qwen&provider_connection_id=11111111-1111-4111-8111-111111111111&provider_model_id=qwen",
    });

    expect(await screen.findByText("Monitor health")).toBeInTheDocument();
    expect(screen.getByText("3 / 12")).toBeInTheDocument();
    expect(screen.getByText("Concurrent tasks")).toBeInTheDocument();
    expect(screen.getByText("2 active workers")).toBeInTheDocument();
    expect(screen.getByText("1 queued")).toBeInTheDocument();
    expect(screen.getByText("2 running")).toBeInTheDocument();
    expect(screen.getByText("1 starting")).toBeInTheDocument();
    expect(screen.getByText("docker, fake")).toBeInTheDocument();
    expect(
      screen.getByText("2 waiting for 9 free slots."),
    ).toBeInTheDocument();
    expect(screen.getByText("gb10-arm64")).toBeInTheDocument();
    expect(screen.getByText("public-beta-x86")).toBeInTheDocument();
    expect(screen.getByText("Active slots")).toBeInTheDocument();
    expect(screen.getByText("Max")).toBeInTheDocument();
    expect(screen.getAllByText("slurm").length).toBeGreaterThanOrEqual(2);
    expect(screen.getByText("1/10")).toBeInTheDocument();
    expect(screen.getByText("2/2")).toBeInTheDocument();
    expect(screen.getByText("601s")).toBeInTheDocument();
    expect(screen.getByText("request_drain")).toBeInTheDocument();
    expect(screen.getByText("idle_excess_capacity")).toBeInTheDocument();
    expect(screen.getByText("scale_up")).toBeInTheDocument();
    expect(screen.getByText("pending_cap")).toBeInTheDocument();

    await waitFor(() => {
      const summaryRequest = fetchMock.mock.calls.find(([input]) =>
        String(input).includes("/api/v1/monitor/summary"),
      );
      expect(summaryRequest).toBeTruthy();
      const url = new URL(String(summaryRequest![0]), "http://localhost");
      expect(url.searchParams.get("view")).toBe("trials");
      expect(url.searchParams.get("state")).toBe("failed");
      expect(url.searchParams.get("team_id")).toBe("team-a");
      expect(url.searchParams.get("benchmark_id")).toBe("mbpp");
      expect(url.searchParams.get("agent_name")).toBe("litellm");
      expect(url.searchParams.get("model_provider")).toBe("openai-compatible");
      expect(url.searchParams.get("model_name")).toBe("qwen");
      expect(url.searchParams.get("provider_connection_id")).toBe(
        "11111111-1111-4111-8111-111111111111",
      );
      expect(url.searchParams.get("provider_model_id")).toBe("qwen");
    });
  });

  it("groups failed trials by diagnostic reason with next-step links", async () => {
    mockFailureMonitorEndpoints();
    renderWithProviders(<Monitor />, {
      route: "/monitor?view=trials&state=failed",
    });

    expect(await screen.findByText("Failure diagnostics")).toBeInTheDocument();
    expect(screen.getByText(/gateway_error/i)).toBeInTheDocument();
    expect(screen.getByText(/provider returned HTTP 502/i)).toBeInTheDocument();
    expect(screen.getByText(/task_image_build_failed/i)).toBeInTheDocument();
    expect(screen.getByText(/Docker build failed/i)).toBeInTheDocument();
    expect(
      screen.getByRole("link", { name: /Open trial-provider/i }),
    ).toHaveAttribute("href", "/trials/trial-provider");
    expect(screen.getByText("no LLM calls")).toBeInTheDocument();
    expect(screen.getByText(/invalid evidence/i)).toBeInTheDocument();
  });
});
