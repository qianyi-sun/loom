import { screen, waitFor } from "@testing-library/react";
import { beforeEach, describe, expect, it, vi } from "vitest";

import Monitor from "../../pages/Monitor";
import { renderWithProviders } from "../../test-utils/renderWithProviders";

function mockMonitorEndpoints(): ReturnType<typeof vi.spyOn> {
  return vi
    .spyOn(globalThis, "fetch")
    .mockImplementation((input: RequestInfo | URL) => {
      const url = typeof input === "string" ? input : String(input);
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
                  submitted_at: "2026-06-19T20:24:00Z",
                  failure_reason: "task_image_build_failed",
                  failure_message: "Docker build failed",
                  team_id: "team-a",
                  team_name: "EAI",
                  owner_team: { id: "team-a", name: "EAI" },
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
    mockMonitorEndpoints();
    renderWithProviders(<Monitor />, { route: "/monitor?view=batches" });

    expect(await screen.findByText("human-readable-batch")).toBeInTheDocument();
    expect(screen.getByText("Monitor quick actions")).toBeInTheDocument();
    expect(screen.getByText("loom eval batch show batch-1")).toBeInTheDocument();
    expect(screen.getByText("Planned trials")).toBeInTheDocument();
    expect(screen.queryByText("Expected")).not.toBeInTheDocument();
    expect(
      screen.getByPlaceholderText("Search batches by name or ID..."),
    ).toBeInTheDocument();
    expect(screen.getByRole("option", { name: "All states" })).toBeInTheDocument();
    expect(
      screen.getByRole("option", { name: "Submitted - waiting for scheduling" }),
    ).toBeInTheDocument();
  });

  it("hydrates trial filters from URL and sends shareable filter params", async () => {
    const fetchMock = mockFilteredMonitorEndpoints();
    renderWithProviders(<Monitor />, {
      route:
        "/monitor?view=trials&state=failed&q=mbpp&team_id=team-a&benchmark_id=mbpp&agent=litellm&model=qwen",
    });

    expect(await screen.findByText("EAI")).toBeInTheDocument();
    expect(screen.getByLabelText("search")).toHaveValue("mbpp");
    expect(screen.getByLabelText("filter by state")).toHaveValue("failed");
    expect(screen.getByLabelText("filter by team")).toHaveValue("team-a");
    expect(screen.getByLabelText("filter by benchmark")).toHaveValue("mbpp");
    expect(screen.getByLabelText("filter by agent")).toHaveValue("litellm");
    expect(screen.getByLabelText("filter by model")).toHaveValue("qwen");

    await waitFor(() => {
      const trialRequest = fetchMock.mock.calls.find(([input]) =>
        String(input).includes("/api/v1/trials"),
      );
      expect(trialRequest).toBeTruthy();
      const url = new URL(String(trialRequest![0]), "http://localhost");
      expect(url.searchParams.get("state")).toBe("failed");
      expect(url.searchParams.get("team_id")).toBe("team-a");
      expect(url.searchParams.get("benchmark_id")).toBe("mbpp");
      expect(url.searchParams.get("agent")).toBe("litellm");
      expect(url.searchParams.get("model")).toBe("qwen");
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
  });
});
