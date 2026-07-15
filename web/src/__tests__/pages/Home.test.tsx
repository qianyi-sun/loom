import { screen } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";

import App from "../../App";
import { renderWithProviders } from "../../test-utils/renderWithProviders";

function jsonResponse(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "Content-Type": "application/json" },
  });
}

const ownerMe = {
  user: {
    id: "owner-user",
    username: "Owner",
    email: "owner@example.com",
    display_name: "Owner Example",
    is_platform_admin: false,
  },
  teams: [{ id: "team-eai", name: "EAI", role: "owner" }],
  current_team: { id: "team-eai", name: "EAI", role: "owner" },
  role: "owner",
  scopes: ["read:own", "submit", "providers:manage", "team:manage"],
  is_platform_admin: false,
  csrf_token: "csrf-owner",
};

function overview(overrides: Record<string, unknown> = {}) {
  return {
    status: "ready",
    summary: "This team can launch model-backed evaluations.",
    team_context: {
      team_id: "team-eai",
      team_name: "EAI",
      role: "owner",
      scopes: ["read:own", "submit", "providers:manage", "team:manage"],
      is_platform_admin: false,
      submissions_paused: false,
    },
    capabilities: {
      can_read: true,
      can_submit: true,
      can_manage_providers: true,
      can_manage_team: true,
    },
    provider_health: {
      total: 2,
      ready: 1,
      needs_attention: 1,
      untested: 0,
      latest: [
        {
          id: "provider-1",
          name: "needs-fix",
          type: "openai-compatible",
          status: "invalid",
          last_validated_at: "2026-06-24T15:00:00Z",
          last_validation_error: "timeout after 5s",
        },
      ],
    },
    benchmark_readiness: {
      total: 2,
      runnable: 1,
      needs_attention: 1,
      blocked: [
        {
          id: "gaia",
          display_name: "GAIA",
          readiness_state: "blocked",
          readiness_label: "Needs publish",
          blocker_reason: "manifest_missing",
          task_count: 0,
        },
      ],
    },
    worker_health: {
      active: 1,
      available_backends: ["docker", "fake"],
      has_default_backend: true,
    },
    run_activity: {
      batches: { submitted: 1, running: 1, finished: 0, cancelled: 0 },
      trials: {
        queued: 1,
        claimed: 0,
        running: 1,
        succeeded: 1,
        failed: 0,
        cancelled: 0,
      },
      latest_batch: {
        id: "batch-1",
        name: "latest running",
        state: "running",
        result_status: null,
        expected_trial_count: 2,
        created_at: "2026-06-24T15:00:00Z",
      },
    },
    next_actions: [
      {
        id: "create_batch",
        label: "Create a batch",
        to: "/batches/new",
        kind: "user",
        priority: 10,
      },
      {
        id: "repair_provider",
        label: "Repair provider connection",
        to: "/providers",
        kind: "user",
        priority: 30,
      },
    ],
    ...overrides,
  };
}

function mockHomeFetch(payload = overview()) {
  vi.spyOn(global, "fetch").mockImplementation(
    async (input: RequestInfo | URL) => {
      const url = String(input);
      if (url.includes("/api/v1/auth/me")) {
        return jsonResponse(ownerMe);
      }
      if (url.includes("/api/v1/overview")) {
        return jsonResponse(payload);
      }
      return jsonResponse({ detail: `unhandled ${url}` }, 404);
    },
  );
}

describe("Home overview", () => {
  afterEach(() => {
    vi.restoreAllMocks();
  });

  it("renders as the authenticated root route with user next actions", async () => {
    mockHomeFetch();

    renderWithProviders(<App />, { route: "/" });

    expect(
      await screen.findByRole("heading", { name: "Team overview" }),
    ).toBeInTheDocument();
    expect(screen.getAllByText("EAI").length).toBeGreaterThan(0);
    expect(screen.getByText("Ready")).toBeInTheDocument();
    expect(screen.getByText("1 ready")).toBeInTheDocument();
    expect(screen.getByText("1 runnable")).toBeInTheDocument();
    expect(screen.getByText("1 active")).toBeInTheDocument();
    expect(
      screen.getByRole("link", { name: "Create a batch" }),
    ).toHaveAttribute("href", "/batches/new");
    expect(
      screen.getByRole("link", { name: "Repair provider connection" }),
    ).toHaveAttribute("href", "/providers");
  });

  it("separates operator prerequisites from user actions", async () => {
    mockHomeFetch(overview({
      status: "needs_setup",
      summary: "Finish the setup items below before launching evaluations.",
      benchmark_readiness: {
        total: 2,
        runnable: 0,
        needs_attention: 2,
        blocked: [],
      },
      worker_health: {
        active: 0,
        available_backends: [],
        has_default_backend: false,
      },
      next_actions: [
        {
          id: "publish_benchmarks",
          label: "Publish benchmark tasks",
          to: "/benchmarks",
          kind: "operator",
          priority: 40,
        },
        {
          id: "start_worker",
          label: "Start at least one worker",
          to: "/monitor",
          kind: "operator",
          priority: 50,
        },
      ],
    }));

    renderWithProviders(<App />, { route: "/" });

    expect(
      await screen.findByRole("heading", { name: "Team overview" }),
    ).toBeInTheDocument();
    expect(screen.getByText("Needs setup")).toBeInTheDocument();
    expect(screen.getByText("Operator actions")).toBeInTheDocument();
    expect(screen.getByText("Publish benchmark tasks")).toBeInTheDocument();
    expect(screen.getByText("Start at least one worker")).toBeInTheDocument();
    expect(screen.queryByText("User actions")).not.toBeInTheDocument();
  });
});
