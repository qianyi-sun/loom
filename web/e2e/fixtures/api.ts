import type { Page, Route } from "@playwright/test";

export type BrowserRole = "logged-out" | "user" | "admin";

const team = { id: "team-eai", name: "EAI", role: "owner" };

function auth(role: Exclude<BrowserRole, "logged-out">) {
  const admin = role === "admin";
  return {
    user: {
      id: admin ? "user-admin" : "user-normal",
      username: admin ? "Admin" : "User",
      email: admin ? "admin@example.test" : "user@example.test",
      display_name: admin ? "Admin User" : "Normal User",
      is_platform_admin: admin,
    },
    teams: [team],
    current_team: team,
    role: admin ? "platform_admin" : "owner",
    scopes: ["read:own", "submit", "providers:manage", "tokens:manage", "team:manage"],
    is_platform_admin: admin,
    csrf_token: "local-browser-fixture",
  };
}

const overview = {
  status: "ready",
  summary: "This team can launch model-backed evaluations.",
  team_context: {
    team_id: team.id,
    team_name: team.name,
    role: team.role,
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
  provider_health: { total: 0, ready: 0, needs_attention: 0, untested: 0, latest: [] },
  benchmark_readiness: { total: 0, runnable: 0, needs_attention: 0, blocked: [] },
  worker_health: { active: 1, available_backends: ["docker"], has_default_backend: true },
  run_activity: {
    batches: { submitted: 0, running: 0, finished: 0, cancelled: 0 },
    trials: { queued: 0, claimed: 0, running: 0, succeeded: 0, failed: 0, cancelled: 0 },
    latest_batch: null,
  },
  next_actions: [],
};

const taskSet = {
  task_set_id: "task-set-1",
  display_name: "Browser Task Set",
  status: "ready",
  status_reason: null,
  intents: ["evaluation", "trajectory_generation"],
  manifest_intents: ["evaluation"],
  inferred_intents: ["trajectory_generation"],
  capabilities: ["evaluation", "trajectory_generation"],
  warnings: [],
  evaluation_ready: true,
  task_count: 2,
  error_summary: [],
  materialization_job_state: null,
  created_at: "2026-07-16T00:00:00Z",
};

function json(route: Route, body: unknown, status = 200): Promise<void> {
  return route.fulfill({ status, contentType: "application/json", body: JSON.stringify(body) });
}

function monitorSummary() {
  return {
    scope: { view: "trials", team_id: team.id, benchmark_id: null, agent: null, model: null, batch_id: null, state: null },
    state_counts: {
      batches: { submitted: 0, running: 0, finished: 0, cancelled: 0 },
      trials: { queued: 0, claimed: 0, running: 0, succeeded: 0, failed: 0, cancelled: 0 },
    },
    queue: {
      queued: 0,
      claimed: 0,
      running: 0,
      waiting: 0,
      active_workers: 1,
      available_backends: ["docker"],
      has_default_backend: true,
      status: "idle",
    },
    resources: {
      aggregate: {
        desired_slots: 1,
        pending_slots: 0,
        current_active_slots: 1,
        max_slots: 1,
        ceiling_slots: 1,
        active_workers: 1,
        draining_workers: 0,
        total_slots: 1,
        draining_slots: 0,
        occupied_slots: 0,
        free_slots: 1,
        running_tasks: 0,
        starting_tasks: 0,
        queued_tasks: 0,
      },
      pools: [],
    },
    failure_counts: [],
    totals: { batches: 0, trials: 0 },
  };
}

export async function installApiFixture(page: Page, role: BrowserRole): Promise<void> {
  await page.route("**/dev/api/**", async (route) => {
    const url = new URL(route.request().url());
    const path = url.pathname.replace(/^\/dev\/api/u, "");
    if (path === "/v1/auth/me") {
      if (role === "logged-out") return json(route, { detail: "unauthorized" }, 401);
      return json(route, auth(role));
    }
    if (path === "/v1/overview") return json(route, overview);
    if (path === "/v1/monitor/summary") return json(route, monitorSummary());
    if (path === "/v1/tasksets") return json(route, { items: [taskSet] });
    if (path === "/v1/tasksets/task-set-1") return json(route, taskSet);
    if (path === "/v1/auth/public-teams") return json(route, { items: [team] });
    if (path.startsWith("/v1/auth/setup/lookup")) return json(route, { detail: "expired setup token" }, 404);
    if (path.startsWith("/v1/auth/reset/lookup")) return json(route, { detail: "expired reset token" }, 404);
    if (path.startsWith("/v1/invites/")) return json(route, { detail: "expired invite" }, 404);
    if (path === `/v1/teams/${team.id}`) {
      return json(route, { ...team, created_at: "2026-07-16T00:00:00Z", quota: null, members: [], user_members: [], disabled_at: null });
    }
    if (path === "/v1/admin/teams") return json(route, { items: [] });
    if (path === "/v1/admin/team-registrations") return json(route, { items: [] });
    if (path === "/v1/admin/registration-requests") return json(route, { items: [], next_cursor: null });
    if (path === "/v1/admin/password-reset-requests") return json(route, { items: [], next_cursor: null });
    if (path === "/v1/admin/audit") return json(route, { items: [], next_cursor: null });
    if (path === "/v1/rate-cards") return json(route, { items: [] });
    if (path === "/v1/tokens") return json(route, { items: [] });
    if (path === "/v1/invites") return json(route, { items: [] });
    if (path === "/v1/usage") return json(route, { degraded: false, buckets: [] });
    if (path === "/v1/backends") return json(route, { items: [{ name: "docker", description: "Docker", available: true }] });
    if (path === "/v1/agents") return json(route, { items: [] });
    if (path.startsWith("/v1/benchmarks")) return json(route, { items: [], next_cursor: null });
    if (path.startsWith("/v1/models")) return json(route, { items: [] });
    if (path.startsWith("/v1/provider-connections")) return json(route, { items: [] });
    if (path === "/v1/local-servers") return json(route, { items: [] });
    if (path === "/v1/tasks/count") return json(route, { count: 0 });
    if (path === "/v1/tasks") return json(route, { items: [], next_cursor: null });
    if (path.startsWith("/v1/run-library")) return json(route, { items: [], next_cursor: null });
    if (path === "/v1/batches") return json(route, { items: [], next_cursor: null });
    if (path === "/v1/trials") return json(route, { items: [], next_cursor: null });
    return json(route, { detail: `local fixture has no response for ${path}` }, 404);
  });
}
