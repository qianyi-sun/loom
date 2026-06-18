/**
 * `apiFetch` is the single point of entry for all `loom_service`
 * calls. It pulls the bearer token from `localStorage`, attaches
 * `Content-Type: application/json` + `Authorization`, and surfaces
 * non-2xx responses as a typed `ApiError`. A 401 from the service
 * fires the registered `setUnauthorizedHandler` callback (used by
 * `AuthContext` to clear the stored token and redirect to /settings).
 */

import type { paths } from "./schema";

export type ApiError = { status: number; detail: string };

let _onUnauthorized: () => void = () => {};
export function setUnauthorizedHandler(cb: () => void): void {
  _onUnauthorized = cb;
}

function getToken(): string | null {
  try {
    return window.localStorage.getItem("loom_token");
  } catch {
    return null;
  }
}

export async function apiFetch<T>(
  path: string,
  init: RequestInit = {},
): Promise<T> {
  const token = getToken();
  const headers: Record<string, string> = {
    "Content-Type": "application/json",
    ...(init.headers as Record<string, string> | undefined),
  };
  if (token) headers.Authorization = `Bearer ${token}`;

  const env = (
    import.meta as unknown as { env: { VITE_API_BASE?: string } }
  ).env;
  const base = env.VITE_API_BASE ?? "";
  const resp = await fetch(`${base}${path}`, { ...init, headers });

  if (resp.status === 401) {
    _onUnauthorized();
    throw { status: 401, detail: "unauthorized" } satisfies ApiError;
  }
  if (!resp.ok) {
    let detail = await resp.text();
    try {
      const parsed: unknown = JSON.parse(detail);
      if (
        typeof parsed === "object" &&
        parsed !== null &&
        "detail" in parsed
      ) {
        const d = (parsed as { detail: unknown }).detail;
        if (typeof d === "string") detail = d;
        else detail = JSON.stringify(d);
      }
    } catch {
      /* keep raw text */
    }
    throw { status: resp.status, detail } satisfies ApiError;
  }
  if (resp.status === 204) return undefined as T;
  return (await resp.json()) as T;
}

type TrialList = paths["/api/v1/trials"]["get"]["responses"][200]["content"]["application/json"];
type TrialDetail =
  paths["/api/v1/trials/{trial_id}"]["get"]["responses"][200]["content"]["application/json"];
type TrajectoryPage =
  paths["/api/v1/trials/{trial_id}/trajectory"]["get"]["responses"][200]["content"]["application/json"];
type TaskList = paths["/api/v1/tasks"]["get"]["responses"][200]["content"]["application/json"];
type BenchmarkList =
  paths["/api/v1/benchmarks"]["get"]["responses"][200]["content"]["application/json"];
type BatchList =
  paths["/api/v1/batches"]["get"]["responses"][200]["content"]["application/json"];
type BatchDetail =
  paths["/api/v1/batches/{id}"]["get"]["responses"][200]["content"]["application/json"];
type BatchCreate =
  paths["/api/v1/batches"]["post"]["responses"][201]["content"]["application/json"];
type Usage =
  paths["/api/v1/usage"]["get"]["responses"][200]["content"]["application/json"];
type Team =
  paths["/api/v1/teams/{team_id}"]["get"]["responses"][200]["content"]["application/json"];
type TokenList = paths["/api/v1/tokens"]["get"]["responses"][200]["content"]["application/json"];

/** Plan 28 PR-3: backend catalog entry returned by GET /api/v1/backends.
 * Driven by the union of `capabilities.backend` reported by active workers. */
export interface Backend {
  name: string;
  description: string;
  /** True when at least one live worker advertises this backend. The
   * SPA renders unavailable backends as greyed-out so users see the
   * full set of drivers Loom ships while understanding which can run
   * a batch right now. */
  available: boolean;
}

/** Plan 28 PR-3: one (agent, model, n_per_task) tuple within a Batch.
 * The submit form always sends a list of these (even single-combo
 * batches send a 1-element list) so the back-end uses one code path. */
export interface Combination {
  label?: string | null;
  agent_name: string;
  agent_model: { provider: string; name: string } | null;
  n_per_task: number;
}

/** Plan 28 PR-3: structured task_filter discriminated by subset_kind.
 * Series/tags PR-2 added `benchmark_ids` (multi-select union, takes
 * precedence over singular `benchmark_id`) and `tag_filters`
 * (per-key value-list, AND across keys, OR within each value list). */
export interface TaskFilter {
  benchmark_id?: string;
  benchmark_ids?: string[];
  tag_filters?: Record<string, string[]>;
  task_ids?: string[];
  license?: string;
  subset_kind?: "all" | "first_n" | "last_n" | "random_n" | "explicit";
  n?: number;
  seed?: number;
}

/** Series/tags PR-2: `/benchmarks/{id}/tags` response. */
export interface BenchmarkTagsResponse {
  items: { key: string; values: string[] }[];
}

export interface CreateBatchBody {
  name: string;
  description?: string;
  backend: string;
  task_filter: TaskFilter;
  trial_config: Record<string, unknown>;
  combinations?: Combination[];
  n_per_task?: number;
  provider_connection_id?: string;
  provider_model_id?: string;
}

export interface ModelEntry {
  provider: string;
  name: string;
  source?: string;
  provider_connection_id?: string;
  provider_connection_name?: string;
  provider_connection_type?: string;
  agent_capable?: boolean;
  recommended?: boolean;
  visibility?: string;
  hidden_reason?: string | null;
  last_seen_at?: string | null;
}

export interface ProviderConnectionModelEntry {
  model_id: string;
  family?: string | null;
  context_length?: number | null;
  capabilities?: Record<string, unknown>;
  visible: boolean;
  hidden_reason?: string | null;
  last_seen_at?: string | null;
  upstream_present?: boolean;
  source?: string;
  agent_capable?: boolean;
  recommended?: boolean;
  visibility?: string;
}

export interface ProviderConnectionEntry {
  id: string;
  name: string;
  type: string;
  status: string;
  rate_card_provider?: string | null;
}

export interface ProviderConnectionDetail extends ProviderConnectionEntry {
  base_url?: string | null;
  allowed_models?: string[] | null;
  created_at?: string;
  updated_at?: string;
}

export interface ProviderConnectionCreateBody {
  name: string;
  type: string;
  base_url: string;
  api_key: string;
  allowed_models?: string[] | null;
  rate_card_provider?: string | null;
}

export interface ProviderConnectionPatchBody {
  name?: string;
  base_url?: string;
  api_key?: string;
  allowed_models?: string[] | null;
  rate_card_provider?: string | null;
}

export interface ProviderConnectionTestResult {
  status: string;
  message?: string | null;
}

export interface ProviderConnectionModelsRefreshResult {
  added: number;
  refreshed: number;
  missing: number;
  items: ProviderConnectionModelEntry[];
}

export interface TeamRegistrationEntry {
  id: string;
  name: string;
  contact_email: string;
  status: "pending" | "approved" | "rejected" | "expired";
  requested_at: string;
  reviewed_at: string | null;
  reviewed_by_actor: string | null;
  approved_team_id: string | null;
}

export interface TeamRegistrationApproval {
  registration: TeamRegistrationEntry;
  team: { id: string; name: string };
  team_token: string;
  token_hash_prefix: string;
  expires_at: string;
}

export interface AdminAuditEvent {
  id: string;
  created_at: string;
  actor: string;
  action: string;
  target_type: string;
  target_id: string;
  request_id: string | null;
  source_ip_hash: string | null;
  user_agent_hash: string | null;
  metadata: Record<string, unknown>;
}

function qs(params: Record<string, string | number | undefined>): string {
  const out: Record<string, string> = {};
  for (const [k, v] of Object.entries(params)) {
    if (v !== undefined && v !== "") out[k] = String(v);
  }
  const s = new URLSearchParams(out).toString();
  return s ? `?${s}` : "";
}

export const api = {
  listTrials: (q: Record<string, string | undefined> = {}) =>
    apiFetch<TrialList>(`/api/v1/trials${qs(q)}`),
  getTrial: (id: string) =>
    apiFetch<TrialDetail>(`/api/v1/trials/${id}`),
  submitTrial: (body: {
    task_id: string;
    config: Record<string, unknown>;
    provider_connection_id?: string;
    provider_model_id?: string;
  }) =>
    apiFetch<{ trial_id: string; state: string }>("/api/v1/trials", {
      method: "POST",
      body: JSON.stringify(body),
    }),
  getTrajectoryPage: (id: string, cursor?: number, limit = 200) =>
    apiFetch<TrajectoryPage>(
      `/api/v1/trials/${id}/trajectory${qs({ cursor, limit })}`,
    ),
  listTasks: (q: Record<string, string | undefined> = {}) =>
    apiFetch<TaskList>(`/api/v1/tasks${qs(q)}`),
  /**
   * POST /api/v1/tasks/count — returns the exact materialized count
   * for a given task_filter. Used by NewBatch to gate submit on the
   * real filtered count instead of an upper-bound estimate when
   * tag_filters are active. Closes #28.
   */
  countTasks: (body: { task_filter: Record<string, unknown> }) =>
    apiFetch<{ count: number }>("/api/v1/tasks/count", {
      method: "POST",
      body: JSON.stringify(body),
    }),
  listBenchmarks: (q: Record<string, string | undefined> = {}) =>
    apiFetch<BenchmarkList>(`/api/v1/benchmarks${qs(q)}`),
  listBenchmarkTags: (id: string) =>
    apiFetch<BenchmarkTagsResponse>(
      `/api/v1/benchmarks/${encodeURIComponent(id)}/tags`,
    ),
  listAgents: () =>
    apiFetch<{
      items: {
        name: string;
        needs_model: boolean;
        kind: "builtin" | "adapter";
        description: string;
        /** PR-A: provider whitelist. ["*"] = any provider the gateway routes. */
        supported_providers: string[];
        /** PR-A: subset of ["api","local-server","hf"]. Empty when needs_model=false. */
        supported_model_sources: string[];
      }[];
    }>("/api/v1/agents"),
  listLocalServers: () =>
    apiFetch<{
      items: {
        name: string;
        base_url: string;
        kind: string | null;
        description: string | null;
      }[];
    }>("/api/v1/local-servers"),
  listModels: (view?: "default" | "raw") =>
    apiFetch<{ items: ModelEntry[] }>(
      `/api/v1/models${qs({ view })}`,
    ),
  listProviderConnections: () =>
    apiFetch<{ items: ProviderConnectionEntry[] }>(
      "/api/v1/provider-connections",
    ),
  getProviderConnection: (id: string) =>
    apiFetch<ProviderConnectionDetail>(`/api/v1/provider-connections/${id}`),
  createProviderConnection: (payload: ProviderConnectionCreateBody) =>
    apiFetch<ProviderConnectionDetail>("/api/v1/provider-connections", {
      method: "POST",
      body: JSON.stringify(payload),
    }),
  updateProviderConnection: (id: string, patch: ProviderConnectionPatchBody) =>
    apiFetch<ProviderConnectionDetail>(
      `/api/v1/provider-connections/${id}`,
      { method: "PATCH", body: JSON.stringify(patch) },
    ),
  deleteProviderConnection: (id: string) =>
    apiFetch<void>(`/api/v1/provider-connections/${id}`, { method: "DELETE" }),
  testProviderConnection: (id: string) =>
    apiFetch<ProviderConnectionTestResult>(
      `/api/v1/provider-connections/${id}/test`,
      { method: "POST" },
    ),
  listProviderConnectionModels: (id: string) =>
    apiFetch<{ items: ProviderConnectionModelEntry[] }>(
      `/api/v1/provider-connections/${id}/models`,
    ),
  addProviderConnectionModel: (
    connectionId: string,
    body: { model_id: string },
  ) =>
    apiFetch<ProviderConnectionModelEntry>(
      `/api/v1/provider-connections/${connectionId}/models`,
      { method: "POST", body: JSON.stringify(body) },
    ),
  refreshProviderConnectionModels: (id: string) =>
    apiFetch<ProviderConnectionModelsRefreshResult>(
      `/api/v1/provider-connections/${id}/models/refresh`,
      { method: "POST" },
    ),
  hideProviderConnectionModel: (id: string, modelId: string) =>
    apiFetch<void>(
      `/api/v1/provider-connections/${id}/models/${encodeURIComponent(modelId)}/hide`,
      { method: "POST" },
    ),
  unhideProviderConnectionModel: (id: string, modelId: string) =>
    apiFetch<void>(
      `/api/v1/provider-connections/${id}/models/${encodeURIComponent(modelId)}/unhide`,
      { method: "POST" },
    ),
  listTokens: () => apiFetch<TokenList>("/api/v1/tokens"),
  createToken: (body: {
    type: string;
    scopes: string[];
    expires_in_days: number;
    team_id?: string;
  }) =>
    apiFetch<{ token: string; token_hash_prefix: string; expires_at: string }>(
      "/api/v1/tokens",
      { method: "POST", body: JSON.stringify(body) },
    ),
  revokeToken: (prefix: string) =>
    apiFetch<void>(`/api/v1/tokens/${prefix}`, { method: "DELETE" }),
  listTeamRegistrations: (
    status: TeamRegistrationEntry["status"] = "pending",
  ) =>
    apiFetch<{ items: TeamRegistrationEntry[] }>(
      `/api/v1/admin/team-registrations${qs({ status })}`,
    ),
  approveTeamRegistration: (id: string, actor: string) =>
    apiFetch<TeamRegistrationApproval>(
      `/api/v1/admin/team-registrations/${encodeURIComponent(id)}/approve`,
      {
        method: "POST",
        headers: { "X-Loom-Admin-Actor": actor },
      },
    ),
  rejectTeamRegistration: (id: string, actor: string, reason?: string) =>
    apiFetch<TeamRegistrationEntry>(
      `/api/v1/admin/team-registrations/${encodeURIComponent(id)}/reject`,
      {
        method: "POST",
        headers: { "X-Loom-Admin-Actor": actor },
        body: JSON.stringify({ reason: reason ?? null }),
      },
    ),
  listAdminAuditEvents: (limit = 50, cursor?: string) =>
    apiFetch<{ items: AdminAuditEvent[]; next_cursor: string | null }>(
      `/api/v1/admin/audit-events${qs({ limit, cursor })}`,
    ),
  listBatches: (q: Record<string, string | undefined> = {}) =>
    apiFetch<BatchList>(`/api/v1/batches${qs(q)}`),
  getBatch: (id: string) =>
    apiFetch<BatchDetail>(`/api/v1/batches/${id}`),
  createBatch: (body: CreateBatchBody) =>
    apiFetch<BatchCreate>("/api/v1/batches", {
      method: "POST",
      body: JSON.stringify(body),
    }),
  listBackends: () =>
    apiFetch<{ items: Backend[] }>("/api/v1/backends"),
  cancelBatch: (id: string) =>
    apiFetch<{ batch_id: string; state: string }>(
      `/api/v1/batches/${id}/cancel`,
      { method: "POST" },
    ),
  listRateCards: () =>
    apiFetch<{ items: unknown[] }>("/api/v1/rate-cards"),
  createRateCard: (body: Record<string, unknown>) =>
    apiFetch<unknown>("/api/v1/rate-cards", {
      method: "POST",
      body: JSON.stringify(body),
    }),
  getUsage: (q: {
    team_id?: string;
    start: string;
    end: string;
    group_by?: string;
  }) => apiFetch<Usage>(`/api/v1/usage${qs(q)}`),
  getTeam: (teamId: string) =>
    apiFetch<Team>(`/api/v1/teams/${teamId}`),
};
