/**
 * `apiFetch` is the single point of entry for all `loom_service`
 * calls. Browser auth uses HttpOnly session cookies, so requests always
 * include credentials. Unsafe methods copy the in-memory CSRF token from
 * `/auth/*` responses into `X-Loom-CSRF`; non-2xx responses surface as
 * typed `ApiError`s.
 */

import type { paths } from "./schema";

export type ApiError = { status: number; detail: string };

let _onUnauthorized: () => void = () => {};
export function setUnauthorizedHandler(cb: () => void): void {
  _onUnauthorized = cb;
}

let _csrfToken: string | null = null;
export function setCsrfToken(token: string | null): void {
  _csrfToken = token;
}

function apiBase(): string {
  const env = (
    import.meta as unknown as { env: { VITE_API_BASE?: string } }
  ).env;
  return env.VITE_API_BASE ?? "";
}

function isUnsafeMethod(method?: string): boolean {
  const m = (method ?? "GET").toUpperCase();
  return !["GET", "HEAD", "OPTIONS"].includes(m);
}

function authHeaders(
  initHeaders?: RequestInit["headers"],
  method?: string,
): Record<string, string> {
  const headers: Record<string, string> = {
    ...(initHeaders as Record<string, string> | undefined),
  };
  if (isUnsafeMethod(method) && !("X-Loom-CSRF" in headers)) {
    if (_csrfToken) headers["X-Loom-CSRF"] = _csrfToken;
  }
  return headers;
}

async function throwIfApiError(resp: Response): Promise<void> {
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
}

export async function apiFetch<T>(
  path: string,
  init: RequestInit = {},
): Promise<T> {
  const headers: Record<string, string> = {
    "Content-Type": "application/json",
    ...authHeaders(init.headers, init.method),
  };

  const resp = await fetch(`${apiBase()}${path}`, {
    ...init,
    headers,
    credentials: "include",
  });

  await throwIfApiError(resp);
  if (resp.status === 204) return undefined as T;
  return (await resp.json()) as T;
}

export async function apiDownload(
  path: string,
  filename: string,
): Promise<void> {
  const resp = await fetch(`${apiBase()}${path}`, {
    headers: authHeaders(),
    credentials: "include",
  });

  await throwIfApiError(resp);

  const blob = await resp.blob();
  const objectUrl = URL.createObjectURL(blob);
  const link = document.createElement("a");
  link.href = objectUrl;
  link.download = filename;
  link.rel = "noreferrer";
  document.body.appendChild(link);
  try {
    link.click();
  } finally {
    link.remove();
    URL.revokeObjectURL(objectUrl);
  }
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
type BatchFailedRerun =
  paths["/api/v1/batches/{id}/rerun-failed"]["post"]["responses"][201]["content"]["application/json"];
type Usage =
  paths["/api/v1/usage"]["get"]["responses"][200]["content"]["application/json"];
type Team =
  paths["/api/v1/teams/{team_id}"]["get"]["responses"][200]["content"]["application/json"];

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

export interface AuthTeam {
  id: string;
  name: string;
  role: string;
}

export interface AuthMe {
  user: {
    id: string;
    email: string;
    display_name: string | null;
    is_platform_admin: boolean;
  };
  teams: AuthTeam[];
  current_team: AuthTeam | null;
  role: string | null;
  scopes: string[];
  is_platform_admin: boolean;
  csrf_token?: string;
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

export interface TeamRegistrationRequestBody {
  name: string;
  contact_email: string;
}

export interface TeamRegistrationApproval {
  registration: TeamRegistrationEntry;
  team: { id: string; name: string };
  invite: InviteEntry;
  invite_code: string;
  invite_link: string;
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

export type InviteStatus = "pending" | "accepted" | "revoked" | "expired";
export type InviteRole = "owner" | "member" | "viewer";

export interface InviteEntry {
  id: string;
  team_id: string;
  team_name: string | null;
  email: string;
  allowed_domain: string | null;
  role: InviteRole;
  status: InviteStatus;
  code_prefix: string;
  max_uses: number | null;
  accepted_uses: number;
  created_by_actor: string;
  created_at: string;
  expires_at: string;
  last_sent_at: string | null;
  accepted_at: string | null;
  revoked_at: string | null;
}

export interface InviteCreateBody {
  email: string;
  team_id?: string;
  role: InviteRole;
  expires_in_days: number;
  max_uses?: number | null;
  allowed_domain?: string | null;
}

export interface InviteReveal {
  invite: InviteEntry;
  invite_code: string;
  invite_link: string;
}

export interface InviteLookup {
  team_name: string;
  role: InviteRole;
  status: InviteStatus;
  code_prefix: string;
}

export interface ApiTokenEntry {
  name: string | null;
  token_hash_prefix: string;
  type: string;
  scopes: string[];
  team_id: string | null;
  issued_at: string;
  expires_at: string | null;
  revoked_at: string | null;
  last_used_at?: string | null;
  created_by_actor?: string | null;
  created_by_user_id?: string | null;
}

export interface ApiTokenList {
  items: ApiTokenEntry[];
}

export interface ApiTokenReveal {
  token: string;
  token_hash_prefix: string;
  expires_at: string | null;
  item?: ApiTokenEntry;
}

export type RunVisibility = "team" | "org" | "private";
export type ShareStatus = "pending_scan" | "shared" | "blocked";
export type ArtifactGroup =
  | "reports"
  | "trajectories"
  | "reusable_outputs"
  | "logs_diagnostics"
  | "raw_diagnostics";

export interface RunLibraryOwnerTeam {
  id: string;
  name: string;
}

export interface RunLibraryArtifact {
  trial_id: string;
  key: string;
  size: number;
  role: ArtifactGroup;
  share_status: ShareStatus;
  blocked_reason?: string | null;
  download_url: string;
}

export type ArtifactSummary = Record<ArtifactGroup, number>;
export type ArtifactInventory = Record<ArtifactGroup, RunLibraryArtifact[]>;

export interface RunLibraryBatch {
  id: string;
  team_id: string;
  owner_team: RunLibraryOwnerTeam;
  name: string;
  description: string | null;
  task_filter: Record<string, unknown>;
  trial_config: Record<string, unknown>;
  backend: string;
  combinations: Combination[];
  provider_connection_id: string | null;
  provider_model_id?: string | null;
  state: string;
  result_status: string | null;
  visibility: RunVisibility;
  share_status: ShareStatus;
  source_provenance: Record<string, unknown>[];
  expected_trial_count: number;
  created_by_token_prefix: string;
  created_at: string;
  finished_at: string | null;
  trial_summary: Record<string, number>;
  aggregate_reward: number | null;
  total_cost_usd: number;
  artifact_summary: ArtifactSummary;
}

export interface RunLibraryBatchDetail extends RunLibraryBatch {
  artifact_inventory: ArtifactInventory;
}

export interface RunLibraryBatchList {
  items: RunLibraryBatch[];
  next_cursor: string | null;
}

export interface CloneRunLibraryBatchResult {
  batch_id: string;
  cloned_from_batch_id: string;
  provider_connection_id: string | null;
  provider_model_id?: string | null;
  source_provenance: Record<string, unknown>[];
  state: string;
  created_at: string;
}

export interface ReuseRunLibraryArtifactResult {
  batch_id: string;
  source_artifact: {
    trial_id: string;
    key: string;
    role: ArtifactGroup;
  };
  source_provenance: Record<string, unknown>[];
  state: string;
  created_at: string;
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
  authMe: () => apiFetch<AuthMe>("/api/v1/auth/me"),
  loginStart: (email: string) =>
    apiFetch<{ status: "sent"; login_token?: string }>(
      "/api/v1/auth/login/start",
      { method: "POST", body: JSON.stringify({ email }) },
    ),
  loginComplete: (token: string) =>
    apiFetch<AuthMe>("/api/v1/auth/login/complete", {
      method: "POST",
      body: JSON.stringify({ token }),
    }),
  lookupInvite: (code: string) =>
    apiFetch<InviteLookup>(
      `/api/v1/invites/lookup${qs({ code })}`,
    ),
  acceptInvite: (body: { code: string; email?: string | null }) =>
    apiFetch<AuthMe>("/api/v1/invites/accept", {
      method: "POST",
      body: JSON.stringify(body),
    }),
  switchTeam: (teamId: string) =>
    apiFetch<AuthMe>("/api/v1/auth/team", {
      method: "POST",
      body: JSON.stringify({ team_id: teamId }),
    }),
  logout: () => apiFetch<void>("/api/v1/auth/logout", { method: "POST" }),
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
  downloadATIF: (id: string) =>
    apiDownload(`/api/v1/trials/${id}/atif`, `${id}-atif.json`),
  downloadTrajectory: (id: string) =>
    apiDownload(
      `/api/v1/trials/${id}/trajectory/download`,
      `${id}-events.jsonl`,
    ),
  downloadArtifact: (id: string, key: string, filename: string) =>
    apiDownload(
      `/api/v1/trials/${id}/artifacts/download${qs({ key })}`,
      filename,
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
        service_mode_ready?: boolean;
        readiness_status?: "ready" | "unavailable";
        readiness_message?: string | null;
        runtime_contract?: {
          execution: string;
          capture: string;
          required_executables: string[];
          required_python_modules: string[];
          required_packages?: string[];
          endpoint_dialect?: string | null;
          api_key_env?: string | null;
          base_url_env?: string | null;
          model_name_template?: string | null;
          sandbox_network?: string;
          install_hint?: string | null;
        };
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
  listTokens: () => apiFetch<ApiTokenList>("/api/v1/tokens"),
  createToken: (body: {
    name: string;
    type: string;
    scopes: string[];
    expires_in_days: number;
    team_id?: string;
  }) =>
    apiFetch<ApiTokenReveal>(
      "/api/v1/tokens",
      { method: "POST", body: JSON.stringify(body) },
    ),
  rotateToken: (prefix: string) =>
    apiFetch<ApiTokenReveal>(
      `/api/v1/tokens/${encodeURIComponent(prefix)}/rotate`,
      { method: "POST" },
    ),
  revokeToken: (prefix: string) =>
    apiFetch<void>(
      `/api/v1/tokens/${encodeURIComponent(prefix)}`,
      { method: "DELETE" },
    ),
  requestTeamRegistration: (body: TeamRegistrationRequestBody) =>
    apiFetch<TeamRegistrationEntry>("/api/v1/teams/register", {
      method: "POST",
      body: JSON.stringify(body),
    }),
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
  listInvites: (q: { team_id?: string; status?: InviteStatus } = {}) =>
    apiFetch<{ items: InviteEntry[] }>(
      `/api/v1/invites${qs(q)}`,
    ),
  createInvite: (body: InviteCreateBody, actor?: string) =>
    apiFetch<InviteReveal>("/api/v1/invites", {
      method: "POST",
      headers: actor ? { "X-Loom-Admin-Actor": actor } : undefined,
      body: JSON.stringify(body),
    }),
  revokeInvite: (id: string, reason?: string, actor?: string) =>
    apiFetch<InviteEntry>(
      `/api/v1/invites/${encodeURIComponent(id)}/revoke`,
      {
        method: "POST",
        headers: actor ? { "X-Loom-Admin-Actor": actor } : undefined,
        body: JSON.stringify({ reason: reason ?? null }),
      },
    ),
  resendInvite: (id: string, actor?: string) =>
    apiFetch<InviteReveal>(
      `/api/v1/invites/${encodeURIComponent(id)}/resend`,
      {
        method: "POST",
        headers: actor ? { "X-Loom-Admin-Actor": actor } : undefined,
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
  listRunLibraryBatches: (
    q: Record<string, string | undefined> = {},
  ) => apiFetch<RunLibraryBatchList>(`/api/v1/run-library/batches${qs(q)}`),
  getRunLibraryBatch: (id: string) =>
    apiFetch<RunLibraryBatchDetail>(`/api/v1/run-library/batches/${id}`),
  cloneRunLibraryBatchConfig: (
    id: string,
    body: {
      name: string;
      description?: string | null;
      provider_connection_id?: string | null;
      provider_model_id?: string | null;
    },
  ) =>
    apiFetch<CloneRunLibraryBatchResult>(
      `/api/v1/run-library/batches/${id}/clone-config`,
      { method: "POST", body: JSON.stringify(body) },
    ),
  reuseRunLibraryArtifact: (
    trialId: string,
    body: {
      key: string;
      name: string;
      description?: string | null;
      provider_connection_id?: string | null;
      provider_model_id?: string | null;
    },
  ) =>
    apiFetch<ReuseRunLibraryArtifactResult>(
      `/api/v1/run-library/trials/${trialId}/artifacts/reuse`,
      { method: "POST", body: JSON.stringify(body) },
    ),
  downloadRunLibraryArtifact: (
    trialId: string,
    key: string,
    filename: string,
  ) =>
    apiDownload(
      `/api/v1/run-library/trials/${trialId}/artifacts/download${qs({ key })}`,
      filename,
    ),
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
  rerunFailedBatch: (id: string) =>
    apiFetch<BatchFailedRerun>(
      `/api/v1/batches/${id}/rerun-failed`,
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
