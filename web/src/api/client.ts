/**
 * `apiFetch` is the single point of entry for all `loom_service`
 * calls. Browser auth uses HttpOnly session cookies, so requests always
 * include credentials. Unsafe methods copy the in-memory CSRF token from
 * `/auth/*` responses into `X-Loom-CSRF`; non-2xx responses surface as
 * typed `ApiError`s.
 */

import { getApiBase } from "../lib/frontendConfig";
import type { paths } from "./schema";

export type ApiError = { status: number; detail: string };

export type AuthSessionLoadFailureKind =
  | "unauthorized"
  | "network"
  | "http"
  | "invalid";

/** Detail-free failure used only by the browser-session bootstrap path. */
export class AuthSessionLoadError extends Error {
  readonly kind: AuthSessionLoadFailureKind;

  constructor(kind: AuthSessionLoadFailureKind) {
    const message =
      kind === "unauthorized"
        ? "browser session is signed out"
        : kind === "network"
          ? "browser session request failed"
          : kind === "http"
            ? "browser session returned an unsuccessful status"
            : "browser session response is invalid";
    super(message);
    this.name = "AuthSessionLoadError";
    this.kind = kind;
  }
}

let _onUnauthorized: () => void = () => {};
export function setUnauthorizedHandler(cb: () => void): void {
  _onUnauthorized = cb;
}

let _csrfToken: string | null = null;
export function setCsrfToken(token: string | null): void {
  _csrfToken = token;
}

function apiBase(): string {
  return getApiBase();
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

async function throwIfApiError(
  resp: Response,
  onUnauthorized: () => void,
): Promise<void> {
  if (resp.status === 401) {
    onUnauthorized();
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
  const onUnauthorized = _onUnauthorized;
  const headers: Record<string, string> = {
    "Content-Type": "application/json",
    ...authHeaders(init.headers, init.method),
  };

  const resp = await fetch(`${apiBase()}${path}`, {
    ...init,
    headers,
    credentials: "include",
  });

  await throwIfApiError(resp, onUnauthorized);
  if (resp.status === 204) return undefined as T;
  return (await resp.json()) as T;
}

export async function apiUpload<T>(
  path: string,
  formData: FormData,
): Promise<T> {
  const onUnauthorized = _onUnauthorized;
  const headers: Record<string, string> = authHeaders(undefined, "POST");

  const resp = await fetch(`${apiBase()}${path}`, {
    method: "POST",
    headers,
    body: formData,
    credentials: "include",
  });

  await throwIfApiError(resp, onUnauthorized);
  if (resp.status === 204) return undefined as T;
  return (await resp.json()) as T;
}

export async function apiDownload(
  path: string,
  filename: string,
): Promise<void> {
  const onUnauthorized = _onUnauthorized;
  const resp = await fetch(`${apiBase()}${path}`, {
    headers: authHeaders(),
    credentials: "include",
  });

  await throwIfApiError(resp, onUnauthorized);

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
export type MonitorSummary =
  paths["/api/v1/monitor/summary"]["get"]["responses"][200]["content"]["application/json"];
export type TrialDetail =
  paths["/api/v1/trials/{trial_id}"]["get"]["responses"][200]["content"]["application/json"];
export type DebugEvidence =
  paths["/api/v1/trials/{trial_id}/debug"]["get"]["responses"][200]["content"]["application/json"];
export type DiagnosisReport =
  paths["/api/v1/trials/{trial_id}/diagnosis"]["get"]["responses"][200]["content"]["application/json"];
type TrajectoryPage =
  paths["/api/v1/trials/{trial_id}/trajectory"]["get"]["responses"][200]["content"]["application/json"];
export type TaskList = paths["/api/v1/tasks"]["get"]["responses"][200]["content"]["application/json"];
export type TaskRow = TaskList["items"][number];
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
export type RerunPlan =
  paths["/api/v1/batches/{id}/rerun-plan"]["get"]["responses"][200]["content"]["application/json"];
export type DeliveryExport =
  paths["/api/v1/batches/{id}/delivery-export"]["get"]["responses"][200]["content"]["application/json"];
type Usage =
  paths["/api/v1/usage"]["get"]["responses"][200]["content"]["application/json"];
type Team =
  paths["/api/v1/teams/{team_id}"]["get"]["responses"][200]["content"]["application/json"];
export type AdminTeam = Team;

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
  provider_connection_id?: string | null;
  provider_model_id?: string | null;
}

export interface CombinationSummary {
  combination_idx: number;
  label: string;
  agent_name: string;
  agent_model?: { provider: string; name: string } | null;
  provider_connection_id?: string | null;
  provider_model_id?: string | null;
  n_per_task?: number;
  expected_trial_count?: number | null;
  trial_count: number;
  completed_trial_count?: number;
  scored_trial_count: number;
  succeeded_count: number;
  failed_count: number;
  aggregate_reward: number | null;
  total_prompt_tokens?: number;
  total_completion_tokens?: number;
  total_tokens?: number;
  llm_calls_count?: number;
  estimated_cost_usd?: number | null;
  cost_currency?: string | null;
  cost_status?: string | null;
}

/** Plan 28 PR-3: structured task_filter discriminated by subset_kind.
 * Series/tags PR-2 added `benchmark_ids` (multi-select union, takes
 * precedence over singular `benchmark_id`) and `tag_filters`
 * (per-key value-list, AND across keys, OR within each value list). */
export interface TaskFilter {
  benchmark_id?: string;
  benchmark_ids?: string[];
  task_set_id?: string;
  task_set_ids?: string[];
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
  team_id?: string;
  name?: string;
  name_suffix?: string;
  description?: string;
  backend: string;
  task_filter: TaskFilter;
  trial_config: Record<string, unknown>;
  combinations?: Combination[];
  n_per_task?: number;
  provider_connection_id?: string;
  provider_model_id?: string;
  budget_usd?: number;
  budget_policy?: "none" | "soft" | "hard";
  budget_confirmed?: boolean;
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
  last_preflight_status?: string | null;
  last_preflight_at?: string | null;
  last_preflight_http_status?: number | null;
  last_preflight_error_code?: string | null;
  last_preflight_error_message?: string | null;
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
  last_preflight_status?: string | null;
  last_preflight_at?: string | null;
  last_preflight_http_status?: number | null;
  last_preflight_error_code?: string | null;
  last_preflight_error_message?: string | null;
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
    username: string;
    email?: string | null;
    display_name: string | null;
    is_platform_admin: boolean;
  };
  teams: AuthTeam[];
  current_team: AuthTeam | null;
  role: string | null;
  scopes: string[];
  is_platform_admin: boolean;
  csrf_token: string;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function isNonEmptyString(value: unknown): value is string {
  return typeof value === "string" && value.trim().length > 0;
}

function parseAuthTeam(value: unknown): AuthTeam | null {
  if (!isRecord(value)) return null;
  if (
    !isNonEmptyString(value.id) ||
    !isNonEmptyString(value.name) ||
    !isNonEmptyString(value.role)
  ) {
    return null;
  }
  return { id: value.id, name: value.name, role: value.role };
}

/** Whitelist the browser-session response so debug fields are never retained. */
export function parseAuthMe(value: unknown): AuthMe {
  if (
    !isRecord(value) ||
    !isRecord(value.user) ||
    !Array.isArray(value.teams) ||
    !Array.isArray(value.scopes)
  ) {
    throw new AuthSessionLoadError("invalid");
  }
  const user = value.user;
  const teams = value.teams.map(parseAuthTeam);
  const currentTeam =
    value.current_team === null ? null : parseAuthTeam(value.current_team);
  if (
    !isNonEmptyString(user.id) ||
    !isNonEmptyString(user.username) ||
    !(typeof user.email === "string" || user.email === null) ||
    !(typeof user.display_name === "string" || user.display_name === null) ||
    typeof user.is_platform_admin !== "boolean" ||
    teams.some((team) => team === null) ||
    (value.current_team !== null && currentTeam === null) ||
    value.scopes.some((scope) => !isNonEmptyString(scope)) ||
    typeof value.is_platform_admin !== "boolean" ||
    value.is_platform_admin !== user.is_platform_admin ||
    (value.role !== null && !isNonEmptyString(value.role)) ||
    !isNonEmptyString(value.csrf_token)
  ) {
    throw new AuthSessionLoadError("invalid");
  }
  const parsedTeams = teams as AuthTeam[];
  if (
    currentTeam !== null &&
    !parsedTeams.some(
      (team) =>
        team.id === currentTeam.id &&
        team.name === currentTeam.name &&
        team.role === currentTeam.role,
    )
  ) {
    throw new AuthSessionLoadError("invalid");
  }

  return {
    user: {
      id: user.id,
      username: user.username,
      email: user.email,
      display_name: user.display_name,
      is_platform_admin: user.is_platform_admin,
    },
    teams: parsedTeams,
    current_team: currentTeam,
    role: value.role as string | null,
    scopes: value.scopes as string[],
    is_platform_admin: value.is_platform_admin,
    csrf_token: value.csrf_token,
  };
}

async function parseAuthSessionResponse(response: Response): Promise<AuthMe> {
  if (response.status === 401) {
    throw new AuthSessionLoadError("unauthorized");
  }
  if (!response.ok) {
    // Session-producing responses can contain proxy diagnostics or echoed
    // request data. Classification never requires consuming their body.
    throw new AuthSessionLoadError("http");
  }
  if (response.status === 204) {
    throw new AuthSessionLoadError("invalid");
  }

  try {
    return parseAuthMe(await response.json());
  } catch (error) {
    if (error instanceof AuthSessionLoadError) throw error;
    throw new AuthSessionLoadError("invalid");
  }
}

export async function loadAuthSession(): Promise<AuthMe> {
  let response: Response;
  try {
    response = await fetch(`${apiBase()}/api/v1/auth/me`, {
      credentials: "include",
      headers: { Accept: "application/json" },
    });
  } catch {
    throw new AuthSessionLoadError("network");
  }

  return parseAuthSessionResponse(response);
}

async function mutateAuthSession(
  path: string,
  body: unknown,
): Promise<AuthMe> {
  let response: Response;
  try {
    response = await fetch(`${apiBase()}${path}`, {
      method: "POST",
      body: JSON.stringify(body),
      credentials: "include",
      headers: {
        "Content-Type": "application/json",
        Accept: "application/json",
        ...authHeaders(undefined, "POST"),
      },
    });
  } catch {
    throw new AuthSessionLoadError("network");
  }

  return parseAuthSessionResponse(response);
}

export interface PublicTeam {
  id: string;
  name: string;
}

export interface UserRegistrationEntry {
  id: string;
  username: string;
  username_normalized: string;
  team_id: string;
  team_name?: string | null;
  role: InviteRole;
  status: "pending" | "approved" | "rejected";
  requested_at: string;
  reviewed_at: string | null;
  reviewed_by_actor: string | null;
  setup_token_prefix?: string | null;
}

export interface AccountActionApproval {
  setup_link?: string;
  reset_link?: string;
  setup_token_prefix?: string;
  reset_token_prefix?: string;
  registration?: UserRegistrationEntry;
  request?: PasswordResetRequestEntry;
  user: { id: string; username: string };
  team?: { id: string; name: string };
}

export interface PasswordResetRequestEntry {
  id: string;
  username: string;
  username_normalized: string;
  status: "pending" | "approved" | "rejected";
  requested_at: string;
  reviewed_at: string | null;
  reviewed_by_actor: string | null;
  reset_token_prefix?: string | null;
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

export interface TeamRegistrationApprovalBody {
  team_id: string;
  role: InviteRole;
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
  id?: string;
  trial_id: string | null;
  key: string;
  size: number;
  role: ArtifactGroup;
  artifact_type?: string;
  artifact_type_label?: string;
  artifact_schema_version?: string;
  owner_team?: RunLibraryOwnerTeam;
  source?: {
    kind?: string | null;
    batch_id?: string | null;
    trial_id?: string | null;
  };
  share_status: ShareStatus;
  safety_state?: string;
  redaction_state?: string;
  content_hash?: string;
  storage?: Record<string, unknown>;
  provenance?: Record<string, unknown>;
  metadata?: Record<string, unknown>;
  parents?: Record<string, unknown>[];
  blocked_reason?: string | null;
  download_url?: string | null;
}

export type ArtifactSummary = Record<ArtifactGroup, number>;
export type ArtifactInventory = Record<ArtifactGroup, RunLibraryArtifact[]>;

export interface RunLibraryBatch {
  id: string;
  team_id: string;
  owner_team: RunLibraryOwnerTeam;
  submitted_by_user?: {
    id: string;
    username: string;
    team_id?: string | null;
    team_name?: string | null;
  } | null;
  name: string;
  description: string | null;
  task_filter: Record<string, unknown>;
  trial_config: Record<string, unknown>;
  backend: string;
  combinations: Combination[];
  combination_summary?: CombinationSummary[];
  effective_combination_summary?: CombinationSummary[];
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
  total_prompt_tokens?: number;
  total_completion_tokens?: number;
  total_tokens?: number;
  llm_calls_count?: number;
  estimated_cost_usd?: number | null;
  cost_currency?: string | null;
  cost_status?: string | null;
  cost_estimate_source?: string | null;
  cost_estimate_confidence?: string | null;
  budget_usd?: number | null;
  budget_policy?: string | null;
  budget_remaining_usd?: number | null;
  budget_status?: string | null;
  artifact_summary: ArtifactSummary;
  artifact_summary_truncated?: boolean;
  debug_evidence?: DebugEvidence;
  diagnosis?: DiagnosisReport;
}

export interface RunLibraryBatchDetail extends RunLibraryBatch {
  artifact_inventory: ArtifactInventory;
  artifact_inventory_truncated?: boolean;
}

export interface RunLibraryBatchList {
  items: RunLibraryBatch[];
  next_cursor: string | null;
}

export interface RetryPolicyView {
  max_attempts: number;
  retry_on: string[];
  backoff: {
    base_sec: number;
    max_sec: number;
    multiplier: number;
    jitter: number;
  };
}

export interface RetryDefaultSnapshotMismatch {
  source: RetryPolicyView;
  current: RetryPolicyView;
}

export interface CloneRunLibraryBatchResult {
  batch_id: string;
  cloned_from_batch_id: string;
  provider_connection_id: string | null;
  provider_model_id?: string | null;
  source_provenance: Record<string, unknown>[];
  state: string;
  created_at: string;
  retry_default_snapshot_mismatch: RetryDefaultSnapshotMismatch | null;
}

export type OverviewStatus = "ready" | "needs_setup" | "blocked";
export type OverviewActionKind = "user" | "operator";

export interface OverviewAction {
  id: string;
  label: string;
  to: string;
  kind: OverviewActionKind;
  priority: number;
}

export interface OverviewSummary {
  status: OverviewStatus;
  summary: string;
  team_context: {
    team_id: string | null;
    team_name: string | null;
    role: string | null;
    scopes: string[];
    is_platform_admin: boolean;
    submissions_paused: boolean;
  };
  capabilities: {
    can_read: boolean;
    can_submit: boolean;
    can_manage_providers: boolean;
    can_manage_team: boolean;
  };
  provider_health: {
    total: number;
    ready: number;
    needs_attention: number;
    untested: number;
    latest: {
      id: string;
      name: string;
      type: string;
      status: string;
      last_validated_at: string | null;
      last_validation_error: string | null;
    }[];
  };
  benchmark_readiness: {
    total: number;
    runnable: number;
    needs_attention: number;
    blocked: {
      id: string;
      display_name: string;
      readiness_state: string;
      readiness_label: string;
      blocker_reason: string | null;
      task_count: number;
    }[];
  };
  worker_health: {
    active: number;
    available_backends: string[];
    has_default_backend: boolean;
  };
  run_activity: {
    batches: Record<string, number>;
    trials: Record<string, number>;
    latest_batch: {
      id: string;
      name: string;
      state: string;
      result_status: string | null;
      expected_trial_count: number;
      created_at: string;
    } | null;
  };
  next_actions: OverviewAction[];
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

// --- TaskSet types (matching Pydantic models in routes/tasksets.py) ---

export interface TaskSetWarning {
  code: string;
  message: string;
}

export interface TaskSetSubmitResponse {
  task_set_id: string;
  status: string;
  intents: string[];
  manifest_intents: string[];
  inferred_intents: string[];
  capabilities: string[];
  warnings: TaskSetWarning[];
  evaluation_ready: boolean;
  task_count: number;
  materialization_job_id: string;
}

export interface TaskSetDetailResponse {
  task_set_id: string;
  status: string;
  status_reason: string | null;
  intents: string[];
  manifest_intents: string[];
  inferred_intents: string[];
  capabilities: string[];
  warnings: TaskSetWarning[];
  evaluation_ready: boolean;
  task_count: number;
  error_summary: { instance_index: number; code: string; message: string }[];
  materialization_job_state: string | null;
}

export interface TaskSetListItem {
  task_set_id: string;
  display_name: string;
  status: string;
  intents: string[];
  evaluation_ready: boolean;
  task_count: number;
  created_at: string;
}

export interface TaskSetListResponse {
  items: TaskSetListItem[];
}

function qs(
  params: Record<string, string | number | boolean | undefined>,
): string {
  const out: Record<string, string> = {};
  for (const [k, v] of Object.entries(params)) {
    if (v !== undefined && v !== "") out[k] = String(v);
  }
  const s = new URLSearchParams(out).toString();
  return s ? `?${s}` : "";
}

export const api = {
  getOverview: () => apiFetch<OverviewSummary>("/api/v1/overview"),
  getMonitorSummary: (
    q: Record<string, string | undefined> = {},
  ) => apiFetch<MonitorSummary>(`/api/v1/monitor/summary${qs(q)}`),
  authMe: loadAuthSession,
  publicTeams: () => apiFetch<{ items: PublicTeam[] }>("/api/v1/auth/public-teams"),
  loginPassword: (username: string, password: string) =>
    mutateAuthSession("/api/v1/auth/login", { username, password }),
  requestRegistration: (body: { username: string; team_id: string }) =>
    apiFetch<UserRegistrationEntry>("/api/v1/auth/registration-requests", {
      method: "POST",
      body: JSON.stringify({ ...body, metadata: {} }),
    }),
  setupLookup: (token: string) =>
    apiFetch<{ username: string; team: PublicTeam | null; expires_at: string }>(
      `/api/v1/auth/setup/lookup${qs({ token })}`,
    ),
  setupComplete: (body: { token: string; password: string; confirm_password: string }) =>
    apiFetch<{ status: string; user: { id: string; username: string } }>(
      "/api/v1/auth/setup/complete",
      { method: "POST", body: JSON.stringify(body) },
    ),
  requestPasswordReset: (username: string) =>
    apiFetch<{ status: "pending" }>("/api/v1/auth/password-reset-requests", {
      method: "POST",
      body: JSON.stringify({ username }),
    }),
  resetLookup: (token: string) =>
    apiFetch<{ username: string; expires_at: string }>(
      `/api/v1/auth/reset/lookup${qs({ token })}`,
    ),
  resetComplete: (body: { token: string; password: string; confirm_password: string }) =>
    apiFetch<{ status: string; user: { id: string; username: string } }>(
      "/api/v1/auth/reset/complete",
      { method: "POST", body: JSON.stringify(body) },
    ),
  loginStart: (email: string) =>
    apiFetch<{ status: "sent"; login_token?: string }>(
      "/api/v1/auth/login/start",
      { method: "POST", body: JSON.stringify({ email }) },
    ),
  loginComplete: (token: string) =>
    mutateAuthSession("/api/v1/auth/login/complete", { token }),
  lookupInvite: (code: string) =>
    apiFetch<InviteLookup>(
      `/api/v1/invites/lookup${qs({ code })}`,
    ),
  acceptInvite: (body: { code: string; email?: string | null }) =>
    mutateAuthSession("/api/v1/invites/accept", body),
  switchTeam: (teamId: string) =>
    mutateAuthSession("/api/v1/auth/team", { team_id: teamId }),
  logout: () => apiFetch<void>("/api/v1/auth/logout", { method: "POST" }),
  listTrials: (q: Record<string, string | undefined> = {}) =>
    apiFetch<TrialList>(`/api/v1/trials${qs(q)}`),
  getTrial: (id: string) =>
    apiFetch<TrialDetail>(`/api/v1/trials/${id}`),
  getTrialDebug: (id: string) =>
    apiFetch<DebugEvidence>(`/api/v1/trials/${id}/debug`),
  getTrialDiagnosis: (id: string) =>
    apiFetch<DiagnosisReport>(`/api/v1/trials/${id}/diagnosis`),
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
        /** User-facing APIs normally return only displayed entries. */
        catalog_visibility?: "displayed" | "internal";
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
  preflightProviderConnectionModel: (id: string, modelId: string) =>
    apiFetch<ProviderConnectionModelEntry>(
      `/api/v1/provider-connections/${id}/models/${encodeURIComponent(modelId)}/preflight`,
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
  createToken: (
    body: {
      name: string;
      type: string;
      scopes: string[];
      expires_in_days: number;
      team_id?: string;
    },
    actor?: string,
  ) =>
    apiFetch<ApiTokenReveal>(
      "/api/v1/tokens",
      {
        method: "POST",
        headers: actor ? { "X-Loom-Admin-Actor": actor } : undefined,
        body: JSON.stringify(body),
      },
    ),
  rotateToken: (prefix: string, actor?: string) =>
    apiFetch<ApiTokenReveal>(
      `/api/v1/tokens/${encodeURIComponent(prefix)}/rotate`,
      {
        method: "POST",
        headers: actor ? { "X-Loom-Admin-Actor": actor } : undefined,
      },
    ),
  revokeToken: (prefix: string, actor?: string) =>
    apiFetch<void>(
      `/api/v1/tokens/${encodeURIComponent(prefix)}`,
      {
        method: "DELETE",
        headers: actor ? { "X-Loom-Admin-Actor": actor } : undefined,
      },
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
  listUserRegistrationRequests: (status = "pending") =>
    apiFetch<{ items: UserRegistrationEntry[] }>(
      `/api/v1/admin/registration-requests${qs({ status })}`,
    ),
  approveUserRegistrationRequest: (id: string, role: InviteRole = "member") =>
    apiFetch<AccountActionApproval>(
      `/api/v1/admin/registration-requests/${encodeURIComponent(id)}/approve`,
      { method: "POST", body: JSON.stringify({ role }) },
    ),
  rejectUserRegistrationRequest: (id: string, reason?: string) =>
    apiFetch<UserRegistrationEntry>(
      `/api/v1/admin/registration-requests/${encodeURIComponent(id)}/reject`,
      { method: "POST", body: JSON.stringify({ reason: reason ?? null }) },
    ),
  listPasswordResetRequests: (status = "pending") =>
    apiFetch<{ items: PasswordResetRequestEntry[] }>(
      `/api/v1/admin/password-reset-requests${qs({ status })}`,
    ),
  approvePasswordResetRequest: (id: string) =>
    apiFetch<AccountActionApproval>(
      `/api/v1/admin/password-reset-requests/${encodeURIComponent(id)}/approve`,
      { method: "POST" },
    ),
  rejectPasswordResetRequest: (id: string, reason?: string) =>
    apiFetch<PasswordResetRequestEntry>(
      `/api/v1/admin/password-reset-requests/${encodeURIComponent(id)}/reject`,
      { method: "POST", body: JSON.stringify({ reason: reason ?? null }) },
    ),
  listAdminTeams: () =>
    apiFetch<{ items: AdminTeam[] }>("/api/v1/admin/teams"),
  createAdminTeam: (body: { name: string }, actor: string) =>
    apiFetch<AdminTeam>("/api/v1/admin/teams", {
      method: "POST",
      headers: { "X-Loom-Admin-Actor": actor },
      body: JSON.stringify(body),
    }),
  updateAdminTeam: (id: string, body: { name: string }, actor: string) =>
    apiFetch<AdminTeam>(`/api/v1/admin/teams/${encodeURIComponent(id)}`, {
      method: "PATCH",
      headers: { "X-Loom-Admin-Actor": actor },
      body: JSON.stringify(body),
    }),
  approveTeamRegistration: (
    id: string,
    actor: string,
    body: TeamRegistrationApprovalBody,
  ) =>
    apiFetch<TeamRegistrationApproval>(
      `/api/v1/admin/team-registrations/${encodeURIComponent(id)}/approve`,
      {
        method: "POST",
        headers: { "X-Loom-Admin-Actor": actor },
        body: JSON.stringify(body),
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
  getBatchDebug: (id: string) =>
    apiFetch<DebugEvidence>(`/api/v1/batches/${id}/debug`),
  getBatchDiagnosis: (id: string) =>
    apiFetch<DiagnosisReport>(`/api/v1/batches/${id}/diagnosis`),
  getBatchDeliveryExport: (id: string) =>
    apiFetch<DeliveryExport>(`/api/v1/batches/${id}/delivery-export`),
  createBatchDeliveryExport: (
    id: string,
    body: { supplemental_batch_ids?: string[] | null } = {},
  ) =>
    apiFetch<DeliveryExport>(`/api/v1/batches/${id}/delivery-export`, {
      method: "POST",
      body: JSON.stringify(body),
    }),
  downloadBatchDeliveryExport: (downloadUrl: string, filename: string) =>
    apiDownload(downloadUrl, filename),
  listRunLibraryBatches: (
    q: Record<string, string | undefined> = {},
  ) => apiFetch<RunLibraryBatchList>(`/api/v1/run-library/batches${qs(q)}`),
  getRunLibraryBatch: (id: string, includeDebug = false) =>
    apiFetch<RunLibraryBatchDetail>(
      `/api/v1/run-library/batches/${id}${qs({
        include_debug: includeDebug ? "true" : undefined,
      })}`,
    ),
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
  exportRunLibraryArtifacts: (
    q: Record<string, string | undefined> = {},
    filename = "run-library-artifacts.jsonl",
  ) =>
    apiDownload(
      `/api/v1/run-library/artifacts/export${qs(q)}`,
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
  getBatchRerunPlan: (
    id: string,
    q: { task_id?: string[]; include_operator_approval?: boolean } = {},
  ) => {
    const params = new URLSearchParams();
    for (const taskId of q.task_id ?? []) params.append("task_id", taskId);
    if (q.include_operator_approval !== undefined) {
      params.set("include_operator_approval", String(q.include_operator_approval));
    }
    const suffix = params.toString() ? `?${params.toString()}` : "";
    return apiFetch<RerunPlan>(`/api/v1/batches/${id}/rerun-plan${suffix}`);
  },
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
    include_batches?: boolean;
  }) => apiFetch<Usage>(`/api/v1/usage${qs(q)}`),
  getTeam: (teamId: string) =>
    apiFetch<Team>(`/api/v1/teams/${teamId}`),

  // --- TaskSets ---
  listTaskSets: () =>
    apiFetch<TaskSetListResponse>("/api/v1/tasksets"),
  getTaskSet: (id: string) =>
    apiFetch<TaskSetDetailResponse>(`/api/v1/tasksets/${id}`),
  submitTaskSet: (formData: FormData) =>
    apiUpload<TaskSetSubmitResponse>("/api/v1/tasksets", formData),
  rebuildTaskSet: (id: string) =>
    apiFetch<TaskSetSubmitResponse>(`/api/v1/tasksets/${id}/rebuild`, {
      method: "POST",
    }),
  deleteTaskSet: (id: string) =>
    apiFetch<void>(`/api/v1/tasksets/${id}`, { method: "DELETE" }),
};
