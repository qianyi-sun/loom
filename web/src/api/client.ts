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
  submitTrial: (body: { task_id: string; config: Record<string, unknown> }) =>
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
  listBenchmarks: (q: Record<string, string | undefined> = {}) =>
    apiFetch<BenchmarkList>(`/api/v1/benchmarks${qs(q)}`),
  listAgents: () =>
    apiFetch<{
      items: {
        name: string;
        needs_model: boolean;
        kind: "builtin" | "adapter";
        description: string;
      }[];
    }>("/api/v1/agents"),
  listModels: () =>
    apiFetch<{
      items: { provider: string; name: string }[];
    }>("/api/v1/models"),
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
  listBatches: (q: Record<string, string | undefined> = {}) =>
    apiFetch<BatchList>(`/api/v1/batches${qs(q)}`),
  getBatch: (id: string) =>
    apiFetch<BatchDetail>(`/api/v1/batches/${id}`),
  createBatch: (body: {
    name: string;
    description?: string;
    task_filter: Record<string, unknown>;
    trial_config: Record<string, unknown>;
    n_per_task?: number;
  }) =>
    apiFetch<BatchCreate>("/api/v1/batches", {
      method: "POST",
      body: JSON.stringify(body),
    }),
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
