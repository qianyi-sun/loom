import { apiFetch, qs } from "./core";

export interface Backend {
  name: string;
  description: string;
  /** True when at least one live worker advertises this backend. */
  available: boolean;
  /** True when a fresh, healthy autoscaler policy or service-execution target
   * can start compatible capacity. This is planning headroom, not immediately
   * executable capacity. */
  cold_start_available: boolean;
  /** Pools contributing scale-from-zero authority for this backend. */
  cold_start_pools: string[];
}

export interface AgentVersionEntry {
  agent_version: string;
  harbor_version: string;
  loom_bridge_revision: string;
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
  /** Only a "rejected" failure blocks batch submission (#948). */
  last_preflight_failure_kind?: "rejected" | "inconclusive" | null;
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
  /** Only a "rejected" failure blocks batch submission (#948). */
  last_preflight_failure_kind?: "rejected" | "inconclusive" | null;
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

export const providersApi = {
  listAgents: () =>
    apiFetch<{
      items: {
        name: string;
        aliases?: string[];
        versions?: AgentVersionEntry[];
        needs_model: boolean;
        kind: "builtin" | "adapter";
        description: string;
        /** PR-A: provider whitelist. ["*"] = any provider the gateway routes. */
        supported_providers: string[];
        /** PR-A: subset of ["api","local-server","hf"]. Empty when needs_model=false. */
        supported_model_sources: string[];
        requires_capabilities?: string[];
        provides_capabilities?: string[];
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
    apiFetch<{ items: ModelEntry[] }>(`/api/v1/models${qs({ view })}`),
  listProviderConnections: (teamId?: string) =>
    apiFetch<{ items: ProviderConnectionEntry[] }>(`/api/v1/provider-connections${qs({ team_id: teamId })}`),
  getProviderConnection: (id: string) =>
    apiFetch<ProviderConnectionDetail>(`/api/v1/provider-connections/${id}`),
  createProviderConnection: (payload: ProviderConnectionCreateBody) =>
    apiFetch<ProviderConnectionDetail>("/api/v1/provider-connections", {
      method: "POST",
      body: JSON.stringify(payload),
    }),
  updateProviderConnection: (id: string, patch: ProviderConnectionPatchBody) =>
    apiFetch<ProviderConnectionDetail>(`/api/v1/provider-connections/${id}`, {
      method: "PATCH",
      body: JSON.stringify(patch),
    }),
  deleteProviderConnection: (id: string) =>
    apiFetch<void>(`/api/v1/provider-connections/${id}`, { method: "DELETE" }),
  testProviderConnection: (id: string) =>
    apiFetch<ProviderConnectionTestResult>(`/api/v1/provider-connections/${id}/test`, { method: "POST" }),
  listProviderConnectionModels: (id: string) =>
    apiFetch<{ items: ProviderConnectionModelEntry[] }>(`/api/v1/provider-connections/${id}/models`),
  addProviderConnectionModel: (connectionId: string, body: { model_id: string }) =>
    apiFetch<ProviderConnectionModelEntry>(`/api/v1/provider-connections/${connectionId}/models`, {
      method: "POST",
      body: JSON.stringify(body),
    }),
  refreshProviderConnectionModels: (id: string) =>
    apiFetch<ProviderConnectionModelsRefreshResult>(`/api/v1/provider-connections/${id}/models/refresh`, {
      method: "POST",
    }),
  preflightProviderConnectionModel: (id: string, modelId: string) =>
    apiFetch<ProviderConnectionModelEntry>(
      `/api/v1/provider-connections/${id}/models/${encodeURIComponent(modelId)}/preflight`,
      { method: "POST" },
    ),
  hideProviderConnectionModel: (id: string, modelId: string) =>
    apiFetch<void>(`/api/v1/provider-connections/${id}/models/${encodeURIComponent(modelId)}/hide`, {
      method: "POST",
    }),
  unhideProviderConnectionModel: (id: string, modelId: string) =>
    apiFetch<void>(`/api/v1/provider-connections/${id}/models/${encodeURIComponent(modelId)}/unhide`, {
      method: "POST",
    }),
  listBackends: () => apiFetch<{ items: Backend[] }>("/api/v1/backends"),
};
