/**
 * Agent + model picker shared by SubmitTrialModal + NewBatch.
 *
 * Behavior:
 *   - Agents: one flat alphabetical dropdown. Built-in vs
 *     loom-launcher adapter is implementation detail — they're peers
 *     at runtime, so the picker treats them the same and the UI
 *     doesn't expose the source. The selected agent's description
 *     renders below the select.
 *   - Model source: PR-C adds a tab switcher between
 *     **Catalog** / **HuggingFace** / **Local server**. Tabs are
 *     filtered by the agent's `supported_model_sources` declared in
 *     the catalog (PR-A). If only one source is supported, no tabs
 *     render — the picker just shows that source's panel.
 *     - Catalog: rate-card-backed dropdown grouped by provider,
 *       with "Custom model…" for ad-hoc IDs the catalog hasn't
 *       imported. Provider list further filtered by
 *       `supported_providers` when not "*".
 *     - HuggingFace: model-id text input + sub-toggle between
 *       "Run via local vLLM" (default — worker spawns vLLM on a GPU
 *       box) and "HF Inference Endpoints" (managed/metered).
 *     - Local server: dropdown of operator-configured local servers
 *       (`GET /api/v1/local-servers`) + a model-id input.
 *
 * The pair `(agent_name, agent_model)` is what TrialConfig requires.
 * `agent_model` is either a ModelSpec or null depending on the
 * selected agent's `needs_model`.
 */
import { useQuery } from "@tanstack/react-query";
import { useEffect, useMemo, useRef, useState } from "react";

import {
  api,
  type ModelEntry,
  type ProviderConnectionEntry,
} from "../api/client";
import { Button } from "./Button";
import { Input } from "./Input";

export type ModelSource = "api" | "local-server" | "hf";
export type HFExecution = "local-vllm" | "inference-api";

export interface AgentModelValue {
  agentName: string;
  source: ModelSource;
  modelProvider: string;
  modelName: string;
  providerConnectionId?: string;
  providerConnectionName?: string;
  manualModel?: boolean;
  /** Required when source = "local-server". Name of an operator-configured server. */
  localServer?: string;
  /** Required when source = "hf". local-vllm (default) spawns vLLM; inference-api hits HF. */
  hfExecution?: HFExecution;
}

export interface AgentModelPickerProps {
  value: AgentModelValue;
  onChange: (v: AgentModelValue) => void;
  /** Disables every input (e.g. while submitting). */
  disabled?: boolean;
}

interface AgentEntry {
  name: string;
  needs_model: boolean;
  kind: "builtin" | "adapter";
  description: string;
  supported_providers: string[];
  supported_model_sources: string[];
}

interface LocalServerEntry {
  name: string;
  base_url: string;
  kind: string | null;
  description: string | null;
}

const SELECT_CLS =
  "block w-full rounded-lg border border-slate-200 bg-white px-3 py-2 text-sm text-slate-800 disabled:cursor-not-allowed disabled:opacity-60";

const CUSTOM_MODEL_KEY = "__custom__";
const ALL_SOURCES: ModelSource[] = ["api", "local-server", "hf"];

function modelKey(m: ModelEntry): string {
  return `${m.provider}|${m.name}|${m.provider_connection_id ?? ""}`;
}

function sourceLabel(s: ModelSource): string {
  return s === "api" ? "Provider API" : s === "hf" ? "HuggingFace" : "Local server";
}

function providerNamespace(conn: ProviderConnectionEntry | undefined): string {
  if (!conn) return "";
  if (conn.rate_card_provider) return conn.rate_card_provider;
  if (conn.type === "openai-compatible") return "openai";
  return conn.type;
}

export function AgentModelPicker({
  value,
  onChange,
  disabled,
}: AgentModelPickerProps): JSX.Element {
  const [showRaw, setShowRaw] = useState(false);
  const [modelSearch, setModelSearch] = useState("");
  const agents = useQuery({
    queryKey: ["agents"],
    queryFn: () => api.listAgents(),
    staleTime: 5 * 60 * 1000,
  });
  const models = useQuery({
    queryKey: ["models", showRaw ? "raw" : "default"],
    queryFn: () => api.listModels(showRaw ? "raw" : "default"),
    staleTime: 5 * 60 * 1000,
  });
  const providerConnections = useQuery({
    queryKey: ["provider-connections"],
    queryFn: () => api.listProviderConnections(),
    staleTime: 5 * 60 * 1000,
  });
  const localServers = useQuery({
    queryKey: ["local-servers"],
    queryFn: () => api.listLocalServers(),
    staleTime: 5 * 60 * 1000,
  });

  const selectedAgent: AgentEntry | undefined = useMemo(
    () => agents.data?.items.find((a) => a.name === value.agentName),
    [agents.data, value.agentName],
  );

  // Default to the first agent when the catalog resolves and the
  // current `agentName` isn't in it.
  useEffect(() => {
    if (!agents.data) return;
    if (
      value.agentName &&
      agents.data.items.some((a) => a.name === value.agentName)
    ) {
      return;
    }
    const first = agents.data.items[0];
    if (!first) return;
    const firstSource = (first.supported_model_sources[0] as ModelSource) ?? "api";
    onChange({
      agentName: first.name,
      source: firstSource,
      modelProvider: "",
      modelName: "",
      providerConnectionId: undefined,
      providerConnectionName: undefined,
      manualModel: false,
      hfExecution: "local-vllm",
    });
  }, [agents.data, value.agentName, onChange]);

  const agentList = useMemo(
    () =>
      [...(agents.data?.items ?? [])].sort((a, b) =>
        a.name.localeCompare(b.name),
      ),
    [agents.data],
  );

  // Sources the SELECTED agent actually supports.
  const availableSources: ModelSource[] = useMemo(() => {
    if (!selectedAgent) return [];
    const supported = new Set(selectedAgent.supported_model_sources);
    return ALL_SOURCES.filter((s) => supported.has(s));
  }, [selectedAgent]);

  // When the agent switches, snap the source into the new agent's
  // supported set. Avoids the picker rendering a tab the route would
  // reject at submit time.
  useEffect(() => {
    if (!selectedAgent || !selectedAgent.needs_model) return;
    if (availableSources.includes(value.source)) return;
    const firstSrc = availableSources[0];
    if (!firstSrc) return;
    onChange({ ...value, source: firstSrc });
  }, [selectedAgent, availableSources, value, onChange]);

  const connectionList = useMemo(
    () =>
      [...(providerConnections.data?.items ?? [])].sort((a, b) =>
        a.name.localeCompare(b.name),
      ),
    [providerConnections.data],
  );

  const selectedConnection = useMemo(
    () =>
      connectionList.find((c) => c.id === value.providerConnectionId),
    [connectionList, value.providerConnectionId],
  );

  const filteredModels: ModelEntry[] = useMemo(() => {
    const items = models.data?.items ?? [];
    const q = modelSearch.trim().toLocaleLowerCase();
    const allowed = selectedAgent?.supported_providers.includes("*")
      ? null
      : new Set(selectedAgent?.supported_providers ?? []);
    return items.filter((m) => {
      if (m.provider_connection_id !== value.providerConnectionId) {
        return false;
      }
      if (allowed !== null && !allowed.has(m.provider)) {
        return false;
      }
      if (q && !m.name.toLocaleLowerCase().includes(q)) {
        return false;
      }
      return true;
    });
  }, [
    models.data,
    modelSearch,
    selectedAgent,
    value.providerConnectionId,
  ]);

  const fallbackCatalogModels: ModelEntry[] = useMemo(() => {
    const items = (models.data?.items ?? []).filter(
      (m) => !m.provider_connection_id,
    );
    if (!selectedAgent || selectedAgent.supported_providers.includes("*")) {
      return items;
    }
    const allowed = new Set(selectedAgent.supported_providers);
    return items.filter((m) => allowed.has(m.provider));
  }, [models.data, selectedAgent]);

  const needsModel = selectedAgent?.needs_model ?? true;

  const inCatalog = useMemo(() => {
    if (!models.data) return false;
    return [...filteredModels, ...fallbackCatalogModels].some(
      (m) =>
        m.provider === value.modelProvider && m.name === value.modelName,
    );
  }, [
    models.data,
    filteredModels,
    fallbackCatalogModels,
    value.modelProvider,
    value.modelName,
  ]);

  const [customMode, setCustomMode] = useState(false);
  const customCacheRef = useRef<{ provider: string; name: string }>({
    provider: "",
    name: "",
  });
  useEffect(() => {
    if (customMode) {
      customCacheRef.current = {
        provider: value.modelProvider,
        name: value.modelName,
      };
    }
  }, [customMode, value.modelProvider, value.modelName]);
  useEffect(() => {
    if (
      needsModel &&
      value.source === "api" &&
      value.modelProvider &&
      value.modelName &&
      models.data &&
      !inCatalog
    ) {
      setCustomMode(true);
    }
  }, [needsModel, value.source, value.modelProvider, value.modelName, models.data, inCatalog]);

  // Switching source always resets the model picker fields; cached
  // catalog selections shouldn't leak across tabs.
  useEffect(() => {
    setCustomMode(false);
  }, [value.source]);

  const enterCustomMode = (): void => {
    setCustomMode(true);
    if (!value.modelProvider && !value.modelName) {
      const cached = customCacheRef.current;
      if (cached.provider || cached.name) {
        onChange({
          ...value,
          modelProvider: cached.provider,
          modelName: cached.name,
        });
      }
    }
  };

  const leaveCustomMode = (): void => {
    setCustomMode(false);
    onChange({
      ...value,
      modelProvider: "",
      modelName: "",
      manualModel: false,
    });
  };

  const selectedModelKey = customMode
    ? CUSTOM_MODEL_KEY
    : value.modelProvider && value.modelName
      ? modelKey({
          provider: value.modelProvider,
          name: value.modelName,
          provider_connection_id: value.providerConnectionId,
        })
      : "";

  const renderCatalogPanel = (): JSX.Element => (
    <div className="space-y-3">
      <label className="block">
        <span className="mb-1 block text-xs font-medium uppercase tracking-wider text-slate-500">
          Provider connection
        </span>
        <select
          aria-label="Provider connection"
          className={SELECT_CLS}
          value={value.providerConnectionId ?? ""}
          disabled={disabled || providerConnections.isPending}
          onChange={(e) => {
            const conn = connectionList.find((c) => c.id === e.target.value);
            setCustomMode(false);
            onChange({
              ...value,
              providerConnectionId: conn?.id,
              providerConnectionName: conn?.name,
              modelProvider: providerNamespace(conn),
              modelName: "",
              manualModel: false,
            });
          }}
        >
          <option value="">Choose a connection…</option>
          {connectionList.map((c) => (
            <option key={c.id} value={c.id}>
              {c.name} ({c.type})
            </option>
          ))}
        </select>
      </label>

      {connectionList.length === 0 && fallbackCatalogModels.length > 0 ? (
        <p className="text-xs text-slate-500">
          No provider connections are registered; showing legacy catalog models.
        </p>
      ) : null}

      <div className="flex flex-col gap-2 sm:flex-row sm:items-end">
        <label className="block flex-1">
          <span className="mb-1 block text-xs font-medium uppercase tracking-wider text-slate-500">
            Search models
          </span>
          <Input
            value={modelSearch}
            onChange={(e) => setModelSearch(e.target.value)}
            placeholder="deepseek, qwen, llama"
            disabled={disabled}
          />
        </label>
        <label className="flex items-center gap-2 pb-2 text-sm text-slate-700">
          <input
            type="checkbox"
            checked={showRaw}
            onChange={(e) => setShowRaw(e.target.checked)}
            disabled={disabled}
            className="h-4 w-4 rounded border-slate-300"
          />
          <span>Show raw</span>
        </label>
      </div>

      <label className="block">
        <span className="mb-1 block text-xs font-medium uppercase tracking-wider text-slate-500">
          Model
        </span>
        <select
          aria-label="Model"
          className={SELECT_CLS}
          value={selectedModelKey}
          disabled={
            disabled || models.isPending ||
            (connectionList.length > 0 && !value.providerConnectionId)
          }
          onChange={(e) => {
            const v = e.target.value;
            if (v === CUSTOM_MODEL_KEY) {
              enterCustomMode();
              const provider = providerNamespace(selectedConnection);
              onChange({
                ...value,
                modelProvider: provider,
                modelName: "",
                manualModel: true,
              });
              return;
            }
            setCustomMode(false);
            const selected = [...filteredModels, ...fallbackCatalogModels]
              .find((m) => modelKey(m) === v);
            onChange({
              ...value,
              modelProvider: selected?.provider ?? "",
              modelName: selected?.name ?? "",
              providerConnectionId: selected?.provider_connection_id
                ?? value.providerConnectionId,
              providerConnectionName: selected?.provider_connection_name
                ?? value.providerConnectionName,
              manualModel: false,
            });
          }}
        >
          <option value="">Choose a model…</option>
          {(value.providerConnectionId ? filteredModels : fallbackCatalogModels)
            .map((m) => (
              <option key={modelKey(m)} value={modelKey(m)}>
                {m.name}
                {showRaw && m.hidden_reason ? ` (${m.hidden_reason})` : ""}
              </option>
            ))}
          <option value={CUSTOM_MODEL_KEY}>Manual model…</option>
        </select>
      </label>
      {customMode ? (
        <div className="space-y-2">
          {selectedConnection ? (
            <label className="block">
              <span className="mb-1 block text-xs font-medium uppercase tracking-wider text-slate-500">
                Manual model id
              </span>
              <Input
                value={value.modelName}
                onChange={(e) =>
                  onChange({
                    ...value,
                    modelProvider: providerNamespace(selectedConnection)
                      || value.modelProvider,
                    modelName: e.target.value,
                    manualModel: true,
                  })
                }
                placeholder="manual-vllm-checkpoint"
                disabled={disabled}
              />
            </label>
          ) : (
            <div className="grid grid-cols-2 gap-2">
              <label className="block">
                <span className="mb-1 block text-xs font-medium uppercase tracking-wider text-slate-500">
                  Provider
                </span>
                <Input
                  value={value.modelProvider}
                  onChange={(e) =>
                    onChange({
                      ...value,
                      modelProvider: e.target.value,
                      manualModel: false,
                    })
                  }
                  placeholder="anthropic"
                  disabled={disabled}
                />
              </label>
              <label className="block">
                <span className="mb-1 block text-xs font-medium uppercase tracking-wider text-slate-500">
                  Model name
                </span>
                <Input
                  value={value.modelName}
                  onChange={(e) =>
                    onChange({
                      ...value,
                      modelName: e.target.value,
                      manualModel: false,
                    })
                  }
                  placeholder="claude-opus-4-7"
                  disabled={disabled}
                />
              </label>
            </div>
          )}
          <Button
            size="sm"
            variant="secondary"
            onClick={leaveCustomMode}
            disabled={disabled}
          >
            Back to discovered models
          </Button>
        </div>
      ) : null}
      {value.providerConnectionId && filteredModels.length === 0 && !customMode ? (
        <p className="text-xs text-amber-700">
          No discovered models match this agent and search.
        </p>
      ) : null}
    </div>
  );

  const renderHFPanel = (): JSX.Element => (
    <div className="space-y-3">
      <label className="block">
        <span className="mb-1 block text-xs font-medium uppercase tracking-wider text-slate-500">
          HuggingFace model id
        </span>
        <Input
          value={value.modelName}
          onChange={(e) =>
            onChange({
              ...value,
              modelProvider: "hf",
              modelName: e.target.value,
            })
          }
          placeholder="meta-llama/Llama-3-8B-Instruct"
          disabled={disabled}
        />
        <p className="mt-1 text-xs text-slate-500">
          Any model id on HuggingFace Hub.
        </p>
      </label>
      <fieldset className="space-y-2">
        <legend className="text-xs font-medium uppercase tracking-wider text-slate-500">
          Execution
        </legend>
        <label className="flex items-start gap-2 text-sm text-slate-700">
          <input
            type="radio"
            checked={(value.hfExecution ?? "local-vllm") === "local-vllm"}
            onChange={() =>
              onChange({ ...value, hfExecution: "local-vllm" })
            }
            disabled={disabled}
            className="mt-1"
          />
          <span>
            <strong>Run via local vLLM</strong> (default) — worker spawns
            vLLM on a GPU box and serves the model locally.
          </span>
        </label>
        <label className="flex items-start gap-2 text-sm text-slate-700">
          <input
            type="radio"
            checked={value.hfExecution === "inference-api"}
            onChange={() =>
              onChange({ ...value, hfExecution: "inference-api" })
            }
            disabled={disabled}
            className="mt-1"
          />
          <span>
            <strong>HuggingFace Inference Endpoints</strong> — managed by
            HF, metered. Requires <code className="rounded bg-slate-100 px-1 py-0.5 font-mono text-xs">HF_TOKEN</code> in the
            gateway.
          </span>
        </label>
      </fieldset>
    </div>
  );

  const renderLocalServerPanel = (): JSX.Element => {
    const items = localServers.data?.items ?? [];
    return (
      <div className="space-y-3">
        <label className="block">
          <span className="mb-1 block text-xs font-medium uppercase tracking-wider text-slate-500">
            Local server
          </span>
          <select
            className={SELECT_CLS}
            value={value.localServer ?? ""}
            disabled={disabled || localServers.isPending}
            onChange={(e) =>
              onChange({ ...value, localServer: e.target.value })
            }
          >
            <option value="">Choose a server…</option>
            {items.map((s: LocalServerEntry) => (
              <option key={s.name} value={s.name}>
                {s.name}
                {s.kind ? ` (${s.kind})` : ""}
              </option>
            ))}
          </select>
          {items.length === 0 && !localServers.isPending ? (
            <p className="mt-1 text-xs text-amber-700">
              No local servers are configured. Operator sets{" "}
              <code className="rounded bg-slate-100 px-1 py-0.5 font-mono text-xs">
                LOOM_SVC_LOCAL_SERVERS_JSON
              </code>{" "}
              to populate this list.
            </p>
          ) : null}
        </label>
        <label className="block">
          <span className="mb-1 block text-xs font-medium uppercase tracking-wider text-slate-500">
            Model id
          </span>
          <Input
            value={value.modelName}
            onChange={(e) =>
              onChange({
                ...value,
                modelProvider: "local",
                modelName: e.target.value,
              })
            }
            placeholder="llama3"
            disabled={disabled}
          />
          <p className="mt-1 text-xs text-slate-500">
            Model identifier the local server recognises (the
            <code className="mx-1 rounded bg-slate-100 px-1 py-0.5 font-mono text-xs">model</code>
            field it expects in OpenAI-compatible requests).
          </p>
        </label>
      </div>
    );
  };

  return (
    <div className="space-y-4">
      <label className="block">
        <span className="mb-1 block text-xs font-medium uppercase tracking-wider text-slate-500">
          Agent
        </span>
        <select
          aria-label="Agent"
          className={SELECT_CLS}
          value={value.agentName}
          disabled={disabled || agents.isPending}
          onChange={(e) => {
            const next = agents.data?.items.find((a) => a.name === e.target.value);
            if (!next) {
              onChange({ ...value, agentName: e.target.value });
              return;
            }
            const nextSource =
              (next.supported_model_sources[0] as ModelSource) ?? "api";
            onChange({
              agentName: e.target.value,
              source: next.needs_model ? nextSource : value.source,
              modelProvider: "",
              modelName: "",
              providerConnectionId: undefined,
              providerConnectionName: undefined,
              manualModel: false,
              hfExecution: "local-vllm",
              localServer: undefined,
            });
            setCustomMode(false);
          }}
        >
          {agents.isPending ? (
            <option value="">Loading…</option>
          ) : (
            agentList.map((a) => (
              <option key={a.name} value={a.name}>
                {a.name}
              </option>
            ))
          )}
        </select>
        {selectedAgent ? (
          <p className="mt-1 text-xs text-slate-500">
            {selectedAgent.description}
          </p>
        ) : null}
      </label>

      {needsModel ? (
        <div className="space-y-3">
          {availableSources.length > 1 ? (
            <div
              role="tablist"
              className="inline-flex rounded-lg border border-slate-200 bg-slate-50 p-0.5"
            >
              {availableSources.map((s) => {
                const active = value.source === s;
                return (
                  <button
                    key={s}
                    type="button"
                    role="tab"
                    aria-selected={active}
                    onClick={() =>
                      onChange({
                        ...value,
                        source: s,
                        modelProvider: "",
                        modelName: "",
                        providerConnectionId: undefined,
                        providerConnectionName: undefined,
                        manualModel: false,
                      })
                    }
                    disabled={disabled}
                    className={
                      "rounded-md px-3 py-1 text-xs font-medium transition-colors " +
                      (active
                        ? "bg-white text-slate-900 shadow-sm"
                        : "text-slate-600 hover:text-slate-900")
                    }
                  >
                    {sourceLabel(s)}
                  </button>
                );
              })}
            </div>
          ) : null}

          {value.source === "api" ? renderCatalogPanel() : null}
          {value.source === "hf" ? renderHFPanel() : null}
          {value.source === "local-server" ? renderLocalServerPanel() : null}
        </div>
      ) : (
        <p className="text-xs text-slate-500">
          The <code className="rounded bg-slate-100 px-1 py-0.5 font-mono">{value.agentName}</code> agent
          doesn't call an LLM, so no model is needed.
        </p>
      )}
    </div>
  );
}

/** Build the TrialConfig.agent_model payload from the picker's value.
 *
 * Returns a ModelSpec-shaped object including the new source /
 * local_server / hf_execution discriminator fields (PR-A). Returns
 * null when the agent doesn't take a model or when required fields
 * are blank (the caller surfaces the "model is required" error). */
export function buildAgentModel(
  value: AgentModelValue,
  needsModel: boolean,
): {
  provider: string;
  name: string;
  source: ModelSource;
  local_server?: string;
  hf_execution?: HFExecution;
} | null {
  if (!needsModel) return null;
  const name = value.modelName.trim();
  if (!name) return null;
  if (value.source === "api") {
    const provider = value.modelProvider.trim();
    if (!provider) return null;
    return { provider, name, source: "api" };
  }
  if (value.source === "hf") {
    return {
      provider: "hf",
      name,
      source: "hf",
      hf_execution: value.hfExecution ?? "local-vllm",
    };
  }
  // source === "local-server"
  const ls = value.localServer?.trim();
  if (!ls) return null;
  return {
    provider: value.modelProvider.trim() || "local",
    name,
    source: "local-server",
    local_server: ls,
  };
}

export interface ProviderOverride {
  provider_connection_id: string;
  provider_model_id: string;
  manual_model: boolean;
}

export function buildProviderOverride(
  value: AgentModelValue,
  needsModel: boolean,
): ProviderOverride | null {
  if (!needsModel || value.source !== "api") return null;
  const connectionId = value.providerConnectionId?.trim();
  const modelId = value.modelName.trim();
  if (!connectionId || !modelId) return null;
  return {
    provider_connection_id: connectionId,
    provider_model_id: modelId,
    manual_model: value.manualModel === true,
  };
}
