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
import { Link } from "react-router-dom";

import {
  api,
  type ModelEntry,
  type ProviderConnectionEntry,
} from "../api/client";
import {
  agentReadinessMessage,
  agentServiceModeReady,
  type AgentReadinessLike,
} from "../lib/agentReadiness";
import { Button } from "./Button";
import { Input } from "./Input";
import { Tabs, type TabItem } from "./Tabs";

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
  /** New Batch hides the default model runner unless the user opts into a specific agent. */
  useSpecificAgent?: boolean;
}

export interface AgentModelPickerProps {
  value: AgentModelValue;
  onChange: (v: AgentModelValue) => void;
  /** Disables every input (e.g. while submitting). */
  disabled?: boolean;
  /** Hide the default model runner behind a "Use a specific agent" toggle. */
  specificAgentToggle?: boolean;
  /** Internal default runner used when `specificAgentToggle` is false. */
  defaultAgentName?: string;
  /** Team whose owned/shared provider connections are valid for submission. */
  teamId?: string | null;
}

interface AgentEntry extends AgentReadinessLike {
  name: string;
  aliases?: string[];
  needs_model: boolean;
  kind: "builtin" | "adapter";
  description: string;
  supported_providers: string[];
  supported_model_sources: string[];
  requires_capabilities?: string[];
  provides_capabilities?: string[];
  readiness_status?: "ready" | "unavailable";
  catalog_visibility?: "displayed" | "internal";
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

function preflightOptionSuffix(m: ModelEntry): string {
  if (m.last_preflight_status === "valid") return " (callable)";
  if (m.last_preflight_status === "failed") return " (preflight failed)";
  return "";
}

function providerNamespace(conn: ProviderConnectionEntry | undefined): string {
  if (!conn) return "";
  if (conn.rate_card_provider) return conn.rate_card_provider;
  if (conn.type === "openai-compatible") return "openai";
  return conn.type;
}

function firstSource(agent: AgentEntry | undefined): ModelSource {
  return (agent?.supported_model_sources[0] as ModelSource | undefined) ?? "api";
}

function supportsModelSelection(agent: AgentEntry, value: AgentModelValue): boolean {
  if (!agent.needs_model) return true;
  if (!agent.supported_model_sources.includes(value.source)) return false;
  if (!value.modelProvider) return true;
  return (
    agent.supported_providers.includes("*") ||
    agent.supported_providers.includes(value.modelProvider)
  );
}

export function AgentModelPicker({
  value,
  onChange,
  disabled,
  specificAgentToggle = false,
  defaultAgentName = "direct-completion",
  teamId,
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
    queryKey: ["provider-connections", teamId],
    queryFn: () => api.listProviderConnections(teamId ?? undefined),
    enabled: teamId !== null,
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

  const defaultAgent: AgentEntry | undefined = useMemo(() => {
    if (!agents.data) return undefined;
    const named = agents.data.items.find(
      (a) => a.name === defaultAgentName && agentServiceModeReady(a),
    );
    if (named) return named;
    return agents.data.items.find(
      (a) => agentServiceModeReady(a) && a.needs_model,
    ) ?? agents.data.items.find(agentServiceModeReady);
  }, [agents.data, defaultAgentName]);

  // Default to the first valid agent when the catalog resolves. In
  // New Batch's model-first mode, keep the internal default runner
  // selected while the specific-agent toggle is off.
  useEffect(() => {
    if (!agents.data) return;
    if (specificAgentToggle && value.useSpecificAgent) {
      if (!value.agentName) return;
      const current = agents.data.items.find((a) => a.name === value.agentName);
      if (current && agentServiceModeReady(current)) return;
      onChange({ ...value, agentName: "" });
      return;
    }

    if (specificAgentToggle) {
      if (!defaultAgent) return;
      const current = agents.data.items.find((a) => a.name === value.agentName);
      if (current?.name === defaultAgent.name && agentServiceModeReady(current)) {
        return;
      }
      const nextSource = defaultAgent.supported_model_sources.includes(value.source)
        ? value.source
        : firstSource(defaultAgent);
      onChange({
        ...value,
        agentName: defaultAgent.name,
        source: defaultAgent.needs_model ? nextSource : value.source,
        useSpecificAgent: false,
        hfExecution: value.hfExecution ?? "local-vllm",
      });
      return;
    }

    const current = agents.data.items.find((a) => a.name === value.agentName);
    if (current && agentServiceModeReady(current)) {
      return;
    }
    const fallback = agents.data.items.find(agentServiceModeReady)
      ?? agents.data.items[0];
    if (!fallback) return;
    onChange({
      agentName: fallback.name,
      source: firstSource(fallback),
      modelProvider: "",
      modelName: "",
      providerConnectionId: undefined,
      providerConnectionName: undefined,
      manualModel: false,
      hfExecution: "local-vllm",
    });
  }, [
    agents.data,
    defaultAgent,
    specificAgentToggle,
    value,
    onChange,
  ]);

  const agentList = useMemo(
    () =>
      [...(agents.data?.items ?? [])]
        .filter((a) => a.catalog_visibility !== "internal")
        .sort((a, b) => a.name.localeCompare(b.name)),
    [agents.data],
  );

  const visibleAgentList = useMemo(
    () =>
      specificAgentToggle
        ? agentList.filter((a) => a.name !== defaultAgentName)
        : agentList,
    [agentList, defaultAgentName, specificAgentToggle],
  );

  const compatibilityAgent = selectedAgent ?? defaultAgent;

  // Sources the SELECTED agent actually supports.
  const availableSources: ModelSource[] = useMemo(() => {
    if (!compatibilityAgent) return [];
    const supported = new Set(compatibilityAgent.supported_model_sources);
    return ALL_SOURCES.filter((s) => supported.has(s));
  }, [compatibilityAgent]);
  const activeSource = availableSources.includes(value.source)
    ? value.source
    : availableSources[0] ?? value.source;
  const sourceTabItems: readonly TabItem<ModelSource>[] = useMemo(
    () => {
      const sources =
        availableSources.length > 0 ? availableSources : [activeSource];
      return sources.map((source) => ({
        value: source,
        label: sourceLabel(source),
        disabled,
        title: `Use ${sourceLabel(source)} as the model source for this agent.`,
      }));
    },
    [activeSource, availableSources, disabled],
  );

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
    const allowed = compatibilityAgent?.supported_providers.includes("*")
      ? null
      : new Set(compatibilityAgent?.supported_providers ?? []);
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
    compatibilityAgent,
    value.providerConnectionId,
  ]);

  const fallbackCatalogModels: ModelEntry[] = useMemo(() => {
    const items = (models.data?.items ?? []).filter(
      (m) => !m.provider_connection_id,
    );
    if (!compatibilityAgent || compatibilityAgent.supported_providers.includes("*")) {
      return items;
    }
    const allowed = new Set(compatibilityAgent.supported_providers);
    return items.filter((m) => allowed.has(m.provider));
  }, [models.data, compatibilityAgent]);

  const needsModel = compatibilityAgent?.needs_model ?? true;
  const selectedAgentReady = selectedAgent
    ? agentServiceModeReady(selectedAgent)
    : true;

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

  const selectedCatalogModel = useMemo(() => {
    if (!models.data || !value.modelProvider || !value.modelName) return undefined;
    return [...filteredModels, ...fallbackCatalogModels].find(
      (m) =>
        m.provider === value.modelProvider &&
        m.name === value.modelName &&
        (m.provider_connection_id ?? undefined) ===
          (value.providerConnectionId ?? undefined),
    );
  }, [
    models.data,
    filteredModels,
    fallbackCatalogModels,
    value.modelProvider,
    value.modelName,
    value.providerConnectionId,
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
  const previousTeamIdRef = useRef(teamId);
  useEffect(() => {
    const previousTeamId = previousTeamIdRef.current;
    previousTeamIdRef.current = teamId;
    if (previousTeamId === teamId || !value.providerConnectionId) return;
    setCustomMode(false);
    customCacheRef.current = { provider: "", name: "" };
    onChange({
      ...value,
      modelProvider: "",
      modelName: "",
      providerConnectionId: undefined,
      providerConnectionName: undefined,
      manualModel: false,
    });
  }, [onChange, teamId, value]);
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
          title="Choose which team provider connection should serve model requests."
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

      {connectionList.length === 0 && !providerConnections.isPending ? (
        <div className="rounded border border-slate-200 bg-slate-50 p-4 text-center text-sm">
          <p className="text-slate-600">No provider connections yet.</p>
          <Link
            to="/providers/new?returnTo=/batches/new"
            className="mt-2 inline-block rounded-md bg-accent px-3 py-1.5 text-white hover:bg-accent-hover"
          >
            Create a provider
          </Link>
        </div>
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
            title="Filter discovered models by name."
          />
        </label>
        <label
          className="flex items-center gap-2 pb-2 text-sm text-slate-700"
          title="Include models discovered from the provider that are hidden from the default picker because they are not recommended, not agent-capable, or operator-hidden."
        >
          <input
            type="checkbox"
            checked={showRaw}
            onChange={(e) => setShowRaw(e.target.checked)}
            disabled={disabled}
            className="h-4 w-4 rounded border-slate-300"
          />
          <span>Include hidden/discovered models</span>
        </label>
      </div>

      <label className="block">
        <span className="mb-1 block text-xs font-medium uppercase tracking-wider text-slate-500">
          Model
        </span>
        <select
          aria-label="Model"
          title="Choose a discovered model, or use an ad-hoc model ID for the selected provider connection."
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
                {preflightOptionSuffix(m)}
                {showRaw && m.hidden_reason ? ` (${m.hidden_reason})` : ""}
              </option>
            ))}
          <option value={CUSTOM_MODEL_KEY}>Ad-hoc model ID...</option>
        </select>
      </label>
      {customMode ? (
        <div className="space-y-2">
          {selectedConnection ? (
            <label className="block">
              <span className="mb-1 block text-xs font-medium uppercase tracking-wider text-slate-500">
                Ad-hoc model ID
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
              <p className="mt-1 text-xs text-slate-500">
                Use this for a model ID that exists on the selected provider
                connection but has not been discovered or added to the catalog yet.
              </p>
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
            title="Return to the discovered model dropdown."
          >
            Back to discovered models
          </Button>
        </div>
      ) : null}
      {!customMode && selectedCatalogModel?.last_preflight_status === "failed" ? (
        <div className="rounded border border-red-200 bg-red-50 p-3 text-sm text-red-800">
          <p className="font-medium">This model failed its last preflight.</p>
          {selectedCatalogModel.last_preflight_error_code ? (
            <p className="mt-1 text-xs">
              {selectedCatalogModel.last_preflight_error_code}
            </p>
          ) : null}
          {selectedCatalogModel.last_preflight_error_message ? (
            <p className="mt-1 break-words text-xs text-red-700">
              {selectedCatalogModel.last_preflight_error_message}
            </p>
          ) : null}
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
            title="Choose an operator-configured local server target."
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

  const selectSource = (source: ModelSource): void => {
    onChange({
      ...value,
      source,
      modelProvider: "",
      modelName: "",
      providerConnectionId: undefined,
      providerConnectionName: undefined,
      manualModel: false,
    });
  };

  const renderSourcePanel = (source: ModelSource): JSX.Element | null => {
    if (source === "api") return renderCatalogPanel();
    if (source === "hf") return renderHFPanel();
    if (source === "local-server") return renderLocalServerPanel();
    return null;
  };

  const showAgentSelector = !specificAgentToggle || value.useSpecificAgent === true;

  const chooseAgent = (agentName: string): void => {
    const next = agents.data?.items.find((a) => a.name === agentName);
    if (!next) {
      onChange({
        ...value,
        agentName,
        useSpecificAgent: specificAgentToggle ? true : value.useSpecificAgent,
      });
      return;
    }
    if (!agentServiceModeReady(next)) return;
    const keepModel = specificAgentToggle && supportsModelSelection(next, value);
    const nextSource = next.needs_model && next.supported_model_sources.includes(value.source)
      ? value.source
      : firstSource(next);
    onChange({
      ...value,
      agentName: next.name,
      source: next.needs_model ? nextSource : value.source,
      modelProvider: keepModel ? value.modelProvider : "",
      modelName: keepModel ? value.modelName : "",
      providerConnectionId: keepModel ? value.providerConnectionId : undefined,
      providerConnectionName: keepModel ? value.providerConnectionName : undefined,
      manualModel: keepModel ? value.manualModel : false,
      hfExecution: value.hfExecution ?? "local-vllm",
      localServer: keepModel ? value.localServer : undefined,
      useSpecificAgent: specificAgentToggle ? true : value.useSpecificAgent,
    });
    setCustomMode(false);
  };

  const setSpecificAgentEnabled = (checked: boolean): void => {
    if (checked) {
      onChange({
        ...value,
        agentName: "",
        useSpecificAgent: true,
      });
      return;
    }
    const next = defaultAgent;
    onChange({
      ...value,
      agentName: next?.name ?? defaultAgentName,
      source: next?.needs_model
        ? next.supported_model_sources.includes(value.source)
          ? value.source
          : firstSource(next)
        : value.source,
      useSpecificAgent: false,
      hfExecution: value.hfExecution ?? "local-vllm",
    });
  };

  return (
    <div className="space-y-4">
      {specificAgentToggle ? (
        <label className="flex items-center gap-2 text-sm text-slate-700">
          <input
            type="checkbox"
            checked={value.useSpecificAgent === true}
            onChange={(e) => setSpecificAgentEnabled(e.target.checked)}
            disabled={disabled}
            className="h-4 w-4 rounded border-slate-300"
          />
          <span>Use a specific agent</span>
        </label>
      ) : null}

      {showAgentSelector ? (
        <label className="block">
          <span className="mb-1 block text-xs font-medium uppercase tracking-wider text-slate-500">
            Agent
          </span>
          <select
            aria-label="Agent"
            title="Choose which agent implementation will run each task."
            className={SELECT_CLS}
            value={value.agentName}
            disabled={disabled || agents.isPending}
            onChange={(e) => chooseAgent(e.target.value)}
          >
            {agents.isPending ? (
              <option value="">Loading...</option>
            ) : (
              <>
                {specificAgentToggle ? (
                  <option value="">Choose an agent...</option>
                ) : null}
                {visibleAgentList.map((a) => {
                  const ready = agentServiceModeReady(a);
                  const reason = ready ? a.description : agentReadinessMessage(a);
                  return (
                    <option
                      key={a.name}
                      value={a.name}
                      disabled={!ready}
                      title={reason}
                    >
                      {a.name}{ready ? "" : " (setup needed)"}
                    </option>
                  );
                })}
              </>
            )}
          </select>
          {selectedAgent && selectedAgentReady ? (
            <p className="mt-1 text-xs text-slate-500">
              {selectedAgent.description}
            </p>
          ) : null}
          {selectedAgent && !selectedAgentReady ? (
            <p className="mt-1 text-xs text-amber-700">
              Setup needed: {agentReadinessMessage(selectedAgent)}
            </p>
          ) : null}
        </label>
      ) : null}

      {!selectedAgentReady ? null : needsModel ? (
        <div className="space-y-3">
          <Tabs
            items={sourceTabItems}
            value={activeSource}
            onValueChange={selectSource}
            ariaLabel="Model source"
            hideTabList={availableSources.length <= 1}
            className="space-y-3"
            tabListClassName="inline-flex rounded-lg border border-slate-200 bg-slate-50 p-0.5"
            tabClassName={({ selected }) =>
              "rounded-md px-3 py-1 text-xs font-medium transition-colors " +
              (selected
                ? "bg-white text-slate-900 shadow-sm"
                : "text-slate-600 hover:text-slate-900")
            }
            renderPanel={renderSourcePanel}
          />
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
