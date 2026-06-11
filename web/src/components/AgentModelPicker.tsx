/**
 * Agent + model picker shared by SubmitTrialModal + NewCampaign.
 *
 * Plan 26 polish based on user feedback ("(builtin) label is ugly";
 * "the model should list available models — if you cannot enumerate
 * all available, have a customizable model name"):
 *   - Agents group into <optgroup> by kind so users see the
 *     "Built-in" vs "Adapter" structure without per-option suffix
 *     labels.
 *   - Models group by provider via <optgroup>, and an explicit
 *     "Custom model…" option at the bottom of the list reveals a
 *     pair of free-text inputs so users can target a model the
 *     rate card hasn't been imported for yet.
 *
 * The pair `(agent_name, agent_model)` is what TrialConfig requires.
 * `agent_model` is either `{provider, name}` or `null` depending on
 * the selected agent's `needs_model`.
 */
import { useQuery } from "@tanstack/react-query";
import { useEffect, useMemo, useRef, useState } from "react";

import { api } from "../api/client";
import { Button } from "./Button";
import { Input } from "./Input";

export interface AgentModelValue {
  agentName: string;
  modelProvider: string;
  modelName: string;
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
}

interface ModelEntry {
  provider: string;
  name: string;
}

const SELECT_CLS =
  "block w-full rounded-lg border border-slate-200 bg-white px-3 py-2 text-sm text-slate-800 disabled:cursor-not-allowed disabled:opacity-60";

const CUSTOM_MODEL_KEY = "__custom__";

function modelKey(m: ModelEntry): string {
  return `${m.provider}|${m.name}`;
}

export function AgentModelPicker({
  value,
  onChange,
  disabled,
}: AgentModelPickerProps): JSX.Element {
  const agents = useQuery({
    queryKey: ["agents"],
    queryFn: () => api.listAgents(),
    staleTime: 5 * 60 * 1000,
  });
  const models = useQuery({
    queryKey: ["models"],
    queryFn: () => api.listModels(),
    staleTime: 5 * 60 * 1000,
  });

  const selectedAgent: AgentEntry | undefined = useMemo(
    () => agents.data?.items.find((a) => a.name === value.agentName),
    [agents.data, value.agentName],
  );

  // If the catalog resolves and the current `agentName` isn't in it,
  // default to the first entry. Wrapped in an effect so we don't
  // set state during render.
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
    onChange({
      agentName: first.name,
      modelProvider: "",
      modelName: "",
    });
  }, [agents.data, value.agentName, onChange]);

  // Group agents by kind for the <optgroup> structure. Order: builtin
  // first (most common), then adapters.
  const agentGroups = useMemo(() => {
    const groups: Record<AgentEntry["kind"], AgentEntry[]> = {
      builtin: [],
      adapter: [],
    };
    for (const a of agents.data?.items ?? []) groups[a.kind].push(a);
    for (const k of ["builtin", "adapter"] as const) {
      groups[k].sort((a, b) => a.name.localeCompare(b.name));
    }
    return groups;
  }, [agents.data]);

  // Same shape for models — grouped by provider.
  const modelGroups = useMemo(() => {
    const grouped: Record<string, ModelEntry[]> = {};
    for (const m of models.data?.items ?? []) {
      (grouped[m.provider] ??= []).push(m);
    }
    return Object.entries(grouped)
      .map(([provider, entries]) => ({
        provider,
        entries: entries.sort((a, b) => a.name.localeCompare(b.name)),
      }))
      .sort((a, b) => a.provider.localeCompare(b.provider));
  }, [models.data]);

  const needsModel = selectedAgent?.needs_model ?? true;

  // Is the currently-set (provider, name) in the catalog?
  const inCatalog = useMemo(() => {
    if (!models.data) return false;
    return models.data.items.some(
      (m) =>
        m.provider === value.modelProvider && m.name === value.modelName,
    );
  }, [models.data, value.modelProvider, value.modelName]);

  // Custom mode: the user picked the "Custom model…" option OR they're
  // editing a (provider, name) pair that isn't (and never was) in the
  // catalog. We persist this in local state so a transient empty
  // catalog response doesn't auto-switch them back to "Choose a
  // model…" while typing.
  const [customMode, setCustomMode] = useState(false);
  // Cache the most-recently-typed custom (provider, name) so switching
  // to a no-model agent (which clears value.modelProvider/Name) and
  // back doesn't make the user retype.
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
    // Auto-enter custom mode if the current value is non-empty but not
    // in the catalog (e.g. server replied with a model the catalog
    // didn't list — defensive).
    if (
      needsModel &&
      value.modelProvider &&
      value.modelName &&
      models.data &&
      !inCatalog
    ) {
      setCustomMode(true);
    }
  }, [needsModel, value.modelProvider, value.modelName, models.data, inCatalog]);

  const enterCustomMode = (): void => {
    setCustomMode(true);
    // If the user hasn't typed anything yet but had a previous custom
    // value cached (from a prior session of the form), restore it.
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
    // Clear so the catalog dropdown reads as "Choose a model…" rather
    // than showing a stale (provider, name) that isn't in the catalog.
    onChange({ ...value, modelProvider: "", modelName: "" });
  };

  const selectedModelKey = customMode
    ? CUSTOM_MODEL_KEY
    : value.modelProvider && value.modelName
      ? modelKey({ provider: value.modelProvider, name: value.modelName })
      : "";

  return (
    <div className="space-y-4">
      <label className="block">
        <span className="mb-1 block text-xs font-medium uppercase tracking-wider text-slate-500">
          Agent
        </span>
        <select
          className={SELECT_CLS}
          value={value.agentName}
          disabled={disabled || agents.isPending}
          onChange={(e) => {
            const next = agents.data?.items.find((a) => a.name === e.target.value);
            // Reset model fields when switching to a no-model agent;
            // otherwise keep whatever the user had typed.
            onChange({
              agentName: e.target.value,
              modelProvider: next?.needs_model ? value.modelProvider : "",
              modelName: next?.needs_model ? value.modelName : "",
            });
            if (!next?.needs_model) setCustomMode(false);
          }}
        >
          {agents.isPending ? (
            <option value="">Loading…</option>
          ) : (
            <>
              {agentGroups.builtin.length > 0 ? (
                <optgroup label="Built-in">
                  {agentGroups.builtin.map((a) => (
                    <option key={a.name} value={a.name}>
                      {a.name}
                    </option>
                  ))}
                </optgroup>
              ) : null}
              {agentGroups.adapter.length > 0 ? (
                <optgroup label="Adapters">
                  {agentGroups.adapter.map((a) => (
                    <option key={a.name} value={a.name}>
                      {a.name}
                    </option>
                  ))}
                </optgroup>
              ) : null}
            </>
          )}
        </select>
        {selectedAgent ? (
          <p className="mt-1 text-xs text-slate-500">
            {selectedAgent.description}
          </p>
        ) : null}
      </label>

      {needsModel ? (
        <div className="space-y-2">
          <label className="block">
            <span className="mb-1 block text-xs font-medium uppercase tracking-wider text-slate-500">
              Model
            </span>
            <select
              className={SELECT_CLS}
              value={selectedModelKey}
              disabled={disabled || models.isPending}
              onChange={(e) => {
                const v = e.target.value;
                if (v === CUSTOM_MODEL_KEY) {
                  enterCustomMode();
                  return;
                }
                setCustomMode(false);
                const [provider, name] = v.split("|");
                onChange({
                  ...value,
                  modelProvider: provider ?? "",
                  modelName: name ?? "",
                });
              }}
            >
              <option value="">Choose a model…</option>
              {modelGroups.map((g) => (
                <optgroup key={g.provider} label={g.provider}>
                  {g.entries.map((m) => (
                    <option key={modelKey(m)} value={modelKey(m)}>
                      {m.name}
                    </option>
                  ))}
                </optgroup>
              ))}
              <option value={CUSTOM_MODEL_KEY}>Custom model…</option>
            </select>
          </label>
          {customMode ? (
            <div className="space-y-2">
              <div className="grid grid-cols-2 gap-2">
                <label className="block">
                  <span className="mb-1 block text-xs font-medium uppercase tracking-wider text-slate-500">
                    Provider
                  </span>
                  <Input
                    value={value.modelProvider}
                    onChange={(e) =>
                      onChange({ ...value, modelProvider: e.target.value })
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
                      onChange({ ...value, modelName: e.target.value })
                    }
                    placeholder="claude-opus-4-7"
                    disabled={disabled}
                  />
                </label>
              </div>
              <Button
                size="sm"
                variant="secondary"
                onClick={leaveCustomMode}
                disabled={disabled}
              >
                Back to catalog
              </Button>
            </div>
          ) : null}
          {models.data?.items.length === 0 && !customMode ? (
            <p className="text-xs text-amber-700">
              No models are registered in the rate-card catalog yet.
              Use "Custom model…" to point at any model the Gateway
              accepts, or ask an admin to import a rate card.
            </p>
          ) : null}
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
 * Returns `{provider, name}` when the agent needs a model AND both
 * fields are filled. Returns null otherwise. The caller is responsible
 * for surfacing a "model is required" error when needs_model is true
 * but the result is null. */
export function buildAgentModel(
  value: AgentModelValue,
  needsModel: boolean,
): { provider: string; name: string } | null {
  if (!needsModel) return null;
  const provider = value.modelProvider.trim();
  const name = value.modelName.trim();
  if (!provider || !name) return null;
  return { provider, name };
}
