/**
 * Agent + model picker shared by SubmitTrialModal + NewCampaign.
 *
 * Populates from server-side catalogs (`/agents`, `/models`) so users
 * pick from known values instead of typing a free-form string. The
 * model dropdown is hidden when the selected agent doesn't need an
 * LLM (oracle, in-box runtimes) and required-but-disabled while the
 * catalog loads so the form can't submit a half-filled state.
 *
 * The pair `(agent_name, agent_model)` is what TrialConfig requires.
 * `agent_model` is either `{provider, name}` or `null` depending on
 * the selected agent's `needs_model`.
 */
import { useQuery } from "@tanstack/react-query";
import { useEffect, useMemo } from "react";

import { api } from "../api/client";

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

  const selectedAgent: AgentEntry | undefined = useMemo(() => {
    return agents.data?.items.find((a) => a.name === value.agentName);
  }, [agents.data, value.agentName]);

  // If the agent list resolves and `value.agentName` isn't in it yet
  // (initial empty / hand-cleared), default to the first entry so the
  // form is always submittable. Wrapped in an effect to avoid setting
  // state during render.
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
      // Reset model when the agent kind changes.
      modelProvider: "",
      modelName: "",
    });
  }, [agents.data, value.agentName, onChange]);

  const needsModel = selectedAgent?.needs_model ?? true;

  const groupedModels: { provider: string; entries: ModelEntry[] }[] =
    useMemo(() => {
      const out: Record<string, ModelEntry[]> = {};
      for (const m of models.data?.items ?? []) {
        (out[m.provider] ??= []).push(m);
      }
      return Object.entries(out).map(([provider, entries]) => ({
        provider,
        entries: entries.sort((a, b) => a.name.localeCompare(b.name)),
      }));
    }, [models.data]);

  const modelKey = (m: ModelEntry): string => `${m.provider}|${m.name}`;
  const selectedModelKey =
    value.modelProvider && value.modelName
      ? `${value.modelProvider}|${value.modelName}`
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
            onChange({
              agentName: e.target.value,
              modelProvider: next?.needs_model ? value.modelProvider : "",
              modelName: next?.needs_model ? value.modelName : "",
            });
          }}
        >
          {agents.isPending ? (
            <option value="">Loading…</option>
          ) : (
            (agents.data?.items ?? []).map((a) => (
              <option key={a.name} value={a.name}>
                {a.name} ({a.kind})
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
        <label className="block">
          <span className="mb-1 block text-xs font-medium uppercase tracking-wider text-slate-500">
            Model
          </span>
          <select
            className={SELECT_CLS}
            value={selectedModelKey}
            disabled={disabled || models.isPending}
            onChange={(e) => {
              const [provider, name] = e.target.value.split("|");
              onChange({
                ...value,
                modelProvider: provider ?? "",
                modelName: name ?? "",
              });
            }}
          >
            <option value="">Choose a model…</option>
            {groupedModels.map((g) => (
              <optgroup key={g.provider} label={g.provider}>
                {g.entries.map((m) => (
                  <option key={modelKey(m)} value={modelKey(m)}>
                    {m.name}
                  </option>
                ))}
              </optgroup>
            ))}
          </select>
          {models.data?.items.length === 0 ? (
            <p className="mt-1 text-xs text-amber-700">
              No models registered in the rate-card catalog yet. Ask an
              admin to import a rate card before submitting a trial that
              needs an LLM.
            </p>
          ) : null}
        </label>
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
 * fields are filled. Returns null otherwise. The caller is
 * responsible for surfacing a "model is required" error when the
 * agent's needs_model is true but the result is null. */
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
