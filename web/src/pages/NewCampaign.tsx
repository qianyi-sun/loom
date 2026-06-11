/**
 * Create a new campaign. Plan 23 reshape:
 *
 *   - Agent + model are required at submission, so they appear as
 *     structured fields (not buried inside a trial_config JSON blob).
 *   - `n_per_task` is a campaign-level integer for n-sampling fan-out.
 *   - task_filter stays a JSON textarea — it's a free-form operator
 *     surface (license / task_ids / benchmark_id) and the existing
 *     user base already knows the schema.
 *   - The legacy trial_config JSON textarea is exposed as an
 *     "Advanced" disclosure for the remaining timeout / retry /
 *     skip_verifier knobs; agent_name + agent_model are injected
 *     into it at submit time and override any duplicates in the
 *     advanced JSON.
 */

import { useMutation } from "@tanstack/react-query";
import { useState } from "react";
import { useNavigate } from "react-router-dom";

import { api } from "../api/client";
import { Button } from "../components/Button";
import { Card } from "../components/Card";
import ErrorState from "../components/ErrorState";
import { Input, Textarea } from "../components/Input";

const DEFAULT_FILTER = `{
  "license": "MIT"
}`;

function tryParse(input: string): {
  ok: true;
  value: Record<string, unknown>;
} | { ok: false; error: string } {
  try {
    const parsed = JSON.parse(input);
    if (typeof parsed !== "object" || parsed === null || Array.isArray(parsed)) {
      return { ok: false, error: "expected a JSON object" };
    }
    return { ok: true, value: parsed as Record<string, unknown> };
  } catch (e) {
    return {
      ok: false,
      error: e instanceof Error ? e.message : String(e),
    };
  }
}

function buildAgentModel(
  provider: string,
  name: string,
): { ok: true; value: { provider: string; name: string } | null }
  | { ok: false; error: string } {
  const p = provider.trim();
  const n = name.trim();
  if (!p && !n) return { ok: true, value: null };
  if (!p || !n) {
    return {
      ok: false,
      error:
        "Model provider and model name must both be set, or both left blank.",
    };
  }
  return { ok: true, value: { provider: p, name: n } };
}

function FieldLabel({
  children,
  hint,
}: {
  children: React.ReactNode;
  hint?: React.ReactNode;
}): JSX.Element {
  return (
    <div className="mb-1 flex items-baseline justify-between">
      <span className="text-xs font-medium uppercase tracking-wider text-slate-500">
        {children}
      </span>
      {hint ? <span className="text-xs text-slate-400">{hint}</span> : null}
    </div>
  );
}

export default function NewCampaign(): JSX.Element {
  const [name, setName] = useState("");
  const [description, setDescription] = useState("");
  const [filterText, setFilterText] = useState(DEFAULT_FILTER);
  const [agentName, setAgentName] = useState("oracle");
  const [modelProvider, setModelProvider] = useState("");
  const [modelName, setModelName] = useState("");
  const [nPerTask, setNPerTask] = useState("1");
  const [advancedOpen, setAdvancedOpen] = useState(false);
  const [advancedText, setAdvancedText] = useState("{}");
  const [localError, setLocalError] = useState<string | null>(null);

  const navigate = useNavigate();
  const create = useMutation({
    mutationFn: (body: {
      name: string;
      description?: string;
      task_filter: Record<string, unknown>;
      trial_config: Record<string, unknown>;
      n_per_task: number;
    }) => api.createCampaign(body),
    onSuccess: (res) => {
      navigate(`/campaigns/${res.campaign_id}`);
    },
  });

  const submit = (): void => {
    setLocalError(null);
    const filter = tryParse(filterText);
    if (!filter.ok) {
      setLocalError(`task_filter: ${filter.error}`);
      return;
    }
    const trimmedAgent = agentName.trim();
    if (!trimmedAgent) {
      setLocalError("Agent is required.");
      return;
    }
    const agentModel = buildAgentModel(modelProvider, modelName);
    if (!agentModel.ok) {
      setLocalError(agentModel.error);
      return;
    }
    const n = Number.parseInt(nPerTask, 10);
    if (!Number.isFinite(n) || n < 1 || n > 100) {
      setLocalError("n_per_task must be an integer between 1 and 100.");
      return;
    }
    let extraConfig: Record<string, unknown> = {};
    if (advancedOpen) {
      const advanced = tryParse(advancedText);
      if (!advanced.ok) {
        setLocalError(`advanced trial_config: ${advanced.error}`);
        return;
      }
      extraConfig = advanced.value;
    }
    create.mutate({
      name: name.trim(),
      description: description.trim() || undefined,
      task_filter: filter.value,
      // Plan 23: agent_name + agent_model are required on TrialConfig;
      // they override any duplicates in the advanced JSON.
      trial_config: {
        ...extraConfig,
        agent_name: trimmedAgent,
        agent_model: agentModel.value,
      },
      n_per_task: n,
    });
  };

  return (
    <div className="space-y-6">
      <header>
        <h1 className="text-2xl font-bold text-slate-900">New campaign</h1>
        <p className="mt-1 text-sm text-slate-500">
          Submit N trials in one batch — pick the task list with the
          filter, the agent + model that runs each, and how many
          samples to draw per task.
        </p>
      </header>

      <Card>
        <Card.Body className="space-y-5">
          <div className="grid grid-cols-1 gap-4 md:grid-cols-2">
            <label className="block">
              <FieldLabel>Name</FieldLabel>
              <Input
                value={name}
                onChange={(e) => setName(e.target.value)}
                placeholder="e.g. MIT slate — run 7"
              />
            </label>
            <label className="block">
              <FieldLabel hint="optional">Description</FieldLabel>
              <Input
                value={description}
                onChange={(e) => setDescription(e.target.value)}
                placeholder="What's this campaign testing?"
              />
            </label>
          </div>

          <label className="block">
            <FieldLabel hint="keys: license, task_ids, benchmark_id">
              Task filter (JSON)
            </FieldLabel>
            <Textarea
              value={filterText}
              onChange={(e) => setFilterText(e.target.value)}
              rows={6}
            />
          </label>

          <div className="grid grid-cols-1 gap-4 md:grid-cols-3">
            <label className="block">
              <FieldLabel>Agent</FieldLabel>
              <Input
                value={agentName}
                onChange={(e) => setAgentName(e.target.value)}
                placeholder="oracle"
              />
            </label>
            <label className="block">
              <FieldLabel hint="optional">Model provider</FieldLabel>
              <Input
                value={modelProvider}
                onChange={(e) => setModelProvider(e.target.value)}
                placeholder="anthropic"
              />
            </label>
            <label className="block">
              <FieldLabel hint="optional">Model name</FieldLabel>
              <Input
                value={modelName}
                onChange={(e) => setModelName(e.target.value)}
                placeholder="claude-opus-4-7"
              />
            </label>
          </div>
          <p className="-mt-2 text-xs text-slate-500">
            Leave both model fields blank for agents that don't call an LLM
            (oracle, in-box runtimes).
          </p>

          <label className="block max-w-xs">
            <FieldLabel hint="1 – 100">Samples per task</FieldLabel>
            <Input
              type="number"
              min={1}
              max={100}
              value={nPerTask}
              onChange={(e) => setNPerTask(e.target.value)}
            />
          </label>

          <details
            open={advancedOpen}
            onToggle={(e) =>
              setAdvancedOpen((e.target as HTMLDetailsElement).open)
            }
            className="rounded-lg border border-slate-200 bg-slate-50/40 px-4 py-3"
          >
            <summary className="cursor-pointer text-xs font-medium uppercase tracking-wider text-slate-500">
              Advanced — extra trial_config (JSON)
            </summary>
            <Textarea
              value={advancedText}
              onChange={(e) => setAdvancedText(e.target.value)}
              rows={6}
              className="mt-3"
            />
            <p className="mt-2 text-xs text-slate-500">
              Timeouts, retry, skip_verifier, etc. agent_name +
              agent_model are taken from the fields above and override
              anything set here.
            </p>
          </details>

          {localError ? (
            <div className="rounded-xl border border-red-200 bg-red-50 px-4 py-3 text-sm text-red-700">
              {localError}
            </div>
          ) : null}
          {create.isError ? <ErrorState error={create.error} /> : null}
        </Card.Body>
        <Card.Footer>
          <div className="flex items-center justify-end">
            <Button
              variant="primary"
              onClick={submit}
              disabled={!name.trim() || create.isPending}
            >
              {create.isPending ? "Creating…" : "Create campaign"}
            </Button>
          </div>
        </Card.Footer>
      </Card>
    </div>
  );
}
