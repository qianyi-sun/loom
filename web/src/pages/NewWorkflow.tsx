/**
 * New workflow form — admin-only. Pins every config field at create
 * time: benchmark, agent (name + version), model (provider/name),
 * backend, concurrency, task_filter, trial_config.
 *
 * The benchmark picker is populated from the Benchmarks API so the
 * admin can't fat-finger an unknown slug. Backend picker uses the
 * same canonical list as the rest of the platform.
 */
import { useMutation, useQuery } from "@tanstack/react-query";
import { useState } from "react";
import { useNavigate } from "react-router-dom";

import { api } from "../api/client";
import { useAuth } from "../auth/useAuth";
import { Button } from "../components/Button";
import { Card } from "../components/Card";
import EmptyState from "../components/EmptyState";
import ErrorState from "../components/ErrorState";
import { Input, Textarea } from "../components/Input";

const BACKENDS = ["docker", "fake", "daytona", "modal"] as const;

const DEFAULT_FILTER = `{
  "benchmark_id": ""
}`;
const DEFAULT_TRIAL_CONFIG = `{}`;

const SELECT_CLASSES =
  "block w-full rounded-lg border border-slate-200 bg-white px-3 py-2 text-sm text-slate-800";

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

export default function NewWorkflow(): JSX.Element {
  const { isAdmin } = useAuth();
  const navigate = useNavigate();
  const [name, setName] = useState("");
  const [description, setDescription] = useState("");
  const [benchmarkId, setBenchmarkId] = useState("");
  const [agentName, setAgentName] = useState("oracle");
  const [agentVersion, setAgentVersion] = useState("2.1.0");
  const [modelProvider, setModelProvider] = useState("anthropic");
  const [modelName, setModelName] = useState("claude-opus-4-7");
  const [backend, setBackend] = useState<(typeof BACKENDS)[number]>("docker");
  const [concurrency, setConcurrency] = useState(1);
  const [filterText, setFilterText] = useState(DEFAULT_FILTER);
  const [configText, setConfigText] = useState(DEFAULT_TRIAL_CONFIG);
  const [localError, setLocalError] = useState<string | null>(null);

  const benchmarks = useQuery({
    queryKey: ["benchmarks-for-workflow"],
    queryFn: () => api.listBenchmarks({ limit: "200" }),
  });

  const create = useMutation({
    mutationFn: (body: Parameters<typeof api.createWorkflow>[0]) =>
      api.createWorkflow(body),
    onSuccess: (res) => navigate(`/workflows/${res.id}`),
  });

  if (!isAdmin) {
    return (
      <Card>
        <Card.Body>
          <EmptyState
            label="Admin only."
            hint="Only tokens with the `admin:workflows` scope can publish workflows."
          />
        </Card.Body>
      </Card>
    );
  }

  const submit = (): void => {
    setLocalError(null);
    if (!benchmarkId) {
      setLocalError("benchmark_id is required");
      return;
    }
    const filter = tryParse(filterText);
    if (!filter.ok) {
      setLocalError(`task_filter: ${filter.error}`);
      return;
    }
    const config = tryParse(configText);
    if (!config.ok) {
      setLocalError(`trial_config: ${config.error}`);
      return;
    }
    create.mutate({
      name: name.trim(),
      description: description.trim() || null,
      benchmark_id: benchmarkId,
      agent_name: agentName,
      agent_version: agentVersion,
      model_provider: modelProvider,
      model_name: modelName,
      backend,
      concurrency,
      task_filter: filter.value,
      trial_config: config.value,
    });
  };

  return (
    <div className="space-y-6">
      <header>
        <h1 className="text-2xl font-bold text-slate-900">New workflow</h1>
        <p className="mt-1 text-sm text-slate-500">
          Save a fully-pinned recipe. Anyone can launch it; only admins
          can edit or delete.
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
                placeholder="e.g. humaneval — claude-opus-4-7 docker"
              />
            </label>
            <label className="block">
              <FieldLabel hint="optional">Description</FieldLabel>
              <Input
                value={description}
                onChange={(e) => setDescription(e.target.value)}
                placeholder="What is this recipe for?"
              />
            </label>
          </div>

          <div className="grid grid-cols-1 gap-4 md:grid-cols-3">
            <label className="block">
              <FieldLabel>Benchmark</FieldLabel>
              <select
                value={benchmarkId}
                onChange={(e) => setBenchmarkId(e.target.value)}
                className={SELECT_CLASSES}
              >
                <option value="">— pick one —</option>
                {benchmarks.data?.items.map((b) => (
                  <option key={b.id} value={b.id}>
                    {b.id} — {b.display_name}
                  </option>
                ))}
              </select>
            </label>
            <label className="block">
              <FieldLabel>Agent name</FieldLabel>
              <Input
                value={agentName}
                onChange={(e) => setAgentName(e.target.value)}
                placeholder="claude-code"
              />
            </label>
            <label className="block">
              <FieldLabel hint="pinned, no `latest`">Agent version</FieldLabel>
              <Input
                value={agentVersion}
                onChange={(e) => setAgentVersion(e.target.value)}
                placeholder="2.1.0"
              />
            </label>
          </div>

          <div className="grid grid-cols-1 gap-4 md:grid-cols-4">
            <label className="block">
              <FieldLabel>Model provider</FieldLabel>
              <Input
                value={modelProvider}
                onChange={(e) => setModelProvider(e.target.value)}
                placeholder="anthropic"
              />
            </label>
            <label className="block">
              <FieldLabel>Model name</FieldLabel>
              <Input
                value={modelName}
                onChange={(e) => setModelName(e.target.value)}
                placeholder="claude-opus-4-7"
              />
            </label>
            <label className="block">
              <FieldLabel>Backend</FieldLabel>
              <select
                value={backend}
                onChange={(e) =>
                  setBackend(e.target.value as (typeof BACKENDS)[number])
                }
                className={SELECT_CLASSES}
              >
                {BACKENDS.map((b) => (
                  <option key={b} value={b}>
                    {b}
                  </option>
                ))}
              </select>
            </label>
            <label className="block">
              <FieldLabel>Concurrency</FieldLabel>
              <Input
                type="number"
                min={1}
                max={64}
                value={concurrency}
                onChange={(e) => setConcurrency(Number(e.target.value) || 1)}
              />
            </label>
          </div>

          <div className="grid grid-cols-1 gap-4 lg:grid-cols-2">
            <label className="block">
              <FieldLabel hint="optional; defaults to all tasks in the benchmark">
                task_filter (JSON)
              </FieldLabel>
              <Textarea
                value={filterText}
                onChange={(e) => setFilterText(e.target.value)}
                rows={6}
              />
            </label>
            <label className="block">
              <FieldLabel hint="forwarded to TrialConfig">
                trial_config (JSON)
              </FieldLabel>
              <Textarea
                value={configText}
                onChange={(e) => setConfigText(e.target.value)}
                rows={6}
              />
            </label>
          </div>

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
              {create.isPending ? "Creating…" : "Create workflow"}
            </Button>
          </div>
        </Card.Footer>
      </Card>
    </div>
  );
}
