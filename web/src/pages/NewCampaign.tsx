/**
 * Create a new campaign. Plan 26 redesign based on user feedback +
 * post-implementation critic review:
 *
 *   - Structured task picker (benchmark dropdown + id substring +
 *     live "N tasks match" preview). Replaces the JSON textarea.
 *     Search is debounced; submit blocks on either an empty filter
 *     (would fan out to the whole catalog) OR a filter that matched
 *     zero tasks OR a count we couldn't compute.
 *   - Hard fan-out cap. The user must explicitly confirm a campaign
 *     that would launch more than CONFIRM_THRESHOLD trials.
 *   - Agent + model via the shared AgentModelPicker (catalog-first
 *     with a "Custom model…" fallback that preserves typed values
 *     across agent switches).
 *   - "Samples per task" clamped to 1..100 on every keystroke AND on
 *     blur, so the input never shows an out-of-range value.
 *   - Advanced disclosure exposes the most-asked TrialConfig knobs
 *     (skip_verifier, retry on transient, agent_timeout_multiplier)
 *     as structured fields with inline help — not a JSON pasteboard.
 */

import { useMutation, useQuery } from "@tanstack/react-query";
import { useState } from "react";
import { useNavigate } from "react-router-dom";

import { api } from "../api/client";
import {
  AgentModelPicker,
  buildAgentModel,
  type AgentModelValue,
} from "../components/AgentModelPicker";
import { Button } from "../components/Button";
import { Card } from "../components/Card";
import ErrorState from "../components/ErrorState";
import { Input } from "../components/Input";
import { useDebouncedValue } from "../hooks/useDebouncedValue";

interface BenchmarkItem {
  id: string;
  display_name?: string;
}

const FAN_OUT_CONFIRM_THRESHOLD = 200;

function clampInRange(raw: string, min: number, max: number): string {
  if (raw === "") return raw;
  const n = Number.parseInt(raw, 10);
  if (!Number.isFinite(n)) return String(min);
  if (n < min) return String(min);
  if (n > max) return String(max);
  return String(n);
}

const INITIAL_PICKER: AgentModelValue = {
  agentName: "",
  modelProvider: "",
  modelName: "",
};

export default function NewCampaign(): JSX.Element {
  const [name, setName] = useState("");
  const [description, setDescription] = useState("");
  const [benchmark, setBenchmark] = useState("");
  const [taskQuery, setTaskQuery] = useState("");
  const debouncedTaskQuery = useDebouncedValue(taskQuery, 250);
  const [picker, setPicker] = useState<AgentModelValue>(INITIAL_PICKER);
  const [nPerTask, setNPerTask] = useState("1");
  const [skipVerifier, setSkipVerifier] = useState(false);
  const [agentTimeoutMult, setAgentTimeoutMult] = useState("1");
  const [retryOnTransient, setRetryOnTransient] = useState(false);
  const [confirmedLargeFanOut, setConfirmedLargeFanOut] = useState(false);
  const [localError, setLocalError] = useState<string | null>(null);

  const navigate = useNavigate();

  const benchmarks = useQuery({
    queryKey: ["benchmarks"],
    queryFn: () => api.listBenchmarks({ limit: "200" }),
    staleTime: 5 * 60 * 1000,
  });
  const agents = useQuery({
    queryKey: ["agents"],
    queryFn: () => api.listAgents(),
    staleTime: 5 * 60 * 1000,
  });

  const matchedTasks = useQuery({
    queryKey: ["tasks-count", benchmark, debouncedTaskQuery],
    queryFn: () =>
      api.listTasks({
        benchmark_id: benchmark || undefined,
        q: debouncedTaskQuery.trim() || undefined,
        limit: "1",
      }) as Promise<{ total: number }>,
  });

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

  const filterIsAtLeastSomewhatNarrow =
    Boolean(benchmark) || debouncedTaskQuery.trim().length > 0;

  const submit = (): void => {
    setLocalError(null);
    if (!name.trim()) {
      setLocalError("Name is required.");
      return;
    }
    if (!filterIsAtLeastSomewhatNarrow) {
      setLocalError(
        "Pick a benchmark or type at least one character in task search — running on the entire catalog by accident is too easy.",
      );
      return;
    }
    if (matchedTasks.isError) {
      setLocalError(
        "Couldn't count matching tasks. Fix the filter or retry before submitting.",
      );
      return;
    }
    const matchedCount = matchedTasks.data?.total;
    if (matchedCount === undefined) {
      setLocalError("Still counting matching tasks — try again in a moment.");
      return;
    }
    if (matchedCount === 0) {
      setLocalError(
        "No tasks match the current filter. Adjust the benchmark or search first.",
      );
      return;
    }
    const selectedAgent = agents.data?.items.find(
      (a) => a.name === picker.agentName,
    );
    if (!selectedAgent) {
      setLocalError("Pick an agent before submitting.");
      return;
    }
    const agentModel = buildAgentModel(picker, selectedAgent.needs_model);
    if (selectedAgent.needs_model && agentModel === null) {
      setLocalError(
        `${selectedAgent.name} needs a model — pick one from the dropdown or fill the Custom model fields.`,
      );
      return;
    }
    const n = Number.parseInt(nPerTask, 10);
    if (!Number.isFinite(n) || n < 1 || n > 100) {
      setLocalError("Samples per task must be between 1 and 100.");
      return;
    }
    const totalTrials = matchedCount * n;
    if (totalTrials > FAN_OUT_CONFIRM_THRESHOLD && !confirmedLargeFanOut) {
      setLocalError(
        `This will launch ${totalTrials} trials (${matchedCount} tasks × ${n} samples). Click the confirm box below if that's intended, then submit again.`,
      );
      return;
    }
    const mult = Number.parseFloat(agentTimeoutMult);
    if (!Number.isFinite(mult) || mult <= 0) {
      setLocalError("Agent timeout multiplier must be a positive number.");
      return;
    }
    const task_filter: Record<string, unknown> = {};
    if (benchmark) task_filter.benchmark_id = benchmark;
    const trial_config: Record<string, unknown> = {
      agent_name: selectedAgent.name,
      agent_model: agentModel,
    };
    if (skipVerifier) trial_config.skip_verifier = true;
    if (mult !== 1) trial_config.agent_timeout_multiplier = mult;
    if (retryOnTransient) {
      trial_config.retry = {
        max_attempts: 3,
        retry_on: ["worker_crash", "env_start_failure"],
      };
    }
    create.mutate({
      name: name.trim(),
      description: description.trim() || undefined,
      task_filter,
      trial_config,
      n_per_task: n,
    });
  };

  const matchedCount = matchedTasks.data?.total;
  const totalTrials =
    matchedCount !== undefined ? matchedCount * (Number.parseInt(nPerTask, 10) || 0) : undefined;
  const showLargeFanOutConfirm =
    totalTrials !== undefined && totalTrials > FAN_OUT_CONFIRM_THRESHOLD;

  let countSummary = "";
  if (matchedTasks.isError) {
    countSummary = "Couldn't load the task count.";
  } else if (debouncedTaskQuery !== taskQuery || matchedTasks.isPending) {
    countSummary = "Counting matching tasks…";
  } else if (matchedCount === undefined) {
    countSummary = "";
  } else if (matchedCount === 0) {
    countSummary = "No tasks match this filter.";
  } else {
    countSummary = `${matchedCount} task${matchedCount === 1 ? "" : "s"} match.`;
  }

  return (
    <div className="space-y-6">
      <header>
        <h1 className="text-2xl font-bold text-slate-900">New campaign</h1>
        <p className="mt-1 text-sm text-slate-500">
          Run a slate of tasks at once. Pick the task set, the agent
          + model, and how many samples to draw per task.
        </p>
      </header>

      <Card>
        <Card.Header title="Identity" />
        <Card.Body className="grid grid-cols-1 gap-4 md:grid-cols-2">
          <label className="block">
            <span className="mb-1 block text-xs font-medium uppercase tracking-wider text-slate-500">
              Name
            </span>
            <Input
              value={name}
              onChange={(e) => setName(e.target.value)}
              placeholder="e.g. MIT slate — run 7"
            />
          </label>
          <label className="block">
            <span className="mb-1 block text-xs font-medium uppercase tracking-wider text-slate-500">
              Description (optional)
            </span>
            <Input
              value={description}
              onChange={(e) => setDescription(e.target.value)}
              placeholder="What's this campaign testing?"
            />
          </label>
        </Card.Body>
      </Card>

      <Card>
        <Card.Header
          title="Which tasks to run"
          description="Pick a benchmark; narrow further with a task-id search if needed. Submission is blocked on an empty filter so the campaign can't accidentally fan out to the entire catalog."
        />
        <Card.Body className="space-y-4">
          <div className="grid grid-cols-1 gap-4 md:grid-cols-2">
            <label className="block">
              <span className="mb-1 block text-xs font-medium uppercase tracking-wider text-slate-500">
                Benchmark
              </span>
              <select
                className="block w-full rounded-lg border border-slate-200 bg-white px-3 py-2 text-sm text-slate-800"
                value={benchmark}
                onChange={(e) => setBenchmark(e.target.value)}
                disabled={benchmarks.isPending}
              >
                <option value="">All benchmarks</option>
                {(benchmarks.data?.items ?? []).map((b: BenchmarkItem) => (
                  <option key={b.id} value={b.id}>
                    {b.display_name ?? b.id}
                  </option>
                ))}
              </select>
            </label>
            <label className="block">
              <span className="mb-1 block text-xs font-medium uppercase tracking-wider text-slate-500">
                Search by task id (optional)
              </span>
              <Input
                value={taskQuery}
                onChange={(e) => setTaskQuery(e.target.value)}
                placeholder="substring of the id, e.g. HumanEval/0"
              />
            </label>
          </div>
          <p
            className="text-xs text-slate-500"
            role="status"
            aria-live="polite"
            aria-busy={matchedTasks.isPending}
          >
            {countSummary}
          </p>
        </Card.Body>
      </Card>

      <Card>
        <Card.Header title="How to run each task" />
        <Card.Body className="space-y-5">
          <div className="max-w-md">
            <AgentModelPicker
              value={picker}
              onChange={setPicker}
              disabled={create.isPending}
            />
          </div>
          <label className="block max-w-xs">
            <span className="mb-1 block text-xs font-medium uppercase tracking-wider text-slate-500">
              Samples per task
            </span>
            <Input
              type="number"
              min={1}
              max={100}
              step={1}
              value={nPerTask}
              onChange={(e) =>
                setNPerTask(clampInRange(e.target.value, 1, 100))
              }
              onBlur={() => setNPerTask((v) => clampInRange(v, 1, 100) || "1")}
            />
            <span className="mt-1 block text-xs text-slate-500">
              Between 1 and 100. The runner submits this many trials per
              matched task.
              {totalTrials !== undefined ? (
                <span className="mt-0.5 block font-medium text-slate-600">
                  Total trials = {totalTrials}.
                </span>
              ) : null}
            </span>
          </label>

          <details className="rounded-lg border border-slate-200 bg-slate-50/40 px-4 py-3">
            <summary className="cursor-pointer text-xs font-medium uppercase tracking-wider text-slate-500">
              Advanced options
            </summary>
            <div className="mt-4 grid grid-cols-1 gap-4 md:grid-cols-2">
              <label className="flex items-center gap-2 text-sm text-slate-700">
                <input
                  type="checkbox"
                  checked={skipVerifier}
                  onChange={(e) => setSkipVerifier(e.target.checked)}
                  className="h-4 w-4 rounded border-slate-300"
                />
                <span>
                  Skip verifier
                  <span className="ml-1 text-xs text-slate-500">
                    (run the agent but don't grade — useful for capturing trajectories only)
                  </span>
                </span>
              </label>
              <label className="flex items-center gap-2 text-sm text-slate-700">
                <input
                  type="checkbox"
                  checked={retryOnTransient}
                  onChange={(e) => setRetryOnTransient(e.target.checked)}
                  className="h-4 w-4 rounded border-slate-300"
                />
                <span>
                  Retry on transient errors
                  <span className="ml-1 text-xs text-slate-500">
                    (up to 3 attempts on worker crashes / env-start failures)
                  </span>
                </span>
              </label>
              <label className="block">
                <span className="mb-1 block text-xs font-medium uppercase tracking-wider text-slate-500">
                  Agent timeout multiplier
                </span>
                <Input
                  type="number"
                  min={0.1}
                  step={0.1}
                  value={agentTimeoutMult}
                  onChange={(e) => setAgentTimeoutMult(e.target.value)}
                />
                <span className="mt-1 block text-xs text-slate-500">
                  Scales the task's default agent timeout. 1 = no change;
                  2 = double the budget.
                </span>
              </label>
            </div>
            <p className="mt-4 text-xs text-slate-500">
              Need a knob that isn't here? Submit via{" "}
              <code className="rounded bg-slate-100 px-1 py-0.5 font-mono">
                POST /api/v1/campaigns
              </code>{" "}
              — the route accepts the full TrialConfig schema.
            </p>
          </details>

          {showLargeFanOutConfirm ? (
            <label className="flex items-start gap-2 rounded-lg border border-amber-200 bg-amber-50 px-3 py-2 text-sm text-amber-800">
              <input
                type="checkbox"
                checked={confirmedLargeFanOut}
                onChange={(e) => setConfirmedLargeFanOut(e.target.checked)}
                className="mt-0.5 h-4 w-4 rounded border-amber-300"
              />
              <span>
                I understand this campaign will launch {totalTrials} trials.
              </span>
            </label>
          ) : null}
        </Card.Body>
      </Card>

      {localError ? (
        <div className="rounded-xl border border-red-200 bg-red-50 px-4 py-3 text-sm text-red-700">
          {localError}
        </div>
      ) : null}
      {create.isError ? <ErrorState error={create.error} /> : null}

      <div className="flex items-center justify-end">
        <Button
          variant="primary"
          onClick={submit}
          disabled={create.isPending}
        >
          {create.isPending ? "Creating…" : "Create campaign"}
        </Button>
      </div>
    </div>
  );
}
