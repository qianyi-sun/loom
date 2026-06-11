/**
 * Create a new campaign. Plan 27 redesign — full TrialConfig
 * surface, every knob the API accepts is reachable from the form
 * with inline help text. Highlights:
 *
 *   - Benchmark is REQUIRED. The previous "All benchmarks" default
 *     was a footgun (one click ran on the whole catalog).
 *   - Task-id substring search narrows further within the chosen
 *     benchmark. Live "N tasks match" preview is debounced.
 *     At submit time, when the search is non-empty we materialize
 *     the matching task ids into the `task_ids` filter so the
 *     campaign actually narrows (was previously dropped on the
 *     floor — the visible count and the submitted filter
 *     disagreed).
 *   - Agent dropdown is one flat sorted list (the built-in vs
 *     loom-launcher distinction is implementation detail; at
 *     runtime they're peers). Model dropdown still groups by
 *     provider because billing differs across providers.
 *   - Advanced fields wrap in a collapsed `<details>` so the form
 *     fits one viewport for the 95% who want defaults. Every
 *     TrialConfig knob lives there grouped by topic (Environment,
 *     Timeouts, Retry, Scheduling) with inline help. No "use the
 *     API directly" hand-wave; the form IS the documentation.
 *   - Retry payload only emits when BOTH `max_attempts > 1` AND
 *     `retry_on` non-empty (was emitting misleading no-op blocks
 *     when only one of the two conditions held).
 *
 *   Multi-benchmark / multi-agent comparison campaigns are deferred —
 *   they need a Campaign.variants column + runner fan-out changes,
 *   not just a UI tweak.
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

const RETRY_REASONS = [
  { value: "worker_crash", label: "Worker crash" },
  { value: "env_start_failure", label: "Env start failure" },
  { value: "agent_timeout", label: "Agent timeout" },
  { value: "verifier_timeout", label: "Verifier timeout" },
  { value: "trajectory_flush_failed", label: "Trajectory flush failed" },
] as const;

type RetryReason = (typeof RETRY_REASONS)[number]["value"];

function clampInt(raw: string, min: number, max: number): string {
  if (raw === "") return raw;
  const n = Number.parseInt(raw, 10);
  if (!Number.isFinite(n)) return String(min);
  if (n < min) return String(min);
  if (n > max) return String(max);
  return String(n);
}

function clampFloat(raw: string, min: number, max?: number): string {
  if (raw === "") return raw;
  const n = Number.parseFloat(raw);
  if (!Number.isFinite(n)) return String(min);
  if (n < min) return String(min);
  if (max !== undefined && n > max) return String(max);
  return raw; // preserve the user's text (e.g. "1.5"), only clamp on out-of-range
}

const INITIAL_PICKER: AgentModelValue = {
  agentName: "",
  modelProvider: "",
  modelName: "",
};

interface AdvancedState {
  // Environment
  forceBuild: boolean;
  deleteEnv: boolean;
  verifierEnvMode: "" | "shared" | "separate";
  skipVerifier: boolean;
  // Timeouts: overrides + multipliers. Empty string = unset.
  overrideAgentTimeoutSec: string;
  agentTimeoutMultiplier: string;
  overrideVerifierTimeoutSec: string;
  verifierTimeoutMultiplier: string;
  overrideEnvBuildTimeoutSec: string;
  envBuildTimeoutMultiplier: string;
  // Retry
  maxAttempts: string;
  retryOn: Set<RetryReason>;
  backoffBaseSec: string;
  backoffMaxSec: string;
  backoffMultiplier: string;
  backoffJitter: string;
  // Scheduling
  submitPriority: string;
}

const INITIAL_ADVANCED: AdvancedState = {
  forceBuild: false,
  deleteEnv: true,
  verifierEnvMode: "",
  skipVerifier: false,
  overrideAgentTimeoutSec: "",
  agentTimeoutMultiplier: "1",
  overrideVerifierTimeoutSec: "",
  verifierTimeoutMultiplier: "1",
  overrideEnvBuildTimeoutSec: "",
  envBuildTimeoutMultiplier: "1",
  maxAttempts: "1",
  retryOn: new Set<RetryReason>(),
  backoffBaseSec: "30",
  backoffMaxSec: "600",
  backoffMultiplier: "2",
  backoffJitter: "0.2",
  submitPriority: "100",
};

function buildAdvancedConfig(
  s: AdvancedState,
): { ok: true; value: Record<string, unknown> } | { ok: false; error: string } {
  const out: Record<string, unknown> = {};
  if (s.forceBuild) out.force_build = true;
  if (!s.deleteEnv) out.delete_env = false; // default is true, only emit when off
  if (s.skipVerifier) out.skip_verifier = true;
  if (s.verifierEnvMode) out.verifier_env_mode = s.verifierEnvMode;
  const numOrErr = (
    raw: string, name: string, opts: { min: number; max?: number; allowEmpty?: boolean } = { min: 0 },
  ): number | undefined | string => {
    if (raw === "") {
      return opts.allowEmpty ? undefined : `${name} is required.`;
    }
    const n = Number.parseFloat(raw);
    if (!Number.isFinite(n)) return `${name} must be a number.`;
    if (n < opts.min) return `${name} must be ≥ ${opts.min}.`;
    if (opts.max !== undefined && n > opts.max) {
      return `${name} must be ≤ ${opts.max}.`;
    }
    return n;
  };
  const opt = (raw: string, name: string, min: number) => {
    if (raw === "") return undefined;
    const v = numOrErr(raw, name, { min });
    return v;
  };

  // Timeouts
  const overrideAgent = opt(s.overrideAgentTimeoutSec, "Override agent timeout", 0.001);
  if (typeof overrideAgent === "string") return { ok: false, error: overrideAgent };
  if (overrideAgent !== undefined) out.override_agent_timeout_sec = overrideAgent;
  const agentMult = numOrErr(s.agentTimeoutMultiplier, "Agent timeout multiplier", { min: 0.001 });
  if (typeof agentMult === "string") return { ok: false, error: agentMult };
  if (agentMult !== 1) out.agent_timeout_multiplier = agentMult;

  const overrideVer = opt(s.overrideVerifierTimeoutSec, "Override verifier timeout", 0.001);
  if (typeof overrideVer === "string") return { ok: false, error: overrideVer };
  if (overrideVer !== undefined) out.override_verifier_timeout_sec = overrideVer;
  const verMult = numOrErr(s.verifierTimeoutMultiplier, "Verifier timeout multiplier", { min: 0.001 });
  if (typeof verMult === "string") return { ok: false, error: verMult };
  if (verMult !== 1) out.verifier_timeout_multiplier = verMult;

  const overrideBuild = opt(s.overrideEnvBuildTimeoutSec, "Override env-build timeout", 0.001);
  if (typeof overrideBuild === "string") return { ok: false, error: overrideBuild };
  if (overrideBuild !== undefined) out.override_env_build_timeout_sec = overrideBuild;
  const buildMult = numOrErr(s.envBuildTimeoutMultiplier, "Env-build timeout multiplier", { min: 0.001 });
  if (typeof buildMult === "string") return { ok: false, error: buildMult };
  if (buildMult !== 1) out.env_build_timeout_multiplier = buildMult;

  // Retry — only emit when BOTH `max_attempts > 1` AND `retry_on` is
  // non-empty. Either alone is a no-op (the runner needs both a reason
  // to retry on AND a budget to spend), so emitting a half-config in
  // those cases would look like "I configured retry" in the saved
  // campaign while doing nothing.
  const maxAttempts = numOrErr(s.maxAttempts, "Max attempts", { min: 1, max: 20 });
  if (typeof maxAttempts === "string") return { ok: false, error: maxAttempts };
  const wantsRetry = maxAttempts > 1 && s.retryOn.size > 0;
  if (wantsRetry) {
    const base = numOrErr(s.backoffBaseSec, "Backoff base seconds", { min: 0.001 });
    if (typeof base === "string") return { ok: false, error: base };
    const max = numOrErr(s.backoffMaxSec, "Backoff max seconds", { min: 0.001 });
    if (typeof max === "string") return { ok: false, error: max };
    const mult = numOrErr(s.backoffMultiplier, "Backoff multiplier", { min: 0.001 });
    if (typeof mult === "string") return { ok: false, error: mult };
    const jitter = numOrErr(s.backoffJitter, "Backoff jitter", { min: 0, max: 1 });
    if (typeof jitter === "string") return { ok: false, error: jitter };
    if (max < base) {
      return {
        ok: false,
        error: "Backoff max seconds must be ≥ backoff base seconds.",
      };
    }
    out.retry = {
      max_attempts: maxAttempts,
      retry_on: Array.from(s.retryOn),
      backoff: {
        base_sec: base,
        max_sec: max,
        multiplier: mult,
        jitter: jitter,
      },
    };
  }

  // Scheduling
  const prio = numOrErr(s.submitPriority, "Submit priority", { min: 0, max: 1000 });
  if (typeof prio === "string") return { ok: false, error: prio };
  if (prio !== 100) out.submit_priority = prio;

  return { ok: true, value: out };
}

function Help({ children }: { children: React.ReactNode }): JSX.Element {
  return <p className="mt-1 text-xs text-slate-500">{children}</p>;
}

function FieldLabel({
  children,
  hint,
}: {
  children: React.ReactNode;
  hint?: React.ReactNode;
}): JSX.Element {
  return (
    <div className="mb-1 flex items-baseline justify-between gap-2">
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
  const [benchmark, setBenchmark] = useState("");
  const [taskQuery, setTaskQuery] = useState("");
  const debouncedTaskQuery = useDebouncedValue(taskQuery, 250);
  const [picker, setPicker] = useState<AgentModelValue>(INITIAL_PICKER);
  const [nPerTask, setNPerTask] = useState("1");
  const [advanced, setAdvanced] = useState<AdvancedState>(INITIAL_ADVANCED);
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

  // Materialize matching task ids alongside the count: when the user
  // narrows with `q`, we need the actual id list at submit time to
  // pass via task_filter.task_ids — otherwise the search is theatre.
  // limit=200 matches the fan-out cap; if the user's filter matches
  // more than that, the search is too broad to materialise and the
  // form errors instead of silently dropping the q on the floor.
  const MAX_MATERIALIZABLE = 200;
  const matchedTasks = useQuery({
    queryKey: ["tasks-count", benchmark, debouncedTaskQuery],
    queryFn: () =>
      api.listTasks({
        benchmark_id: benchmark || undefined,
        q: debouncedTaskQuery.trim() || undefined,
        limit: String(MAX_MATERIALIZABLE),
      }) as Promise<{ items: { id: string }[]; total: number }>,
    enabled: Boolean(benchmark),
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

  const setAdv = <K extends keyof AdvancedState>(
    key: K,
    val: AdvancedState[K],
  ): void => {
    setAdvanced((s) => ({ ...s, [key]: val }));
  };
  const toggleRetryReason = (reason: RetryReason): void => {
    setAdvanced((s) => {
      const next = new Set(s.retryOn);
      if (next.has(reason)) next.delete(reason);
      else next.add(reason);
      return { ...s, retryOn: next };
    });
  };

  const submit = (): void => {
    setLocalError(null);
    if (!name.trim()) {
      setLocalError("Name is required.");
      return;
    }
    if (!benchmark) {
      setLocalError("Pick a benchmark.");
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
        "No tasks match the current benchmark + search. Adjust the search before submitting.",
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
        `This will launch ${totalTrials} trials (${matchedCount} tasks × ${n} samples). Tick the confirm box below if that's intended, then submit again.`,
      );
      return;
    }
    const adv = buildAdvancedConfig(advanced);
    if (!adv.ok) {
      setLocalError(`Advanced options: ${adv.error}`);
      return;
    }
    const trial_config: Record<string, unknown> = {
      ...adv.value,
      agent_name: selectedAgent.name,
      agent_model: agentModel,
    };
    const task_filter: Record<string, unknown> = { benchmark_id: benchmark };
    // When the user has narrowed with a task-id search, materialise
    // matching ids into `task_filter.task_ids`. The backend filter
    // accepts {license, task_ids, benchmark_id} — there's no server
    // side `q` knob — so without this the search would be theatre
    // and the campaign would fan out to the whole benchmark.
    if (debouncedTaskQuery.trim()) {
      const items = matchedTasks.data?.items ?? [];
      if (matchedCount! > items.length) {
        setLocalError(
          `Task-id search matches ${matchedCount} tasks but only the first ${items.length} can be materialised. Narrow the search to ≤ ${MAX_MATERIALIZABLE} before submitting.`,
        );
        return;
      }
      task_filter.task_ids = items.map((t) => t.id);
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
    matchedCount !== undefined
      ? matchedCount * (Number.parseInt(nPerTask, 10) || 0)
      : undefined;
  const showLargeFanOutConfirm =
    totalTrials !== undefined && totalTrials > FAN_OUT_CONFIRM_THRESHOLD;

  let countSummary = "";
  if (!benchmark) {
    countSummary = "Pick a benchmark to count matching tasks.";
  } else if (matchedTasks.isError) {
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
          Run a benchmark slate with one (agent, model) combination.
          Multi-agent / multi-model comparison runs aren't supported
          by the data model yet.
        </p>
      </header>

      <Card>
        <Card.Header title="Identity" />
        <Card.Body className="grid grid-cols-1 gap-4 md:grid-cols-2">
          <label className="block">
            <FieldLabel>Name</FieldLabel>
            <Input
              value={name}
              onChange={(e) => setName(e.target.value)}
              placeholder="e.g. humaneval — claude-opus-4-7 — run 7"
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
        </Card.Body>
      </Card>

      <Card>
        <Card.Header
          title="Which tasks to run"
          description="Pick a benchmark; narrow further with a task-id search if needed."
        />
        <Card.Body className="space-y-4">
          <div className="grid grid-cols-1 gap-4 md:grid-cols-2">
            <label className="block">
              <FieldLabel hint="required">Benchmark</FieldLabel>
              <select
                className="block w-full rounded-lg border border-slate-200 bg-white px-3 py-2 text-sm text-slate-800"
                value={benchmark}
                onChange={(e) => setBenchmark(e.target.value)}
                disabled={benchmarks.isPending}
              >
                <option value="">Choose a benchmark…</option>
                {(benchmarks.data?.items ?? []).map((b: BenchmarkItem) => (
                  <option key={b.id} value={b.id}>
                    {b.display_name ?? b.id}
                  </option>
                ))}
              </select>
            </label>
            <label className="block">
              <FieldLabel hint="optional">Task-id search</FieldLabel>
              <Input
                value={taskQuery}
                onChange={(e) => setTaskQuery(e.target.value)}
                placeholder="substring, e.g. HumanEval/0"
                disabled={!benchmark}
              />
            </label>
          </div>
          <p
            className="text-xs text-slate-500"
            role="status"
            aria-live="polite"
            aria-busy={Boolean(benchmark) && matchedTasks.isPending}
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
            <FieldLabel hint="1 – 100">Samples per task</FieldLabel>
            <Input
              type="number"
              min={1}
              max={100}
              step={1}
              value={nPerTask}
              onChange={(e) => setNPerTask(clampInt(e.target.value, 1, 100))}
              onBlur={() => setNPerTask((v) => clampInt(v, 1, 100) || "1")}
            />
            <Help>
              The runner submits this many trials per matched task.
              {totalTrials !== undefined ? (
                <span className="mt-0.5 block font-medium text-slate-600">
                  Total trials = {totalTrials}.
                </span>
              ) : null}
            </Help>
          </label>
        </Card.Body>
      </Card>

      <Card>
        <details className="group">
          <summary className="flex cursor-pointer items-start gap-2 px-6 py-4 text-sm font-semibold text-slate-900">
            <span className="flex-1">
              Advanced options
              <span className="ml-2 text-xs font-normal text-slate-500">
                (click to expand — defaults are sensible)
              </span>
            </span>
            <span className="text-slate-400 transition-transform group-open:rotate-90">
              ›
            </span>
          </summary>
          <div className="space-y-6 px-6 pb-6">
          {/* Environment */}
          <fieldset className="space-y-3">
            <legend className="text-sm font-semibold text-slate-700">
              Environment
            </legend>
            <label className="flex items-start gap-2 text-sm text-slate-700">
              <input
                type="checkbox"
                checked={advanced.forceBuild}
                onChange={(e) => setAdv("forceBuild", e.target.checked)}
                className="mt-1 h-4 w-4 rounded border-slate-300"
              />
              <span>
                Force rebuild env image
                <Help>
                  Default: off. When on, the worker rebuilds the task's docker image even if a
                  cached layer exists. Useful when the task's Dockerfile changed but the
                  checksum didn't.
                </Help>
              </span>
            </label>
            <label className="flex items-start gap-2 text-sm text-slate-700">
              <input
                type="checkbox"
                checked={!advanced.deleteEnv}
                onChange={(e) => setAdv("deleteEnv", !e.target.checked)}
                className="mt-1 h-4 w-4 rounded border-slate-300"
              />
              <span>
                Keep env container after the trial finishes
                <Help>
                  Default: off (env is deleted). Turn on for post-mortem
                  inspection — you'll need to clean up the container by
                  hand.
                </Help>
              </span>
            </label>
            <label className="flex items-start gap-2 text-sm text-slate-700">
              <input
                type="checkbox"
                checked={advanced.skipVerifier}
                onChange={(e) => setAdv("skipVerifier", e.target.checked)}
                className="mt-1 h-4 w-4 rounded border-slate-300"
              />
              <span>
                Skip verifier
                <Help>
                  Default: off. When on, the agent runs but no grading
                  happens. Use for trajectory-capture-only runs.
                </Help>
              </span>
            </label>
            <label className="block max-w-sm">
              <FieldLabel hint="default: task setting">Verifier env mode</FieldLabel>
              <select
                className="block w-full rounded-lg border border-slate-200 bg-white px-3 py-2 text-sm text-slate-800"
                value={advanced.verifierEnvMode}
                onChange={(e) =>
                  setAdv("verifierEnvMode", e.target.value as AdvancedState["verifierEnvMode"])
                }
              >
                <option value="">Use task default</option>
                <option value="shared">shared (verifier runs in the same env as the agent)</option>
                <option value="separate">separate (verifier runs in a fresh env)</option>
              </select>
            </label>
          </fieldset>

          {/* Timeouts */}
          <fieldset className="space-y-3">
            <legend className="text-sm font-semibold text-slate-700">
              Timeouts
            </legend>
            <p className="text-xs text-slate-500">
              Each row has an absolute override (in seconds — leave blank to use the task's default)
              and a multiplier that scales the resolved value (1 = no change).
            </p>
            {[
              { label: "Agent", override: "overrideAgentTimeoutSec" as const, mult: "agentTimeoutMultiplier" as const },
              { label: "Verifier", override: "overrideVerifierTimeoutSec" as const, mult: "verifierTimeoutMultiplier" as const },
              { label: "Env build", override: "overrideEnvBuildTimeoutSec" as const, mult: "envBuildTimeoutMultiplier" as const },
            ].map((row) => (
              <div key={row.label} className="grid grid-cols-1 gap-2 md:grid-cols-2">
                <label className="block">
                  <FieldLabel>{row.label} timeout override (s)</FieldLabel>
                  <Input
                    type="number"
                    min={0.001}
                    step={1}
                    value={advanced[row.override]}
                    onChange={(e) => setAdv(row.override, e.target.value)}
                    placeholder="task default"
                  />
                </label>
                <label className="block">
                  <FieldLabel>{row.label} timeout multiplier</FieldLabel>
                  <Input
                    type="number"
                    min={0.001}
                    step={0.1}
                    value={advanced[row.mult]}
                    onChange={(e) => setAdv(row.mult, e.target.value)}
                    onBlur={() =>
                      setAdv(
                        row.mult,
                        clampFloat(advanced[row.mult], 0.001) || "1",
                      )
                    }
                  />
                </label>
              </div>
            ))}
          </fieldset>

          {/* Retry */}
          <fieldset className="space-y-3">
            <legend className="text-sm font-semibold text-slate-700">
              Retry on transient errors
            </legend>
            <div className="grid grid-cols-1 gap-3 md:grid-cols-2">
              <label className="block">
                <FieldLabel hint="1 – 20">Max attempts</FieldLabel>
                <Input
                  type="number"
                  min={1}
                  max={20}
                  step={1}
                  value={advanced.maxAttempts}
                  onChange={(e) => setAdv("maxAttempts", clampInt(e.target.value, 1, 20))}
                  onBlur={() =>
                    setAdv("maxAttempts", clampInt(advanced.maxAttempts, 1, 20) || "1")
                  }
                />
                <Help>
                  Total attempts including the first. 1 = no retry.
                </Help>
              </label>
              <div>
                <FieldLabel hint="pick zero or more">Retry on</FieldLabel>
                <div className="space-y-1">
                  {RETRY_REASONS.map((r) => (
                    <label key={r.value} className="flex items-center gap-2 text-sm text-slate-700">
                      <input
                        type="checkbox"
                        checked={advanced.retryOn.has(r.value)}
                        onChange={() => toggleRetryReason(r.value)}
                        className="h-4 w-4 rounded border-slate-300"
                      />
                      <span>{r.label}</span>
                    </label>
                  ))}
                </div>
                <Help>
                  No boxes ticked = no retry (even if max attempts &gt; 1).
                </Help>
              </div>
            </div>
            <div className="grid grid-cols-2 gap-3 md:grid-cols-4">
              <label className="block">
                <FieldLabel>Backoff base (s)</FieldLabel>
                <Input
                  type="number"
                  min={0.001}
                  step={1}
                  value={advanced.backoffBaseSec}
                  onChange={(e) => setAdv("backoffBaseSec", e.target.value)}
                />
              </label>
              <label className="block">
                <FieldLabel>Backoff max (s)</FieldLabel>
                <Input
                  type="number"
                  min={0.001}
                  step={1}
                  value={advanced.backoffMaxSec}
                  onChange={(e) => setAdv("backoffMaxSec", e.target.value)}
                />
              </label>
              <label className="block">
                <FieldLabel>Backoff multiplier</FieldLabel>
                <Input
                  type="number"
                  min={0.001}
                  step={0.1}
                  value={advanced.backoffMultiplier}
                  onChange={(e) => setAdv("backoffMultiplier", e.target.value)}
                />
              </label>
              <label className="block">
                <FieldLabel hint="0 – 1">Backoff jitter</FieldLabel>
                <Input
                  type="number"
                  min={0}
                  max={1}
                  step={0.05}
                  value={advanced.backoffJitter}
                  onChange={(e) => setAdv("backoffJitter", e.target.value)}
                />
              </label>
            </div>
            <Help>
              Sleep before each retry is min(base × multiplier<sup>attempt</sup>, max), randomised by jitter.
            </Help>
          </fieldset>

          {/* Scheduling */}
          <fieldset className="space-y-3">
            <legend className="text-sm font-semibold text-slate-700">
              Scheduling
            </legend>
            <label className="block max-w-xs">
              <FieldLabel hint="0 – 1000">Submit priority</FieldLabel>
              <Input
                type="number"
                min={0}
                max={1000}
                step={1}
                value={advanced.submitPriority}
                onChange={(e) =>
                  setAdv("submitPriority", clampInt(e.target.value, 0, 1000))
                }
                onBlur={() =>
                  setAdv(
                    "submitPriority",
                    clampInt(advanced.submitPriority, 0, 1000) || "100",
                  )
                }
              />
              <Help>
                Higher = scheduled first. Default 100. The DRF tie-break uses
                this when two trials compete for the same slot.
              </Help>
            </label>
          </fieldset>
          </div>
        </details>
      </Card>

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
