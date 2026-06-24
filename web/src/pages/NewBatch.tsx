/**
 * Create a new batch — Plan 28 PR-4 redesign.
 *
 * Two-column layout (Harbor-style) on xl screens:
 *   - LEFT: identity + backend + which tasks (benchmark dropdown +
 *     subset radio: all / first_n / last_n / random_n / explicit).
 *     For explicit, a paste textarea drives the smart parser and
 *     materialises ids into task_filter.task_ids at submit.
 *   - RIGHT (slate-50 surface): one-or-more Combinations. Each row
 *     is (agent, model, n_per_task, label). Cap at 16.
 *
 * The Advanced disclosure preserves every TrialConfig knob from the
 * pre-redesign form (timeouts, retry, scheduling) verbatim; only
 * agent_name / agent_model / n_per_task have been LIFTED out of
 * trial_config and into the combinations array. Every submit (even
 * single-combo) sends a 1-element `combinations` list — the route
 * has one shape now and the SPA matches it.
 *
 * Submit-button label is dynamic: `Submit N trial(s)`. A
 * confirmation banner above submit lists per-combination chips +
 * total fan-out (= matched_tasks × Σ n_per_task) and blocks submit
 * over the FAN_OUT_CONFIRM_THRESHOLD until the user ticks confirm.
 */

import { useMutation, useQueries, useQuery } from "@tanstack/react-query";
import { useEffect, useMemo, useState } from "react";
import { useNavigate } from "react-router-dom";

import {
  api,
  type BenchmarkTagsResponse,
  type Combination,
  type CreateBatchBody,
} from "../api/client";
import {
  AgentModelPicker,
  buildAgentModel,
  buildProviderOverride,
  type ProviderOverride,
  type AgentModelValue,
} from "../components/AgentModelPicker";
import { Button } from "../components/Button";
import { Card } from "../components/Card";
import CommandSnippet from "../components/CommandSnippet";
import DocsCallout from "../components/DocsCallout";
import ErrorState from "../components/ErrorState";
import { Input, Textarea } from "../components/Input";
import {
  agentReadinessMessage,
  agentServiceModeReady,
} from "../lib/agentReadiness";
import { EMPTY_BENCHMARK_HELP } from "../lib/helpText";
import { parseTaskIds } from "../lib/parseTaskIds";
import {
  oracleSmokeBatchCommand,
  providerSmokeBatchCommand,
} from "../lib/quickstartSnippets";

interface BenchmarkItem {
  id: string;
  display_name?: string;
  task_count?: number;
  raw_task_count?: number;
  valid_task_config_count?: number;
  invalid_task_config_count?: number;
  license_allowed_task_count?: number;
  license_blocked_task_count?: number;
  blocked_licenses?: string[];
  readiness_state?: string;
  readiness_label?: string;
  readiness_message?: string | null;
  selectable?: boolean;
  blocker_reason?: string | null;
  series?: string | null;
}

const FAN_OUT_CONFIRM_THRESHOLD = 200;
const MAX_COMBINATIONS = 16;

const RETRY_REASONS = [
  { value: "worker_crash", label: "Worker crash" },
  { value: "env_start_failure", label: "Env start failure" },
  { value: "agent_timeout", label: "Agent timeout" },
  { value: "verifier_timeout", label: "Verifier timeout" },
  { value: "trajectory_flush_failed", label: "Trajectory flush failed" },
] as const;

type RetryReason = (typeof RETRY_REASONS)[number]["value"];

type SubsetKind = "all" | "first_n" | "last_n" | "random_n" | "explicit";

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
  return raw;
}

const INITIAL_PICKER: AgentModelValue = {
  agentName: "",
  source: "api",
  modelProvider: "",
  modelName: "",
  hfExecution: "local-vllm",
};

interface ComboRow {
  picker: AgentModelValue;
  nPerTask: string;
  label: string;
}

type ProviderSelectionResult =
  | { ok: true; value: ProviderOverride | null }
  | { ok: false; error: string };

function newRow(): ComboRow {
  return { picker: { ...INITIAL_PICKER }, nPerTask: "1", label: "" };
}

interface AdvancedState {
  forceBuild: boolean;
  deleteEnv: boolean;
  verifierEnvMode: "" | "shared" | "separate";
  skipVerifier: boolean;
  overrideAgentTimeoutSec: string;
  agentTimeoutMultiplier: string;
  overrideVerifierTimeoutSec: string;
  verifierTimeoutMultiplier: string;
  overrideEnvBuildTimeoutSec: string;
  envBuildTimeoutMultiplier: string;
  maxAttempts: string;
  retryOn: Set<RetryReason>;
  backoffBaseSec: string;
  backoffMaxSec: string;
  backoffMultiplier: string;
  backoffJitter: string;
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
  if (!s.deleteEnv) out.delete_env = false;
  if (s.skipVerifier) out.skip_verifier = true;
  if (s.verifierEnvMode) out.verifier_env_mode = s.verifierEnvMode;
  const numOrErr = (
    raw: string,
    name: string,
    opts: { min: number; max?: number; allowEmpty?: boolean } = { min: 0 },
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
    return numOrErr(raw, name, { min });
  };

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

  const maxAttempts = numOrErr(s.maxAttempts, "Max attempts", { min: 1, max: 20 });
  if (typeof maxAttempts === "string") return { ok: false, error: maxAttempts };
  const wantsRetry = (maxAttempts as number) > 1 && s.retryOn.size > 0;
  if (wantsRetry) {
    const base = numOrErr(s.backoffBaseSec, "Backoff base seconds", { min: 0.001 });
    if (typeof base === "string") return { ok: false, error: base };
    const max = numOrErr(s.backoffMaxSec, "Backoff max seconds", { min: 0.001 });
    if (typeof max === "string") return { ok: false, error: max };
    const mult = numOrErr(s.backoffMultiplier, "Backoff multiplier", { min: 0.001 });
    if (typeof mult === "string") return { ok: false, error: mult };
    const jitter = numOrErr(s.backoffJitter, "Backoff jitter", { min: 0, max: 1 });
    if (typeof jitter === "string") return { ok: false, error: jitter };
    if ((max as number) < (base as number)) {
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

function benchmarkSelectable(r: BenchmarkItem): boolean {
  if (typeof r.selectable === "boolean") return r.selectable;
  return (r.task_count ?? 0) > 0;
}

function benchmarkReadinessLabel(r: BenchmarkItem): string {
  if (r.readiness_label) return r.readiness_label;
  return benchmarkSelectable(r) ? "Ready" : "Needs publish";
}

function benchmarkReadinessMessage(r: BenchmarkItem): string | undefined {
  if (r.readiness_message) return r.readiness_message;
  return benchmarkSelectable(r) ? undefined : EMPTY_BENCHMARK_HELP;
}

function benchmarkCountText(r: BenchmarkItem): string | null {
  const valid = r.valid_task_config_count ?? r.task_count;
  const raw = r.raw_task_count;
  if (valid === undefined) return null;
  if (raw !== undefined && raw > valid) {
    return `${valid}/${raw} runnable`;
  }
  return `${valid} task${valid === 1 ? "" : "s"}`;
}

function benchmarkReadinessBadgeClass(r: BenchmarkItem): string {
  if (benchmarkSelectable(r)) {
    return "rounded border border-emerald-200 bg-emerald-50 px-1.5 py-0.5 text-[11px] font-medium text-emerald-700";
  }
  if (r.blocker_reason === "manifest_missing") {
    return "rounded border border-amber-200 bg-amber-50 px-1.5 py-0.5 text-[11px] font-medium text-amber-700";
  }
  return "rounded border border-rose-200 bg-rose-50 px-1.5 py-0.5 text-[11px] font-medium text-rose-700";
}

/**
 * Series-grouped benchmark multi-select.
 *
 * Groups rows by `series` (NULL → "Other" at the bottom). Each group
 * has a "Select all" affordance — the SPA's group-select path the
 * series catalog redesign was built for. The picker is purely
 * controlled; selection state lives in the parent.
 */
function BenchmarkPicker({
  items,
  loading,
  selected,
  onChange,
}: {
  items: BenchmarkItem[];
  loading: boolean;
  selected: Set<string>;
  onChange: (next: Set<string>) => void;
}): JSX.Element {
  const groups = useMemo(() => {
    const bySeries = new Map<string, BenchmarkItem[]>();
    for (const b of items) {
      const key = b.series ?? "";
      const bucket = bySeries.get(key) ?? [];
      bucket.push(b);
      bySeries.set(key, bucket);
    }
    return Array.from(bySeries.entries())
      .map(([series, rows]) => ({
        series,
        rows: rows.sort((a, b) =>
          (a.display_name ?? a.id).localeCompare(b.display_name ?? b.id),
        ),
      }))
      .sort((a, b) => {
        // "Other" (empty series) sinks to the bottom.
        if (a.series === "" && b.series !== "") return 1;
        if (b.series === "" && a.series !== "") return -1;
        return a.series.localeCompare(b.series);
      });
  }, [items]);

  const toggleOne = (id: string): void => {
    const next = new Set(selected);
    if (next.has(id)) next.delete(id);
    else next.add(id);
    onChange(next);
  };
  const toggleGroup = (rows: BenchmarkItem[]): void => {
    const selectable = rows.filter(benchmarkSelectable);
    if (selectable.length === 0) return;
    const next = new Set(selected);
    const allOn = selectable.every((r) => next.has(r.id));
    for (const r of selectable) {
      if (allOn) next.delete(r.id);
      else next.add(r.id);
    }
    onChange(next);
  };

  if (loading && items.length === 0) {
    return (
      <p className="mt-1 text-xs text-slate-500">Loading benchmarks…</p>
    );
  }

  return (
    <div className="mt-1 max-h-72 overflow-y-auto rounded-lg border border-slate-200 bg-white">
      {groups.map(({ series, rows }) => {
        const seriesLabel = series === "" ? "Other" : series;
        const selectableRows = rows.filter(benchmarkSelectable);
        const allOn =
          selectableRows.length > 0 &&
          selectableRows.every((r) => selected.has(r.id));
        const someOn =
          !allOn && selectableRows.some((r) => selected.has(r.id));
        const populated = selectableRows.length;
        return (
          <div key={seriesLabel} className="border-b border-slate-100 last:border-b-0">
            <label
              className={
                populated > 0
                  ? "flex items-center gap-2 bg-indigo-50/60 px-3 py-2 text-sm font-semibold text-indigo-900"
                  : "flex items-center gap-2 bg-slate-50 px-3 py-2 text-sm font-semibold text-slate-400"
              }
            >
              <input
                type="checkbox"
                checked={allOn}
                ref={(el) => {
                  if (el) el.indeterminate = someOn;
                }}
                onChange={() => toggleGroup(rows)}
                disabled={populated === 0}
                aria-label={`Select all in series ${seriesLabel}`}
                title={
                  populated > 0
                    ? `Select or clear all ready benchmarks in the ${seriesLabel} series.`
                    : `The ${seriesLabel} series has no ready benchmarks to select.`
                }
                className="h-4 w-4 border-slate-300 disabled:cursor-not-allowed disabled:opacity-50"
              />
              <span
                className={
                  populated > 0
                    ? "inline-block h-2 w-2 rounded-full bg-indigo-400"
                    : "inline-block h-2 w-2 rounded-full bg-slate-300"
                }
                aria-hidden="true"
              />
              <span className="uppercase tracking-wider text-xs">
                {seriesLabel}
              </span>
              <span
                className={
                  populated > 0
                    ? "ml-auto font-normal normal-case text-xs text-indigo-600/70"
                    : "ml-auto font-normal normal-case text-xs text-slate-400"
                }
              >
                {populated}/{rows.length} ready
              </span>
            </label>
            {rows.map((r) => {
              const label = r.display_name ?? r.id;
              const countText = benchmarkCountText(r);
              const selectable = benchmarkSelectable(r);
              const readinessLabel = benchmarkReadinessLabel(r);
              const readinessMessage = benchmarkReadinessMessage(r);
              return (
                <label
                  key={r.id}
                  className={
                    !selectable
                      ? "flex items-center gap-2 pl-9 pr-3 py-1.5 text-sm text-slate-400 cursor-not-allowed"
                      : "flex items-center gap-2 pl-9 pr-3 py-1.5 text-sm text-slate-700 hover:bg-slate-50"
                  }
                  title={readinessMessage}
                >
                  <input
                    type="checkbox"
                    checked={selected.has(r.id)}
                    onChange={() => toggleOne(r.id)}
                    disabled={!selectable}
                    aria-label={`Select benchmark ${r.id}`}
                    className="h-4 w-4 border-slate-300 disabled:cursor-not-allowed"
                  />
                  <span className="min-w-0 flex-1">
                    <span className="block truncate">{label}</span>
                    {!selectable && readinessMessage ? (
                      <span className="block truncate text-xs text-slate-500">
                        {readinessMessage}
                      </span>
                    ) : null}
                  </span>
                  {countText ? (
                    <span
                      className={
                        !selectable
                          ? "text-xs italic text-slate-400"
                          : "text-xs text-slate-400"
                      }
                    >
                      {countText}
                    </span>
                  ) : null}
                  <span className={benchmarkReadinessBadgeClass(r)}>
                    {readinessLabel}
                  </span>
                </label>
              );
            })}
          </div>
        );
      })}
    </div>
  );
}

/**
 * Tag-filter card.
 *
 * Renders one labelled checkbox-group per distinct tag key discovered
 * across the selected benchmarks. AND across keys, OR within each
 * key's value list — matches the backend resolver added in PR-2.
 * Empty value lists are skipped at submit so a mid-edit "no values
 * checked under this key" state is a no-op rather than a filter that
 * matches zero rows.
 */
function TagFiltersCard({
  schema,
  value,
  onChange,
  loading,
}: {
  schema: { key: string; values: string[] }[];
  value: Record<string, Set<string>>;
  onChange: (
    next:
      | Record<string, Set<string>>
      | ((prev: Record<string, Set<string>>) => Record<string, Set<string>>),
  ) => void;
  loading: boolean;
}): JSX.Element | null {
  if (loading && schema.length === 0) {
    return (
      <p className="text-xs text-slate-500">Loading tag schema…</p>
    );
  }
  if (schema.length === 0) return null;

  const toggle = (key: string, val: string): void => {
    onChange((prev) => {
      const next = { ...prev };
      const bucket = new Set(next[key] ?? []);
      if (bucket.has(val)) bucket.delete(val);
      else bucket.add(val);
      next[key] = bucket;
      return next;
    });
  };
  const clearKey = (key: string): void => {
    onChange((prev) => {
      const next = { ...prev };
      next[key] = new Set();
      return next;
    });
  };
  const anyActive = Object.values(value).some((s) => s.size > 0);

  return (
    <details className="rounded-lg border border-slate-200 bg-slate-50/50" open={anyActive}>
      <summary className="flex cursor-pointer items-center gap-2 px-3 py-2 text-sm font-medium text-slate-700">
        <span>Filter by tag</span>
        <span className="text-xs font-normal text-slate-500">
          {anyActive ? "active" : "narrow the slate further"}
        </span>
      </summary>
      <div className="space-y-3 px-3 py-2">
        {schema.map(({ key, values }) => {
          const active = value[key] ?? new Set<string>();
          return (
            <div key={key}>
              <div className="mb-1 flex items-baseline gap-2">
                <span className="text-xs font-medium uppercase tracking-wider text-slate-500">
                  {key}
                </span>
                {active.size > 0 ? (
                  <button
                    type="button"
                    onClick={() => clearKey(key)}
                    title={`Clear selected ${key} tag values.`}
                    className="text-xs text-indigo-600 hover:underline"
                  >
                    clear
                  </button>
                ) : null}
              </div>
              <div className="flex flex-wrap gap-1.5">
                {values.map((v) => {
                  const on = active.has(v);
                  return (
                    <button
                      key={v}
                      type="button"
                      onClick={() => toggle(key, v)}
                      aria-pressed={on}
                      title={
                        on
                          ? `Remove ${key}=${v} from the task filter.`
                          : `Add ${key}=${v} to the task filter.`
                      }
                      className={
                        on
                          ? "rounded-md border border-indigo-500 bg-indigo-50 px-2 py-0.5 text-xs font-medium text-indigo-700"
                          : "rounded-md border border-slate-200 bg-white px-2 py-0.5 text-xs text-slate-700 hover:border-slate-300"
                      }
                    >
                      {v}
                    </button>
                  );
                })}
              </div>
            </div>
          );
        })}
      </div>
    </details>
  );
}

function freshSeed(): number {
  // 32-bit unsigned seed — Math.random() over 2**31 gives the route's
  // validation a value it'll always accept.
  return Math.floor(Math.random() * 2 ** 31);
}

export default function NewBatch(): JSX.Element {
  const [name, setName] = useState("");
  const [description, setDescription] = useState("");
  const [backend, setBackend] = useState("");
  const [selectedBenchmarks, setSelectedBenchmarks] = useState<Set<string>>(
    () => new Set(),
  );
  const [tagFilters, setTagFilters] = useState<Record<string, Set<string>>>(
    {},
  );
  const [subsetKind, setSubsetKind] = useState<SubsetKind>("all");
  const [subsetN, setSubsetN] = useState("10");
  const [subsetSeed, setSubsetSeed] = useState<string>(() => String(freshSeed()));
  const [explicitText, setExplicitText] = useState("");
  const [rows, setRows] = useState<ComboRow[]>(() => [newRow()]);
  const [advanced, setAdvanced] = useState<AdvancedState>(INITIAL_ADVANCED);
  const [confirmedLargeFanOut, setConfirmedLargeFanOut] = useState(false);
  const [localError, setLocalError] = useState<string | null>(null);

  const navigate = useNavigate();

  const benchmarks = useQuery({
    queryKey: ["benchmarks", "with-pending"],
    // New Batch is the user-facing benchmark surface. Keep pending benchmark
    // rows visible with backend readiness diagnostics so users can see the
    // supported catalog roadmap, while disabled checkboxes still prevent
    // submitting rows that need publish/repair work.
    queryFn: () => api.listBenchmarks({ limit: "200", include_empty: "true" }),
    staleTime: 5 * 60 * 1000,
  });
  const agents = useQuery({
    queryKey: ["agents"],
    queryFn: () => api.listAgents(),
    staleTime: 5 * 60 * 1000,
  });
  const backends = useQuery({
    queryKey: ["backends"],
    queryFn: () => api.listBackends(),
    staleTime: 5 * 60 * 1000,
  });

  // Default-pick the backend once the catalog loads: docker if
  // advertised, else the first `available` backend, else the first
  // entry. Skips when the user has already picked one.
  useEffect(() => {
    if (backend || !backends.data) return;
    const items = backends.data.items;
    const docker = items.find((b) => b.name === "docker");
    const firstLive = items.find((b) => b.available);
    const pick = docker ?? firstLive ?? items[0];
    if (pick) setBackend(pick.name);
  }, [backend, backends.data]);

  // Parse the explicit textarea continuously so we can show the
  // "Parsed N ids" preview as the user types.
  const parsed = useMemo(() => parseTaskIds(explicitText), [explicitText]);

  // Per-benchmark tag-key discovery. Fires once per selected benchmark
  // so we can union the keys/values and render one checkbox group per
  // distinct key. Cheap enough to refetch on selection changes; keyed
  // by benchmark id so React Query dedupes identical refetches.
  const tagQueries = useQueries({
    queries: Array.from(selectedBenchmarks).map((id) => ({
      queryKey: ["benchmark-tags", id],
      queryFn: () => api.listBenchmarkTags(id),
      staleTime: 5 * 60 * 1000,
    })),
  });

  // Merged tag schema across the selected benchmarks. Each key's value
  // set is the union of the per-benchmark values — matches the backend
  // resolver where tag_filters is applied across the unioned slate.
  const tagSchema = useMemo<{ key: string; values: string[] }[]>(() => {
    const merged = new Map<string, Set<string>>();
    for (const q of tagQueries) {
      const data = q.data as BenchmarkTagsResponse | undefined;
      if (!data) continue;
      for (const { key, values } of data.items) {
        const bucket = merged.get(key) ?? new Set<string>();
        for (const v of values) bucket.add(v);
        merged.set(key, bucket);
      }
    }
    return Array.from(merged.entries())
      .sort(([a], [b]) => a.localeCompare(b))
      .map(([key, vs]) => ({ key, values: Array.from(vs).sort() }));
  }, [tagQueries]);

  // Drop tag-filter entries whose key is no longer in the schema (the
  // user deselected its benchmark). Keeps the submit payload tight.
  useEffect(() => {
    setTagFilters((prev) => {
      const validKeys = new Set(tagSchema.map((s) => s.key));
      let changed = false;
      const next: Record<string, Set<string>> = {};
      for (const [k, vs] of Object.entries(prev)) {
        if (validKeys.has(k)) next[k] = vs;
        else changed = true;
      }
      return changed ? next : prev;
    });
  }, [tagSchema]);

  // PR-2 backend takes the candidate count from a real query; the SPA
  // gets a "good enough" estimate by summing per-benchmark task_count
  // from /benchmarks. When tag_filters are active the estimate is a
  // pure upper bound, so we now also issue a real count via
  // POST /api/v1/tasks/count (issue #28) and prefer that when it's
  // ready. The estimate stays as the live display while the count
  // query is in flight.
  const hasTagFilter = Object.values(tagFilters).some((v) => v.size > 0);
  const sumOfSelectedTasks = useMemo(() => {
    if (!benchmarks.data || selectedBenchmarks.size === 0) return undefined;
    let total = 0;
    let allKnown = true;
    for (const b of benchmarks.data.items as BenchmarkItem[]) {
      if (!selectedBenchmarks.has(b.id)) continue;
      if (typeof b.task_count !== "number") {
        allKnown = false;
        break;
      }
      total += b.task_count;
    }
    return allKnown ? total : undefined;
  }, [benchmarks.data, selectedBenchmarks]);

  // Build the same `task_filter` the submit handler would send. The
  // count endpoint returns the runnable count after stored TaskConfig
  // validation, so placeholder/unpublished rows do not make the form
  // advertise launchable trials. Keyed by the filter shape so React
  // Query dedupes and refetches only when the filter actually changes.
  const benchmarkIdsSorted = useMemo(
    () => Array.from(selectedBenchmarks).sort(),
    [selectedBenchmarks],
  );
  const tagFiltersPayload = useMemo(() => {
    const out: Record<string, string[]> = {};
    for (const [k, vs] of Object.entries(tagFilters)) {
      if (vs.size > 0) out[k] = Array.from(vs).sort();
    }
    return out;
  }, [tagFilters]);
  const countTaskFilter = useMemo<Record<string, unknown> | null>(() => {
    if (subsetKind === "explicit") return null; // explicit ⇒ count is local
    if (selectedBenchmarks.size === 0) return null;
    const f: Record<string, unknown> = {
      subset_kind: subsetKind,
      benchmark_ids: benchmarkIdsSorted,
    };
    if (Object.keys(tagFiltersPayload).length > 0) {
      f.tag_filters = tagFiltersPayload;
    }
    if (subsetKind !== "all") {
      const n = Number.parseInt(subsetN, 10);
      if (Number.isFinite(n) && n > 0) f.n = n;
    }
    if (subsetKind === "random_n") {
      const seed = Number.parseInt(subsetSeed, 10);
      if (Number.isFinite(seed)) f.seed = seed;
    }
    return f;
  }, [
    subsetKind, selectedBenchmarks.size, benchmarkIdsSorted,
    tagFiltersPayload, subsetN, subsetSeed,
  ]);
  // Only issue the count call when a tag filter is active — without
  // one the estimate from sumOfSelectedTasks is exact and a network
  // round-trip is wasted. Keyed on JSON-stringified filter so the
  // cache key is stable for identical shapes.
  const exactCount = useQuery({
    queryKey: ["tasks-count", JSON.stringify(countTaskFilter)],
    queryFn: () =>
      api.countTasks({ task_filter: countTaskFilter ?? {} }),
    enabled: countTaskFilter !== null && hasTagFilter,
    staleTime: 30 * 1000,
  });

  const matchedTaskCount: number | undefined = (() => {
    if (subsetKind === "explicit") return parsed.ids.length;
    // When a tag filter is active and we have a real count, use it.
    // Otherwise fall back to the upper-bound estimate.
    if (hasTagFilter && exactCount.data !== undefined) {
      return exactCount.data.count;
    }
    if (sumOfSelectedTasks === undefined) return undefined;
    if (subsetKind === "all") return sumOfSelectedTasks;
    const n = Number.parseInt(subsetN, 10);
    if (!Number.isFinite(n)) return undefined;
    return Math.min(sumOfSelectedTasks, Math.max(0, n));
  })();

  const create = useMutation({
    mutationFn: (body: CreateBatchBody) => api.createBatch(body),
    onSuccess: (res) => {
      navigate(`/batches/${res.batch_id}`);
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

  const updateRow = (i: number, patch: Partial<ComboRow>): void => {
    setRows((rs) => rs.map((r, idx) => (idx === i ? { ...r, ...patch } : r)));
  };
  const addRow = (): void => {
    setRows((rs) =>
      rs.length >= MAX_COMBINATIONS ? rs : [...rs, newRow()],
    );
  };
  const removeRow = (i: number): void => {
    setRows((rs) => (rs.length <= 1 ? rs : rs.filter((_, idx) => idx !== i)));
  };

  const sumNPerTask = rows.reduce((acc, r) => {
    const n = Number.parseInt(r.nPerTask, 10);
    return acc + (Number.isFinite(n) && n > 0 ? n : 0);
  }, 0);

  const totalTrials =
    matchedTaskCount !== undefined && sumNPerTask > 0
      ? matchedTaskCount * sumNPerTask
      : undefined;
  const showLargeFanOutConfirm =
    totalTrials !== undefined && totalTrials > FAN_OUT_CONFIRM_THRESHOLD;

  function buildCombinations(): { ok: true; value: Combination[] } | { ok: false; error: string } {
    const labels = new Set<string>();
    const out: Combination[] = [];
    for (let i = 0; i < rows.length; i++) {
      const r = rows[i];
      const selectedAgent = agents.data?.items.find(
        (a) => a.name === r.picker.agentName,
      );
      if (!selectedAgent) {
        return { ok: false, error: `Combination ${i + 1}: pick an agent.` };
      }
      if (!agentServiceModeReady(selectedAgent)) {
        return {
          ok: false,
          error: `Combination ${i + 1}: ${agentReadinessMessage(selectedAgent)}`,
        };
      }
      const agentModel = buildAgentModel(r.picker, selectedAgent.needs_model);
      if (selectedAgent.needs_model && agentModel === null) {
        return {
          ok: false,
          error: `Combination ${i + 1}: ${selectedAgent.name} needs a model — pick one from the dropdown or use the custom-model fields.`,
        };
      }
      const n = Number.parseInt(r.nPerTask, 10);
      if (!Number.isFinite(n) || n < 1 || n > 100) {
        return {
          ok: false,
          error: `Combination ${i + 1}: samples per task must be between 1 and 100.`,
        };
      }
      const label = r.label.trim();
      if (label) {
        if (labels.has(label)) {
          return { ok: false, error: `Combination labels must be unique — "${label}" is repeated.` };
        }
        labels.add(label);
      }
      const combo: Combination = {
        agent_name: selectedAgent.name,
        agent_model: agentModel,
        n_per_task: n,
      };
      if (label) combo.label = label;
      out.push(combo);
    }
    return { ok: true, value: out };
  }

  function buildProviderSelection(): ProviderSelectionResult {
    const overrides: ProviderOverride[] = [];
    for (let i = 0; i < rows.length; i++) {
      const r = rows[i];
      const selectedAgent = agents.data?.items.find(
        (a) => a.name === r.picker.agentName,
      );
      if (!selectedAgent) continue;
      const override = buildProviderOverride(r.picker, selectedAgent.needs_model);
      if (override) overrides.push(override);
    }
    if (overrides.length === 0) return { ok: true, value: null };
    const first = overrides[0];
    const mismatch = overrides.find(
      (o) =>
        o.provider_connection_id !== first.provider_connection_id ||
        o.provider_model_id !== first.provider_model_id,
    );
    if (mismatch) {
      return {
        ok: false,
        error: (
          "BYO provider batches currently require one provider " +
          "connection and model across all combinations."
        ),
      };
    }
    return {
      ok: true,
      value: {
        ...first,
        manual_model: overrides.some((o) => o.manual_model),
      },
    };
  }

  const submit = async (): Promise<void> => {
    setLocalError(null);
    if (!name.trim()) {
      setLocalError("Name is required.");
      return;
    }
    if (!backend) {
      setLocalError("Pick a backend.");
      return;
    }
    if (subsetKind === "explicit") {
      if (parsed.error || parsed.ids.length === 0) {
        setLocalError("Paste at least one task id.");
        return;
      }
    } else {
      if (selectedBenchmarks.size === 0) {
        setLocalError("Pick at least one benchmark.");
        return;
      }
      if (subsetKind !== "all") {
        const n = Number.parseInt(subsetN, 10);
        if (!Number.isFinite(n) || n < 1) {
          setLocalError("Subset N must be a positive integer.");
          return;
        }
      }
      if (subsetKind === "random_n") {
        const seedN = Number.parseInt(subsetSeed, 10);
        if (!Number.isFinite(seedN) || seedN < 0 || seedN > 2 ** 31 - 1) {
          setLocalError("Seed must be a non-negative 32-bit integer.");
          return;
        }
      }
      if (matchedTaskCount === undefined) {
        setLocalError("Still counting matching tasks — try again in a moment.");
        return;
      }
      // Issue #28: with tag_filters active the SPA used to skip this
      // check because the local estimate was a pure upper bound; the
      // user could submit a batch that resolved to zero tasks and got
      // a confusing late 400. Now `matchedTaskCount` comes from the
      // real `/tasks/count` endpoint when `hasTagFilter`, so the gate
      // applies uniformly.
      if (matchedTaskCount === 0) {
        setLocalError(
          hasTagFilter
            ? "Tag filters narrow the slate to zero tasks — adjust the filters or unselect them."
            : "No tasks match the current benchmark + subset.",
        );
        return;
      }
    }

    const combos = buildCombinations();
    if (!combos.ok) {
      setLocalError(combos.error);
      return;
    }
    const providerSelection = buildProviderSelection();
    if (!providerSelection.ok) {
      setLocalError(providerSelection.error);
      return;
    }

    if (totalTrials !== undefined && totalTrials > FAN_OUT_CONFIRM_THRESHOLD && !confirmedLargeFanOut) {
      setLocalError(
        `This will launch ${totalTrials} trials. Tick the confirm box below, then submit again.`,
      );
      return;
    }

    const adv = buildAdvancedConfig(advanced);
    if (!adv.ok) {
      setLocalError(`Advanced options: ${adv.error}`);
      return;
    }

    const trial_config: Record<string, unknown> = { ...adv.value };
    const task_filter: CreateBatchBody["task_filter"] = {
      subset_kind: subsetKind,
    };
    if (subsetKind === "explicit") {
      task_filter.task_ids = parsed.ids;
    } else {
      task_filter.benchmark_ids = Array.from(selectedBenchmarks).sort();
      const tagPayload: Record<string, string[]> = {};
      for (const [k, vs] of Object.entries(tagFilters)) {
        if (vs.size > 0) tagPayload[k] = Array.from(vs).sort();
      }
      if (Object.keys(tagPayload).length > 0) {
        task_filter.tag_filters = tagPayload;
      }
      if (subsetKind !== "all") {
        task_filter.n = Number.parseInt(subsetN, 10);
      }
      if (subsetKind === "random_n") {
        task_filter.seed = Number.parseInt(subsetSeed, 10);
      }
    }

    const providerOverride = providerSelection.value;
    try {
      if (providerOverride?.manual_model) {
        await api.addProviderConnectionModel(
          providerOverride.provider_connection_id,
          { model_id: providerOverride.provider_model_id },
        );
      }
    } catch (e) {
      setLocalError(
        e instanceof Error ? e.message : "Could not save manual model id.",
      );
      return;
    }

    create.mutate({
      name: name.trim(),
      description: description.trim() || undefined,
      backend,
      task_filter,
      trial_config,
      combinations: combos.value,
      ...(providerOverride
        ? {
            provider_connection_id: providerOverride.provider_connection_id,
            provider_model_id: providerOverride.provider_model_id,
          }
        : {}),
    });
  };

  const submitButtonLabel = (() => {
    if (create.isPending) return "Submitting…";
    if (totalTrials === undefined || totalTrials === 0) {
      return "Submit batch";
    }
    return `Submit ${totalTrials} trial${totalTrials === 1 ? "" : "s"}`;
  })();

  let countSummary = "";
  if (subsetKind === "explicit") {
    countSummary = "";
  } else if (selectedBenchmarks.size === 0) {
    countSummary = "Pick at least one benchmark to count matching tasks.";
  } else if (hasTagFilter && exactCount.isLoading) {
    // Real count is in flight; show the running upper-bound while we
    // wait so the page stays responsive.
    countSummary =
      sumOfSelectedTasks !== undefined
        ? `Counting tasks under tag filters (up to ${sumOfSelectedTasks})…`
        : "Counting matching tasks…";
  } else if (matchedTaskCount === undefined) {
    countSummary = "Counting matching tasks…";
  } else if (matchedTaskCount === 0) {
    countSummary = hasTagFilter
      ? "Tag filters narrow the slate to zero tasks — adjust the filters or unselect them."
      : "No runnable tasks are provisioned for the selected benchmarks. Ask an admin or operator to run the deployment catalog provisioning step, or pick a different ready benchmark.";
  } else if (hasTagFilter) {
    countSummary = `${matchedTaskCount} task${matchedTaskCount === 1 ? "" : "s"} match the current benchmark + tag filters.`;
  } else {
    const benchN = selectedBenchmarks.size;
    countSummary = `${matchedTaskCount} task${matchedTaskCount === 1 ? "" : "s"} match across ${benchN} benchmark${benchN === 1 ? "" : "s"}.`;
  }

  return (
    <div className="space-y-6">
      <header>
        <h1 className="text-2xl font-bold text-slate-900">New batch</h1>
        <p className="mt-1 text-sm text-slate-500">
          Choose the tasks, choose one or more agent/model combinations,
          then review how many trials Loom will launch.
        </p>
      </header>

      <DocsCallout title="CLI/API equivalent" tone="info">
        <p>
          The form below builds the same task filter, agent, model, and retry
          shape as the CLI. Start with the oracle canary, then run a
          provider-backed smoke after adding a provider connection.
        </p>
        <CommandSnippet
          label="Smoke batch examples"
          command={[
            oracleSmokeBatchCommand(),
            "# provider-backed smoke after provider setup",
            providerSmokeBatchCommand("smoke-openai", "gpt-4o-mini"),
          ].join("\n\n")}
        />
      </DocsCallout>

      <div className="grid grid-cols-1 gap-6 xl:grid-cols-[minmax(0,1.3fr)_minmax(0,1fr)]">
        {/* LEFT column */}
        <div className="space-y-6">
          <Card>
            <Card.Header title="Identity" />
            <Card.Body className="space-y-4">
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
                  placeholder="What's this batch testing?"
                />
              </label>
              <label className="block max-w-sm">
                <FieldLabel hint="required">Backend</FieldLabel>
                <select
                  className="block w-full rounded-lg border border-slate-200 bg-white px-3 py-2 text-sm text-slate-800"
                  value={backend}
                  onChange={(e) => setBackend(e.target.value)}
                  disabled={backends.isPending}
                  aria-label="Backend"
                >
                  {(backends.data?.items ?? []).map((b) => (
                    <option key={b.name} value={b.name}>
                      {b.name}{b.available ? "" : " (no live worker)"}
                    </option>
                  ))}
                </select>
                {backend ? (
                  <Help>
                    {backends.data?.items.find((b) => b.name === backend)
                      ?.description ?? null}
                    {backends.data?.items.find((b) => b.name === backend)
                      ?.available === false ? (
                      <span className="mt-0.5 block text-amber-700">
                        No live worker advertises this backend right now —
                        batches will queue until one comes online.
                      </span>
                    ) : null}
                  </Help>
                ) : (
                  <Help>
                    The sandbox provider that runs each trial. Loom ships
                    drivers for docker, daytona, modal, and fake; entries
                    marked &quot;no live worker&quot; have a driver but no
                    worker advertising them right now.
                  </Help>
                )}
              </label>
            </Card.Body>
          </Card>

          <Card>
            <Card.Header
              title="Task selection"
              description="Choose the benchmark tasks to run. You can select whole benchmarks, narrow by tags, take a subset, or paste exact task IDs."
            />
            <Card.Body className="space-y-4">
              <fieldset
                className="block"
                disabled={subsetKind === "explicit"}
                aria-label="Benchmarks"
              >
                <FieldLabel hint={subsetKind === "explicit" ? "implied by ids" : "required"}>
                  Benchmarks
                </FieldLabel>
                <BenchmarkPicker
                  items={(benchmarks.data?.items ?? []) as BenchmarkItem[]}
                  loading={benchmarks.isPending}
                  selected={selectedBenchmarks}
                  onChange={setSelectedBenchmarks}
                />
                {!benchmarks.isPending &&
                (benchmarks.data?.items.length ?? 0) === 0 ? (
                  <Help>
                    No runnable benchmarks are provisioned in this
                    environment yet. Ask an admin or operator to run the
                    public beta catalog provisioning step from the deployment
                    runbook, then refresh this page.
                  </Help>
                ) : null}
              </fieldset>

              {subsetKind !== "explicit" && selectedBenchmarks.size > 0 ? (
                <TagFiltersCard
                  schema={tagSchema}
                  value={tagFilters}
                  onChange={setTagFilters}
                  loading={tagQueries.some((q) => q.isPending)}
                />
              ) : null}

              <fieldset className="space-y-2">
                <legend className="mb-1 block text-xs font-medium uppercase tracking-wider text-slate-500">
                  Subset
                </legend>
                {(
                  [
                    ["all", "All tasks in the benchmark"],
                    ["first_n", "First N by id"],
                    ["last_n", "Last N by id"],
                    ["random_n", "Random N (seeded)"],
                    ["explicit", "Explicit task ids (paste)"],
                  ] as Array<[SubsetKind, string]>
                ).map(([value, label]) => (
                  <label
                    key={value}
                    className="flex items-center gap-2 text-sm text-slate-700"
                  >
                    <input
                      type="radio"
                      name="subset"
                      value={value}
                      checked={subsetKind === value}
                      onChange={() => setSubsetKind(value)}
                      className="h-4 w-4 border-slate-300"
                    />
                    {label}
                  </label>
                ))}
              </fieldset>

              {subsetKind === "first_n" ||
              subsetKind === "last_n" ||
              subsetKind === "random_n" ? (
                <div className="grid grid-cols-1 gap-3 md:grid-cols-2">
                  <label className="block max-w-xs">
                    <FieldLabel>N</FieldLabel>
                    <Input
                      type="number"
                      min={1}
                      step={1}
                      value={subsetN}
                      onChange={(e) =>
                        setSubsetN(clampInt(e.target.value, 1, 1_000_000))
                      }
                      aria-label="Subset N"
                    />
                  </label>
                  {subsetKind === "random_n" ? (
                    <label className="block max-w-xs">
                      <FieldLabel hint="0 – 2^31 - 1">Seed</FieldLabel>
                      <div className="flex items-center gap-2">
                        <Input
                          type="number"
                          min={0}
                          step={1}
                          value={subsetSeed}
                          onChange={(e) => setSubsetSeed(e.target.value)}
                          aria-label="Seed"
                        />
                        <Button
                          variant="secondary"
                          size="sm"
                          onClick={() => setSubsetSeed(String(freshSeed()))}
                          title="Generate a new random seed for this random subset."
                        >
                          Reroll
                        </Button>
                      </div>
                    </label>
                  ) : null}
                </div>
              ) : null}

              {subsetKind === "explicit" ? (
                <div className="space-y-2">
                  <label className="block">
                    <FieldLabel hint="one per line or any accepted format">
                      Explicit task ids
                    </FieldLabel>
                    <Textarea
                      value={explicitText}
                      onChange={(e) => setExplicitText(e.target.value)}
                      rows={8}
                      placeholder={"HumanEval/0\nHumanEval/1\nHumanEval/2"}
                      aria-label="Explicit task ids"
                    />
                  </label>
                  <p className="text-xs">
                    {parsed.error ? (
                      <span className="text-red-700">{parsed.error}</span>
                    ) : parsed.ids.length === 0 ? (
                      <span className="text-slate-500">
                        Paste ids above to preview.
                      </span>
                    ) : (
                      <span className="text-slate-600">
                        Parsed {parsed.ids.length} id
                        {parsed.ids.length === 1 ? "" : "s"}.
                      </span>
                    )}
                  </p>
                  <details className="text-xs text-slate-600">
                    <summary className="cursor-pointer font-medium text-slate-700">
                      Accepted formats
                    </summary>
                    <ul className="ml-4 mt-2 list-disc space-y-0.5">
                      <li>One id per line</li>
                      <li>Comma / semicolon / pipe / tab / 2+ space separated</li>
                      <li>JSON array (single or double quotes)</li>
                      <li>Range shorthand: <code>HumanEval/0-4</code></li>
                      <li>Prefix shorthand: <code>HumanEval/0,1,2,3</code></li>
                      <li>Markdown bullets / numbered lists / single-col tables</li>
                      <li>CSV with header (first column wins)</li>
                      <li>Triple-backtick code fences (stripped)</li>
                      <li><code>#</code> comments (rest-of-line stripped)</li>
                      <li>URL prefixes <code>/api/v1/tasks/</code> + <code>/tasks/</code></li>
                    </ul>
                    <p className="mt-2">
                      Full rules: <code>docs/user-guide.md#pasting-task-ids</code>.
                    </p>
                  </details>
                </div>
              ) : null}

              {subsetKind !== "explicit" ? (
                <p
                  className="text-xs text-slate-500"
                  role="status"
                  aria-live="polite"
                  aria-busy={
                    selectedBenchmarks.size > 0 &&
                    (benchmarks.isPending || matchedTaskCount === undefined)
                  }
                >
                  {countSummary}
                </p>
              ) : null}
            </Card.Body>
          </Card>

          <Card>
            <details className="group">
              <summary className="flex cursor-pointer items-start gap-2 px-6 py-4 text-sm font-semibold text-slate-900">
                <span className="flex-1">
                  Advanced trial settings
                  <span className="ml-2 text-xs font-normal text-slate-500">
                    (defaults are sensible)
                  </span>
                  <span className="mt-1 block text-xs font-normal text-slate-500">
                    Shared settings applied to every trial unless a combination overrides them.
                  </span>
                </span>
                <span className="text-slate-400 transition-transform group-open:rotate-90">
                  ›
                </span>
              </summary>
              <div className="space-y-6 px-6 pb-6">
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
                        cached layer exists.
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
                        Default: off (env is deleted). Turn on for post-mortem inspection.
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
                        Default: off. When on, no grading happens.
                      </Help>
                    </span>
                  </label>
                  <label className="block max-w-sm">
                    <FieldLabel hint="default: task setting">Verifier env mode</FieldLabel>
                    <select
                      className="block w-full rounded-lg border border-slate-200 bg-white px-3 py-2 text-sm text-slate-800"
                      value={advanced.verifierEnvMode}
                      onChange={(e) =>
                        setAdv(
                          "verifierEnvMode",
                          e.target.value as AdvancedState["verifierEnvMode"],
                        )
                      }
                    >
                      <option value="">Use task default</option>
                      <option value="shared">shared</option>
                      <option value="separate">separate</option>
                    </select>
                  </label>
                </fieldset>

                <fieldset className="space-y-3">
                  <legend className="text-sm font-semibold text-slate-700">
                    Timeouts
                  </legend>
                  <p className="text-xs text-slate-500">
                    Override = absolute seconds (blank = use task default). Multiplier scales it
                    (1 = no change).
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
                      <Help>Total attempts including the first. 1 = no retry.</Help>
                    </label>
                    <div>
                      <div className="mb-1 flex items-baseline justify-between gap-2">
                        <span className="text-xs font-medium uppercase tracking-wider text-slate-500">
                          Retry on
                        </span>
                        <div className="flex gap-2 text-xs">
                          <button
                            type="button"
                            onClick={() =>
                              setAdv(
                                "retryOn",
                                new Set(RETRY_REASONS.map((r) => r.value)),
                              )
                            }
                            title="Retry on every listed transient failure reason."
                            className="font-medium text-accent hover:text-accent-hover"
                          >
                            Select all
                          </button>
                          <span className="text-slate-300">·</span>
                          <button
                            type="button"
                            onClick={() => setAdv("retryOn", new Set())}
                            title="Disable retry reasons so retries will not run."
                            className="font-medium text-slate-500 hover:text-slate-700"
                          >
                            Clear
                          </button>
                        </div>
                      </div>
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
                  <div className="space-y-2">
                    <p className="text-xs text-slate-600">
                      <span className="font-semibold text-slate-700">Backoff</span> is
                      how long the runner waits before each retry. Sleep =
                      min(base × multiplier<sup>attempt</sup>, max), then
                      randomised by jitter (0 = exact, 1 = ±100%). Defaults
                      (30s base, 2× per attempt, capped at 10min, 20% jitter)
                      give 30s → 60s → 120s → 240s.
                    </p>
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
                  </div>
                </fieldset>

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
                    <Help>Higher = scheduled first. Default 100.</Help>
                  </label>
                </fieldset>
              </div>
            </details>
          </Card>
        </div>

        {/* RIGHT column */}
        <div className="space-y-6">
          <div className="rounded-2xl bg-slate-50 p-5">
            <div className="mb-4 flex items-center justify-between">
              <div>
                <h2 className="text-sm font-semibold text-slate-800">
                  Agent/model combinations
                </h2>
                <p className="mt-0.5 text-xs text-slate-500">
                  Each row runs the selected tasks with its own agent, model,
                  and samples-per-task count.
                </p>
              </div>
              <Button
                variant="secondary"
                size="sm"
                onClick={addRow}
                disabled={rows.length >= MAX_COMBINATIONS}
                title="Add another agent/model combination to run on the same task slate."
              >
                + Add combination
              </Button>
            </div>
            <div className="space-y-4">
              {rows.map((r, i) => (
                <div
                  key={i}
                  className="space-y-3 rounded-xl border border-slate-200 bg-white p-4 shadow-sm"
                >
                  <div className="flex items-center justify-between">
                    <span className="text-xs font-semibold uppercase tracking-wider text-slate-500">
                      Combination {i + 1}
                    </span>
                    {rows.length > 1 ? (
                      <Button
                        variant="secondary"
                        size="sm"
                        onClick={() => removeRow(i)}
                        title={`Remove combination ${i + 1} from this batch.`}
                      >
                        Remove
                      </Button>
                    ) : null}
                  </div>
                  <AgentModelPicker
                    value={r.picker}
                    onChange={(v) => updateRow(i, { picker: v })}
                    disabled={create.isPending}
                  />
                  <div className="grid grid-cols-2 gap-3">
                    <label className="block">
                      <FieldLabel hint="1 – 100">Samples per task</FieldLabel>
                      <Input
                        type="number"
                        min={1}
                        max={100}
                        step={1}
                        value={r.nPerTask}
                        onChange={(e) =>
                          updateRow(i, {
                            nPerTask: clampInt(e.target.value, 1, 100),
                          })
                        }
                        onBlur={() =>
                          updateRow(i, {
                            nPerTask: clampInt(r.nPerTask, 1, 100) || "1",
                          })
                        }
                        aria-label={`Samples per task (combination ${i + 1})`}
                      />
                    </label>
                    <label className="block">
                      <FieldLabel hint="optional">Label</FieldLabel>
                      <Input
                        value={r.label}
                        onChange={(e) => updateRow(i, { label: e.target.value })}
                        placeholder="auto"
                        aria-label={`Label (combination ${i + 1})`}
                      />
                    </label>
                  </div>
                </div>
              ))}
            </div>
            {rows.length >= MAX_COMBINATIONS ? (
              <p className="mt-3 text-xs text-amber-700">
                Cap is {MAX_COMBINATIONS} combinations per batch.
              </p>
            ) : null}
          </div>
        </div>
      </div>

      {/* Confirmation banner — hidden when totalTrials is 0 since the
          inline countSummary above already explains why (no tasks
          registered / no parsed ids), and a "Will launch 0 trials"
          banner just adds noise to an already-actionable empty state. */}
      {totalTrials !== undefined && totalTrials > 0 ? (
        <div className="rounded-xl border border-slate-200 bg-white px-4 py-3 text-sm">
          <p className="text-slate-700">
            Will launch{" "}
            <span className="font-semibold text-slate-900">
              {totalTrials} trial{totalTrials === 1 ? "" : "s"}
            </span>{" "}
            (matched {matchedTaskCount ?? 0} task
            {matchedTaskCount === 1 ? "" : "s"} × Σ n_per_task ={" "}
            {sumNPerTask}).
          </p>
          <div className="mt-2 flex flex-wrap gap-2">
            {rows.map((r, i) => {
              const sel = agents.data?.items.find(
                (a) => a.name === r.picker.agentName,
              );
              const modelTxt =
                sel && !sel.needs_model
                  ? "(no model)"
                  : r.picker.modelProvider && r.picker.modelName
                    ? `${r.picker.modelProvider}/${r.picker.modelName}`
                    : "(no model)";
              const lbl = r.label.trim() || `combo${i + 1}`;
              return (
                <span
                  key={i}
                  className="inline-flex items-center gap-1 rounded-md border border-slate-200 bg-slate-50 px-2 py-1 text-xs text-slate-700"
                >
                  <span className="font-semibold">{lbl}</span>
                  <span className="text-slate-500">
                    {sel?.name ?? "?"} · {modelTxt} · n={r.nPerTask}
                  </span>
                </span>
              );
            })}
          </div>
        </div>
      ) : null}

      {showLargeFanOutConfirm ? (
        <label className="flex items-start gap-2 rounded-lg border border-amber-200 bg-amber-50 px-3 py-2 text-sm text-amber-800">
          <input
            type="checkbox"
            checked={confirmedLargeFanOut}
            onChange={(e) => setConfirmedLargeFanOut(e.target.checked)}
            className="mt-0.5 h-4 w-4 rounded border-amber-300"
          />
          <span>
            I understand this batch will launch {totalTrials} trials.
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
          onClick={() => {
            void submit();
          }}
          disabled={create.isPending || totalTrials === 0}
          title={
            totalTrials === 0
              ? "Pick benchmarks and a valid agent/model combination before submitting."
              : `Create this batch with ${totalTrials} planned trial${totalTrials === 1 ? "" : "s"}.`
          }
        >
          {submitButtonLabel}
        </Button>
      </div>
    </div>
  );
}
