/**
 * Batch detail — one batch's aggregate stats + per-state trial
 * counts + the original filter/config that submitted it. Live-polls
 * while the batch is active, stops once terminal.
 */
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { Link, useParams } from "react-router-dom";

import { api } from "../api/client";
import { Button } from "../components/Button";
import { Card } from "../components/Card";
import CommandSnippet from "../components/CommandSnippet";
import { DebugEvidenceCard } from "../components/DebugEvidenceCard";
import { DiagnosisCard } from "../components/DiagnosisCard";
import { DiagnosticPanel } from "../components/DiagnosticPanel";
import ErrorState from "../components/ErrorState";
import LoadingState from "../components/LoadingState";
import { StatCard } from "../components/StatCard";
import { StatusPill } from "../components/StatusPill";
import { useAdaptivePolling } from "../hooks/useAdaptivePolling";
import { formatLocalDateTime } from "../lib/dateTime";
import { humanizeTaskFilter } from "../lib/humanizeTaskFilter";
import { humanizeTrialConfig } from "../lib/humanizeTrialConfig";
import { modelLabel } from "../lib/modelLabel";
import { ownershipLabel } from "../lib/ownership";
import { provenanceLabel } from "../lib/provenanceLabel";
import { batchInspectionCommands } from "../lib/quickstartSnippets";
import { batchStateVariant, trialStateVariant } from "../lib/statusVariant";
import { formatTokenUsage } from "../lib/tokenUsage";
import {
  formatUsageCost,
  usageCostStatus,
  usageEstimateConfidence,
} from "../lib/usageCost";

function resultStatusVariant(s: string): "success" | "warning" | "failed" | "cancelled" | "neutral" {
  switch (s) {
    case "succeeded":
      return "success";
    case "partial_failed":
      return "warning";
    case "all_failed":
      return "failed";
    case "cancelled":
      return "cancelled";
    default:
      return "neutral";
  }
}

const ACTIVE_STATES = new Set(["submitted", "running"]);

function comboSummary(
  combo: {
    label?: string | null;
    agent_name: string;
    agent_model: unknown;
    n_per_task: number;
  },
  index: number,
): string {
  const label = combo.label || `combo${index + 1}`;
  return `${label} / ${combo.agent_name} / ${modelLabel(combo.agent_model)} / n=${combo.n_per_task}`;
}

function scoreText(value: number | null): string {
  return value != null ? value.toFixed(3) : "—";
}

export default function BatchDetail(): JSX.Element {
  const { batchId } = useParams<{ batchId: string }>();
  const queryClient = useQueryClient();

  const polling = useAdaptivePolling({
    baseIntervalMs: 5_000,
    minIntervalMs: 3_000,
    maxIntervalMs: 60_000,
    hiddenBehavior: "pause",
    blurBehavior: "slow",
  });

  const query = useQuery({
    queryKey: ["batch", batchId],
    queryFn: () => api.getBatch(batchId!),
    enabled: !!batchId,
    refetchInterval: (q) => {
      const data = q.state.data as { state: string } | undefined;
      if (!data || !ACTIVE_STATES.has(data.state)) return false;
      return polling.refetchInterval;
    },
  });

  const diagnosticsQuery = useQuery({
    queryKey: ["batch-diagnostics", batchId],
    queryFn: async () => {
      const [diagnosis, debugEvidence] = await Promise.all([
        api.getBatchDiagnosis(batchId!),
        api.getBatchDebug(batchId!),
      ]);
      return { diagnosis, debugEvidence };
    },
    enabled: false,
  });

  const cancel = useMutation({
    mutationFn: () => api.cancelBatch(batchId!),
    onSuccess: () =>
      queryClient.invalidateQueries({ queryKey: ["batch", batchId] }),
  });

  const rerunFailed = useMutation({
    mutationFn: () => api.rerunFailedBatch(batchId!),
    onSuccess: () =>
      queryClient.invalidateQueries({ queryKey: ["batch", batchId] }),
  });

  if (!batchId) {
    return <ErrorState error={new Error("missing batchId")} />;
  }
  if (query.isPending) return <LoadingState />;
  if (query.isError) return <ErrorState error={query.error} />;
  if (!query.data) return <ErrorState error={new Error("no data")} />;
  const c = query.data;
  const taskSummary = humanizeTaskFilter(c.task_filter, {
    matchedTaskCount: c.expected_trial_count,
  });
  const trialConfigSummary = humanizeTrialConfig(c.trial_config);
  const rerunnableFailedCount = c.rerunnable_failed_count ?? 0;
  const effectiveSucceeded = c.effective_trial_summary?.succeeded ?? 0;
  const effectiveFailed = c.effective_trial_summary?.failed ?? 0;
  const benchmarkSummary = Array.isArray(c.benchmark_summary)
    ? c.benchmark_summary
    : [];
  const showBenchmarkSummary = benchmarkSummary.length > 1;
  const provenance = Array.isArray(c.source_provenance)
    ? c.source_provenance
    : [];
  const diagnostics = [
    { title: "task_filter", data: c.task_filter, expanded: true },
    { title: "trial_config", data: c.trial_config, expanded: true },
    ...(c.combinations && c.combinations.length > 0
      ? [{ title: "combinations", data: c.combinations }]
      : []),
    ...(c.fanout_errors.length > 0
      ? [{ title: "fanout_errors", data: c.fanout_errors }]
      : []),
  ];
  const hasCostProjection =
    "estimated_cost_usd" in c || "cost_status" in c || "cost_currency" in c;
  const hasEffectiveCostProjection =
    "effective_estimated_cost_usd" in c ||
    "effective_cost_status" in c ||
    "effective_cost_currency" in c;
  const noCallTrialCount = c.no_call_trial_count ?? 0;
  const showNoCallWarning =
    c.llm_evidence_status === "no_calls_invalid" ||
    c.llm_evidence_status === "partial_no_calls";
  const noCallWarningTitle =
    c.llm_evidence_status === "partial_no_calls" ||
    (c.llm_calls_count ?? 0) > 0
      ? "Some terminal trials made no LLM calls"
      : "No LLM calls recorded";
  const diagnosis = c.diagnosis ?? diagnosticsQuery.data?.diagnosis ?? null;
  const debugEvidence =
    c.debug_evidence ?? diagnosticsQuery.data?.debugEvidence ?? null;
  const hasDiagnostics = Boolean(diagnosis || debugEvidence);

  return (
    <div className="space-y-6">
      <div>
        <Link
          to="/monitor?view=batches"
          title="Return to the batch monitor table."
          className="text-xs font-medium text-slate-500 hover:text-slate-700"
        >
          ← All batches
        </Link>
      </div>

      <Card>
        <Card.Body className="space-y-5">
          <div className="flex flex-wrap items-start justify-between gap-3">
            <div className="min-w-0">
              <h1 className="text-2xl font-bold text-slate-900">{c.name}</h1>
              {c.description ? (
                <p className="mt-1 text-sm text-slate-500">{c.description}</p>
              ) : null}
              <p className="mt-2 font-mono text-xs text-slate-500">
                id = {c.id}
              </p>
            </div>
            <div className="flex flex-wrap items-center gap-2">
              <StatusPill variant={batchStateVariant(c.state)}>
                {c.state}
              </StatusPill>
              {c.result_status ? (
                <StatusPill variant={resultStatusVariant(c.result_status)}>
                  {c.result_status}
                </StatusPill>
              ) : null}
              {c.backend ? (
                <span className="inline-flex items-center rounded-md border border-slate-200 bg-slate-50 px-2 py-0.5 text-xs font-medium text-slate-700">
                  backend: {c.backend}
                </span>
              ) : null}
            </div>
          </div>

          {c.combinations && c.combinations.length > 0 ? (
            <div className="flex flex-wrap gap-2">
              {c.combinations.map((combo, i) => {
                const lbl = combo.label || `combo${i + 1}`;
                const modelTxt = combo.agent_model
                  ? `${combo.agent_model.provider}/${combo.agent_model.name}`
                  : "(no model)";
                return (
                  <span
                    key={i}
                    className="inline-flex items-center gap-1 rounded-md border border-slate-200 bg-slate-50 px-2 py-1 text-xs text-slate-700"
                  >
                    <span className="font-semibold">{lbl}</span>
                    <span className="text-slate-500">
                      {combo.agent_name} · {modelTxt} · n={combo.n_per_task}
                    </span>
                  </span>
                );
              })}
            </div>
          ) : null}

          {c.failure_reason ? (
            <div className="rounded-md border border-red-200 bg-red-50 px-3 py-2 text-sm text-red-900">
              <div className="font-semibold">{c.failure_reason}</div>
              {c.failure_message ? (
                <div className="mt-1 break-words text-red-800">
                  {c.failure_message}
                </div>
              ) : null}
              {c.fanout_errors.length > 1 ? (
                <div className="mt-1 text-xs text-red-700">
                  {c.fanout_errors.length} fan-out submissions failed.
                </div>
              ) : null}
            </div>
          ) : null}

          {showNoCallWarning ? (
            <div className="rounded-md border border-red-200 bg-red-50 px-3 py-2 text-sm text-red-900">
              <div className="font-semibold">{noCallWarningTitle}</div>
              <div className="mt-1 text-red-800">
                {noCallTrialCount} terminal model-backed trials finished
                without reaching the gateway; this is invalid benchmark
                evidence.
              </div>
            </div>
          ) : null}

          <div className="grid grid-cols-2 gap-3 md:grid-cols-4 lg:grid-cols-8">
            <StatCard
              label="Owner"
              value={ownershipLabel(c)}
            />
            <StatCard
              label="Visibility"
              value={`${c.visibility ?? "team"} / ${c.share_status ?? "pending_scan"}`}
            />
            <StatCard label="Expected" value={c.expected_trial_count} />
            <StatCard
              label="Reward (avg)"
              value={
                c.aggregate_reward != null
                  ? c.aggregate_reward.toFixed(3)
                  : "—"
              }
            />
            <StatCard
              label="LLM calls"
              value={c.llm_calls_count}
            />
            <StatCard
              label="Tokens"
              value={formatTokenUsage(
                c.total_prompt_tokens,
                c.total_completion_tokens,
              )}
            />
            {hasCostProjection ? (
              <>
                <StatCard
                  label="Estimated LLM cost"
                  value={formatUsageCost(c)}
                />
                <StatCard
                  label="Cost status"
                  value={usageCostStatus(c)}
                />
                <StatCard
                  label="Usage confidence"
                  value={usageEstimateConfidence(c)}
                />
              </>
            ) : null}
            <StatCard
              label="Created"
              value={formatLocalDateTime(c.created_at)}
            />
            <StatCard
              label="Finished"
              value={formatLocalDateTime(c.finished_at)}
            />
          </div>

          {provenance.length > 0 ? (
            <div className="rounded-md border border-slate-200 bg-slate-50 px-3 py-2 text-sm text-slate-700">
              <div className="font-semibold text-slate-900">Provenance</div>
              <ul className="mt-1 space-y-1 text-xs text-slate-600">
                {provenance.map((item, index) => (
                  <li key={index}>{provenanceLabel(item)}</li>
                ))}
              </ul>
            </div>
          ) : null}

          <div className="grid gap-3 lg:grid-cols-2">
            {batchInspectionCommands(c.id).map((command) => (
              <CommandSnippet key={command} label="Batch CLI" command={command} />
            ))}
          </div>

          {!hasDiagnostics ? (
            <div className="space-y-2">
              <Button
                variant="secondary"
                onClick={() => void diagnosticsQuery.refetch()}
                disabled={diagnosticsQuery.isFetching}
                title="Fetch batch diagnosis and debug evidence for this batch."
              >
                {diagnosticsQuery.isFetching
                  ? "Loading diagnostics..."
                  : "Load diagnostics"}
              </Button>
              {diagnosticsQuery.isError ? (
                <ErrorState error={diagnosticsQuery.error} />
              ) : null}
            </div>
          ) : null}

          {ACTIVE_STATES.has(c.state) ? (
            <div className="space-y-2">
              <Button
                variant="danger"
                onClick={() => cancel.mutate()}
                disabled={cancel.isPending}
                title="Stop queued or running trials in this batch."
              >
                {cancel.isPending ? "Cancelling…" : "Cancel batch"}
              </Button>
              {cancel.isError ? <ErrorState error={cancel.error} /> : null}
            </div>
          ) : null}

          {!ACTIVE_STATES.has(c.state) &&
          rerunnableFailedCount > 0 &&
          effectiveFailed > 0 ? (
            <div className="rounded-md border border-amber-200 bg-amber-50 px-3 py-3 text-sm text-amber-950">
              <div className="flex flex-wrap items-center justify-between gap-3">
                <div>
                  <div className="font-semibold">
                    {rerunnableFailedCount} failed case
                    {rerunnableFailedCount === 1 ? "" : "s"} can be rerun
                  </div>
                  <div className="mt-1 text-xs text-amber-800">
                    Creates a linked batch for transient gateway failures and
                    keeps original results available.
                  </div>
                </div>
                <Button
                  variant="secondary"
                  onClick={() => rerunFailed.mutate()}
                  disabled={rerunFailed.isPending}
                  title="Create a linked batch that reruns only the failed transient cases."
                >
                  {rerunFailed.isPending
                    ? "Queueing rerun…"
                    : "Rerun failed cases"}
                </Button>
              </div>
              {rerunFailed.isError ? (
                <ErrorState error={rerunFailed.error} />
              ) : null}
              {rerunFailed.data ? (
                <div className="mt-2 text-xs text-amber-800">
                  <Link
                    to={`/batches/${rerunFailed.data.batch_id}`}
                    className="font-semibold text-accent hover:text-accent-hover"
                    title="Open the linked rerun batch."
                  >
                    Rerun queued
                  </Link>{" "}
                  for {rerunFailed.data.rerun_target_count} failed case
                  {rerunFailed.data.rerun_target_count === 1 ? "" : "s"}.
                </div>
              ) : null}
            </div>
          ) : null}

          {c.rerun_batches.length > 0 ? (
            <div className="rounded-md border border-slate-200 bg-slate-50 px-3 py-2 text-sm text-slate-700">
              <div className="font-semibold text-slate-900">
                Effective result after linked reruns
              </div>
              <div className="mt-1 text-xs text-slate-600">
                {effectiveSucceeded} succeeded, {effectiveFailed} failed ·
                reward {c.effective_aggregate_reward != null
                  ? c.effective_aggregate_reward.toFixed(3)
                  : "—"} · {c.effective_llm_calls_count} LLM calls · tokens{" "}
                {formatTokenUsage(
                  c.effective_total_prompt_tokens,
                  c.effective_total_completion_tokens,
                )}
                {hasEffectiveCostProjection
                  ? ` · cost ${formatUsageCost({
                      estimated_cost_usd: c.effective_estimated_cost_usd,
                      cost_currency: c.effective_cost_currency,
                      cost_status: c.effective_cost_status,
                      pricing_modes: c.effective_pricing_modes,
                    })} (${usageCostStatus({
                      estimated_cost_usd: c.effective_estimated_cost_usd,
                      cost_currency: c.effective_cost_currency,
                      cost_status: c.effective_cost_status,
                      pricing_modes: c.effective_pricing_modes,
                      usage_estimate_confidence:
                        c.effective_usage_estimate_confidence,
                    })})`
                  : ""}
              </div>
            </div>
          ) : null}
        </Card.Body>
      </Card>

      <DiagnosisCard
        diagnosis={diagnosis}
        onRerunFailed={
          rerunnableFailedCount > 0 ? () => rerunFailed.mutate() : undefined
        }
        rerunDisabled={rerunFailed.isPending}
      />
      <DebugEvidenceCard evidence={debugEvidence} />

      {showBenchmarkSummary ? (
        <Card>
          <Card.Header
            title="Benchmark results"
            description="Per-benchmark score and platform failure counts for this multi-benchmark batch."
          />
          <Card.Body className="p-0">
            <div className="overflow-x-auto">
              <table className="min-w-full divide-y divide-slate-200 text-sm">
                <thead>
                  <tr className="bg-slate-50/50">
                    <th className="px-4 py-3 text-left text-xs font-medium uppercase tracking-wider text-slate-500">
                      Benchmark
                    </th>
                    <th className="px-4 py-3 text-left text-xs font-medium uppercase tracking-wider text-slate-500">
                      Score
                    </th>
                    <th className="px-4 py-3 text-left text-xs font-medium uppercase tracking-wider text-slate-500">
                      Completed
                    </th>
                    <th className="px-4 py-3 text-left text-xs font-medium uppercase tracking-wider text-slate-500">
                      Platform failures
                    </th>
                    <th className="px-4 py-3 text-left text-xs font-medium uppercase tracking-wider text-slate-500">
                      Trial states
                    </th>
                  </tr>
                </thead>
                <tbody className="divide-y divide-slate-100">
                  {benchmarkSummary.map((row) => {
                    const succeeded = row.trial_summary.succeeded ?? 0;
                    const running =
                      (row.trial_summary.queued ?? 0) +
                      (row.trial_summary.claimed ?? 0) +
                      (row.trial_summary.running ?? 0);
                    return (
                      <tr key={row.benchmark_id ?? row.display_name}>
                        <td className="px-4 py-3">
                          <div className="font-medium text-slate-900">
                            {row.display_name}
                          </div>
                          {row.benchmark_id ? (
                            <div className="mt-0.5 font-mono text-xs text-slate-500">
                              {row.benchmark_id}
                            </div>
                          ) : null}
                        </td>
                        <td className="px-4 py-3">
                          <div className="font-semibold text-slate-900">
                            {scoreText(row.aggregate_reward)}
                          </div>
                          <div className="mt-0.5 text-xs text-slate-500">
                            {row.metric_name}
                          </div>
                        </td>
                        <td className="px-4 py-3 text-slate-700">
                          {row.completed_trial_count} /{" "}
                          {row.expected_trial_count}
                        </td>
                        <td className="px-4 py-3 text-slate-700">
                          {row.platform_failed_count} failed
                        </td>
                        <td className="px-4 py-3 text-slate-700">
                          {succeeded} succeeded
                          {running > 0 ? ` · ${running} active` : ""}
                        </td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>
          </Card.Body>
        </Card>
      ) : null}

      <Card>
        <details className="group">
          <summary
            className="flex cursor-pointer items-center gap-2 border-b border-slate-200 px-5 py-4 text-sm font-semibold text-slate-800"
            title="Expand the per-state trial counts for this batch."
          >
            <span className="flex-1">Show trials in this batch</span>
            <Link
              to={`/monitor?view=trials&batch_id=${c.id}`}
              className="text-xs font-medium text-accent hover:text-accent-hover"
              title="Open the Monitor filtered to this batch's trials."
              onClick={(e) => e.stopPropagation()}
            >
              Open in Monitor →
            </Link>
            <span className="text-slate-400 transition-transform group-open:rotate-90">
              ›
            </span>
          </summary>
          <Card.Body className="p-0">
            <div className="overflow-x-auto">
              <table className="min-w-full divide-y divide-slate-200 text-sm">
                <thead>
                  <tr className="bg-slate-50/50">
                    <th className="px-4 py-3 text-left text-xs font-medium uppercase tracking-wider text-slate-500">
                      State
                    </th>
                    <th className="px-4 py-3 text-left text-xs font-medium uppercase tracking-wider text-slate-500">
                      Count
                    </th>
                  </tr>
                </thead>
                <tbody className="divide-y divide-slate-100">
                  {Object.entries(c.trial_summary).map(([state, n]) => (
                    <tr key={state}>
                      <td className="px-4 py-3">
                        <StatusPill variant={trialStateVariant(state)}>
                          {state}
                        </StatusPill>
                      </td>
                      <td className="px-4 py-3 text-slate-700">{n}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </Card.Body>
        </details>
      </Card>

      <Card>
        <Card.Header
          title="Run plan"
          description="What this batch will run, how often each task runs, and which shared settings apply to every trial."
        />
        <Card.Body className="space-y-5">
          <div className="grid gap-4 md:grid-cols-2">
            <div>
              <p className="text-xs font-medium uppercase tracking-wider text-slate-500">
                Task selection
              </p>
              <p className="mt-1 text-sm font-semibold text-slate-900">
                {taskSummary.primary}
              </p>
              {taskSummary.details.length > 0 ? (
                <ul className="mt-2 space-y-1 text-xs text-slate-600">
                  {taskSummary.details.map((detail) => (
                    <li key={detail}>{detail}</li>
                  ))}
                </ul>
              ) : null}
            </div>

            <div>
              <p className="text-xs font-medium uppercase tracking-wider text-slate-500">
                Agent/model combinations
              </p>
              {c.combinations && c.combinations.length > 0 ? (
                <ul className="mt-1 space-y-1 text-sm font-semibold text-slate-900">
                  {c.combinations.map((combo, i) => (
                    <li key={`${combo.label ?? "combo"}-${i}`}>
                      {comboSummary(combo, i)}
                    </li>
                  ))}
                </ul>
              ) : (
                <p className="mt-1 text-sm font-semibold text-slate-900">
                  Single default combination
                </p>
              )}
            </div>

            <div>
              <p className="text-xs font-medium uppercase tracking-wider text-slate-500">
                Shared trial settings
              </p>
              <p className="mt-1 text-sm font-semibold text-slate-900">
                {trialConfigSummary.primary}
              </p>
              {trialConfigSummary.items.length > 0 ? (
                <ul className="mt-2 space-y-1 text-xs text-slate-600">
                  {trialConfigSummary.items.map((item) => (
                    <li key={item}>{item}</li>
                  ))}
                </ul>
              ) : null}
            </div>

            <div>
              <p className="text-xs font-medium uppercase tracking-wider text-slate-500">
                Execution backend
              </p>
              <p className="mt-1 text-sm font-semibold text-slate-900">
                {c.backend || "Default backend"}
              </p>
              <p className="mt-2 text-xs text-slate-600">
                The backend decides which worker pool and runtime adapter runs
                the trials.
              </p>
            </div>
          </div>

          {taskSummary.diagnostics.length > 0 ||
          trialConfigSummary.diagnostics.length > 0 ? (
            <div className="rounded-md border border-amber-200 bg-amber-50 px-3 py-2 text-xs text-amber-900">
              {[...taskSummary.diagnostics, ...trialConfigSummary.diagnostics]
                .join("; ")}
            </div>
          ) : null}

          <DiagnosticPanel blocks={diagnostics} />
        </Card.Body>
      </Card>
    </div>
  );
}
