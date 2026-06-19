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
import { DiagnosticPanel } from "../components/DiagnosticPanel";
import ErrorState from "../components/ErrorState";
import LoadingState from "../components/LoadingState";
import { StatCard } from "../components/StatCard";
import { StatusPill } from "../components/StatusPill";
import { useAdaptivePolling } from "../hooks/useAdaptivePolling";
import { humanizeTaskFilter } from "../lib/humanizeTaskFilter";
import { humanizeTrialConfig } from "../lib/humanizeTrialConfig";
import { modelLabel } from "../lib/modelLabel";
import { batchStateVariant, trialStateVariant } from "../lib/statusVariant";

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

  const cancel = useMutation({
    mutationFn: () => api.cancelBatch(batchId!),
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

          <div className="grid grid-cols-2 gap-3 md:grid-cols-3 lg:grid-cols-5">
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
              label="Cost"
              value={`$${c.total_cost_usd.toFixed(4)}`}
            />
            <StatCard
              label="Created"
              value={c.created_at.slice(0, 16).replace("T", " ")}
            />
            <StatCard
              label="Finished"
              value={c.finished_at?.slice(0, 16).replace("T", " ") ?? "—"}
            />
          </div>

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
        </Card.Body>
      </Card>

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
