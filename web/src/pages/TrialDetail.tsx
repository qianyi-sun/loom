/**
 * Per-trial detail: header card with summary stats, trajectory
 * viewer with action-type-pill rows + JSON expansion, download buttons
 * for ATIF and the raw trajectory. Live-polls while the trial is
 * non-terminal; pauses cleanly once done.
 */
import { useQuery } from "@tanstack/react-query";
import { useState } from "react";
import { Link, useParams } from "react-router-dom";

import { api } from "../api/client";
import type { components } from "../api/schema";
import { Button } from "../components/Button";
import { Card } from "../components/Card";
import CommandSnippet from "../components/CommandSnippet";
import { DebugEvidenceCard } from "../components/DebugEvidenceCard";
import { DiagnosisCard } from "../components/DiagnosisCard";
import DocsCallout from "../components/DocsCallout";
import ErrorState from "../components/ErrorState";
import EventTimeline from "../components/EventTimeline";
import LoadingState from "../components/LoadingState";
import { StatCard } from "../components/StatCard";
import { StatusPill } from "../components/StatusPill";
import { useAdaptivePolling } from "../hooks/useAdaptivePolling";
import { useTrialEventStream } from "../hooks/useTrialEventStream";
import { formatLocalDateTime } from "../lib/dateTime";
import { humanizeFailureReason } from "../lib/humanizeFailureReason";
import { modelLabel } from "../lib/modelLabel";
import { ownershipLabel } from "../lib/ownership";
import { provenanceLabel } from "../lib/provenanceLabel";
import { trialDownloadCommands } from "../lib/quickstartSnippets";
import { trialStateVariant } from "../lib/statusVariant";
import { formatTokenUsage } from "../lib/tokenUsage";
import {
  formatUsageCost,
  usageCostStatus,
  usageEstimateConfidence,
} from "../lib/usageCost";

type TrajEvent = components["schemas"]["TrajectoryEvent"];
type TrialArtifact = components["schemas"]["TrialDetail"]["artifacts"][number];

const ACTIVE_TRIAL_STATES = new Set([
  "queued",
  "submitted",
  "claimed",
  "running",
]);

function artifactLabel(artifact: TrialArtifact): string {
  return artifact.key || artifact.step_name || "artifact";
}

function artifactDownloadName(artifact: TrialArtifact): string {
  const label = artifactLabel(artifact).replace(/\/+$/, "");
  return label.split("/").pop() || "artifact";
}

function artifactShareLabel(artifact: TrialArtifact): {
  label: string;
  className: string;
} {
  if (artifact.share_status === "blocked") {
    return {
      label: "Sharing blocked",
      className: "border-amber-200 bg-amber-50 text-amber-800",
    };
  }
  if (artifact.share_status === "shared") {
    return {
      label: "Shared",
      className: "border-emerald-200 bg-emerald-50 text-emerald-700",
    };
  }
  return {
    label: "Share scan pending",
    className: "border-slate-200 bg-slate-50 text-slate-600",
  };
}

function formatBytes(size: number): string {
  if (!Number.isFinite(size) || size <= 0) return "0 B";
  const units = ["B", "KB", "MB", "GB"];
  let value = size;
  let unit = 0;
  while (value >= 1024 && unit < units.length - 1) {
    value /= 1024;
    unit += 1;
  }
  if (unit === 0) return `${Math.round(value)} ${units[unit]}`;
  return `${value.toFixed(value >= 10 ? 1 : 2)} ${units[unit]}`;
}

function trialOutcome(
  trial: components["schemas"]["TrialDetail"],
): { label: string; description: string } {
  if (ACTIVE_TRIAL_STATES.has(trial.state)) {
    return {
      label: "Trial is active",
      description: "A worker has not produced a final platform outcome yet.",
    };
  }
  if (trial.state === "succeeded") {
    return {
      label: "Trial succeeded",
      description:
        "The platform run finished and persisted its final artifacts. The evaluator score below is separate.",
    };
  }
  if (trial.state === "failed" || trial.state === "failed_terminal") {
    return {
      label: "Trial failed",
      description:
        "The platform could not complete this trial successfully. Use the failure reason and trajectory log for diagnosis.",
    };
  }
  if (trial.state === "cancelled") {
    return {
      label: "Trial cancelled",
      description: "The trial was stopped before normal completion.",
    };
  }
  return {
    label: "Trial outcome unknown",
    description: "The service returned a state this UI does not yet describe.",
  };
}

function TrialHeader({
  trial,
}: {
  trial: components["schemas"]["TrialDetail"];
}): JSX.Element {
  const outcome = trialOutcome(trial);
  const failure = trial.failure_reason
    ? humanizeFailureReason(trial.failure_reason)
    : null;
  const provenance = Array.isArray(trial.source_provenance)
    ? trial.source_provenance
    : [];
  const firstArtifactKey = trial.artifacts[0]?.key ?? null;
  const hasCostProjection =
    "estimated_cost_usd" in trial ||
    "cost_status" in trial ||
    "cost_currency" in trial;

  return (
    <Card>
      <Card.Body className="space-y-5">
        <div className="flex flex-wrap items-start justify-between gap-3">
          <div className="min-w-0">
            <p className="text-xs uppercase tracking-wider text-slate-400">
              Trial
            </p>
            <h1 className="mt-1 font-mono text-xl font-semibold text-slate-900 break-all">
              {trial.id}
            </h1>
            <p className="mt-2 text-sm text-slate-500">
              Task{" "}
              <code className="rounded bg-slate-100 px-1.5 py-0.5 font-mono text-xs text-slate-700">
                {trial.task_id}
              </code>
            </p>
          </div>
          <StatusPill variant={trialStateVariant(trial.state)}>
            {trial.state}
          </StatusPill>
        </div>

        <div className="rounded-xl border border-slate-200 bg-slate-50/70 px-4 py-3">
          <p className="text-xs font-semibold uppercase tracking-wider text-slate-500">
            Platform outcome
          </p>
          <p className="mt-1 text-sm font-semibold text-slate-900">
            {outcome.label}
          </p>
          <p className="mt-1 text-xs text-slate-600">{outcome.description}</p>
        </div>

        <div className="grid grid-cols-2 gap-3 md:grid-cols-3 lg:grid-cols-5">
          <StatCard
            label="Owner"
            value={ownershipLabel(trial)}
          />
          <StatCard
            label="Visibility"
            value={`${trial.visibility ?? "team"} / ${trial.share_status ?? "pending_scan"}`}
          />
          <StatCard label="Agent" value={trial.agent_name ?? "—"} />
          <StatCard label="Model" value={modelLabel(trial.model)} />
          <StatCard
            label="Evaluator score"
            value={
              trial.aggregate_reward != null
                ? trial.aggregate_reward.toFixed(3)
                : "—"
            }
          />
          <StatCard
            label="LLM calls"
            value={trial.llm_calls_count}
          />
          <StatCard
            label="Tokens"
            value={formatTokenUsage(
              trial.total_prompt_tokens,
              trial.total_completion_tokens,
            )}
          />
          {hasCostProjection ? (
            <>
              <StatCard
                label="Estimated LLM cost"
                value={formatUsageCost(trial)}
              />
              <StatCard
                label="Cost status"
                value={usageCostStatus(trial)}
              />
              <StatCard
                label="Usage confidence"
                value={usageEstimateConfidence(trial)}
              />
            </>
          ) : null}
          <StatCard
            label="Submitted"
            value={formatLocalDateTime(trial.submitted_at)}
          />
          <StatCard
            label="Finished"
            value={formatLocalDateTime(trial.finished_at)}
          />
          <StatCard label="Attempts" value={trial.attempt_count} />
        </div>
        <p className="text-xs text-slate-500">
          Evaluator score measures task performance when a verifier reports one;
          it is separate from whether the platform run succeeded or failed.
        </p>

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

        {failure ? (
          <div className="rounded-xl border border-red-200 bg-red-50 px-4 py-3">
            <p className="text-xs font-semibold uppercase tracking-wider text-red-800">
              Failure reason
            </p>
            <p className="mt-1 text-sm font-semibold text-red-950">
              {failure.label}
            </p>
            <p className="mt-1 text-xs text-red-800">
              {failure.description}
            </p>
            <p className="mt-2 text-xs text-red-700">
              Raw code:{" "}
              <code className="font-mono">{failure.code}</code>
            </p>
          </div>
        ) : null}

        <div className="flex flex-wrap gap-2">
          {trial.atif_ready ? (
            <Button
              variant="secondary"
              title="Download the finalized ATIF artifact for this trial."
              onClick={() => void api.downloadATIF(trial.id)}
            >
              Download ATIF report
            </Button>
          ) : (
            <Button
              variant="secondary"
              disabled
              title="ATIF is generated at finalize."
            >
              ATIF unavailable
            </Button>
          )}
          {trial.trajectory_ready ? (
            <Button
              variant="secondary"
              title="Download the raw trajectory events for this trial."
              onClick={() => void api.downloadTrajectory(trial.id)}
            >
              Download trajectory log
            </Button>
          ) : (
            <Button
              variant="secondary"
              disabled
              title="Trajectory is written once the worker starts the trial."
            >
              Trajectory pending
            </Button>
          )}
        </div>

        <DocsCallout title="Trial download commands" tone="info">
          <div className="grid gap-3 lg:grid-cols-2">
            {trialDownloadCommands(trial.id, firstArtifactKey).map((command) => (
              <CommandSnippet
                key={command}
                label="Trial CLI"
                command={command}
              />
            ))}
          </div>
        </DocsCallout>

        {trial.artifacts.length > 0 ? (
          <div className="space-y-2 rounded-lg border border-slate-200 bg-slate-50/60 p-3">
            <p className="text-xs font-semibold uppercase tracking-wider text-slate-500">
              Artifacts
            </p>
            <div className="space-y-2">
              {trial.artifacts.map((artifact, index) => {
                const label = artifactLabel(artifact);
                const share = artifactShareLabel(artifact);
                return (
                  <div
                    key={`${artifact.key}-${index}`}
                    className="rounded-md border border-slate-200 bg-white px-3 py-2"
                  >
                    <button
                      title={`Download artifact ${label}.`}
                      type="button"
                      onClick={() =>
                        void api.downloadArtifact(
                          trial.id,
                          artifact.key,
                          artifactDownloadName(artifact),
                        )
                      }
                      className="flex w-full items-center justify-between gap-3 text-sm"
                    >
                      <span className="min-w-0 truncate font-medium text-slate-700">
                        Download artifact {label}
                      </span>
                      <span className="shrink-0 font-mono text-xs text-slate-400">
                        {formatBytes(artifact.size)}
                      </span>
                    </button>
                    <div className="mt-2 flex flex-wrap items-center gap-2 text-xs">
                      <span
                        className={`rounded-md border px-1.5 py-0.5 font-medium ${share.className}`}
                      >
                        {share.label}
                      </span>
                      {artifact.blocked_reason ? (
                        <span className="text-slate-500">
                          {artifact.blocked_reason}
                        </span>
                      ) : null}
                    </div>
                  </div>
                );
              })}
            </div>
          </div>
        ) : null}
      </Card.Body>
    </Card>
  );
}

function Trajectory({ trialId }: { trialId: string }): JSX.Element {
  // #5 Slice 6: switch the trajectory viewer from manual-paginate
  // `/trajectory?cursor=N` to SSE `/stream`. Events stream in live
  // while the trial is non-terminal; the connection closes itself
  // on the server's `complete` event. The legacy paginated path is
  // retained as a fallback for environments where EventSource is
  // unavailable (some corp proxies strip text/event-stream).
  const stream = useTrialEventStream(trialId);
  const events = stream.events as TrajEvent[];

  const useFallback = stream.status === "error";

  const [fallbackPages, setFallbackPages] = useState<TrajEvent[][]>([]);
  const [fallbackCursor, setFallbackCursor] = useState<number | undefined>(undefined);
  const [fallbackDone, setFallbackDone] = useState(false);

  const fallback = useQuery({
    queryKey: ["trajectory-fallback", trialId, fallbackPages.length],
    queryFn: async () => {
      const result = await api.getTrajectoryPage(trialId, fallbackCursor, 200);
      setFallbackPages((prev) => [...prev, result.events]);
      if (result.next_cursor === null) {
        setFallbackDone(true);
      } else {
        setFallbackCursor(result.next_cursor);
      }
      return result;
    },
    enabled: useFallback && !fallbackDone && fallbackPages.length === 0,
  });

  const renderedEvents = useFallback
    ? (fallbackPages.flat() as TrajEvent[])
    : events;

  const liveBadge = (() => {
    if (useFallback) return "fallback polling";
    if (stream.status === "open") return "live";
    if (stream.status === "connecting") return "connecting…";
    if (stream.status === "complete") return "complete";
    if (stream.status === "reconnect") return "reconnecting…";
    return stream.status;
  })();

  return (
    <Card>
      <Card.Header
        title="Trajectory"
        description={`${renderedEvents.length} events · ${liveBadge}`}
      />
      <Card.Body className="space-y-3">
        {!useFallback && stream.status === "connecting" && events.length === 0 ? (
          <LoadingState />
        ) : null}
        {useFallback && fallback.isPending && fallbackPages.length === 0 ? (
          <LoadingState />
        ) : null}
        {useFallback && fallback.isError ? (
          <ErrorState error={fallback.error} />
        ) : null}
        <EventTimeline events={renderedEvents} />
        {useFallback && fallback.isError ? (
          <Button
            onClick={() => fallback.refetch()}
            disabled={fallback.isFetching}
            title="Retry loading trajectory events after the previous request failed."
          >
            {fallback.isFetching ? "Retrying…" : "Retry"}
          </Button>
        ) : useFallback && !fallbackDone ? (
          <Button
            onClick={() => fallback.refetch()}
            disabled={fallback.isFetching}
            title="Load the next page of trajectory events."
          >
            {fallback.isFetching ? "Loading…" : "Load more"}
          </Button>
        ) : stream.status === "complete" && events.length > 0 ? (
          <p className="pt-1 text-center text-xs text-slate-400">
            End of trajectory.
          </p>
        ) : null}
      </Card.Body>
    </Card>
  );
}

export default function TrialDetail(): JSX.Element {
  const { trialId } = useParams<{ trialId: string }>();

  const polling = useAdaptivePolling({
    baseIntervalMs: 4_000,
    minIntervalMs: 2_000,
    maxIntervalMs: 60_000,
    hiddenBehavior: "pause",
    blurBehavior: "slow",
  });

  const trial = useQuery({
    queryKey: ["trial", trialId],
    queryFn: () => api.getTrial(trialId!),
    enabled: !!trialId,
    refetchInterval: (q) => {
      const data = q.state.data as { state: string } | undefined;
      if (!data || !ACTIVE_TRIAL_STATES.has(data.state)) return false;
      return polling.refetchInterval;
    },
  });

  if (!trialId) {
    return <ErrorState error={new Error("missing trialId")} />;
  }
  if (trial.isPending) return <LoadingState />;
  if (trial.isError) return <ErrorState error={trial.error} />;
  if (!trial.data) return <ErrorState error={new Error("no data")} />;

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between gap-4">
        <Link
          to="/trials"
          title="Return to the trial monitor table."
          className="text-xs font-medium text-slate-500 hover:text-slate-700"
        >
          ← All trials
        </Link>
        <Link
          to={`/trials/compare?a=${trialId}`}
          title="Start a side-by-side comparison using this trial."
          className="text-xs font-medium text-slate-500 hover:text-slate-700"
        >
          Compare with another trial →
        </Link>
      </div>
      <TrialHeader trial={trial.data} />
      <DiagnosisCard diagnosis={trial.data.diagnosis} />
      <DebugEvidenceCard evidence={trial.data.debug_evidence} />
      <Trajectory trialId={trialId} />
    </div>
  );
}
