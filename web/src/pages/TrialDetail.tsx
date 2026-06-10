/**
 * Per-trial detail: header card with summary stats, trajectory
 * viewer with action-type-pill rows + JSON expansion, download links
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
import ErrorState from "../components/ErrorState";
import EventTimeline from "../components/EventTimeline";
import LoadingState from "../components/LoadingState";
import { StatCard } from "../components/StatCard";
import { StatusPill } from "../components/StatusPill";
import { useAdaptivePolling } from "../hooks/useAdaptivePolling";
import { trialStateVariant } from "../lib/statusVariant";

type TrajEvent = components["schemas"]["TrajectoryEvent"];

const ACTIVE_TRIAL_STATES = new Set([
  "queued",
  "submitted",
  "claimed",
  "running",
]);

function TrialHeader({
  trial,
}: {
  trial: components["schemas"]["TrialDetail"];
}): JSX.Element {
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

        <div className="grid grid-cols-2 gap-3 md:grid-cols-3 lg:grid-cols-4">
          <StatCard label="Agent" value={trial.agent_name ?? "—"} />
          <StatCard label="Model" value={trial.model ?? "—"} />
          <StatCard
            label="Reward"
            value={
              trial.aggregate_reward != null
                ? trial.aggregate_reward.toFixed(3)
                : "—"
            }
          />
          <StatCard
            label="Cost"
            value={`$${trial.cost_usd.toFixed(4)}`}
          />
          <StatCard
            label="Submitted"
            value={trial.submitted_at.slice(0, 16).replace("T", " ")}
          />
          <StatCard
            label="Finished"
            value={
              trial.finished_at?.slice(0, 16).replace("T", " ") ?? "—"
            }
          />
          <StatCard label="Attempts" value={trial.attempt_count} />
        </div>

        {trial.failure_reason ? (
          <div className="rounded-xl border border-red-200 bg-red-50 px-4 py-3">
            <p className="text-xs font-semibold uppercase tracking-wider text-red-800">
              Failure reason
            </p>
            <p className="mt-1 font-mono text-xs text-red-700">
              {trial.failure_reason}
            </p>
          </div>
        ) : null}

        <div className="flex flex-wrap gap-2">
          {trial.atif_ready ? (
            <a
              href={trial.atif_url}
              target="_blank"
              rel="noreferrer"
              className="contents"
            >
              <Button variant="secondary">Download ATIF</Button>
            </a>
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
            <a
              href={trial.trajectory_url}
              target="_blank"
              rel="noreferrer"
              className="contents"
            >
              <Button variant="secondary">Download trajectory</Button>
            </a>
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
      </Card.Body>
    </Card>
  );
}

function Trajectory({ trialId }: { trialId: string }): JSX.Element {
  const [pages, setPages] = useState<TrajEvent[][]>([]);
  const [nextCursor, setNextCursor] = useState<number | undefined>(undefined);
  const [done, setDone] = useState(false);

  const page = useQuery({
    queryKey: ["trajectory", trialId, pages.length],
    queryFn: async () => {
      const result = await api.getTrajectoryPage(trialId, nextCursor, 200);
      setPages((prev) => [...prev, result.events]);
      if (result.next_cursor === null) {
        setDone(true);
      } else {
        setNextCursor(result.next_cursor);
      }
      return result;
    },
    enabled: !done && pages.length === 0,
  });

  const flat = pages.flat();

  return (
    <Card>
      <Card.Header
        title="Trajectory"
        description={
          done ? `${flat.length} events` : `${flat.length} events loaded`
        }
      />
      <Card.Body className="space-y-3">
        {page.isPending && pages.length === 0 ? <LoadingState /> : null}
        {page.isError ? <ErrorState error={page.error} /> : null}
        <EventTimeline events={flat} />
        {!done ? (
          <Button
            onClick={() => page.refetch()}
            disabled={page.isFetching}
          >
            {page.isFetching ? "Loading…" : "Load more"}
          </Button>
        ) : (
          <p className="pt-1 text-center text-xs text-slate-400">
            End of trajectory.
          </p>
        )}
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
          className="text-xs font-medium text-slate-500 hover:text-slate-700"
        >
          ← All trials
        </Link>
        <Link
          to={`/trials/compare?a=${trialId}`}
          className="text-xs font-medium text-slate-500 hover:text-slate-700"
        >
          Compare with another trial →
        </Link>
      </div>
      <TrialHeader trial={trial.data} />
      <Trajectory trialId={trialId} />
    </div>
  );
}
