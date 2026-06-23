/**
 * Side-by-side comparison of two trials. Useful for A/B'ing model
 * choices: same task, different agent/model, eyeball the trajectory
 * and metrics next to each other.
 *
 * Reads `?a=<trial_id>&b=<trial_id>` from the URL; renders an input
 * for the second trial id when not provided. Each column shows the
 * trial header (state, agent, model, reward, usage) plus a compact
 * event-timeline view.
 */
import { useQuery } from "@tanstack/react-query";
import { useSearchParams } from "react-router-dom";
import { useEffect, useState } from "react";

import { api } from "../api/client";
import type { components } from "../api/schema";
import { Button } from "../components/Button";
import { Card } from "../components/Card";
import DocsCallout from "../components/DocsCallout";
import ErrorState from "../components/ErrorState";
import EventTimeline from "../components/EventTimeline";
import { Input } from "../components/Input";
import LoadingState from "../components/LoadingState";
import { StatCard } from "../components/StatCard";
import { StatusPill } from "../components/StatusPill";
import { modelLabel } from "../lib/modelLabel";
import { trialStateVariant } from "../lib/statusVariant";

type Trial = components["schemas"]["TrialDetail"];
type TrajEvent = components["schemas"]["TrajectoryEvent"];

function TrialColumn({ trialId }: { trialId: string }): JSX.Element {
  const trial = useQuery<Trial>({
    queryKey: ["trial", trialId],
    queryFn: () => api.getTrial(trialId),
    enabled: !!trialId,
  });

  // Load a single page of the trajectory — sufficient for at-a-glance
  // comparison; users can open the full TrialDetail for a deep dive.
  const traj = useQuery<{ events: TrajEvent[]; next_cursor: number | null }>({
    queryKey: ["trajectory", trialId, "compare-first-page"],
    queryFn: () => api.getTrajectoryPage(trialId, undefined, 200),
    enabled: !!trialId,
  });

  if (trial.isPending) return <LoadingState />;
  if (trial.isError) return <ErrorState error={trial.error} />;
  if (!trial.data) return <ErrorState error={new Error("no data")} />;
  const t = trial.data;

  return (
    <Card>
      <Card.Body className="space-y-5">
        <div className="flex flex-wrap items-start justify-between gap-3">
          <div className="min-w-0">
            <p className="text-xs uppercase tracking-wider text-slate-400">
              Trial
            </p>
            <p className="mt-1 font-mono text-sm text-slate-900 break-all">
              {t.id}
            </p>
            <p className="mt-2 text-xs text-slate-500">
              <code className="rounded bg-slate-100 px-1.5 py-0.5 font-mono text-xs">
                {t.task_id}
              </code>
            </p>
          </div>
          <StatusPill variant={trialStateVariant(t.state)}>
            {t.state}
          </StatusPill>
        </div>

        <div className="grid grid-cols-2 gap-3">
          <StatCard label="Agent" value={t.agent_name ?? "—"} />
          <StatCard label="Model" value={modelLabel(t.model)} />
          <StatCard
            label="Reward"
            value={
              t.aggregate_reward != null
                ? t.aggregate_reward.toFixed(3)
                : "—"
            }
          />
          <StatCard label="LLM calls" value={t.llm_calls_count} />
          <StatCard
            label="Tokens"
            value={`P ${t.total_prompt_tokens} / C ${t.total_completion_tokens}`}
          />
        </div>

        <div>
          <p className="mb-2 text-xs font-medium uppercase tracking-wider text-slate-500">
            Trajectory (first page)
          </p>
          {traj.isPending ? <LoadingState /> : null}
          {traj.isError ? <ErrorState error={traj.error} /> : null}
          {traj.data ? <EventTimeline events={traj.data.events} /> : null}
        </div>
      </Card.Body>
    </Card>
  );
}

function PickerForB({
  onPick,
}: {
  onPick: (id: string) => void;
}): JSX.Element {
  const [value, setValue] = useState("");
  return (
    <Card>
      <Card.Body className="space-y-3">
        <p className="text-sm text-slate-600">
          Paste a second trial id to compare alongside.
        </p>
        <Input
          placeholder="00000000-0000-0000-0000-000000000000"
          value={value}
          onChange={(e) => setValue(e.target.value)}
        />
        <div className="flex justify-end">
          <Button
            variant="primary"
            disabled={!value.trim()}
            onClick={() => onPick(value.trim())}
          >
            Compare
          </Button>
        </div>
      </Card.Body>
    </Card>
  );
}

export default function TrialCompare(): JSX.Element {
  const [params, setParams] = useSearchParams();
  const a = params.get("a") ?? "";
  const b = params.get("b") ?? "";
  const [localB, setLocalB] = useState(b);

  // Keep the local copy of `b` in sync if the URL changes externally.
  useEffect(() => {
    setLocalB(b);
  }, [b]);

  const setB = (id: string): void => {
    const next = new URLSearchParams(params);
    if (id) next.set("b", id);
    else next.delete("b");
    setParams(next);
  };

  if (!a) {
    return (
      <div className="space-y-6">
        <header>
          <h1 className="text-2xl font-bold text-slate-900">
            Compare trials
          </h1>
        </header>
        <DocsCallout title="Compare checklist" tone="info">
          <p>
            Compare trials with the same task when you are reviewing model,
            agent, or provider changes. Open a trial first so the left column is
            prefilled, then paste the second trial ID here.
          </p>
        </DocsCallout>
        <Card>
          <Card.Body>
            <p className="text-sm text-slate-500">
              Open a trial from the Trials list and click "Compare with
              another trial" — that pre-fills the first column here.
            </p>
          </Card.Body>
        </Card>
      </div>
    );
  }

  return (
    <div className="space-y-6">
      <header>
        <h1 className="text-2xl font-bold text-slate-900">Compare trials</h1>
        <p className="mt-1 text-sm text-slate-500">
          Side-by-side view. Same task, different runs.
        </p>
      </header>

      <div className="grid grid-cols-1 gap-4 xl:grid-cols-2">
        <TrialColumn trialId={a} />
        {localB ? (
          <TrialColumn trialId={localB} />
        ) : (
          <PickerForB onPick={setB} />
        )}
      </div>
    </div>
  );
}
