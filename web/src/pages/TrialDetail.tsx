/**
 * Per-trial detail: header card + trajectory viewer + ATIF link.
 * Trajectory is paginated; loads more on demand.
 */

import { useQuery } from "@tanstack/react-query";
import { useState } from "react";
import { Link, useParams } from "react-router-dom";

import { api } from "../api/client";
import type { components } from "../api/schema";
import EventTimeline from "../components/EventTimeline";
import ErrorState from "../components/ErrorState";
import LoadingState from "../components/LoadingState";

type TrajEvent = components["schemas"]["TrajectoryEvent"];

function TrialHeader({
  trial,
}: {
  trial: components["schemas"]["TrialDetail"];
}): JSX.Element {
  return (
    <div className="loom-card">
      <div
        style={{
          display: "flex",
          justifyContent: "space-between",
          alignItems: "flex-start",
        }}
      >
        <div>
          <h1 style={{ margin: 0 }}>
            <span className="loom-mono">{trial.id}</span>
          </h1>
          <p className="loom-muted" style={{ margin: "0.4rem 0" }}>
            Task <code className="loom-mono">{trial.task_id}</code>
          </p>
        </div>
        <span className={`loom-state-pill ${trial.state}`}>
          {trial.state}
        </span>
      </div>

      <div
        style={{
          display: "grid",
          gridTemplateColumns: "repeat(auto-fit, minmax(180px, 1fr))",
          gap: "0.6rem",
          marginTop: "1rem",
        }}
      >
        <Stat label="Agent" value={trial.agent_name ?? "—"} />
        <Stat
          label="Model"
          value={trial.model ?? "—"}
        />
        <Stat
          label="Reward"
          value={
            trial.aggregate_reward != null
              ? trial.aggregate_reward.toFixed(3)
              : "—"
          }
        />
        <Stat label="Cost" value={`$${trial.cost_usd.toFixed(4)}`} />
        <Stat
          label="Submitted"
          value={trial.submitted_at.slice(0, 16).replace("T", " ")}
        />
        <Stat
          label="Finished"
          value={
            trial.finished_at?.slice(0, 16).replace("T", " ") ?? "—"
          }
        />
        <Stat label="Attempts" value={String(trial.attempt_count)} />
      </div>

      {trial.failure_reason ? (
        <div style={{ marginTop: "0.8rem" }}>
          <strong>Failure reason: </strong>
          <span className="loom-mono">{trial.failure_reason}</span>
        </div>
      ) : null}

      <div
        style={{
          marginTop: "1rem",
          display: "flex",
          gap: "0.6rem",
          flexWrap: "wrap",
        }}
      >
        {trial.atif_ready ? (
          <a
            href={trial.atif_url}
            target="_blank"
            rel="noreferrer"
            className="loom-button-link"
          >
            <button>Download ATIF</button>
          </a>
        ) : (
          <button disabled title="ATIF is generated at finalize.">
            ATIF unavailable
          </button>
        )}
        {trial.trajectory_ready ? (
          <a
            href={trial.trajectory_url}
            target="_blank"
            rel="noreferrer"
          >
            <button>Download trajectory</button>
          </a>
        ) : (
          <button
            disabled
            title="Trajectory is written once the worker starts the trial."
          >
            Trajectory pending
          </button>
        )}
      </div>
    </div>
  );
}

function Stat({
  label,
  value,
}: {
  label: string;
  value: string;
}): JSX.Element {
  return (
    <div>
      <div className="loom-muted" style={{ fontSize: "0.8em" }}>
        {label}
      </div>
      <div>{value}</div>
    </div>
  );
}

function Trajectory({ trialId }: { trialId: string }): JSX.Element {
  // Accumulated pages: cursor `null` means we still have more, the
  // explicit number is the next-page cursor to fetch.
  const [pages, setPages] = useState<TrajEvent[][]>([]);
  const [nextCursor, setNextCursor] = useState<number | undefined>(
    undefined,
  );
  const [done, setDone] = useState(false);

  // We only kick a query when the user clicks "Load more" (or on
  // first mount). The query key uses `pages.length` so each call is
  // a fresh fetch — TanStack Query caches by key.
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
    <div className="loom-card">
      <h2 style={{ marginTop: 0 }}>Trajectory</h2>
      {page.isPending && pages.length === 0 ? <LoadingState /> : null}
      {page.isError ? <ErrorState error={page.error} /> : null}
      <EventTimeline events={flat} />
      {!done ? (
        <button
          style={{ marginTop: "0.6rem" }}
          onClick={() => page.refetch()}
          disabled={page.isFetching}
        >
          {page.isFetching ? "Loading…" : "Load more"}
        </button>
      ) : (
        <div
          className="loom-muted"
          style={{ marginTop: "0.6rem", textAlign: "center" }}
        >
          End of trajectory ({flat.length} events).
        </div>
      )}
    </div>
  );
}

export default function TrialDetail(): JSX.Element {
  const { trialId } = useParams<{ trialId: string }>();
  const trial = useQuery({
    queryKey: ["trial", trialId],
    queryFn: () => api.getTrial(trialId!),
    enabled: !!trialId,
  });

  if (!trialId) {
    return <ErrorState error={new Error("missing trialId")} />;
  }
  if (trial.isPending) return <LoadingState />;
  if (trial.isError) return <ErrorState error={trial.error} />;
  if (!trial.data) return <ErrorState error={new Error("no data")} />;

  return (
    <>
      <p>
        <Link to="/trials">← All trials</Link>
      </p>
      <TrialHeader trial={trial.data} />
      <Trajectory trialId={trialId} />
    </>
  );
}
