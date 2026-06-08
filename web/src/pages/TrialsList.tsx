import { useQuery } from "@tanstack/react-query";
import { useState } from "react";
import { Link } from "react-router-dom";

import { api } from "../api/client";
import EmptyState from "../components/EmptyState";
import ErrorState from "../components/ErrorState";
import LoadingState from "../components/LoadingState";
import Pagination, {
  initialPage,
  nextPage,
  prevPage,
  type PageState,
} from "../components/Pagination";

const STATE_OPTIONS = [
  "queued",
  "claimed",
  "running",
  "succeeded",
  "failed",
  "cancelled",
];

export default function TrialsList(): JSX.Element {
  const [state, setState] = useState<string>("");
  const [page, setPage] = useState<PageState>(initialPage);

  const query = useQuery({
    queryKey: ["trials", state, page.current],
    queryFn: () =>
      api.listTrials({
        state: state || undefined,
        cursor: page.current ?? undefined,
        limit: "50",
      }),
  });

  return (
    <>
      <div className="loom-page-header">
        <h1>Trials</h1>
      </div>
      <div className="loom-filters">
        <label>
          State:&nbsp;
          <select
            value={state}
            onChange={(e) => {
              setState(e.target.value);
              setPage(initialPage);
            }}
          >
            <option value="">all</option>
            {STATE_OPTIONS.map((s) => (
              <option key={s} value={s}>
                {s}
              </option>
            ))}
          </select>
        </label>
      </div>

      {query.isPending ? <LoadingState /> : null}
      {query.isError ? <ErrorState error={query.error} /> : null}
      {query.data ? (
        query.data.items.length === 0 ? (
          <EmptyState label="No trials match this filter." />
        ) : (
          <>
            <table>
              <thead>
                <tr>
                  <th>ID</th>
                  <th>Task</th>
                  <th>State</th>
                  <th>Agent</th>
                  <th>Reward</th>
                  <th>Cost</th>
                  <th>Submitted</th>
                </tr>
              </thead>
              <tbody>
                {query.data.items.map((t) => (
                  <tr key={t.id}>
                    <td>
                      <Link to={`/trials/${t.id}`} className="loom-mono">
                        {t.id.slice(0, 8)}
                      </Link>
                    </td>
                    <td className="loom-mono">{t.task_id}</td>
                    <td>
                      <span className={`loom-state-pill ${t.state}`}>
                        {t.state}
                      </span>
                    </td>
                    <td>{t.agent_name ?? "—"}</td>
                    <td>
                      {t.aggregate_reward != null
                        ? t.aggregate_reward.toFixed(3)
                        : "—"}
                    </td>
                    <td>${t.cost_usd.toFixed(4)}</td>
                    <td className="loom-muted">
                      {t.submitted_at.slice(0, 16).replace("T", " ")}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
            <Pagination
              state={page}
              hasNext={query.data.next_cursor !== null}
              onNext={() => {
                if (query.data?.next_cursor) {
                  setPage((p) => nextPage(p, query.data!.next_cursor!));
                }
              }}
              onPrev={() => setPage((p) => prevPage(p))}
            />
          </>
        )
      ) : null}
    </>
  );
}
