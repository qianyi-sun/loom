/**
 * Trials list — paginated table of all trials visible to the signed-in
 * token, with a state filter. Uses adaptive polling so the page stays
 * fresh while you're watching it but stops hammering the API when you
 * switch tabs.
 */
import { useQuery } from "@tanstack/react-query";
import { useState } from "react";
import { Link } from "react-router-dom";

import { api } from "../api/client";
import { Card } from "../components/Card";
import EmptyState from "../components/EmptyState";
import ErrorState from "../components/ErrorState";
import LoadingState from "../components/LoadingState";
import Pagination, {
  initialPage,
  nextPage,
  prevPage,
  type PageState,
} from "../components/Pagination";
import { StatusPill } from "../components/StatusPill";
import { useAdaptivePolling } from "../hooks/useAdaptivePolling";
import { trialStateVariant } from "../lib/statusVariant";

const STATE_OPTIONS = [
  "queued",
  "claimed",
  "running",
  "succeeded",
  "failed",
  "cancelled",
];

const SELECT_CLASSES =
  "rounded-lg border border-slate-200 bg-white px-3 py-2 text-sm text-slate-700";

export default function TrialsList(): JSX.Element {
  const [state, setState] = useState<string>("");
  const [page, setPage] = useState<PageState>(initialPage);

  const polling = useAdaptivePolling({
    baseIntervalMs: 5_000,
    minIntervalMs: 3_000,
    maxIntervalMs: 60_000,
    hiddenBehavior: "pause",
    blurBehavior: "slow",
  });

  const query = useQuery({
    queryKey: ["trials", state, page.current],
    queryFn: () =>
      api.listTrials({
        state: state || undefined,
        cursor: page.current ?? undefined,
        limit: "50",
      }),
    refetchInterval: polling.refetchInterval,
  });

  const tableBody = (() => {
    if (query.isPending) return <LoadingState />;
    if (query.isError) {
      return (
        <div className="p-5">
          <ErrorState error={query.error} />
        </div>
      );
    }
    if (!query.data) return null;
    if (query.data.items.length === 0) {
      return (
        <EmptyState
          label="No trials match this filter."
          hint={state ? `Try changing state from "${state}" to "all".` : undefined}
        />
      );
    }
    return (
      <div className="overflow-x-auto">
        <table className="min-w-full divide-y divide-slate-200 text-sm">
          <thead>
            <tr className="bg-slate-50/50">
              {[
                "ID",
                "Task",
                "State",
                "Agent",
                "Reward",
                "Cost",
                "Submitted",
              ].map((h) => (
                <th
                  key={h}
                  className="px-4 py-3 text-left text-xs font-medium uppercase tracking-wider text-slate-500"
                >
                  {h}
                </th>
              ))}
            </tr>
          </thead>
          <tbody className="divide-y divide-slate-100">
            {query.data.items.map((t) => (
              <tr key={t.id} className="hover:bg-slate-50">
                <td className="px-4 py-3">
                  <Link
                    to={`/trials/${t.id}`}
                    className="font-mono text-xs text-accent hover:text-accent-hover"
                  >
                    {t.id.slice(0, 8)}
                  </Link>
                </td>
                <td className="px-4 py-3 font-mono text-xs text-slate-700">
                  {t.task_id}
                </td>
                <td className="px-4 py-3">
                  <StatusPill variant={trialStateVariant(t.state)}>
                    {t.state}
                  </StatusPill>
                </td>
                <td className="px-4 py-3 text-slate-700">
                  {t.agent_name ?? "—"}
                </td>
                <td className="px-4 py-3 text-slate-700">
                  {t.aggregate_reward != null
                    ? t.aggregate_reward.toFixed(3)
                    : "—"}
                </td>
                <td className="px-4 py-3 text-slate-700">
                  ${t.cost_usd.toFixed(4)}
                </td>
                <td className="px-4 py-3 text-xs text-slate-500">
                  {t.submitted_at.slice(0, 16).replace("T", " ")}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    );
  })();

  return (
    <div className="space-y-6">
      <header className="flex flex-wrap items-end justify-between gap-4">
        <div>
          <h1 className="text-2xl font-bold text-slate-900">Trials</h1>
          <p className="mt-1 text-sm text-slate-500">
            Every benchmark run you've submitted. Live-updates while the
            tab is focused.
          </p>
        </div>
        <label className="flex items-center gap-2 text-sm text-slate-600">
          <span className="text-xs uppercase tracking-wider text-slate-400">
            State
          </span>
          <select
            value={state}
            onChange={(e) => {
              setState(e.target.value);
              setPage(initialPage);
            }}
            className={SELECT_CLASSES}
            aria-label="filter trials by state"
          >
            <option value="">all</option>
            {STATE_OPTIONS.map((s) => (
              <option key={s} value={s}>
                {s}
              </option>
            ))}
          </select>
        </label>
      </header>

      <Card>
        <Card.Body className="p-0">{tableBody}</Card.Body>
        {query.data && query.data.items.length > 0 ? (
          <Card.Footer>
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
          </Card.Footer>
        ) : null}
      </Card>
    </div>
  );
}
