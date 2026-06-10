/**
 * Campaigns list — paginated table with state filter and a "+ New
 * campaign" action that routes to the NewCampaign form.
 */
import { useQuery } from "@tanstack/react-query";
import { useState } from "react";
import { Link } from "react-router-dom";

import { api } from "../api/client";
import { Button } from "../components/Button";
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
import { campaignStateVariant } from "../lib/statusVariant";

const STATE_OPTIONS = ["submitted", "running", "finished", "cancelled"];

const SELECT_CLASSES =
  "rounded-lg border border-slate-200 bg-white px-3 py-2 text-sm text-slate-700";

export default function CampaignsList(): JSX.Element {
  const [state, setState] = useState("");
  const [page, setPage] = useState<PageState>(initialPage);

  const polling = useAdaptivePolling({
    baseIntervalMs: 5_000,
    minIntervalMs: 3_000,
    maxIntervalMs: 60_000,
    hiddenBehavior: "pause",
    blurBehavior: "slow",
  });

  const query = useQuery({
    queryKey: ["campaigns", state, page.current],
    queryFn: () =>
      api.listCampaigns({
        state: state || undefined,
        cursor: page.current ?? undefined,
        limit: "50",
      }),
    refetchInterval: polling.refetchInterval,
  });

  const body = (() => {
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
          label="No campaigns yet."
          hint={
            state
              ? `Try clearing the "${state}" filter.`
              : "Click '+ New campaign' to submit one."
          }
        />
      );
    }
    return (
      <div className="overflow-x-auto">
        <table className="min-w-full divide-y divide-slate-200 text-sm">
          <thead>
            <tr className="bg-slate-50/50">
              {["Name", "State", "Expected", "Created", "Created by"].map(
                (h) => (
                  <th
                    key={h}
                    className="px-4 py-3 text-left text-xs font-medium uppercase tracking-wider text-slate-500"
                  >
                    {h}
                  </th>
                ),
              )}
            </tr>
          </thead>
          <tbody className="divide-y divide-slate-100">
            {query.data.items.map((c) => (
              <tr key={c.id} className="hover:bg-slate-50">
                <td className="px-4 py-3">
                  <Link
                    to={`/campaigns/${c.id}`}
                    className="font-medium text-accent hover:text-accent-hover"
                  >
                    {c.name}
                  </Link>
                </td>
                <td className="px-4 py-3">
                  <StatusPill variant={campaignStateVariant(c.state)}>
                    {c.state}
                  </StatusPill>
                </td>
                <td className="px-4 py-3 text-slate-700">
                  {c.expected_trial_count}
                </td>
                <td className="px-4 py-3 text-xs text-slate-500">
                  {c.created_at.slice(0, 16).replace("T", " ")}
                </td>
                <td className="px-4 py-3 font-mono text-xs text-slate-500">
                  {c.created_by_token_prefix}
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
          <h1 className="text-2xl font-bold text-slate-900">Campaigns</h1>
          <p className="mt-1 text-sm text-slate-500">
            A campaign submits N trials in one batch — same task list,
            many trials.
          </p>
        </div>
        <div className="flex items-center gap-3">
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
              aria-label="filter campaigns by state"
            >
              <option value="">all</option>
              {STATE_OPTIONS.map((s) => (
                <option key={s} value={s}>
                  {s}
                </option>
              ))}
            </select>
          </label>
          <Link to="/campaigns/new">
            <Button variant="primary">+ New campaign</Button>
          </Link>
        </div>
      </header>

      <Card>
        <Card.Body className="p-0">{body}</Card.Body>
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
