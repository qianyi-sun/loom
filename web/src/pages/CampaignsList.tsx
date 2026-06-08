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

const STATE_OPTIONS = ["submitted", "running", "finished", "cancelled"];

export default function CampaignsList(): JSX.Element {
  const [state, setState] = useState("");
  const [page, setPage] = useState<PageState>(initialPage);

  const query = useQuery({
    queryKey: ["campaigns", state, page.current],
    queryFn: () =>
      api.listCampaigns({
        state: state || undefined,
        cursor: page.current ?? undefined,
        limit: "50",
      }),
  });

  return (
    <>
      <div className="loom-page-header">
        <h1>Campaigns</h1>
        <Link to="/campaigns/new">
          <button>+ New campaign</button>
        </Link>
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
          <EmptyState label="No campaigns yet." />
        ) : (
          <>
            <table>
              <thead>
                <tr>
                  <th>Name</th>
                  <th>State</th>
                  <th>Expected</th>
                  <th>Created</th>
                  <th>Created by</th>
                </tr>
              </thead>
              <tbody>
                {query.data.items.map((c) => (
                  <tr key={c.id}>
                    <td>
                      <Link to={`/campaigns/${c.id}`}>{c.name}</Link>
                    </td>
                    <td>
                      <span className={`loom-state-pill ${c.state}`}>
                        {c.state}
                      </span>
                    </td>
                    <td>{c.expected_trial_count}</td>
                    <td className="loom-muted">
                      {c.created_at.slice(0, 16).replace("T", " ")}
                    </td>
                    <td className="loom-mono">
                      {c.created_by_token_prefix}
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
