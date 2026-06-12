/**
 * Workflows list — global saved recipes any team can launch. Admin
 * tokens see a "+ New workflow" CTA; team tokens see only the table.
 *
 * Each row pins the full config (benchmark, agent + version, model,
 * backend, concurrency) so a launch is reproducible: no `*-latest`
 * resolution at runtime, no surprise version skew.
 */
import { useQuery } from "@tanstack/react-query";
import { useState } from "react";
import { Link } from "react-router-dom";

import { api } from "../api/client";
import { useAuth } from "../auth/useAuth";
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

export default function Workflows(): JSX.Element {
  const { isAdmin } = useAuth();
  const [page, setPage] = useState<PageState>(initialPage);

  const query = useQuery({
    queryKey: ["workflows", page.current],
    queryFn: () =>
      api.listWorkflows({
        cursor: page.current ?? undefined,
        limit: "50",
      }),
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
          label="No workflows yet."
          hint={
            isAdmin
              ? "Click '+ New workflow' to save your first recipe."
              : "Ask an admin to publish one."
          }
        />
      );
    }
    return (
      <div className="overflow-x-auto">
        <table className="min-w-full divide-y divide-slate-200 text-sm">
          <thead>
            <tr className="bg-slate-50/50">
              {[
                "Name",
                "Benchmark",
                "Agent",
                "Model",
                "Backend",
                "Created",
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
            {query.data.items.map((w) => (
              <tr key={w.id} className="hover:bg-slate-50">
                <td className="px-4 py-3">
                  <Link
                    to={`/workflows/${w.id}`}
                    className="font-medium text-accent hover:text-accent-hover"
                  >
                    {w.name}
                  </Link>
                  {w.description ? (
                    <p className="mt-0.5 text-xs text-slate-500">
                      {w.description}
                    </p>
                  ) : null}
                </td>
                <td className="px-4 py-3 font-mono text-xs text-slate-700">
                  {w.benchmark_id}
                </td>
                <td className="px-4 py-3 text-slate-700">
                  {w.agent_name}{" "}
                  <span className="font-mono text-xs text-slate-400">
                    @{w.agent_version}
                  </span>
                </td>
                <td className="px-4 py-3 font-mono text-xs text-slate-700">
                  {w.model_provider}/{w.model_name}
                </td>
                <td className="px-4 py-3 text-slate-700">{w.backend}</td>
                <td className="px-4 py-3 text-xs text-slate-500">
                  {w.created_at.slice(0, 10)}
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
          <h1 className="text-2xl font-bold text-slate-900">Workflows</h1>
          <p className="mt-1 text-sm text-slate-500">
            Global saved recipes. Admins author them; any team can
            launch them as a batch.
          </p>
        </div>
        {isAdmin ? (
          <Link to="/workflows/new">
            <Button variant="primary">+ New workflow</Button>
          </Link>
        ) : null}
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
