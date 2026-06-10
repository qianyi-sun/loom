/**
 * Benchmarks list — registered benchmark suites. Rows are populated by
 * `loom service up` (which runs `seed_test_data.py --mode dev` to
 * register the 14 shipped adapters) or by an operator using the
 * `loom_benchmark_tool` CLI in production.
 */
import { useQuery } from "@tanstack/react-query";
import { useState } from "react";

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

export default function Benchmarks(): JSX.Element {
  const [page, setPage] = useState<PageState>(initialPage);
  const query = useQuery({
    queryKey: ["benchmarks", page.current],
    queryFn: () =>
      api.listBenchmarks({
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
          label="No benchmarks registered."
          hint="Run `loom service up` to populate the slate from the entry-points registry."
        />
      );
    }
    return (
      <div className="overflow-x-auto">
        <table className="min-w-full divide-y divide-slate-200 text-sm">
          <thead>
            <tr className="bg-slate-50/50">
              {["ID", "Name", "License", "Source", "Imported"].map((h) => (
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
            {query.data.items.map((b) => (
              <tr key={b.id} className="hover:bg-slate-50">
                <td className="px-4 py-3 font-mono text-xs text-slate-700">
                  {b.id}
                </td>
                <td className="px-4 py-3 text-slate-700">{b.display_name}</td>
                <td className="px-4 py-3 text-slate-700">{b.license_spdx}</td>
                <td className="px-4 py-3 font-mono text-xs text-slate-500">
                  {b.upstream_kind}: {b.upstream_locator}
                </td>
                <td className="px-4 py-3 text-xs text-slate-500">
                  {b.imported_at.slice(0, 10)}
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
      <header>
        <h1 className="text-2xl font-bold text-slate-900">Benchmarks</h1>
        <p className="mt-1 text-sm text-slate-500">
          Registered benchmark suites. Each row corresponds to one
          `BenchmarkAdapter` discovered via entry-points.
        </p>
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
