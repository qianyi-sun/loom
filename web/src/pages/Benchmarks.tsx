/**
 * Benchmarks list — registered benchmark suites grouped by series.
 *
 * Rows are populated by `loom service up` (which runs
 * `seed_test_data.py --mode dev` to register every entry-point
 * adapter) or by an operator running `loom_benchmark_tool` in
 * production.
 *
 * Rows are grouped by `benchmarks.series` so AIME (AIMO validation +
 * 2025), SWE-Bench (full + Multimodal), and any future siblings sit
 * together. NULL-series benchmarks land in an "Other" group at the
 * bottom — matches the picker on NewBatch so users see the same
 * mental model in both places.
 */
import { useQuery } from "@tanstack/react-query";
import { useMemo, useState } from "react";

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

interface BenchmarkRow {
  id: string;
  display_name: string;
  series: string | null;
  license_spdx: string;
  upstream_kind: string;
  upstream_locator: string;
  imported_at: string;
  task_count?: number;
}

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

  const groups = useMemo(() => {
    if (!query.data) return [];
    const bySeries = new Map<string, BenchmarkRow[]>();
    for (const b of query.data.items as BenchmarkRow[]) {
      const key = b.series ?? "";
      const bucket = bySeries.get(key) ?? [];
      bucket.push(b);
      bySeries.set(key, bucket);
    }
    return Array.from(bySeries.entries())
      .map(([series, rows]) => ({
        series,
        rows: rows.sort((a, b) =>
          (a.display_name ?? a.id).localeCompare(b.display_name ?? b.id),
        ),
      }))
      .sort((a, b) => {
        if (a.series === "" && b.series !== "") return 1;
        if (b.series === "" && a.series !== "") return -1;
        return a.series.localeCompare(b.series);
      });
  }, [query.data]);

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
              {["ID", "Name", "Tasks", "License", "Source", "Imported"].map((h) => (
                <th
                  key={h}
                  className="px-4 py-3 text-left text-xs font-medium uppercase tracking-wider text-slate-500"
                >
                  {h}
                </th>
              ))}
            </tr>
          </thead>
          {groups.map(({ series, rows }) => {
            const seriesLabel = series === "" ? "Other" : series;
            return (
              <tbody
                key={seriesLabel}
                className="divide-y divide-slate-100 border-t border-slate-200"
              >
                <tr className="bg-slate-50">
                  <th
                    colSpan={6}
                    className="px-4 py-2 text-left text-xs font-semibold uppercase tracking-wider text-slate-600"
                  >
                    <span className="mr-2">{seriesLabel}</span>
                    <span className="font-normal normal-case text-slate-400">
                      {rows.length} benchmark{rows.length === 1 ? "" : "s"}
                    </span>
                  </th>
                </tr>
                {rows.map((b) => (
                  <tr key={b.id} className="hover:bg-slate-50">
                    <td className="px-4 py-3 font-mono text-xs text-slate-700">
                      {b.id}
                    </td>
                    <td className="px-4 py-3 text-slate-700">{b.display_name}</td>
                    <td className="px-4 py-3 text-xs text-slate-500">
                      {b.task_count ?? <span className="text-slate-300">—</span>}
                    </td>
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
            );
          })}
        </table>
      </div>
    );
  })();

  return (
    <div className="space-y-6">
      <header>
        <h1 className="text-2xl font-bold text-slate-900">Benchmarks</h1>
        <p className="mt-1 text-sm text-slate-500">
          Registered benchmark suites grouped by series. Each row corresponds
          to one `BenchmarkAdapter` discovered via entry-points.
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
