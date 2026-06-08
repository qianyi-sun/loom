import { useQuery } from "@tanstack/react-query";
import { useState } from "react";

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

export default function Tasks(): JSX.Element {
  const [benchmark, setBenchmark] = useState("");
  const [license, setLicense] = useState("");
  const [page, setPage] = useState<PageState>(initialPage);

  const query = useQuery({
    queryKey: ["tasks", benchmark, license, page.current],
    queryFn: () =>
      api.listTasks({
        benchmark_id: benchmark || undefined,
        license: license || undefined,
        cursor: page.current ?? undefined,
        limit: "50",
      }),
  });

  return (
    <>
      <div className="loom-page-header">
        <h1>Tasks</h1>
      </div>
      <div className="loom-filters">
        <input
          placeholder="benchmark id (e.g. humaneval)"
          value={benchmark}
          onChange={(e) => {
            setBenchmark(e.target.value);
            setPage(initialPage);
          }}
        />
        <input
          placeholder="license SPDX (e.g. MIT)"
          value={license}
          onChange={(e) => {
            setLicense(e.target.value);
            setPage(initialPage);
          }}
        />
      </div>

      {query.isPending ? <LoadingState /> : null}
      {query.isError ? <ErrorState error={query.error} /> : null}
      {query.data ? (
        query.data.items.length === 0 ? (
          <EmptyState label="No tasks match this filter." />
        ) : (
          <>
            <table>
              <thead>
                <tr>
                  <th>ID</th>
                  <th>Benchmark</th>
                  <th>License</th>
                  <th>Source</th>
                </tr>
              </thead>
              <tbody>
                {query.data.items.map((t) => (
                  <tr key={t.id}>
                    <td className="loom-mono">{t.id}</td>
                    <td className="loom-mono">{t.benchmark_id ?? "—"}</td>
                    <td>{t.license ?? "—"}</td>
                    <td className="loom-mono loom-muted">
                      {t.source ?? "—"}
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
