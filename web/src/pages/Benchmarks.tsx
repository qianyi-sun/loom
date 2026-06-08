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

  return (
    <>
      <div className="loom-page-header">
        <h1>Benchmarks</h1>
      </div>

      {query.isPending ? <LoadingState /> : null}
      {query.isError ? <ErrorState error={query.error} /> : null}
      {query.data ? (
        query.data.items.length === 0 ? (
          <EmptyState label="No benchmarks registered." />
        ) : (
          <>
            <table>
              <thead>
                <tr>
                  <th>ID</th>
                  <th>Name</th>
                  <th>License</th>
                  <th>Source</th>
                  <th>Imported</th>
                </tr>
              </thead>
              <tbody>
                {query.data.items.map((b) => (
                  <tr key={b.id}>
                    <td className="loom-mono">{b.id}</td>
                    <td>{b.display_name}</td>
                    <td>{b.license_spdx}</td>
                    <td className="loom-mono loom-muted">
                      {b.upstream_kind}: {b.upstream_locator}
                    </td>
                    <td className="loom-muted">
                      {b.imported_at.slice(0, 10)}
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
