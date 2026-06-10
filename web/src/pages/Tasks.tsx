/**
 * Tasks list — registered task instances with filter controls + an
 * inline "Submit trial" action per row. Clicking submit opens a
 * modal-form that POSTs to /api/v1/trials and redirects to the new
 * trial's detail page on success.
 */
import { useQuery } from "@tanstack/react-query";
import { useState } from "react";

import { api } from "../api/client";
import { Button } from "../components/Button";
import { Card } from "../components/Card";
import EmptyState from "../components/EmptyState";
import ErrorState from "../components/ErrorState";
import { Input } from "../components/Input";
import LoadingState from "../components/LoadingState";
import Pagination, {
  initialPage,
  nextPage,
  prevPage,
  type PageState,
} from "../components/Pagination";
import { SubmitTrialModal } from "../components/SubmitTrialModal";

export default function Tasks(): JSX.Element {
  const [benchmark, setBenchmark] = useState("");
  const [license, setLicense] = useState("");
  const [page, setPage] = useState<PageState>(initialPage);
  const [submitTaskId, setSubmitTaskId] = useState<string | null>(null);

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
          label="No tasks match this filter."
          hint="Try clearing the benchmark or license filter."
        />
      );
    }
    return (
      <div className="overflow-x-auto">
        <table className="min-w-full divide-y divide-slate-200 text-sm">
          <thead>
            <tr className="bg-slate-50/50">
              {["ID", "Benchmark", "License", "Source", ""].map((h) => (
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
                <td className="px-4 py-3 font-mono text-xs text-slate-700">
                  {t.id}
                </td>
                <td className="px-4 py-3 font-mono text-xs text-slate-700">
                  {t.benchmark_id ?? "—"}
                </td>
                <td className="px-4 py-3 text-slate-700">{t.license ?? "—"}</td>
                <td className="px-4 py-3 font-mono text-xs text-slate-500">
                  {t.source ?? "—"}
                </td>
                <td className="px-4 py-3 text-right">
                  <Button
                    size="sm"
                    variant="primary"
                    onClick={() => setSubmitTaskId(t.id)}
                  >
                    Submit trial
                  </Button>
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
        <h1 className="text-2xl font-bold text-slate-900">Tasks</h1>
        <p className="mt-1 text-sm text-slate-500">
          Browse registered task instances. Click `Submit trial` on a row
          to run that task against an agent + model.
        </p>
      </header>

      <div className="flex flex-wrap items-center gap-3">
        <Input
          placeholder="benchmark id (e.g. humaneval)"
          value={benchmark}
          onChange={(e) => {
            setBenchmark(e.target.value);
            setPage(initialPage);
          }}
          className="max-w-xs"
        />
        <Input
          placeholder="license SPDX (e.g. MIT)"
          value={license}
          onChange={(e) => {
            setLicense(e.target.value);
            setPage(initialPage);
          }}
          className="max-w-xs"
        />
      </div>

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

      {submitTaskId ? (
        <SubmitTrialModal
          taskId={submitTaskId}
          open={true}
          onClose={() => setSubmitTaskId(null)}
        />
      ) : null}
    </div>
  );
}
