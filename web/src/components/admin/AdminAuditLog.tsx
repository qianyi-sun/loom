import { useQuery } from "@tanstack/react-query";

import { api, type AdminAuditEvent } from "../../api/client";
import { useCursorPage } from "../../hooks/useCursorPage";
import { formatLocalDateTime } from "../../lib/dateTime";
import { Card } from "../Card";
import EmptyState from "../EmptyState";
import ErrorState from "../ErrorState";
import LoadingState from "../LoadingState";
import Pagination from "../Pagination";

function AuditRows({ events }: { events: AdminAuditEvent[] }): JSX.Element {
  if (events.length === 0) return <EmptyState label="No admin audit events." />;
  return (
    <div className="overflow-x-auto">
      <table className="min-w-full divide-y divide-slate-200 text-sm">
        <thead className="bg-slate-50 text-left text-xs uppercase tracking-wider text-slate-500">
          <tr>
            <th className="px-3 py-2 font-semibold">Time</th>
            <th className="px-3 py-2 font-semibold">Actor</th>
            <th className="px-3 py-2 font-semibold">Action</th>
            <th className="px-3 py-2 font-semibold">Target</th>
          </tr>
        </thead>
        <tbody className="divide-y divide-slate-100 bg-white">
          {events.map((event) => (
            <tr key={event.id}>
              <td className="whitespace-nowrap px-3 py-2 text-slate-600">
                {formatLocalDateTime(event.created_at)}
              </td>
              <td className="whitespace-nowrap px-3 py-2 font-medium text-slate-800">
                {event.actor}
              </td>
              <td className="whitespace-nowrap px-3 py-2 font-mono text-xs text-slate-700">
                {event.action}
              </td>
              <td className="px-3 py-2 text-slate-600">
                {event.target_type}:{event.target_id}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

export default function AdminAuditLog(): JSX.Element {
  const page = useCursorPage("admin-audit-events");
  const query = useQuery({
    queryKey: ["admin", "audit-events", page.cursor],
    queryFn: () => api.listAdminAuditEvents(50, page.cursor ?? undefined),
  });

  return (
    <Card>
      <Card.Header
        title="Audit log"
        description="Admin access decisions with actor, action, and target."
      />
      <Card.Body>
        {query.isPending ? <LoadingState label="Loading audit events…" /> : null}
        {query.isError ? <ErrorState error={query.error} /> : null}
        {query.data ? <AuditRows events={query.data.items} /> : null}
      </Card.Body>
      <Card.Footer>
        <Pagination
          state={page.state}
          hasNext={query.data?.next_cursor != null}
          isLoading={query.isPending || query.isFetching}
          isError={query.isError}
          onNext={() => {
            const cursor = query.data?.next_cursor;
            if (cursor) page.next(cursor);
          }}
          onPrev={page.prev}
          onRetry={() => void query.refetch()}
          className="mt-0"
        />
      </Card.Footer>
    </Card>
  );
}
