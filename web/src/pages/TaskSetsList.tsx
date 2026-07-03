import { useQuery } from "@tanstack/react-query";
import { Link } from "react-router-dom";

import { api, type TaskSetListItem } from "../api/client";
import { Card } from "../components/Card";
import EmptyState from "../components/EmptyState";
import LoadingState from "../components/LoadingState";
import { StatusPill, type StatusVariant } from "../components/StatusPill";

function taskSetStatusVariant(status: string): StatusVariant {
  switch (status) {
    case "ready":
      return "success";
    case "materializing":
      return "running";
    case "partial":
      return "warning";
    case "failed":
      return "failed";
    case "deleted":
      return "neutral";
    default:
      return "neutral";
  }
}

function capabilityLabel(item: TaskSetListItem): string {
  if (item.evaluation_ready && item.intents.includes("data_production")) {
    return "both";
  }
  if (item.evaluation_ready) return "evaluation-ready";
  return "trajectory-only";
}

export default function TaskSetsList(): JSX.Element {
  const { data, isLoading, error } = useQuery({
    queryKey: ["taskSets"],
    queryFn: () => api.listTaskSets(),
  });

  if (isLoading) return <LoadingState />;
  if (error) {
    return (
      <Card>
        <Card.Body>
          <p className="text-red-700">Could not load task sets.</p>
        </Card.Body>
      </Card>
    );
  }

  const items = data?.items ?? [];

  return (
    <div className="space-y-4">
      <header className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold text-slate-900">Task Sets</h1>
          <p className="text-sm text-slate-500">
            Team-owned task sets for evaluation and data-production runs.
          </p>
        </div>
        <Link
          to="/task-sets/new"
          className="rounded-md bg-accent px-4 py-2 text-sm font-medium text-white hover:bg-accent-hover"
        >
          + Submit Task Set
        </Link>
      </header>

      {items.length === 0 ? (
        <EmptyState
          label="No task sets yet"
          hint="Submit a task set to get started with custom evaluations."
        />
      ) : (
        <Card>
          <Card.Body className="p-0">
            <table className="min-w-full divide-y divide-slate-200 text-sm">
              <thead>
                <tr className="bg-slate-50/50">
                  {["Name", "Status", "Capability", "Tasks", "Created"].map(
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
                {items.map((ts) => (
                  <tr key={ts.task_set_id} className="hover:bg-slate-50/50">
                    <td className="px-4 py-3">
                      <Link
                        to={`/task-sets/${encodeURIComponent(ts.task_set_id)}`}
                        className="font-medium text-accent hover:underline"
                      >
                        {ts.display_name}
                      </Link>
                    </td>
                    <td className="px-4 py-3">
                      <StatusPill variant={taskSetStatusVariant(ts.status)}>
                        {ts.status}
                      </StatusPill>
                    </td>
                    <td className="px-4 py-3 text-slate-600">
                      {capabilityLabel(ts)}
                    </td>
                    <td className="px-4 py-3 text-slate-600">
                      {ts.task_count}
                    </td>
                    <td className="px-4 py-3 text-slate-500">
                      {new Date(ts.created_at).toLocaleDateString()}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </Card.Body>
        </Card>
      )}
    </div>
  );
}
