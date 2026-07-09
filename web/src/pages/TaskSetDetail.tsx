import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { useState } from "react";
import { Link, useNavigate, useParams, useSearchParams } from "react-router-dom";

import { api, type TaskSetDetailResponse } from "../api/client";
import { Button } from "../components/Button";
import { Card } from "../components/Card";
import LoadingState from "../components/LoadingState";
import { Modal } from "../components/Modal";
import { StatusPill, type StatusVariant } from "../components/StatusPill";
import { useAdaptivePolling } from "../hooks/useAdaptivePolling";

type TabName = "overview" | "errors";
const TAB_NAMES: TabName[] = ["overview", "errors"];

function parseTab(raw: string | null): TabName {
  return TAB_NAMES.includes(raw as TabName) ? (raw as TabName) : "overview";
}

function statusVariant(status: string): StatusVariant {
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

const ACTIVE_STATUSES = new Set(["materializing"]);

function capabilityLabel(ts: TaskSetDetailResponse): string {
  if (ts.evaluation_ready && ts.intents.includes("trajectory_generation")) {
    return "both";
  }
  if (ts.evaluation_ready) return "evaluation-ready";
  return "trajectory-only";
}

export default function TaskSetDetail(): JSX.Element {
  const { id = "" } = useParams<{ id: string }>();
  const navigate = useNavigate();
  const queryClient = useQueryClient();
  const [searchParams, setSearchParams] = useSearchParams();
  const tab = parseTab(searchParams.get("tab"));
  const [deleteOpen, setDeleteOpen] = useState(false);

  const polling = useAdaptivePolling({
    baseIntervalMs: 3000,
    minIntervalMs: 2000,
    maxIntervalMs: 10000,
  });

  const query = useQuery({
    queryKey: ["taskSets", id],
    queryFn: () => api.getTaskSet(id),
    enabled: !!id,
    refetchInterval: (q) => {
      const data = q.state.data as TaskSetDetailResponse | undefined;
      if (!data || !ACTIVE_STATUSES.has(data.status)) return false;
      return polling.refetchInterval;
    },
  });

  const rebuild = useMutation({
    mutationFn: () => api.rebuildTaskSet(id),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["taskSets", id] });
      queryClient.invalidateQueries({ queryKey: ["taskSets"] });
    },
  });

  const deleteMutation = useMutation({
    mutationFn: () => api.deleteTaskSet(id),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["taskSets"] });
      navigate("/task-sets");
    },
  });

  const setTab = (nextTab: TabName): void => {
    const next = new URLSearchParams(searchParams);
    if (nextTab === "overview") next.delete("tab");
    else next.set("tab", nextTab);
    setSearchParams(next);
  };

  if (query.isLoading) return <LoadingState />;
  if (query.error) {
    const status =
      typeof query.error === "object" &&
      query.error !== null &&
      "status" in query.error
        ? (query.error as { status: number }).status
        : 0;
    if (status === 404) {
      return (
        <Card>
          <Card.Body>
            <p className="text-slate-700">Task set not found.</p>
          </Card.Body>
        </Card>
      );
    }
    return (
      <Card>
        <Card.Body>
          <p className="text-red-700">Could not load task set.</p>
        </Card.Body>
      </Card>
    );
  }

  const ts = query.data as TaskSetDetailResponse;
  if (!ts) return <LoadingState />;

  return (
    <div className="space-y-4">
      <Link
        to="/task-sets"
        className="text-sm text-accent hover:underline"
      >
        &larr; All task sets
      </Link>

      <header className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold text-slate-900">
            {ts.task_set_id}
          </h1>
          <div className="mt-1 flex items-center gap-2">
            <StatusPill variant={statusVariant(ts.status)}>
              {ts.status}
            </StatusPill>
            <span className="text-sm text-slate-500">
              {capabilityLabel(ts)}
            </span>
          </div>
        </div>
        <div className="flex gap-2">
          <Button
            variant="secondary"
            size="sm"
            onClick={() => rebuild.mutate()}
            disabled={rebuild.isPending}
          >
            {rebuild.isPending ? "Rebuilding..." : "Rebuild"}
          </Button>
          <Button
            variant="danger"
            size="sm"
            onClick={() => setDeleteOpen(true)}
          >
            Delete
          </Button>
        </div>
      </header>

      <div role="tablist" className="flex gap-2 border-b border-slate-200">
        {TAB_NAMES.map((t) => (
          <button
            key={t}
            role="tab"
            aria-selected={tab === t}
            onClick={() => setTab(t)}
            className={
              tab === t
                ? "border-b-2 border-accent px-3 py-2 text-sm font-medium text-accent"
                : "px-3 py-2 text-sm font-medium text-slate-500 hover:text-slate-700"
            }
          >
            {t === "overview" ? "Overview" : `Errors (${ts.error_summary.length})`}
          </button>
        ))}
      </div>

      {tab === "overview" ? (
        <OverviewPanel ts={ts} />
      ) : (
        <ErrorsPanel ts={ts} />
      )}

      <Modal
        open={deleteOpen}
        onClose={() => setDeleteOpen(false)}
        title="Delete task set"
        description="This will soft-delete the task set. Run history referencing it will be preserved."
        size="sm"
        footer={
          <>
            <Button variant="secondary" onClick={() => setDeleteOpen(false)}>
              Cancel
            </Button>
            <Button
              variant="danger"
              onClick={() => {
                deleteMutation.mutate();
                setDeleteOpen(false);
              }}
            >
              Delete
            </Button>
          </>
        }
      >
        <p className="text-sm text-slate-700">
          Are you sure you want to delete{" "}
          <span className="font-medium">{ts.task_set_id}</span>?
        </p>
      </Modal>
    </div>
  );
}

function OverviewPanel({ ts }: { ts: TaskSetDetailResponse }): JSX.Element {
  return (
    <Card>
      <Card.Body className="space-y-4">
        <dl className="grid grid-cols-2 gap-4 text-sm">
          <div>
            <dt className="font-medium text-slate-500">Task Count</dt>
            <dd className="mt-1 text-slate-900">{ts.task_count}</dd>
          </div>
          <div>
            <dt className="font-medium text-slate-500">Evaluation Ready</dt>
            <dd className="mt-1 text-slate-900">
              {ts.evaluation_ready ? "Yes" : "No"}
            </dd>
          </div>
          <div>
            <dt className="font-medium text-slate-500">Intents</dt>
            <dd className="mt-1 text-slate-900">
              {ts.intents.length > 0 ? ts.intents.join(", ") : "None"}
            </dd>
          </div>
          <div>
            <dt className="font-medium text-slate-500">Capabilities</dt>
            <dd className="mt-1 text-slate-900">
              {ts.capabilities.length > 0
                ? ts.capabilities.join(", ")
                : "None"}
            </dd>
          </div>
          {ts.materialization_job_state ? (
            <div>
              <dt className="font-medium text-slate-500">
                Materialization Job
              </dt>
              <dd className="mt-1 text-slate-900">
                {ts.materialization_job_state}
              </dd>
            </div>
          ) : null}
          {ts.status_reason ? (
            <div className="col-span-2">
              <dt className="font-medium text-slate-500">Status Reason</dt>
              <dd className="mt-1 text-slate-900">{ts.status_reason}</dd>
            </div>
          ) : null}
        </dl>

        {ts.warnings.length > 0 ? (
          <div className="rounded-md border border-amber-200 bg-amber-50 p-3">
            <p className="text-xs font-medium uppercase text-amber-800">
              Warnings
            </p>
            <ul className="mt-1 space-y-1 text-sm text-amber-700">
              {ts.warnings.map((w, i) => (
                <li key={i}>
                  <span className="font-medium">{w.code}:</span> {w.message}
                </li>
              ))}
            </ul>
          </div>
        ) : null}
      </Card.Body>
    </Card>
  );
}

function ErrorsPanel({ ts }: { ts: TaskSetDetailResponse }): JSX.Element {
  if (ts.error_summary.length === 0) {
    return (
      <Card>
        <Card.Body>
          <p className="text-sm text-slate-500">
            No errors recorded for this task set.
          </p>
        </Card.Body>
      </Card>
    );
  }

  return (
    <Card>
      <Card.Body className="p-0">
        <table className="min-w-full divide-y divide-slate-200 text-sm">
          <thead>
            <tr className="bg-slate-50/50">
              <th className="px-4 py-3 text-left text-xs font-medium uppercase tracking-wider text-slate-500">
                Instance
              </th>
              <th className="px-4 py-3 text-left text-xs font-medium uppercase tracking-wider text-slate-500">
                Code
              </th>
              <th className="px-4 py-3 text-left text-xs font-medium uppercase tracking-wider text-slate-500">
                Message
              </th>
            </tr>
          </thead>
          <tbody className="divide-y divide-slate-100">
            {ts.error_summary.map((err, i) => (
              <tr key={i}>
                <td className="px-4 py-2 text-slate-600">
                  {err.instance_index ?? i}
                </td>
                <td className="px-4 py-2 font-mono text-xs text-slate-700">
                  {err.code ?? "unknown"}
                </td>
                <td className="px-4 py-2 text-slate-600">
                  {err.message ?? JSON.stringify(err)}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </Card.Body>
    </Card>
  );
}
