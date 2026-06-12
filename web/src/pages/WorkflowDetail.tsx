/**
 * Workflow detail — pinned recipe + Launch action.
 *
 * Launch creates a Batch that deep-copies the workflow's
 * task_filter + trial_config at submit time; subsequent workflow
 * edits do NOT retroactively change the historical run. Admin users
 * additionally see a Delete action (soft-delete on the server).
 */
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { useState } from "react";
import { Link, useNavigate, useParams } from "react-router-dom";

import { api } from "../api/client";
import { useAuth } from "../auth/useAuth";
import { Button } from "../components/Button";
import { Card } from "../components/Card";
import ErrorState from "../components/ErrorState";
import { Input } from "../components/Input";
import JsonViewer from "../components/JsonViewer";
import LoadingState from "../components/LoadingState";
import { Modal } from "../components/Modal";
import { StatCard } from "../components/StatCard";

export default function WorkflowDetail(): JSX.Element {
  const { workflowId } = useParams<{ workflowId: string }>();
  const { isAdmin } = useAuth();
  const navigate = useNavigate();
  const queryClient = useQueryClient();

  const [launchOpen, setLaunchOpen] = useState(false);
  const [launchName, setLaunchName] = useState("");
  const [launchError, setLaunchError] = useState<unknown>(null);
  const [deleteConfirm, setDeleteConfirm] = useState(false);

  const query = useQuery({
    queryKey: ["workflow", workflowId],
    queryFn: () => api.getWorkflow(workflowId!),
    enabled: !!workflowId,
  });

  const launch = useMutation({
    mutationFn: (body: { name?: string }) =>
      api.launchWorkflow(workflowId!, body),
    onSuccess: (result) => {
      setLaunchOpen(false);
      setLaunchError(null);
      navigate(`/batches/${result.batch_id}`);
    },
    onError: (err) => setLaunchError(err),
  });

  const remove = useMutation({
    mutationFn: () => api.deleteWorkflow(workflowId!),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["workflows"] });
      navigate("/workflows");
    },
  });

  if (!workflowId) {
    return <ErrorState error={new Error("missing workflowId")} />;
  }
  if (query.isPending) return <LoadingState />;
  if (query.isError) return <ErrorState error={query.error} />;
  if (!query.data) return <ErrorState error={new Error("no data")} />;
  const w = query.data;

  return (
    <div className="space-y-6">
      <div>
        <Link
          to="/workflows"
          className="text-xs font-medium text-slate-500 hover:text-slate-700"
        >
          ← All workflows
        </Link>
      </div>

      <Card>
        <Card.Body className="space-y-5">
          <div className="flex flex-wrap items-start justify-between gap-3">
            <div className="min-w-0">
              <p className="text-xs uppercase tracking-wider text-slate-400">
                Workflow
              </p>
              <h1 className="mt-1 text-2xl font-bold text-slate-900">
                {w.name}
              </h1>
              {w.description ? (
                <p className="mt-1 text-sm text-slate-500">{w.description}</p>
              ) : null}
              <p className="mt-2 font-mono text-xs text-slate-500">
                id = {w.id}
              </p>
            </div>
            <div className="flex gap-2">
              <Button variant="primary" onClick={() => setLaunchOpen(true)}>
                Launch
              </Button>
              {isAdmin ? (
                <Button
                  variant="danger"
                  onClick={() => setDeleteConfirm(true)}
                  disabled={remove.isPending}
                >
                  Delete
                </Button>
              ) : null}
            </div>
          </div>

          <div className="grid grid-cols-2 gap-3 md:grid-cols-3 lg:grid-cols-6">
            <StatCard label="Benchmark" value={w.benchmark_id} />
            <StatCard
              label="Agent"
              value={w.agent_name}
              note={`@${w.agent_version}`}
            />
            <StatCard
              label="Model"
              value={`${w.model_provider}/${w.model_name}`}
            />
            <StatCard label="Backend" value={w.backend} />
            <StatCard label="Concurrency" value={w.concurrency} />
            <StatCard label="Updated" value={w.updated_at.slice(0, 10)} />
          </div>
        </Card.Body>
      </Card>

      <Card>
        <Card.Header
          title="Frozen config"
          description="A launch deep-copies these into a Batch. Edits to the workflow after launch do not change historical runs."
        />
        <Card.Body className="space-y-4 lg:grid lg:grid-cols-2 lg:gap-4 lg:space-y-0">
          <div>
            <p className="mb-2 text-xs font-medium uppercase tracking-wider text-slate-500">
              task_filter
            </p>
            <JsonViewer data={w.task_filter} expanded />
          </div>
          <div>
            <p className="mb-2 text-xs font-medium uppercase tracking-wider text-slate-500">
              trial_config
            </p>
            <JsonViewer data={w.trial_config} expanded />
          </div>
        </Card.Body>
      </Card>

      <Modal
        open={launchOpen}
        onClose={() => {
          setLaunchOpen(false);
          setLaunchError(null);
        }}
        title="Launch workflow"
        description="A new Batch is created in this team. You'll be redirected to the batch detail page on success."
        size="md"
        footer={
          <>
            <Button
              variant="secondary"
              onClick={() => {
                setLaunchOpen(false);
                setLaunchError(null);
              }}
              disabled={launch.isPending}
            >
              Cancel
            </Button>
            <Button
              variant="primary"
              onClick={() =>
                launch.mutate({ name: launchName.trim() || undefined })
              }
              disabled={launch.isPending}
            >
              {launch.isPending ? "Launching…" : "Launch"}
            </Button>
          </>
        }
      >
        <div className="space-y-3">
          {launchError ? <ErrorState error={launchError} /> : null}
          <label className="block">
            <span className="mb-1 block text-xs font-medium uppercase tracking-wider text-slate-500">
              Batch name (optional)
            </span>
            <Input
              value={launchName}
              onChange={(e) => setLaunchName(e.target.value)}
              placeholder={`${w.name} — <ISO timestamp>`}
            />
            <p className="mt-1 text-xs text-slate-500">
              Defaults to "{w.name} — &lt;now&gt;" so multiple launches
              don't collide.
            </p>
          </label>
        </div>
      </Modal>

      <Modal
        open={deleteConfirm}
        onClose={() => setDeleteConfirm(false)}
        title="Delete workflow?"
        description="Soft-deletes the workflow. Historical batches launched from it are unaffected; new launches return 404."
        size="sm"
        footer={
          <>
            <Button
              variant="secondary"
              onClick={() => setDeleteConfirm(false)}
              disabled={remove.isPending}
            >
              Cancel
            </Button>
            <Button
              variant="danger"
              onClick={() => remove.mutate()}
              disabled={remove.isPending}
            >
              {remove.isPending ? "Deleting…" : "Delete"}
            </Button>
          </>
        }
      >
        <p className="text-sm text-slate-600">
          This soft-deletes <strong>{w.name}</strong>. The name is freed
          for reuse and historical batch back-references remain
          valid.
        </p>
      </Modal>
    </div>
  );
}
