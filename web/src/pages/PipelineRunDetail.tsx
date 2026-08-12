import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { useEffect, useRef, useState } from "react";
import { Link, useNavigate, useParams } from "react-router-dom";

import { api, type PipelineRunDetail, type PipelineStageRunSummary } from "../api/client";
import { useAuth } from "../auth/useAuth";
import { Button } from "../components/Button";
import { Card } from "../components/Card";
import { DestructiveActionDialog } from "../components/DestructiveActionDialog";
import ErrorState from "../components/ErrorState";
import { Textarea } from "../components/Input";
import LoadingState from "../components/LoadingState";
import { Modal } from "../components/Modal";
import { StatusPill } from "../components/StatusPill";
import PipelineBudgetSummary from "../components/pipelines/PipelineBudgetSummary";
import PipelineDag from "../components/pipelines/PipelineDag";
import PipelineStageDrawer from "../components/pipelines/PipelineStageDrawer";
import PipelineStageList from "../components/pipelines/PipelineStageList";
import { usePipelineEventPoller } from "../hooks/usePipelineEventPoller";
import { formatLocalDateTime } from "../lib/dateTime";
import { pipelineResultPresentation, PIPELINE_RUN_STATE, truncateNfcUtf8 } from "../lib/pipelinePresentation";

function validReason(value: string): boolean { const bytes = new TextEncoder().encode(value.normalize("NFC")).length; return bytes >= 1 && bytes <= 500; }

function RetryDialog({ stage, run, onClose }: { stage: PipelineStageRunSummary | null; run: PipelineRunDetail; onClose: () => void }): JSX.Element {
  const navigate = useNavigate(); const keyRef = useRef<string | null>(null);
  const suggested = truncateNfcUtf8(`${run.display_name ?? run.recipe.name} retry ${stage?.node_key ?? ""}/${stage?.shard_key ?? ""}`, 200);
  const [displayName, setDisplayName] = useState(suggested); const [budget, setBudget] = useState(run.source_budget);
  useEffect(() => { if (stage) { keyRef.current = crypto.randomUUID(); setDisplayName(truncateNfcUtf8(`${run.display_name ?? run.recipe.name} retry ${stage.node_key}/${stage.shard_key}`, 200)); setBudget(run.source_budget); } else keyRef.current = null; }, [run, stage]);
  const mutation = useMutation({ mutationFn: () => api.retryPipelineStageRun(stage!.id, { budget, display_name: displayName || null }, keyRef.current!), onSuccess: (result) => { keyRef.current = null; onClose(); navigate(`/pipelines/${result.pipeline_run_id}`); } });
  return <Modal open={stage !== null} onClose={onClose} title="Create full replay PipelineRun" description="The old StageRun remains immutable; checkpoints and outputs are not reused." size="lg" dismissible={!mutation.isPending} footer={<><Button disabled={mutation.isPending} onClick={onClose}>Cancel</Button><Button variant="primary" disabled={mutation.isPending} onClick={() => mutation.mutate()}>{mutation.isPending ? "Creating…" : "Create replay"}</Button></>}><div className="grid gap-3 md:grid-cols-2"><label className="md:col-span-2 text-sm">Display name<input value={displayName} onChange={(e) => setDisplayName(truncateNfcUtf8(e.target.value, 200))} className="mt-1 block w-full rounded border px-2 py-2" /></label>{(Object.keys(budget) as Array<keyof typeof budget>).map((key) => <label key={key} className="text-sm">{key}<input value={budget[key]} onChange={(e) => setBudget({ ...budget, [key]: key === "max_provider_cost_usd" ? e.target.value : Number(e.target.value) })} className="mt-1 block w-full rounded border px-2 py-2" /></label>)}</div>{mutation.isError ? <ErrorState error={mutation.error} /> : null}</Modal>;
}

export default function PipelineRunDetailPage(): JSX.Element {
  const { runId } = useParams(); const auth = useAuth(); const client = useQueryClient();
  const query = useQuery({ queryKey: ["pipeline-run", runId], queryFn: () => api.getPipelineRun(runId!), enabled: Boolean(runId) });
  const poller = usePipelineEventPoller(runId); const [selectedNode, setSelectedNode] = useState<string | null>(null); const [drawer, setDrawer] = useState<PipelineStageRunSummary | null>(null); const [retryStage, setRetryStage] = useState<PipelineStageRunSummary | null>(null); const [cancelOpen, setCancelOpen] = useState(false); const [reason, setReason] = useState("");
  const cancelMutation = useMutation({ mutationFn: () => api.cancelPipelineRun(runId!, { reason: reason.normalize("NFC") }), onSuccess: async () => { setCancelOpen(false); setReason(""); await client.invalidateQueries({ queryKey: ["pipeline-run", runId] }); poller.retry(); } });
  useEffect(() => { if (query.data && typeof performance !== "undefined") requestAnimationFrame(() => requestAnimationFrame(() => performance.mark("loom-pipeline-detail-interactive"))); }, [query.data]);
  const canSubmit = auth.isAdmin || auth.me?.scopes.includes("submit");
  if (query.isPending) return <LoadingState />; if (query.isError) return <ErrorState error={query.error} />;
  const run = query.data; const result = pipelineResultPresentation(run.result); const canCancel = canSubmit && ["submitted", "running", "cancelling"].includes(run.state);
  const headerRows = [["Run UUID", run.id], ["Creator", run.created_by_user_id ?? "—"], ["Created", formatLocalDateTime(run.created_at)], ["Started", formatLocalDateTime(run.started_at)], ["Finished", formatLocalDateTime(run.finished_at)], ["Terminal cause/reason", run.budget?.terminal_cause ?? run.reason ?? "—"], ["Recipe digest", run.recipe.digest], ["Graph digest", run.graph_digest], ["Control digest", run.control_binding_snapshots_digest], ["Retry lineage", run.retry_of_pipeline_run_id ? `${run.retry_of_pipeline_run_id} / ${run.retry_from_stage_run_id}` : "original"]];
  return <div className="space-y-6"><div className="flex flex-wrap justify-between gap-3"><div><Link to="/pipelines" className="text-sm text-accent">← Pipelines</Link><h1 className="text-2xl font-bold">{run.display_name ?? run.recipe.name}</h1><p>{run.recipe.name}@{run.recipe.version}</p><div className="mt-2 flex gap-2"><StatusPill variant={PIPELINE_RUN_STATE[run.state].variant}>{PIPELINE_RUN_STATE[run.state].label}</StatusPill><StatusPill variant={result.variant}>{result.label}</StatusPill></div></div>{canCancel ? <Button variant="danger" onClick={() => setCancelOpen(true)}>Cancel PipelineRun</Button> : null}</div>
    {poller.degradedMessage ? <p role="alert" className="rounded bg-amber-50 p-3 text-amber-800">{poller.degradedMessage}</p> : null}{poller.error ? <div className="flex items-center gap-3"><ErrorState error={poller.error} /><Button size="sm" onClick={poller.retry}>Retry live updates</Button></div> : null}{poller.olderEventsOmitted > 0 ? <p>Older events omitted: {poller.olderEventsOmitted}</p> : null}
    <Card><Card.Header title="Run identity and lineage" headingLevel="h2" /><Card.Body className="grid gap-2 md:grid-cols-2">{headerRows.map(([label, value]) => <p key={label} className="break-all text-sm"><strong>{label}:</strong> {value}</p>)}</Card.Body></Card>
    <Card><Card.Header title="Budget ledger" headingLevel="h2" /><Card.Body><PipelineBudgetSummary budget={run.budget} /></Card.Body></Card>
    <Card><Card.Header title="Node topology" description="Select a node to filter StageRuns; select it again to clear." headingLevel="h2" /><Card.Body><PipelineDag stages={run.stages} selectedNodeKey={selectedNode} onSelectNode={setSelectedNode} /></Card.Body></Card>
    <Card><Card.Header title="StageRuns" headingLevel="h2" /><Card.Body><PipelineStageList stages={run.stages} selectedNodeKey={selectedNode} onOpen={setDrawer} /></Card.Body></Card>
    <PipelineStageDrawer stage={drawer} events={poller.events} onClose={() => setDrawer(null)} onRetry={(stage) => { setDrawer(null); setRetryStage(stage); }} /><RetryDialog stage={retryStage} run={run} onClose={() => setRetryStage(null)} />
    <DestructiveActionDialog open={cancelOpen} title="Cancel PipelineRun" target={run.display_name ?? run.id} consequence="Cancellation stops new work; committed Artifacts remain immutable." confirmLabel="Request cancellation" pendingLabel="Requesting…" confirmation={{ type: "simple" }} pending={cancelMutation.isPending} error={cancelMutation.error} confirmDisabled={!validReason(reason)} onClose={() => setCancelOpen(false)} onConfirm={async () => { await cancelMutation.mutateAsync(); }}><label className="text-sm">Reason (1..500 UTF-8 bytes)<Textarea required value={reason} onChange={(e) => setReason(e.target.value)} /></label></DestructiveActionDialog>
  </div>;
}
