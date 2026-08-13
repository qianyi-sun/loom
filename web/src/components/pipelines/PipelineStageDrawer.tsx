import { useQuery } from "@tanstack/react-query";
import { Link } from "react-router-dom";

import { api, type PipelineEventPage, type PipelineStageRunSummary } from "../../api/client";
import { Modal } from "../Modal";
import ErrorState from "../ErrorState";
import LoadingState from "../LoadingState";
import { StatusPill } from "../StatusPill";
import { PIPELINE_ATTEMPT_STATE, PIPELINE_STAGE_STATE } from "../../lib/pipelinePresentation";
import BehaviorRolloutLivePreview from "../artifacts/BehaviorRolloutLivePreview";

export default function PipelineStageDrawer({ stage, events, onClose, onRetry }: { stage: PipelineStageRunSummary | null; events: PipelineEventPage["events"]; onClose: () => void; onRetry: (stage: PipelineStageRunSummary) => void }): JSX.Element {
  const detail = useQuery({ queryKey: ["pipeline-stage", stage?.id], queryFn: () => api.getPipelineStageRun(stage!.id), enabled: stage !== null });
  const attempts = useQuery({ queryKey: ["pipeline-stage-attempts", stage?.id], queryFn: () => api.listPipelineStageAttempts(stage!.id), enabled: stage !== null });
  const logs = events.filter((event) => event.stage_run_id === stage?.id);
  const activeAttempt = attempts.data?.items
    .filter((attempt) => attempt.state === "running")
    .sort((left, right) => right.attempt_number - left.attempt_number)[0];
  const committedRollout = detail.data?.artifacts.find(
    (artifact) => artifact.artifact_type === "behavior_rollout_bundle.v1",
  );
  return <Modal open={stage !== null} onClose={onClose} title={stage ? `${stage.node_key}/${stage.shard_key}` : "Stage"} size="lg" footer={detail.data?.retry_allowed ? <button type="button" onClick={() => stage && onRetry(stage)} className="rounded bg-accent px-3 py-2 text-sm text-white">Create full replay</button> : null}>
    {detail.isPending || attempts.isPending ? <LoadingState /> : detail.isError || attempts.isError ? <ErrorState error={detail.error ?? attempts.error} /> : detail.data && attempts.data ? <div aria-label="Pipeline Stage details" className="max-h-[70vh] space-y-5 overflow-auto text-sm" tabIndex={0}>
      <div className="grid gap-2 md:grid-cols-2"><p><strong>Kind:</strong> {detail.data.node_kind}</p><p><strong>Resource:</strong> {detail.data.resource_profile_name ?? "controller"} / {detail.data.resource_class}</p><p><strong>State:</strong> <StatusPill variant={PIPELINE_STAGE_STATE[detail.data.state].variant}>{PIPELINE_STAGE_STATE[detail.data.state].label}</StatusPill></p><p><strong>Domain outcome:</strong> {detail.data.domain_outcome ?? "—"}</p><p><strong>Reason:</strong> {detail.data.reason_code ?? "—"}</p><p><strong>Retry:</strong> {detail.data.retry_allowed ? "eligible" : detail.data.retry_ineligible_reason ?? "ineligible"}</p></div>
      <section><h3 className="font-semibold">Frozen digests</h3>{[["Execution", detail.data.execution_spec_digest], ["Profile", detail.data.resource_profile_digest], ["Inputs", detail.data.input_bindings_digest], ["Renderer", detail.data.request_renderer_digest]].map(([label, value]) => <p key={label} className="break-all font-mono text-xs"><strong>{label}:</strong> {value ?? "—"}</p>)}</section>
      {detail.data.live_preview_eligible && activeAttempt ? <BehaviorRolloutLivePreview
        key={activeAttempt.id}
        attempt={activeAttempt}
        committedArtifactPath={committedRollout?.detail_path ?? null}
        onHandoff={() => { void detail.refetch(); }}
        runId={detail.data.pipeline_run_id}
        stageRunId={detail.data.id}
      /> : null}
      <section><h3 className="font-semibold">Attempts ({attempts.data.items.length})</h3><ol className="space-y-2">{attempts.data.items.map((attempt) => <li key={attempt.id} className="rounded border p-2"><StatusPill variant={PIPELINE_ATTEMPT_STATE[attempt.state].variant}>{PIPELINE_ATTEMPT_STATE[attempt.state].label}</StatusPill><p>Attempt {attempt.attempt_number} · pool {attempt.worker_pool_class ?? "redacted"} · rc {attempt.exit_code ?? "—"}</p><p>Retry class/reason: {attempt.retry_class ?? "—"} / {attempt.reason_code ?? "—"}</p><p>Cancellation acknowledgement: {attempt.cancellation_observed_at ?? attempt.cleanup_acknowledged_at ?? "—"}</p></li>)}</ol></section>
      <section><h3 className="font-semibold">Committed Artifacts</h3>{detail.data.artifacts.length === 0 ? <p>None</p> : <ul>{detail.data.artifacts.map((artifact) => <li key={artifact.id} className="py-1"><Link to={artifact.detail_path} className="text-accent">{artifact.name}</Link> · {artifact.artifact_type} · {artifact.content_sha256}{artifact.share_status === "pending_scan" ? <span className="ml-2 text-amber-700">Team private — scan pending</span> : null}</li>)}</ul>}</section>
      <section><h3 className="font-semibold">Filtered Pipeline events</h3>{logs.length === 0 ? <p>No retained events.</p> : <ol>{logs.map((event) => <li key={event.seq}>#{event.seq} {event.event_type} · {event.created_at}</li>)}</ol>}</section>
    </div> : null}
  </Modal>;
}
