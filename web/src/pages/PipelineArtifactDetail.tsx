import { useQuery } from "@tanstack/react-query";
import { Link, useParams } from "react-router-dom";

import { api } from "../api/client";
import ArtifactRenderer from "../components/artifacts/ArtifactRenderer";
import ErrorState from "../components/ErrorState";
import LoadingState from "../components/LoadingState";

export default function PipelineArtifactDetail(): JSX.Element {
  const { runId, stageRunId, artifactId } = useParams();
  const query = useQuery({
    queryKey: ["pipeline-artifact", runId, stageRunId, artifactId],
    queryFn: ({ signal }) => api.getPipelineArtifact(runId!, stageRunId!, artifactId!, signal),
    enabled: Boolean(runId && stageRunId && artifactId),
  });
  if (query.isPending) return <LoadingState />;
  if (query.isError) return <ErrorState error={query.error} />;
  const artifact = query.data;
  return <div className="space-y-6">
    <nav aria-label="Breadcrumb" className="text-sm">
      <Link to="/pipelines" className="text-accent">Pipelines</Link> /{" "}
      <Link to={`/pipelines/${runId}`} className="text-accent">{runId}</Link> /{" "}
      <span>{artifact.name}</span>
    </nav>
    <header>
      <h1 className="text-2xl font-bold">{artifact.name}</h1>
      <p>{artifact.artifact_type} · {artifact.stored_size_bytes ?? 0} bytes</p>
    </header>
    <ArtifactRenderer artifact={artifact} />
    <section aria-labelledby="artifact-provenance" className="rounded border p-4">
      <h2 id="artifact-provenance" className="text-lg font-semibold">Provenance and lineage</h2>
      <dl className="grid gap-2 md:grid-cols-2">
        <div><dt>Artifact</dt><dd className="break-all font-mono">{artifact.id}</dd></div>
        <div><dt>Run</dt><dd className="break-all font-mono">{artifact.pipeline_run_id}</dd></div>
        <div><dt>StageRun</dt><dd className="break-all font-mono">{artifact.pipeline_stage_run_id}</dd></div>
        <div><dt>ExecutionAttempt</dt><dd className="break-all font-mono">{artifact.execution_attempt_id}</dd></div>
        <div><dt>Content SHA-256</dt><dd className="break-all font-mono">{artifact.content_sha256}</dd></div>
        <div><dt>Manifest SHA-256</dt><dd className="break-all font-mono">{artifact.manifest_sha256}</dd></div>
      </dl>
      <h3 className="mt-3 font-semibold">Input Artifact lineage</h3>
      {artifact.lineage_artifact_ids.length ? <ul>{artifact.lineage_artifact_ids.map((id, index) => <li key={id} className="break-all font-mono text-xs">{id} · {artifact.lineage_digests[index]}</li>)}</ul> : <p>None</p>}
    </section>
  </div>;
}
