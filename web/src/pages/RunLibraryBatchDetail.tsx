import { useMutation, useQuery } from "@tanstack/react-query";
import { useState } from "react";
import { Link, useParams } from "react-router-dom";

import {
  api,
  type ArtifactGroup,
  type RunLibraryArtifact,
  type RunLibraryBatchDetail,
} from "../api/client";
import { Button } from "../components/Button";
import { Card } from "../components/Card";
import CommandSnippet from "../components/CommandSnippet";
import DocsCallout from "../components/DocsCallout";
import ErrorState from "../components/ErrorState";
import LoadingState from "../components/LoadingState";
import { StatCard } from "../components/StatCard";
import { StatusPill } from "../components/StatusPill";
import { humanizeTaskFilter } from "../lib/humanizeTaskFilter";
import { humanizeTrialConfig } from "../lib/humanizeTrialConfig";
import { modelLabel } from "../lib/modelLabel";
import { provenanceLabel } from "../lib/provenanceLabel";
import { trialDownloadCommands } from "../lib/quickstartSnippets";
import { batchStateVariant } from "../lib/statusVariant";

const GROUP_LABELS: Record<ArtifactGroup, string> = {
  reports: "Reports",
  trajectories: "Trajectories",
  reusable_outputs: "Reusable outputs",
  logs_diagnostics: "Logs/diagnostics",
  raw_diagnostics: "Raw/internal diagnostics",
};

const GROUP_ORDER: ArtifactGroup[] = [
  "reports",
  "trajectories",
  "reusable_outputs",
  "logs_diagnostics",
  "raw_diagnostics",
];

function formatBytes(size: number): string {
  if (!Number.isFinite(size) || size <= 0) return "0 B";
  const units = ["B", "KB", "MB", "GB"];
  let value = size;
  let unit = 0;
  while (value >= 1024 && unit < units.length - 1) {
    value /= 1024;
    unit += 1;
  }
  return unit === 0
    ? `${Math.round(value)} ${units[unit]}`
    : `${value.toFixed(value >= 10 ? 1 : 2)} ${units[unit]}`;
}

function artifactName(artifact: RunLibraryArtifact): string {
  return artifact.key.replace(/\/+$/, "");
}

function artifactDownloadName(artifact: RunLibraryArtifact): string {
  const label = artifact.key.replace(/\/+$/, "");
  return label.split("/").pop() || "artifact";
}

function comboText(batch: RunLibraryBatchDetail): string {
  if (batch.combinations.length > 0) {
    return batch.combinations
      .map((combo) => `${combo.agent_name} / ${modelLabel(combo.agent_model)}`)
      .join(", ");
  }
  const agent = batch.trial_config.agent_name;
  return `${typeof agent === "string" ? agent : "default"} / ${modelLabel(
    batch.trial_config.agent_model,
  )}`;
}

function ArtifactRow({
  artifact,
  onReuse,
}: {
  artifact: RunLibraryArtifact;
  onReuse: (artifact: RunLibraryArtifact) => void;
}): JSX.Element {
  const label = artifactName(artifact);
  const shared = artifact.share_status === "shared";

  function copyUrl(): void {
    if (navigator.clipboard) {
      void navigator.clipboard.writeText(artifact.download_url);
    }
  }

  return (
    <div className="rounded-md border border-slate-200 bg-white px-3 py-2">
      <div className="flex flex-wrap items-center justify-between gap-3">
        <div className="min-w-0">
          <p className="break-all font-medium text-slate-800">{label}</p>
          <p className="mt-1 text-xs text-slate-500">
            {formatBytes(artifact.size)} · {artifact.share_status}
          </p>
        </div>
        <div className="flex flex-wrap gap-2">
          {shared ? (
            <>
              <Button
                size="sm"
                title="Download this shared artifact through the Loom API."
                onClick={() =>
                  void api.downloadRunLibraryArtifact(
                    artifact.trial_id,
                    artifact.key,
                    artifactDownloadName(artifact),
                  )
                }
              >
                Download {label}
              </Button>
              <Button
                size="sm"
                title="Copy the authenticated Loom API artifact URL."
                onClick={copyUrl}
              >
                Copy URL
              </Button>
              <Button
                size="sm"
                title="Create new work that records this artifact as source provenance."
                onClick={() => onReuse(artifact)}
              >
                Reuse {label}
              </Button>
            </>
          ) : null}
        </div>
      </div>
      {!shared && artifact.blocked_reason ? (
        <p className="mt-2 text-xs text-amber-800">{artifact.blocked_reason}</p>
      ) : null}
    </div>
  );
}

export default function RunLibraryBatchDetail(): JSX.Element {
  const { batchId } = useParams<{ batchId: string }>();
  const [providerConnectionId, setProviderConnectionId] = useState("");

  const query = useQuery({
    queryKey: ["run-library-batch", batchId],
    queryFn: () => api.getRunLibraryBatch(batchId!),
    enabled: !!batchId,
  });

  const providers = useQuery({
    queryKey: ["providers"],
    queryFn: () => api.listProviderConnections(),
  });

  const clone = useMutation({
    mutationFn: (batch: RunLibraryBatchDetail) =>
      api.cloneRunLibraryBatchConfig(batch.id, {
        name: `${batch.name} copy`,
        description: `Cloned from ${batch.owner_team.name}.`,
        provider_connection_id: providerConnectionId || undefined,
      }),
  });

  const reuse = useMutation({
    mutationFn: (artifact: RunLibraryArtifact) =>
      api.reuseRunLibraryArtifact(artifact.trial_id, {
        key: artifact.key,
        name: `Reuse ${artifactDownloadName(artifact)}`,
      }),
  });

  if (!batchId) return <ErrorState error={new Error("missing batchId")} />;
  if (query.isPending) return <LoadingState />;
  if (query.isError) return <ErrorState error={query.error} />;
  if (!query.data) return <ErrorState error={new Error("no data")} />;

  const batch = query.data;
  const providerRequired = batch.provider_connection_id != null;
  const taskSummary = humanizeTaskFilter(batch.task_filter, {
    matchedTaskCount: batch.expected_trial_count,
  });
  const configSummary = humanizeTrialConfig(batch.trial_config);
  const firstSharedArtifact = GROUP_ORDER.flatMap(
    (group) => batch.artifact_inventory[group] ?? [],
  ).find((artifact) => artifact.share_status === "shared");

  return (
    <div className="space-y-6">
      <Link
        to="/library"
        className="text-xs font-medium text-slate-500 hover:text-slate-700"
      >
        ← Run Library
      </Link>

      <Card>
        <Card.Body className="space-y-5">
          <div className="flex flex-wrap items-start justify-between gap-3">
            <div className="min-w-0">
              <h1 className="text-2xl font-bold text-slate-900">{batch.name}</h1>
              <p className="mt-2 font-mono text-xs text-slate-500">
                id = {batch.id}
              </p>
            </div>
            <div className="flex flex-wrap gap-2">
              <StatusPill variant={batchStateVariant(batch.state)}>
                {batch.result_status ?? batch.state}
              </StatusPill>
              <span className="rounded-md border border-slate-200 bg-slate-50 px-2 py-0.5 text-xs font-medium text-slate-700">
                {batch.backend}
              </span>
            </div>
          </div>

          <div className="grid grid-cols-2 gap-3 md:grid-cols-3 lg:grid-cols-6">
            <StatCard label="Owner team" value={batch.owner_team.name} />
            <StatCard
              label="Visibility"
              value={`${batch.visibility} / ${batch.share_status}`}
            />
            <StatCard label="Trials" value={batch.expected_trial_count} />
            <StatCard
              label="Score"
              value={
                batch.aggregate_reward != null
                  ? batch.aggregate_reward.toFixed(3)
                  : "--"
              }
            />
            <StatCard
              label="Cost"
              value={`$${batch.total_cost_usd.toFixed(4)}`}
            />
            <StatCard
              label="Created"
              value={batch.created_at.slice(0, 16).replace("T", " ")}
            />
          </div>

          <div className="grid gap-4 md:grid-cols-2">
            <div>
              <p className="text-xs font-medium uppercase tracking-wider text-slate-500">
                Task selection
              </p>
              <p className="mt-1 text-sm font-semibold text-slate-900">
                {taskSummary.primary}
              </p>
            </div>
            <div>
              <p className="text-xs font-medium uppercase tracking-wider text-slate-500">
                Agent/model
              </p>
              <p className="mt-1 text-sm font-semibold text-slate-900">
                {comboText(batch)}
              </p>
            </div>
            <div>
              <p className="text-xs font-medium uppercase tracking-wider text-slate-500">
                Shared trial settings
              </p>
              <p className="mt-1 text-sm font-semibold text-slate-900">
                {configSummary.primary}
              </p>
            </div>
          </div>

          <div className="flex flex-wrap items-end gap-2">
            <label className="space-y-1 text-xs font-medium uppercase tracking-wider text-slate-500">
              Provider connection
              <select
                value={providerConnectionId}
                onChange={(event) =>
                  setProviderConnectionId(event.target.value)
                }
                className="mt-1 block min-w-56 rounded-lg border border-slate-200 bg-white px-3 py-2 text-sm normal-case tracking-normal text-slate-800"
              >
                <option value="">
                  {providerRequired
                    ? "Choose one from your team"
                    : "Use platform default"}
                </option>
                {(providers.data?.items ?? []).map((provider) => (
                  <option key={provider.id} value={provider.id}>
                    {provider.name}
                  </option>
                ))}
              </select>
            </label>
            <Button
              variant="secondary"
              onClick={() => clone.mutate(batch)}
              disabled={
                clone.isPending
                || (providerRequired && !providerConnectionId)
              }
              title="Create a new batch from this shared run's config under your current team."
            >
              Clone config
            </Button>
            {clone.data ? (
              <Link
                to={`/batches/${clone.data.batch_id}`}
                className="self-center text-xs font-medium text-accent hover:text-accent-hover"
              >
                Clone queued
              </Link>
            ) : null}
          </div>
          {clone.isError ? <ErrorState error={clone.error} /> : null}
        </Card.Body>
      </Card>

      <Card>
        <Card.Header
          title="Provenance"
          description="Source runs or artifacts used to derive this work."
        />
        <Card.Body>
          {batch.source_provenance.length === 0 ? (
            <p className="text-sm text-slate-500">No upstream source recorded.</p>
          ) : (
            <ul className="space-y-1 text-sm text-slate-700">
              {batch.source_provenance.map((item, index) => (
                <li key={index}>{provenanceLabel(item)}</li>
              ))}
            </ul>
          )}
        </Card.Body>
      </Card>

      <Card>
        <Card.Header
          title="Artifacts"
          description="Shared files are downloadable through Loom API URLs; blocked files stay owner-team diagnostics."
        />
        <Card.Body className="space-y-5">
          {firstSharedArtifact ? (
            <DocsCallout title="Library CLI downloads" tone="info">
              <CommandSnippet
                label="Shared artifact CLI"
                command={
                  trialDownloadCommands(
                    firstSharedArtifact.trial_id,
                    firstSharedArtifact.key,
                  ).find((command) => command.includes("--kind artifact")) ?? ""
                }
              />
            </DocsCallout>
          ) : null}
          {GROUP_ORDER.map((group) => {
            const artifacts = batch.artifact_inventory[group] ?? [];
            if (artifacts.length === 0) return null;
            return (
              <section key={group} className="space-y-2">
                <h2 className="text-xs font-semibold uppercase tracking-wider text-slate-500">
                  {GROUP_LABELS[group]}
                </h2>
                {artifacts.map((artifact) => (
                  <ArtifactRow
                    key={`${artifact.trial_id}-${artifact.key}`}
                    artifact={artifact}
                    onReuse={(item) => reuse.mutate(item)}
                  />
                ))}
              </section>
            );
          })}
          {reuse.data ? (
            <Link
              to={`/batches/${reuse.data.batch_id}`}
              className="text-xs font-medium text-accent hover:text-accent-hover"
            >
              Reuse queued
            </Link>
          ) : null}
          {reuse.isError ? <ErrorState error={reuse.error} /> : null}
        </Card.Body>
      </Card>
    </div>
  );
}
