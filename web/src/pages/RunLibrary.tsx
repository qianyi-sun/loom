import { useQuery } from "@tanstack/react-query";
import { Link, useSearchParams } from "react-router-dom";

import { api, type ArtifactSummary, type RunLibraryBatch } from "../api/client";
import { useAuth } from "../auth/useAuth";
import { Card } from "../components/Card";
import DocsCallout from "../components/DocsCallout";
import EmptyState from "../components/EmptyState";
import ErrorState from "../components/ErrorState";
import { Input } from "../components/Input";
import LoadingState from "../components/LoadingState";
import Pagination from "../components/Pagination";
import { StatusPill } from "../components/StatusPill";
import { useCursorPage } from "../hooks/useCursorPage";
import { formatLocalDateTime } from "../lib/dateTime";
import { humanizeTaskFilter } from "../lib/humanizeTaskFilter";
import { modelLabel } from "../lib/modelLabel";
import { ownershipLabel } from "../lib/ownership";
import { batchStateVariant } from "../lib/statusVariant";
import { formatUsageCost } from "../lib/usageCost";

const TERMINAL_STATES = new Set(["finished", "cancelled"]);

const ARTIFACT_LABELS: Array<[keyof ArtifactSummary, string]> = [
  ["reports", "Reports"],
  ["trajectories", "Trajectories"],
  ["reusable_outputs", "Outputs"],
  ["logs_diagnostics", "Logs"],
  ["raw_diagnostics", "Raw/internal"],
];

const TYPED_ARTIFACT_LABELS: Array<[string, string]> = [
  ["trajectory", "Trajectory"],
  ["atif_projection", "ATIF projection"],
  ["trajectory_bundle", "Trajectory bundle"],
  ["completion_set", "Completion set"],
  ["task_set", "Task set"],
  ["task_split", "Task split"],
  ["skill_markdown", "Skill markdown"],
  ["workflow_spec", "Workflow spec"],
  ["verifier_replay", "Verifier replay"],
  ["debug_bundle", "Debug bundle"],
  ["metric_table", "Metric table"],
  ["evidence_bundle", "Evidence bundle"],
  ["training_data_export", "Training data export"],
];

const STATE_OPTIONS = [
  ["", "Any state"],
  ["finished", "Finished"],
  ["cancelled", "Cancelled"],
  ["running", "Running"],
  ["submitted", "Submitted"],
];

function scopeFromParams(params: URLSearchParams): "my" | "all" {
  return params.get("scope") === "all" ? "all" : "my";
}

function primaryModel(batch: RunLibraryBatch): string {
  if (batch.combinations.length > 0) {
    return batch.combinations
      .map((combo) => `${combo.agent_name} / ${modelLabel(combo.agent_model)}`)
      .join(", ");
  }
  const model = batch.trial_config.agent_model;
  const agent = batch.trial_config.agent_name;
  return `${typeof agent === "string" ? agent : "default"} / ${modelLabel(model)}`;
}

function formatDate(value: string | null): string {
  return formatLocalDateTime(value, { fallback: "--" });
}

function costStatusLabel(batch: RunLibraryBatch): string {
  const raw = batch as unknown as Record<string, unknown>;
  const costStatus =
    typeof raw.cost_status === "string" && raw.cost_status
      ? raw.cost_status
      : "unknown";
  const source =
    typeof raw.cost_estimate_source === "string" && raw.cost_estimate_source
      ? raw.cost_estimate_source
      : "unknown";
  const status = costStatus === "price_unknown" ? "unknown" : costStatus;
  return `${status}/${source}`;
}

function ArtifactBadges({
  summary,
  truncated = false,
}: {
  summary: ArtifactSummary;
  truncated?: boolean;
}): JSX.Element {
  const visible = ARTIFACT_LABELS.filter(([key]) => summary[key] > 0);
  if (visible.length === 0) return <span className="text-xs text-slate-600">None</span>;
  return (
    <div className="flex flex-wrap gap-1">
      {visible.map(([key, label]) => (
        <span
          key={key}
          className="rounded-md border border-slate-200 bg-slate-50 px-1.5 py-0.5 text-xs text-slate-600"
        >
          {label} {summary[key]}
          {truncated ? "+" : ""}
        </span>
      ))}
    </div>
  );
}

function ScopeButton({
  active,
  children,
  onClick,
}: {
  active: boolean;
  children: React.ReactNode;
  onClick: () => void;
}): JSX.Element {
  return (
    <button
      type="button"
      aria-pressed={active}
      onClick={onClick}
      className={
        "rounded-md px-3 py-1 text-sm font-medium transition-colors " +
        (active
          ? "bg-white text-slate-900 shadow-sm"
          : "text-slate-600 hover:text-slate-900")
      }
    >
      {children}
    </button>
  );
}

export default function RunLibrary(): JSX.Element {
  const [searchParams, setSearchParams] = useSearchParams();
  const auth = useAuth();
  const scope = scopeFromParams(searchParams);
  const teamId = searchParams.get("team_id") ?? "";
  const state = searchParams.get("state") ?? "";
  const artifactType = searchParams.get("artifact_type") ?? "";
  const search = searchParams.get("q") ?? "";
  const benchmarkId = searchParams.get("benchmark_id") ?? "";
  const agentName = searchParams.get("agent_name") ?? searchParams.get("agent") ?? "";
  const modelProvider = searchParams.get("model_provider") ?? "";
  const modelName = searchParams.get("model_name") ?? searchParams.get("model") ?? "";
  const providerConnectionId = searchParams.get("provider_connection_id") ?? "";
  const providerModelId = searchParams.get("provider_model_id") ?? "";
  const pipelineOnly = searchParams.get("producer_kind") === "pipeline";
  const pipelineRecipe = searchParams.get("pipeline_recipe") ?? "";
  const pipelineResult = searchParams.get("pipeline_result") ?? "";
  const page = useCursorPage(
    JSON.stringify([
      scope,
      teamId,
      state,
      artifactType,
      search,
      benchmarkId,
      agentName,
      modelProvider,
      modelName,
      providerConnectionId,
      providerModelId,
      pipelineOnly,
      pipelineRecipe,
      pipelineResult,
    ]),
  );

  const teamsQuery = useQuery({
    queryKey: ["admin-teams", auth.isAdmin],
    queryFn: () => api.listAdminTeams(),
    enabled: auth.isAdmin,
  });
  const teamOptions = auth.isAdmin ? teamsQuery.data?.items ?? [] : auth.teams;
  const selectedTeamKnown = teamOptions.some((team) => team.id === teamId);

  const query = useQuery({
    queryKey: [
      "run-library",
      scope,
      teamId,
      state,
      artifactType,
      search,
      benchmarkId,
      agentName,
      modelProvider,
      modelName,
      providerConnectionId,
      providerModelId,
      page.cursor,
    ],
    queryFn: () =>
      api.listRunLibraryBatches({
        scope: scope === "all" ? "all" : undefined,
        team_id: teamId || undefined,
        state: state || undefined,
        artifact_type: artifactType || undefined,
        q: search || undefined,
        benchmark_id: benchmarkId || undefined,
        agent_name: agentName || undefined,
        model_provider: modelProvider || undefined,
        model_name: modelName || undefined,
        provider_connection_id: providerConnectionId || undefined,
        provider_model_id: providerModelId || undefined,
        cursor: page.cursor ?? undefined,
        limit: "50",
      }),
    enabled: !pipelineOnly,
  });

  const pipelineArtifactsQuery = useQuery({
    queryKey: ["run-library-pipeline-artifacts", scope, teamId, artifactType, pipelineRecipe, pipelineResult],
    queryFn: () => api.listRunLibraryArtifacts({ producer_kind: "pipeline", pipeline_recipe: pipelineRecipe || undefined, pipeline_result: pipelineResult || undefined, team_id: teamId || undefined, artifact_type: artifactType || undefined, scope: scope === "all" ? "all" : undefined }),
    enabled: pipelineOnly,
  });

  function updateParam(key: string, value: string): void {
    const next = new URLSearchParams(searchParams);
    if (value) next.set(key, value);
    else next.delete(key);
    setSearchParams(next);
  }

  function updateParamWithAliases(
    key: string,
    value: string,
    aliases: string[] = [],
  ): void {
    const next = new URLSearchParams(searchParams);
    for (const alias of aliases) next.delete(alias);
    if (value) next.set(key, value);
    else next.delete(key);
    setSearchParams(next);
  }

  function setScope(nextScope: "my" | "all"): void {
    const next = new URLSearchParams(searchParams);
    if (nextScope === "all") next.set("scope", "all");
    else next.delete("scope");
    next.delete("team_id");
    setSearchParams(next);
  }

  return (
    <div className="space-y-6">
      <div className="flex flex-wrap items-start justify-between gap-4">
        <div>
          <h1 className="text-2xl font-bold text-slate-900">Run Library</h1>
          <p className="mt-1 text-sm text-slate-500">
            Completed runs and safe shared artifacts available for inspection,
            cloning, and reuse.
          </p>
        </div>
        <div className="inline-flex rounded-lg border border-slate-200 bg-slate-50 p-0.5">
          <ScopeButton active={scope === "my"} onClick={() => setScope("my")}>
            My team
          </ScopeButton>
          <ScopeButton active={scope === "all"} onClick={() => setScope("all")}>
            All teams
          </ScopeButton>
        </div>
      </div>

      <DocsCallout title="Reuse guide" tone="info">
        <p>
          Clone copies the run shape into your current team. Provider
          credentials are not copied; choose one of your own provider
          connections on the shared run detail page before queueing the clone.
        </p>
      </DocsCallout>

      <Card>
        <Card.Body className="grid gap-3 md:grid-cols-2 xl:grid-cols-4">
          <label className="flex items-center gap-2 text-sm font-medium text-slate-700">
            <input type="checkbox" checked={pipelineOnly} onChange={(event) => updateParam("producer_kind", event.target.checked ? "pipeline" : "")} />
            Pipeline artifacts only
          </label>
          <label className="space-y-1 text-xs font-medium uppercase tracking-wider text-slate-500">Pipeline Recipe<Input value={pipelineRecipe} onChange={(event) => updateParam("pipeline_recipe", event.target.value.normalize("NFC"))} placeholder="name@version" className="mt-1 normal-case tracking-normal" /></label>
          <label className="space-y-1 text-xs font-medium uppercase tracking-wider text-slate-500">Pipeline result<select value={pipelineResult} onChange={(event) => updateParam("pipeline_result", event.target.value)} className="mt-1 block w-full rounded-lg border border-slate-200 bg-white px-3 py-2 text-sm normal-case tracking-normal text-slate-800"><option value="">Any result</option>{["succeeded", "partial_failed", "failed", "cancelled", "budget_exhausted"].map((value) => <option key={value}>{value}</option>)}</select></label>
          <label className="space-y-1 text-xs font-medium uppercase tracking-wider text-slate-500">
            Search
            <Input
              value={search}
              onChange={(event) => updateParam("q", event.target.value)}
              placeholder="Name, description, or ID"
              className="mt-1 normal-case tracking-normal"
            />
          </label>
          <label className="space-y-1 text-xs font-medium uppercase tracking-wider text-slate-500">
            Benchmark
            <Input
              value={benchmarkId}
              onChange={(event) =>
                updateParam("benchmark_id", event.target.value)
              }
              placeholder="humaneval"
              className="mt-1 normal-case tracking-normal"
            />
          </label>
          <label className="space-y-1 text-xs font-medium uppercase tracking-wider text-slate-500">
            Agent
            <Input
              value={agentName}
              onChange={(event) =>
                updateParamWithAliases("agent_name", event.target.value, ["agent"])
              }
              placeholder="direct-completion"
              className="mt-1 normal-case tracking-normal"
            />
          </label>
          <label className="space-y-1 text-xs font-medium uppercase tracking-wider text-slate-500">
            Model provider
            <Input
              value={modelProvider}
              onChange={(event) =>
                updateParam("model_provider", event.target.value)
              }
              placeholder="openai"
              className="mt-1 normal-case tracking-normal"
            />
          </label>
          <label className="space-y-1 text-xs font-medium uppercase tracking-wider text-slate-500">
            Model name
            <Input
              value={modelName}
              onChange={(event) =>
                updateParamWithAliases("model_name", event.target.value, ["model"])
              }
              placeholder="gpt-4o-mini"
              className="mt-1 normal-case tracking-normal"
            />
          </label>
          <label className="space-y-1 text-xs font-medium uppercase tracking-wider text-slate-500">
            Provider connection
            <Input
              value={providerConnectionId}
              onChange={(event) =>
                updateParam("provider_connection_id", event.target.value)
              }
              placeholder="connection ID"
              className="mt-1 normal-case tracking-normal"
            />
          </label>
          <label className="space-y-1 text-xs font-medium uppercase tracking-wider text-slate-500">
            Provider model
            <Input
              value={providerModelId}
              onChange={(event) =>
                updateParam("provider_model_id", event.target.value)
              }
              placeholder="provider model ID"
              className="mt-1 normal-case tracking-normal"
            />
          </label>
          <label className="space-y-1 text-xs font-medium uppercase tracking-wider text-slate-500">
            Team
            <select
              value={teamId}
              onChange={(event) => updateParam("team_id", event.target.value)}
              className="mt-1 block w-full rounded-lg border border-slate-200 bg-white px-3 py-2 text-sm normal-case tracking-normal text-slate-800"
            >
              <option value="">Current scope</option>
              {teamId && !selectedTeamKnown ? (
                <option value={teamId}>{teamId}</option>
              ) : null}
              {teamOptions.map((team) => (
                <option key={team.id} value={team.id}>
                  {team.name}
                </option>
              ))}
            </select>
          </label>
          <label className="space-y-1 text-xs font-medium uppercase tracking-wider text-slate-500">
            State
            <select
              value={state}
              onChange={(event) => updateParam("state", event.target.value)}
              className="mt-1 block w-full rounded-lg border border-slate-200 bg-white px-3 py-2 text-sm normal-case tracking-normal text-slate-800"
            >
              {STATE_OPTIONS.map(([value, label]) => (
                <option key={value} value={value}>
                  {label}
                </option>
              ))}
            </select>
          </label>
          <label className="space-y-1 text-xs font-medium uppercase tracking-wider text-slate-500">
            Artifact type
            <select
              value={artifactType}
              onChange={(event) =>
                updateParam("artifact_type", event.target.value)
              }
              className="mt-1 block w-full rounded-lg border border-slate-200 bg-white px-3 py-2 text-sm normal-case tracking-normal text-slate-800"
            >
              <option value="">Any artifact</option>
              {ARTIFACT_LABELS.map(([value, label]) => (
                <option key={value} value={value}>
                  {label}
                </option>
              ))}
              {TYPED_ARTIFACT_LABELS.map(([value, label]) => (
                <option key={value} value={value}>
                  {label}
                </option>
              ))}
            </select>
          </label>
        </Card.Body>
      </Card>

      <Card>
        <Card.Body className="p-0">
          {pipelineOnly ? pipelineArtifactsQuery.isPending ? (
            <div className="p-5"><LoadingState /></div>
          ) : pipelineArtifactsQuery.isError ? (
            <div className="p-5"><ErrorState error={pipelineArtifactsQuery.error} /></div>
          ) : pipelineArtifactsQuery.data.items.length === 0 ? (
            <EmptyState label="No Pipeline artifacts match this view." hint="Clear a Pipeline filter or select another team." />
          ) : (
            <div className="overflow-x-auto"><table className="min-w-full text-sm"><thead><tr>{["Artifact", "Type", "Producer Pipeline", "Producer Stage", "Recipe", "Result", "Team", "Scan status"].map((header) => <th key={header} className="px-4 py-3 text-left text-xs uppercase text-slate-500">{header}</th>)}</tr></thead><tbody>{pipelineArtifactsQuery.data.items.map((artifact) => <tr key={artifact.id ?? artifact.key} className="border-t"><td className="px-4 py-3">{artifact.key}</td><td className="px-4 py-3">{artifact.artifact_type ?? "—"}</td><td className="px-4 py-3">{artifact.pipeline ? <Link className="text-accent" to={`/pipelines/${artifact.pipeline.run_id}`}>{artifact.pipeline.run_id}</Link> : "—"}</td><td className="px-4 py-3">{artifact.pipeline?.stage_run_id ?? "—"}</td><td className="px-4 py-3">{artifact.pipeline?.recipe ?? "—"}</td><td className="px-4 py-3">{artifact.pipeline?.result ?? "Pending"}</td><td className="px-4 py-3">{artifact.owner_team?.name ?? "—"}</td><td className="px-4 py-3">{artifact.share_status === "pending_scan" ? "Team private — scan pending" : artifact.share_status}</td></tr>)}</tbody></table></div>
          ) : query.isPending ? (
            <div className="p-5">
              <LoadingState />
            </div>
          ) : query.isError ? (
            <div className="p-5">
              <ErrorState error={query.error} />
            </div>
          ) : query.data.items.length === 0 ? (
            <EmptyState
              label="No runs match this library view."
              hint="Try All teams or clear the filters."
            />
          ) : (
            <div className="overflow-x-auto">
              <table className="min-w-full divide-y divide-slate-200 text-sm">
                <thead>
                  <tr className="bg-slate-50/50">
                    {[
                      "Run",
                      "Owner",
                      "Benchmark / task subset",
                      "Agent / model",
                      "Status",
                      "Score",
                      "Trials",
                      "Usage cost",
                      "Token",
                      "Created",
                      "Artifacts",
                      "Visibility",
                    ].map((header) => (
                      <th
                        key={header}
                        className="px-4 py-3 text-left text-xs font-medium uppercase tracking-wider text-slate-500"
                      >
                        {header}
                      </th>
                    ))}
                  </tr>
                </thead>
                <tbody className="divide-y divide-slate-100">
                  {query.data.items.map((batch) => {
                    const task = humanizeTaskFilter(batch.task_filter, {
                      matchedTaskCount: batch.expected_trial_count,
                    });
                    const terminal = TERMINAL_STATES.has(batch.state);
                    return (
                      <tr key={batch.id} className="hover:bg-slate-50">
                        <td className="px-4 py-3">
                          <Link
                            to={`/library/batches/${batch.id}`}
                            className="font-medium text-accent hover:text-accent-hover"
                          >
                            {batch.name}
                          </Link>
                        </td>
                        <td className="px-4 py-3 text-slate-700">
                          {ownershipLabel(batch)}
                        </td>
                        <td className="px-4 py-3 text-slate-700">
                          {task.primary}
                        </td>
                        <td className="px-4 py-3 text-slate-700">
                          {primaryModel(batch)}
                        </td>
                        <td className="px-4 py-3">
                          <StatusPill variant={batchStateVariant(batch.state)}>
                            {batch.result_status && terminal
                              ? batch.result_status
                              : batch.state}
                          </StatusPill>
                        </td>
                        <td className="px-4 py-3 text-slate-700">
                          <div>
                            {batch.aggregate_reward != null
                              ? batch.aggregate_reward.toFixed(3)
                              : "--"}
                          </div>
                          <div className="text-xs text-slate-500">
                            {costStatusLabel(batch)}
                          </div>
                        </td>
                        <td className="px-4 py-3 text-slate-700">
                          {batch.expected_trial_count}
                        </td>
                        <td className="px-4 py-3 text-slate-700">
                          {formatUsageCost(batch)}
                        </td>
                        <td className="px-4 py-3 font-mono text-xs text-slate-500">
                          {batch.created_by_token_prefix}
                        </td>
                        <td className="px-4 py-3 text-xs text-slate-500">
                          {formatDate(batch.created_at)}
                        </td>
                        <td className="px-4 py-3">
                          <ArtifactBadges
                            summary={batch.artifact_summary}
                            truncated={batch.artifact_summary_truncated}
                          />
                        </td>
                        <td className="px-4 py-3 text-xs text-slate-600">
                          {batch.visibility} / {batch.share_status}
                        </td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>
          )}
        </Card.Body>
        <Card.Footer>
          {pipelineOnly ? <p className="text-xs text-slate-500">Pipeline Artifact results are bounded to the authorized server page.</p> : <Pagination
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
          />}
        </Card.Footer>
      </Card>
    </div>
  );
}
