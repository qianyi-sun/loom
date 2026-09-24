import { queryKeys } from "../api/queryKeys";
import { useQuery } from "@tanstack/react-query";
import { api } from "../api";
import { Card } from "../components/Card";
import ErrorState from "../components/ErrorState";
import { StatusPill } from "../components/StatusPill";
import { ProgressSummary } from "../components/TrialProgress";
import { useAdaptivePolling } from "../hooks/useAdaptivePolling";
import { useDebouncedValue } from "../hooks/useDebouncedValue";
import { plural, queueStatusText, queueStatusVariant, stateCount, type View } from "./monitorPresentation";
import { CountBox, NebiusExecutionBreakdown, ResourcePoolBreakdown } from "./MonitorResources";

export function MonitorHealthSummary({
  view,
  compact = false,
  search,
  stateFilter,
  teamFilter,
  benchmarkFilter,
  agentFilter,
  modelProviderFilter,
  modelNameFilter,
  providerConnectionFilter,
  providerModelFilter,
  batchId,
}: {
  view: View;
  compact?: boolean;
  search: string;
  stateFilter: string;
  teamFilter: string;
  benchmarkFilter: string;
  agentFilter: string;
  modelProviderFilter: string;
  modelNameFilter: string;
  providerConnectionFilter: string;
  providerModelFilter: string;
  batchId?: string;
}): JSX.Element | null {
  const debouncedSearch = useDebouncedValue(search, 300);
  const polling = useAdaptivePolling({
    baseIntervalMs: 4_000,
    minIntervalMs: 3_000,
    maxIntervalMs: 60_000,
    hiddenBehavior: "pause",
    blurBehavior: "slow",
  });
  const query = useQuery({
    queryKey: queryKeys["monitor-summary"](
      view,
      debouncedSearch,
      stateFilter,
      teamFilter,
      benchmarkFilter,
      agentFilter,
      modelProviderFilter,
      modelNameFilter,
      providerConnectionFilter,
      providerModelFilter,
      batchId,
    ),
    queryFn: () =>
      api.getMonitorSummary({
        view,
        q: debouncedSearch || undefined,
        state: stateFilter || undefined,
        team_id: teamFilter || undefined,
        benchmark_id: benchmarkFilter || undefined,
        agent_name: agentFilter || undefined,
        model_provider: modelProviderFilter || undefined,
        model_name: modelNameFilter || undefined,
        provider_connection_id: providerConnectionFilter || undefined,
        provider_model_id: providerModelFilter || undefined,
        batch_id: batchId || undefined,
      }),
    refetchInterval: polling.refetchInterval,
  });

  if (query.isError) {
    return (
      <Card>
        <Card.Header
          title="Monitor health"
          description="State counters and worker capacity for the current URL scope."
          headingLevel="h2"
        />
        <Card.Body>
          <ErrorState error={query.error} />
        </Card.Body>
      </Card>
    );
  }
  const data = query.data;
  if (!data) {
    return (
      <Card>
        <Card.Header
          title="Monitor health"
          description="State counters and worker capacity for the current URL scope."
          headingLevel="h2"
        />
        <Card.Body>
          <div className="grid gap-3 md:grid-cols-4">
            {Array.from({ length: 4 }).map((_, i) => (
              <div key={i} className="h-16 animate-pulse rounded-lg bg-slate-100" />
            ))}
          </div>
        </Card.Body>
      </Card>
    );
  }
  const resources = data.resources?.aggregate;
  return (
    <Card>
      <Card.Header
        title="Monitor health"
        description="State counters and worker capacity for the current URL scope."
        headingLevel="h2"
        actions={<StatusPill variant={queueStatusVariant(data.queue.status)}>{data.queue.status}</StatusPill>}
      />
      <Card.Body className="space-y-4">
        <ProgressSummary progress={data.progress} batchId={batchId} />
        <p className="text-sm text-slate-600">{queueStatusText(data)}</p>
        <details open={!compact}>
          <summary className="cursor-pointer text-sm font-medium">Nodes, scheduling and capacity diagnostics</summary>
          <div className="mt-4 space-y-4">
        {!(data.progress && data.service_execution?.targets.length) ? (
          <>
            <div className="grid gap-3 md:grid-cols-4">
              <CountBox
                label="Concurrent tasks"
                value={
                  resources
                    ? `${resources.occupied_slots} / ${
                        resources.current_active_slots ?? resources.total_slots
                      }`
                    : stateCount(data.queue.running + data.queue.claimed, "active")
                }
              />
              <CountBox
                label="Queued"
                value={stateCount(
                  resources?.queued_tasks ?? data.queue.queued + data.queue.protected_pending,
                  "queued",
                )}
              />
              <CountBox
                label="Running"
                value={stateCount(resources?.running_tasks ?? data.queue.running, "running")}
              />
              <CountBox
                label="Starting"
                value={stateCount(resources?.starting_tasks ?? data.queue.claimed, "starting")}
              />
            </div>
            <div className="grid gap-3 text-sm md:grid-cols-2">
              <div className="rounded-lg border border-slate-200 bg-slate-50 px-3 py-2">
                <p className="text-xs font-medium uppercase tracking-wider text-slate-600">Queue health</p>

                <p className="mt-1 text-xs text-slate-500">
                  <span>{plural(data.queue.active_workers, "active worker")}</span>
                  <span className="px-1">·</span>
                  <span>{stateCount(data.state_counts.trials.failed, "failed")}</span>
                </p>
              </div>
              <div className="rounded-lg border border-slate-200 bg-slate-50 px-3 py-2">
                <p
                  className="text-xs font-medium uppercase tracking-wider text-slate-600"
                  title="Adapters advertised by live legacy workers. Submissions always run on Nebius."
                >
                  Worker adapters
                </p>
                <p className="mt-1 text-slate-700">
                  {data.queue.available_backends.length > 0
                    ? data.queue.available_backends.join(", ")
                    : "No active worker adapter"}
                </p>
              </div>
            </div>
            <ResourcePoolBreakdown resources={data.resources} />
          </>
        ) : null}
        <NebiusExecutionBreakdown serviceExecution={data.service_execution} />
          </div>
        </details>
        <div className="flex flex-wrap gap-2 text-xs text-slate-500">
          <span>{stateCount(data.state_counts.trials["protected-pending"], "protected pending")}</span>
          <span>
            Batches: {data.state_counts.batches.submitted} submitted, {data.state_counts.batches.running}{" "}
            running, {data.state_counts.batches.finished} finished
          </span>
          <span>
            Trials: {data.state_counts.trials.succeeded} succeeded, {data.state_counts.trials.failed} failed,{" "}
            {data.state_counts.trials.materializing} materializing, {data.state_counts.trials.cancelled}{" "}
            cancelled
          </span>
        </div>
      </Card.Body>
    </Card>
  );
}
