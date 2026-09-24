import { type MonitorSummary } from "../api";
import { NebiusPlacement } from "../components/NebiusPlacement";
import { StatusPill } from "../components/StatusPill";
import { formatLocalDateTime } from "../lib/dateTime";
import { formatBytes } from "./monitorPresentation";

export function CountBox({ label, value }: { label: string; value: string }): JSX.Element {
  return (
    <div className="rounded-lg border border-slate-200 bg-white px-3 py-2">
      <p className="text-xs font-medium uppercase tracking-wider text-slate-600">{label}</p>
      <p className="mt-1 text-sm font-semibold text-slate-900">{value}</p>
    </div>
  );
}

export function ResourcePoolBreakdown({
  resources,
}: {
  resources: MonitorSummary["resources"];
}): JSX.Element | null {
  if (!resources?.pools.length) return null;
  return (
    <div className="overflow-x-auto" role="region" aria-label="Worker pool resources" tabIndex={0}>
      <table aria-label="Resource pools" className="min-w-full divide-y divide-slate-200 text-sm">
        <thead>
          <tr className="bg-slate-50/50">
            {[
              "Pool",
              "Backend",
              "Arch",
              "Used / active slots",
              "Draining",
              "Running",
              "Starting",
              "Queued",
              "Workers",
            ].map((h) => (
              <th
                scope="col"
                key={h}
                className="whitespace-nowrap px-3 py-2 text-left text-xs font-medium uppercase tracking-wider text-slate-500"
              >
                {h}
              </th>
            ))}
          </tr>
        </thead>
        <tbody className="divide-y divide-slate-100">
          {resources.pools.map((pool) => {
            const activeSlots = pool.current_active_slots ?? pool.total_slots;
            return (
              <tr key={`${pool.pool_name}:${pool.backend}:${pool.cpu_arch}`} className="bg-white">
                <td className="whitespace-nowrap px-3 py-2 font-medium text-slate-900">{pool.pool_name}</td>
                <td className="px-3 py-2 text-slate-700">{pool.backend}</td>
                <td className="px-3 py-2 text-slate-700">{pool.cpu_arch}</td>
                <td className="px-3 py-2 font-mono text-xs text-slate-900">
                  {pool.occupied_slots}/{activeSlots}
                </td>
                <td className="px-3 py-2 text-slate-700">{pool.draining_slots}</td>
                <td className="px-3 py-2 text-slate-700">{pool.running_tasks}</td>
                <td className="px-3 py-2 text-slate-700">{pool.starting_tasks}</td>
                <td className="px-3 py-2 text-slate-700">{pool.queued_tasks}</td>
                <td className="px-3 py-2 text-slate-700">{pool.active_workers}</td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}

export function NebiusExecutionBreakdown({
  serviceExecution,
}: {
  serviceExecution: MonitorSummary["service_execution"];
}): JSX.Element | null {
  const activity = serviceExecution?.activity;
  if (!serviceExecution || (!serviceExecution.targets.length && !activity?.lease_count)) {
    return null;
  }
  return (
    <div className="space-y-3 rounded-xl border border-sky-200 bg-sky-50/50 p-4">
      <div>
        <h3 className="text-sm font-semibold text-slate-900">Nebius service execution</h3>
        <p className="mt-1 text-xs text-slate-600">
          Image builds and executions share node capacity. Scale headroom becomes executable capacity only
          after nodes are ready.
        </p>
      </div>
      {serviceExecution.targets
        .filter((target) => !["disabled", "retired"].includes(target.desired_state))
        .map((target) => {
          const observation = target.observation;
          const profile = target.resource_profile;
          const draining = target.desired_state === "draining";
          const healthy = target.health_status === "healthy" && observation?.is_fresh === true;
          return (
            <div key={target.target_id ?? `${target.pool_id}:${target.region}`} className="rounded-lg border border-sky-200 bg-white p-3">
              <div className="flex flex-wrap items-center justify-between gap-2">
                <p className="font-semibold text-slate-900">
                  {target.pool_id} · {target.environment} · {target.region}
                </p>
                <StatusPill variant={draining ? "neutral" : healthy ? "success" : "failed"}>
                  {draining ? "Draining" : healthy ? "fresh" : "blocked/stale"}
                </StatusPill>
              </div>
              <div className="mt-3 grid grid-cols-2 gap-2 md:grid-cols-5">
                <CountBox
                  label="Executable now"
                  value={
                    profile?.immediate_executable_slots == null
                      ? "Unknown"
                      : `${profile.immediate_executable_slots} slots`
                  }
                />
                <CountBox
                  label="Scale headroom"
                  value={
                    profile?.configured_scale_headroom_slots == null
                      ? "Unknown"
                      : `${profile.configured_scale_headroom_slots} slots`
                  }
                />
                <CountBox
                  label="Configured total"
                  value={
                    profile?.configured_total_fit_slots == null
                      ? "Unknown"
                      : `${profile.configured_total_fit_slots} slots`
                  }
                />
                <CountBox
                  label="Capacity-accounted nodes"
                  value={`${observation?.active_nodes ?? "unknown"}`}
                />
                <CountBox label="Pending jobs" value={`${observation?.pending_jobs ?? "Unknown"}`} />
              </div>
              {observation?.node_states ? (
                <p className="mt-2 text-xs text-slate-600">
                  Nodes: desired {observation.node_states.desired} · provisioning (estimated){" "}
                  {observation.node_states.creating} · ready {observation.node_states.ready} · occupied{" "}
                  {observation.occupied_nodes ?? "unknown"} · draining{" "}
                  {observation.draining_nodes ?? "unknown"} · stalled/not ready{" "}
                  {observation.node_states.failed} · awaiting removal (estimated){" "}
                  {observation.node_states.deleting}
                </p>
              ) : null}
              <p className="mt-2 text-xs text-slate-600">
                Configured node maximum: {target.policy?.max_nodes ?? "unknown"}. Capacity-accounted nodes is
                the largest of Kubernetes inventory, provider actual nodes and provider target; it is not
                occupied nodes or quota. Occupied counts nodes hosting this target's execution or build Pods.
                Lifecycle counts can overlap. Task cancellation releases task resources before the autoscaler
                finishes reclaiming nodes.
              </p>
              <div className="mt-3 flex flex-wrap gap-x-4 gap-y-1 text-xs text-slate-600">
                <span>
                  observed:{" "}
                  {observation?.observed_at ? formatLocalDateTime(observation.observed_at) : "unavailable"}
                </span>
                <span>
                  fresh until:{" "}
                  {observation?.fresh_until ? formatLocalDateTime(observation.fresh_until) : "unavailable"}
                </span>
                <span>autoscaler: {observation?.autoscaler_state ?? "unknown"}</span>
                <span>provider: {observation?.provider_capacity_state ?? "unknown"}</span>
                <span>
                  quota:{" "}
                  {observation?.provider_used_vcpu_millis == null
                    ? "unknown"
                    : Math.round(observation.provider_used_vcpu_millis / 1000)}{" "}
                  /{" "}
                  {observation?.provider_quota_vcpu_millis == null
                    ? "unknown"
                    : Math.round(observation.provider_quota_vcpu_millis / 1000)}{" "}
                  vCPU
                </span>
                <span>commands waiting: {target.command_backlog}</span>
              </div>
              {profile?.immediate_executable_slots == null ? <p className="mt-2 text-xs text-slate-600">Capacity is unknown because a current, complete observation is unavailable. This does not mean zero available capacity.</p> : null}
              <NebiusPlacement targetId={target.target_id} />
              {[...target.blockers, ...(profile?.blockers ?? [])].length > 0 ? (
                <div className="mt-2 text-xs text-amber-800">
                  <p>Scheduling is waiting on capacity or service readiness. Review the node observation and placement above.</p>
                  <details><summary className="cursor-pointer">Technical blocker details</summary>
                    <p className="break-words">Blockers: {[...new Set([...target.blockers, ...(profile?.blockers ?? [])])].join(", ")}</p>
                  </details>
                </div>
              ) : null}
            </div>
          );
        })}
      {serviceExecution.targets.some((target) => ["disabled", "retired"].includes(target.desired_state)) ? (
        <details className="rounded-lg border border-slate-200 bg-white p-3">
          <summary className="cursor-pointer text-sm font-medium">Inactive regions</summary>
          {serviceExecution.targets
            .filter((target) => ["disabled", "retired"].includes(target.desired_state))
            .map((target) => (
              <p key={target.target_id ?? `${target.pool_id}:${target.region}`} className="mt-2 text-sm text-slate-600">
                {target.pool_id} · {target.region} ·{" "}
                {target.desired_state === "disabled" ? "Disabled" : target.desired_state}
              </p>
            ))}
          <p className="mt-2 text-xs text-slate-600">
            Excluded from active capacity. Historical observations do not indicate a current service fault.
          </p>
        </details>
      ) : null}
      {activity ? (
        <div className="space-y-2">
          <div className="grid grid-cols-2 gap-2 md:grid-cols-6">
            <CountBox label="Latest execution attempts" value={`${activity.lease_count}`} />
            <CountBox label="Archiving output" value={`${activity.materialization.backlog}`} />
            <CountBox
              label="Oldest pending"
              value={
                activity.materialization.oldest_pending_age_seconds == null
                  ? "—"
                  : `${activity.materialization.oldest_pending_age_seconds}s`
              }
            />
            <CountBox label="Unavailable" value={`${activity.materialization.states.unavailable ?? 0}`} />
            <CountBox label="Transfer retries" value={`${activity.materialization.retry_attempts}`} />
            <CountBox
              label="Transfer backlog bytes"
              value={formatBytes(activity.materialization.pending_bytes)}
            />
            <CountBox
              label="Source spool retained"
              value={formatBytes(activity.materialization.source_retained_bytes)}
            />
          </div>
          <p className="text-xs text-slate-600">
            Lifecycle:{" "}
            {Object.entries(activity.lifecycle_stages)
              .filter(([, count]) => count > 0)
              .map(([state, count]) => `${state} ${count}`)
              .join(" · ") || "none"}
          </p>
          <p className="text-xs text-slate-600">
            Provider execution:{" "}
            {Object.entries(activity.execution_states)
              .map(([state, count]) => `${state} ${count}`)
              .join(" · ") || "none"}
          </p>
          <p className="text-xs text-slate-600">
            Source cleanup:{" "}
            {Object.entries(activity.source_cleanup_states)
              .map(([state, count]) => `${state} ${count}`)
              .join(" · ") || "none"}
          </p>
          <p className="text-xs text-slate-600">
            Last canonical acknowledgement:{" "}
            {activity.materialization.last_committed_at
              ? formatLocalDateTime(activity.materialization.last_committed_at)
              : "none"}
          </p>
        </div>
      ) : null}
    </div>
  );
}
