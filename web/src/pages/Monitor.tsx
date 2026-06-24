/**
 * Monitor — single route with a segmented toggle between Batches and
 * Trials. Shares a filter bar (search + state) across both views and
 * persists the active view in `?view=batches|trials`. Harbor pattern:
 * switching the toggle preserves filter state so users don't lose
 * context when drilling between aggregate (Batch) and per-trial views.
 *
 * Adaptive polling at the Harbor cadence (base 4s / min 3s / max
 * 60s). When every row is in a terminal state we throttle to 60s.
 * Skeleton-in-table-body (not a separate spinner) preserves the table
 * layout on data arrival.
 */
import { useQuery } from "@tanstack/react-query";
import { useMemo, useState } from "react";
import { Link, useSearchParams } from "react-router-dom";

import { api } from "../api/client";
import { useAuth } from "../auth/useAuth";
import { Card } from "../components/Card";
import CommandSnippet from "../components/CommandSnippet";
import DocsCallout from "../components/DocsCallout";
import EmptyState from "../components/EmptyState";
import ErrorState from "../components/ErrorState";
import { Input } from "../components/Input";
import Pagination, {
  initialPage,
  nextPage,
  prevPage,
  type PageState,
} from "../components/Pagination";
import { StatusPill } from "../components/StatusPill";
import { useAdaptivePolling } from "../hooks/useAdaptivePolling";
import { useDebouncedValue } from "../hooks/useDebouncedValue";
import { batchInspectionCommands } from "../lib/quickstartSnippets";
import { batchStateVariant, trialStateVariant } from "../lib/statusVariant";

type View = "batches" | "trials";

const BATCH_STATE_OPTIONS = ["submitted", "running", "finished", "cancelled"];
const TRIAL_STATE_OPTIONS = [
  "queued",
  "claimed",
  "running",
  "succeeded",
  "failed",
  "cancelled",
];
const TERMINAL_BATCH_STATES = new Set(["finished", "cancelled"]);
const TERMINAL_TRIAL_STATES = new Set(["succeeded", "failed", "cancelled"]);

const STATE_OPTION_LABELS: Record<string, string> = {
  cancelled: "Cancelled - stopped",
  claimed: "Claimed - worker reserved it",
  failed: "Failed - needs diagnosis",
  finished: "Finished - all trials terminal",
  queued: "Queued - waiting for worker",
  running: "Running - in progress",
  submitted: "Submitted - waiting for scheduling",
  succeeded: "Succeeded - platform run completed",
};

function stateOptionLabel(state: string): string {
  return STATE_OPTION_LABELS[state] ?? state.replaceAll("_", " ");
}

function SegmentedToggle({
  value,
  onChange,
}: {
  value: View;
  onChange: (v: View) => void;
}): JSX.Element {
  return (
    <div className="inline-flex rounded-lg border border-slate-200 bg-slate-50 p-0.5">
      {(["batches", "trials"] as const).map((v) => {
        const active = v === value;
        return (
          <button
            key={v}
            type="button"
            onClick={() => onChange(v)}
            aria-pressed={active}
            title={
              v === "batches"
                ? "Show aggregate batch rows and their overall state."
                : "Show individual trial rows across batches."
            }
            className={
              "rounded-md px-3 py-1 text-sm font-medium transition-colors " +
              (active
                ? "bg-white text-slate-900 shadow-sm"
                : "text-slate-600 hover:text-slate-900")
            }
          >
            {v === "batches" ? "Batches" : "Trials"}
          </button>
        );
      })}
    </div>
  );
}

function SkeletonRows({
  rows = 5,
  cols,
}: {
  rows?: number;
  cols: number;
}): JSX.Element {
  return (
    <>
      {Array.from({ length: rows }).map((_, i) => (
        <tr key={i}>
          <td colSpan={cols} className="px-4 py-2">
            <div className="h-12 animate-pulse rounded-xl bg-slate-100" />
          </td>
        </tr>
      ))}
    </>
  );
}

interface BatchRow {
  id: string;
  team_id?: string;
  team_name?: string | null;
  owner_team?: { id: string; name: string } | null;
  name: string;
  state: string;
  expected_trial_count: number;
  created_at: string;
  created_by_token_prefix: string;
}
interface TrialRow {
  id: string;
  team_id?: string;
  team_name?: string | null;
  owner_team?: { id: string; name: string } | null;
  task_id: string;
  state: string;
  agent_name: string | null;
  model?: { provider?: string; name?: string } | null;
  aggregate_reward: number | null;
  total_prompt_tokens: number;
  total_completion_tokens: number;
  llm_calls_count: number;
  failure_reason?: string | null;
  failure_message?: string | null;
  submitted_at: string;
}

interface FailureGroup {
  reason: string;
  count: number;
  firstTrialId: string;
  messages: string[];
}

function BatchesView({
  search,
  stateFilter,
  teamFilter,
  benchmarkFilter,
  agentFilter,
  modelFilter,
}: {
  search: string;
  stateFilter: string;
  teamFilter: string;
  benchmarkFilter: string;
  agentFilter: string;
  modelFilter: string;
}): JSX.Element {
  const [page, setPage] = useState<PageState>(initialPage);
  const debouncedSearch = useDebouncedValue(search, 300);

  const polling = useAdaptivePolling({
    baseIntervalMs: 4_000,
    minIntervalMs: 3_000,
    maxIntervalMs: 60_000,
    hiddenBehavior: "pause",
    blurBehavior: "slow",
  });

  const query = useQuery({
    queryKey: ["batches", stateFilter, debouncedSearch, page.current],
    queryFn: () =>
      api.listBatches({
        state: stateFilter || undefined,
        q: debouncedSearch || undefined,
        team_id: teamFilter || undefined,
        benchmark_id: benchmarkFilter || undefined,
        agent: agentFilter || undefined,
        model: modelFilter || undefined,
        cursor: page.current ?? undefined,
        limit: "50",
      }),
    refetchInterval: (q) => {
      const data = q.state.data as { items: BatchRow[] } | undefined;
      if (!data) return polling.refetchInterval;
      const allTerminal = data.items.every((row) =>
        TERMINAL_BATCH_STATES.has(row.state),
      );
      return allTerminal ? 60_000 : polling.refetchInterval;
    },
  });

  // Filter client-side by search if backend doesn't support `q` on
  // batches — safe fallback.
  const items: BatchRow[] = useMemo(() => {
    const raw = (query.data?.items ?? []) as BatchRow[];
    const q = debouncedSearch.trim().toLowerCase();
    if (!q) return raw;
    return raw.filter(
      (b) =>
        b.name.toLowerCase().includes(q) || b.id.toLowerCase().includes(q),
    );
  }, [query.data, debouncedSearch]);

  const COLS = 6;
  return (
    <div className="space-y-3">
      {items.length > 0 ? (
        <DocsCallout title="Monitor quick actions" tone="info">
          <div className="grid gap-3 lg:grid-cols-2">
            {batchInspectionCommands(items[0].id).map((command) => (
              <CommandSnippet key={command} label="Batch CLI" command={command} />
            ))}
          </div>
        </DocsCallout>
      ) : null}
      <Card>
        <Card.Body className="p-0">
          <div className="overflow-x-auto">
            <table className="min-w-full divide-y divide-slate-200 text-sm">
            <thead>
              <tr className="bg-slate-50/50">
                {[
                  "Name",
                  "Team",
                  "State",
                  "Planned trials",
                  "Created",
                  "Created by",
                ].map((h) => (
                  <th
                    key={h}
                    className="px-4 py-3 text-left text-xs font-medium uppercase tracking-wider text-slate-500"
                  >
                    {h}
                  </th>
                ))}
              </tr>
            </thead>
            <tbody className="divide-y divide-slate-100">
              {query.isPending ? (
                <SkeletonRows cols={COLS} />
              ) : query.isError ? (
                <tr>
                  <td colSpan={COLS} className="px-4 py-5">
                    <ErrorState error={query.error} />
                  </td>
                </tr>
              ) : items.length === 0 ? (
                <tr>
                  <td colSpan={COLS}>
                    <EmptyState
                      label="No batches match this filter."
                      hint={
                        stateFilter
                          ? `Try clearing the "${stateFilter}" state filter.`
                          : "Click '+ New batch' to submit one."
                      }
                    />
                  </td>
                </tr>
              ) : (
                items.map((c) => (
                  <tr key={c.id} className="hover:bg-slate-50">
                    <td className="px-4 py-3">
                      <Link
                        to={`/batches/${c.id}`}
                        title="Open this batch's detail page."
                        className="font-medium text-accent hover:text-accent-hover"
                      >
                        {c.name}
                      </Link>
                    </td>
                    <td className="px-4 py-3 text-slate-700">
                      {c.owner_team?.name ?? c.team_name ?? "—"}
                    </td>
                    <td className="px-4 py-3">
                      <StatusPill variant={batchStateVariant(c.state)}>
                        {c.state}
                      </StatusPill>
                    </td>
                    <td className="px-4 py-3 text-slate-700">
                      {c.expected_trial_count}
                    </td>
                    <td className="px-4 py-3 text-xs text-slate-500">
                      {c.created_at.slice(0, 16).replace("T", " ")}
                    </td>
                    <td className="px-4 py-3 font-mono text-xs text-slate-500">
                      {c.created_by_token_prefix}
                    </td>
                  </tr>
                ))
              )}
            </tbody>
            </table>
          </div>
        </Card.Body>
        {query.data && items.length > 0 ? (
          <Card.Footer>
            <Pagination
              state={page}
              hasNext={query.data.next_cursor !== null}
              onNext={() => {
                if (query.data?.next_cursor) {
                  setPage((p) => nextPage(p, query.data!.next_cursor!));
                }
              }}
              onPrev={() => setPage((p) => prevPage(p))}
            />
          </Card.Footer>
        ) : null}
      </Card>
    </div>
  );
}

function TrialsView({
  search,
  stateFilter,
  batchId,
  teamFilter,
  benchmarkFilter,
  agentFilter,
  modelFilter,
}: {
  search: string;
  stateFilter: string;
  batchId?: string;
  teamFilter: string;
  benchmarkFilter: string;
  agentFilter: string;
  modelFilter: string;
}): JSX.Element {
  const [page, setPage] = useState<PageState>(initialPage);
  const debouncedSearch = useDebouncedValue(search, 300);

  const polling = useAdaptivePolling({
    baseIntervalMs: 4_000,
    minIntervalMs: 3_000,
    maxIntervalMs: 60_000,
    hiddenBehavior: "pause",
    blurBehavior: "slow",
  });

  const query = useQuery({
    queryKey: [
      "trials",
      stateFilter,
      debouncedSearch,
      batchId,
      teamFilter,
      benchmarkFilter,
      agentFilter,
      modelFilter,
      page.current,
    ],
    queryFn: () =>
      api.listTrials({
        state: stateFilter || undefined,
        batch_id: batchId,
        team_id: teamFilter || undefined,
        benchmark_id: benchmarkFilter || undefined,
        agent: agentFilter || undefined,
        model: modelFilter || undefined,
        cursor: page.current ?? undefined,
        limit: "50",
      }),
    refetchInterval: (q) => {
      const data = q.state.data as { items: TrialRow[] } | undefined;
      if (!data) return polling.refetchInterval;
      const allTerminal = data.items.every((row) =>
        TERMINAL_TRIAL_STATES.has(row.state),
      );
      return allTerminal ? 60_000 : polling.refetchInterval;
    },
  });

  const items: TrialRow[] = useMemo(() => {
    const raw = (query.data?.items ?? []) as TrialRow[];
    const q = debouncedSearch.trim().toLowerCase();
    if (!q) return raw;
    return raw.filter(
      (t) =>
        t.task_id.toLowerCase().includes(q) ||
        t.id.toLowerCase().includes(q) ||
        (t.owner_team?.name ?? t.team_name ?? "").toLowerCase().includes(q),
    );
  }, [query.data, debouncedSearch]);
  const failureGroups: FailureGroup[] = useMemo(() => {
    const groups = new Map<string, FailureGroup>();
    for (const item of items) {
      if (item.state !== "failed") continue;
      const reason = item.failure_reason || "unknown_failure";
      const group = groups.get(reason) ?? {
        reason,
        count: 0,
        firstTrialId: item.id,
        messages: [],
      };
      group.count += 1;
      if (item.failure_message && !group.messages.includes(item.failure_message)) {
        group.messages.push(item.failure_message);
      }
      groups.set(reason, group);
    }
    return Array.from(groups.values()).sort((a, b) =>
      b.count === a.count ? a.reason.localeCompare(b.reason) : b.count - a.count,
    );
  }, [items]);

  const COLS = 8;
  return (
    <div className="space-y-3">
      {failureGroups.length > 0 ? (
        <Card>
          <Card.Header
            title="Failure diagnostics"
            description="Failed trials grouped by the platform diagnostic reason returned by the API."
          />
          <Card.Body className="space-y-3">
            {failureGroups.map((group) => (
              <div
                key={group.reason}
                className="rounded-lg border border-slate-200 bg-slate-50 px-3 py-2 text-sm"
              >
                <div className="flex flex-wrap items-center justify-between gap-2">
                  <div>
                    <p className="font-semibold text-slate-900">{group.reason}</p>
                    <p className="text-xs text-slate-500">
                      {group.count} failed trial{group.count === 1 ? "" : "s"}
                    </p>
                  </div>
                  <Link
                    to={`/trials/${group.firstTrialId}`}
                    className="text-sm font-medium text-accent hover:text-accent-hover"
                  >
                    Open {group.firstTrialId}
                  </Link>
                </div>
                {group.messages.length > 0 ? (
                  <ul className="mt-2 space-y-1 text-xs text-slate-600">
                    {group.messages.slice(0, 3).map((message) => (
                      <li key={message}>{message}</li>
                    ))}
                  </ul>
                ) : (
                  <p className="mt-2 text-xs text-slate-500">
                    No failure message was reported; open the trial for logs and artifacts.
                  </p>
                )}
              </div>
            ))}
          </Card.Body>
        </Card>
      ) : null}
      <Card>
      <Card.Body className="p-0">
        <div className="overflow-x-auto">
          <table className="min-w-full divide-y divide-slate-200 text-sm">
            <thead>
              <tr className="bg-slate-50/50">
                {[
                  "ID",
                  "Task",
                  "Team",
                  "State",
                  "Agent",
                  "Evaluator score",
                  "LLM usage",
                  "Submitted",
                ].map((h) => (
                  <th
                    key={h}
                    title={
                      h === "Evaluator score"
                        ? "Reward reported by the evaluator; platform success or failure is shown in State."
                        : h === "LLM usage"
                          ? "Recorded model calls and prompt/completion tokens for this trial."
                        : undefined
                    }
                    className="px-4 py-3 text-left text-xs font-medium uppercase tracking-wider text-slate-500"
                  >
                    {h}
                  </th>
                ))}
              </tr>
            </thead>
            <tbody className="divide-y divide-slate-100">
              {query.isPending ? (
                <SkeletonRows cols={COLS} />
              ) : query.isError ? (
                <tr>
                  <td colSpan={COLS} className="px-4 py-5">
                    <ErrorState error={query.error} />
                  </td>
                </tr>
              ) : items.length === 0 ? (
                <tr>
                  <td colSpan={COLS}>
                    <EmptyState
                      label="No trials match this filter."
                      hint={
                        stateFilter
                          ? `Try changing state from "${stateFilter}" to "all".`
                          : undefined
                      }
                    />
                  </td>
                </tr>
              ) : (
                items.map((t) => (
                  <tr key={t.id} className="hover:bg-slate-50">
                    <td className="px-4 py-3">
                      <Link
                        to={`/trials/${t.id}`}
                        title="Open this trial's detail page, logs, and artifacts."
                        className="font-mono text-xs text-accent hover:text-accent-hover"
                      >
                        {t.id.slice(0, 8)}
                      </Link>
                    </td>
                    <td className="px-4 py-3 font-mono text-xs text-slate-700">
                      {t.task_id}
                    </td>
                    <td className="px-4 py-3 text-slate-700">
                      {t.owner_team?.name ?? t.team_name ?? "—"}
                    </td>
                    <td className="px-4 py-3">
                      <StatusPill variant={trialStateVariant(t.state)}>
                        {t.state}
                      </StatusPill>
                    </td>
                    <td className="px-4 py-3 text-slate-700">
                      {t.agent_name ?? "—"}
                    </td>
                    <td className="px-4 py-3 text-slate-700">
                      {t.aggregate_reward != null
                        ? t.aggregate_reward.toFixed(3)
                        : "—"}
                    </td>
                    <td className="px-4 py-3 text-slate-700">
                      <div>{t.llm_calls_count} calls</div>
                      <div className="text-xs text-slate-500">
                        P {t.total_prompt_tokens} / C{" "}
                        {t.total_completion_tokens}
                      </div>
                    </td>
                    <td className="px-4 py-3 text-xs text-slate-500">
                      {t.submitted_at.slice(0, 16).replace("T", " ")}
                    </td>
                  </tr>
                ))
              )}
            </tbody>
          </table>
        </div>
      </Card.Body>
      {query.data && items.length > 0 ? (
        <Card.Footer>
          <Pagination
            state={page}
            hasNext={query.data.next_cursor !== null}
            onNext={() => {
              if (query.data?.next_cursor) {
                setPage((p) => nextPage(p, query.data!.next_cursor!));
              }
            }}
            onPrev={() => setPage((p) => prevPage(p))}
          />
        </Card.Footer>
      ) : null}
      </Card>
    </div>
  );
}

export default function Monitor(): JSX.Element {
  const [searchParams, setSearchParams] = useSearchParams();
  const auth = useAuth();
  const view: View = searchParams.get("view") === "trials" ? "trials" : "batches";
  const batchIdFilter = searchParams.get("batch_id") ?? undefined;
  const search = searchParams.get("q") ?? "";
  const stateFilter = searchParams.get("state") ?? "";
  const teamFilter = searchParams.get("team_id") ?? "";
  const benchmarkFilter = searchParams.get("benchmark_id") ?? "";
  const agentFilter = searchParams.get("agent") ?? "";
  const modelFilter = searchParams.get("model") ?? "";

  const teamsQuery = useQuery({
    queryKey: ["admin-teams", auth.isAdmin],
    queryFn: () => api.listAdminTeams(),
    enabled: auth.isAdmin,
  });
  const adminTeams = teamsQuery.data?.items ?? [];
  const selectedTeamKnown = adminTeams.some((team) => team.id === teamFilter);

  const updateParam = (key: string, value: string): void => {
    const next = new URLSearchParams(searchParams);
    if (value) next.set(key, value);
    else next.delete(key);
    setSearchParams(next);
  };

  const setView = (v: View): void => {
    const next = new URLSearchParams(searchParams);
    next.set("view", v);
    setSearchParams(next);
  };

  const stateOptions = view === "batches" ? BATCH_STATE_OPTIONS : TRIAL_STATE_OPTIONS;

  return (
    <div className="space-y-6">
      <header className="flex flex-wrap items-end justify-between gap-4">
        <div>
          <h1 className="text-2xl font-bold text-slate-900">Monitor</h1>
          <p className="mt-1 text-sm text-slate-500">
            Live-updating view of your batches and trials.
          </p>
        </div>
        <SegmentedToggle value={view} onChange={setView} />
      </header>

      <div className="flex flex-wrap items-center gap-3">
        <Input
          value={search}
          onChange={(e) => updateParam("q", e.target.value)}
          placeholder={
            view === "batches"
              ? "Search batches by name or ID..."
              : "Search trials by task ID or trial ID..."
          }
          className="max-w-sm"
          aria-label="search"
          title={
            view === "batches"
              ? "Filter batches by human name or batch ID."
              : "Filter trials by task ID or trial ID."
          }
        />
        <label className="flex items-center gap-2 text-sm text-slate-600">
          <span className="text-xs uppercase tracking-wider text-slate-400">State</span>
          <select
            value={stateFilter}
            onChange={(e) => updateParam("state", e.target.value)}
            className="rounded-lg border border-slate-200 bg-white px-3 py-2 text-sm text-slate-700"
            aria-label="filter by state"
            title="Limit the table to one lifecycle state, or choose all."
          >
            <option value="">All states</option>
            {stateOptions.map((s) => (
              <option key={s} value={s}>
                {stateOptionLabel(s)}
              </option>
            ))}
          </select>
        </label>
        {auth.isAdmin ? (
          <label className="flex items-center gap-2 text-sm text-slate-600">
            <span className="text-xs uppercase tracking-wider text-slate-400">Team</span>
            <select
              value={teamFilter}
              onChange={(e) => updateParam("team_id", e.target.value)}
              className="rounded-lg border border-slate-200 bg-white px-3 py-2 text-sm text-slate-700"
              aria-label="filter by team"
              title="Limit results to one internal team. Platform admins can inspect all teams."
            >
              <option value="">All teams</option>
              {teamFilter && !selectedTeamKnown ? (
                <option value={teamFilter}>{teamFilter}</option>
              ) : null}
              {adminTeams.map((team) => (
                <option key={team.id} value={team.id}>
                  {team.name}
                </option>
              ))}
            </select>
          </label>
        ) : null}
        <Input
          value={benchmarkFilter}
          onChange={(e) => updateParam("benchmark_id", e.target.value)}
          placeholder="Benchmark"
          className="max-w-[10rem]"
          aria-label="filter by benchmark"
          title="Filter by benchmark id when the API can resolve it."
        />
        <Input
          value={agentFilter}
          onChange={(e) => updateParam("agent", e.target.value)}
          placeholder="Agent"
          className="max-w-[10rem]"
          aria-label="filter by agent"
          title="Filter by agent adapter name."
        />
        <Input
          value={modelFilter}
          onChange={(e) => updateParam("model", e.target.value)}
          placeholder="Model"
          className="max-w-[10rem]"
          aria-label="filter by model"
          title="Filter by model id or model name."
        />
        {view === "trials" && batchIdFilter ? (
          <span className="rounded-md border border-slate-200 bg-slate-50 px-2 py-1 text-xs text-slate-700">
            batch_id = <code className="font-mono">{batchIdFilter.slice(0, 8)}</code>
            <button
              type="button"
              className="ml-2 text-slate-500 hover:text-slate-900"
              title="Remove this batch_id filter and show trials from all batches."
              onClick={() => {
                const next = new URLSearchParams(searchParams);
                next.delete("batch_id");
                setSearchParams(next);
              }}
            >
              clear
            </button>
          </span>
        ) : null}
      </div>

      {view === "batches" ? (
        <BatchesView
          search={search}
          stateFilter={stateFilter}
          teamFilter={teamFilter}
          benchmarkFilter={benchmarkFilter}
          agentFilter={agentFilter}
          modelFilter={modelFilter}
        />
      ) : (
        <TrialsView
          search={search}
          stateFilter={stateFilter}
          batchId={batchIdFilter}
          teamFilter={teamFilter}
          benchmarkFilter={benchmarkFilter}
          agentFilter={agentFilter}
          modelFilter={modelFilter}
        />
      )}
    </div>
  );
}
