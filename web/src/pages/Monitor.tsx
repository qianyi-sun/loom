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
  name: string;
  state: string;
  expected_trial_count: number;
  created_at: string;
  created_by_token_prefix: string;
}
interface TrialRow {
  id: string;
  task_id: string;
  state: string;
  agent_name: string | null;
  aggregate_reward: number | null;
  total_prompt_tokens: number;
  total_completion_tokens: number;
  llm_calls_count: number;
  submitted_at: string;
}

function BatchesView({
  search,
  stateFilter,
}: {
  search: string;
  stateFilter: string;
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

  const COLS = 5;
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
}: {
  search: string;
  stateFilter: string;
  batchId?: string;
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
    queryKey: ["trials", stateFilter, debouncedSearch, batchId, page.current],
    queryFn: () =>
      api.listTrials({
        state: stateFilter || undefined,
        batch_id: batchId,
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
        t.task_id.toLowerCase().includes(q) || t.id.toLowerCase().includes(q),
    );
  }, [query.data, debouncedSearch]);

  const COLS = 7;
  return (
    <Card>
      <Card.Body className="p-0">
        <div className="overflow-x-auto">
          <table className="min-w-full divide-y divide-slate-200 text-sm">
            <thead>
              <tr className="bg-slate-50/50">
                {[
                  "ID",
                  "Task",
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
  );
}

export default function Monitor(): JSX.Element {
  const [searchParams, setSearchParams] = useSearchParams();
  const view: View = searchParams.get("view") === "trials" ? "trials" : "batches";
  const batchIdFilter = searchParams.get("batch_id") ?? undefined;

  const setView = (v: View): void => {
    const next = new URLSearchParams(searchParams);
    next.set("view", v);
    setSearchParams(next);
  };

  const [search, setSearch] = useState("");
  const [stateFilter, setStateFilter] = useState("");

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
          onChange={(e) => setSearch(e.target.value)}
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
            onChange={(e) => setStateFilter(e.target.value)}
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
        <BatchesView search={search} stateFilter={stateFilter} />
      ) : (
        <TrialsView
          search={search}
          stateFilter={stateFilter}
          batchId={batchIdFilter}
        />
      )}
    </div>
  );
}
