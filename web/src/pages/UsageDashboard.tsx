/**
 * Usage dashboard — date-range picker + per-bucket SVG bar chart.
 * Inline SVG keeps deps small; no charting library.
 */

import { useQuery } from "@tanstack/react-query";
import { useMemo, useState } from "react";

import { api } from "../api/client";
import type { components } from "../api/schema";
import { useAuth } from "../auth/useAuth";
import { Card } from "../components/Card";
import CommandSnippet from "../components/CommandSnippet";
import EmptyState from "../components/EmptyState";
import ErrorState from "../components/ErrorState";
import LoadingState from "../components/LoadingState";
import { StatCard } from "../components/StatCard";
import { usageCommand } from "../lib/quickstartSnippets";
import {
  formatUsageCost,
  summarizeUsageCost,
  usageCostAmount,
  usageCostStatus,
  usageEstimateConfidence,
} from "../lib/usageCost";

const SELECT_CLASSES =
  "rounded-lg border border-slate-200 bg-white px-3 py-2 text-sm text-slate-700";
const DATE_INPUT_CLASSES =
  "rounded-lg border border-slate-200 bg-white px-3 py-2 text-sm text-slate-700";

function defaultStart(): string {
  const d = new Date();
  d.setDate(d.getDate() - 13);
  return d.toISOString().slice(0, 10);
}
function defaultEnd(): string {
  return new Date().toISOString().slice(0, 10);
}

export default function UsageDashboard(): JSX.Element {
  const auth = useAuth();
  const [start, setStart] = useState(defaultStart());
  const [end, setEnd] = useState(defaultEnd());
  const [groupBy, setGroupBy] = useState<"day" | "week" | "month">("day");
  const [teamId, setTeamId] = useState("");

  const teamsQuery = useQuery({
    queryKey: ["admin-teams", auth.isAdmin],
    queryFn: () => api.listAdminTeams(),
    enabled: auth.isAdmin,
  });
  const adminTeams = teamsQuery.data?.items ?? [];
  const selectedTeamKnown = adminTeams.some((team) => team.id === teamId);
  const currentTeam = auth.teams.find((team) => team.id === auth.currentTeamId);
  const currentTeamName = currentTeam?.name ?? "Current team";

  const query = useQuery({
    queryKey: ["usage", start, end, groupBy, teamId, auth.isAdmin],
    queryFn: () =>
      api.getUsage({
        start,
        end,
        group_by: groupBy,
        team_id: teamId || undefined,
        include_batches: auth.isAdmin ? true : undefined,
      }),
  });

  return (
    <div className="space-y-6">
      <header>
        <h1 className="text-2xl font-bold text-slate-900">Usage</h1>
        <p className="mt-1 text-sm text-slate-500">
          Trials, tokens, and estimated LLM cost per bucket over the selected
          range. Cost is derived from recorded LLM calls and active rate cards.
        </p>
      </header>

      <div className="flex flex-wrap items-end gap-3">
        <label className="block">
          <span className="mb-1 block text-xs font-medium uppercase tracking-wider text-slate-500">
            Start
          </span>
          <input
            type="date"
            value={start}
            onChange={(e) => setStart(e.target.value)}
            className={DATE_INPUT_CLASSES}
          />
        </label>
        <label className="block">
          <span className="mb-1 block text-xs font-medium uppercase tracking-wider text-slate-500">
            End
          </span>
          <input
            type="date"
            value={end}
            onChange={(e) => setEnd(e.target.value)}
            className={DATE_INPUT_CLASSES}
          />
        </label>
        <label className="block">
          <span className="mb-1 block text-xs font-medium uppercase tracking-wider text-slate-500">
            Group by
          </span>
          <select
            value={groupBy}
            onChange={(e) =>
              setGroupBy(e.target.value as "day" | "week" | "month")
            }
            className={SELECT_CLASSES}
          >
            <option value="day">day</option>
            <option value="week">week</option>
            <option value="month">month</option>
          </select>
        </label>
        {auth.isAdmin ? (
          <label className="block min-w-[260px] flex-1">
            <span className="mb-1 block text-xs font-medium uppercase tracking-wider text-slate-500">
              Team
            </span>
            <select
              value={teamId}
              onChange={(e) => setTeamId(e.target.value)}
              className={SELECT_CLASSES + " w-full"}
              title="Limit usage to one internal team. Leave blank for platform-wide usage."
            >
              <option value="">All teams</option>
              {teamId && !selectedTeamKnown ? (
                <option value={teamId}>{teamId}</option>
              ) : null}
              {adminTeams.map((team) => (
                <option key={team.id} value={team.id}>
                  {team.name}
                </option>
              ))}
            </select>
          </label>
        ) : (
          <div className="min-w-[220px] flex-1 rounded-lg border border-slate-200 bg-slate-50 px-3 py-2">
            <span className="block text-xs font-medium uppercase tracking-wider text-slate-500">
              Current team
            </span>
            <p className="mt-1 text-sm font-medium text-slate-800">
              {currentTeamName}
            </p>
          </div>
        )}
      </div>

      <CommandSnippet
        label="Usage CLI"
        command={usageCommand(start, end, teamId, auth.isAdmin)}
        helperText={
          auth.isAdmin
            ? teamId
              ? "The web filter is scoped to the selected internal team."
              : "Leave the team filter blank to inspect platform-wide usage."
            : `Usage is scoped to ${currentTeamName}.`
        }
      />

      {query.isPending ? <LoadingState /> : null}
      {query.isError ? <ErrorState error={query.error} /> : null}
      {query.data ? (
        query.data.degraded ? (
          <EmptyState label="LLM call data is not available." />
        ) : query.data.buckets.length === 0 ? (
          <EmptyState label="No LLM activity in this range." />
        ) : (
          <UsageContent buckets={query.data.buckets} />
        )
      ) : null}
    </div>
  );
}

type Bucket = components["schemas"]["UsageBucket"];
type BatchUsage = components["schemas"]["UsageBatch"];

function UsageContent({ buckets }: { buckets: Bucket[] }): JSX.Element {
  const totals = useMemo(
    () => {
      const usageCost = summarizeUsageCost(buckets);
      const counters = buckets.reduce(
        (acc, b) => ({
          trial_count: acc.trial_count + b.trial_count,
          llm_input_tokens: acc.llm_input_tokens + b.llm_input_tokens,
          llm_output_tokens: acc.llm_output_tokens + b.llm_output_tokens,
        }),
        {
          trial_count: 0,
          llm_input_tokens: 0,
          llm_output_tokens: 0,
        },
      );
      return { ...counters, usageCost };
    },
    [buckets],
  );
  const hasBatchBreakdown = buckets.some((b) => (b.batches ?? []).length > 0);

  return (
    <div className="space-y-6">
      <div className="grid grid-cols-2 gap-3 md:grid-cols-5">
        <StatCard label="Trials" value={totals.trial_count.toLocaleString()} />
        <StatCard
          label="Estimated LLM cost"
          value={formatUsageCost(totals.usageCost)}
          note={usageCostStatus(totals.usageCost)}
        />
        <StatCard
          label="Usage confidence"
          value={usageEstimateConfidence(totals.usageCost)}
        />
        <StatCard
          label="Input tokens"
          value={totals.llm_input_tokens.toLocaleString()}
        />
        <StatCard
          label="Output tokens"
          value={totals.llm_output_tokens.toLocaleString()}
        />
      </div>
      <p className="text-xs text-slate-500">
        Trial counts reflect currently stored trial states. Token totals come
        from LLM gateway call records, not evaluator rewards.
      </p>

      <Card>
        <Card.Header
          title="Estimated LLM cost per bucket"
          description="Each bar is the sum of recorded model-call costs for that time bucket."
        />
        <Card.Body>
          <Chart
            buckets={buckets}
            getValue={(b) => usageCostAmount(b) ?? 0}
            formatValue={(_v, b) => formatUsageCost(b)}
          />
        </Card.Body>
      </Card>

      <Card>
        <Card.Header title="Breakdown" />
        <Card.Body className="p-0">
          <div className="overflow-x-auto">
            <table className="min-w-full divide-y divide-slate-200 text-sm">
              <thead>
                <tr className="bg-slate-50/50">
                  {[
                    "Bucket",
                    "Trials",
                    "Succeeded",
                    "Failed",
                    "Input tokens",
                    "Output tokens",
                    "Cost status",
                    "Usage confidence",
                    "LLM cost",
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
                {buckets.map((b) => (
                  <UsageBucketRows
                    key={b.start_at}
                    bucket={b}
                    showBatchBreakdown={hasBatchBreakdown}
                  />
                ))}
              </tbody>
            </table>
          </div>
        </Card.Body>
      </Card>
    </div>
  );
}

function UsageBucketRows({
  bucket,
  showBatchBreakdown,
}: {
  bucket: Bucket;
  showBatchBreakdown: boolean;
}): JSX.Element {
  return (
    <>
      <tr className="hover:bg-slate-50">
        <td className="px-4 py-3 font-mono text-xs text-slate-700">
          {bucket.start_at.slice(0, 10)}
        </td>
        <td className="px-4 py-3 text-slate-700">{bucket.trial_count}</td>
        <td className="px-4 py-3 text-emerald-700">
          {bucket.trials_currently_succeeded}
        </td>
        <td className="px-4 py-3 text-red-700">
          {bucket.trials_currently_failed}
        </td>
        <td className="px-4 py-3 text-slate-700">
          {bucket.llm_input_tokens.toLocaleString()}
        </td>
        <td className="px-4 py-3 text-slate-700">
          {bucket.llm_output_tokens.toLocaleString()}
        </td>
        <td className="px-4 py-3 text-slate-700">
          {usageCostStatus(bucket)}
        </td>
        <td className="px-4 py-3 text-slate-700">
          {usageEstimateConfidence(bucket)}
        </td>
        <td className="px-4 py-3 text-slate-700">
          {formatUsageCost(bucket)}
        </td>
      </tr>
      {showBatchBreakdown && (bucket.batches ?? []).length > 0 ? (
        <tr>
          <td colSpan={9} className="bg-slate-50/70 px-4 py-3">
            <div className="overflow-x-auto">
              <table className="min-w-full text-xs">
                <thead>
                  <tr>
                    {[
                      "Batch",
                      "Team",
                      "Trials",
                      "Input tokens",
                      "Output tokens",
                      "Cost status",
                      "Usage confidence",
                      "LLM cost",
                    ].map((h) => (
                      <th
                        key={h}
                        className="px-3 py-2 text-left font-medium uppercase tracking-wider text-slate-500"
                      >
                        {h}
                      </th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {(bucket.batches ?? []).map((batch) => (
                    <UsageBatchRow key={batch.batch_id} batch={batch} />
                  ))}
                </tbody>
              </table>
            </div>
          </td>
        </tr>
      ) : null}
    </>
  );
}

function UsageBatchRow({ batch }: { batch: BatchUsage }): JSX.Element {
  return (
    <tr className="border-t border-slate-200">
      <td className="px-3 py-2 text-slate-800">
        <div className="font-medium">{batch.batch_name}</div>
        <div className="font-mono text-[11px] text-slate-500">
          {batch.batch_id}
        </div>
      </td>
      <td className="px-3 py-2 text-slate-600">
        {batch.team_name ?? batch.team_id}
      </td>
      <td className="px-3 py-2 text-slate-700">{batch.trial_count}</td>
      <td className="px-3 py-2 text-slate-700">
        {batch.llm_input_tokens.toLocaleString()}
      </td>
      <td className="px-3 py-2 text-slate-700">
        {batch.llm_output_tokens.toLocaleString()}
      </td>
      <td className="px-3 py-2 text-slate-700">
        {usageCostStatus(batch)}
      </td>
      <td className="px-3 py-2 text-slate-700">
        {usageEstimateConfidence(batch)}
      </td>
      <td className="px-3 py-2 text-slate-700">
        {formatUsageCost(batch)}
      </td>
    </tr>
  );
}

function Chart({
  buckets,
  getValue,
  formatValue,
}: {
  buckets: Bucket[];
  getValue: (b: Bucket) => number;
  formatValue: (v: number, b: Bucket) => string;
}): JSX.Element {
  const values = buckets.map(getValue);
  const max = Math.max(1e-9, ...values);
  const width = 800;
  const height = 200;
  const barGap = 4;
  const barWidth = Math.max(
    8,
    (width - barGap * (buckets.length + 1)) / buckets.length,
  );

  return (
    <svg
      viewBox={`0 0 ${width} ${height + 30}`}
      preserveAspectRatio="none"
      className="h-auto w-full"
    >
      {buckets.map((b, i) => {
        const v = getValue(b);
        const h = (v / max) * height;
        const x = barGap + i * (barWidth + barGap);
        const y = height - h;
        return (
          <g key={b.start_at}>
            <rect
              x={x}
              y={y}
              width={barWidth}
              height={h}
              fill="#6366f1"
              opacity={0.85}
            >
              <title>{`${b.start_at.slice(0, 10)} — ${formatValue(v, b)}`}</title>
            </rect>
            <text
              x={x + barWidth / 2}
              y={height + 14}
              fontSize="9"
              textAnchor="middle"
              fill="#64748b"
            >
              {b.start_at.slice(5, 10)}
            </text>
          </g>
        );
      })}
    </svg>
  );
}
