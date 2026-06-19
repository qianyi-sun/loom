/**
 * Usage dashboard — date-range picker + per-bucket SVG bar chart.
 * Inline SVG keeps deps small; no charting library.
 */

import { useQuery } from "@tanstack/react-query";
import { useMemo, useState } from "react";

import { api } from "../api/client";
import { Card } from "../components/Card";
import EmptyState from "../components/EmptyState";
import ErrorState from "../components/ErrorState";
import { Input } from "../components/Input";
import LoadingState from "../components/LoadingState";
import { StatCard } from "../components/StatCard";

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
  const [start, setStart] = useState(defaultStart());
  const [end, setEnd] = useState(defaultEnd());
  const [groupBy, setGroupBy] = useState<"day" | "week" | "month">("day");
  const [teamId, setTeamId] = useState("");

  const query = useQuery({
    queryKey: ["usage", start, end, groupBy, teamId],
    queryFn: () =>
      api.getUsage({
        start,
        end,
        group_by: groupBy,
        team_id: teamId || undefined,
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
        <label className="block min-w-[260px] flex-1">
          <span className="mb-1 block text-xs font-medium uppercase tracking-wider text-slate-500">
            Team
          </span>
          <Input
            placeholder="UUID, blank for own team"
            value={teamId}
            onChange={(e) => setTeamId(e.target.value)}
          />
        </label>
      </div>

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

type Bucket = {
  start_at: string;
  trial_count: number;
  total_cost_usd: number;
  llm_input_tokens: number;
  llm_output_tokens: number;
  trials_currently_succeeded: number;
  trials_currently_failed: number;
};

function UsageContent({ buckets }: { buckets: Bucket[] }): JSX.Element {
  const totals = useMemo(
    () =>
      buckets.reduce(
        (acc, b) => ({
          trial_count: acc.trial_count + b.trial_count,
          total_cost_usd: acc.total_cost_usd + b.total_cost_usd,
          llm_input_tokens: acc.llm_input_tokens + b.llm_input_tokens,
          llm_output_tokens: acc.llm_output_tokens + b.llm_output_tokens,
        }),
        {
          trial_count: 0,
          total_cost_usd: 0,
          llm_input_tokens: 0,
          llm_output_tokens: 0,
        },
      ),
    [buckets],
  );

  return (
    <div className="space-y-6">
      <div className="grid grid-cols-2 gap-3 md:grid-cols-4">
        <StatCard label="Trials" value={totals.trial_count.toLocaleString()} />
        <StatCard
          label="Estimated LLM cost"
          value={`$${totals.total_cost_usd.toFixed(4)}`}
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
            getValue={(b) => b.total_cost_usd}
            formatValue={(v) => `$${v.toFixed(4)}`}
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
                  <tr key={b.start_at} className="hover:bg-slate-50">
                    <td className="px-4 py-3 font-mono text-xs text-slate-700">
                      {b.start_at.slice(0, 10)}
                    </td>
                    <td className="px-4 py-3 text-slate-700">{b.trial_count}</td>
                    <td className="px-4 py-3 text-emerald-700">
                      {b.trials_currently_succeeded}
                    </td>
                    <td className="px-4 py-3 text-red-700">
                      {b.trials_currently_failed}
                    </td>
                    <td className="px-4 py-3 text-slate-700">
                      {b.llm_input_tokens.toLocaleString()}
                    </td>
                    <td className="px-4 py-3 text-slate-700">
                      {b.llm_output_tokens.toLocaleString()}
                    </td>
                    <td className="px-4 py-3 text-slate-700">
                      ${b.total_cost_usd.toFixed(4)}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </Card.Body>
      </Card>
    </div>
  );
}

function Chart({
  buckets,
  getValue,
  formatValue,
}: {
  buckets: Bucket[];
  getValue: (b: Bucket) => number;
  formatValue: (v: number) => string;
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
              <title>{`${b.start_at.slice(0, 10)} — ${formatValue(v)}`}</title>
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
