/**
 * Usage dashboard — date-range picker + per-bucket SVG bar chart.
 * Inline SVG keeps deps small; no charting library.
 */

import { useQuery } from "@tanstack/react-query";
import { useMemo, useState } from "react";

import { api } from "../api/client";
import EmptyState from "../components/EmptyState";
import ErrorState from "../components/ErrorState";
import LoadingState from "../components/LoadingState";

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
  const [groupBy, setGroupBy] = useState<"day" | "week" | "month">(
    "day",
  );
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
    <>
      <div className="loom-page-header">
        <h1>Usage</h1>
      </div>

      <div className="loom-filters">
        <label>
          Start:&nbsp;
          <input
            type="date"
            value={start}
            onChange={(e) => setStart(e.target.value)}
          />
        </label>
        <label>
          End:&nbsp;
          <input
            type="date"
            value={end}
            onChange={(e) => setEnd(e.target.value)}
          />
        </label>
        <label>
          Group by:&nbsp;
          <select
            value={groupBy}
            onChange={(e) =>
              setGroupBy(e.target.value as "day" | "week" | "month")
            }
          >
            <option value="day">day</option>
            <option value="week">week</option>
            <option value="month">month</option>
          </select>
        </label>
        <label>
          Team:&nbsp;
          <input
            placeholder="UUID, blank for own team"
            value={teamId}
            onChange={(e) => setTeamId(e.target.value)}
            style={{ width: "240px" }}
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
    </>
  );
}

function UsageContent({
  buckets,
}: {
  buckets: {
    start_at: string;
    trial_count: number;
    total_cost_usd: number;
    llm_input_tokens: number;
    llm_output_tokens: number;
    trials_currently_succeeded: number;
    trials_currently_failed: number;
  }[];
}): JSX.Element {
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
    <>
      <div className="loom-card">
        <div
          style={{
            display: "grid",
            gridTemplateColumns: "repeat(auto-fit, minmax(140px, 1fr))",
            gap: "0.6rem",
          }}
        >
          <Stat label="Trials" value={totals.trial_count.toLocaleString()} />
          <Stat
            label="Cost"
            value={`$${totals.total_cost_usd.toFixed(4)}`}
          />
          <Stat
            label="Input tokens"
            value={totals.llm_input_tokens.toLocaleString()}
          />
          <Stat
            label="Output tokens"
            value={totals.llm_output_tokens.toLocaleString()}
          />
        </div>
      </div>

      <div className="loom-card">
        <h2 style={{ marginTop: 0 }}>Cost per bucket</h2>
        <Chart
          buckets={buckets}
          getValue={(b) => b.total_cost_usd}
          formatValue={(v) => `$${v.toFixed(4)}`}
        />
      </div>

      <div className="loom-card">
        <h2 style={{ marginTop: 0 }}>Breakdown</h2>
        <table>
          <thead>
            <tr>
              <th>Bucket</th>
              <th>Trials</th>
              <th>Succeeded</th>
              <th>Failed</th>
              <th>Input</th>
              <th>Output</th>
              <th>Cost</th>
            </tr>
          </thead>
          <tbody>
            {buckets.map((b) => (
              <tr key={b.start_at}>
                <td className="loom-mono">{b.start_at.slice(0, 10)}</td>
                <td>{b.trial_count}</td>
                <td>{b.trials_currently_succeeded}</td>
                <td>{b.trials_currently_failed}</td>
                <td>{b.llm_input_tokens.toLocaleString()}</td>
                <td>{b.llm_output_tokens.toLocaleString()}</td>
                <td>${b.total_cost_usd.toFixed(4)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </>
  );
}

function Chart({
  buckets,
  getValue,
  formatValue,
}: {
  buckets: { start_at: string }[] & { length: number };
  getValue: (
    b: { start_at: string; total_cost_usd: number },
  ) => number;
  formatValue: (v: number) => string;
}): JSX.Element {
  const values = (
    buckets as { start_at: string; total_cost_usd: number }[]
  ).map(getValue);
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
      style={{ width: "100%", height: "auto" }}
    >
      {(buckets as { start_at: string; total_cost_usd: number }[]).map(
        (b, i) => {
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
                fill="var(--color-accent)"
                opacity={0.8}
              >
                <title>{`${b.start_at.slice(0, 10)} — ${formatValue(v)}`}</title>
              </rect>
              <text
                x={x + barWidth / 2}
                y={height + 14}
                fontSize="9"
                textAnchor="middle"
                fill="var(--color-muted)"
              >
                {b.start_at.slice(5, 10)}
              </text>
            </g>
          );
        },
      )}
    </svg>
  );
}

function Stat({
  label,
  value,
}: {
  label: string;
  value: string;
}): JSX.Element {
  return (
    <div>
      <div className="loom-muted" style={{ fontSize: "0.8em" }}>
        {label}
      </div>
      <div>{value}</div>
    </div>
  );
}
