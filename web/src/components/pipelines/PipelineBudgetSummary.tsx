import type { PipelineRunDetail } from "../../api/client";
import { formatMicrousd } from "../../lib/pipelinePresentation";

const ROWS = [
  "max_wall_seconds",
  "max_gpu_seconds",
  "max_provider_cost_usd",
  "max_artifact_bytes",
  "max_stage_runs",
  "max_attempts_total",
] as const;

export default function PipelineBudgetSummary({
  budget,
}: {
  budget: PipelineRunDetail["budget"];
}): JSX.Element {
  if (!budget) return <p className="text-sm text-slate-500">Budget ledger unavailable.</p>;
  return (
    <div className="overflow-x-auto">
      <table className="min-w-full text-sm">
        <thead><tr>{["Budget", "Limit", "Reserved", "Settled", "Remaining"].map((label) => <th key={label} className="px-3 py-2 text-left text-xs uppercase text-slate-500">{label}</th>)}</tr></thead>
        <tbody>
          {ROWS.map((name) => {
            const row = budget[name];
            const invalid = row.remaining < 0 || row.reserved + row.settled > row.limit;
            const value = (amount: number): string => name === "max_provider_cost_usd" ? formatMicrousd(amount) : amount.toLocaleString();
            return (
              <tr key={name} className={invalid ? "bg-red-50 text-red-800" : "border-t border-slate-100"}>
                <th className="px-3 py-2 text-left font-medium">{name}</th>
                <td className="px-3 py-2">{value(row.limit)}</td><td className="px-3 py-2">{value(row.reserved)}</td><td className="px-3 py-2">{value(row.settled)}</td><td className="px-3 py-2">{invalid ? "budget ledger invariant" : value(row.remaining)}</td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}
