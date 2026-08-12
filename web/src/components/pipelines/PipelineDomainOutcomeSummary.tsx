import { bytewiseCompare } from "../../lib/pipelinePresentation";

export default function PipelineDomainOutcomeSummary({
  outcomes,
}: {
  outcomes: Record<string, number>;
}): JSX.Element {
  const entries = Object.entries(outcomes).sort(([left], [right]) => bytewiseCompare(left, right));
  if (entries.length === 0) return <span className="text-slate-500">—</span>;
  const visible = entries.slice(0, 5);
  return (
    <span aria-label="Succeeded stage domain outcomes" className="text-xs text-slate-700">
      {visible.map(([outcome, count]) => `${outcome} × ${count}`).join(", ")}
      {entries.length > visible.length ? ` +${entries.length - visible.length} more` : ""}
    </span>
  );
}
