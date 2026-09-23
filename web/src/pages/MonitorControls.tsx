import type { View } from "./monitorPresentation";
export function SegmentedToggle({
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
              (active ? "bg-white text-slate-900 shadow-sm" : "text-slate-600 hover:text-slate-900")
            }
          >
            {v === "batches" ? "Batches" : "Trials"}
          </button>
        );
      })}
    </div>
  );
}

export function SkeletonRows({ rows = 5, cols }: { rows?: number; cols: number }): JSX.Element {
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
