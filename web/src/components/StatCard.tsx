/**
 * StatCard — labeled metric block. Uppercase tracking-wide label
 * above a large value, optional note underneath. Used on overview
 * dashboards and detail-page summaries.
 */
import { cn } from "../lib/cn";

export interface StatCardProps {
  label: string;
  value: React.ReactNode;
  note?: React.ReactNode;
  valueAside?: React.ReactNode;
  className?: string;
}

export function StatCard({
  label,
  value,
  note,
  valueAside,
  className,
}: StatCardProps): JSX.Element {
  return (
    <div
      className={cn(
        "glass-card glass-card-hover p-5 transition-colors",
        className,
      )}
    >
      <p className="mb-2 text-xs font-medium uppercase tracking-wider text-slate-500">
        {label}
      </p>
      <div className="flex items-end gap-2">
        <p className="text-2xl font-bold text-slate-800">{value}</p>
        {valueAside ? <div className="shrink-0">{valueAside}</div> : null}
      </div>
      {note ? <p className="mt-2 text-xs text-slate-500">{note}</p> : null}
    </div>
  );
}
