import { cn } from "../lib/cn";

export default function EmptyState({
  label,
  hint,
  className,
}: {
  label: string;
  hint?: string;
  className?: string;
}): JSX.Element {
  return (
    <div
      className={cn(
        "flex flex-col items-center justify-center gap-1 px-6 py-12 text-center",
        className,
      )}
    >
      <p className="text-sm font-medium text-slate-600">{label}</p>
      {hint ? <p className="text-xs text-slate-400">{hint}</p> : null}
    </div>
  );
}
