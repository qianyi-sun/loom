export function Help({ children }: { children: React.ReactNode }): JSX.Element {
  return <p className="mt-1 text-xs text-slate-500">{children}</p>;
}

export function FieldLabel({
  children,
  hint,
}: {
  children: React.ReactNode;
  hint?: React.ReactNode;
}): JSX.Element {
  return (
    <div className="mb-1 flex min-w-0 flex-wrap items-baseline gap-x-2 gap-y-0.5">
      <span className="min-w-0 text-xs font-medium uppercase tracking-wider text-slate-500">{children}</span>
      {hint ? (
        <span className="shrink-0 text-xs font-normal normal-case tracking-normal text-slate-500">
          {hint}
        </span>
      ) : null}
    </div>
  );
}
