export default function LoadingState({
  label = "Loading…",
  announce = true,
}: {
  label?: string;
  announce?: boolean;
}): JSX.Element {
  return (
    <div role={announce ? "status" : undefined} aria-live={announce ? "polite" : undefined} className="flex items-center justify-center px-6 py-12 text-sm text-slate-500">
      <span
        aria-hidden="true"
        className="mr-2 h-2.5 w-2.5 animate-pulse rounded-full bg-accent"
      />
      <span>{label}</span>
    </div>
  );
}
