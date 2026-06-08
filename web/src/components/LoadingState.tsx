export default function LoadingState({
  label = "Loading…",
}: {
  label?: string;
}): JSX.Element {
  return (
    <div className="loom-empty">
      <em>{label}</em>
    </div>
  );
}
