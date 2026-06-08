export default function EmptyState({
  label,
}: {
  label: string;
}): JSX.Element {
  return <div className="loom-empty">{label}</div>;
}
