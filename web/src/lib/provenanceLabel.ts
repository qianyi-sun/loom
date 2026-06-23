export function provenanceLabel(item: Record<string, unknown>): string {
  const kind = typeof item.kind === "string" ? item.kind : "source";
  const batch =
    typeof item.source_batch_id === "string"
      ? `batch ${item.source_batch_id}`
      : null;
  const trial =
    typeof item.source_trial_id === "string"
      ? `trial ${item.source_trial_id}`
      : null;
  const artifact =
    typeof item.source_artifact_key === "string"
      ? item.source_artifact_key
      : null;

  return [kind.replaceAll("_", " "), batch, trial, artifact]
    .filter(Boolean)
    .join(" · ");
}
