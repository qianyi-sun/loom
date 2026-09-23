import type { ArtifactSummary } from "../api";

export const ARTIFACT_LABELS: Array<[keyof ArtifactSummary, string]> = [
  ["reports", "Reports"],
  ["trajectories", "Trajectories"],
  ["reusable_outputs", "Outputs"],
  ["logs_diagnostics", "Logs"],
  ["raw_diagnostics", "Raw/internal"],
];

