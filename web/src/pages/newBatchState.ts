import { type ProviderOverride } from "../components/agentModelSelection";
import { type BatchPurpose } from "./newBatch/formState";
export const FAN_OUT_CONFIRM_THRESHOLD = 200;

export const MAX_COMBINATIONS = 16;

export const PURPOSE_OPTIONS: Array<{
  value: BatchPurpose;
  title: string;
  blurb: string;
  /** Accessible name for tests + screen readers. */
  radioName: string;
}> = [
  {
    value: "evaluation",
    title: "Evaluate",
    blurb: "Official benchmarks with verification",
    radioName: "Evaluation",
  },
  {
    value: "trajectory_generation",
    title: "Generate trajectories",
    blurb: "TaskSets first; benchmarks optional",
    radioName: "Trajectory generation",
  },
];

export type ProviderSelectionResult = { ok: true; value: ProviderOverride[] } | { ok: false; error: string };

export function preflightLabel(
  status?: string | null,
  failureKind?: string | null,
): string {
  if (status === "valid") return "preflight valid";
  if (status === "failed") {
    return failureKind === "inconclusive" ? "preflight inconclusive" : "preflight failed";
  }
  return "not preflighted";
}

export function freshSeed(): number {
  // 32-bit unsigned seed — Math.random() over 2**31 gives the route's
  // validation a value it'll always accept.
  return Math.floor(Math.random() * 2 ** 31);
}
