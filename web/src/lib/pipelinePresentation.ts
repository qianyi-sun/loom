import type {
  PipelineExecutionAttemptList,
  PipelineRunListItem,
  PipelineStageRunSummary,
} from "../api/client";
import type { StatusVariant } from "../components/StatusPill";

type RunState = PipelineRunListItem["state"];
type RunResult = Exclude<PipelineRunListItem["result"], null>;
type StageState = PipelineStageRunSummary["state"];
type AttemptState = PipelineExecutionAttemptList["items"][number]["state"];

export type PipelinePresentation = {
  label: string;
  variant: StatusVariant;
};

export const PIPELINE_RUN_STATE: Record<RunState, PipelinePresentation> = {
  submitted: { label: "Submitted", variant: "queued" },
  running: { label: "Running", variant: "running" },
  cancelling: { label: "Cancelling", variant: "warning" },
  finished: { label: "Finished", variant: "neutral" },
};

export const PIPELINE_RUN_RESULT: Record<RunResult, PipelinePresentation> = {
  succeeded: { label: "Succeeded", variant: "success" },
  partial_failed: { label: "Partially failed", variant: "warning" },
  failed: { label: "Failed", variant: "failed" },
  cancelled: { label: "Cancelled", variant: "cancelled" },
  budget_exhausted: { label: "Budget exhausted", variant: "failed" },
};

export const PIPELINE_PENDING_RESULT: PipelinePresentation = {
  label: "Pending",
  variant: "neutral",
};

export const PIPELINE_STAGE_STATE: Record<StageState, PipelinePresentation> = {
  blocked: { label: "Blocked", variant: "neutral" },
  ready: { label: "Ready", variant: "queued" },
  queued: { label: "Queued", variant: "queued" },
  claimed: { label: "Claimed", variant: "running" },
  running: { label: "Running", variant: "running" },
  retry_wait: { label: "Retry waiting", variant: "warning" },
  succeeded: { label: "Succeeded", variant: "success" },
  failed: { label: "Failed", variant: "failed" },
  cancelled: { label: "Cancelled", variant: "cancelled" },
  skipped: { label: "Skipped", variant: "neutral" },
};

export const PIPELINE_ATTEMPT_STATE: Record<AttemptState, PipelinePresentation> = {
  fault_pending: { label: "Preparing acceptance fault", variant: "warning" },
  queued: { label: "Queued", variant: "queued" },
  claimed: { label: "Claimed", variant: "running" },
  running: { label: "Running", variant: "running" },
  succeeded: { label: "Succeeded", variant: "success" },
  failed: { label: "Failed", variant: "failed" },
  cancelled: { label: "Cancelled", variant: "cancelled" },
  lost: { label: "Lost after verified cleanup", variant: "failed" },
};

export function pipelineResultPresentation(
  result: PipelineRunListItem["result"],
): PipelinePresentation {
  return result === null ? PIPELINE_PENDING_RESULT : PIPELINE_RUN_RESULT[result];
}

export function formatMicrousd(value: number): string {
  const sign = value < 0 ? "-" : "";
  const absolute = Math.abs(value);
  const whole = Math.floor(absolute / 1_000_000);
  const fraction = String(absolute % 1_000_000).padStart(6, "0");
  return `${sign}$${whole}.${fraction}`;
}

export function bytewiseCompare(left: string, right: string): number {
  const encoder = new TextEncoder();
  const a = encoder.encode(left);
  const b = encoder.encode(right);
  for (let index = 0; index < Math.min(a.length, b.length); index += 1) {
    if (a[index] !== b[index]) return a[index] - b[index];
  }
  return a.length - b.length;
}

export function truncateNfcUtf8(value: string, maximumBytes: number): string {
  const normalized = value.normalize("NFC");
  const encoder = new TextEncoder();
  if (encoder.encode(normalized).length <= maximumBytes) return normalized;
  let result = "";
  for (const character of normalized) {
    if (encoder.encode(result + character).length > maximumBytes) break;
    result += character;
  }
  return result;
}
