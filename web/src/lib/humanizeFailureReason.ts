export interface FailureReasonSummary {
  code: string;
  label: string;
  description: string;
}

const FAILURE_REASON_HELP: Record<string, Omit<FailureReasonSummary, "code">> = {
  agent_error: {
    label: "Agent error",
    description: "The agent process failed before the verifier could score the task.",
  },
  agent_timeout: {
    label: "Agent timeout",
    description: "The agent exceeded its configured runtime limit.",
  },
  artifact_upload_failed: {
    label: "Artifact upload failed",
    description: "The trial finished work but could not persist one or more artifacts.",
  },
  env_start_failure: {
    label: "Environment start failure",
    description: "The worker could not build or start the task environment.",
  },
  fanout_submit_failed: {
    label: "Batch fan-out submission failed",
    description: "The batch runner could not submit one or more child trials.",
  },
  internal_error: {
    label: "Internal worker error",
    description: "The worker hit an unexpected platform error.",
  },
  retry_exhausted: {
    label: "Retries exhausted",
    description: "The trial used all configured attempts and still did not complete.",
  },
  trajectory_flush_failed: {
    label: "Trajectory flush failed",
    description: "The worker could not persist the trial trajectory log.",
  },
  verifier_error: {
    label: "Verifier error",
    description: "The verifier process failed while grading the agent output.",
  },
  verifier_timeout: {
    label: "Verifier timeout",
    description: "The verifier exceeded its configured runtime limit.",
  },
};

export function humanizeFailureReason(code: string): FailureReasonSummary {
  const known = FAILURE_REASON_HELP[code];
  if (known) return { code, ...known };
  return {
    code,
    label: titleCaseCode(code),
    description: "The platform reported this failure code without a specific explanation.",
  };
}

function titleCaseCode(code: string): string {
  const words = code
    .split(/[_\s-]+/)
    .filter(Boolean)
    .map((part) => part.toLowerCase());
  if (words.length === 0) return "Unknown failure";
  return [capitalize(words[0]), ...words.slice(1)].join(" ");
}

function capitalize(value: string): string {
  return value.charAt(0).toUpperCase() + value.slice(1);
}
