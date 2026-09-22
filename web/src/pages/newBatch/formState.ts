/** Shared form values used by the page and identity preview. */

import type { AgentModelValue } from "../../components/AgentModelPicker";

export const DEFAULT_AGENT_NAME = "direct-completion";
// Fixed hosted backend; the server resolves it, so it is display-only here.
export const NEBIUS_BACKEND = "nebius";

export type SubsetKind = "all" | "first_n" | "last_n" | "random_n" | "explicit";
export type BatchPurpose = "evaluation" | "trajectory_generation";

const INITIAL_PICKER: AgentModelValue = {
  agentName: "",
  source: "api",
  modelProvider: "",
  modelName: "",
  hfExecution: "local-vllm",
};

export interface ComboRow {
  picker: AgentModelValue;
  nPerTask: string;
  label: string;
}

export function newRow(): ComboRow {
  return { picker: { ...INITIAL_PICKER }, nPerTask: "1", label: "" };
}
