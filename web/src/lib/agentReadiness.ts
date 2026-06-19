export interface RuntimeContract {
  install_hint?: string | null;
}

export interface AgentReadinessLike {
  name: string;
  service_mode_ready?: boolean;
  readiness_message?: string | null;
  runtime_contract?: RuntimeContract | null;
}

export function agentServiceModeReady(agent: AgentReadinessLike): boolean {
  return agent.service_mode_ready !== false;
}

export function agentReadinessMessage(agent: AgentReadinessLike): string {
  return agent.readiness_message
    ?? agent.runtime_contract?.install_hint
    ?? `Agent ${agent.name} needs service-mode runtime setup.`;
}
