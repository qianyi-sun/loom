"""CLI-local agent factory. Mirrors `loom_worker.main_loop._default_agent_factory`
but takes no Control Plane client — in stateless mode, the SubprocessAgent's
CP-related hooks are replaced by no-ops, and the Gateway URL is irrelevant
because the SubprocessAgent will pass it through to the launched adapter
(adapter then talks to an upstream provider directly using the env-var
API key the CLI puts in the sandbox).
"""

from __future__ import annotations

from pathlib import Path
from uuid import UUID

from loom.agent.base import AgentRuntime
from loom.agent.gateway_client import LLMGatewayClient
from loom.agent.litellm import LiteLLMAgent
from loom.agent.oracle import OracleAgent
from loom.errors import AgentError
from loom.models.types import ModelSpec
from loom_worker.trial_runner import AgentFactory


class _NoopCPClient:
    """SubprocessAgent needs a step token provider.

    In CLI-local mode there is no Control Plane, so use a deterministic
    placeholder token for the adapter environment.
    """

    async def mint_step_token(
        self,
        *,
        team_id: UUID,
        trial_id: UUID,
        step_id: str,
        ttl_sec: int,
    ) -> str:
        return "loom_step_cli-token"


def build_agent_factory(
    *,
    team_id: UUID,
    trial_id: UUID,
) -> AgentFactory:
    """Return a factory matching `loom_worker.trial_runner.AgentFactory`.

    Routes:
      - "oracle"  -> OracleAgent
      - "litellm" -> LiteLLMAgent (requires model)
      - any other -> SubprocessAgent wrapping loom_launcher.get_adapter(name)
                     (requires model; AgentError if name is unknown)
    """

    def make(
        task_dir: Path,
        gateway: LLMGatewayClient,
        model: ModelSpec | None,
        agent_name: str,
    ) -> AgentRuntime:
        agent: AgentRuntime
        if agent_name == "oracle":
            agent = OracleAgent(task_dir=task_dir, trial_id=trial_id)
        elif agent_name == "litellm":
            if model is None:
                raise AgentError(
                    "litellm agent requires task.agent.model to be set",
                )
            agent = LiteLLMAgent(  # type: ignore[assignment]
                model=model,
                gateway=gateway,
                team_id=str(team_id),
                trial_id=trial_id,
            )
        else:
            from loom_launcher import get_adapter

            from loom.agent.subprocess import SubprocessAgent

            adapter = get_adapter(agent_name)
            if adapter is None:
                raise AgentError(
                    f"unknown agent {agent_name!r} — not 'oracle'/'litellm' "
                    f"and not registered in loom-launcher",
                )
            if model is None:
                raise AgentError(
                    f"{agent_name} requires task.agent.model to be set",
                )
            agent = SubprocessAgent(  # type: ignore[assignment]
                adapter=adapter,
                model=model,
                cp_client=_NoopCPClient(),
                gateway_url="",
                team_id=team_id,
                trial_id=trial_id,
            )
        return agent

    return make
