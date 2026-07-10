"""Factory routing tests for Harbor-embedded terminus-2 (#744)."""

from __future__ import annotations

from uuid import uuid4

from loom.agent.gateway_client import FakeLLMGatewayClient
from loom.agent.terminus2.runtime import LoomTerminus2Runtime
from loom.models.types import ModelSpec
from loom_worker.main_loop import _default_agent_factory


def test_default_agent_factory_returns_harbor_runtime() -> None:
    class _NoopCP:
        async def mint_step_token(self, **kwargs: object) -> str:
            return "token"

    factory = _default_agent_factory(
        team_id=uuid4(),
        trial_id=uuid4(),
        cp_client=_NoopCP(),  # type: ignore[arg-type]
        gateway_url="http://gateway",
    )
    agent = factory(
        task_dir=__import__("pathlib").Path("/tmp"),
        gateway=FakeLLMGatewayClient(scripted=[]),
        model=ModelSpec(provider="openai", name="gpt-4"),
        agent_name="terminus-2",
    )
    assert isinstance(agent, LoomTerminus2Runtime)
    assert agent.name == "terminus-2"
    assert agent.emits_gateway_llm_call_events is True
