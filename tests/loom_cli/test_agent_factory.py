"""CLI agent factory mirrors loom_worker.main_loop._default_agent_factory
but takes no CP client. SubprocessAgent gets a no-op CP shim so it
still constructs."""

from __future__ import annotations

from pathlib import Path
from uuid import uuid4

import pytest

from loom.agent.gateway_client import FakeLLMGatewayClient
from loom.agent.litellm import LiteLLMAgent
from loom.agent.oracle import OracleAgent
from loom.models.types import ModelSpec
from loom_cli.agent_factory import build_agent_factory


def test_oracle_route(tmp_path: Path) -> None:
    trial_id = uuid4()
    factory = build_agent_factory(team_id=uuid4(), trial_id=trial_id)
    agent = factory(tmp_path, FakeLLMGatewayClient(scripted=[]), None, "oracle")
    assert isinstance(agent, OracleAgent)


def test_litellm_route_requires_model(tmp_path: Path) -> None:
    factory = build_agent_factory(team_id=uuid4(), trial_id=uuid4())
    with pytest.raises(Exception, match="model"):
        factory(tmp_path, FakeLLMGatewayClient(scripted=[]), None, "litellm")


def test_litellm_route_returns_litellm_agent(tmp_path: Path) -> None:
    factory = build_agent_factory(team_id=uuid4(), trial_id=uuid4())
    agent = factory(
        tmp_path,
        FakeLLMGatewayClient(scripted=[]),
        ModelSpec(provider="anthropic", name="claude-opus-4-7"),
        "litellm",
    )
    assert isinstance(agent, LiteLLMAgent)


def test_unknown_agent_raises(tmp_path: Path) -> None:
    factory = build_agent_factory(team_id=uuid4(), trial_id=uuid4())
    with pytest.raises(Exception, match="unknown agent"):
        factory(
            tmp_path,
            FakeLLMGatewayClient(scripted=[]),
            ModelSpec(provider="anthropic", name="claude-opus-4-7"),
            "no-such-agent-zzz",
        )


