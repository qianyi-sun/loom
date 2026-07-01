"""SubprocessAgent flips codex's `wire_api` to "chat" when the configured
gateway does not implement `/v1/responses`. Non-codex adapters are never
probed.

Downstream from #277 (codex hangs on yibuapi's missing Responses API).
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from loom_launcher.adapters.codex import CodexAdapter

from loom.agent import subprocess as subprocess_module
from loom.agent.subprocess import SubprocessAgent


@pytest.fixture(autouse=True)
def _reset_probe_cache() -> None:
    from loom.agent.endpoint_probe import _PROBE_CACHE
    _PROBE_CACHE.clear()
    yield
    _PROBE_CACHE.clear()


def _make_agent(adapter: object) -> SubprocessAgent:
    """Duck-typed SubprocessAgent — bypass full construction, set only
    the fields `_select_active_adapter` reads."""
    agent = SubprocessAgent.__new__(SubprocessAgent)
    agent.adapter = adapter  # type: ignore[attr-defined]
    agent.model = SimpleNamespace(name="glm-5.1-thinking")  # type: ignore[attr-defined]
    return agent


async def test_codex_active_adapter_uses_responses_when_supported(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        subprocess_module,
        "responses_api_supported",
        AsyncMock(return_value=True),
    )
    agent = _make_agent(CodexAdapter())
    active = await agent._select_active_adapter(
        base_url="http://gw/openai/v1", step_token="tok",
    )
    assert active.wire_api == "responses"
    assert active is agent.adapter  # untouched


async def test_codex_active_adapter_falls_back_to_chat_when_not_supported(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        subprocess_module,
        "responses_api_supported",
        AsyncMock(return_value=False),
    )
    agent = _make_agent(CodexAdapter())
    active = await agent._select_active_adapter(
        base_url="http://gw/openai/v1", step_token="tok",
    )
    assert active.wire_api == "chat"
    assert active is not agent.adapter  # replaced


async def test_non_codex_adapter_is_not_probed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    mock_probe = AsyncMock(return_value=False)
    monkeypatch.setattr(subprocess_module, "responses_api_supported", mock_probe)

    litellm_like = SimpleNamespace(name="claude-code", wire_api="anthropic")
    agent = _make_agent(litellm_like)
    active = await agent._select_active_adapter(
        base_url="http://gw/anthropic", step_token="tok",
    )
    assert active is agent.adapter
    mock_probe.assert_not_awaited()


async def test_codex_without_wire_api_attribute_is_left_alone(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Defensive: if a future codex adapter drops the wire_api field,
    the selector must not crash. It just returns the adapter as-is."""
    mock_probe = AsyncMock(return_value=False)
    monkeypatch.setattr(subprocess_module, "responses_api_supported", mock_probe)

    # `SimpleNamespace` without wire_api attribute (frozen-dataclass
    # semantics would refuse to replace anyway).
    unadorned_codex = SimpleNamespace(name="codex")
    agent = _make_agent(unadorned_codex)
    active = await agent._select_active_adapter(
        base_url="http://gw/openai/v1", step_token="tok",
    )
    # Duck-typed passthrough (no replace attempted; no crash).
    assert active is agent.adapter
