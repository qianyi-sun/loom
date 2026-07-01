"""CodexAdapter.wire_api — toggle between the Responses and Chat Completions
wire APIs written into codex's TOML provider config.

Default stays "responses" (backward compatible with codex 0.141+ deployments
that target OpenAI direct). Callers who need "chat" (BYO providers like
yibuapi that don't implement /v1/responses) construct via
`dataclasses.replace(codex_adapter, wire_api="chat")`.

Downstream from #277 (codex hangs on yibuapi's missing Responses API).
"""

from __future__ import annotations

import dataclasses
from pathlib import PurePosixPath

from loom_launcher.adapter import ModelSpec
from loom_launcher.adapters.codex import CodexAdapter


def _invocation_toml_snippet(wire_api: str) -> str:
    adapter = dataclasses.replace(CodexAdapter(), wire_api=wire_api)
    argv = adapter.build_invocation(
        instruction="do the thing",
        workdir=PurePosixPath("/workspace"),
        model=ModelSpec(provider="yibuapi", name="glm-5.1-thinking"),
        env={
            "OPENAI_API_KEY": "sk-fake",
            "OPENAI_BASE_URL": "http://host.docker.internal:30444/openai/v1",
        },
    )
    # `provider_config` is argv position 6 (0=sh, 1=-c, 2=script,
    # 3=loom-codex, 4=model_name, 5=workdir, 6=provider_config, 7=instruction).
    return argv[6]


def test_default_wire_api_is_responses() -> None:
    adapter = CodexAdapter()
    assert adapter.wire_api == "responses"


def test_provider_config_uses_configured_wire_api_chat() -> None:
    snippet = _invocation_toml_snippet("chat")
    assert 'wire_api = "chat"' in snippet
    assert 'wire_api = "responses"' not in snippet


def test_provider_config_uses_configured_wire_api_responses() -> None:
    snippet = _invocation_toml_snippet("responses")
    assert 'wire_api = "responses"' in snippet
    assert 'wire_api = "chat"' not in snippet


def test_replace_produces_independent_instance() -> None:
    base = CodexAdapter()
    chat = dataclasses.replace(base, wire_api="chat")
    assert base.wire_api == "responses"
    assert chat.wire_api == "chat"
    assert base is not chat
