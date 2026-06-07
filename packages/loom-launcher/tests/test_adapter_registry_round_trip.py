"""Adapter table sweep: every spec §8.1 adapter resolves with the
correct endpoint_dialect, api_key_env, base_url_env, model_template,
supports_multi_turn from the spec table.
"""

from __future__ import annotations

import pytest

from loom_launcher import get_adapter

# (name, dialect, api_key_env, base_url_env, model_template, multi_turn)
_TABLE: list[tuple[str, str, str, str, str, bool]] = [
    ("codex",          "openai_responses", "OPENAI_API_KEY",    "OPENAI_BASE_URL",        "{model_id}",        False),
    ("opencode",       "openai_chat",      "OPENAI_API_KEY",    "OPENAI_BASE_URL",        "openai/{model_id}", False),
    ("aider",          "openai_chat",      "OPENAI_API_KEY",    "OPENAI_API_BASE",        "openai/{model_id}", True),
    ("openhands",      "openai_chat",      "LLM_API_KEY",       "LLM_BASE_URL",           "openai/{model_id}", True),
    ("openhands-sdk",  "openai_chat",      "LLM_API_KEY",       "LLM_BASE_URL",           "openai/{model_id}", False),
    ("swe-agent",      "openai_chat",      "OPENAI_API_KEY",    "OPENAI_API_BASE",        "openai/{model_id}", False),
    ("mini-swe-agent", "openai_chat",      "OPENAI_API_KEY",    "OPENAI_BASE_URL",        "openai/{model_id}", False),
    ("claude-code",    "anthropic",        "ANTHROPIC_API_KEY", "ANTHROPIC_BASE_URL",     "{model_id}",        True),
    ("gemini-cli",     "gemini",           "GOOGLE_API_KEY",    "GOOGLE_GEMINI_BASE_URL", "google/{model_id}", True),
    ("qwen-cli",       "openai_chat",      "OPENAI_API_KEY",    "OPENAI_BASE_URL",        "{model_id}",        False),
    ("kimi-cli",       "openai_chat",      "OPENAI_API_KEY",    "OPENAI_BASE_URL",        "openai/{model_id}", False),
]


@pytest.mark.parametrize(
    ("name", "dialect", "api_key_env", "base_url_env", "model_template", "multi_turn"),
    _TABLE,
)
def test_adapter_matches_spec_table(
    name: str,
    dialect: str,
    api_key_env: str,
    base_url_env: str,
    model_template: str,
    multi_turn: bool,
) -> None:
    adapter = get_adapter(name)
    assert adapter is not None, f"adapter {name!r} not registered"
    assert adapter.endpoint_dialect == dialect
    assert adapter.api_key_env == api_key_env
    assert adapter.base_url_env == base_url_env
    assert adapter.model_name_template == model_template
    assert adapter.supports_multi_turn is multi_turn
    assert "linux" in adapter.supports_os
