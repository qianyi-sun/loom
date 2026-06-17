"""GatewaySettings.local_providers — parse env vars
`LOOM_GW_LOCAL_<NAME>_BASE_URL` + optional `_API_KEY` into a dict of
LocalProviderConfig. This is the operator-facing surface for
registering local LLM servers in service mode."""

from __future__ import annotations

import pytest

from loom_llm_gateway.config import (
    GatewaySettings,
    LocalProviderConfig,
    parse_local_providers_from_env,
)


def _make_settings(monkeypatch: pytest.MonkeyPatch) -> GatewaySettings:
    """Construct a GatewaySettings with required fields stubbed so
    we can probe the local_providers property in isolation."""
    monkeypatch.setenv(
        "LOOM_GW_DB_URL", "postgresql://x:x@h/db",
    )
    monkeypatch.setenv("LOOM_GW_STEP_JWT_SIGNING_KEY", "test-key")
    return GatewaySettings(_env_file=None)  # type: ignore[call-arg]


def _clear_local_env(monkeypatch: pytest.MonkeyPatch) -> None:
    import os
    for k in list(os.environ.keys()):
        if k.startswith("LOOM_GW_LOCAL_"):
            monkeypatch.delenv(k, raising=False)


def test_empty_when_no_env_vars(monkeypatch: pytest.MonkeyPatch) -> None:
    _clear_local_env(monkeypatch)
    settings = _make_settings(monkeypatch)
    assert settings.local_providers == {}


def test_single_provider_base_url_only(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _clear_local_env(monkeypatch)
    monkeypatch.setenv(
        "LOOM_GW_LOCAL_VLLM_BASE_URL", "http://vllm.internal:8000/v1",
    )
    settings = _make_settings(monkeypatch)
    assert settings.local_providers == {
        "vllm": LocalProviderConfig(
            base_url="http://vllm.internal:8000/v1", api_key=None,
        ),
    }


def test_provider_with_api_key(monkeypatch: pytest.MonkeyPatch) -> None:
    _clear_local_env(monkeypatch)
    monkeypatch.setenv("LOOM_GW_LOCAL_OLLAMA_BASE_URL", "http://ollama:11434/v1")
    monkeypatch.setenv("LOOM_GW_LOCAL_OLLAMA_API_KEY", "sk-cluster-shared")
    settings = _make_settings(monkeypatch)
    assert settings.local_providers["ollama"].api_key == "sk-cluster-shared"


def test_multiple_providers(monkeypatch: pytest.MonkeyPatch) -> None:
    _clear_local_env(monkeypatch)
    monkeypatch.setenv("LOOM_GW_LOCAL_VLLM_BASE_URL", "http://vllm:8000/v1")
    monkeypatch.setenv("LOOM_GW_LOCAL_OLLAMA_BASE_URL", "http://ollama:11434/v1")
    monkeypatch.setenv("LOOM_GW_LOCAL_LLAMACPP_BASE_URL", "http://llama:8080")
    settings = _make_settings(monkeypatch)
    assert set(settings.local_providers.keys()) == {"vllm", "ollama", "llamacpp"}


def test_api_key_without_base_url_is_silently_dropped(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """If an operator misconfigures (sets _API_KEY but not _BASE_URL),
    we drop the incomplete entry rather than crashing at startup —
    matches the loose-config convention used elsewhere."""
    _clear_local_env(monkeypatch)
    monkeypatch.setenv("LOOM_GW_LOCAL_BROKEN_API_KEY", "sk-foo")
    settings = _make_settings(monkeypatch)
    assert "broken" not in settings.local_providers


def test_parse_function_lowercases_name(monkeypatch: pytest.MonkeyPatch) -> None:
    _clear_local_env(monkeypatch)
    monkeypatch.setenv("LOOM_GW_LOCAL_MYSERVER_BASE_URL", "http://x:1/v1")
    parsed = parse_local_providers_from_env()
    # Name was uppercase in env var, lowercased to match TOML/CLI convention
    assert "myserver" in parsed
    assert "MYSERVER" not in parsed
