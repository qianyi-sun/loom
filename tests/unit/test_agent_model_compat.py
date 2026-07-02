"""Validation matrix for agent ⇄ model compatibility.

The catalog declares each agent's `supported_providers` +
`supported_model_sources`; `validate_agent_model_compat` is the single
gate used by both the SPA-facing batch route and the trial route.
Tests pin every interesting combination so accidental drift in either
side breaks loudly."""

from __future__ import annotations

from loom.models.types import ModelSpec
from loom_service.agent_catalog import (
    get_agent,
    list_agents,
    validate_agent_model_compat,
)


def test_oracle_with_no_model_ok() -> None:
    assert validate_agent_model_compat("oracle", None) is None


def test_oracle_with_a_model_rejects() -> None:
    err = validate_agent_model_compat(
        "oracle",
        ModelSpec(provider="anthropic", name="claude-opus-4"),
    )
    assert err is not None
    assert "does not take a model" in err


def test_litellm_accepts_any_provider_api() -> None:
    assert (
        validate_agent_model_compat(
            "litellm",
            ModelSpec(provider="anthropic", name="claude-opus-4"),
        )
        is None
    )
    assert (
        validate_agent_model_compat(
            "litellm",
            ModelSpec(provider="openai", name="gpt-4o"),
        )
        is None
    )


def test_litellm_accepts_hf_source() -> None:
    assert (
        validate_agent_model_compat(
            "litellm",
            ModelSpec(
                provider="hf",
                name="meta-llama/Llama-3-8B-Instruct",
                source="hf",
            ),
        )
        is None
    )


def test_litellm_accepts_local_server() -> None:
    assert (
        validate_agent_model_compat(
            "litellm",
            ModelSpec(
                provider="local",
                name="llama3",
                source="local-server",
                local_server="ollama",
            ),
        )
        is None
    )


def test_local_server_requires_local_server_field() -> None:
    err = validate_agent_model_compat(
        "litellm",
        ModelSpec(provider="local", name="llama3", source="local-server"),
    )
    assert err is not None
    assert "model.local_server" in err


def test_local_server_field_only_with_local_source() -> None:
    err = validate_agent_model_compat(
        "litellm",
        ModelSpec(
            provider="anthropic",
            name="claude-opus-4",
            source="api",
            local_server="ollama",
        ),
    )
    assert err is not None
    assert "local_server" in err


def test_claude_code_rejects_openai() -> None:
    """claude-code (the loom-launcher adapter) is bound to anthropic.
    Equivalent compatibility coverage previously lived under
    `test_claude_code_inbox_rejects_openai` — that catalog entry was
    retired (the inbox name was a redundant alias for the same code)."""
    err = validate_agent_model_compat(
        "claude-code",
        ModelSpec(provider="openai", name="gpt-4o"),
    )
    assert err is not None
    assert "anthropic" in err


def test_claude_code_rejects_hf_source() -> None:
    err = validate_agent_model_compat(
        "claude-code",
        ModelSpec(
            provider="anthropic",
            name="claude-opus-4",
            source="hf",
        ),
    )
    assert err is not None
    assert "sources" in err


def test_needs_model_with_null_model_rejects() -> None:
    err = validate_agent_model_compat("litellm", None)
    assert err is not None
    assert "requires a model" in err


def test_unknown_agent_rejects() -> None:
    err = validate_agent_model_compat(
        "doesnt-exist",
        ModelSpec(provider="anthropic", name="claude-opus-4"),
    )
    assert err is not None
    assert "unknown agent_name" in err


def test_to_dict_includes_new_metadata() -> None:
    """SPA reads these fields from /api/v1/agents — make sure they're
    in the dict shape, not just on the dataclass."""
    a = get_agent("litellm")
    assert a is not None
    d = a.to_dict()
    assert d["supported_providers"] == ["*"]
    assert d["supported_model_sources"] == ["api", "local-server", "hf"]


def test_catalog_entries_include_service_mode_runtime_contract() -> None:
    """Every displayed agent needs enough runtime metadata for the SPA
    and direct API callers to understand whether service-mode can run it."""
    displayed = {
        "oracle",
        "litellm",
        "aider",
        "claude-code",
        "codex",
        "gemini-cli",
        "hello",
        "kimi-cli",
        "mini-swe-agent",
        "opencode",
        "openhands",
        "openhands-sdk",
        "qwen-cli",
        "swe-agent",
    }
    by_name = {a.name: a.to_dict() for a in list_agents()}
    assert displayed.issubset(by_name)

    for name in displayed:
        data = by_name[name]
        assert isinstance(data["service_mode_ready"], bool)
        assert data["readiness_status"] in {"ready", "unavailable"}
        contract = data["runtime_contract"]
        assert isinstance(contract, dict)
        assert "execution" in contract
        assert "capture" in contract
        assert "required_executables" in contract
        assert "required_python_modules" in contract
        assert "endpoint_dialect" in contract
        assert "api_key_env" in contract
        assert "base_url_env" in contract


def test_catalog_package_hints_use_verified_install_sources() -> None:
    by_name = {agent.name: agent.to_dict() for agent in list_agents()}

    assert by_name["opencode"]["runtime_contract"]["required_packages"] == [
        "opencode-ai",
    ]
    assert by_name["kimi-cli"]["runtime_contract"]["required_packages"] == [
        "@moonshot-ai/kimi-code",
    ]
    assert by_name["swe-agent"]["runtime_contract"]["required_packages"] == [
        "git+https://github.com/SWE-agent/SWE-agent",
    ]
    assert by_name["openhands-sdk"]["runtime_contract"]["required_python_modules"] == [
        "loom_launcher.openhands_sdk_runner",
        "openhands.sdk",
    ]
    assert by_name["openhands-sdk"]["runtime_contract"]["required_packages"] == [
        "openhands-sdk",
    ]
    assert by_name["openhands"]["runtime_contract"]["required_python_modules"] == [
        "loom_launcher.openhands_sdk_runner",
        "openhands.sdk",
    ]
    assert by_name["openhands"]["runtime_contract"]["required_packages"] == [
        "openhands-sdk",
    ]


def test_opencode_runtime_ready_allows_compatible_model() -> None:
    err = validate_agent_model_compat(
        "opencode",
        ModelSpec(provider="openai", name="gpt-4o"),
    )
    assert err is None


def test_terminus_2_accepts_openai_compatible_gateway_models() -> None:
    err = validate_agent_model_compat(
        "terminus-2",
        ModelSpec(provider="openai", name="deepseek-chat"),
    )
    assert err is None


def test_terminus_2_catalog_advertises_any_gateway_provider() -> None:
    terminus = get_agent("terminus-2")
    assert terminus is not None
    assert terminus.supported_providers == ("*",)
    assert terminus.supported_model_sources == ("api",)


def test_existing_modelspec_default_source_is_api() -> None:
    """Backwards-compat: callers that don't set `source` get the legacy
    catalog-backed API path."""
    m = ModelSpec(provider="anthropic", name="claude-opus-4")
    assert m.source == "api"
