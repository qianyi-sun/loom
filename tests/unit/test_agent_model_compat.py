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
    assert validate_agent_model_compat(
        "litellm",
        ModelSpec(provider="anthropic", name="claude-opus-4"),
    ) is None
    assert validate_agent_model_compat(
        "litellm",
        ModelSpec(provider="openai", name="gpt-4o"),
    ) is None


def test_litellm_accepts_hf_source() -> None:
    assert validate_agent_model_compat(
        "litellm",
        ModelSpec(
            provider="hf", name="meta-llama/Llama-3-8B-Instruct",
            source="hf",
        ),
    ) is None


def test_litellm_accepts_local_server() -> None:
    assert validate_agent_model_compat(
        "litellm",
        ModelSpec(
            provider="local", name="llama3", source="local-server",
            local_server="ollama",
        ),
    ) is None


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
            provider="anthropic", name="claude-opus-4",
            source="api", local_server="ollama",
        ),
    )
    assert err is not None
    assert "local_server" in err


def test_claude_code_inbox_rejects_openai() -> None:
    err = validate_agent_model_compat(
        "claude-code-inbox",
        ModelSpec(provider="openai", name="gpt-4o"),
    )
    assert err is not None
    assert "anthropic" in err


def test_claude_code_inbox_rejects_hf_source() -> None:
    err = validate_agent_model_compat(
        "claude-code-inbox",
        ModelSpec(
            provider="anthropic", name="claude-opus-4", source="hf",
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


def test_existing_modelspec_default_source_is_api() -> None:
    """Backwards-compat: callers that don't set `source` get the legacy
    catalog-backed API path."""
    m = ModelSpec(provider="anthropic", name="claude-opus-4")
    assert m.source == "api"
