"""#2054: a failed-case rerun submits new trials, so each rerun selection
must pass current submission policy instead of inheriting retired choices."""

from __future__ import annotations

from loom_service.routes.batches import _rerun_selection_error


def test_supported_selection_passes() -> None:
    selection = {
        "agent_name": "terminus-2",
        "agent_model": {"provider": "openai", "name": "gpt-4o"},
    }

    assert _rerun_selection_error(selection) is None


def test_oracle_without_model_passes() -> None:
    assert _rerun_selection_error({"agent_name": "oracle", "agent_model": None}) is None


def test_deferred_agent_is_rejected() -> None:
    selection = {
        "agent_name": "swe-agent",
        "agent_model": {"provider": "openai", "name": "gpt-4o"},
    }

    err = _rerun_selection_error(selection)

    assert err is not None
    assert "not available for new submissions" in err


def test_retired_model_source_is_rejected() -> None:
    selection = {
        "agent_name": "direct-completion",
        "agent_model": {
            "provider": "local",
            "name": "llama3",
            "source": "local-server",
            "local_server": "ollama",
        },
    }

    err = _rerun_selection_error(selection)

    assert err is not None
    assert "retired" in err


def test_legacy_openhands_name_resolves_through_alias() -> None:
    selection = {
        "agent_name": "openhands",
        "agent_model": {"provider": "openai", "name": "gpt-4o"},
    }

    assert _rerun_selection_error(selection) is None


def test_malformed_model_is_rejected() -> None:
    selection = {"agent_name": "codex", "agent_model": {"provider": "openai"}}

    err = _rerun_selection_error(selection)

    assert err is not None
    assert "agent_model failed to validate" in err
