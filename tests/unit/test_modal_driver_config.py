"""ModalConfig env loader + redacted repr."""

from __future__ import annotations

import pytest

from loom_drivers.modal.config import ModalConfig, ModalConfigError


def test_loads_from_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MODAL_TOKEN_ID", "ak-test-id")
    monkeypatch.setenv("MODAL_TOKEN_SECRET", "as-test-secret")
    monkeypatch.setenv("MODAL_WORKSPACE", "hongjian-dev")
    cfg = ModalConfig.from_env()
    assert cfg.token_id == "ak-test-id"
    assert cfg.token_secret == "as-test-secret"
    assert cfg.workspace == "hongjian-dev"


def test_workspace_optional(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MODAL_TOKEN_ID", "ak-test-id")
    monkeypatch.setenv("MODAL_TOKEN_SECRET", "as-test-secret")
    monkeypatch.delenv("MODAL_WORKSPACE", raising=False)
    cfg = ModalConfig.from_env()
    assert cfg.workspace is None


def test_missing_token_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    for var in ("MODAL_TOKEN_ID", "MODAL_TOKEN_SECRET", "MODAL_WORKSPACE"):
        monkeypatch.delenv(var, raising=False)
    with pytest.raises(ModalConfigError) as ei:
        ModalConfig.from_env()
    assert "MODAL_TOKEN_ID" in str(ei.value)


def test_secret_is_redacted_in_repr(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MODAL_TOKEN_ID", "ak-test-id")
    monkeypatch.setenv("MODAL_TOKEN_SECRET", "as-very-secret-value")
    cfg = ModalConfig.from_env()
    assert "as-very-secret-value" not in repr(cfg)
    assert "***" in repr(cfg) or "REDACTED" in repr(cfg)


def test_app_name_defaults_to_loom_runs() -> None:
    cfg = ModalConfig(token_id="x", token_secret="y", workspace=None)
    assert cfg.app_name == "loom-runs"
