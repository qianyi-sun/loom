from __future__ import annotations

from typing import Any

import pytest

from loom_cli import agents_cmd
from loom_cli.agent_runtime_readiness import AgentRuntimeAuditItem


def _item(*, state: str = "blocked") -> AgentRuntimeAuditItem:
    return AgentRuntimeAuditItem(
        image="loom-sandbox:test",
        name="opencode",
        kind="adapter",
        catalog_ready=False,
        dependency_state="missing",
        readiness_state=state,
        blocker_reason="missing_runtime_dependency",
        required_executables=["opencode"],
        required_python_modules=[],
        required_packages=["opencode-ai"],
        missing_executables=["opencode"],
        missing_python_modules=[],
    )


def test_audit_runtime_requires_image(capsys: pytest.CaptureFixture[str]) -> None:
    rc = agents_cmd.dispatch(["audit-runtime"])

    assert rc == 2
    assert "--image" in capsys.readouterr().err


def test_audit_runtime_prints_json_and_returns_nonzero_when_not_ready(
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fake_audit(**kwargs: Any) -> list[AgentRuntimeAuditItem]:
        assert kwargs["image"] == "loom-sandbox:test"
        assert kwargs["agents"] == ["opencode"]
        return [_item()]

    monkeypatch.setattr(agents_cmd, "build_runtime_audit_items", fake_audit)
    rc = agents_cmd.dispatch(
        [
            "audit-runtime",
            "--image",
            "loom-sandbox:test",
            "--agent",
            "opencode",
            "--json",
        ]
    )

    assert rc == 1
    out = capsys.readouterr().out
    assert '"name": "opencode"' in out
    assert '"readiness_state": "blocked"' in out


def test_audit_runtime_prints_table_and_returns_zero_when_ready(
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fake_audit(**_kwargs: Any) -> list[AgentRuntimeAuditItem]:
        return [_item(state="ready")]

    monkeypatch.setattr(agents_cmd, "build_runtime_audit_items", fake_audit)
    rc = agents_cmd.dispatch(["audit-runtime", "--image", "loom-sandbox:test"])

    assert rc == 0
    out = capsys.readouterr().out
    assert "AGENT" in out
    assert "opencode" in out
