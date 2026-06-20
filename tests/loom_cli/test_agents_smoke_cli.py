from __future__ import annotations

from typing import Any

import pytest

from loom_cli import agents_cmd
from loom_cli.agent_runtime_smoke import AgentRuntimeSmokeItem


def _item(*, state: str = "failed") -> AgentRuntimeSmokeItem:
    return AgentRuntimeSmokeItem(
        image="loom-sandbox:test",
        name="hello",
        kind="adapter",
        smoke_state=state,
        trial_state="succeeded" if state == "passed" else "failed",
        failure_reason=None if state == "passed" else "agent_error",
        failure_message=None if state == "passed" else "boom",
        duration_sec=0.1,
        trajectory_uri="s3://trajectories/team/trial/events.jsonl",
        atif_uri="s3://trajectories/team/trial/atif.json",
    )


def test_smoke_runtime_requires_image(capsys: pytest.CaptureFixture[str]) -> None:
    rc = agents_cmd.dispatch(["smoke-runtime"])

    assert rc == 2
    assert "--image" in capsys.readouterr().err


def test_smoke_runtime_prints_json_and_returns_nonzero_when_any_smoke_fails(
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fake_smoke(**kwargs: Any) -> list[AgentRuntimeSmokeItem]:
        assert kwargs["image"] == "loom-sandbox:test"
        assert kwargs["agents"] == ["hello"]
        assert kwargs["timeout_sec"] == 3.0
        assert kwargs["model_name"] == "smoke-model"
        return [_item()]

    monkeypatch.setattr(agents_cmd, "run_agent_smoke_matrix", fake_smoke)

    rc = agents_cmd.dispatch(
        [
            "smoke-runtime",
            "--image",
            "loom-sandbox:test",
            "--agent",
            "hello",
            "--timeout-sec",
            "3",
            "--model-name",
            "smoke-model",
            "--json",
        ]
    )

    assert rc == 1
    out = capsys.readouterr().out
    assert '"name": "hello"' in out
    assert '"smoke_state": "failed"' in out


def test_smoke_runtime_prints_table_and_returns_zero_when_all_pass(
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        agents_cmd,
        "run_agent_smoke_matrix",
        lambda **_kwargs: [_item(state="passed")],
    )

    rc = agents_cmd.dispatch(["smoke-runtime", "--image", "loom-sandbox:test"])

    assert rc == 0
    out = capsys.readouterr().out
    assert "AGENT" in out
    assert "hello" in out


def test_smoke_runtime_reports_unknown_agent(
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fake_smoke(**_kwargs: Any) -> list[AgentRuntimeSmokeItem]:
        raise ValueError("unknown agent(s): nope")

    monkeypatch.setattr(agents_cmd, "run_agent_smoke_matrix", fake_smoke)

    rc = agents_cmd.dispatch(["smoke-runtime", "--image", "loom-sandbox:test", "--agent", "nope"])

    assert rc == 2
    assert "unknown agent(s): nope" in capsys.readouterr().err
