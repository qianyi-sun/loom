from __future__ import annotations

from typing import Any

import pytest

from loom_cli import agent_runtime_smoke as smoke


def _item(name: str, *, state: str = "passed") -> smoke.AgentRuntimeSmokeItem:
    return smoke.AgentRuntimeSmokeItem(
        image="loom-sandbox:test",
        name=name,
        kind="adapter",
        smoke_state=state,
        trial_state="succeeded" if state == "passed" else "failed",
        failure_reason=None if state == "passed" else "agent_error",
        failure_message=None if state == "passed" else "boom",
        duration_sec=0.1,
        trajectory_uri="s3://trajectories/team/trial/events.jsonl",
        atif_uri="s3://trajectories/team/trial/atif.json",
    )


def test_run_agent_smoke_matrix_filters_requested_agent(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    seen: list[tuple[str, str, float, str]] = []

    async def fake_run_one(**kwargs: Any) -> smoke.AgentRuntimeSmokeItem:
        agent = kwargs["agent"]
        seen.append(
            (
                agent.name,
                kwargs["image"],
                kwargs["timeout_sec"],
                kwargs["model_name"],
            )
        )
        return _item(agent.name)

    monkeypatch.setattr(smoke, "_run_one_agent_smoke", fake_run_one)

    items = smoke.run_agent_smoke_matrix(
        image="loom-sandbox:test",
        agents=["hello"],
        timeout_sec=2.5,
        model_name="smoke-model",
    )

    assert [item.name for item in items] == ["hello"]
    assert seen == [("hello", "loom-sandbox:test", 2.5, "smoke-model")]


def test_run_agent_smoke_matrix_rejects_unknown_agent() -> None:
    with pytest.raises(ValueError, match="unknown agent"):
        smoke.run_agent_smoke_matrix(
            image="loom-sandbox:test",
            agents=["does-not-exist"],
        )


def test_run_agent_smoke_matrix_records_agent_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def fake_run_one(**_kwargs: Any) -> smoke.AgentRuntimeSmokeItem:
        raise RuntimeError("kaboom")

    monkeypatch.setattr(smoke, "_run_one_agent_smoke", fake_run_one)

    items = smoke.run_agent_smoke_matrix(
        image="loom-sandbox:test",
        agents=["hello"],
    )

    assert len(items) == 1
    assert items[0].name == "hello"
    assert items[0].smoke_state == "failed"
    assert items[0].failure_reason == "runtime_error"
    assert "kaboom" in (items[0].failure_message or "")
