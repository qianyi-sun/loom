"""Result formatting for the `loom run` CLI."""

from __future__ import annotations

import json

from loom.models.result import TrialResult


def _duration_sec(result: TrialResult) -> float:
    if result.started_at is None or result.finished_at is None:
        return 0.0
    return (result.finished_at - result.started_at).total_seconds()


def format_text_line(result: TrialResult) -> str:
    reward = ""
    if result.reward:
        reward = " " + " ".join(
            f"{k}={v:.3f}" for k, v in sorted(result.reward.items())
        )
    fr = ""
    if result.failure_reason is not None:
        fr = f" failure_reason={result.failure_reason.value}"
    dur = _duration_sec(result)
    return (
        f"{result.task_id} "
        f"[{result.state.value.upper()}] "
        f"agent={result.agent.name} "
        f"dur={dur:.2f}s"
        f"{reward}{fr}"
    )


def format_json_line(result: TrialResult) -> str:
    payload = {
        "trial_id": str(result.id),
        "task_id": result.task_id,
        "team_id": str(result.team_id),
        "state": result.state.value,
        "failure_reason": (
            result.failure_reason.value if result.failure_reason else None
        ),
        "reward": dict(result.reward) if result.reward else None,
        "agent": {
            "name": result.agent.name,
            "version": result.agent.version,
            "mode": result.agent.mode,
            "model": (
                {"provider": result.agent.model.provider,
                 "name": result.agent.model.name}
                if result.agent.model else None
            ),
        },
        "trajectory_uri": result.trajectory_uri,
        "atif_uri": result.atif_uri,
        "duration_sec": _duration_sec(result),
        "started_at": (
            result.started_at.isoformat() if result.started_at else None
        ),
        "finished_at": (
            result.finished_at.isoformat() if result.finished_at else None
        ),
    }
    return json.dumps(payload, sort_keys=True)
