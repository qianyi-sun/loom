"""Human + JSON output for trial results."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from uuid import UUID

from loom.models.result import AgentInfo, TrialResult, TrialState
from loom_cli.output import format_json_line, format_text_line
from tests._trial_config_defaults import stub_trial_config


def _result() -> TrialResult:
    return TrialResult(
        id=UUID("11111111-1111-1111-1111-111111111111"),
        task_id="humaneval/HumanEval-0",
        task_checksum="x" * 64,
        team_id=UUID("22222222-2222-2222-2222-222222222222"),
        agent=AgentInfo(
            name="claude-code", version="1.0",
            mode="out-of-box",
            model=None,
        ),
        config=stub_trial_config(),
        state=TrialState.SUCCEEDED,
        started_at=datetime(2026, 6, 8, 0, 0, 0, tzinfo=UTC),
        finished_at=datetime(2026, 6, 8, 0, 0, 5, tzinfo=UTC),
        reward={"pass_at_1": 1.0},
    )


def test_format_text_line_contains_task_and_state() -> None:
    line = format_text_line(_result())
    assert "humaneval/HumanEval-0" in line
    assert "SUCCEEDED" in line
    assert "pass_at_1=1.000" in line


def test_format_json_line_is_valid_json_with_expected_fields() -> None:
    line = format_json_line(_result())
    obj = json.loads(line)
    assert obj["trial_id"] == "11111111-1111-1111-1111-111111111111"
    assert obj["task_id"] == "humaneval/HumanEval-0"
    assert obj["state"] == "succeeded"
    assert obj["reward"] == {"pass_at_1": 1.0}
    assert obj["agent"]["name"] == "claude-code"
    assert obj["duration_sec"] == 5.0
