from uuid import uuid4

import pytest
from pydantic import ValidationError

from loom.trajectory.atif import (
    AtifMetadata,
    AtifStep,
    AtifStepMetrics,
    AtifTrajectory,
)


def test_atif_step_metrics_required_when_llm_calls_present():
    """ATIF v1.7: llm_call_count >= 1 → metrics required."""
    AtifStep(
        step_id="0", llm_call_count=1, is_copied_context=False,
        metrics=AtifStepMetrics(
            input_tokens=10, output_tokens=5,
            cached_input_tokens=0, cache_write_tokens=0,
            thinking_tokens=0, cost_usd=0.0,
        ),
        messages=[{"role": "user", "content": "hi"}],
    )


def test_atif_step_no_metrics_when_zero_calls():
    """llm_call_count == 0 → messages + metrics + reasoning MUST be absent."""
    AtifStep(step_id="0", llm_call_count=0, is_copied_context=False)


def test_atif_step_zero_calls_rejects_metrics():
    with pytest.raises(ValidationError):
        AtifStep(
            step_id="0", llm_call_count=0, is_copied_context=False,
            metrics=AtifStepMetrics(
                input_tokens=1, output_tokens=1,
                cached_input_tokens=0, cache_write_tokens=0,
                thinking_tokens=0, cost_usd=0.0,
            ),
        )


def test_atif_step_calls_require_metrics():
    """llm_call_count > 0 with metrics=None is an invalid construction."""
    with pytest.raises(ValidationError):
        AtifStep(
            step_id="0", llm_call_count=2, is_copied_context=False,
            messages=[{"role": "user", "content": "hi"}],
        )


def test_atif_trajectory_round_trip():
    traj = AtifTrajectory(
        trajectory_id=str(uuid4()),
        session_id=str(uuid4()),
        schema_version="1.7",
        metadata=AtifMetadata(task_id="t", agent_name="oracle", agent_version="1.0"),
        steps=[AtifStep(step_id="0", llm_call_count=0, is_copied_context=False)],
    )
    dumped = traj.model_dump_json()
    parsed = AtifTrajectory.model_validate_json(dumped)
    assert parsed.trajectory_id == traj.trajectory_id
