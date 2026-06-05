from datetime import UTC, datetime
from uuid import uuid4

import pytest
from pydantic import TypeAdapter, ValidationError

from loom.models.trajectory import (
    ToolUseEvent,
    TrajectoryEvent,
    VerifierEndEvent,
)


def _env(**overrides):
    base = {
        "emitted_at": datetime.now(UTC).isoformat(),
        "trial_id": str(uuid4()),
        "step_id": "main",
        "seq": 0,
    }
    base.update(overrides)
    return base


def test_tool_use_event():
    e = ToolUseEvent(
        **_env(),
        tool_name="read_file",
        args={"path": "/a"},
        result={"content": "ok"},
        duration_sec=0.01,
    )
    assert e.tool_name == "read_file"


def test_verifier_event_round_trip():
    payload = _env(
        kind="verifier_end",
        result={"rewards": {"passed": 1.0}, "checks": [], "confidence": None,
                "structured": None, "error": None},
    )
    adapter = TypeAdapter(TrajectoryEvent)
    parsed = adapter.validate_python(payload)
    assert isinstance(parsed, VerifierEndEvent)


def test_jsonl_round_trip_via_union():
    """Append-only JSONL → re-parse via discriminated union."""
    events_raw = [
        _env(kind="step_start", instruction_excerpt="x"),
        _env(kind="agent_thought", content="thinking..."),
        _env(kind="tool_use", tool_name="t", args={}, result={}, duration_sec=0.1),
    ]
    adapter = TypeAdapter(TrajectoryEvent)
    parsed = [adapter.validate_python(e) for e in events_raw]
    assert [p.kind for p in parsed] == ["step_start", "agent_thought", "tool_use"]


def test_union_rejects_unknown_kind():
    adapter = TypeAdapter(TrajectoryEvent)
    with pytest.raises(ValidationError):
        adapter.validate_python(_env(kind="bogus"))
