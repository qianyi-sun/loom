import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import uuid4

import pytest

from loom.models.trajectory import EventKind
from loom.trajectory.excerpt import (
    AllExcerpt,
    StepSummaryExcerpt,
    TailExcerpt,
    ToolUseOnlyExcerpt,
)
from loom.trajectory.reader import TrajectoryReader


def _event_dict(kind: str, seq: int, step_id: str = "main", **extras: Any) -> dict[str, Any]:
    base: dict[str, Any] = {
        "kind": kind,
        "emitted_at": datetime.now(UTC).isoformat(),
        "trial_id": str(uuid4()),
        "step_id": step_id,
        "seq": seq,
    }
    base.update(extras)
    return base


@pytest.fixture
def jsonl_file(tmp_path: Path) -> Path:
    f = tmp_path / "events.jsonl"
    rows: list[dict[str, Any]] = [
        _event_dict("step_start", 0, instruction_excerpt="phase-1"),
        _event_dict("tool_use", 1, tool_name="read", args={}, result={}, duration_sec=0.1),
        _event_dict("agent_thought", 2, content="thinking"),
        _event_dict("tool_use", 3, tool_name="write", args={}, result={}, duration_sec=0.1),
        _event_dict("step_end", 4, summary={"reward": 1.0}),
        _event_dict("step_start", 5, step_id="phase-2", instruction_excerpt="phase-2"),
        _event_dict("tool_use", 6, step_id="phase-2",
                    tool_name="search", args={}, result={}, duration_sec=0.1),
        _event_dict("step_end", 7, step_id="phase-2", summary={"reward": 0.5}),
    ]
    with f.open("wb") as fh:
        for row in rows:
            fh.write(json.dumps(row).encode() + b"\n")
    return f


def test_tail_excerpt(jsonl_file: Path):
    reader = TrajectoryReader(jsonl_file)
    events = reader.excerpt(TailExcerpt(n=3), max_tokens=10_000)
    assert len(events) == 3
    assert events[-1].kind == EventKind.STEP_END


def test_all_excerpt(jsonl_file: Path):
    reader = TrajectoryReader(jsonl_file)
    events = reader.excerpt(AllExcerpt(), max_tokens=10_000)
    assert len(events) == 8


def test_tool_use_only(jsonl_file: Path):
    reader = TrajectoryReader(jsonl_file)
    events = reader.excerpt(ToolUseOnlyExcerpt(), max_tokens=10_000)
    assert all(e.kind in (EventKind.TOOL_USE, EventKind.LLM_CALL) for e in events)


def test_step_summary(jsonl_file: Path):
    reader = TrajectoryReader(jsonl_file)
    events = reader.excerpt(StepSummaryExcerpt(), max_tokens=10_000)
    kinds = [e.kind for e in events]
    assert kinds.count(EventKind.STEP_START) == 2
    assert kinds.count(EventKind.STEP_END) == 2


def test_max_tokens_prunes_oldest(jsonl_file: Path):
    reader = TrajectoryReader(jsonl_file)
    events = reader.excerpt(AllExcerpt(), max_tokens=50)
    assert len(events) < 8
