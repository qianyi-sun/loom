import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import uuid4

import pytest

from loom.models.trajectory import EventKind
from loom.trajectory.reader import TrajectoryReader


def _step_start_dict(seq: int) -> dict[str, Any]:
    return {
        "kind": "step_start",
        "emitted_at": datetime.now(UTC).isoformat(),
        "trial_id": str(uuid4()),
        "step_id": "main",
        "seq": seq,
        "instruction_excerpt": f"event {seq}",
    }


def _tool_use_dict(seq: int) -> dict[str, Any]:
    return {
        "kind": "tool_use",
        "emitted_at": datetime.now(UTC).isoformat(),
        "trial_id": str(uuid4()),
        "step_id": "main",
        "seq": seq,
        "tool_name": "read",
        "args": {"path": f"/x/{seq}"},
        "result": {"content": ""},
        "duration_sec": 0.01,
    }


@pytest.fixture
def jsonl_file(tmp_path: Path) -> Path:
    f = tmp_path / "events.jsonl"
    with f.open("wb") as fh:
        for i in range(5):
            fh.write(json.dumps(_step_start_dict(i)).encode() + b"\n")
            if i % 2 == 0:
                fh.write(json.dumps(_tool_use_dict(i + 100)).encode() + b"\n")
    return f


def test_iter_all(jsonl_file: Path):
    reader = TrajectoryReader(jsonl_file)
    events = list(reader.iter_all())
    assert len(events) == 8


def test_iter_kind_filter(jsonl_file: Path):
    reader = TrajectoryReader(jsonl_file)
    tool_uses = list(reader.iter_kind(EventKind.TOOL_USE))
    assert len(tool_uses) == 3
    assert all(e.kind == EventKind.TOOL_USE for e in tool_uses)


def test_tail(jsonl_file: Path):
    reader = TrajectoryReader(jsonl_file)
    tail = reader.tail(3)
    assert len(tail) == 3
    assert tail == list(reader.iter_all())[-3:]


def test_tail_more_than_total(jsonl_file: Path):
    reader = TrajectoryReader(jsonl_file)
    assert len(reader.tail(100)) == 8


def test_iter_skips_blank_lines(tmp_path: Path):
    f = tmp_path / "events.jsonl"
    f.write_text("\n\n" + json.dumps(_step_start_dict(0)) + "\n\n")
    reader = TrajectoryReader(f)
    assert len(list(reader.iter_all())) == 1
