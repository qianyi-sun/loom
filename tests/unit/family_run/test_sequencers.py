"""Family sequencer plugins."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

import pytest

from loom.family_run.sequencers import (
    AlphabeticalSequencer,
    RankingFileSequencer,
    SubmittedOrderSequencer,
)


@dataclass
class _Task:
    id: str
    source: str | None = None
    tags: dict[str, str] | None = None


def test_alphabetical_orders_by_task_id():
    seq = AlphabeticalSequencer()
    tasks = [_Task(id="b/1"), _Task(id="a/2"), _Task(id="a/1")]
    ordered = seq.sequence("k", tasks, {})
    assert ordered == ["a/1", "a/2", "b/1"]


def test_submitted_order_preserves_input_order():
    seq = SubmittedOrderSequencer()
    tasks = [_Task(id="c"), _Task(id="a"), _Task(id="b")]
    ordered = seq.sequence("k", tasks, {})
    assert ordered == ["c", "a", "b"]


def test_ranking_file_orders_by_json_array(tmp_path: Path):
    ranking = tmp_path / "ALL_TASK_DIFFICULTY_RANKING.json"
    ranking.write_text(json.dumps(["task-b", "task-a", "task-c"]))
    seq = RankingFileSequencer()
    seq.default_params = {"path": str(ranking)}
    tasks = [_Task(id=f"fam/{n}") for n in ("task-a", "task-c", "task-b", "task-d")]
    ordered = seq.sequence("fam", tasks, {"path": str(ranking)})
    assert ordered == ["fam/task-b", "fam/task-a", "fam/task-c", "fam/task-d"]


def test_ranking_snapshot_tags_override_missing_worker_bundle_path(tmp_path: Path):
    seq = RankingFileSequencer()
    tasks = [
        _Task(id="fam/task-a", tags={"dev_fixture": "true", "family_run_rank": "1"}),
        _Task(id="fam/task-b", tags={"dev_fixture": "true", "family_run_rank": "2"}),
        _Task(id="fam/task-c", tags={"dev_fixture": "true", "family_run_rank": "0"}),
    ]

    ordered = seq.sequence("fam", tasks, {"path": str(tmp_path / "worker-only.json")})

    assert ordered == ["fam/task-c", "fam/task-a", "fam/task-b"]


def test_partial_ranking_snapshot_tags_fall_back_without_reordering(tmp_path: Path):
    seq = RankingFileSequencer()
    tasks = [
        _Task(id="fam/b", tags={"dev_fixture": "true", "family_run_rank": "0"}),
        _Task(id="fam/a", tags={}),
    ]

    ordered = seq.sequence("fam", tasks, {"path": str(tmp_path / "missing.json")})

    assert ordered == ["fam/a", "fam/b"]


def test_ranking_file_missing_falls_back_to_alphabetical(tmp_path: Path):
    seq = RankingFileSequencer()
    tasks = [_Task(id="fam/b"), _Task(id="fam/a")]
    ordered = seq.sequence("fam", tasks, {"path": str(tmp_path / "missing.json")})
    assert ordered == ["fam/a", "fam/b"]


def test_ranking_file_rejects_non_array_json(tmp_path: Path):
    bad = tmp_path / "ranking.json"
    bad.write_text('{"not": "an array"}')
    seq = RankingFileSequencer()
    tasks = [_Task(id="fam/a")]
    with pytest.raises(ValueError, match="JSON array of task names"):
        seq.sequence("fam", tasks, {"path": str(bad)})
