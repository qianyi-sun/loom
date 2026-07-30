from __future__ import annotations

import json
from pathlib import Path

import pytest

from loom_cli.rollout.desired_state_store import (
    ConcurrentUpdateError,
    DesiredState,
    DesiredStateError,
    DesiredStateStore,
)

_T0 = "2026-07-30T20:00:00Z"
_T1 = "2026-07-30T20:05:00Z"


def _store(tmp_path: Path, *, environment: str = "staging") -> DesiredStateStore:
    return DesiredStateStore(tmp_path / "desired-state.json", environment=environment)


def test_absent_store_reads_none_and_generation_zero(tmp_path: Path) -> None:
    store = _store(tmp_path)
    assert store.read() is None
    assert store.current_generation() == 0


def test_first_compare_and_set_creates_generation_one(tmp_path: Path) -> None:
    store = _store(tmp_path)
    state = store.compare_and_set(
        "a" * 40, expected_generation=0, updated_by="hongjian", updated_at=_T0, note="first"
    )
    assert state.generation == 1
    assert state.version == "a" * 40
    read_back = store.read()
    assert read_back == state
    assert read_back is not None and read_back.note == "first"


def test_successive_sets_advance_generation(tmp_path: Path) -> None:
    store = _store(tmp_path)
    store.compare_and_set("v1", expected_generation=0, updated_by="a", updated_at=_T0)
    second = store.compare_and_set("v2", expected_generation=1, updated_by="b", updated_at=_T1)
    assert second.generation == 2
    assert store.current_generation() == 2
    assert store.read().version == "v2"  # type: ignore[union-attr]


def test_stale_generation_loses_the_race_and_does_not_write(tmp_path: Path) -> None:
    store = _store(tmp_path)
    store.compare_and_set("v1", expected_generation=0, updated_by="a", updated_at=_T0)

    with pytest.raises(ConcurrentUpdateError, match="expected 0, found 1"):
        store.compare_and_set("v2", expected_generation=0, updated_by="b", updated_at=_T1)

    # the losing write left the store untouched
    current = store.read()
    assert current is not None and current.version == "v1" and current.generation == 1


def test_read_rejects_environment_mismatch(tmp_path: Path) -> None:
    _store(tmp_path, environment="staging").compare_and_set(
        "v1", expected_generation=0, updated_by="a", updated_at=_T0
    )
    other = _store(tmp_path, environment="prod")
    with pytest.raises(DesiredStateError, match="does not match 'prod'"):
        other.read()


def test_empty_version_is_rejected(tmp_path: Path) -> None:
    store = _store(tmp_path)
    with pytest.raises(DesiredStateError, match="version must be non-empty"):
        store.compare_and_set("", expected_generation=0, updated_by="a", updated_at=_T0)


def test_malformed_json_raises(tmp_path: Path) -> None:
    path = tmp_path / "desired-state.json"
    path.write_text("{not json", encoding="utf-8")
    with pytest.raises(DesiredStateError, match="not valid JSON"):
        DesiredStateStore(path, environment="staging").read()


def test_atomic_write_leaves_no_temp_and_valid_json(tmp_path: Path) -> None:
    store = _store(tmp_path)
    store.compare_and_set("v1", expected_generation=0, updated_by="a", updated_at=_T0)
    names = [p.name for p in tmp_path.iterdir()]
    # the store file exists and no half-written temp file was left behind
    assert "desired-state.json" in names
    assert not any(".tmp." in name for name in names)
    doc = json.loads((tmp_path / "desired-state.json").read_text())
    assert doc["schema_version"] == 1 and doc["generation"] == 1


def test_desired_state_dict_round_trip() -> None:
    state = DesiredState(
        environment="staging",
        version="deadbeef",
        generation=3,
        updated_at=_T0,
        updated_by="hongjian",
        note="cutover",
    )
    assert DesiredState.from_dict(state.to_dict()) == state


@pytest.mark.parametrize(
    ("mutation", "match"),
    [
        ({"schema_version": 2}, "schema_version must be 1"),
        ({"generation": 0}, "generation must be a positive integer"),
        ({"generation": "1"}, "generation must be a positive integer"),
        ({"version": ""}, "version must be non-empty"),
    ],
)
def test_from_dict_rejects_bad_records(mutation: dict, match: str) -> None:
    base = DesiredState("staging", "v1", 1, _T0, "a").to_dict()
    base.update(mutation)
    with pytest.raises(DesiredStateError, match=match):
        DesiredState.from_dict(base)


def test_from_dict_reports_missing_key() -> None:
    base = DesiredState("staging", "v1", 1, _T0, "a").to_dict()
    del base["updated_by"]
    with pytest.raises(DesiredStateError, match="missing key"):
        DesiredState.from_dict(base)
