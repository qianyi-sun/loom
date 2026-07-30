from __future__ import annotations

from pathlib import Path

import pytest

from loom_cli.rollout.version_ledger import (
    AppliedVersion,
    VersionLedger,
    VersionLedgerError,
)

_T0 = "2026-07-30T20:00:00Z"


def _ledger(tmp_path: Path, *, environment: str = "staging") -> VersionLedger:
    return VersionLedger(tmp_path / "version-ledger.json", environment=environment)


def test_empty_ledger_needs_apply_for_anything(tmp_path: Path) -> None:
    ledger = _ledger(tmp_path)
    assert ledger.applied("database-migration") is None
    assert ledger.needs_apply("database-migration", 1) is True


def test_record_then_skip_already_applied(tmp_path: Path) -> None:
    ledger = _ledger(tmp_path)
    ledger.record_applied(
        "database-migration", ordinal=5, label="rev_abc", applied_at=_T0, applied_by="a"
    )
    assert ledger.needs_apply("database-migration", 5) is False  # at target → skip
    assert ledger.needs_apply("database-migration", 4) is False  # behind target → skip
    assert ledger.needs_apply("database-migration", 6) is True  # ahead → run
    applied = ledger.applied("database-migration")
    assert applied is not None and applied.ordinal == 5 and applied.label == "rev_abc"


def test_forward_advance_updates_position(tmp_path: Path) -> None:
    ledger = _ledger(tmp_path)
    ledger.record_applied("mutation-epoch", ordinal=1, label="e1", applied_at=_T0, applied_by="a")
    entry = ledger.record_applied(
        "mutation-epoch", ordinal=2, label="e2", applied_at=_T0, applied_by="a"
    )
    assert entry.ordinal == 2
    assert ledger.applied("mutation-epoch").ordinal == 2  # type: ignore[union-attr]


def test_regression_is_refused(tmp_path: Path) -> None:
    ledger = _ledger(tmp_path)
    ledger.record_applied("mutation-epoch", ordinal=3, label="e3", applied_at=_T0, applied_by="a")
    with pytest.raises(VersionLedgerError, match="cannot regress"):
        ledger.record_applied(
            "mutation-epoch", ordinal=2, label="e2", applied_at=_T0, applied_by="a"
        )
    assert ledger.applied("mutation-epoch").ordinal == 3  # type: ignore[union-attr]


def test_same_ordinal_same_label_is_idempotent(tmp_path: Path) -> None:
    ledger = _ledger(tmp_path)
    ledger.record_applied("supervisors", ordinal=4, label="v4", applied_at=_T0, applied_by="a")
    # a retry of the exact same apply is tolerated
    entry = ledger.record_applied(
        "supervisors", ordinal=4, label="v4", applied_at="later", applied_by="b"
    )
    assert entry.ordinal == 4


def test_same_ordinal_different_label_is_a_conflict(tmp_path: Path) -> None:
    ledger = _ledger(tmp_path)
    ledger.record_applied("supervisors", ordinal=4, label="v4", applied_at=_T0, applied_by="a")
    with pytest.raises(VersionLedgerError, match="conflict"):
        ledger.record_applied(
            "supervisors", ordinal=4, label="DIFFERENT", applied_at=_T0, applied_by="a"
        )


def test_multiple_components_are_independent(tmp_path: Path) -> None:
    ledger = _ledger(tmp_path)
    ledger.record_applied("migration", ordinal=7, label="m7", applied_at=_T0, applied_by="a")
    ledger.record_applied("epoch", ordinal=2, label="e2", applied_at=_T0, applied_by="a")
    assert ledger.applied("migration").ordinal == 7  # type: ignore[union-attr]
    assert ledger.applied("epoch").ordinal == 2  # type: ignore[union-attr]
    assert ledger.needs_apply("epoch", 3) is True
    assert ledger.needs_apply("migration", 7) is False


def test_environment_mismatch_is_refused(tmp_path: Path) -> None:
    _ledger(tmp_path, environment="staging").record_applied(
        "migration", ordinal=1, label="m1", applied_at=_T0, applied_by="a"
    )
    with pytest.raises(VersionLedgerError, match="does not match 'prod'"):
        _ledger(tmp_path, environment="prod").applied("migration")


def test_applied_version_round_trip() -> None:
    version = AppliedVersion("migration", 9, "rev_z", _T0, "hongjian")
    assert AppliedVersion.from_dict(version.to_dict()) == version


def test_from_dict_rejects_negative_ordinal() -> None:
    bad = AppliedVersion("migration", 1, "m1", _T0, "a").to_dict()
    bad["ordinal"] = -1
    with pytest.raises(VersionLedgerError, match="non-negative"):
        AppliedVersion.from_dict(bad)
