from __future__ import annotations

import json
import os
from pathlib import Path

import pytest

from loom_capacity_executor.journal import (
    ExecutorJournal,
    JournalCorruptionError,
    JournalLockError,
    JournalRegressionError,
)


def test_journal_is_exclusive_fsynced_hash_chain_and_exactly_recoverable(
    tmp_path: Path,
) -> None:
    path = tmp_path / "executor.journal"
    with ExecutorJournal(path) as journal:
        first = journal.append(
            "reservation-accepted",
            "a" * 64,
            object_kind="tranche",
            object_id="tranche-1",
        )
        second = journal.append(
            "intent-launch-ready",
            "b" * 64,
            object_kind="intent",
            object_id="intent-1",
        )
        assert first.sequence == 1
        assert second.sequence == 2
        assert second.previous_digest == first.record_digest
        assert journal.head.sequence == 2
        assert journal.head.digest == second.record_digest
        assert journal.latest("tranche", "tranche-1") == first
        assert journal.latest("intent", "intent-1") == second
        assert os.stat(path).st_mode & 0o077 == 0

        with pytest.raises(JournalLockError):
            with ExecutorJournal(path):
                pass

    with ExecutorJournal(path) as recovered:
        assert recovered.head.sequence == 2
        assert recovered.head.digest == second.record_digest
        recovered.assert_covers(2, second.record_digest)
        assert recovered.latest("tranche", "tranche-1") == first
        assert recovered.latest("intent", "intent-1") == second


def test_journal_rejects_torn_or_tampered_records_and_symlink_paths(
    tmp_path: Path,
) -> None:
    path = tmp_path / "executor.journal"
    with ExecutorJournal(path) as journal:
        journal.append(
            "reservation-accepted",
            "a" * 64,
            object_kind="tranche",
            object_id="tranche-1",
        )

    path.write_bytes(path.read_bytes() + b'{"schema_version":1')
    with pytest.raises(JournalCorruptionError, match="torn"):
        with ExecutorJournal(path):
            pass

    path.unlink()
    target = tmp_path / "target"
    target.write_text("", encoding="utf-8")
    path.symlink_to(target)
    with pytest.raises(JournalCorruptionError, match="symlink"):
        with ExecutorJournal(path):
            pass


def test_journal_rejects_digest_tampering_and_central_high_water_regression(
    tmp_path: Path,
) -> None:
    path = tmp_path / "executor.journal"
    with ExecutorJournal(path) as journal:
        head = journal.append(
            "reservation-accepted",
            "a" * 64,
            object_kind="tranche",
            object_id="tranche-1",
        )

    line = json.loads(path.read_text(encoding="ascii"))
    line["payload_digest"] = "b" * 64
    path.write_text(
        json.dumps(line, sort_keys=True, separators=(",", ":")) + "\n",
        encoding="ascii",
    )
    with pytest.raises(JournalCorruptionError, match="digest"):
        with ExecutorJournal(path):
            pass

    path.unlink()
    with ExecutorJournal(path) as empty:
        with pytest.raises(JournalRegressionError, match="behind"):
            empty.assert_covers(head.sequence, head.record_digest)
        with pytest.raises(JournalRegressionError, match="digest"):
            empty.assert_covers(0, "f" * 64)


def test_journal_rejects_invalid_object_bindings(tmp_path: Path) -> None:
    path = tmp_path / "executor.journal"
    with ExecutorJournal(path) as journal:
        with pytest.raises(ValueError, match="object kind"):
            journal.append(
                "reservation-accepted",
                "a" * 64,
                object_kind="unknown",
                object_id="tranche-1",
            )
        with pytest.raises(ValueError, match="object identity"):
            journal.append(
                "reservation-accepted",
                "a" * 64,
                object_kind="tranche",
                object_id="../escape",
            )


def test_journal_rejects_a_symlinked_or_shared_state_directory(tmp_path: Path) -> None:
    shared = tmp_path / "shared"
    shared.mkdir(mode=0o755)
    shared.chmod(0o755)
    with pytest.raises(JournalCorruptionError, match="directory permissions"):
        with ExecutorJournal(shared / "executor.journal"):
            pass

    private = tmp_path / "private"
    private.mkdir(mode=0o700)
    linked = tmp_path / "linked"
    linked.symlink_to(private, target_is_directory=True)
    with pytest.raises(JournalCorruptionError, match="directory symlink"):
        with ExecutorJournal(linked / "executor.journal"):
            pass


def test_journal_rejects_noncanonical_or_wrongly_typed_json(tmp_path: Path) -> None:
    path = tmp_path / "executor.journal"
    with ExecutorJournal(path) as journal:
        journal.append(
            "reservation-accepted",
            "a" * 64,
            object_kind="tranche",
            object_id="tranche-1",
        )
    value = json.loads(path.read_text(encoding="ascii"))
    path.write_text(json.dumps(value) + "\n", encoding="ascii")
    with pytest.raises(JournalCorruptionError, match="canonical"):
        with ExecutorJournal(path):
            pass

    value["schema_version"] = True
    path.write_text(
        json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n",
        encoding="ascii",
    )
    with pytest.raises(JournalCorruptionError, match="binding"):
        with ExecutorJournal(path):
            pass
