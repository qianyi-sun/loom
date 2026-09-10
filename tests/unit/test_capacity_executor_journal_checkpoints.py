"""Journal compaction preserves exact evidence and never outruns manager authority."""

from hashlib import sha256

import pytest

from loom_capacity_executor.journal import ExecutorJournal, JournalRegressionError


def append(journal, event, identity, payload=b"evidence", kind="executor"):
    return journal.append(event, sha256(payload).hexdigest(), object_kind=kind,
        object_id=identity, payload=payload)


def test_checkpoint_reopen_retains_evidence_and_enforces_manager_floor(tmp_path):
    path = tmp_path / "journal"
    with ExecutorJournal(path) as journal:
        retained = append(journal, "job-retained", "live", kind="job")
        for index in range(30):
            append(journal, "telemetry-confirmed", "old", bytes([index + 1]) * 1024)
        old_size = path.stat().st_size
        checkpoint = journal.prepare_checkpoint(
            retained_sequences=(retained.sequence,), retained_anchors=(0,), reserved_bytes=4096)
        assert path.stat().st_size > old_size
        with pytest.raises(JournalRegressionError, match="acknowledg"):
            journal.commit_checkpoint(central_sequence=retained.sequence,
                central_digest=retained.record_digest)
        append(journal, "heartbeat-confirmed", "lease")
        final = journal.head
        journal.commit_checkpoint(central_sequence=checkpoint.sequence,
            central_digest=checkpoint.record_digest)
        assert path.stat().st_size < old_size
        assert journal.head == final
    with ExecutorJournal(path) as journal:
        assert journal.head == final
        assert journal.latest("job", "live") == retained
        assert journal.latest("executor", "old") is None
        journal.assert_evidence_covers(0, "0" * 64)
        with pytest.raises(JournalRegressionError, match="floor"):
            journal.assert_covers(0, "0" * 64)
        journal.assert_covers(checkpoint.sequence, checkpoint.record_digest)
        following = append(journal, "telemetry-confirmed", "new")
        assert following.sequence == final.sequence + 1


def test_checkpoint_resumes_before_replacement_without_competing_snapshot(tmp_path):
    path = tmp_path / "journal"
    with ExecutorJournal(path) as journal:
        retained = append(journal, "job-retained", "live", kind="job")
        checkpoint = journal.prepare_checkpoint(retained_sequences=(retained.sequence,),
            retained_anchors=(), reserved_bytes=0)
    with ExecutorJournal(path) as journal:
        assert journal.pending_checkpoint() == checkpoint
        with pytest.raises(JournalRegressionError, match="pending checkpoint"):
            journal.prepare_checkpoint(retained_sequences=(), retained_anchors=(), reserved_bytes=0)
        journal.commit_checkpoint(central_sequence=checkpoint.sequence,
            central_digest=checkpoint.record_digest)
        assert journal.pending_checkpoint() is None
    with ExecutorJournal(path) as journal:
        assert journal.pending_checkpoint() is None
        assert journal.latest("job", "live") == retained


@pytest.mark.parametrize("when", ("prepare", "commit"))
def test_checkpoint_refuses_pending_requests(tmp_path, when):
    with ExecutorJournal(tmp_path / "journal") as journal:
        if when == "commit":
            checkpoint = journal.prepare_checkpoint(retained_sequences=(),
                retained_anchors=(), reserved_bytes=0)
        append(journal, "inventory-publish-requested", "pending", kind="inventory")
        with pytest.raises(JournalRegressionError, match="unresolved"):
            if when == "prepare":
                journal.prepare_checkpoint(retained_sequences=(), retained_anchors=(), reserved_bytes=0)
            else:
                journal.commit_checkpoint(central_sequence=checkpoint.sequence,
                    central_digest=checkpoint.record_digest)


def test_checkpoint_budget_refusal_has_no_side_effects(tmp_path, monkeypatch):
    from loom_capacity_executor.journal import JournalError

    path = tmp_path / "journal"
    with ExecutorJournal(path) as journal:
        retained = append(journal, "job-retained", "live", b"x" * 4096, kind="job")
        original = path.read_bytes()
        monkeypatch.setattr("loom_capacity_executor.journal._MAX_JOURNAL_BYTES", len(original) + 1024)
        with pytest.raises(JournalError, match="capacity"):
            journal.prepare_checkpoint(retained_sequences=(retained.sequence,),
                retained_anchors=(), reserved_bytes=0)
        assert path.read_bytes() == original
        assert tuple(tmp_path.glob("*.snapshot-*")) == ()


def test_checkpoint_snapshot_corruption_fails_closed(tmp_path):
    from loom_capacity_executor.journal import JournalCorruptionError

    path = tmp_path / "journal"
    with ExecutorJournal(path) as journal:
        checkpoint = journal.prepare_checkpoint(retained_sequences=(), retained_anchors=(), reserved_bytes=0)
        journal.commit_checkpoint(central_sequence=checkpoint.sequence,
            central_digest=checkpoint.record_digest)
    snapshot, = tmp_path.glob("*.snapshot-*")
    snapshot.write_bytes(b"corrupt")
    with pytest.raises(JournalCorruptionError, match="snapshot"):
        with ExecutorJournal(path):
            pass
