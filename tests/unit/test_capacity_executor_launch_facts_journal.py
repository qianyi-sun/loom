"""Retain exact launch facts within the existing bounded, locked journal."""

from importlib import import_module
from hashlib import sha256

import pytest

from loom_capacity_executor.journal import ExecutorJournal, JournalRegressionError
from loom_capacity_manager.launch_subject_contracts import canonical_launch_subject_bytes
from tests.unit.test_capacity_launch_subject_contract import response


def test_launch_facts_survive_restart_without_manager_and_are_idempotent(tmp_path):
    module = import_module("loom_capacity_executor.launch_facts_journal")
    value = response()
    path = tmp_path / "executor.journal"
    with ExecutorJournal(path) as journal:
        reference = module.retain_launch_facts(journal, value)
        head = journal.head
        assert module.retain_launch_facts(journal, value) == reference
        assert journal.head == head
        assert journal.pending_requests() == ()
    with ExecutorJournal(path) as journal:
        assert module.load_launch_facts(journal, reference) == value


def test_launch_facts_resume_partial_chunks_and_reject_missing_chunks(tmp_path, monkeypatch):
    module = import_module("loom_capacity_executor.launch_facts_journal")
    # A smaller chunk size exercises the same framing with the ordinary fixture.
    monkeypatch.setattr(module, "LAUNCH_FACTS_CHUNK_BYTES", 1024)
    value = response()
    with ExecutorJournal(tmp_path / "executor.journal") as journal:
        original = journal.append
        calls = 0

        def interrupted(*args, **kwargs):
            nonlocal calls
            calls += 1
            if calls == 2:
                raise OSError("simulated interruption before second chunk")
            return original(*args, **kwargs)

        monkeypatch.setattr(journal, "append", interrupted)
        with pytest.raises(OSError):
            module.retain_launch_facts(journal, value)
        first = journal.head
        monkeypatch.setattr(journal, "append", original)
        reference = module.retain_launch_facts(journal, value)
        assert first.sequence == 1
        assert journal.head.sequence == reference.chunk_count
        assert module.load_launch_facts(journal, reference) == value
        with ExecutorJournal(tmp_path / "empty.journal") as empty:
            with pytest.raises(JournalRegressionError, match="absent"):
                module.load_launch_facts(empty, reference)


@pytest.mark.parametrize("tamper", ("digest", "count", "bytes", "schema"))
def test_launch_facts_reject_changed_reference(tmp_path, tamper):
    module = import_module("loom_capacity_executor.launch_facts_journal")
    with ExecutorJournal(tmp_path / "executor.journal") as journal:
        reference = module.retain_launch_facts(journal, response())
        fields = {"digest": {"sha256": "f" * 64}, "count": {"chunk_count": 0},
                  "bytes": {"byte_count": reference.byte_count + 1}, "schema": {"schema_version": 3.0}}
        changed = reference.model_copy(update=fields[tamper])
        with pytest.raises((ValueError, JournalRegressionError)):
            module.load_launch_facts(journal, changed)


def test_launch_facts_reject_corrupt_retained_chunk_without_overwriting(tmp_path):
    module = import_module("loom_capacity_executor.launch_facts_journal")
    value = response()
    digest = sha256(canonical_launch_subject_bytes(value)).hexdigest()
    with ExecutorJournal(tmp_path / "executor.journal") as journal:
        journal.append("launch-facts-retained", sha256(b"wrong").hexdigest(),
            object_kind="executor", object_id=f"launch-facts:{digest}:0", payload=b"wrong")
        head = journal.head
        with pytest.raises(JournalRegressionError):
            module.retain_launch_facts(journal, value)
        assert journal.head == head


def test_launch_facts_capacity_rejection_preserves_head_and_cleanup_space(tmp_path, monkeypatch):
    module = import_module("loom_capacity_executor.launch_facts_journal")
    journal_module = import_module("loom_capacity_executor.journal")
    monkeypatch.setattr(journal_module, "_MAX_JOURNAL_BYTES", 8 * 1024 * 1024)
    with ExecutorJournal(tmp_path / "executor.journal") as journal:
        head = journal.head
        with pytest.raises(journal_module.JournalError, match="capacity"):
            module.retain_launch_facts(journal, response())
        assert journal.head == head
        payload = b"cleanup evidence"
        journal.append("intent-close-requested", sha256(payload).hexdigest(),
            object_kind="intent", object_id="cleanup", payload=payload)
        assert journal.head.sequence == 1
