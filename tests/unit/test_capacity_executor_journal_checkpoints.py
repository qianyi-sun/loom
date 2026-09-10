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
        append(journal, "heartbeat-confirmed", "lease", kind="heartbeat")
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


def test_completed_runtime_work_after_prepare_cannot_invalidate_snapshot_dependencies(tmp_path):
    with ExecutorJournal(tmp_path / "journal") as journal:
        checkpoint = journal.prepare_checkpoint(retained_sequences=(), retained_anchors=(), reserved_bytes=0)
        append(journal, "slurm-submit-confirmed", "new-launch", kind="job")
        with pytest.raises(JournalRegressionError, match="unexpected runtime work"):
            journal.commit_checkpoint(central_sequence=checkpoint.sequence,
                central_digest=checkpoint.record_digest)


def test_repeated_checkpoints_are_self_contained_and_reclaim_old_snapshot(tmp_path):
    path = tmp_path / "journal"
    with ExecutorJournal(path) as journal:
        retained = append(journal, "job-retained", "live", kind="job")
        for _ in range(3):
            checkpoint = journal.prepare_checkpoint(retained_sequences=(retained.sequence,),
                retained_anchors=(), reserved_bytes=0)
            journal.commit_checkpoint(central_sequence=checkpoint.sequence,
                central_digest=checkpoint.record_digest)
            assert len(tuple(tmp_path.glob("*.snapshot-*"))) == 1
            assert journal.latest("job", "live") == retained
    with ExecutorJournal(path) as journal:
        assert journal.latest("job", "live") == retained
        journal.assert_covers(checkpoint.sequence, checkpoint.record_digest)


@pytest.mark.parametrize("after_replace", (False, True))
def test_crash_at_atomic_publication_reopens_one_complete_generation(tmp_path, monkeypatch, after_replace):
    import os

    path = tmp_path / "journal"
    with ExecutorJournal(path) as journal:
        retained = append(journal, "job-retained", "live", kind="job")
        checkpoint = journal.prepare_checkpoint(retained_sequences=(retained.sequence,),
            retained_anchors=(), reserved_bytes=0)
        real_replace = os.replace

        def crash(source, target):
            if after_replace:
                real_replace(source, target)
            raise OSError("power loss")

        with monkeypatch.context() as patch:
            patch.setattr(os, "replace", crash)
            with pytest.raises(OSError, match="power loss"):
                journal.commit_checkpoint(central_sequence=checkpoint.sequence,
                    central_digest=checkpoint.record_digest)
    with ExecutorJournal(path) as journal:
        assert journal.latest("job", "live") == retained
        journal.assert_covers(checkpoint.sequence, checkpoint.record_digest)
        assert (journal.pending_checkpoint() is None) == after_replace
        assert not tuple(tmp_path.glob("*.checkpoint-tmp-*"))


def test_orphan_snapshot_before_anchor_is_reclaimed_on_restart(tmp_path, monkeypatch):
    path = tmp_path / "journal"
    with ExecutorJournal(path) as journal:
        def crash(*args, **kwargs):
            raise OSError("crash before anchor")

        monkeypatch.setattr(journal, "append", crash)
        with pytest.raises(OSError):
            journal.prepare_checkpoint(retained_sequences=(), retained_anchors=(), reserved_bytes=0)
        assert len(tuple(tmp_path.glob("*.snapshot-*"))) == 1
    with ExecutorJournal(path) as journal:
        assert journal.head.sequence == 0
        assert not tuple(tmp_path.glob("*.snapshot-*"))


async def test_real_heartbeat_handshake_and_historical_chunked_inventory_survive_checkpoint(tmp_path):
    from loom_capacity_executor.heartbeat import ExecutableHeartbeatLoop
    from loom_capacity_executor.inventory_journal import (
        complete_inventory_request,
        load_journal_inventory,
        retain_inventory_request,
    )
    from tests.unit.test_capacity_executor_heartbeat import RecordingHeartbeatClient, _registration
    from tests.unit.test_capacity_executor_inventory_journal import large_inventory

    path = tmp_path / "journal"
    client = RecordingHeartbeatClient()
    with ExecutorJournal(path) as journal:
        value = large_inventory()
        retain_inventory_request(journal, value)
        complete_inventory_request(journal, value, rejected=False)
        inventory_record = journal.latest("inventory", str(value.executor_incarnation))
        retained = tuple(range(1, journal.head.sequence + 1))
        checkpoint = journal.prepare_checkpoint(retained_sequences=retained,
            retained_anchors=(0,), reserved_bytes=4096)
        heartbeat = await ExecutableHeartbeatLoop(_registration(), journal, client).heartbeat()
        assert heartbeat.journal_sequence == checkpoint.sequence
        readback = await client.executable_checkpoint()
        journal.commit_checkpoint(central_sequence=readback.journal_sequence,
            central_digest=readback.journal_digest)
    with ExecutorJournal(path) as journal:
        assert load_journal_inventory(journal, inventory_record) == value
        next_heartbeat = await ExecutableHeartbeatLoop(_registration(), journal, client).heartbeat()
        assert next_heartbeat.heartbeat_sequence == 2
        with pytest.raises(JournalRegressionError, match="floor"):
            journal.assert_covers(0, "0" * 64)


def test_checkpoint_does_not_resurrect_resolved_requests(tmp_path):
    with ExecutorJournal(tmp_path / "journal") as journal:
        request = append(journal, "intent-close-requested", "closed", kind="intent")
        append(journal, "intent-close-confirmed", "closed", kind="intent")
        with pytest.raises(JournalRegressionError, match="resurrect"):
            journal.prepare_checkpoint(retained_sequences=(request.sequence,),
                retained_anchors=(), reserved_bytes=0)


def test_checkpoint_header_obeys_record_size_bound_before_snapshot_read(tmp_path, monkeypatch):
    from loom_capacity_executor.journal import JournalCorruptionError

    path = tmp_path / "journal"
    with ExecutorJournal(path) as journal:
        checkpoint = journal.prepare_checkpoint(retained_sequences=(), retained_anchors=(), reserved_bytes=0)
        journal.commit_checkpoint(central_sequence=checkpoint.sequence,
            central_digest=checkpoint.record_digest)
    monkeypatch.setattr("loom_capacity_executor.journal._MAX_RECORD_BYTES", path.stat().st_size - 2)
    with pytest.raises(JournalCorruptionError, match="record bound"):
        with ExecutorJournal(path):
            pass


@pytest.mark.parametrize("mutation", ("symlink", "permissions", "hardlink"))
def test_checkpoint_snapshot_rejects_unsafe_files(tmp_path, mutation):
    from loom_capacity_executor.journal import JournalCorruptionError

    path = tmp_path / "journal"
    with ExecutorJournal(path) as journal:
        checkpoint = journal.prepare_checkpoint(retained_sequences=(), retained_anchors=(), reserved_bytes=0)
        journal.commit_checkpoint(central_sequence=checkpoint.sequence,
            central_digest=checkpoint.record_digest)
    snapshot, = tmp_path.glob("*.snapshot-*")
    if mutation == "permissions":
        snapshot.chmod(0o644)
    elif mutation == "hardlink":
        (tmp_path / "alias").hardlink_to(snapshot)
    else:
        target = tmp_path / "target"
        snapshot.rename(target)
        snapshot.symlink_to(target)
    with pytest.raises(JournalCorruptionError):
        with ExecutorJournal(path):
            pass


@pytest.mark.parametrize("ambiguous", (False, True))
async def test_checkpoint_handshake_resumes_exact_heartbeat_after_transport_failure(tmp_path, ambiguous):
    from loom_capacity_executor.heartbeat import ExecutableHeartbeatLoop
    from tests.unit.test_capacity_executor_heartbeat import RecordingHeartbeatClient, _registration

    class Client(RecordingHeartbeatClient):
        fail = True

        async def heartbeat_executable_executor(self, heartbeat):
            if self.fail:
                self.fail = False
                if ambiguous:
                    await super().heartbeat_executable_executor(heartbeat)
                raise ConnectionError("response lost")
            return await super().heartbeat_executable_executor(heartbeat)

    client = Client()
    path = tmp_path / "journal"
    with ExecutorJournal(path) as journal:
        checkpoint = journal.prepare_checkpoint(retained_sequences=(), retained_anchors=(), reserved_bytes=0)
        with pytest.raises(ConnectionError):
            await ExecutableHeartbeatLoop(_registration(), journal, client).finish_checkpoint()
        assert journal.pending_checkpoint() == checkpoint
        pending = journal.latest("heartbeat", str(_registration().executor_incarnation))
        original_payload = pending.durable_payload()
    with ExecutorJournal(path) as journal:
        assert await ExecutableHeartbeatLoop(_registration(), journal, client).finish_checkpoint()
        assert journal.pending_checkpoint() is None
        from loom_capacity_manager.executable_contracts import canonical_executable_bytes

        assert canonical_executable_bytes(client.heartbeats[-1]) == original_payload
        assert not await ExecutableHeartbeatLoop(_registration(), journal, client).finish_checkpoint()


async def test_checkpoint_handshake_requires_durable_readback_not_just_heartbeat_receipt(tmp_path):
    from loom_capacity_executor.heartbeat import ExecutableHeartbeatLoop
    from tests.unit.test_capacity_executor_heartbeat import RecordingHeartbeatClient, _registration

    class Client(RecordingHeartbeatClient):
        async def heartbeat_executable_executor(self, heartbeat):
            result = await super().heartbeat_executable_executor(heartbeat)
            self.journal_sequence = 0
            self.journal_digest = "0" * 64
            return result

    with ExecutorJournal(tmp_path / "journal") as journal:
        checkpoint = journal.prepare_checkpoint(retained_sequences=(), retained_anchors=(), reserved_bytes=0)
        with pytest.raises(JournalRegressionError, match="acknowledged"):
            await ExecutableHeartbeatLoop(_registration(), journal, Client()).finish_checkpoint()
        assert journal.pending_checkpoint() == checkpoint


async def test_checkpoint_retries_readback_without_growing_acknowledged_tail(tmp_path):
    from loom_capacity_executor.heartbeat import ExecutableHeartbeatLoop
    from tests.unit.test_capacity_executor_heartbeat import RecordingHeartbeatClient, _registration

    class Client(RecordingHeartbeatClient):
        reads = 0

        async def executable_checkpoint(self):
            self.reads += 1
            if self.reads == 2:
                raise ConnectionError("readback lost")
            return await super().executable_checkpoint()

    client = Client()
    with ExecutorJournal(tmp_path / "journal") as journal:
        journal.prepare_checkpoint(retained_sequences=(), retained_anchors=(), reserved_bytes=0)
        loop = ExecutableHeartbeatLoop(_registration(), journal, client)
        with pytest.raises(ConnectionError):
            await loop.finish_checkpoint()
        confirmed_head = journal.head
        assert await loop.finish_checkpoint()
        assert journal.head == confirmed_head
        assert len(client.heartbeats) == 1
