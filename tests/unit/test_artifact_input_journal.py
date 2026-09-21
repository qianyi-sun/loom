from __future__ import annotations

from pathlib import Path
from uuid import uuid4

import pytest

from loom.pipeline.keys import canonical_document, digest_bytes
from loom_worker.artifact_input_journal import (
    ArtifactInputJournal,
    ArtifactInputJournalError,
    allocatable_capacity,
    validate_registration_capacity,
)


def _journal(tmp_path: Path, capacity: int = 1000) -> ArtifactInputJournal:
    return ArtifactInputJournal(
        database_path=tmp_path / "state/input-cache.sqlite3",
        cas_root=tmp_path / "cas",
        capacity_bytes=capacity,
    )


def _ready(journal: ArtifactInputJournal, digest: str, size: int = 10) -> None:
    owner = uuid4()
    assert journal.reserve(
        manifest_sha256=digest,
        unpacked_size_bytes=size,
        file_count=1,
        owner_attempt_id=owner,
    ) is None
    root = journal.ready_path(digest)
    (root / "payload").mkdir(parents=True)
    ready = canonical_document({"manifest_sha256": digest})
    (root / "READY.json").write_bytes(ready)
    journal.mark_ready(
        manifest_sha256=digest,
        ready_path=root,
        ready_sha256=digest_bytes(ready),
    )


def test_capacity_lease_release_and_zero_ref_gc(tmp_path: Path) -> None:
    journal = _journal(tmp_path, capacity=100)
    digest = "sha256:" + "a" * 64
    _ready(journal, digest, 70)
    attempt = uuid4()
    journal.acquire_lease(
        execution_attempt_id=attempt,
        binding_name="dataset",
        item_key="singleton",
        manifest_sha256=digest,
    )
    assert journal.capacity_snapshot().input_cache_reserved_bytes == 70
    assert journal.gc_zero_ref(target_bytes=0) == 0
    assert journal.release_attempt(attempt) == 1
    assert journal.gc_zero_ref(target_bytes=0) == 70
    assert journal.get_entry(digest) is None


def test_capacity_rejects_overcommit_and_formula_is_exact(tmp_path: Path) -> None:
    journal = _journal(tmp_path, capacity=85)
    assert allocatable_capacity(100) == 85
    assert allocatable_capacity(1_940_314_637_252) >= 1_649_267_441_664
    validate_registration_capacity(
        capacity_bytes=85, reserved_bytes=0, ready_bytes=0
    )
    with pytest.raises(ArtifactInputJournalError, match="input_cache_capacity"):
        journal.reserve(
            manifest_sha256="sha256:" + "b" * 64,
            unpacked_size_bytes=86,
            file_count=1,
            owner_attempt_id=uuid4(),
        )


def test_restart_resumes_local_gc_from_historical_tombstone(tmp_path: Path, monkeypatch) -> None:
    import loom_worker.artifact_input_journal as module

    journal = _journal(tmp_path)
    digest = "sha256:" + "c" * 64
    _ready(journal, digest)
    remove = module._remove_tree_no_links

    def interrupted(_path: Path) -> None:
        raise OSError("simulated GC interruption")

    monkeypatch.setattr(module, "_remove_tree_no_links", interrupted)
    with pytest.raises(OSError, match="simulated GC interruption"):
        journal.gc_zero_ref(target_bytes=0)
    assert journal.get_entry(digest).state == "deleting"
    monkeypatch.setattr(module, "_remove_tree_no_links", remove)
    recovered = _journal(tmp_path)
    recovered.reconcile()
    assert recovered.get_entry(digest) is None
    assert not recovered.ready_path(digest).exists()
