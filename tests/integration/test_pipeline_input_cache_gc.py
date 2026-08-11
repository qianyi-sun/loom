from __future__ import annotations

from pathlib import Path
from uuid import uuid4

from loom.pipeline.keys import canonical_document, digest_bytes
from loom_worker.artifact_input_journal import ArtifactInputJournal


def test_gc_keeps_live_lease_and_reclaims_lru_after_release(tmp_path: Path) -> None:
    journal = ArtifactInputJournal(
        database_path=tmp_path / "state/cache.sqlite3",
        cas_root=tmp_path / "cas",
        capacity_bytes=100,
    )
    digest = "sha256:" + "a" * 64
    owner = uuid4()
    journal.reserve(
        manifest_sha256=digest,
        unpacked_size_bytes=80,
        file_count=1,
        owner_attempt_id=owner,
    )
    ready_path = journal.ready_path(digest)
    (ready_path / "payload").mkdir(parents=True)
    ready = canonical_document({"manifest_sha256": digest})
    (ready_path / "READY.json").write_bytes(ready)
    journal.mark_ready(
        manifest_sha256=digest,
        ready_path=ready_path,
        ready_sha256=digest_bytes(ready),
    )
    journal.acquire_lease(
        execution_attempt_id=owner,
        binding_name="dataset",
        item_key="singleton",
        manifest_sha256=digest,
    )

    assert journal.gc_zero_ref() == 0
    journal.release_attempt(owner)
    assert journal.gc_zero_ref() == 80
    assert not ready_path.exists()
