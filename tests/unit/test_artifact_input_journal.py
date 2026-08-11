from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

import pytest

import loom_worker.artifact_input_journal as journal_module
from loom.pipeline.keys import canonical_document, digest_bytes
from loom.pipeline.work_protocol import AcceptanceEvictionGrantV1
from loom_worker.artifact_input_journal import (
    AcceptanceEvictionCommandHandler,
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


class _Authority:
    def __init__(self, grant: AcceptanceEvictionGrantV1) -> None:
        self.grant = grant

    async def authorize(self, **_: object) -> AcceptanceEvictionGrantV1:
        return self.grant


async def test_acceptance_eviction_is_all_five_and_replayable(tmp_path: Path) -> None:
    journal = _journal(tmp_path)
    manifests = tuple(f"sha256:{index:064x}" for index in range(1, 6))
    _ready(journal, manifests[0])
    grant = AcceptanceEvictionGrantV1(
        schema_version="loom.acceptance-eviction-grant.v1",
        command_id=uuid4(),
        authorization_id=uuid4(),
        candidate_sha256="sha256:" + "c" * 64,
        worker_id=uuid4(),
        worker_lease_epoch=1,
        ordered_manifest_sha256s=list(manifests),
        pipeline_run_id=uuid4(),
        exclusive_fence_id=uuid4(),
        authorization_snapshot_sha256="sha256:" + "d" * 64,
        backend_variant_id="oldlab-rtx5080-2gpu",
        policy_id="behavior-gpu-oldlab",
        policy_config_sha256="sha256:" + "e" * 64,
        policy_activation_epoch=1,
        slurm_cluster_id="oldlab",
        slurm_cluster_config_sha256="sha256:" + "f" * 64,
        slurm_allocation_id="123",
        worker_capability_snapshot_digest="sha256:" + "1" * 64,
        action="matrix",
    )
    authority = _Authority(grant)
    handler = AcceptanceEvictionCommandHandler(journal=journal, authority=authority)

    first = await handler.evict_acceptance_entries(
        authorization_id=grant.authorization_id,
        candidate_sha256=grant.candidate_sha256,
        worker_id=grant.worker_id,
        ordered_manifest_sha256s=manifests,
    )
    replay = await handler.evict_acceptance_entries(
        authorization_id=grant.authorization_id,
        candidate_sha256=grant.candidate_sha256,
        worker_id=grant.worker_id,
        ordered_manifest_sha256s=manifests,
    )

    assert first == replay
    assert first.finished_at <= datetime.now(UTC)
    assert first.evicted_count == 1
    assert first.absence_verified is True
    assert all(journal.get_entry(digest) is None for digest in manifests)


async def test_restart_resumes_acceptance_tombstone_to_exact_result(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    journal = _journal(tmp_path)
    manifests = tuple(f"sha256:{index:064x}" for index in range(11, 16))
    _ready(journal, manifests[0])
    grant = AcceptanceEvictionGrantV1(
        schema_version="loom.acceptance-eviction-grant.v1",
        command_id=uuid4(),
        authorization_id=uuid4(),
        candidate_sha256="sha256:" + "a" * 64,
        worker_id=uuid4(),
        worker_lease_epoch=1,
        ordered_manifest_sha256s=list(manifests),
        pipeline_run_id=uuid4(),
        exclusive_fence_id=uuid4(),
        authorization_snapshot_sha256="sha256:" + "b" * 64,
        backend_variant_id="gb10-shared-1gpu",
        policy_id="behavior-gpu-gb10",
        policy_config_sha256="sha256:" + "c" * 64,
        policy_activation_epoch=1,
        slurm_cluster_id="gb10",
        slurm_cluster_config_sha256="sha256:" + "d" * 64,
        slurm_allocation_id="456",
        worker_capability_snapshot_digest="sha256:" + "e" * 64,
        action="matrix",
    )
    original_remove = journal_module._remove_tree_no_links

    def crash(_path: Path) -> None:
        raise RuntimeError("crash seam")

    monkeypatch.setattr(journal_module, "_remove_tree_no_links", crash)
    handler = AcceptanceEvictionCommandHandler(
        journal=journal, authority=_Authority(grant)
    )
    with pytest.raises(RuntimeError, match="crash seam"):
        await handler.evict_acceptance_entries(
            authorization_id=grant.authorization_id,
            candidate_sha256=grant.candidate_sha256,
            worker_id=grant.worker_id,
            ordered_manifest_sha256s=manifests,
        )

    monkeypatch.setattr(journal_module, "_remove_tree_no_links", original_remove)
    restarted = _journal(tmp_path)
    restarted.reconcile()
    replay = await AcceptanceEvictionCommandHandler(
        journal=restarted, authority=_Authority(grant)
    ).evict_acceptance_entries(
        authorization_id=grant.authorization_id,
        candidate_sha256=grant.candidate_sha256,
        worker_id=grant.worker_id,
        ordered_manifest_sha256s=manifests,
    )

    assert replay.evicted_count == 1
    assert replay.absence_verified is True
