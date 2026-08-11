from __future__ import annotations

from pathlib import Path
from uuid import uuid4

import pytest

from loom.pipeline.keys import canonical_document, digest_bytes
from loom.pipeline.work_protocol import AcceptanceEvictionGrantV1
from loom_worker.artifact_input_journal import (
    AcceptanceEvictionCommandHandler,
    ArtifactInputJournal,
    ArtifactInputJournalError,
)


class _Authority:
    def __init__(self, grant: AcceptanceEvictionGrantV1) -> None:
        self.grant = grant

    async def authorize(self, **_: object) -> AcceptanceEvictionGrantV1:
        return self.grant


async def test_acceptance_eviction_refuses_any_live_lease_without_partial_mutation(
    tmp_path: Path,
) -> None:
    journal = ArtifactInputJournal(
        database_path=tmp_path / "state/cache.sqlite3",
        cas_root=tmp_path / "cas",
        capacity_bytes=1000,
    )
    manifests = tuple(f"sha256:{index:064x}" for index in range(1, 6))
    digest = manifests[0]
    attempt = uuid4()
    journal.reserve(
        manifest_sha256=digest,
        unpacked_size_bytes=10,
        file_count=1,
        owner_attempt_id=attempt,
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
        execution_attempt_id=attempt,
        binding_name="dataset",
        item_key="singleton",
        manifest_sha256=digest,
    )
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
        slurm_allocation_id="allocation-1",
        worker_capability_snapshot_digest="sha256:" + "e" * 64,
        action="matrix",
    )

    with pytest.raises(ArtifactInputJournalError, match="precondition"):
        await AcceptanceEvictionCommandHandler(
            journal=journal, authority=_Authority(grant)
        ).evict_acceptance_entries(
            authorization_id=grant.authorization_id,
            candidate_sha256=grant.candidate_sha256,
            worker_id=grant.worker_id,
            ordered_manifest_sha256s=manifests,
        )

    entry = journal.get_entry(digest)
    assert entry is not None and entry.state == "ready" and entry.refcount == 1
    assert ready_path.exists()
