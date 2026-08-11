from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

from sqlalchemy import create_engine, inspect

from loom.db.schema import PipelineInputMaterializationEvidence
from loom.pipeline.keys import canonical_document, digest_bytes
from loom.pipeline.work_protocol import (
    PipelineInputMaterializationEvidenceRefV1,
    PipelineInputMaterializationEvidenceV1,
)
from loom_worker.artifact_input_journal import ArtifactInputJournal
from loom_worker.artifact_inputs import (
    ArtifactInputMaterializer,
    DeterministicReadOnlyViewMounter,
)
from tests.pipeline_input_helpers import NeverCancelled, claim, scalar_artifact


class _MapReader:
    def __init__(self, records: dict[str, tuple[object, bytes]]) -> None:
        self.records = records
        self.manifest_opens = 0
        self.file_opens = 0

    async def read_manifest(self, *, item: object, **_: object) -> object:
        self.manifest_opens += 1
        return self.records[item.manifest_sha256][0]  # type: ignore[attr-defined]

    async def read_file(
        self, *, item: object, destination: Path, **_: object
    ) -> tuple[int, int]:
        self.file_opens += 1
        payload = self.records[item.manifest_sha256][1]  # type: ignore[attr-defined]
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes(payload)
        return len(payload), 1


def test_evidence_document_and_reference_are_exact_and_immutable_shaped() -> None:
    attempt_id = uuid4()
    worker_id = uuid4()
    evidence = PipelineInputMaterializationEvidenceV1(
        schema_version="loom.pipeline-input-materialization-evidence.v1",
        execution_attempt_id=attempt_id,
        worker_id=worker_id,
        lease_epoch=1,
        cache_expectation="warm_reuse_only",
        ordered_manifest_sha256s=[f"sha256:{index:064x}" for index in range(5)],
        manifest_open_count=5,
        file_open_count=0,
        file_bytes=0,
        archive_extraction_count=0,
        cas_rename_count=0,
        input_view_sha256="sha256:" + "a" * 64,
        materialized_at=datetime.now(UTC),
    )
    encoded = canonical_document(evidence.model_dump(mode="json"))
    reference = PipelineInputMaterializationEvidenceRefV1(
        attempt_id=attempt_id,
        worker_id=worker_id,
        lease_epoch=1,
        evidence_sha256=digest_bytes(encoded),
    )

    assert encoded.endswith(b"\n")
    assert reference.model_dump().keys() == {
        "attempt_id",
        "worker_id",
        "lease_epoch",
        "evidence_sha256",
    }
    assert PipelineInputMaterializationEvidence.__table__.primary_key.columns.keys() == [
        "execution_attempt_id"
    ]
    assert "updated_at" not in PipelineInputMaterializationEvidence.__table__.columns


def test_evidence_migration_has_exact_attempt_pk_fk_and_no_cleanup_fields(
    postgres_url: str,
) -> None:
    engine = create_engine(postgres_url)
    try:
        inspector = inspect(engine)
        columns = {
            column["name"]
            for column in inspector.get_columns("pipeline_input_materialization_evidence")
        }
        primary_key = inspector.get_pk_constraint(
            "pipeline_input_materialization_evidence"
        )
        foreign_keys = inspector.get_foreign_keys(
            "pipeline_input_materialization_evidence"
        )
    finally:
        engine.dispose()

    assert primary_key["constrained_columns"] == ["execution_attempt_id"]
    assert any(
        key["referred_table"] == "execution_attempts"
        and key["constrained_columns"] == ["execution_attempt_id", "worker_id"]
        for key in foreign_keys
    )
    assert not {
        "signature",
        "cleanup_sha256",
        "view_released",
        "leases_released",
        "updated_at",
    } & columns


async def test_acceptance_cold_then_warm_counters_are_frozen_while_leases_live(
    tmp_path: Path,
) -> None:
    names = ["task_set", "task_instances", "dataset", "policy", "mop_bank"]
    bindings = []
    records: dict[str, tuple[object, bytes]] = {}
    for name in names:
        payload, manifest, binding = scalar_artifact()
        binding = binding.model_copy(update={"binding_name": name})
        bindings.append(binding)
        records[binding.items[0].manifest_sha256] = (manifest, payload)
    reader = _MapReader(records)
    journal = ArtifactInputJournal(
        database_path=tmp_path / "state/cache.sqlite3",
        cas_root=tmp_path / "cas",
        capacity_bytes=1024 * 1024,
    )
    materializer = ArtifactInputMaterializer(
        read_client=reader,  # type: ignore[arg-type]
        journal=journal,
        attempt_input_root=tmp_path / "attempts",
        mounter=DeterministicReadOnlyViewMounter(),
    )
    cold_claim = claim(bindings[0]).model_copy(
        update={
            "input_bindings": bindings,
            "acceptance_preflight": SimpleNamespace(
                cache_expectation="cold_after_eviction"
            ),
        }
    )
    cold = await materializer.materialize_inputs(
        claim=cold_claim, cancellation=NeverCancelled()
    )
    async with cold:
        report = cold.acceptance_evidence_report(worker_id=uuid4())
        assert report.manifest_open_count == 5
        assert report.file_open_count == 5
        assert report.cas_rename_count == 5

    warm_claim = claim(bindings[0]).model_copy(
        update={
            "input_bindings": bindings,
            "acceptance_preflight": SimpleNamespace(
                cache_expectation="warm_reuse_only"
            ),
        }
    )
    warm = await materializer.materialize_inputs(
        claim=warm_claim, cancellation=NeverCancelled()
    )
    async with warm:
        report = warm.acceptance_evidence_report(worker_id=uuid4())
        assert report.manifest_open_count == 5
        assert report.file_open_count == 0
        assert report.file_bytes == 0
        assert report.archive_extraction_count == 0
        assert report.cas_rename_count == 0

    assert reader.manifest_opens == 10
    assert reader.file_opens == 5
