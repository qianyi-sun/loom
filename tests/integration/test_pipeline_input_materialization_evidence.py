from __future__ import annotations

from datetime import UTC, datetime
from uuid import uuid4

from sqlalchemy import create_engine, inspect

from loom.db.schema import PipelineInputMaterializationEvidence
from loom.pipeline.keys import canonical_document, digest_bytes
from loom.pipeline.work_protocol import (
    PipelineInputMaterializationEvidenceRefV1,
    PipelineInputMaterializationEvidenceV1,
)


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
