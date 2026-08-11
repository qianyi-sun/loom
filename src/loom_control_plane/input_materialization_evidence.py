"""Immutable acceptance input-materialization evidence authority."""

from __future__ import annotations

from datetime import UTC, datetime

from fastapi import HTTPException
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from loom.db.schema import (
    Artifact,
    ArtifactUploadSession,
    ExecutionAttempt,
    PipelineAcceptancePreflightPrerequisite,
    PipelineInputMaterializationEvidence,
    PipelineStageRun,
)
from loom.pipeline.keys import canonical_document, canonical_identity, digest_bytes
from loom.pipeline.spec import BindingSetV1
from loom.pipeline.work_protocol import (
    PipelineInputMaterializationEvidenceRefV1,
    PipelineInputMaterializationEvidenceReportV1,
    PipelineInputMaterializationEvidenceV1,
)


class PipelineInputMaterializationEvidenceService:
    async def persist(
        self,
        *,
        attempt: ExecutionAttempt,
        report: PipelineInputMaterializationEvidenceReportV1,
        session: AsyncSession,
    ) -> PipelineInputMaterializationEvidenceRefV1:
        existing = (
            await session.execute(
                select(PipelineInputMaterializationEvidence)
                .where(
                    PipelineInputMaterializationEvidence.execution_attempt_id == attempt.id
                )
                .with_for_update()
            )
        ).scalar_one_or_none()
        if existing is not None:
            persisted = PipelineInputMaterializationEvidenceV1.model_validate_json(
                existing.evidence_json
            )
            if self._report_fields(persisted) != self._report_fields(report):
                raise HTTPException(status_code=409, detail="idempotency_conflict")
            return self._reference(existing)

        if (
            attempt.state != "claimed"
            or attempt.worker_id != report.worker_id
            or attempt.lease_epoch != report.lease_epoch
            or attempt.started_at is not None
            or attempt.container_id is not None
            or attempt.runtime_started_at is not None
            or attempt.step_jwt_id is not None
        ):
            raise HTTPException(status_code=409, detail="materialization_evidence_phase")
        stage = (
            await session.execute(
                select(PipelineStageRun)
                .where(PipelineStageRun.id == attempt.stage_run_id)
                .with_for_update()
            )
        ).scalar_one()
        prerequisite = (
            await session.execute(
                select(PipelineAcceptancePreflightPrerequisite)
                .where(
                    PipelineAcceptancePreflightPrerequisite.pipeline_run_id
                    == stage.pipeline_run_id
                )
                .with_for_update()
            )
        ).scalar_one_or_none()
        consumed_stage = None
        if prerequisite is not None and prerequisite.consumed_attempt_id is not None:
            consumed_attempt = await session.get(
                ExecutionAttempt, prerequisite.consumed_attempt_id
            )
            if consumed_attempt is not None:
                consumed_stage = await session.get(
                    PipelineStageRun, consumed_attempt.stage_run_id
                )
        acceptance_phase_chain = (
            stage.node_key.endswith("acceptance_preflight_cold")
            and prerequisite is not None
            and prerequisite.consumed_attempt_id == attempt.id
        ) or (
            stage.node_key.endswith("acceptance_preflight_warm")
            and consumed_stage is not None
            and consumed_stage.pipeline_run_id == stage.pipeline_run_id
            and consumed_stage.node_key.endswith("acceptance_preflight_cold")
        )
        if (
            prerequisite is None
            or prerequisite.state != "consumed"
            or prerequisite.fence_state != "active"
            or not acceptance_phase_chain
            or prerequisite.worker_id != report.worker_id
            or prerequisite.worker_lease_epoch != report.lease_epoch
            or prerequisite.exclusive_fence_id is None
            or prerequisite.authorization_id is None
        ):
            raise HTTPException(status_code=409, detail="acceptance_preflight_not_active")
        expected_phase = (
            "cold_after_eviction"
            if stage.node_key.endswith("acceptance_preflight_cold")
            else "warm_reuse_only"
            if stage.node_key.endswith("acceptance_preflight_warm")
            else None
        )
        if expected_phase is None or report.cache_expectation != expected_phase:
            raise HTTPException(status_code=409, detail="cache_expectation_drift")
        raw_bindings = stage.resolved_input_bindings_json
        if not isinstance(raw_bindings, list):
            raise HTTPException(status_code=409, detail="input_descriptor_drift")
        try:
            bindings = [BindingSetV1.model_validate(value) for value in raw_bindings]
        except ValueError as exc:
            raise HTTPException(status_code=409, detail="input_descriptor_drift") from exc
        manifests: list[str] = []
        view_records: list[dict[str, str]] = []
        expected_file_opens = 0
        expected_file_bytes = 0
        expected_extractions = 0
        expected_binding_names = ["task_set", "task_instances", "dataset", "policy", "mop_bank"]
        if [binding.binding_name for binding in bindings] != expected_binding_names:
            raise HTTPException(status_code=409, detail="input_descriptor_drift")
        for binding in bindings:
            if binding.cardinality != "one" or len(binding.items) != 1:
                raise HTTPException(status_code=409, detail="input_descriptor_drift")
            for item in binding.items:
                manifests.append(item.manifest_sha256)
                view_records.append(
                    {
                        "binding_name": binding.binding_name,
                        "view_digest": digest_bytes(
                            canonical_identity(
                                {
                                    "binding_name": binding.binding_name,
                                    "artifact_type": binding.artifact_type,
                                    "cardinality": "one",
                                    "manifest_sha256": item.manifest_sha256,
                                }
                            )
                        ),
                    }
                )
                artifact = await session.get(Artifact, item.artifact_id)
                upload = (
                    await session.get(
                        ArtifactUploadSession, artifact.artifact_upload_session_id
                    )
                    if artifact is not None
                    and artifact.artifact_upload_session_id is not None
                    else None
                )
                root = upload.canonical_manifest_json if upload is not None else None
                records = root.get("artifacts", []) if isinstance(root, dict) else []
                record = next(
                    (
                        value
                        for value in records
                        if value.get("artifact_id") == str(item.artifact_id)
                    ),
                    None,
                )
                stored_files = record.get("stored_files") if isinstance(record, dict) else None
                if not isinstance(stored_files, list):
                    raise HTTPException(status_code=409, detail="input_descriptor_drift")
                expected_file_opens += len(stored_files)
                expected_file_bytes += sum(int(value.get("size_bytes", -1)) for value in stored_files)
                expected_extractions += sum(
                    value.get("archive_format") != "none" for value in stored_files
                )
        if manifests != report.ordered_manifest_sha256s or len(manifests) != 5:
            raise HTTPException(status_code=409, detail="input_descriptor_drift")
        expected_view = digest_bytes(
            canonical_identity(
                [*view_records, {"stage_request_sha256": attempt.stage_request_digest}]
            )
        )
        if report.input_view_sha256 != expected_view:
            raise HTTPException(status_code=409, detail="input_view_digest_drift")
        if report.manifest_open_count != 5:
            raise HTTPException(status_code=409, detail="materialization_counter_drift")
        if expected_phase == "warm_reuse_only":
            if any(
                value != 0
                for value in (
                    report.file_open_count,
                    report.file_bytes,
                    report.archive_extraction_count,
                    report.cas_rename_count,
                )
            ):
                raise HTTPException(status_code=409, detail="materialization_counter_drift")
        elif (
            report.file_open_count != expected_file_opens
            or report.file_bytes != expected_file_bytes
            or report.archive_extraction_count != expected_extractions
            or report.cas_rename_count != len(set(manifests))
        ):
            raise HTTPException(status_code=409, detail="materialization_counter_drift")

        materialized_at = datetime.now(UTC)
        evidence = PipelineInputMaterializationEvidenceV1(
            **report.model_dump(mode="python", exclude={"schema_version"}),
            schema_version="loom.pipeline-input-materialization-evidence.v1",
            materialized_at=materialized_at,
        )
        evidence_json = canonical_document(evidence.model_dump(mode="json"))
        evidence_sha256 = digest_bytes(evidence_json)
        row = PipelineInputMaterializationEvidence(
            execution_attempt_id=attempt.id,
            worker_id=report.worker_id,
            lease_epoch=report.lease_epoch,
            cache_expectation=report.cache_expectation,
            ordered_manifest_sha256s_json=canonical_document(
                report.ordered_manifest_sha256s
            ),
            manifest_open_count=report.manifest_open_count,
            file_open_count=report.file_open_count,
            file_bytes=report.file_bytes,
            archive_extraction_count=report.archive_extraction_count,
            cas_rename_count=report.cas_rename_count,
            input_view_sha256=report.input_view_sha256,
            materialized_at=materialized_at,
            evidence_json=evidence_json,
            evidence_sha256=evidence_sha256,
        )
        session.add(row)
        await session.flush()
        return self._reference(row)

    @staticmethod
    def _report_fields(
        report: PipelineInputMaterializationEvidenceReportV1
        | PipelineInputMaterializationEvidenceV1,
    ) -> dict[str, object]:
        excluded = {"schema_version", "materialized_at"}
        return report.model_dump(mode="json", exclude=excluded)

    @staticmethod
    def _reference(
        row: PipelineInputMaterializationEvidence,
    ) -> PipelineInputMaterializationEvidenceRefV1:
        return PipelineInputMaterializationEvidenceRefV1(
            attempt_id=row.execution_attempt_id,
            worker_id=row.worker_id,
            lease_epoch=row.lease_epoch,
            evidence_sha256=row.evidence_sha256,
        )


__all__ = ["PipelineInputMaterializationEvidenceService"]
