from __future__ import annotations

from uuid import uuid4

import pytest
from pydantic import ValidationError

from loom.pipeline.artifact_commit import (
    AcceptanceEvidenceProducerV1,
    ArtifactCommitError,
    ArtifactCommitService,
    InputImportProducerV1,
    ProfileCalibrationEvidenceProducerV1,
    UploadFilePlanV1,
)
from loom.trajectory.storage import FakeObjectStore


def test_six_kind_conditional_shapes_are_closed() -> None:
    with pytest.raises(ValidationError):
        AcceptanceEvidenceProducerV1(
            commit_kind="acceptance_evidence",
            team_id=uuid4(),
            pipeline_acceptance_authorization_id=uuid4(),
            acceptance_action="matrix",
            acceptance_candidate_sha256="sha256:" + "a" * 64,
            acceptance_result_kind="success",
            acceptance_termination_reason="operator_abort",
            actor_user_id=uuid4(),
        )
    certification = ProfileCalibrationEvidenceProducerV1(
        commit_kind="profile_calibration_evidence",
        team_id=uuid4(),
        pipeline_profile_calibration_authorization_id=uuid4(),
        profile_calibration_spec_sha256="sha256:" + "b" * 64,
        profile_calibration_result_kind="certification",
        profile_calibration_scenario_id="S01",
        profile_calibration_candidate_identity_sha256="sha256:" + "c" * 64,
        profile_calibration_run_ordinal=1,
        profile_calibration_source_pipeline_run_id=uuid4(),
        profile_calibration_termination_reason=None,
        actor_user_id=uuid4(),
    )
    assert certification.profile_calibration_result_kind == "certification"


async def test_input_import_plan_is_server_fixed() -> None:
    service = ArtifactCommitService(store=FakeObjectStore(), bucket="artifacts")
    producer = InputImportProducerV1(
        commit_kind="input_import",
        team_id=uuid4(),
        pipeline_input_import_id=uuid4(),
        actor_user_id=uuid4(),
    )
    invalid = UploadFilePlanV1(
        file_index=0,
        preallocated_artifact_id=uuid4(),
        relative_path="artifact.json",
        artifact_name="dataset",
        artifact_type="behavior.dataset.v1",
        producer="container",
        media_type="application/json",
        role="semantic_document",
        archive_format="none",
        expected_max_bytes=10,
        expected_sha256=None,
        expected_size=None,
    )
    with pytest.raises(ArtifactCommitError, match="invalid_input_import_plan"):
        await service.prepare_session(
            producer=producer,
            files=[invalid],
            idempotency_key="import",
            request_digest="sha256:" + "d" * 64,
        )
