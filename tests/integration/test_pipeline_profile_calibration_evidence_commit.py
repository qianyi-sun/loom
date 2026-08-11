from __future__ import annotations

from uuid import uuid4

from loom.pipeline.artifact_commit import (
    AcceptanceControllerServiceAuthV1,
    ArtifactCommitService,
    AuthoritativeArtifactDocumentV1,
    ProfileCalibrationEvidenceProducerV1,
)
from loom.pipeline.keys import canonical_digest
from loom.trajectory.storage import FakeObjectStore


class ProfileAuthority:
    async def load_catalog_and_lock(self, **kwargs):
        producer = ProfileCalibrationEvidenceProducerV1(
            commit_kind="profile_calibration_evidence",
            team_id=uuid4(),
            pipeline_profile_calibration_authorization_id=kwargs["authorization_id"],
            profile_calibration_spec_sha256=kwargs["calibration_spec_sha256"],
            profile_calibration_result_kind="catalog",
            profile_calibration_scenario_id=None,
            profile_calibration_candidate_identity_sha256=None,
            profile_calibration_run_ordinal=None,
            profile_calibration_source_pipeline_run_id=None,
            profile_calibration_termination_reason=None,
            actor_user_id=uuid4(),
        )
        raw = {
            "producer": producer,
            "artifact_id": uuid4(),
            "artifact_name": "catalog",
            "artifact_type": "behavior_recovery_profile_catalog.v1",
            "relative_path": "catalog.json",
            "semantic_document": {"schema_version": "behavior.recovery-profile-catalog.v1"},
            "max_bytes": 1024,
        }
        return AuthoritativeArtifactDocumentV1(
            **raw, declaration_digest=canonical_digest(raw, persisted=False)
        )

    async def load_certification_and_lock(self, **kwargs):
        raise AssertionError(kwargs)

    async def load_terminal_and_lock(self, **kwargs):
        raise AssertionError(kwargs)

    async def complete_locked(self, *, artifact_id):
        self.artifact_id = artifact_id


async def test_catalog_finalization_consumes_one_authoritative_document() -> None:
    authority = ProfileAuthority()
    service = ArtifactCommitService(
        store=FakeObjectStore(), bucket="artifacts", profile_calibration_authority=authority
    )
    result = await service.finalize_profile_calibration_catalog(
        authorization_id=uuid4(),
        calibration_spec_sha256="sha256:" + "b" * 64,
        auth=AcceptanceControllerServiceAuthV1(
            subject_kind="official_service",
            subject_id=uuid4(),
            service_name="loom-pipeline-acceptance-controller",
        ),
    )
    assert authority.artifact_id == result.artifacts[0].id
