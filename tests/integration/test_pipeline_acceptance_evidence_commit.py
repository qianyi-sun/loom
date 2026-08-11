from __future__ import annotations

from uuid import uuid4

from loom.pipeline.artifact_commit import (
    AcceptanceControllerServiceAuthV1,
    AcceptanceEvidenceProducerV1,
    ArtifactCommitService,
    AuthoritativeArtifactDocumentV1,
)
from loom.pipeline.keys import canonical_digest
from loom.trajectory.storage import FakeObjectStore


class Authority:
    def __init__(self) -> None:
        self.completed = None

    async def load_success_and_lock(self, **kwargs):
        producer = AcceptanceEvidenceProducerV1(
            commit_kind="acceptance_evidence",
            team_id=uuid4(),
            pipeline_acceptance_authorization_id=kwargs["authorization_id"],
            acceptance_action=kwargs["action"],
            acceptance_candidate_sha256=kwargs["candidate_sha256"],
            acceptance_result_kind="success",
            acceptance_termination_reason=None,
            actor_user_id=uuid4(),
        )
        raw = {
            "producer": producer,
            "artifact_id": uuid4(),
            "artifact_name": "evidence",
            "artifact_type": "pipeline_acceptance_evidence.v1",
            "relative_path": "evidence.json",
            "semantic_document": {"schema_version": "pipeline.acceptance-evidence.v1"},
            "max_bytes": 1024,
        }
        return AuthoritativeArtifactDocumentV1(
            **raw, declaration_digest=canonical_digest(raw, persisted=False)
        )

    async def load_terminal_and_lock(self, **kwargs):
        raise AssertionError(kwargs)

    async def complete_locked(self, *, artifact_id):
        self.completed = artifact_id


async def test_controller_authority_is_the_only_byte_source() -> None:
    authority = Authority()
    service = ArtifactCommitService(
        store=FakeObjectStore(), bucket="artifacts", acceptance_authority=authority
    )
    result = await service.finalize_acceptance_evidence(
        authorization_id=uuid4(),
        action="matrix",
        candidate_sha256="sha256:" + "a" * 64,
        auth=AcceptanceControllerServiceAuthV1(
            subject_kind="official_service",
            subject_id=uuid4(),
            service_name="loom-pipeline-acceptance-controller",
        ),
    )
    assert authority.completed == result.artifacts[0].id
