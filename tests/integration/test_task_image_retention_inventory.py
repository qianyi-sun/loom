"""Real issuance persists cleanup inventory before any candidate exists."""

from sqlalchemy import func, select

from loom.db.schema import (
    TaskImageMaterialization,
    TaskImageMaterializationAttempt,
    TaskImagePublicationCandidate,
    TaskImageRegistryCredentialGeneration,
)
from loom_task_image_authority.retention_inventory import derive_attempt_repository_inventory
from tests.integration.test_task_image_candidate_v2 import _prepared
from tests.integration.test_task_image_publication_jobs import (
    registry_authority_session as registry_authority_session,
)
from tests.integration.test_task_image_registry_credentials import (
    registry_issuer as registry_issuer,
)


async def test_committed_credential_without_candidate_retains_exact_repository(
    registry_authority_session,
    registry_issuer,
):
    async with registry_authority_session() as session:
        _, materialization, request, _ = await _prepared(session, registry_issuer)
        materialization_id = materialization.id
        await session.commit()
    async with registry_authority_session() as session:
        row = await session.scalar(
            select(TaskImageMaterialization)
            .where(
                TaskImageMaterialization.id == materialization_id,
            )
            .with_for_update()
        )
        attempt = await session.scalar(
            select(TaskImageMaterializationAttempt)
            .where(
                TaskImageMaterializationAttempt.id == request.attempt_id,
            )
            .with_for_update()
        )
        credentials = list(
            await session.scalars(
                select(TaskImageRegistryCredentialGeneration)
                .where(
                    TaskImageRegistryCredentialGeneration.materialization_attempt_id == attempt.id,
                )
                .with_for_update()
            )
        )
        assert (
            await session.scalar(select(func.count()).select_from(TaskImagePublicationCandidate))
            == 0
        )
        inventory = derive_attempt_repository_inventory(
            materialization=row,
            attempt=attempt,
            credentials=credentials,
            registry_origin=registry_issuer.registry_origin,
        )
        assert len(inventory.repositories) == 1
        assert inventory.repositories[0].repository == credentials[0].repository
        assert inventory.repositories[0].last_credential_expires_at == credentials[0].expires_at
        assert inventory.repositories[0].credential_count == 1
        # This is discovery evidence only; no retirement/readiness is conferred.
        assert row.state == "claimed" and not row.registry_images and row.ready_at is None
