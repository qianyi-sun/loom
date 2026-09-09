"""A seeded retirement marker is a hard fence, not proof of retirement eligibility."""

import hashlib
from datetime import timedelta
from uuid import UUID, uuid4

import pytest
from sqlalchemy import select

from loom.db.schema import (
    TaskImageAttemptRetention,
    TaskImageMaterialization,
    TaskImageMaterializationAttempt,
    TaskImageRegistryCredentialGeneration,
)
from loom_task_image_authority.materializations import (
    TaskImageSessionMaterializationConflictError,
    claim_session_materialization,
    get_session_materialization_build_plan,
    heartbeat_session_materialization,
    release_session_materialization,
    start_session_materialization,
)
from loom_task_image_authority.publication_jobs import PublicationJobAuthorizationError
from loom_task_image_authority.publication_store import submit_publication_job
from loom_task_image_authority.retention_inventory import derive_attempt_repository_inventory
from tests.integration.test_task_image_candidate_v2 import _prepared, _record
from tests.integration.test_task_image_publication_completion import (
    _complete,
    _signed_job,
    completion,
)
from tests.integration.test_task_image_publication_jobs import (
    registry_authority_session as registry_authority_session,
)
from tests.integration.test_task_image_registry_credentials import (
    NOW,
    _claimed_attempt,
    _issue_first,
)
from tests.integration.test_task_image_registry_credentials import (
    registry_issuer as registry_issuer,
)


async def _mark_retired(session, attempt_id):
    attempt = await session.get(TaskImageMaterializationAttempt, attempt_id)
    row = await session.get(TaskImageMaterialization, attempt.materialization_id)
    credentials = list(
        await session.scalars(
            select(TaskImageRegistryCredentialGeneration).where(
                TaskImageRegistryCredentialGeneration.materialization_attempt_id == attempt.id,
            )
        )
    )
    inventory = derive_attempt_repository_inventory(
        materialization=row,
        attempt=attempt,
        credentials=credentials,
        registry_origin="https://registry.example:5443",
    )
    marker = await session.get(TaskImageAttemptRetention, attempt.id)
    if marker is None:
        marker = TaskImageAttemptRetention(attempt_id=attempt.id)
    marker.observed_at = NOW + timedelta(days=2)
    marker.unreferenced_since = NOW + timedelta(days=1)
    marker.retired_at = NOW + timedelta(days=2)
    marker.canonical_inventory = inventory.canonical_bytes
    marker.inventory_sha256 = hashlib.sha256(inventory.canonical_bytes).hexdigest()
    session.add(marker)
    await session.commit()


@pytest.mark.parametrize("replay", [False, True])
async def test_retired_attempt_refuses_registry_issue_and_exact_replay(
    registry_authority_session, registry_issuer, replay
):
    async with registry_authority_session() as session:
        auth, _, _, build_session, secrets, row, attempt = await _claimed_attempt(session)
        options = dict(
            authorization=auth,
            build_session=build_session,
            secrets=secrets,
            row=row,
            attempt=attempt,
            issuer=registry_issuer,
        )
        if replay:
            await _issue_first(session, **options)
        await _mark_retired(session, attempt.id)
        # Deliberately use the old live clock: retirement must not be a TTL.
        with pytest.raises(
            TaskImageSessionMaterializationConflictError, match="permanently retired"
        ):
            await _issue_first(session, **options)


@pytest.mark.parametrize("surface", ["claim_replay", "plan"])
async def test_retired_attempt_refuses_builder_input_authority(registry_authority_session, surface):
    async with registry_authority_session() as session:
        auth, _, _, _, _, row, attempt = await _claimed_attempt(session)
        await _mark_retired(session, attempt.id)
        with pytest.raises(
            TaskImageSessionMaterializationConflictError, match="permanently retired"
        ):
            if surface == "claim_replay":
                await claim_session_materialization(
                    session,
                    authorization=auth,
                    claim_id=attempt.claim_id,
                    now=NOW + timedelta(seconds=12),
                    lease_seconds=300,
                )
            else:
                await get_session_materialization_build_plan(
                    session,
                    authorization=auth,
                    materialization_id=row.id,
                    attempt_id=attempt.id,
                    lease_epoch=attempt.lease_epoch,
                    now=NOW + timedelta(seconds=12),
                )


@pytest.mark.parametrize("surface", ["candidate", "job"])
async def test_retired_attempt_refuses_candidate_and_job_submission(
    registry_authority_session, registry_issuer, surface
):
    async with registry_authority_session() as session:
        auth, _row, request, _ = await _prepared(session, registry_issuer)
        await _record(session, auth, request)
        await _mark_retired(session, request.attempt_id)
        if surface == "candidate":
            with pytest.raises(
                TaskImageSessionMaterializationConflictError, match="permanently retired"
            ):
                await _record(session, auth, request)
        else:
            with pytest.raises(PublicationJobAuthorizationError, match="permanently retired"):
                await submit_publication_job(
                    session,
                    authorization=auth,
                    operation_id=uuid4(),
                    materialization_id=request.materialization_id,
                    attempt_id=request.attempt_id,
                    lease_epoch=request.lease_epoch,
                    registry_origin="https://registry.example:5443",
                    clock=lambda: NOW + timedelta(seconds=14),
                )


async def test_retired_attempt_refuses_completion(registry_authority_session, registry_issuer):
    async with registry_authority_session() as session:
        values = await _signed_job(session, registry_issuer)
        await _mark_retired(session, UUID(values[0].snapshot.attempt_id))
        with pytest.raises(PublicationJobAuthorizationError, match="permanently retired"):
            await _complete(session, values)


async def test_retired_attempt_preserves_completed_receipt_replay(
    registry_authority_session, registry_issuer
):
    async with registry_authority_session() as session:
        values = await _signed_job(session, registry_issuer)
        receipt = await _complete(session, values)
        await _mark_retired(session, UUID(values[0].snapshot.attempt_id))
        assert (
            await completion().replay_completed_publication(
                session, operation_id=values[0].operation_id
            )
            == receipt
        )


@pytest.mark.parametrize("surface", ["start", "heartbeat"])
@pytest.mark.parametrize("replay", [False, True])
async def test_retired_attempt_refuses_live_operation_and_replay(
    registry_authority_session, surface, replay
):
    async with registry_authority_session() as session:
        auth, _, _, _, _, row, attempt = await _claimed_attempt(session)
        options = dict(
            authorization=auth,
            materialization_id=row.id,
            attempt_id=attempt.id,
            lease_epoch=attempt.lease_epoch,
            now=NOW + timedelta(seconds=12),
        )
        if surface == "heartbeat":
            await start_session_materialization(session, operation_id=uuid4(), **options)
        operation = (
            start_session_materialization
            if surface == "start"
            else heartbeat_session_materialization
        )
        operation_id = uuid4()
        if replay:
            await operation(session, operation_id=operation_id, **options)
        await _mark_retired(session, attempt.id)
        with pytest.raises(
            TaskImageSessionMaterializationConflictError, match="permanently retired"
        ):
            await operation(session, operation_id=operation_id, **options)


@pytest.mark.parametrize("replay", [False, True])
async def test_retired_attempt_preserves_cleanup_only_release(registry_authority_session, replay):
    async with registry_authority_session() as session:
        auth, _, _, _, _, row, attempt = await _claimed_attempt(session)
        options = dict(
            authorization=auth,
            materialization_id=row.id,
            attempt_id=attempt.id,
            lease_epoch=attempt.lease_epoch,
            now=NOW + timedelta(seconds=12),
            operation_id=uuid4(),
        )
        if replay:
            await release_session_materialization(session, **options)
        await _mark_retired(session, attempt.id)
        released = await release_session_materialization(session, **options)
        assert released.state == "queued" and released.claimed_by is None


async def test_retirement_fence_ignores_cached_unretired_observation(registry_authority_session):
    async with registry_authority_session() as reader:
        auth, _, _, _, _, row, attempt = await _claimed_attempt(reader)
        cached = TaskImageAttemptRetention(attempt_id=attempt.id, observed_at=NOW)
        reader.add(cached)
        await reader.commit()
        async with registry_authority_session() as writer:
            await _mark_retired(writer, attempt.id)
        assert cached.retired_at is None
        with pytest.raises(
            TaskImageSessionMaterializationConflictError, match="permanently retired"
        ):
            await get_session_materialization_build_plan(
                reader,
                authorization=auth,
                materialization_id=row.id,
                attempt_id=attempt.id,
                lease_epoch=attempt.lease_epoch,
                now=NOW + timedelta(seconds=12),
            )
