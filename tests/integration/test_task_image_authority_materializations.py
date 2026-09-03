from __future__ import annotations

from collections.abc import AsyncIterator
from dataclasses import replace
from datetime import timedelta
from uuid import UUID, uuid4

import pytest
from sqlalchemy import delete, func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from loom.db.schema import (
    TaskImageBuildContainmentAttestation,
    TaskImageBuildGrant,
    TaskImageBuildGrantEvent,
    TaskImageBuildProjection,
    TaskImageBuildProjectionEvent,
    TaskImageBuildSessionGeneration,
    TaskImageMaterialization,
    TaskImageMaterializationAttempt,
    TaskImageMaterializationOperationEvent,
    TaskImagePublicationEvidence,
)
from loom.task_image_materialization import task_image_materialization_key
from loom_task_image_authority.contracts import TaskImageSessionRenewalV1
from loom_task_image_authority.materializations import (
    TaskImageSessionMaterializationAuthorizationError,
    TaskImageSessionMaterializationConflictError,
    claim_session_materialization,
    fail_session_materialization,
    get_session_materialization_build_plan,
    heartbeat_session_materialization,
    release_containment_failed_session_materialization,
    release_session_materialization,
    start_session_materialization,
)
from loom_task_image_authority.store import (
    TaskImageBuildSessionAuthorization,
    authorize_task_image_build_session,
    exchange_task_image_bootstrap,
    renew_task_image_build_session,
)
from tests.integration.test_task_image_projection_store import (
    GRANT_ID,
    NEXT_SESSION_ID,
    NOW,
    RENEWAL_ID,
    _attestation,
    _exchange,
    _MemorySecretStore,
    _project_grant,
)

CLAIM_ID = UUID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")
START_ID = UUID("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb")
HEARTBEAT_ID = UUID("cccccccc-cccc-cccc-cccc-cccccccccccc")
RELEASE_ID = UUID("dddddddd-dddd-dddd-dddd-dddddddddddd")
FAIL_ID = UUID("eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee")


@pytest.fixture
async def authority_materialization_session(
    postgres_url: str,
) -> AsyncIterator[async_sessionmaker[AsyncSession]]:
    engine = create_async_engine(postgres_url)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    try:
        yield factory
    finally:
        async with factory() as session:
            await session.execute(delete(TaskImageMaterializationOperationEvent))
            await session.execute(delete(TaskImagePublicationEvidence))
            await session.execute(delete(TaskImageMaterializationAttempt))
            await session.execute(delete(TaskImageMaterialization))
            await session.execute(delete(TaskImageBuildSessionGeneration))
            await session.execute(delete(TaskImageBuildContainmentAttestation))
            await session.execute(delete(TaskImageBuildProjectionEvent))
            await session.execute(delete(TaskImageBuildProjection))
            await session.execute(delete(TaskImageBuildGrantEvent))
            await session.execute(delete(TaskImageBuildGrant))
            await session.commit()
        await engine.dispose()


def _config(*, task_id: str = "phase2c/session-bound") -> dict[str, object]:
    return {
        "schema_version": "1",
        "task": {"id": task_id, "name": "session-bound"},
        "environment": {
            "os": "linux",
            "cpu_arch": "arm64",
            "dockerfile": "environment/Dockerfile",
            "build_timeout_sec": 600.0,
        },
        "agent": {"name": "oracle"},
        "verifier": {"name": "pytest"},
    }


async def _queued_materialization(
    session: AsyncSession,
    *,
    task_id: str = "phase2c/session-bound",
    checksum: str = "4" * 64,
) -> TaskImageMaterialization:
    row = TaskImageMaterialization(
        materialization_key=task_image_materialization_key(
            task_id=task_id,
            task_checksum=checksum,
            cpu_arch="arm64",
        ),
        task_id=task_id,
        task_checksum=checksum,
        cpu_arch="arm64",
        task_config=_config(task_id=task_id),
        task_source=f"s3://loom-bundles/{task_id}/",
        task_source_provenance={
            "bundle_file_metadata_sha256": "sha256:" + "5" * 64,
        },
    )
    session.add(row)
    await session.flush()
    return row


async def _active_authorization(
    session: AsyncSession,
) -> tuple[
    TaskImageBuildSessionAuthorization,
    object,
    object,
    object,
    _MemorySecretStore,
]:
    secrets = _MemorySecretStore()
    _grant, principal, proof, receipt = await _project_grant(
        session,
        secret_store=secrets,
    )
    build_session = await exchange_task_image_bootstrap(
        session,
        principal=principal,
        request=_exchange(receipt),
        now=NOW + timedelta(seconds=8),
        secret_store=secrets,
        session_token_factory=lambda: "loom_tibs_" + "B" * 64,
    )
    authorization = await authorize_task_image_build_session(
        session,
        grant_id=GRANT_ID,
        session_id=build_session.session_id,
        session_generation=build_session.generation,
        raw_session_token=build_session.session_token,
        now=NOW + timedelta(seconds=9),
    )
    return authorization, principal, proof, build_session, secrets


async def test_shadow_session_cannot_claim_the_production_queue(
    authority_materialization_session: async_sessionmaker[AsyncSession],
) -> None:
    campaign_id = UUID("99999999-9999-9999-9999-999999999999")
    async with authority_materialization_session() as session:
        authorization, *_ = await _active_authorization(session)
        grant = await session.scalar(
            select(TaskImageBuildGrant).where(TaskImageBuildGrant.id == authorization.grant_id)
        )
        assert grant is not None
        grant.authority_spec = {
            **grant.authority_spec,
            "purpose": "shadow",
            "shadow_campaign_id": str(campaign_id),
        }
        shadow_authorization = replace(
            authorization,
            purpose="shadow",
            shadow_campaign_id=campaign_id,
        )
        row = await _queued_materialization(session)

        with pytest.raises(TaskImageSessionMaterializationAuthorizationError):
            await claim_session_materialization(
                session,
                authorization=shadow_authorization,
                claim_id=CLAIM_ID,
                now=NOW + timedelta(seconds=10),
                lease_seconds=300,
            )

        assert row.state == "queued"
        assert await session.scalar(select(func.count(TaskImageMaterializationAttempt.id))) == 0


async def _attempt(
    session: AsyncSession,
    *,
    claim_id: UUID = CLAIM_ID,
) -> TaskImageMaterializationAttempt:
    return (
        await session.scalars(
            select(TaskImageMaterializationAttempt).where(
                TaskImageMaterializationAttempt.claim_id == claim_id
            )
        )
    ).one()


async def test_claim_is_session_derived_and_exactly_replayable(
    authority_materialization_session: async_sessionmaker[AsyncSession],
) -> None:
    async with authority_materialization_session() as session:
        authorization, *_ = await _active_authorization(session)
        materialization = await _queued_materialization(session)

        claimed = await claim_session_materialization(
            session,
            authorization=authorization,
            claim_id=CLAIM_ID,
            now=NOW + timedelta(seconds=10),
            lease_seconds=300,
        )
        replay = await claim_session_materialization(
            session,
            authorization=authorization,
            claim_id=CLAIM_ID,
            now=NOW + timedelta(seconds=11),
            lease_seconds=300,
        )

        assert claimed is not None
        assert replay is not None
        claimed_row, plan = claimed
        replay_row, replay_plan = replay
        assert claimed_row.id == replay_row.id == materialization.id
        assert replay_plan == plan
        assert claimed_row.state == "claimed"
        assert claimed_row.claimed_by == f"rootless:{authorization.session_id.hex}"
        assert claimed_row.attempt_count == 0
        assert claimed_row.lease_epoch == 1
        assert claimed_row.lease_expires_at == NOW + timedelta(seconds=310)
        assert plan.materialization_id == materialization.id
        attempt = await _attempt(session)
        assert attempt.attempt_number == 1
        assert attempt.grant_id == authorization.grant_id
        assert attempt.session_id == authorization.session_id
        assert attempt.session_generation == authorization.session_generation
        assert attempt.claim_id == CLAIM_ID
        assert await session.scalar(select(func.count(TaskImageMaterializationAttempt.id))) == 1


async def test_live_claim_prevents_a_second_concurrent_claim(
    authority_materialization_session: async_sessionmaker[AsyncSession],
) -> None:
    async with authority_materialization_session() as session:
        authorization, *_ = await _active_authorization(session)
        await _queued_materialization(session)
        first = await claim_session_materialization(
            session,
            authorization=authorization,
            claim_id=CLAIM_ID,
            now=NOW + timedelta(seconds=10),
            lease_seconds=300,
        )
        second = await claim_session_materialization(
            session,
            authorization=authorization,
            claim_id=uuid4(),
            now=NOW + timedelta(seconds=11),
            lease_seconds=300,
        )

        assert first is not None
        assert second is None
        assert await session.scalar(select(func.count(TaskImageMaterializationAttempt.id))) == 1


async def test_one_session_cannot_hold_two_live_materialization_leases(
    authority_materialization_session: async_sessionmaker[AsyncSession],
) -> None:
    async with authority_materialization_session() as session:
        authorization, *_ = await _active_authorization(session)
        first_row = await _queued_materialization(session)
        second_row = await _queued_materialization(
            session,
            task_id="phase2c/session-bound-second",
            checksum="6" * 64,
        )

        first = await claim_session_materialization(
            session,
            authorization=authorization,
            claim_id=CLAIM_ID,
            now=NOW + timedelta(seconds=10),
            lease_seconds=300,
        )
        second = await claim_session_materialization(
            session,
            authorization=authorization,
            claim_id=uuid4(),
            now=NOW + timedelta(seconds=11),
            lease_seconds=300,
        )

        assert first is not None
        assert first[0].id in {first_row.id, second_row.id}
        assert second is None
        assert {first_row.state, second_row.state} == {"claimed", "queued"}
        assert await session.scalar(select(func.count(TaskImageMaterializationAttempt.id))) == 1


async def test_start_and_heartbeat_replays_do_not_extend_the_lease_twice(
    authority_materialization_session: async_sessionmaker[AsyncSession],
) -> None:
    async with authority_materialization_session() as session:
        authorization, *_ = await _active_authorization(session)
        await _queued_materialization(session)
        claimed = await claim_session_materialization(
            session,
            authorization=authorization,
            claim_id=CLAIM_ID,
            now=NOW + timedelta(seconds=10),
            lease_seconds=300,
        )
        assert claimed is not None
        row, _plan = claimed
        attempt = await _attempt(session)

        started = await start_session_materialization(
            session,
            authorization=authorization,
            materialization_id=row.id,
            attempt_id=attempt.id,
            lease_epoch=row.lease_epoch,
            operation_id=START_ID,
            now=NOW + timedelta(seconds=11),
        )
        replayed_start = await start_session_materialization(
            session,
            authorization=authorization,
            materialization_id=row.id,
            attempt_id=attempt.id,
            lease_epoch=row.lease_epoch,
            operation_id=START_ID,
            now=NOW + timedelta(seconds=12),
        )
        assert started.state == replayed_start.state == "running"
        assert replayed_start.lease_expires_at == NOW + timedelta(seconds=311)

        heartbeat = await heartbeat_session_materialization(
            session,
            authorization=authorization,
            materialization_id=row.id,
            attempt_id=attempt.id,
            lease_epoch=row.lease_epoch,
            operation_id=HEARTBEAT_ID,
            now=NOW + timedelta(seconds=13),
        )
        replayed_heartbeat = await heartbeat_session_materialization(
            session,
            authorization=authorization,
            materialization_id=row.id,
            attempt_id=attempt.id,
            lease_epoch=row.lease_epoch,
            operation_id=HEARTBEAT_ID,
            now=NOW + timedelta(seconds=14),
        )
        assert heartbeat.lease_expires_at == replayed_heartbeat.lease_expires_at
        assert replayed_heartbeat.lease_expires_at == NOW + timedelta(seconds=313)
        events = list(
            (
                await session.scalars(
                    select(TaskImageMaterializationOperationEvent).order_by(
                        TaskImageMaterializationOperationEvent.recorded_at
                    )
                )
            ).all()
        )
        assert [event.operation_type for event in events] == ["start", "heartbeat"]

        with pytest.raises(TaskImageSessionMaterializationConflictError):
            await release_session_materialization(
                session,
                authorization=authorization,
                materialization_id=row.id,
                attempt_id=attempt.id,
                lease_epoch=row.lease_epoch,
                operation_id=HEARTBEAT_ID,
                now=NOW + timedelta(seconds=15),
            )


async def test_operation_replay_survives_a_later_materialization_lease_epoch(
    authority_materialization_session: async_sessionmaker[AsyncSession],
) -> None:
    async with authority_materialization_session() as session:
        authorization, *_ = await _active_authorization(session)
        await _queued_materialization(session)
        claimed = await claim_session_materialization(
            session,
            authorization=authorization,
            claim_id=CLAIM_ID,
            now=NOW + timedelta(seconds=10),
            lease_seconds=300,
        )
        assert claimed is not None
        row, _plan = claimed
        first_attempt = await _attempt(session)
        await start_session_materialization(
            session,
            authorization=authorization,
            materialization_id=row.id,
            attempt_id=first_attempt.id,
            lease_epoch=row.lease_epoch,
            operation_id=START_ID,
            now=NOW + timedelta(seconds=11),
        )
        await release_session_materialization(
            session,
            authorization=authorization,
            materialization_id=row.id,
            attempt_id=first_attempt.id,
            lease_epoch=row.lease_epoch,
            operation_id=RELEASE_ID,
            now=NOW + timedelta(seconds=12),
        )
        row.next_attempt_at = NOW + timedelta(seconds=13)
        second_claim = await claim_session_materialization(
            session,
            authorization=authorization,
            claim_id=uuid4(),
            now=NOW + timedelta(seconds=14),
            lease_seconds=300,
        )
        assert second_claim is not None
        assert row.lease_epoch == 2

        replay = await start_session_materialization(
            session,
            authorization=authorization,
            materialization_id=row.id,
            attempt_id=first_attempt.id,
            lease_epoch=1,
            operation_id=START_ID,
            now=NOW + timedelta(seconds=15),
        )

        assert replay.id == row.id
        event = await session.scalar(
            select(TaskImageMaterializationOperationEvent).where(
                TaskImageMaterializationOperationEvent.operation_id == START_ID
            )
        )
        assert event is not None
        assert event.lease_epoch == 1
        assert event.result_state == "running"
        assert event.result_lease_expires_at == NOW + timedelta(seconds=311)


async def test_infrastructure_release_preserves_failure_budget_and_applies_backoff(
    authority_materialization_session: async_sessionmaker[AsyncSession],
) -> None:
    async with authority_materialization_session() as session:
        authorization, *_ = await _active_authorization(session)
        await _queued_materialization(session)
        first = await claim_session_materialization(
            session,
            authorization=authorization,
            claim_id=CLAIM_ID,
            now=NOW + timedelta(seconds=10),
            lease_seconds=300,
        )
        assert first is not None
        row, _plan = first
        first_attempt = await _attempt(session)
        released = await release_session_materialization(
            session,
            authorization=authorization,
            materialization_id=row.id,
            attempt_id=first_attempt.id,
            lease_epoch=row.lease_epoch,
            operation_id=RELEASE_ID,
            now=NOW + timedelta(seconds=11),
        )
        replay = await release_session_materialization(
            session,
            authorization=authorization,
            materialization_id=row.id,
            attempt_id=first_attempt.id,
            lease_epoch=row.lease_epoch,
            operation_id=RELEASE_ID,
            now=NOW + timedelta(seconds=12),
        )
        assert released.state == replay.state == "queued"
        assert replay.attempt_count == 0
        assert replay.next_attempt_at == NOW + timedelta(seconds=41)

        assert (
            await claim_session_materialization(
                session,
                authorization=authorization,
                claim_id=uuid4(),
                now=NOW + timedelta(seconds=13),
                lease_seconds=300,
            )
            is None
        )


async def test_containment_release_preserves_failure_budget_and_revokes_session(
    authority_materialization_session: async_sessionmaker[AsyncSession],
) -> None:
    async with authority_materialization_session() as session:
        authorization, *_ = await _active_authorization(session)
        await _queued_materialization(session)
        claimed = await claim_session_materialization(
            session,
            authorization=authorization,
            claim_id=CLAIM_ID,
            now=NOW + timedelta(seconds=10),
            lease_seconds=300,
        )
        assert claimed is not None
        row, _plan = claimed
        attempt = await _attempt(session)
        containment_release = await release_containment_failed_session_materialization(
            session,
            authorization=authorization,
            materialization_id=row.id,
            attempt_id=attempt.id,
            lease_epoch=row.lease_epoch,
            operation_id=uuid4(),
            now=NOW + timedelta(seconds=11),
        )
        assert containment_release.state == "queued"
        assert containment_release.attempt_count == 0
        projection = await session.scalar(
            select(TaskImageBuildProjection).where(
                TaskImageBuildProjection.grant_id == authorization.grant_id
            )
        )
        assert projection is not None
        assert projection.state == "revoked"
        assert projection.revoke_reason == "containment_failure"

        with pytest.raises(TaskImageSessionMaterializationAuthorizationError):
            await claim_session_materialization(
                session,
                authorization=authorization,
                claim_id=uuid4(),
                now=NOW + timedelta(seconds=12),
                lease_seconds=300,
            )


async def test_only_deterministic_failure_advances_the_failure_budget(
    authority_materialization_session: async_sessionmaker[AsyncSession],
) -> None:
    async with authority_materialization_session() as session:
        authorization, *_ = await _active_authorization(session)
        await _queued_materialization(session)
        claimed = await claim_session_materialization(
            session,
            authorization=authorization,
            claim_id=CLAIM_ID,
            now=NOW + timedelta(seconds=10),
            lease_seconds=300,
        )
        assert claimed is not None
        row, _plan = claimed
        attempt = await _attempt(session)
        failed = await fail_session_materialization(
            session,
            authorization=authorization,
            materialization_id=row.id,
            attempt_id=attempt.id,
            lease_epoch=row.lease_epoch,
            operation_id=FAIL_ID,
            now=NOW + timedelta(seconds=11),
        )
        replay = await fail_session_materialization(
            session,
            authorization=authorization,
            materialization_id=row.id,
            attempt_id=attempt.id,
            lease_epoch=row.lease_epoch,
            operation_id=FAIL_ID,
            now=NOW + timedelta(seconds=12),
        )

        assert failed.state == replay.state == "queued"
        assert replay.attempt_count == 1
        assert replay.failure_reason == "deterministic_build_failure"
        assert replay.next_attempt_at == NOW + timedelta(seconds=41)


async def test_wrong_binding_or_expired_lease_cannot_mutate_materialization(
    authority_materialization_session: async_sessionmaker[AsyncSession],
) -> None:
    async with authority_materialization_session() as session:
        authorization, *_ = await _active_authorization(session)
        await _queued_materialization(session)
        claimed = await claim_session_materialization(
            session,
            authorization=authorization,
            claim_id=CLAIM_ID,
            now=NOW + timedelta(seconds=10),
            lease_seconds=5,
        )
        assert claimed is not None
        row, _plan = claimed
        attempt = await _attempt(session)

        for materialization_id, attempt_id, lease_epoch, now in (
            (uuid4(), attempt.id, row.lease_epoch, NOW + timedelta(seconds=11)),
            (row.id, uuid4(), row.lease_epoch, NOW + timedelta(seconds=11)),
            (row.id, attempt.id, row.lease_epoch + 1, NOW + timedelta(seconds=11)),
            (row.id, attempt.id, row.lease_epoch, NOW + timedelta(seconds=15)),
        ):
            with pytest.raises(TaskImageSessionMaterializationConflictError):
                await start_session_materialization(
                    session,
                    authorization=authorization,
                    materialization_id=materialization_id,
                    attempt_id=attempt_id,
                    lease_epoch=lease_epoch,
                    operation_id=uuid4(),
                    now=now,
                )
        assert row.state == "claimed"


async def test_renewal_supersedes_old_session_but_preserves_grant_owned_lease(
    authority_materialization_session: async_sessionmaker[AsyncSession],
) -> None:
    async with authority_materialization_session() as session:
        authorization, principal, proof, build_session, secrets = await _active_authorization(
            session
        )
        await _queued_materialization(session)
        await _queued_materialization(
            session,
            task_id="phase2c/session-bound-renewal-spare",
            checksum="7" * 64,
        )
        claimed = await claim_session_materialization(
            session,
            authorization=authorization,
            claim_id=CLAIM_ID,
            now=NOW + timedelta(seconds=10),
            lease_seconds=300,
        )
        assert claimed is not None
        row, _plan = claimed
        attempt = await _attempt(session)

        next_attestation = _attestation(proof, generation=2)
        renewed_session = await renew_task_image_build_session(
            session,
            principal=principal,
            request=TaskImageSessionRenewalV1(
                renewal_id=RENEWAL_ID,
                grant_id=GRANT_ID,
                session_id=build_session.session_id,
                session_generation=build_session.generation,
                session_token=build_session.session_token,
                attestation=next_attestation,
                observed_at=next_attestation.issued_at,
            ),
            now=NOW + timedelta(seconds=13),
            secret_store=secrets,
            session_token_factory=lambda: "loom_tibs_" + "C" * 64,
            session_id_factory=lambda: NEXT_SESSION_ID,
        )
        renewed_authorization = await authorize_task_image_build_session(
            session,
            grant_id=GRANT_ID,
            session_id=renewed_session.session_id,
            session_generation=renewed_session.generation,
            raw_session_token=renewed_session.session_token,
            now=NOW + timedelta(seconds=14),
        )

        assert (
            await claim_session_materialization(
                session,
                authorization=renewed_authorization,
                claim_id=uuid4(),
                now=NOW + timedelta(seconds=14),
                lease_seconds=300,
            )
            is None
        )

        renewed_plan = await get_session_materialization_build_plan(
            session,
            authorization=renewed_authorization,
            materialization_id=row.id,
            attempt_id=attempt.id,
            lease_epoch=row.lease_epoch,
            now=NOW + timedelta(seconds=14),
        )
        assert renewed_plan.session_id == renewed_session.session_id
        assert renewed_plan.session_generation == renewed_session.generation
        with pytest.raises(TaskImageSessionMaterializationAuthorizationError):
            await get_session_materialization_build_plan(
                session,
                authorization=authorization,
                materialization_id=row.id,
                attempt_id=attempt.id,
                lease_epoch=row.lease_epoch,
                now=NOW + timedelta(seconds=14),
            )

        with pytest.raises(TaskImageSessionMaterializationAuthorizationError):
            await start_session_materialization(
                session,
                authorization=authorization,
                materialization_id=row.id,
                attempt_id=attempt.id,
                lease_epoch=row.lease_epoch,
                operation_id=START_ID,
                now=NOW + timedelta(seconds=14),
            )

        started = await start_session_materialization(
            session,
            authorization=renewed_authorization,
            materialization_id=row.id,
            attempt_id=attempt.id,
            lease_epoch=row.lease_epoch,
            operation_id=START_ID,
            now=NOW + timedelta(seconds=14),
        )
        assert started.state == "running"
        event = (
            await session.scalars(
                select(TaskImageMaterializationOperationEvent).where(
                    TaskImageMaterializationOperationEvent.operation_id == START_ID
                )
            )
        ).one()
        assert event.session_id == renewed_session.session_id
        assert event.session_generation == renewed_session.generation

        with pytest.raises(TaskImageSessionMaterializationConflictError):
            await claim_session_materialization(
                session,
                authorization=renewed_authorization,
                claim_id=CLAIM_ID,
                now=NOW + timedelta(seconds=15),
                lease_seconds=300,
            )


@pytest.mark.parametrize(
    "changes",
    [
        {"grant_id": UUID("99999999-9999-9999-9999-999999999999")},
        {"session_id": UUID("99999999-9999-9999-9999-999999999999")},
        {"session_generation": 2},
        {"authority_version": 1, "builder_release_sha256": None},
        {"builder_release_sha256": "8" * 64},
        {"supervisor_executable_sha256": "8" * 64},
        {
            "purpose": "shadow",
            "shadow_campaign_id": UUID("99999999-9999-9999-9999-999999999999"),
        },
        {"environment": "other"},
        {"pool_id": "other-pool"},
        {"cpu_arch": "x86_64"},
        {"attestation_generation": 2},
        {"attestation_sha256": "8" * 64},
        {"attestation_expires_at": NOW + timedelta(seconds=41)},
        {"session_expires_at": NOW + timedelta(seconds=41)},
        {"grant_expires_at": NOW + timedelta(hours=3)},
    ],
)
async def test_mutated_session_authorization_is_rejected_before_claim(
    authority_materialization_session: async_sessionmaker[AsyncSession],
    changes: dict[str, object],
) -> None:
    async with authority_materialization_session() as session:
        authorization, *_ = await _active_authorization(session)
        await _queued_materialization(session)
        changed = replace(authorization, **changes)  # type: ignore[arg-type]

        with pytest.raises(TaskImageSessionMaterializationAuthorizationError):
            await claim_session_materialization(
                session,
                authorization=changed,
                claim_id=uuid4(),
                now=NOW + timedelta(seconds=10),
                lease_seconds=300,
            )
