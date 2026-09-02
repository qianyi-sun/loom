from __future__ import annotations

from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta
from uuid import UUID, uuid4

import pytest
from pydantic import ValidationError
from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from loom.db.schema import TaskImageBuildGrant, TaskImageBuildGrantEvent
from loom_control_plane.task_image_build_environment import (
    RootlessBuildResourceRequestV1,
    SlurmBuildEnvironmentPolicyV1,
    SlurmBuildInventoryV1,
    SlurmBuildJobObservationV1,
    canonical_request_sha256,
    issue_slurm_build_grant,
)
from loom_control_plane.task_image_build_grants import (
    TaskImageBuildGrantConflictError,
    _stored_grant,
    begin_task_image_build_submission,
    issue_task_image_build_grant,
    reconcile_task_image_build_submission,
    record_task_image_build_release,
)
from loom_task_image_authority.contracts import (
    TaskImageBuildGrantAuthorityV1,
    canonical_authority_sha256,
)

_NOW = datetime(2026, 8, 22, 2, 0, tzinfo=UTC)


def _policy() -> SlurmBuildEnvironmentPolicyV1:
    return SlurmBuildEnvironmentPolicyV1(
        schema="loom.task-image-build-environment-policy/v1",
        enabled=False,
        activation_blockers=("guard_missing",),
        slurm_cluster_id="gb10",
        cpu_arch="arm64",
        submitting_identity="loom-builder",
        partition="loom-task-builder",
        account="loom-task-builder",
        qos="loom-task-image-builder-rootless-gb10",
        feature_constraint="loom_rootless_buildkit",
        supervisor_path="/usr/local/libexec/loom-task-builder-supervisor",
        sbatch_path="/usr/bin/sbatch",
        resources=RootlessBuildResourceRequestV1(
            cpus=8,
            memory_mib=32768,
            pids=4096,
            scratch_bytes=107374182400,
            scratch_inodes=1000000,
            wall_time="02:00:00",
            swap_bytes=0,
        ),
    )


def _authority(
    policy: SlurmBuildEnvironmentPolicyV1,
    **changes: object,
) -> TaskImageBuildGrantAuthorityV1:
    values: dict[str, object] = {
        "purpose": "production",
        "shadow_campaign_id": None,
        "environment": "staging",
        "pool_id": "staging-gb10-task-image",
        "slurm_cluster_id": policy.slurm_cluster_id,
        "cpu_arch": policy.cpu_arch,
        "slurm_request_sha256": canonical_request_sha256(policy.request_identity()),
        "builder_release_sha256": "2" * 64,
        "build_policy_sha256": "3" * 64,
        "containment_policy_sha256": "4" * 64,
        "resource_profile_sha256": "5" * 64,
        "issued_at": _NOW - timedelta(minutes=1),
        "expires_at": _NOW + timedelta(hours=2),
    }
    values.update(changes)
    return TaskImageBuildGrantAuthorityV1.model_validate(values)


def _grant(grant_id: UUID | None = None, **authority_changes: object):
    policy = _policy()
    return issue_slurm_build_grant(
        policy,
        grant_id=grant_id or uuid4(),
        authority=_authority(policy, **authority_changes),
    )


def _inventory(
    grant,
    *,
    job_id: str,
    state: str,
    held: bool,
    observed_at: datetime = _NOW,
):
    return SlurmBuildInventoryV1(
        controller_authoritative=True,
        accounting_authoritative=True,
        observed_at=observed_at,
        jobs=(
            SlurmBuildJobObservationV1(
                job_id=job_id,
                state=state,
                held=held,
                comment=grant.comment,
                submitting_identity=grant.request.submitting_identity,
                request=grant.request,
            ),
        ),
    )


@pytest.fixture
async def grant_session(
    postgres_url: str,
) -> AsyncIterator[async_sessionmaker[AsyncSession]]:
    engine = create_async_engine(postgres_url)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    try:
        yield factory
    finally:
        async with factory() as session:
            await session.execute(delete(TaskImageBuildGrantEvent))
            await session.execute(delete(TaskImageBuildGrant))
            await session.commit()
        await engine.dispose()


async def test_submission_invocation_is_journaled_exactly_once_before_external_call(
    grant_session: async_sessionmaker[AsyncSession],
) -> None:
    grant = _grant(UUID("11111111-1111-1111-1111-111111111111"))
    async with grant_session() as session:
        row = await issue_task_image_build_grant(
            session,
            environment="staging",
            grant=grant,
            ambiguity_settle_seconds=30,
            now=_NOW,
        )
        await session.commit()
        assert row.state == "issued"
        assert row.journal_sequence == 1
        assert row.authority_spec == grant.authority.model_dump(
            mode="json", exclude_none=False
        )
        assert row.authority_sha256 == canonical_authority_sha256(grant.authority)
        assert row.grant_expires_at == grant.authority.expires_at

        begun = await begin_task_image_build_submission(session, grant_id=grant.grant_id, now=_NOW)
        invocation_started_at = begun.invocation_started_at
        with pytest.raises(TaskImageBuildGrantConflictError, match="already invoked"):
            await begin_task_image_build_submission(
                session,
                grant_id=grant.grant_id,
                now=_NOW + timedelta(seconds=1),
            )
        await session.commit()

        refreshed = await session.get(TaskImageBuildGrant, grant.grant_id)
        events = list(
            (
                await session.scalars(
                    select(TaskImageBuildGrantEvent)
                    .where(TaskImageBuildGrantEvent.grant_id == grant.grant_id)
                    .order_by(TaskImageBuildGrantEvent.sequence)
                )
            ).all()
        )
        assert refreshed is not None
        assert refreshed.state == "submitting"
        assert refreshed.invocation_started_at == invocation_started_at
        assert refreshed.journal_sequence == 2
        assert [(event.sequence, event.event_type) for event in events] == [
            (1, "issued"),
            (2, "submission_started"),
        ]


async def test_issuance_rejects_environment_expiry_or_stored_authority_drift(
    grant_session: async_sessionmaker[AsyncSession],
) -> None:
    grant = _grant()
    async with grant_session() as session:
        with pytest.raises(ValueError, match="environment"):
            await issue_task_image_build_grant(
                session,
                environment="production",
                grant=grant,
                ambiguity_settle_seconds=30,
                now=_NOW,
            )

        expired = _grant(
            issued_at=_NOW - timedelta(hours=2),
            expires_at=_NOW,
        )
        with pytest.raises(ValueError, match="expired"):
            await issue_task_image_build_grant(
                session,
                environment="staging",
                grant=expired,
                ambiguity_settle_seconds=30,
                now=_NOW,
            )

        row = await issue_task_image_build_grant(
            session,
            environment="staging",
            grant=grant,
            ambiguity_settle_seconds=30,
            now=_NOW,
        )
        row.authority_spec = {
            **row.authority_spec,
            "pool_id": "staging-gb10-other",
        }
        with pytest.raises(ValidationError, match="authority digest"):
            _stored_grant(row)
        row.authority_spec = grant.authority.model_dump(mode="json", exclude_none=False)
        row.grant_expires_at = grant.authority.expires_at + timedelta(seconds=1)
        with pytest.raises(ValueError, match="authority changed"):
            _stored_grant(row)


async def test_issuance_revalidates_unchecked_grant_copies_before_persistence(
    grant_session: async_sessionmaker[AsyncSession],
) -> None:
    grant = _grant()
    forged = grant.model_copy(
        update={
            "authority": grant.authority.model_copy(
                update={"pool_id": "staging-gb10-other"}
            )
        }
    )
    async with grant_session() as session:
        with pytest.raises(ValidationError, match="authority digest"):
            await issue_task_image_build_grant(
                session,
                environment="staging",
                grant=forged,
                ambiguity_settle_seconds=30,
                now=_NOW,
            )


async def test_expired_authority_cannot_start_submission_or_release_held_job(
    grant_session: async_sessionmaker[AsyncSession],
) -> None:
    grant = _grant(expires_at=_NOW + timedelta(seconds=2))
    async with grant_session() as session:
        await issue_task_image_build_grant(
            session,
            environment="staging",
            grant=grant,
            ambiguity_settle_seconds=30,
            now=_NOW,
        )
        await session.commit()
        with pytest.raises(TaskImageBuildGrantConflictError, match="expired"):
            await begin_task_image_build_submission(
                session,
                grant_id=grant.grant_id,
                now=grant.authority.expires_at,
            )
        await session.rollback()

        await begin_task_image_build_submission(
            session,
            grant_id=grant.grant_id,
            now=_NOW,
        )
        await reconcile_task_image_build_submission(
            session,
            grant_id=grant.grant_id,
            inventory=_inventory(
                grant,
                job_id="12345",
                state="pending",
                held=True,
            ),
            now=_NOW + timedelta(seconds=1),
        )
        with pytest.raises(TaskImageBuildGrantConflictError, match="expired"):
            await record_task_image_build_release(
                session,
                grant_id=grant.grant_id,
                job_id="12345",
                now=grant.authority.expires_at,
            )


async def test_expired_authority_cannot_reconcile_a_submission(
    grant_session: async_sessionmaker[AsyncSession],
) -> None:
    grant = _grant(expires_at=_NOW + timedelta(seconds=2))
    async with grant_session() as session:
        await issue_task_image_build_grant(
            session,
            environment="staging",
            grant=grant,
            ambiguity_settle_seconds=30,
            now=_NOW,
        )
        await begin_task_image_build_submission(
            session,
            grant_id=grant.grant_id,
            now=_NOW,
        )
        await session.commit()

        with pytest.raises(TaskImageBuildGrantConflictError, match="expired"):
            await reconcile_task_image_build_submission(
                session,
                grant_id=grant.grant_id,
                inventory=_inventory(
                    grant,
                    job_id="12345",
                    state="pending",
                    held=True,
                    observed_at=grant.authority.expires_at,
                ),
                now=grant.authority.expires_at,
            )


async def test_ambiguity_settle_window_starts_with_the_submission_invocation(
    grant_session: async_sessionmaker[AsyncSession],
) -> None:
    grant = _grant()
    invocation_started_at = _NOW + timedelta(minutes=5)
    async with grant_session() as session:
        await issue_task_image_build_grant(
            session,
            environment="staging",
            grant=grant,
            ambiguity_settle_seconds=30,
            now=_NOW,
        )
        begun = await begin_task_image_build_submission(
            session,
            grant_id=grant.grant_id,
            now=invocation_started_at,
        )

        assert begun.ambiguity_settle_until == invocation_started_at + timedelta(seconds=30)


async def test_exact_held_job_binds_durably_before_release(
    grant_session: async_sessionmaker[AsyncSession],
) -> None:
    grant = _grant()
    async with grant_session() as session:
        await issue_task_image_build_grant(
            session,
            environment="staging",
            grant=grant,
            ambiguity_settle_seconds=30,
            now=_NOW,
        )
        await begin_task_image_build_submission(session, grant_id=grant.grant_id, now=_NOW)
        await session.commit()

        decision = await reconcile_task_image_build_submission(
            session,
            grant_id=grant.grant_id,
            inventory=_inventory(grant, job_id="12345", state="pending", held=True),
            now=_NOW + timedelta(seconds=1),
        )
        assert decision.action == "bind"
        bound = await session.get(TaskImageBuildGrant, grant.grant_id)
        assert bound is not None
        assert (bound.state, bound.slurm_job_id, bound.released_at) == (
            "bound",
            "12345",
            None,
        )
        await session.commit()

        with pytest.raises(TaskImageBuildGrantConflictError, match="not bound"):
            await record_task_image_build_release(
                session,
                grant_id=grant.grant_id,
                job_id="54321",
                now=_NOW + timedelta(seconds=2),
            )
        released = await record_task_image_build_release(
            session,
            grant_id=grant.grant_id,
            job_id="12345",
            now=_NOW + timedelta(seconds=2),
        )
        await session.commit()
        assert released.state == "released"
        assert released.released_at == _NOW + timedelta(seconds=2)


async def test_reconciliation_revokes_terminal_or_zero_and_journals_cancellation_once(
    grant_session: async_sessionmaker[AsyncSession],
) -> None:
    empty_grant, terminal_grant, cancel_grant = (_grant() for _ in range(3))
    async with grant_session() as session:
        for grant in (empty_grant, terminal_grant, cancel_grant):
            await issue_task_image_build_grant(
                session,
                environment="staging",
                grant=grant,
                ambiguity_settle_seconds=1,
                now=_NOW,
            )
            await begin_task_image_build_submission(session, grant_id=grant.grant_id, now=_NOW)
        await session.commit()

        empty = await reconcile_task_image_build_submission(
            session,
            grant_id=empty_grant.grant_id,
            inventory=SlurmBuildInventoryV1(
                controller_authoritative=True,
                accounting_authoritative=True,
                observed_at=_NOW + timedelta(seconds=2),
                jobs=(),
            ),
            now=_NOW + timedelta(seconds=2),
        )
        terminal = await reconcile_task_image_build_submission(
            session,
            grant_id=terminal_grant.grant_id,
            inventory=_inventory(
                terminal_grant,
                job_id="22222",
                state="terminal",
                held=False,
            ),
            now=_NOW + timedelta(seconds=2),
        )
        running_inventory = _inventory(
            cancel_grant,
            job_id="33333",
            state="running",
            held=False,
        )
        cancel_first = await reconcile_task_image_build_submission(
            session,
            grant_id=cancel_grant.grant_id,
            inventory=running_inventory,
            now=_NOW + timedelta(seconds=2),
        )
        cancel_repeat = await reconcile_task_image_build_submission(
            session,
            grant_id=cancel_grant.grant_id,
            inventory=running_inventory,
            now=_NOW + timedelta(seconds=3),
        )
        await session.commit()

        assert (empty.action, terminal.action) == ("revoke", "revoke")
        assert cancel_first == cancel_repeat
        assert cancel_first.action == "cancel_then_reconcile"
        rows = {
            row.id: row
            for row in (
                await session.scalars(
                    select(TaskImageBuildGrant).where(
                        TaskImageBuildGrant.id.in_(
                            (empty_grant.grant_id, terminal_grant.grant_id, cancel_grant.grant_id)
                        )
                    )
                )
            ).all()
        }
        assert rows[empty_grant.grant_id].state == "revoked"
        assert rows[terminal_grant.grant_id].state == "revoked"
        assert rows[cancel_grant.grant_id].state == "submitting"
        cancellation_events = list(
            (
                await session.scalars(
                    select(TaskImageBuildGrantEvent).where(
                        TaskImageBuildGrantEvent.grant_id == cancel_grant.grant_id,
                        TaskImageBuildGrantEvent.event_type == "cancellation_requested",
                    )
                )
            ).all()
        )
        assert len(cancellation_events) == 1
        assert cancellation_events[0].payload == {"job_ids": ["33333"]}


async def test_revoked_grant_cancels_a_late_live_job_without_becoming_bindable(
    grant_session: async_sessionmaker[AsyncSession],
) -> None:
    grant = _grant()
    async with grant_session() as session:
        await issue_task_image_build_grant(
            session,
            environment="staging",
            grant=grant,
            ambiguity_settle_seconds=1,
            now=_NOW,
        )
        await begin_task_image_build_submission(session, grant_id=grant.grant_id, now=_NOW)
        revoked = await reconcile_task_image_build_submission(
            session,
            grant_id=grant.grant_id,
            inventory=SlurmBuildInventoryV1(
                controller_authoritative=True,
                accounting_authoritative=True,
                observed_at=_NOW + timedelta(seconds=2),
                jobs=(),
            ),
            now=_NOW + timedelta(seconds=2),
        )
        assert revoked.action == "revoke"

        stale_cleanup = await reconcile_task_image_build_submission(
            session,
            grant_id=grant.grant_id,
            inventory=SlurmBuildInventoryV1(
                controller_authoritative=True,
                accounting_authoritative=True,
                observed_at=_NOW,
                jobs=(),
            ),
            now=_NOW + timedelta(seconds=2),
        )
        assert stale_cleanup.action == "wait"
        assert stale_cleanup.reason == "inventory_snapshot_precedes_revocation"

        late_inventory = _inventory(
            grant,
            job_id="44444",
            state="pending",
            held=True,
            observed_at=_NOW + timedelta(seconds=3),
        )
        cancel_first = await reconcile_task_image_build_submission(
            session,
            grant_id=grant.grant_id,
            inventory=late_inventory,
            now=_NOW + timedelta(seconds=3),
        )
        cancel_repeat = await reconcile_task_image_build_submission(
            session,
            grant_id=grant.grant_id,
            inventory=late_inventory,
            now=_NOW + timedelta(seconds=3),
        )
        terminal = await reconcile_task_image_build_submission(
            session,
            grant_id=grant.grant_id,
            inventory=_inventory(
                grant,
                job_id="44444",
                state="terminal",
                held=False,
                observed_at=_NOW + timedelta(seconds=4),
            ),
            now=_NOW + timedelta(seconds=4),
        )
        await session.commit()

        assert cancel_first == cancel_repeat
        assert cancel_first.action == "cancel_then_reconcile"
        assert cancel_first.cancel_job_ids == ("44444",)
        assert terminal.action == "revoke"
        row = await session.get(TaskImageBuildGrant, grant.grant_id)
        assert row is not None
        assert (row.state, row.slurm_job_id, row.released_at) == ("revoked", None, None)
        events = list(
            (
                await session.scalars(
                    select(TaskImageBuildGrantEvent)
                    .where(TaskImageBuildGrantEvent.grant_id == grant.grant_id)
                    .order_by(TaskImageBuildGrantEvent.sequence)
                )
            ).all()
        )
        assert [event.event_type for event in events].count("revoked") == 1
        assert [event.event_type for event in events].count("cancellation_requested") == 1
