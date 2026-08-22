from __future__ import annotations

from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta
from uuid import UUID, uuid4

import pytest
from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from loom.db.schema import TaskImageBuildGrant, TaskImageBuildGrantEvent
from loom_control_plane.task_image_build_environment import (
    RootlessBuildResourceRequestV1,
    SlurmBuildEnvironmentPolicyV1,
    SlurmBuildInventoryV1,
    SlurmBuildJobObservationV1,
    issue_slurm_build_grant,
)
from loom_control_plane.task_image_build_grants import (
    TaskImageBuildGrantConflictError,
    begin_task_image_build_submission,
    issue_task_image_build_grant,
    reconcile_task_image_build_submission,
    record_task_image_build_release,
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


def _grant(grant_id: UUID | None = None):
    return issue_slurm_build_grant(_policy(), grant_id=grant_id or uuid4())


def _inventory(grant, *, job_id: str, state: str, held: bool):
    return SlurmBuildInventoryV1(
        controller_authoritative=True,
        accounting_authoritative=True,
        observed_at=_NOW,
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
                observed_at=_NOW,
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
