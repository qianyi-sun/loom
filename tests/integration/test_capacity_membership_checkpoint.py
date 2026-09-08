"""Authenticated current membership checkpoints and typed revision conflicts."""

from uuid import UUID

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from loom_capacity_manager.executable_contracts import ExecutionDrainV2
from loom_capacity_manager.membership_store import CapacityMembershipStore
from loom_capacity_manager.store import ConfigurationConflictError, ExecutionConflictError
from tests.integration.test_capacity_membership import DELEGATE, _active_v3, _request


async def test_checkpoint_tracks_current_membership_without_changing_execution(
    capacity_session: AsyncSession,
) -> None:
    fixture, active = await _active_v3(capacity_session)
    membership = CapacityMembershipStore(fixture.store)
    empty = await membership.checkpoint(capacity_session, actor=DELEGATE)
    assert empty.execution == active
    assert empty.revision == 0 and empty.head_sha256 == "0" * 64
    request = _request(active)
    created = await membership.apply(
        capacity_session, request, actor=DELEGATE, idempotency_key=UUID(int=22000)
    )
    current = await membership.checkpoint(capacity_session, actor=DELEGATE)
    assert current.execution == active
    assert current.namespace_id == request.namespace_id
    assert (current.revision, current.head_sha256) == (created.revision, created.head_sha256)


async def test_checkpoint_rejects_an_unprepared_principal(capacity_session: AsyncSession) -> None:
    fixture, _ = await _active_v3(capacity_session)
    with pytest.raises(ExecutionConflictError):
        await CapacityMembershipStore(fixture.store).checkpoint(
            capacity_session, actor="another-management-principal"
        )


async def test_membership_revision_conflict_is_typed(capacity_session: AsyncSession) -> None:
    from loom_capacity_manager.membership_store import PersonalMembershipRevisionConflictError

    fixture, active = await _active_v3(capacity_session)
    membership = CapacityMembershipStore(fixture.store)
    with pytest.raises(PersonalMembershipRevisionConflictError):
        await membership.apply(
            capacity_session,
            _request(active, expected_revision=1),
            actor=DELEGATE,
            idempotency_key=UUID(int=22001),
        )


async def test_membership_checkpoint_does_not_present_drain_as_active(
    capacity_session: AsyncSession,
) -> None:
    fixture, active = await _active_v3(capacity_session)
    await fixture.store.begin_execution_drain(
        capacity_session,
        ExecutionDrainV2(
            authority_incarnation=active.authority_incarnation,
            expected_writer_epoch=active.writer_epoch,
            execution_epoch=active.execution_epoch,
            execution_manifest_sha256=active.execution_manifest_sha256,
            expected_executable_new_capacity_ceiling=active.executable_new_capacity_ceiling,
            expected_executable_new_capacity_rate_per_minute=active.executable_new_capacity_rate_per_minute,
        ),
        actor="drain-operator",
        idempotency_key=UUID(int=22002),
    )
    with pytest.raises(ExecutionConflictError):
        await CapacityMembershipStore(fixture.store).checkpoint(capacity_session, actor=DELEGATE)


async def test_historical_membership_snapshot_requires_an_exact_existing_revision(
    capacity_session: AsyncSession,
) -> None:
    fixture, active = await _active_v3(capacity_session)
    membership = CapacityMembershipStore(fixture.store)
    request = _request(active)
    await membership.apply(
        capacity_session, request, actor=DELEGATE, idempotency_key=UUID(int=22010)
    )
    original = await membership.snapshot(capacity_session, active.execution_epoch)
    projection = request.projection.model_copy(
        update={
            "operation_kind": "capacity",
            "operation_epoch": 2,
            "operation_id": UUID(int=22011),
            "configuration_generation": 2,
            "min_slots": 1,
        }
    )
    await membership.apply(
        capacity_session,
        _request(active, projection, expected_revision=1),
        actor=DELEGATE,
        idempotency_key=UUID(int=22012),
    )
    assert (
        await membership.snapshot(capacity_session, active.execution_epoch, through_revision=1)
        == original
    )
    empty = await membership.snapshot(capacity_session, active.execution_epoch, through_revision=0)
    assert empty.revision == 0 and empty.members == () and empty.head_sha256 == "0" * 64
    for invalid in (-1, 3, True):
        with pytest.raises(ConfigurationConflictError):
            await membership.snapshot(
                capacity_session, active.execution_epoch, through_revision=invalid
            )
