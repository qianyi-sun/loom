"""Fresh typed demand reaches sealed allocation; runtime activation is separate."""

import json
from copy import deepcopy
from importlib import import_module

import pytest
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from loom_capacity_manager.allocator import allocate_shadow
from loom_capacity_manager.build_membership_contracts import ExecutionPreparationPolicyV4
from loom_capacity_manager.membership_launch_authority import resolve_allocation_launch_subject
from loom_capacity_manager.models import (
    CapacityAllocationEpoch,
    CapacityAuthorityState,
    CapacityExecutionEpoch,
)
from loom_capacity_manager.reconciler import _commit_reconciled_epoch
from loom_capacity_manager.store import (
    AuthorityRecoveryError,
    CapacityManagementStore,
    CapacityStoreError,
    ExecutionConflictError,
    WriterFence,
)
from tests.capacity_build_membership_fixtures import (
    application_request,
    build_request,
    typed_sql_execution,
)
from tests.capacity_execution_fixtures import execution_policy
from tests.capacity_fixtures import pool_observation
from tests.integration.test_capacity_mixed_membership_store import apply, transition
from tests.integration.test_capacity_typed_membership_demand import report


def typed_management(preparation):
    policy = ExecutionPreparationPolicyV4.model_validate(execution_policy().model_dump(mode="python") | {
        "schema_version": 4,
        **{name: getattr(preparation, name) for name in (
            "personal_membership", "personal_builds", "managed_application_origins",
            "managed_build_origins", "retired_source", "subject_acknowledgements", "executors",
        )},
    })
    return CapacityManagementStore(execution_policy=policy)


@pytest.mark.parametrize("tamper", ("none", "legacy-policy", "build-policy", "executor-policy"))
async def test_typed_allocation_authority_requires_exact_operator_and_executor_evidence(capacity_session, tamper):
    legacy, preparation, _fleet, execution = await typed_sql_execution(capacity_session)
    management = typed_management(preparation)
    if tamper == "legacy-policy":
        management = legacy
    elif tamper == "build-policy":
        management = typed_management(preparation.model_copy(update={
            "personal_builds": preparation.personal_builds.model_copy(update={"max_slots_per_subject": 1}),
        }))
    elif tamper == "executor-policy":
        management = typed_management(preparation.model_copy(update={
            "executors": tuple(executor.model_copy(update={"signing_key_sha256": "f" * 64}) for executor in preparation.executors),
        }))
    if tamper == "none":
        assert await management.execution_authority(capacity_session) == execution
    else:
        with pytest.raises(AuthorityRecoveryError):
            await management.execution_authority(capacity_session)


@pytest.mark.parametrize("frozen", (False, True))
async def test_typed_two_owner_demand_is_sealed_without_erasing_build_membership(isolated_capacity_postgres_url, frozen):
    module = import_module("loom_capacity_manager.membership_execution")
    engine = create_async_engine(isolated_capacity_postgres_url, isolation_level="SERIALIZABLE")
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    try:
        async with sessions() as session, session.begin():
            management, preparation, _fleet, execution = await typed_sql_execution(session)
            management = typed_management(preparation)
            # Complete the SQL-only active fixture; production activation still
            # owns unfreezing and remains disabled for V4 until runtime is wired.
            authority = await session.get(CapacityAuthorityState, 1)
            authority.increase_freeze = frozen
            authority.increase_freeze_reason = "test-freeze" if frozen else None
            builds = []
            for index, owner in enumerate((88010, 88011)):
                build = await apply(session, build_request(preparation, execution, owner=owner, revision=index * 2), key=111000 + index * 2)
                application = await apply(session, application_request(preparation, execution, owner=owner, revision=index * 2 + 1), key=111001 + index * 2)
                builds.append(build.member.configuration.subject_id)
                await management.ingest_demand_snapshot(session, report(application.member.configuration), actor="owner-agent")
            for pool in ("gb10", "oldlab"):
                await management.ingest_pool_observation(session, pool_observation(sequence=1, pool_id=pool), actor=f"{pool}-reporter")
        writer = WriterFence(authority_incarnation=execution.authority_incarnation, writer_epoch=execution.writer_epoch)
        async with sessions() as reader, reader.begin():
            value = await management.load_allocation_input(reader, writer)
        async with sessions() as committer:
            if frozen:
                with pytest.raises(CapacityStoreError, match="increases are frozen"):
                    await _commit_reconciled_epoch(committer, management, writer, allocate_shadow(value))
                return
            allocation_id, sealed = await _commit_reconciled_epoch(committer, management, writer, allocate_shadow(value))
        assert sealed.schema_version == 4
        assert sealed.membership == value.membership
        assert len(sealed.membership.members) == 4
        assert {member.purpose for member in sealed.membership.members} == {"personal-application", "personal-build-worker"}
        assert sealed.configuration == value.configuration
        assert any(allocation.desired_shapes for allocation in sealed.allocations)
        assert not any(allocation.desired_shapes for allocation in sealed.allocations if allocation.subject_id in builds)
        async with sessions() as reader:
            row = await reader.get(CapacityAllocationEpoch, allocation_id)
            assert row.sealed and row.executable
            assert module.parse_executable_epoch(json.dumps(row.complete_payload)) == sealed
            epoch = await reader.get(CapacityExecutionEpoch, execution.execution_epoch)
            for member in sealed.membership.members:
                resolved = await resolve_allocation_launch_subject(reader, epoch, row,
                    subject_id=member.configuration.subject_id, require_current=member.purpose == "personal-application")
                assert resolved.configuration == member.configuration
                assert resolved.acknowledgement == member.acknowledgement
                assert resolved.authority.purpose == ("application-worker" if member.purpose == "personal-application" else "personal-build-worker")
                assert resolved.authority.membership.owner_id == member.owner_id
                assert resolved.authority.membership.revision == member.revision
                if member.revision != sealed.membership.revision:
                    assert resolved.authority.membership.head_sha256 != sealed.membership.head_sha256
                if member.purpose == "personal-build-worker":
                    with pytest.raises(ExecutionConflictError):
                        await resolve_allocation_launch_subject(reader, epoch, row,
                            subject_id=member.configuration.subject_id, require_current=True)
            selected = sealed.membership.members[1]
            for tamper in ("snapshot-head", "legacy-downgrade"):
                changed = CapacityAllocationEpoch(**{column.key: getattr(row, column.key) for column in row.__table__.columns})
                changed.complete_payload = deepcopy(row.complete_payload)
                if tamper == "snapshot-head":
                    changed.complete_payload["membership"]["head_sha256"] = "f" * 64
                else:
                    changed.complete_payload["schema_version"] = 2
                    del changed.complete_payload["membership"]
                with pytest.raises(ExecutionConflictError):
                    await resolve_allocation_launch_subject(reader, epoch, changed,
                        subject_id=selected.configuration.subject_id, require_current=False)
            # A real generation rotation invalidates old launch authority but
            # preserves its exact purpose/event for accounting and cleanup.
            original = application_request(preparation, execution, owner=88010, revision=1)
            await apply(reader, transition(original, "update", revision=4), key=111010)
            with pytest.raises(ExecutionConflictError):
                await resolve_allocation_launch_subject(reader, epoch, row,
                    subject_id=selected.configuration.subject_id, require_current=True)
            historical = await resolve_allocation_launch_subject(reader, epoch, row,
                subject_id=selected.configuration.subject_id, require_current=False)
            assert historical.configuration == selected.configuration
            assert historical.authority.membership.revision == selected.revision
    finally:
        await engine.dispose()
