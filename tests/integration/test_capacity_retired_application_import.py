"""Retired typed applications become a complete immutable base, not installations."""

from uuid import UUID

import pytest
from sqlalchemy import func, select, text

from loom_capacity_manager.models import (
    CapacityCandidate,
    CapacityConfigGeneration,
    CapacityConfigurationEpoch,
)
from loom_capacity_manager.store import AuthorityRecoveryError, ConfigurationConflictError
from loom_capacity_manager.typed_membership_store import CapacityTypedMembershipStore
from tests.capacity_build_membership_fixtures import (
    application_request,
    build_request,
    managed_application_request,
    typed_sql_execution,
)
from tests.integration.test_capacity_mixed_membership_store import apply, transition
from tests.integration.test_capacity_typed_managed_base_history import prepared


async def retire(session, management, preparation, execution):
    from loom_capacity_manager.executable_contracts import ExecutableExecutorHeartbeatV2
    from loom_capacity_manager.execution_store import CapacityExecutionStore
    from tests.integration.test_capacity_manager_execution_epoch import (
        _drain_request,
        _publish_final_safe_evidence,
        _retirement_request,
    )

    store = CapacityExecutionStore()
    for binding in preparation.executors:
        await store.heartbeat_executor(session, ExecutableExecutorHeartbeatV2(
            execution=execution, executor_id=binding.executor_id, executor_incarnation=binding.executor_incarnation,
            pool_id=binding.pool_id, pool_generation=binding.pool_generation, heartbeat_sequence=1,
            journal_sequence=0, journal_digest="0" * 64))
    drained = await management.begin_execution_drain(session, _drain_request(execution),
        actor="retirement-operator", idempotency_key=UUID(int=99001))
    checkpoints = await _publish_final_safe_evidence(session, drained)
    await management.retire_execution_epoch(session, _retirement_request(drained, checkpoints),
        actor="retirement-operator", idempotency_key=UUID(int=99002))


async def import_apps(session, management, execution, snapshot, *, key=99003):
    from loom_capacity_manager.retired_application_import import import_retired_applications

    return await import_retired_applications(session, management, execution_epoch=execution.execution_epoch,
        expected_snapshot=snapshot, actor="configuration-operator", idempotency_key=UUID(int=key))


@pytest.mark.parametrize("initial", ("fresh", "managed-capacity", "managed-update"))
async def test_import_reuses_retained_generations_and_preserves_all_applications(capacity_session, initial):
    if initial == "fresh":
        management, preparation, _fleet, execution = await typed_sql_execution(capacity_session)
        request = application_request(preparation, execution)
    else:
        management, preparation, _fleet, execution = await prepared(capacity_session)
        request = managed_application_request(preparation, execution,
            operation="update" if initial == "managed-update" else "capacity")
    first = await apply(capacity_session, request)
    second = await apply(capacity_session, application_request(preparation, execution, owner=88011, revision=1), key=99004)
    snapshot = await CapacityTypedMembershipStore().snapshot(capacity_session, execution.execution_epoch)
    candidates = {row.id: row.attestation_payload for row in (await capacity_session.scalars(select(CapacityCandidate))).all()}
    await retire(capacity_session, management, preparation, execution)
    imported = await import_apps(capacity_session, management, execution, snapshot)
    assert imported.configuration.configuration_epoch == execution.configuration_epoch + 1
    assert {origin.configuration.subject_id: origin.configuration for origin in imported.origins} == {
        member.configuration.subject_id: member.configuration for member in (first.member, second.member)}
    assert len(imported.configuration.snapshot.subjects) == len(preparation.subject_acknowledgements) + (2 if initial == "fresh" else 1)
    assert {row.id: row.attestation_payload for row in (await capacity_session.scalars(select(CapacityCandidate))).all()} == candidates
    assert await import_apps(capacity_session, management, execution, snapshot) == imported
    assert await capacity_session.scalar(select(func.max(CapacityConfigurationEpoch.configuration_epoch))) == imported.configuration.configuration_epoch
    assert await CapacityTypedMembershipStore().snapshot(capacity_session, execution.execution_epoch) == snapshot
    from loom_capacity_manager.build_membership_contracts import ExecutionPreparationV4
    from loom_capacity_manager.store import CapacityManagementStore, WriterFence
    from tests.capacity_build_membership_fixtures import seed_typed_sql_execution
    from tests.capacity_execution_fixtures import PreparedExecutionFixture

    managed_ids = tuple(origin.configuration.subject_id for origin in imported.origins)
    successor = ExecutionPreparationV4.model_validate(preparation.model_dump(mode="python") | {
        "configuration_epoch": imported.configuration.configuration_epoch,
        "managed_application_origins": imported.origins,
        "personal_membership": preparation.personal_membership.model_copy(update={"managed_base_subject_ids": managed_ids}),
        "subject_acknowledgements": tuple(ack for ack in preparation.subject_acknowledgements if ack.subject_id not in managed_ids)
            + tuple(origin.acknowledgement for origin in imported.origins),
        "executors": tuple(binding.model_copy(update={"executor_incarnation": UUID(int=99100 + index)})
            for index, binding in enumerate(preparation.executors)),
    })
    manager = CapacityManagementStore(execution_policy=management.execution_policy.model_copy(update={"executors": successor.executors}))
    writer = WriterFence(authority_incarnation=execution.authority_incarnation, writer_epoch=execution.writer_epoch)
    fixture = PreparedExecutionFixture(store=manager, writer=writer, request=successor)
    active = await seed_typed_sql_execution(capacity_session, fixture, successor, execution_epoch=43)
    resized_request = managed_application_request(successor, active, subject_id=first.member.configuration.subject_id)
    await apply(capacity_session, resized_request, key=99006)
    updated = await apply(capacity_session, transition(resized_request, "update", revision=1), key=99007)
    current = await manager.load_allocation_input(capacity_session, writer)
    assert sum(item.configuration.subject_id == updated.member.configuration.subject_id for item in current.subjects) == 1
    assert await CapacityTypedMembershipStore().snapshot(capacity_session, execution.execution_epoch) == snapshot
    with pytest.raises(AuthorityRecoveryError):
        await import_apps(capacity_session, manager, execution, snapshot)


@pytest.mark.parametrize("corruption", ("head", "reporter", "installation", "materialization"))
async def test_import_rejects_changed_source_without_partial_configuration(capacity_session, corruption):
    management, preparation, _fleet, execution = await typed_sql_execution(capacity_session)
    request = application_request(preparation, execution)
    created = await apply(capacity_session, request)
    snapshot = await CapacityTypedMembershipStore().snapshot(capacity_session, execution.execution_epoch)
    await retire(capacity_session, management, preparation, execution)
    if corruption == "head":
        snapshot = snapshot.model_copy(update={"head_sha256": "f" * 64})
    else:
        statements = {
            "reporter": "UPDATE capacity_demand_reporters SET token_sha256=repeat('f',64) WHERE subject_id=:subject",
            "installation": "UPDATE capacity_candidates SET attestation_payload='{}'::jsonb WHERE subject_id=:subject",
            "materialization": "UPDATE capacity_subjects SET max_slots=99 WHERE subject_id=:subject",
        }
        await capacity_session.execute(text(statements[corruption]), {"subject": created.member.configuration.subject_id})
    with pytest.raises(ConfigurationConflictError):
        await import_apps(capacity_session, management, execution, snapshot)
    assert await capacity_session.scalar(select(func.max(CapacityConfigurationEpoch.configuration_epoch))) == execution.configuration_epoch


async def test_import_cannot_skip_retirement_or_discard_build_members(capacity_session):
    management, preparation, _fleet, execution = await typed_sql_execution(capacity_session)
    request = build_request(preparation, execution)
    await apply(capacity_session, request)
    snapshot = await CapacityTypedMembershipStore().snapshot(capacity_session, execution.execution_epoch)
    with pytest.raises(AuthorityRecoveryError):
        await import_apps(capacity_session, management, execution, snapshot)
    await retire(capacity_session, management, preparation, execution)
    with pytest.raises(ConfigurationConflictError, match="build"):
        await import_apps(capacity_session, management, execution, snapshot)


async def test_import_preserves_disabled_application_and_unmodified_managed_origin(capacity_session):
    from loom_capacity_manager.models import CapacityPoolReporter
    from tests.capacity_fixtures import pool_observation

    management, preparation, _fleet, execution = await prepared(capacity_session)
    for pool_id in ("gb10", "oldlab"):
        await management.ingest_pool_observation(capacity_session, pool_observation(sequence=1, pool_id=pool_id), actor=f"{pool_id}-reporter")
    pool_reporters = {row.id: (row.high_water, row.last_digest, row.last_receipt_time)
        for row in (await capacity_session.scalars(select(CapacityPoolReporter))).all()}
    assert all(value[0] == 1 for value in pool_reporters.values())
    request = application_request(preparation, execution, owner=88011)
    await apply(capacity_session, request)
    disabled = await apply(capacity_session, transition(request, "destroy", revision=1), key=99004)
    snapshot = await CapacityTypedMembershipStore().snapshot(capacity_session, execution.execution_epoch)
    await retire(capacity_session, management, preparation, execution)
    imported = await import_apps(capacity_session, management, execution, snapshot)
    origins = {origin.configuration.subject_id: origin for origin in imported.origins}
    assert origins[disabled.member.configuration.subject_id].configuration == disabled.member.configuration
    original = preparation.managed_application_origins[0]
    assert origins[original.configuration.subject_id] == original
    assert len(imported.configuration.snapshot.subjects) == len(preparation.subject_acknowledgements) + 1
    assert {row.id: (row.high_water, row.last_digest, row.last_receipt_time)
        for row in (await capacity_session.scalars(select(CapacityPoolReporter).execution_options(populate_existing=True))).all()} == pool_reporters


async def test_import_rolls_back_first_proposal_when_later_generation_conflicts(capacity_session):
    management, preparation, _fleet, execution = await typed_sql_execution(capacity_session)
    request = application_request(preparation, execution)
    await apply(capacity_session, request)
    await apply(capacity_session, application_request(preparation, execution, owner=88011, revision=1), key=99004)
    snapshot = await CapacityTypedMembershipStore().snapshot(capacity_session, execution.execution_epoch)
    await retire(capacity_session, management, preparation, execution)
    # Corrupt the final member's existing generation, after a fresh proposal would
    # otherwise be added. All proposals and activation must roll back together.
    from loom_capacity_manager.contracts import canonical_digest

    subject = snapshot.members[1].configuration
    wrong = subject.model_copy(update={"max_slots": 99})
    capacity_session.add(CapacityConfigGeneration(scope="subject", subject_id=subject.subject_id,
        subject_incarnation=subject.subject_incarnation, scope_generation=subject.configuration_generation,
        digest=canonical_digest(wrong), payload=wrong.model_dump(mode="json"), state="proposed",
        actor="conflicting-operator", idempotency_key=UUID(int=99005)))
    await capacity_session.flush()
    count = await capacity_session.scalar(select(func.count()).select_from(CapacityConfigGeneration))
    with pytest.raises(ConfigurationConflictError):
        await import_apps(capacity_session, management, execution, snapshot)
    assert await capacity_session.scalar(select(func.count()).select_from(CapacityConfigGeneration)) == count
    assert await capacity_session.scalar(select(func.max(CapacityConfigurationEpoch.configuration_epoch))) == execution.configuration_epoch


@pytest.mark.parametrize("change", ("retirement", "aborted", "snapshot-member", "static-profile", "static-reporter", "static-candidate", "pool-reporter-missing", "pool-reporter-generation"))
async def test_import_checks_retirement_and_complete_static_evidence(capacity_session, change):
    from loom_capacity_manager.store import ExecutionConflictError

    management, preparation, _fleet, execution = await typed_sql_execution(capacity_session)
    await apply(capacity_session, application_request(preparation, execution))
    snapshot = await CapacityTypedMembershipStore().snapshot(capacity_session, execution.execution_epoch)
    await retire(capacity_session, management, preparation, execution)
    if change == "snapshot-member":
        member = snapshot.members[0]
        snapshot = snapshot.model_copy(update={"members": (member.model_copy(update={"configuration":
            member.configuration.model_copy(update={"max_slots": 99})}),)})
    else:
        statements = {
            "retirement": "UPDATE capacity_execution_epochs SET retirement_request_digest=repeat('f',64) WHERE execution_epoch=:epoch",
            "aborted": "UPDATE capacity_execution_epochs SET activated_at=NULL WHERE execution_epoch=:epoch",
            "static-profile": "UPDATE capacity_worker_profiles SET profile_generation=99 WHERE subject_id=:subject",
            "static-reporter": "UPDATE capacity_demand_reporters SET configuration_generation=99 WHERE subject_id=:subject",
            "static-candidate": "UPDATE capacity_candidates SET candidate_identity=repeat('f',64) WHERE subject_id=:subject",
            "pool-reporter-missing": "DELETE FROM capacity_pool_reporters WHERE pool_id='gb10'",
            "pool-reporter-generation": "UPDATE capacity_pool_reporters SET pool_generation=99 WHERE pool_id='gb10'",
        }
        values = {"epoch": execution.execution_epoch, "subject": preparation.subject_acknowledgements[0].subject_id}
        if change in {"retirement", "aborted"}:
            from sqlalchemy.exc import DBAPIError

            from loom_capacity_manager.models import CapacityExecutionEpoch
            from loom_capacity_manager.retired_application_import import _require_retired

            with pytest.raises(DBAPIError) as error:
                async with capacity_session.begin_nested():
                    await capacity_session.execute(text(statements[change]), values)
            assert error.value.orig.sqlstate == "23514"
            retained = await capacity_session.get(CapacityExecutionEpoch, execution.execution_epoch)
            copied = CapacityExecutionEpoch(**{column.key: getattr(retained, column.key) for column in retained.__table__.columns})
            if change == "retirement":
                copied.retirement_request_digest = "f" * 64
            else:
                copied.activated_at = None
            with pytest.raises(ExecutionConflictError):
                _require_retired(copied)
            return
        await capacity_session.execute(text(statements[change]), values)
    error = ExecutionConflictError if change in {"retirement", "aborted"} else ConfigurationConflictError
    with pytest.raises(error):
        await import_apps(capacity_session, management, execution, snapshot)


async def test_import_refreshes_cached_shadow_authority_after_concurrent_preparation(isolated_capacity_postgres_url):
    from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

    from loom_capacity_manager.models import CapacityAuthorityState
    from loom_capacity_manager.store import CapacityManagementStore, WriterFence
    from tests.capacity_build_membership_fixtures import seed_typed_sql_execution
    from tests.capacity_execution_fixtures import PreparedExecutionFixture

    engine = create_async_engine(isolated_capacity_postgres_url, isolation_level="SERIALIZABLE")
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    try:
        async with sessions() as stale:
            async with stale.begin():
                management, preparation, _fleet, execution = await typed_sql_execution(stale)
                await apply(stale, application_request(preparation, execution))
                snapshot = await CapacityTypedMembershipStore().snapshot(stale, execution.execution_epoch)
                await retire(stale, management, preparation, execution)
                cached = (await stale.scalars(select(CapacityAuthorityState))).one()
                assert cached.execution_state == "shadow"
            successor = preparation.model_copy(update={"executors": tuple(
                binding.model_copy(update={"executor_incarnation": UUID(int=99100 + index)})
                for index, binding in enumerate(preparation.executors))})
            manager = CapacityManagementStore(execution_policy=management.execution_policy.model_copy(update={"executors": successor.executors}))
            fixture = PreparedExecutionFixture(store=manager, request=successor,
                writer=WriterFence(authority_incarnation=execution.authority_incarnation, writer_epoch=execution.writer_epoch))
            async with sessions() as concurrent, concurrent.begin():
                await seed_typed_sql_execution(concurrent, fixture, successor, execution_epoch=43, activate=False)
            assert cached.execution_state == "shadow"
            with pytest.raises(AuthorityRecoveryError):
                await import_apps(stale, manager, execution, snapshot)
            async with sessions() as readback:
                current = (await readback.scalars(select(CapacityAuthorityState))).one()
                assert current.execution_state == "prepared" and current.execution_epoch == 43
                assert await readback.scalar(select(func.max(CapacityConfigurationEpoch.configuration_epoch))) == execution.configuration_epoch
    finally:
        await engine.dispose()
