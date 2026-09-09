"""Retired typed applications become a complete immutable base, not installations."""

from uuid import UUID

import pytest
from sqlalchemy import func, select, text

from loom_capacity_manager.models import CapacityCandidate, CapacityConfigurationEpoch
from loom_capacity_manager.store import ConfigurationConflictError, ExecutionConflictError
from loom_capacity_manager.typed_membership_store import CapacityTypedMembershipStore
from tests.capacity_build_membership_fixtures import (
    application_request,
    build_request,
    managed_application_request,
    typed_sql_execution,
)
from tests.integration.test_capacity_mixed_membership_store import apply
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
    with pytest.raises(ExecutionConflictError):
        await import_apps(capacity_session, management, execution, snapshot)
    await retire(capacity_session, management, preparation, execution)
    with pytest.raises(ConfigurationConflictError, match="build"):
        await import_apps(capacity_session, management, execution, snapshot)
