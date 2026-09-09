"""Durable typed history feeds accounting, never application-only launch paths."""

from uuid import UUID

import pytest
from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from loom_capacity_manager.allocator import allocate_shadow
from loom_capacity_manager.build_membership_contracts import DelegatedAllocationInputV3
from loom_capacity_manager.contracts import ObservedCommitmentV1
from loom_capacity_manager.membership import resolved_subject_references
from loom_capacity_manager.models import (
    CapacityAllocationEpoch,
    CapacityAuthorityState,
    CapacityCandidate,
    CapacityConfigGeneration,
    CapacityExecutionEpoch,
    CapacitySubject,
)
from loom_capacity_manager.reconciler import _commit_reconciled_epoch
from loom_capacity_manager.store import (
    CapacityStoreError,
    ConfigurationConflictError,
    ExecutionConflictError,
    StaleWriterError,
    WriterFence,
)
from loom_capacity_manager.typed_membership_store import CapacityTypedMembershipStore
from tests.capacity_build_membership_fixtures import build_request, typed_sql_execution
from tests.integration.test_capacity_typed_membership_store import _apply, _transition


async def test_typed_history_empty_and_two_owner_prefixes(capacity_session):
    _management, preparation, _fleet, execution = await typed_sql_execution(capacity_session)
    store = CapacityTypedMembershipStore()
    empty = await store.snapshot(capacity_session, execution.execution_epoch)
    assert empty.revision == 0 and empty.head_sha256 == "0" * 64 and empty.members == ()
    assert empty.namespace_id == preparation.personal_membership.namespace_id
    first_request = build_request(preparation, execution)
    first = await _apply(capacity_session, first_request)
    second = await _apply(capacity_session, build_request(preparation, execution, owner=88011, revision=1), key=92001)
    current = await store.snapshot(capacity_session, execution.execution_epoch)
    assert current.members == (first.member, second.member)
    assert (current.revision, current.head_sha256) == (2, second.head_sha256)
    historical = await store.snapshot(capacity_session, execution.execution_epoch, through_revision=1)
    assert historical.members == (first.member,)
    assert historical.head_sha256 == first.head_sha256
    assert await store.snapshot(capacity_session, execution.execution_epoch, through_revision=0) == empty


@pytest.mark.parametrize("revision", (-1, True, 0.0, 1, "0"))
async def test_typed_history_rejects_invalid_or_missing_revision(capacity_session, revision):
    _management, _preparation, _fleet, execution = await typed_sql_execution(capacity_session)
    with pytest.raises(ConfigurationConflictError):
        await CapacityTypedMembershipStore().snapshot(capacity_session, execution.execution_epoch, through_revision=revision)


@pytest.mark.parametrize("epoch", (999, 0, True))
async def test_typed_history_rejects_unavailable_epoch(capacity_session, epoch):
    with pytest.raises((ConfigurationConflictError, ExecutionConflictError)):
        await CapacityTypedMembershipStore().snapshot(capacity_session, epoch)


async def test_typed_history_keeps_original_prefix_after_rotation_and_destroy(capacity_session):
    _management, preparation, _fleet, execution = await typed_sql_execution(capacity_session)
    store = CapacityTypedMembershipStore()
    request = build_request(preparation, execution)
    created = await _apply(capacity_session, request)
    original = await store.snapshot(capacity_session, execution.execution_epoch)
    request = _transition(request, "update")
    await _apply(capacity_session, request, key=92001)
    request = _transition(request, "destroy")
    destroyed = await _apply(capacity_session, request, key=92002)
    assert await store.snapshot(capacity_session, execution.execution_epoch, through_revision=1) == original
    current = await store.snapshot(capacity_session, execution.execution_epoch)
    assert current.members == (destroyed.member,)
    assert current.members[0].configuration.lifecycle_state == "disabled"
    assert created.member.configuration.demand_reporter_incarnation != destroyed.member.configuration.demand_reporter_incarnation
    epoch = await capacity_session.get(CapacityExecutionEpoch, execution.execution_epoch)
    await store.verify_snapshot_materialization(capacity_session, epoch, current)
    with pytest.raises(ConfigurationConflictError):
        await store.verify_snapshot_materialization(capacity_session, epoch, original)


async def test_typed_history_refreshes_retained_candidate_for_historical_reads(capacity_session):
    _management, preparation, _fleet, execution = await typed_sql_execution(capacity_session)
    request = build_request(preparation, execution)
    first = await _apply(capacity_session, request)
    cached = (await capacity_session.scalars(select(CapacityCandidate).where(CapacityCandidate.subject_id == first.member.configuration.subject_id))).one()
    await _apply(capacity_session, _transition(request, "update"), key=92001)
    await capacity_session.execute(text("UPDATE capacity_candidates SET artifact_payload = jsonb_set(artifact_payload, '{runtime_candidate,schema_version}', '2.0') WHERE id=:id"), {"id": cached.id})
    with pytest.raises(ConfigurationConflictError):
        await CapacityTypedMembershipStore().snapshot(capacity_session, execution.execution_epoch, through_revision=1)


async def test_typed_allocation_input_preserves_base_two_owners_and_disabled_service(capacity_session):
    management, preparation, _fleet, execution = await typed_sql_execution(capacity_session)
    first_request = build_request(preparation, execution)
    first = await _apply(capacity_session, first_request)
    second = await _apply(capacity_session, build_request(preparation, execution, owner=88011, revision=1), key=92001)
    writer = WriterFence(authority_incarnation=execution.authority_incarnation, writer_epoch=execution.writer_epoch)
    value = await management.load_allocation_input(capacity_session, writer)
    assert isinstance(value, DelegatedAllocationInputV3)
    assert value.membership.members == (first.member, second.member)
    expected = {ref.subject_id for ref in value.configuration.subjects} | {first.member.configuration.subject_id, second.member.configuration.subject_id}
    assert {ref.subject_id for ref in resolved_subject_references(value)} == expected
    assert {item.configuration.subject_id for item in value.subjects} == expected
    assert len(value.configuration.subjects) > 0
    assert not allocate_shadow(value).executable
    destroyed_request = _transition(first_request, "destroy").model_copy(update={"expected_revision": 2})
    destroyed = await _apply(capacity_session, destroyed_request, key=92002)
    after = await management.load_allocation_input(capacity_session, writer)
    assert isinstance(after, DelegatedAllocationInputV3)
    assert destroyed.member in after.membership.members
    assert {item.configuration.subject_id for item in after.subjects} == expected


async def test_typed_allocation_input_rejects_materialized_configuration_tamper(capacity_session):
    management, preparation, _fleet, execution = await typed_sql_execution(capacity_session)
    first = await _apply(capacity_session, build_request(preparation, execution))
    row = (await capacity_session.scalars(select(CapacitySubject).where(CapacitySubject.subject_id == first.member.configuration.subject_id))).one()
    row.max_slots = 1
    await capacity_session.flush()
    with pytest.raises(ConfigurationConflictError):
        await management.load_allocation_input(capacity_session, WriterFence(authority_incarnation=execution.authority_incarnation, writer_epoch=execution.writer_epoch))


async def test_typed_empty_history_rejects_changed_cached_fleet_document(capacity_session):
    _management, _preparation, _fleet, execution = await typed_sql_execution(capacity_session)
    cached = (await capacity_session.scalars(select(CapacityConfigGeneration).where(CapacityConfigGeneration.scope == "fleet"))).one()
    await capacity_session.execute(text("UPDATE capacity_config_generations SET payload=jsonb_set(payload,'{fleet_generation}','99') WHERE id=:id"), {"id": cached.id})
    with pytest.raises(ConfigurationConflictError):
        await CapacityTypedMembershipStore().snapshot(capacity_session, execution.execution_epoch)


async def test_typed_teardown_keeps_old_generation_physical_charge_in_allocation_input(capacity_session):
    management, preparation, _fleet, execution = await typed_sql_execution(capacity_session)
    request = build_request(preparation, execution)
    created = await _apply(capacity_session, request)
    subject = created.member.configuration
    profile = subject.profiles[0]
    shape = profile.worker_shapes[0]
    physical = ObservedCommitmentV1(kind="physical", commitment_id="build-job-before-update",
        physical_identity="build-job-before-update", subject_id=subject.subject_id,
        subject_incarnation=subject.subject_incarnation, pool_id=profile.pool_id,
        pool_generation=profile.pool_generation, deployment_generation=subject.deployment_generation,
        profile_id=shape.shape_id, profile_generation=profile.profile_generation,
        profile_digest=profile.profile_digest, shape_id=shape.shape_id, resources=shape.total_resources,
        state="live")
    await management._upsert_commitment(capacity_session, kind="physical",
        source_incarnation=UUID(int=99100), sequence=1, observed=physical,
        now=await capacity_session.scalar(select(func.now())))
    request = _transition(request, "update")
    await _apply(capacity_session, request, key=92001)
    await _apply(capacity_session, _transition(request, "destroy"), key=92002)
    value = await management.load_allocation_input(capacity_session,
        WriterFence(authority_incarnation=execution.authority_incarnation, writer_epoch=execution.writer_epoch))
    assert physical in value.observed_commitments
    assert value.membership.members[0].configuration.lifecycle_state == "disabled"
    shadow = allocate_shadow(value)
    assert "build-job-before-update" in value.observed_commitment_ids
    assert not shadow.executable


async def test_typed_durable_input_cannot_be_committed_as_legacy_executable(isolated_capacity_postgres_url):
    engine = create_async_engine(isolated_capacity_postgres_url, isolation_level="SERIALIZABLE")
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    try:
        async with sessions() as session, session.begin():
            management, preparation, _fleet, execution = await typed_sql_execution(session)
            await _apply(session, build_request(preparation, execution))
            writer = WriterFence(authority_incarnation=execution.authority_incarnation, writer_epoch=execution.writer_epoch)
            value = await management.load_allocation_input(session, writer)
            shadow = allocate_shadow(value)
        async with sessions() as session:
            with pytest.raises(CapacityStoreError, match="unsupported executable allocation input"):
                await _commit_reconciled_epoch(session, management, writer, shadow)
        async with sessions() as session:
            assert await session.scalar(select(func.count()).select_from(CapacityAllocationEpoch)) == 0
    finally:
        await engine.dispose()


async def test_typed_allocation_read_refreshes_writer_fence_across_sessions(isolated_capacity_postgres_url):
    engine = create_async_engine(isolated_capacity_postgres_url, isolation_level="SERIALIZABLE")
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    try:
        async with sessions() as session, session.begin():
            management, _preparation, _fleet, execution = await typed_sql_execution(session)
        writer = WriterFence(authority_incarnation=execution.authority_incarnation, writer_epoch=execution.writer_epoch)
        async with sessions() as reader:
            async with reader.begin():
                cached = await reader.get(CapacityAuthorityState, 1)
                await management.load_allocation_input(reader, writer)
            async with sessions() as successor:
                replacement = await management.register_writer(successor, writer.authority_incarnation, expected_epoch=writer.writer_epoch)
            assert cached.writer_epoch != replacement.writer_epoch
            with pytest.raises(StaleWriterError):
                await management.load_allocation_input(reader, writer)
    finally:
        await engine.dispose()
