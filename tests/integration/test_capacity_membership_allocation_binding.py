"""Exact historical allocation generations and per-owner increase fences."""

from importlib import import_module
from uuid import UUID

import pytest
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from loom_capacity_manager.allocator import allocate_shadow
from loom_capacity_manager.contracts import canonical_digest
from loom_capacity_manager.executable_contracts import canonical_executable_digest
from loom_capacity_manager.execution_store import CapacityExecutionStore
from loom_capacity_manager.membership_store import CapacityMembershipStore
from loom_capacity_manager.models import (
    CapacityAllocation,
    CapacityAllocationEpoch,
    CapacityDemandReporter,
    CapacityExecutionEpoch,
    CapacitySubject,
)
from loom_capacity_manager.reconciler import _commit_reconciled_epoch
from loom_capacity_manager.store import ExecutionConflictError
from tests.capacity_fixtures import demand_snapshot, pool_observation
from tests.integration.test_capacity_membership import DELEGATE, _active_v3, _projection, _request


@pytest.fixture
async def pinned_personal_allocation(isolated_capacity_postgres_url: str):  # type: ignore[no-untyped-def]
    engine = create_async_engine(isolated_capacity_postgres_url, isolation_level="SERIALIZABLE")
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    try:
        async with sessions() as session, session.begin():
            fixture, active = await _active_v3(session)
            request = _request(active)
            await CapacityMembershipStore(fixture.store).apply(
                session, request, actor=DELEGATE, idempotency_key=UUID(int=22300)
            )
            await fixture.store.ingest_demand_snapshot(
                session,
                demand_snapshot(sequence=1, pending_attempt_ids=("base-attempt",)),
                actor="development",
            )
            for pool in ("gb10", "oldlab"):
                await fixture.store.ingest_pool_observation(
                    session, pool_observation(sequence=1, pool_id=pool), actor=f"{pool}-reporter"
                )
        async with sessions() as session, session.begin():
            value = await fixture.store.load_allocation_input(session, fixture.writer)
        async with sessions() as session:
            allocation_id, _ = await _commit_reconciled_epoch(
                session, fixture.store, fixture.writer, allocate_shadow(value)
            )
        yield sessions, fixture, active, request, allocation_id
    finally:
        await engine.dispose()


async def test_pinned_generation_fences_capacity_change_but_not_other_subject(
    pinned_personal_allocation,  # type: ignore[no-untyped-def]
):
    module = import_module("loom_capacity_manager.membership_execution_store")
    sessions, fixture, active, request, allocation_id = pinned_personal_allocation
    async with sessions() as session, session.begin():
        epoch = await session.get(CapacityExecutionEpoch, active.execution_epoch)
        allocation = await session.get(CapacityAllocationEpoch, allocation_id)
        original = await module.resolve_allocation_subject(
            session,
            epoch,
            allocation,
            subject_id=request.projection.subject_id,
            require_current=True,
        )
        assert original[1] == request.acknowledgement
        base_id = fixture.request.subject_acknowledgements[0].subject_id
        base = await module.resolve_allocation_subject(
            session,
            epoch,
            allocation,
            subject_id=base_id,
            require_current=True,
        )
        changed = request.projection.model_copy(
            update={
                "operation_kind": "capacity",
                "operation_epoch": 2,
                "operation_id": UUID(int=22301),
                "configuration_generation": 2,
                "max_slots": 1,
            }
        )
        await CapacityMembershipStore(fixture.store).apply(
            session,
            _request(active, changed, expected_revision=1),
            actor=DELEGATE,
            idempotency_key=UUID(int=22302),
        )
        with pytest.raises(ExecutionConflictError, match="generation"):
            await module.resolve_allocation_subject(
                session,
                epoch,
                allocation,
                subject_id=request.projection.subject_id,
                require_current=True,
            )
        assert (
            await module.resolve_allocation_subject(
                session,
                epoch,
                allocation,
                subject_id=request.projection.subject_id,
                require_current=False,
            )
            == original
        )
        assert (
            await module.resolve_allocation_subject(
                session,
                epoch,
                allocation,
                subject_id=base_id,
                require_current=True,
            )
            == base
        )


@pytest.mark.parametrize("tamper", ("missing", "head", "revision", "input", "writer", "base"))
async def test_allocation_reader_rejects_missing_or_changed_membership_evidence(
    pinned_personal_allocation,
    tamper: str,  # type: ignore[no-untyped-def]
):
    module = import_module("loom_capacity_manager.membership_execution_store")
    sessions, _, active, request, allocation_id = pinned_personal_allocation
    async with sessions() as session:
        epoch = await session.get(CapacityExecutionEpoch, active.execution_epoch)
        allocation = await session.scalar(
            select(CapacityAllocationEpoch).where(
                CapacityAllocationEpoch.allocation_epoch == allocation_id,
            )
        )
        payload = allocation.complete_payload.copy()
        if tamper == "missing":
            payload.pop("membership")
            payload["schema_version"] = 2
        elif tamper in {"head", "revision"}:
            membership = payload["membership"].copy()
            membership["head_sha256" if tamper == "head" else "revision"] = (
                "f" * 64 if tamper == "head" else 2
            )
            payload["membership"] = membership
        elif tamper == "base":
            payload["configuration"] = payload["configuration"] | {"subjects": []}
        elif tamper == "writer":
            payload["execution"] = payload["execution"] | {"writer_epoch": 99}
        else:
            payload["input_digest"] = "f" * 64
        # Exercise the reader's defense independently of immutable SQL write guards.
        session.expunge(allocation)
        allocation.complete_payload = payload
        with pytest.raises(ExecutionConflictError):
            await module.resolve_allocation_subject(
                session,
                epoch,
                allocation,
                subject_id=request.projection.subject_id,
                require_current=False,
            )


@pytest.mark.parametrize("state", ("active", "drain-only"))
async def test_currentness_rejects_manifest_downgrade_before_version_dispatch(
    pinned_personal_allocation,
    state: str,
):
    sessions, _, active, request, allocation_id = pinned_personal_allocation
    async with sessions() as session:
        epoch = await session.get(CapacityExecutionEpoch, active.execution_epoch)
        allocation = await session.get(CapacityAllocationEpoch, allocation_id)
        assert epoch is not None and allocation is not None
        assert await CapacityExecutionStore._membership_target_current(
            session,
            epoch,
            allocation,
            subject_id=request.projection.subject_id,
        )
        # Isolate the reader's authentication from immutable database write guards.
        session.expunge(epoch)
        payload = dict(epoch.manifest_payload)
        payload.pop("personal_membership")
        payload["schema_version"] = 2
        epoch.manifest_payload = payload
        epoch.state = state
        with pytest.raises(ExecutionConflictError, match="manifest digest"):
            await CapacityExecutionStore._membership_target_current(
                session,
                epoch,
                allocation,
                subject_id=request.projection.subject_id,
            )


async def test_historical_allocation_resolves_exact_member_and_base(
    pinned_personal_allocation,  # type: ignore[no-untyped-def]
):
    module = import_module("loom_capacity_manager.membership_execution_store")
    sessions, fixture, active, request, allocation_id = pinned_personal_allocation
    async with sessions() as session:
        epoch = await session.get(CapacityExecutionEpoch, active.execution_epoch)
        allocation = await session.get(CapacityAllocationEpoch, allocation_id)
        _, acknowledgement = await module.resolve_allocation_subject(
            session,
            epoch,
            allocation,
            subject_id=request.projection.subject_id,
            require_current=False,
        )
        assert acknowledgement == request.acknowledgement
        base_ack = fixture.request.subject_acknowledgements[0]
        _, acknowledgement = await module.resolve_allocation_subject(
            session,
            epoch,
            allocation,
            subject_id=base_ack.subject_id,
            require_current=False,
        )
        assert acknowledgement == base_ack


async def test_executor_current_subject_rejects_same_deployment_capacity_supersession(
    pinned_personal_allocation,  # type: ignore[no-untyped-def]
):
    sessions, fixture, active, request, allocation_id = pinned_personal_allocation
    async with sessions() as session, session.begin():
        epoch = await session.get(CapacityExecutionEpoch, active.execution_epoch)
        allocation = await session.scalar(
            select(CapacityAllocation)
            .where(
                CapacityAllocation.allocation_epoch == allocation_id,
                CapacityAllocation.subject_id == request.projection.subject_id,
            )
            .limit(1)
        )
        assert allocation is not None
        await CapacityExecutionStore._current_subject(session, epoch, allocation)
        changed = request.projection.model_copy(
            update={
                "operation_kind": "capacity",
                "operation_epoch": 2,
                "operation_id": UUID(int=22311),
                "configuration_generation": 2,
                "max_slots": 1,
            }
        )
        await CapacityMembershipStore(fixture.store).apply(
            session,
            _request(active, changed, expected_revision=1),
            actor=DELEGATE,
            idempotency_key=UUID(int=22312),
        )
        with pytest.raises(ExecutionConflictError, match="generation"):
            await CapacityExecutionStore._current_subject(session, epoch, allocation)


async def test_retained_reporter_requires_pinned_identity_and_legitimate_rollover(
    pinned_personal_allocation,  # type: ignore[no-untyped-def]
):
    module = import_module("loom_capacity_manager.membership_execution_store")
    sessions, fixture, active, request, allocation_id = pinned_personal_allocation
    async with sessions() as session, session.begin():
        epoch = await session.get(CapacityExecutionEpoch, active.execution_epoch)
        allocation = await session.get(CapacityAllocationEpoch, allocation_id)
        identity = dict(
            subject_id=request.projection.subject_id,
            reporter_incarnation=request.projection.demand_reporter_incarnation,
        )
        original = await module.resolve_allocation_reporter(session, epoch, allocation, **identity)
        arbitrary_fence = await session.begin_nested()
        await session.execute(
            update(CapacityDemandReporter)
            .where(
                CapacityDemandReporter.reporter_incarnation == identity["reporter_incarnation"],
            )
            .values(state="fenced")
        )
        with pytest.raises(ExecutionConflictError):
            await module.resolve_allocation_reporter(session, epoch, allocation, **identity)
        await arbitrary_fence.rollback()
        changed = request.projection.model_copy(
            update={
                "operation_kind": "update",
                "operation_epoch": 2,
                "operation_id": UUID(int=22321),
                "configuration_generation": 2,
                "candidate_generation": 2,
                "deployment_generation": 2,
                "demand_reporter_incarnation": UUID(int=22322),
                "demand_reporter_token_sha256": "2" * 64,
            }
        )
        await CapacityMembershipStore(fixture.store).apply(
            session,
            _request(active, changed, expected_revision=1),
            actor=DELEGATE,
            idempotency_key=UUID(int=22323),
        )
        retained = await module.resolve_allocation_reporter(session, epoch, allocation, **identity)
        assert retained.id == original.id and retained.state == "fenced"
        with pytest.raises(ExecutionConflictError):
            await module.resolve_allocation_reporter(
                session,
                epoch,
                allocation,
                subject_id=identity["subject_id"],
                reporter_incarnation=changed.demand_reporter_incarnation,
            )
        assert not await module.allocation_subject_is_current(
            session,
            epoch,
            allocation,
            subject_id=identity["subject_id"],
        )


async def test_supersession_rejects_tampered_successor_materialization(
    pinned_personal_allocation,  # type: ignore[no-untyped-def]
):
    module = import_module("loom_capacity_manager.membership_execution_store")
    sessions, fixture, active, request, allocation_id = pinned_personal_allocation
    async with sessions() as session, session.begin():
        epoch = await session.get(CapacityExecutionEpoch, active.execution_epoch)
        allocation = await session.get(CapacityAllocationEpoch, allocation_id)
        changed = request.projection.model_copy(
            update={
                "operation_kind": "capacity",
                "operation_epoch": 2,
                "operation_id": UUID(int=22331),
                "configuration_generation": 2,
                "max_slots": 1,
            }
        )
        await CapacityMembershipStore(fixture.store).apply(
            session,
            _request(active, changed, expected_revision=1),
            actor=DELEGATE,
            idempotency_key=UUID(int=22332),
        )
        await session.execute(
            update(CapacitySubject)
            .where(
                CapacitySubject.subject_id == request.projection.subject_id,
            )
            .values(max_slots=2)
        )
        with pytest.raises(ExecutionConflictError):
            await module.allocation_subject_is_current(
                session,
                epoch,
                allocation,
                subject_id=request.projection.subject_id,
            )


@pytest.mark.parametrize("tamper", ({"deployment_generation": 2}, {"configuration_generation": 99}))
async def test_historical_reporter_rejects_unrecorded_generation(
    pinned_personal_allocation,
    tamper: dict[str, int],  # type: ignore[no-untyped-def]
):
    module = import_module("loom_capacity_manager.membership_execution_store")
    sessions, _, active, request, allocation_id = pinned_personal_allocation
    async with sessions() as session, session.begin():
        epoch = await session.get(CapacityExecutionEpoch, active.execution_epoch)
        allocation = await session.get(CapacityAllocationEpoch, allocation_id)
        await session.execute(
            update(CapacityDemandReporter)
            .where(
                CapacityDemandReporter.reporter_incarnation
                == request.acknowledgement.reporter_incarnation,
            )
            .values(**tamper)
        )
        with pytest.raises(ExecutionConflictError):
            await module.resolve_allocation_reporter(
                session,
                epoch,
                allocation,
                subject_id=request.projection.subject_id,
                reporter_incarnation=request.acknowledgement.reporter_incarnation,
            )


async def test_launch_authority_derives_base_and_member_provenance_from_database(
    pinned_personal_allocation,
):
    module = import_module("loom_capacity_manager.membership_launch_authority")
    sessions, fixture, active, request, allocation_id = pinned_personal_allocation
    async with sessions() as session, session.begin():
        epoch = await session.get(CapacityExecutionEpoch, active.execution_epoch)
        allocation = await session.get(CapacityAllocationEpoch, allocation_id)
        for subject_id, delegated in ((request.projection.subject_id, True), (fixture.request.subject_acknowledgements[0].subject_id, False)):
            result = await module.resolve_allocation_launch_subject(session, epoch, allocation, subject_id=subject_id, require_current=True)
            authority = result.authority
            assert authority.purpose == "application-worker"
            assert authority.configuration.subject_id == result.configuration.subject_id == subject_id
            assert authority.configuration.digest == canonical_digest(result.configuration)
            assert authority.acknowledgement_sha256 == canonical_executable_digest(result.acknowledgement)
            assert authority.acknowledgement_sha256 != result.acknowledgement.acknowledgement_sha256
            if delegated:
                assert authority.source == "personal-membership"
                assert authority.membership.owner_id == request.projection.owner_id
                assert authority.membership.namespace_id == request.namespace_id
                assert authority.membership.revision == 1
                assert authority.membership.head_sha256 == allocation.complete_payload["membership"]["head_sha256"]
                assert result.acknowledgement.candidate == request.acknowledgement.candidate
            else:
                assert authority.source == "immutable-base"
                assert authority.membership is None


async def test_launch_authority_preserves_genuine_v2_base_allocation(capacity_session):
    from tests.integration.test_capacity_manager_execution_store import _active_plan
    module = import_module("loom_capacity_manager.membership_launch_authority")
    active, allocation_id = await _active_plan(capacity_session)
    epoch = await capacity_session.get(CapacityExecutionEpoch, active.execution_epoch)
    allocation = await capacity_session.get(CapacityAllocationEpoch, allocation_id)
    assert allocation.complete_payload["schema_version"] == 2
    subject_id = UUID(allocation.complete_payload["configuration"]["subjects"][0]["subject_id"])
    result = await module.resolve_allocation_launch_subject(capacity_session, epoch, allocation, subject_id=subject_id, require_current=True)
    assert result.authority.source == "immutable-base"
    assert result.authority.membership is None
    assert result.authority.configuration.digest == canonical_digest(result.configuration)
    assert result.authority.acknowledgement_sha256 == canonical_executable_digest(result.acknowledgement)
    assert await module.resolve_allocation_launch_subject(capacity_session, epoch, allocation, subject_id=subject_id, require_current=False) == result


async def test_launch_authority_preserves_selected_event_after_another_owner_joins(
    pinned_personal_allocation,
):
    module = import_module("loom_capacity_manager.membership_launch_authority")
    sessions, fixture, active, request, allocation_id = pinned_personal_allocation
    async with sessions() as session, session.begin():
        epoch = await session.get(CapacityExecutionEpoch, active.execution_epoch)
        allocation = await session.get(CapacityAllocationEpoch, allocation_id)
        original = await module.resolve_allocation_launch_subject(session, epoch, allocation, subject_id=request.projection.subject_id, require_current=True)
        other = _projection(
            subject_id=UUID(int=22801), subject_incarnation=UUID(int=22802), owner_id=UUID(int=22803),
            environment_name="carol", reporter_incarnation=UUID(int=22804), operation_id=UUID(int=22805),
        ).model_copy(update={"demand_reporter_token_sha256": "e" * 64})
        await CapacityMembershipStore(fixture.store).apply(session, _request(active, other, expected_revision=1), actor=DELEGATE, idempotency_key=UUID(int=22806))
        assert await module.resolve_allocation_launch_subject(session, epoch, allocation, subject_id=request.projection.subject_id, require_current=True) == original
        # A newly sealed allocation contains both events, but this subject still
        # derives provenance from its own immutable event, not the shared latest head.
    async with sessions() as session, session.begin():
        value = await fixture.store.load_allocation_input(session, fixture.writer)
    async with sessions() as session:
        new_id, _ = await _commit_reconciled_epoch(session, fixture.store, fixture.writer, allocate_shadow(value))
    async with sessions() as session, session.begin():
        epoch = await session.get(CapacityExecutionEpoch, active.execution_epoch)
        allocation = await session.get(CapacityAllocationEpoch, new_id)
        assert allocation.complete_payload["membership"]["revision"] == 2
        resolved = await module.resolve_allocation_launch_subject(session, epoch, allocation, subject_id=request.projection.subject_id, require_current=True)
        assert resolved == original
        assert resolved.authority.membership.head_sha256 != allocation.complete_payload["membership"]["head_sha256"]


async def test_launch_authority_capacity_supersession_blocks_increase_but_keeps_history(
    pinned_personal_allocation,
):
    module = import_module("loom_capacity_manager.membership_launch_authority")
    sessions, fixture, active, request, allocation_id = pinned_personal_allocation
    async with sessions() as session, session.begin():
        epoch = await session.get(CapacityExecutionEpoch, active.execution_epoch)
        allocation = await session.get(CapacityAllocationEpoch, allocation_id)
        original = await module.resolve_allocation_launch_subject(session, epoch, allocation, subject_id=request.projection.subject_id, require_current=True)
        changed = request.projection.model_copy(update={
            "operation_kind": "capacity", "operation_epoch": 2, "operation_id": UUID(int=22811),
            "configuration_generation": 2, "max_slots": 1,
        })
        await CapacityMembershipStore(fixture.store).apply(session, _request(active, changed, expected_revision=1), actor=DELEGATE, idempotency_key=UUID(int=22812))
        with pytest.raises(ExecutionConflictError, match="generation"):
            await module.resolve_allocation_launch_subject(session, epoch, allocation, subject_id=request.projection.subject_id, require_current=True)
        assert await module.resolve_allocation_launch_subject(session, epoch, allocation, subject_id=request.projection.subject_id, require_current=False) == original


@pytest.mark.parametrize("tamper", ("purpose", "candidate", "namespace", "owner", "missing_membership"))
async def test_launch_authority_rejects_forged_allocation_evidence_without_base_fallback(
    pinned_personal_allocation, tamper,
):
    module = import_module("loom_capacity_manager.membership_launch_authority")
    sessions, _fixture, active, request, allocation_id = pinned_personal_allocation
    async with sessions() as session, session.begin():
        epoch = await session.get(CapacityExecutionEpoch, active.execution_epoch)
        allocation = await session.get(CapacityAllocationEpoch, allocation_id)
        import copy
        payload = copy.deepcopy(allocation.complete_payload)
        membership = payload["membership"]
        if tamper == "missing_membership":
            payload.pop("membership")
            payload["schema_version"] = 2
        elif tamper == "namespace":
            membership["namespace_id"] = str(UUID(int=22821))
        elif tamper == "candidate":
            membership["members"][0]["acknowledgement"]["candidate"]["publication_sha256"] = "f" * 64
        else:
            membership["members"][0]["purpose" if tamper == "purpose" else "owner_id"] = "personal-build-worker" if tamper == "purpose" else str(UUID(int=22822))
        session.expunge(allocation)
        allocation.complete_payload = payload
        with pytest.raises(ExecutionConflictError):
            await module.resolve_allocation_launch_subject(session, epoch, allocation, subject_id=request.projection.subject_id, require_current=False)
