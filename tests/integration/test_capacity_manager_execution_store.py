"""Transactional coverage for the executable-v2 capacity work ledger."""

from __future__ import annotations

import asyncio
import base64
import hashlib
import json
from datetime import UTC, datetime, timedelta
from typing import Literal, cast
from uuid import UUID, uuid4, uuid5

import pytest
from sqlalchemy import event, func, select, text, update
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

import loom_capacity_manager.executable_contracts as executable_contracts_module
import loom_capacity_manager.execution_store as execution_store_module
from loom_capacity_agent.admission_convergence import ProtectedAdmissionPlanCoordinator
from loom_capacity_agent.executable_bootstrap import ProtectedExecutableBootstrapCoordinator
from loom_capacity_manager.executable_contracts import (
    ExecutableAdmissionAcknowledgementV2,
    ExecutableAdmissionPlanProposalV2,
    ExecutableBootstrapAcknowledgementV2,
    ExecutableBootstrapProposalV2,
    ExecutableBootstrapRegistrationV2,
    ExecutableExecutorHeartbeatV2,
    ExecutableExecutorInventoryV2,
    ExecutableExecutorRegistrationV2,
    ExecutableIntentBindingV2,
    ExecutableIntentCloseV2,
    ExecutableInventoryRecordV2,
    ExecutableLaunchPermitV2,
    ExecutableOwnershipMetadataV2,
    ExecutablePartialReleaseV2,
    ExecutablePermitConsumptionV2,
    ExecutableProtectedReleaseV2,
    ExecutableReservationAcceptanceV2,
    ExecutableReservationProposalV2,
    ExecutionContextV2,
    ExecutionDrainV2,
    ExecutionPreparationV2,
    ExecutionRetirementExecutorCheckpointV2,
    ExecutionRetirementV2,
    ProtectedAdmissionAssignmentV2,
    SignedExecutableOwnershipProofV2,
    canonical_executable_bytes,
    canonical_executable_digest,
    canonical_inventory_confirmation_journal_head,
)
from loom_capacity_manager.execution_store import CapacityExecutionStore
from loom_capacity_manager.models import (
    CapacityAccountPolicy,
    CapacityAllocation,
    CapacityAllocationEpoch,
    CapacityAuthorityState,
    CapacityCandidate,
    CapacityDemandReporter,
    CapacityExecutableAdmissionAcknowledgement,
    CapacityExecutableAdmissionProposal,
    CapacityExecutableBootstrapAcknowledgement,
    CapacityExecutableBootstrapProposal,
    CapacityExecutableExecutorState,
    CapacityExecutableIntent,
    CapacityExecutableTranche,
    CapacityExecutionEpoch,
    CapacityPool,
    CapacityPoolObservation,
    CapacitySubject,
    CapacityTier,
    CapacityWorkerProfile,
)
from loom_capacity_manager.ownership import OwnershipKeyring
from loom_capacity_manager.reconciler import reconcile_shadow_once
from loom_capacity_manager.store import CapacityManagementStore, ExecutionConflictError
from tests.capacity_execution_fixtures import (
    EXECUTOR_KEYS,
    execution_policy,
    executor_binding,
    ready_execution_activation,
    register_execution_executors,
    setup_execution,
)
from tests.capacity_fixtures import demand_snapshot, pool_observation, resource_vector, shape
from tests.integration.test_capacity_agent_executable_admission import (
    _capture_lifecycle,
    _initialize_manager_bound_admission_agent,
    _seed_lifecycle_attempt,
    _serializable_agent_session,
)
from tests.integration.test_capacity_agent_executable_admission import (
    _owner_session as _guard_owner_session,
)


def test_execution_store_is_a_distinct_v2_ledger() -> None:
    """Removing the dedicated executable store must break v2 queue ownership."""

    assert CapacityExecutionStore.__module__ == "loom_capacity_manager.execution_store"


async def _active_plan(session: AsyncSession):  # type: ignore[no-untyped-def]
    return await _active_plan_with_policy(session, policy=execution_policy())


async def _active_plan_with_policy(  # type: ignore[no-untyped-def]
    session: AsyncSession,
    *,
    policy,
):
    fixture = await setup_execution(session, execution_policy=policy)
    request = fixture.request.model_copy(
        update={
            "requested_ceiling": policy.executable_new_capacity_ceiling,
            "requested_rate_per_minute": policy.executable_new_capacity_rate_per_minute,
        }
    )
    await fixture.store.ingest_demand_snapshot(
        session,
        demand_snapshot(sequence=1, pending_attempt_ids=(str(UUID(int=991)),)),
        actor="development",
    )
    for pool_id in ("gb10", "oldlab"):
        await fixture.store.ingest_pool_observation(
            session,
            pool_observation(sequence=1, pool_id=pool_id),
            actor=f"{pool_id}-reporter",
        )
    prepared = await fixture.store.prepare_execution_epoch(
        session,
        request,
        actor="activation-operator",
        idempotency_key=UUID(int=901),
    )
    await register_execution_executors(session, fixture, prepared)
    activation = await ready_execution_activation(
        session,
        fixture.store,
        request,
        prepared,
    )
    active = await fixture.store.activate_execution_epoch(
        session,
        activation,
        actor="activation-operator",
        idempotency_key=UUID(int=902),
    )
    session_factory = async_sessionmaker(
        bind=session.bind,
        expire_on_commit=False,
        join_transaction_mode="create_savepoint",
    )
    result = await reconcile_shadow_once(
        session_factory,
        fixture.writer,
        store=fixture.store,
    )
    assert result.status == "committed"
    allocation_epoch = (
        await session.execute(select(CapacityAllocationEpoch.allocation_epoch))
    ).scalar_one()
    return active, allocation_epoch


async def _active_batched_plan(
    session: AsyncSession,
    *,
    slots: int = 2,
    candidate: executable_contracts_module.CandidateBindingV2 | None = None,
):  # type: ignore[no-untyped-def]
    policy = execution_policy(candidate).model_copy(
        update={
            "executable_new_capacity_ceiling": slots,
            "executable_new_capacity_rate_per_minute": slots,
        }
    )
    fixture = await setup_execution(
        session,
        execution_policy=policy,
        candidate=candidate,
    )
    request = fixture.request.model_copy(
        update={
            "requested_ceiling": slots,
            "requested_rate_per_minute": slots,
        }
    )
    demand = demand_snapshot(
        sequence=1,
        pending_attempt_ids=tuple(str(UUID(int=991 + index)) for index in range(slots)),
    )
    bucket = demand.pending_unassigned[0].model_copy(
        update={"eligible_pool_ids": ("gb10",)}
    )
    await fixture.store.ingest_demand_snapshot(
        session,
        demand.model_copy(update={"pending_unassigned": (bucket,)}),
        actor="development",
    )
    for pool_id in ("gb10", "oldlab"):
        await fixture.store.ingest_pool_observation(
            session,
            pool_observation(sequence=1, pool_id=pool_id),
            actor=f"{pool_id}-reporter",
        )
    prepared = await fixture.store.prepare_execution_epoch(
        session,
        request,
        actor="activation-operator",
        idempotency_key=UUID(int=911),
    )
    await register_execution_executors(session, fixture, prepared)
    activation = await ready_execution_activation(
        session,
        fixture.store,
        request,
        prepared,
    )
    active = await fixture.store.activate_execution_epoch(
        session,
        activation,
        actor="activation-operator",
        idempotency_key=UUID(int=912),
    )
    session_factory = async_sessionmaker(
        bind=session.bind,
        expire_on_commit=False,
        join_transaction_mode="create_savepoint",
    )
    result = await reconcile_shadow_once(
        session_factory,
        fixture.writer,
        store=fixture.store,
    )
    assert result.status == "committed"
    epoch = (
        await session.execute(select(CapacityAllocationEpoch))
    ).scalar_one()
    ranks = tuple(
        item
        for item in epoch.complete_payload["hypothetical_launch_rank"]
        if item["pool_id"] == "gb10"
    )
    assert len(ranks) == slots
    return active, epoch.allocation_epoch


async def _interleave_other_subject_between_batched_shapes(
    session: AsyncSession,
    *,
    allocation_epoch: int,
) -> tuple[dict[str, object], dict[str, object], dict[str, object]]:
    epoch = (
        await session.execute(
            select(CapacityAllocationEpoch).where(
                CapacityAllocationEpoch.allocation_epoch == allocation_epoch
            )
        )
    ).scalar_one()
    complete_payload = json.loads(json.dumps(epoch.complete_payload))
    gb10_ranks = [
        rank
        for rank in complete_payload["hypothetical_launch_rank"]
        if rank["pool_id"] == "gb10"
    ]
    assert len(gb10_ranks) >= 2
    first_rank = json.loads(json.dumps(gb10_ranks[0]))
    trailing_rank = json.loads(json.dumps(gb10_ranks[1]))

    source_allocation = (
        await session.execute(
            select(CapacityAllocation).where(
                CapacityAllocation.allocation_epoch == allocation_epoch,
                CapacityAllocation.subject_id == UUID(str(first_rank["subject_id"])),
                CapacityAllocation.pool_id == "gb10",
            )
        )
    ).scalar_one()
    source_subject = (
        await session.execute(
            select(CapacitySubject).where(
                CapacitySubject.configuration_epoch == epoch.configuration_epoch,
                CapacitySubject.subject_id == source_allocation.subject_id,
                CapacitySubject.subject_incarnation == source_allocation.subject_incarnation,
            )
        )
    ).scalar_one()
    source_candidate = (
        await session.execute(
            select(CapacityCandidate).where(
                CapacityCandidate.subject_id == source_subject.subject_id,
                CapacityCandidate.subject_incarnation == source_subject.subject_incarnation,
                CapacityCandidate.candidate_generation == source_subject.candidate_generation,
            )
        )
    ).scalar_one()
    source_profile = (
        await session.execute(
            select(CapacityWorkerProfile).where(
                CapacityWorkerProfile.subject_id == source_subject.subject_id,
                CapacityWorkerProfile.subject_incarnation == source_subject.subject_incarnation,
                CapacityWorkerProfile.deployment_generation == source_subject.deployment_generation,
                CapacityWorkerProfile.pool_id == "gb10",
            )
        )
    ).scalar_one()

    other_subject_id = UUID(int=921)
    other_subject_incarnation = UUID(int=922)
    other_subject_payload = json.loads(json.dumps(source_subject.payload))
    other_subject_payload["subject_id"] = str(other_subject_id)
    other_subject_payload["subject_incarnation"] = str(other_subject_incarnation)
    other_subject_payload["display_name"] = "development-interleaved"
    session.add(
        CapacitySubject(
            configuration_epoch=source_subject.configuration_epoch,
            subject_id=other_subject_id,
            subject_incarnation=other_subject_incarnation,
            display_name="development-interleaved",
            account_id=source_subject.account_id,
            tier_id=source_subject.tier_id,
            min_slots=source_subject.min_slots,
            max_slots=source_subject.max_slots,
            rollout_surge_slots=source_subject.rollout_surge_slots,
            max_pending_slots=source_subject.max_pending_slots,
            max_pending_jobs=source_subject.max_pending_jobs,
            submission_rate_per_minute=source_subject.submission_rate_per_minute,
            lifecycle_state=source_subject.lifecycle_state,
            candidate_generation=source_subject.candidate_generation,
            deployment_generation=source_subject.deployment_generation,
            configuration_generation=source_subject.configuration_generation,
            demand_reporter_incarnation=source_subject.demand_reporter_incarnation,
            payload=other_subject_payload,
        )
    )
    session.add(
        CapacityCandidate(
            subject_id=other_subject_id,
            subject_incarnation=other_subject_incarnation,
            candidate_generation=source_candidate.candidate_generation,
            candidate_digest=source_candidate.candidate_digest,
            candidate_identity_algorithm=source_candidate.candidate_identity_algorithm,
            candidate_identity=source_candidate.candidate_identity,
            source_payload=json.loads(json.dumps(source_candidate.source_payload)),
            artifact_payload=json.loads(json.dumps(source_candidate.artifact_payload)),
            architecture_payload=json.loads(json.dumps(source_candidate.architecture_payload)),
            launcher_payload=json.loads(json.dumps(source_candidate.launcher_payload)),
            attestation_payload=json.loads(json.dumps(source_candidate.attestation_payload)),
            protocol_payload=json.loads(json.dumps(source_candidate.protocol_payload)),
        )
    )
    session.add(
        CapacityWorkerProfile(
            subject_id=other_subject_id,
            subject_incarnation=other_subject_incarnation,
            deployment_generation=source_profile.deployment_generation,
            pool_id=source_profile.pool_id,
            pool_generation=source_profile.pool_generation,
            profile_generation=source_profile.profile_generation,
            profile_digest=source_profile.profile_digest,
            shape_catalog=json.loads(json.dumps(source_profile.shape_catalog)),
            narrowing_constraints=json.loads(json.dumps(source_profile.narrowing_constraints)),
        )
    )
    await _test_only_insert_without_guard(
        session,
        table_name="capacity_allocations",
        trigger_name="capacity_allocation_binding_guard",
        row=CapacityAllocation(
            allocation_epoch=source_allocation.allocation_epoch,
            subject_id=other_subject_id,
            subject_incarnation=other_subject_incarnation,
            deployment_generation=source_allocation.deployment_generation,
            pool_id=source_allocation.pool_id,
            desired_shapes=json.loads(json.dumps(source_allocation.desired_shapes)),
            desired_resources=json.loads(json.dumps(source_allocation.desired_resources)),
            commitments=json.loads(json.dumps(source_allocation.commitments)),
            drains=json.loads(json.dumps(source_allocation.drains)),
            allowances=json.loads(json.dumps(source_allocation.allowances)),
            witness=json.loads(json.dumps(source_allocation.witness)),
            mode=source_allocation.mode,
            executable=source_allocation.executable,
            execution_epoch=source_allocation.execution_epoch,
            execution_manifest_sha256=source_allocation.execution_manifest_sha256,
        ),
    )

    shape_id = str(source_profile.shape_catalog[0]["shape_id"])
    other_shape_instance_id = (
        "shape-"
        + hashlib.sha256(f"{other_subject_id}:gb10:{shape_id}".encode()).hexdigest()[:24]
        + "-00000001"
    )
    interleaved_rank = json.loads(json.dumps(trailing_rank))
    interleaved_rank["rank"] = 2
    interleaved_rank["subject_id"] = str(other_subject_id)
    interleaved_rank["shape_instance_id"] = other_shape_instance_id
    trailing_rank["rank"] = 3
    retained_shape_instance_ids = {
        str(first_rank["shape_instance_id"]),
        str(trailing_rank["shape_instance_id"]),
    }
    retained_allowances = [
        json.loads(json.dumps(allowance))
        for allowance in source_allocation.allowances
        if allowance["shape_instance_id"] in retained_shape_instance_ids
    ]
    retained_matches = [
        (attempt_id, shape_instance_id)
        for attempt_id, shape_instance_id in zip(
            source_allocation.witness["attempt_ids"],
            source_allocation.witness["shape_instance_ids"],
            strict=True,
        )
        if any(
            shape_instance_id.startswith(f"{retained_shape_instance_id}-slot-")
            for retained_shape_instance_id in retained_shape_instance_ids
        )
    ]
    retained_witness = json.loads(json.dumps(source_allocation.witness))
    retained_witness["matched_slots"] = len(retained_matches)
    retained_witness["attempt_ids"] = [item[0] for item in retained_matches]
    retained_witness["shape_instance_ids"] = [item[1] for item in retained_matches]
    with session.no_autoflush:
        await session.execute(
            text(
                "ALTER TABLE public.capacity_allocations "
                "DISABLE TRIGGER capacity_allocation_binding_guard"
            )
        )
        try:
            await session.execute(
                update(CapacityAllocation)
                .where(CapacityAllocation.id == source_allocation.id)
                .values(
                    allowances=retained_allowances,
                    witness=retained_witness,
                )
                .execution_options(synchronize_session=False)
            )
        finally:
            await session.execute(
                text(
                    "ALTER TABLE public.capacity_allocations "
                    "ENABLE TRIGGER capacity_allocation_binding_guard"
                )
            )
    manager_source_allocation = next(
        allocation
        for allocation in complete_payload["allocations"]
        if allocation["subject_id"] == str(source_subject.subject_id)
        and allocation["pool_id"] == "gb10"
    )
    manager_source_allocation["new_allowance_slots"] = len(retained_allowances)
    manager_source_allocation["placement_allowances"] = retained_allowances
    manager_source_allocation["matching_witness"] = retained_witness
    remaining_ranks = [
        rank
        for rank in complete_payload["hypothetical_launch_rank"]
        if rank["pool_id"] != "gb10"
    ]
    complete_payload["hypothetical_launch_rank"] = [
        first_rank,
        interleaved_rank,
        trailing_rank,
        *remaining_ranks,
    ]

    gb10_witness = next(
        witness for witness in complete_payload["pool_witnesses"] if witness["pool_id"] == "gb10"
    )
    first_placement = next(
        placement
        for placement in gb10_witness["placements"]
        if placement["instance_id"] == first_rank["shape_instance_id"]
    )
    trailing_placement = next(
        placement
        for placement in gb10_witness["placements"]
        if placement["instance_id"] == trailing_rank["shape_instance_id"]
    )
    interleaved_placement = json.loads(json.dumps(trailing_placement))
    interleaved_placement["instance_id"] = other_shape_instance_id
    gb10_witness["placements"] = [
        first_placement,
        interleaved_placement,
        trailing_placement,
    ]
    with session.no_autoflush:
        await session.execute(
            text("ALTER TABLE public.capacity_allocation_epochs DISABLE TRIGGER ALL")
        )
        try:
            await session.execute(
                update(CapacityAllocationEpoch)
                .where(CapacityAllocationEpoch.allocation_epoch == allocation_epoch)
                .values(complete_payload=complete_payload)
                .execution_options(synchronize_session=False)
            )
        finally:
            await session.execute(
                text("ALTER TABLE public.capacity_allocation_epochs ENABLE TRIGGER ALL")
            )
    return first_rank, interleaved_rank, trailing_rank


async def _replace_launch_ranks_without_guard(
    session: AsyncSession,
    *,
    allocation_epoch: int,
    ranks: list[dict[str, object]],
) -> None:
    epoch = (
        await session.execute(
            select(CapacityAllocationEpoch).where(
                CapacityAllocationEpoch.allocation_epoch == allocation_epoch
            )
        )
    ).scalar_one()
    complete_payload = json.loads(json.dumps(epoch.complete_payload))
    complete_payload["hypothetical_launch_rank"] = ranks
    with session.no_autoflush:
        await session.execute(
            text("ALTER TABLE public.capacity_allocation_epochs DISABLE TRIGGER ALL")
        )
        try:
            await session.execute(
                update(CapacityAllocationEpoch)
                .where(CapacityAllocationEpoch.allocation_epoch == allocation_epoch)
                .values(complete_payload=complete_payload)
                .execution_options(synchronize_session=False)
            )
        finally:
            await session.execute(
                text("ALTER TABLE public.capacity_allocation_epochs ENABLE TRIGGER ALL")
            )
    session.expire(epoch)


async def _clone_sealed_epoch(  # type: ignore[no-untyped-def]
    session: AsyncSession,
    *,
    allocation_epoch: int,
    input_valid_until: datetime | None,
) -> int:
    await session.execute(
        text("SET CONSTRAINTS public.capacity_executable_allocation_seal_guard DEFERRED")
    )
    parent = (
        await session.execute(
            select(CapacityAllocationEpoch).where(
                CapacityAllocationEpoch.allocation_epoch == allocation_epoch
            )
        )
    ).scalar_one()
    children = tuple(
        (
            await session.execute(
                select(CapacityAllocation).where(
                    CapacityAllocation.allocation_epoch == allocation_epoch
                )
            )
        )
        .scalars()
        .all()
    )
    complete_payload = json.loads(json.dumps(parent.complete_payload))
    for index, rank in enumerate(complete_payload.get("hypothetical_launch_rank", ())):
        rank["shape_instance_id"] = f"{rank['shape_instance_id']}-next-{index}"
    for witness in complete_payload.get("pool_witnesses", ()):
        for index, placement in enumerate(witness.get("placements", ())):
            placement["instance_id"] = f"{placement['instance_id']}-next-{index}"
    clone = CapacityAllocationEpoch(
        writer_epoch=parent.writer_epoch,
        configuration_epoch=parent.configuration_epoch,
        input_digest="f" * 64,
        status="executable",
        failure_reason=None,
        complete_payload=complete_payload,
        executable=True,
        execution_epoch=parent.execution_epoch,
        execution_manifest_sha256=parent.execution_manifest_sha256,
        input_valid_until=input_valid_until,
        sealed=False,
        allocation_count=parent.allocation_count,
        committed_at=datetime.now(UTC),
    )
    session.add(clone)
    await session.flush()
    for child in children:
        session.add(
            CapacityAllocation(
                allocation_epoch=clone.allocation_epoch,
                subject_id=child.subject_id,
                subject_incarnation=child.subject_incarnation,
                deployment_generation=child.deployment_generation,
                pool_id=child.pool_id,
                desired_shapes=json.loads(json.dumps(child.desired_shapes)),
                desired_resources=json.loads(json.dumps(child.desired_resources)),
                commitments=json.loads(json.dumps(child.commitments)),
                drains=json.loads(json.dumps(child.drains)),
                allowances=json.loads(json.dumps(child.allowances)),
                witness=json.loads(json.dumps(child.witness)),
                mode="executable",
                executable=True,
                execution_epoch=child.execution_epoch,
                execution_manifest_sha256=child.execution_manifest_sha256,
            )
        )
    await session.flush()
    clone.sealed = True
    await session.flush()
    return clone.allocation_epoch


async def _executor_state(
    session: AsyncSession,
    execution: ExecutionContextV2,
    *,
    pool_id: str,
) -> CapacityExecutableExecutorState:
    return (
        await session.execute(
            select(CapacityExecutableExecutorState).where(
                CapacityExecutableExecutorState.execution_epoch == execution.execution_epoch,
                CapacityExecutableExecutorState.pool_id == pool_id,
            )
        )
    ).scalar_one()


async def _heartbeat(
    store: CapacityExecutionStore,
    session: AsyncSession,
    active,  # type: ignore[no-untyped-def]
    *,
    pool_id: str,
):  # type: ignore[no-untyped-def]
    binding = executor_binding(pool_id)
    state = await _executor_state(session, active, pool_id=pool_id)
    heartbeat_contract = ExecutableExecutorHeartbeatV2(
        execution=active,
        executor_id=binding.executor_id,
        executor_incarnation=binding.executor_incarnation,
        pool_id=binding.pool_id,
        pool_generation=binding.pool_generation,
        heartbeat_sequence=state.heartbeat_high_water + 1,
        journal_sequence=state.journal_high_water,
        journal_digest=state.journal_digest,
        journal_checkpoint_sequence=state.journal_high_water,
        journal_checkpoint_digest=state.journal_digest,
    )
    heartbeat = await store.heartbeat_executor(
        session,
        heartbeat_contract,
    )
    return heartbeat_contract, heartbeat


async def _next_inventory(
    session: AsyncSession,
    execution: ExecutionContextV2,
    binding,
    *,
    records=(),
):  # type: ignore[no-untyped-def]
    state = await _executor_state(session, execution, pool_id=binding.pool_id)
    return ExecutableExecutorInventoryV2(
        execution=execution,
        executor_id=binding.executor_id,
        executor_incarnation=binding.executor_incarnation,
        pool_id=binding.pool_id,
        pool_generation=binding.pool_generation,
        inventory_sequence=state.inventory_high_water + 1,
        journal_sequence=state.journal_high_water,
        journal_digest=state.journal_digest,
        journal_checkpoint_sequence=state.journal_high_water,
        journal_checkpoint_digest=state.journal_digest,
        records=records,
    )


def _inventory_execution(binding):  # type: ignore[no-untyped-def]
    active_payload = binding.execution.model_dump(mode="python")
    active_payload.pop("allocation_epoch")
    active_payload.pop("executable")
    return ExecutionContextV2.model_validate(active_payload)


def _ownership_proof(binding):  # type: ignore[no-untyped-def]
    metadata = ExecutableOwnershipMetadataV2(
        binding=binding,
        controller_authority_sha256=("c" if binding.pool_id == "gb10" else "d") * 64,
        trusted_launcher_sha256="e" * 64,
        slurm_cluster=f"{binding.pool_id}-controller",
        submitter_identity="loom",
        association="loom",
        submitted_at=datetime.now(UTC),
    )
    return SignedExecutableOwnershipProofV2(
        metadata=metadata,
        signing_key_id=f"{binding.pool_id}-key",
        signature_base64=base64.b64encode(
            EXECUTOR_KEYS[binding.pool_id].sign(canonical_executable_bytes(metadata))
        ).decode("ascii"),
    )


def _inventory_record(
    binding,  # type: ignore[no-untyped-def]
    *,
    physical_identity: str,
    state: str = "active",
    physical_kind: str = "slurm-job",
    terminal_evidence_sha256: str | None = None,
):
    return ExecutableInventoryRecordV2(
        physical_identity=physical_identity,
        physical_kind=physical_kind,
        authority_scope="dedicated-loom-association",
        state=state,
        resources=binding.resources,
        node_ids=binding.node_ids,
        controller_evidence_sha256="9" * 64,
        ownership_proof=_ownership_proof(binding),
        terminal_evidence_sha256=terminal_evidence_sha256,
    )


def _record_queue_lock_order(  # type: ignore[no-untyped-def]
    monkeypatch: pytest.MonkeyPatch,
    store: CapacityExecutionStore,
) -> list[str]:
    lock_order: list[str] = []
    original_lock_authority = store._lock_authority
    original_lock_current_epoch = store._lock_current_epoch
    original_exact_registration = store._exact_registration
    original_runtime_state = store._runtime_state
    original_locked_intent = store._locked_intent
    original_locked_intent_by_tranche = store._locked_intent_by_tranche
    original_locked_allocation_intents = store._locked_allocation_intents

    async def record_authority(session):
        lock_order.append("authority")
        return await original_lock_authority(session)

    async def record_epoch(session, authority):
        lock_order.append("epoch")
        return await original_lock_current_epoch(session, authority)

    async def record_registration(session, epoch, executor):
        lock_order.append("executor-registration")
        return await original_exact_registration(session, epoch, executor)

    async def record_state(session, registration, epoch, *, create):
        lock_order.append("executor-state")
        return await original_runtime_state(session, registration, epoch, create=create)

    async def record_intent(session, intent_id):
        lock_order.append("intent")
        return await original_locked_intent(session, intent_id)

    async def record_intent_by_tranche(session, tranche_id):
        lock_order.append("intent")
        return await original_locked_intent_by_tranche(session, tranche_id)

    async def record_allocation_intents(session, target):
        lock_order.append("intent")
        return await original_locked_allocation_intents(session, target)

    monkeypatch.setattr(store, "_lock_authority", record_authority)
    monkeypatch.setattr(store, "_lock_current_epoch", record_epoch)
    monkeypatch.setattr(store, "_exact_registration", record_registration)
    monkeypatch.setattr(store, "_runtime_state", record_state)
    monkeypatch.setattr(store, "_locked_intent", record_intent)
    monkeypatch.setattr(store, "_locked_intent_by_tranche", record_intent_by_tranche)
    monkeypatch.setattr(store, "_locked_allocation_intents", record_allocation_intents)
    return lock_order


def _record_admission_lock_order(  # type: ignore[no-untyped-def]
    session: AsyncSession,
):
    """Capture only row-locking SELECTs participating in admission convergence."""

    tables = (
        "capacity_authority_state",
        "capacity_execution_epochs",
        "capacity_execution_executors",
        "capacity_executable_executor_states",
        "capacity_allocation_epochs",
        "capacity_demand_reporters",
        "capacity_executable_intents",
        "capacity_executable_bootstrap_proposals",
        "capacity_executable_admission_proposals",
        "capacity_executable_bootstrap_acknowledgements",
        "capacity_executable_admission_acknowledgements",
    )
    lock_order: list[str] = []

    def capture_statement(
        _connection,  # type: ignore[no-untyped-def]
        _cursor,  # type: ignore[no-untyped-def]
        statement: str,
        _parameters,  # type: ignore[no-untyped-def]
        _context,  # type: ignore[no-untyped-def]
        _executemany: bool,
    ) -> None:
        normalized = " ".join(statement.split())
        if not normalized.startswith("SELECT ") or not (
            " FOR UPDATE" in normalized or " FOR SHARE" in normalized
        ):
            return
        for table in tables:
            if f"FROM {table}" in normalized:
                lock_order.append(table)
                return

    engine = session.bind
    assert engine is not None
    event.listen(engine.sync_engine, "before_cursor_execute", capture_statement)
    return lock_order, engine, capture_statement


async def _test_only_insert_without_guard(
    session: AsyncSession,
    *,
    table_name: str,
    trigger_name: str,
    row,
) -> None:  # type: ignore[no-untyped-def]
    """Insert synthetic fixtures without weakening the production guard."""

    with session.no_autoflush:
        await session.execute(
            text(f"ALTER TABLE public.{table_name} DISABLE TRIGGER {trigger_name}")
        )
        try:
            session.add(row)
            await session.flush()
        finally:
            await session.execute(
                text(f"ALTER TABLE public.{table_name} ENABLE TRIGGER {trigger_name}")
            )


@pytest.mark.parametrize("writer_transitions", (1, 2))
async def test_drain_telemetry_accepts_only_the_retained_active_registration(
    capacity_session: AsyncSession,
    writer_transitions: int,
) -> None:
    """A draining executor keeps reporting after one or more writer failovers."""

    store = CapacityExecutionStore()
    active, _allocation_epoch = await _active_plan(capacity_session)
    manager = CapacityManagementStore()
    writer_epoch = active.writer_epoch
    for _ in range(writer_transitions):
        successor = await manager.register_writer(
            capacity_session,
            active.authority_incarnation,
            expected_epoch=writer_epoch,
        )
        writer_epoch = successor.writer_epoch
    executor = executor_binding("gb10")

    _heartbeat_contract, heartbeat = await _heartbeat(
        store,
        capacity_session,
        active,
        pool_id=executor.pool_id,
    )
    state = (
        await capacity_session.execute(
            select(CapacityExecutableExecutorState).where(
                CapacityExecutableExecutorState.execution_epoch == active.execution_epoch,
                CapacityExecutableExecutorState.pool_id == executor.pool_id,
            )
        )
    ).scalar_one()
    inventory_contract = ExecutableExecutorInventoryV2(
        execution=active,
        executor_id=executor.executor_id,
        executor_incarnation=executor.executor_incarnation,
        pool_id=executor.pool_id,
        pool_generation=executor.pool_generation,
        inventory_sequence=state.inventory_high_water + 1,
        journal_sequence=state.journal_high_water,
        journal_digest=state.journal_digest,
        journal_checkpoint_sequence=state.journal_high_water,
        journal_checkpoint_digest=state.journal_digest,
        records=(),
    )
    inventory = await store.ingest_executor_inventory(
        capacity_session,
        inventory_contract,
    )

    assert heartbeat.heartbeat_sequence == 3
    assert inventory.inventory_digest == store.contract_digest(inventory_contract)


@pytest.mark.parametrize("telemetry_kind", ("heartbeat", "inventory"))
@pytest.mark.parametrize("changed_field", ("writer_epoch", "execution_state", "ceiling", "rate"))
async def test_drain_telemetry_rejects_a_changed_original_active_context(
    capacity_session: AsyncSession,
    telemetry_kind: str,
    changed_field: str,
) -> None:
    """Drain telemetry must retain every immutable field from active registration."""

    store = CapacityExecutionStore()
    active, _allocation_epoch = await _active_plan(capacity_session)
    await CapacityManagementStore().register_writer(
        capacity_session,
        active.authority_incarnation,
        expected_epoch=active.writer_epoch,
    )
    executor = executor_binding("gb10")
    await _heartbeat(
        store,
        capacity_session,
        active,
        pool_id=executor.pool_id,
    )
    state = (
        await capacity_session.execute(
            select(CapacityExecutableExecutorState).where(
                CapacityExecutableExecutorState.execution_epoch == active.execution_epoch,
                CapacityExecutableExecutorState.pool_id == "gb10",
            )
        )
    ).scalar_one()
    before = (
        state.heartbeat_high_water,
        state.inventory_high_water,
        state.last_inventory_digest,
    )
    if changed_field == "writer_epoch":
        changed = active.model_copy(update={"writer_epoch": active.writer_epoch + 1})
    elif changed_field == "execution_state":
        changed = active.model_copy(
            update={
                "execution_state": "drain-only",
                "executable_new_capacity_ceiling": 0,
                "executable_new_capacity_rate_per_minute": 0,
            }
        )
    elif changed_field == "ceiling":
        changed = active.model_copy(
            update={"executable_new_capacity_ceiling": active.executable_new_capacity_ceiling + 1}
        )
    else:
        changed = active.model_copy(
            update={
                "executable_new_capacity_rate_per_minute": (
                    active.executable_new_capacity_rate_per_minute + 1
                )
            }
        )

    with pytest.raises(ExecutionConflictError, match="execution fence"):
        if telemetry_kind == "heartbeat":
            await store.heartbeat_executor(
                capacity_session,
                ExecutableExecutorHeartbeatV2(
                    execution=changed,
                    executor_id=executor.executor_id,
                    executor_incarnation=executor.executor_incarnation,
                    pool_id=executor.pool_id,
                    pool_generation=executor.pool_generation,
                    heartbeat_sequence=state.heartbeat_high_water + 1,
                    journal_sequence=state.journal_high_water,
                    journal_digest=state.journal_digest,
                ),
            )
        else:
            await store.ingest_executor_inventory(
                capacity_session,
                ExecutableExecutorInventoryV2(
                    execution=changed,
                    executor_id=executor.executor_id,
                    executor_incarnation=executor.executor_incarnation,
                    pool_id=executor.pool_id,
                    pool_generation=executor.pool_generation,
                    inventory_sequence=state.inventory_high_water + 1,
                    journal_sequence=state.journal_high_water,
                    journal_digest=state.journal_digest,
                ),
            )
    await capacity_session.refresh(state)
    assert (
        state.heartbeat_high_water,
        state.inventory_high_water,
        state.last_inventory_digest,
    ) == before


async def _launch_ready(
    store: CapacityExecutionStore,
    session: AsyncSession,
    *,
    policy=None,
):
    resolved_policy = execution_policy() if policy is None else policy
    active, _allocation_epoch = await _active_plan_with_policy(
        session,
        policy=resolved_policy,
    )
    binding = executor_binding("gb10")
    await _heartbeat(store, session, active, pool_id="gb10")
    proposal = await store.next_pool_work(session, binding)
    assert isinstance(proposal, ExecutableReservationProposalV2)
    accepted = await store.accept_reservation(
        session,
        ExecutableReservationAcceptanceV2(
            execution=proposal.execution,
            tranche_id=proposal.tranche_id,
            proposal_digest=store.contract_digest(proposal),
            pool_id=binding.pool_id,
            pool_generation=binding.pool_generation,
            executor_id=binding.executor_id,
            executor_incarnation=binding.executor_incarnation,
            command_sequence=1,
        ),
    )
    intent = await store.next_pool_work(session, binding)
    assert intent is not None
    assert intent.intent_id == accepted.intent_ids[0]
    bootstrap = ExecutableBootstrapProposalV2(
        binding=intent,
        command_sequence=2,
        proposal_epoch=1,
        bootstrap_sha256="7" * 64,
        expires_at=datetime.now(UTC) + timedelta(minutes=1),
    )
    await store.propose_bootstrap(session, bootstrap)
    subject_acknowledgement = next(
        acknowledgement
        for acknowledgement in resolved_policy.subject_acknowledgements
        if acknowledgement.subject_id == intent.subject_id
    )
    await store.acknowledge_bootstrap(
        session,
        ExecutableBootstrapAcknowledgementV2(
            binding=intent,
            proposal_epoch=bootstrap.proposal_epoch,
            proposal_digest=store.contract_digest(bootstrap),
            reporter_incarnation=subject_acknowledgement.reporter_incarnation,
            bootstrap_registration_epoch=1,
            bootstrap_evidence_sha256="8" * 64,
            protected_admission_sha256=(subject_acknowledgement.protected_admission_sha256),
        ),
        actor="development",
        idempotency_key=UUID(int=995),
    )
    admission = await store.next_subject_admission_plan(
        session,
        subject_id=intent.subject_id,
        subject_incarnation=intent.subject_incarnation,
        reporter_incarnation=subject_acknowledgement.reporter_incarnation,
    )
    assert admission is not None
    await store.acknowledge_admission_plan(
        session,
        _admission_acknowledgement(admission),
        actor="development",
        idempotency_key=UUID(int=997),
    )
    permit = await store.next_pool_work(session, binding)
    assert permit is not None
    assert permit.binding.intent_id == intent.intent_id
    return permit


async def _accepted_binding(
    store: CapacityExecutionStore,
    session: AsyncSession,
):
    active, _allocation_epoch = await _active_plan(session)
    binding = executor_binding("gb10")
    await _heartbeat(store, session, active, pool_id="gb10")
    proposal = await store.next_pool_work(session, binding)
    assert isinstance(proposal, ExecutableReservationProposalV2)
    accepted = await store.accept_reservation(
        session,
        ExecutableReservationAcceptanceV2(
            execution=proposal.execution,
            tranche_id=proposal.tranche_id,
            proposal_digest=store.contract_digest(proposal),
            pool_id=binding.pool_id,
            pool_generation=binding.pool_generation,
            executor_id=binding.executor_id,
            executor_incarnation=binding.executor_incarnation,
            command_sequence=1,
        ),
    )
    intent = await store.next_pool_work(session, binding)
    assert isinstance(intent, ExecutableIntentBindingV2)
    assert intent.intent_id == accepted.intent_ids[0]
    return active, intent


async def _consume_then_publish_empty_inventory(
    store: CapacityExecutionStore,
    session: AsyncSession,
):  # type: ignore[no-untyped-def]
    permit = await _launch_ready(store, session)
    await store.consume_launch_permit(
        session,
        ExecutablePermitConsumptionV2(
            permit_id=permit.permit_id,
            permit_digest=store.contract_digest(permit),
            binding=permit.binding,
            command_sequence=3,
        ),
    )
    inventory = await _next_inventory(
        session,
        _inventory_execution(permit.binding),
        permit.binding,
    )
    await store.ingest_executor_inventory(session, inventory)
    return permit, inventory


def _submission_recovery(
    store: CapacityExecutionStore,
    permit,
    inventory: ExecutableExecutorInventoryV2,
    *,
    command_sequence: int = 4,
) -> executable_contracts_module.ExecutableSubmissionRecoveryV2:
    return executable_contracts_module.ExecutableSubmissionRecoveryV2(
        binding=permit.binding,
        permit_id=permit.permit_id,
        permit_digest=store.contract_digest(permit),
        command_sequence=command_sequence,
        inventory_sequence=inventory.inventory_sequence,
        inventory_digest=store.contract_digest(inventory),
        controller_query_completed_at=datetime.now(UTC),
        submit_process_absent=True,
        scheduler_submission_absent=True,
        controller_evidence_sha256="a" * 64,
    )


def _successor_binding(binding):  # type: ignore[no-untyped-def]
    return binding.model_copy(
        update={
            "tranche_id": uuid4(),
            "intent_id": uuid4(),
            "shape_instance_id": f"{binding.shape_instance_id}-successor",
        }
    )


def _scaled_resources(resources, factor: int):  # type: ignore[no-untyped-def]
    return resource_vector(
        slots=resources.slots * factor,
        cpu_millicores=resources.cpu_millicores * factor,
        memory_bytes=resources.memory_bytes * factor,
        gpu_count=resources.gpu_count * factor,
        generic={key: value * factor for key, value in resources.generic.items()},
    )


def _authenticated_physical_commitment(permit):  # type: ignore[no-untyped-def]
    return (
        pool_observation(
            pool_id=permit.binding.pool_id,
            commitment_ids=(f"physical-{permit.binding.intent_id}",),
        )
        .commitments[0]
        .model_copy(
            update={
                "physical_identity": permit.binding.shape_instance_id,
                "reservation_identity": str(permit.binding.intent_id),
                "ownership_state": "authenticated",
                "subject_id": permit.binding.subject_id,
                "subject_incarnation": permit.binding.subject_incarnation,
                "deployment_generation": permit.binding.deployment_generation,
                "pool_generation": permit.binding.pool_generation,
                "profile_id": permit.binding.profile_id,
                "profile_generation": permit.binding.profile_generation,
                "profile_digest": permit.binding.profile_digest,
                "shape_id": permit.binding.shape_id,
                "resources": permit.binding.resources,
                "state": "live",
                "node_ids": permit.binding.node_ids,
            }
        )
    )


def _successor_proposed_row(
    store: CapacityExecutionStore,
    row: CapacityExecutableIntent,
    binding,
) -> CapacityExecutableIntent:
    return CapacityExecutableIntent(
        intent_id=binding.intent_id,
        tranche_id=binding.tranche_id,
        shape_instance_id=binding.shape_instance_id,
        execution_epoch=row.execution_epoch,
        execution_manifest_sha256=row.execution_manifest_sha256,
        configuration_epoch=row.configuration_epoch,
        allocation_epoch=row.allocation_epoch,
        executor_id=row.executor_id,
        executor_incarnation=row.executor_incarnation,
        pool_id=row.pool_id,
        pool_generation=row.pool_generation,
        subject_id=row.subject_id,
        subject_incarnation=row.subject_incarnation,
        launch_rank=row.launch_rank + 1,
        proposal_digest=row.proposal_digest,
        proposal_payload=json.loads(json.dumps(row.proposal_payload)),
        binding_digest=store.contract_digest(binding),
        binding_payload=binding.model_dump(mode="json", exclude_none=False),
        state="proposed",
    )


async def _quarantined_consumed_permit(
    store: CapacityExecutionStore,
    session: AsyncSession,
    *,
    policy=None,
):  # type: ignore[no-untyped-def]
    resolved_policy = execution_policy(ceiling=2) if policy is None else policy
    permit = await _launch_ready(store, session, policy=resolved_policy)
    await store.consume_launch_permit(
        session,
        ExecutablePermitConsumptionV2(
            permit_id=permit.permit_id,
            permit_digest=store.contract_digest(permit),
            binding=permit.binding,
            command_sequence=3,
        ),
    )
    inventory = await _next_inventory(
        session,
        _inventory_execution(permit.binding),
        permit.binding,
    )
    await store.ingest_executor_inventory(session, inventory)
    await store.recover_unsubmitted_permit(session, _submission_recovery(store, permit, inventory))
    return permit, await _intent_row(session, permit.binding.intent_id)


async def _intent_row(
    session: AsyncSession,
    intent_id: UUID,
) -> CapacityExecutableIntent:
    return (
        await session.execute(
            select(CapacityExecutableIntent).where(CapacityExecutableIntent.intent_id == intent_id)
        )
    ).scalar_one()


async def _post_inventory_heartbeat(
    store: CapacityExecutionStore,
    session: AsyncSession,
    execution: ExecutionContextV2,
    *,
    pool_id: str,
) -> None:
    state = (
        await session.execute(
            select(CapacityExecutableExecutorState).where(
                CapacityExecutableExecutorState.pool_id == pool_id
            )
        )
    ).scalar_one()
    binding = executor_binding(pool_id)
    await store.heartbeat_executor(
        session,
        ExecutableExecutorHeartbeatV2(
            execution=execution,
            executor_id=binding.executor_id,
            executor_incarnation=binding.executor_incarnation,
            pool_id=binding.pool_id,
            pool_generation=binding.pool_generation,
            heartbeat_sequence=state.heartbeat_high_water + 1,
            journal_sequence=state.journal_high_water,
            journal_digest=state.journal_digest,
            journal_checkpoint_sequence=state.journal_high_water,
            journal_checkpoint_digest=state.journal_digest,
        ),
    )


async def _mark_retirement_safe(
    session: AsyncSession,
    *,
    pool_id: str,
) -> ExecutionRetirementExecutorCheckpointV2:
    state = (
        await session.execute(
            select(CapacityExecutableExecutorState).where(
                CapacityExecutableExecutorState.pool_id == pool_id
            )
        )
    ).scalar_one()
    inventory = ExecutableExecutorInventoryV2.model_validate_json(
        json.dumps(state.inventory_payload)
    )
    _confirmation_sequence, confirmation_digest = canonical_inventory_confirmation_journal_head(
        inventory
    )
    state.journal_high_water = _confirmation_sequence
    state.journal_digest = confirmation_digest
    state.inventory_confirmation_journal_digest = confirmation_digest
    state.retirement_safe = True
    state.retirement_inventory_digest = state.last_inventory_digest
    await session.flush()
    return ExecutionRetirementExecutorCheckpointV2(
        executor_id=state.executor_id,
        executor_incarnation=state.executor_incarnation,
        pool_id=pool_id,  # type: ignore[arg-type]
        pool_generation=state.pool_generation,
        heartbeat_sequence=state.heartbeat_high_water,
        command_sequence=state.command_high_water,
        journal_sequence=state.journal_high_water,
        journal_digest=state.journal_digest,
        inventory_sequence=state.inventory_high_water,
        inventory_digest=state.last_inventory_digest,
    )


async def _test_only_update_without_guard(
    session: AsyncSession,
    *,
    table_name: str,
    trigger_name: str,
    statement,
) -> None:  # type: ignore[no-untyped-def]
    """Build otherwise-impossible fixtures without weakening production guards."""

    with session.no_autoflush:
        await session.execute(
            text(f"ALTER TABLE public.{table_name} DISABLE TRIGGER {trigger_name}")
        )
        await session.execute(statement.execution_options(synchronize_session=False))
        await session.execute(
            text(f"ALTER TABLE public.{table_name} ENABLE TRIGGER {trigger_name}")
        )


async def _test_only_release_intent(
    session: AsyncSession,
    intent_id: UUID,
    *,
    terminal_identity: str | None = None,
) -> None:
    values: dict[str, object] = {
        "state": "released",
        "released_at": datetime.now(UTC),
    }
    if terminal_identity is not None:
        values["terminal_identity"] = terminal_identity
    await _test_only_update_without_guard(
        session,
        table_name="capacity_executable_intents",
        trigger_name="capacity_executable_intent_mutation_guard",
        statement=(
            update(CapacityExecutableIntent)
            .where(CapacityExecutableIntent.intent_id == intent_id)
            .values(**values)
        ),
    )


async def _test_only_add_intent_without_guard(
    session: AsyncSession,
    row: CapacityExecutableIntent,
) -> None:
    """Insert a synthetic later-state row used only to isolate another invariant."""

    tranche = await session.get(CapacityExecutableTranche, row.tranche_id)
    if tranche is None:
        session.add(
            CapacityExecutableTranche(
                tranche_id=row.tranche_id,
                execution_epoch=row.execution_epoch,
                execution_manifest_sha256=row.execution_manifest_sha256,
                configuration_epoch=row.configuration_epoch,
                allocation_epoch=row.allocation_epoch,
                executor_id=row.executor_id,
                executor_incarnation=row.executor_incarnation,
                pool_id=row.pool_id,
                pool_generation=row.pool_generation,
                subject_id=row.subject_id,
                subject_incarnation=row.subject_incarnation,
                proposal_digest=row.proposal_digest,
                proposal_payload=json.loads(json.dumps(row.proposal_payload)),
            )
        )
        await session.flush()
    await session.execute(
        text(
            "ALTER TABLE public.capacity_executable_intents "
            "DISABLE TRIGGER capacity_executable_intent_mutation_guard"
        )
    )
    session.add(row)
    await session.flush()
    await session.execute(
        text(
            "ALTER TABLE public.capacity_executable_intents "
            "ENABLE TRIGGER capacity_executable_intent_mutation_guard"
        )
    )


def _test_only_synthetic_permit(
    row: CapacityExecutableIntent,
    binding: ExecutableIntentBindingV2,
    now: datetime,
) -> ExecutableLaunchPermitV2:
    """Build work for a synthetic row whose real acknowledgement is intentionally absent."""

    assert row.bootstrap_registration_epoch is not None
    assert row.bootstrap_evidence_sha256 is not None
    return ExecutableLaunchPermitV2(
        permit_id=uuid4(),
        binding=binding,
        permit_epoch=1,
        launch_rank=row.launch_rank,
        expires_at=now + timedelta(minutes=1),
        bootstrap_registration_epoch=row.bootstrap_registration_epoch,
        bootstrap_evidence_sha256=row.bootstrap_evidence_sha256,
    )


async def test_multi_shape_allocation_creates_one_batched_reservation(
    capacity_session: AsyncSession,
) -> None:
    """Creating one tranche per shape would preserve the hidden single-worker ceiling."""

    store = CapacityExecutionStore()
    active, allocation_epoch = await _active_batched_plan(capacity_session)
    binding = executor_binding("gb10")
    await _heartbeat(store, capacity_session, active, pool_id="gb10")

    proposal = await store.next_pool_work(capacity_session, binding)

    assert isinstance(proposal, ExecutableReservationProposalV2)
    assert len(proposal.shapes) == 2
    assert len({item.intent_id for item in proposal.shapes}) == 2
    assert tuple(item.shape_instance_id for item in proposal.shapes) == tuple(
        sorted(item.shape_instance_id for item in proposal.shapes)
    )
    rows = tuple(
        (
            await capacity_session.execute(
                select(CapacityExecutableIntent)
                .where(
                    CapacityExecutableIntent.allocation_epoch == allocation_epoch,
                    CapacityExecutableIntent.tranche_id == proposal.tranche_id,
                )
                .order_by(CapacityExecutableIntent.launch_rank)
            )
        )
        .scalars()
        .all()
    )
    assert {row.intent_id for row in rows} == {item.intent_id for item in proposal.shapes}
    assert {row.proposal_digest for row in rows} == {store.contract_digest(proposal)}
    tranche = (
        await capacity_session.execute(
            select(CapacityExecutableTranche).where(
                CapacityExecutableTranche.tranche_id == proposal.tranche_id
            )
        )
    ).scalar_one()
    assert tranche.allocation_epoch == allocation_epoch
    assert tranche.proposal_payload == proposal.model_dump(mode="json", exclude_none=False)


async def test_noncontiguous_same_subject_ranks_share_one_ordered_tranche(
    capacity_session: AsyncSession,
) -> None:
    """One protected plan must cover A@1/A@3 without bootstrapping A@3 before B@2."""

    store = CapacityExecutionStore()
    active, allocation_epoch = await _active_batched_plan(capacity_session, slots=3)
    executor = executor_binding("gb10")
    await _heartbeat(store, capacity_session, active, pool_id="gb10")
    first_rank, interleaved_rank, trailing_rank = (
        await _interleave_other_subject_between_batched_shapes(
            capacity_session,
            allocation_epoch=allocation_epoch,
        )
    )

    first = await store.next_pool_work(capacity_session, executor)

    assert isinstance(first, ExecutableReservationProposalV2)
    assert {shape.shape_instance_id for shape in first.shapes} == {
        str(first_rank["shape_instance_id"]),
        str(trailing_rank["shape_instance_id"]),
    }
    assert str(interleaved_rank["shape_instance_id"]) not in {
        shape.shape_instance_id for shape in first.shapes
    }
    await store.accept_reservation(
        capacity_session,
        ExecutableReservationAcceptanceV2(
            execution=first.execution,
            tranche_id=first.tranche_id,
            proposal_digest=store.contract_digest(first),
            pool_id=executor.pool_id,
            pool_generation=executor.pool_generation,
            executor_id=executor.executor_id,
            executor_incarnation=executor.executor_incarnation,
            command_sequence=1,
        ),
    )
    first_intent_id = next(
        shape.intent_id
        for shape in first.shapes
        if shape.shape_instance_id == str(first_rank["shape_instance_id"])
    )
    trailing_intent_id = next(
        shape.intent_id
        for shape in first.shapes
        if shape.shape_instance_id == str(trailing_rank["shape_instance_id"])
    )
    await _test_only_update_without_guard(
        capacity_session,
        table_name="capacity_executable_intents",
        trigger_name="capacity_executable_intent_mutation_guard",
        statement=(
            update(CapacityExecutableIntent)
            .where(CapacityExecutableIntent.intent_id == first_intent_id)
            .values(state="observed", observed_state="active", inventory_sequence=2)
        ),
    )
    capacity_session.expire_all()

    second = await store.next_pool_work(capacity_session, executor)

    assert isinstance(second, ExecutableReservationProposalV2)
    assert tuple(shape.shape_instance_id for shape in second.shapes) == (
        str(interleaved_rank["shape_instance_id"]),
    )
    await store.accept_reservation(
        capacity_session,
        ExecutableReservationAcceptanceV2(
            execution=second.execution,
            tranche_id=second.tranche_id,
            proposal_digest=store.contract_digest(second),
            pool_id=executor.pool_id,
            pool_generation=executor.pool_generation,
            executor_id=executor.executor_id,
            executor_incarnation=executor.executor_incarnation,
            command_sequence=2,
        ),
    )
    await _test_only_update_without_guard(
        capacity_session,
        table_name="capacity_executable_intents",
        trigger_name="capacity_executable_intent_mutation_guard",
        statement=(
            update(CapacityExecutableIntent)
            .where(CapacityExecutableIntent.tranche_id == second.tranche_id)
            .values(state="observed", observed_state="active", inventory_sequence=3)
        ),
    )
    capacity_session.expire_all()

    third = await store.next_pool_work(capacity_session, executor)

    assert isinstance(
        third, executable_contracts_module.ExecutableIntentBindingV2
    )
    assert third.intent_id == trailing_intent_id


async def test_observed_prior_group_allows_next_subject_reservation(
    capacity_session: AsyncSession,
) -> None:
    """Healthy earlier work must not hide the next missing reservation group."""

    store = CapacityExecutionStore()
    active, allocation_epoch = await _active_batched_plan(capacity_session)
    executor = executor_binding("gb10")
    await _heartbeat(store, capacity_session, active, pool_id="gb10")
    first_rank, interleaved_rank, _trailing_rank = (
        await _interleave_other_subject_between_batched_shapes(
            capacity_session,
            allocation_epoch=allocation_epoch,
        )
    )
    await _replace_launch_ranks_without_guard(
        capacity_session,
        allocation_epoch=allocation_epoch,
        ranks=[first_rank, interleaved_rank],
    )

    first = await store.next_pool_work(capacity_session, executor)
    assert isinstance(first, ExecutableReservationProposalV2)
    assert tuple(shape.shape_instance_id for shape in first.shapes) == (
        str(first_rank["shape_instance_id"]),
    )
    await store.accept_reservation(
        capacity_session,
        ExecutableReservationAcceptanceV2(
            execution=first.execution,
            tranche_id=first.tranche_id,
            proposal_digest=store.contract_digest(first),
            pool_id=executor.pool_id,
            pool_generation=executor.pool_generation,
            executor_id=executor.executor_id,
            executor_incarnation=executor.executor_incarnation,
            command_sequence=1,
        ),
    )
    await _test_only_update_without_guard(
        capacity_session,
        table_name="capacity_executable_intents",
        trigger_name="capacity_executable_intent_mutation_guard",
        statement=(
            update(CapacityExecutableIntent)
            .where(CapacityExecutableIntent.tranche_id == first.tranche_id)
            .values(state="observed", observed_state="active", inventory_sequence=2)
        ),
    )
    capacity_session.expire_all()

    second = await store.next_pool_work(capacity_session, executor)

    assert isinstance(second, ExecutableReservationProposalV2)
    assert tuple(shape.shape_instance_id for shape in second.shapes) == (
        str(interleaved_rank["shape_instance_id"]),
    )


async def test_batched_reservation_replay_preserves_exact_payload_and_ids(
    capacity_session: AsyncSession,
) -> None:
    """Nondeterministic tranche IDs or payload ordering would break exact replay."""

    store = CapacityExecutionStore()
    active, _allocation_epoch = await _active_batched_plan(capacity_session)
    binding = executor_binding("gb10")
    await _heartbeat(store, capacity_session, active, pool_id="gb10")

    first = await store.next_pool_work(capacity_session, binding)
    second = await store.next_pool_work(capacity_session, binding)

    assert isinstance(first, ExecutableReservationProposalV2)
    assert isinstance(second, ExecutableReservationProposalV2)
    assert second == first
    assert canonical_executable_bytes(second) == canonical_executable_bytes(first)


async def test_batched_reservation_acceptance_advances_every_intent_atomically(
    capacity_session: AsyncSession,
) -> None:
    """Accepting only the first shape would strand protected plan assignments."""

    store = CapacityExecutionStore()
    active, _allocation_epoch = await _active_batched_plan(capacity_session)
    binding = executor_binding("gb10")
    await _heartbeat(store, capacity_session, active, pool_id="gb10")
    proposal = await store.next_pool_work(capacity_session, binding)
    assert isinstance(proposal, ExecutableReservationProposalV2)

    accepted = await store.accept_reservation(
        capacity_session,
        ExecutableReservationAcceptanceV2(
            execution=proposal.execution,
            tranche_id=proposal.tranche_id,
            proposal_digest=store.contract_digest(proposal),
            pool_id=binding.pool_id,
            pool_generation=binding.pool_generation,
            executor_id=binding.executor_id,
            executor_incarnation=binding.executor_incarnation,
            command_sequence=1,
        ),
    )

    assert set(accepted.intent_ids) == {item.intent_id for item in proposal.shapes}
    states = tuple(
        (
            await capacity_session.execute(
                select(CapacityExecutableIntent.state)
                .where(CapacityExecutableIntent.tranche_id == proposal.tranche_id)
                .order_by(CapacityExecutableIntent.launch_rank)
            )
        ).scalars()
    )
    assert states == ("accepted", "accepted")


async def test_batched_acceptance_rejects_changed_digest_without_advancing_any_intent(
    capacity_session: AsyncSession,
) -> None:
    """Accepting one row after a digest mismatch would split the tranche."""

    store = CapacityExecutionStore()
    active, _allocation_epoch = await _active_batched_plan(capacity_session)
    binding = executor_binding("gb10")
    await _heartbeat(store, capacity_session, active, pool_id="gb10")
    proposal = await store.next_pool_work(capacity_session, binding)
    assert isinstance(proposal, ExecutableReservationProposalV2)

    with pytest.raises(ExecutionConflictError, match="proposal digest changed"):
        await store.accept_reservation(
            capacity_session,
            ExecutableReservationAcceptanceV2(
                execution=proposal.execution,
                tranche_id=proposal.tranche_id,
                proposal_digest="0" * 64,
                pool_id=binding.pool_id,
                pool_generation=binding.pool_generation,
                executor_id=binding.executor_id,
                executor_incarnation=binding.executor_incarnation,
                command_sequence=1,
            ),
        )

    states = tuple(
        (
            await capacity_session.execute(
                select(CapacityExecutableIntent.state)
                .where(CapacityExecutableIntent.tranche_id == proposal.tranche_id)
                .order_by(CapacityExecutableIntent.launch_rank)
            )
        ).scalars()
    )
    assert states == ("proposed", "proposed")


async def test_observed_worker_does_not_hide_later_accepted_intent(
    capacity_session: AsyncSession,
) -> None:
    """Selecting only the first open row would stop a batch after one worker."""

    store = CapacityExecutionStore()
    active, _allocation_epoch = await _active_batched_plan(capacity_session)
    binding = executor_binding("gb10")
    await _heartbeat(store, capacity_session, active, pool_id="gb10")
    proposal = await store.next_pool_work(capacity_session, binding)
    assert isinstance(proposal, ExecutableReservationProposalV2)
    await store.accept_reservation(
        capacity_session,
        ExecutableReservationAcceptanceV2(
            execution=proposal.execution,
            tranche_id=proposal.tranche_id,
            proposal_digest=store.contract_digest(proposal),
            pool_id=binding.pool_id,
            pool_generation=binding.pool_generation,
            executor_id=binding.executor_id,
            executor_incarnation=binding.executor_incarnation,
            command_sequence=1,
        ),
    )
    rows = tuple(
        (
            await capacity_session.execute(
                select(CapacityExecutableIntent)
                .where(CapacityExecutableIntent.tranche_id == proposal.tranche_id)
                .order_by(CapacityExecutableIntent.launch_rank)
            )
        )
        .scalars()
        .all()
    )
    await _test_only_update_without_guard(
        capacity_session,
        table_name="capacity_executable_intents",
        trigger_name="capacity_executable_intent_mutation_guard",
        statement=(
            update(CapacityExecutableIntent)
            .where(CapacityExecutableIntent.intent_id == rows[0].intent_id)
            .values(state="observed", observed_state="active", inventory_sequence=2)
        ),
    )
    capacity_session.expire_all()

    work = await store.next_pool_work(capacity_session, binding)

    assert work is not None
    assert work.intent_id == rows[1].intent_id


async def test_newer_sealed_epoch_closes_every_unlaunched_intent_in_batched_tranche(
    capacity_session: AsyncSession,
) -> None:
    """Leaving a later accepted row open would replay stale executable work."""

    store = CapacityExecutionStore()
    active, sealed_epoch = await _active_batched_plan(capacity_session)
    binding = executor_binding("gb10")
    await _heartbeat(store, capacity_session, active, pool_id="gb10")
    proposal = await store.next_pool_work(capacity_session, binding)
    assert isinstance(proposal, ExecutableReservationProposalV2)
    await store.accept_reservation(
        capacity_session,
        ExecutableReservationAcceptanceV2(
            execution=proposal.execution,
            tranche_id=proposal.tranche_id,
            proposal_digest=store.contract_digest(proposal),
            pool_id=binding.pool_id,
            pool_generation=binding.pool_generation,
            executor_id=binding.executor_id,
            executor_incarnation=binding.executor_incarnation,
            command_sequence=1,
        ),
    )
    await _clone_sealed_epoch(
        capacity_session,
        allocation_epoch=sealed_epoch,
        input_valid_until=datetime.now(UTC) + timedelta(minutes=5),
    )
    rows = tuple(
        (
            await capacity_session.execute(
                select(CapacityExecutableIntent)
                .where(CapacityExecutableIntent.tranche_id == proposal.tranche_id)
                .order_by(CapacityExecutableIntent.launch_rank)
            )
        )
        .scalars()
        .all()
    )

    first = await store.next_pool_work(capacity_session, binding)

    assert isinstance(first, ExecutableIntentCloseV2)
    assert first.binding.intent_id == rows[0].intent_id
    await _test_only_release_intent(capacity_session, rows[0].intent_id)
    capacity_session.expire_all()

    second = await store.next_pool_work(capacity_session, binding)

    assert isinstance(second, ExecutableIntentCloseV2)
    assert second.binding.intent_id == rows[1].intent_id


async def test_queue_never_crosses_pool(capacity_session: AsyncSession) -> None:
    store = CapacityExecutionStore()
    active, _allocation_epoch = await _active_plan(capacity_session)
    await _heartbeat(store, capacity_session, active, pool_id="gb10")
    await _heartbeat(store, capacity_session, active, pool_id="oldlab")

    work = await store.next_pool_work(capacity_session, executor_binding("oldlab"))

    assert work is None or work.pool_id == "oldlab"


async def test_queue_ignores_a_newer_unsealed_allocation_epoch(
    capacity_session: AsyncSession,
) -> None:
    """A transaction-local, incomplete epoch must never displace sealed work."""

    store = CapacityExecutionStore()
    active, sealed_epoch = await _active_plan(capacity_session)
    await _heartbeat(store, capacity_session, active, pool_id="gb10")
    parent = (
        await capacity_session.execute(
            select(CapacityAllocationEpoch).where(
                CapacityAllocationEpoch.allocation_epoch == sealed_epoch
            )
        )
    ).scalar_one()
    children = tuple(
        (
            await capacity_session.execute(
                select(CapacityAllocation).where(
                    CapacityAllocation.allocation_epoch == sealed_epoch
                )
            )
        )
        .scalars()
        .all()
    )
    await capacity_session.execute(
        text("SET CONSTRAINTS public.capacity_executable_allocation_seal_guard DEFERRED")
    )
    incomplete = CapacityAllocationEpoch(
        writer_epoch=parent.writer_epoch,
        configuration_epoch=parent.configuration_epoch,
        input_digest="f" * 64,
        status="executable",
        failure_reason=None,
        complete_payload=json.loads(json.dumps(parent.complete_payload)),
        executable=True,
        execution_epoch=parent.execution_epoch,
        execution_manifest_sha256=parent.execution_manifest_sha256,
        input_valid_until=datetime.now(UTC) + timedelta(minutes=5),
        sealed=False,
        allocation_count=parent.allocation_count,
        committed_at=datetime.now(UTC),
    )
    capacity_session.add(incomplete)
    await capacity_session.flush()
    for child in children:
        capacity_session.add(
            CapacityAllocation(
                allocation_epoch=incomplete.allocation_epoch,
                subject_id=child.subject_id,
                subject_incarnation=child.subject_incarnation,
                deployment_generation=child.deployment_generation,
                pool_id=child.pool_id,
                desired_shapes=json.loads(json.dumps(child.desired_shapes)),
                desired_resources=json.loads(json.dumps(child.desired_resources)),
                commitments=json.loads(json.dumps(child.commitments)),
                drains=json.loads(json.dumps(child.drains)),
                allowances=json.loads(json.dumps(child.allowances)),
                witness=json.loads(json.dumps(child.witness)),
                mode="executable",
                executable=True,
                execution_epoch=child.execution_epoch,
                execution_manifest_sha256=child.execution_manifest_sha256,
            )
        )
    await capacity_session.flush()

    work = await store.next_pool_work(capacity_session, executor_binding("gb10"))

    assert isinstance(work, ExecutableReservationProposalV2)
    assert work.execution.allocation_epoch == sealed_epoch


async def test_newer_sealed_epoch_supersedes_a_stale_proposal(
    capacity_session: AsyncSession,
) -> None:
    store = CapacityExecutionStore()
    active, sealed_epoch = await _active_plan(capacity_session)
    await _heartbeat(store, capacity_session, active, pool_id="gb10")
    stale = await store.next_pool_work(capacity_session, executor_binding("gb10"))
    assert isinstance(stale, ExecutableReservationProposalV2)
    newer_epoch = await _clone_sealed_epoch(
        capacity_session,
        allocation_epoch=sealed_epoch,
        input_valid_until=datetime.now(UTC) + timedelta(minutes=5),
    )

    work = await store.next_pool_work(capacity_session, executor_binding("gb10"))

    assert isinstance(work, ExecutableReservationProposalV2)
    assert work.execution.allocation_epoch == newer_epoch
    assert work.tranche_id != stale.tranche_id
    stale_row = (
        await capacity_session.execute(
            select(CapacityExecutableIntent).where(
                CapacityExecutableIntent.tranche_id == stale.tranche_id
            )
        )
    ).scalar_one()
    assert stale_row.state == "released"


async def test_newer_sealed_epoch_turns_an_old_permit_into_close(
    capacity_session: AsyncSession,
) -> None:
    store = CapacityExecutionStore()
    permit = await _launch_ready(store, capacity_session)
    await _clone_sealed_epoch(
        capacity_session,
        allocation_epoch=permit.binding.execution.allocation_epoch,
        input_valid_until=datetime.now(UTC) + timedelta(minutes=5),
    )

    work = await store.next_pool_work(capacity_session, executor_binding(permit.binding.pool_id))

    assert isinstance(work, ExecutableIntentCloseV2)
    assert work.binding.intent_id == permit.binding.intent_id


async def test_superseded_accepted_bootstrap_acknowledgement_is_cleanup_only(
    capacity_session: AsyncSession,
) -> None:
    """A stale accepted intent must gain revocation evidence without reopening launch."""

    store = CapacityExecutionStore()
    active, sealed_epoch = await _active_plan(capacity_session)
    executor = executor_binding("gb10")
    await _heartbeat(store, capacity_session, active, pool_id="gb10")
    reservation = await store.next_pool_work(capacity_session, executor)
    assert isinstance(reservation, ExecutableReservationProposalV2)
    await store.accept_reservation(
        capacity_session,
        ExecutableReservationAcceptanceV2(
            execution=reservation.execution,
            tranche_id=reservation.tranche_id,
            proposal_digest=store.contract_digest(reservation),
            pool_id=executor.pool_id,
            pool_generation=executor.pool_generation,
            executor_id=executor.executor_id,
            executor_incarnation=executor.executor_incarnation,
            command_sequence=1,
        ),
    )
    await _clone_sealed_epoch(
        capacity_session,
        allocation_epoch=sealed_epoch,
        input_valid_until=datetime.now(UTC) + timedelta(minutes=5),
    )
    close = await store.next_pool_work(capacity_session, executor)
    assert isinstance(close, ExecutableIntentCloseV2)
    assert close.bootstrap_registration_epoch is None
    bootstrap = ExecutableBootstrapProposalV2(
        binding=close.binding,
        command_sequence=close.command_sequence,
        proposal_epoch=1,
        bootstrap_sha256="7" * 64,
        expires_at=datetime.now(UTC) + timedelta(minutes=1),
    )
    await store.propose_bootstrap(capacity_session, bootstrap)

    await store.acknowledge_bootstrap(
        capacity_session,
        ExecutableBootstrapAcknowledgementV2(
            binding=close.binding,
            proposal_epoch=bootstrap.proposal_epoch,
            proposal_digest=store.contract_digest(bootstrap),
            reporter_incarnation=demand_snapshot().reporter_incarnation,
            bootstrap_registration_epoch=1,
            bootstrap_evidence_sha256="8" * 64,
            protected_admission_sha256="3" * 64,
        ),
        actor="development",
        idempotency_key=UUID(int=1004),
    )

    row = (
        await capacity_session.execute(
            select(CapacityExecutableIntent).where(
                CapacityExecutableIntent.intent_id == close.binding.intent_id
            )
        )
    ).scalar_one()
    assert row.state == "bootstrap-acknowledged"
    assert row.launch_ready_at is None
    assert (
        await capacity_session.execute(
            select(func.count(CapacityExecutableAdmissionProposal.id))
        )
    ).scalar_one() == 0
    successor = await store.next_pool_work(capacity_session, executor)
    assert isinstance(successor, ExecutableIntentCloseV2)
    assert successor.bootstrap_registration_epoch == 1
    assert successor.bootstrap_evidence_sha256 == "8" * 64


async def test_acceptance_cannot_claim_another_executor_proposal(
    capacity_session: AsyncSession,
) -> None:
    store = CapacityExecutionStore()
    active, _allocation_epoch = await _active_plan(capacity_session)
    gb10 = executor_binding("gb10")
    oldlab = executor_binding("oldlab")
    await _heartbeat(store, capacity_session, active, pool_id="gb10")
    await _heartbeat(store, capacity_session, active, pool_id="oldlab")
    proposal = await store.next_pool_work(capacity_session, gb10)
    assert isinstance(proposal, ExecutableReservationProposalV2)

    with pytest.raises(ExecutionConflictError, match="executor binding"):
        await store.accept_reservation(
            capacity_session,
            ExecutableReservationAcceptanceV2(
                execution=proposal.execution,
                tranche_id=proposal.tranche_id,
                proposal_digest=store.contract_digest(proposal),
                pool_id=oldlab.pool_id,
                pool_generation=oldlab.pool_generation,
                executor_id=oldlab.executor_id,
                executor_incarnation=oldlab.executor_incarnation,
                command_sequence=1,
            ),
        )


async def test_accept_reservation_locks_executor_context_before_intent(
    capacity_session: AsyncSession,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = CapacityExecutionStore()
    active, _allocation_epoch = await _active_plan(capacity_session)
    binding = executor_binding("gb10")
    await _heartbeat(store, capacity_session, active, pool_id="gb10")
    proposal = await store.next_pool_work(capacity_session, binding)
    assert isinstance(proposal, ExecutableReservationProposalV2)
    lock_order = _record_queue_lock_order(monkeypatch, store)

    await store.accept_reservation(
        capacity_session,
        ExecutableReservationAcceptanceV2(
            execution=proposal.execution,
            tranche_id=proposal.tranche_id,
            proposal_digest=store.contract_digest(proposal),
            pool_id=binding.pool_id,
            pool_generation=binding.pool_generation,
            executor_id=binding.executor_id,
            executor_incarnation=binding.executor_incarnation,
            command_sequence=1,
        ),
    )

    assert lock_order[:5] == [
        "authority",
        "epoch",
        "executor-registration",
        "executor-state",
        "intent",
    ]


async def test_executor_equivocation_is_durably_fenced(
    capacity_session: AsyncSession,
) -> None:
    store = CapacityExecutionStore()
    active, _allocation_epoch = await _active_plan(capacity_session)
    binding = executor_binding("gb10")
    heartbeat_contract, _heartbeat_result = await _heartbeat(
        store,
        capacity_session,
        active,
        pool_id="gb10",
    )
    changed = heartbeat_contract.model_copy(
        update={
            "journal_sequence": heartbeat_contract.journal_sequence + 1,
            "journal_digest": "7" * 64,
        }
    )

    with pytest.raises(ExecutionConflictError, match="equivocated"):
        await store.heartbeat_executor(capacity_session, changed)

    state = (
        await capacity_session.execute(
            select(CapacityExecutableExecutorState).where(
                CapacityExecutableExecutorState.executor_incarnation == binding.executor_incarnation
            )
        )
    ).scalar_one()
    assert state.state == "equivocal"
    with pytest.raises(ExecutionConflictError, match="unavailable"):
        await store.executor_checkpoint(capacity_session, binding)


async def test_heartbeat_requires_exact_journal_checkpoint_sequence(
    capacity_session: AsyncSession,
) -> None:
    store = CapacityExecutionStore()
    active, _allocation_epoch = await _active_plan(capacity_session)
    binding = executor_binding("gb10")
    initial_state = await _executor_state(capacity_session, active, pool_id="gb10")
    initial_heartbeat = initial_state.heartbeat_high_water
    initial_journal = initial_state.journal_high_water
    initial_digest = initial_state.journal_digest
    await store.heartbeat_executor(
        capacity_session,
        ExecutableExecutorHeartbeatV2(
            execution=active,
            executor_id=binding.executor_id,
            executor_incarnation=binding.executor_incarnation,
            pool_id=binding.pool_id,
            pool_generation=binding.pool_generation,
            heartbeat_sequence=initial_heartbeat + 1,
            journal_sequence=initial_journal + 1,
            journal_digest="a" * 64,
            journal_checkpoint_sequence=initial_journal,
            journal_checkpoint_digest=initial_digest,
        ),
    )

    with pytest.raises(ExecutionConflictError, match="journal checkpoint diverged"):
        await store.heartbeat_executor(
            capacity_session,
            ExecutableExecutorHeartbeatV2(
                execution=active,
                executor_id=binding.executor_id,
                executor_incarnation=binding.executor_incarnation,
                pool_id=binding.pool_id,
                pool_generation=binding.pool_generation,
                heartbeat_sequence=initial_heartbeat + 2,
                journal_sequence=initial_journal + 2,
                journal_digest="b" * 64,
                journal_checkpoint_sequence=initial_journal,
                journal_checkpoint_digest=initial_digest,
            ),
        )


async def test_inventory_requires_exact_journal_checkpoint_sequence(
    capacity_session: AsyncSession,
) -> None:
    store = CapacityExecutionStore()
    active, _allocation_epoch = await _active_plan(capacity_session)
    binding = executor_binding("gb10")
    initial_state = await _executor_state(capacity_session, active, pool_id="gb10")
    initial_heartbeat = initial_state.heartbeat_high_water
    initial_inventory = initial_state.inventory_high_water
    initial_journal = initial_state.journal_high_water
    initial_digest = initial_state.journal_digest
    await store.heartbeat_executor(
        capacity_session,
        ExecutableExecutorHeartbeatV2(
            execution=active,
            executor_id=binding.executor_id,
            executor_incarnation=binding.executor_incarnation,
            pool_id=binding.pool_id,
            pool_generation=binding.pool_generation,
            heartbeat_sequence=initial_heartbeat + 1,
            journal_sequence=initial_journal + 1,
            journal_digest="a" * 64,
            journal_checkpoint_sequence=initial_journal,
            journal_checkpoint_digest=initial_digest,
        ),
    )
    await store.ingest_executor_inventory(
        capacity_session,
        ExecutableExecutorInventoryV2(
            execution=active,
            executor_id=binding.executor_id,
            executor_incarnation=binding.executor_incarnation,
            pool_id=binding.pool_id,
            pool_generation=binding.pool_generation,
            inventory_sequence=initial_inventory + 1,
            journal_sequence=initial_journal + 1,
            journal_digest="a" * 64,
            journal_checkpoint_sequence=initial_journal + 1,
            journal_checkpoint_digest="a" * 64,
        ),
    )

    with pytest.raises(ExecutionConflictError, match="journal diverged"):
        await store.ingest_executor_inventory(
            capacity_session,
            ExecutableExecutorInventoryV2(
                execution=active,
                executor_id=binding.executor_id,
                executor_incarnation=binding.executor_incarnation,
                pool_id=binding.pool_id,
                pool_generation=binding.pool_generation,
                inventory_sequence=initial_inventory + 2,
                journal_sequence=initial_journal + 2,
                journal_digest="b" * 64,
                journal_checkpoint_sequence=initial_journal,
                journal_checkpoint_digest=initial_digest,
            ),
        )


async def test_inventory_advances_the_stored_journal_head(
    capacity_session: AsyncSession,
) -> None:
    store = CapacityExecutionStore()
    active, _allocation_epoch = await _active_plan(capacity_session)
    binding = executor_binding("gb10")
    initial_state = await _executor_state(capacity_session, active, pool_id="gb10")
    initial_heartbeat = initial_state.heartbeat_high_water
    initial_inventory = initial_state.inventory_high_water
    initial_journal = initial_state.journal_high_water
    initial_digest = initial_state.journal_digest
    await store.heartbeat_executor(
        capacity_session,
        ExecutableExecutorHeartbeatV2(
            execution=active,
            executor_id=binding.executor_id,
            executor_incarnation=binding.executor_incarnation,
            pool_id=binding.pool_id,
            pool_generation=binding.pool_generation,
            heartbeat_sequence=initial_heartbeat + 1,
            journal_sequence=initial_journal + 1,
            journal_digest="a" * 64,
            journal_checkpoint_sequence=initial_journal,
            journal_checkpoint_digest=initial_digest,
        ),
    )

    await store.ingest_executor_inventory(
        capacity_session,
        ExecutableExecutorInventoryV2(
            execution=active,
            executor_id=binding.executor_id,
            executor_incarnation=binding.executor_incarnation,
            pool_id=binding.pool_id,
            pool_generation=binding.pool_generation,
            inventory_sequence=initial_inventory + 1,
            journal_sequence=initial_journal + 2,
            journal_digest="b" * 64,
            journal_checkpoint_sequence=initial_journal + 1,
            journal_checkpoint_digest="a" * 64,
        ),
    )

    checkpoint = await store.executor_checkpoint(capacity_session, binding)

    assert checkpoint.journal_sequence == initial_journal + 2
    assert checkpoint.journal_digest == "b" * 64


async def test_heartbeat_rejects_changed_journal_digest_at_same_head_sequence(
    capacity_session: AsyncSession,
) -> None:
    store = CapacityExecutionStore()
    active, _allocation_epoch = await _active_plan(capacity_session)
    binding = executor_binding("gb10")
    initial_state = await _executor_state(capacity_session, active, pool_id="gb10")
    initial_heartbeat = initial_state.heartbeat_high_water
    initial_journal = initial_state.journal_high_water
    initial_digest = initial_state.journal_digest
    await store.heartbeat_executor(
        capacity_session,
        ExecutableExecutorHeartbeatV2(
            execution=active,
            executor_id=binding.executor_id,
            executor_incarnation=binding.executor_incarnation,
            pool_id=binding.pool_id,
            pool_generation=binding.pool_generation,
            heartbeat_sequence=initial_heartbeat + 1,
            journal_sequence=initial_journal + 1,
            journal_digest="a" * 64,
            journal_checkpoint_sequence=initial_journal,
            journal_checkpoint_digest=initial_digest,
        ),
    )

    replay = await store.heartbeat_executor(
        capacity_session,
        ExecutableExecutorHeartbeatV2(
            execution=active,
            executor_id=binding.executor_id,
            executor_incarnation=binding.executor_incarnation,
            pool_id=binding.pool_id,
            pool_generation=binding.pool_generation,
            heartbeat_sequence=initial_heartbeat + 2,
            journal_sequence=initial_journal + 1,
            journal_digest="a" * 64,
            journal_checkpoint_sequence=initial_journal + 1,
            journal_checkpoint_digest="a" * 64,
        ),
    )
    assert replay.replayed is False

    with pytest.raises(ExecutionConflictError, match="journal digest changed"):
        await store.heartbeat_executor(
            capacity_session,
            ExecutableExecutorHeartbeatV2(
                execution=active,
                executor_id=binding.executor_id,
                executor_incarnation=binding.executor_incarnation,
                pool_id=binding.pool_id,
                pool_generation=binding.pool_generation,
                heartbeat_sequence=initial_heartbeat + 3,
                journal_sequence=initial_journal + 1,
                journal_digest="b" * 64,
                journal_checkpoint_sequence=initial_journal + 1,
                journal_checkpoint_digest="a" * 64,
            ),
        )


async def test_inventory_rejects_changed_journal_digest_at_same_head_sequence(
    capacity_session: AsyncSession,
) -> None:
    store = CapacityExecutionStore()
    active, _allocation_epoch = await _active_plan(capacity_session)
    binding = executor_binding("gb10")
    initial_state = await _executor_state(capacity_session, active, pool_id="gb10")
    initial_heartbeat = initial_state.heartbeat_high_water
    initial_inventory = initial_state.inventory_high_water
    initial_journal = initial_state.journal_high_water
    initial_digest = initial_state.journal_digest
    await store.heartbeat_executor(
        capacity_session,
        ExecutableExecutorHeartbeatV2(
            execution=active,
            executor_id=binding.executor_id,
            executor_incarnation=binding.executor_incarnation,
            pool_id=binding.pool_id,
            pool_generation=binding.pool_generation,
            heartbeat_sequence=initial_heartbeat + 1,
            journal_sequence=initial_journal + 1,
            journal_digest="a" * 64,
            journal_checkpoint_sequence=initial_journal,
            journal_checkpoint_digest=initial_digest,
        ),
    )
    replay = await store.ingest_executor_inventory(
        capacity_session,
        ExecutableExecutorInventoryV2(
            execution=active,
            executor_id=binding.executor_id,
            executor_incarnation=binding.executor_incarnation,
            pool_id=binding.pool_id,
            pool_generation=binding.pool_generation,
            inventory_sequence=initial_inventory + 1,
            journal_sequence=initial_journal + 1,
            journal_digest="a" * 64,
            journal_checkpoint_sequence=initial_journal + 1,
            journal_checkpoint_digest="a" * 64,
        ),
    )
    assert replay.replayed is False

    with pytest.raises(ExecutionConflictError, match="journal digest changed"):
        await store.ingest_executor_inventory(
            capacity_session,
            ExecutableExecutorInventoryV2(
                execution=active,
                executor_id=binding.executor_id,
                executor_incarnation=binding.executor_incarnation,
                pool_id=binding.pool_id,
                pool_generation=binding.pool_generation,
                inventory_sequence=initial_inventory + 2,
                journal_sequence=initial_journal + 1,
                journal_digest="b" * 64,
                journal_checkpoint_sequence=initial_journal + 1,
                journal_checkpoint_digest="a" * 64,
            ),
        )


async def test_ceiling_zero_blocks_increase_work(capacity_session: AsyncSession) -> None:
    store = CapacityExecutionStore()

    assert await store.next_pool_work(capacity_session, executor_binding("gb10")) is None


async def test_permit_consumption_rechecks_execution_fence(
    capacity_session: AsyncSession,
) -> None:
    store = CapacityExecutionStore()
    permit = await _launch_ready(store, capacity_session)
    await CapacityManagementStore().register_writer(
        capacity_session,
        permit.binding.execution.authority_incarnation,
        expected_epoch=permit.binding.execution.writer_epoch,
    )
    with pytest.raises(ExecutionConflictError, match="execution fence"):
        await store.consume_launch_permit(
            capacity_session,
            ExecutablePermitConsumptionV2(
                permit_id=permit.permit_id,
                permit_digest=store.contract_digest(permit),
                binding=permit.binding,
                command_sequence=3,
            ),
        )


async def _proposed_bootstrap(
    store: CapacityExecutionStore,
    session: AsyncSession,
    *,
    expires_in: timedelta = timedelta(minutes=1),
):  # type: ignore[no-untyped-def]
    active, _allocation_epoch = await _active_plan(session)
    executor = executor_binding("gb10")
    await _heartbeat(store, session, active, pool_id="gb10")
    reservation = await store.next_pool_work(session, executor)
    assert isinstance(reservation, ExecutableReservationProposalV2)
    accepted = await store.accept_reservation(
        session,
        ExecutableReservationAcceptanceV2(
            execution=reservation.execution,
            tranche_id=reservation.tranche_id,
            proposal_digest=store.contract_digest(reservation),
            pool_id=executor.pool_id,
            pool_generation=executor.pool_generation,
            executor_id=executor.executor_id,
            executor_incarnation=executor.executor_incarnation,
            command_sequence=1,
        ),
    )
    binding = await store.next_pool_work(session, executor)
    assert binding is not None
    assert binding.intent_id == accepted.intent_ids[0]
    database_now = (await session.execute(select(func.clock_timestamp()))).scalar_one()
    bootstrap = ExecutableBootstrapProposalV2(
        binding=binding,
        command_sequence=2,
        proposal_epoch=1,
        bootstrap_sha256="7" * 64,
        expires_at=database_now + expires_in,
    )
    await store.propose_bootstrap(session, bootstrap)
    return executor, binding, bootstrap


async def _unresolved_interleaved_global_bootstrap(
    store: CapacityExecutionStore,
    session: AsyncSession,
    *,
    existing_admission_proposal: bool,
):  # type: ignore[no-untyped-def]
    """Create A@1 observed, B@2 accepted, A@3 accepted in one allocation."""

    active, allocation_epoch = await _active_batched_plan(session, slots=3)
    executor = executor_binding("gb10")
    await _heartbeat(store, session, active, pool_id="gb10")
    first_rank, interleaved_rank, trailing_rank = (
        await _interleave_other_subject_between_batched_shapes(
            session,
            allocation_epoch=allocation_epoch,
        )
    )
    first = await store.next_pool_work(session, executor)
    assert isinstance(first, ExecutableReservationProposalV2)
    await store.accept_reservation(
        session,
        ExecutableReservationAcceptanceV2(
            execution=first.execution,
            tranche_id=first.tranche_id,
            proposal_digest=store.contract_digest(first),
            pool_id=executor.pool_id,
            pool_generation=executor.pool_generation,
            executor_id=executor.executor_id,
            executor_incarnation=executor.executor_incarnation,
            command_sequence=1,
        ),
    )
    first_rows = tuple(
        (
            await session.execute(
                select(CapacityExecutableIntent)
                .where(CapacityExecutableIntent.tranche_id == first.tranche_id)
                .order_by(CapacityExecutableIntent.launch_rank)
            )
        )
        .scalars()
        .all()
    )
    assert tuple(row.launch_rank for row in first_rows) == (1, 3)
    first_binding = ExecutableIntentBindingV2.model_validate_json(
        json.dumps(first_rows[0].binding_payload)
    )
    trailing_binding = ExecutableIntentBindingV2.model_validate_json(
        json.dumps(first_rows[1].binding_payload)
    )
    next_command_sequence = 2
    if existing_admission_proposal:
        database_now = (await session.execute(select(func.clock_timestamp()))).scalar_one()
        bootstrap = ExecutableBootstrapProposalV2(
            binding=first_binding,
            command_sequence=next_command_sequence,
            proposal_epoch=1,
            bootstrap_sha256="7" * 64,
            expires_at=database_now + timedelta(minutes=1),
        )
        await store.propose_bootstrap(session, bootstrap)
        await store.acknowledge_bootstrap(
            session,
            ExecutableBootstrapAcknowledgementV2(
                binding=first_binding,
                proposal_epoch=bootstrap.proposal_epoch,
                proposal_digest=store.contract_digest(bootstrap),
                reporter_incarnation=demand_snapshot().reporter_incarnation,
                bootstrap_registration_epoch=1,
                bootstrap_evidence_sha256="8" * 64,
                protected_admission_sha256="3" * 64,
            ),
            actor="development",
            idempotency_key=UUID(int=995),
        )
        next_command_sequence += 1

    await _test_only_update_without_guard(
        session,
        table_name="capacity_executable_intents",
        trigger_name="capacity_executable_intent_mutation_guard",
        statement=(
            update(CapacityExecutableIntent)
            .where(CapacityExecutableIntent.intent_id == first_binding.intent_id)
            .values(state="observed", observed_state="active", inventory_sequence=2)
        ),
    )
    session.expire_all()
    middle = await store.next_pool_work(session, executor)
    assert isinstance(middle, ExecutableReservationProposalV2)
    assert tuple(shape.shape_instance_id for shape in middle.shapes) == (
        str(interleaved_rank["shape_instance_id"]),
    )
    await store.accept_reservation(
        session,
        ExecutableReservationAcceptanceV2(
            execution=middle.execution,
            tranche_id=middle.tranche_id,
            proposal_digest=store.contract_digest(middle),
            pool_id=executor.pool_id,
            pool_generation=executor.pool_generation,
            executor_id=executor.executor_id,
            executor_incarnation=executor.executor_incarnation,
            command_sequence=next_command_sequence,
        ),
    )
    next_command_sequence += 1
    global_rows = tuple(
        (
            await session.execute(
                select(CapacityExecutableIntent)
                .where(
                    CapacityExecutableIntent.execution_epoch
                    == trailing_binding.execution.execution_epoch,
                    CapacityExecutableIntent.allocation_epoch
                    == trailing_binding.execution.allocation_epoch,
                )
                .order_by(CapacityExecutableIntent.launch_rank)
            )
        )
        .scalars()
        .all()
    )
    assert tuple((row.launch_rank, row.state) for row in global_rows) == (
        (int(first_rank["rank"]), "observed"),
        (int(interleaved_rank["rank"]), "accepted"),
        (int(trailing_rank["rank"]), "accepted"),
    )
    return trailing_binding, next_command_sequence


async def _insert_test_bootstrap_proposal(
    store: CapacityExecutionStore,
    session: AsyncSession,
    *,
    binding: ExecutableIntentBindingV2,
    command_sequence: int,
) -> ExecutableBootstrapProposalV2:
    database_now = (await session.execute(select(func.clock_timestamp()))).scalar_one()
    proposal = ExecutableBootstrapProposalV2(
        binding=binding,
        command_sequence=command_sequence,
        proposal_epoch=1,
        bootstrap_sha256="9" * 64,
        expires_at=database_now + timedelta(minutes=1),
    )
    session.add(
        CapacityExecutableBootstrapProposal(
            intent_id=binding.intent_id,
            execution_epoch=binding.execution.execution_epoch,
            execution_manifest_sha256=binding.execution.execution_manifest_sha256,
            proposal_epoch=proposal.proposal_epoch,
            command_sequence=proposal.command_sequence,
            bootstrap_sha256=proposal.bootstrap_sha256,
            expires_at=proposal.expires_at,
            proposal_digest=store.contract_digest(proposal),
            proposal_payload=proposal.model_dump(mode="json", exclude_none=False),
        )
    )
    await session.flush()
    return proposal


async def test_bootstrap_proposal_rejects_unresolved_interleaved_global_rank(
    capacity_session: AsyncSession,
) -> None:
    """Removing the proposal gate must let A@3 bypass unresolved B@2."""

    store = CapacityExecutionStore()
    binding, command_sequence = await _unresolved_interleaved_global_bootstrap(
        store,
        capacity_session,
        existing_admission_proposal=False,
    )
    database_now = (
        await capacity_session.execute(select(func.clock_timestamp()))
    ).scalar_one()
    proposal = ExecutableBootstrapProposalV2(
        binding=binding,
        command_sequence=command_sequence,
        proposal_epoch=1,
        bootstrap_sha256="9" * 64,
        expires_at=database_now + timedelta(minutes=1),
    )

    with pytest.raises(ExecutionConflictError, match="earlier global launch"):
        await store.propose_bootstrap(capacity_session, proposal)


async def test_bootstrap_acknowledgement_rejects_unresolved_interleaved_global_rank(
    capacity_session: AsyncSession,
) -> None:
    """Removing the acknowledgement gate must let A@3 bypass unresolved B@2."""

    store = CapacityExecutionStore()
    binding, command_sequence = await _unresolved_interleaved_global_bootstrap(
        store,
        capacity_session,
        existing_admission_proposal=False,
    )
    proposal = await _insert_test_bootstrap_proposal(
        store,
        capacity_session,
        binding=binding,
        command_sequence=command_sequence,
    )

    with pytest.raises(ExecutionConflictError, match="earlier global launch"):
        await store.acknowledge_bootstrap(
            capacity_session,
            ExecutableBootstrapAcknowledgementV2(
                binding=binding,
                proposal_epoch=proposal.proposal_epoch,
                proposal_digest=store.contract_digest(proposal),
                reporter_incarnation=demand_snapshot().reporter_incarnation,
                bootstrap_registration_epoch=1,
                bootstrap_evidence_sha256="a" * 64,
                protected_admission_sha256="3" * 64,
            ),
            actor="development",
            idempotency_key=UUID(int=998),
        )


async def test_existing_admission_proposal_does_not_bypass_global_bootstrap_order(
    capacity_session: AsyncSession,
) -> None:
    """Returning an existing A plan before the rank check must let A@3 bypass B@2."""

    store = CapacityExecutionStore()
    binding, command_sequence = await _unresolved_interleaved_global_bootstrap(
        store,
        capacity_session,
        existing_admission_proposal=True,
    )
    assert (
        await capacity_session.execute(
            select(func.count(CapacityExecutableAdmissionProposal.id))
        )
    ).scalar_one() == 1
    proposal = await _insert_test_bootstrap_proposal(
        store,
        capacity_session,
        binding=binding,
        command_sequence=command_sequence,
    )

    with pytest.raises(ExecutionConflictError, match="earlier global launch"):
        await store.acknowledge_bootstrap(
            capacity_session,
            ExecutableBootstrapAcknowledgementV2(
                binding=binding,
                proposal_epoch=proposal.proposal_epoch,
                proposal_digest=store.contract_digest(proposal),
                reporter_incarnation=demand_snapshot().reporter_incarnation,
                bootstrap_registration_epoch=1,
                bootstrap_evidence_sha256="a" * 64,
                protected_admission_sha256="3" * 64,
            ),
            actor="development",
            idempotency_key=UUID(int=998),
        )


async def test_propose_bootstrap_locks_executor_context_before_intent(
    capacity_session: AsyncSession,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = CapacityExecutionStore()
    active, _allocation_epoch = await _active_plan(capacity_session)
    binding = executor_binding("gb10")
    await _heartbeat(store, capacity_session, active, pool_id="gb10")
    proposal = await store.next_pool_work(capacity_session, binding)
    assert isinstance(proposal, ExecutableReservationProposalV2)
    await store.accept_reservation(
        capacity_session,
        ExecutableReservationAcceptanceV2(
            execution=proposal.execution,
            tranche_id=proposal.tranche_id,
            proposal_digest=store.contract_digest(proposal),
            pool_id=binding.pool_id,
            pool_generation=binding.pool_generation,
            executor_id=binding.executor_id,
            executor_incarnation=binding.executor_incarnation,
            command_sequence=1,
        ),
    )
    intent = await store.next_pool_work(capacity_session, binding)
    assert intent is not None
    lock_order = _record_queue_lock_order(monkeypatch, store)

    await store.propose_bootstrap(
        capacity_session,
        ExecutableBootstrapProposalV2(
            binding=intent,
            command_sequence=2,
            proposal_epoch=1,
            bootstrap_sha256="7" * 64,
            expires_at=datetime.now(UTC) + timedelta(minutes=1),
        ),
    )

    assert lock_order[:5] == [
        "authority",
        "epoch",
        "executor-registration",
        "executor-state",
        "intent",
    ]


async def test_executor_cannot_self_register_protected_bootstrap(
    capacity_session: AsyncSession,
) -> None:
    store = CapacityExecutionStore()
    active, _allocation_epoch = await _active_plan(capacity_session)
    binding = executor_binding("gb10")
    await _heartbeat(store, capacity_session, active, pool_id="gb10")
    proposal = await store.next_pool_work(capacity_session, binding)
    assert isinstance(proposal, ExecutableReservationProposalV2)
    await store.accept_reservation(
        capacity_session,
        ExecutableReservationAcceptanceV2(
            execution=proposal.execution,
            tranche_id=proposal.tranche_id,
            proposal_digest=store.contract_digest(proposal),
            pool_id=binding.pool_id,
            pool_generation=binding.pool_generation,
            executor_id=binding.executor_id,
            executor_incarnation=binding.executor_incarnation,
            command_sequence=1,
        ),
    )
    intent = await store.next_pool_work(capacity_session, binding)
    assert intent is not None

    with pytest.raises(
        ExecutionConflictError,
        match="protected bootstrap acknowledgement is required",
    ):
        await store.register_bootstrap(
            capacity_session,
            ExecutableBootstrapRegistrationV2(
                binding=intent,
                command_sequence=2,
                bootstrap_registration_epoch=1,
                bootstrap_evidence_sha256="8" * 64,
            ),
        )


def _admission_acknowledgement(
    proposal: ExecutableAdmissionPlanProposalV2,
    *,
    prepared_plan_digest: str = "9" * 64,
) -> ExecutableAdmissionAcknowledgementV2:
    anchor = proposal.shapes[0].binding
    assignments = tuple(
        ProtectedAdmissionAssignmentV2(
            transition_id=uuid5(UUID(int=996), f"transition:{allowance.allowance_id}"),
            allowance_id=allowance.allowance_id,
            protected_attempt_id=allowance.protected_attempt_id,
            execution_generation=1,
            requirements_digest="a" * 64,
            shape_instance_id=allowance.shape_instance_id,
            shape_slot_index=allowance.shape_slot_index,
            submission_intent_id=allowance.submission_intent_id,
            lifecycle_sequence=1,
        )
        for allowance in proposal.allowances
    )
    return ExecutableAdmissionAcknowledgementV2(
        execution=anchor.execution,
        tranche_id=anchor.tranche_id,
        proposal_id=proposal.proposal_id,
        plan_id=proposal.plan_id,
        admission_incarnation=proposal.admission_incarnation,
        subject_id=anchor.subject_id,
        subject_incarnation=anchor.subject_incarnation,
        pool_id=cast(Literal["oldlab", "gb10"], anchor.pool_id),
        reporter_incarnation=proposal.reporter_incarnation,
        protected_admission_sha256=proposal.protected_admission_sha256,
        proposal_digest=canonical_executable_digest(proposal),
        prepared_plan_digest=prepared_plan_digest,
        assignment_count=len(assignments),
        assignments=assignments,
    )


async def _bootstrap_acknowledged_admission(
    store: CapacityExecutionStore,
    session: AsyncSession,
    *,
    expires_in: timedelta = timedelta(minutes=1),
):  # type: ignore[no-untyped-def]
    executor, binding, bootstrap = await _proposed_bootstrap(
        store,
        session,
        expires_in=expires_in,
    )
    await store.acknowledge_bootstrap(
        session,
        ExecutableBootstrapAcknowledgementV2(
            binding=binding,
            proposal_epoch=bootstrap.proposal_epoch,
            proposal_digest=store.contract_digest(bootstrap),
            reporter_incarnation=demand_snapshot().reporter_incarnation,
            bootstrap_registration_epoch=1,
            bootstrap_evidence_sha256="8" * 64,
            protected_admission_sha256="3" * 64,
        ),
        actor="development",
        idempotency_key=UUID(int=995),
    )
    proposal = await store.next_subject_admission_plan(
        session,
        subject_id=binding.subject_id,
        subject_incarnation=binding.subject_incarnation,
        reporter_incarnation=demand_snapshot().reporter_incarnation,
    )
    assert proposal is not None
    return executor, binding, proposal


async def test_admission_generation_rejects_work_above_shared_response_bound(
    capacity_session: AsyncSession,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An undeliverable complete batch must never reach durable proposal storage."""

    monkeypatch.setattr(
        executable_contracts_module,
        "MAX_EXECUTABLE_ADMISSION_WORK_BYTES",
        1,
    )
    store = CapacityExecutionStore()
    with pytest.raises(ExecutionConflictError, match="response byte bound"):
        await _bootstrap_acknowledged_admission(store, capacity_session)

    assert (
        await capacity_session.execute(
            select(func.count(CapacityExecutableAdmissionProposal.id))
        )
    ).scalar_one() == 0


async def _batched_admission_proposal(
    store: CapacityExecutionStore,
    session: AsyncSession,
):  # type: ignore[no-untyped-def]
    active, _allocation_epoch = await _active_batched_plan(session)
    executor = executor_binding("gb10")
    await _heartbeat(store, session, active, pool_id="gb10")
    reservation = await store.next_pool_work(session, executor)
    assert isinstance(reservation, ExecutableReservationProposalV2)
    await store.accept_reservation(
        session,
        ExecutableReservationAcceptanceV2(
            execution=reservation.execution,
            tranche_id=reservation.tranche_id,
            proposal_digest=store.contract_digest(reservation),
            pool_id=executor.pool_id,
            pool_generation=executor.pool_generation,
            executor_id=executor.executor_id,
            executor_incarnation=executor.executor_incarnation,
            command_sequence=1,
        ),
    )
    intent_rows = tuple(
        (
            await session.execute(
                select(CapacityExecutableIntent)
                .where(CapacityExecutableIntent.tranche_id == reservation.tranche_id)
                .order_by(CapacityExecutableIntent.launch_rank)
            )
        )
        .scalars()
        .all()
    )
    bindings = tuple(
        executable_contracts_module.ExecutableIntentBindingV2.model_validate_json(
            json.dumps(row.binding_payload)
        )
        for row in intent_rows
    )
    database_now = (await session.execute(select(func.clock_timestamp()))).scalar_one()
    bootstrap = ExecutableBootstrapProposalV2(
        binding=bindings[0],
        command_sequence=2,
        proposal_epoch=1,
        bootstrap_sha256="7" * 64,
        expires_at=database_now + timedelta(minutes=1),
    )
    await store.propose_bootstrap(session, bootstrap)
    await store.acknowledge_bootstrap(
        session,
        ExecutableBootstrapAcknowledgementV2(
            binding=bindings[0],
            proposal_epoch=bootstrap.proposal_epoch,
            proposal_digest=store.contract_digest(bootstrap),
            reporter_incarnation=demand_snapshot().reporter_incarnation,
            bootstrap_registration_epoch=1,
            bootstrap_evidence_sha256="8" * 64,
            protected_admission_sha256="3" * 64,
        ),
        actor="development",
        idempotency_key=UUID(int=995),
    )
    proposal = await store.next_subject_admission_plan(
        session,
        subject_id=bindings[0].subject_id,
        subject_incarnation=bindings[0].subject_incarnation,
        reporter_incarnation=demand_snapshot().reporter_incarnation,
    )
    assert proposal is not None
    assert len(proposal.shapes) == 2
    return executor, bindings, proposal


async def test_bootstrap_admission_convergence_uses_canonical_lock_order(
    capacity_session: AsyncSession,
) -> None:
    """Taking an intent before the shared proposal can deadlock admission acknowledgement."""

    store = CapacityExecutionStore()
    _executor, binding, bootstrap = await _proposed_bootstrap(store, capacity_session)
    acknowledgement = ExecutableBootstrapAcknowledgementV2(
        binding=binding,
        proposal_epoch=bootstrap.proposal_epoch,
        proposal_digest=store.contract_digest(bootstrap),
        reporter_incarnation=demand_snapshot().reporter_incarnation,
        bootstrap_registration_epoch=1,
        bootstrap_evidence_sha256="8" * 64,
        protected_admission_sha256="3" * 64,
    )
    lock_order, engine, listener = _record_admission_lock_order(capacity_session)
    try:
        await store.acknowledge_bootstrap(
            capacity_session,
            acknowledgement,
            actor="development",
            idempotency_key=UUID(int=995),
        )
    finally:
        event.remove(engine.sync_engine, "before_cursor_execute", listener)

    assert lock_order == [
        "capacity_authority_state",
        "capacity_execution_epochs",
        "capacity_execution_executors",
        "capacity_executable_executor_states",
        "capacity_allocation_epochs",
        "capacity_demand_reporters",
        "capacity_executable_intents",
        "capacity_executable_bootstrap_proposals",
        "capacity_executable_admission_proposals",
        "capacity_executable_admission_acknowledgements",
    ]


async def test_admission_acknowledgement_uses_canonical_lock_order(
    capacity_session: AsyncSession,
) -> None:
    """Taking the proposal before the execution fence and intents creates a wait cycle."""

    store = CapacityExecutionStore()
    _executor, _binding, proposal = await _bootstrap_acknowledged_admission(
        store, capacity_session
    )
    lock_order, engine, listener = _record_admission_lock_order(capacity_session)
    try:
        await store.acknowledge_admission_plan(
            capacity_session,
            _admission_acknowledgement(proposal),
            actor="development",
            idempotency_key=UUID(int=997),
        )
    finally:
        event.remove(engine.sync_engine, "before_cursor_execute", listener)

    assert lock_order == [
        "capacity_authority_state",
        "capacity_execution_epochs",
        "capacity_allocation_epochs",
        "capacity_demand_reporters",
        "capacity_executable_intents",
        "capacity_executable_admission_proposals",
        "capacity_executable_admission_acknowledgements",
    ]


async def test_bootstrap_and_admission_acknowledgements_do_not_deadlock(
    isolated_capacity_postgres_url: str,
) -> None:
    """Proposal-first admission locking must not deadlock bootstrap convergence."""

    engine = create_async_engine(
        isolated_capacity_postgres_url,
        isolation_level="SERIALIZABLE",
    )
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    try:
        async with engine.connect() as setup_connection:
            setup_session = AsyncSession(
                bind=setup_connection,
                expire_on_commit=False,
                join_transaction_mode="create_savepoint",
            )
            store = CapacityExecutionStore()
            _executor, bindings, proposal = await _batched_admission_proposal(
                store,
                setup_session,
            )
            await _test_only_update_without_guard(
                setup_session,
                table_name="capacity_executable_intents",
                trigger_name="capacity_executable_intent_mutation_guard",
                statement=(
                    update(CapacityExecutableIntent)
                    .where(CapacityExecutableIntent.intent_id == bindings[0].intent_id)
                    .values(
                        state="observed",
                        observed_state="active",
                        inventory_sequence=2,
                    )
                ),
            )
            setup_session.expire_all()
            database_now = (
                await setup_session.execute(select(func.clock_timestamp()))
            ).scalar_one()
            second_bootstrap = ExecutableBootstrapProposalV2(
                binding=bindings[1],
                command_sequence=3,
                proposal_epoch=1,
                bootstrap_sha256="9" * 64,
                expires_at=database_now + timedelta(minutes=1),
            )
            await store.propose_bootstrap(setup_session, second_bootstrap)
            await setup_session.commit()
            await setup_session.close()

        second_acknowledgement = ExecutableBootstrapAcknowledgementV2(
            binding=bindings[1],
            proposal_epoch=second_bootstrap.proposal_epoch,
            proposal_digest=store.contract_digest(second_bootstrap),
            reporter_incarnation=demand_snapshot().reporter_incarnation,
            bootstrap_registration_epoch=1,
            bootstrap_evidence_sha256="a" * 64,
            protected_admission_sha256="3" * 64,
        )
        admission_acknowledgement = _admission_acknowledgement(proposal)
        forged_assignment = admission_acknowledgement.assignments[0].model_copy(
            update={"protected_attempt_id": UUID(int=999)}
        )
        forged_admission_acknowledgement = admission_acknowledgement.model_copy(
            update={
                "assignments": (
                    forged_assignment,
                    *admission_acknowledgement.assignments[1:],
                )
            }
        )

        async with (
            session_factory() as blocker_session,
            session_factory() as admission_session,
            session_factory() as bootstrap_session,
            session_factory() as observer_session,
        ):
            await blocker_session.execute(
                select(CapacityExecutableAdmissionProposal)
                .where(
                    CapacityExecutableAdmissionProposal.proposal_id
                    == proposal.proposal_id
                )
                .with_for_update()
            )
            admission_pid = (
                await admission_session.execute(text("SELECT pg_backend_pid()"))
            ).scalar_one()
            bootstrap_pid = (
                await bootstrap_session.execute(text("SELECT pg_backend_pid()"))
            ).scalar_one()

            async def wait_for_lock(
                pid: int,
                task: asyncio.Task[object],
                *,
                operation: str,
            ) -> None:
                async with asyncio.timeout(5):
                    while True:
                        blockers = (
                            await observer_session.execute(
                                text("SELECT pg_blocking_pids(:pid)"),
                                {"pid": pid},
                            )
                        ).scalar_one()
                        if blockers:
                            return
                        if task.done():
                            pytest.fail(
                                f"{operation} completed before the intended lock overlap: "
                                f"{task.exception()!r}"
                            )
                        await asyncio.sleep(0.01)

            admission_task = asyncio.create_task(
                CapacityExecutionStore().acknowledge_admission_plan(
                    admission_session,
                    forged_admission_acknowledgement,
                    actor="development",
                    idempotency_key=UUID(int=997),
                )
            )
            await wait_for_lock(
                admission_pid,
                admission_task,
                operation="admission acknowledgement",
            )
            bootstrap_task = asyncio.create_task(
                CapacityExecutionStore().acknowledge_bootstrap(
                    bootstrap_session,
                    second_acknowledgement,
                    actor="development",
                    idempotency_key=UUID(int=998),
                )
            )
            await wait_for_lock(
                bootstrap_pid,
                bootstrap_task,
                operation="bootstrap acknowledgement",
            )

            await blocker_session.commit()
            async with asyncio.timeout(5):
                admission_result, bootstrap_result = await asyncio.gather(
                    admission_task,
                    bootstrap_task,
                    return_exceptions=True,
                )

            assert isinstance(admission_result, ExecutionConflictError)
            assert str(admission_result) == (
                "admission acknowledgement assignment set changed"
            )
            bootstrap_sqlstate = getattr(
                getattr(getattr(bootstrap_result, "__cause__", None), "orig", None),
                "sqlstate",
                None,
            )
            assert not isinstance(bootstrap_result, BaseException), (
                f"{bootstrap_result!r} (SQLSTATE {bootstrap_sqlstate})"
            )
            assert bootstrap_result.intent_id == bindings[1].intent_id
    finally:
        await engine.dispose()


async def test_admission_acknowledgement_rejects_forged_tranche_before_intent_locks(
    capacity_session: AsyncSession,
) -> None:
    """Locking the caller tranche before proposal validation lets reporters touch it."""

    store = CapacityExecutionStore()
    _executor, _binding, proposal = await _bootstrap_acknowledged_admission(
        store, capacity_session
    )
    forged = _admission_acknowledgement(proposal).model_copy(
        update={"tranche_id": UUID(int=998)}
    )
    lock_order, engine, listener = _record_admission_lock_order(capacity_session)
    try:
        with pytest.raises(ExecutionConflictError, match="binding changed"):
            await store.acknowledge_admission_plan(
                capacity_session,
                forged,
                actor="development",
                idempotency_key=UUID(int=997),
            )
    finally:
        event.remove(engine.sync_engine, "before_cursor_execute", listener)

    assert lock_order == []


async def test_admission_acknowledgement_is_the_launch_permit_gate(
    capacity_session: AsyncSession,
) -> None:
    """Removing the protected-plan gate must expose launch after bootstrap alone."""

    store = CapacityExecutionStore()
    executor, binding, bootstrap = await _proposed_bootstrap(store, capacity_session)

    intent = (
        await capacity_session.execute(
            select(CapacityExecutableIntent).where(
                CapacityExecutableIntent.intent_id == binding.intent_id
            )
        )
    ).scalar_one()
    assert intent.state == "accepted"
    assert await store.next_pool_work(capacity_session, executor) is None
    subject_work = await store.next_subject_bootstrap(
        capacity_session,
        subject_id=binding.subject_id,
        subject_incarnation=binding.subject_incarnation,
        reporter_incarnation=demand_snapshot().reporter_incarnation,
    )
    assert subject_work == bootstrap

    await store.acknowledge_bootstrap(
        capacity_session,
        ExecutableBootstrapAcknowledgementV2(
            binding=binding,
            proposal_epoch=bootstrap.proposal_epoch,
            proposal_digest=store.contract_digest(bootstrap),
            reporter_incarnation=demand_snapshot().reporter_incarnation,
            bootstrap_registration_epoch=1,
            bootstrap_evidence_sha256="8" * 64,
            protected_admission_sha256="3" * 64,
        ),
        actor="development",
        idempotency_key=UUID(int=995),
    )

    await capacity_session.refresh(intent)
    assert intent.state == "bootstrap-acknowledged"
    assert intent.launch_ready_at is None
    assert await store.next_pool_work(capacity_session, executor) is None

    admission = await store.next_subject_admission_plan(
        capacity_session,
        subject_id=binding.subject_id,
        subject_incarnation=binding.subject_incarnation,
        reporter_incarnation=demand_snapshot().reporter_incarnation,
    )
    assert admission is not None
    assert tuple(shape.binding for shape in admission.shapes) == (binding,)
    assert admission.reporter_incarnation == demand_snapshot().reporter_incarnation
    assert admission.protected_admission_sha256 == "3" * 64
    assert len(admission.allowances) == 1
    assert admission.allowances[0].protected_attempt_id == UUID(int=991)
    assert admission.allowances[0].shape_instance_id == binding.shape_instance_id
    assert admission.allowances[0].shape_slot_index == 0
    assert admission.allowances[0].submission_intent_id == binding.intent_id

    acknowledgement = _admission_acknowledgement(admission)
    registered = await store.acknowledge_admission_plan(
        capacity_session,
        acknowledgement,
        actor="development",
        idempotency_key=UUID(int=997),
    )
    assert registered.proposal_id == admission.proposal_id
    assert registered.prepared_plan_digest == "9" * 64
    assert registered.replayed is False

    await capacity_session.refresh(intent)
    assert intent.state == "launch-ready"
    assert intent.launch_ready_at is not None
    permit = await store.next_pool_work(capacity_session, executor)
    assert permit is not None
    assert permit.binding.intent_id == binding.intent_id
    assert permit.bootstrap_registration_epoch == 1
    assert permit.bootstrap_evidence_sha256 == "8" * 64


async def test_admission_acknowledgement_rejects_incomplete_assignment_set_atomically(
    capacity_session: AsyncSession,
) -> None:
    """Dropping one manager allowance must leave every covered intent non-launchable."""

    store = CapacityExecutionStore()
    executor, binding, bootstrap = await _proposed_bootstrap(store, capacity_session)
    await store.acknowledge_bootstrap(
        capacity_session,
        ExecutableBootstrapAcknowledgementV2(
            binding=binding,
            proposal_epoch=bootstrap.proposal_epoch,
            proposal_digest=store.contract_digest(bootstrap),
            reporter_incarnation=demand_snapshot().reporter_incarnation,
            bootstrap_registration_epoch=1,
            bootstrap_evidence_sha256="8" * 64,
            protected_admission_sha256="3" * 64,
        ),
        actor="development",
        idempotency_key=UUID(int=995),
    )
    proposal = await store.next_subject_admission_plan(
        capacity_session,
        subject_id=binding.subject_id,
        subject_incarnation=binding.subject_incarnation,
        reporter_incarnation=demand_snapshot().reporter_incarnation,
    )
    assert proposal is not None
    acknowledgement = _admission_acknowledgement(proposal).model_copy(
        update={"assignment_count": 0, "assignments": ()}
    )

    with pytest.raises(ExecutionConflictError, match="assignment"):
        await store.acknowledge_admission_plan(
            capacity_session,
            acknowledgement,
            actor="development",
            idempotency_key=UUID(int=997),
        )

    row = (
        await capacity_session.execute(
            select(CapacityExecutableIntent).where(
                CapacityExecutableIntent.intent_id == binding.intent_id
            )
        )
    ).scalar_one()
    assert row.state == "bootstrap-acknowledged"
    assert await store.next_pool_work(capacity_session, executor) is None
    assert (
        await capacity_session.execute(
            select(func.count(CapacityExecutableAdmissionAcknowledgement.id))
        )
    ).scalar_one() == 0


@pytest.mark.parametrize(
    ("forgery", "expected_error"),
    (
        ("protected-root", "binding"),
        ("protected-attempt", "assignment set"),
        ("shape-slot", "assignment set"),
        ("submission-intent", "assignment set"),
    ),
)
async def test_admission_acknowledgement_rejects_changed_protected_binding_atomically(
    capacity_session: AsyncSession,
    forgery: str,
    expected_error: str,
) -> None:
    """Only the exact protected root and manager allowance set may open launch."""

    store = CapacityExecutionStore()
    executor, binding, proposal = await _bootstrap_acknowledged_admission(
        store, capacity_session
    )
    acknowledgement = _admission_acknowledgement(proposal)
    if forgery == "protected-root":
        changed = acknowledgement.model_copy(
            update={"protected_admission_sha256": "f" * 64}
        )
    else:
        assignment = acknowledgement.assignments[0]
        if forgery == "protected-attempt":
            assignment = assignment.model_copy(update={"protected_attempt_id": UUID(int=999)})
        elif forgery == "shape-slot":
            assignment = assignment.model_copy(update={"shape_slot_index": 1})
        else:
            assignment = assignment.model_copy(update={"submission_intent_id": UUID(int=999)})
        changed = acknowledgement.model_copy(update={"assignments": (assignment,)})

    with pytest.raises(ExecutionConflictError, match=expected_error):
        await store.acknowledge_admission_plan(
            capacity_session,
            changed,
            actor="development",
            idempotency_key=UUID(int=997),
        )

    row = (
        await capacity_session.execute(
            select(CapacityExecutableIntent).where(
                CapacityExecutableIntent.intent_id == binding.intent_id
            )
        )
    ).scalar_one()
    assert row.state == "bootstrap-acknowledged"
    assert await store.next_pool_work(capacity_session, executor) is None
    assert (
        await capacity_session.execute(
            select(func.count(CapacityExecutableAdmissionAcknowledgement.id))
        )
    ).scalar_one() == 0


async def test_admission_acknowledgement_replay_is_exact_and_cannot_equivocate(
    capacity_session: AsyncSession,
) -> None:
    """Changing an acknowledged local plan under one key must fail closed."""

    store = CapacityExecutionStore()
    _executor, _binding, proposal = await _bootstrap_acknowledged_admission(
        store, capacity_session
    )
    acknowledgement = _admission_acknowledgement(proposal)
    first = await store.acknowledge_admission_plan(
        capacity_session,
        acknowledgement,
        actor="development",
        idempotency_key=UUID(int=997),
    )
    replay = await store.acknowledge_admission_plan(
        capacity_session,
        acknowledgement,
        actor="development",
        idempotency_key=UUID(int=997),
    )
    assert replay.receipt_digest == first.receipt_digest
    assert replay.replayed is True

    changed = acknowledgement.model_copy(update={"prepared_plan_digest": "b" * 64})
    with pytest.raises(ExecutionConflictError, match="idempotency key"):
        await store.acknowledge_admission_plan(
            capacity_session,
            changed,
            actor="development",
            idempotency_key=UUID(int=997),
        )

    changed_assignment = acknowledgement.assignments[0].model_copy(
        update={"execution_generation": acknowledgement.assignments[0].execution_generation + 1}
    )
    changed_local_fact = acknowledgement.model_copy(
        update={"assignments": (changed_assignment,)}
    )
    with pytest.raises(ExecutionConflictError, match="idempotency key"):
        await store.acknowledge_admission_plan(
            capacity_session,
            changed_local_fact,
            actor="development",
            idempotency_key=UUID(int=997),
        )


async def test_stale_allocation_rejects_admission_acknowledgement_atomically(
    capacity_session: AsyncSession,
) -> None:
    """A later sealed allocation must fence an otherwise exact local plan."""

    store = CapacityExecutionStore()
    executor, binding, proposal = await _bootstrap_acknowledged_admission(
        store, capacity_session
    )
    await _clone_sealed_epoch(
        capacity_session,
        allocation_epoch=binding.execution.allocation_epoch,
        input_valid_until=datetime.now(UTC) + timedelta(minutes=5),
    )

    with pytest.raises(ExecutionConflictError, match="allocation"):
        await store.acknowledge_admission_plan(
            capacity_session,
            _admission_acknowledgement(proposal),
            actor="development",
            idempotency_key=UUID(int=997),
        )
    row = (
        await capacity_session.execute(
            select(CapacityExecutableIntent).where(
                CapacityExecutableIntent.intent_id == binding.intent_id
            )
        )
    ).scalar_one()
    assert row.state == "bootstrap-acknowledged"
    work = await store.next_pool_work(capacity_session, executor)
    assert isinstance(work, ExecutableIntentCloseV2)
    assert work.binding.intent_id == binding.intent_id


async def test_expired_admission_plan_closes_unlaunched_intent(
    capacity_session: AsyncSession,
) -> None:
    """Reinterpreting an expired protected plan as another bootstrap must fail."""

    store = CapacityExecutionStore()
    executor, binding, proposal = await _bootstrap_acknowledged_admission(
        store,
        capacity_session,
        expires_in=timedelta(seconds=1),
    )
    await capacity_session.execute(text("SELECT pg_sleep(1.1)"))

    with pytest.raises(ExecutionConflictError, match="expired"):
        await store.acknowledge_admission_plan(
            capacity_session,
            _admission_acknowledgement(proposal),
            actor="development",
            idempotency_key=UUID(int=997),
        )
    work = await store.next_pool_work(capacity_session, executor)
    assert isinstance(work, ExecutableIntentCloseV2)
    assert work.binding.intent_id == binding.intent_id


async def test_batched_admission_ack_covers_later_bootstrap_atomically(
    capacity_session: AsyncSession,
) -> None:
    """Splitting one tranche into per-intent protected plans must fail this test."""

    store = CapacityExecutionStore()
    active, _allocation_epoch = await _active_batched_plan(capacity_session)
    executor = executor_binding("gb10")
    await _heartbeat(store, capacity_session, active, pool_id="gb10")
    reservation = await store.next_pool_work(capacity_session, executor)
    assert isinstance(reservation, ExecutableReservationProposalV2)
    accepted = await store.accept_reservation(
        capacity_session,
        ExecutableReservationAcceptanceV2(
            execution=reservation.execution,
            tranche_id=reservation.tranche_id,
            proposal_digest=store.contract_digest(reservation),
            pool_id=executor.pool_id,
            pool_generation=executor.pool_generation,
            executor_id=executor.executor_id,
            executor_incarnation=executor.executor_incarnation,
            command_sequence=1,
        ),
    )
    assert len(accepted.intent_ids) == 2
    intent_rows = tuple(
        (
            await capacity_session.execute(
                select(CapacityExecutableIntent)
                .where(CapacityExecutableIntent.tranche_id == reservation.tranche_id)
                .order_by(CapacityExecutableIntent.launch_rank)
            )
        )
        .scalars()
        .all()
    )
    bindings = tuple(
        executable_contracts_module.ExecutableIntentBindingV2.model_validate_json(
            json.dumps(row.binding_payload)
        )
        for row in intent_rows
    )
    database_now = (
        await capacity_session.execute(select(func.clock_timestamp()))
    ).scalar_one()
    first_bootstrap = ExecutableBootstrapProposalV2(
        binding=bindings[0],
        command_sequence=2,
        proposal_epoch=1,
        bootstrap_sha256="7" * 64,
        expires_at=database_now + timedelta(minutes=1),
    )
    await store.propose_bootstrap(capacity_session, first_bootstrap)
    await store.acknowledge_bootstrap(
        capacity_session,
        ExecutableBootstrapAcknowledgementV2(
            binding=bindings[0],
            proposal_epoch=1,
            proposal_digest=store.contract_digest(first_bootstrap),
            reporter_incarnation=demand_snapshot().reporter_incarnation,
            bootstrap_registration_epoch=1,
            bootstrap_evidence_sha256="8" * 64,
            protected_admission_sha256="3" * 64,
        ),
        actor="development",
        idempotency_key=UUID(int=995),
    )
    proposal = await store.next_subject_admission_plan(
        capacity_session,
        subject_id=bindings[0].subject_id,
        subject_incarnation=bindings[0].subject_incarnation,
        reporter_incarnation=demand_snapshot().reporter_incarnation,
    )
    assert proposal is not None
    assert {shape.binding.intent_id for shape in proposal.shapes} == {
        binding.intent_id for binding in bindings
    }
    assert len(proposal.allowances) == 2
    await store.acknowledge_admission_plan(
        capacity_session,
        _admission_acknowledgement(proposal),
        actor="development",
        idempotency_key=UUID(int=997),
    )
    await _test_only_update_without_guard(
        capacity_session,
        table_name="capacity_executable_intents",
        trigger_name="capacity_executable_intent_mutation_guard",
        statement=(
            update(CapacityExecutableIntent)
            .where(CapacityExecutableIntent.intent_id == bindings[0].intent_id)
            .values(
                state="observed",
                observed_state="active",
                inventory_sequence=2,
            )
        ),
    )
    capacity_session.expire_all()

    second_bootstrap = ExecutableBootstrapProposalV2(
        binding=bindings[1],
        command_sequence=3,
        proposal_epoch=1,
        bootstrap_sha256="9" * 64,
        expires_at=database_now + timedelta(minutes=1),
    )
    await store.propose_bootstrap(capacity_session, second_bootstrap)
    second_acknowledgement = ExecutableBootstrapAcknowledgementV2(
        binding=bindings[1],
        proposal_epoch=1,
        proposal_digest=store.contract_digest(second_bootstrap),
        reporter_incarnation=demand_snapshot().reporter_incarnation,
        bootstrap_registration_epoch=1,
        bootstrap_evidence_sha256="a" * 64,
        protected_admission_sha256="3" * 64,
    )
    with pytest.raises(ExecutionConflictError, match="registration epoch"):
        await store.acknowledge_bootstrap(
            capacity_session,
            second_acknowledgement.model_copy(
                update={"bootstrap_registration_epoch": 2}
            ),
            actor="development",
            idempotency_key=UUID(int=998),
        )

    await store.acknowledge_bootstrap(
        capacity_session,
        second_acknowledgement,
        actor="development",
        idempotency_key=UUID(int=998),
    )
    rows = tuple(
        (
            await capacity_session.execute(
                select(CapacityExecutableIntent)
                .where(CapacityExecutableIntent.tranche_id == reservation.tranche_id)
                .order_by(CapacityExecutableIntent.launch_rank)
            )
        )
        .scalars()
        .all()
    )
    assert tuple(row.state for row in rows) == ("observed", "launch-ready")
    permit = await store.next_pool_work(capacity_session, executor)
    assert isinstance(permit, executable_contracts_module.ExecutableLaunchPermitV2)
    assert permit.binding.intent_id == bindings[1].intent_id


async def _real_manager_guard_admission_fixture(
    capacity_session: AsyncSession,
    capacity_guard_database: dict[str, object],
    *,
    expires_in: timedelta = timedelta(minutes=1),
):  # type: ignore[no-untyped-def]
    store = CapacityExecutionStore()
    admission_candidate = executable_contracts_module.CandidateBindingV2(
        algorithm="source-sha256",
        identity="1" * 64,
        publication_sha256="1" * 64,
    )
    active, _allocation_epoch = await _active_batched_plan(
        capacity_session,
        candidate=admission_candidate,
    )
    executor = executor_binding("gb10")
    await _heartbeat(store, capacity_session, active, pool_id="gb10")
    reservation = await store.next_pool_work(capacity_session, executor)
    assert isinstance(reservation, ExecutableReservationProposalV2)
    accepted = await store.accept_reservation(
        capacity_session,
        ExecutableReservationAcceptanceV2(
            execution=reservation.execution,
            tranche_id=reservation.tranche_id,
            proposal_digest=store.contract_digest(reservation),
            pool_id=executor.pool_id,
            pool_generation=executor.pool_generation,
            executor_id=executor.executor_id,
            executor_incarnation=executor.executor_incarnation,
            command_sequence=1,
        ),
    )
    assert len(accepted.intent_ids) == 2
    intent_rows = tuple(
        (
            await capacity_session.execute(
                select(CapacityExecutableIntent)
                .where(CapacityExecutableIntent.tranche_id == reservation.tranche_id)
                .order_by(CapacityExecutableIntent.launch_rank)
            )
        )
        .scalars()
        .all()
    )
    bindings = tuple(
        ExecutableIntentBindingV2.model_validate_json(json.dumps(row.binding_payload))
        for row in intent_rows
    )
    reporter_incarnation = demand_snapshot().reporter_incarnation
    protected_admission_sha256 = "3" * 64
    registration, configuration = await _initialize_manager_bound_admission_agent(
        capacity_guard_database,
        binding=bindings[0],
        reporter_incarnation=reporter_incarnation,
        protected_admission_sha256=protected_admission_sha256,
    )
    for protected_attempt_id in (UUID(int=991), UUID(int=992)):
        await _seed_lifecycle_attempt(
            capacity_guard_database,
            protected_attempt_id=protected_attempt_id,
            execution_generation=7,
            required_pool="gb10",
        )
    pending = await _capture_lifecycle(
        capacity_guard_database,
        registration,
        expected_high_water=0,
    )

    database_now = (
        await capacity_session.execute(select(func.clock_timestamp()))
    ).scalar_one()
    bootstrap = ExecutableBootstrapProposalV2(
        binding=bindings[0],
        command_sequence=2,
        proposal_epoch=1,
        bootstrap_sha256="7" * 64,
        expires_at=database_now + expires_in,
    )
    await store.propose_bootstrap(capacity_session, bootstrap)
    async with _serializable_agent_session(capacity_guard_database) as guard_session:
        protected = await ProtectedExecutableBootstrapCoordinator(
            guard_session,
            configuration=configuration,
        ).protect(bootstrap)
    await store.acknowledge_bootstrap(
        capacity_session,
        ExecutableBootstrapAcknowledgementV2(
            binding=bindings[0],
            proposal_epoch=bootstrap.proposal_epoch,
            proposal_digest=store.contract_digest(bootstrap),
            reporter_incarnation=reporter_incarnation,
            bootstrap_registration_epoch=(
                protected.acknowledgement.bootstrap_registration_epoch
            ),
            bootstrap_evidence_sha256=(
                protected.acknowledgement.bootstrap_evidence_sha256
            ),
            protected_admission_sha256=protected_admission_sha256,
        ),
        actor="development",
        idempotency_key=UUID(int=995),
    )
    proposal = await store.next_subject_admission_plan(
        capacity_session,
        subject_id=bindings[0].subject_id,
        subject_incarnation=bindings[0].subject_incarnation,
        reporter_incarnation=reporter_incarnation,
    )
    assert proposal is not None
    assert len(proposal.shapes) == 2
    assert len(proposal.allowances) == 2
    return store, executor, bindings, proposal, registration, configuration, pending


async def test_real_manager_guard_convergence_opens_only_exact_acknowledged_launch(
    capacity_session: AsyncSession,
    capacity_guard_database: dict[str, object],
) -> None:
    """Catch a dropped batched shape or a permit issued before exact local convergence."""

    (
        store,
        executor,
        bindings,
        proposal,
        _registration,
        configuration,
        pending,
    ) = await _real_manager_guard_admission_fixture(
        capacity_session,
        capacity_guard_database,
    )
    before_ack = await store.next_pool_work(capacity_session, executor)
    assert not isinstance(before_ack, ExecutableLaunchPermitV2)

    async with _serializable_agent_session(capacity_guard_database) as guard_session:
        converged = await ProtectedAdmissionPlanCoordinator(
            guard_session,
            configuration=configuration,
        ).converge(proposal, pending)

    acknowledgement = converged.acknowledgement
    assert acknowledgement.assignment_count == 2
    assert {
        (
            assignment.protected_attempt_id,
            assignment.execution_generation,
            assignment.requirements_digest,
            assignment.lifecycle_sequence,
        )
        for assignment in acknowledgement.assignments
    } == {
        (
            UUID(int=991),
            7,
            "ad66ac6aefdd0204ab1fb6747d20c9a1da0c382679e040f0d789c1d265573cb3",
            1,
        ),
        (
            UUID(int=992),
            7,
            "ad66ac6aefdd0204ab1fb6747d20c9a1da0c382679e040f0d789c1d265573cb3",
            1,
        ),
    }
    assert {
        (
            assignment.allowance_id,
            assignment.shape_instance_id,
            assignment.shape_slot_index,
            assignment.submission_intent_id,
        )
        for assignment in acknowledgement.assignments
    } == {
        (
            allowance.allowance_id,
            allowance.shape_instance_id,
            allowance.shape_slot_index,
            allowance.submission_intent_id,
        )
        for allowance in proposal.allowances
    }

    registered = await store.acknowledge_admission_plan(
        capacity_session,
        acknowledgement,
        actor="development",
        idempotency_key=UUID(int=997),
    )
    assert registered.proposal_id == proposal.proposal_id
    async with _guard_owner_session(capacity_guard_database) as (_, _, guard_session):
        assigned = tuple(
            (
                await guard_session.execute(
                    text(
                        "SELECT head.protected_attempt_id, head.transition_id, "
                        "head.transition_sequence, head.lifecycle_state, "
                        "event.execution_generation, event.requirements_digest, "
                        "event.allowance_id, event.plan_id, event.admission_incarnation, "
                        "event.manager_allocation_epoch, event.pool_id, "
                        "event.shape_instance_id, event.submission_intent_id "
                        "FROM loom_capacity_guard.attempt_lifecycle_heads AS head "
                        "JOIN loom_capacity_guard.attempt_lifecycle_events AS event "
                        "ON event.transition_id = head.transition_id "
                        "AND event.protected_attempt_id = head.protected_attempt_id "
                        "ORDER BY head.protected_attempt_id"
                    )
                )
            )
            .mappings()
            .all()
        )
    expected_allowances = {
        allowance.protected_attempt_id: allowance for allowance in proposal.allowances
    }
    expected_assignments = {
        assignment.protected_attempt_id: assignment
        for assignment in acknowledgement.assignments
    }
    assert len(assigned) == 2
    for head in assigned:
        allowance = expected_allowances[head["protected_attempt_id"]]
        assignment = expected_assignments[head["protected_attempt_id"]]
        assert (
            head["transition_id"],
            head["transition_sequence"],
            head["lifecycle_state"],
            head["execution_generation"],
            head["requirements_digest"],
            head["allowance_id"],
            head["plan_id"],
            head["admission_incarnation"],
            head["manager_allocation_epoch"],
            head["pool_id"],
            head["shape_instance_id"],
            head["submission_intent_id"],
        ) == (
            assignment.transition_id,
            1,
            "assigned",
            7,
            "ad66ac6aefdd0204ab1fb6747d20c9a1da0c382679e040f0d789c1d265573cb3",
            allowance.allowance_id,
            proposal.plan_id,
            proposal.admission_incarnation,
            bindings[0].execution.allocation_epoch,
            "gb10",
            allowance.shape_instance_id,
            allowance.submission_intent_id,
        )

    permit = await store.next_pool_work(capacity_session, executor)
    assert isinstance(permit, ExecutableLaunchPermitV2)
    assert permit.binding.intent_id == bindings[0].intent_id


async def test_crash_after_local_commit_replays_expired_cleanup_without_orphaning_attempts(
    capacity_session: AsyncSession,
    capacity_guard_database: dict[str, object],
) -> None:
    """Filtering an expired proposal to null must not strand its protected assignments."""

    (
        store,
        _executor,
        bindings,
        proposal,
        _registration,
        configuration,
        pending,
    ) = await _real_manager_guard_admission_fixture(
        capacity_session,
        capacity_guard_database,
        expires_in=timedelta(seconds=1),
    )
    async with _serializable_agent_session(capacity_guard_database) as guard_session:
        await ProtectedAdmissionPlanCoordinator(
            guard_session,
            configuration=configuration,
        ).converge(proposal, pending)

    await capacity_session.execute(text("SELECT pg_sleep(1.1)"))
    closure = await store.next_subject_admission_plan(
        capacity_session,
        subject_id=bindings[0].subject_id,
        subject_incarnation=bindings[0].subject_incarnation,
        reporter_incarnation=demand_snapshot().reporter_incarnation,
    )
    assert isinstance(
        closure,
        executable_contracts_module.ExecutableAdmissionPlanClosureV2,
    )
    assert closure.proposal == proposal
    assert closure.close_reason == "expired"

    async with _serializable_agent_session(capacity_guard_database) as guard_session:
        first = await ProtectedAdmissionPlanCoordinator(
            guard_session,
            configuration=configuration,
        ).abandon(closure, pending)
    async with _serializable_agent_session(capacity_guard_database) as guard_session:
        replay = await ProtectedAdmissionPlanCoordinator(
            guard_session,
            configuration=configuration,
        ).abandon(closure, pending)

    assert replay == first
    repeated = await store.next_subject_admission_plan(
        capacity_session,
        subject_id=bindings[0].subject_id,
        subject_incarnation=bindings[0].subject_incarnation,
        reporter_incarnation=demand_snapshot().reporter_incarnation,
    )
    assert repeated == closure

    receipt_key = first.idempotency_key
    registered = await store.acknowledge_admission_plan_closure(
        capacity_session,
        first.acknowledgement,
        actor="development",
        idempotency_key=receipt_key,
    )
    registered_replay = await store.acknowledge_admission_plan_closure(
        capacity_session,
        first.acknowledgement,
        actor="development",
        idempotency_key=receipt_key,
    )
    assert registered.closure_id == closure.closure_id
    assert registered.disposition_kind == first.acknowledgement.disposition_kind
    assert registered.disposition_digest == first.acknowledgement.disposition_digest
    assert registered.replayed is False
    assert registered_replay.receipt_digest == registered.receipt_digest
    assert registered_replay.replayed is True

    equivocation = first.acknowledgement.model_copy(
        update={"disposition_digest": "f" * 64}
    )
    with pytest.raises(ExecutionConflictError, match="idempotency key"):
        await store.acknowledge_admission_plan_closure(
            capacity_session,
            equivocation,
            actor="development",
            idempotency_key=receipt_key,
        )

    assert (
        await store.next_subject_admission_plan(
            capacity_session,
            subject_id=bindings[0].subject_id,
            subject_incarnation=bindings[0].subject_incarnation,
            reporter_incarnation=demand_snapshot().reporter_incarnation,
        )
        is None
    )
    async with _guard_owner_session(capacity_guard_database) as (_, _, guard_session):
        counts = (
            await guard_session.execute(
                text(
                    "SELECT "
                    "(SELECT count(*) FROM loom_capacity_guard.abandoned_admission_plans) "
                    "AS abandoned, "
                    "(SELECT count(*) FROM loom_capacity_guard.attempt_lifecycle_heads "
                    "WHERE lifecycle_state = 'assigned') AS assigned"
                )
            )
        ).mappings().one()
    assert dict(counts) == {"abandoned": 1, "assigned": 0}


async def test_closure_receipt_advances_delivery_to_later_durable_work(
    capacity_session: AsyncSession,
    capacity_guard_database: dict[str, object],
) -> None:
    """An offline guard tombstones the oldest closure before later durable work."""

    store = CapacityExecutionStore()
    _executor, binding, proposal = await _bootstrap_acknowledged_admission(
        store,
        capacity_session,
        expires_in=timedelta(seconds=1),
    )
    _registration, configuration = await _initialize_manager_bound_admission_agent(
        capacity_guard_database,
        binding=binding,
        reporter_incarnation=demand_snapshot().reporter_incarnation,
        protected_admission_sha256=proposal.protected_admission_sha256,
    )
    async with _serializable_agent_session(capacity_guard_database) as guard_session:
        protected_bootstrap = await ProtectedExecutableBootstrapCoordinator(
            guard_session,
            configuration=configuration,
        ).protect(
            ExecutableBootstrapProposalV2(
                binding=binding,
                command_sequence=2,
                proposal_epoch=1,
                bootstrap_sha256="7" * 64,
                expires_at=proposal.lease_not_after,
            )
        )
    assert (
        protected_bootstrap.acknowledgement.bootstrap_registration_epoch
        == proposal.shapes[0].bootstrap_registration_epoch
    )
    await capacity_session.execute(text("SELECT pg_sleep(1.1)"))
    first = await store.next_subject_admission_plan(
        capacity_session,
        subject_id=binding.subject_id,
        subject_incarnation=binding.subject_incarnation,
        reporter_incarnation=demand_snapshot().reporter_incarnation,
    )
    assert isinstance(first, executable_contracts_module.ExecutableAdmissionPlanClosureV2)

    later_allocation_epoch = binding.execution.allocation_epoch + 100
    later_shapes = tuple(
        shape.model_copy(
            update={
                "binding": shape.binding.model_copy(
                    update={
                        "execution": shape.binding.execution.model_copy(
                            update={"allocation_epoch": later_allocation_epoch}
                        )
                    }
                )
            }
        )
        for shape in proposal.shapes
    )
    database_now = (
        await capacity_session.execute(select(func.clock_timestamp()))
    ).scalar_one()
    later = proposal.model_copy(
        update={
            "proposal_id": uuid4(),
            "plan_id": uuid4(),
            "admission_incarnation": uuid4(),
            "manager_input_digest": "d" * 64,
            "manager_allocation_digest": "e" * 64,
            "lease_not_after": database_now + timedelta(minutes=1),
            "shapes": later_shapes,
        }
    )
    await capacity_session.execute(
        text(
            "ALTER TABLE public.capacity_executable_admission_proposals "
            "DISABLE TRIGGER capacity_executable_admission_proposal_insert_guard"
        )
    )
    try:
        capacity_session.add(
            CapacityExecutableAdmissionProposal(
                proposal_id=later.proposal_id,
                plan_id=later.plan_id,
                admission_incarnation=later.admission_incarnation,
                tranche_id=binding.tranche_id,
                execution_epoch=binding.execution.execution_epoch,
                execution_manifest_sha256=(
                    binding.execution.execution_manifest_sha256
                ),
                allocation_epoch=later_allocation_epoch,
                subject_id=binding.subject_id,
                subject_incarnation=binding.subject_incarnation,
                pool_id=binding.pool_id,
                reporter_incarnation=later.reporter_incarnation,
                protected_admission_sha256=later.protected_admission_sha256,
                manager_input_digest=later.manager_input_digest,
                manager_allocation_digest=later.manager_allocation_digest,
                proposal_digest=canonical_executable_digest(later),
                proposal_payload=later.model_dump(mode="json", exclude_none=False),
                expires_at=later.lease_not_after,
                created_at=database_now + timedelta(microseconds=1),
            )
        )
        await capacity_session.flush()
    finally:
        await capacity_session.execute(
            text(
                "ALTER TABLE public.capacity_executable_admission_proposals "
                "ENABLE TRIGGER capacity_executable_admission_proposal_insert_guard"
            )
        )

    assert (
        await store.next_subject_admission_plan(
            capacity_session,
            subject_id=binding.subject_id,
            subject_incarnation=binding.subject_incarnation,
            reporter_incarnation=demand_snapshot().reporter_incarnation,
        )
        == first
    )
    later_closure = executable_contracts_module.ExecutableAdmissionPlanClosureV2(
        closure_id=execution_store_module._admission_closure_id(
            later.proposal_id,
            canonical_executable_digest(later),
            "allocation-superseded",
        ),
        proposal=later,
        close_reason="allocation-superseded",
    )
    skipped_acknowledgement = (
        executable_contracts_module.ExecutableAdmissionPlanClosureAcknowledgementV2(
            closure_id=later_closure.closure_id,
            proposal_id=later.proposal_id,
            proposal_digest=canonical_executable_digest(later),
            plan_id=later.plan_id,
            admission_incarnation=later.admission_incarnation,
            subject_id=binding.subject_id,
            subject_incarnation=binding.subject_incarnation,
            reporter_incarnation=demand_snapshot().reporter_incarnation,
            protected_admission_sha256=later.protected_admission_sha256,
            close_reason=later_closure.close_reason,
            disposition_kind="never-converged",
            disposition_digest="e" * 64,
        )
    )
    with pytest.raises(ExecutionConflictError, match="delivery high-water"):
        await store.acknowledge_admission_plan_closure(
            capacity_session,
            skipped_acknowledgement,
            actor="development",
            idempotency_key=uuid4(),
        )
    with pytest.raises(DBAPIError, match="closure acknowledgement binding changed"):
        async with capacity_session.begin_nested():
            await _insert_direct_admission_closure_acknowledgement(
                capacity_session,
                skipped_acknowledgement,
            )
    async with _serializable_agent_session(capacity_guard_database) as guard_session:
        cleanup = await ProtectedAdmissionPlanCoordinator(
            guard_session,
            configuration=configuration,
        ).close(first, None)
    assert cleanup.acknowledgement.disposition_kind == "never-converged"
    await store.acknowledge_admission_plan_closure(
        capacity_session,
        cleanup.acknowledgement,
        actor="development",
        idempotency_key=cleanup.idempotency_key,
    )

    following = await store.next_subject_admission_plan(
        capacity_session,
        subject_id=binding.subject_id,
        subject_incarnation=binding.subject_incarnation,
        reporter_incarnation=demand_snapshot().reporter_incarnation,
    )
    assert isinstance(
        following,
        executable_contracts_module.ExecutableAdmissionPlanClosureV2,
    )
    assert following.proposal == later
    assert following.close_reason == "allocation-superseded"


async def _retire_with_pending_admission_closure(
    capacity_session: AsyncSession,
) -> tuple[
    ExecutableIntentBindingV2,
    ExecutableAdmissionPlanProposalV2,
    ExecutionPreparationV2,
]:
    store = CapacityExecutionStore()
    _executor, binding, proposal = await _bootstrap_acknowledged_admission(
        store,
        capacity_session,
    )
    execution_row = (
        await capacity_session.execute(
            select(CapacityExecutionEpoch).where(
                CapacityExecutionEpoch.execution_epoch
                == binding.execution.execution_epoch
            )
        )
    ).scalar_one()
    preparation = ExecutionPreparationV2.model_validate_json(
        json.dumps(execution_row.manifest_payload)
    )
    await store.acknowledge_protected_release(
        capacity_session,
        ExecutableProtectedReleaseV2(
            binding=binding,
            reporter_incarnation=demand_snapshot().reporter_incarnation,
            bootstrap_registration_epoch=1,
            protected_registration_epoch=2,
            bootstrap_revoked=True,
            protected_release_sha256="b" * 64,
        ),
        actor="development",
        idempotency_key=UUID(int=996),
    )
    await store.begin_intent_close(
        capacity_session,
        ExecutableIntentCloseV2(binding=binding, command_sequence=3),
    )
    release = await store.next_pool_work(capacity_session, executor_binding("gb10"))
    assert isinstance(release, ExecutablePartialReleaseV2)
    await store.release_shapes(capacity_session, release)

    active = _inventory_execution(binding)
    await _heartbeat(store, capacity_session, active, pool_id="oldlab")
    drained = await CapacityManagementStore().begin_execution_drain(
        capacity_session,
        ExecutionDrainV2(
            authority_incarnation=active.authority_incarnation,
            expected_writer_epoch=active.writer_epoch,
            execution_epoch=active.execution_epoch,
            execution_manifest_sha256=active.execution_manifest_sha256,
            expected_executable_new_capacity_ceiling=(
                active.executable_new_capacity_ceiling
            ),
            expected_executable_new_capacity_rate_per_minute=(
                active.executable_new_capacity_rate_per_minute
            ),
        ),
        actor="activation-operator",
        idempotency_key=UUID(int=998),
    )
    await _post_inventory_heartbeat(store, capacity_session, active, pool_id="gb10")
    await _post_inventory_heartbeat(store, capacity_session, active, pool_id="oldlab")
    checkpoints = tuple(
        [
            await _mark_retirement_safe(capacity_session, pool_id="gb10"),
            await _mark_retirement_safe(capacity_session, pool_id="oldlab"),
        ]
    )
    await CapacityManagementStore().retire_execution_epoch(
        capacity_session,
        ExecutionRetirementV2(
            authority_incarnation=drained.authority_incarnation,
            expected_writer_epoch=drained.writer_epoch,
            execution_epoch=drained.execution_epoch,
            execution_manifest_sha256=drained.execution_manifest_sha256,
            executor_checkpoints=checkpoints,
        ),
        actor="activation-operator",
        idempotency_key=UUID(int=999),
    )
    return binding, proposal, preparation


async def test_admission_closure_delivery_and_receipt_survive_shadow_epoch_gap(
    capacity_session: AsyncSession,
) -> None:
    """Retiring the current epoch must not strand older protected cleanup work."""

    store = CapacityExecutionStore()
    binding, proposal, _preparation = await _retire_with_pending_admission_closure(
        capacity_session
    )

    closure = await store.next_subject_admission_plan(
        capacity_session,
        subject_id=binding.subject_id,
        subject_incarnation=binding.subject_incarnation,
        reporter_incarnation=demand_snapshot().reporter_incarnation,
    )
    assert isinstance(
        closure,
        executable_contracts_module.ExecutableAdmissionPlanClosureV2,
    )
    assert closure.proposal == proposal
    assert closure.close_reason == "allocation-superseded"
    acknowledgement = (
        executable_contracts_module.ExecutableAdmissionPlanClosureAcknowledgementV2(
            closure_id=closure.closure_id,
            proposal_id=proposal.proposal_id,
            proposal_digest=canonical_executable_digest(proposal),
            plan_id=proposal.plan_id,
            admission_incarnation=proposal.admission_incarnation,
            subject_id=binding.subject_id,
            subject_incarnation=binding.subject_incarnation,
            reporter_incarnation=demand_snapshot().reporter_incarnation,
            protected_admission_sha256=proposal.protected_admission_sha256,
            close_reason=closure.close_reason,
            disposition_kind="never-converged",
            disposition_digest="e" * 64,
        )
    )
    registered = await store.acknowledge_admission_plan_closure(
        capacity_session,
        acknowledgement,
        actor="development",
        idempotency_key=uuid4(),
    )
    assert registered.closure_id == closure.closure_id
    assert (
        await store.next_subject_admission_plan(
            capacity_session,
            subject_id=binding.subject_id,
            subject_incarnation=binding.subject_incarnation,
            reporter_incarnation=demand_snapshot().reporter_incarnation,
        )
        is None
    )


async def test_admission_closure_survives_active_epoch_before_first_allocation(
    capacity_session: AsyncSession,
) -> None:
    """A newly active epoch cannot hide cleanup while its first allocation is absent."""

    store = CapacityExecutionStore()
    binding, proposal, preparation = await _retire_with_pending_admission_closure(
        capacity_session
    )
    next_executors = tuple(
        executor.model_copy(
            update={
                "executor_incarnation": uuid5(
                    executor.executor_incarnation,
                    "next-execution-epoch",
                )
            }
        )
        for executor in preparation.executors
    )
    preparation = preparation.model_copy(update={"executors": next_executors})
    policy = execution_policy().model_copy(update={"executors": next_executors})
    manager = CapacityManagementStore(execution_policy=policy)
    prepared = await manager.prepare_execution_epoch(
        capacity_session,
        preparation,
        actor="activation-operator",
        idempotency_key=UUID(int=1000),
    )
    for index, executor in enumerate(preparation.executors, start=1):
        await manager.register_execution_executor(
            capacity_session,
            ExecutableExecutorRegistrationV2(
                execution=prepared,
                executor_id=executor.executor_id,
                executor_incarnation=executor.executor_incarnation,
                pool_id=executor.pool_id,
                pool_generation=executor.pool_generation,
                signing_key_id=f"{executor.pool_id}-key",
                signing_key_sha256=executor.signing_key_sha256,
                local_authority_sha256=executor.local_authority_sha256,
                controller_authority_sha256=executor.controller_authority_sha256,
            ),
            actor="executor-installer",
            idempotency_key=UUID(int=1000 + index),
        )
    activation = await ready_execution_activation(
        capacity_session,
        manager,
        preparation,
        prepared,
    )
    await manager.activate_execution_epoch(
        capacity_session,
        activation,
        actor="activation-operator",
        idempotency_key=UUID(int=1003),
    )

    closure = await store.next_subject_admission_plan(
        capacity_session,
        subject_id=binding.subject_id,
        subject_incarnation=binding.subject_incarnation,
        reporter_incarnation=demand_snapshot().reporter_incarnation,
    )
    assert isinstance(
        closure,
        executable_contracts_module.ExecutableAdmissionPlanClosureV2,
    )
    assert closure.proposal == proposal
    assert closure.close_reason == "allocation-superseded"
    acknowledgement = (
        executable_contracts_module.ExecutableAdmissionPlanClosureAcknowledgementV2(
            closure_id=closure.closure_id,
            proposal_id=proposal.proposal_id,
            proposal_digest=canonical_executable_digest(proposal),
            plan_id=proposal.plan_id,
            admission_incarnation=proposal.admission_incarnation,
            subject_id=binding.subject_id,
            subject_incarnation=binding.subject_incarnation,
            reporter_incarnation=demand_snapshot().reporter_incarnation,
            protected_admission_sha256=proposal.protected_admission_sha256,
            close_reason=closure.close_reason,
            disposition_kind="never-converged",
            disposition_digest="e" * 64,
        )
    )
    registered = await store.acknowledge_admission_plan_closure(
        capacity_session,
        acknowledgement,
        actor="development",
        idempotency_key=uuid4(),
    )
    assert registered.closure_id == closure.closure_id
    assert (
        await store.next_subject_admission_plan(
            capacity_session,
            subject_id=binding.subject_id,
            subject_incarnation=binding.subject_incarnation,
            reporter_incarnation=demand_snapshot().reporter_incarnation,
        )
        is None
    )


async def test_real_manager_guard_digest_mismatch_persists_nothing_and_denies_launch(
    capacity_session: AsyncSession,
    capacity_guard_database: dict[str, object],
) -> None:
    """Catch changed local requirements being partially acknowledged or launchable."""

    (
        store,
        executor,
        _bindings,
        proposal,
        registration,
        configuration,
        pending,
    ) = await _real_manager_guard_admission_fixture(
        capacity_session,
        capacity_guard_database,
    )
    changed_attempt = pending.attempts[1].model_copy(
        update={"requirements_digest": "f" * 64}
    )
    changed = pending.model_copy(
        update={"attempts": (pending.attempts[0], changed_attempt)}
    )

    with pytest.raises(DBAPIError, match=r"allowance|digest|lifecycle"):
        async with _serializable_agent_session(capacity_guard_database) as guard_session:
            await ProtectedAdmissionPlanCoordinator(
                guard_session,
                configuration=configuration,
            ).converge(proposal, changed)

    async with _guard_owner_session(capacity_guard_database) as (_, _, guard_session):
        counts = (
            (
                await guard_session.execute(
                    text(
                        "SELECT "
                        "(SELECT count(*) FROM "
                        "loom_capacity_guard.prepared_admission_plans) AS plans, "
                        "(SELECT count(*) FROM "
                        "loom_capacity_guard.prepared_worker_shapes) AS shapes, "
                        "(SELECT count(*) FROM "
                        "loom_capacity_guard.prepared_placement_allowances) AS allowances, "
                        "(SELECT count(*) FROM loom_capacity_guard.attempt_lifecycle_heads "
                        "WHERE lifecycle_state = 'assigned') AS assignments"
                    )
                )
            )
            .mappings()
            .one()
        )
    assert dict(counts) == {
        "plans": 0,
        "shapes": 0,
        "allowances": 0,
        "assignments": 0,
    }
    still_pending = await _capture_lifecycle(
        capacity_guard_database,
        registration,
        expected_high_water=1,
    )
    assert {
        (attempt.protected_attempt_id, attempt.lifecycle_state, attempt.lifecycle_sequence)
        for attempt in still_pending.attempts
    } == {
        (UUID(int=991), "pending-unassigned", 0),
        (UUID(int=992), "pending-unassigned", 0),
    }
    assert (
        await capacity_session.execute(
            select(func.count(CapacityExecutableAdmissionAcknowledgement.id))
        )
    ).scalar_one() == 0
    work = await store.next_pool_work(capacity_session, executor)
    assert not isinstance(work, ExecutableLaunchPermitV2)


async def _insert_direct_bootstrap_acknowledgement(
    session: AsyncSession,
    acknowledgement: ExecutableBootstrapAcknowledgementV2,
    *,
    acknowledgement_payload: dict[str, object] | None = None,
) -> None:
    await session.execute(
        text(
            "INSERT INTO public.capacity_executable_bootstrap_acknowledgements "
            "(id, idempotency_key, intent_id, execution_epoch, "
            "execution_manifest_sha256, proposal_epoch, proposal_digest, "
            "reporter_incarnation, bootstrap_registration_epoch, "
            "bootstrap_evidence_sha256, protected_admission_sha256, "
            "acknowledgement_digest, actor_id, acknowledgement_payload) VALUES "
            "(:id, :idempotency_key, :intent_id, :execution_epoch, "
            ":execution_manifest_sha256, :proposal_epoch, :proposal_digest, "
            ":reporter_incarnation, :bootstrap_registration_epoch, "
            ":bootstrap_evidence_sha256, :protected_admission_sha256, "
            ":acknowledgement_digest, 'direct-sql-test', "
            "CAST(:acknowledgement_payload AS jsonb))"
        ),
        {
            "id": uuid4(),
            "idempotency_key": uuid4(),
            "intent_id": acknowledgement.binding.intent_id,
            "execution_epoch": acknowledgement.binding.execution.execution_epoch,
            "execution_manifest_sha256": (
                acknowledgement.binding.execution.execution_manifest_sha256
            ),
            "proposal_epoch": acknowledgement.proposal_epoch,
            "proposal_digest": acknowledgement.proposal_digest,
            "reporter_incarnation": acknowledgement.reporter_incarnation,
            "bootstrap_registration_epoch": (acknowledgement.bootstrap_registration_epoch),
            "bootstrap_evidence_sha256": acknowledgement.bootstrap_evidence_sha256,
            "protected_admission_sha256": acknowledgement.protected_admission_sha256,
            "acknowledgement_digest": "a" * 64,
            "acknowledgement_payload": json.dumps(
                acknowledgement.model_dump(mode="json", exclude_none=False)
                if acknowledgement_payload is None
                else acknowledgement_payload
            ),
        },
    )


async def _insert_direct_admission_acknowledgement(
    session: AsyncSession,
    acknowledgement: ExecutableAdmissionAcknowledgementV2,
    *,
    acknowledgement_payload: dict[str, object] | None = None,
) -> None:
    await session.execute(
        text(
            "INSERT INTO public.capacity_executable_admission_acknowledgements "
            "(id, idempotency_key, proposal_id, plan_id, admission_incarnation, "
            "tranche_id, execution_epoch, execution_manifest_sha256, allocation_epoch, "
            "subject_id, subject_incarnation, pool_id, reporter_incarnation, "
            "protected_admission_sha256, proposal_digest, prepared_plan_digest, "
            "acknowledgement_digest, actor_id, acknowledgement_payload) VALUES "
            "(:id, :idempotency_key, :proposal_id, :plan_id, :admission_incarnation, "
            ":tranche_id, :execution_epoch, :execution_manifest_sha256, :allocation_epoch, "
            ":subject_id, :subject_incarnation, :pool_id, :reporter_incarnation, "
            ":protected_admission_sha256, :proposal_digest, :prepared_plan_digest, "
            ":acknowledgement_digest, 'direct-sql-test', "
            "CAST(:acknowledgement_payload AS jsonb))"
        ),
        {
            "id": uuid4(),
            "idempotency_key": uuid4(),
            "proposal_id": acknowledgement.proposal_id,
            "plan_id": acknowledgement.plan_id,
            "admission_incarnation": acknowledgement.admission_incarnation,
            "tranche_id": acknowledgement.tranche_id,
            "execution_epoch": acknowledgement.execution.execution_epoch,
            "execution_manifest_sha256": (
                acknowledgement.execution.execution_manifest_sha256
            ),
            "allocation_epoch": acknowledgement.execution.allocation_epoch,
            "subject_id": acknowledgement.subject_id,
            "subject_incarnation": acknowledgement.subject_incarnation,
            "pool_id": acknowledgement.pool_id,
            "reporter_incarnation": acknowledgement.reporter_incarnation,
            "protected_admission_sha256": (
                acknowledgement.protected_admission_sha256
            ),
            "proposal_digest": acknowledgement.proposal_digest,
            "prepared_plan_digest": acknowledgement.prepared_plan_digest,
            "acknowledgement_digest": canonical_executable_digest(acknowledgement),
            "acknowledgement_payload": json.dumps(
                acknowledgement.model_dump(mode="json", exclude_none=False)
                if acknowledgement_payload is None
                else acknowledgement_payload
            ),
        },
    )


async def _insert_direct_admission_closure_acknowledgement(
    session: AsyncSession,
    acknowledgement: executable_contracts_module.ExecutableAdmissionPlanClosureAcknowledgementV2,
    *,
    acknowledgement_digest: str | None = None,
) -> None:
    await session.execute(
        text(
            "INSERT INTO public.capacity_executable_admission_closure_acknowledgements "
            "(id, idempotency_key, closure_id, proposal_id, proposal_digest, plan_id, "
            "admission_incarnation, subject_id, subject_incarnation, reporter_incarnation, "
            "protected_admission_sha256, close_reason, disposition_kind, "
            "disposition_digest, "
            "acknowledgement_digest, actor_id, acknowledgement_payload) VALUES "
            "(:id, :idempotency_key, :closure_id, :proposal_id, :proposal_digest, :plan_id, "
            ":admission_incarnation, :subject_id, :subject_incarnation, "
            ":reporter_incarnation, :protected_admission_sha256, :close_reason, "
            ":disposition_kind, :disposition_digest, :acknowledgement_digest, "
            "'direct-sql-test', "
            "CAST(:acknowledgement_payload AS jsonb))"
        ),
        {
            "id": uuid4(),
            "idempotency_key": uuid4(),
            "closure_id": acknowledgement.closure_id,
            "proposal_id": acknowledgement.proposal_id,
            "proposal_digest": acknowledgement.proposal_digest,
            "plan_id": acknowledgement.plan_id,
            "admission_incarnation": acknowledgement.admission_incarnation,
            "subject_id": acknowledgement.subject_id,
            "subject_incarnation": acknowledgement.subject_incarnation,
            "reporter_incarnation": acknowledgement.reporter_incarnation,
            "protected_admission_sha256": (
                acknowledgement.protected_admission_sha256
            ),
            "close_reason": acknowledgement.close_reason,
            "disposition_kind": acknowledgement.disposition_kind,
            "disposition_digest": acknowledgement.disposition_digest,
            "acknowledgement_digest": (
                canonical_executable_digest(acknowledgement)
                if acknowledgement_digest is None
                else acknowledgement_digest
            ),
            "acknowledgement_payload": json.dumps(
                acknowledgement.model_dump(mode="json", exclude_none=False)
            ),
        },
    )


def _manager_closed_admission_closure_acknowledgement(
    proposal: ExecutableAdmissionPlanProposalV2,
) -> executable_contracts_module.ExecutableAdmissionPlanClosureAcknowledgementV2:
    anchor = proposal.shapes[0].binding
    close_reason = "manager-closed"
    return executable_contracts_module.ExecutableAdmissionPlanClosureAcknowledgementV2(
        closure_id=execution_store_module._admission_closure_id(
            proposal.proposal_id,
            canonical_executable_digest(proposal),
            close_reason,
        ),
        proposal_id=proposal.proposal_id,
        proposal_digest=canonical_executable_digest(proposal),
        plan_id=proposal.plan_id,
        admission_incarnation=proposal.admission_incarnation,
        subject_id=anchor.subject_id,
        subject_incarnation=anchor.subject_incarnation,
        reporter_incarnation=proposal.reporter_incarnation,
        protected_admission_sha256=proposal.protected_admission_sha256,
        close_reason=close_reason,
        disposition_kind="never-converged",
        disposition_digest="e" * 64,
    )


async def test_admission_ack_guard_rejects_existing_closure_acknowledgement(
    capacity_session: AsyncSession,
) -> None:
    """Positive launch authority cannot follow final protected cleanup evidence."""

    store = CapacityExecutionStore()
    _executor, binding, proposal = await _bootstrap_acknowledged_admission(
        store,
        capacity_session,
    )
    await store.begin_intent_close(
        capacity_session,
        ExecutableIntentCloseV2(binding=binding, command_sequence=3),
    )
    await _insert_direct_admission_closure_acknowledgement(
        capacity_session,
        _manager_closed_admission_closure_acknowledgement(proposal),
    )

    with pytest.raises(DBAPIError, match="admission acknowledgement binding changed"):
        await _insert_direct_admission_acknowledgement(
            capacity_session,
            _admission_acknowledgement(proposal),
        )


@pytest.mark.parametrize("first_kind", ("positive", "closure"))
async def test_admission_ack_guards_serialize_positive_and_closure_race(
    isolated_capacity_postgres_url: str,
    first_kind: str,
) -> None:
    """Concurrent mutually exclusive outcomes must serialize on their proposal."""

    engine = create_async_engine(
        isolated_capacity_postgres_url,
        isolation_level="SERIALIZABLE",
    )
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    try:
        async with engine.connect() as setup_connection:
            setup_session = AsyncSession(
                bind=setup_connection,
                expire_on_commit=False,
                join_transaction_mode="create_savepoint",
            )
            store = CapacityExecutionStore()
            _executor, binding, proposal = await _bootstrap_acknowledged_admission(
                store,
                setup_session,
            )
            await store.begin_intent_close(
                setup_session,
                ExecutableIntentCloseV2(binding=binding, command_sequence=3),
            )
            await setup_session.commit()
            await setup_session.close()

        positive = _admission_acknowledgement(proposal)
        closure = _manager_closed_admission_closure_acknowledgement(proposal)
        async with (
            session_factory() as first_session,
            session_factory() as second_session,
            session_factory() as observer_session,
        ):
            if first_kind == "positive":
                await _insert_direct_admission_acknowledgement(
                    first_session,
                    positive,
                )

                async def insert_second() -> None:
                    await _insert_direct_admission_closure_acknowledgement(
                        second_session,
                        closure,
                    )
                    await second_session.commit()

            else:
                await _insert_direct_admission_closure_acknowledgement(
                    first_session,
                    closure,
                )

                async def insert_second() -> None:
                    await _insert_direct_admission_acknowledgement(
                        second_session,
                        positive,
                    )
                    await second_session.commit()

            second_pid = (
                await second_session.execute(text("SELECT pg_backend_pid()"))
            ).scalar_one()
            second_task = asyncio.create_task(insert_second())
            blocked = False
            for _attempt in range(200):
                if second_task.done():
                    break
                wait_event_type = (
                    await observer_session.execute(
                        text(
                            "SELECT wait_event_type FROM pg_stat_activity "
                            "WHERE pid = :pid"
                        ),
                        {"pid": second_pid},
                    )
                ).scalar_one()
                if wait_event_type == "Lock":
                    blocked = True
                    break
                await asyncio.sleep(0.01)
            assert blocked, "opposite admission receipt did not wait on the proposal fence"

            await first_session.commit()
            with pytest.raises(DBAPIError):
                await second_task
            await second_session.rollback()
            await observer_session.rollback()

        async with session_factory() as verification_session:
            counts = (
                await verification_session.execute(
                    text(
                        "SELECT "
                        "(SELECT count(*) FROM "
                        "public.capacity_executable_admission_acknowledgements) "
                        "AS positive, "
                        "(SELECT count(*) FROM "
                        "public.capacity_executable_admission_closure_acknowledgements) "
                        "AS closure"
                    )
                )
            ).mappings().one()
            assert sum(counts.values()) == 1
            assert counts[first_kind] == 1
    finally:
        await engine.dispose()


async def _replace_admission_proposal_payload_without_guard(
    session: AsyncSession,
    *,
    proposal_id: UUID,
    proposal_payload: dict[str, object],
) -> None:
    await _test_only_update_without_guard(
        session,
        table_name="capacity_executable_admission_proposals",
        trigger_name="capacity_executable_admission_proposals_append_only_guard",
        statement=(
            update(CapacityExecutableAdmissionProposal)
            .where(CapacityExecutableAdmissionProposal.proposal_id == proposal_id)
            .values(proposal_payload=proposal_payload)
        ),
    )


def _skeletal_admission_proposal_payload(
    proposal: ExecutableAdmissionPlanProposalV2,
) -> dict[str, object]:
    payload = proposal.model_dump(mode="json", exclude_none=False)
    payload.pop("schema_version")
    payload["shapes"] = [
        {
            "binding": shape["binding"],
            "bootstrap_registration_epoch": shape["bootstrap_registration_epoch"],
        }
        for shape in cast(list[dict[str, object]], payload["shapes"])
    ]
    payload["allowances"] = []
    return payload


def _skeletal_admission_acknowledgement_payload(
    acknowledgement: ExecutableAdmissionAcknowledgementV2,
) -> dict[str, object]:
    payload = acknowledgement.model_dump(mode="json", exclude_none=False)
    payload.pop("schema_version")
    payload["assignment_count"] = 0
    payload["assignments"] = []
    return payload


async def _insert_unchecked_admission_acknowledgement(
    session: AsyncSession,
    acknowledgement: ExecutableAdmissionAcknowledgementV2,
    *,
    acknowledgement_payload: dict[str, object],
) -> None:
    await session.execute(
        text(
            "ALTER TABLE public.capacity_executable_admission_acknowledgements "
            "DISABLE TRIGGER capacity_executable_admission_ack_insert_guard"
        )
    )
    try:
        await _insert_direct_admission_acknowledgement(
            session,
            acknowledgement,
            acknowledgement_payload=acknowledgement_payload,
        )
    finally:
        await session.execute(
            text(
                "ALTER TABLE public.capacity_executable_admission_acknowledgements "
                "ENABLE TRIGGER capacity_executable_admission_ack_insert_guard"
            )
        )


async def test_admission_closure_ack_guard_rejects_forged_delivery_high_water(
    capacity_session: AsyncSession,
) -> None:
    """Direct SQL must not advance closure delivery with a fabricated closure identity."""

    store = CapacityExecutionStore()
    _executor, binding, proposal = await _bootstrap_acknowledged_admission(
        store,
        capacity_session,
        expires_in=timedelta(seconds=1),
    )
    await capacity_session.execute(text("SELECT pg_sleep(1.1)"))
    closure = await store.next_subject_admission_plan(
        capacity_session,
        subject_id=binding.subject_id,
        subject_incarnation=binding.subject_incarnation,
        reporter_incarnation=demand_snapshot().reporter_incarnation,
    )
    assert isinstance(
        closure,
        executable_contracts_module.ExecutableAdmissionPlanClosureV2,
    )
    acknowledgement = (
        executable_contracts_module.ExecutableAdmissionPlanClosureAcknowledgementV2(
            closure_id=uuid4(),
            proposal_id=proposal.proposal_id,
            proposal_digest=canonical_executable_digest(proposal),
            plan_id=proposal.plan_id,
            admission_incarnation=proposal.admission_incarnation,
            subject_id=binding.subject_id,
            subject_incarnation=binding.subject_incarnation,
            reporter_incarnation=demand_snapshot().reporter_incarnation,
            protected_admission_sha256=proposal.protected_admission_sha256,
            close_reason="expired",
            disposition_kind="never-converged",
            disposition_digest="e" * 64,
        )
    )

    with pytest.raises(DBAPIError, match="closure acknowledgement binding changed"):
        await _insert_direct_admission_closure_acknowledgement(
            capacity_session,
            acknowledgement,
        )


async def test_admission_closure_ack_guard_rejects_noncanonical_digest_under_direct_sql(
    capacity_session: AsyncSession,
) -> None:
    """Direct SQL cannot decouple a closure receipt digest from its canonical payload."""

    store = CapacityExecutionStore()
    _executor, binding, proposal = await _bootstrap_acknowledged_admission(
        store,
        capacity_session,
        expires_in=timedelta(seconds=1),
    )
    await capacity_session.execute(text("SELECT pg_sleep(1.1)"))
    closure = await store.next_subject_admission_plan(
        capacity_session,
        subject_id=binding.subject_id,
        subject_incarnation=binding.subject_incarnation,
        reporter_incarnation=demand_snapshot().reporter_incarnation,
    )
    assert isinstance(
        closure,
        executable_contracts_module.ExecutableAdmissionPlanClosureV2,
    )
    acknowledgement = (
        executable_contracts_module.ExecutableAdmissionPlanClosureAcknowledgementV2(
            closure_id=closure.closure_id,
            proposal_id=proposal.proposal_id,
            proposal_digest=canonical_executable_digest(proposal),
            plan_id=proposal.plan_id,
            admission_incarnation=proposal.admission_incarnation,
            subject_id=binding.subject_id,
            subject_incarnation=binding.subject_incarnation,
            reporter_incarnation=demand_snapshot().reporter_incarnation,
            protected_admission_sha256=proposal.protected_admission_sha256,
            close_reason="expired",
            disposition_kind="never-converged",
            disposition_digest="e" * 64,
        )
    )

    with pytest.raises(DBAPIError, match="closure acknowledgement binding changed"):
        await _insert_direct_admission_closure_acknowledgement(
            capacity_session,
            acknowledgement,
            acknowledgement_digest="f" * 64,
        )


@pytest.mark.parametrize(
    "forgery",
    (
        "skeletal",
        "extra-field",
        "invalid-transition-id",
        "zero-execution-generation",
        "null-execution-generation",
        "invalid-requirements-digest",
        "null-requirements-digest",
        "zero-lifecycle-sequence",
        "null-lifecycle-sequence",
    ),
)
async def test_admission_ack_guard_rejects_forged_local_assignment_facts_under_direct_sql(
    capacity_session: AsyncSession,
    forgery: str,
) -> None:
    """Copied manager allowances are not protected local assignment evidence."""

    store = CapacityExecutionStore()
    _executor, _binding, proposal = await _bootstrap_acknowledged_admission(
        store, capacity_session
    )
    acknowledgement = _admission_acknowledgement(proposal)
    payload = acknowledgement.model_dump(mode="json", exclude_none=False)
    assignment = cast(list[dict[str, object]], payload["assignments"])[0]
    if forgery == "skeletal":
        assignment = {
            key: assignment[key]
            for key in (
                "schema_version",
                "allowance_id",
                "protected_attempt_id",
                "shape_instance_id",
                "shape_slot_index",
                "submission_intent_id",
            )
        }
    elif forgery == "extra-field":
        assignment["unprotected"] = True
    elif forgery == "invalid-transition-id":
        assignment["transition_id"] = "not-a-uuid"
    elif forgery == "zero-execution-generation":
        assignment["execution_generation"] = 0
    elif forgery == "null-execution-generation":
        assignment["execution_generation"] = None
    elif forgery == "invalid-requirements-digest":
        assignment["requirements_digest"] = "not-a-digest"
    elif forgery == "null-requirements-digest":
        assignment["requirements_digest"] = None
    elif forgery == "zero-lifecycle-sequence":
        assignment["lifecycle_sequence"] = 0
    else:
        assignment["lifecycle_sequence"] = None
    payload["assignments"] = [assignment]

    with pytest.raises(DBAPIError, match="payload is not exact"):
        async with capacity_session.begin_nested():
            await _insert_direct_admission_acknowledgement(
                capacity_session,
                acknowledgement,
                acknowledgement_payload=payload,
            )


async def test_admission_proposal_guard_rejects_truncated_intent_set_under_direct_sql(
    capacity_session: AsyncSession,
) -> None:
    """A proposal must bind every intent in its immutable reservation tranche."""

    store = CapacityExecutionStore()
    _executor, _bindings, proposal = await _batched_admission_proposal(
        store, capacity_session
    )
    stored = (
        await capacity_session.execute(
            select(CapacityExecutableAdmissionProposal).where(
                CapacityExecutableAdmissionProposal.proposal_id == proposal.proposal_id
            )
        )
    ).scalar_one()
    truncated_payload = proposal.model_dump(mode="json", exclude_none=False)
    truncated_payload["shapes"] = cast(list[object], truncated_payload["shapes"])[:1]
    replacement = CapacityExecutableAdmissionProposal(
        proposal_id=stored.proposal_id,
        plan_id=stored.plan_id,
        admission_incarnation=stored.admission_incarnation,
        tranche_id=stored.tranche_id,
        execution_epoch=stored.execution_epoch,
        execution_manifest_sha256=stored.execution_manifest_sha256,
        allocation_epoch=stored.allocation_epoch,
        subject_id=stored.subject_id,
        subject_incarnation=stored.subject_incarnation,
        pool_id=stored.pool_id,
        reporter_incarnation=stored.reporter_incarnation,
        protected_admission_sha256=stored.protected_admission_sha256,
        manager_input_digest=stored.manager_input_digest,
        manager_allocation_digest=stored.manager_allocation_digest,
        proposal_digest=stored.proposal_digest,
        proposal_payload=truncated_payload,
        expires_at=stored.expires_at,
    )
    await capacity_session.execute(
        text(
            "ALTER TABLE public.capacity_executable_admission_proposals "
            "DISABLE TRIGGER capacity_executable_admission_proposals_append_only_guard"
        )
    )
    try:
        await capacity_session.execute(
            text(
                "DELETE FROM public.capacity_executable_admission_proposals "
                "WHERE proposal_id = :proposal_id"
            ),
            {"proposal_id": stored.proposal_id},
        )
    finally:
        await capacity_session.execute(
            text(
                "ALTER TABLE public.capacity_executable_admission_proposals "
                "ENABLE TRIGGER capacity_executable_admission_proposals_append_only_guard"
            )
        )
    capacity_session.expunge(stored)

    with pytest.raises(DBAPIError, match="intent set changed"):
        async with capacity_session.begin_nested():
            capacity_session.add(replacement)
            await capacity_session.flush()


async def test_admission_proposal_guard_rejects_skeletal_empty_allowance_plan_under_direct_sql(
    capacity_session: AsyncSession,
) -> None:
    """Exact bindings alone cannot replace the sealed manager plan and allowances."""

    store = CapacityExecutionStore()
    _executor, _bindings, proposal = await _batched_admission_proposal(
        store, capacity_session
    )
    stored = (
        await capacity_session.execute(
            select(CapacityExecutableAdmissionProposal).where(
                CapacityExecutableAdmissionProposal.proposal_id == proposal.proposal_id
            )
        )
    ).scalar_one()
    replacement = CapacityExecutableAdmissionProposal(
        proposal_id=stored.proposal_id,
        plan_id=stored.plan_id,
        admission_incarnation=stored.admission_incarnation,
        tranche_id=stored.tranche_id,
        execution_epoch=stored.execution_epoch,
        execution_manifest_sha256=stored.execution_manifest_sha256,
        allocation_epoch=stored.allocation_epoch,
        subject_id=stored.subject_id,
        subject_incarnation=stored.subject_incarnation,
        pool_id=stored.pool_id,
        reporter_incarnation=stored.reporter_incarnation,
        protected_admission_sha256=stored.protected_admission_sha256,
        manager_input_digest=stored.manager_input_digest,
        manager_allocation_digest=stored.manager_allocation_digest,
        proposal_digest=stored.proposal_digest,
        proposal_payload=_skeletal_admission_proposal_payload(proposal),
        expires_at=stored.expires_at,
    )
    await capacity_session.execute(
        text(
            "ALTER TABLE public.capacity_executable_admission_proposals "
            "DISABLE TRIGGER capacity_executable_admission_proposals_append_only_guard"
        )
    )
    try:
        await capacity_session.execute(
            text(
                "DELETE FROM public.capacity_executable_admission_proposals "
                "WHERE proposal_id = :proposal_id"
            ),
            {"proposal_id": stored.proposal_id},
        )
    finally:
        await capacity_session.execute(
            text(
                "ALTER TABLE public.capacity_executable_admission_proposals "
                "ENABLE TRIGGER capacity_executable_admission_proposals_append_only_guard"
            )
        )
    capacity_session.expunge(stored)

    with pytest.raises(DBAPIError, match="payload is not exact"):
        async with capacity_session.begin_nested():
            capacity_session.add(replacement)
            await capacity_session.flush()


@pytest.mark.parametrize(
    "forgery",
    (
        "protocol-generation",
        "protocol-digest",
        "worker-shape",
        "worker-shape-digest",
        "null-worker-shape-digest",
        "missing-shape-field",
        "extra-shape-field",
        "duplicate-allowance-id",
        "duplicate-attempt-id",
        "duplicate-shape-slot",
        "unsealed-allowance",
    ),
)
async def test_admission_proposal_guard_rejects_mutated_sealed_plan_under_direct_sql(
    capacity_session: AsyncSession,
    forgery: str,
) -> None:
    """Every proposal field must remain derived from current sealed manager facts."""

    store = CapacityExecutionStore()
    _executor, _bindings, proposal = await _batched_admission_proposal(
        store, capacity_session
    )
    stored = (
        await capacity_session.execute(
            select(CapacityExecutableAdmissionProposal).where(
                CapacityExecutableAdmissionProposal.proposal_id == proposal.proposal_id
            )
        )
    ).scalar_one()
    payload = proposal.model_dump(mode="json", exclude_none=False)
    shapes = cast(list[dict[str, object]], payload["shapes"])
    allowances = cast(list[dict[str, object]], payload["allowances"])
    assert len(allowances) >= 2
    if forgery == "protocol-generation":
        shapes[0]["protocol_generation"] = (
            cast(int, shapes[0]["protocol_generation"]) + 1
        )
    elif forgery == "protocol-digest":
        shapes[0]["protocol_digest"] = "c" * 64
    elif forgery == "worker-shape":
        worker_shape = cast(dict[str, object], shapes[0]["worker_shape"])
        worker_shape["concurrency_slots"] = (
            cast(int, worker_shape["concurrency_slots"]) + 1
        )
    elif forgery == "worker-shape-digest":
        shapes[0]["worker_shape_digest"] = "c" * 64
    elif forgery == "null-worker-shape-digest":
        shapes[0]["worker_shape_digest"] = None
    elif forgery == "missing-shape-field":
        shapes[0].pop("protocol_digest")
    elif forgery == "extra-shape-field":
        shapes[0]["untrusted"] = True
    elif forgery == "duplicate-allowance-id":
        allowances[1]["allowance_id"] = allowances[0]["allowance_id"]
    elif forgery == "duplicate-attempt-id":
        allowances[1]["protected_attempt_id"] = allowances[0][
            "protected_attempt_id"
        ]
    elif forgery == "duplicate-shape-slot":
        allowances[1]["shape_instance_id"] = allowances[0]["shape_instance_id"]
        allowances[1]["shape_slot_index"] = allowances[0]["shape_slot_index"]
    else:
        allowances[0]["protected_attempt_id"] = str(uuid4())

    replacement = CapacityExecutableAdmissionProposal(
        proposal_id=stored.proposal_id,
        plan_id=stored.plan_id,
        admission_incarnation=stored.admission_incarnation,
        tranche_id=stored.tranche_id,
        execution_epoch=stored.execution_epoch,
        execution_manifest_sha256=stored.execution_manifest_sha256,
        allocation_epoch=stored.allocation_epoch,
        subject_id=stored.subject_id,
        subject_incarnation=stored.subject_incarnation,
        pool_id=stored.pool_id,
        reporter_incarnation=stored.reporter_incarnation,
        protected_admission_sha256=stored.protected_admission_sha256,
        manager_input_digest=stored.manager_input_digest,
        manager_allocation_digest=stored.manager_allocation_digest,
        proposal_digest=stored.proposal_digest,
        proposal_payload=payload,
        expires_at=stored.expires_at,
    )
    await capacity_session.execute(
        text(
            "ALTER TABLE public.capacity_executable_admission_proposals "
            "DISABLE TRIGGER capacity_executable_admission_proposals_append_only_guard"
        )
    )
    try:
        await capacity_session.execute(
            text(
                "DELETE FROM public.capacity_executable_admission_proposals "
                "WHERE proposal_id = :proposal_id"
            ),
            {"proposal_id": stored.proposal_id},
        )
    finally:
        await capacity_session.execute(
            text(
                "ALTER TABLE public.capacity_executable_admission_proposals "
                "ENABLE TRIGGER capacity_executable_admission_proposals_append_only_guard"
            )
        )
    capacity_session.expunge(stored)

    with pytest.raises(DBAPIError, match="payload is not exact"):
        async with capacity_session.begin_nested():
            capacity_session.add(replacement)
            await capacity_session.flush()


@pytest.mark.parametrize(
    "forgery",
    (
        "manager-allocation-digest",
        "worker-shape-digest",
        "proposal-digest",
        "extended-lease",
    ),
)
async def test_admission_proposal_guard_rejects_self_asserted_manager_authority(
    capacity_session: AsyncSession,
    forgery: str,
) -> None:
    """Proposal digests and lease must be derived from sealed manager evidence."""

    store = CapacityExecutionStore()
    _executor, _bindings, proposal = await _batched_admission_proposal(
        store, capacity_session
    )
    stored = (
        await capacity_session.execute(
            select(CapacityExecutableAdmissionProposal).where(
                CapacityExecutableAdmissionProposal.proposal_id == proposal.proposal_id
            )
        )
    ).scalar_one()
    changed = proposal
    if forgery == "manager-allocation-digest":
        changed = proposal.model_copy(update={"manager_allocation_digest": "c" * 64})
    elif forgery == "worker-shape-digest":
        changed_shape = proposal.shapes[0].model_copy(
            update={"worker_shape_digest": "c" * 64}
        )
        changed = proposal.model_copy(
            update={"shapes": (changed_shape, *proposal.shapes[1:])}
        )
    elif forgery == "extended-lease":
        changed = proposal.model_copy(
            update={"lease_not_after": proposal.lease_not_after + timedelta(minutes=1)}
        )
    changed_digest = canonical_executable_digest(changed)
    if forgery == "proposal-digest":
        changed_digest = "c" * 64
    replacement = CapacityExecutableAdmissionProposal(
        proposal_id=stored.proposal_id,
        plan_id=stored.plan_id,
        admission_incarnation=stored.admission_incarnation,
        tranche_id=stored.tranche_id,
        execution_epoch=stored.execution_epoch,
        execution_manifest_sha256=stored.execution_manifest_sha256,
        allocation_epoch=stored.allocation_epoch,
        subject_id=stored.subject_id,
        subject_incarnation=stored.subject_incarnation,
        pool_id=stored.pool_id,
        reporter_incarnation=stored.reporter_incarnation,
        protected_admission_sha256=stored.protected_admission_sha256,
        manager_input_digest=stored.manager_input_digest,
        manager_allocation_digest=changed.manager_allocation_digest,
        proposal_digest=changed_digest,
        proposal_payload=changed.model_dump(mode="json", exclude_none=False),
        expires_at=changed.lease_not_after,
    )
    await capacity_session.execute(
        text(
            "ALTER TABLE public.capacity_executable_admission_proposals "
            "DISABLE TRIGGER capacity_executable_admission_proposals_append_only_guard"
        )
    )
    try:
        await capacity_session.execute(
            text(
                "DELETE FROM public.capacity_executable_admission_proposals "
                "WHERE proposal_id = :proposal_id"
            ),
            {"proposal_id": stored.proposal_id},
        )
    finally:
        await capacity_session.execute(
            text(
                "ALTER TABLE public.capacity_executable_admission_proposals "
                "ENABLE TRIGGER capacity_executable_admission_proposals_append_only_guard"
            )
        )
    capacity_session.expunge(stored)

    with pytest.raises(DBAPIError, match="payload is not exact"):
        async with capacity_session.begin_nested():
            capacity_session.add(replacement)
            await capacity_session.flush()


async def test_admission_ack_guard_rejects_skeletal_plan_and_ack_under_direct_sql(
    capacity_session: AsyncSession,
) -> None:
    """Acknowledgement insertion must revalidate the full sealed manager plan."""

    store = CapacityExecutionStore()
    _executor, _bindings, proposal = await _batched_admission_proposal(
        store, capacity_session
    )
    acknowledgement = _admission_acknowledgement(proposal)
    await _replace_admission_proposal_payload_without_guard(
        capacity_session,
        proposal_id=proposal.proposal_id,
        proposal_payload=_skeletal_admission_proposal_payload(proposal),
    )

    with pytest.raises(DBAPIError, match="proposal payload is not exact"):
        async with capacity_session.begin_nested():
            await _insert_direct_admission_acknowledgement(
                capacity_session,
                acknowledgement,
                acknowledgement_payload=(
                    _skeletal_admission_acknowledgement_payload(acknowledgement)
                ),
            )


async def test_intent_launch_guard_rejects_skeletal_plan_and_ack_under_direct_sql(
    capacity_session: AsyncSession,
) -> None:
    """Test-only stored skeletal evidence must remain unable to open launch."""

    store = CapacityExecutionStore()
    _executor, bindings, proposal = await _batched_admission_proposal(
        store, capacity_session
    )
    acknowledgement = _admission_acknowledgement(proposal)
    await _replace_admission_proposal_payload_without_guard(
        capacity_session,
        proposal_id=proposal.proposal_id,
        proposal_payload=_skeletal_admission_proposal_payload(proposal),
    )
    await _insert_unchecked_admission_acknowledgement(
        capacity_session,
        acknowledgement,
        acknowledgement_payload=_skeletal_admission_acknowledgement_payload(
            acknowledgement
        ),
    )

    with pytest.raises(DBAPIError, match="admission acknowledgement"):
        async with capacity_session.begin_nested():
            await capacity_session.execute(
                update(CapacityExecutableIntent)
                .where(CapacityExecutableIntent.intent_id == bindings[0].intent_id)
                .values(state="launch-ready", launch_ready_at=func.clock_timestamp())
            )


async def test_intent_launch_guard_rejects_duplicate_assignment_coverage(
    capacity_session: AsyncSession,
) -> None:
    """Distinct transitions cannot cover one allowance twice and omit another."""

    store = CapacityExecutionStore()
    _executor, bindings, proposal = await _batched_admission_proposal(
        store, capacity_session
    )
    acknowledgement = _admission_acknowledgement(proposal)
    payload = acknowledgement.model_dump(mode="json", exclude_none=False)
    assignments = cast(list[dict[str, object]], payload["assignments"])
    assert len(assignments) == 2
    for field in (
        "allowance_id",
        "protected_attempt_id",
        "shape_instance_id",
        "shape_slot_index",
        "submission_intent_id",
    ):
        assignments[1][field] = assignments[0][field]
    await _insert_unchecked_admission_acknowledgement(
        capacity_session,
        acknowledgement,
        acknowledgement_payload=payload,
    )

    with pytest.raises(DBAPIError, match="admission acknowledgement"):
        async with capacity_session.begin_nested():
            await capacity_session.execute(
                update(CapacityExecutableIntent)
                .where(CapacityExecutableIntent.intent_id == bindings[0].intent_id)
                .values(state="launch-ready", launch_ready_at=func.clock_timestamp())
            )


async def test_admission_ack_guard_rejects_truncated_stored_proposal_under_direct_sql(
    capacity_session: AsyncSession,
) -> None:
    """Acknowledgement insertion must independently recheck exact tranche coverage."""

    store = CapacityExecutionStore()
    _executor, _bindings, proposal = await _batched_admission_proposal(
        store, capacity_session
    )
    truncated_payload = proposal.model_dump(mode="json", exclude_none=False)
    truncated_payload["shapes"] = cast(list[object], truncated_payload["shapes"])[:1]
    await _replace_admission_proposal_payload_without_guard(
        capacity_session,
        proposal_id=proposal.proposal_id,
        proposal_payload=truncated_payload,
    )

    with pytest.raises(DBAPIError, match="proposal payload is not exact"):
        async with capacity_session.begin_nested():
            await _insert_direct_admission_acknowledgement(
                capacity_session,
                _admission_acknowledgement(proposal),
            )


async def test_intent_launch_guard_requires_exact_proposal_shape_under_direct_sql(
    capacity_session: AsyncSession,
) -> None:
    """Tranche-level evidence must not authorize an intent omitted from the plan."""

    store = CapacityExecutionStore()
    _executor, bindings, proposal = await _batched_admission_proposal(
        store, capacity_session
    )
    acknowledgement = _admission_acknowledgement(proposal)
    await _insert_direct_admission_acknowledgement(capacity_session, acknowledgement)
    corrupted_payload = proposal.model_dump(mode="json", exclude_none=False)
    corrupted_payload["shapes"] = cast(list[object], corrupted_payload["shapes"])[1:]
    await _replace_admission_proposal_payload_without_guard(
        capacity_session,
        proposal_id=proposal.proposal_id,
        proposal_payload=corrupted_payload,
    )

    with pytest.raises(DBAPIError, match="admission acknowledgement"):
        async with capacity_session.begin_nested():
            await capacity_session.execute(
                update(CapacityExecutableIntent)
                .where(CapacityExecutableIntent.intent_id == bindings[0].intent_id)
                .values(state="launch-ready", launch_ready_at=func.clock_timestamp())
            )


async def test_admission_ack_guard_rejects_nested_execution_fence_forgery(
    capacity_session: AsyncSession,
) -> None:
    """Changing only the payload fence must not preserve trusted acknowledgement columns."""

    store = CapacityExecutionStore()
    _executor, _binding, proposal = await _bootstrap_acknowledged_admission(
        store, capacity_session
    )
    acknowledgement = _admission_acknowledgement(proposal)
    payload = acknowledgement.model_dump(mode="json", exclude_none=False)
    execution = cast(dict[str, object], payload["execution"])
    execution["execution_epoch"] = acknowledgement.execution.execution_epoch + 1

    with pytest.raises(DBAPIError, match="execution fence changed"):
        async with capacity_session.begin_nested():
            await _insert_direct_admission_acknowledgement(
                capacity_session,
                acknowledgement,
                acknowledgement_payload=payload,
            )


async def _make_admission_authority_stale(
    session: AsyncSession,
    *,
    staleness: str,
    binding,  # type: ignore[no-untyped-def]
    acknowledgement: ExecutableAdmissionAcknowledgementV2,
) -> None:
    if staleness == "reporter-rotated":
        await session.execute(
            update(CapacityDemandReporter)
            .where(
                CapacityDemandReporter.subject_id == binding.subject_id,
                CapacityDemandReporter.subject_incarnation == binding.subject_incarnation,
                CapacityDemandReporter.reporter_incarnation
                == acknowledgement.reporter_incarnation,
            )
            .values(state="fenced")
        )
        return
    if staleness == "newer-allocation":
        await _clone_sealed_epoch(
            session,
            allocation_epoch=binding.execution.allocation_epoch,
            input_valid_until=datetime.now(UTC) + timedelta(minutes=5),
        )
        return
    triggers = (
        "capacity_executable_allocation_seal_guard",
        "capacity_allocation_epoch_binding_guard",
    )
    for trigger_name in triggers:
        await session.execute(
            text(
                "ALTER TABLE public.capacity_allocation_epochs "
                f"DISABLE TRIGGER {trigger_name}"
            )
        )
    try:
        await session.execute(
            update(CapacityAllocationEpoch)
            .where(
                CapacityAllocationEpoch.allocation_epoch
                == binding.execution.allocation_epoch
            )
            .values(input_valid_until=datetime.now(UTC) - timedelta(seconds=1))
        )
    finally:
        for trigger_name in reversed(triggers):
            await session.execute(
                text(
                    "ALTER TABLE public.capacity_allocation_epochs "
                    f"ENABLE TRIGGER {trigger_name}"
                )
            )


@pytest.mark.parametrize(
    ("staleness", "error"),
    (
        ("reporter-rotated", "reporter changed"),
        ("newer-allocation", "allocation changed"),
        ("allocation-input-expired", "allocation changed or expired"),
    ),
)
async def test_admission_ack_guard_revalidates_current_authority_under_direct_sql(
    capacity_session: AsyncSession,
    staleness: str,
    error: str,
) -> None:
    """Persisting stale admission evidence would let the intent trigger open launch."""

    store = CapacityExecutionStore()
    _executor, binding, proposal = await _bootstrap_acknowledged_admission(
        store, capacity_session
    )
    acknowledgement = _admission_acknowledgement(proposal)
    await _make_admission_authority_stale(
        capacity_session,
        staleness=staleness,
        binding=binding,
        acknowledgement=acknowledgement,
    )

    with pytest.raises(DBAPIError, match=error):
        async with capacity_session.begin_nested():
            await _insert_direct_admission_acknowledgement(
                capacity_session, acknowledgement
            )


@pytest.mark.parametrize(
    "staleness",
    ("reporter-rotated", "newer-allocation", "allocation-input-expired"),
)
async def test_intent_launch_guard_revalidates_admission_freshness_under_direct_sql(
    capacity_session: AsyncSession,
    staleness: str,
) -> None:
    """Admission evidence that becomes stale must never open launch readiness."""

    store = CapacityExecutionStore()
    _executor, binding, proposal = await _bootstrap_acknowledged_admission(
        store, capacity_session
    )
    acknowledgement = _admission_acknowledgement(proposal)
    await _insert_direct_admission_acknowledgement(capacity_session, acknowledgement)
    await _make_admission_authority_stale(
        capacity_session,
        staleness=staleness,
        binding=binding,
        acknowledgement=acknowledgement,
    )

    with pytest.raises(DBAPIError, match="admission acknowledgement"):
        async with capacity_session.begin_nested():
            await capacity_session.execute(
                update(CapacityExecutableIntent)
                .where(CapacityExecutableIntent.intent_id == binding.intent_id)
                .values(state="launch-ready", launch_ready_at=func.clock_timestamp())
            )


@pytest.mark.parametrize(
    ("forgery", "error"),
    (
        ("wrong-reporter", "reporter changed"),
        ("wrong-protected-admission", "protected admission changed"),
        ("unknown-payload-key", "payload binding changed"),
    ),
)
async def test_bootstrap_ack_guard_rejects_direct_sql_forgery(
    capacity_session: AsyncSession,
    forgery: str,
    error: str,
) -> None:
    store = CapacityExecutionStore()
    _executor, binding, proposal = await _proposed_bootstrap(store, capacity_session)
    acknowledgement = ExecutableBootstrapAcknowledgementV2(
        binding=binding,
        proposal_epoch=proposal.proposal_epoch,
        proposal_digest=store.contract_digest(proposal),
        reporter_incarnation=demand_snapshot().reporter_incarnation,
        bootstrap_registration_epoch=1,
        bootstrap_evidence_sha256="8" * 64,
        protected_admission_sha256="3" * 64,
    )
    payload = acknowledgement.model_dump(mode="json", exclude_none=False)
    if forgery == "wrong-reporter":
        acknowledgement = acknowledgement.model_copy(update={"reporter_incarnation": uuid4()})
    elif forgery == "wrong-protected-admission":
        acknowledgement = acknowledgement.model_copy(
            update={"protected_admission_sha256": "f" * 64}
        )
    else:
        payload["unexpected"] = "forged"

    with pytest.raises(DBAPIError, match=error):
        async with capacity_session.begin_nested():
            await _insert_direct_bootstrap_acknowledgement(
                capacity_session,
                acknowledgement,
                acknowledgement_payload=(payload if forgery == "unknown-payload-key" else None),
            )


async def test_direct_sql_cannot_mark_an_unacknowledged_intent_launch_ready(
    capacity_session: AsyncSession,
) -> None:
    store = CapacityExecutionStore()
    _executor, binding, _proposal = await _proposed_bootstrap(store, capacity_session)

    with pytest.raises(DBAPIError, match="bootstrap acknowledgement"):
        async with capacity_session.begin_nested():
            await capacity_session.execute(
                update(CapacityExecutableIntent)
                .where(CapacityExecutableIntent.intent_id == binding.intent_id)
                .values(
                    state="launch-ready",
                    bootstrap_registration_epoch=1,
                    bootstrap_evidence_sha256="8" * 64,
                    launch_ready_at=func.clock_timestamp(),
                )
            )


async def test_bootstrap_proposal_supersession_uses_wall_clock_and_fences_stale_ack(
    capacity_session: AsyncSession,
) -> None:
    store = CapacityExecutionStore()
    _executor, binding, first = await _proposed_bootstrap(
        store,
        capacity_session,
        expires_in=timedelta(seconds=1),
    )
    database_now = (await capacity_session.execute(select(func.clock_timestamp()))).scalar_one()
    second = first.model_copy(
        update={
            "command_sequence": 3,
            "proposal_epoch": 2,
            "bootstrap_sha256": "9" * 64,
            "expires_at": database_now + timedelta(minutes=1),
        }
    )
    with pytest.raises(ExecutionConflictError, match="still current"):
        await store.propose_bootstrap(capacity_session, second)

    await capacity_session.execute(text("SELECT pg_sleep(1.1)"))
    database_now = (await capacity_session.execute(select(func.clock_timestamp()))).scalar_one()
    second = second.model_copy(update={"expires_at": database_now + timedelta(minutes=1)})
    await store.propose_bootstrap(capacity_session, second)
    subject_work = await store.next_subject_bootstrap(
        capacity_session,
        subject_id=binding.subject_id,
        subject_incarnation=binding.subject_incarnation,
        reporter_incarnation=demand_snapshot().reporter_incarnation,
    )
    assert subject_work == second

    stale = ExecutableBootstrapAcknowledgementV2(
        binding=binding,
        proposal_epoch=first.proposal_epoch,
        proposal_digest=store.contract_digest(first),
        reporter_incarnation=demand_snapshot().reporter_incarnation,
        bootstrap_registration_epoch=1,
        bootstrap_evidence_sha256="8" * 64,
        protected_admission_sha256="3" * 64,
    )
    with pytest.raises(DBAPIError, match="proposal changed or expired"):
        async with capacity_session.begin_nested():
            await _insert_direct_bootstrap_acknowledgement(capacity_session, stale)


async def test_bootstrap_evidence_is_append_only_under_direct_sql(
    capacity_session: AsyncSession,
) -> None:
    store = CapacityExecutionStore()
    await _launch_ready(store, capacity_session)

    for statement in (
        "UPDATE public.capacity_executable_bootstrap_proposals "
        "SET bootstrap_sha256 = repeat('f', 64)",
        "DELETE FROM public.capacity_executable_bootstrap_proposals",
        "TRUNCATE public.capacity_executable_bootstrap_proposals CASCADE",
        "UPDATE public.capacity_executable_bootstrap_acknowledgements "
        "SET bootstrap_evidence_sha256 = repeat('f', 64)",
        "DELETE FROM public.capacity_executable_bootstrap_acknowledgements",
        "TRUNCATE public.capacity_executable_bootstrap_acknowledgements",
    ):
        with pytest.raises(DBAPIError, match="append-only"):
            async with capacity_session.begin_nested():
                await capacity_session.execute(text(statement))

    assert (
        await capacity_session.execute(select(func.count(CapacityExecutableBootstrapProposal.id)))
    ).scalar_one() == 1
    assert (
        await capacity_session.execute(
            select(func.count(CapacityExecutableBootstrapAcknowledgement.id))
        )
    ).scalar_one() == 1


async def test_old_permit_cannot_consume_after_newer_sealed_epoch(
    capacity_session: AsyncSession,
) -> None:
    store = CapacityExecutionStore()
    permit = await _launch_ready(store, capacity_session)
    await _clone_sealed_epoch(
        capacity_session,
        allocation_epoch=permit.binding.execution.allocation_epoch,
        input_valid_until=datetime.now(UTC) + timedelta(minutes=5),
    )

    with pytest.raises(ExecutionConflictError, match="allocation"):
        await store.consume_launch_permit(
            capacity_session,
            ExecutablePermitConsumptionV2(
                permit_id=permit.permit_id,
                permit_digest=store.contract_digest(permit),
                binding=permit.binding,
                command_sequence=3,
            ),
        )


async def test_expired_allocation_input_cannot_propose(
    capacity_session: AsyncSession,
) -> None:
    store = CapacityExecutionStore()
    active, allocation_epoch = await _active_plan(capacity_session)
    await _heartbeat(store, capacity_session, active, pool_id="gb10")
    await _clone_sealed_epoch(
        capacity_session,
        allocation_epoch=allocation_epoch,
        input_valid_until=datetime.now(UTC) - timedelta(seconds=1),
    )

    work = await store.next_pool_work(capacity_session, executor_binding("gb10"))

    assert work is None


async def test_expired_allocation_input_cannot_consume(
    capacity_session: AsyncSession,
) -> None:
    store = CapacityExecutionStore()
    permit = await _launch_ready(store, capacity_session)
    prior = (
        await capacity_session.execute(
            select(CapacityExecutableIntent).where(
                CapacityExecutableIntent.intent_id == permit.binding.intent_id
            )
        )
    ).scalar_one()
    await _test_only_release_intent(capacity_session, prior.intent_id)
    await capacity_session.refresh(prior)
    expired_epoch = await _clone_sealed_epoch(
        capacity_session,
        allocation_epoch=permit.binding.execution.allocation_epoch,
        input_valid_until=datetime.now(UTC) - timedelta(seconds=1),
    )
    expired_execution = permit.binding.execution.model_copy(
        update={"allocation_epoch": expired_epoch}
    )
    expired_binding = permit.binding.model_copy(
        update={
            "execution": expired_execution,
            "tranche_id": uuid4(),
            "intent_id": uuid4(),
            "shape_instance_id": f"{permit.binding.shape_instance_id}-expired",
        }
    )
    expired_row = CapacityExecutableIntent(
        intent_id=expired_binding.intent_id,
        tranche_id=expired_binding.tranche_id,
        shape_instance_id=expired_binding.shape_instance_id,
        execution_epoch=prior.execution_epoch,
        execution_manifest_sha256=prior.execution_manifest_sha256,
        configuration_epoch=prior.configuration_epoch,
        allocation_epoch=expired_epoch,
        executor_id=prior.executor_id,
        executor_incarnation=prior.executor_incarnation,
        pool_id=prior.pool_id,
        pool_generation=prior.pool_generation,
        subject_id=prior.subject_id,
        subject_incarnation=prior.subject_incarnation,
        launch_rank=1,
        proposal_digest=prior.proposal_digest,
        proposal_payload=json.loads(json.dumps(prior.proposal_payload)),
        binding_digest=store.contract_digest(expired_binding),
        binding_payload=expired_binding.model_dump(mode="json", exclude_none=False),
        state="launch-ready",
        accepted_at=datetime.now(UTC),
        bootstrap_registration_epoch=1,
        bootstrap_evidence_sha256="8" * 64,
        launch_ready_at=datetime.now(UTC),
    )
    await _test_only_add_intent_without_guard(capacity_session, expired_row)
    expired_permit = _test_only_synthetic_permit(
        expired_row,
        expired_binding,
        datetime.now(UTC),
    )
    expired_row.permit_id = expired_permit.permit_id
    expired_row.permit_epoch = expired_permit.permit_epoch
    expired_row.permit_digest = store.contract_digest(expired_permit)
    expired_row.permit_payload = expired_permit.model_dump(mode="json", exclude_none=False)
    expired_row.permit_expires_at = expired_permit.expires_at
    expired_row.state = "permitted"

    with pytest.raises(ExecutionConflictError, match="allocation input expired"):
        await store.consume_launch_permit(
            capacity_session,
            ExecutablePermitConsumptionV2(
                permit_id=expired_permit.permit_id,
                permit_digest=store.contract_digest(expired_permit),
                binding=expired_binding,
                command_sequence=3,
            ),
        )


async def test_expired_permit_is_reissued_without_deadlocking_queue(
    capacity_session: AsyncSession,
) -> None:
    store = CapacityExecutionStore()
    permit = await _launch_ready(store, capacity_session)
    await _test_only_update_without_guard(
        capacity_session,
        table_name="capacity_executable_intents",
        trigger_name="capacity_executable_intent_mutation_guard",
        statement=(
            update(CapacityExecutableIntent)
            .where(CapacityExecutableIntent.intent_id == permit.binding.intent_id)
            .values(permit_expires_at=datetime.now(UTC) - timedelta(seconds=1))
        ),
    )

    replacement = await store.next_pool_work(
        capacity_session, executor_binding(permit.binding.pool_id)
    )

    assert replacement is not None
    assert replacement.permit_id != permit.permit_id
    assert replacement.permit_epoch == permit.permit_epoch + 1


async def test_permit_consumption_spends_all_executable_rate_tokens(
    capacity_session: AsyncSession,
) -> None:
    store = CapacityExecutionStore()
    permit = await _launch_ready(store, capacity_session)

    await store.consume_launch_permit(
        capacity_session,
        ExecutablePermitConsumptionV2(
            permit_id=permit.permit_id,
            permit_digest=store.contract_digest(permit),
            binding=permit.binding,
            command_sequence=3,
        ),
    )

    rows = (
        await capacity_session.execute(
            text(
                "SELECT scope, capacity_microtokens - available_microtokens "
                "FROM capacity_executable_launch_rate_buckets ORDER BY scope"
            )
        )
    ).all()
    assert rows == [
        ("account", 1_000_000),
        ("global", 1_000_000),
        ("pool", 1_000_000),
        ("subject", 1_000_000),
    ]


async def test_consume_launch_permit_locks_executor_context_before_intent(
    capacity_session: AsyncSession,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = CapacityExecutionStore()
    permit = await _launch_ready(store, capacity_session)
    lock_order = _record_queue_lock_order(monkeypatch, store)

    await store.consume_launch_permit(
        capacity_session,
        ExecutablePermitConsumptionV2(
            permit_id=permit.permit_id,
            permit_digest=store.contract_digest(permit),
            binding=permit.binding,
            command_sequence=3,
        ),
    )

    assert lock_order[:5] == [
        "authority",
        "epoch",
        "executor-registration",
        "executor-state",
        "intent",
    ]


async def test_consume_launch_permit_locks_allocation_intents_in_canonical_order(
    capacity_session: AsyncSession,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = CapacityExecutionStore()
    permit = await _launch_ready(store, capacity_session)
    earlier_row = (
        await capacity_session.execute(
            select(CapacityExecutableIntent).where(
                CapacityExecutableIntent.intent_id == permit.binding.intent_id
            )
        )
    ).scalar_one()
    await _test_only_release_intent(capacity_session, earlier_row.intent_id)
    await capacity_session.refresh(earlier_row)
    target_binding = permit.binding.model_copy(
        update={
            "intent_id": uuid4(),
            "tranche_id": uuid4(),
            "shape_instance_id": f"{permit.binding.shape_instance_id}-later",
        }
    )
    target_row = CapacityExecutableIntent(
        intent_id=target_binding.intent_id,
        tranche_id=target_binding.tranche_id,
        shape_instance_id=target_binding.shape_instance_id,
        execution_epoch=earlier_row.execution_epoch,
        execution_manifest_sha256=earlier_row.execution_manifest_sha256,
        configuration_epoch=earlier_row.configuration_epoch,
        allocation_epoch=earlier_row.allocation_epoch,
        executor_id=earlier_row.executor_id,
        executor_incarnation=earlier_row.executor_incarnation,
        pool_id=earlier_row.pool_id,
        pool_generation=earlier_row.pool_generation,
        subject_id=earlier_row.subject_id,
        subject_incarnation=earlier_row.subject_incarnation,
        launch_rank=earlier_row.launch_rank + 1,
        proposal_digest=earlier_row.proposal_digest,
        proposal_payload=json.loads(json.dumps(earlier_row.proposal_payload)),
        binding_digest=store.contract_digest(target_binding),
        binding_payload=target_binding.model_dump(mode="json", exclude_none=False),
        state="launch-ready",
        accepted_at=datetime.now(UTC),
        bootstrap_registration_epoch=1,
        bootstrap_evidence_sha256="8" * 64,
        launch_ready_at=datetime.now(UTC),
    )
    await _test_only_add_intent_without_guard(capacity_session, target_row)
    target_permit = _test_only_synthetic_permit(
        target_row,
        target_binding,
        datetime.now(UTC),
    )
    target_row.permit_id = target_permit.permit_id
    target_row.permit_epoch = target_permit.permit_epoch
    target_row.permit_digest = store.contract_digest(target_permit)
    target_row.permit_payload = target_permit.model_dump(mode="json", exclude_none=False)
    target_row.permit_expires_at = target_permit.expires_at
    target_row.state = "permitted"
    lock_order: list[UUID] = []
    original_locked_allocation_intents = getattr(store, "_locked_allocation_intents", None)

    async def record_locked_allocation_intents(session, target):  # type: ignore[no-untyped-def]
        result = await original_locked_allocation_intents(session, target)
        lock_order.extend(item.intent_id for item in result)
        return result

    monkeypatch.setattr(
        store,
        "_locked_allocation_intents",
        record_locked_allocation_intents,
        raising=False,
    )

    await store.consume_launch_permit(
        capacity_session,
        ExecutablePermitConsumptionV2(
            permit_id=target_permit.permit_id,
            permit_digest=store.contract_digest(target_permit),
            binding=target_binding,
            command_sequence=3,
        ),
    )

    assert lock_order == [earlier_row.intent_id, target_binding.intent_id]


async def test_crash_before_submit_recovery_quarantines_and_remains_charged(
    capacity_session: AsyncSession,
) -> None:
    store = CapacityExecutionStore()
    permit, inventory = await _consume_then_publish_empty_inventory(store, capacity_session)
    recovery = _submission_recovery(store, permit, inventory)

    recovered = await store.recover_unsubmitted_permit(capacity_session, recovery)

    row = await _intent_row(capacity_session, permit.binding.intent_id)
    assert recovered.intent_id == permit.binding.intent_id
    assert recovered.replayed is False
    assert row.state == "quarantined"
    assert row.inventory_sequence is None
    assert row.observed_state is None
    assert row.terminal_kind is None
    assert row.terminal_identity is None
    assert row.terminal_evidence_sha256 is None
    receipt_payload = (
        await capacity_session.execute(
            text(
                "SELECT result_payload FROM capacity_executable_command_receipts "
                "WHERE executor_incarnation = :executor_incarnation "
                "AND command_sequence = :command_sequence "
                "AND operation_kind = 'submission-recovery'"
            ),
            {
                "executor_incarnation": permit.binding.executor_incarnation,
                "command_sequence": recovery.command_sequence,
            },
        )
    ).scalar_one()
    assert receipt_payload["recovery"]["inventory_sequence"] == inventory.inventory_sequence
    assert receipt_payload["recovery"]["inventory_digest"] == store.contract_digest(inventory)

    assert await store.next_pool_work(capacity_session, executor_binding("gb10")) is None
    context = await store._locked_execution_context(
        capacity_session,
        permit.binding.execution,
        executor_binding(permit.binding.pool_id),
    )
    await capacity_session.execute(
        update(CapacityAuthorityState)
        .where(CapacityAuthorityState.singleton_id == 1)
        .values(global_pending_slot_ceiling=1, global_pending_job_ceiling=1)
    )
    second_binding = permit.binding.model_copy(
        update={
            "tranche_id": uuid4(),
            "intent_id": uuid4(),
            "shape_instance_id": f"{permit.binding.shape_instance_id}-second",
        }
    )
    second_row = CapacityExecutableIntent(
        intent_id=second_binding.intent_id,
        tranche_id=second_binding.tranche_id,
        shape_instance_id=second_binding.shape_instance_id,
        execution_epoch=row.execution_epoch,
        execution_manifest_sha256=row.execution_manifest_sha256,
        configuration_epoch=row.configuration_epoch,
        allocation_epoch=row.allocation_epoch,
        executor_id=row.executor_id,
        executor_incarnation=row.executor_incarnation,
        pool_id=row.pool_id,
        pool_generation=row.pool_generation,
        subject_id=row.subject_id,
        subject_incarnation=row.subject_incarnation,
        launch_rank=row.launch_rank + 1,
        proposal_digest=row.proposal_digest,
        proposal_payload=json.loads(json.dumps(row.proposal_payload)),
        binding_digest=store.contract_digest(second_binding),
        binding_payload=second_binding.model_dump(mode="json", exclude_none=False),
        state="proposed",
    )
    await _test_only_add_intent_without_guard(capacity_session, second_row)
    with pytest.raises(ExecutionConflictError, match="global pending limit"):
        await store._assert_pending_limits(capacity_session, context, second_row)
    with pytest.raises(ExecutionConflictError, match="capacity ceiling"):
        await store._assert_increase_eligible(
            capacity_session,
            context,
            proposed=permit.binding,
        )

    replay = await store.recover_unsubmitted_permit(capacity_session, recovery)
    assert replay.replayed is True
    replay_row = await _intent_row(capacity_session, permit.binding.intent_id)
    assert replay_row.state == "quarantined"
    assert replay_row.terminal_kind is None


async def test_crash_before_submit_recovery_rejects_preconsumption_inventory(
    capacity_session: AsyncSession,
) -> None:
    store = CapacityExecutionStore()
    permit = await _launch_ready(store, capacity_session)
    await store.consume_launch_permit(
        capacity_session,
        ExecutablePermitConsumptionV2(
            permit_id=permit.permit_id,
            permit_digest=store.contract_digest(permit),
            binding=permit.binding,
            command_sequence=3,
        ),
    )
    runtime = (
        await capacity_session.execute(
            select(CapacityExecutableExecutorState).where(
                CapacityExecutableExecutorState.executor_incarnation
                == permit.binding.executor_incarnation
            )
        )
    ).scalar_one()
    assert runtime.last_inventory_digest is not None
    recovery = executable_contracts_module.ExecutableSubmissionRecoveryV2(
        binding=permit.binding,
        permit_id=permit.permit_id,
        permit_digest=store.contract_digest(permit),
        command_sequence=4,
        inventory_sequence=runtime.inventory_high_water,
        inventory_digest=runtime.last_inventory_digest,
        controller_query_completed_at=datetime.now(UTC),
        submit_process_absent=True,
        scheduler_submission_absent=True,
        controller_evidence_sha256="a" * 64,
    )

    with pytest.raises(ExecutionConflictError, match="post-consumption inventory"):
        await store.recover_unsubmitted_permit(capacity_session, recovery)


async def test_crash_before_submit_recovery_survives_drain_only_but_blocks_retirement(
    capacity_session: AsyncSession,
) -> None:
    store = CapacityExecutionStore()
    permit = await _launch_ready(store, capacity_session)
    await store.consume_launch_permit(
        capacity_session,
        ExecutablePermitConsumptionV2(
            permit_id=permit.permit_id,
            permit_digest=store.contract_digest(permit),
            binding=permit.binding,
            command_sequence=3,
        ),
    )
    await CapacityManagementStore().register_writer(
        capacity_session,
        permit.binding.execution.authority_incarnation,
        expected_epoch=permit.binding.execution.writer_epoch,
    )
    inventory = await _next_inventory(
        capacity_session,
        _inventory_execution(permit.binding),
        permit.binding,
    )
    await store.ingest_executor_inventory(capacity_session, inventory)
    recovery = _submission_recovery(store, permit, inventory)

    recovered = await store.recover_unsubmitted_permit(capacity_session, recovery)

    assert recovered.intent_id == permit.binding.intent_id
    row = await _intent_row(capacity_session, permit.binding.intent_id)
    assert row.state == "quarantined"
    assert row.inventory_sequence is None
    assert row.terminal_kind is None
    assert row.terminal_identity is None
    assert row.terminal_evidence_sha256 is None
    assert await store.next_pool_work(capacity_session, executor_binding("gb10")) is None

    execution = _inventory_execution(permit.binding)
    await _heartbeat(store, capacity_session, execution, pool_id="oldlab")
    await _post_inventory_heartbeat(store, capacity_session, execution, pool_id="gb10")
    await _post_inventory_heartbeat(store, capacity_session, execution, pool_id="oldlab")
    checkpoints = tuple(
        [
            await _mark_retirement_safe(capacity_session, pool_id="gb10"),
            await _mark_retirement_safe(capacity_session, pool_id="oldlab"),
        ]
    )
    authority = (
        await capacity_session.execute(
            select(CapacityAuthorityState).where(CapacityAuthorityState.singleton_id == 1)
        )
    ).scalar_one()
    epoch = (
        await capacity_session.execute(
            select(CapacityAllocationEpoch.execution_epoch).where(
                CapacityAllocationEpoch.allocation_epoch
                == permit.binding.execution.allocation_epoch
            )
        )
    ).scalar_one()
    del epoch
    retirement = ExecutionRetirementV2(
        authority_incarnation=authority.authority_incarnation,
        expected_writer_epoch=authority.writer_epoch,
        execution_epoch=permit.binding.execution.execution_epoch,
        execution_manifest_sha256=permit.binding.execution.execution_manifest_sha256,
        executor_checkpoints=checkpoints,
    )

    with pytest.raises(ExecutionConflictError, match="every executable intent must be released"):
        await CapacityManagementStore().retire_execution_epoch(
            capacity_session,
            retirement,
            actor="activation-operator",
            idempotency_key=UUID(int=995),
        )


async def test_permit_consumption_rechecks_final_deadlines_without_committing_state(
    capacity_session: AsyncSession,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = CapacityExecutionStore()
    permit = await _launch_ready(store, capacity_session)
    base = datetime(2026, 1, 1, tzinfo=UTC)
    final_now = base + timedelta(seconds=2)
    deadline = final_now + timedelta(milliseconds=500)
    row = (
        await capacity_session.execute(
            select(CapacityExecutableIntent).where(
                CapacityExecutableIntent.intent_id == permit.binding.intent_id
            )
        )
    ).scalar_one()
    runtime = (
        await capacity_session.execute(
            select(CapacityExecutableExecutorState).where(
                CapacityExecutableExecutorState.executor_incarnation
                == permit.binding.executor_incarnation
            )
        )
    ).scalar_one()
    await _test_only_update_without_guard(
        capacity_session,
        table_name="capacity_executable_intents",
        trigger_name="capacity_executable_intent_mutation_guard",
        statement=(
            update(CapacityExecutableIntent)
            .where(CapacityExecutableIntent.intent_id == row.intent_id)
            .values(permit_expires_at=deadline)
        ),
    )
    await _test_only_update_without_guard(
        capacity_session,
        table_name="capacity_executable_executor_states",
        trigger_name="capacity_executable_executor_state_mutation_guard",
        statement=(
            update(CapacityExecutableExecutorState)
            .where(CapacityExecutableExecutorState.id == runtime.id)
            .values(lease_expires_at=deadline, last_inventory_at=base)
        ),
    )
    await capacity_session.refresh(row)
    await capacity_session.refresh(runtime)
    await capacity_session.execute(
        update(CapacityPoolObservation)
        .where(CapacityPoolObservation.pool_id == permit.binding.pool_id)
        .values(database_received_at=base)
    )
    times = iter((base, base, base, base, final_now))

    async def fake_database_now(session):  # type: ignore[no-untyped-def]
        del session
        return next(times)

    monkeypatch.setattr(execution_store_module, "_database_now", fake_database_now)

    with pytest.raises(ExecutionConflictError, match="final deadline"):
        await store.consume_launch_permit(
            capacity_session,
            ExecutablePermitConsumptionV2(
                permit_id=permit.permit_id,
                permit_digest=store.contract_digest(permit),
                binding=permit.binding,
                command_sequence=3,
            ),
        )

    await capacity_session.refresh(row)
    assert row.state == "permitted"
    assert row.permit_consumed_at is None
    assert (
        await capacity_session.execute(
            text("SELECT count(*) FROM capacity_executable_launch_rate_buckets")
        )
    ).scalar_one() == 0


async def test_permit_consumption_rechecks_non_target_pool_freshness_at_final_fence(
    capacity_session: AsyncSession,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = CapacityExecutionStore(inventory_freshness_seconds=3)
    permit = await _launch_ready(store, capacity_session)
    base = datetime(2026, 1, 1, tzinfo=UTC)
    final_now = base + timedelta(seconds=2)
    row = (
        await capacity_session.execute(
            select(CapacityExecutableIntent).where(
                CapacityExecutableIntent.intent_id == permit.binding.intent_id
            )
        )
    ).scalar_one()
    runtime = (
        await capacity_session.execute(
            select(CapacityExecutableExecutorState).where(
                CapacityExecutableExecutorState.executor_incarnation
                == permit.binding.executor_incarnation
            )
        )
    ).scalar_one()
    await _test_only_update_without_guard(
        capacity_session,
        table_name="capacity_executable_intents",
        trigger_name="capacity_executable_intent_mutation_guard",
        statement=(
            update(CapacityExecutableIntent)
            .where(CapacityExecutableIntent.intent_id == row.intent_id)
            .values(permit_expires_at=base + timedelta(minutes=5))
        ),
    )
    await _test_only_update_without_guard(
        capacity_session,
        table_name="capacity_executable_executor_states",
        trigger_name="capacity_executable_executor_state_mutation_guard",
        statement=(
            update(CapacityExecutableExecutorState)
            .where(CapacityExecutableExecutorState.id == runtime.id)
            .values(
                lease_expires_at=base + timedelta(minutes=5),
                last_inventory_at=base + timedelta(seconds=1),
            )
        ),
    )
    await capacity_session.refresh(row)
    await capacity_session.refresh(runtime)
    await capacity_session.execute(
        update(CapacityPoolObservation)
        .where(CapacityPoolObservation.pool_id == permit.binding.pool_id)
        .values(database_received_at=base + timedelta(seconds=1))
    )
    await capacity_session.execute(
        update(CapacityPoolObservation)
        .where(CapacityPoolObservation.pool_id == "oldlab")
        .values(database_received_at=base)
    )
    times = iter((base, base, base, base, final_now))

    async def fake_database_now(session):  # type: ignore[no-untyped-def]
        del session
        return next(times)

    monkeypatch.setattr(execution_store_module, "_database_now", fake_database_now)

    with pytest.raises(ExecutionConflictError, match="final deadline"):
        await store.consume_launch_permit(
            capacity_session,
            ExecutablePermitConsumptionV2(
                permit_id=permit.permit_id,
                permit_digest=store.contract_digest(permit),
                binding=permit.binding,
                command_sequence=3,
            ),
        )

    await capacity_session.refresh(row)
    assert row.state == "permitted"
    assert row.permit_consumed_at is None
    assert (
        await capacity_session.execute(
            text("SELECT count(*) FROM capacity_executable_launch_rate_buckets")
        )
    ).scalar_one() == 0


async def test_pending_intent_query_uses_canonical_order(
    capacity_session: AsyncSession,
) -> None:
    store = CapacityExecutionStore()
    permit = await _launch_ready(store, capacity_session)
    context = await store._locked_execution_context(
        capacity_session,
        permit.binding.execution,
        executor_binding(permit.binding.pool_id),
    )
    row = await store._locked_intent(capacity_session, permit.binding.intent_id)
    statements: list[str] = []

    def capture_statement(
        _connection,  # type: ignore[no-untyped-def]
        _cursor,  # type: ignore[no-untyped-def]
        statement: str,
        _parameters,  # type: ignore[no-untyped-def]
        _context,  # type: ignore[no-untyped-def]
        _executemany: bool,
    ) -> None:
        statements.append(statement)

    engine = capacity_session.bind
    assert engine is not None
    event.listen(engine.sync_engine, "before_cursor_execute", capture_statement)
    try:
        await store._assert_pending_limits(capacity_session, context, row)
    finally:
        event.remove(engine.sync_engine, "before_cursor_execute", capture_statement)

    pending_sql = next(
        statement
        for statement in statements
        if "FROM capacity_executable_intents" in statement and "observed_state" in statement
    )
    assert (
        "ORDER BY capacity_executable_intents.allocation_epoch, "
        "capacity_executable_intents.launch_rank, capacity_executable_intents.intent_id"
    ) in pending_sql


async def test_begin_intent_close_locks_executor_context_before_intent(
    capacity_session: AsyncSession,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = CapacityExecutionStore()
    permit = await _launch_ready(store, capacity_session)
    lock_order = _record_queue_lock_order(monkeypatch, store)

    await store.begin_intent_close(
        capacity_session,
        ExecutableIntentCloseV2(binding=permit.binding, command_sequence=3),
    )

    assert lock_order[:5] == [
        "authority",
        "epoch",
        "executor-registration",
        "executor-state",
        "intent",
    ]


async def test_inventory_intent_scan_uses_canonical_order(
    capacity_session: AsyncSession,
) -> None:
    store = CapacityExecutionStore()
    permit = await _launch_ready(store, capacity_session)
    first_row = (
        await capacity_session.execute(
            select(CapacityExecutableIntent).where(
                CapacityExecutableIntent.intent_id == permit.binding.intent_id
            )
        )
    ).scalar_one()
    second_binding = permit.binding.model_copy(
        update={
            "intent_id": uuid4(),
            "tranche_id": uuid4(),
            "shape_instance_id": f"{permit.binding.shape_instance_id}-later",
        }
    )
    second_row = CapacityExecutableIntent(
        intent_id=second_binding.intent_id,
        tranche_id=second_binding.tranche_id,
        shape_instance_id=second_binding.shape_instance_id,
        execution_epoch=first_row.execution_epoch,
        execution_manifest_sha256=first_row.execution_manifest_sha256,
        configuration_epoch=first_row.configuration_epoch,
        allocation_epoch=first_row.allocation_epoch,
        executor_id=first_row.executor_id,
        executor_incarnation=first_row.executor_incarnation,
        pool_id=first_row.pool_id,
        pool_generation=first_row.pool_generation,
        subject_id=first_row.subject_id,
        subject_incarnation=first_row.subject_incarnation,
        launch_rank=first_row.launch_rank + 1,
        proposal_digest=first_row.proposal_digest,
        proposal_payload=json.loads(json.dumps(first_row.proposal_payload)),
        binding_digest=store.contract_digest(second_binding),
        binding_payload=second_binding.model_dump(mode="json", exclude_none=False),
        state="released",
        released_at=datetime.now(UTC),
    )
    await _test_only_add_intent_without_guard(capacity_session, second_row)

    locked = await store._locked_inventory_intents(
        capacity_session,
        ExecutableExecutorInventoryV2(
            execution=_inventory_execution(permit.binding),
            executor_id=permit.binding.executor_id,
            executor_incarnation=permit.binding.executor_incarnation,
            pool_id=permit.binding.pool_id,
            pool_generation=permit.binding.pool_generation,
            inventory_sequence=2,
            journal_sequence=0,
            journal_digest="0" * 64,
            records=(
                _inventory_record(second_binding, physical_identity="job-222"),
                _inventory_record(permit.binding, physical_identity="job-111"),
            ),
        ),
    )

    assert list(locked) == [permit.binding.intent_id, second_binding.intent_id]


async def test_permit_consumption_rechecks_global_pending_limit(
    capacity_session: AsyncSession,
) -> None:
    store = CapacityExecutionStore()
    permit = await _launch_ready(store, capacity_session)
    await capacity_session.execute(
        update(CapacityAuthorityState)
        .where(CapacityAuthorityState.singleton_id == 1)
        .values(global_pending_slot_ceiling=0, global_pending_job_ceiling=0)
    )

    with pytest.raises(ExecutionConflictError, match="global pending limit"):
        await store.consume_launch_permit(
            capacity_session,
            ExecutablePermitConsumptionV2(
                permit_id=permit.permit_id,
                permit_digest=store.contract_digest(permit),
                binding=permit.binding,
                command_sequence=3,
            ),
        )


async def test_pending_physical_commitment_can_exhaust_global_limit(
    capacity_session: AsyncSession,
) -> None:
    store = CapacityExecutionStore()
    permit = await _launch_ready(store, capacity_session)
    await capacity_session.execute(
        update(CapacityAuthorityState)
        .where(CapacityAuthorityState.singleton_id == 1)
        .values(global_pending_slot_ceiling=1, global_pending_job_ceiling=1)
    )
    pending = pool_observation(pool_id="oldlab", commitment_ids=("pending-global",)).model_copy(
        update={
            "commitments": (
                pool_observation(pool_id="oldlab", commitment_ids=("pending-global",))
                .commitments[0]
                .model_copy(
                    update={
                        "state": "pending",
                        "resources": resource_vector(slots=0, cpu_millicores=0, memory_bytes=0),
                    }
                ),
            )
        }
    )
    await capacity_session.execute(
        update(CapacityPoolObservation)
        .where(CapacityPoolObservation.pool_id == "oldlab")
        .values(payload=pending.model_dump(mode="json", exclude_none=False))
    )
    context = await store._locked_execution_context(
        capacity_session,
        permit.binding.execution,
        executor_binding(permit.binding.pool_id),
    )
    row = await store._locked_intent(capacity_session, permit.binding.intent_id)

    with pytest.raises(ExecutionConflictError, match="global pending limit"):
        await store._assert_pending_limits(capacity_session, context, row)


async def test_pending_physical_commitment_can_exhaust_pool_limit(
    capacity_session: AsyncSession,
) -> None:
    store = CapacityExecutionStore()
    permit = await _launch_ready(store, capacity_session)
    await capacity_session.execute(
        update(CapacityPool)
        .where(
            CapacityPool.configuration_epoch == permit.binding.execution.configuration_epoch,
            CapacityPool.pool_id == permit.binding.pool_id,
        )
        .values(max_pending_slots=1, max_pending_jobs=1)
    )
    pending = pool_observation(pool_id=permit.binding.pool_id, commitment_ids=("pending-pool",))
    pending = pending.model_copy(
        update={
            "commitments": (
                pending.commitments[0].model_copy(
                    update={
                        "state": "pending",
                        "resources": resource_vector(slots=0, cpu_millicores=0, memory_bytes=0),
                    }
                ),
            )
        }
    )
    await capacity_session.execute(
        update(CapacityPoolObservation)
        .where(CapacityPoolObservation.pool_id == permit.binding.pool_id)
        .values(payload=pending.model_dump(mode="json", exclude_none=False))
    )
    context = await store._locked_execution_context(
        capacity_session,
        permit.binding.execution,
        executor_binding(permit.binding.pool_id),
    )
    row = await store._locked_intent(capacity_session, permit.binding.intent_id)

    with pytest.raises(ExecutionConflictError, match="pool pending limit"):
        await store._assert_pending_limits(capacity_session, context, row)


async def test_pending_physical_commitment_can_exhaust_subject_limit(
    capacity_session: AsyncSession,
) -> None:
    store = CapacityExecutionStore()
    permit = await _launch_ready(store, capacity_session)
    await capacity_session.execute(
        update(CapacityPool)
        .where(
            CapacityPool.configuration_epoch == permit.binding.execution.configuration_epoch,
            CapacityPool.pool_id == permit.binding.pool_id,
        )
        .values(max_pending_slots=8, max_pending_jobs=8)
    )
    await capacity_session.execute(
        update(CapacityAuthorityState)
        .where(CapacityAuthorityState.singleton_id == 1)
        .values(global_pending_slot_ceiling=8, global_pending_job_ceiling=8)
    )
    pending = pool_observation(pool_id=permit.binding.pool_id, commitment_ids=("pending-subject",))
    pending = pending.model_copy(
        update={
            "commitments": (
                pending.commitments[0].model_copy(
                    update={
                        "state": "pending",
                        "resources": resource_vector(slots=0, cpu_millicores=0, memory_bytes=0),
                    }
                ),
            )
        }
    )
    await capacity_session.execute(
        update(CapacityPoolObservation)
        .where(CapacityPoolObservation.pool_id == permit.binding.pool_id)
        .values(payload=pending.model_dump(mode="json", exclude_none=False))
    )
    await capacity_session.execute(
        update(CapacitySubject)
        .where(
            CapacitySubject.configuration_epoch == permit.binding.execution.configuration_epoch,
            CapacitySubject.subject_id == permit.binding.subject_id,
            CapacitySubject.subject_incarnation == permit.binding.subject_incarnation,
        )
        .values(max_pending_slots=1, max_pending_jobs=1)
    )
    context = await store._locked_execution_context(
        capacity_session,
        permit.binding.execution,
        executor_binding(permit.binding.pool_id),
    )
    row = await store._locked_intent(capacity_session, permit.binding.intent_id)

    with pytest.raises(ExecutionConflictError, match="subject pending limit"):
        await store._assert_pending_limits(capacity_session, context, row)


@pytest.mark.parametrize("external_state", ["proposed", "accepted"])
async def test_external_physical_commitment_pending_states_count_against_global_limit(
    capacity_session: AsyncSession,
    external_state: str,
) -> None:
    store = CapacityExecutionStore()
    permit = await _launch_ready(store, capacity_session)
    await capacity_session.execute(
        update(CapacityAuthorityState)
        .where(CapacityAuthorityState.singleton_id == 1)
        .values(global_pending_slot_ceiling=1, global_pending_job_ceiling=1)
    )
    external = pool_observation(pool_id="oldlab", commitment_ids=(f"{external_state}-global",))
    external = external.model_copy(
        update={
            "commitments": (
                external.commitments[0].model_copy(
                    update={
                        "state": external_state,
                        "resources": resource_vector(slots=0, cpu_millicores=0, memory_bytes=0),
                    }
                ),
            )
        }
    )
    await capacity_session.execute(
        update(CapacityPoolObservation)
        .where(CapacityPoolObservation.pool_id == "oldlab")
        .values(payload=external.model_dump(mode="json", exclude_none=False))
    )
    context = await store._locked_execution_context(
        capacity_session,
        permit.binding.execution,
        executor_binding(permit.binding.pool_id),
    )
    row = await store._locked_intent(capacity_session, permit.binding.intent_id)

    with pytest.raises(ExecutionConflictError, match="global pending limit"):
        await store._assert_pending_limits(capacity_session, context, row)


async def test_exact_authenticated_pending_physical_coalesces_with_charged_intent(
    capacity_session: AsyncSession,
) -> None:
    store = CapacityExecutionStore()
    permit, row = await _quarantined_consumed_permit(
        store,
        capacity_session,
        policy=execution_policy(ceiling=2),
    )
    await capacity_session.execute(
        update(CapacityAuthorityState)
        .where(CapacityAuthorityState.singleton_id == 1)
        .values(global_pending_slot_ceiling=8, global_pending_job_ceiling=2)
    )
    physical = _authenticated_physical_commitment(permit).model_copy(update={"state": "pending"})
    observation = pool_observation(pool_id=permit.binding.pool_id).model_copy(
        update={"commitments": (physical,)}
    )
    await capacity_session.execute(
        update(CapacityPoolObservation)
        .where(CapacityPoolObservation.pool_id == permit.binding.pool_id)
        .values(payload=observation.model_dump(mode="json", exclude_none=False))
    )
    context = await store._locked_execution_context(
        capacity_session,
        permit.binding.execution,
        executor_binding(permit.binding.pool_id),
    )
    successor = _successor_binding(permit.binding)
    successor_row = _successor_proposed_row(store, row, successor)
    await _test_only_add_intent_without_guard(capacity_session, successor_row)

    await store._assert_pending_limits(capacity_session, context, successor_row)


@pytest.mark.parametrize("drift", ("resources", "pool-generation", "nodes"))
async def test_authenticated_pending_physical_drift_counts_separately_from_charged_intent(
    capacity_session: AsyncSession,
    drift: str,
) -> None:
    store = CapacityExecutionStore()
    permit, row = await _quarantined_consumed_permit(
        store,
        capacity_session,
        policy=execution_policy(ceiling=2),
    )
    await capacity_session.execute(
        update(CapacityAuthorityState)
        .where(CapacityAuthorityState.singleton_id == 1)
        .values(global_pending_slot_ceiling=8, global_pending_job_ceiling=2)
    )
    physical = _authenticated_physical_commitment(permit).model_copy(update={"state": "pending"})
    if drift == "resources":
        physical = physical.model_copy(
            update={"resources": _scaled_resources(permit.binding.resources, 2)}
        )
    elif drift == "pool-generation":
        physical = physical.model_copy(
            update={"pool_generation": permit.binding.pool_generation + 1}
        )
    else:
        physical = physical.model_copy(update={"node_ids": ("gb10-drift",)})
    observation = pool_observation(pool_id=permit.binding.pool_id).model_copy(
        update={"commitments": (physical,)}
    )
    await capacity_session.execute(
        update(CapacityPoolObservation)
        .where(CapacityPoolObservation.pool_id == permit.binding.pool_id)
        .values(payload=observation.model_dump(mode="json", exclude_none=False))
    )
    context = await store._locked_execution_context(
        capacity_session,
        permit.binding.execution,
        executor_binding(permit.binding.pool_id),
    )
    successor = _successor_binding(permit.binding)
    successor_row = _successor_proposed_row(store, row, successor)
    await _test_only_add_intent_without_guard(capacity_session, successor_row)

    with pytest.raises(ExecutionConflictError, match="global pending limit"):
        await store._assert_pending_limits(capacity_session, context, successor_row)


async def test_closing_intent_remains_charged_until_release(
    capacity_session: AsyncSession,
) -> None:
    store = CapacityExecutionStore()
    permit = await _launch_ready(store, capacity_session)
    close = ExecutableIntentCloseV2(
        binding=permit.binding,
        command_sequence=3,
    )
    await store.begin_intent_close(capacity_session, close)
    context = await store._locked_execution_context(
        capacity_session,
        permit.binding.execution,
        executor_binding(permit.binding.pool_id),
    )

    with pytest.raises(ExecutionConflictError, match="capacity ceiling"):
        await store._assert_increase_eligible(
            capacity_session,
            context,
            proposed=permit.binding,
        )


@pytest.mark.parametrize(
    ("scope", "expected"),
    (
        ("pool", "pool pending limit"),
        ("subject", "subject pending limit"),
        ("account", "account pending limit"),
        ("tier", "tier pending limit"),
    ),
)
async def test_quarantined_intent_counts_against_scoped_pending_limits(
    capacity_session: AsyncSession,
    scope: str,
    expected: str,
) -> None:
    store = CapacityExecutionStore()
    permit, row = await _quarantined_consumed_permit(store, capacity_session)
    await capacity_session.execute(
        update(CapacityAuthorityState)
        .where(CapacityAuthorityState.singleton_id == 1)
        .values(global_pending_slot_ceiling=8, global_pending_job_ceiling=8)
    )
    if scope == "pool":
        await capacity_session.execute(
            update(CapacityPool)
            .where(
                CapacityPool.configuration_epoch == permit.binding.execution.configuration_epoch,
                CapacityPool.pool_id == permit.binding.pool_id,
            )
            .values(max_pending_slots=1, max_pending_jobs=1)
        )
    elif scope == "subject":
        await capacity_session.execute(
            update(CapacitySubject)
            .where(
                CapacitySubject.configuration_epoch == permit.binding.execution.configuration_epoch,
                CapacitySubject.subject_id == permit.binding.subject_id,
                CapacitySubject.subject_incarnation == permit.binding.subject_incarnation,
            )
            .values(max_pending_slots=1, max_pending_jobs=1)
        )
    elif scope == "account":
        await capacity_session.execute(
            update(CapacityAccountPolicy)
            .where(
                CapacityAccountPolicy.configuration_epoch
                == permit.binding.execution.configuration_epoch,
                CapacityAccountPolicy.account_id == permit.binding.account_id,
            )
            .values(max_pending_slots=1, max_pending_jobs=1)
        )
    else:
        await capacity_session.execute(
            update(CapacityTier)
            .where(
                CapacityTier.configuration_epoch == permit.binding.execution.configuration_epoch,
                CapacityTier.tier_id == permit.binding.tier_id,
            )
            .values(max_pending_slots=1, max_pending_jobs=1)
        )
    context = await store._locked_execution_context(
        capacity_session,
        permit.binding.execution,
        executor_binding(permit.binding.pool_id),
    )
    successor = _successor_binding(permit.binding)
    successor_row = _successor_proposed_row(store, row, successor)
    await _test_only_add_intent_without_guard(capacity_session, successor_row)

    with pytest.raises(ExecutionConflictError, match=expected):
        await store._assert_pending_limits(capacity_session, context, successor_row)


async def test_quarantined_intent_counts_against_pool_resource_headroom(
    capacity_session: AsyncSession,
) -> None:
    store = CapacityExecutionStore()
    permit, _row = await _quarantined_consumed_permit(store, capacity_session)
    pool = (
        await capacity_session.execute(
            select(CapacityPool).where(
                CapacityPool.configuration_epoch == permit.binding.execution.configuration_epoch,
                CapacityPool.pool_id == permit.binding.pool_id,
            )
        )
    ).scalar_one()
    topology = json.loads(json.dumps(pool.topology))
    topology["resource_domains"][0]["nodes"][0]["allocatable"] = (
        permit.binding.resources.model_dump(mode="json", exclude_none=False)
    )
    pool.topology = topology
    context = await store._locked_execution_context(
        capacity_session,
        permit.binding.execution,
        executor_binding(permit.binding.pool_id),
    )

    with pytest.raises(ExecutionConflictError, match="pool headroom"):
        await store._assert_increase_eligible(
            capacity_session,
            context,
            proposed=_successor_binding(permit.binding),
        )


async def test_authenticated_physical_observation_deduplicates_pool_resource_headroom(
    capacity_session: AsyncSession,
) -> None:
    store = CapacityExecutionStore()
    permit = await _launch_ready(store, capacity_session, policy=execution_policy(ceiling=2))
    await store.consume_launch_permit(
        capacity_session,
        ExecutablePermitConsumptionV2(
            permit_id=permit.permit_id,
            permit_digest=store.contract_digest(permit),
            binding=permit.binding,
            command_sequence=3,
        ),
    )
    pool = (
        await capacity_session.execute(
            select(CapacityPool).where(
                CapacityPool.configuration_epoch == permit.binding.execution.configuration_epoch,
                CapacityPool.pool_id == permit.binding.pool_id,
            )
        )
    ).scalar_one()
    topology = json.loads(json.dumps(pool.topology))
    topology["resource_domains"][0]["nodes"] = [
        {
            **topology["resource_domains"][0]["nodes"][0],
            "node_id": permit.binding.node_ids[0],
            "allocatable": _scaled_resources(permit.binding.resources, 2).model_dump(
                mode="json",
                exclude_none=False,
            ),
        }
    ]
    pool.topology = topology
    observation = pool_observation(pool_id=permit.binding.pool_id).model_copy(
        update={"commitments": (_authenticated_physical_commitment(permit),)}
    )
    await capacity_session.execute(
        update(CapacityPoolObservation)
        .where(CapacityPoolObservation.pool_id == permit.binding.pool_id)
        .values(payload=observation.model_dump(mode="json", exclude_none=False))
    )
    context = await store._locked_execution_context(
        capacity_session,
        permit.binding.execution,
        executor_binding(permit.binding.pool_id),
    )

    await store._assert_increase_eligible(
        capacity_session,
        context,
        proposed=_successor_binding(permit.binding),
    )


async def test_quarantined_intent_combines_with_external_resource_commitments(
    capacity_session: AsyncSession,
) -> None:
    store = CapacityExecutionStore()
    permit, _row = await _quarantined_consumed_permit(store, capacity_session)
    pool = (
        await capacity_session.execute(
            select(CapacityPool).where(
                CapacityPool.configuration_epoch == permit.binding.execution.configuration_epoch,
                CapacityPool.pool_id == permit.binding.pool_id,
            )
        )
    ).scalar_one()
    topology = json.loads(json.dumps(pool.topology))
    template = topology["resource_domains"][0]["nodes"][0]
    topology["resource_domains"][0]["nodes"] = [
        {
            **template,
            "node_id": permit.binding.node_ids[0],
            "allocatable": permit.binding.resources.model_dump(mode="json", exclude_none=False),
        },
        {
            **template,
            "node_id": "gb10-node-b",
            "allocatable": permit.binding.resources.model_dump(mode="json", exclude_none=False),
        },
    ]
    pool.topology = topology
    external = pool_observation(pool_id=permit.binding.pool_id).model_copy(
        update={
            "commitments": (
                pool_observation(
                    pool_id=permit.binding.pool_id,
                    commitment_ids=("external-resource",),
                )
                .commitments[0]
                .model_copy(
                    update={
                        "resources": permit.binding.resources,
                        "node_ids": ("gb10-node-b",),
                    }
                ),
            )
        }
    )
    await capacity_session.execute(
        update(CapacityPoolObservation)
        .where(CapacityPoolObservation.pool_id == permit.binding.pool_id)
        .values(payload=external.model_dump(mode="json", exclude_none=False))
    )
    context = await store._locked_execution_context(
        capacity_session,
        permit.binding.execution,
        executor_binding(permit.binding.pool_id),
    )

    with pytest.raises(ExecutionConflictError, match="pool headroom"):
        await store._assert_increase_eligible(
            capacity_session,
            context,
            proposed=_successor_binding(permit.binding),
        )


async def test_authenticated_physical_observation_deduplicates_selected_node_topology(
    capacity_session: AsyncSession,
) -> None:
    store = CapacityExecutionStore()
    permit = await _launch_ready(store, capacity_session, policy=execution_policy(ceiling=2))
    await store.consume_launch_permit(
        capacity_session,
        ExecutablePermitConsumptionV2(
            permit_id=permit.permit_id,
            permit_digest=store.contract_digest(permit),
            binding=permit.binding,
            command_sequence=3,
        ),
    )
    pool = (
        await capacity_session.execute(
            select(CapacityPool).where(
                CapacityPool.configuration_epoch == permit.binding.execution.configuration_epoch,
                CapacityPool.pool_id == permit.binding.pool_id,
            )
        )
    ).scalar_one()
    topology = json.loads(json.dumps(pool.topology))
    template = topology["resource_domains"][0]["nodes"][0]
    topology["resource_domains"][0]["nodes"] = [
        {
            **template,
            "node_id": permit.binding.node_ids[0],
            "allocatable": _scaled_resources(permit.binding.resources, 2).model_dump(
                mode="json",
                exclude_none=False,
            ),
        },
        {
            **template,
            "node_id": "gb10-spare",
            "allocatable": permit.binding.resources.model_dump(
                mode="json",
                exclude_none=False,
            ),
        },
    ]
    pool.topology = topology
    observation = pool_observation(pool_id=permit.binding.pool_id).model_copy(
        update={"commitments": (_authenticated_physical_commitment(permit),)}
    )
    await capacity_session.execute(
        update(CapacityPoolObservation)
        .where(CapacityPoolObservation.pool_id == permit.binding.pool_id)
        .values(payload=observation.model_dump(mode="json", exclude_none=False))
    )
    context = await store._locked_execution_context(
        capacity_session,
        permit.binding.execution,
        executor_binding(permit.binding.pool_id),
    )

    await store._assert_increase_eligible(
        capacity_session,
        context,
        proposed=_successor_binding(permit.binding),
    )


async def test_quarantined_intent_counts_against_selected_node_headroom(
    capacity_session: AsyncSession,
) -> None:
    store = CapacityExecutionStore()
    permit, _row = await _quarantined_consumed_permit(store, capacity_session)
    pool = (
        await capacity_session.execute(
            select(CapacityPool).where(
                CapacityPool.configuration_epoch == permit.binding.execution.configuration_epoch,
                CapacityPool.pool_id == permit.binding.pool_id,
            )
        )
    ).scalar_one()
    topology = json.loads(json.dumps(pool.topology))
    template = topology["resource_domains"][0]["nodes"][0]
    topology["resource_domains"][0]["nodes"] = [
        {
            **template,
            "node_id": permit.binding.node_ids[0],
            "allocatable": permit.binding.resources.model_dump(mode="json", exclude_none=False),
        },
        {
            **template,
            "node_id": "gb10-spare",
            "allocatable": permit.binding.resources.model_dump(mode="json", exclude_none=False),
        },
    ]
    pool.topology = topology
    context = await store._locked_execution_context(
        capacity_session,
        permit.binding.execution,
        executor_binding(permit.binding.pool_id),
    )

    with pytest.raises(ExecutionConflictError, match="node headroom"):
        await store._assert_increase_eligible(
            capacity_session,
            context,
            proposed=_successor_binding(permit.binding),
        )


async def test_permit_consumption_rechecks_selected_node_headroom(
    capacity_session: AsyncSession,
) -> None:
    store = CapacityExecutionStore()
    permit = await _launch_ready(store, capacity_session)
    pool = (
        await capacity_session.execute(
            select(CapacityPool).where(
                CapacityPool.configuration_epoch == permit.binding.execution.configuration_epoch,
                CapacityPool.pool_id == permit.binding.pool_id,
            )
        )
    ).scalar_one()
    topology = json.loads(json.dumps(pool.topology))
    spare = json.loads(json.dumps(topology["resource_domains"][0]["nodes"][0]))
    spare["node_id"] = "gb10-spare"
    topology["resource_domains"][0]["nodes"].append(spare)
    pool.topology = topology
    occupied = pool_observation(pool_id="gb10").model_copy(
        update={
            "commitments": (
                pool_observation(pool_id="gb10", commitment_ids=("occupied",))
                .commitments[0]
                .model_copy(
                    update={
                        "resources": resource_vector(
                            slots=8,
                            cpu_millicores=8_000,
                            memory_bytes=17_179_869_184,
                        ),
                        "node_ids": permit.binding.node_ids,
                    }
                ),
            )
        }
    )
    await capacity_session.execute(
        update(CapacityPoolObservation)
        .where(CapacityPoolObservation.pool_id == permit.binding.pool_id)
        .values(payload=occupied.model_dump(mode="json", exclude_none=False))
    )

    with pytest.raises(ExecutionConflictError, match="node headroom"):
        await store.consume_launch_permit(
            capacity_session,
            ExecutablePermitConsumptionV2(
                permit_id=permit.permit_id,
                permit_digest=store.contract_digest(permit),
                binding=permit.binding,
                command_sequence=3,
            ),
        )


async def _heterogeneous_multi_node_permit(
    store: CapacityExecutionStore,
    session: AsyncSession,
    *,
    reverse_shape_parts: bool,
    commitment_headroom: bool = False,
    policy=None,
):  # type: ignore[no-untyped-def]
    permit = await _launch_ready(store, session, policy=policy)
    small = resource_vector(slots=0, cpu_millicores=2, memory_bytes=2)
    large = resource_vector(slots=1, cpu_millicores=8, memory_bytes=8)
    worker_shape = shape(
        "heterogeneous-two-node",
        concurrency_slots=1,
        total=resource_vector(slots=1, cpu_millicores=10, memory_bytes=10),
        per_node=(large, small) if reverse_shape_parts else (small, large),
        compatible_domain_ids=("gb10-arm",),
    )
    pool = (
        await session.execute(
            select(CapacityPool).where(
                CapacityPool.configuration_epoch == permit.binding.execution.configuration_epoch,
                CapacityPool.pool_id == permit.binding.pool_id,
            )
        )
    ).scalar_one()
    topology = json.loads(json.dumps(pool.topology))
    template = topology["resource_domains"][0]["nodes"][0]
    topology["resource_domains"][0]["nodes"] = [
        {
            **template,
            "node_id": "gb10-node-a",
            "allocatable": resource_vector(
                slots=0,
                cpu_millicores=3 if commitment_headroom else 2,
                memory_bytes=3 if commitment_headroom else 2,
            ).model_dump(mode="json", exclude_none=False),
        },
        {
            **template,
            "node_id": "gb10-node-z",
            "allocatable": resource_vector(
                slots=1,
                cpu_millicores=9 if commitment_headroom else 8,
                memory_bytes=9 if commitment_headroom else 8,
            ).model_dump(mode="json", exclude_none=False),
        },
    ]
    pool.topology = topology
    profile = (
        await session.execute(
            select(CapacityWorkerProfile).where(
                CapacityWorkerProfile.pool_id == permit.binding.pool_id,
                CapacityWorkerProfile.profile_generation == permit.binding.profile_generation,
            )
        )
    ).scalar_one()
    profile.shape_catalog = [worker_shape.model_dump(mode="json", exclude_none=False)]
    binding_payload = permit.binding.model_dump(mode="python")
    binding_payload.update(
        {
            "shape_id": worker_shape.shape_id,
            "concurrency_slots": worker_shape.concurrency_slots,
            "resources": worker_shape.total_resources,
            "node_ids": ("gb10-node-z", "gb10-node-a"),
        }
    )
    binding = type(permit.binding).model_validate(binding_payload)
    changed = permit.model_copy(update={"binding": binding})
    intent = (
        await session.execute(
            select(CapacityExecutableIntent).where(
                CapacityExecutableIntent.intent_id == binding.intent_id
            )
        )
    ).scalar_one()
    await _test_only_update_without_guard(
        session,
        table_name="capacity_executable_intents",
        trigger_name="capacity_executable_intent_mutation_guard",
        statement=(
            update(CapacityExecutableIntent)
            .where(CapacityExecutableIntent.intent_id == intent.intent_id)
            .values(
                binding_digest=store.contract_digest(binding),
                binding_payload=binding.model_dump(mode="json", exclude_none=False),
                permit_digest=store.contract_digest(changed),
                permit_payload=changed.model_dump(mode="json", exclude_none=False),
            )
        ),
    )
    return changed


async def test_selected_node_feasibility_does_not_depend_on_canonical_node_order(
    capacity_session: AsyncSession,
) -> None:
    store = CapacityExecutionStore()
    permit = await _heterogeneous_multi_node_permit(
        store,
        capacity_session,
        reverse_shape_parts=True,
    )

    consumed = await store.consume_launch_permit(
        capacity_session,
        ExecutablePermitConsumptionV2(
            permit_id=permit.permit_id,
            permit_digest=store.contract_digest(permit),
            binding=permit.binding,
            command_sequence=3,
        ),
    )

    assert consumed.intent_id == permit.binding.intent_id


async def test_overlapping_multi_node_commitment_fails_closed(
    capacity_session: AsyncSession,
) -> None:
    store = CapacityExecutionStore()
    permit = await _heterogeneous_multi_node_permit(
        store,
        capacity_session,
        reverse_shape_parts=False,
        commitment_headroom=True,
    )
    ambiguous = (
        pool_observation(
            pool_id=permit.binding.pool_id,
            commitment_ids=("ambiguous-multi-node",),
        )
        .commitments[0]
        .model_copy(
            update={
                "resources": resource_vector(slots=0, cpu_millicores=1, memory_bytes=1),
                "node_ids": permit.binding.node_ids,
            }
        )
    )
    occupied = pool_observation(pool_id=permit.binding.pool_id).model_copy(
        update={"commitments": (ambiguous,)}
    )
    await capacity_session.execute(
        update(CapacityPoolObservation)
        .where(CapacityPoolObservation.pool_id == permit.binding.pool_id)
        .values(payload=occupied.model_dump(mode="json", exclude_none=False))
    )

    with pytest.raises(ExecutionConflictError, match="node headroom"):
        await store.consume_launch_permit(
            capacity_session,
            ExecutablePermitConsumptionV2(
                permit_id=permit.permit_id,
                permit_digest=store.contract_digest(permit),
                binding=permit.binding,
                command_sequence=3,
            ),
        )


async def test_quarantined_intent_counts_against_multi_node_topology(
    capacity_session: AsyncSession,
) -> None:
    store = CapacityExecutionStore()
    permit = await _heterogeneous_multi_node_permit(
        store,
        capacity_session,
        reverse_shape_parts=False,
        policy=execution_policy(ceiling=2),
    )
    await store.consume_launch_permit(
        capacity_session,
        ExecutablePermitConsumptionV2(
            permit_id=permit.permit_id,
            permit_digest=store.contract_digest(permit),
            binding=permit.binding,
            command_sequence=3,
        ),
    )
    inventory = await _next_inventory(
        capacity_session,
        _inventory_execution(permit.binding),
        permit.binding,
    )
    await store.ingest_executor_inventory(capacity_session, inventory)
    await store.recover_unsubmitted_permit(
        capacity_session,
        _submission_recovery(store, permit, inventory),
    )
    pool = (
        await capacity_session.execute(
            select(CapacityPool).where(
                CapacityPool.configuration_epoch == permit.binding.execution.configuration_epoch,
                CapacityPool.pool_id == permit.binding.pool_id,
            )
        )
    ).scalar_one()
    topology = json.loads(json.dumps(pool.topology))
    template = topology["resource_domains"][0]["nodes"][0]
    topology["resource_domains"][0]["nodes"].append(
        {
            **template,
            "node_id": "gb10-spare",
            "allocatable": permit.binding.resources.model_dump(mode="json", exclude_none=False),
        }
    )
    pool.topology = topology
    context = await store._locked_execution_context(
        capacity_session,
        permit.binding.execution,
        executor_binding(permit.binding.pool_id),
    )

    with pytest.raises(ExecutionConflictError, match="node headroom"):
        await store._assert_increase_eligible(
            capacity_session,
            context,
            proposed=_successor_binding(permit.binding),
        )


async def test_increase_freeze_begins_central_close_for_accepted_work(
    capacity_session: AsyncSession,
) -> None:
    store = CapacityExecutionStore()
    active, _allocation_epoch = await _active_plan(capacity_session)
    binding = executor_binding("gb10")
    await _heartbeat(store, capacity_session, active, pool_id="gb10")
    proposal = await store.next_pool_work(capacity_session, binding)
    assert isinstance(proposal, ExecutableReservationProposalV2)
    await store.accept_reservation(
        capacity_session,
        ExecutableReservationAcceptanceV2(
            execution=proposal.execution,
            tranche_id=proposal.tranche_id,
            proposal_digest=store.contract_digest(proposal),
            pool_id=binding.pool_id,
            pool_generation=binding.pool_generation,
            executor_id=binding.executor_id,
            executor_incarnation=binding.executor_incarnation,
            command_sequence=1,
        ),
    )
    await capacity_session.execute(
        update(CapacityAuthorityState)
        .where(CapacityAuthorityState.singleton_id == 1)
        .values(increase_freeze=True, increase_freeze_reason="synthetic failure")
    )

    work = await store.next_pool_work(capacity_session, binding)

    assert isinstance(work, ExecutableIntentCloseV2)
    result = await store.begin_intent_close(capacity_session, work)
    assert result.intent_id == work.binding.intent_id
    row = (
        await capacity_session.execute(
            select(CapacityExecutableIntent).where(
                CapacityExecutableIntent.executor_incarnation == binding.executor_incarnation,
                CapacityExecutableIntent.pool_id == binding.pool_id,
            )
        )
    ).scalar_one()
    assert row.state == "closing"


async def test_global_authority_rate_ceiling_limits_second_consumption(
    capacity_session: AsyncSession,
) -> None:
    store = CapacityExecutionStore()
    policy = execution_policy().model_copy(update={"executable_new_capacity_rate_per_minute": 2})
    permit = await _launch_ready(store, capacity_session, policy=policy)
    await capacity_session.execute(
        update(CapacityAuthorityState)
        .where(CapacityAuthorityState.singleton_id == 1)
        .values(global_submission_rate_ceiling=1)
    )
    await capacity_session.execute(
        update(CapacityPool)
        .where(
            CapacityPool.configuration_epoch == permit.binding.execution.configuration_epoch,
            CapacityPool.pool_id == permit.binding.pool_id,
        )
        .values(submission_rate_per_minute=2)
    )
    await capacity_session.execute(
        update(CapacitySubject)
        .where(
            CapacitySubject.configuration_epoch == permit.binding.execution.configuration_epoch,
            CapacitySubject.subject_id == permit.binding.subject_id,
            CapacitySubject.subject_incarnation == permit.binding.subject_incarnation,
        )
        .values(submission_rate_per_minute=2)
    )
    await capacity_session.execute(
        text(
            "UPDATE capacity_account_policies SET submission_rate_per_minute = 2 "
            "WHERE configuration_epoch = :configuration_epoch AND account_id = :account_id"
        ),
        {
            "configuration_epoch": permit.binding.execution.configuration_epoch,
            "account_id": permit.binding.account_id,
        },
    )
    await store.consume_launch_permit(
        capacity_session,
        ExecutablePermitConsumptionV2(
            permit_id=permit.permit_id,
            permit_digest=store.contract_digest(permit),
            binding=permit.binding,
            command_sequence=3,
        ),
    )
    first_row = (
        await capacity_session.execute(
            select(CapacityExecutableIntent).where(
                CapacityExecutableIntent.intent_id == permit.binding.intent_id
            )
        )
    ).scalar_one()
    await _test_only_release_intent(capacity_session, first_row.intent_id)
    await capacity_session.refresh(first_row)
    second_binding = permit.binding.model_copy(
        update={
            "tranche_id": uuid4(),
            "intent_id": uuid4(),
            "shape_instance_id": f"{permit.binding.shape_instance_id}-second",
        }
    )
    second_row = CapacityExecutableIntent(
        intent_id=second_binding.intent_id,
        tranche_id=second_binding.tranche_id,
        shape_instance_id=second_binding.shape_instance_id,
        execution_epoch=first_row.execution_epoch,
        execution_manifest_sha256=first_row.execution_manifest_sha256,
        configuration_epoch=first_row.configuration_epoch,
        allocation_epoch=first_row.allocation_epoch,
        executor_id=first_row.executor_id,
        executor_incarnation=first_row.executor_incarnation,
        pool_id=first_row.pool_id,
        pool_generation=first_row.pool_generation,
        subject_id=first_row.subject_id,
        subject_incarnation=first_row.subject_incarnation,
        launch_rank=2,
        proposal_digest=first_row.proposal_digest,
        proposal_payload=json.loads(json.dumps(first_row.proposal_payload)),
        binding_digest=store.contract_digest(second_binding),
        binding_payload=second_binding.model_dump(mode="json", exclude_none=False),
        state="launch-ready",
        accepted_at=first_row.accepted_at,
        bootstrap_registration_epoch=1,
        bootstrap_evidence_sha256="8" * 64,
        launch_ready_at=datetime.now(UTC),
    )
    await _test_only_add_intent_without_guard(capacity_session, second_row)
    second_permit = _test_only_synthetic_permit(
        second_row,
        second_binding,
        datetime.now(UTC),
    )
    second_row.permit_id = second_permit.permit_id
    second_row.permit_epoch = second_permit.permit_epoch
    second_row.permit_digest = store.contract_digest(second_permit)
    second_row.permit_payload = second_permit.model_dump(mode="json", exclude_none=False)
    second_row.permit_expires_at = second_permit.expires_at
    second_row.state = "permitted"

    with pytest.raises(ExecutionConflictError, match="launch rate is exhausted"):
        await store.consume_launch_permit(
            capacity_session,
            ExecutablePermitConsumptionV2(
                permit_id=second_permit.permit_id,
                permit_digest=store.contract_digest(second_permit),
                binding=second_binding,
                command_sequence=4,
            ),
        )


async def test_drain_only_writer_transition_allows_retained_intent_close(
    capacity_session: AsyncSession,
) -> None:
    store = CapacityExecutionStore()
    active, _allocation_epoch = await _active_plan(capacity_session)
    binding = executor_binding("gb10")
    await _heartbeat(store, capacity_session, active, pool_id="gb10")
    proposal = await store.next_pool_work(capacity_session, binding)
    assert isinstance(proposal, ExecutableReservationProposalV2)
    await store.accept_reservation(
        capacity_session,
        ExecutableReservationAcceptanceV2(
            execution=proposal.execution,
            tranche_id=proposal.tranche_id,
            proposal_digest=store.contract_digest(proposal),
            pool_id=binding.pool_id,
            pool_generation=binding.pool_generation,
            executor_id=binding.executor_id,
            executor_incarnation=binding.executor_incarnation,
            command_sequence=1,
        ),
    )
    await CapacityManagementStore().register_writer(
        capacity_session,
        active.authority_incarnation,
        expected_epoch=active.writer_epoch,
    )

    close = await store.next_pool_work(capacity_session, binding)

    assert isinstance(close, ExecutableIntentCloseV2)
    assert close.bootstrap_registration_epoch is None
    assert close.bootstrap_evidence_sha256 is None
    bootstrap = ExecutableBootstrapProposalV2(
        binding=close.binding,
        command_sequence=close.command_sequence,
        proposal_epoch=1,
        bootstrap_sha256="7" * 64,
        expires_at=datetime.now(UTC) + timedelta(minutes=1),
    )
    await store.propose_bootstrap(capacity_session, bootstrap)
    await store.acknowledge_bootstrap(
        capacity_session,
        ExecutableBootstrapAcknowledgementV2(
            binding=close.binding,
            proposal_epoch=bootstrap.proposal_epoch,
            proposal_digest=store.contract_digest(bootstrap),
            reporter_incarnation=demand_snapshot().reporter_incarnation,
            bootstrap_registration_epoch=1,
            bootstrap_evidence_sha256="8" * 64,
            protected_admission_sha256="3" * 64,
        ),
        actor="development",
        idempotency_key=UUID(int=996),
    )
    row = (
        await capacity_session.execute(
            select(CapacityExecutableIntent).where(
                CapacityExecutableIntent.intent_id == close.binding.intent_id
            )
        )
    ).scalar_one()
    assert row.state == "bootstrap-acknowledged"
    assert row.launch_ready_at is None
    assert row.permit_id is None
    assert (
        await capacity_session.execute(
            select(func.count(CapacityExecutableAdmissionProposal.id))
        )
    ).scalar_one() == 0
    assert (
        await capacity_session.execute(
            select(func.count(CapacityExecutableAdmissionAcknowledgement.id))
        )
    ).scalar_one() == 0

    successor = await store.next_pool_work(capacity_session, binding)

    assert isinstance(successor, ExecutableIntentCloseV2)
    assert successor.bootstrap_registration_epoch == 1
    assert successor.bootstrap_evidence_sha256 == "8" * 64
    result = await store.begin_intent_close(capacity_session, successor)
    assert result.intent_id == close.binding.intent_id


async def test_drain_only_transition_emits_close_for_superseded_observed_worker(
    capacity_session: AsyncSession,
) -> None:
    store = CapacityExecutionStore(
        ownership_keyring=OwnershipKeyring({"gb10-key": EXECUTOR_KEYS["gb10"].public_key()})
    )
    permit = await _launch_ready(store, capacity_session)
    await store.consume_launch_permit(
        capacity_session,
        ExecutablePermitConsumptionV2(
            permit_id=permit.permit_id,
            permit_digest=store.contract_digest(permit),
            binding=permit.binding,
            command_sequence=3,
        ),
    )
    binding = permit.binding
    metadata = ExecutableOwnershipMetadataV2(
        binding=binding,
        controller_authority_sha256="c" * 64,
        trusted_launcher_sha256="e" * 64,
        slurm_cluster="gb10-controller",
        submitter_identity="loom",
        association="loom",
        submitted_at=datetime.now(UTC),
    )
    proof = SignedExecutableOwnershipProofV2(
        metadata=metadata,
        signing_key_id="gb10-key",
        signature_base64=base64.b64encode(
            EXECUTOR_KEYS["gb10"].sign(canonical_executable_bytes(metadata))
        ).decode("ascii"),
    )
    active_payload = binding.execution.model_dump(mode="python")
    active_payload.pop("allocation_epoch")
    active_payload.pop("executable")
    await store.ingest_executor_inventory(
        capacity_session,
        await _next_inventory(
            capacity_session,
            ExecutionContextV2.model_validate(active_payload),
            binding,
            records=(
                ExecutableInventoryRecordV2(
                    physical_identity="job-123",
                    physical_kind="slurm-job",
                    authority_scope="dedicated-loom-association",
                    state="active",
                    resources=binding.resources,
                    node_ids=binding.node_ids,
                    controller_evidence_sha256="9" * 64,
                    ownership_proof=proof,
                ),
            ),
        ),
    )
    await _clone_sealed_epoch(
        capacity_session,
        allocation_epoch=binding.execution.allocation_epoch,
        input_valid_until=datetime.now(UTC) + timedelta(minutes=5),
    )
    await CapacityManagementStore().register_writer(
        capacity_session,
        binding.execution.authority_incarnation,
        expected_epoch=binding.execution.writer_epoch,
    )

    close = await store.next_pool_work(capacity_session, executor_binding("gb10"))

    assert isinstance(close, ExecutableIntentCloseV2)
    result = await store.begin_intent_close(capacity_session, close)
    assert result.intent_id == binding.intent_id


async def test_duplicate_inventory_claims_for_one_intent_fence_the_executor(
    capacity_session: AsyncSession,
) -> None:
    store = CapacityExecutionStore(
        ownership_keyring=OwnershipKeyring({"gb10-key": EXECUTOR_KEYS["gb10"].public_key()})
    )
    permit = await _launch_ready(store, capacity_session)
    await store.consume_launch_permit(
        capacity_session,
        ExecutablePermitConsumptionV2(
            permit_id=permit.permit_id,
            permit_digest=store.contract_digest(permit),
            binding=permit.binding,
            command_sequence=3,
        ),
    )

    with pytest.raises(ExecutionConflictError, match="duplicate executable inventory claim"):
        await store.ingest_executor_inventory(
            capacity_session,
            await _next_inventory(
                capacity_session,
                _inventory_execution(permit.binding),
                permit.binding,
                records=(
                    _inventory_record(permit.binding, physical_identity="job-123"),
                    _inventory_record(permit.binding, physical_identity="job-124"),
                ),
            ),
        )

    state = (
        await capacity_session.execute(
            select(CapacityExecutableExecutorState).where(
                CapacityExecutableExecutorState.executor_incarnation
                == permit.binding.executor_incarnation
            )
        )
    ).scalar_one()
    assert state.state == "equivocal"


async def test_pre_consumption_valid_inventory_quarantines_the_intent(
    capacity_session: AsyncSession,
) -> None:
    store = CapacityExecutionStore(
        ownership_keyring=OwnershipKeyring({"gb10-key": EXECUTOR_KEYS["gb10"].public_key()})
    )
    permit = await _launch_ready(store, capacity_session)

    await store.ingest_executor_inventory(
        capacity_session,
        await _next_inventory(
            capacity_session,
            _inventory_execution(permit.binding),
            permit.binding,
            records=(_inventory_record(permit.binding, physical_identity="job-123"),),
        ),
    )

    row = (
        await capacity_session.execute(
            select(CapacityExecutableIntent).where(
                CapacityExecutableIntent.intent_id == permit.binding.intent_id
            )
        )
    ).scalar_one()
    assert row.state == "quarantined"


async def test_bootstrap_acknowledged_inventory_quarantines_without_launch_permit(
    capacity_session: AsyncSession,
) -> None:
    """Unexpected physical work before admission must fail closed durably."""

    store = CapacityExecutionStore(
        ownership_keyring=OwnershipKeyring({"gb10-key": EXECUTOR_KEYS["gb10"].public_key()})
    )
    _executor, binding, bootstrap = await _proposed_bootstrap(store, capacity_session)
    await store.acknowledge_bootstrap(
        capacity_session,
        ExecutableBootstrapAcknowledgementV2(
            binding=binding,
            proposal_epoch=bootstrap.proposal_epoch,
            proposal_digest=store.contract_digest(bootstrap),
            reporter_incarnation=demand_snapshot().reporter_incarnation,
            bootstrap_registration_epoch=1,
            bootstrap_evidence_sha256="8" * 64,
            protected_admission_sha256="3" * 64,
        ),
        actor="development",
        idempotency_key=UUID(int=995),
    )
    assert (
        await capacity_session.execute(
            select(func.count(CapacityExecutableAdmissionAcknowledgement.id))
        )
    ).scalar_one() == 0

    inventory = await _next_inventory(
        capacity_session,
        _inventory_execution(binding),
        binding,
        records=(_inventory_record(binding, physical_identity="job-before-permit"),),
    )
    ingested = await store.ingest_executor_inventory(capacity_session, inventory)

    row = (
        await capacity_session.execute(
            select(CapacityExecutableIntent).where(
                CapacityExecutableIntent.intent_id == binding.intent_id
            )
        )
    ).scalar_one()
    assert ingested.inventory_sequence == 2
    assert row.state == "quarantined"
    assert row.bootstrap_registration_epoch == 1
    assert row.bootstrap_evidence_sha256 == "8" * 64
    assert row.launch_ready_at is None
    assert row.permit_id is None
    assert row.inventory_sequence is None


async def test_released_intent_state_is_preserved_by_late_terminal_inventory(
    capacity_session: AsyncSession,
) -> None:
    store = CapacityExecutionStore(
        ownership_keyring=OwnershipKeyring({"gb10-key": EXECUTOR_KEYS["gb10"].public_key()})
    )
    permit = await _launch_ready(store, capacity_session)
    row = (
        await capacity_session.execute(
            select(CapacityExecutableIntent).where(
                CapacityExecutableIntent.intent_id == permit.binding.intent_id
            )
        )
    ).scalar_one()
    await _test_only_release_intent(capacity_session, row.intent_id)
    await capacity_session.refresh(row)

    await store.ingest_executor_inventory(
        capacity_session,
        await _next_inventory(
            capacity_session,
            _inventory_execution(permit.binding),
            permit.binding,
            records=(
                _inventory_record(
                    permit.binding,
                    physical_identity="job-123",
                    state="terminal",
                    terminal_evidence_sha256="a" * 64,
                ),
            ),
        ),
    )

    assert row.state == "released"


async def test_released_intent_state_ignores_invalid_late_inventory(
    capacity_session: AsyncSession,
) -> None:
    store = CapacityExecutionStore(
        ownership_keyring=OwnershipKeyring({"gb10-key": EXECUTOR_KEYS["gb10"].public_key()})
    )
    permit = await _launch_ready(store, capacity_session)
    row = (
        await capacity_session.execute(
            select(CapacityExecutableIntent).where(
                CapacityExecutableIntent.intent_id == permit.binding.intent_id
            )
        )
    ).scalar_one()
    await _test_only_release_intent(capacity_session, row.intent_id)
    await capacity_session.refresh(row)

    await store.ingest_executor_inventory(
        capacity_session,
        await _next_inventory(
            capacity_session,
            _inventory_execution(permit.binding),
            permit.binding,
            records=(
                _inventory_record(permit.binding, physical_identity="job-123").model_copy(
                    update={"authority_scope": "registered-loom"}
                ),
            ),
        ),
    )

    assert row.state == "released"


async def test_released_intent_state_ignores_identity_drift(
    capacity_session: AsyncSession,
) -> None:
    store = CapacityExecutionStore(
        ownership_keyring=OwnershipKeyring({"gb10-key": EXECUTOR_KEYS["gb10"].public_key()})
    )
    permit = await _launch_ready(store, capacity_session)
    row = (
        await capacity_session.execute(
            select(CapacityExecutableIntent).where(
                CapacityExecutableIntent.intent_id == permit.binding.intent_id
            )
        )
    ).scalar_one()
    await _test_only_release_intent(
        capacity_session,
        row.intent_id,
        terminal_identity="job-123",
    )
    await capacity_session.refresh(row)

    await store.ingest_executor_inventory(
        capacity_session,
        await _next_inventory(
            capacity_session,
            _inventory_execution(permit.binding),
            permit.binding,
            records=(_inventory_record(permit.binding, physical_identity="job-456"),),
        ),
    )

    assert row.state == "released"
    assert row.terminal_identity == "job-123"


async def test_terminal_intent_quarantines_on_binding_matching_invalid_inventory(
    capacity_session: AsyncSession,
) -> None:
    store = CapacityExecutionStore(
        ownership_keyring=OwnershipKeyring({"gb10-key": EXECUTOR_KEYS["gb10"].public_key()})
    )
    permit = await _launch_ready(store, capacity_session)
    await store.consume_launch_permit(
        capacity_session,
        ExecutablePermitConsumptionV2(
            permit_id=permit.permit_id,
            permit_digest=store.contract_digest(permit),
            binding=permit.binding,
            command_sequence=3,
        ),
    )
    await store.ingest_executor_inventory(
        capacity_session,
        await _next_inventory(
            capacity_session,
            _inventory_execution(permit.binding),
            permit.binding,
            records=(
                _inventory_record(
                    permit.binding,
                    physical_identity="job-123",
                    state="terminal",
                    terminal_evidence_sha256="a" * 64,
                ),
            ),
        ),
    )
    await store.ingest_executor_inventory(
        capacity_session,
        await _next_inventory(
            capacity_session,
            _inventory_execution(permit.binding),
            permit.binding,
            records=(
                _inventory_record(
                    permit.binding,
                    physical_identity="job-123",
                ).model_copy(update={"authority_scope": "registered-loom"}),
            ),
        ),
    )

    row = (
        await capacity_session.execute(
            select(CapacityExecutableIntent).where(
                CapacityExecutableIntent.intent_id == permit.binding.intent_id
            )
        )
    ).scalar_one()
    assert row.state == "quarantined"


async def test_closing_terminal_intent_quarantines_on_conflicting_terminal_evidence(
    capacity_session: AsyncSession,
) -> None:
    store = CapacityExecutionStore(
        ownership_keyring=OwnershipKeyring({"gb10-key": EXECUTOR_KEYS["gb10"].public_key()})
    )
    permit = await _launch_ready(store, capacity_session)
    await store.consume_launch_permit(
        capacity_session,
        ExecutablePermitConsumptionV2(
            permit_id=permit.permit_id,
            permit_digest=store.contract_digest(permit),
            binding=permit.binding,
            command_sequence=3,
        ),
    )
    await store.ingest_executor_inventory(
        capacity_session,
        await _next_inventory(
            capacity_session,
            _inventory_execution(permit.binding),
            permit.binding,
            records=(
                _inventory_record(
                    permit.binding,
                    physical_identity="job-123",
                    state="terminal",
                    terminal_evidence_sha256="a" * 64,
                ),
            ),
        ),
    )
    protected = ExecutableProtectedReleaseV2(
        binding=permit.binding,
        reporter_incarnation=demand_snapshot().reporter_incarnation,
        bootstrap_registration_epoch=1,
        protected_registration_epoch=2,
        bootstrap_revoked=True,
        protected_release_sha256="b" * 64,
    )
    await store.acknowledge_protected_release(
        capacity_session,
        protected,
        actor="development",
        idempotency_key=UUID(int=990),
    )
    await store.begin_intent_close(
        capacity_session,
        ExecutableIntentCloseV2(binding=permit.binding, command_sequence=4),
    )
    await store.ingest_executor_inventory(
        capacity_session,
        await _next_inventory(
            capacity_session,
            _inventory_execution(permit.binding),
            permit.binding,
            records=(
                _inventory_record(
                    permit.binding,
                    physical_identity="job-123",
                    state="terminal",
                    terminal_evidence_sha256="c" * 64,
                ),
            ),
        ),
    )

    row = (
        await capacity_session.execute(
            select(CapacityExecutableIntent).where(
                CapacityExecutableIntent.intent_id == permit.binding.intent_id
            )
        )
    ).scalar_one()
    assert row.state == "quarantined"


async def test_unused_closing_intent_quarantines_on_any_physical_inventory(
    capacity_session: AsyncSession,
) -> None:
    store = CapacityExecutionStore(
        ownership_keyring=OwnershipKeyring({"gb10-key": EXECUTOR_KEYS["gb10"].public_key()})
    )
    permit = await _launch_ready(store, capacity_session)
    await store.begin_intent_close(
        capacity_session,
        ExecutableIntentCloseV2(binding=permit.binding, command_sequence=3),
    )
    await store.ingest_executor_inventory(
        capacity_session,
        await _next_inventory(
            capacity_session,
            _inventory_execution(permit.binding),
            permit.binding,
            records=(_inventory_record(permit.binding, physical_identity="job-123"),),
        ),
    )

    row = (
        await capacity_session.execute(
            select(CapacityExecutableIntent).where(
                CapacityExecutableIntent.intent_id == permit.binding.intent_id
            )
        )
    ).scalar_one()
    assert row.state == "quarantined"


async def test_inventory_identity_drift_quarantines_an_observed_intent(
    capacity_session: AsyncSession,
) -> None:
    store = CapacityExecutionStore(
        ownership_keyring=OwnershipKeyring({"gb10-key": EXECUTOR_KEYS["gb10"].public_key()})
    )
    permit = await _launch_ready(store, capacity_session)
    await store.consume_launch_permit(
        capacity_session,
        ExecutablePermitConsumptionV2(
            permit_id=permit.permit_id,
            permit_digest=store.contract_digest(permit),
            binding=permit.binding,
            command_sequence=3,
        ),
    )
    await store.ingest_executor_inventory(
        capacity_session,
        await _next_inventory(
            capacity_session,
            _inventory_execution(permit.binding),
            permit.binding,
            records=(_inventory_record(permit.binding, physical_identity="job-123"),),
        ),
    )
    await store.ingest_executor_inventory(
        capacity_session,
        await _next_inventory(
            capacity_session,
            _inventory_execution(permit.binding),
            permit.binding,
            records=(_inventory_record(permit.binding, physical_identity="job-456"),),
        ),
    )

    row = (
        await capacity_session.execute(
            select(CapacityExecutableIntent).where(
                CapacityExecutableIntent.intent_id == permit.binding.intent_id
            )
        )
    ).scalar_one()
    assert row.state == "quarantined"
    assert row.terminal_identity == "job-123"


async def test_release_requires_matching_protected_and_physical_terminal_evidence(
    capacity_session: AsyncSession,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = CapacityExecutionStore(
        ownership_keyring=OwnershipKeyring({"gb10-key": EXECUTOR_KEYS["gb10"].public_key()})
    )
    permit = await _launch_ready(store, capacity_session)
    await store.consume_launch_permit(
        capacity_session,
        ExecutablePermitConsumptionV2(
            permit_id=permit.permit_id,
            permit_digest=store.contract_digest(permit),
            binding=permit.binding,
            command_sequence=3,
        ),
    )
    binding = permit.binding
    metadata = ExecutableOwnershipMetadataV2(
        binding=binding,
        controller_authority_sha256="c" * 64,
        trusted_launcher_sha256="e" * 64,
        slurm_cluster="gb10-controller",
        submitter_identity="loom",
        association="loom",
        submitted_at=datetime.now(UTC),
    )
    proof = SignedExecutableOwnershipProofV2(
        metadata=metadata,
        signing_key_id="gb10-key",
        signature_base64=base64.b64encode(
            EXECUTOR_KEYS["gb10"].sign(canonical_executable_bytes(metadata))
        ).decode("ascii"),
    )
    active_payload = binding.execution.model_dump(mode="python")
    active_payload.pop("allocation_epoch")
    active_payload.pop("executable")
    inventory = await _next_inventory(
        capacity_session,
        ExecutionContextV2.model_validate(active_payload),
        binding,
        records=(
            ExecutableInventoryRecordV2(
                physical_identity="job-123",
                physical_kind="slurm-job",
                authority_scope="dedicated-loom-association",
                state="terminal",
                resources=binding.resources,
                node_ids=binding.node_ids,
                controller_evidence_sha256="9" * 64,
                ownership_proof=proof,
                terminal_evidence_sha256="a" * 64,
            ),
        ),
    )
    await store.ingest_executor_inventory(capacity_session, inventory)
    protected = ExecutableProtectedReleaseV2(
        binding=binding,
        reporter_incarnation=demand_snapshot().reporter_incarnation,
        bootstrap_registration_epoch=1,
        protected_registration_epoch=2,
        bootstrap_revoked=True,
        protected_release_sha256="b" * 64,
    )
    await store.acknowledge_protected_release(
        capacity_session,
        protected,
        actor="development",
        idempotency_key=UUID(int=903),
    )
    close = await store.next_pool_work(capacity_session, executor_binding("gb10"))
    assert isinstance(close, ExecutableIntentCloseV2)
    await store.begin_intent_close(capacity_session, close)
    release = await store.next_pool_work(capacity_session, executor_binding("gb10"))
    assert isinstance(release, ExecutablePartialReleaseV2)

    lock_order: list[str] = []
    original_lock_authority = store._lock_authority
    original_lock_current_epoch = store._lock_current_epoch
    original_exact_registration = store._exact_registration
    original_runtime_state = store._runtime_state
    original_locked_intent = store._locked_intent

    async def record_authority(session):  # type: ignore[no-untyped-def]
        lock_order.append("authority")
        return await original_lock_authority(session)

    async def record_epoch(session, authority):  # type: ignore[no-untyped-def]
        lock_order.append("epoch")
        return await original_lock_current_epoch(session, authority)

    async def record_registration(session, epoch, executor):  # type: ignore[no-untyped-def]
        lock_order.append("executor-registration")
        return await original_exact_registration(session, epoch, executor)

    async def record_state(  # type: ignore[no-untyped-def]
        session,
        registration,
        epoch,
        *,
        create,
    ):
        lock_order.append("executor-state")
        return await original_runtime_state(session, registration, epoch, create=create)

    async def record_intent(session, intent_id):  # type: ignore[no-untyped-def]
        lock_order.append("intent")
        return await original_locked_intent(session, intent_id)

    monkeypatch.setattr(store, "_lock_authority", record_authority)
    monkeypatch.setattr(store, "_lock_current_epoch", record_epoch)
    monkeypatch.setattr(store, "_exact_registration", record_registration)
    monkeypatch.setattr(store, "_runtime_state", record_state)
    monkeypatch.setattr(store, "_locked_intent", record_intent)

    released = await store.release_shapes(capacity_session, release)

    assert lock_order[:5] == [
        "authority",
        "epoch",
        "executor-registration",
        "executor-state",
        "intent",
    ]
    assert released.released_shape_ids == (binding.shape_instance_id,)


async def test_protected_release_receipts_retain_monotonic_successors_and_old_replays(
    capacity_session: AsyncSession,
) -> None:
    store = CapacityExecutionStore(
        ownership_keyring=OwnershipKeyring({"gb10-key": EXECUTOR_KEYS["gb10"].public_key()})
    )
    permit = await _launch_ready(store, capacity_session)
    await store.consume_launch_permit(
        capacity_session,
        ExecutablePermitConsumptionV2(
            permit_id=permit.permit_id,
            permit_digest=store.contract_digest(permit),
            binding=permit.binding,
            command_sequence=3,
        ),
    )
    await store.ingest_executor_inventory(
        capacity_session,
        await _next_inventory(
            capacity_session,
            _inventory_execution(permit.binding),
            permit.binding,
            records=(
                _inventory_record(
                    permit.binding,
                    physical_identity="job-123",
                    state="terminal",
                    terminal_evidence_sha256="a" * 64,
                ),
            ),
        ),
    )
    first = ExecutableProtectedReleaseV2(
        binding=permit.binding,
        reporter_incarnation=demand_snapshot().reporter_incarnation,
        bootstrap_registration_epoch=1,
        protected_registration_epoch=2,
        bootstrap_revoked=True,
        protected_release_sha256="b" * 64,
    )
    successor = first.model_copy(
        update={
            "protected_registration_epoch": 3,
            "protected_release_sha256": "c" * 64,
        }
    )
    first_key = UUID(int=992)
    successor_key = UUID(int=993)
    first_result = await store.acknowledge_protected_release(
        capacity_session,
        first,
        actor="development",
        idempotency_key=first_key,
    )
    successor_result = await store.acknowledge_protected_release(
        capacity_session,
        successor,
        actor="development",
        idempotency_key=successor_key,
    )

    replay = await store.acknowledge_protected_release(
        capacity_session,
        first,
        actor="development",
        idempotency_key=first_key,
    )

    assert first_result.replayed is False
    assert successor_result.replayed is False
    assert replay.replayed is True
    assert replay.protected_release_sha256 == "b" * 64
    receipts = (
        await capacity_session.execute(
            text(
                "SELECT protected_registration_epoch, protected_release_sha256 "
                "FROM capacity_executable_protected_release_receipts "
                "WHERE intent_id = :intent_id ORDER BY protected_registration_epoch"
            ),
            {"intent_id": permit.binding.intent_id},
        )
    ).all()
    assert receipts == [(2, "b" * 64), (3, "c" * 64)]

    close = await store.next_pool_work(capacity_session, executor_binding("gb10"))
    assert isinstance(close, ExecutableIntentCloseV2)
    await store.begin_intent_close(capacity_session, close)
    release = await store.next_pool_work(capacity_session, executor_binding("gb10"))
    assert isinstance(release, ExecutablePartialReleaseV2)
    assert release.releases[0].protected_registration_epoch == 3
    assert release.releases[0].protected_release_sha256 == "c" * 64


async def test_protected_release_validates_authority_before_idempotent_replay(
    capacity_session: AsyncSession,
) -> None:
    store = CapacityExecutionStore()
    permit, _inventory = await _consume_then_publish_empty_inventory(store, capacity_session)
    release = ExecutableProtectedReleaseV2(
        binding=permit.binding,
        reporter_incarnation=demand_snapshot().reporter_incarnation,
        bootstrap_registration_epoch=1,
        protected_registration_epoch=2,
        bootstrap_revoked=True,
        protected_release_sha256="b" * 64,
    )
    idempotency_key = UUID(int=994)
    await store.acknowledge_protected_release(
        capacity_session,
        release,
        actor="development",
        idempotency_key=idempotency_key,
    )
    stale_execution = release.binding.execution.model_copy(
        update={"writer_epoch": release.binding.execution.writer_epoch + 10}
    )
    stale = release.model_copy(
        update={"binding": release.binding.model_copy(update={"execution": stale_execution})}
    )

    with pytest.raises(ExecutionConflictError, match="execution fence changed"):
        await store.acknowledge_protected_release(
            capacity_session,
            stale,
            actor="development",
            idempotency_key=idempotency_key,
        )


@pytest.mark.parametrize(
    "invalid_initial",
    ("non-proposed", "nonempty-evidence", "binding-mismatch"),
)
async def test_intent_insert_guard_requires_pristine_bound_proposal(
    capacity_session: AsyncSession,
    invalid_initial: str,
) -> None:
    store = CapacityExecutionStore()
    permit = await _launch_ready(store, capacity_session)
    row = (
        await capacity_session.execute(
            select(CapacityExecutableIntent).where(
                CapacityExecutableIntent.intent_id == permit.binding.intent_id
            )
        )
    ).scalar_one()
    clone_binding = permit.binding.model_copy(
        update={
            "intent_id": uuid4(),
            "tranche_id": uuid4(),
            "shape_instance_id": f"{permit.binding.shape_instance_id}-{invalid_initial}",
        }
    )
    binding_payload = clone_binding.model_dump(mode="json", exclude_none=False)
    if invalid_initial == "binding-mismatch":
        binding_payload["shape_instance_id"] = f"{clone_binding.shape_instance_id}-forged"
    clone = CapacityExecutableIntent(
        intent_id=clone_binding.intent_id,
        tranche_id=clone_binding.tranche_id,
        shape_instance_id=clone_binding.shape_instance_id,
        execution_epoch=row.execution_epoch,
        execution_manifest_sha256=row.execution_manifest_sha256,
        configuration_epoch=row.configuration_epoch,
        allocation_epoch=row.allocation_epoch,
        executor_id=row.executor_id,
        executor_incarnation=row.executor_incarnation,
        pool_id=row.pool_id,
        pool_generation=row.pool_generation,
        subject_id=row.subject_id,
        subject_incarnation=row.subject_incarnation,
        launch_rank=row.launch_rank + 100,
        proposal_digest=row.proposal_digest,
        proposal_payload=json.loads(json.dumps(row.proposal_payload)),
        binding_digest=store.contract_digest(clone_binding),
        binding_payload=binding_payload,
        state="released" if invalid_initial == "non-proposed" else "proposed",
        released_at=datetime.now(UTC) if invalid_initial == "non-proposed" else None,
        permit_id=uuid4() if invalid_initial == "nonempty-evidence" else None,
        permit_epoch=1 if invalid_initial == "nonempty-evidence" else None,
        permit_digest="a" * 64 if invalid_initial == "nonempty-evidence" else None,
        permit_payload={} if invalid_initial == "nonempty-evidence" else None,
        permit_expires_at=(
            datetime.now(UTC) + timedelta(seconds=30)
            if invalid_initial == "nonempty-evidence"
            else None
        ),
    )

    with pytest.raises(DBAPIError, match="executable intent"):
        async with capacity_session.begin_nested():
            capacity_session.add(clone)
            await capacity_session.flush()


async def test_intent_guard_rejects_same_epoch_permit_replacement(
    capacity_session: AsyncSession,
) -> None:
    store = CapacityExecutionStore()
    permit = await _launch_ready(store, capacity_session)
    forged = permit.model_copy(
        update={
            "permit_id": uuid4(),
            "expires_at": permit.expires_at + timedelta(seconds=30),
        }
    )

    with pytest.raises(DBAPIError, match="executable intent"):
        async with capacity_session.begin_nested():
            await capacity_session.execute(
                update(CapacityExecutableIntent)
                .where(CapacityExecutableIntent.intent_id == permit.binding.intent_id)
                .values(
                    permit_id=forged.permit_id,
                    permit_digest=store.contract_digest(forged),
                    permit_payload=forged.model_dump(mode="json", exclude_none=False),
                    permit_expires_at=forged.expires_at,
                )
            )


async def test_intent_guard_rejects_same_state_terminal_fabrication(
    capacity_session: AsyncSession,
) -> None:
    store = CapacityExecutionStore()
    permit = await _launch_ready(store, capacity_session)
    await store.consume_launch_permit(
        capacity_session,
        ExecutablePermitConsumptionV2(
            permit_id=permit.permit_id,
            permit_digest=store.contract_digest(permit),
            binding=permit.binding,
            command_sequence=3,
        ),
    )

    with pytest.raises(DBAPIError, match="executable intent"):
        async with capacity_session.begin_nested():
            await capacity_session.execute(
                update(CapacityExecutableIntent)
                .where(CapacityExecutableIntent.intent_id == permit.binding.intent_id)
                .values(
                    inventory_sequence=1,
                    observed_state="terminal",
                    terminal_kind="unused",
                    terminal_identity=permit.binding.shape_instance_id,
                    terminal_evidence_sha256="b" * 64,
                )
            )


async def test_intent_guard_freezes_terminal_kind_after_recovery(
    capacity_session: AsyncSession,
) -> None:
    store = CapacityExecutionStore()
    permit, inventory = await _consume_then_publish_empty_inventory(store, capacity_session)
    await store.recover_unsubmitted_permit(
        capacity_session,
        executable_contracts_module.ExecutableSubmissionRecoveryV2(
            binding=permit.binding,
            permit_id=permit.permit_id,
            permit_digest=store.contract_digest(permit),
            command_sequence=4,
            inventory_sequence=inventory.inventory_sequence,
            inventory_digest=store.contract_digest(inventory),
            controller_query_completed_at=datetime.now(UTC),
            submit_process_absent=True,
            scheduler_submission_absent=True,
            controller_evidence_sha256="a" * 64,
        ),
    )

    with pytest.raises(DBAPIError, match="executable intent"):
        async with capacity_session.begin_nested():
            await capacity_session.execute(
                update(CapacityExecutableIntent)
                .where(CapacityExecutableIntent.intent_id == permit.binding.intent_id)
                .values(terminal_kind="worker")
            )


async def test_intent_guard_rejects_legal_transition_with_unrelated_evidence(
    capacity_session: AsyncSession,
) -> None:
    store = CapacityExecutionStore()
    permit = await _launch_ready(store, capacity_session)

    with pytest.raises(DBAPIError, match="executable intent"):
        async with capacity_session.begin_nested():
            await capacity_session.execute(
                update(CapacityExecutableIntent)
                .where(CapacityExecutableIntent.intent_id == permit.binding.intent_id)
                .values(state="closing", released_at=datetime.now(UTC))
            )


async def test_intent_guard_rejects_release_without_terminal_and_protected_evidence(
    capacity_session: AsyncSession,
) -> None:
    store = CapacityExecutionStore(
        ownership_keyring=OwnershipKeyring({"gb10-key": EXECUTOR_KEYS["gb10"].public_key()})
    )
    permit = await _launch_ready(store, capacity_session)
    await store.consume_launch_permit(
        capacity_session,
        ExecutablePermitConsumptionV2(
            permit_id=permit.permit_id,
            permit_digest=store.contract_digest(permit),
            binding=permit.binding,
            command_sequence=3,
        ),
    )
    await store.ingest_executor_inventory(
        capacity_session,
        await _next_inventory(
            capacity_session,
            _inventory_execution(permit.binding),
            permit.binding,
            records=(_inventory_record(permit.binding, physical_identity="job-live"),),
        ),
    )
    await store.begin_intent_close(
        capacity_session,
        ExecutableIntentCloseV2(binding=permit.binding, command_sequence=4),
    )

    with pytest.raises(DBAPIError, match="executable intent release"):
        async with capacity_session.begin_nested():
            await capacity_session.execute(
                update(CapacityExecutableIntent)
                .where(CapacityExecutableIntent.intent_id == permit.binding.intent_id)
                .values(state="released", released_at=datetime.now(UTC))
            )


async def test_intent_guard_rejects_release_without_protected_receipt(
    capacity_session: AsyncSession,
) -> None:
    store = CapacityExecutionStore(
        ownership_keyring=OwnershipKeyring({"gb10-key": EXECUTOR_KEYS["gb10"].public_key()})
    )
    permit = await _launch_ready(store, capacity_session)
    await store.consume_launch_permit(
        capacity_session,
        ExecutablePermitConsumptionV2(
            permit_id=permit.permit_id,
            permit_digest=store.contract_digest(permit),
            binding=permit.binding,
            command_sequence=3,
        ),
    )
    await store.ingest_executor_inventory(
        capacity_session,
        await _next_inventory(
            capacity_session,
            _inventory_execution(permit.binding),
            permit.binding,
            records=(
                _inventory_record(
                    permit.binding,
                    physical_identity="job-123",
                    state="terminal",
                    terminal_evidence_sha256="a" * 64,
                ),
            ),
        ),
    )
    await store.begin_intent_close(
        capacity_session,
        ExecutableIntentCloseV2(binding=permit.binding, command_sequence=4),
    )

    with pytest.raises(DBAPIError, match="executable intent release"):
        async with capacity_session.begin_nested():
            await capacity_session.execute(
                update(CapacityExecutableIntent)
                .where(CapacityExecutableIntent.intent_id == permit.binding.intent_id)
                .values(state="released", released_at=datetime.now(UTC))
            )


async def test_queue_database_guards_reject_direct_state_and_high_water_rewrites(
    capacity_session: AsyncSession,
) -> None:
    store = CapacityExecutionStore()
    permit = await _launch_ready(store, capacity_session)

    with pytest.raises(DBAPIError, match="intent state transition is invalid"):
        async with capacity_session.begin_nested():
            await capacity_session.execute(
                update(CapacityExecutableIntent)
                .where(CapacityExecutableIntent.intent_id == permit.binding.intent_id)
                .values(state="released")
            )

    await store.consume_launch_permit(
        capacity_session,
        ExecutablePermitConsumptionV2(
            permit_id=permit.permit_id,
            permit_digest=store.contract_digest(permit),
            binding=permit.binding,
            command_sequence=3,
        ),
    )
    with pytest.raises(DBAPIError, match="launch rate bucket transition is invalid"):
        async with capacity_session.begin_nested():
            await capacity_session.execute(
                text(
                    "UPDATE public.capacity_executable_launch_rate_buckets "
                    "SET available_microtokens = capacity_microtokens"
                )
            )

    with pytest.raises(DBAPIError, match="executor high-water regressed"):
        async with capacity_session.begin_nested():
            await capacity_session.execute(
                update(CapacityExecutableExecutorState)
                .where(
                    CapacityExecutableExecutorState.executor_incarnation
                    == permit.binding.executor_incarnation
                )
                .values(inventory_high_water=0)
            )

    await capacity_session.execute(
        update(CapacityExecutableExecutorState)
        .where(
            CapacityExecutableExecutorState.executor_incarnation
            == permit.binding.executor_incarnation
        )
        .values(state="fenced")
    )
    with pytest.raises(DBAPIError, match="executor cannot be unfenced"):
        async with capacity_session.begin_nested():
            await capacity_session.execute(
                update(CapacityExecutableExecutorState)
                .where(
                    CapacityExecutableExecutorState.executor_incarnation
                    == permit.binding.executor_incarnation
                )
                .values(state="current")
            )


async def test_queue_receipts_are_append_only_under_direct_sql(
    capacity_session: AsyncSession,
) -> None:
    store = CapacityExecutionStore()
    permit, inventory = await _consume_then_publish_empty_inventory(store, capacity_session)
    recovery = executable_contracts_module.ExecutableSubmissionRecoveryV2(
        binding=permit.binding,
        permit_id=permit.permit_id,
        permit_digest=store.contract_digest(permit),
        command_sequence=4,
        inventory_sequence=inventory.inventory_sequence,
        inventory_digest=store.contract_digest(inventory),
        controller_query_completed_at=datetime.now(UTC),
        submit_process_absent=True,
        scheduler_submission_absent=True,
        controller_evidence_sha256="a" * 64,
    )
    await store.recover_unsubmitted_permit(capacity_session, recovery)
    protected = ExecutableProtectedReleaseV2(
        binding=permit.binding,
        reporter_incarnation=demand_snapshot().reporter_incarnation,
        bootstrap_registration_epoch=1,
        protected_registration_epoch=2,
        bootstrap_revoked=True,
        protected_release_sha256="b" * 64,
    )
    await store.acknowledge_protected_release(
        capacity_session,
        protected,
        actor="development",
        idempotency_key=UUID(int=995),
    )

    for statement in (
        "UPDATE public.capacity_executable_command_receipts SET request_digest = repeat('f', 64)",
        "DELETE FROM public.capacity_executable_command_receipts",
        "TRUNCATE public.capacity_executable_command_receipts",
        "UPDATE public.capacity_executable_protected_release_receipts "
        "SET protected_release_sha256 = repeat('f', 64)",
        "DELETE FROM public.capacity_executable_protected_release_receipts",
        "TRUNCATE public.capacity_executable_protected_release_receipts",
    ):
        with pytest.raises(DBAPIError, match="append-only"):
            async with capacity_session.begin_nested():
                await capacity_session.execute(text(statement))


def _direct_protected_release_params(
    release: ExecutableProtectedReleaseV2,
    *,
    release_payload: dict[str, object] | None = None,
    reporter_incarnation: UUID | None = None,
    bootstrap_registration_epoch: int | None = None,
) -> dict[str, object]:
    return {
        "id": uuid4(),
        "idempotency_key": uuid4(),
        "intent_id": release.binding.intent_id,
        "execution_epoch": release.binding.execution.execution_epoch,
        "execution_manifest_sha256": release.binding.execution.execution_manifest_sha256,
        "reporter_incarnation": reporter_incarnation or release.reporter_incarnation,
        "bootstrap_registration_epoch": (
            release.bootstrap_registration_epoch
            if bootstrap_registration_epoch is None
            else bootstrap_registration_epoch
        ),
        "protected_registration_epoch": release.protected_registration_epoch,
        "protected_release_sha256": release.protected_release_sha256,
        "acknowledgement_digest": "c" * 64,
        "actor_id": "direct-sql-test",
        "release_payload": json.dumps(
            release.model_dump(mode="json", exclude_none=False)
            if release_payload is None
            else release_payload
        ),
    }


async def _insert_direct_protected_release(
    session: AsyncSession,
    params: dict[str, object],
) -> None:
    await session.execute(
        text(
            "INSERT INTO public.capacity_executable_protected_release_receipts "
            "(id, idempotency_key, intent_id, execution_epoch, "
            "execution_manifest_sha256, reporter_incarnation, "
            "bootstrap_registration_epoch, protected_registration_epoch, "
            "protected_release_sha256, acknowledgement_digest, actor_id, "
            "release_payload) VALUES "
            "(:id, :idempotency_key, :intent_id, :execution_epoch, "
            ":execution_manifest_sha256, :reporter_incarnation, "
            ":bootstrap_registration_epoch, :protected_registration_epoch, "
            ":protected_release_sha256, :acknowledgement_digest, :actor_id, "
            "CAST(:release_payload AS jsonb))"
        ),
        params,
    )


@pytest.mark.parametrize(
    "forgery",
    ("missing", "null-binding", "wrong-reporter", "wrong-bootstrap", "wrong-binding"),
)
async def test_protected_release_insert_guard_rejects_unbound_direct_sql(
    capacity_session: AsyncSession,
    forgery: str,
) -> None:
    store = CapacityExecutionStore()
    permit, inventory = await _consume_then_publish_empty_inventory(store, capacity_session)
    recovery = executable_contracts_module.ExecutableSubmissionRecoveryV2(
        binding=permit.binding,
        permit_id=permit.permit_id,
        permit_digest=store.contract_digest(permit),
        command_sequence=4,
        inventory_sequence=inventory.inventory_sequence,
        inventory_digest=store.contract_digest(inventory),
        controller_query_completed_at=datetime.now(UTC),
        submit_process_absent=True,
        scheduler_submission_absent=True,
        controller_evidence_sha256="a" * 64,
    )
    await store.recover_unsubmitted_permit(capacity_session, recovery)
    release = ExecutableProtectedReleaseV2(
        binding=permit.binding,
        reporter_incarnation=demand_snapshot().reporter_incarnation,
        bootstrap_registration_epoch=1,
        protected_registration_epoch=2,
        bootstrap_revoked=True,
        protected_release_sha256="b" * 64,
    )
    payload = release.model_dump(mode="json", exclude_none=False)
    reporter_incarnation = release.reporter_incarnation
    bootstrap_registration_epoch = release.bootstrap_registration_epoch
    if forgery == "missing":
        payload = {}
    elif forgery == "null-binding":
        payload["binding"] = None
    elif forgery == "wrong-reporter":
        reporter_incarnation = UUID(int=996)
        payload["reporter_incarnation"] = str(reporter_incarnation)
    elif forgery == "wrong-bootstrap":
        bootstrap_registration_epoch = 0
        payload["bootstrap_registration_epoch"] = bootstrap_registration_epoch
    else:
        forged_binding = json.loads(json.dumps(payload["binding"]))
        forged_binding["shape_instance_id"] = f"{permit.binding.shape_instance_id}-forged"
        payload["binding"] = forged_binding

    await capacity_session.execute(text("SET LOCAL search_path = pg_temp, public"))
    with pytest.raises(DBAPIError, match="protected release receipt"):
        async with capacity_session.begin_nested():
            await _insert_direct_protected_release(
                capacity_session,
                _direct_protected_release_params(
                    release,
                    release_payload=payload,
                    reporter_incarnation=reporter_incarnation,
                    bootstrap_registration_epoch=bootstrap_registration_epoch,
                ),
            )


async def test_protected_release_insert_guard_serializes_concurrent_epochs(
    isolated_capacity_postgres_url: str,
) -> None:
    engine = create_async_engine(
        isolated_capacity_postgres_url,
        isolation_level="SERIALIZABLE",
    )
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    try:
        async with engine.connect() as setup_connection:
            setup_session = AsyncSession(
                bind=setup_connection,
                expire_on_commit=False,
                join_transaction_mode="create_savepoint",
            )
            store = CapacityExecutionStore()
            permit, inventory = await _consume_then_publish_empty_inventory(store, setup_session)
            recovery = executable_contracts_module.ExecutableSubmissionRecoveryV2(
                binding=permit.binding,
                permit_id=permit.permit_id,
                permit_digest=store.contract_digest(permit),
                command_sequence=4,
                inventory_sequence=inventory.inventory_sequence,
                inventory_digest=store.contract_digest(inventory),
                controller_query_completed_at=datetime.now(UTC),
                submit_process_absent=True,
                scheduler_submission_absent=True,
                controller_evidence_sha256="a" * 64,
            )
            await store.recover_unsubmitted_permit(setup_session, recovery)
            release = ExecutableProtectedReleaseV2(
                binding=permit.binding,
                reporter_incarnation=demand_snapshot().reporter_incarnation,
                bootstrap_registration_epoch=1,
                protected_registration_epoch=2,
                bootstrap_revoked=True,
                protected_release_sha256="b" * 64,
            )
            await setup_session.commit()
            await setup_session.close()

        later = release.model_copy(
            update={
                "protected_registration_epoch": 3,
                "protected_release_sha256": "d" * 64,
            }
        )
        async with (
            session_factory() as later_session,
            session_factory() as earlier_session,
            session_factory() as observer_session,
        ):
            earlier_pid = (
                await earlier_session.execute(text("SELECT pg_backend_pid()"))
            ).scalar_one()
            await _insert_direct_protected_release(
                later_session,
                _direct_protected_release_params(later),
            )

            async def insert_earlier_epoch() -> None:
                await _insert_direct_protected_release(
                    earlier_session,
                    _direct_protected_release_params(release),
                )
                await earlier_session.commit()

            earlier_task = asyncio.create_task(insert_earlier_epoch())
            blocked = False
            for _attempt in range(200):
                wait_event_type = (
                    await observer_session.execute(
                        text("SELECT wait_event_type FROM pg_stat_activity WHERE pid = :pid"),
                        {"pid": earlier_pid},
                    )
                ).scalar_one()
                if wait_event_type == "Lock":
                    blocked = True
                    break
                await asyncio.sleep(0.01)
            assert blocked, "earlier receipt insert did not wait on the intent fence"

            await later_session.commit()
            with pytest.raises(DBAPIError):
                await earlier_task
            await earlier_session.rollback()
            await observer_session.rollback()
            committed_epochs = (
                (
                    await observer_session.execute(
                        text(
                            "SELECT protected_registration_epoch FROM "
                            "public.capacity_executable_protected_release_receipts "
                            "WHERE intent_id = :intent_id ORDER BY protected_registration_epoch"
                        ),
                        {"intent_id": release.binding.intent_id},
                    )
                )
                .scalars()
                .all()
            )
            assert committed_epochs == [3]
    finally:
        await engine.dispose()


async def test_intent_parent_guard_uses_public_schema_under_hostile_search_path(
    capacity_session: AsyncSession,
) -> None:
    store = CapacityExecutionStore()
    permit = await _launch_ready(store, capacity_session)
    row = (
        await capacity_session.execute(
            select(CapacityExecutableIntent).where(
                CapacityExecutableIntent.intent_id == permit.binding.intent_id
            )
        )
    ).scalar_one()
    await capacity_session.execute(
        text(
            "ALTER TABLE public.capacity_allocation_epochs "
            "DISABLE TRIGGER capacity_executable_allocation_seal_guard"
        )
    )
    await capacity_session.execute(
        text(
            "ALTER TABLE public.capacity_allocation_epochs "
            "DISABLE TRIGGER capacity_allocation_epoch_binding_guard"
        )
    )
    await capacity_session.execute(
        update(CapacityAllocationEpoch)
        .where(CapacityAllocationEpoch.allocation_epoch == row.allocation_epoch)
        .values(sealed=False)
    )
    await capacity_session.execute(
        text(
            "ALTER TABLE public.capacity_allocation_epochs "
            "ENABLE TRIGGER capacity_allocation_epoch_binding_guard"
        )
    )
    await capacity_session.execute(
        text(
            "ALTER TABLE public.capacity_allocation_epochs "
            "ENABLE TRIGGER capacity_executable_allocation_seal_guard"
        )
    )
    await capacity_session.execute(text("SET LOCAL search_path = pg_temp, public"))
    await capacity_session.execute(
        text(
            "CREATE TEMP TABLE capacity_allocation_epochs "
            "(LIKE public.capacity_allocation_epochs INCLUDING ALL)"
        )
    )
    await capacity_session.execute(
        text(
            "INSERT INTO pg_temp.capacity_allocation_epochs "
            "SELECT * FROM public.capacity_allocation_epochs "
            "WHERE allocation_epoch = :allocation_epoch"
        ),
        {"allocation_epoch": row.allocation_epoch},
    )
    await capacity_session.execute(
        text(
            "UPDATE pg_temp.capacity_allocation_epochs SET sealed = true "
            "WHERE allocation_epoch = :allocation_epoch"
        ),
        {"allocation_epoch": row.allocation_epoch},
    )
    clone = CapacityExecutableIntent(
        intent_id=uuid4(),
        tranche_id=uuid4(),
        shape_instance_id=f"{row.shape_instance_id}-hostile",
        execution_epoch=row.execution_epoch,
        execution_manifest_sha256=row.execution_manifest_sha256,
        configuration_epoch=row.configuration_epoch,
        allocation_epoch=row.allocation_epoch,
        executor_id=row.executor_id,
        executor_incarnation=row.executor_incarnation,
        pool_id=row.pool_id,
        pool_generation=row.pool_generation,
        subject_id=row.subject_id,
        subject_incarnation=row.subject_incarnation,
        launch_rank=row.launch_rank + 100,
        proposal_digest=row.proposal_digest,
        proposal_payload=json.loads(json.dumps(row.proposal_payload)),
        binding_digest=row.binding_digest,
        binding_payload=json.loads(json.dumps(row.binding_payload)),
        state="proposed",
    )

    with pytest.raises(DBAPIError, match="sealed executable allocation parent"):
        async with capacity_session.begin_nested():
            capacity_session.add(clone)
            await capacity_session.flush()

    functions = (
        await capacity_session.execute(
            text(
                "SELECT p.proname, p.proconfig FROM pg_catalog.pg_proc AS p "
                "JOIN pg_catalog.pg_namespace AS n ON n.oid = p.pronamespace "
                "WHERE n.nspname = 'public' "
                "AND p.proname LIKE 'capacity_executable_%_guard'"
            )
        )
    ).all()
    assert functions
    assert all(config == ["search_path=pg_catalog"] for _name, config in functions)
