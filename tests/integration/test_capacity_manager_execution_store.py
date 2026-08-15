"""Transactional coverage for the executable-v2 capacity work ledger."""

from __future__ import annotations

import asyncio
import base64
import json
from datetime import UTC, datetime, timedelta
from uuid import UUID, uuid4

import pytest
from sqlalchemy import event, select, text, update
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

import loom_capacity_manager.executable_contracts as executable_contracts_module
import loom_capacity_manager.execution_store as execution_store_module
from loom_capacity_manager.executable_contracts import (
    ExecutableBootstrapRegistrationV2,
    ExecutableExecutorHeartbeatV2,
    ExecutableExecutorInventoryV2,
    ExecutableIntentBindingV2,
    ExecutableIntentCloseV2,
    ExecutableInventoryRecordV2,
    ExecutableOwnershipMetadataV2,
    ExecutablePartialReleaseV2,
    ExecutablePermitConsumptionV2,
    ExecutableProtectedReleaseV2,
    ExecutableReservationAcceptanceV2,
    ExecutableReservationProposalV2,
    ExecutionActivationV2,
    ExecutionContextV2,
    ExecutionRetirementExecutorCheckpointV2,
    ExecutionRetirementV2,
    SignedExecutableOwnershipProofV2,
    canonical_executable_bytes,
    canonical_inventory_confirmation_journal_head,
)
from loom_capacity_manager.execution_store import CapacityExecutionStore
from loom_capacity_manager.models import (
    CapacityAccountPolicy,
    CapacityAllocation,
    CapacityAllocationEpoch,
    CapacityAuthorityState,
    CapacityExecutableExecutorState,
    CapacityExecutableIntent,
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
    register_execution_executors,
    setup_execution,
)
from tests.capacity_fixtures import demand_snapshot, pool_observation, resource_vector, shape


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
        demand_snapshot(sequence=1, pending_attempt_ids=("attempt-pending",)),
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
    active = await fixture.store.activate_execution_epoch(
        session,
        ExecutionActivationV2(
            authority_incarnation=prepared.authority_incarnation,
            expected_writer_epoch=prepared.writer_epoch,
            execution_epoch=prepared.execution_epoch,
            execution_manifest_sha256=prepared.execution_manifest_sha256,
            executable_new_capacity_ceiling=policy.executable_new_capacity_ceiling,
            executable_new_capacity_rate_per_minute=policy.executable_new_capacity_rate_per_minute,
        ),
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


async def _heartbeat(
    store: CapacityExecutionStore,
    session: AsyncSession,
    active,  # type: ignore[no-untyped-def]
    *,
    pool_id: str,
) -> None:
    binding = executor_binding(pool_id)
    await store.heartbeat_executor(
        session,
        ExecutableExecutorHeartbeatV2(
            execution=active,
            executor_id=binding.executor_id,
            executor_incarnation=binding.executor_incarnation,
            pool_id=binding.pool_id,
            pool_generation=binding.pool_generation,
            heartbeat_sequence=1,
            journal_sequence=0,
            journal_digest="0" * 64,
        ),
    )
    await store.ingest_executor_inventory(
        session,
        ExecutableExecutorInventoryV2(
            execution=active,
            executor_id=binding.executor_id,
            executor_incarnation=binding.executor_incarnation,
            pool_id=binding.pool_id,
            pool_generation=binding.pool_generation,
            inventory_sequence=1,
            journal_sequence=0,
            journal_digest="0" * 64,
        ),
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

    heartbeat = await store.heartbeat_executor(
        capacity_session,
        ExecutableExecutorHeartbeatV2(
            execution=active,
            executor_id=executor.executor_id,
            executor_incarnation=executor.executor_incarnation,
            pool_id=executor.pool_id,
            pool_generation=executor.pool_generation,
            heartbeat_sequence=1,
            journal_sequence=0,
            journal_digest="0" * 64,
        ),
    )
    inventory_contract = ExecutableExecutorInventoryV2(
        execution=active,
        executor_id=executor.executor_id,
        executor_incarnation=executor.executor_incarnation,
        pool_id=executor.pool_id,
        pool_generation=executor.pool_generation,
        inventory_sequence=1,
        journal_sequence=0,
        journal_digest="0" * 64,
    )
    inventory = await store.ingest_executor_inventory(
        capacity_session,
        inventory_contract,
    )

    assert heartbeat.heartbeat_sequence == 1
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
    await store.heartbeat_executor(
        capacity_session,
        ExecutableExecutorHeartbeatV2(
            execution=active,
            executor_id=executor.executor_id,
            executor_incarnation=executor.executor_incarnation,
            pool_id=executor.pool_id,
            pool_generation=executor.pool_generation,
            heartbeat_sequence=1,
            journal_sequence=0,
            journal_digest="0" * 64,
        ),
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
                    heartbeat_sequence=2,
                    journal_sequence=0,
                    journal_digest="0" * 64,
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
                    inventory_sequence=1,
                    journal_sequence=0,
                    journal_digest="0" * 64,
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
    active, _allocation_epoch = await _active_plan_with_policy(
        session,
        policy=execution_policy() if policy is None else policy,
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
    await store.register_bootstrap(
        session,
        ExecutableBootstrapRegistrationV2(
            binding=intent,
            command_sequence=2,
            bootstrap_registration_epoch=1,
            bootstrap_evidence_sha256="8" * 64,
        ),
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
    inventory = ExecutableExecutorInventoryV2(
        execution=_inventory_execution(permit.binding),
        executor_id=permit.binding.executor_id,
        executor_incarnation=permit.binding.executor_incarnation,
        pool_id=permit.binding.pool_id,
        pool_generation=permit.binding.pool_generation,
        inventory_sequence=2,
        journal_sequence=0,
        journal_digest="0" * 64,
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
    inventory = ExecutableExecutorInventoryV2(
        execution=_inventory_execution(permit.binding),
        executor_id=permit.binding.executor_id,
        executor_incarnation=permit.binding.executor_incarnation,
        pool_id=permit.binding.pool_id,
        pool_generation=permit.binding.pool_generation,
        inventory_sequence=2,
        journal_sequence=0,
        journal_digest="0" * 64,
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

    await session.execute(text(f"ALTER TABLE public.{table_name} DISABLE TRIGGER {trigger_name}"))
    await session.execute(statement.execution_options(synchronize_session=False))
    await session.execute(text(f"ALTER TABLE public.{table_name} ENABLE TRIGGER {trigger_name}"))


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
    await _heartbeat(store, capacity_session, active, pool_id="gb10")
    changed = ExecutableExecutorHeartbeatV2(
        execution=active,
        executor_id=binding.executor_id,
        executor_incarnation=binding.executor_incarnation,
        pool_id=binding.pool_id,
        pool_generation=binding.pool_generation,
        heartbeat_sequence=1,
        journal_sequence=1,
        journal_digest="7" * 64,
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
    await store.heartbeat_executor(
        capacity_session,
        ExecutableExecutorHeartbeatV2(
            execution=active,
            executor_id=binding.executor_id,
            executor_incarnation=binding.executor_incarnation,
            pool_id=binding.pool_id,
            pool_generation=binding.pool_generation,
            heartbeat_sequence=1,
            journal_sequence=2,
            journal_digest="a" * 64,
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
                heartbeat_sequence=2,
                journal_sequence=3,
                journal_digest="b" * 64,
                journal_checkpoint_sequence=1,
                journal_checkpoint_digest="a" * 64,
            ),
        )


async def test_inventory_requires_exact_journal_checkpoint_sequence(
    capacity_session: AsyncSession,
) -> None:
    store = CapacityExecutionStore()
    active, _allocation_epoch = await _active_plan(capacity_session)
    binding = executor_binding("gb10")
    await store.heartbeat_executor(
        capacity_session,
        ExecutableExecutorHeartbeatV2(
            execution=active,
            executor_id=binding.executor_id,
            executor_incarnation=binding.executor_incarnation,
            pool_id=binding.pool_id,
            pool_generation=binding.pool_generation,
            heartbeat_sequence=1,
            journal_sequence=2,
            journal_digest="a" * 64,
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
            inventory_sequence=1,
            journal_sequence=2,
            journal_digest="a" * 64,
            journal_checkpoint_sequence=2,
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
                inventory_sequence=2,
                journal_sequence=3,
                journal_digest="b" * 64,
                journal_checkpoint_sequence=1,
                journal_checkpoint_digest="a" * 64,
            ),
        )


async def test_inventory_advances_the_stored_journal_head(
    capacity_session: AsyncSession,
) -> None:
    store = CapacityExecutionStore()
    active, _allocation_epoch = await _active_plan(capacity_session)
    binding = executor_binding("gb10")
    await store.heartbeat_executor(
        capacity_session,
        ExecutableExecutorHeartbeatV2(
            execution=active,
            executor_id=binding.executor_id,
            executor_incarnation=binding.executor_incarnation,
            pool_id=binding.pool_id,
            pool_generation=binding.pool_generation,
            heartbeat_sequence=1,
            journal_sequence=1,
            journal_digest="a" * 64,
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
            inventory_sequence=1,
            journal_sequence=2,
            journal_digest="b" * 64,
            journal_checkpoint_sequence=1,
            journal_checkpoint_digest="a" * 64,
        ),
    )

    checkpoint = await store.executor_checkpoint(capacity_session, binding)

    assert checkpoint.journal_sequence == 2
    assert checkpoint.journal_digest == "b" * 64


async def test_heartbeat_rejects_changed_journal_digest_at_same_head_sequence(
    capacity_session: AsyncSession,
) -> None:
    store = CapacityExecutionStore()
    active, _allocation_epoch = await _active_plan(capacity_session)
    binding = executor_binding("gb10")
    await store.heartbeat_executor(
        capacity_session,
        ExecutableExecutorHeartbeatV2(
            execution=active,
            executor_id=binding.executor_id,
            executor_incarnation=binding.executor_incarnation,
            pool_id=binding.pool_id,
            pool_generation=binding.pool_generation,
            heartbeat_sequence=1,
            journal_sequence=2,
            journal_digest="a" * 64,
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
            heartbeat_sequence=2,
            journal_sequence=2,
            journal_digest="a" * 64,
            journal_checkpoint_sequence=2,
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
                heartbeat_sequence=3,
                journal_sequence=2,
                journal_digest="b" * 64,
                journal_checkpoint_sequence=2,
                journal_checkpoint_digest="a" * 64,
            ),
        )


async def test_inventory_rejects_changed_journal_digest_at_same_head_sequence(
    capacity_session: AsyncSession,
) -> None:
    store = CapacityExecutionStore()
    active, _allocation_epoch = await _active_plan(capacity_session)
    binding = executor_binding("gb10")
    await store.heartbeat_executor(
        capacity_session,
        ExecutableExecutorHeartbeatV2(
            execution=active,
            executor_id=binding.executor_id,
            executor_incarnation=binding.executor_incarnation,
            pool_id=binding.pool_id,
            pool_generation=binding.pool_generation,
            heartbeat_sequence=1,
            journal_sequence=2,
            journal_digest="a" * 64,
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
            inventory_sequence=1,
            journal_sequence=2,
            journal_digest="a" * 64,
            journal_checkpoint_sequence=2,
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
                inventory_sequence=2,
                journal_sequence=2,
                journal_digest="b" * 64,
                journal_checkpoint_sequence=2,
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


async def test_register_bootstrap_locks_executor_context_before_intent(
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

    await store.register_bootstrap(
        capacity_session,
        ExecutableBootstrapRegistrationV2(
            binding=intent,
            command_sequence=2,
            bootstrap_registration_epoch=1,
            bootstrap_evidence_sha256="8" * 64,
        ),
    )

    assert lock_order[:5] == [
        "authority",
        "epoch",
        "executor-registration",
        "executor-state",
        "intent",
    ]


@pytest.mark.parametrize("mode", ("increase-freeze", "drain-only"))
async def test_cleanup_bootstrap_registration_converges_for_accepted_intent_without_permit(
    capacity_session: AsyncSession,
    mode: str,
) -> None:
    store = CapacityExecutionStore()
    active, intent = await _accepted_binding(store, capacity_session)
    if mode == "increase-freeze":
        await capacity_session.execute(
            update(CapacityAuthorityState)
            .where(CapacityAuthorityState.singleton_id == 1)
            .values(increase_freeze=True, increase_freeze_reason="synthetic failure")
        )
    else:
        await CapacityManagementStore().register_writer(
            capacity_session,
            active.authority_incarnation,
            expected_epoch=active.writer_epoch,
        )

    result = await store.register_bootstrap(
        capacity_session,
        ExecutableBootstrapRegistrationV2(
            binding=intent,
            command_sequence=2,
            bootstrap_registration_epoch=1,
            bootstrap_evidence_sha256="8" * 64,
        ),
    )

    assert result.intent_id == intent.intent_id
    row = await _intent_row(capacity_session, intent.intent_id)
    assert row.state == "launch-ready"
    assert row.permit_id is None
    assert row.permit_consumed_at is None
    successor = await store.next_pool_work(capacity_session, executor_binding(intent.pool_id))
    assert isinstance(successor, ExecutableIntentCloseV2)
    assert successor.binding == intent


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
    expired_permit = store._new_permit(expired_row, datetime.now(UTC))
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
    target_permit = store._new_permit(target_row, datetime.now(UTC))
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
    inventory = ExecutableExecutorInventoryV2(
        execution=_inventory_execution(permit.binding),
        executor_id=permit.binding.executor_id,
        executor_incarnation=permit.binding.executor_incarnation,
        pool_id=permit.binding.pool_id,
        pool_generation=permit.binding.pool_generation,
        inventory_sequence=2,
        journal_sequence=0,
        journal_digest="0" * 64,
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
    inventory = ExecutableExecutorInventoryV2(
        execution=_inventory_execution(permit.binding),
        executor_id=permit.binding.executor_id,
        executor_incarnation=permit.binding.executor_incarnation,
        pool_id=permit.binding.pool_id,
        pool_generation=permit.binding.pool_generation,
        inventory_sequence=2,
        journal_sequence=0,
        journal_digest="0" * 64,
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
    second_permit = store._new_permit(second_row, datetime.now(UTC))
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
    result = await store.begin_intent_close(capacity_session, close)
    assert result.intent_id == close.binding.intent_id


async def test_drain_only_transition_emits_close_for_observed_worker(
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
        ExecutableExecutorInventoryV2(
            execution=ExecutionContextV2.model_validate(active_payload),
            executor_id=binding.executor_id,
            executor_incarnation=binding.executor_incarnation,
            pool_id=binding.pool_id,
            pool_generation=binding.pool_generation,
            inventory_sequence=2,
            journal_sequence=0,
            journal_digest="0" * 64,
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
        ExecutableExecutorInventoryV2(
            execution=_inventory_execution(permit.binding),
            executor_id=permit.binding.executor_id,
            executor_incarnation=permit.binding.executor_incarnation,
            pool_id=permit.binding.pool_id,
            pool_generation=permit.binding.pool_generation,
            inventory_sequence=2,
            journal_sequence=0,
            journal_digest="0" * 64,
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
        ExecutableExecutorInventoryV2(
            execution=_inventory_execution(permit.binding),
            executor_id=permit.binding.executor_id,
            executor_incarnation=permit.binding.executor_incarnation,
            pool_id=permit.binding.pool_id,
            pool_generation=permit.binding.pool_generation,
            inventory_sequence=2,
            journal_sequence=0,
            journal_digest="0" * 64,
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
        ExecutableExecutorInventoryV2(
            execution=_inventory_execution(permit.binding),
            executor_id=permit.binding.executor_id,
            executor_incarnation=permit.binding.executor_incarnation,
            pool_id=permit.binding.pool_id,
            pool_generation=permit.binding.pool_generation,
            inventory_sequence=3,
            journal_sequence=0,
            journal_digest="0" * 64,
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
        ExecutableExecutorInventoryV2(
            execution=_inventory_execution(permit.binding),
            executor_id=permit.binding.executor_id,
            executor_incarnation=permit.binding.executor_incarnation,
            pool_id=permit.binding.pool_id,
            pool_generation=permit.binding.pool_generation,
            inventory_sequence=3,
            journal_sequence=0,
            journal_digest="0" * 64,
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
        ExecutableExecutorInventoryV2(
            execution=_inventory_execution(permit.binding),
            executor_id=permit.binding.executor_id,
            executor_incarnation=permit.binding.executor_incarnation,
            pool_id=permit.binding.pool_id,
            pool_generation=permit.binding.pool_generation,
            inventory_sequence=2,
            journal_sequence=0,
            journal_digest="0" * 64,
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
        ExecutableExecutorInventoryV2(
            execution=_inventory_execution(permit.binding),
            executor_id=permit.binding.executor_id,
            executor_incarnation=permit.binding.executor_incarnation,
            pool_id=permit.binding.pool_id,
            pool_generation=permit.binding.pool_generation,
            inventory_sequence=2,
            journal_sequence=0,
            journal_digest="0" * 64,
            records=(_inventory_record(permit.binding, physical_identity="job-123"),),
        ),
    )
    await store.ingest_executor_inventory(
        capacity_session,
        ExecutableExecutorInventoryV2(
            execution=_inventory_execution(permit.binding),
            executor_id=permit.binding.executor_id,
            executor_incarnation=permit.binding.executor_incarnation,
            pool_id=permit.binding.pool_id,
            pool_generation=permit.binding.pool_generation,
            inventory_sequence=3,
            journal_sequence=0,
            journal_digest="0" * 64,
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
    inventory = ExecutableExecutorInventoryV2(
        execution=ExecutionContextV2.model_validate(active_payload),
        executor_id=binding.executor_id,
        executor_incarnation=binding.executor_incarnation,
        pool_id=binding.pool_id,
        pool_generation=binding.pool_generation,
        inventory_sequence=2,
        journal_sequence=0,
        journal_digest="0" * 64,
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
        ExecutableExecutorInventoryV2(
            execution=_inventory_execution(permit.binding),
            executor_id=permit.binding.executor_id,
            executor_incarnation=permit.binding.executor_incarnation,
            pool_id=permit.binding.pool_id,
            pool_generation=permit.binding.pool_generation,
            inventory_sequence=2,
            journal_sequence=0,
            journal_digest="0" * 64,
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
