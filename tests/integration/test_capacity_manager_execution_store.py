"""Transactional coverage for the executable-v2 capacity work ledger."""

from __future__ import annotations

import base64
import json
from datetime import UTC, datetime, timedelta
from uuid import UUID

import pytest
from sqlalchemy import select, text, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from loom_capacity_executor.keys import ExecutorOwnershipKey
from loom_capacity_executor.launch_renderer import (
    OperatorLaunchProfileV2,
    OperatorResourceDomainV2,
    TrustedLaunchContextV2,
    canonical_launch_policy_digest,
    render_signed_launch,
)
from loom_capacity_executor.slurm_contracts import SlurmExecutableIdentityV2
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
    ExecutionDrainV2,
    ExecutionPreparationPolicyV2,
    ExecutionRetirementExecutorCheckpointV2,
    ExecutionRetirementV2,
    PoolControllerAuthorityV2,
    SignedExecutableOwnershipProofV2,
    canonical_executable_bytes,
    canonical_executable_digest,
)
from loom_capacity_manager.execution_store import CapacityExecutionStore
from loom_capacity_manager.models import (
    CapacityAllocationEpoch,
    CapacityAuthorityState,
    CapacityExecutableExecutorState,
    CapacityExecutableIntent,
    CapacityExecutionExecutor,
    CapacityPool,
    CapacityPoolObservation,
    CapacityWorkerProfile,
)
from loom_capacity_manager.ownership import OwnershipKeyring, public_key_fingerprint
from loom_capacity_manager.reconciler import reconcile_shadow_once
from loom_capacity_manager.store import CapacityManagementStore, ExecutionConflictError
from tests.capacity_execution_fixtures import (
    EXECUTOR_KEYS,
    TRUSTED_RELEASE,
    execution_policy,
    executor_binding,
    register_execution_executors,
    setup_execution,
)
from tests.capacity_fixtures import (
    demand_snapshot,
    pool_observation,
    profile_reference,
    resource_vector,
    shape,
)


def test_execution_store_is_a_distinct_v2_ledger() -> None:
    """Removing the dedicated executable store must break v2 queue ownership."""

    assert CapacityExecutionStore.__module__ == "loom_capacity_manager.execution_store"


async def _active_plan(
    session: AsyncSession,
    *,
    policy: ExecutionPreparationPolicyV2 | None = None,
):  # type: ignore[no-untyped-def]
    fixture = await setup_execution(
        session,
        execution_policy=execution_policy() if policy is None else policy,
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
        fixture.request,
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
            executable_new_capacity_ceiling=1,
            executable_new_capacity_rate_per_minute=1,
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


def _drain_request(active):  # type: ignore[no-untyped-def]
    return ExecutionDrainV2(
        authority_incarnation=active.authority_incarnation,
        expected_writer_epoch=active.writer_epoch,
        execution_epoch=active.execution_epoch,
        execution_manifest_sha256=active.execution_manifest_sha256,
        expected_executable_new_capacity_ceiling=(active.executable_new_capacity_ceiling),
        expected_executable_new_capacity_rate_per_minute=(
            active.executable_new_capacity_rate_per_minute
        ),
    )


async def _drain_active(
    session: AsyncSession,
    active,  # type: ignore[no-untyped-def]
) -> tuple[CapacityManagementStore, ExecutionContextV2]:
    manager = CapacityManagementStore(execution_policy=execution_policy())
    drained = await manager.begin_execution_drain(
        session,
        _drain_request(active),
        actor="activation-operator",
        idempotency_key=UUID(int=12101),
    )
    return manager, drained


async def _publish_pool_retirement_evidence(
    store: CapacityExecutionStore,
    session: AsyncSession,
    drained: ExecutionContextV2,
    *,
    pool_id: str,
    records: tuple[ExecutableInventoryRecordV2, ...] = (),
    later_heartbeat: bool = True,
) -> ExecutionRetirementExecutorCheckpointV2:
    binding = executor_binding(pool_id)
    state = (
        await session.execute(
            select(CapacityExecutableExecutorState).where(
                CapacityExecutableExecutorState.execution_epoch == drained.execution_epoch,
                CapacityExecutableExecutorState.pool_id == pool_id,
            )
        )
    ).scalar_one_or_none()
    heartbeat_sequence = 1 if state is None else state.heartbeat_high_water + 1
    inventory_sequence = 1 if state is None else state.inventory_high_water + 1
    journal_sequence = 0 if state is None else state.journal_high_water
    journal_digest = "0" * 64 if state is None else state.journal_digest
    common = {
        "execution": drained,
        "executor_id": binding.executor_id,
        "executor_incarnation": binding.executor_incarnation,
        "pool_id": binding.pool_id,
        "pool_generation": binding.pool_generation,
    }
    await store.heartbeat_executor(
        session,
        ExecutableExecutorHeartbeatV2(
            **common,
            heartbeat_sequence=heartbeat_sequence,
            journal_sequence=journal_sequence,
            journal_digest=journal_digest,
            journal_checkpoint_sequence=journal_sequence,
            journal_checkpoint_digest=journal_digest,
        ),
    )
    inventory = ExecutableExecutorInventoryV2(
        **common,
        inventory_sequence=inventory_sequence,
        journal_sequence=journal_sequence,
        journal_digest=journal_digest,
        journal_checkpoint_sequence=journal_sequence,
        journal_checkpoint_digest=journal_digest,
        records=records,
    )
    await store.ingest_executor_inventory(session, inventory)
    if later_heartbeat:
        heartbeat_sequence += 1
        await store.heartbeat_executor(
            session,
            ExecutableExecutorHeartbeatV2(
                **common,
                heartbeat_sequence=heartbeat_sequence,
                journal_sequence=journal_sequence,
                journal_digest=journal_digest,
                journal_checkpoint_sequence=journal_sequence,
                journal_checkpoint_digest=journal_digest,
            ),
        )
    state = (
        await session.execute(
            select(CapacityExecutableExecutorState).where(
                CapacityExecutableExecutorState.execution_epoch == drained.execution_epoch,
                CapacityExecutableExecutorState.pool_id == pool_id,
            )
        )
    ).scalar_one()
    return ExecutionRetirementExecutorCheckpointV2(
        executor_id=state.executor_id,
        executor_incarnation=state.executor_incarnation,
        pool_id=state.pool_id,
        pool_generation=state.pool_generation,
        heartbeat_sequence=state.heartbeat_high_water,
        command_sequence=state.command_high_water,
        journal_sequence=state.journal_high_water,
        journal_digest=state.journal_digest,
        inventory_sequence=state.inventory_high_water,
        inventory_digest=canonical_executable_digest(inventory),
    )


async def _publish_retirement_evidence(
    store: CapacityExecutionStore,
    session: AsyncSession,
    drained: ExecutionContextV2,
    *,
    records_by_pool: dict[str, tuple[ExecutableInventoryRecordV2, ...]] | None = None,
) -> tuple[ExecutionRetirementExecutorCheckpointV2, ...]:
    records_by_pool = records_by_pool or {}
    return tuple(
        [
            await _publish_pool_retirement_evidence(
                store,
                session,
                drained,
                pool_id=pool_id,
                records=records_by_pool.get(pool_id, ()),
            )
            for pool_id in ("gb10", "oldlab")
        ]
    )


def _retirement_request(
    drained: ExecutionContextV2,
    checkpoints: tuple[ExecutionRetirementExecutorCheckpointV2, ...],
) -> ExecutionRetirementV2:
    return ExecutionRetirementV2(
        authority_incarnation=drained.authority_incarnation,
        expected_writer_epoch=drained.writer_epoch,
        execution_epoch=drained.execution_epoch,
        execution_manifest_sha256=drained.execution_manifest_sha256,
        executor_checkpoints=checkpoints,
    )


async def _proposed_intent(
    store: CapacityExecutionStore,
    session: AsyncSession,
    active,  # type: ignore[no-untyped-def]
) -> tuple[ExecutableReservationProposalV2, CapacityExecutableIntent]:
    await _heartbeat(store, session, active, pool_id="gb10")
    proposal = await store.next_pool_work(session, executor_binding("gb10"))
    assert isinstance(proposal, ExecutableReservationProposalV2)
    intent = (
        await session.execute(
            select(CapacityExecutableIntent).where(
                CapacityExecutableIntent.execution_epoch == active.execution_epoch
            )
        )
    ).scalar_one()
    return proposal, intent


def _signed_inventory_record(
    binding: ExecutableIntentBindingV2,
    *,
    state: str,
    valid_signature: bool = True,
) -> ExecutableInventoryRecordV2:
    metadata = ExecutableOwnershipMetadataV2(
        binding=binding,
        controller_authority_sha256="c" * 64,
        trusted_launcher_sha256="7" * 64,
        slurm_cluster="gb10-controller",
        submitter_identity="loom",
        association="loom",
        submitted_at=datetime.now(UTC),
    )
    signature = (
        EXECUTOR_KEYS["gb10"].sign(canonical_executable_bytes(metadata))
        if valid_signature
        else b"\0" * 64
    )
    return ExecutableInventoryRecordV2(
        physical_identity="job-final-gb10",
        physical_kind="slurm-job",
        authority_scope="dedicated-loom-association",
        state=state,
        resources=binding.resources,
        node_ids=binding.node_ids,
        controller_evidence_sha256="9" * 64,
        ownership_proof=SignedExecutableOwnershipProofV2(
            metadata=metadata,
            signing_key_id="gb10-key",
            signature_base64=base64.b64encode(signature).decode("ascii"),
        ),
        terminal_evidence_sha256="a" * 64 if state == "terminal" else None,
    )


async def _launch_ready(
    store: CapacityExecutionStore,
    session: AsyncSession,
    *,
    policy: ExecutionPreparationPolicyV2 | None = None,
    controller_authority_sha256: str | None = None,
):
    active, _allocation_epoch = await _active_plan(session, policy=policy)
    binding = executor_binding(
        "gb10",
        controller_authority_sha256=controller_authority_sha256,
    )
    await _heartbeat(store, session, active, pool_id="gb10")
    proposal = await store.next_pool_work(session, binding)
    assert isinstance(proposal, ExecutableReservationProposalV2)
    accepted = await store.accept_reservation(
        session,
        ExecutableReservationAcceptanceV2(
            execution=proposal.execution,
            tranche_id=proposal.tranche_id,
            proposal_digest=store.contract_digest(proposal),
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


async def test_real_manager_profile_digest_is_not_reinterpreted_as_launch_policy(
    capacity_session: AsyncSession,
) -> None:
    manager_profile = profile_reference()
    manager_shape = manager_profile.worker_shapes[0]
    unsigned_profile = OperatorLaunchProfileV2(
        pool_id=manager_profile.pool_id,
        pool_generation=manager_profile.pool_generation,
        profile_id=manager_shape.shape_id,
        profile_generation=manager_profile.profile_generation,
        profile_digest=manager_profile.profile_digest,
        shape_id=manager_shape.shape_id,
        concurrency_slots=manager_shape.concurrency_slots,
        controller_authority_sha256="0" * 64,
        slurm_cluster="gb10-controller",
        controller_host="ctl.gb10.internal",
        partition="loom",
        association="loom",
        submitter="loom",
        qos="loom",
        job_name_prefix="loom-worker",
        resource_domains=(
            OperatorResourceDomainV2(
                domain_id="gb10-arm",
                node_ids=("gb10-node",),
                features=("arm64",),
            ),
        ),
        cpus=1,
        resources=manager_shape.total_resources,
        time_limit_seconds=3_600,
        launcher=SlurmExecutableIdentityV2(
            path="/opt/loom/bin/trusted-worker-launcher",
            sha256="a" * 64,
            owner_uid=0,
        ),
        trusted_launcher_release_sha256=TRUSTED_RELEASE,
        image_digest="registry.internal/loom/worker@sha256:" + "b" * 64,
    )
    controller_digest = canonical_launch_policy_digest(unsigned_profile)
    profile = unsigned_profile.model_copy(update={"controller_authority_sha256": controller_digest})
    policy = execution_policy(controller_digests={"gb10": controller_digest})
    store = CapacityExecutionStore()
    permit = await _launch_ready(
        store,
        capacity_session,
        policy=policy,
        controller_authority_sha256=controller_digest,
    )
    binding = permit.binding
    persisted_profile = (
        await capacity_session.execute(
            select(CapacityWorkerProfile).where(
                CapacityWorkerProfile.subject_id == binding.subject_id,
                CapacityWorkerProfile.subject_incarnation == binding.subject_incarnation,
                CapacityWorkerProfile.deployment_generation == binding.deployment_generation,
                CapacityWorkerProfile.pool_id == binding.pool_id,
                CapacityWorkerProfile.profile_generation == binding.profile_generation,
            )
        )
    ).scalar_one()
    registered_executor = (
        await capacity_session.execute(
            select(CapacityExecutionExecutor).where(
                CapacityExecutionExecutor.execution_epoch == binding.execution.execution_epoch,
                CapacityExecutionExecutor.pool_id == binding.pool_id,
            )
        )
    ).scalar_one()
    private_key = EXECUTOR_KEYS["gb10"]
    context = TrustedLaunchContextV2(
        binding=binding,
        profile=profile,
        controller_authority=PoolControllerAuthorityV2(
            pool_id="gb10",
            controller_authority_sha256=(registered_executor.controller_authority_sha256),
        ),
        ownership_key=ExecutorOwnershipKey(
            signing_key_id="gb10-key",
            private_key=private_key,
            public_key_sha256=public_key_fingerprint(private_key.public_key()),
        ),
        submitted_at=datetime.now(UTC),
    )

    rendered = render_signed_launch(context)

    assert persisted_profile.profile_digest == binding.profile_digest
    assert profile.profile_digest == binding.profile_digest
    assert profile.resources == binding.resources
    assert profile.resource_domains[0].node_ids == binding.node_ids
    assert controller_digest != binding.profile_digest
    assert registered_executor.controller_authority_sha256 == controller_digest
    assert rendered.ownership_proof.metadata.binding == binding
    assert rendered.ownership_proof.metadata.controller_authority_sha256 == controller_digest


async def test_queue_never_crosses_pool(capacity_session: AsyncSession) -> None:
    store = CapacityExecutionStore()
    active, _allocation_epoch = await _active_plan(capacity_session)
    await _heartbeat(store, capacity_session, active, pool_id="gb10")
    await _heartbeat(store, capacity_session, active, pool_id="oldlab")

    work = await store.next_pool_work(capacity_session, executor_binding("oldlab"))

    assert work is None or work.pool_id == "oldlab"


async def test_acceptance_cannot_claim_another_executor_proposal(
    capacity_session: AsyncSession,
) -> None:
    store = CapacityExecutionStore()
    active, _allocation_epoch = await _active_plan(capacity_session)
    gb10 = executor_binding("gb10")
    oldlab = executor_binding("oldlab")
    await _heartbeat(store, capacity_session, active, pool_id="gb10")
    proposal = await store.next_pool_work(capacity_session, gb10)
    assert isinstance(proposal, ExecutableReservationProposalV2)

    with pytest.raises(ExecutionConflictError, match="executor binding"):
        await store.accept_reservation(
            capacity_session,
            ExecutableReservationAcceptanceV2(
                execution=proposal.execution,
                tranche_id=proposal.tranche_id,
                proposal_digest=store.contract_digest(proposal),
                pool_generation=oldlab.pool_generation,
                executor_id=oldlab.executor_id,
                executor_incarnation=oldlab.executor_incarnation,
                command_sequence=1,
            ),
        )


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


async def test_ceiling_zero_blocks_increase_work(capacity_session: AsyncSession) -> None:
    store = CapacityExecutionStore()

    assert await store.next_pool_work(capacity_session, executor_binding("gb10")) is None


async def test_permit_consumption_rechecks_execution_fence(
    capacity_session: AsyncSession,
) -> None:
    store = CapacityExecutionStore()
    permit = await _launch_ready(store, capacity_session)
    await capacity_session.execute(
        update(CapacityAuthorityState)
        .where(CapacityAuthorityState.singleton_id == 1)
        .values(execution_state="drain-only", executable_new_capacity_ceiling=0)
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


async def test_expired_permit_is_reissued_without_deadlocking_queue(
    capacity_session: AsyncSession,
) -> None:
    store = CapacityExecutionStore()
    permit = await _launch_ready(store, capacity_session)
    await capacity_session.execute(
        update(CapacityExecutableIntent)
        .where(CapacityExecutableIntent.intent_id == permit.binding.intent_id)
        .values(permit_expires_at=datetime.now(UTC) - timedelta(seconds=1))
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
):  # type: ignore[no-untyped-def]
    permit = await _launch_ready(store, session)
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
    intent.binding_digest = store.contract_digest(binding)
    intent.binding_payload = binding.model_dump(mode="json", exclude_none=False)
    intent.permit_digest = store.contract_digest(changed)
    intent.permit_payload = changed.model_dump(mode="json", exclude_none=False)
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


async def test_increase_freeze_turns_accepted_work_into_close(
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


# Production break caught: once a signed physical worker is observed, entering
# drain-only authority could strand it forever because the pool queue emitted no
# close work and the close transition rejected the observed state.
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
        trusted_launcher_sha256="7" * 64,
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


async def test_release_requires_matching_protected_and_physical_terminal_evidence(
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
        trusted_launcher_sha256="7" * 64,
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
    released = await store.release_shapes(capacity_session, release)
    assert released.released_shape_ids == (binding.shape_instance_id,)


@pytest.mark.parametrize(
    "intent_state",
    (
        "proposed",
        "accepted",
        "launch-ready",
        "permitted",
        "submitting-unknown",
        "bound",
        "observed",
        "terminal",
        "closing",
        "quarantined",
    ),
)
async def test_retirement_rejects_every_nonreleased_intent_without_reusing_charge(
    capacity_session: AsyncSession,
    intent_state: str,
) -> None:
    """No retained executable intent state may make its slot reusable."""

    store = CapacityExecutionStore()
    active, _allocation_epoch = await _active_plan(capacity_session)
    _proposal, intent = await _proposed_intent(store, capacity_session, active)
    intent.state = intent_state
    await capacity_session.flush()
    manager, drained = await _drain_active(capacity_session, active)
    checkpoints = await _publish_retirement_evidence(
        store,
        capacity_session,
        drained,
    )

    with pytest.raises(ExecutionConflictError):
        await manager.retire_execution_epoch(
            capacity_session,
            _retirement_request(drained, checkpoints),
            actor="activation-operator",
            idempotency_key=UUID(int=12102),
        )

    await capacity_session.refresh(intent)
    authority = (await capacity_session.execute(select(CapacityAuthorityState))).scalar_one()
    assert (authority.execution_state, authority.executable_new_capacity_ceiling) == (
        "drain-only",
        0,
    )
    assert intent.state == intent_state
    assert intent.binding_payload["concurrency_slots"] == 1


@pytest.mark.parametrize(
    "blocker",
    (
        "missing-executor",
        "stale-lease",
        "missing-inventory",
        "old-inventory",
        "heartbeat-before-inventory",
        "fenced",
        "equivocal",
    ),
)
async def test_retirement_rejects_incomplete_or_stale_executor_evidence_atomically(
    capacity_session: AsyncSession,
    blocker: str,
) -> None:
    """One unavailable final executor must leave exact drain-only authority intact."""

    store = CapacityExecutionStore()
    active, _allocation_epoch = await _active_plan(capacity_session)
    manager, drained = await _drain_active(capacity_session, active)
    checkpoints = await _publish_retirement_evidence(store, capacity_session, drained)
    state = (
        await capacity_session.execute(
            select(CapacityExecutableExecutorState).where(
                CapacityExecutableExecutorState.pool_id == "gb10"
            )
        )
    ).scalar_one()
    if blocker == "missing-executor":
        await capacity_session.delete(state)
    elif blocker == "stale-lease":
        state.lease_expires_at = datetime.now(UTC) - timedelta(seconds=1)
    elif blocker == "missing-inventory":
        state.inventory_high_water = 0
        state.last_inventory_digest = None
        state.inventory_payload = None
        state.last_inventory_at = None
        state.retirement_safe = False
        state.retirement_inventory_digest = None
    elif blocker == "old-inventory":
        state.last_inventory_at = datetime.now(UTC) - timedelta(minutes=10)
    elif blocker == "heartbeat-before-inventory":
        assert state.last_inventory_at is not None
        state.last_heartbeat_at = state.last_inventory_at - timedelta(seconds=1)
    else:
        state.state = blocker
    await capacity_session.flush()

    with pytest.raises(ExecutionConflictError):
        await manager.retire_execution_epoch(
            capacity_session,
            _retirement_request(drained, checkpoints),
            actor="activation-operator",
            idempotency_key=UUID(int=12103),
        )

    authority = (await capacity_session.execute(select(CapacityAuthorityState))).scalar_one()
    assert (
        authority.execution_state,
        authority.executable_new_capacity_ceiling,
        authority.increase_freeze,
    ) == ("drain-only", 0, True)


@pytest.mark.parametrize(
    "checkpoint_change",
    (
        "executor",
        "heartbeat",
        "command",
        "journal",
        "inventory-sequence",
        "inventory-digest",
    ),
)
async def test_retirement_rejects_any_changed_final_checkpoint_binding(
    capacity_session: AsyncSession,
    checkpoint_change: str,
) -> None:
    """A request cannot substitute any executor, command, journal, or inventory head."""

    store = CapacityExecutionStore()
    active, _allocation_epoch = await _active_plan(capacity_session)
    manager, drained = await _drain_active(capacity_session, active)
    checkpoints = await _publish_retirement_evidence(store, capacity_session, drained)
    changed_values: dict[str, object]
    if checkpoint_change == "executor":
        changed_values = {
            "executor_id": "changed-gb10-executor",
            "executor_incarnation": UUID(int=12109),
        }
    elif checkpoint_change == "heartbeat":
        changed_values = {"heartbeat_sequence": checkpoints[0].heartbeat_sequence + 1}
    elif checkpoint_change == "command":
        changed_values = {"command_sequence": checkpoints[0].command_sequence + 1}
    elif checkpoint_change == "journal":
        changed_values = {"journal_sequence": 1, "journal_digest": "f" * 64}
    elif checkpoint_change == "inventory-sequence":
        changed_values = {"inventory_sequence": checkpoints[0].inventory_sequence + 1}
    else:
        changed_values = {"inventory_digest": "f" * 64}
    changed_checkpoint = checkpoints[0].model_copy(update=changed_values)
    changed_request = _retirement_request(
        drained,
        (changed_checkpoint, checkpoints[1]),
    )

    with pytest.raises(ExecutionConflictError):
        await manager.retire_execution_epoch(
            capacity_session,
            changed_request,
            actor="activation-operator",
            idempotency_key=UUID(int=12104),
        )

    authority = (await capacity_session.execute(select(CapacityAuthorityState))).scalar_one()
    assert (authority.execution_state, authority.executable_new_capacity_ceiling) == (
        "drain-only",
        0,
    )


async def test_proofless_loom_scoped_final_record_blocks_retirement(
    capacity_session: AsyncSession,
) -> None:
    """A Loom-looking physical record without exact ownership proof stays ambiguous."""

    store = CapacityExecutionStore()
    active, _allocation_epoch = await _active_plan(capacity_session)
    manager, drained = await _drain_active(capacity_session, active)
    proofless = ExecutableInventoryRecordV2(
        physical_identity="unproved-loom-job",
        physical_kind="slurm-job",
        authority_scope="registered-loom",
        state="terminal",
        resources=resource_vector(),
        node_ids=("gb10-node",),
        controller_evidence_sha256="9" * 64,
        terminal_evidence_sha256="a" * 64,
    )
    checkpoints = await _publish_retirement_evidence(
        store,
        capacity_session,
        drained,
        records_by_pool={"gb10": (proofless,)},
    )
    state = (
        await capacity_session.execute(
            select(CapacityExecutableExecutorState).where(
                CapacityExecutableExecutorState.pool_id == "gb10"
            )
        )
    ).scalar_one()

    assert state.retirement_safe is False
    assert state.retirement_inventory_digest is None
    with pytest.raises(ExecutionConflictError):
        await manager.retire_execution_epoch(
            capacity_session,
            _retirement_request(drained, checkpoints),
            actor="activation-operator",
            idempotency_key=UUID(int=12105),
        )


@pytest.mark.parametrize(
    ("valid_signature", "physical_state"),
    ((False, "terminal"), (True, "active")),
)
async def test_invalid_or_nonterminal_loom_record_blocks_without_unreleasing_intent(
    capacity_session: AsyncSession,
    valid_signature: bool,
    physical_state: str,
) -> None:
    """Ambiguous retained Loom work blocks but never regresses a released ledger row."""

    store = CapacityExecutionStore(
        ownership_keyring=OwnershipKeyring({"gb10-key": EXECUTOR_KEYS["gb10"].public_key()})
    )
    active, _allocation_epoch = await _active_plan(capacity_session)
    _proposal, intent = await _proposed_intent(store, capacity_session, active)
    binding = ExecutableIntentBindingV2.model_validate_json(json.dumps(intent.binding_payload))
    intent.state = "released"
    intent.released_at = datetime.now(UTC)
    await capacity_session.flush()
    manager, drained = await _drain_active(capacity_session, active)
    record = _signed_inventory_record(
        binding,
        state=physical_state,
        valid_signature=valid_signature,
    )
    checkpoints = await _publish_retirement_evidence(
        store,
        capacity_session,
        drained,
        records_by_pool={"gb10": (record,)},
    )

    await capacity_session.refresh(intent)
    assert intent.state == "released"
    gb10_state = (
        await capacity_session.execute(
            select(CapacityExecutableExecutorState).where(
                CapacityExecutableExecutorState.pool_id == "gb10"
            )
        )
    ).scalar_one()
    assert gb10_state.retirement_safe is False
    assert gb10_state.retirement_inventory_digest is None
    with pytest.raises(ExecutionConflictError):
        await manager.retire_execution_epoch(
            capacity_session,
            _retirement_request(drained, checkpoints),
            actor="activation-operator",
            idempotency_key=UUID(int=12106),
        )


async def test_later_terminal_inventory_keeps_released_intent_and_can_retire(
    capacity_session: AsyncSession,
) -> None:
    """A final terminal observation must not regress release or strand authority."""

    store = CapacityExecutionStore(
        ownership_keyring=OwnershipKeyring({"gb10-key": EXECUTOR_KEYS["gb10"].public_key()})
    )
    active, _allocation_epoch = await _active_plan(capacity_session)
    _proposal, intent = await _proposed_intent(store, capacity_session, active)
    binding = ExecutableIntentBindingV2.model_validate_json(json.dumps(intent.binding_payload))
    intent.state = "released"
    intent.released_at = datetime.now(UTC)
    await capacity_session.flush()
    manager, drained = await _drain_active(capacity_session, active)
    terminal = _signed_inventory_record(binding, state="terminal")
    checkpoints = await _publish_retirement_evidence(
        store,
        capacity_session,
        drained,
        records_by_pool={"gb10": (terminal,)},
    )

    retired = await manager.retire_execution_epoch(
        capacity_session,
        _retirement_request(drained, checkpoints),
        actor="activation-operator",
        idempotency_key=UUID(int=12107),
    )

    await capacity_session.refresh(intent)
    assert intent.state == "released"
    assert retired.execution_epoch == drained.execution_epoch


async def test_foreign_record_is_preserved_and_does_not_establish_loom_ownership(
    capacity_session: AsyncSession,
) -> None:
    """A foreign job remains foreign and does not itself block safe retirement."""

    store = CapacityExecutionStore()
    active, _allocation_epoch = await _active_plan(capacity_session)
    manager, drained = await _drain_active(capacity_session, active)
    foreign = ExecutableInventoryRecordV2(
        physical_identity="foreign-job-42",
        physical_kind="slurm-job",
        authority_scope="foreign",
        state="active",
        resources=resource_vector(),
        node_ids=("gb10-node",),
        controller_evidence_sha256="9" * 64,
    )
    checkpoints = await _publish_retirement_evidence(
        store,
        capacity_session,
        drained,
        records_by_pool={"gb10": (foreign,)},
    )
    gb10_state = (
        await capacity_session.execute(
            select(CapacityExecutableExecutorState).where(
                CapacityExecutableExecutorState.pool_id == "gb10"
            )
        )
    ).scalar_one()

    assert gb10_state.inventory_payload["records"][0]["authority_scope"] == "foreign"
    assert gb10_state.inventory_payload["records"][0]["physical_identity"] == "foreign-job-42"
    assert gb10_state.retirement_safe is True
    await manager.retire_execution_epoch(
        capacity_session,
        _retirement_request(drained, checkpoints),
        actor="activation-operator",
        idempotency_key=UUID(int=12108),
    )


async def test_empty_complete_inventory_is_unsafe_with_a_retained_released_intent(
    capacity_session: AsyncSession,
) -> None:
    """An empty final inventory cannot stand in for one retained intent's evidence."""

    store = CapacityExecutionStore()
    active, _allocation_epoch = await _active_plan(capacity_session)
    _proposal, intent = await _proposed_intent(store, capacity_session, active)
    intent.state = "released"
    intent.released_at = datetime.now(UTC)
    await capacity_session.flush()
    _manager, drained = await _drain_active(capacity_session, active)

    await _publish_pool_retirement_evidence(
        store,
        capacity_session,
        drained,
        pool_id="gb10",
    )
    state = (
        await capacity_session.execute(
            select(CapacityExecutableExecutorState).where(
                CapacityExecutableExecutorState.pool_id == "gb10"
            )
        )
    ).scalar_one()

    assert state.retirement_safe is False
    assert state.retirement_inventory_digest is None


async def test_duplicate_exact_records_for_one_intent_are_retirement_unsafe(
    capacity_session: AsyncSession,
) -> None:
    """Two physical records claiming one intent are ambiguous final evidence."""

    store = CapacityExecutionStore(
        ownership_keyring=OwnershipKeyring({"gb10-key": EXECUTOR_KEYS["gb10"].public_key()})
    )
    active, _allocation_epoch = await _active_plan(capacity_session)
    _proposal, intent = await _proposed_intent(store, capacity_session, active)
    binding = ExecutableIntentBindingV2.model_validate_json(json.dumps(intent.binding_payload))
    intent.state = "released"
    intent.released_at = datetime.now(UTC)
    await capacity_session.flush()
    _manager, drained = await _drain_active(capacity_session, active)
    first = _signed_inventory_record(binding, state="terminal")
    duplicate = first.model_copy(
        update={
            "physical_identity": "job-final-gb10-duplicate",
            "terminal_evidence_sha256": "b" * 64,
        }
    )

    await _publish_pool_retirement_evidence(
        store,
        capacity_session,
        drained,
        pool_id="gb10",
        records=(first, duplicate),
    )
    state = (
        await capacity_session.execute(
            select(CapacityExecutableExecutorState).where(
                CapacityExecutableExecutorState.pool_id == "gb10"
            )
        )
    ).scalar_one()

    assert state.retirement_safe is False
    assert state.retirement_inventory_digest is None


async def test_retirement_needs_fresh_postrelease_inventory_and_later_pool_heartbeat(
    capacity_session: AsyncSession,
) -> None:
    """An earlier inventory or heartbeat cannot be reused as final release evidence."""

    store = CapacityExecutionStore(
        ownership_keyring=OwnershipKeyring({"gb10-key": EXECUTOR_KEYS["gb10"].public_key()})
    )
    active, _allocation_epoch = await _active_plan(capacity_session)
    _proposal, intent = await _proposed_intent(store, capacity_session, active)
    binding = ExecutableIntentBindingV2.model_validate_json(json.dumps(intent.binding_payload))
    intent.state = "released"
    intent.released_at = datetime.now(UTC)
    await capacity_session.flush()
    manager, drained = await _drain_active(capacity_session, active)
    oldlab = await _publish_pool_retirement_evidence(
        store,
        capacity_session,
        drained,
        pool_id="oldlab",
    )
    gb10_state = (
        await capacity_session.execute(
            select(CapacityExecutableExecutorState).where(
                CapacityExecutableExecutorState.pool_id == "gb10"
            )
        )
    ).scalar_one()
    await store.heartbeat_executor(
        capacity_session,
        ExecutableExecutorHeartbeatV2(
            execution=drained,
            executor_id=gb10_state.executor_id,
            executor_incarnation=gb10_state.executor_incarnation,
            pool_id=gb10_state.pool_id,
            pool_generation=gb10_state.pool_generation,
            heartbeat_sequence=gb10_state.heartbeat_high_water + 1,
            journal_sequence=gb10_state.journal_high_water,
            journal_digest=gb10_state.journal_digest,
            journal_checkpoint_sequence=gb10_state.journal_high_water,
            journal_checkpoint_digest=gb10_state.journal_digest,
        ),
    )
    await capacity_session.refresh(gb10_state)
    stale_gb10 = ExecutionRetirementExecutorCheckpointV2(
        executor_id=gb10_state.executor_id,
        executor_incarnation=gb10_state.executor_incarnation,
        pool_id=gb10_state.pool_id,
        pool_generation=gb10_state.pool_generation,
        heartbeat_sequence=gb10_state.heartbeat_high_water,
        command_sequence=gb10_state.command_high_water,
        journal_sequence=gb10_state.journal_high_water,
        journal_digest=gb10_state.journal_digest,
        inventory_sequence=gb10_state.inventory_high_water,
        inventory_digest=gb10_state.last_inventory_digest,
    )
    with pytest.raises(ExecutionConflictError):
        await manager.retire_execution_epoch(
            capacity_session,
            _retirement_request(drained, (stale_gb10, oldlab)),
            actor="activation-operator",
            idempotency_key=UUID(int=12110),
        )

    terminal = _signed_inventory_record(binding, state="terminal")
    gb10 = await _publish_pool_retirement_evidence(
        store,
        capacity_session,
        drained,
        pool_id="gb10",
        records=(terminal,),
        later_heartbeat=False,
    )
    with pytest.raises(ExecutionConflictError):
        await manager.retire_execution_epoch(
            capacity_session,
            _retirement_request(drained, (gb10, oldlab)),
            actor="activation-operator",
            idempotency_key=UUID(int=12111),
        )
    executor = executor_binding("gb10")
    await store.heartbeat_executor(
        capacity_session,
        ExecutableExecutorHeartbeatV2(
            execution=drained,
            executor_id=executor.executor_id,
            executor_incarnation=executor.executor_incarnation,
            pool_id=executor.pool_id,
            pool_generation=executor.pool_generation,
            heartbeat_sequence=gb10.heartbeat_sequence + 1,
            journal_sequence=gb10.journal_sequence,
            journal_digest=gb10.journal_digest,
            journal_checkpoint_sequence=gb10.journal_sequence,
            journal_checkpoint_digest=gb10.journal_digest,
        ),
    )
    fresh_gb10 = gb10.model_copy(update={"heartbeat_sequence": gb10.heartbeat_sequence + 1})
    retired = await manager.retire_execution_epoch(
        capacity_session,
        _retirement_request(drained, (fresh_gb10, oldlab)),
        actor="activation-operator",
        idempotency_key=UUID(int=12112),
    )
    assert retired.execution_epoch == drained.execution_epoch
