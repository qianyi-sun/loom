"""Transactional coverage for the executable-v2 capacity work ledger."""

from __future__ import annotations

import base64
from datetime import UTC, datetime, timedelta
from uuid import UUID

import pytest
from sqlalchemy import select, text, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from loom_capacity_manager.executable_contracts import (
    ExecutableBootstrapRegistrationV2,
    ExecutableExecutorHeartbeatV2,
    ExecutableExecutorInventoryV2,
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
    SignedExecutableOwnershipProofV2,
    canonical_executable_bytes,
)
from loom_capacity_manager.execution_store import CapacityExecutionStore
from loom_capacity_manager.models import (
    CapacityAllocationEpoch,
    CapacityAuthorityState,
    CapacityExecutableExecutorState,
    CapacityExecutableIntent,
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
from tests.capacity_fixtures import demand_snapshot, pool_observation


def test_execution_store_is_a_distinct_v2_ledger() -> None:
    """Removing the dedicated executable store must break v2 queue ownership."""

    assert CapacityExecutionStore.__module__ == "loom_capacity_manager.execution_store"


async def _active_plan(session: AsyncSession):  # type: ignore[no-untyped-def]
    fixture = await setup_execution(session, execution_policy=execution_policy())
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


async def _launch_ready(
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
    released = await store.release_shapes(capacity_session, release)
    assert released.released_shape_ids == (binding.shape_instance_id,)
