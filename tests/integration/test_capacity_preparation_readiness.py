"""Prepared zero-ceiling readiness is derived only from durable manager evidence."""

from __future__ import annotations

import asyncio
from uuid import UUID

import pytest
from sqlalchemy import delete, select, text, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from loom_capacity_manager.contracts import ResourceVectorV1
from loom_capacity_manager.executable_contracts import (
    ExecutableExecutorHeartbeatV2,
    ExecutableExecutorInventoryV2,
    ExecutableExecutorRegistrationV2,
    ExecutableInventoryRecordV2,
    ExecutionContextV2,
    ExecutionPreparationPolicyV2,
    canonical_executable_digest,
    canonical_inventory_confirmation_journal_head,
)
from loom_capacity_manager.execution_store import CapacityExecutionStore
from loom_capacity_manager.models import (
    Base,
    CapacityAuthorityState,
    CapacityCandidate,
    CapacityExecutableExecutorState,
)
from loom_capacity_manager.preparation_readiness import (
    PreparedExecutionReadinessV2,
    load_prepared_execution_readiness,
)
from tests.capacity_execution_fixtures import (
    PreparedExecutionFixture,
    execution_policy,
    register_execution_executors,
    setup_execution,
)


async def _prepare(
    session: AsyncSession,
    *,
    policy: ExecutionPreparationPolicyV2 | None = None,
) -> tuple[PreparedExecutionFixture, ExecutionContextV2, ExecutionPreparationPolicyV2]:
    resolved = execution_policy() if policy is None else policy
    fixture = await setup_execution(session, execution_policy=resolved)
    prepared = await fixture.store.prepare_execution_epoch(
        session,
        fixture.request,
        actor="preparation-operator",
        idempotency_key=UUID(int=14001),
    )
    return fixture, prepared, resolved


async def _register_one(
    session: AsyncSession,
    fixture: PreparedExecutionFixture,
    prepared: ExecutionContextV2,
    *,
    index: int,
) -> None:
    binding = fixture.request.executors[index]
    await fixture.store.register_execution_executor(
        session,
        ExecutableExecutorRegistrationV2(
            execution=prepared,
            executor_id=binding.executor_id,
            executor_incarnation=binding.executor_incarnation,
            pool_id=binding.pool_id,
            pool_generation=binding.pool_generation,
            signing_key_id=f"{binding.pool_id}-key",
            signing_key_sha256=binding.signing_key_sha256,
            local_authority_sha256=binding.local_authority_sha256,
            controller_authority_sha256=binding.controller_authority_sha256,
        ),
        actor="executor-installer",
        idempotency_key=UUID(int=14010 + index),
    )


async def _publish_prepared_inventories(
    session: AsyncSession,
    fixture: PreparedExecutionFixture,
    prepared: ExecutionContextV2,
    *,
    records_by_pool: dict[str, tuple[ExecutableInventoryRecordV2, ...]] | None = None,
    executor_lease_seconds: int = 120,
    confirm_inventory: bool = True,
) -> None:
    await register_execution_executors(session, fixture, prepared)
    execution_store = CapacityExecutionStore(
        executor_lease_seconds=executor_lease_seconds,
    )
    for binding in fixture.request.executors:
        await execution_store.heartbeat_executor(
            session,
            ExecutableExecutorHeartbeatV2(
                execution=prepared,
                executor_id=binding.executor_id,
                executor_incarnation=binding.executor_incarnation,
                pool_id=binding.pool_id,
                pool_generation=binding.pool_generation,
                heartbeat_sequence=1,
                journal_sequence=0,
                journal_digest="0" * 64,
            ),
        )
        inventory = ExecutableExecutorInventoryV2(
            execution=prepared,
            executor_id=binding.executor_id,
            executor_incarnation=binding.executor_incarnation,
            pool_id=binding.pool_id,
            pool_generation=binding.pool_generation,
            inventory_sequence=1,
            journal_sequence=0,
            journal_digest="0" * 64,
            records=() if records_by_pool is None else records_by_pool.get(binding.pool_id, ()),
        )
        await execution_store.ingest_executor_inventory(session, inventory)
        if confirm_inventory:
            confirmation_sequence, confirmation_digest = (
                canonical_inventory_confirmation_journal_head(inventory)
            )
            await execution_store.heartbeat_executor(
                session,
                ExecutableExecutorHeartbeatV2(
                    execution=prepared,
                    executor_id=binding.executor_id,
                    executor_incarnation=binding.executor_incarnation,
                    pool_id=binding.pool_id,
                    pool_generation=binding.pool_generation,
                    heartbeat_sequence=2,
                    journal_sequence=confirmation_sequence,
                    journal_digest=confirmation_digest,
                ),
            )


async def _readiness(
    session: AsyncSession,
    policy: ExecutionPreparationPolicyV2 | None,
    *,
    freshness_seconds: int = 120,
) -> PreparedExecutionReadinessV2:
    return await load_prepared_execution_readiness(
        session,
        execution_policy=policy,
        execution_policy_sha256=(None if policy is None else canonical_executable_digest(policy)),
        freshness_seconds=freshness_seconds,
    )


async def _reset_committed_capacity_state(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    async with session_factory() as session, session.begin():
        authority_trigger_disabled = False
        await session.execute(
            text(
                "ALTER TABLE capacity_authority_state DISABLE TRIGGER "
                "capacity_authority_execution_transition_guard"
            )
        )
        authority_trigger_disabled = True
        try:
            await session.execute(
                update(CapacityAuthorityState)
                .where(CapacityAuthorityState.singleton_id == 1)
                .values(
                    writer_epoch=0,
                    recovery_state="shadow",
                    increase_freeze=True,
                    increase_freeze_reason="initial_shadow_freeze",
                    executable_new_capacity_ceiling=0,
                    execution_epoch=0,
                    execution_state="shadow",
                    execution_manifest_sha256=None,
                    global_pending_slot_ceiling=0,
                    global_pending_job_ceiling=0,
                    global_submission_rate_ceiling=0,
                )
            )
            for table in reversed(Base.metadata.sorted_tables):
                if table.name == CapacityAuthorityState.__tablename__:
                    continue
                table_name = table.name
                await session.execute(text(f"ALTER TABLE {table_name} DISABLE TRIGGER USER"))
                try:
                    await session.execute(delete(table))
                finally:
                    await session.execute(text(f"ALTER TABLE {table_name} ENABLE TRIGGER USER"))
        finally:
            if authority_trigger_disabled:
                await session.execute(
                    text(
                        "ALTER TABLE capacity_authority_state ENABLE TRIGGER "
                        "capacity_authority_execution_transition_guard"
                    )
                )


async def test_readiness_distinguishes_disabled_and_shadow_manager(
    capacity_session: AsyncSession,
) -> None:
    """A healthy shadow manager must not imply a prepared deployment."""

    fixture = await setup_execution(capacity_session, execution_policy=None)
    disabled = await _readiness(capacity_session, None)
    assert fixture.store.execution_policy is None
    assert disabled.ready is False
    assert disabled.policy_mode == "disabled"
    assert disabled.execution is None
    assert disabled.blockers == ("execution-policy-disabled", "manager-shadow")

    pinned = execution_policy()
    await setup_execution(capacity_session, execution_policy=pinned)
    shadow = await _readiness(capacity_session, pinned)
    assert shadow.ready is False
    assert shadow.policy_mode == "pinned"
    assert shadow.policy_sha256 == canonical_executable_digest(pinned)
    assert shadow.execution is None
    assert shadow.blockers == ("manager-shadow",)


async def test_readiness_reports_the_exact_digest_verified_by_the_file_loader(
    capacity_session: AsyncSession,
) -> None:
    """Formatting-sensitive policy identity is not replaced by a model digest."""

    policy = execution_policy()
    await setup_execution(capacity_session, execution_policy=policy)

    result = await load_prepared_execution_readiness(
        capacity_session,
        execution_policy=policy,
        execution_policy_sha256="f" * 64,
        freshness_seconds=120,
    )

    assert result.policy_sha256 == "f" * 64
    assert result.blockers == ("manager-shadow",)


async def test_readiness_reports_zero_one_and_two_executor_progress(
    capacity_session: AsyncSession,
) -> None:
    """Registration, leases, and inventory remain separate readiness facts."""

    fixture, prepared, policy = await _prepare(capacity_session)
    none = await _readiness(capacity_session, policy)
    assert [item.pool_id for item in none.executors] == ["gb10", "oldlab"]
    assert none.blockers == ("executor-registration-missing",)
    assert all(item.blockers == ("executor-registration-missing",) for item in none.executors)

    await _register_one(capacity_session, fixture, prepared, index=0)
    one = await _readiness(capacity_session, policy)
    assert one.ready is False
    assert one.executors[0].registered_executor_id == "gb10-executor"
    assert one.executors[0].blockers == (
        "executor-inventory-missing",
        "executor-lease-expired",
    )
    assert one.executors[1].blockers == ("executor-registration-missing",)

    await _register_one(capacity_session, fixture, prepared, index=1)
    two = await _readiness(capacity_session, policy)
    assert two.ready is False
    assert two.blockers == ("executor-inventory-missing", "executor-lease-expired")
    assert all(item.inventory_sequence == 0 for item in two.executors)


async def test_complete_empty_two_pool_inventory_is_ready(
    capacity_session: AsyncSession,
) -> None:
    """Only fresh post-inventory evidence from both exact pools is ready."""

    fixture, prepared, policy = await _prepare(capacity_session)
    await _publish_prepared_inventories(capacity_session, fixture, prepared)

    result = await _readiness(capacity_session, policy)

    assert result.ready is True
    assert result.execution == prepared
    assert result.expected_subject_count == 1
    assert result.acknowledged_subject_count == 1
    assert result.blockers == ()
    assert result.executable is False
    assert [item.pool_id for item in result.executors] == ["gb10", "oldlab"]
    for item in result.executors:
        assert item.registered is True
        assert item.current is True
        assert item.lease_fresh is True
        assert item.inventory_fresh is True
        assert item.post_inventory_heartbeat is True
        assert item.inventory_sequence == 1
        assert item.journal_sequence == 2
        assert item.inventory_record_count == 0
        assert item.blockers == ()


async def test_readiness_samples_database_time_after_waiting_for_evidence_locks(
    capacity_session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """A lock wait cannot preserve already-expired readiness with an old clock sample."""

    await _reset_committed_capacity_state(capacity_session_factory)
    try:
        async with capacity_session_factory() as setup_session:
            fixture, prepared, policy = await _prepare(setup_session)
            await _publish_prepared_inventories(
                setup_session,
                fixture,
                prepared,
                executor_lease_seconds=1,
            )
            await setup_session.commit()

        async with (
            capacity_session_factory() as blocker_session,
            capacity_session_factory() as readiness_session,
            capacity_session_factory() as observer_session,
        ):
            await blocker_session.execute(
                select(CapacityAuthorityState)
                .where(CapacityAuthorityState.singleton_id == 1)
                .with_for_update()
            )
            readiness_pid = (
                await readiness_session.execute(text("SELECT pg_backend_pid()"))
            ).scalar_one()
            readiness_task = asyncio.create_task(
                _readiness(readiness_session, policy, freshness_seconds=1)
            )
            blocked = False
            for _attempt in range(200):
                wait_event_type = (
                    await observer_session.execute(
                        text("SELECT wait_event_type FROM pg_stat_activity WHERE pid = :pid"),
                        {"pid": readiness_pid},
                    )
                ).scalar_one()
                if wait_event_type == "Lock":
                    blocked = True
                    break
                await asyncio.sleep(0.01)
            assert blocked, "readiness did not wait on the authority evidence lock"

            await asyncio.sleep(1.05)
            await blocker_session.commit()
            result = await readiness_task

            assert result.ready is False
            assert "executor-lease-expired" in result.blockers
            assert "executor-inventory-stale" in result.blockers
    finally:
        await _reset_committed_capacity_state(capacity_session_factory)


@pytest.mark.parametrize(
    ("mutation", "expected_blocker"),
    (
        ("lease", "executor-lease-expired"),
        ("inventory-time", "executor-inventory-stale"),
        ("heartbeat-time", "executor-post-inventory-heartbeat-missing"),
        ("journal", "executor-inventory-invalid"),
        ("payload", "executor-inventory-invalid"),
    ),
)
async def test_readiness_fails_closed_for_stale_or_contradictory_executor_evidence(
    capacity_session: AsyncSession,
    mutation: str,
    expected_blocker: str,
) -> None:
    """A durable but stale or internally contradictory checkpoint is not ready."""

    fixture, prepared, policy = await _prepare(capacity_session)
    await _publish_prepared_inventories(
        capacity_session,
        fixture,
        prepared,
        executor_lease_seconds=1 if mutation == "lease" else 120,
        confirm_inventory=mutation != "heartbeat-time",
    )
    row = (
        await capacity_session.execute(
            select(CapacityExecutableExecutorState).where(
                CapacityExecutableExecutorState.pool_id == "gb10"
            )
        )
    ).scalar_one()
    if mutation == "lease":
        await capacity_session.execute(text("SELECT pg_sleep(1.05)"))
    elif mutation == "inventory-time":
        await capacity_session.execute(text("SELECT pg_sleep(1.05)"))
    elif mutation == "heartbeat-time":
        pass
    elif mutation == "journal":
        binding = fixture.request.executors[0]
        await CapacityExecutionStore().heartbeat_executor(
            capacity_session,
            ExecutableExecutorHeartbeatV2(
                execution=prepared,
                executor_id=binding.executor_id,
                executor_incarnation=binding.executor_incarnation,
                pool_id=binding.pool_id,
                pool_generation=binding.pool_generation,
                heartbeat_sequence=3,
                journal_sequence=row.journal_high_water + 1,
                journal_digest="f" * 64,
                journal_checkpoint_sequence=row.journal_high_water,
                journal_checkpoint_digest=row.journal_digest,
            ),
        )
    else:
        await capacity_session.execute(
            text("ALTER TABLE capacity_executable_executor_states DISABLE TRIGGER USER")
        )
        try:
            await capacity_session.execute(
                update(CapacityExecutableExecutorState)
                .where(CapacityExecutableExecutorState.id == row.id)
                .values(inventory_payload={})
            )
        finally:
            await capacity_session.execute(
                text("ALTER TABLE capacity_executable_executor_states ENABLE TRIGGER USER")
            )

    result = await _readiness(
        capacity_session,
        policy,
        freshness_seconds=1 if mutation == "inventory-time" else 120,
    )

    assert result.ready is False
    assert expected_blocker in result.blockers
    assert result.blockers == tuple(sorted(set(result.blockers)))


@pytest.mark.parametrize(
    ("authority_scope", "state", "expected_specific"),
    (
        ("foreign", "active", "executor-inventory-foreign"),
        ("dedicated-loom-association", "unknown", "executor-inventory-unknown"),
        (
            "registered-loom",
            "active",
            "executor-inventory-ownership-missing",
        ),
    ),
)
async def test_readiness_quarantines_unsafe_physical_inventory(
    capacity_session: AsyncSession,
    authority_scope: str,
    state: str,
    expected_specific: str,
) -> None:
    """Foreign, unknown, and unsigned Loom work remain visible and blocking."""

    fixture, prepared, policy = await _prepare(capacity_session)
    record = ExecutableInventoryRecordV2(
        physical_identity="unsafe-physical-work",
        physical_kind="slurm-job",
        authority_scope=authority_scope,  # type: ignore[arg-type]
        state=state,  # type: ignore[arg-type]
        resources=ResourceVectorV1(slots=1),
        controller_evidence_sha256="9" * 64,
    )
    await _publish_prepared_inventories(
        capacity_session,
        fixture,
        prepared,
        records_by_pool={"gb10": (record,)},
    )

    result = await _readiness(capacity_session, policy)

    assert result.ready is False
    assert expected_specific in result.blockers
    assert "executor-inventory-quarantined" in result.blockers
    gb10 = result.executors[0]
    assert gb10.inventory_record_count == 1
    assert gb10.quarantined_record_count == 1


async def test_readiness_revalidates_executor_and_subject_bindings(
    capacity_session: AsyncSession,
) -> None:
    """A policy or candidate change invalidates a previously complete rehearsal."""

    fixture, prepared, policy = await _prepare(capacity_session)
    await _publish_prepared_inventories(capacity_session, fixture, prepared)
    changed_binding = policy.executors[0].model_copy(update={"executor_id": "changed-executor"})
    changed_policy = policy.model_copy(update={"executors": (changed_binding, policy.executors[1])})
    changed = await _readiness(capacity_session, changed_policy)
    assert changed.ready is False
    assert "executor-binding-changed" in changed.blockers

    candidate = (await capacity_session.execute(select(CapacityCandidate))).scalar_one()
    await capacity_session.execute(
        update(CapacityCandidate)
        .where(CapacityCandidate.id == candidate.id)
        .values(source_payload={"publication_sha256": "f" * 64})
    )
    incomplete = await _readiness(capacity_session, policy)
    assert incomplete.ready is False
    assert incomplete.expected_subject_count == 1
    assert incomplete.acknowledged_subject_count == 0
    assert "subject-acknowledgements-incomplete" in incomplete.blockers
