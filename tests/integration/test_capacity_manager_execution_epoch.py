"""Durable execution-epoch preparation and activation boundaries."""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from uuid import UUID

import pytest
from sqlalchemy import select, text, update
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from loom_capacity_manager.contracts import (
    ConfigurationActivationV1,
    ConfigurationGenerationRefV1,
    canonical_digest,
)
from loom_capacity_manager.executable_contracts import (
    ExecutableExecutorHeartbeatV2,
    ExecutableExecutorInventoryV2,
    ExecutableExecutorRegistrationV2,
    ExecutionActivationV2,
    ExecutionContextV2,
    ExecutionDrainV2,
    ExecutionPreparationPolicyV2,
    ExecutionRetirementExecutorCheckpointV2,
    ExecutionRetirementV2,
    canonical_executable_digest,
    canonical_inventory_confirmation_journal_head,
)
from loom_capacity_manager.execution_store import CapacityExecutionStore
from loom_capacity_manager.models import (
    CapacityAllocationEpoch,
    CapacityAuthorityState,
    CapacityCandidate,
    CapacityExecutionEpoch,
    CapacityExecutionExecutor,
    CapacityPoolReporter,
)
from loom_capacity_manager.reconciler import reconcile_shadow_once
from loom_capacity_manager.store import (
    AuthorityRecoveryError,
    CapacityManagementStore,
    ExecutionConflictError,
    ExecutionPreparationDisabledError,
    IdempotencyConflictError,
    StaleWriterError,
)
from tests.capacity_execution_fixtures import (
    PreparedExecutionFixture as _PreparedFixture,
)
from tests.capacity_execution_fixtures import (
    execution_policy as _policy,
)
from tests.capacity_execution_fixtures import (
    executor_binding as _executor_binding,
)
from tests.capacity_execution_fixtures import (
    protected_candidate as _protected_candidate,
)
from tests.capacity_execution_fixtures import (
    register_execution_executors as _register_execution_executors,
)
from tests.capacity_execution_fixtures import (
    setup_execution as _setup,
)
from tests.capacity_fixtures import (
    AUTHORITY_ID,
    demand_snapshot,
    development_projection,
    fleet_manifest,
    pool_observation,
    subject_configuration,
)


async def _activate_fixture(
    capacity_session: AsyncSession,
    *,
    policy: ExecutionPreparationPolicyV2 | None = None,
) -> tuple[
    _PreparedFixture,
    ExecutionContextV2,
    ExecutionActivationV2,
    ExecutionContextV2,
]:
    effective_policy = _policy() if policy is None else policy
    fixture = await _setup(capacity_session, execution_policy=effective_policy)
    prepared = await fixture.store.prepare_execution_epoch(
        capacity_session,
        fixture.request,
        actor="activation-operator",
        idempotency_key=UUID(int=780),
    )
    await _register_execution_executors(capacity_session, fixture, prepared)
    activation = ExecutionActivationV2(
        authority_incarnation=AUTHORITY_ID,
        expected_writer_epoch=fixture.writer.writer_epoch,
        execution_epoch=prepared.execution_epoch,
        execution_manifest_sha256=prepared.execution_manifest_sha256,
        executable_new_capacity_ceiling=1,
        executable_new_capacity_rate_per_minute=1,
    )
    active = await fixture.store.activate_execution_epoch(
        capacity_session,
        activation,
        actor="activation-operator",
        idempotency_key=UUID(int=781),
    )
    return fixture, prepared, activation, active


async def _activate_execution(capacity_session: AsyncSession):  # type: ignore[no-untyped-def]
    fixture = await _setup(capacity_session, execution_policy=_policy())
    prepared = await fixture.store.prepare_execution_epoch(
        capacity_session,
        fixture.request,
        actor="activation-operator",
        idempotency_key=UUID(int=12010),
    )
    await _register_execution_executors(capacity_session, fixture, prepared)
    active = await fixture.store.activate_execution_epoch(
        capacity_session,
        ExecutionActivationV2(
            authority_incarnation=prepared.authority_incarnation,
            expected_writer_epoch=prepared.writer_epoch,
            execution_epoch=prepared.execution_epoch,
            execution_manifest_sha256=prepared.execution_manifest_sha256,
            executable_new_capacity_ceiling=1,
            executable_new_capacity_rate_per_minute=1,
        ),
        actor="activation-operator",
        idempotency_key=UUID(int=12011),
    )
    return fixture, active


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


async def _publish_final_safe_evidence(
    capacity_session: AsyncSession,
    drained,  # type: ignore[no-untyped-def]
) -> tuple[ExecutionRetirementExecutorCheckpointV2, ...]:
    execution_store = CapacityExecutionStore()
    checkpoints = []
    for pool_id in ("gb10", "oldlab"):
        binding = _executor_binding(pool_id)
        common = {
            "execution": drained,
            "executor_id": binding.executor_id,
            "executor_incarnation": binding.executor_incarnation,
            "pool_id": binding.pool_id,
            "pool_generation": binding.pool_generation,
        }
        await execution_store.heartbeat_executor(
            capacity_session,
            ExecutableExecutorHeartbeatV2(
                **common,
                heartbeat_sequence=1,
                journal_sequence=0,
                journal_digest="0" * 64,
            ),
        )
        inventory = ExecutableExecutorInventoryV2(
            **common,
            inventory_sequence=1,
            journal_sequence=0,
            journal_digest="0" * 64,
            records=(),
        )
        await execution_store.ingest_executor_inventory(capacity_session, inventory)
        confirmation_sequence, confirmation_digest = canonical_inventory_confirmation_journal_head(
            inventory
        )
        await execution_store.heartbeat_executor(
            capacity_session,
            ExecutableExecutorHeartbeatV2(
                **common,
                heartbeat_sequence=2,
                journal_sequence=confirmation_sequence,
                journal_digest=confirmation_digest,
            ),
        )
        checkpoints.append(
            ExecutionRetirementExecutorCheckpointV2(
                executor_id=binding.executor_id,
                executor_incarnation=binding.executor_incarnation,
                pool_id=binding.pool_id,
                pool_generation=binding.pool_generation,
                heartbeat_sequence=2,
                command_sequence=0,
                journal_sequence=confirmation_sequence,
                journal_digest=confirmation_digest,
                inventory_sequence=1,
                inventory_digest=canonical_executable_digest(inventory),
            )
        )
    return tuple(checkpoints)


def _retirement_request(
    drained,  # type: ignore[no-untyped-def]
    checkpoints: tuple[ExecutionRetirementExecutorCheckpointV2, ...],
) -> ExecutionRetirementV2:
    return ExecutionRetirementV2(
        authority_incarnation=drained.authority_incarnation,
        expected_writer_epoch=drained.writer_epoch,
        execution_epoch=drained.execution_epoch,
        execution_manifest_sha256=drained.execution_manifest_sha256,
        executor_checkpoints=checkpoints,
    )


async def test_execution_preparation_is_disabled_without_owner_policy(
    capacity_session: AsyncSession,
) -> None:
    """A caller-supplied manifest alone must never create executable authority."""

    fixture = await _setup(capacity_session)
    with pytest.raises(ExecutionPreparationDisabledError):
        await fixture.store.prepare_execution_epoch(
            capacity_session,
            fixture.request,
            actor="activation-operator",
            idempotency_key=UUID(int=720),
        )


async def test_prepare_rejects_executor_pool_generation_outside_active_fleet(
    capacity_session: AsyncSession,
) -> None:
    """Owner policy cannot prepare an executor against a stale pool generation."""

    policy = _policy()
    stale_executors = tuple(
        binding.model_copy(update={"pool_generation": 2}) if binding.pool_id == "gb10" else binding
        for binding in policy.executors
    )
    stale_policy = policy.model_copy(update={"executors": stale_executors})
    fixture = await _setup(capacity_session, execution_policy=stale_policy)
    stale_request = fixture.request.model_copy(update={"executors": stale_executors})

    with pytest.raises(ExecutionConflictError, match="executor pool generation"):
        await fixture.store.prepare_execution_epoch(
            capacity_session,
            stale_request,
            actor="activation-operator",
            idempotency_key=UUID(int=719),
        )


async def test_prepare_is_exact_replay_and_keeps_the_ceiling_zero(
    capacity_session: AsyncSession,
) -> None:
    """Preparation must persist immutable evidence without enabling capacity."""

    fixture = await _setup(capacity_session, execution_policy=_policy())
    first = await fixture.store.prepare_execution_epoch(
        capacity_session,
        fixture.request,
        actor="activation-operator",
        idempotency_key=UUID(int=721),
    )
    replay = await fixture.store.prepare_execution_epoch(
        capacity_session,
        fixture.request,
        actor="activation-operator",
        idempotency_key=UUID(int=721),
    )

    assert replay == first
    assert first.execution_state == "prepared"
    assert first.executable_new_capacity_ceiling == 0
    assert first.execution_manifest_sha256 == canonical_executable_digest(fixture.request)
    authority = (await capacity_session.execute(select(CapacityAuthorityState))).scalar_one()
    assert (authority.execution_state, authority.executable_new_capacity_ceiling) == (
        "prepared",
        0,
    )

    with pytest.raises(DBAPIError, match="execution epoch immutable evidence changed"):
        async with capacity_session.begin_nested():
            await capacity_session.execute(
                update(CapacityExecutionEpoch).values(fleet_digest="f" * 64)
            )
    row = (await capacity_session.execute(select(CapacityExecutionEpoch))).scalar_one()
    with pytest.raises(DBAPIError, match="must be inserted prepared"):
        async with capacity_session.begin_nested():
            capacity_session.add(
                CapacityExecutionEpoch(
                    execution_epoch=row.execution_epoch + 1,
                    authority_incarnation=row.authority_incarnation,
                    prepared_writer_epoch=row.prepared_writer_epoch,
                    current_writer_epoch=row.current_writer_epoch,
                    configuration_epoch=row.configuration_epoch,
                    fleet_generation=row.fleet_generation,
                    fleet_digest=row.fleet_digest,
                    execution_manifest_sha256="9" * 64,
                    manifest_payload=row.manifest_payload,
                    trusted_fleet_release_sha256=row.trusted_fleet_release_sha256,
                    oldlab_executor_id=row.oldlab_executor_id,
                    oldlab_executor_incarnation=row.oldlab_executor_incarnation,
                    oldlab_pool_id=row.oldlab_pool_id,
                    oldlab_pool_generation=row.oldlab_pool_generation,
                    oldlab_signing_key_sha256=row.oldlab_signing_key_sha256,
                    oldlab_local_authority_sha256=row.oldlab_local_authority_sha256,
                    oldlab_controller_authority_sha256=(row.oldlab_controller_authority_sha256),
                    gb10_executor_id=row.gb10_executor_id,
                    gb10_executor_incarnation=row.gb10_executor_incarnation,
                    gb10_pool_id=row.gb10_pool_id,
                    gb10_pool_generation=row.gb10_pool_generation,
                    gb10_signing_key_sha256=row.gb10_signing_key_sha256,
                    gb10_local_authority_sha256=row.gb10_local_authority_sha256,
                    gb10_controller_authority_sha256=row.gb10_controller_authority_sha256,
                    environment_acknowledgements_sha256=(row.environment_acknowledgements_sha256),
                    legacy_writer_manifest_sha256=row.legacy_writer_manifest_sha256,
                    rollback_evidence_sha256=row.rollback_evidence_sha256,
                    requested_ceiling=1,
                    effective_ceiling=1,
                    requested_rate_per_minute=1,
                    effective_rate_per_minute=1,
                    state="active",
                    actor="forged-operator",
                    idempotency_key=UUID(int=727),
                    request_digest="9" * 64,
                    activation_actor="forged-operator",
                    activation_idempotency_key=UUID(int=728),
                    activation_request_digest="8" * 64,
                    activated_at=datetime.now(UTC),
                )
            )
            await capacity_session.flush()
    changed = fixture.request.model_copy(update={"requested_ceiling": 2})
    with pytest.raises(IdempotencyConflictError):
        await fixture.store.prepare_execution_epoch(
            capacity_session,
            changed,
            actor="activation-operator",
            idempotency_key=UUID(int=721),
        )


async def test_prepare_replay_revalidates_current_candidate_provenance(
    capacity_session: AsyncSession,
) -> None:
    """An idempotent replay cannot bypass candidate drift discovered later."""

    fixture = await _setup(capacity_session, execution_policy=_policy())
    await fixture.store.prepare_execution_epoch(
        capacity_session,
        fixture.request,
        actor="activation-operator",
        idempotency_key=UUID(int=782),
    )
    await capacity_session.execute(update(CapacityCandidate).values(candidate_identity="9" * 64))

    with pytest.raises(ExecutionConflictError, match="candidate provenance"):
        await fixture.store.prepare_execution_epoch(
            capacity_session,
            fixture.request,
            actor="activation-operator",
            idempotency_key=UUID(int=782),
        )


async def test_prepare_and_executor_registration_replays_stop_after_activation(
    capacity_session: AsyncSession,
) -> None:
    """Prepared-only operations cannot replay into positive active authority."""

    fixture, prepared, _, _ = await _activate_fixture(capacity_session)

    with pytest.raises(ExecutionConflictError, match="only while prepared"):
        await fixture.store.prepare_execution_epoch(
            capacity_session,
            fixture.request,
            actor="activation-operator",
            idempotency_key=UUID(int=780),
        )

    binding = fixture.request.executors[0]
    with pytest.raises(ExecutionConflictError, match="only while prepared"):
        await fixture.store.register_execution_executor(
            capacity_session,
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
            idempotency_key=UUID(int=741),
        )


async def test_prepare_rejects_incomplete_subject_or_legacy_inventory(
    capacity_session: AsyncSession,
) -> None:
    """Omitting a subject or configured writer must not produce a prepared epoch."""

    fixture = await _setup(capacity_session, execution_policy=_policy())
    acknowledgement = fixture.request.subject_acknowledgements[0]
    for changed in (
        fixture.request.model_copy(update={"subject_acknowledgements": ()}),
        fixture.request.model_copy(update={"legacy_writer_fences": ()}),
        fixture.request.model_copy(
            update={
                "subject_acknowledgements": (
                    acknowledgement.model_copy(update={"acknowledgement_sha256": "9" * 64}),
                )
            }
        ),
        fixture.request.model_copy(update={"rollback_evidence_sha256": "9" * 64}),
        fixture.request.model_copy(update={"requested_ceiling": 2}),
    ):
        with pytest.raises(ExecutionConflictError):
            await fixture.store.prepare_execution_epoch(
                capacity_session,
                changed,
                actor="activation-operator",
                idempotency_key=UUID(int=722),
            )


async def test_activation_rechecks_freeze_and_exact_evidence(
    capacity_session: AsyncSession,
) -> None:
    """Changing a prepared fence must block the active authority transition."""

    fixture = await _setup(capacity_session, execution_policy=_policy())
    prepared = await fixture.store.prepare_execution_epoch(
        capacity_session,
        fixture.request,
        actor="activation-operator",
        idempotency_key=UUID(int=723),
    )
    activation = ExecutionActivationV2(
        authority_incarnation=AUTHORITY_ID,
        expected_writer_epoch=fixture.writer.writer_epoch,
        execution_epoch=prepared.execution_epoch,
        execution_manifest_sha256=prepared.execution_manifest_sha256,
        executable_new_capacity_ceiling=1,
        executable_new_capacity_rate_per_minute=1,
    )
    await _register_execution_executors(capacity_session, fixture, prepared)
    await capacity_session.execute(
        update(CapacityAuthorityState).values(
            increase_freeze=False,
            increase_freeze_reason=None,
        )
    )
    with pytest.raises(ExecutionConflictError, match="increase freeze"):
        await fixture.store.activate_execution_epoch(
            capacity_session,
            activation,
            actor="activation-operator",
            idempotency_key=UUID(int=724),
        )


async def test_preparation_rejects_ceiling_above_exact_fleet_slots(
    capacity_session: AsyncSession,
) -> None:
    """A requested envelope larger than the two bound pools must not prepare."""

    policy = _policy(ceiling=17)
    fixture = await _setup(
        capacity_session,
        execution_policy=policy,
        ceiling=17,
    )
    with pytest.raises(ExecutionConflictError, match="configured fleet capacity"):
        await fixture.store.prepare_execution_epoch(
            capacity_session,
            fixture.request,
            actor="activation-operator",
            idempotency_key=UUID(int=11901),
        )


async def test_active_fact_ingestion_preserves_bound_identity(
    capacity_session: AsyncSession,
) -> None:
    """Active authority accepts bound facts but rejects configuration mutation."""

    fixture = await _setup(
        capacity_session,
        execution_policy=_policy(ceiling=2),
        ceiling=2,
    )
    await fixture.store.ingest_demand_snapshot(
        capacity_session, demand_snapshot(sequence=1), actor="dev-a"
    )
    for pool_id in ("oldlab", "gb10"):
        await fixture.store.ingest_pool_observation(
            capacity_session,
            pool_observation(sequence=1, pool_id=pool_id),
            actor=f"{pool_id}-reporter",
        )
    prepared = await fixture.store.prepare_execution_epoch(
        capacity_session,
        fixture.request,
        actor="activation-operator",
        idempotency_key=UUID(int=11902),
    )
    await _register_execution_executors(capacity_session, fixture, prepared)
    await fixture.store.activate_execution_epoch(
        capacity_session,
        ExecutionActivationV2(
            authority_incarnation=AUTHORITY_ID,
            expected_writer_epoch=fixture.writer.writer_epoch,
            execution_epoch=prepared.execution_epoch,
            execution_manifest_sha256=prepared.execution_manifest_sha256,
            executable_new_capacity_ceiling=2,
            executable_new_capacity_rate_per_minute=1,
        ),
        actor="activation-operator",
        idempotency_key=UUID(int=11903),
    )

    before = canonical_digest(
        await fixture.store.load_allocation_input(capacity_session, fixture.writer)
    )
    demand = await fixture.store.ingest_demand_snapshot(
        capacity_session, demand_snapshot(sequence=2), actor="dev-a"
    )
    oldlab = await fixture.store.ingest_pool_observation(
        capacity_session, pool_observation(sequence=2, pool_id="oldlab"), actor="oldlab"
    )
    gb10 = await fixture.store.ingest_pool_observation(
        capacity_session, pool_observation(sequence=2, pool_id="gb10"), actor="gb10"
    )
    assert (demand.sequence, oldlab.sequence, gb10.sequence) == (2, 2, 2)
    assert (
        canonical_digest(
            await fixture.store.load_allocation_input(capacity_session, fixture.writer)
        )
        != before
    )
    input_digest = canonical_digest(
        await fixture.store.load_allocation_input(capacity_session, fixture.writer)
    )
    session_factory = async_sessionmaker(
        bind=capacity_session.bind,
        expire_on_commit=False,
        join_transaction_mode="create_savepoint",
    )
    result = await reconcile_shadow_once(
        session_factory,
        fixture.writer,
        store=fixture.store,
    )
    committed = (await capacity_session.execute(select(CapacityAllocationEpoch))).scalar_one()
    assert result.status == "committed"
    assert result.input_digest == input_digest
    assert (committed.status, committed.executable, committed.input_digest) == (
        "executable",
        True,
        input_digest,
    )

    with pytest.raises(AuthorityRecoveryError, match="shadow-only"):
        await fixture.store.propose_fleet_configuration(
            capacity_session,
            fleet_manifest(fleet_generation=2),
            actor="fleet-operator",
            idempotency_key=UUID(int=11904),
        )
    with pytest.raises(AuthorityRecoveryError, match="shadow-only"):
        await fixture.store.propose_subject_configuration(
            capacity_session,
            subject_configuration(
                configuration_generation=2,
                demand_reporter_incarnation=UUID(int=11905),
            ),
            actor="environment-state",
            idempotency_key=UUID(int=11905),
        )
    with pytest.raises(AuthorityRecoveryError, match="shadow-only"):
        await fixture.store.activate_configuration(
            capacity_session,
            ConfigurationActivationV1(
                expected_configuration_epoch=1,
                fleet=ConfigurationGenerationRefV1(scope="fleet", generation=2, digest="1" * 64),
                subjects=(
                    ConfigurationGenerationRefV1(
                        scope="subject",
                        generation=2,
                        digest="2" * 64,
                        subject_id=UUID(int=11905),
                        subject_incarnation=UUID(int=11906),
                    ),
                ),
            ),
            actor="fleet-operator",
            idempotency_key=UUID(int=11907),
        )
    for operation_kind in ("create", "update", "destroy"):
        with pytest.raises(AuthorityRecoveryError, match="shadow-only"):
            await fixture.store.project_development_subject(
                capacity_session,
                development_projection(operation_kind=operation_kind),
                actor="environment-lifecycle",
                idempotency_key=UUID(int=11906),
            )


async def test_active_pool_fact_rejects_same_generation_replacement_reporter(
    capacity_session: AsyncSession,
) -> None:
    """A current reporter outside the immutable fleet binding cannot add active facts."""

    fixture = await _setup(capacity_session, execution_policy=_policy())
    prepared = await fixture.store.prepare_execution_epoch(
        capacity_session,
        fixture.request,
        actor="activation-operator",
        idempotency_key=UUID(int=11920),
    )
    await _register_execution_executors(capacity_session, fixture, prepared)
    await fixture.store.activate_execution_epoch(
        capacity_session,
        ExecutionActivationV2(
            authority_incarnation=AUTHORITY_ID,
            expected_writer_epoch=fixture.writer.writer_epoch,
            execution_epoch=prepared.execution_epoch,
            execution_manifest_sha256=prepared.execution_manifest_sha256,
            executable_new_capacity_ceiling=1,
            executable_new_capacity_rate_per_minute=1,
        ),
        actor="activation-operator",
        idempotency_key=UUID(int=11921),
    )
    replacement = UUID(int=11922)
    await capacity_session.execute(
        update(CapacityPoolReporter)
        .where(CapacityPoolReporter.pool_id == "oldlab")
        .values(state="fenced")
    )
    capacity_session.add(
        CapacityPoolReporter(
            pool_id="oldlab",
            reporter_incarnation=replacement,
            pool_generation=1,
            state="current",
        )
    )
    await capacity_session.flush()

    report = pool_observation(sequence=1, pool_id="oldlab").model_copy(
        update={"reporter_incarnation": replacement}
    )
    with pytest.raises(AuthorityRecoveryError, match="pool reporter binding changed"):
        await fixture.store.ingest_pool_observation(
            capacity_session,
            report,
            actor="replacement-oldlab-reporter",
        )


async def test_prepared_fact_and_drain_only_fact_ingestion_are_rejected(
    capacity_session: AsyncSession,
) -> None:
    """Only active authority may accept facts; zero-ceiling transitions cannot."""

    fixture = await _setup(capacity_session, execution_policy=_policy())
    prepared = await fixture.store.prepare_execution_epoch(
        capacity_session,
        fixture.request,
        actor="activation-operator",
        idempotency_key=UUID(int=11910),
    )
    with pytest.raises(AuthorityRecoveryError, match="fact ingestion"):
        await fixture.store.ingest_demand_snapshot(
            capacity_session, demand_snapshot(sequence=1), actor="dev-a"
        )
    await _register_execution_executors(capacity_session, fixture, prepared)
    await fixture.store.activate_execution_epoch(
        capacity_session,
        ExecutionActivationV2(
            authority_incarnation=AUTHORITY_ID,
            expected_writer_epoch=fixture.writer.writer_epoch,
            execution_epoch=prepared.execution_epoch,
            execution_manifest_sha256=prepared.execution_manifest_sha256,
            executable_new_capacity_ceiling=1,
            executable_new_capacity_rate_per_minute=1,
        ),
        actor="activation-operator",
        idempotency_key=UUID(int=11911),
    )
    await fixture.store.register_writer(
        capacity_session,
        AUTHORITY_ID,
        expected_epoch=fixture.writer.writer_epoch,
    )
    with pytest.raises(AuthorityRecoveryError, match="fact ingestion"):
        await fixture.store.ingest_pool_observation(
            capacity_session,
            pool_observation(sequence=1, pool_id="oldlab"),
            actor="oldlab-reporter",
        )


async def test_execution_rechecks_durable_candidate_provenance(
    capacity_session: AsyncSession,
) -> None:
    """A policy-matching acknowledgement cannot override durable candidate state."""

    fixture = await _setup(capacity_session, execution_policy=_policy())
    await capacity_session.execute(update(CapacityCandidate).values(candidate_identity="9" * 64))
    with pytest.raises(ExecutionConflictError, match="candidate provenance"):
        await fixture.store.prepare_execution_epoch(
            capacity_session,
            fixture.request,
            actor="activation-operator",
            idempotency_key=UUID(int=733),
        )

    await capacity_session.execute(update(CapacityCandidate).values(candidate_identity="1" * 64))
    prepared = await fixture.store.prepare_execution_epoch(
        capacity_session,
        fixture.request,
        actor="activation-operator",
        idempotency_key=UUID(int=734),
    )
    await _register_execution_executors(capacity_session, fixture, prepared)
    activation = ExecutionActivationV2(
        authority_incarnation=AUTHORITY_ID,
        expected_writer_epoch=fixture.writer.writer_epoch,
        execution_epoch=prepared.execution_epoch,
        execution_manifest_sha256=prepared.execution_manifest_sha256,
        executable_new_capacity_ceiling=1,
        executable_new_capacity_rate_per_minute=1,
    )
    await capacity_session.execute(
        update(CapacityCandidate).values(source_payload={"publication_sha256": "8" * 64})
    )
    with pytest.raises(ExecutionConflictError, match="candidate provenance"):
        await fixture.store.activate_execution_epoch(
            capacity_session,
            activation,
            actor="activation-operator",
            idempotency_key=UUID(int=735),
        )
    await capacity_session.execute(
        update(CapacityCandidate).values(source_payload={"publication_sha256": "2" * 64})
    )
    await fixture.store.activate_execution_epoch(
        capacity_session,
        activation,
        actor="activation-operator",
        idempotency_key=UUID(int=738),
    )
    await capacity_session.execute(update(CapacityCandidate).values(candidate_identity="7" * 64))
    with pytest.raises(AuthorityRecoveryError, match="owner policy"):
        await fixture.store.execution_authority(capacity_session)


async def test_execution_accepts_exact_protected_git_candidate(
    capacity_session: AsyncSession,
) -> None:
    """A protected release retains its exact 40-character Git identity."""

    candidate = _protected_candidate()
    fixture = await _setup(
        capacity_session,
        execution_policy=_policy(candidate),
        candidate=candidate,
    )

    prepared = await fixture.store.prepare_execution_epoch(
        capacity_session,
        fixture.request,
        actor="activation-operator",
        idempotency_key=UUID(int=739),
    )

    assert prepared.execution_state == "prepared"


async def test_execution_rejects_stale_protected_git_candidate(
    capacity_session: AsyncSession,
) -> None:
    """A protected acknowledgement cannot substitute another exact Git commit."""

    candidate = _protected_candidate()
    fixture = await _setup(
        capacity_session,
        execution_policy=_policy(candidate),
        candidate=candidate,
    )
    await capacity_session.execute(update(CapacityCandidate).values(candidate_identity="9" * 40))

    with pytest.raises(ExecutionConflictError, match="candidate provenance"):
        await fixture.store.prepare_execution_epoch(
            capacity_session,
            fixture.request,
            actor="activation-operator",
            idempotency_key=UUID(int=740),
        )


async def test_writer_restart_retires_a_stale_preparation(
    capacity_session: AsyncSession,
) -> None:
    """A restarted writer must not strand a prepared authority on an old fence."""

    fixture = await _setup(capacity_session, execution_policy=_policy())
    prepared = await fixture.store.prepare_execution_epoch(
        capacity_session,
        fixture.request,
        actor="activation-operator",
        idempotency_key=UUID(int=729),
    )
    await _register_execution_executors(capacity_session, fixture, prepared)

    successor = await fixture.store.register_writer(
        capacity_session,
        AUTHORITY_ID,
        expected_epoch=fixture.writer.writer_epoch,
    )

    assert successor.writer_epoch == fixture.writer.writer_epoch + 1
    assert await fixture.store.execution_authority(capacity_session) is None
    row = (
        await capacity_session.execute(
            select(CapacityExecutionEpoch).where(
                CapacityExecutionEpoch.execution_epoch == prepared.execution_epoch
            )
        )
    ).scalar_one()
    assert row.state == "retired"
    assert row.retired_at is not None
    expected_payload = {
        "schema_version": 2,
        "transition": "retire-prepared",
        "reason": "writer-replacement",
        "authority_incarnation": str(AUTHORITY_ID),
        "previous_writer_epoch": fixture.writer.writer_epoch,
        "successor_writer_epoch": successor.writer_epoch,
        "execution_epoch": prepared.execution_epoch,
        "execution_manifest_sha256": prepared.execution_manifest_sha256,
        "executable": True,
    }
    expected_name = (
        f"retire-prepared:{AUTHORITY_ID}:{fixture.writer.writer_epoch}:"
        f"{successor.writer_epoch}:{prepared.execution_epoch}:"
        f"{prepared.execution_manifest_sha256}"
    )
    expected_uuid_bytes = bytearray(
        hashlib.sha256(
            UUID("9e40e05d-f1c0-4aa8-9ee2-21cc4b46f489").bytes + expected_name.encode("utf-8")
        ).digest()[:16]
    )
    expected_uuid_bytes[6] = (expected_uuid_bytes[6] & 0x0F) | 0x80
    expected_uuid_bytes[8] = (expected_uuid_bytes[8] & 0x3F) | 0x80
    assert row.retirement_actor == f"capacity-manager:{AUTHORITY_ID}"
    assert row.retirement_idempotency_key == UUID(bytes=bytes(expected_uuid_bytes))
    assert row.retirement_idempotency_key.version == 8
    assert row.retirement_request_payload == expected_payload
    assert (
        row.retirement_request_digest
        == hashlib.sha256(
            json.dumps(
                expected_payload,
                sort_keys=True,
                separators=(",", ":"),
                ensure_ascii=True,
                allow_nan=False,
            ).encode("ascii")
        ).hexdigest()
    )

    with pytest.raises(ExecutionConflictError, match="only while prepared"):
        await fixture.store.prepare_execution_epoch(
            capacity_session,
            fixture.request,
            actor="activation-operator",
            idempotency_key=UUID(int=729),
        )

    binding = fixture.request.executors[0]
    with pytest.raises(ExecutionConflictError, match="only while prepared"):
        await fixture.store.register_execution_executor(
            capacity_session,
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
            idempotency_key=UUID(int=741),
        )


async def test_database_rejects_fabricated_prepared_writer_retirement(
    capacity_session: AsyncSession,
) -> None:
    """Prepared writer replacement accepts only its exact derived evidence."""

    fixture = await _setup(capacity_session, execution_policy=_policy())
    prepared = await fixture.store.prepare_execution_epoch(
        capacity_session,
        fixture.request,
        actor="activation-operator",
        idempotency_key=UUID(int=7291),
    )
    row = (
        await capacity_session.execute(
            select(CapacityExecutionEpoch).where(
                CapacityExecutionEpoch.execution_epoch == prepared.execution_epoch
            )
        )
    ).scalar_one()

    with pytest.raises(DBAPIError, match="prepared execution retirement evidence"):
        async with capacity_session.begin_nested():
            row.state = "retired"
            row.current_writer_epoch = fixture.writer.writer_epoch + 1
            row.retirement_actor = "fabricated-writer-replacement"
            row.retirement_idempotency_key = UUID(int=7292)
            row.retirement_request_digest = "f" * 64
            row.retirement_request_payload = {"schema_version": 2}
            row.retired_at = datetime.now(UTC)
            await capacity_session.flush()


async def test_active_writer_restart_clamps_to_drain_only_and_refences(
    capacity_session: AsyncSession,
) -> None:
    """A replacement writer must atomically remove stale scale-up authority."""

    fixture = await _setup(capacity_session, execution_policy=_policy())
    prepared = await fixture.store.prepare_execution_epoch(
        capacity_session,
        fixture.request,
        actor="activation-operator",
        idempotency_key=UUID(int=736),
    )
    await _register_execution_executors(capacity_session, fixture, prepared)
    await fixture.store.activate_execution_epoch(
        capacity_session,
        ExecutionActivationV2(
            authority_incarnation=AUTHORITY_ID,
            expected_writer_epoch=fixture.writer.writer_epoch,
            execution_epoch=prepared.execution_epoch,
            execution_manifest_sha256=prepared.execution_manifest_sha256,
            executable_new_capacity_ceiling=1,
            executable_new_capacity_rate_per_minute=1,
        ),
        actor="activation-operator",
        idempotency_key=UUID(int=737),
    )

    successor = await fixture.store.register_writer(
        capacity_session,
        AUTHORITY_ID,
        expected_epoch=fixture.writer.writer_epoch,
    )

    authority = await fixture.store.execution_authority(capacity_session)
    assert authority is not None
    assert authority.execution_state == "drain-only"
    assert authority.executable_new_capacity_ceiling == 0
    assert authority.executable_new_capacity_rate_per_minute == 0
    assert authority.writer_epoch == successor.writer_epoch
    row = (await capacity_session.execute(select(CapacityExecutionEpoch))).scalar_one()
    assert row.state == "drain-only"
    assert row.current_writer_epoch == successor.writer_epoch
    with pytest.raises(StaleWriterError):
        await fixture.store.load_allocation_input(capacity_session, fixture.writer)

    replacement = await fixture.store.register_writer(
        capacity_session,
        AUTHORITY_ID,
        expected_epoch=successor.writer_epoch,
    )
    authority = await fixture.store.execution_authority(capacity_session)
    assert authority is not None
    assert authority.execution_state == "drain-only"
    assert authority.writer_epoch == replacement.writer_epoch
    await capacity_session.refresh(row)
    assert row.current_writer_epoch == replacement.writer_epoch
    with pytest.raises(StaleWriterError):
        await fixture.store.load_allocation_input(capacity_session, successor)


async def test_dry_run_executor_registration_is_not_executable_provenance(
    capacity_session: AsyncSession,
) -> None:
    """A v1 rehearsal registration must never authorize a v2 activation."""

    fixture = await _setup(capacity_session, execution_policy=_policy())
    prepared = await fixture.store.prepare_execution_epoch(
        capacity_session,
        fixture.request,
        actor="activation-operator",
        idempotency_key=UUID(int=730),
    )
    activation = ExecutionActivationV2(
        authority_incarnation=AUTHORITY_ID,
        expected_writer_epoch=fixture.writer.writer_epoch,
        execution_epoch=prepared.execution_epoch,
        execution_manifest_sha256=prepared.execution_manifest_sha256,
        executable_new_capacity_ceiling=1,
        executable_new_capacity_rate_per_minute=1,
    )

    with pytest.raises(DBAPIError, match="executable executor evidence"):
        async with capacity_session.begin_nested():
            await capacity_session.execute(
                update(CapacityExecutionEpoch).values(
                    state="active",
                    effective_ceiling=1,
                    effective_rate_per_minute=1,
                    activation_actor="forged-operator",
                    activation_idempotency_key=UUID(int=732),
                    activation_request_digest="9" * 64,
                    activated_at=datetime.now(UTC),
                )
            )

    with pytest.raises(ExecutionConflictError, match="executable executor"):
        await fixture.store.activate_execution_epoch(
            capacity_session,
            activation,
            actor="activation-operator",
            idempotency_key=UUID(int=731),
        )


async def test_activation_is_atomic_and_exact_replay(
    capacity_session: AsyncSession,
) -> None:
    """Activation must update the epoch and singleton as one exact transaction."""

    fixture = await _setup(capacity_session, execution_policy=_policy())
    prepared = await fixture.store.prepare_execution_epoch(
        capacity_session,
        fixture.request,
        actor="activation-operator",
        idempotency_key=UUID(int=725),
    )
    activation = ExecutionActivationV2(
        authority_incarnation=AUTHORITY_ID,
        expected_writer_epoch=fixture.writer.writer_epoch,
        execution_epoch=prepared.execution_epoch,
        execution_manifest_sha256=prepared.execution_manifest_sha256,
        executable_new_capacity_ceiling=1,
        executable_new_capacity_rate_per_minute=1,
    )
    await _register_execution_executors(capacity_session, fixture, prepared)
    with pytest.raises(DBAPIError, match="append-only"):
        async with capacity_session.begin_nested():
            await capacity_session.execute(
                update(CapacityExecutionExecutor).values(local_authority_sha256="9" * 64)
            )
    first = await fixture.store.activate_execution_epoch(
        capacity_session,
        activation,
        actor="activation-operator",
        idempotency_key=UUID(int=726),
    )
    replay = await fixture.store.activate_execution_epoch(
        capacity_session,
        activation,
        actor="activation-operator",
        idempotency_key=UUID(int=726),
    )

    assert replay == first
    assert first.execution_state == "active"
    assert first.executable_new_capacity_ceiling == 1
    authority = await fixture.store.execution_authority(capacity_session)
    assert authority == first
    row = (await capacity_session.execute(select(CapacityExecutionEpoch))).scalar_one()
    assert row.state == "active"
    assert row.effective_ceiling == 1
    singleton = (await capacity_session.execute(select(CapacityAuthorityState))).scalar_one()
    assert singleton.increase_freeze is False

    with pytest.raises(DBAPIError, match="without transition"):
        async with capacity_session.begin_nested():
            await capacity_session.execute(
                update(CapacityExecutionEpoch).values(activation_actor="forged-operator")
            )

    changed = activation.model_copy(update={"executable_new_capacity_ceiling": 2})
    with pytest.raises(IdempotencyConflictError):
        await fixture.store.activate_execution_epoch(
            capacity_session,
            changed,
            actor="activation-operator",
            idempotency_key=UUID(int=726),
        )

    with pytest.raises(AuthorityRecoveryError, match="owner policy"):
        await CapacityManagementStore().execution_authority(capacity_session)


async def test_activation_replay_revalidates_policy_and_current_writer_fence(
    capacity_session: AsyncSession,
) -> None:
    """An old activation idempotency key cannot survive policy or writer drift."""

    fixture, _, activation, _ = await _activate_fixture(capacity_session)
    drifted_policy = _policy().model_copy(update={"rollback_evidence_sha256": "7" * 64})
    drifted_store = CapacityManagementStore(execution_policy=drifted_policy)
    with pytest.raises(ExecutionConflictError, match="owner policy"):
        await drifted_store.activate_execution_epoch(
            capacity_session,
            activation,
            actor="activation-operator",
            idempotency_key=UUID(int=781),
        )

    await fixture.store.register_writer(
        capacity_session,
        AUTHORITY_ID,
        expected_epoch=fixture.writer.writer_epoch,
    )
    with pytest.raises(ExecutionConflictError, match="exact active"):
        await fixture.store.activate_execution_epoch(
            capacity_session,
            activation,
            actor="activation-operator",
            idempotency_key=UUID(int=781),
        )


async def test_database_activation_rejects_executor_authority_hash_drift(
    capacity_session: AsyncSession,
) -> None:
    """Matching names cannot substitute for signed executor authority bindings."""

    fixture = await _setup(capacity_session, execution_policy=_policy())
    prepared = await fixture.store.prepare_execution_epoch(
        capacity_session,
        fixture.request,
        actor="activation-operator",
        idempotency_key=UUID(int=783),
    )
    for index, binding in enumerate(fixture.request.executors, start=1):
        capacity_session.add(
            CapacityExecutionExecutor(
                execution_epoch=prepared.execution_epoch,
                execution_manifest_sha256=prepared.execution_manifest_sha256,
                executor_id=binding.executor_id,
                executor_incarnation=binding.executor_incarnation,
                pool_id=binding.pool_id,
                pool_generation=binding.pool_generation,
                signing_key_id=f"{binding.pool_id}-key",
                signing_key_sha256=str(index) * 64,
                local_authority_sha256=str(index + 2) * 64,
                controller_authority_sha256=str(index + 4) * 64,
                actor="forged-installer",
                idempotency_key=UUID(int=783 + index),
                registration_digest=str(index + 6) * 64,
                registration_payload={},
            )
        )
    await capacity_session.flush()

    with pytest.raises(DBAPIError, match="executable executor evidence"):
        async with capacity_session.begin_nested():
            await capacity_session.execute(
                update(CapacityExecutionEpoch).values(
                    state="active",
                    effective_ceiling=1,
                    effective_rate_per_minute=1,
                    activation_actor="forged-operator",
                    activation_idempotency_key=UUID(int=786),
                    activation_request_digest="9" * 64,
                    activated_at=datetime.now(UTC),
                )
            )


async def test_executor_registration_rejects_a_second_key_for_the_same_pool(
    capacity_session: AsyncSession,
) -> None:
    """A duplicate pool registration must be a domain conflict, not a raw DB error."""

    fixture = await _setup(capacity_session, execution_policy=_policy())
    prepared = await fixture.store.prepare_execution_epoch(
        capacity_session,
        fixture.request,
        actor="activation-operator",
        idempotency_key=UUID(int=787),
    )
    await _register_execution_executors(capacity_session, fixture, prepared)
    binding = fixture.request.executors[0]

    with pytest.raises(ExecutionConflictError, match="already registered"):
        await fixture.store.register_execution_executor(
            capacity_session,
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
            idempotency_key=UUID(int=788),
        )


async def test_database_rejects_executor_bound_to_another_manifest(
    capacity_session: AsyncSession,
) -> None:
    """A mismatched manifest cannot permanently occupy an epoch's pool slot."""

    fixture = await _setup(capacity_session, execution_policy=_policy())
    prepared = await fixture.store.prepare_execution_epoch(
        capacity_session,
        fixture.request,
        actor="activation-operator",
        idempotency_key=UUID(int=789),
    )
    binding = fixture.request.executors[0]

    with pytest.raises(DBAPIError, match="epoch_manifest"):
        async with capacity_session.begin_nested():
            capacity_session.add(
                CapacityExecutionExecutor(
                    execution_epoch=prepared.execution_epoch,
                    execution_manifest_sha256="9" * 64,
                    executor_id=binding.executor_id,
                    executor_incarnation=binding.executor_incarnation,
                    pool_id=binding.pool_id,
                    pool_generation=binding.pool_generation,
                    signing_key_id=f"{binding.pool_id}-key",
                    signing_key_sha256=binding.signing_key_sha256,
                    local_authority_sha256=binding.local_authority_sha256,
                    controller_authority_sha256=binding.controller_authority_sha256,
                    actor="forged-installer",
                    idempotency_key=UUID(int=790),
                    registration_digest="8" * 64,
                    registration_payload={},
                )
            )
            await capacity_session.flush()


async def test_database_activation_ignores_temporary_executor_table_shadow(
    capacity_session: AsyncSession,
) -> None:
    """Session-local relations cannot replace durable executor evidence."""

    fixture = await _setup(capacity_session, execution_policy=_policy())
    prepared = await fixture.store.prepare_execution_epoch(
        capacity_session,
        fixture.request,
        actor="activation-operator",
        idempotency_key=UUID(int=791),
    )
    fake_rows = [
        {
            "execution_epoch": prepared.execution_epoch,
            "execution_manifest_sha256": prepared.execution_manifest_sha256,
            "pool_id": binding.pool_id,
            "executor_id": binding.executor_id,
            "executor_incarnation": binding.executor_incarnation,
            "pool_generation": binding.pool_generation,
            "signing_key_sha256": binding.signing_key_sha256,
            "local_authority_sha256": binding.local_authority_sha256,
            "controller_authority_sha256": binding.controller_authority_sha256,
        }
        for binding in fixture.request.executors
    ]

    with pytest.raises(DBAPIError, match="executable executor evidence"):
        async with capacity_session.begin_nested():
            await capacity_session.execute(
                text(
                    "CREATE TEMPORARY TABLE capacity_execution_executors ("
                    "execution_epoch bigint NOT NULL, "
                    "execution_manifest_sha256 text NOT NULL, "
                    "pool_id text NOT NULL, executor_id text NOT NULL, "
                    "executor_incarnation uuid NOT NULL, pool_generation bigint NOT NULL, "
                    "signing_key_sha256 text NOT NULL, local_authority_sha256 text NOT NULL, "
                    "controller_authority_sha256 text NOT NULL) ON COMMIT DROP"
                )
            )
            await capacity_session.execute(
                text(
                    "INSERT INTO capacity_execution_executors ("
                    "execution_epoch, execution_manifest_sha256, pool_id, executor_id, "
                    "executor_incarnation, pool_generation, signing_key_sha256, "
                    "local_authority_sha256, controller_authority_sha256) VALUES ("
                    ":execution_epoch, :execution_manifest_sha256, :pool_id, :executor_id, "
                    ":executor_incarnation, :pool_generation, :signing_key_sha256, "
                    ":local_authority_sha256, :controller_authority_sha256)"
                ),
                fake_rows,
            )
            now = datetime.now(UTC)
            await capacity_session.execute(
                update(CapacityExecutionEpoch).values(
                    state="active",
                    effective_ceiling=1,
                    effective_rate_per_minute=1,
                    activation_actor="forged-operator",
                    activation_idempotency_key=UUID(int=792),
                    activation_request_digest="9" * 64,
                    activated_at=now,
                )
            )
            await capacity_session.execute(
                update(CapacityAuthorityState).values(
                    execution_state="active",
                    executable_new_capacity_ceiling=1,
                )
            )


async def test_recovery_rejects_corrupted_executor_authority_hashes(
    capacity_session: AsyncSession,
) -> None:
    """Recovery must verify more than the presence of both pool names."""

    fixture, _, _, _ = await _activate_fixture(capacity_session)
    await capacity_session.execute(
        text("ALTER TABLE capacity_execution_executors DISABLE TRIGGER USER")
    )
    try:
        await capacity_session.execute(
            update(CapacityExecutionExecutor)
            .where(CapacityExecutionExecutor.pool_id == "gb10")
            .values(controller_authority_sha256="9" * 64)
        )
    finally:
        await capacity_session.execute(
            text("ALTER TABLE capacity_execution_executors ENABLE TRIGGER USER")
        )

    with pytest.raises(AuthorityRecoveryError, match="executor binding"):
        await fixture.store.execution_authority(capacity_session)


async def test_database_rejects_detaching_an_active_authority_to_shadow(
    capacity_session: AsyncSession,
) -> None:
    """Nullable shadow bindings cannot bypass the execution transition graph."""

    await _activate_fixture(capacity_session)
    with pytest.raises(DBAPIError, match="authority execution transition"):
        async with capacity_session.begin_nested():
            await capacity_session.execute(
                update(CapacityAuthorityState).values(
                    execution_epoch=0,
                    execution_state="shadow",
                    execution_manifest_sha256=None,
                    executable_new_capacity_ceiling=0,
                )
            )


async def test_operator_drain_is_exact_replay_and_keeps_the_writer(
    capacity_session: AsyncSession,
) -> None:
    """A duplicate drain may converge, but no changed idempotency identity may."""

    fixture, active = await _activate_execution(capacity_session)
    request = _drain_request(active)
    first = await fixture.store.begin_execution_drain(
        capacity_session,
        request,
        actor="activation-operator",
        idempotency_key=UUID(int=12001),
    )
    replay = await fixture.store.begin_execution_drain(
        capacity_session,
        request,
        actor="activation-operator",
        idempotency_key=UUID(int=12001),
    )

    assert replay == first
    assert (
        first.execution_state,
        first.executable_new_capacity_ceiling,
        first.executable_new_capacity_rate_per_minute,
        first.writer_epoch,
    ) == ("drain-only", 0, 0, active.writer_epoch)
    singleton = (await capacity_session.execute(select(CapacityAuthorityState))).scalar_one()
    assert singleton.increase_freeze is True

    changed = request.model_copy(
        update={
            "expected_executable_new_capacity_ceiling": (
                request.expected_executable_new_capacity_ceiling + 1
            )
        }
    )
    with pytest.raises(IdempotencyConflictError):
        await fixture.store.begin_execution_drain(
            capacity_session,
            changed,
            actor="activation-operator",
            idempotency_key=UUID(int=12001),
        )
    with pytest.raises(IdempotencyConflictError):
        await fixture.store.begin_execution_drain(
            capacity_session,
            request,
            actor="another-operator",
            idempotency_key=UUID(int=12001),
        )
    with pytest.raises(ExecutionConflictError):
        await fixture.store.begin_execution_drain(
            capacity_session,
            request,
            actor="activation-operator",
            idempotency_key=UUID(int=12003),
        )


async def test_operator_drain_then_safe_retirement_returns_shadow(
    capacity_session: AsyncSession,
) -> None:
    """Only both final inventories and their later exact heartbeats free authority."""

    fixture, active = await _activate_execution(capacity_session)
    drained = await fixture.store.begin_execution_drain(
        capacity_session,
        _drain_request(active),
        actor="activation-operator",
        idempotency_key=UUID(int=12001),
    )
    checkpoints = await _publish_final_safe_evidence(capacity_session, drained)
    retired = await fixture.store.retire_execution_epoch(
        capacity_session,
        _retirement_request(drained, checkpoints),
        actor="activation-operator",
        idempotency_key=UUID(int=12002),
    )

    assert retired.execution_epoch == drained.execution_epoch
    assert retired.execution_manifest_sha256 == drained.execution_manifest_sha256
    assert retired.retired_at.tzinfo is not None
    assert retired.replayed is False
    assert await fixture.store.execution_authority(capacity_session) is None
    singleton = (await capacity_session.execute(select(CapacityAuthorityState))).scalar_one()
    assert (
        singleton.execution_state,
        singleton.execution_epoch,
        singleton.execution_manifest_sha256,
        singleton.executable_new_capacity_ceiling,
        singleton.increase_freeze,
    ) == ("shadow", 0, None, 0, True)
    epoch = (await capacity_session.execute(select(CapacityExecutionEpoch))).scalar_one()
    assert (epoch.state, epoch.effective_ceiling, epoch.effective_rate_per_minute) == (
        "retired",
        0,
        0,
    )


async def test_retirement_replay_is_exact_after_shadow_restoration(
    capacity_session: AsyncSession,
) -> None:
    """Only the original retirement identity may replay after authority is shadow."""

    fixture, active = await _activate_execution(capacity_session)
    drained = await fixture.store.begin_execution_drain(
        capacity_session,
        _drain_request(active),
        actor="activation-operator",
        idempotency_key=UUID(int=12001),
    )
    checkpoints = await _publish_final_safe_evidence(capacity_session, drained)
    request = _retirement_request(drained, checkpoints)
    first = await fixture.store.retire_execution_epoch(
        capacity_session,
        request,
        actor="activation-operator",
        idempotency_key=UUID(int=12002),
    )
    replay = await fixture.store.retire_execution_epoch(
        capacity_session,
        request,
        actor="activation-operator",
        idempotency_key=UUID(int=12002),
    )

    assert replay.execution_epoch == first.execution_epoch
    assert replay.execution_manifest_sha256 == first.execution_manifest_sha256
    assert replay.retired_at == first.retired_at
    assert replay.replayed is True
    changed_checkpoint = checkpoints[0].model_copy(update={"heartbeat_sequence": 3})
    changed = request.model_copy(
        update={"executor_checkpoints": (changed_checkpoint, checkpoints[1])}
    )
    with pytest.raises(IdempotencyConflictError):
        await fixture.store.retire_execution_epoch(
            capacity_session,
            changed,
            actor="activation-operator",
            idempotency_key=UUID(int=12002),
        )
    with pytest.raises(IdempotencyConflictError):
        await fixture.store.retire_execution_epoch(
            capacity_session,
            request,
            actor="another-operator",
            idempotency_key=UUID(int=12002),
        )
    with pytest.raises(ExecutionConflictError):
        await fixture.store.retire_execution_epoch(
            capacity_session,
            request,
            actor="activation-operator",
            idempotency_key=UUID(int=12004),
        )
