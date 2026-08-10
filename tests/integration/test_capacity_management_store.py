"""Serializable configuration, reporting, and shadow-ledger store tests."""

from __future__ import annotations

from uuid import UUID

import pytest
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine

from loom_capacity_manager.models import (
    CapacityAllocationEpoch,
    CapacityAuthorityState,
    CapacityConfigurationEpoch,
    CapacityDemandReporter,
    CapacityObservedCommitment,
    CapacityWorkerProfile,
)
from loom_capacity_manager.store import (
    CapacityManagementStore,
    CapacityStoreError,
    ConfigurationConflictError,
    IdempotencyConflictError,
    ReportEquivocationError,
    StaleReportError,
    StaleWriterError,
    WriterFence,
)
from tests.capacity_fixtures import (
    ACTIVATION_KEY,
    AUTHORITY_ID,
    CONFIG_KEY_A,
    CONFIG_KEY_B,
    configuration_activation,
    demand_snapshot,
    fleet_manifest,
    pool_observation,
    resource_vector,
    shadow_epoch,
    subject_configuration,
)


async def _activate_default(
    session: AsyncSession,
    *,
    fleet=None,
    subject=None,
) -> tuple[CapacityManagementStore, object]:
    store = CapacityManagementStore()
    fleet_proposal = await store.propose_fleet_configuration(
        session,
        fleet or fleet_manifest(),
        actor="fleet-operator",
        idempotency_key=CONFIG_KEY_A,
    )
    subject_proposal = await store.propose_subject_configuration(
        session,
        subject or subject_configuration(),
        actor="environment-state",
        idempotency_key=CONFIG_KEY_B,
    )
    active = await store.activate_configuration(
        session,
        configuration_activation(fleet=fleet_proposal, subjects=(subject_proposal,)),
        actor="fleet-operator",
        idempotency_key=ACTIVATION_KEY,
    )
    return store, active


async def _authority_uuid(session: AsyncSession) -> UUID:
    return (
        await session.execute(
            select(CapacityAuthorityState.authority_incarnation).where(
                CapacityAuthorityState.singleton_id == 1
            )
        )
    ).scalar_one()


async def _allocation_epoch_count(session: AsyncSession) -> int:
    return int(
        (
            await session.execute(select(func.count()).select_from(CapacityAllocationEpoch))
        ).scalar_one()
    )


async def test_configuration_proposal_replay_is_idempotent_but_payload_reuse_fails(
    capacity_session: AsyncSession,
) -> None:
    store = CapacityManagementStore()
    manifest = fleet_manifest()
    first = await store.propose_fleet_configuration(
        capacity_session,
        manifest,
        actor="fleet-operator",
        idempotency_key=CONFIG_KEY_A,
    )
    replay = await store.propose_fleet_configuration(
        capacity_session,
        manifest,
        actor="fleet-operator",
        idempotency_key=CONFIG_KEY_A,
    )
    assert replay == first

    changed = manifest.model_copy(update={"global_max_pending_slots": 31})
    with pytest.raises(IdempotencyConflictError):
        await store.propose_fleet_configuration(
            capacity_session,
            changed,
            actor="fleet-operator",
            idempotency_key=CONFIG_KEY_A,
        )


async def test_mutation_rejects_nonserializable_database_session(
    capacity_postgres_url: str,
) -> None:
    engine = create_async_engine(capacity_postgres_url)
    async with engine.connect() as connection:
        outer = await connection.begin()
        session = AsyncSession(
            bind=connection,
            expire_on_commit=False,
            join_transaction_mode="create_savepoint",
        )
        try:
            with pytest.raises(CapacityStoreError, match="SERIALIZABLE"):
                await CapacityManagementStore().propose_fleet_configuration(
                    session,
                    fleet_manifest(),
                    actor="fleet-operator",
                    idempotency_key=CONFIG_KEY_A,
                )
        finally:
            await session.close()
            await outer.rollback()
    await engine.dispose()


async def test_incompatible_configuration_proposals_never_become_partly_active(
    capacity_session: AsyncSession,
) -> None:
    store = CapacityManagementStore()
    fleet = await store.propose_fleet_configuration(
        capacity_session,
        fleet_manifest(pool_generation=2),
        actor="fleet-operator",
        idempotency_key=CONFIG_KEY_A,
    )
    subject = await store.propose_subject_configuration(
        capacity_session,
        subject_configuration(),
        actor="environment-state",
        idempotency_key=CONFIG_KEY_B,
    )
    with pytest.raises(ConfigurationConflictError, match="pool generation"):
        await store.activate_configuration(
            capacity_session,
            configuration_activation(fleet=fleet, subjects=(subject,)),
            actor="fleet-operator",
            idempotency_key=ACTIVATION_KEY,
        )
    assert (
        await capacity_session.execute(select(func.count()).select_from(CapacityConfigurationEpoch))
    ).scalar_one() == 0


async def test_activation_registers_exact_reporter_incarnations(
    capacity_session: AsyncSession,
) -> None:
    _, active = await _activate_default(capacity_session)
    assert active.configuration_epoch == 1
    reporters = (await capacity_session.execute(select(CapacityDemandReporter))).scalars().all()
    assert len(reporters) == 1
    assert reporters[0].state == "current"
    assert reporters[0].high_water == 0


async def test_activation_idempotency_replays_before_epoch_conflict(
    capacity_session: AsyncSession,
) -> None:
    store = CapacityManagementStore()
    fleet = await store.propose_fleet_configuration(
        capacity_session,
        fleet_manifest(),
        actor="fleet-operator",
        idempotency_key=CONFIG_KEY_A,
    )
    subject = await store.propose_subject_configuration(
        capacity_session,
        subject_configuration(),
        actor="environment-state",
        idempotency_key=CONFIG_KEY_B,
    )
    proposal = configuration_activation(fleet=fleet, subjects=(subject,))
    first = await store.activate_configuration(
        capacity_session,
        proposal,
        actor="fleet-operator",
        idempotency_key=ACTIVATION_KEY,
    )
    replay = await store.activate_configuration(
        capacity_session,
        proposal,
        actor="fleet-operator",
        idempotency_key=ACTIVATION_KEY,
    )

    assert replay == first
    with pytest.raises(IdempotencyConflictError):
        await store.activate_configuration(
            capacity_session,
            proposal,
            actor="different-operator",
            idempotency_key=ACTIVATION_KEY,
        )


async def test_activation_materializes_exact_worker_profile_bindings(
    capacity_session: AsyncSession,
) -> None:
    await _activate_default(capacity_session)

    profiles = (
        (
            await capacity_session.execute(
                select(CapacityWorkerProfile).order_by(CapacityWorkerProfile.pool_id)
            )
        )
        .scalars()
        .all()
    )

    assert [item.pool_id for item in profiles] == ["gb10", "oldlab"]
    assert all(item.pool_generation == 1 for item in profiles)
    assert all(item.profile_generation == 1 for item in profiles)
    assert all(len(item.shape_catalog) == 1 for item in profiles)


async def test_exact_report_replay_is_idempotent_but_equivocation_fences(
    capacity_session: AsyncSession,
) -> None:
    store, _ = await _activate_default(capacity_session)
    report = demand_snapshot(sequence=7)
    first = await store.ingest_demand_snapshot(capacity_session, report, actor="dev-a")
    replay = await store.ingest_demand_snapshot(capacity_session, report, actor="dev-a")
    assert first.snapshot_id == replay.snapshot_id
    assert replay.replayed

    changed = report.model_copy(update={"pending_unassigned": ()})
    with pytest.raises(ReportEquivocationError):
        await store.ingest_demand_snapshot(capacity_session, changed, actor="dev-a")
    state = (
        await capacity_session.execute(
            select(CapacityDemandReporter.state).where(
                CapacityDemandReporter.reporter_incarnation == report.reporter_incarnation
            )
        )
    ).scalar_one()
    assert state == "equivocal"


async def test_lower_report_sequence_is_rejected_without_changing_high_water(
    capacity_session: AsyncSession,
) -> None:
    store, _ = await _activate_default(capacity_session)
    await store.ingest_demand_snapshot(capacity_session, demand_snapshot(sequence=2), actor="dev-a")
    with pytest.raises(StaleReportError):
        await store.ingest_demand_snapshot(
            capacity_session, demand_snapshot(sequence=1), actor="dev-a"
        )
    high_water = (
        await capacity_session.execute(select(CapacityDemandReporter.high_water))
    ).scalar_one()
    assert high_water == 2


async def test_newer_report_omission_cannot_release_observed_commitment(
    capacity_session: AsyncSession,
) -> None:
    store, _ = await _activate_default(capacity_session)
    writer = await store.register_writer(
        capacity_session, await _authority_uuid(capacity_session), expected_epoch=0
    )
    await store.ingest_demand_snapshot(
        capacity_session,
        demand_snapshot(sequence=1, fixed_claim_ids=("claim-a",)),
        actor="dev-a",
    )
    await store.ingest_demand_snapshot(
        capacity_session,
        demand_snapshot(sequence=2, fixed_claim_ids=()),
        actor="dev-a",
    )
    allocation = await store.load_allocation_input(capacity_session, writer)
    assert allocation.observed_commitment_ids == ("claim-a",)
    states = (
        (await capacity_session.execute(select(CapacityObservedCommitment.state))).scalars().all()
    )
    assert states == ["observed"]


async def test_pool_observation_replay_is_idempotent(
    capacity_session: AsyncSession,
) -> None:
    store, _ = await _activate_default(capacity_session)
    observation = pool_observation(sequence=1, commitment_ids=("worker-a",))
    first = await store.ingest_pool_observation(
        capacity_session, observation, actor="gb10-reporter"
    )
    replay = await store.ingest_pool_observation(
        capacity_session, observation, actor="gb10-reporter"
    )
    assert first.snapshot_id == replay.snapshot_id
    assert replay.replayed


async def test_newer_pool_report_can_advance_commitment_lifecycle_state(
    capacity_session: AsyncSession,
) -> None:
    store, _ = await _activate_default(capacity_session)
    first = pool_observation(sequence=1, commitment_ids=("worker-a",))
    second = pool_observation(sequence=2).model_copy(
        update={"commitments": (first.commitments[0].model_copy(update={"state": "live"}),)}
    )

    await store.ingest_pool_observation(capacity_session, first, actor="gb10-reporter")
    await store.ingest_pool_observation(capacity_session, second, actor="gb10-reporter")

    rows = (await capacity_session.execute(select(CapacityObservedCommitment))).scalars().all()
    assert len(rows) == 1
    assert rows[0].state == "live"
    assert rows[0].binding_payload["observed_contract"]["state"] == "live"


async def test_conflicting_commitment_evidence_is_charged_separately(
    capacity_session: AsyncSession,
) -> None:
    store, _ = await _activate_default(capacity_session)
    first = demand_snapshot(sequence=1, fixed_claim_ids=("claim-a",))
    changed_claim = first.fixed_claims[0].model_copy(
        update={"resources": resource_vector(cpu_millicores=2_000)}
    )
    second = demand_snapshot(sequence=2, fixed_claim_ids=()).model_copy(
        update={"fixed_claims": (changed_claim,)}
    )
    await store.ingest_demand_snapshot(capacity_session, first, actor="dev-a")
    await store.ingest_demand_snapshot(capacity_session, second, actor="dev-a")
    rows = (
        (
            await capacity_session.execute(
                select(CapacityObservedCommitment).order_by(
                    CapacityObservedCommitment.commitment_identity
                )
            )
        )
        .scalars()
        .all()
    )
    assert len(rows) == 2
    assert {row.state for row in rows} == {"quarantined"}


async def test_stale_writer_cannot_commit_complete_shadow_epoch(
    capacity_session: AsyncSession,
) -> None:
    store, _ = await _activate_default(capacity_session)
    authority = await _authority_uuid(capacity_session)
    old = await store.register_writer(capacity_session, authority, expected_epoch=0)
    allocation_input = await store.load_allocation_input(capacity_session, old)
    new = await store.register_writer(capacity_session, authority, expected_epoch=old.writer_epoch)
    with pytest.raises(StaleWriterError):
        await store.commit_shadow_epoch(capacity_session, old, shadow_epoch(allocation_input))
    assert await _allocation_epoch_count(capacity_session) == 0
    assert new.writer_epoch == old.writer_epoch + 1


async def test_status_page_rejects_unbounded_limit(capacity_session: AsyncSession) -> None:
    store, _ = await _activate_default(capacity_session)
    with pytest.raises(ValueError, match="limit"):
        await store.status(capacity_session, cursor=None, limit=501)


def test_writer_fence_is_immutable() -> None:
    fence = WriterFence(authority_incarnation=AUTHORITY_ID, writer_epoch=1)
    with pytest.raises(AttributeError):
        fence.writer_epoch = 2
