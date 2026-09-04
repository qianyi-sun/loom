"""Serializable configuration, reporting, and shadow-ledger store tests."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from threading import Event
from uuid import UUID, uuid4

import pytest
from sqlalchemy import delete, event, func, select, text, update
from sqlalchemy.ext.asyncio import (
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from loom_capacity_manager.allocator import ShadowAllocatorError, allocate_shadow
from loom_capacity_manager.contracts import (
    ConfigurationActivationV1,
    ConfigurationGenerationRefV1,
    ConfigurationRollbackV1,
    DynamicDevelopmentSubjectProjectionV1,
    ObservedCommitmentV1,
    StaticCandidateProvenanceV1,
    canonical_digest,
    canonical_digest_excluding,
)
from loom_capacity_manager.executable_contracts import (
    ExecutionPreparationPolicyV2,
    ExecutionPreparationV2,
    LegacyWriterFenceV2,
    PoolControllerAuthorityV2,
    SubjectExecutionAcknowledgementV2,
)
from loom_capacity_manager.execution_store import CapacityExecutionStore
from loom_capacity_manager.models import (
    Base,
    CapacityAccountPolicy,
    CapacityAllocation,
    CapacityAllocationEpoch,
    CapacityAuditEvent,
    CapacityAuthorityState,
    CapacityCandidate,
    CapacityConfigGeneration,
    CapacityConfigurationEpoch,
    CapacityDemandReporter,
    CapacityDeploymentGeneration,
    CapacityDevelopmentProjection,
    CapacityFairnessState,
    CapacityObservedCommitment,
    CapacityPool,
    CapacityPoolReporter,
    CapacitySubject,
    CapacityWorkerProfile,
)
from loom_capacity_manager.reconciler import reconcile_shadow_once
from loom_capacity_manager.store import (
    AuthorityRecoveryError,
    CapacityManagementStore,
    CapacityStoreError,
    ConfigurationConflictError,
    IdempotencyConflictError,
    ReportEquivocationError,
    StaleReportError,
    StaleWriterError,
    WriterFence,
    _deduplicate_observed_commitments,
)
from tests.capacity_execution_fixtures import (
    TRUSTED_RELEASE,
    PreparedExecutionFixture,
    executor_binding,
    ready_execution_activation,
    register_execution_executors,
)
from tests.capacity_fixtures import (
    ACTIVATION_KEY,
    AUTHORITY_ID,
    CONFIG_KEY_A,
    CONFIG_KEY_B,
    DEMAND_REPORTER_ID,
    SUBJECT_ID,
    SUBJECT_INCARNATION,
    configuration_activation,
    demand_snapshot,
    development_projection,
    fleet_manifest,
    fleet_with_development_template,
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


async def _configuration_epoch_row(
    session: AsyncSession,
    configuration_epoch: int,
) -> CapacityConfigurationEpoch:
    return (
        await session.execute(
            select(CapacityConfigurationEpoch).where(
                CapacityConfigurationEpoch.configuration_epoch == configuration_epoch
            )
        )
    ).scalar_one()


async def _configuration_rollback(
    session: AsyncSession,
    *,
    expected_configuration_epoch: int,
    restore_configuration_epoch: int,
    expected_configuration_digest: str | None = None,
    restore_configuration_digest: str | None = None,
    rollback_evidence_sha256: str = "7" * 64,
) -> ConfigurationRollbackV1:
    current = await _configuration_epoch_row(session, expected_configuration_epoch)
    restore = await _configuration_epoch_row(session, restore_configuration_epoch)
    return ConfigurationRollbackV1(
        expected_configuration_epoch=expected_configuration_epoch,
        expected_configuration_digest=(
            current.canonical_digest
            if expected_configuration_digest is None
            else expected_configuration_digest
        ),
        restore_configuration_epoch=restore_configuration_epoch,
        restore_configuration_digest=(
            restore.canonical_digest
            if restore_configuration_digest is None
            else restore_configuration_digest
        ),
        rollback_evidence_sha256=rollback_evidence_sha256,
    )


async def _rollback_ready_projection(
    session: AsyncSession,
) -> tuple[CapacityManagementStore, object, object, DynamicDevelopmentSubjectProjectionV1]:
    await session.execute(
        update(CapacityAuthorityState)
        .where(CapacityAuthorityState.singleton_id == 1)
        .values(authority_incarnation=AUTHORITY_ID)
    )
    await session.flush()
    fleet = fleet_with_development_template()
    store, active = await _activate_default(
        session,
        fleet=fleet,
        subject=subject_configuration(fleet),
    )
    projection = development_projection(expected_configuration_epoch=active.configuration_epoch)
    projected = await store.project_development_subject(
        session,
        projection,
        actor="personal-lifecycle",
        idempotency_key=uuid4(),
    )
    return store, active, projected, projection


async def _rollback_execution_fixture(
    session: AsyncSession,
) -> PreparedExecutionFixture:
    authority = (await session.execute(select(CapacityAuthorityState))).scalar_one()
    configuration = (
        (
            await session.execute(
                select(CapacityConfigurationEpoch).order_by(
                    CapacityConfigurationEpoch.configuration_epoch.desc()
                )
            )
        )
        .scalars()
        .first()
    )
    if configuration is None:
        raise RuntimeError("rollback execution fixture requires an active configuration")
    subjects = (
        (
            await session.execute(
                select(CapacitySubject)
                .where(CapacitySubject.configuration_epoch == configuration.configuration_epoch)
                .order_by(CapacitySubject.subject_id)
            )
        )
        .scalars()
        .all()
    )
    subject_acknowledgements: list[SubjectExecutionAcknowledgementV2] = []
    for index, subject_row in enumerate(subjects, start=1):
        candidate = await CapacityExecutionStore._current_candidate(session, subject_row)
        subject_acknowledgements.append(
            SubjectExecutionAcknowledgementV2(
                subject_id=subject_row.subject_id,
                subject_incarnation=subject_row.subject_incarnation,
                configuration_generation=subject_row.configuration_generation,
                deployment_generation=subject_row.deployment_generation,
                candidate=candidate,
                reporter_incarnation=subject_row.demand_reporter_incarnation,
                protected_admission_sha256=f"{index}" * 64,
                legacy_writer_high_water=0,
                acknowledgement_sha256=f"{index + 2}" * 64,
            )
        )
    executors = tuple(executor_binding(pool_id) for pool_id in ("gb10", "oldlab"))
    policy = ExecutionPreparationPolicyV2(
        trusted_fleet_release_sha256=TRUSTED_RELEASE,
        executable_new_capacity_ceiling=1,
        executable_new_capacity_rate_per_minute=1,
        executors=executors,
        subject_acknowledgements=tuple(subject_acknowledgements),
        rollback_evidence_sha256="6" * 64,
        controller_authorities=tuple(
            PoolControllerAuthorityV2(
                pool_id=item.pool_id,
                controller_authority_sha256=item.controller_authority_sha256,
            )
            for item in executors
        ),
        legacy_writer_fences=(
            LegacyWriterFenceV2(
                writer_id="global-dev-supervisor",
                writer_kind="allocation",
                scope_kind="global",
                scope_id="development",
                high_water=9,
                freeze_evidence_sha256="5" * 64,
                state="frozen",
            ),
        ),
    )
    store = CapacityManagementStore(execution_policy=policy)
    writer = await store.register_writer(
        session,
        authority.authority_incarnation,
        expected_epoch=authority.writer_epoch,
    )
    request = ExecutionPreparationV2(
        authority_incarnation=authority.authority_incarnation,
        expected_writer_epoch=writer.writer_epoch,
        configuration_epoch=configuration.configuration_epoch,
        fleet_generation=configuration.fleet_generation,
        fleet_digest=configuration.fleet_digest,
        trusted_fleet_release_sha256=policy.trusted_fleet_release_sha256,
        requested_ceiling=policy.executable_new_capacity_ceiling,
        requested_rate_per_minute=policy.executable_new_capacity_rate_per_minute,
        executors=policy.executors,
        subject_acknowledgements=policy.subject_acknowledgements,
        legacy_writer_fences=policy.legacy_writer_fences,
        rollback_evidence_sha256=policy.rollback_evidence_sha256,
    )
    return PreparedExecutionFixture(store=store, writer=writer, request=request)


async def _enter_execution_state(
    session: AsyncSession,
    *,
    target_state: str,
) -> None:
    fixture = await _rollback_execution_fixture(session)
    prepared = await fixture.store.prepare_execution_epoch(
        session,
        fixture.request,
        actor="activation-operator",
        idempotency_key=uuid4(),
    )
    if target_state == "prepared":
        return
    if target_state != "active":
        raise ValueError(f"unsupported execution state: {target_state}")
    await register_execution_executors(session, fixture, prepared)
    activation = await ready_execution_activation(
        session,
        fixture.store,
        fixture.request,
        prepared,
    )
    await fixture.store.activate_execution_epoch(
        session,
        activation,
        actor="activation-operator",
        idempotency_key=uuid4(),
    )


def test_authenticated_commitment_deduplication_requires_one_exact_reservation() -> None:
    authenticated = ObservedCommitmentV1(
        kind="physical",
        commitment_id="job-1",
        physical_identity="job-1",
        reservation_identity="intent-a",
        ownership_state="authenticated",
        subject_id=AUTHORITY_ID,
        subject_incarnation=UUID(int=20),
        deployment_generation=1,
        pool_id="gb10",
        pool_generation=1,
        profile_id="one-slot",
        profile_generation=1,
        profile_digest="a" * 64,
        shape_id="one-slot",
        resources=resource_vector(),
        state="pending",
    )
    unverified = authenticated.model_copy(
        update={"ownership_state": "unverified", "reservation_identity": None}
    )
    preferred = _deduplicate_observed_commitments([unverified, authenticated])
    assert preferred == (authenticated,)

    rebound = authenticated.model_copy(update={"reservation_identity": "intent-b"})
    conflicting = _deduplicate_observed_commitments([authenticated, rebound])
    assert len(conflicting) == 2
    assert {item.state for item in conflicting} == {"quarantined"}
    assert {item.ownership_state for item in conflicting} == {"unverified"}


async def _reset_committed_capacity_state(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    async with session_factory() as session, session.begin():
        await session.execute(
            text(
                "ALTER TABLE capacity_authority_state DISABLE TRIGGER "
                "capacity_authority_execution_transition_guard"
            )
        )
        for table in reversed(Base.metadata.sorted_tables):
            if table.name != CapacityAuthorityState.__tablename__:
                await session.execute(delete(table))
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
            )
        )
        await session.execute(
            text(
                "ALTER TABLE capacity_authority_state ENABLE TRIGGER "
                "capacity_authority_execution_transition_guard"
            )
        )


@pytest.fixture
async def registered_writer(
    capacity_session_factory: async_sessionmaker[AsyncSession],
) -> AsyncIterator[WriterFence]:
    await _reset_committed_capacity_state(capacity_session_factory)
    async with capacity_session_factory() as session:
        store, _ = await _activate_default(session)
        writer = await store.register_writer(
            session,
            await _authority_uuid(session),
            expected_epoch=0,
        )
        await store.ingest_demand_snapshot(
            session,
            demand_snapshot(sequence=1),
            actor="dev-a",
        )
        await store.ingest_pool_observation(
            session,
            pool_observation(sequence=1, pool_id="gb10"),
            actor="gb10-reporter",
        )
        await store.ingest_pool_observation(
            session,
            pool_observation(sequence=1, pool_id="oldlab"),
            actor="oldlab-reporter",
        )
        await session.commit()
    try:
        yield writer
    finally:
        await _reset_committed_capacity_state(capacity_session_factory)


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


async def test_activation_can_add_attested_static_subject_without_removing_existing(
    capacity_session: AsyncSession,
) -> None:
    store = CapacityManagementStore()
    fleet_proposal = await store.propose_fleet_configuration(
        capacity_session,
        fleet_manifest(),
        actor="fleet-operator",
        idempotency_key=uuid4(),
    )
    existing = subject_configuration()
    existing_proposal = await store.propose_subject_configuration(
        capacity_session,
        existing,
        actor="environment-state",
        idempotency_key=uuid4(),
    )
    first = await store.activate_configuration(
        capacity_session,
        configuration_activation(
            fleet=fleet_proposal,
            subjects=(existing_proposal,),
        ),
        actor="fleet-operator",
        idempotency_key=uuid4(),
    )
    added = subject_configuration(
        subject_id=UUID("00000000-0000-4000-8000-00000000f101"),
        subject_incarnation=UUID("00000000-0000-4000-8000-00000000f102"),
        demand_reporter_incarnation=UUID("00000000-0000-4000-8000-00000000f103"),
        display_name="staging-added",
    )
    added_proposal = await store.propose_subject_configuration(
        capacity_session,
        added,
        actor="environment-state",
        idempotency_key=uuid4(),
    )
    provenance = StaticCandidateProvenanceV1(
        subject_id=added.subject_id,
        subject_incarnation=added.subject_incarnation,
        candidate_generation=added.candidate_generation,
        algorithm="source-sha256",
        identity="3" * 64,
        publication_sha256="4" * 64,
    )

    activated = await store.activate_configuration(
        capacity_session,
        configuration_activation(
            fleet=fleet_proposal,
            subjects=(existing_proposal, added_proposal),
            expected_configuration_epoch=first.configuration_epoch,
            static_candidate_provenance=(provenance,),
        ),
        actor="fleet-operator",
        idempotency_key=uuid4(),
    )

    assert activated.configuration_epoch == 2
    assert {reference.subject_id for reference in activated.snapshot.subjects} == {
        existing.subject_id,
        added.subject_id,
    }
    assert {
        row.subject_id
        for row in (await capacity_session.execute(select(CapacitySubject))).scalars()
    } == {existing.subject_id, added.subject_id}


async def test_activation_rejects_incomplete_existing_subject_manifest(
    capacity_session: AsyncSession,
) -> None:
    store = CapacityManagementStore()
    fleet_proposal = await store.propose_fleet_configuration(
        capacity_session,
        fleet_manifest(),
        actor="fleet-operator",
        idempotency_key=uuid4(),
    )
    retained = subject_configuration()
    removed = subject_configuration(
        subject_id=UUID("00000000-0000-4000-8000-00000000f301"),
        subject_incarnation=UUID("00000000-0000-4000-8000-00000000f302"),
        demand_reporter_incarnation=UUID("00000000-0000-4000-8000-00000000f303"),
        display_name="must-remain",
    )
    retained_proposal = await store.propose_subject_configuration(
        capacity_session,
        retained,
        actor="environment-state",
        idempotency_key=uuid4(),
    )
    removed_proposal = await store.propose_subject_configuration(
        capacity_session,
        removed,
        actor="environment-state",
        idempotency_key=uuid4(),
    )
    first = await store.activate_configuration(
        capacity_session,
        configuration_activation(
            fleet=fleet_proposal,
            subjects=(retained_proposal, removed_proposal),
        ),
        actor="fleet-operator",
        idempotency_key=uuid4(),
    )

    with pytest.raises(ConfigurationConflictError, match="deletion is unavailable"):
        await store.activate_configuration(
            capacity_session,
            configuration_activation(
                fleet=fleet_proposal,
                subjects=(retained_proposal,),
                expected_configuration_epoch=first.configuration_epoch,
                static_candidate_provenance=(),
            ),
            actor="fleet-operator",
            idempotency_key=uuid4(),
        )


async def test_activation_requires_exact_provenance_for_new_static_subject(
    capacity_session: AsyncSession,
) -> None:
    store = CapacityManagementStore()
    fleet_proposal = await store.propose_fleet_configuration(
        capacity_session,
        fleet_manifest(),
        actor="fleet-operator",
        idempotency_key=uuid4(),
    )
    existing = subject_configuration()
    existing_proposal = await store.propose_subject_configuration(
        capacity_session,
        existing,
        actor="environment-state",
        idempotency_key=uuid4(),
    )
    first = await store.activate_configuration(
        capacity_session,
        configuration_activation(fleet=fleet_proposal, subjects=(existing_proposal,)),
        actor="fleet-operator",
        idempotency_key=uuid4(),
    )
    added = subject_configuration(
        subject_id=UUID("00000000-0000-4000-8000-00000000f401"),
        subject_incarnation=UUID("00000000-0000-4000-8000-00000000f402"),
        demand_reporter_incarnation=UUID("00000000-0000-4000-8000-00000000f403"),
        display_name="unattested-addition",
    )
    added_proposal = await store.propose_subject_configuration(
        capacity_session,
        added,
        actor="environment-state",
        idempotency_key=uuid4(),
    )

    with pytest.raises(ConfigurationConflictError, match="provenance is missing"):
        await store.activate_configuration(
            capacity_session,
            configuration_activation(
                fleet=fleet_proposal,
                subjects=(existing_proposal, added_proposal),
                expected_configuration_epoch=first.configuration_epoch,
                static_candidate_provenance=(),
            ),
            actor="fleet-operator",
            idempotency_key=uuid4(),
        )


async def test_activation_rejects_replacing_an_active_subject_incarnation(
    capacity_session: AsyncSession,
) -> None:
    store = CapacityManagementStore()
    fleet_proposal = await store.propose_fleet_configuration(
        capacity_session,
        fleet_manifest(),
        actor="fleet-operator",
        idempotency_key=uuid4(),
    )
    existing = subject_configuration()
    existing_proposal = await store.propose_subject_configuration(
        capacity_session,
        existing,
        actor="environment-state",
        idempotency_key=uuid4(),
    )
    first = await store.activate_configuration(
        capacity_session,
        configuration_activation(fleet=fleet_proposal, subjects=(existing_proposal,)),
        actor="fleet-operator",
        idempotency_key=uuid4(),
    )
    replacement = subject_configuration(
        subject_id=existing.subject_id,
        subject_incarnation=UUID("00000000-0000-4000-8000-00000000f202"),
        demand_reporter_incarnation=UUID("00000000-0000-4000-8000-00000000f203"),
        display_name="replacement",
    )
    replacement_proposal = await store.propose_subject_configuration(
        capacity_session,
        replacement,
        actor="environment-state",
        idempotency_key=uuid4(),
    )
    replacement_provenance = StaticCandidateProvenanceV1(
        subject_id=replacement.subject_id,
        subject_incarnation=replacement.subject_incarnation,
        candidate_generation=replacement.candidate_generation,
        algorithm="source-sha256",
        identity="3" * 64,
        publication_sha256="4" * 64,
    )

    with pytest.raises(ConfigurationConflictError, match="deletion is unavailable"):
        await store.activate_configuration(
            capacity_session,
            configuration_activation(
                fleet=fleet_proposal,
                subjects=(replacement_proposal,),
                expected_configuration_epoch=first.configuration_epoch,
                static_candidate_provenance=(replacement_provenance,),
            ),
            actor="fleet-operator",
            idempotency_key=uuid4(),
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


async def test_activation_accepts_changed_pool_digest_without_generation_bump(
    capacity_session: AsyncSession,
) -> None:
    store = CapacityManagementStore()
    initial_fleet = fleet_with_development_template()
    initial_subject = subject_configuration(initial_fleet)
    initial_fleet_proposal = await store.propose_fleet_configuration(
        capacity_session,
        initial_fleet,
        actor="fleet-operator",
        idempotency_key=uuid4(),
    )
    initial_subject_proposal = await store.propose_subject_configuration(
        capacity_session,
        initial_subject,
        actor="environment-state",
        idempotency_key=uuid4(),
    )
    first = await store.activate_configuration(
        capacity_session,
        configuration_activation(
            fleet=initial_fleet_proposal,
            subjects=(initial_subject_proposal,),
        ),
        actor="fleet-operator",
        idempotency_key=uuid4(),
    )

    initial_gb10 = next(pool for pool in initial_fleet.pools if pool.pool_id == "gb10")
    changed_gb10 = initial_gb10.model_copy(
        update={"max_slots": initial_gb10.max_slots - 1, "pool_digest": "0" * 64}
    )
    changed_gb10 = changed_gb10.model_copy(
        update={"pool_digest": canonical_digest_excluding(changed_gb10, "pool_digest")}
    )
    assert changed_gb10.pool_generation == initial_gb10.pool_generation == 1
    assert changed_gb10.pool_digest != initial_gb10.pool_digest

    template = initial_fleet.development_subject_template
    assert template is not None
    initial_template_profile = next(
        profile for profile in template.profiles if profile.pool_id == "gb10"
    )
    changed_template_profile = initial_template_profile.model_copy(
        update={
            "pool_digest": changed_gb10.pool_digest,
            "profile_generation": 2,
            "profile_digest": "0" * 64,
        }
    )
    changed_template_profile = changed_template_profile.model_copy(
        update={
            "profile_digest": canonical_digest_excluding(
                changed_template_profile,
                "profile_digest",
            )
        }
    )
    changed_template = template.model_copy(
        update={
            "profiles": tuple(
                changed_template_profile if profile.pool_id == "gb10" else profile
                for profile in template.profiles
            )
        }
    )
    changed_fleet = initial_fleet.model_copy(
        update={
            "fleet_generation": 2,
            "fleet_digest": "0" * 64,
            "pools": tuple(
                changed_gb10 if pool.pool_id == "gb10" else pool for pool in initial_fleet.pools
            ),
            "development_subject_template": changed_template,
        }
    )
    changed_fleet = changed_fleet.model_copy(
        update={"fleet_digest": canonical_digest_excluding(changed_fleet, "fleet_digest")}
    )

    initial_subject_profile = next(
        profile for profile in initial_subject.profiles if profile.pool_id == "gb10"
    )
    changed_subject_profile = initial_subject_profile.model_copy(
        update={
            "pool_digest": changed_gb10.pool_digest,
            "profile_generation": 2,
            "profile_digest": "0" * 64,
        }
    )
    changed_subject_profile = changed_subject_profile.model_copy(
        update={
            "profile_digest": canonical_digest_excluding(
                changed_subject_profile,
                "profile_digest",
            )
        }
    )
    changed_subject = initial_subject.model_copy(
        update={
            "configuration_generation": 2,
            "profiles": tuple(
                changed_subject_profile if profile.pool_id == "gb10" else profile
                for profile in initial_subject.profiles
            ),
        }
    )
    changed_fleet_proposal = await store.propose_fleet_configuration(
        capacity_session,
        changed_fleet,
        actor="fleet-operator",
        idempotency_key=uuid4(),
    )
    changed_subject_proposal = await store.propose_subject_configuration(
        capacity_session,
        changed_subject,
        actor="environment-state",
        idempotency_key=uuid4(),
    )

    activated = await store.activate_configuration(
        capacity_session,
        configuration_activation(
            fleet=changed_fleet_proposal,
            subjects=(changed_subject_proposal,),
            expected_configuration_epoch=first.configuration_epoch,
            static_candidate_provenance=(),
        ),
        actor="fleet-operator",
        idempotency_key=uuid4(),
    )

    assert first.configuration_epoch == 1
    assert activated.configuration_epoch == 2
    assert activated.snapshot.fleet.generation == 2
    assert activated.snapshot.fleet.digest == changed_fleet_proposal.digest
    materialized_pool = (
        await capacity_session.execute(
            select(CapacityPool).where(
                CapacityPool.configuration_epoch == activated.configuration_epoch,
                CapacityPool.pool_id == "gb10",
            )
        )
    ).scalar_one()
    assert materialized_pool.pool_generation == 1
    assert materialized_pool.pool_digest == changed_gb10.pool_digest
    materialized_profile = (
        await capacity_session.execute(
            select(CapacityWorkerProfile).where(
                CapacityWorkerProfile.subject_id == changed_subject.subject_id,
                CapacityWorkerProfile.subject_incarnation == changed_subject.subject_incarnation,
                CapacityWorkerProfile.deployment_generation
                == changed_subject.deployment_generation,
                CapacityWorkerProfile.pool_id == "gb10",
                CapacityWorkerProfile.profile_generation == 2,
            )
        )
    ).scalar_one()
    assert materialized_profile.pool_generation == 1
    assert materialized_profile.profile_digest == changed_subject_profile.profile_digest
    materialized_subject = (
        await capacity_session.execute(
            select(CapacitySubject).where(
                CapacitySubject.configuration_epoch == activated.configuration_epoch,
                CapacitySubject.subject_id == changed_subject.subject_id,
            )
        )
    ).scalar_one()
    materialized_gb10_profile = next(
        profile
        for profile in materialized_subject.payload["profiles"]
        if profile["pool_id"] == "gb10"
    )
    assert materialized_gb10_profile["pool_generation"] == 1
    assert materialized_gb10_profile["pool_digest"] == changed_gb10.pool_digest
    assert materialized_gb10_profile["profile_generation"] == 2


async def test_activation_seeds_static_candidate_provenance_for_executable_lookup(
    capacity_session: AsyncSession,
) -> None:
    store = CapacityManagementStore()
    fleet_proposal = await store.propose_fleet_configuration(
        capacity_session,
        fleet_manifest(),
        actor="fleet-operator",
        idempotency_key=uuid4(),
    )
    subject = subject_configuration()
    subject_proposal = await store.propose_subject_configuration(
        capacity_session,
        subject,
        actor="environment-state",
        idempotency_key=uuid4(),
    )
    provenance = StaticCandidateProvenanceV1(
        subject_id=subject.subject_id,
        subject_incarnation=subject.subject_incarnation,
        candidate_generation=subject.candidate_generation,
        algorithm="source-sha256",
        identity="1" * 64,
        publication_sha256="2" * 64,
    )

    await store.activate_configuration(
        capacity_session,
        configuration_activation(
            fleet=fleet_proposal,
            subjects=(subject_proposal,),
            static_candidate_provenance=(provenance,),
        ),
        actor="fleet-operator",
        idempotency_key=uuid4(),
    )

    subject_row = (await capacity_session.execute(select(CapacitySubject))).scalar_one()
    candidate = await CapacityExecutionStore._current_candidate(capacity_session, subject_row)

    assert candidate.algorithm == provenance.algorithm
    assert candidate.identity == provenance.identity
    assert candidate.publication_sha256 == provenance.publication_sha256


async def test_activation_rejects_unmatched_static_candidate_provenance(
    capacity_session: AsyncSession,
) -> None:
    """Extra static provenance must not be silently ignored during activation."""

    store = CapacityManagementStore()
    fleet_proposal = await store.propose_fleet_configuration(
        capacity_session,
        fleet_manifest(),
        actor="fleet-operator",
        idempotency_key=uuid4(),
    )
    subject = subject_configuration()
    subject_proposal = await store.propose_subject_configuration(
        capacity_session,
        subject,
        actor="environment-state",
        idempotency_key=uuid4(),
    )
    matched = StaticCandidateProvenanceV1(
        subject_id=subject.subject_id,
        subject_incarnation=subject.subject_incarnation,
        candidate_generation=subject.candidate_generation,
        algorithm="source-sha256",
        identity="1" * 64,
        publication_sha256="2" * 64,
    )
    unmatched = StaticCandidateProvenanceV1(
        subject_id=UUID("00000000-0000-4000-8000-00000000f001"),
        subject_incarnation=subject.subject_incarnation,
        candidate_generation=subject.candidate_generation,
        algorithm="source-sha256",
        identity="3" * 64,
        publication_sha256="4" * 64,
    )

    with pytest.raises(ConfigurationConflictError, match="static candidate provenance"):
        await store.activate_configuration(
            capacity_session,
            configuration_activation(
                fleet=fleet_proposal,
                subjects=(subject_proposal,),
                static_candidate_provenance=(matched, unmatched),
            ),
            actor="fleet-operator",
            idempotency_key=uuid4(),
        )

    assert (
        await capacity_session.execute(select(func.count()).select_from(CapacityCandidate))
    ).scalar_one() == 0


async def test_dynamic_development_projection_is_atomic_idempotent_and_shadow_only(
    capacity_session: AsyncSession,
) -> None:
    fleet = fleet_with_development_template()
    store, active = await _activate_default(
        capacity_session,
        fleet=fleet,
        subject=subject_configuration(fleet),
    )
    key = uuid4()
    request = development_projection(expected_configuration_epoch=active.configuration_epoch)

    result = await store.project_development_subject(
        capacity_session,
        request,
        actor="personal-lifecycle",
        idempotency_key=key,
    )
    replay = await store.project_development_subject(
        capacity_session,
        request,
        actor="personal-lifecycle",
        idempotency_key=key,
    )

    assert result.configuration_epoch == 2
    assert result.subject.display_name == "dev-alice"
    assert result.subject.min_slots == 0
    assert result.subject.account_id.startswith("dev-owner-")
    assert {profile.pool_id for profile in result.subject.profiles} == {"gb10", "oldlab"}
    assert not result.replayed
    assert replay.replayed
    assert replay.configuration_digest == result.configuration_digest
    assert (
        await capacity_session.execute(
            select(func.count())
            .select_from(CapacityCandidate)
            .where(CapacityCandidate.subject_id == request.subject_id)
        )
    ).scalar_one() == 1
    candidate = (
        await capacity_session.execute(
            select(CapacityCandidate).where(CapacityCandidate.subject_id == request.subject_id)
        )
    ).scalar_one()
    assert candidate.candidate_digest == request.candidate_sha256
    assert candidate.candidate_identity_algorithm == "source-sha256"
    assert candidate.candidate_identity == request.candidate_sha256
    assert (
        await capacity_session.execute(
            select(func.count()).select_from(CapacityDeploymentGeneration)
        )
    ).scalar_one() == 1
    assert (
        await capacity_session.execute(
            select(func.count()).select_from(CapacityDevelopmentProjection)
        )
    ).scalar_one() == 1
    reporter = (
        await capacity_session.execute(
            select(CapacityDemandReporter).where(
                CapacityDemandReporter.reporter_incarnation == request.demand_reporter_incarnation
            )
        )
    ).scalar_one()
    assert reporter.token_sha256 == request.demand_reporter_token_sha256
    authority = (
        await capacity_session.execute(
            select(CapacityAuthorityState).where(CapacityAuthorityState.singleton_id == 1)
        )
    ).scalar_one()
    assert authority.executable_new_capacity_ceiling == 0
    writer = await store.register_writer(
        capacity_session,
        authority.authority_incarnation,
        expected_epoch=authority.writer_epoch,
    )
    allocation_input = await store.load_allocation_input(capacity_session, writer)
    projected_account_ids = {
        account.account_id for account in allocation_input.effective_account_policies
    }
    assert result.account.account_id in projected_account_ids
    assert allocate_shadow(allocation_input).executable is False


async def test_dynamic_projection_accepts_consumed_local_generations_and_capacity_only_update(
    capacity_session: AsyncSession,
) -> None:
    fleet = fleet_with_development_template()
    store, active = await _activate_default(
        capacity_session,
        fleet=fleet,
        subject=subject_configuration(fleet),
    )
    created_request = development_projection(
        expected_configuration_epoch=active.configuration_epoch,
        operation_epoch=4,
        candidate_generation=3,
        deployment_generation=3,
        configuration_generation=4,
    )
    created = await store.project_development_subject(
        capacity_session,
        created_request,
        actor="personal-lifecycle",
        idempotency_key=uuid4(),
    )

    capacity_request = created_request.model_copy(
        update={
            "expected_configuration_epoch": created.configuration_epoch,
            "operation_kind": "capacity",
            "operation_id": uuid4(),
            "operation_epoch": 6,
            "configuration_generation": 6,
            "min_slots": 1,
            "max_slots": 3,
        }
    )
    resized = await store.project_development_subject(
        capacity_session,
        capacity_request,
        actor="personal-lifecycle",
        idempotency_key=uuid4(),
    )

    assert resized.subject.configuration_generation == 6
    assert resized.subject.deployment_generation == 3
    assert resized.subject.min_slots == 1
    assert resized.subject.max_slots == 3
    assert (
        await capacity_session.execute(
            select(func.count())
            .select_from(CapacityCandidate)
            .where(CapacityCandidate.subject_id == created_request.subject_id)
        )
    ).scalar_one() == 1
    assert (
        await capacity_session.execute(
            select(func.count()).select_from(CapacityDeploymentGeneration)
        )
    ).scalar_one() == 1


async def test_capacity_only_projection_cannot_change_candidate_or_reporter_binding(
    capacity_session: AsyncSession,
) -> None:
    fleet = fleet_with_development_template()
    store, active = await _activate_default(
        capacity_session,
        fleet=fleet,
        subject=subject_configuration(fleet),
    )
    initial = development_projection(expected_configuration_epoch=active.configuration_epoch)
    created = await store.project_development_subject(
        capacity_session,
        initial,
        actor="personal-lifecycle",
        idempotency_key=uuid4(),
    )
    base_capacity = initial.model_copy(
        update={
            "expected_configuration_epoch": created.configuration_epoch,
            "operation_kind": "capacity",
            "operation_id": uuid4(),
            "operation_epoch": 2,
            "configuration_generation": 2,
        }
    )

    for change in (
        {"candidate_sha256": "9" * 64},
        {
            "demand_reporter_incarnation": uuid4(),
            "demand_reporter_token_sha256": "8" * 64,
        },
    ):
        with pytest.raises(ConfigurationConflictError, match="non-deployment"):
            await store.project_development_subject(
                capacity_session,
                base_capacity.model_copy(update=change),
                actor="personal-lifecycle",
                idempotency_key=uuid4(),
            )
        await capacity_session.rollback()


async def test_destroy_projection_requires_an_active_subject_and_retires_owner_authority(
    capacity_session: AsyncSession,
) -> None:
    fleet = fleet_with_development_template()
    store, active = await _activate_default(
        capacity_session,
        fleet=fleet,
        subject=subject_configuration(fleet),
    )
    initial = development_projection(expected_configuration_epoch=active.configuration_epoch)
    absent = initial.model_copy(
        update={
            "operation_kind": "destroy",
            "operation_id": uuid4(),
            "operation_epoch": 2,
            "configuration_generation": 2,
        }
    )
    with pytest.raises(ConfigurationConflictError, match="not active"):
        await store.project_development_subject(
            capacity_session,
            absent,
            actor="personal-lifecycle",
            idempotency_key=uuid4(),
        )
    await capacity_session.rollback()

    created = await store.project_development_subject(
        capacity_session,
        initial,
        actor="personal-lifecycle",
        idempotency_key=uuid4(),
    )
    retirement = initial.model_copy(
        update={
            "expected_configuration_epoch": created.configuration_epoch,
            "operation_kind": "destroy",
            "operation_id": uuid4(),
            "operation_epoch": 2,
            "configuration_generation": 2,
        }
    )
    retired = await store.project_development_subject(
        capacity_session,
        retirement,
        actor="personal-lifecycle",
        idempotency_key=uuid4(),
    )

    assert retired.subject.lifecycle_state == "disabled"
    assert retired.subject.min_slots == 0
    assert retired.subject.max_slots == 0
    assert (
        await capacity_session.execute(
            select(func.count())
            .select_from(CapacityAccountPolicy)
            .where(
                CapacityAccountPolicy.configuration_epoch == retired.configuration_epoch,
                CapacityAccountPolicy.kind == "owner",
            )
        )
    ).scalar_one() == 0
    reporter = (
        await capacity_session.execute(
            select(CapacityDemandReporter).where(
                CapacityDemandReporter.subject_id == retirement.subject_id,
                CapacityDemandReporter.reporter_incarnation
                == retirement.demand_reporter_incarnation,
            )
        )
    ).scalar_one()
    assert reporter.state == "fenced"


async def test_dynamic_projection_identity_reuse_and_owner_limits_fail_closed(
    capacity_session: AsyncSession,
) -> None:
    fleet = fleet_with_development_template(owner_max_live_subjects=1)
    store, active = await _activate_default(
        capacity_session,
        fleet=fleet,
        subject=subject_configuration(fleet),
    )
    first = development_projection(expected_configuration_epoch=active.configuration_epoch)
    key = uuid4()
    await store.project_development_subject(
        capacity_session,
        first,
        actor="personal-lifecycle",
        idempotency_key=key,
    )

    with pytest.raises(IdempotencyConflictError):
        await store.project_development_subject(
            capacity_session,
            first.model_copy(update={"max_slots": 3}),
            actor="personal-lifecycle",
            idempotency_key=key,
        )

    second = development_projection(
        expected_configuration_epoch=2,
        operation_id=uuid4(),
        environment_name="bob",
        subject_id=uuid4(),
        subject_incarnation=uuid4(),
        demand_reporter_incarnation=uuid4(),
    ).model_copy(update={"demand_reporter_token_sha256": "e" * 64})
    with pytest.raises(ConfigurationConflictError, match="max_live_subjects"):
        await store.project_development_subject(
            capacity_session,
            second,
            actor="personal-lifecycle",
            idempotency_key=uuid4(),
        )
    assert (
        await capacity_session.execute(
            select(func.count()).select_from(CapacityDevelopmentProjection)
        )
    ).scalar_one() == 1


async def test_static_fleet_activation_rederives_existing_personal_owner_accounts(
    capacity_session: AsyncSession,
) -> None:
    fleet = fleet_with_development_template()
    store, active = await _activate_default(
        capacity_session,
        fleet=fleet,
        subject=subject_configuration(fleet),
    )
    projected = await store.project_development_subject(
        capacity_session,
        development_projection(expected_configuration_epoch=active.configuration_epoch),
        actor="personal-lifecycle",
        idempotency_key=uuid4(),
    )
    changed = fleet.model_copy(update={"fleet_generation": 2, "fleet_digest": "f" * 64})
    changed = changed.model_copy(
        update={"fleet_digest": canonical_digest_excluding(changed, "fleet_digest")}
    )
    fleet_proposal = await store.propose_fleet_configuration(
        capacity_session,
        changed,
        actor="fleet-operator",
        idempotency_key=uuid4(),
    )
    subject_rows = (
        (
            await capacity_session.execute(
                select(CapacityConfigGeneration).where(
                    CapacityConfigGeneration.scope == "subject",
                    CapacityConfigGeneration.state == "active",
                )
            )
        )
        .scalars()
        .all()
    )
    activated = await store.activate_configuration(
        capacity_session,
        ConfigurationActivationV1(
            expected_configuration_epoch=projected.configuration_epoch,
            fleet=ConfigurationGenerationRefV1(
                scope="fleet",
                generation=fleet_proposal.generation,
                digest=fleet_proposal.digest,
            ),
            subjects=tuple(
                ConfigurationGenerationRefV1(
                    scope="subject",
                    generation=row.scope_generation,
                    digest=row.digest,
                    subject_id=row.subject_id,
                    subject_incarnation=row.subject_incarnation,
                )
                for row in subject_rows
            ),
        ),
        actor="fleet-operator",
        idempotency_key=uuid4(),
    )

    derived = (
        await capacity_session.execute(
            select(CapacityAccountPolicy).where(
                CapacityAccountPolicy.configuration_epoch == activated.configuration_epoch,
                CapacityAccountPolicy.kind == "owner",
            )
        )
    ).scalar_one()
    assert derived.account_id == projected.account.account_id
    assert derived.payload == projected.account.model_dump(mode="json", exclude_none=False)


async def test_configuration_rollback_clones_predecessor_monotonically_and_fences_removed_subject(
    capacity_session: AsyncSession,
) -> None:
    store, active, projected, projection = await _rollback_ready_projection(capacity_session)
    await store.ingest_demand_snapshot(
        capacity_session,
        demand_snapshot(sequence=2),
        actor="development",
    )
    await store.ingest_pool_observation(
        capacity_session,
        pool_observation(sequence=2, pool_id="gb10"),
        actor="gb10-reporter",
    )
    await store.ingest_pool_observation(
        capacity_session,
        pool_observation(sequence=2, pool_id="oldlab"),
        actor="oldlab-reporter",
    )
    await store.ingest_demand_snapshot(
        capacity_session,
        demand_snapshot(
            subject_id=projection.subject_id,
            subject_incarnation=projection.subject_incarnation,
            reporter_incarnation=projection.demand_reporter_incarnation,
        ),
        actor="personal-lifecycle",
    )
    request = await _configuration_rollback(
        capacity_session,
        expected_configuration_epoch=projected.configuration_epoch,
        restore_configuration_epoch=active.configuration_epoch,
    )

    rolled = await store.rollback_configuration(
        capacity_session,
        request,
        actor="fleet-operator",
        idempotency_key=uuid4(),
    )

    assert rolled.configuration_epoch == projected.configuration_epoch + 1
    assert canonical_digest(rolled.snapshot) == rolled.digest
    assert rolled.snapshot.fleet.generation == 2
    assert {reference.subject_id for reference in rolled.snapshot.subjects} == {SUBJECT_ID}
    active_fleet = (
        await capacity_session.execute(
            select(CapacityConfigGeneration).where(
                CapacityConfigGeneration.scope == "fleet",
                CapacityConfigGeneration.state == "active",
            )
        )
    ).scalar_one()
    assert active_fleet.scope_generation == 2
    active_subject = (
        await capacity_session.execute(
            select(CapacitySubject).where(
                CapacitySubject.configuration_epoch == rolled.configuration_epoch,
                CapacitySubject.subject_id == SUBJECT_ID,
            )
        )
    ).scalar_one()
    assert active_subject.configuration_generation == 2
    assert active_subject.subject_incarnation == SUBJECT_INCARNATION
    reporter = (
        await capacity_session.execute(
            select(CapacityDemandReporter).where(
                CapacityDemandReporter.subject_id == SUBJECT_ID,
                CapacityDemandReporter.reporter_incarnation == DEMAND_REPORTER_ID,
            )
        )
    ).scalar_one()
    assert reporter.state == "current"
    assert reporter.high_water == 2
    assert reporter.configuration_generation == 2
    retired_projected_reporter = (
        await capacity_session.execute(
            select(CapacityDemandReporter).where(
                CapacityDemandReporter.subject_id == projection.subject_id,
                CapacityDemandReporter.reporter_incarnation
                == projection.demand_reporter_incarnation,
            )
        )
    ).scalar_one()
    assert retired_projected_reporter.state == "fenced"
    assert (
        await capacity_session.execute(
            select(func.count())
            .select_from(CapacityCandidate)
            .where(CapacityCandidate.subject_id == projection.subject_id)
        )
    ).scalar_one() == 1
    assert (
        await capacity_session.execute(
            select(func.count())
            .select_from(CapacityWorkerProfile)
            .where(CapacityWorkerProfile.subject_id == SUBJECT_ID)
        )
    ).scalar_one() == 2
    pool_reporters = (
        await capacity_session.execute(
            select(CapacityPoolReporter).order_by(CapacityPoolReporter.pool_id)
        )
    ).scalars()
    assert [(item.pool_id, item.state, item.high_water) for item in pool_reporters] == [
        ("gb10", "current", 2),
        ("oldlab", "current", 2),
    ]
    authority = (await capacity_session.execute(select(CapacityAuthorityState))).scalar_one()
    assert authority.execution_state == "shadow"
    assert authority.execution_epoch == 0
    assert authority.executable_new_capacity_ceiling == 0
    assert authority.increase_freeze is True
    audit = (
        (
            await capacity_session.execute(
                select(CapacityAuditEvent).order_by(CapacityAuditEvent.id.desc())
            )
        )
        .scalars()
        .first()
    )
    assert audit is not None
    assert audit.event_kind == "capacity_configuration_rolled_back"


async def test_configuration_rollback_replays_exactly_and_conflicts_on_mismatch(
    capacity_session: AsyncSession,
) -> None:
    store, active, projected, _projection = await _rollback_ready_projection(capacity_session)
    request = await _configuration_rollback(
        capacity_session,
        expected_configuration_epoch=projected.configuration_epoch,
        restore_configuration_epoch=active.configuration_epoch,
    )
    key = uuid4()

    first = await store.rollback_configuration(
        capacity_session,
        request,
        actor="fleet-operator",
        idempotency_key=key,
    )
    replay = await store.rollback_configuration(
        capacity_session,
        request,
        actor="fleet-operator",
        idempotency_key=key,
    )

    assert replay == first
    with pytest.raises(IdempotencyConflictError):
        await store.rollback_configuration(
            capacity_session,
            request.model_copy(update={"rollback_evidence_sha256": "8" * 64}),
            actor="fleet-operator",
            idempotency_key=key,
        )


async def test_configuration_rollback_replay_conflicts_on_actor_mismatch(
    capacity_session: AsyncSession,
) -> None:
    store, active, projected, _projection = await _rollback_ready_projection(capacity_session)
    request = await _configuration_rollback(
        capacity_session,
        expected_configuration_epoch=projected.configuration_epoch,
        restore_configuration_epoch=active.configuration_epoch,
    )
    key = uuid4()

    await store.rollback_configuration(
        capacity_session,
        request,
        actor="fleet-operator",
        idempotency_key=key,
    )

    with pytest.raises(IdempotencyConflictError):
        await store.rollback_configuration(
            capacity_session,
            request,
            actor="different-rollback-operator",
            idempotency_key=key,
        )


async def test_configuration_rollback_conflicts_with_activation_idempotency_key_reuse(
    capacity_session: AsyncSession,
) -> None:
    store, active, projected, _projection = await _rollback_ready_projection(capacity_session)
    request = await _configuration_rollback(
        capacity_session,
        expected_configuration_epoch=projected.configuration_epoch,
        restore_configuration_epoch=active.configuration_epoch,
    )

    with pytest.raises(IdempotencyConflictError):
        await store.rollback_configuration(
            capacity_session,
            request,
            actor="fleet-operator",
            idempotency_key=ACTIVATION_KEY,
        )


@pytest.mark.parametrize(
    ("request_update", "authority_update", "authority_state", "error_type", "message"),
    (
        (
            {"expected_configuration_digest": "0" * 64},
            None,
            None,
            ConfigurationConflictError,
            "expected active configuration",
        ),
        (
            {"restore_configuration_digest": "0" * 64},
            None,
            None,
            ConfigurationConflictError,
            "restore configuration",
        ),
        (
            {"restore_configuration_epoch": 2},
            None,
            None,
            ConfigurationConflictError,
            "immediate predecessor",
        ),
        (
            None,
            {"authority_incarnation": UUID("00000000-0000-4000-8000-00000000aa01")},
            None,
            ConfigurationConflictError,
            "authority incarnation",
        ),
        (None, {"increase_freeze": False}, None, ConfigurationConflictError, "freeze"),
        (
            None,
            None,
            "prepared",
            AuthorityRecoveryError,
            "shadow-only",
        ),
        (
            None,
            None,
            "active",
            AuthorityRecoveryError,
            "shadow-only",
        ),
    ),
)
async def test_configuration_rollback_rejects_stale_request_or_unfrozen_authority(
    capacity_session: AsyncSession,
    request_update: dict[str, object] | None,
    authority_update: dict[str, object] | None,
    authority_state: str | None,
    error_type: type[Exception],
    message: str,
) -> None:
    store, active, projected, _projection = await _rollback_ready_projection(capacity_session)
    if authority_state is not None:
        await _enter_execution_state(capacity_session, target_state=authority_state)
    elif authority_update is not None:
        await capacity_session.execute(
            update(CapacityAuthorityState)
            .where(CapacityAuthorityState.singleton_id == 1)
            .values(**authority_update)
        )
        await capacity_session.flush()
    request = await _configuration_rollback(
        capacity_session,
        expected_configuration_epoch=projected.configuration_epoch,
        restore_configuration_epoch=active.configuration_epoch,
    )
    if request_update is not None:
        request = request.model_copy(update=request_update)

    with pytest.raises(error_type, match=message):
        await store.rollback_configuration(
            capacity_session,
            request,
            actor="fleet-operator",
            idempotency_key=uuid4(),
        )


async def test_configuration_rollback_refuses_to_reactivate_absent_subjects(
    capacity_session: AsyncSession,
) -> None:
    store, _active, projected, projection = await _rollback_ready_projection(capacity_session)
    retired = await store.project_development_subject(
        capacity_session,
        projection.model_copy(
            update={
                "expected_configuration_epoch": projected.configuration_epoch,
                "operation_kind": "destroy",
                "operation_id": uuid4(),
                "operation_epoch": 2,
                "configuration_generation": 2,
            }
        ),
        actor="personal-lifecycle",
        idempotency_key=uuid4(),
    )
    request = await _configuration_rollback(
        capacity_session,
        expected_configuration_epoch=retired.configuration_epoch,
        restore_configuration_epoch=projected.configuration_epoch,
    )

    with pytest.raises(ConfigurationConflictError, match="absent from current target"):
        await store.rollback_configuration(
            capacity_session,
            request,
            actor="fleet-operator",
            idempotency_key=uuid4(),
        )


@pytest.mark.parametrize(
    ("label", "mutate", "message"),
    (
        (
            "pool_generation",
            lambda session, epoch: session.execute(
                update(CapacityPool)
                .where(
                    CapacityPool.configuration_epoch == epoch,
                    CapacityPool.pool_id == "gb10",
                )
                .values(pool_generation=2)
            ),
            "pool generation",
        ),
        (
            "pool_reporter",
            lambda session, _epoch: session.execute(
                update(CapacityPoolReporter)
                .where(
                    CapacityPoolReporter.pool_id == "gb10",
                    CapacityPoolReporter.state == "current",
                )
                .values(reporter_incarnation=UUID("00000000-0000-4000-8000-00000000aa02"))
            ),
            "pool reporter",
        ),
        (
            "protocol_generation",
            lambda session, epoch: session.execute(
                update(CapacityPool)
                .where(
                    CapacityPool.configuration_epoch == epoch,
                    CapacityPool.pool_id == "gb10",
                )
                .values(protocol_generation=2, protocol_digest="8" * 64)
            ),
            "protocol",
        ),
        (
            "subject_incarnation",
            lambda session, epoch: session.execute(
                update(CapacitySubject)
                .where(
                    CapacitySubject.configuration_epoch == epoch,
                    CapacitySubject.subject_id == SUBJECT_ID,
                )
                .values(subject_incarnation=UUID("00000000-0000-4000-8000-00000000aa03"))
            ),
            "subject incarnation",
        ),
        (
            "candidate_generation",
            lambda session, epoch: session.execute(
                update(CapacitySubject)
                .where(
                    CapacitySubject.configuration_epoch == epoch,
                    CapacitySubject.subject_id == SUBJECT_ID,
                )
                .values(candidate_generation=2)
            ),
            "candidate generation",
        ),
        (
            "deployment_generation",
            lambda session, epoch: session.execute(
                update(CapacitySubject)
                .where(
                    CapacitySubject.configuration_epoch == epoch,
                    CapacitySubject.subject_id == SUBJECT_ID,
                )
                .values(deployment_generation=2)
            ),
            "deployment generation",
        ),
        (
            "demand_reporter",
            lambda session, epoch: session.execute(
                update(CapacitySubject)
                .where(
                    CapacitySubject.configuration_epoch == epoch,
                    CapacitySubject.subject_id == SUBJECT_ID,
                )
                .values(demand_reporter_incarnation=UUID("00000000-0000-4000-8000-00000000aa04"))
            ),
            "demand reporter",
        ),
    ),
)
async def test_configuration_rollback_rejects_identity_regressions(
    capacity_session: AsyncSession,
    label: str,
    mutate,  # type: ignore[no-untyped-def]
    message: str,
) -> None:
    del label
    store, active, projected, _projection = await _rollback_ready_projection(capacity_session)
    await mutate(capacity_session, projected.configuration_epoch)
    await capacity_session.flush()
    request = await _configuration_rollback(
        capacity_session,
        expected_configuration_epoch=projected.configuration_epoch,
        restore_configuration_epoch=active.configuration_epoch,
    )

    with pytest.raises(ConfigurationConflictError, match=message):
        await store.rollback_configuration(
            capacity_session,
            request,
            actor="fleet-operator",
            idempotency_key=uuid4(),
        )


async def test_concurrent_personal_projections_serialize_and_both_converge_after_retry(
    capacity_session_factory: async_sessionmaker[AsyncSession],
) -> None:
    await _reset_committed_capacity_state(capacity_session_factory)
    try:
        fleet = fleet_with_development_template()
        async with capacity_session_factory() as session:
            await _activate_default(
                session,
                fleet=fleet,
                subject=subject_configuration(fleet),
            )
            await session.commit()

        requests = (
            development_projection(environment_name="alice"),
            development_projection(
                operation_id=uuid4(),
                environment_name="bob",
                subject_id=uuid4(),
                subject_incarnation=uuid4(),
                owner_id=uuid4(),
                demand_reporter_incarnation=uuid4(),
            ).model_copy(update={"demand_reporter_token_sha256": "e" * 64}),
        )
        keys = (uuid4(), uuid4())

        async def project(index: int, expected_epoch: int):
            async with capacity_session_factory() as session:
                return await CapacityManagementStore().project_development_subject(
                    session,
                    requests[index].model_copy(
                        update={"expected_configuration_epoch": expected_epoch}
                    ),
                    actor=f"personal-lifecycle-{index}",
                    idempotency_key=keys[index],
                )

        first_round = await asyncio.gather(
            project(0, 1),
            project(1, 1),
            return_exceptions=True,
        )
        winners = [
            index for index, result in enumerate(first_round) if not isinstance(result, Exception)
        ]
        losers = [
            index for index, result in enumerate(first_round) if isinstance(result, Exception)
        ]
        assert len(winners) == 1
        assert len(losers) == 1
        assert isinstance(first_round[losers[0]], CapacityStoreError)

        retried = await project(losers[0], 2)
        assert retried.configuration_epoch == 3
        async with capacity_session_factory() as session:
            assert (
                await session.execute(
                    select(func.count()).select_from(CapacityDevelopmentProjection)
                )
            ).scalar_one() == 2
    finally:
        await _reset_committed_capacity_state(capacity_session_factory)


async def test_activation_persists_configurable_account_and_subject_submission_rates(
    capacity_session: AsyncSession,
) -> None:
    manifest = fleet_manifest()
    account = manifest.account_policies[0].model_copy(update={"submission_rate_per_minute": 6})
    manifest = fleet_manifest(account_policies=(account,))
    subject = subject_configuration(
        manifest,
        submission_rate_per_minute=4,
    )
    await _activate_default(capacity_session, fleet=manifest, subject=subject)

    account_rate = (
        await capacity_session.execute(select(CapacityAccountPolicy.submission_rate_per_minute))
    ).scalar_one()
    subject_rate = (
        await capacity_session.execute(select(CapacitySubject.submission_rate_per_minute))
    ).scalar_one()
    assert (account_rate, subject_rate) == (6, 4)


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


async def test_replacement_writer_restores_increase_freeze_until_fresh_epoch(
    capacity_session: AsyncSession,
) -> None:
    store, _ = await _activate_default(capacity_session)
    authority = await _authority_uuid(capacity_session)
    old = await store.register_writer(capacity_session, authority, expected_epoch=0)
    allocation_input = await store.load_allocation_input(capacity_session, old)
    await store.commit_shadow_epoch(capacity_session, old, shadow_epoch(allocation_input))
    assert (
        await capacity_session.execute(select(CapacityAuthorityState.increase_freeze))
    ).scalar_one() is False

    await store.register_writer(
        capacity_session,
        authority,
        expected_epoch=old.writer_epoch,
    )

    state = (
        await capacity_session.execute(
            select(
                CapacityAuthorityState.increase_freeze,
                CapacityAuthorityState.increase_freeze_reason,
            )
        )
    ).one()
    assert state == (True, "writer_epoch_changed")


async def test_status_page_rejects_unbounded_limit(capacity_session: AsyncSession) -> None:
    store, _ = await _activate_default(capacity_session)
    with pytest.raises(ValueError, match="limit"):
        await store.status(capacity_session, cursor=None, limit=501)


def test_writer_fence_is_immutable() -> None:
    fence = WriterFence(authority_incarnation=AUTHORITY_ID, writer_epoch=1)
    with pytest.raises(AttributeError):
        fence.writer_epoch = 2


class BlockingAllocator:
    def __init__(self) -> None:
        self.started = Event()
        self.release = Event()

    def __call__(self, value):  # type: ignore[no-untyped-def]
        self.started.set()
        if not self.release.wait(timeout=5):
            raise TimeoutError("test allocator was not released")
        return allocate_shadow(value)


class TimeoutAllocator:
    async def __call__(self, value):  # type: ignore[no-untyped-def]
        del value
        await asyncio.sleep(5)


class InvalidAllocator:
    def __call__(self, value):  # type: ignore[no-untyped-def]
        del value
        raise ShadowAllocatorError("invalid synthetic topology")


class IncompleteAllocator:
    def __call__(self, value):  # type: ignore[no-untyped-def]
        return shadow_epoch(value)


class CommitFailureStore(CapacityManagementStore):
    async def commit_shadow_epoch(self, session, writer, epoch):  # type: ignore[no-untyped-def]
        del session, writer, epoch
        raise RuntimeError("synthetic database write failure")


class ChangingInputAllocator:
    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
    ) -> None:
        self._session_factory = session_factory
        self._sequence = 1

    async def __call__(self, value):  # type: ignore[no-untyped-def]
        self._sequence += 1
        async with self._session_factory() as session:
            await CapacityManagementStore().ingest_demand_snapshot(
                session,
                demand_snapshot(sequence=self._sequence),
                actor="dev-a",
            )
            await session.commit()
        return allocate_shadow(value)


async def publish_newer_report(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    async with session_factory() as session:
        await CapacityManagementStore().ingest_demand_snapshot(
            session,
            demand_snapshot(sequence=2),
            actor="dev-a",
        )
        await session.commit()


async def committed_shadow_epoch_count(
    session_factory: async_sessionmaker[AsyncSession],
) -> int:
    async with session_factory() as session:
        return int(
            (
                await session.execute(
                    select(func.count())
                    .select_from(CapacityAllocationEpoch)
                    .where(CapacityAllocationEpoch.status == "shadow")
                )
            ).scalar_one()
        )


async def allocation_row_count(
    session_factory: async_sessionmaker[AsyncSession],
) -> int:
    async with session_factory() as session:
        return int(
            (
                await session.execute(select(func.count()).select_from(CapacityAllocation))
            ).scalar_one()
        )


async def latest_audit_kind(
    session_factory: async_sessionmaker[AsyncSession],
) -> str | None:
    async with session_factory() as session:
        return (
            (
                await session.execute(
                    select(CapacityAuditEvent.event_kind).order_by(CapacityAuditEvent.id.desc())
                )
            )
            .scalars()
            .first()
        )


async def test_input_change_during_allocation_rejects_whole_epoch(
    capacity_session_factory: async_sessionmaker[AsyncSession],
    registered_writer: WriterFence,
) -> None:
    allocator = BlockingAllocator()
    task = asyncio.create_task(
        reconcile_shadow_once(
            capacity_session_factory,
            registered_writer,
            allocator=allocator,
        )
    )
    assert await asyncio.to_thread(allocator.started.wait, 2)
    await publish_newer_report(capacity_session_factory)
    allocator.release.set()
    result = await task

    assert result.status == "committed"
    assert result.attempt_count == 2
    assert await committed_shadow_epoch_count(capacity_session_factory) == 1


async def test_successful_reconciliation_commits_one_complete_shadow_epoch(
    capacity_session_factory: async_sessionmaker[AsyncSession],
    registered_writer: WriterFence,
) -> None:
    result = await reconcile_shadow_once(
        capacity_session_factory,
        registered_writer,
    )

    async with capacity_session_factory() as session:
        epoch = (await session.execute(select(CapacityAllocationEpoch))).scalar_one()
        allocations = (
            (await session.execute(select(CapacityAllocation).order_by(CapacityAllocation.pool_id)))
            .scalars()
            .all()
        )
        authority = (await session.execute(select(CapacityAuthorityState))).scalar_one()
    assert result.status == "committed"
    assert epoch.status == "shadow"
    assert epoch.executable is False
    assert epoch.complete_payload["executable"] is False
    assert len(allocations) == 2
    assert all(allocation.executable is False for allocation in allocations)
    assert authority.executable_new_capacity_ceiling == 0
    assert authority.increase_freeze is False


async def test_allocator_timeout_records_failure_without_partial_allocations(
    capacity_session_factory: async_sessionmaker[AsyncSession],
    registered_writer: WriterFence,
) -> None:
    result = await reconcile_shadow_once(
        capacity_session_factory,
        registered_writer,
        allocator=TimeoutAllocator(),
        allocation_timeout_seconds=0.01,
    )

    assert result.status == "failed"
    assert await allocation_row_count(capacity_session_factory) == 0
    assert await latest_audit_kind(capacity_session_factory) == "shadow_allocation_timeout"


async def test_invalid_allocator_input_sets_freeze_and_records_failed_epoch(
    capacity_session_factory: async_sessionmaker[AsyncSession],
    registered_writer: WriterFence,
) -> None:
    result = await reconcile_shadow_once(
        capacity_session_factory,
        registered_writer,
        allocator=InvalidAllocator(),
    )

    async with capacity_session_factory() as session:
        failed_epochs = (
            await session.execute(
                select(func.count())
                .select_from(CapacityAllocationEpoch)
                .where(CapacityAllocationEpoch.status == "failed")
            )
        ).scalar_one()
        freeze = (
            await session.execute(select(CapacityAuthorityState.increase_freeze))
        ).scalar_one()
    assert result.status == "failed"
    assert failed_epochs == 1
    assert freeze is True
    assert await allocation_row_count(capacity_session_factory) == 0
    assert await latest_audit_kind(capacity_session_factory) == "shadow_allocation_invalid"


async def test_incomplete_allocator_epoch_is_rejected_before_persistence(
    capacity_session_factory: async_sessionmaker[AsyncSession],
    registered_writer: WriterFence,
) -> None:
    result = await reconcile_shadow_once(
        capacity_session_factory,
        registered_writer,
        allocator=IncompleteAllocator(),
    )

    assert result.status == "failed"
    assert await committed_shadow_epoch_count(capacity_session_factory) == 0
    assert await allocation_row_count(capacity_session_factory) == 0
    assert await latest_audit_kind(capacity_session_factory) == "shadow_allocation_invalid"


async def test_epoch_commit_failure_leaves_no_partial_allocations(
    capacity_session_factory: async_sessionmaker[AsyncSession],
    registered_writer: WriterFence,
) -> None:
    result = await reconcile_shadow_once(
        capacity_session_factory,
        registered_writer,
        store=CommitFailureStore(),
    )

    assert result.status == "failed"
    assert await committed_shadow_epoch_count(capacity_session_factory) == 0
    assert await allocation_row_count(capacity_session_factory) == 0
    assert await latest_audit_kind(capacity_session_factory) == "shadow_allocation_failure"


async def test_shadow_epoch_post_flush_failure_rolls_back_all_commit_state(
    capacity_session: AsyncSession,
) -> None:
    store, _ = await _activate_default(capacity_session)
    authority = await _authority_uuid(capacity_session)
    writer = await store.register_writer(capacity_session, authority, expected_epoch=0)
    allocation_input = await store.load_allocation_input(capacity_session, writer)
    fairness_before = tuple(
        (
            await capacity_session.execute(
                select(CapacityFairnessState).order_by(CapacityFairnessState.id)
            )
        )
        .scalars()
        .all()
    )
    failed = False

    def fail_after_flush(*_args: object) -> None:
        nonlocal failed
        if not failed:
            failed = True
            raise RuntimeError("synthetic post-flush failure")

    event.listen(capacity_session.sync_session, "after_flush_postexec", fail_after_flush)
    try:
        with pytest.raises(RuntimeError, match="synthetic post-flush failure"):
            await store.commit_shadow_epoch(
                capacity_session,
                writer,
                allocate_shadow(allocation_input),
            )
    finally:
        event.remove(capacity_session.sync_session, "after_flush_postexec", fail_after_flush)

    assert failed is True
    assert (
        await capacity_session.execute(select(func.count()).select_from(CapacityAllocationEpoch))
    ).scalar_one() == 0
    assert (
        await capacity_session.execute(select(func.count()).select_from(CapacityAllocation))
    ).scalar_one() == 0
    assert (
        tuple(
            (
                await capacity_session.execute(
                    select(CapacityFairnessState).order_by(CapacityFairnessState.id)
                )
            )
            .scalars()
            .all()
        )
        == fairness_before
    )


async def test_writer_change_during_allocation_fences_old_result(
    capacity_session_factory: async_sessionmaker[AsyncSession],
    registered_writer: WriterFence,
) -> None:
    allocator = BlockingAllocator()
    task = asyncio.create_task(
        reconcile_shadow_once(
            capacity_session_factory,
            registered_writer,
            allocator=allocator,
        )
    )
    assert await asyncio.to_thread(allocator.started.wait, 2)
    async with capacity_session_factory() as session:
        await CapacityManagementStore().register_writer(
            session,
            registered_writer.authority_incarnation,
            expected_epoch=registered_writer.writer_epoch,
        )
        await session.commit()
    allocator.release.set()
    result = await task

    assert result.status == "failed"
    assert result.reason == "capacity writer fence changed"
    assert await committed_shadow_epoch_count(capacity_session_factory) == 0
    assert await allocation_row_count(capacity_session_factory) == 0


async def test_continuous_input_churn_preserves_prior_epoch(
    capacity_session_factory: async_sessionmaker[AsyncSession],
    registered_writer: WriterFence,
) -> None:
    result = await reconcile_shadow_once(
        capacity_session_factory,
        registered_writer,
        allocator=ChangingInputAllocator(capacity_session_factory),
        max_attempts=3,
    )

    assert result.status == "input-contention"
    assert result.attempt_count == 3
    assert await committed_shadow_epoch_count(capacity_session_factory) == 0
