"""Serializable dry-run reservation and executor protocol tests."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from uuid import UUID

import pytest
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from sqlalchemy import func, select, text, update
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncSession

from loom_capacity_manager.allocator import allocate_shadow
from loom_capacity_manager.contracts import ResourceVectorV1, ShadowEpochV1
from loom_capacity_manager.grant_contracts import (
    DryRunBootstrapRegistrationV1,
    DryRunExecutorHeartbeatV1,
    DryRunExecutorInventoryV1,
    DryRunExecutorRegistrationV1,
    DryRunIntentCloseV1,
    DryRunLaunchPermitV1,
    DryRunPartialReleaseV1,
    DryRunPermitConsumptionV1,
    DryRunProtectedReleaseAcknowledgementV1,
    DryRunReservationAcceptanceV1,
    DryRunReservationProposalV1,
    ExecutorInventoryRecordV1,
    OwnershipMetadataV1,
    ReleasedShapeV1,
    ReservationShapeV1,
    canonical_grant_digest,
)
from loom_capacity_manager.grant_store import (
    CapacityGrantStore,
    ExecutorEquivocationError,
    ExecutorJournalError,
    GrantConflictError,
    IdempotencyConflictError,
    LaunchOrderError,
    ProposalExpiredError,
    ProposalSupersededError,
    RateLimitError,
    StaleCommandError,
)
from loom_capacity_manager.models import (
    CapacityAllocation,
    CapacityAuthorityState,
    CapacityExecutor,
    CapacityExecutorObservation,
    CapacityLaunchPermit,
    CapacityLaunchRateBucket,
    CapacityObservedCommitment,
    CapacityPool,
    CapacityProtectedReleaseAcknowledgement,
    CapacityReservationReleaseEvidence,
    CapacityReservationShape,
    CapacityReservationTranche,
    CapacitySubject,
    CapacitySubmissionIntent,
    CapacityWorkerProfile,
)
from loom_capacity_manager.ownership import (
    OwnershipKeyring,
    public_key_fingerprint,
    sign_ownership,
)
from loom_capacity_manager.store import (
    CapacityManagementStore,
    CommittedShadowEpoch,
    WriterFence,
)
from tests.capacity_fixtures import (
    ACTIVATION_KEY,
    CONFIG_KEY_A,
    CONFIG_KEY_B,
    DEMAND_REPORTER_ID,
    configuration_activation,
    demand_snapshot,
    fleet_manifest,
    pool_observation,
    subject_configuration,
)

_EXECUTOR_INCARNATION = UUID("00000000-0000-4000-8000-000000000101")
_NEXT_EXECUTOR_INCARNATION = UUID("00000000-0000-4000-8000-000000000102")
_TRANCHE_ID = UUID("00000000-0000-4000-8000-000000000103")
_INTENT_ID = UUID("00000000-0000-4000-8000-000000000104")
_REGISTRATION_KEY = UUID("00000000-0000-4000-8000-000000000105")
_PROPOSAL_KEY = UUID("00000000-0000-4000-8000-000000000106")
_POOL_PRIVATE_KEYS = {
    "gb10": Ed25519PrivateKey.from_private_bytes(b"\x01" * 32),
    "oldlab": Ed25519PrivateKey.from_private_bytes(b"\x02" * 32),
}


def _grant_store(
    *,
    executor_lease_seconds: int = 120,
    proposal_ttl_seconds: int = 30,
    permit_ttl_seconds: int = 15,
) -> CapacityGrantStore:
    return CapacityGrantStore(
        ownership_keyring=OwnershipKeyring(
            {
                f"{pool_id}-key-1": private_key.public_key()
                for pool_id, private_key in _POOL_PRIVATE_KEYS.items()
            }
        ),
        executor_lease_seconds=executor_lease_seconds,
        proposal_ttl_seconds=proposal_ttl_seconds,
        permit_ttl_seconds=permit_ttl_seconds,
    )


async def _committed_shadow(
    session: AsyncSession,
    *,
    pending_attempt_ids: tuple[str, ...] = ("attempt-pending",),
) -> tuple[
    CapacityManagementStore,
    WriterFence,
    tuple[CommittedShadowEpoch, ShadowEpochV1],
]:
    store = CapacityManagementStore()
    manifest = fleet_manifest()
    subject = subject_configuration(manifest)
    fleet_proposal = await store.propose_fleet_configuration(
        session,
        manifest,
        actor="fleet-operator",
        idempotency_key=CONFIG_KEY_A,
    )
    subject_proposal = await store.propose_subject_configuration(
        session,
        subject,
        actor="environment-state",
        idempotency_key=CONFIG_KEY_B,
    )
    await store.activate_configuration(
        session,
        configuration_activation(fleet=fleet_proposal, subjects=(subject_proposal,)),
        actor="fleet-operator",
        idempotency_key=ACTIVATION_KEY,
    )
    authority = (
        await session.execute(select(CapacityAuthorityState.authority_incarnation))
    ).scalar_one()
    writer = await store.register_writer(session, authority, expected_epoch=0)
    await store.ingest_demand_snapshot(
        session,
        demand_snapshot(sequence=1, pending_attempt_ids=pending_attempt_ids),
        actor="development",
    )
    for pool_id in ("gb10", "oldlab"):
        await store.ingest_pool_observation(
            session,
            pool_observation(sequence=1, pool_id=pool_id),
            actor=f"{pool_id}-reporter",
        )
    allocation_input = await store.load_allocation_input(session, writer)
    shadow = allocate_shadow(allocation_input)
    committed = await store.commit_shadow_epoch(session, writer, shadow)
    return store, writer, (committed, shadow)


def _registration(
    *,
    pool_id: str,
    incarnation: UUID = _EXECUTOR_INCARNATION,
    signing_key_sha256: str | None = None,
) -> DryRunExecutorRegistrationV1:
    if signing_key_sha256 is None:
        signing_key_sha256 = public_key_fingerprint(_POOL_PRIVATE_KEYS[pool_id].public_key())
    return DryRunExecutorRegistrationV1(
        executor_id=f"{pool_id}-executor",
        executor_incarnation=incarnation,
        pool_id=pool_id,
        pool_generation=1,
        signing_key_id=f"{pool_id}-key-1",
        signing_key_sha256=signing_key_sha256,
        local_authority_sha256="b" * 64,
    )


async def _proposal(
    session: AsyncSession,
    writer: WriterFence,
    committed_and_shadow: tuple[CommittedShadowEpoch, ShadowEpochV1],
    *,
    rank_index: int = 0,
    tranche_id: UUID = _TRANCHE_ID,
    intent_id: UUID = _INTENT_ID,
    executor_incarnation: UUID = _EXECUTOR_INCARNATION,
) -> DryRunReservationProposalV1:
    committed, shadow = committed_and_shadow
    ranked = shadow.hypothetical_launch_rank[rank_index]
    allocation = (
        await session.execute(
            select(CapacityAllocation).where(
                CapacityAllocation.allocation_epoch == committed.allocation_epoch,
                CapacityAllocation.subject_id == ranked.subject_id,
                CapacityAllocation.pool_id == ranked.pool_id,
            )
        )
    ).scalar_one()
    subject = (
        await session.execute(
            select(CapacitySubject).where(
                CapacitySubject.configuration_epoch == shadow.configuration.configuration_epoch,
                CapacitySubject.subject_id == allocation.subject_id,
                CapacitySubject.subject_incarnation == allocation.subject_incarnation,
            )
        )
    ).scalar_one()
    profile = (
        await session.execute(
            select(CapacityWorkerProfile).where(
                CapacityWorkerProfile.subject_id == allocation.subject_id,
                CapacityWorkerProfile.subject_incarnation == allocation.subject_incarnation,
                CapacityWorkerProfile.pool_id == allocation.pool_id,
                CapacityWorkerProfile.deployment_generation == allocation.deployment_generation,
            )
        )
    ).scalar_one()
    shape = profile.shape_catalog[0]
    witness = next(item for item in shadow.pool_witnesses if item.pool_id == allocation.pool_id)
    placement = next(
        item for item in witness.placements if item.instance_id == ranked.shape_instance_id
    )
    return DryRunReservationProposalV1(
        tranche_id=tranche_id,
        authority_incarnation=writer.authority_incarnation,
        writer_epoch=writer.writer_epoch,
        configuration_epoch=shadow.configuration.configuration_epoch,
        allocation_epoch=committed.allocation_epoch,
        subject_id=allocation.subject_id,
        subject_incarnation=allocation.subject_incarnation,
        account_id=subject.account_id,
        tier_id=subject.tier_id,
        candidate_generation=subject.candidate_generation,
        deployment_generation=allocation.deployment_generation,
        pool_id=allocation.pool_id,
        pool_generation=profile.pool_generation,
        executor_id=f"{allocation.pool_id}-executor",
        executor_incarnation=executor_incarnation,
        shapes=(
            ReservationShapeV1(
                shape_instance_id=ranked.shape_instance_id,
                intent_id=intent_id,
                shape_id=shape["shape_id"],
                profile_id=shape["shape_id"],
                profile_generation=profile.profile_generation,
                profile_digest=profile.profile_digest,
                concurrency_slots=shape["concurrency_slots"],
                resources=ResourceVectorV1.model_validate(shape["total_resources"]),
                node_ids=placement.node_ids,
            ),
        ),
    )


async def test_executor_registration_is_exact_replay_and_fences_predecessor(
    capacity_session: AsyncSession,
) -> None:
    _, writer, _ = await _committed_shadow(capacity_session)
    request = _registration(pool_id="gb10")
    with pytest.raises(GrantConflictError, match="signing key"):
        await CapacityGrantStore().register_executor(
            capacity_session,
            writer,
            request,
            actor="executor-installer",
            idempotency_key=UUID(int=999),
        )

    grants = _grant_store()

    first = await grants.register_executor(
        capacity_session,
        writer,
        request,
        actor="executor-installer",
        idempotency_key=_REGISTRATION_KEY,
    )
    replay = await grants.register_executor(
        capacity_session,
        writer,
        request,
        actor="executor-installer",
        idempotency_key=_REGISTRATION_KEY,
    )
    assert first.executor_row_id == replay.executor_row_id
    assert replay.replayed is True
    assert replay.executable is False

    changed = request.model_copy(update={"signing_key_sha256": "c" * 64})
    with pytest.raises(IdempotencyConflictError):
        await grants.register_executor(
            capacity_session,
            writer,
            changed,
            actor="executor-installer",
            idempotency_key=_REGISTRATION_KEY,
        )

    successor = await grants.register_executor(
        capacity_session,
        writer,
        _registration(pool_id="gb10", incarnation=_NEXT_EXECUTOR_INCARNATION),
        actor="executor-installer",
        idempotency_key=UUID(int=107),
    )
    states = (
        (
            await capacity_session.execute(
                select(CapacityExecutor.state).order_by(CapacityExecutor.created_at)
            )
        )
        .scalars()
        .all()
    )
    assert states == ["fenced", "dry-run"]
    assert successor.executable is False


async def test_executor_incarnation_cannot_move_between_pools(
    capacity_session: AsyncSession,
) -> None:
    _, writer, _ = await _committed_shadow(capacity_session)
    grants = _grant_store()
    await grants.register_executor(
        capacity_session,
        writer,
        _registration(pool_id="gb10"),
        actor="executor-installer",
        idempotency_key=_REGISTRATION_KEY,
    )

    with pytest.raises(GrantConflictError, match="incarnation"):
        await grants.register_executor(
            capacity_session,
            writer,
            _registration(pool_id="oldlab"),
            actor="executor-installer",
            idempotency_key=UUID(int=108),
        )


async def test_executor_heartbeat_renews_exactly_and_equivocation_is_fenced(
    capacity_session: AsyncSession,
) -> None:
    _, writer, _ = await _committed_shadow(capacity_session)
    grants = _grant_store(executor_lease_seconds=120)
    registered = await grants.register_executor(
        capacity_session,
        writer,
        _registration(pool_id="gb10"),
        actor="executor-installer",
        idempotency_key=_REGISTRATION_KEY,
    )
    heartbeat = DryRunExecutorHeartbeatV1(
        authority_incarnation=writer.authority_incarnation,
        writer_epoch=writer.writer_epoch,
        executor_id="gb10-executor",
        executor_incarnation=_EXECUTOR_INCARNATION,
        pool_id="gb10",
        pool_generation=1,
        heartbeat_sequence=1,
        journal_sequence=1,
        journal_digest="a" * 64,
    )

    first = await grants.heartbeat_executor(capacity_session, heartbeat)
    replay = await grants.heartbeat_executor(capacity_session, heartbeat)
    assert first.lease_expires_at > registered.lease_expires_at
    assert replay.lease_expires_at == first.lease_expires_at
    assert first.replayed is False
    assert replay.replayed is True
    assert replay.executable is False

    with pytest.raises(ExecutorEquivocationError):
        await grants.heartbeat_executor(
            capacity_session,
            heartbeat.model_copy(update={"journal_digest": "b" * 64}),
        )
    executor = (await capacity_session.execute(select(CapacityExecutor))).scalar_one()
    assert executor.state == "equivocal"


async def test_executor_heartbeat_fences_regressed_local_journal(
    capacity_session: AsyncSession,
) -> None:
    _, writer, _ = await _committed_shadow(capacity_session)
    grants = _grant_store()
    await grants.register_executor(
        capacity_session,
        writer,
        _registration(pool_id="oldlab"),
        actor="executor-installer",
        idempotency_key=_REGISTRATION_KEY,
    )
    common = {
        "authority_incarnation": writer.authority_incarnation,
        "writer_epoch": writer.writer_epoch,
        "executor_id": "oldlab-executor",
        "executor_incarnation": _EXECUTOR_INCARNATION,
        "pool_id": "oldlab",
        "pool_generation": 1,
        "journal_digest": "c" * 64,
    }
    await grants.heartbeat_executor(
        capacity_session,
        DryRunExecutorHeartbeatV1(
            **common,
            heartbeat_sequence=1,
            journal_sequence=2,
        ),
    )

    with pytest.raises(ExecutorJournalError, match="regressed"):
        await grants.heartbeat_executor(
            capacity_session,
            DryRunExecutorHeartbeatV1(
                **common,
                heartbeat_sequence=2,
                journal_sequence=1,
            ),
        )
    executor = (await capacity_session.execute(select(CapacityExecutor))).scalar_one()
    assert executor.state == "fenced"


async def test_executor_heartbeat_requires_central_journal_ancestry(
    capacity_session: AsyncSession,
) -> None:
    _, writer, _ = await _committed_shadow(capacity_session)
    grants = _grant_store()
    await grants.register_executor(
        capacity_session,
        writer,
        _registration(pool_id="oldlab"),
        actor="executor-installer",
        idempotency_key=_REGISTRATION_KEY,
    )
    base = DryRunExecutorHeartbeatV1(
        authority_incarnation=writer.authority_incarnation,
        writer_epoch=writer.writer_epoch,
        executor_id="oldlab-executor",
        executor_incarnation=_EXECUTOR_INCARNATION,
        pool_id="oldlab",
        pool_generation=1,
        heartbeat_sequence=1,
        journal_sequence=2,
        journal_digest="c" * 64,
    )
    await grants.heartbeat_executor(capacity_session, base)

    with pytest.raises(ExecutorJournalError, match="central checkpoint"):
        await grants.heartbeat_executor(
            capacity_session,
            base.model_copy(
                update={
                    "heartbeat_sequence": 2,
                    "journal_sequence": 3,
                    "journal_digest": "d" * 64,
                }
            ),
        )
    executor = (await capacity_session.execute(select(CapacityExecutor))).scalar_one()
    assert executor.state == "fenced"


async def test_expired_incumbent_can_recover_only_its_exact_journal(
    capacity_session: AsyncSession,
) -> None:
    _, writer, _ = await _committed_shadow(capacity_session)
    grants = _grant_store(executor_lease_seconds=1)
    await grants.register_executor(
        capacity_session,
        writer,
        _registration(pool_id="oldlab"),
        actor="executor-installer",
        idempotency_key=_REGISTRATION_KEY,
    )
    first = DryRunExecutorHeartbeatV1(
        authority_incarnation=writer.authority_incarnation,
        writer_epoch=writer.writer_epoch,
        executor_id="oldlab-executor",
        executor_incarnation=_EXECUTOR_INCARNATION,
        pool_id="oldlab",
        pool_generation=1,
        heartbeat_sequence=1,
        journal_sequence=1,
        journal_digest="c" * 64,
    )
    await grants.heartbeat_executor(capacity_session, first)
    await capacity_session.execute(
        update(CapacityExecutor).values(lease_expires_at=datetime.now(UTC) - timedelta(seconds=1))
    )

    recovered = await grants.heartbeat_executor(
        capacity_session,
        first.model_copy(
            update={
                "heartbeat_sequence": 2,
                "journal_checkpoint_sequence": 1,
                "journal_checkpoint_digest": "c" * 64,
            }
        ),
    )
    assert recovered.lease_expires_at > datetime.now(UTC)


def _ownership_metadata(
    proposal: DryRunReservationProposalV1,
) -> OwnershipMetadataV1:
    shape = proposal.shapes[0]
    return OwnershipMetadataV1(
        authority_incarnation=proposal.authority_incarnation,
        writer_epoch=proposal.writer_epoch,
        configuration_epoch=proposal.configuration_epoch,
        allocation_epoch=proposal.allocation_epoch,
        tranche_id=proposal.tranche_id,
        intent_id=shape.intent_id,
        shape_instance_id=shape.shape_instance_id,
        subject_id=proposal.subject_id,
        subject_incarnation=proposal.subject_incarnation,
        account_id=proposal.account_id,
        tier_id=proposal.tier_id,
        candidate_generation=proposal.candidate_generation,
        deployment_generation=proposal.deployment_generation,
        pool_id=proposal.pool_id,
        pool_generation=proposal.pool_generation,
        shape_id=shape.shape_id,
        profile_id=shape.profile_id,
        profile_generation=shape.profile_generation,
        profile_digest=shape.profile_digest,
        concurrency_slots=shape.concurrency_slots,
        resources=shape.resources,
        node_ids=shape.node_ids,
        executor_id=proposal.executor_id,
        executor_incarnation=proposal.executor_incarnation,
    )


def _protected_release_acknowledgement(
    proposal: DryRunReservationProposalV1,
    *,
    bootstrap_registration_epoch: int,
    protected_registration_epoch: int,
    protected_release_sha256: str,
    shape_index: int = 0,
) -> DryRunProtectedReleaseAcknowledgementV1:
    shape = proposal.shapes[shape_index]
    return DryRunProtectedReleaseAcknowledgementV1(
        authority_incarnation=proposal.authority_incarnation,
        writer_epoch=proposal.writer_epoch,
        configuration_epoch=proposal.configuration_epoch,
        allocation_epoch=proposal.allocation_epoch,
        tranche_id=proposal.tranche_id,
        shape_instance_id=shape.shape_instance_id,
        intent_id=shape.intent_id,
        subject_id=proposal.subject_id,
        subject_incarnation=proposal.subject_incarnation,
        reporter_incarnation=DEMAND_REPORTER_ID,
        deployment_generation=proposal.deployment_generation,
        pool_id=proposal.pool_id,
        pool_generation=proposal.pool_generation,
        bootstrap_registration_epoch=bootstrap_registration_epoch,
        protected_registration_epoch=protected_registration_epoch,
        bootstrap_revoked=True,
        protected_release_sha256=protected_release_sha256,
    )


async def _accepted_with_ownership_key(
    session: AsyncSession,
) -> tuple[
    CapacityGrantStore,
    WriterFence,
    DryRunReservationProposalV1,
    Ed25519PrivateKey,
]:
    _, writer, committed = await _committed_shadow(session)
    proposal = await _proposal(session, writer, committed)
    private_key = Ed25519PrivateKey.generate()
    public_key = private_key.public_key()
    grants = CapacityGrantStore(
        ownership_keyring=OwnershipKeyring({f"{proposal.pool_id}-key-1": public_key})
    )
    await grants.register_executor(
        session,
        writer,
        _registration(
            pool_id=proposal.pool_id,
            signing_key_sha256=public_key_fingerprint(public_key),
        ),
        actor="executor-installer",
        idempotency_key=_REGISTRATION_KEY,
    )
    proposed = await grants.propose_reservation(
        session,
        writer,
        proposal,
        idempotency_key=_PROPOSAL_KEY,
    )
    await grants.accept_reservation(
        session,
        DryRunReservationAcceptanceV1(
            tranche_id=proposal.tranche_id,
            proposal_digest=proposed.proposal_digest,
            executor_id=proposal.executor_id,
            executor_incarnation=proposal.executor_incarnation,
            command_sequence=1,
        ),
    )
    return grants, writer, proposal, private_key


async def _consumed_with_ownership_key(
    session: AsyncSession,
) -> tuple[
    CapacityGrantStore,
    WriterFence,
    DryRunReservationProposalV1,
    Ed25519PrivateKey,
]:
    grants, writer, proposal, private_key = await _accepted_with_ownership_key(session)
    await grants.register_bootstrap(
        session,
        DryRunBootstrapRegistrationV1(
            tranche_id=proposal.tranche_id,
            intent_id=proposal.shapes[0].intent_id,
            executor_id=proposal.executor_id,
            executor_incarnation=proposal.executor_incarnation,
            command_sequence=2,
            bootstrap_registration_epoch=1,
            bootstrap_evidence_sha256="b" * 64,
        ),
    )
    permit = DryRunLaunchPermitV1(
        permit_id=UUID(int=116),
        intent_id=proposal.shapes[0].intent_id,
        allocation_epoch=proposal.allocation_epoch,
        configuration_epoch=proposal.configuration_epoch,
        executor_id=proposal.executor_id,
        executor_incarnation=proposal.executor_incarnation,
        permit_epoch=1,
        launch_rank=1,
    )
    issued = await grants.issue_launch_permit(
        session,
        writer,
        permit,
        idempotency_key=UUID(int=117),
    )
    await grants.consume_launch_permit(
        session,
        DryRunPermitConsumptionV1(
            permit_id=permit.permit_id,
            permit_digest=issued.permit_digest,
            intent_id=permit.intent_id,
            executor_id=permit.executor_id,
            executor_incarnation=permit.executor_incarnation,
            command_sequence=3,
        ),
    )
    return grants, writer, proposal, private_key


async def test_complete_executor_inventory_authenticates_and_replays_exact_job(
    capacity_session: AsyncSession,
) -> None:
    grants, writer, proposal, private_key = await _accepted_with_ownership_key(capacity_session)
    await grants.register_bootstrap(
        capacity_session,
        DryRunBootstrapRegistrationV1(
            tranche_id=proposal.tranche_id,
            intent_id=proposal.shapes[0].intent_id,
            executor_id=proposal.executor_id,
            executor_incarnation=proposal.executor_incarnation,
            command_sequence=2,
            bootstrap_registration_epoch=1,
            bootstrap_evidence_sha256="b" * 64,
        ),
    )
    permit = DryRunLaunchPermitV1(
        permit_id=UUID(int=112),
        intent_id=proposal.shapes[0].intent_id,
        allocation_epoch=proposal.allocation_epoch,
        configuration_epoch=proposal.configuration_epoch,
        executor_id=proposal.executor_id,
        executor_incarnation=proposal.executor_incarnation,
        permit_epoch=1,
        launch_rank=1,
    )
    issued = await grants.issue_launch_permit(
        capacity_session,
        writer,
        permit,
        idempotency_key=UUID(int=113),
    )
    await grants.consume_launch_permit(
        capacity_session,
        DryRunPermitConsumptionV1(
            permit_id=permit.permit_id,
            permit_digest=issued.permit_digest,
            intent_id=permit.intent_id,
            executor_id=permit.executor_id,
            executor_incarnation=permit.executor_incarnation,
            command_sequence=3,
        ),
    )
    proof = sign_ownership(
        private_key,
        signing_key_id=f"{proposal.pool_id}-key-1",
        metadata=_ownership_metadata(proposal),
    )
    inventory = DryRunExecutorInventoryV1(
        authority_incarnation=writer.authority_incarnation,
        writer_epoch=writer.writer_epoch,
        executor_id=proposal.executor_id,
        executor_incarnation=proposal.executor_incarnation,
        pool_id=proposal.pool_id,
        pool_generation=proposal.pool_generation,
        inventory_sequence=1,
        journal_sequence=3,
        journal_digest="d" * 64,
        records=(
            ExecutorInventoryRecordV1(
                physical_identity="job-1001",
                physical_kind="slurm-job",
                authority_scope="registered-loom",
                state="pending",
                resources=proposal.shapes[0].resources,
                node_ids=proposal.shapes[0].node_ids,
                controller_evidence_sha256="e" * 64,
                ownership_proof=proof,
            ),
        ),
    )

    first = await grants.ingest_executor_inventory(capacity_session, inventory)
    replay = await grants.ingest_executor_inventory(capacity_session, inventory)
    observed = (await capacity_session.execute(select(CapacityObservedCommitment))).scalar_one()
    intent = (await capacity_session.execute(select(CapacitySubmissionIntent))).scalar_one()
    assert first.authenticated_count == 1
    assert first.quarantined_count == first.foreign_count == 0
    assert replay.observation_id == first.observation_id
    assert replay.replayed is True
    assert observed.commitment_identity == "job-1001"
    assert observed.state == "pending"
    assert intent.state == "bound"
    assert intent.ownership_metadata_sha256 == canonical_grant_digest(proof.metadata)
    allocation_input = await CapacityManagementStore().load_allocation_input(
        capacity_session,
        writer,
    )
    assert {item.kind for item in allocation_input.observed_commitments} >= {
        "physical",
        "reserve",
    }
    reconciled = allocate_shadow(allocation_input)
    reconciled_allocation = next(
        item
        for item in reconciled.allocations
        if item.subject_id == proposal.subject_id and item.pool_id == proposal.pool_id
    )
    assert reconciled_allocation.desired_slots == 1
    assert "fixed_commitment_mapping_ambiguous" not in reconciled_allocation.blockers
    assert [
        item
        for item in reconciled.hypothetical_launch_rank
        if item.subject_id == proposal.subject_id and item.pool_id == proposal.pool_id
    ] == []

    missing = inventory.model_copy(update={"inventory_sequence": 2, "records": ()})
    missing_result = await grants.ingest_executor_inventory(capacity_session, missing)
    await capacity_session.refresh(observed)
    await capacity_session.refresh(intent)
    assert missing_result.authenticated_count == 0
    assert observed.state == "quarantined"
    assert intent.state == "quarantined"
    delayed_inventory = await grants.ingest_executor_inventory(
        capacity_session,
        inventory,
    )
    assert delayed_inventory.observation_id == first.observation_id
    assert delayed_inventory.replayed is True
    await capacity_session.refresh(observed)
    assert observed.state == "quarantined"

    changed = missing.model_copy(
        update={
            "records": (
                inventory.records[0].model_copy(update={"controller_evidence_sha256": "f" * 64}),
            )
        }
    )
    with pytest.raises(ExecutorEquivocationError):
        await grants.ingest_executor_inventory(capacity_session, changed)
    executor = (await capacity_session.execute(select(CapacityExecutor))).scalar_one()
    assert executor.state == "equivocal"


async def test_foreign_reclassification_quarantines_prior_authenticated_identity(
    capacity_session: AsyncSession,
) -> None:
    grants, writer, proposal, private_key = await _consumed_with_ownership_key(capacity_session)
    proof = sign_ownership(
        private_key,
        signing_key_id=f"{proposal.pool_id}-key-1",
        metadata=_ownership_metadata(proposal),
    )
    authenticated = ExecutorInventoryRecordV1(
        physical_identity="job-lost-authority",
        physical_kind="slurm-job",
        authority_scope="registered-loom",
        state="pending",
        resources=proposal.shapes[0].resources,
        node_ids=proposal.shapes[0].node_ids,
        controller_evidence_sha256="c" * 64,
        ownership_proof=proof,
    )

    def inventory(
        sequence: int,
        record: ExecutorInventoryRecordV1,
    ) -> DryRunExecutorInventoryV1:
        return DryRunExecutorInventoryV1(
            authority_incarnation=writer.authority_incarnation,
            writer_epoch=writer.writer_epoch,
            executor_id=proposal.executor_id,
            executor_incarnation=proposal.executor_incarnation,
            pool_id=proposal.pool_id,
            pool_generation=proposal.pool_generation,
            inventory_sequence=sequence,
            journal_sequence=3,
            journal_digest="d" * 64,
            records=(record,),
        )

    first = await grants.ingest_executor_inventory(
        capacity_session,
        inventory(1, authenticated),
    )
    assert first.authenticated_count == 1
    foreign = authenticated.model_copy(
        update={
            "authority_scope": "foreign",
            "state": "active",
            "controller_evidence_sha256": "e" * 64,
            "ownership_proof": None,
        }
    )
    second = await grants.ingest_executor_inventory(
        capacity_session,
        inventory(2, foreign),
    )
    commitment = (await capacity_session.execute(select(CapacityObservedCommitment))).scalar_one()
    intent = (await capacity_session.execute(select(CapacitySubmissionIntent))).scalar_one()
    assert second.foreign_count == 1
    assert commitment.state == "quarantined"
    assert intent.state == "quarantined"


async def test_conflicting_ownership_reclassification_quarantines_prior_intent(
    capacity_session: AsyncSession,
) -> None:
    grants, writer, proposal, private_key = await _consumed_with_ownership_key(capacity_session)
    metadata = _ownership_metadata(proposal)
    authenticated = ExecutorInventoryRecordV1(
        physical_identity="job-conflicting-ownership",
        physical_kind="slurm-job",
        authority_scope="registered-loom",
        state="pending",
        resources=proposal.shapes[0].resources,
        node_ids=proposal.shapes[0].node_ids,
        controller_evidence_sha256="c" * 64,
        ownership_proof=sign_ownership(
            private_key,
            signing_key_id=f"{proposal.pool_id}-key-1",
            metadata=metadata,
        ),
    )

    def inventory(
        sequence: int,
        record: ExecutorInventoryRecordV1,
    ) -> DryRunExecutorInventoryV1:
        return DryRunExecutorInventoryV1(
            authority_incarnation=writer.authority_incarnation,
            writer_epoch=writer.writer_epoch,
            executor_id=proposal.executor_id,
            executor_incarnation=proposal.executor_incarnation,
            pool_id=proposal.pool_id,
            pool_generation=proposal.pool_generation,
            inventory_sequence=sequence,
            journal_sequence=3,
            journal_digest="d" * 64,
            records=(record,),
        )

    await grants.ingest_executor_inventory(capacity_session, inventory(1, authenticated))
    conflicting = authenticated.model_copy(
        update={
            "controller_evidence_sha256": "e" * 64,
            "ownership_proof": sign_ownership(
                private_key,
                signing_key_id=f"{proposal.pool_id}-key-1",
                metadata=metadata.model_copy(update={"account_id": "another-account"}),
            ),
        }
    )
    second = await grants.ingest_executor_inventory(
        capacity_session,
        inventory(2, conflicting),
    )

    intent = (await capacity_session.execute(select(CapacitySubmissionIntent))).scalar_one()
    commitment = (
        await capacity_session.execute(
            select(CapacityObservedCommitment).where(
                CapacityObservedCommitment.commitment_identity == "job-conflicting-ownership"
            )
        )
    ).scalar_one()
    assert second.quarantined_count == 1
    assert commitment.state == "quarantined"
    assert intent.state == "quarantined"


async def test_physical_identity_cannot_move_between_two_valid_intents(
    capacity_session: AsyncSession,
) -> None:
    _management, writer, committed = await _committed_shadow(
        capacity_session,
        pending_attempt_ids=tuple(f"attempt-identity-conflict-{index}" for index in range(6)),
    )
    shadow = committed[1]
    ranks_by_pool: dict[str, list[int]] = {}
    for index, ranked in enumerate(shadow.hypothetical_launch_rank):
        ranks_by_pool.setdefault(ranked.pool_id, []).append(index)
    first_index, second_index = next(
        tuple(indices[:2]) for indices in ranks_by_pool.values() if len(indices) >= 2
    )
    first = await _proposal(
        capacity_session,
        writer,
        committed,
        rank_index=first_index,
        intent_id=UUID(int=401),
    )
    second = await _proposal(
        capacity_session,
        writer,
        committed,
        rank_index=second_index,
        tranche_id=first.tranche_id,
        intent_id=UUID(int=402),
    )
    proposal = DryRunReservationProposalV1.model_validate(
        {
            **first.model_dump(mode="python"),
            "shapes": first.shapes + second.shapes,
        }
    )
    private_key = Ed25519PrivateKey.generate()
    public_key = private_key.public_key()
    grants = CapacityGrantStore(
        ownership_keyring=OwnershipKeyring({f"{proposal.pool_id}-key-1": public_key})
    )
    await grants.register_executor(
        capacity_session,
        writer,
        _registration(
            pool_id=proposal.pool_id,
            signing_key_sha256=public_key_fingerprint(public_key),
        ),
        actor="executor-installer",
        idempotency_key=_REGISTRATION_KEY,
    )
    proposed = await grants.propose_reservation(
        capacity_session,
        writer,
        proposal,
        idempotency_key=_PROPOSAL_KEY,
    )
    await grants.accept_reservation(
        capacity_session,
        DryRunReservationAcceptanceV1(
            tranche_id=proposal.tranche_id,
            proposal_digest=proposed.proposal_digest,
            executor_id=proposal.executor_id,
            executor_incarnation=proposal.executor_incarnation,
            command_sequence=1,
        ),
    )

    permits: dict[UUID, tuple[UUID, str]] = {}
    rank_by_shape = {
        ranked.shape_instance_id: ranked.rank for ranked in shadow.hypothetical_launch_rank
    }
    for offset, shape in enumerate(proposal.shapes, start=2):
        await grants.register_bootstrap(
            capacity_session,
            DryRunBootstrapRegistrationV1(
                tranche_id=proposal.tranche_id,
                intent_id=shape.intent_id,
                executor_id=proposal.executor_id,
                executor_incarnation=proposal.executor_incarnation,
                command_sequence=offset,
                bootstrap_registration_epoch=1,
                bootstrap_evidence_sha256=f"{offset:064x}",
            ),
        )
        permit = DryRunLaunchPermitV1(
            permit_id=UUID(int=410 + offset),
            intent_id=shape.intent_id,
            allocation_epoch=proposal.allocation_epoch,
            configuration_epoch=proposal.configuration_epoch,
            executor_id=proposal.executor_id,
            executor_incarnation=proposal.executor_incarnation,
            permit_epoch=1,
            launch_rank=rank_by_shape[shape.shape_instance_id],
        )
        issued = await grants.issue_launch_permit(
            capacity_session,
            writer,
            permit,
            idempotency_key=UUID(int=420 + offset),
        )
        permits[shape.intent_id] = (permit.permit_id, issued.permit_digest)

    for command_sequence, shape in enumerate(
        sorted(proposal.shapes, key=lambda item: rank_by_shape[item.shape_instance_id]),
        start=4,
    ):
        permit_id, permit_digest = permits[shape.intent_id]
        await grants.consume_launch_permit(
            capacity_session,
            DryRunPermitConsumptionV1(
                permit_id=permit_id,
                permit_digest=permit_digest,
                intent_id=shape.intent_id,
                executor_id=proposal.executor_id,
                executor_incarnation=proposal.executor_incarnation,
                command_sequence=command_sequence,
            ),
        )

    def inventory(
        sequence: int,
        shape_index: int,
        state: str,
    ) -> DryRunExecutorInventoryV1:
        shape = proposal.shapes[shape_index]
        metadata = _ownership_metadata(
            proposal.model_copy(update={"shapes": (shape,)})
        )
        return DryRunExecutorInventoryV1(
            authority_incarnation=writer.authority_incarnation,
            writer_epoch=writer.writer_epoch,
            executor_id=proposal.executor_id,
            executor_incarnation=proposal.executor_incarnation,
            pool_id=proposal.pool_id,
            pool_generation=proposal.pool_generation,
            inventory_sequence=sequence,
            journal_sequence=5,
            journal_digest="d" * 64,
            records=(
                ExecutorInventoryRecordV1(
                    physical_identity="job-reused-between-valid-intents",
                    physical_kind="slurm-job",
                    authority_scope="registered-loom",
                    state=state,
                    resources=shape.resources,
                    node_ids=shape.node_ids,
                    controller_evidence_sha256=f"{sequence:064x}",
                    terminal_evidence_sha256=("e" * 64 if state == "terminal" else None),
                    ownership_proof=sign_ownership(
                        private_key,
                        signing_key_id=f"{proposal.pool_id}-key-1",
                        metadata=metadata,
                    ),
                ),
            ),
        )

    first_result = await grants.ingest_executor_inventory(
        capacity_session,
        inventory(1, 0, "pending"),
    )
    conflict_result = await grants.ingest_executor_inventory(
        capacity_session,
        inventory(2, 1, "terminal"),
    )

    intents = {
        row.id: row.state
        for row in (
            (await capacity_session.execute(select(CapacitySubmissionIntent))).scalars().all()
        )
    }
    shapes = {
        row.intent_id: row.state
        for row in (
            (await capacity_session.execute(select(CapacityReservationShape))).scalars().all()
        )
    }
    commitments = (
        (await capacity_session.execute(select(CapacityObservedCommitment))).scalars().all()
    )
    assert first_result.authenticated_count == 1
    assert conflict_result.authenticated_count == 0
    assert conflict_result.quarantined_count == 1
    assert set(intents.values()) == {"quarantined"}
    assert set(shapes.values()) == {"accepted"}
    assert len(commitments) == 2
    assert {row.state for row in commitments} == {"quarantined"}


@pytest.mark.parametrize(
    "changed_fields",
    (
        {"terminal_evidence_sha256": "f" * 64},
        {"physical_kind": "worker"},
    ),
    ids=("terminal-evidence", "physical-kind"),
)
async def test_terminal_physical_identity_conflict_blocks_release(
    capacity_session: AsyncSession,
    changed_fields: dict[str, str],
) -> None:
    grants, writer, proposal, private_key = await _consumed_with_ownership_key(
        capacity_session
    )
    proof = sign_ownership(
        private_key,
        signing_key_id=f"{proposal.pool_id}-key-1",
        metadata=_ownership_metadata(proposal),
    )
    terminal = ExecutorInventoryRecordV1(
        physical_identity="job-terminal-evidence-conflict",
        physical_kind="slurm-job",
        authority_scope="registered-loom",
        state="terminal",
        resources=proposal.shapes[0].resources,
        node_ids=proposal.shapes[0].node_ids,
        controller_evidence_sha256="c" * 64,
        ownership_proof=proof,
        terminal_evidence_sha256="e" * 64,
    )

    def inventory(
        sequence: int,
        record: ExecutorInventoryRecordV1,
    ) -> DryRunExecutorInventoryV1:
        return DryRunExecutorInventoryV1(
            authority_incarnation=writer.authority_incarnation,
            writer_epoch=writer.writer_epoch,
            executor_id=proposal.executor_id,
            executor_incarnation=proposal.executor_incarnation,
            pool_id=proposal.pool_id,
            pool_generation=proposal.pool_generation,
            inventory_sequence=sequence,
            journal_sequence=3,
            journal_digest="d" * 64,
            records=(record,),
        )

    first = await grants.ingest_executor_inventory(
        capacity_session,
        inventory(1, terminal),
    )
    changed = terminal.model_copy(
        update={
            "controller_evidence_sha256": "a" * 64,
            **changed_fields,
        }
    )
    conflict = await grants.ingest_executor_inventory(
        capacity_session,
        inventory(2, changed),
    )
    reverted = await grants.ingest_executor_inventory(
        capacity_session,
        inventory(3, terminal),
    )

    intent = (await capacity_session.execute(select(CapacitySubmissionIntent))).scalar_one()
    commitments = (
        (await capacity_session.execute(select(CapacityObservedCommitment))).scalars().all()
    )
    assert first.authenticated_count == 1
    assert conflict.authenticated_count == 0
    assert conflict.quarantined_count == 1
    assert reverted.authenticated_count == 0
    assert reverted.quarantined_count == 1
    assert intent.state == "quarantined"
    assert {row.state for row in commitments} == {"quarantined"}


async def test_forged_and_foreign_inventory_never_authenticate_or_disappear(
    capacity_session: AsyncSession,
) -> None:
    grants, writer, proposal, private_key = await _accepted_with_ownership_key(capacity_session)
    valid = sign_ownership(
        private_key,
        signing_key_id=f"{proposal.pool_id}-key-1",
        metadata=_ownership_metadata(proposal),
    )
    forged = valid.model_copy(
        update={"metadata": valid.metadata.model_copy(update={"allocation_epoch": 99})}
    )
    inventory = DryRunExecutorInventoryV1(
        authority_incarnation=writer.authority_incarnation,
        writer_epoch=writer.writer_epoch,
        executor_id=proposal.executor_id,
        executor_incarnation=proposal.executor_incarnation,
        pool_id=proposal.pool_id,
        pool_generation=proposal.pool_generation,
        inventory_sequence=1,
        journal_sequence=1,
        journal_digest="d" * 64,
        records=(
            ExecutorInventoryRecordV1(
                physical_identity="job-premature",
                physical_kind="slurm-job",
                authority_scope="registered-loom",
                state="pending",
                resources=proposal.shapes[0].resources,
                node_ids=proposal.shapes[0].node_ids,
                controller_evidence_sha256="d" * 64,
                ownership_proof=valid,
            ),
            ExecutorInventoryRecordV1(
                physical_identity="job-forged",
                physical_kind="slurm-job",
                authority_scope="dedicated-loom-association",
                state="unknown",
                resources=proposal.shapes[0].resources,
                node_ids=proposal.shapes[0].node_ids,
                controller_evidence_sha256="e" * 64,
                ownership_proof=forged,
            ),
            ExecutorInventoryRecordV1(
                physical_identity="job-foreign",
                physical_kind="slurm-job",
                authority_scope="foreign",
                state="active",
                resources=proposal.shapes[0].resources,
                controller_evidence_sha256="f" * 64,
            ),
        ),
    )

    result = await grants.ingest_executor_inventory(capacity_session, inventory)
    observation = (await capacity_session.execute(select(CapacityExecutorObservation))).scalar_one()
    commitments = (
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
    assert result.authenticated_count == 0
    assert result.quarantined_count == 2
    assert result.foreign_count == 1
    assert observation.validity == "valid"
    assert [(item.commitment_identity, item.state) for item in commitments] == [
        ("job-forged", "quarantined"),
        ("job-premature", "quarantined"),
    ]
    intent = (await capacity_session.execute(select(CapacitySubmissionIntent))).scalar_one()
    assert intent.state == "quarantined"


async def test_duplicate_authenticated_jobs_for_one_intent_are_both_quarantined(
    capacity_session: AsyncSession,
) -> None:
    grants, writer, proposal, private_key = await _consumed_with_ownership_key(capacity_session)
    proof = sign_ownership(
        private_key,
        signing_key_id=f"{proposal.pool_id}-key-1",
        metadata=_ownership_metadata(proposal),
    )
    record = ExecutorInventoryRecordV1(
        physical_identity="job-duplicate-a",
        physical_kind="slurm-job",
        authority_scope="registered-loom",
        state="pending",
        resources=proposal.shapes[0].resources,
        node_ids=proposal.shapes[0].node_ids,
        controller_evidence_sha256="c" * 64,
        ownership_proof=proof,
    )
    inventory = DryRunExecutorInventoryV1(
        authority_incarnation=writer.authority_incarnation,
        writer_epoch=writer.writer_epoch,
        executor_id=proposal.executor_id,
        executor_incarnation=proposal.executor_incarnation,
        pool_id=proposal.pool_id,
        pool_generation=proposal.pool_generation,
        inventory_sequence=1,
        journal_sequence=3,
        journal_digest="d" * 64,
        records=(
            record,
            record.model_copy(
                update={
                    "physical_identity": "job-duplicate-b",
                    "controller_evidence_sha256": "e" * 64,
                }
            ),
        ),
    )

    result = await grants.ingest_executor_inventory(capacity_session, inventory)

    intent = (await capacity_session.execute(select(CapacitySubmissionIntent))).scalar_one()
    commitments = (
        (await capacity_session.execute(select(CapacityObservedCommitment))).scalars().all()
    )
    assert (result.authenticated_count, result.quarantined_count) == (0, 2)
    assert intent.state == "quarantined"
    assert len(commitments) == 2
    assert {item.state for item in commitments} == {"quarantined"}


async def test_resource_mismatch_quarantine_cannot_rebind_but_can_reach_terminal(
    capacity_session: AsyncSession,
) -> None:
    grants, writer, proposal, private_key = await _consumed_with_ownership_key(capacity_session)
    proof = sign_ownership(
        private_key,
        signing_key_id=f"{proposal.pool_id}-key-1",
        metadata=_ownership_metadata(proposal),
    )
    base = ExecutorInventoryRecordV1(
        physical_identity="job-resource-mismatch",
        physical_kind="slurm-job",
        authority_scope="registered-loom",
        state="pending",
        resources=proposal.shapes[0].resources.model_copy(
            update={"cpu_millicores": proposal.shapes[0].resources.cpu_millicores + 1}
        ),
        node_ids=proposal.shapes[0].node_ids,
        controller_evidence_sha256="c" * 64,
        ownership_proof=proof,
    )

    def inventory(
        sequence: int,
        record: ExecutorInventoryRecordV1,
    ) -> DryRunExecutorInventoryV1:
        return DryRunExecutorInventoryV1(
            authority_incarnation=writer.authority_incarnation,
            writer_epoch=writer.writer_epoch,
            executor_id=proposal.executor_id,
            executor_incarnation=proposal.executor_incarnation,
            pool_id=proposal.pool_id,
            pool_generation=proposal.pool_generation,
            inventory_sequence=sequence,
            journal_sequence=3,
            journal_digest="d" * 64,
            records=(record,),
        )

    mismatched = await grants.ingest_executor_inventory(
        capacity_session,
        inventory(1, base),
    )
    intent = (await capacity_session.execute(select(CapacitySubmissionIntent))).scalar_one()
    assert mismatched.quarantined_count == 1
    assert intent.state == "quarantined"

    exact_pending = base.model_copy(
        update={
            "resources": proposal.shapes[0].resources,
            "controller_evidence_sha256": "e" * 64,
        }
    )
    still_quarantined = await grants.ingest_executor_inventory(
        capacity_session,
        inventory(2, exact_pending),
    )
    await capacity_session.refresh(intent)
    assert still_quarantined.quarantined_count == 1
    assert intent.state == "quarantined"

    terminal = exact_pending.model_copy(
        update={
            "state": "terminal",
            "terminal_evidence_sha256": "f" * 64,
        }
    )
    terminal_result = await grants.ingest_executor_inventory(
        capacity_session,
        inventory(3, terminal),
    )
    await capacity_session.refresh(intent)
    assert terminal_result.authenticated_count == 1
    assert intent.state == "terminal"


async def test_authenticated_terminal_inventory_is_required_for_physical_release(
    capacity_session: AsyncSession,
) -> None:
    grants, writer, proposal, private_key = await _accepted_with_ownership_key(capacity_session)
    await grants.register_bootstrap(
        capacity_session,
        DryRunBootstrapRegistrationV1(
            tranche_id=proposal.tranche_id,
            intent_id=proposal.shapes[0].intent_id,
            executor_id=proposal.executor_id,
            executor_incarnation=proposal.executor_incarnation,
            command_sequence=2,
            bootstrap_registration_epoch=1,
            bootstrap_evidence_sha256="b" * 64,
        ),
    )
    permit = DryRunLaunchPermitV1(
        permit_id=UUID(int=114),
        intent_id=proposal.shapes[0].intent_id,
        allocation_epoch=proposal.allocation_epoch,
        configuration_epoch=proposal.configuration_epoch,
        executor_id=proposal.executor_id,
        executor_incarnation=proposal.executor_incarnation,
        permit_epoch=1,
        launch_rank=1,
    )
    issued = await grants.issue_launch_permit(
        capacity_session,
        writer,
        permit,
        idempotency_key=UUID(int=115),
    )
    await grants.consume_launch_permit(
        capacity_session,
        DryRunPermitConsumptionV1(
            permit_id=permit.permit_id,
            permit_digest=issued.permit_digest,
            intent_id=permit.intent_id,
            executor_id=permit.executor_id,
            executor_incarnation=permit.executor_incarnation,
            command_sequence=3,
        ),
    )
    terminal_digest = "e" * 64
    inventory = DryRunExecutorInventoryV1(
        authority_incarnation=writer.authority_incarnation,
        writer_epoch=writer.writer_epoch,
        executor_id=proposal.executor_id,
        executor_incarnation=proposal.executor_incarnation,
        pool_id=proposal.pool_id,
        pool_generation=proposal.pool_generation,
        inventory_sequence=1,
        journal_sequence=3,
        journal_digest="d" * 64,
        records=(
            ExecutorInventoryRecordV1(
                physical_identity="job-terminal",
                physical_kind="slurm-job",
                authority_scope="registered-loom",
                state="terminal",
                resources=proposal.shapes[0].resources,
                node_ids=proposal.shapes[0].node_ids,
                controller_evidence_sha256="c" * 64,
                terminal_evidence_sha256=terminal_digest,
                ownership_proof=sign_ownership(
                    private_key,
                    signing_key_id=f"{proposal.pool_id}-key-1",
                    metadata=_ownership_metadata(proposal),
                ),
            ),
        ),
    )
    await grants.ingest_executor_inventory(capacity_session, inventory)
    base_release = DryRunPartialReleaseV1(
        tranche_id=proposal.tranche_id,
        executor_id=proposal.executor_id,
        executor_incarnation=proposal.executor_incarnation,
        command_sequence=4,
        releases=(
            ReleasedShapeV1(
                shape_instance_id=proposal.shapes[0].shape_instance_id,
                intent_id=proposal.shapes[0].intent_id,
                inventory_sequence=1,
                terminal_kind="slurm-job",
                terminal_identity="job-terminal",
                terminal_evidence_sha256=terminal_digest,
                protected_registration_epoch=1,
                bootstrap_revoked=True,
                protected_release_sha256="f" * 64,
            ),
        ),
    )

    with pytest.raises(GrantConflictError, match="protection epoch"):
        await grants.release_shapes(capacity_session, base_release)
    release = base_release.model_copy(
        update={
            "releases": (
                base_release.releases[0].model_copy(update={"protected_registration_epoch": 2}),
            )
        }
    )
    with pytest.raises(GrantConflictError, match="protected-agent acknowledgement"):
        await grants.release_shapes(capacity_session, release)
    acknowledgement = _protected_release_acknowledgement(
        proposal,
        bootstrap_registration_epoch=1,
        protected_registration_epoch=2,
        protected_release_sha256="f" * 64,
    )
    first_ack = await grants.acknowledge_protected_release(
        capacity_session,
        acknowledgement,
        actor="development-agent",
        idempotency_key=UUID(int=117),
    )
    replay_ack = await grants.acknowledge_protected_release(
        capacity_session,
        acknowledgement,
        actor="development-agent",
        idempotency_key=UUID(int=117),
    )
    assert replay_ack.acknowledgement_id == first_ack.acknowledgement_id
    assert replay_ack.replayed is True
    differently_keyed_replay = await grants.acknowledge_protected_release(
        capacity_session,
        acknowledgement,
        actor="development-agent",
        idempotency_key=UUID(int=118),
    )
    assert differently_keyed_replay.acknowledgement_id == first_ack.acknowledgement_id
    assert differently_keyed_replay.replayed is True
    with pytest.raises(IdempotencyConflictError):
        await grants.acknowledge_protected_release(
            capacity_session,
            acknowledgement.model_copy(update={"protected_release_sha256": "a" * 64}),
            actor="development-agent",
            idempotency_key=UUID(int=117),
        )
    with pytest.raises(GrantConflictError, match="acknowledgement conflicts"):
        await grants.acknowledge_protected_release(
            capacity_session,
            acknowledgement.model_copy(update={"protected_release_sha256": "a" * 64}),
            actor="development-agent",
            idempotency_key=UUID(int=119),
        )
    await grants.release_shapes(capacity_session, release)

    intent = (await capacity_session.execute(select(CapacitySubmissionIntent))).scalar_one()
    tranche = (await capacity_session.execute(select(CapacityReservationTranche))).scalar_one()
    physical_count = (
        await capacity_session.execute(select(func.count()).select_from(CapacityObservedCommitment))
    ).scalar_one()
    assert intent.state == "closed"
    assert (tranche.state, tranche.closure_reason) == ("closed", "fully-released")
    assert physical_count == 0


async def test_executor_heartbeat_and_inventory_continue_for_unavailable_pool(
    capacity_session: AsyncSession,
) -> None:
    grants, writer, proposal, _ = await _accepted_with_ownership_key(capacity_session)
    await capacity_session.execute(
        update(CapacityPool)
        .where(
            CapacityPool.configuration_epoch == proposal.configuration_epoch,
            CapacityPool.pool_id == proposal.pool_id,
            CapacityPool.pool_generation == proposal.pool_generation,
        )
        .values(health="unavailable")
    )
    heartbeat = await grants.heartbeat_executor(
        capacity_session,
        DryRunExecutorHeartbeatV1(
            authority_incarnation=writer.authority_incarnation,
            writer_epoch=writer.writer_epoch,
            executor_id=proposal.executor_id,
            executor_incarnation=proposal.executor_incarnation,
            pool_id=proposal.pool_id,
            pool_generation=proposal.pool_generation,
            heartbeat_sequence=1,
            journal_sequence=1,
            journal_digest="d" * 64,
        ),
    )
    inventory = DryRunExecutorInventoryV1(
        authority_incarnation=writer.authority_incarnation,
        writer_epoch=writer.writer_epoch,
        executor_id=proposal.executor_id,
        executor_incarnation=proposal.executor_incarnation,
        pool_id=proposal.pool_id,
        pool_generation=proposal.pool_generation,
        inventory_sequence=1,
        journal_sequence=1,
        journal_digest="d" * 64,
    )

    ingested = await grants.ingest_executor_inventory(capacity_session, inventory)
    assert heartbeat.executable is False
    assert ingested.inventory_sequence == 1
    assert ingested.executable is False


async def test_unhealthy_pool_observation_blocks_proposal_and_acceptance(
    capacity_session: AsyncSession,
) -> None:
    management, writer, committed = await _committed_shadow(capacity_session)
    proposal = await _proposal(capacity_session, writer, committed)
    grants = _grant_store()
    await grants.register_executor(
        capacity_session,
        writer,
        _registration(pool_id=proposal.pool_id),
        actor="executor-installer",
        idempotency_key=_REGISTRATION_KEY,
    )
    unhealthy = pool_observation(sequence=2, pool_id=proposal.pool_id).model_copy(
        update={"health": "unhealthy"}
    )
    await management.ingest_pool_observation(
        capacity_session,
        unhealthy,
        actor=f"{proposal.pool_id}-reporter",
    )
    with pytest.raises(GrantConflictError, match="pool observation is not launch-eligible"):
        await grants.propose_reservation(
            capacity_session,
            writer,
            proposal,
            idempotency_key=_PROPOSAL_KEY,
        )

    await management.ingest_pool_observation(
        capacity_session,
        pool_observation(sequence=3, pool_id=proposal.pool_id),
        actor=f"{proposal.pool_id}-reporter",
    )
    proposed = await grants.propose_reservation(
        capacity_session,
        writer,
        proposal,
        idempotency_key=_PROPOSAL_KEY,
    )
    await management.ingest_pool_observation(
        capacity_session,
        unhealthy.model_copy(update={"sequence": 4}),
        actor=f"{proposal.pool_id}-reporter",
    )
    with pytest.raises(GrantConflictError, match="pool observation is not launch-eligible"):
        await grants.accept_reservation(
            capacity_session,
            DryRunReservationAcceptanceV1(
                tranche_id=proposal.tranche_id,
                proposal_digest=proposed.proposal_digest,
                executor_id=proposal.executor_id,
                executor_incarnation=proposal.executor_incarnation,
                command_sequence=1,
            ),
        )
    tranche = (await capacity_session.execute(select(CapacityReservationTranche))).scalar_one()
    intent_count = (
        await capacity_session.execute(select(func.count()).select_from(CapacitySubmissionIntent))
    ).scalar_one()
    assert tranche.state == "proposed"
    assert intent_count == 0


async def test_unhealthy_pool_observation_blocks_permit_issue_and_consumption(
    capacity_session: AsyncSession,
) -> None:
    grants, writer, proposal = await _accepted(capacity_session)
    await grants.register_bootstrap(
        capacity_session,
        DryRunBootstrapRegistrationV1(
            tranche_id=proposal.tranche_id,
            intent_id=proposal.shapes[0].intent_id,
            executor_id=proposal.executor_id,
            executor_incarnation=proposal.executor_incarnation,
            command_sequence=2,
            bootstrap_registration_epoch=1,
            bootstrap_evidence_sha256="c" * 64,
        ),
    )
    management = CapacityManagementStore()
    unhealthy = pool_observation(sequence=2, pool_id=proposal.pool_id).model_copy(
        update={"health": "unhealthy"}
    )
    await management.ingest_pool_observation(
        capacity_session,
        unhealthy,
        actor=f"{proposal.pool_id}-reporter",
    )
    permit = DryRunLaunchPermitV1(
        permit_id=UUID(int=150),
        intent_id=proposal.shapes[0].intent_id,
        allocation_epoch=proposal.allocation_epoch,
        configuration_epoch=proposal.configuration_epoch,
        executor_id=proposal.executor_id,
        executor_incarnation=proposal.executor_incarnation,
        permit_epoch=1,
        launch_rank=1,
    )
    with pytest.raises(GrantConflictError, match="pool observation is not launch-eligible"):
        await grants.issue_launch_permit(
            capacity_session,
            writer,
            permit,
            idempotency_key=UUID(int=151),
        )

    await management.ingest_pool_observation(
        capacity_session,
        pool_observation(sequence=3, pool_id=proposal.pool_id),
        actor=f"{proposal.pool_id}-reporter",
    )
    issued = await grants.issue_launch_permit(
        capacity_session,
        writer,
        permit,
        idempotency_key=UUID(int=151),
    )
    await management.ingest_pool_observation(
        capacity_session,
        unhealthy.model_copy(update={"sequence": 4}),
        actor=f"{proposal.pool_id}-reporter",
    )
    with pytest.raises(GrantConflictError, match="pool observation is not launch-eligible"):
        await grants.consume_launch_permit(
            capacity_session,
            DryRunPermitConsumptionV1(
                permit_id=permit.permit_id,
                permit_digest=issued.permit_digest,
                intent_id=permit.intent_id,
                executor_id=permit.executor_id,
                executor_incarnation=permit.executor_incarnation,
                command_sequence=3,
            ),
        )
    intent = (await capacity_session.execute(select(CapacitySubmissionIntent))).scalar_one()
    permit_row = (await capacity_session.execute(select(CapacityLaunchPermit))).scalar_one()
    assert (intent.state, permit_row.state) == ("launch-ready", "current")


async def _accepted(
    session: AsyncSession,
) -> tuple[CapacityGrantStore, WriterFence, DryRunReservationProposalV1]:
    _, writer, committed = await _committed_shadow(session)
    proposal = await _proposal(session, writer, committed)
    grants = _grant_store()
    await grants.register_executor(
        session,
        writer,
        _registration(pool_id=proposal.pool_id),
        actor="executor-installer",
        idempotency_key=_REGISTRATION_KEY,
    )
    proposed = await grants.propose_reservation(
        session,
        writer,
        proposal,
        idempotency_key=_PROPOSAL_KEY,
    )
    await grants.accept_reservation(
        session,
        DryRunReservationAcceptanceV1(
            tranche_id=proposal.tranche_id,
            proposal_digest=proposed.proposal_digest,
            executor_id=proposal.executor_id,
            executor_incarnation=proposal.executor_incarnation,
            command_sequence=1,
        ),
    )
    return grants, writer, proposal


async def test_reservation_proposal_rejects_a_changed_profile_identity(
    capacity_session: AsyncSession,
) -> None:
    _, writer, committed = await _committed_shadow(capacity_session)
    proposal = await _proposal(capacity_session, writer, committed)
    grants = _grant_store()
    await grants.register_executor(
        capacity_session,
        writer,
        _registration(pool_id=proposal.pool_id),
        actor="executor-installer",
        idempotency_key=_REGISTRATION_KEY,
    )
    changed = proposal.model_copy(
        update={
            "shapes": (proposal.shapes[0].model_copy(update={"profile_id": "another-profile"}),)
        }
    )

    with pytest.raises(GrantConflictError, match="resource binding changed"):
        await grants.propose_reservation(
            capacity_session,
            writer,
            changed,
            idempotency_key=_PROPOSAL_KEY,
        )

    changed_nodes = proposal.model_copy(
        update={
            "shapes": (
                proposal.shapes[0].model_copy(update={"node_ids": ("invented-node",)}),
            )
        }
    )
    with pytest.raises(GrantConflictError, match="topology binding changed"):
        await grants.propose_reservation(
            capacity_session,
            writer,
            changed_nodes,
            idempotency_key=_PROPOSAL_KEY,
        )


async def test_reservation_acceptance_atomically_creates_prepared_intent_and_replays(
    capacity_session: AsyncSession,
) -> None:
    _, writer, committed = await _committed_shadow(capacity_session)
    proposal = await _proposal(capacity_session, writer, committed)
    grants = _grant_store()
    await grants.register_executor(
        capacity_session,
        writer,
        _registration(pool_id=proposal.pool_id),
        actor="executor-installer",
        idempotency_key=_REGISTRATION_KEY,
    )
    proposed = await grants.propose_reservation(
        capacity_session,
        writer,
        proposal,
        idempotency_key=_PROPOSAL_KEY,
    )
    acceptance = DryRunReservationAcceptanceV1(
        tranche_id=proposal.tranche_id,
        proposal_digest=proposed.proposal_digest,
        executor_id=proposal.executor_id,
        executor_incarnation=proposal.executor_incarnation,
        command_sequence=1,
    )

    first = await grants.accept_reservation(capacity_session, acceptance)
    replay = await grants.accept_reservation(capacity_session, acceptance)
    tranche = (await capacity_session.execute(select(CapacityReservationTranche))).scalar_one()
    shape = (await capacity_session.execute(select(CapacityReservationShape))).scalar_one()
    intent = (await capacity_session.execute(select(CapacitySubmissionIntent))).scalar_one()
    assert first.intent_ids == replay.intent_ids == (_INTENT_ID,)
    assert replay.replayed is True
    assert (tranche.state, shape.state, intent.state) == (
        "accepted",
        "accepted",
        "prepared",
    )
    assert tranche.executable is False
    assert intent.executable is False


async def test_open_reservation_is_charged_and_retained_in_next_allocation(
    capacity_session: AsyncSession,
) -> None:
    management, writer, committed = await _committed_shadow(capacity_session)
    proposal = await _proposal(capacity_session, writer, committed)
    grants = _grant_store()
    await grants.register_executor(
        capacity_session,
        writer,
        _registration(pool_id=proposal.pool_id),
        actor="executor-installer",
        idempotency_key=_REGISTRATION_KEY,
    )
    await grants.propose_reservation(
        capacity_session,
        writer,
        proposal,
        idempotency_key=_PROPOSAL_KEY,
    )

    allocation_input = await management.load_allocation_input(capacity_session, writer)
    reserves = [item for item in allocation_input.observed_commitments if item.kind == "reserve"]
    next_shadow = allocate_shadow(allocation_input)
    allocation = next(
        item
        for item in next_shadow.allocations
        if item.subject_id == proposal.subject_id and item.pool_id == proposal.pool_id
    )
    assert len(reserves) == 1
    assert reserves[0].reservation_identity == str(proposal.shapes[0].intent_id)
    assert reserves[0].state == "proposed"
    assert allocation.desired_slots == proposal.shapes[0].concurrency_slots
    assert allocation.desired_shapes[0].count == 1
    assert [
        item
        for item in next_shadow.hypothetical_launch_rank
        if item.subject_id == proposal.subject_id and item.pool_id == proposal.pool_id
    ] == []


async def test_expired_proposal_is_not_retained_and_can_be_replaced(
    capacity_session: AsyncSession,
) -> None:
    management, writer, committed = await _committed_shadow(capacity_session)
    proposal = await _proposal(capacity_session, writer, committed)
    grants = _grant_store()
    await grants.register_executor(
        capacity_session,
        writer,
        _registration(pool_id=proposal.pool_id),
        actor="executor-installer",
        idempotency_key=_REGISTRATION_KEY,
    )
    await grants.propose_reservation(
        capacity_session,
        writer,
        proposal,
        idempotency_key=_PROPOSAL_KEY,
    )
    await capacity_session.execute(
        update(CapacityReservationTranche)
        .where(CapacityReservationTranche.id == proposal.tranche_id)
        .values(expires_at=datetime.now(UTC) - timedelta(seconds=1))
    )

    allocation_input = await management.load_allocation_input(capacity_session, writer)
    assert not [item for item in allocation_input.observed_commitments if item.kind == "reserve"]
    replacement_shadow = allocate_shadow(allocation_input)
    replacement_committed = await management.commit_shadow_epoch(
        capacity_session,
        writer,
        replacement_shadow,
    )
    replacement = await _proposal(
        capacity_session,
        writer,
        (replacement_committed, replacement_shadow),
        tranche_id=UUID(int=120),
        intent_id=UUID(int=121),
    )
    await grants.propose_reservation(
        capacity_session,
        writer,
        replacement,
        idempotency_key=UUID(int=122),
    )

    tranches = (
        (
            await capacity_session.execute(
                select(CapacityReservationTranche).order_by(CapacityReservationTranche.created_at)
            )
        )
        .scalars()
        .all()
    )
    assert [(row.state, row.closure_reason) for row in tranches] == [
        ("closed", "proposal-expired"),
        ("proposed", None),
    ]


async def test_command_equivocation_is_persistently_fenced(
    capacity_session: AsyncSession,
) -> None:
    _, writer, committed = await _committed_shadow(capacity_session)
    proposal = await _proposal(capacity_session, writer, committed)
    grants = _grant_store()
    await grants.register_executor(
        capacity_session,
        writer,
        _registration(pool_id=proposal.pool_id),
        actor="executor-installer",
        idempotency_key=_REGISTRATION_KEY,
    )
    proposed = await grants.propose_reservation(
        capacity_session,
        writer,
        proposal,
        idempotency_key=_PROPOSAL_KEY,
    )
    acceptance = DryRunReservationAcceptanceV1(
        tranche_id=proposal.tranche_id,
        proposal_digest=proposed.proposal_digest,
        executor_id=proposal.executor_id,
        executor_incarnation=proposal.executor_incarnation,
        command_sequence=1,
    )
    await grants.accept_reservation(capacity_session, acceptance)

    with pytest.raises(ExecutorEquivocationError):
        await grants.accept_reservation(
            capacity_session,
            acceptance.model_copy(update={"proposal_digest": "f" * 64}),
        )
    state = (await capacity_session.execute(select(CapacityExecutor.state))).scalar_one()
    assert state == "equivocal"


async def test_proposal_validation_and_expiry_fail_without_prepared_intents(
    capacity_session: AsyncSession,
) -> None:
    _, writer, committed = await _committed_shadow(capacity_session)
    proposal = await _proposal(capacity_session, writer, committed)
    grants = _grant_store(proposal_ttl_seconds=30)
    await grants.register_executor(
        capacity_session,
        writer,
        _registration(pool_id=proposal.pool_id),
        actor="executor-installer",
        idempotency_key=_REGISTRATION_KEY,
    )
    wrong_shape = proposal.shapes[0].model_copy(
        update={"resources": ResourceVectorV1(slots=1, cpu_millicores=2_000)}
    )
    with pytest.raises(GrantConflictError, match="resource"):
        await grants.propose_reservation(
            capacity_session,
            writer,
            proposal.model_copy(update={"shapes": (wrong_shape,)}),
            idempotency_key=_PROPOSAL_KEY,
        )

    proposed = await grants.propose_reservation(
        capacity_session,
        writer,
        proposal,
        idempotency_key=_PROPOSAL_KEY,
    )
    await capacity_session.execute(
        update(CapacityReservationTranche)
        .where(CapacityReservationTranche.id == proposal.tranche_id)
        .values(expires_at=datetime.now(UTC) - timedelta(seconds=1))
    )
    acceptance = DryRunReservationAcceptanceV1(
        tranche_id=proposal.tranche_id,
        proposal_digest=proposed.proposal_digest,
        executor_id=proposal.executor_id,
        executor_incarnation=proposal.executor_incarnation,
        command_sequence=1,
    )
    with pytest.raises(ProposalExpiredError):
        await grants.accept_reservation(capacity_session, acceptance)
    assert (
        await capacity_session.execute(select(func.count()).select_from(CapacitySubmissionIntent))
    ).scalar_one() == 0


async def test_superseded_proposal_closes_durably_and_same_shape_can_be_reproposed(
    capacity_session: AsyncSession,
) -> None:
    management, writer, committed = await _committed_shadow(capacity_session)
    proposal = await _proposal(capacity_session, writer, committed)
    grants = _grant_store()
    await grants.register_executor(
        capacity_session,
        writer,
        _registration(pool_id=proposal.pool_id),
        actor="executor-installer",
        idempotency_key=_REGISTRATION_KEY,
    )
    proposed = await grants.propose_reservation(
        capacity_session,
        writer,
        proposal,
        idempotency_key=_PROPOSAL_KEY,
    )

    allocation_input = await management.load_allocation_input(capacity_session, writer)
    second_shadow = allocate_shadow(allocation_input)
    await management.commit_shadow_epoch(
        capacity_session,
        writer,
        second_shadow,
    )
    acceptance = DryRunReservationAcceptanceV1(
        tranche_id=proposal.tranche_id,
        proposal_digest=proposed.proposal_digest,
        executor_id=proposal.executor_id,
        executor_incarnation=proposal.executor_incarnation,
        command_sequence=1,
    )
    with pytest.raises(ProposalSupersededError):
        await grants.accept_reservation(capacity_session, acceptance)
    with pytest.raises(ProposalSupersededError):
        await grants.accept_reservation(capacity_session, acceptance)

    closed = (
        await capacity_session.execute(
            select(CapacityReservationTranche).where(
                CapacityReservationTranche.id == proposal.tranche_id
            )
        )
    ).scalar_one()
    executor = (await capacity_session.execute(select(CapacityExecutor))).scalar_one()
    assert (closed.state, closed.closure_reason) == (
        "closed",
        "proposal-superseded",
    )
    assert executor.command_high_water == 1

    replacement_input = await management.load_allocation_input(capacity_session, writer)
    replacement_shadow = allocate_shadow(replacement_input)
    replacement_committed = await management.commit_shadow_epoch(
        capacity_session,
        writer,
        replacement_shadow,
    )
    replacement = await _proposal(
        capacity_session,
        writer,
        (replacement_committed, replacement_shadow),
        tranche_id=UUID(int=109),
        intent_id=UUID(int=110),
    )
    assert replacement.shapes[0].shape_instance_id == proposal.shapes[0].shape_instance_id
    replacement_result = await grants.propose_reservation(
        capacity_session,
        writer,
        replacement,
        idempotency_key=UUID(int=111),
    )
    assert replacement_result.tranche_id == replacement.tranche_id


async def test_accepted_unused_intent_can_close_after_newer_allocation(
    capacity_session: AsyncSession,
) -> None:
    management, writer, committed = await _committed_shadow(capacity_session)
    proposal = await _proposal(capacity_session, writer, committed)
    grants = _grant_store()
    await grants.register_executor(
        capacity_session,
        writer,
        _registration(pool_id=proposal.pool_id),
        actor="executor-installer",
        idempotency_key=_REGISTRATION_KEY,
    )
    proposed = await grants.propose_reservation(
        capacity_session,
        writer,
        proposal,
        idempotency_key=_PROPOSAL_KEY,
    )
    await grants.accept_reservation(
        capacity_session,
        DryRunReservationAcceptanceV1(
            tranche_id=proposal.tranche_id,
            proposal_digest=proposed.proposal_digest,
            executor_id=proposal.executor_id,
            executor_incarnation=proposal.executor_incarnation,
            command_sequence=1,
        ),
    )
    allocation_input = await management.load_allocation_input(capacity_session, writer)
    await management.commit_shadow_epoch(
        capacity_session,
        writer,
        allocate_shadow(allocation_input),
    )

    closing = await grants.begin_intent_close(
        capacity_session,
        DryRunIntentCloseV1(
            tranche_id=proposal.tranche_id,
            intent_id=proposal.shapes[0].intent_id,
            executor_id=proposal.executor_id,
            executor_incarnation=proposal.executor_incarnation,
            command_sequence=2,
        ),
    )
    assert closing.intent_id == proposal.shapes[0].intent_id
    assert closing.replayed is False


async def test_bootstrap_permit_and_rate_consumption_remain_dry_run_and_idempotent(
    capacity_session: AsyncSession,
) -> None:
    grants, writer, proposal = await _accepted(capacity_session)
    bootstrap = DryRunBootstrapRegistrationV1(
        tranche_id=proposal.tranche_id,
        intent_id=proposal.shapes[0].intent_id,
        executor_id=proposal.executor_id,
        executor_incarnation=proposal.executor_incarnation,
        command_sequence=2,
        bootstrap_registration_epoch=1,
        bootstrap_evidence_sha256="c" * 64,
    )
    first_ready = await grants.register_bootstrap(capacity_session, bootstrap)
    replay_ready = await grants.register_bootstrap(capacity_session, bootstrap)
    assert first_ready.replayed is False
    assert replay_ready.replayed is True
    assert replay_ready.executable is False

    permit = DryRunLaunchPermitV1(
        permit_id=UUID(int=108),
        intent_id=proposal.shapes[0].intent_id,
        allocation_epoch=proposal.allocation_epoch,
        configuration_epoch=proposal.configuration_epoch,
        executor_id=proposal.executor_id,
        executor_incarnation=proposal.executor_incarnation,
        permit_epoch=1,
        launch_rank=1,
    )
    issued = await grants.issue_launch_permit(
        capacity_session,
        writer,
        permit,
        idempotency_key=UUID(int=109),
    )
    replay_issued = await grants.issue_launch_permit(
        capacity_session,
        writer,
        permit,
        idempotency_key=UUID(int=109),
    )
    assert issued.permit_digest == replay_issued.permit_digest
    assert replay_issued.replayed is True
    assert replay_issued.executable is False

    consumption = DryRunPermitConsumptionV1(
        permit_id=permit.permit_id,
        permit_digest=issued.permit_digest,
        intent_id=permit.intent_id,
        executor_id=permit.executor_id,
        executor_incarnation=permit.executor_incarnation,
        command_sequence=3,
    )
    consumed = await grants.consume_launch_permit(capacity_session, consumption)
    buckets_after_first = {
        (row.scope, row.scope_identity): row.available_microtokens
        for row in (await capacity_session.execute(select(CapacityLaunchRateBucket)))
        .scalars()
        .all()
    }
    replay_consumed = await grants.consume_launch_permit(
        capacity_session,
        consumption,
    )
    buckets_after_replay = {
        (row.scope, row.scope_identity): row.available_microtokens
        for row in (await capacity_session.execute(select(CapacityLaunchRateBucket)))
        .scalars()
        .all()
    }
    intent = (await capacity_session.execute(select(CapacitySubmissionIntent))).scalar_one()
    permit_row = (await capacity_session.execute(select(CapacityLaunchPermit))).scalar_one()
    assert consumed.replayed is False
    assert replay_consumed.replayed is True
    assert consumed.executable is replay_consumed.executable is False
    assert buckets_after_replay == buckets_after_first
    assert len(buckets_after_first) == 4
    assert min(buckets_after_first.values()) >= 7_000_000
    assert (intent.state, permit_row.state) == ("submitting-unknown", "consumed")


async def test_newer_launch_permit_supersedes_old_permit_exactly(
    capacity_session: AsyncSession,
) -> None:
    grants, writer, proposal = await _accepted(capacity_session)
    await grants.register_bootstrap(
        capacity_session,
        DryRunBootstrapRegistrationV1(
            tranche_id=proposal.tranche_id,
            intent_id=proposal.shapes[0].intent_id,
            executor_id=proposal.executor_id,
            executor_incarnation=proposal.executor_incarnation,
            command_sequence=2,
            bootstrap_registration_epoch=1,
            bootstrap_evidence_sha256="c" * 64,
        ),
    )
    first = DryRunLaunchPermitV1(
        permit_id=UUID(int=510),
        intent_id=proposal.shapes[0].intent_id,
        allocation_epoch=proposal.allocation_epoch,
        configuration_epoch=proposal.configuration_epoch,
        executor_id=proposal.executor_id,
        executor_incarnation=proposal.executor_incarnation,
        permit_epoch=1,
        launch_rank=1,
    )
    first_result = await grants.issue_launch_permit(
        capacity_session,
        writer,
        first,
        idempotency_key=UUID(int=511),
    )
    replacement = first.model_copy(
        update={
            "permit_id": UUID(int=512),
            "permit_epoch": 2,
        }
    )
    replacement_result = await grants.issue_launch_permit(
        capacity_session,
        writer,
        replacement,
        idempotency_key=UUID(int=513),
    )

    with pytest.raises(GrantConflictError, match="not current"):
        await grants.consume_launch_permit(
            capacity_session,
            DryRunPermitConsumptionV1(
                permit_id=first.permit_id,
                permit_digest=first_result.permit_digest,
                intent_id=first.intent_id,
                executor_id=first.executor_id,
                executor_incarnation=first.executor_incarnation,
                command_sequence=3,
            ),
        )

    consumed = await grants.consume_launch_permit(
        capacity_session,
        DryRunPermitConsumptionV1(
            permit_id=replacement.permit_id,
            permit_digest=replacement_result.permit_digest,
            intent_id=replacement.intent_id,
            executor_id=replacement.executor_id,
            executor_incarnation=replacement.executor_incarnation,
            command_sequence=3,
        ),
    )
    permits = {
        row.id: row.state
        for row in (await capacity_session.execute(select(CapacityLaunchPermit))).scalars().all()
    }
    assert consumed.intent_id == proposal.shapes[0].intent_id
    assert permits == {
        first.permit_id: "superseded",
        replacement.permit_id: "consumed",
    }


async def test_surge_permit_consumption_fails_closed_without_protected_drain_ack(
    capacity_session: AsyncSession,
) -> None:
    grants, writer, proposal = await _accepted(capacity_session)
    await capacity_session.execute(
        update(CapacityReservationShape)
        .where(CapacityReservationShape.intent_id == proposal.shapes[0].intent_id)
        .values(rollout_surge_slots=1, old_shape_backing_id="old-worker-exact")
    )
    await grants.register_bootstrap(
        capacity_session,
        DryRunBootstrapRegistrationV1(
            tranche_id=proposal.tranche_id,
            intent_id=proposal.shapes[0].intent_id,
            executor_id=proposal.executor_id,
            executor_incarnation=proposal.executor_incarnation,
            command_sequence=2,
            bootstrap_registration_epoch=1,
            bootstrap_evidence_sha256="c" * 64,
        ),
    )
    permit = DryRunLaunchPermitV1(
        permit_id=UUID(int=298),
        intent_id=proposal.shapes[0].intent_id,
        allocation_epoch=proposal.allocation_epoch,
        configuration_epoch=proposal.configuration_epoch,
        executor_id=proposal.executor_id,
        executor_incarnation=proposal.executor_incarnation,
        permit_epoch=1,
        launch_rank=1,
    )
    issued = await grants.issue_launch_permit(
        capacity_session,
        writer,
        permit,
        idempotency_key=UUID(int=299),
    )

    with pytest.raises(
        GrantConflictError,
        match="protected old-worker drain acknowledgement is unavailable",
    ):
        await grants.consume_launch_permit(
            capacity_session,
            DryRunPermitConsumptionV1(
                permit_id=permit.permit_id,
                permit_digest=issued.permit_digest,
                intent_id=permit.intent_id,
                executor_id=permit.executor_id,
                executor_incarnation=permit.executor_incarnation,
                command_sequence=3,
            ),
        )

    intent = (await capacity_session.execute(select(CapacitySubmissionIntent))).scalar_one()
    permit_row = (await capacity_session.execute(select(CapacityLaunchPermit))).scalar_one()
    buckets = (await capacity_session.execute(select(CapacityLaunchRateBucket))).scalars().all()
    assert (intent.state, permit_row.state) == ("launch-ready", "current")
    assert all(row.available_microtokens == row.capacity_microtokens for row in buckets)


async def test_zero_subject_rate_blocks_consumption_without_spending_other_scopes(
    capacity_session: AsyncSession,
) -> None:
    grants, writer, proposal = await _accepted(capacity_session)
    await grants.register_bootstrap(
        capacity_session,
        DryRunBootstrapRegistrationV1(
            tranche_id=proposal.tranche_id,
            intent_id=proposal.shapes[0].intent_id,
            executor_id=proposal.executor_id,
            executor_incarnation=proposal.executor_incarnation,
            command_sequence=2,
            bootstrap_registration_epoch=1,
            bootstrap_evidence_sha256="c" * 64,
        ),
    )
    await capacity_session.execute(
        update(CapacitySubject)
        .where(CapacitySubject.subject_id == proposal.subject_id)
        .values(submission_rate_per_minute=0)
    )
    permit = DryRunLaunchPermitV1(
        permit_id=UUID(int=110),
        intent_id=proposal.shapes[0].intent_id,
        allocation_epoch=proposal.allocation_epoch,
        configuration_epoch=proposal.configuration_epoch,
        executor_id=proposal.executor_id,
        executor_incarnation=proposal.executor_incarnation,
        permit_epoch=1,
        launch_rank=1,
    )
    issued = await grants.issue_launch_permit(
        capacity_session,
        writer,
        permit,
        idempotency_key=UUID(int=111),
    )
    before = {
        row.id: row.available_microtokens
        for row in (await capacity_session.execute(select(CapacityLaunchRateBucket)))
        .scalars()
        .all()
    }
    with pytest.raises(RateLimitError, match="subject"):
        await grants.consume_launch_permit(
            capacity_session,
            DryRunPermitConsumptionV1(
                permit_id=permit.permit_id,
                permit_digest=canonical_grant_digest(permit),
                intent_id=permit.intent_id,
                executor_id=permit.executor_id,
                executor_incarnation=permit.executor_incarnation,
                command_sequence=3,
            ),
        )
    after = {
        row.id: row.available_microtokens
        for row in (await capacity_session.execute(select(CapacityLaunchRateBucket)))
        .scalars()
        .all()
    }
    assert issued.permit_digest == canonical_grant_digest(permit)
    assert after == before


async def test_final_launch_cas_revalidates_pending_limits_without_spending_tokens(
    capacity_session: AsyncSession,
) -> None:
    grants, writer, proposal = await _accepted(capacity_session)
    await grants.register_bootstrap(
        capacity_session,
        DryRunBootstrapRegistrationV1(
            tranche_id=proposal.tranche_id,
            intent_id=proposal.shapes[0].intent_id,
            executor_id=proposal.executor_id,
            executor_incarnation=proposal.executor_incarnation,
            command_sequence=2,
            bootstrap_registration_epoch=1,
            bootstrap_evidence_sha256="c" * 64,
        ),
    )
    permit = DryRunLaunchPermitV1(
        permit_id=UUID(int=118),
        intent_id=proposal.shapes[0].intent_id,
        allocation_epoch=proposal.allocation_epoch,
        configuration_epoch=proposal.configuration_epoch,
        executor_id=proposal.executor_id,
        executor_incarnation=proposal.executor_incarnation,
        permit_epoch=1,
        launch_rank=1,
    )
    issued = await grants.issue_launch_permit(
        capacity_session,
        writer,
        permit,
        idempotency_key=UUID(int=119),
    )
    before = {
        row.id: row.available_microtokens
        for row in (await capacity_session.execute(select(CapacityLaunchRateBucket)))
        .scalars()
        .all()
    }
    await capacity_session.execute(
        update(CapacitySubject)
        .where(CapacitySubject.subject_id == proposal.subject_id)
        .values(max_pending_jobs=0)
    )
    consumption = DryRunPermitConsumptionV1(
        permit_id=permit.permit_id,
        permit_digest=issued.permit_digest,
        intent_id=permit.intent_id,
        executor_id=permit.executor_id,
        executor_incarnation=permit.executor_incarnation,
        command_sequence=3,
    )

    with pytest.raises(GrantConflictError, match="pending limit"):
        await grants.consume_launch_permit(capacity_session, consumption)

    after = {
        row.id: row.available_microtokens
        for row in (await capacity_session.execute(select(CapacityLaunchRateBucket)))
        .scalars()
        .all()
    }
    permit_row = (await capacity_session.execute(select(CapacityLaunchPermit))).scalar_one()
    intent = (await capacity_session.execute(select(CapacitySubmissionIntent))).scalar_one()
    assert before == after
    assert permit_row.state == "current"
    assert intent.state == "launch-ready"


async def test_quarantined_pending_inventory_consumes_global_pending_limit(
    capacity_session: AsyncSession,
) -> None:
    grants, writer, proposal, _private_key = await _accepted_with_ownership_key(capacity_session)
    await grants.ingest_executor_inventory(
        capacity_session,
        DryRunExecutorInventoryV1(
            authority_incarnation=writer.authority_incarnation,
            writer_epoch=writer.writer_epoch,
            executor_id=proposal.executor_id,
            executor_incarnation=proposal.executor_incarnation,
            pool_id=proposal.pool_id,
            pool_generation=proposal.pool_generation,
            inventory_sequence=1,
            journal_sequence=1,
            journal_digest="d" * 64,
            records=(
                ExecutorInventoryRecordV1(
                    physical_identity="job-unverified-pending",
                    physical_kind="slurm-job",
                    authority_scope="dedicated-loom-association",
                    state="pending",
                    resources=proposal.shapes[0].resources,
                    node_ids=proposal.shapes[0].node_ids,
                    controller_evidence_sha256="e" * 64,
                ),
            ),
        ),
    )
    await grants.register_bootstrap(
        capacity_session,
        DryRunBootstrapRegistrationV1(
            tranche_id=proposal.tranche_id,
            intent_id=proposal.shapes[0].intent_id,
            executor_id=proposal.executor_id,
            executor_incarnation=proposal.executor_incarnation,
            command_sequence=2,
            bootstrap_registration_epoch=1,
            bootstrap_evidence_sha256="b" * 64,
        ),
    )
    permit = DryRunLaunchPermitV1(
        permit_id=UUID(int=176),
        intent_id=proposal.shapes[0].intent_id,
        allocation_epoch=proposal.allocation_epoch,
        configuration_epoch=proposal.configuration_epoch,
        executor_id=proposal.executor_id,
        executor_incarnation=proposal.executor_incarnation,
        permit_epoch=1,
        launch_rank=1,
    )
    issued = await grants.issue_launch_permit(
        capacity_session,
        writer,
        permit,
        idempotency_key=UUID(int=177),
    )
    await capacity_session.execute(
        update(CapacityAuthorityState).values(global_pending_job_ceiling=1)
    )
    await capacity_session.execute(
        update(CapacityPool)
        .where(
            CapacityPool.configuration_epoch == proposal.configuration_epoch,
            CapacityPool.pool_id == proposal.pool_id,
        )
        .values(max_pending_jobs=1)
    )

    with pytest.raises(GrantConflictError, match="global pending limit"):
        await grants.consume_launch_permit(
            capacity_session,
            DryRunPermitConsumptionV1(
                permit_id=permit.permit_id,
                permit_digest=issued.permit_digest,
                intent_id=permit.intent_id,
                executor_id=permit.executor_id,
                executor_incarnation=permit.executor_incarnation,
                command_sequence=3,
            ),
        )
    intent = (await capacity_session.execute(select(CapacitySubmissionIntent))).scalar_one()
    assert intent.state == "launch-ready"


async def test_global_launch_order_cannot_be_reversed_by_pool_polling_order(
    capacity_session: AsyncSession,
) -> None:
    _, writer, committed = await _committed_shadow(
        capacity_session,
        pending_attempt_ids=("attempt-a", "attempt-b"),
    )
    _, shadow = committed
    assert len(shadow.hypothetical_launch_rank) == 2
    grants = _grant_store()
    pool_ids = tuple(sorted({item.pool_id for item in shadow.hypothetical_launch_rank}))
    incarnation_by_pool = {pool_id: UUID(int=200 + index) for index, pool_id in enumerate(pool_ids)}
    for index, pool_id in enumerate(pool_ids):
        await grants.register_executor(
            capacity_session,
            writer,
            _registration(
                pool_id=pool_id,
                incarnation=incarnation_by_pool[pool_id],
            ),
            actor="executor-installer",
            idempotency_key=UUID(int=220 + index),
        )

    sequence_by_pool = dict.fromkeys(pool_ids, 0)
    proposals: list[DryRunReservationProposalV1] = []
    permit_digests: list[str] = []
    for index, ranked in enumerate(shadow.hypothetical_launch_rank):
        proposal = await _proposal(
            capacity_session,
            writer,
            committed,
            rank_index=index,
            tranche_id=UUID(int=230 + index),
            intent_id=UUID(int=240 + index),
            executor_incarnation=incarnation_by_pool[ranked.pool_id],
        )
        proposals.append(proposal)
        proposed = await grants.propose_reservation(
            capacity_session,
            writer,
            proposal,
            idempotency_key=UUID(int=250 + index),
        )
        sequence_by_pool[ranked.pool_id] += 1
        await grants.accept_reservation(
            capacity_session,
            DryRunReservationAcceptanceV1(
                tranche_id=proposal.tranche_id,
                proposal_digest=proposed.proposal_digest,
                executor_id=proposal.executor_id,
                executor_incarnation=proposal.executor_incarnation,
                command_sequence=sequence_by_pool[ranked.pool_id],
            ),
        )

    for index, (ranked, proposal) in enumerate(
        zip(shadow.hypothetical_launch_rank, proposals, strict=True)
    ):
        sequence_by_pool[ranked.pool_id] += 1
        await grants.register_bootstrap(
            capacity_session,
            DryRunBootstrapRegistrationV1(
                tranche_id=proposal.tranche_id,
                intent_id=proposal.shapes[0].intent_id,
                executor_id=proposal.executor_id,
                executor_incarnation=proposal.executor_incarnation,
                command_sequence=sequence_by_pool[ranked.pool_id],
                bootstrap_registration_epoch=1,
                bootstrap_evidence_sha256=f"{index + 1:064x}",
            ),
        )
        permit = DryRunLaunchPermitV1(
            permit_id=UUID(int=260 + index),
            intent_id=proposal.shapes[0].intent_id,
            allocation_epoch=proposal.allocation_epoch,
            configuration_epoch=proposal.configuration_epoch,
            executor_id=proposal.executor_id,
            executor_incarnation=proposal.executor_incarnation,
            permit_epoch=1,
            launch_rank=ranked.rank,
        )
        issued = await grants.issue_launch_permit(
            capacity_session,
            writer,
            permit,
            idempotency_key=UUID(int=270 + index),
        )
        permit_digests.append(issued.permit_digest)

    later = proposals[1]
    later_pool = shadow.hypothetical_launch_rank[1].pool_id
    with pytest.raises(LaunchOrderError):
        await grants.consume_launch_permit(
            capacity_session,
            DryRunPermitConsumptionV1(
                permit_id=UUID(int=261),
                permit_digest=permit_digests[1],
                intent_id=later.shapes[0].intent_id,
                executor_id=later.executor_id,
                executor_incarnation=later.executor_incarnation,
                command_sequence=sequence_by_pool[later_pool] + 1,
            ),
        )

    for index, (ranked, proposal) in enumerate(
        zip(shadow.hypothetical_launch_rank, proposals, strict=True)
    ):
        sequence_by_pool[ranked.pool_id] += 1
        await grants.consume_launch_permit(
            capacity_session,
            DryRunPermitConsumptionV1(
                permit_id=UUID(int=260 + index),
                permit_digest=permit_digests[index],
                intent_id=proposal.shapes[0].intent_id,
                executor_id=proposal.executor_id,
                executor_incarnation=proposal.executor_incarnation,
                command_sequence=sequence_by_pool[ranked.pool_id],
            ),
        )

    global_bucket = (
        await capacity_session.execute(
            select(CapacityLaunchRateBucket).where(CapacityLaunchRateBucket.scope == "global")
        )
    ).scalar_one()
    assert 30_000_000 <= global_bucket.available_microtokens < 31_000_000


async def test_global_launch_order_ignores_ready_intent_without_current_permit(
    capacity_session: AsyncSession,
) -> None:
    _, writer, committed = await _committed_shadow(
        capacity_session,
        pending_attempt_ids=("attempt-unpermitted", "attempt-permitted"),
    )
    _, shadow = committed
    assert len(shadow.hypothetical_launch_rank) == 2
    grants = _grant_store()
    pool_ids = tuple(sorted({item.pool_id for item in shadow.hypothetical_launch_rank}))
    incarnation_by_pool = {pool_id: UUID(int=450 + index) for index, pool_id in enumerate(pool_ids)}
    for index, pool_id in enumerate(pool_ids):
        await grants.register_executor(
            capacity_session,
            writer,
            _registration(
                pool_id=pool_id,
                incarnation=incarnation_by_pool[pool_id],
            ),
            actor="executor-installer",
            idempotency_key=UUID(int=460 + index),
        )

    sequence_by_pool = dict.fromkeys(pool_ids, 0)
    proposals: list[DryRunReservationProposalV1] = []
    for index, ranked in enumerate(shadow.hypothetical_launch_rank):
        proposal = await _proposal(
            capacity_session,
            writer,
            committed,
            rank_index=index,
            tranche_id=UUID(int=470 + index),
            intent_id=UUID(int=480 + index),
            executor_incarnation=incarnation_by_pool[ranked.pool_id],
        )
        proposals.append(proposal)
        proposed = await grants.propose_reservation(
            capacity_session,
            writer,
            proposal,
            idempotency_key=UUID(int=490 + index),
        )
        sequence_by_pool[ranked.pool_id] += 1
        await grants.accept_reservation(
            capacity_session,
            DryRunReservationAcceptanceV1(
                tranche_id=proposal.tranche_id,
                proposal_digest=proposed.proposal_digest,
                executor_id=proposal.executor_id,
                executor_incarnation=proposal.executor_incarnation,
                command_sequence=sequence_by_pool[ranked.pool_id],
            ),
        )

    for index, (ranked, proposal) in enumerate(
        zip(shadow.hypothetical_launch_rank, proposals, strict=True)
    ):
        sequence_by_pool[ranked.pool_id] += 1
        await grants.register_bootstrap(
            capacity_session,
            DryRunBootstrapRegistrationV1(
                tranche_id=proposal.tranche_id,
                intent_id=proposal.shapes[0].intent_id,
                executor_id=proposal.executor_id,
                executor_incarnation=proposal.executor_incarnation,
                command_sequence=sequence_by_pool[ranked.pool_id],
                bootstrap_registration_epoch=1,
                bootstrap_evidence_sha256=f"{index + 1:064x}",
            ),
        )

    later_rank = shadow.hypothetical_launch_rank[1]
    later = proposals[1]
    permit = DryRunLaunchPermitV1(
        permit_id=UUID(int=500),
        intent_id=later.shapes[0].intent_id,
        allocation_epoch=later.allocation_epoch,
        configuration_epoch=later.configuration_epoch,
        executor_id=later.executor_id,
        executor_incarnation=later.executor_incarnation,
        permit_epoch=1,
        launch_rank=later_rank.rank,
    )
    issued = await grants.issue_launch_permit(
        capacity_session,
        writer,
        permit,
        idempotency_key=UUID(int=501),
    )
    later_pool = later_rank.pool_id
    sequence_by_pool[later_pool] += 1
    consumed = await grants.consume_launch_permit(
        capacity_session,
        DryRunPermitConsumptionV1(
            permit_id=permit.permit_id,
            permit_digest=issued.permit_digest,
            intent_id=permit.intent_id,
            executor_id=permit.executor_id,
            executor_incarnation=permit.executor_incarnation,
            command_sequence=sequence_by_pool[later_pool],
        ),
    )

    intents = {
        row.id: row.state
        for row in (
            (await capacity_session.execute(select(CapacitySubmissionIntent))).scalars().all()
        )
    }
    assert consumed.intent_id == later.shapes[0].intent_id
    assert intents[proposals[0].shapes[0].intent_id] == "launch-ready"
    assert intents[later.shapes[0].intent_id] == "submitting-unknown"


async def test_unused_close_requires_two_steps_and_release_evidence_is_append_only(
    capacity_session: AsyncSession,
) -> None:
    grants, writer, proposal = await _accepted(capacity_session)
    close = DryRunIntentCloseV1(
        tranche_id=proposal.tranche_id,
        intent_id=proposal.shapes[0].intent_id,
        executor_id=proposal.executor_id,
        executor_incarnation=proposal.executor_incarnation,
        command_sequence=2,
    )
    first_close = await grants.begin_intent_close(capacity_session, close)
    replay_close = await grants.begin_intent_close(capacity_session, close)
    assert first_close.replayed is False
    assert replay_close.replayed is True

    inventory = DryRunExecutorInventoryV1(
        authority_incarnation=writer.authority_incarnation,
        writer_epoch=writer.writer_epoch,
        executor_id=proposal.executor_id,
        executor_incarnation=proposal.executor_incarnation,
        pool_id=proposal.pool_id,
        pool_generation=proposal.pool_generation,
        inventory_sequence=1,
        journal_sequence=2,
        journal_digest="c" * 64,
    )
    inventory_result = await grants.ingest_executor_inventory(
        capacity_session,
        inventory,
    )

    release = DryRunPartialReleaseV1(
        tranche_id=proposal.tranche_id,
        executor_id=proposal.executor_id,
        executor_incarnation=proposal.executor_incarnation,
        command_sequence=3,
        releases=(
            ReleasedShapeV1(
                shape_instance_id=proposal.shapes[0].shape_instance_id,
                intent_id=proposal.shapes[0].intent_id,
                inventory_sequence=1,
                terminal_kind="unused",
                terminal_identity=proposal.shapes[0].shape_instance_id,
                terminal_evidence_sha256=inventory_result.inventory_digest,
                protected_registration_epoch=1,
                bootstrap_revoked=True,
                protected_release_sha256="e" * 64,
            ),
        ),
    )
    acknowledgement = _protected_release_acknowledgement(
        proposal,
        bootstrap_registration_epoch=0,
        protected_registration_epoch=1,
        protected_release_sha256="e" * 64,
    )
    await grants.acknowledge_protected_release(
        capacity_session,
        acknowledgement,
        actor="development-agent",
        idempotency_key=UUID(int=270),
    )
    first_release = await grants.release_shapes(capacity_session, release)
    replay_release = await grants.release_shapes(capacity_session, release)
    shape = (await capacity_session.execute(select(CapacityReservationShape))).scalar_one()
    intent = (await capacity_session.execute(select(CapacitySubmissionIntent))).scalar_one()
    evidence = (
        await capacity_session.execute(select(CapacityReservationReleaseEvidence))
    ).scalar_one()
    protected_ack = (
        await capacity_session.execute(select(CapacityProtectedReleaseAcknowledgement))
    ).scalar_one()
    assert first_release.released_shape_ids == replay_release.released_shape_ids
    assert replay_release.replayed is True
    assert first_release.executable is replay_release.executable is False
    assert (shape.state, intent.state) == ("released", "closed")
    tranche = (await capacity_session.execute(select(CapacityReservationTranche))).scalar_one()
    assert (tranche.state, tranche.closure_reason) == ("closed", "fully-released")
    assert evidence.protected_registration_epoch == 1
    assert evidence.bootstrap_revoked is True
    assert protected_ack.acknowledgement_digest == canonical_grant_digest(acknowledgement)

    await capacity_session.execute(
        update(CapacityExecutor).values(
            command_high_water=4,
            last_command_digest="f" * 64,
        )
    )
    delayed_replay = await grants.release_shapes(capacity_session, release)
    assert delayed_replay.replayed is True
    changed_release = release.model_copy(
        update={
            "releases": (
                release.releases[0].model_copy(update={"protected_release_sha256": "a" * 64}),
            )
        }
    )
    with pytest.raises(StaleCommandError, match="exact durable evidence"):
        await grants.release_shapes(capacity_session, changed_release)

    with pytest.raises(DBAPIError, match="append-only"):
        async with capacity_session.begin_nested():
            await capacity_session.execute(
                update(CapacityReservationReleaseEvidence)
                .where(CapacityReservationReleaseEvidence.id == evidence.id)
                .values(protected_release_sha256="f" * 64)
            )
    with pytest.raises(DBAPIError, match="append-only"):
        async with capacity_session.begin_nested():
            await capacity_session.execute(
                update(CapacityProtectedReleaseAcknowledgement)
                .where(CapacityProtectedReleaseAcknowledgement.id == protected_ack.id)
                .values(protected_release_sha256="f" * 64)
            )
    for table in (
        "capacity_reservation_release_evidence",
        "capacity_protected_release_acknowledgements",
    ):
        with pytest.raises(DBAPIError, match="append-only"):
            async with capacity_session.begin_nested():
                await capacity_session.execute(text(f"TRUNCATE {table}"))


async def test_multi_shape_partial_release_keeps_unnamed_shape_accepted_and_charged(
    capacity_session: AsyncSession,
) -> None:
    _management, writer, committed = await _committed_shadow(
        capacity_session,
        pending_attempt_ids=tuple(f"attempt-partial-{index}" for index in range(6)),
    )
    shadow = committed[1]
    ranks_by_pool: dict[str, list[int]] = {}
    for index, ranked in enumerate(shadow.hypothetical_launch_rank):
        ranks_by_pool.setdefault(ranked.pool_id, []).append(index)
    first_index, second_index = next(
        tuple(indices[:2]) for indices in ranks_by_pool.values() if len(indices) >= 2
    )
    proposal = await _proposal(
        capacity_session,
        writer,
        committed,
        rank_index=first_index,
        intent_id=UUID(int=301),
    )
    second = await _proposal(
        capacity_session,
        writer,
        committed,
        rank_index=second_index,
        tranche_id=proposal.tranche_id,
        intent_id=UUID(int=302),
    )
    proposal = DryRunReservationProposalV1.model_validate(
        {
            **proposal.model_dump(mode="python"),
            "shapes": proposal.shapes + second.shapes,
        }
    )
    grants = _grant_store()
    await grants.register_executor(
        capacity_session,
        writer,
        _registration(pool_id=proposal.pool_id),
        actor="executor-installer",
        idempotency_key=_REGISTRATION_KEY,
    )
    proposed = await grants.propose_reservation(
        capacity_session,
        writer,
        proposal,
        idempotency_key=_PROPOSAL_KEY,
    )
    accepted = await grants.accept_reservation(
        capacity_session,
        DryRunReservationAcceptanceV1(
            tranche_id=proposal.tranche_id,
            proposal_digest=proposed.proposal_digest,
            executor_id=proposal.executor_id,
            executor_incarnation=proposal.executor_incarnation,
            command_sequence=1,
        ),
    )
    assert accepted.intent_ids == tuple(shape.intent_id for shape in proposal.shapes)

    first_shape, second_shape = proposal.shapes
    await grants.begin_intent_close(
        capacity_session,
        DryRunIntentCloseV1(
            tranche_id=proposal.tranche_id,
            intent_id=first_shape.intent_id,
            executor_id=proposal.executor_id,
            executor_incarnation=proposal.executor_incarnation,
            command_sequence=2,
        ),
    )
    first_inventory = await grants.ingest_executor_inventory(
        capacity_session,
        DryRunExecutorInventoryV1(
            authority_incarnation=writer.authority_incarnation,
            writer_epoch=writer.writer_epoch,
            executor_id=proposal.executor_id,
            executor_incarnation=proposal.executor_incarnation,
            pool_id=proposal.pool_id,
            pool_generation=proposal.pool_generation,
            inventory_sequence=1,
            journal_sequence=2,
            journal_digest="c" * 64,
        ),
    )
    await grants.acknowledge_protected_release(
        capacity_session,
        _protected_release_acknowledgement(
            proposal,
            bootstrap_registration_epoch=0,
            protected_registration_epoch=1,
            protected_release_sha256="d" * 64,
            shape_index=0,
        ),
        actor="development-agent",
        idempotency_key=UUID(int=303),
    )
    first_release = DryRunPartialReleaseV1(
        tranche_id=proposal.tranche_id,
        executor_id=proposal.executor_id,
        executor_incarnation=proposal.executor_incarnation,
        command_sequence=3,
        releases=(
            ReleasedShapeV1(
                shape_instance_id=first_shape.shape_instance_id,
                intent_id=first_shape.intent_id,
                inventory_sequence=1,
                terminal_kind="unused",
                terminal_identity=first_shape.shape_instance_id,
                terminal_evidence_sha256=first_inventory.inventory_digest,
                protected_registration_epoch=1,
                bootstrap_revoked=True,
                protected_release_sha256="d" * 64,
            ),
        ),
    )
    released = await grants.release_shapes(capacity_session, first_release)
    assert released.released_shape_ids == (first_shape.shape_instance_id,)

    tranche = (
        await capacity_session.execute(
            select(CapacityReservationTranche).where(
                CapacityReservationTranche.id == proposal.tranche_id
            )
        )
    ).scalar_one()
    shapes = (
        (
            await capacity_session.execute(
                select(CapacityReservationShape).order_by(
                    CapacityReservationShape.shape_instance_id
                )
            )
        )
        .scalars()
        .all()
    )
    intents = {
        row.id: row
        for row in (
            (await capacity_session.execute(select(CapacitySubmissionIntent))).scalars().all()
        )
    }
    assert tranche.state == "accepted"
    assert {shape.shape_instance_id: shape.state for shape in shapes} == {
        first_shape.shape_instance_id: "released",
        second_shape.shape_instance_id: "accepted",
    }
    assert intents[first_shape.intent_id].state == "closed"
    assert intents[second_shape.intent_id].state == "prepared"
