"""Read-only predecessor release evidence for personal recreation."""

import json
from copy import deepcopy
from datetime import UTC, datetime, timedelta
from uuid import UUID

import pytest
from sqlalchemy import select, text, update
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncSession

from loom_capacity_manager.contracts import ObservedCommitmentV1, SubjectConfigurationV1
from loom_capacity_manager.executable_contracts import (
    ExecutableIntentCloseV2,
    ExecutablePartialReleaseV2,
    ExecutablePermitConsumptionV2,
    ExecutableProtectedReleaseV2,
    ExecutionDrainV2,
)
from loom_capacity_manager.execution_store import CapacityExecutionStore
from loom_capacity_manager.grant_contracts import (
    DryRunBootstrapRegistrationV1,
    DryRunExecutorInventoryV1,
    DryRunIntentCloseV1,
    DryRunLaunchPermitV1,
    DryRunPartialReleaseV1,
    DryRunPermitConsumptionV1,
    DryRunReservationAcceptanceV1,
    ExecutorInventoryRecordV1,
    ReleasedShapeV1,
)
from loom_capacity_manager.grant_store import ProposalExpiredError
from loom_capacity_manager.membership_release import predecessor_release_sha256 as release_digest
from loom_capacity_manager.models import (
    CapacityExecutableIntent,
    CapacityExecutableProtectedReleaseReceipt,
    CapacityExecutableTerminalInventoryEvidence,
    CapacityExecutorObservation,
    CapacityObservedCommitment,
    CapacityProtectedReleaseAcknowledgement,
    CapacityReservationReleaseEvidence,
    CapacityReservationTranche,
    CapacitySubject,
)
from loom_capacity_manager.ownership import OwnershipKeyring, sign_ownership
from loom_capacity_manager.store import CapacityManagementStore, ConfigurationConflictError
from loom_capacity_manager.store import _canonical_json_digest
from tests.capacity_execution_fixtures import EXECUTOR_KEYS, executor_binding
from tests.capacity_fixtures import demand_snapshot
from tests.integration.test_capacity_grant_store import (
    _PROPOSAL_KEY,
    _REGISTRATION_KEY,
    _accepted,
    _accepted_with_ownership_key,
    _committed_shadow,
    _grant_store,
    _ownership_metadata,
    _protected_release_acknowledgement,
    _registration,
)
from tests.integration.test_capacity_grant_store import (
    _proposal as _legacy_proposal,
)
from tests.integration.test_capacity_manager_execution_store import (
    _active_plan,
    _heartbeat,
    _inventory_execution,
    _inventory_record,
    _launch_ready,
    _next_inventory,
    _test_only_update_without_guard,
)
from tests.integration.test_capacity_membership import (
    DELEGATE,
    CapacityMembershipStore,
    _active_v3,
    _request,
)


async def assert_sql_release_matches(
    session: AsyncSession, predecessor: SubjectConfigurationV1
) -> None:
    expected = await release_digest(session, predecessor)
    actual = await session.scalar(
        text("SELECT public.capacity_personal_predecessor_release_digest(:subject, :incarnation)"),
        {"subject": predecessor.subject_id, "incarnation": predecessor.subject_incarnation},
    )
    assert actual == expected


async def assert_sql_release_rejected(
    session: AsyncSession, predecessor: SubjectConfigurationV1
) -> None:
    with pytest.raises(DBAPIError) as failure:
        async with session.begin_nested():
            await session.scalar(
                text("SELECT public.capacity_personal_predecessor_release_digest(:subject, :incarnation)"),
                {"subject": predecessor.subject_id, "incarnation": predecessor.subject_incarnation},
            )
    assert failure.value.orig.sqlstate == "23514"


@pytest.mark.parametrize("microsecond", (0, 1, 100000, 999999))
async def test_sql_release_timestamp_and_key_order_match_wire_encoding(
    capacity_session: AsyncSession, microsecond: int
) -> None:
    at = datetime(2026, 9, 10, 12, 0, 0, microsecond, tzinfo=UTC)
    await capacity_session.execute(text("SET LOCAL TIME ZONE 'America/Toronto'"))
    assert await capacity_session.scalar(
        text("SELECT public.capacity_personal_release_timestamp(:at)"), {"at": at}
    ) == at.isoformat()
    payload = {"released_at": at.isoformat(), "release_digest": "a" * 64}
    assert await capacity_session.scalar(
        text("SELECT public.capacity_personal_release_json_digest(CAST(:payload AS jsonb))"),
        {"payload": json.dumps(payload)},
    ) == _canonical_json_digest(payload)


async def test_empty_release_set_is_stable_and_identity_bound(
    capacity_session: AsyncSession,
) -> None:
    fixture, active = await _active_v3(capacity_session)
    result = await CapacityMembershipStore(fixture.store).apply(
        capacity_session, _request(active), actor=DELEGATE, idempotency_key=UUID(int=21000)
    )
    subject = result.member.configuration
    first = await release_digest(capacity_session, subject)
    assert first != "0" * 64
    assert first == await release_digest(capacity_session, subject)
    changed = subject.model_copy(update={"subject_incarnation": UUID(int=21001)})
    assert first != await release_digest(capacity_session, changed)
    await assert_sql_release_matches(capacity_session, subject)


@pytest.mark.parametrize("pending", ("new", "dirty", "deleted"))
async def test_release_proof_preserves_and_rejects_unflushed_ledger_edits(
    capacity_session: AsyncSession, pending: str
) -> None:
    active, _ = await _active_plan(capacity_session)
    store = CapacityExecutionStore()
    await _heartbeat(store, capacity_session, active, pool_id="gb10")
    await store.next_pool_work(capacity_session, executor_binding("gb10"))
    intent = (await capacity_session.scalars(select(CapacityExecutableIntent))).first()
    assert intent is not None
    subject = (
        await capacity_session.scalars(
            select(CapacitySubject).where(CapacitySubject.subject_id == intent.subject_id)
        )
    ).one()
    configuration = SubjectConfigurationV1.model_validate_json(json.dumps(subject.payload))
    if pending == "new":
        # Deliberately incomplete: a proof must not flush callers' pending work.
        capacity_session.add(CapacityObservedCommitment(commitment_identity="pending-proof"))
    elif pending == "dirty":
        intent.state = "released"
    else:
        await capacity_session.delete(intent)
    before = (set(capacity_session.new), set(capacity_session.dirty), set(capacity_session.deleted))
    with capacity_session.no_autoflush:
        with pytest.raises(ConfigurationConflictError, match="witness has unflushed changes"):
            await release_digest(capacity_session, configuration)
    assert before == (set(capacity_session.new), set(capacity_session.dirty), set(capacity_session.deleted))
    assert intent.state == ("released" if pending == "dirty" else "proposed")


@pytest.mark.parametrize("same_predecessor", (False, True))
async def test_quarantined_payload_attribution_blocks_only_its_exact_predecessor(
    capacity_session: AsyncSession, same_predecessor: bool
) -> None:
    fixture, active = await _active_v3(capacity_session)
    result = await CapacityMembershipStore(fixture.store).apply(
        capacity_session, _request(active), actor=DELEGATE, idempotency_key=UUID(int=21050)
    )
    predecessor = result.member.configuration
    profile = predecessor.profiles[0]
    shape = profile.worker_shapes[0]
    observed = ObservedCommitmentV1(
        kind="physical",
        commitment_id="quarantined-predecessor",
        physical_identity="quarantined-predecessor",
        subject_id=predecessor.subject_id,
        subject_incarnation=(
            predecessor.subject_incarnation if same_predecessor else UUID(int=21051)
        ),
        deployment_generation=predecessor.deployment_generation,
        pool_id=profile.pool_id,
        pool_generation=profile.pool_generation,
        profile_id=shape.shape_id,
        profile_generation=profile.profile_generation,
        profile_digest=profile.profile_digest,
        shape_id=shape.shape_id,
        resources=shape.total_resources,
        state="quarantined",
    )
    now = datetime.now(UTC)
    # Retained claimed identity must still block release when reconciliation has
    # quarantined the physical row and removed authenticated scalar attribution.
    capacity_session.add(
        CapacityObservedCommitment(
            kind="physical",
            commitment_identity=observed.commitment_id,
            source_incarnation=UUID(int=21052),
            subject_id=None,
            subject_incarnation=None,
            pool_id=profile.pool_id,
            pool_generation=profile.pool_generation,
            binding_payload={
                "observed_contract": observed.model_dump(mode="json", exclude_none=False)
            },
            resource_vector=observed.resources.model_dump(mode="json", exclude_none=False),
            state="quarantined",
            first_reporter_high_water=1,
            last_reporter_high_water=1,
            first_receipt_time=now,
            last_receipt_time=now,
        )
    )
    await capacity_session.flush()
    if same_predecessor:
        with pytest.raises(ConfigurationConflictError, match="unreleased observed commitments"):
            await release_digest(capacity_session, predecessor)
        await assert_sql_release_rejected(capacity_session, predecessor)
    else:
        assert await release_digest(capacity_session, predecessor) != "0" * 64


@pytest.mark.parametrize(
    "state",
    (
        "proposed",
        "accepted",
        "submitting-unknown",
        "observed",
        "quarantined",
        "terminal",
    ),
)
async def test_every_unreleased_predecessor_intent_blocks_proof(
    capacity_session: AsyncSession, state: str
) -> None:
    active, _ = await _active_plan(capacity_session)
    store = CapacityExecutionStore()
    await _heartbeat(store, capacity_session, active, pool_id="gb10")
    await store.next_pool_work(capacity_session, executor_binding("gb10"))
    row = (await capacity_session.execute(select(CapacityExecutableIntent))).scalars().first()
    assert row is not None
    if state != "proposed":
        # Fixture-only fault injection: prove the release predicate rejects every
        # charged state without pretending to exercise each scheduler transition.
        await _test_only_update_without_guard(
            capacity_session,
            table_name="capacity_executable_intents",
            trigger_name="capacity_executable_intent_mutation_guard",
            statement=update(CapacityExecutableIntent)
            .where(CapacityExecutableIntent.intent_id == row.intent_id)
            .values(state=state),
        )
        await capacity_session.refresh(row)
    subject = (
        await capacity_session.execute(
            select(CapacitySubject).where(
                CapacitySubject.subject_id == row.subject_id,
            )
        )
    ).scalar_one()
    with pytest.raises(ConfigurationConflictError, match=r"predecessor.*unreleased"):
        await release_digest(
            capacity_session,
            SubjectConfigurationV1.model_validate_json(json.dumps(subject.payload)),
        )
    await assert_sql_release_rejected(
        capacity_session, SubjectConfigurationV1.model_validate_json(json.dumps(subject.payload))
    )


async def test_never_accepted_proposal_uses_existing_release_path(
    capacity_session: AsyncSession,
) -> None:
    active, _ = await _active_plan(capacity_session)
    store = CapacityExecutionStore()
    await _heartbeat(store, capacity_session, active, pool_id="gb10")
    await store.next_pool_work(capacity_session, executor_binding("gb10"))
    await CapacityManagementStore().begin_execution_drain(
        capacity_session,
        ExecutionDrainV2(
            authority_incarnation=active.authority_incarnation,
            expected_writer_epoch=active.writer_epoch,
            execution_epoch=active.execution_epoch,
            execution_manifest_sha256=active.execution_manifest_sha256,
            expected_executable_new_capacity_ceiling=active.executable_new_capacity_ceiling,
            expected_executable_new_capacity_rate_per_minute=active.executable_new_capacity_rate_per_minute,
        ),
        actor="drain-operator",
        idempotency_key=UUID(int=21010),
    )
    assert await store.next_pool_work(capacity_session, executor_binding("gb10")) is None
    row = (await capacity_session.execute(select(CapacityExecutableIntent))).scalars().first()
    assert row is not None and row.state == "released" and row.accepted_at is None
    subject = (
        await capacity_session.execute(
            select(CapacitySubject).where(CapacitySubject.subject_id == row.subject_id)
        )
    ).scalar_one()
    assert (
        await release_digest(
            capacity_session,
            SubjectConfigurationV1.model_validate_json(json.dumps(subject.payload)),
        )
        != "0" * 64
    )

    # Keep the released ORM instance alive while the actual ledger changes.
    # A retained session must not authorize recreation from its identity map.
    await assert_sql_release_matches(
        capacity_session, SubjectConfigurationV1.model_validate_json(json.dumps(subject.payload))
    )
    await _test_only_update_without_guard(
        capacity_session,
        table_name="capacity_executable_intents",
        trigger_name="capacity_executable_intent_mutation_guard",
        statement=update(CapacityExecutableIntent)
        .where(CapacityExecutableIntent.intent_id == row.intent_id)
        .values(state="quarantined")
        .execution_options(synchronize_session=False),
    )
    assert row.state == "released"
    with pytest.raises(ConfigurationConflictError, match="unreleased executable intents"):
        await release_digest(
            capacity_session,
            SubjectConfigurationV1.model_validate_json(json.dumps(subject.payload)),
        )


@pytest.mark.parametrize(
    ("physical", "tamper"),
    (
        (False, None),
        (True, None),
        (True, "protected"),
        (True, "terminal"),
        (True, "resealed-resource"),
        (True, "resealed-execution"),
    ),
)
async def test_accepted_release_requires_exact_durable_witnesses(
    capacity_session: AsyncSession, physical: bool, tamper: str | None
) -> None:
    store = CapacityExecutionStore(
        ownership_keyring=OwnershipKeyring({"gb10-key": EXECUTOR_KEYS["gb10"].public_key()})
    )
    permit = await _launch_ready(store, capacity_session)
    binding = permit.binding
    if physical:
        await store.consume_launch_permit(
            capacity_session,
            ExecutablePermitConsumptionV2(
                permit_id=permit.permit_id,
                permit_digest=store.contract_digest(permit),
                binding=binding,
                command_sequence=3,
            ),
        )
        inventory = await _next_inventory(
            capacity_session,
            _inventory_execution(binding),
            binding,
            records=(
                _inventory_record(
                    binding,
                    physical_identity="job-123",
                    state="terminal",
                    terminal_evidence_sha256="a" * 64,
                ),
            ),
        )
        await store.ingest_executor_inventory(capacity_session, inventory)
    await store.begin_intent_close(
        capacity_session,
        ExecutableIntentCloseV2(
            binding=binding,
            command_sequence=4 if physical else 3,
        ),
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
        idempotency_key=UUID(int=21020),
    )
    release = await store.next_pool_work(capacity_session, executor_binding("gb10"))
    assert isinstance(release, ExecutablePartialReleaseV2)
    await store.release_shapes(capacity_session, release)
    subject = (
        await capacity_session.execute(
            select(CapacitySubject).where(
                CapacitySubject.subject_id == binding.subject_id,
            )
        )
    ).scalar_one()
    configuration = SubjectConfigurationV1.model_validate_json(json.dumps(subject.payload))
    if tamper is not None and tamper.startswith("resealed-"):
        terminal = (await capacity_session.scalars(select(CapacityExecutableTerminalInventoryEvidence))).one()
        payload = deepcopy(terminal.evidence_payload)
        if tamper == "resealed-resource":
            payload["record"]["resources"]["memory_bytes"] += 1
        else:
            payload["inventory_execution"]["writer_epoch"] += 1
        await _test_only_update_without_guard(
            capacity_session,
            table_name="capacity_executable_terminal_inventory_evidence",
            trigger_name="capacity_executable_terminal_inventory_append_only_guard",
            statement=update(CapacityExecutableTerminalInventoryEvidence)
            .where(CapacityExecutableTerminalInventoryEvidence.id == terminal.id)
            .values(evidence_payload=payload, evidence_digest=_canonical_json_digest(payload))
            .execution_options(synchronize_session=False),
        )
        with pytest.raises(ConfigurationConflictError, match="terminal release witness is invalid"):
            await release_digest(capacity_session, configuration)
        await assert_sql_release_rejected(capacity_session, configuration)
    elif tamper is None:
        original_digest = await release_digest(capacity_session, configuration)
        await assert_sql_release_matches(capacity_session, configuration)
        await capacity_session.execute(text("SET LOCAL TIME ZONE 'America/Toronto'"))
        assert await release_digest(capacity_session, configuration) == original_digest
        await assert_sql_release_matches(capacity_session, configuration)
        assert original_digest != "0" * 64
        await store.acknowledge_protected_release(
            capacity_session,
            ExecutableProtectedReleaseV2(
                binding=binding,
                reporter_incarnation=demand_snapshot().reporter_incarnation,
                bootstrap_registration_epoch=1,
                protected_registration_epoch=3,
                bootstrap_revoked=True,
                protected_release_sha256="c" * 64,
            ),
            actor="development",
            idempotency_key=UUID(int=21021),
        )
        assert await release_digest(capacity_session, configuration) == original_digest
        await assert_sql_release_matches(capacity_session, configuration)
    else:
        if tamper == "protected":
            row = (
                await capacity_session.execute(select(CapacityExecutableProtectedReleaseReceipt))
            ).scalar_one()
            row.acknowledgement_digest = "0" * 64
        else:
            row = (
                await capacity_session.execute(select(CapacityExecutableTerminalInventoryEvidence))
            ).scalar_one()
            row.evidence_digest = "0" * 64
        with capacity_session.no_autoflush:
            with pytest.raises(ConfigurationConflictError, match=r"predecessor.*witness"):
                await release_digest(capacity_session, configuration)


async def test_closed_unaccepted_legacy_proposal_needs_no_worker_receipt(
    capacity_session: AsyncSession,
) -> None:
    _, writer, committed = await _committed_shadow(capacity_session)
    proposal = await _legacy_proposal(capacity_session, writer, committed)
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
    subject = (await capacity_session.execute(select(CapacitySubject))).scalar_one()
    configuration = SubjectConfigurationV1.model_validate_json(json.dumps(subject.payload))
    with pytest.raises(ConfigurationConflictError, match="legacy"):
        await release_digest(capacity_session, configuration)
    await capacity_session.execute(
        update(CapacityReservationTranche).values(
            expires_at=datetime.now(UTC) - timedelta(seconds=1),
        )
    )
    with pytest.raises(ProposalExpiredError):
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
    assert await release_digest(capacity_session, configuration) != "0" * 64
    await assert_sql_release_matches(capacity_session, configuration)


@pytest.mark.parametrize("tamper", (None, "release", "protected", "inventory"))
async def test_released_legacy_reservation_requires_its_durable_evidence(
    capacity_session: AsyncSession,
    tamper: str | None,
) -> None:
    grants, writer, proposal = await _accepted(capacity_session)
    subject = (await capacity_session.execute(select(CapacitySubject))).scalar_one()
    configuration = SubjectConfigurationV1.model_validate_json(json.dumps(subject.payload))
    with pytest.raises(ConfigurationConflictError, match="legacy"):
        await release_digest(capacity_session, configuration)
    await grants.begin_intent_close(
        capacity_session,
        DryRunIntentCloseV1(
            tranche_id=proposal.tranche_id,
            intent_id=proposal.shapes[0].intent_id,
            executor_id=proposal.executor_id,
            executor_incarnation=proposal.executor_incarnation,
            command_sequence=2,
        ),
    )
    inventory = await grants.ingest_executor_inventory(
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
            protected_release_sha256="e" * 64,
        ),
        actor="development-agent",
        idempotency_key=UUID(int=21030),
    )
    await grants.release_shapes(
        capacity_session,
        DryRunPartialReleaseV1(
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
                    terminal_evidence_sha256=inventory.inventory_digest,
                    protected_registration_epoch=1,
                    bootstrap_revoked=True,
                    protected_release_sha256="e" * 64,
                ),
            ),
        ),
    )
    if tamper:
        if tamper == "release":
            witness = (
                await capacity_session.scalars(select(CapacityReservationReleaseEvidence))
            ).one()
            witness.evidence_digest = "0" * 64
        elif tamper == "protected":
            protected = (
                await capacity_session.scalars(select(CapacityProtectedReleaseAcknowledgement))
            ).one()
            protected.acknowledgement_digest = "0" * 64
        else:
            inventory_row = (
                await capacity_session.scalars(select(CapacityExecutorObservation))
            ).one()
            inventory_row.inventory_digest = "0" * 64
        with capacity_session.no_autoflush:
            with pytest.raises(ConfigurationConflictError, match="legacy"):
                await release_digest(capacity_session, configuration)
    else:
        assert await release_digest(capacity_session, configuration) != "0" * 64
        await assert_sql_release_matches(capacity_session, configuration)


@pytest.mark.parametrize("tamper", (None, "classification", "ownership", "duplicate-classification"))
async def test_legacy_physical_release_retains_authenticated_inventory(
    capacity_session: AsyncSession, tamper: str | None
) -> None:
    grants, writer, proposal, private_key = await _accepted_with_ownership_key(capacity_session)
    shape = proposal.shapes[0]
    await grants.register_bootstrap(
        capacity_session,
        DryRunBootstrapRegistrationV1(
            tranche_id=proposal.tranche_id,
            intent_id=shape.intent_id,
            executor_id=proposal.executor_id,
            executor_incarnation=proposal.executor_incarnation,
            command_sequence=2,
            bootstrap_registration_epoch=1,
            bootstrap_evidence_sha256="b" * 64,
        ),
    )
    permit = DryRunLaunchPermitV1(
        permit_id=UUID(int=21040),
        intent_id=shape.intent_id,
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
        idempotency_key=UUID(int=21041),
    )
    await grants.consume_launch_permit(
        capacity_session,
        DryRunPermitConsumptionV1(
            permit_id=permit.permit_id,
            permit_digest=issued.permit_digest,
            intent_id=shape.intent_id,
            executor_id=proposal.executor_id,
            executor_incarnation=proposal.executor_incarnation,
            command_sequence=3,
        ),
    )
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
            journal_sequence=3,
            journal_digest="d" * 64,
            records=(
                ExecutorInventoryRecordV1(
                    physical_identity="job-predecessor-terminal",
                    physical_kind="slurm-job",
                    authority_scope="registered-loom",
                    state="terminal",
                    resources=shape.resources,
                    node_ids=shape.node_ids,
                    controller_evidence_sha256="c" * 64,
                    terminal_evidence_sha256="e" * 64,
                    ownership_proof=sign_ownership(
                        private_key,
                        signing_key_id=f"{proposal.pool_id}-key-1",
                        metadata=_ownership_metadata(proposal),
                    ),
                ),
            ),
        ),
    )
    await grants.acknowledge_protected_release(
        capacity_session,
        _protected_release_acknowledgement(
            proposal,
            bootstrap_registration_epoch=1,
            protected_registration_epoch=2,
            protected_release_sha256="f" * 64,
        ),
        actor="development-agent",
        idempotency_key=UUID(int=21042),
    )
    await grants.release_shapes(
        capacity_session,
        DryRunPartialReleaseV1(
            tranche_id=proposal.tranche_id,
            executor_id=proposal.executor_id,
            executor_incarnation=proposal.executor_incarnation,
            command_sequence=4,
            releases=(
                ReleasedShapeV1(
                    shape_instance_id=shape.shape_instance_id,
                    intent_id=shape.intent_id,
                    inventory_sequence=1,
                    terminal_kind="slurm-job",
                    terminal_identity="job-predecessor-terminal",
                    terminal_evidence_sha256="e" * 64,
                    protected_registration_epoch=2,
                    bootstrap_revoked=True,
                    protected_release_sha256="f" * 64,
                ),
            ),
        ),
    )
    subject = (await capacity_session.scalars(select(CapacitySubject))).one()
    configuration = SubjectConfigurationV1.model_validate_json(json.dumps(subject.payload))
    original = await release_digest(capacity_session, configuration)
    if tamper is None:
        assert original == await release_digest(capacity_session, configuration)
        await assert_sql_release_matches(capacity_session, configuration)
        return
    observation = (await capacity_session.scalars(select(CapacityExecutorObservation))).one()
    if tamper == "duplicate-classification":
        await capacity_session.execute(
            update(CapacityExecutorObservation).where(CapacityExecutorObservation.id == observation.id)
            .values(classification_payload=[
                {"physical_identity": "job-predecessor-terminal", "classification": "foreign"},
                {"physical_identity": "job-predecessor-terminal", "classification": "authenticated"},
            ]).execution_options(synchronize_session=False)
        )
        with pytest.raises(ConfigurationConflictError, match="legacy predecessor"):
            await release_digest(capacity_session, configuration)
        await assert_sql_release_rejected(capacity_session, configuration)
        return
    if tamper == "classification":
        observation.classification_payload = [
            {
                "physical_identity": "job-predecessor-terminal",
                "classification": "foreign",
            }
        ]
    else:
        # Corrupt a retained record only in this test session; no SQL guard is relaxed.
        observation.payload = {
            **observation.payload,
            "records": [
                {**observation.payload["records"][0], "ownership_proof": None},
            ],
        }
    with capacity_session.no_autoflush:
        with pytest.raises(ConfigurationConflictError, match="legacy"):
            await release_digest(capacity_session, configuration)
