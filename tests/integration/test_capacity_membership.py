"""Durable personal membership under active execution authority."""

from __future__ import annotations

from dataclasses import replace
from pathlib import Path
from uuid import UUID

import pytest
from alembic import command
from alembic.config import Config as AlembicConfig
from sqlalchemy import delete, select, text, update
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import (
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from loom_capacity_manager.allocator import allocate_shadow
from loom_capacity_manager.contracts import canonical_bytes, canonical_digest
from loom_capacity_manager.executable_contracts import (
    CandidateBindingV2,
    ExecutionDrainV2,
    SubjectExecutionAcknowledgementV2,
)
from loom_capacity_manager.membership_contracts import (
    DelegatedAllocationInputV2,
    ExecutionPreparationPolicyV3,
    PersonalApplicationMembershipMutationV1,
    PersonalMembershipPolicyV1,
)
from loom_capacity_manager.membership_store import (
    CapacityMembershipStore,
    resolve_subject_acknowledgement,
)
from loom_capacity_manager.models import (
    CapacityAccountPolicy,
    CapacityCandidate,
    CapacityExecutionEpoch,
    CapacityPersonalMembershipEvent,
    CapacitySubject,
)
from loom_capacity_manager.reconciler import _commit_reconciled_epoch
from loom_capacity_manager.store import (
    ConfigurationConflictError,
    ExecutionConflictError,
    IdempotencyConflictError,
    StaleAllocationInputError,
    _derive_development_subject,
)
from tests.capacity_execution_fixtures import (
    execution_acknowledgement,
    execution_policy,
    ready_execution_activation,
    register_execution_executors,
    setup_execution,
)
from tests.capacity_fixtures import (
    development_projection,
    fleet_with_development_template,
    subject_configuration,
)

NAMESPACE_ID = UUID("00000000-0000-4000-8000-000000002001")
BOB_OWNER_ID = UUID("00000000-0000-4000-8000-000000002002")
BOB_SUBJECT_ID = UUID("00000000-0000-4000-8000-000000002003")
BOB_INCARNATION = UUID("00000000-0000-4000-8000-000000002004")
BOB_REPORTER = UUID("00000000-0000-4000-8000-000000002005")
DEFAULT_OPERATION_ID = UUID("00000000-0000-4000-8000-000000002006")
DELEGATE = "personal-membership-manager"


def _projection(
    *,
    operation_kind: str = "create",
    operation_epoch: int = 1,
    operation_id: UUID = DEFAULT_OPERATION_ID,
    subject_id: UUID = BOB_SUBJECT_ID,
    subject_incarnation: UUID = BOB_INCARNATION,
    owner_id: UUID = BOB_OWNER_ID,
    environment_name: str = "bob",
    expected_configuration_epoch: int = 1,
    reporter_incarnation: UUID = BOB_REPORTER,
    min_slots: int = 0,
    max_slots: int = 2,
    deployment_generation: int | None = None,
):  # type: ignore[no-untyped-def]
    value = development_projection(
        expected_configuration_epoch=expected_configuration_epoch,
        operation_kind=operation_kind,
        operation_epoch=operation_epoch,
        environment_name=environment_name,
        operation_id=operation_id,
        subject_id=subject_id,
        subject_incarnation=subject_incarnation,
        owner_id=owner_id,
        demand_reporter_incarnation=reporter_incarnation,
        min_slots=min_slots,
        max_slots=max_slots,
        candidate_generation=(
            operation_epoch if deployment_generation is None else deployment_generation
        ),
        deployment_generation=(
            operation_epoch if deployment_generation is None else deployment_generation
        ),
        configuration_generation=operation_epoch,
    )
    if operation_kind != "create":
        token_generation = (
            operation_epoch if deployment_generation is None else deployment_generation
        )
        value = value.model_copy(
            update={"demand_reporter_token_sha256": f"{token_generation % 10}" * 64}
        )
    return value


def _acknowledgement(projection) -> SubjectExecutionAcknowledgementV2:  # type: ignore[no-untyped-def]
    return SubjectExecutionAcknowledgementV2(
        subject_id=projection.subject_id,
        subject_incarnation=projection.subject_incarnation,
        configuration_generation=projection.configuration_generation,
        deployment_generation=projection.deployment_generation,
        candidate=CandidateBindingV2(
            algorithm="source-sha256",
            identity=projection.candidate_sha256,
            publication_sha256=projection.candidate_publication_sha256,
        ),
        reporter_incarnation=projection.demand_reporter_incarnation,
        protected_admission_sha256=projection.protected_admission_sha256,
        legacy_writer_high_water=0,
        acknowledgement_sha256="9" * 64,
    )


async def _active_v3(
    capacity_session: AsyncSession,
    *,
    max_subjects: int = 2,
    owner_min_reservation_slots: int = 4,
):  # type: ignore[no-untyped-def]
    fleet = fleet_with_development_template(owner_min_reservation_slots=owner_min_reservation_slots)
    base = subject_configuration(fleet)
    base_ack = execution_acknowledgement(subject=base)
    membership_policy = PersonalMembershipPolicyV1(
        namespace_id=NAMESPACE_ID,
        management_principal_id=DELEGATE,
        development_template_sha256=canonical_digest(fleet.development_subject_template),
        max_subjects=max_subjects,
        managed_base_subject_ids=(),
    )
    policy = execution_policy(
        subject_acknowledgements=(base_ack,),
        personal_membership=membership_policy,
    )
    assert isinstance(policy, ExecutionPreparationPolicyV3)
    fixture = await setup_execution(
        capacity_session,
        execution_policy=policy,
        fleet=fleet,
        subjects=(base,),
    )
    prepared = await fixture.store.prepare_execution_epoch(
        capacity_session,
        fixture.request,
        actor="preparation-operator",
        idempotency_key=UUID(int=20010),
    )
    await register_execution_executors(capacity_session, fixture, prepared)
    activation = await ready_execution_activation(
        capacity_session,
        fixture.store,
        fixture.request,
        prepared,
    )
    active = await fixture.store.activate_execution_epoch(
        capacity_session,
        activation,
        actor="activation-operator",
        idempotency_key=UUID(int=20011),
    )
    return fixture, active


async def _active_v3_with_managed_base(
    capacity_session: AsyncSession,
    *,
    delegate_base: bool = True,
):  # type: ignore[no-untyped-def]
    fleet = fleet_with_development_template()
    generic = subject_configuration(fleet)
    base_projection = development_projection(expected_configuration_epoch=1)
    managed = _derive_development_subject(fleet, base_projection)
    policy_contract = PersonalMembershipPolicyV1(
        namespace_id=NAMESPACE_ID,
        management_principal_id=DELEGATE,
        development_template_sha256=canonical_digest(fleet.development_subject_template),
        max_subjects=2,
        managed_base_subject_ids=(managed.subject_id,) if delegate_base else (),
    )
    policy = execution_policy(
        subject_acknowledgements=(
            execution_acknowledgement(subject=generic),
            _acknowledgement(base_projection),
        ),
        personal_membership=policy_contract,
    )
    fixture = await setup_execution(
        capacity_session,
        execution_policy=policy,
        fleet=fleet,
        subjects=(generic,),
    )
    projected = await fixture.store.project_development_subject(
        capacity_session,
        base_projection,
        actor="environment-lifecycle",
        idempotency_key=UUID(int=20030),
    )
    assert projected.subject == managed
    fixture = replace(
        fixture,
        request=fixture.request.model_copy(update={"configuration_epoch": 2}),
    )
    prepared = await fixture.store.prepare_execution_epoch(
        capacity_session,
        fixture.request,
        actor="preparation-operator",
        idempotency_key=UUID(int=20031),
    )
    await register_execution_executors(capacity_session, fixture, prepared)
    activation = await ready_execution_activation(
        capacity_session, fixture.store, fixture.request, prepared
    )
    active = await fixture.store.activate_execution_epoch(
        capacity_session,
        activation,
        actor="activation-operator",
        idempotency_key=UUID(int=20032),
    )
    return fixture, active, managed


def _request(active, projection=None, *, expected_revision: int = 0):  # type: ignore[no-untyped-def]
    resolved = _projection() if projection is None else projection
    return PersonalApplicationMembershipMutationV1(
        execution=active,
        namespace_id=NAMESPACE_ID,
        expected_revision=expected_revision,
        projection=resolved,
        acknowledgement=_acknowledgement(resolved),
    )


async def test_active_membership_admission_preserves_immutable_execution_and_base(
    capacity_session: AsyncSession,
) -> None:
    """Advancing membership must not rewrite the global configuration or execution."""

    fixture, active = await _active_v3(capacity_session)
    before = await fixture.store.execution_authority(capacity_session)
    base_input = await fixture.store.load_allocation_input(capacity_session, fixture.writer)
    base_configuration_bytes = canonical_bytes(base_input.configuration)
    membership = CapacityMembershipStore(fixture.store)
    request = _request(active)

    result = await membership.apply(
        capacity_session,
        request,
        actor=DELEGATE,
        idempotency_key=UUID(int=20012),
    )

    assert result.revision == 1
    assert result.member.owner_id == BOB_OWNER_ID
    assert await fixture.store.execution_authority(capacity_session) == before
    value = await fixture.store.load_allocation_input(capacity_session, fixture.writer)
    assert isinstance(value, DelegatedAllocationInputV2)
    assert canonical_bytes(value.configuration) == base_configuration_bytes
    assert value.membership.members[0].owner_id == BOB_OWNER_ID


async def test_exact_replay_returns_original_checkpoint_and_conflicts_on_any_rebinding(
    capacity_session: AsyncSession,
) -> None:
    """Reusing either operation identity must never target a different mutation."""

    fixture, active = await _active_v3(capacity_session)
    membership = CapacityMembershipStore(fixture.store)
    request = _request(active)
    key = UUID(int=20100)
    first = await membership.apply(capacity_session, request, actor=DELEGATE, idempotency_key=key)
    replay = await membership.apply(capacity_session, request, actor=DELEGATE, idempotency_key=key)
    assert replay.replayed is True
    assert replay.revision == first.revision
    assert replay.head_sha256 == first.head_sha256

    update_projection = _projection(
        operation_kind="update",
        operation_epoch=2,
        operation_id=UUID(int=20102),
        reporter_incarnation=UUID(int=20103),
    )
    await membership.apply(
        capacity_session,
        _request(active, update_projection, expected_revision=1),
        actor=DELEGATE,
        idempotency_key=UUID(int=20104),
    )
    historical_replay = await membership.apply(
        capacity_session, request, actor=DELEGATE, idempotency_key=key
    )
    assert historical_replay.replayed is True
    assert historical_replay.revision == first.revision
    assert historical_replay.head_sha256 == first.head_sha256

    changed = request.model_copy(update={"expected_revision": 1})
    with pytest.raises(IdempotencyConflictError, match="different input"):
        await membership.apply(capacity_session, changed, actor=DELEGATE, idempotency_key=key)


@pytest.mark.parametrize("failure", ("actor", "fence", "revision", "base-takeover"))
async def test_membership_rejects_wrong_authority_and_compare_and_swap(
    capacity_session: AsyncSession,
    failure: str,
) -> None:
    """Actor, execution fence, revision, and immutable base are independent gates."""

    fixture, active = await _active_v3(capacity_session)
    membership = CapacityMembershipStore(fixture.store)
    request = _request(active)
    actor = DELEGATE
    expected_error: type[Exception] = ExecutionConflictError
    if failure == "actor":
        actor = "different-principal"
    elif failure == "fence":
        request = request.model_copy(
            update={"execution": active.model_copy(update={"execution_manifest_sha256": "8" * 64})}
        )
    elif failure == "revision":
        request = request.model_copy(update={"expected_revision": 1})
        expected_error = ConfigurationConflictError
    else:
        base_subject_id = subject_configuration(fleet_with_development_template()).subject_id
        projection = _projection(subject_id=base_subject_id)
        request = _request(active, projection)
        expected_error = ConfigurationConflictError

    with pytest.raises(expected_error):
        await membership.apply(
            capacity_session,
            request,
            actor=actor,
            idempotency_key=UUID(int=20101),
        )
    assert (
        await capacity_session.execute(select(CapacityPersonalMembershipEvent))
    ).scalar_one_or_none() is None


async def test_update_capacity_destroy_retain_exact_generation_history_and_reject_recreation(
    capacity_session: AsyncSession,
) -> None:
    """Every generation remains resolvable while a disabled identity stays closed."""

    fixture, active = await _active_v3(capacity_session)
    membership = CapacityMembershipStore(fixture.store)
    projections = (
        _projection(),
        _projection(
            operation_kind="update",
            operation_epoch=2,
            operation_id=UUID(int=20202),
            reporter_incarnation=UUID(int=20212),
            deployment_generation=2,
        ),
        _projection(
            operation_kind="capacity",
            operation_epoch=3,
            operation_id=UUID(int=20203),
            reporter_incarnation=UUID(int=20212),
            deployment_generation=2,
        ),
        _projection(
            operation_kind="destroy",
            operation_epoch=4,
            operation_id=UUID(int=20204),
            reporter_incarnation=UUID(int=20212),
            min_slots=1,
            max_slots=2,
            deployment_generation=2,
        ),
    )
    results = []
    for expected, projection in enumerate(projections):
        results.append(
            await membership.apply(
                capacity_session,
                _request(active, projection, expected_revision=expected),
                actor=DELEGATE,
                idempotency_key=UUID(int=20220 + expected),
            )
        )
    assert [result.revision for result in results] == [1, 2, 3, 4]
    assert results[-1].member.configuration.lifecycle_state == "disabled"
    assert results[-1].member.configuration.min_slots == 0
    assert results[-1].member.configuration.max_slots == 0

    execution_row = (
        await capacity_session.execute(
            select(CapacityExecutionEpoch).where(CapacityExecutionEpoch.state == "active")
        )
    ).scalar_one()
    snapshot = await membership.snapshot(capacity_session, execution_row)
    assert snapshot.revision == 4
    assert snapshot.members[0] == results[-1].member
    for result in results:
        configuration = result.member.configuration
        assert (
            await resolve_subject_acknowledgement(
                capacity_session,
                execution_row,
                subject_id=configuration.subject_id,
                subject_incarnation=configuration.subject_incarnation,
                configuration_generation=configuration.configuration_generation,
                deployment_generation=configuration.deployment_generation,
                reporter_incarnation=configuration.demand_reporter_incarnation,
            )
            == result.member.acknowledgement
        )

    first_configuration = results[0].member.configuration
    for changed in (
        {"configuration_generation": 99},
        {"deployment_generation": 99},
        {"reporter_incarnation": UUID(int=20299)},
        {"subject_incarnation": UUID(int=20298)},
    ):
        with pytest.raises(
            ConfigurationConflictError,
            match="acknowledgement is unavailable",
        ):
            await resolve_subject_acknowledgement(
                capacity_session,
                execution_row,
                subject_id=first_configuration.subject_id,
                subject_incarnation=changed.get(
                    "subject_incarnation", first_configuration.subject_incarnation
                ),
                configuration_generation=changed.get(
                    "configuration_generation",
                    first_configuration.configuration_generation,
                ),
                deployment_generation=changed.get(
                    "deployment_generation", first_configuration.deployment_generation
                ),
                reporter_incarnation=changed.get(
                    "reporter_incarnation",
                    first_configuration.demand_reporter_incarnation,
                ),
            )

    reactivation = _projection(
        operation_kind="update",
        operation_epoch=5,
        operation_id=UUID(int=20205),
        reporter_incarnation=UUID(int=20215),
    )
    with pytest.raises(ConfigurationConflictError, match="cannot be reactivated"):
        await membership.apply(
            capacity_session,
            _request(active, reactivation, expected_revision=4),
            actor=DELEGATE,
            idempotency_key=UUID(int=20225),
        )
    reincarnated = reactivation.model_copy(update={"subject_incarnation": UUID(int=20216)})
    with pytest.raises(ConfigurationConflictError):
        await membership.apply(
            capacity_session,
            _request(active, reincarnated, expected_revision=4),
            actor=DELEGATE,
            idempotency_key=UUID(int=20226),
        )

    await fixture.store.begin_execution_drain(
        capacity_session,
        ExecutionDrainV2(
            authority_incarnation=active.authority_incarnation,
            expected_writer_epoch=active.writer_epoch,
            execution_epoch=active.execution_epoch,
            execution_manifest_sha256=active.execution_manifest_sha256,
            expected_executable_new_capacity_ceiling=(active.executable_new_capacity_ceiling),
            expected_executable_new_capacity_rate_per_minute=(
                active.executable_new_capacity_rate_per_minute
            ),
        ),
        actor="drain-operator",
        idempotency_key=UUID(int=20229),
    )
    assert (
        await resolve_subject_acknowledgement(
            capacity_session,
            execution_row,
            subject_id=first_configuration.subject_id,
            subject_incarnation=first_configuration.subject_incarnation,
            configuration_generation=first_configuration.configuration_generation,
            deployment_generation=first_configuration.deployment_generation,
            reporter_incarnation=first_configuration.demand_reporter_incarnation,
        )
        == results[0].member.acknowledgement
    )


async def test_loader_rejects_tampered_materialization_and_owner_quota_violation(
    capacity_session: AsyncSession,
) -> None:
    """The log is authoritative and owner reservation limits remain enforced."""

    fixture, active = await _active_v3(capacity_session, owner_min_reservation_slots=1)
    membership = CapacityMembershipStore(fixture.store)
    projection = _projection(min_slots=2, max_slots=2)
    with pytest.raises(ConfigurationConflictError, match="minimum aggregate"):
        await membership.apply(
            capacity_session,
            _request(active, projection),
            actor=DELEGATE,
            idempotency_key=UUID(int=20300),
        )

    result = await membership.apply(
        capacity_session,
        _request(active),
        actor=DELEGATE,
        idempotency_key=UUID(int=20301),
    )
    assert result.revision == 1
    await capacity_session.execute(
        update(CapacitySubject)
        .where(CapacitySubject.subject_id == BOB_SUBJECT_ID)
        .values(max_slots=7)
    )
    with pytest.raises(ConfigurationConflictError, match="materialized subject changed"):
        await fixture.store.load_allocation_input(capacity_session, fixture.writer)


async def test_loader_rejects_tampered_account_materialization(
    capacity_session: AsyncSession,
) -> None:
    """Delegated account scalar columns must remain exact to their payload and policy."""

    fixture, active = await _active_v3(capacity_session)
    await CapacityMembershipStore(fixture.store).apply(
        capacity_session,
        _request(active),
        actor=DELEGATE,
        idempotency_key=UUID(int=20310),
    )
    await capacity_session.execute(
        update(CapacityAccountPolicy)
        .where(CapacityAccountPolicy.owner_id == BOB_OWNER_ID)
        .values(max_slots=99)
    )
    with pytest.raises(ConfigurationConflictError, match="materialized account changed"):
        await fixture.store.load_allocation_input(capacity_session, fixture.writer)


async def test_capacity_change_rejects_tampered_retained_candidate_evidence(
    capacity_session: AsyncSession,
) -> None:
    """A capacity-only generation cannot inherit altered deployment evidence."""

    fixture, active = await _active_v3(capacity_session)
    membership = CapacityMembershipStore(fixture.store)
    await membership.apply(
        capacity_session,
        _request(active),
        actor=DELEGATE,
        idempotency_key=UUID(int=20320),
    )
    await capacity_session.execute(
        update(CapacityCandidate)
        .where(CapacityCandidate.subject_id == BOB_SUBJECT_ID)
        .values(architecture_payload={"supported_architectures": [], "supported_pool_ids": []})
    )
    capacity_projection = _projection(
        operation_kind="capacity",
        operation_epoch=2,
        operation_id=UUID(int=20321),
        deployment_generation=1,
    )
    with pytest.raises(ConfigurationConflictError, match="cannot change candidate"):
        await membership.apply(
            capacity_session,
            _request(active, capacity_projection, expected_revision=1),
            actor=DELEGATE,
            idempotency_key=UUID(int=20322),
        )


async def test_snapshot_revalidates_stored_request_bindings(
    capacity_session: AsyncSession,
) -> None:
    """A stored result is unusable if its authenticated request no longer binds it."""

    fixture, active = await _active_v3(capacity_session)
    membership = CapacityMembershipStore(fixture.store)
    await membership.apply(
        capacity_session,
        _request(active),
        actor=DELEGATE,
        idempotency_key=UUID(int=20330),
    )
    row = (await capacity_session.execute(select(CapacityPersonalMembershipEvent))).scalar_one()
    execution_row = (
        await capacity_session.execute(
            select(CapacityExecutionEpoch).where(
                CapacityExecutionEpoch.execution_epoch == active.execution_epoch
            )
        )
    ).scalar_one()
    row.request_payload = {**row.request_payload, "namespace_id": str(UUID(int=20331))}
    with capacity_session.no_autoflush:
        with pytest.raises(ConfigurationConflictError, match="event binding changed"):
            await membership.snapshot(capacity_session, execution_row)


@pytest.mark.parametrize("operation", ("update", "delete", "truncate"))
async def test_membership_event_log_is_sql_immutable(
    capacity_session: AsyncSession,
    operation: str,
) -> None:
    """Direct SQL cannot rewrite, remove, or truncate authenticated history."""

    fixture, active = await _active_v3(capacity_session)
    membership = CapacityMembershipStore(fixture.store)
    await membership.apply(
        capacity_session,
        _request(active),
        actor=DELEGATE,
        idempotency_key=UUID(int=20400),
    )
    statement = {
        "update": text("UPDATE capacity_personal_membership_events SET actor = 'replacement'"),
        "delete": delete(CapacityPersonalMembershipEvent),
        "truncate": text("TRUNCATE capacity_personal_membership_events"),
    }[operation]
    with pytest.raises(DBAPIError):
        async with capacity_session.begin_nested():
            await capacity_session.execute(statement)


async def test_membership_event_log_rejects_malformed_direct_insert(
    capacity_session: AsyncSession,
) -> None:
    """The SQL append guard validates an inserted event independently of Python."""

    _fixture, active = await _active_v3(capacity_session)
    request = _request(active)
    projection = request.projection
    capacity_session.add(
        CapacityPersonalMembershipEvent(
            execution_epoch=active.execution_epoch,
            execution_manifest_sha256=active.execution_manifest_sha256,
            authority_incarnation=active.authority_incarnation,
            writer_epoch=active.writer_epoch,
            namespace_id=NAMESPACE_ID,
            revision=1,
            previous_sha256="0" * 64,
            head_sha256="0" * 64,
            actor=DELEGATE,
            idempotency_key=UUID(int=20410),
            operation_id=projection.operation_id,
            request_digest="0" * 64,
            request_payload=request.model_dump(mode="json", exclude_none=False),
            subject_id=projection.subject_id,
            subject_incarnation=projection.subject_incarnation,
            owner_id=projection.owner_id,
            configuration_generation=projection.configuration_generation,
            deployment_generation=projection.deployment_generation,
            reporter_incarnation=projection.demand_reporter_incarnation,
            result_payload={"schema_version": 1},
        )
    )
    with pytest.raises(DBAPIError):
        async with capacity_session.begin_nested():
            await capacity_session.flush()


async def test_v2_active_execution_cannot_mutate_personal_membership(
    capacity_session: AsyncSession,
) -> None:
    """The legacy execution contract never silently acquires delegation."""

    policy = execution_policy()
    fixture = await setup_execution(capacity_session, execution_policy=policy)
    prepared = await fixture.store.prepare_execution_epoch(
        capacity_session,
        fixture.request,
        actor="preparation-operator",
        idempotency_key=UUID(int=20500),
    )
    await register_execution_executors(capacity_session, fixture, prepared)
    activation = await ready_execution_activation(
        capacity_session, fixture.store, fixture.request, prepared
    )
    active = await fixture.store.activate_execution_epoch(
        capacity_session,
        activation,
        actor="activation-operator",
        idempotency_key=UUID(int=20501),
    )
    with pytest.raises(ExecutionConflictError, match="does not delegate"):
        await CapacityMembershipStore(fixture.store).apply(
            capacity_session,
            _request(active),
            actor=DELEGATE,
            idempotency_key=UUID(int=20502),
        )


async def test_managed_base_requires_projection_evidence_and_can_advance_without_base_rewrite(
    capacity_session: AsyncSession,
) -> None:
    """A managed immutable base is eligible only through its real projection lineage."""

    fixture, active, managed = await _active_v3_with_managed_base(capacity_session)
    before = await fixture.store.load_allocation_input(capacity_session, fixture.writer)
    assert isinstance(before, DelegatedAllocationInputV2)
    assert before.managed_base_subjects == (managed,)
    managed_owner = UUID(hex=managed.account_id.removeprefix("dev-owner-"))
    projection = _projection(
        operation_kind="update",
        operation_epoch=2,
        operation_id=UUID(int=20520),
        subject_id=managed.subject_id,
        subject_incarnation=managed.subject_incarnation,
        owner_id=managed_owner,
        environment_name=managed.display_name.removeprefix("dev-"),
        expected_configuration_epoch=2,
        reporter_incarnation=UUID(int=20521),
    )
    result = await CapacityMembershipStore(fixture.store).apply(
        capacity_session,
        _request(active, projection),
        actor=DELEGATE,
        idempotency_key=UUID(int=20522),
    )
    after = await fixture.store.load_allocation_input(capacity_session, fixture.writer)
    assert result.revision == 1
    assert isinstance(after, DelegatedAllocationInputV2)
    assert canonical_bytes(after.configuration) == canonical_bytes(before.configuration)
    assert after.managed_base_subjects == (managed,)
    assert after.membership.members[0].configuration.configuration_generation == 2


@pytest.mark.parametrize(
    ("delegate_base", "update_base"), ((True, False), (False, False), (True, True))
)
async def test_new_owner_admission_preserves_existing_personal_base(
    capacity_session: AsyncSession, delegate_base: bool, update_base: bool
) -> None:
    """Base owners remain valid before and after their first membership event."""

    fixture, active, managed = await _active_v3_with_managed_base(
        capacity_session, delegate_base=delegate_base
    )
    membership = CapacityMembershipStore(fixture.store)
    before = await fixture.store.load_allocation_input(capacity_session, fixture.writer)
    revision = 0
    if update_base:
        projection = _projection(
            operation_kind="update",
            operation_epoch=2,
            operation_id=UUID(int=20800),
            subject_id=managed.subject_id,
            subject_incarnation=managed.subject_incarnation,
            owner_id=UUID(hex=managed.account_id.removeprefix("dev-owner-")),
            environment_name=managed.display_name.removeprefix("dev-"),
            expected_configuration_epoch=2,
            reporter_incarnation=UUID(int=20801),
        )
        result = await membership.apply(
            capacity_session,
            _request(active, projection),
            actor=DELEGATE,
            idempotency_key=UUID(int=20802),
        )
        revision = result.revision
    bob = _projection(expected_configuration_epoch=2).model_copy(
        update={"demand_reporter_token_sha256": "8" * 64}
    )
    admitted = await membership.apply(
        capacity_session,
        _request(active, bob, expected_revision=revision),
        actor=DELEGATE,
        idempotency_key=UUID(int=20803),
    )
    after = await fixture.store.load_allocation_input(capacity_session, fixture.writer)
    assert admitted.revision == revision + 1
    assert canonical_bytes(after.configuration) == canonical_bytes(before.configuration)
    assert {entry.configuration.subject_id for entry in after.subjects} == {
        *(entry.configuration.subject_id for entry in before.subjects),
        BOB_SUBJECT_ID,
    }
    assert managed.account_id in {value.account_id for value in after.effective_account_policies}
    if update_base:
        excessive = _projection(
            expected_configuration_epoch=2,
            operation_id=UUID(int=20804),
            owner_id=UUID(int=20805),
            subject_id=UUID(int=20806),
            subject_incarnation=UUID(int=20807),
            reporter_incarnation=UUID(int=20808),
            environment_name="carol",
        ).model_copy(update={"demand_reporter_token_sha256": "7" * 64})
        with pytest.raises(ConfigurationConflictError, match="exceeds its subject bound"):
            await membership.apply(
                capacity_session,
                _request(active, excessive, expected_revision=admitted.revision),
                actor=DELEGATE,
                idempotency_key=UUID(int=20809),
            )


@pytest.mark.parametrize("schema_version", (2, 3))
@pytest.mark.parametrize("tamper", (None, "configuration_generation", "payload"))
async def test_preparation_requires_exact_base_materialization(
    capacity_session: AsyncSession, schema_version: int, tamper: str | None
) -> None:
    """Both preparation versions reject corruption before reaching readiness."""

    fleet = fleet_with_development_template()
    base = subject_configuration(fleet)
    delegation = (
        PersonalMembershipPolicyV1(
            namespace_id=NAMESPACE_ID,
            management_principal_id=DELEGATE,
            development_template_sha256=canonical_digest(fleet.development_subject_template),
            max_subjects=2,
        )
        if schema_version == 3
        else None
    )
    fixture = await setup_execution(
        capacity_session,
        fleet=fleet,
        subjects=(base,),
        execution_policy=execution_policy(
            subject_acknowledgements=(execution_acknowledgement(subject=base),),
            personal_membership=delegation,
        ),
    )
    if tamper is not None:
        changed = (
            99
            if tamper == "configuration_generation"
            else {
                **base.model_dump(mode="json"),
                "display_name": "changed-name",
            }
        )
        await capacity_session.execute(
            update(CapacitySubject)
            .where(CapacitySubject.subject_id == base.subject_id)
            .values(**{tamper: changed})
        )
        with pytest.raises(ExecutionConflictError, match="base subject materialization changed"):
            await fixture.store.prepare_execution_epoch(
                capacity_session,
                fixture.request,
                actor="preparation-operator",
                idempotency_key=UUID(int=20820),
            )
    else:
        prepared = await fixture.store.prepare_execution_epoch(
            capacity_session,
            fixture.request,
            actor="preparation-operator",
            idempotency_key=UUID(int=20820),
        )
        assert prepared.execution_state == "prepared"


async def test_allocation_compare_and_swap_observes_cross_session_membership_change(
    isolated_capacity_postgres_url: str,
) -> None:
    """An allocation computed before another session's admission cannot commit."""

    engine = create_async_engine(isolated_capacity_postgres_url, isolation_level="SERIALIZABLE")
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    try:
        async with sessions() as setup_session, setup_session.begin():
            fixture, active = await _active_v3(setup_session)
        async with sessions() as reader, reader.begin():
            stale_input = await fixture.store.load_allocation_input(reader, fixture.writer)
        stale_allocation = allocate_shadow(stale_input)
        async with sessions() as mutator, mutator.begin():
            await CapacityMembershipStore(fixture.store).apply(
                mutator,
                _request(active),
                actor=DELEGATE,
                idempotency_key=UUID(int=20600),
            )
        async with sessions() as committer:
            with pytest.raises(StaleAllocationInputError, match="input changed"):
                await _commit_reconciled_epoch(
                    committer, fixture.store, fixture.writer, stale_allocation
                )
    finally:
        await engine.dispose()


@pytest.mark.parametrize(
    ("with_event", "error"),
    (
        (False, "delegated execution exists"),
        (True, "personal membership exists"),
    ),
)
async def test_capacity_0016_downgrade_refuses_delegated_authority_or_history(
    isolated_capacity_postgres_url: str,
    monkeypatch: pytest.MonkeyPatch,
    with_event: bool,
    error: str,
) -> None:
    """Schema rollback cannot erase active delegation or an immutable event log."""

    engine = create_async_engine(
        isolated_capacity_postgres_url,
        isolation_level="SERIALIZABLE",
    )
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    try:
        async with sessions() as session, session.begin():
            fixture, active = await _active_v3(session)
            if with_event:
                await CapacityMembershipStore(fixture.store).apply(
                    session,
                    _request(active),
                    actor=DELEGATE,
                    idempotency_key=UUID(int=20700),
                )
    finally:
        await engine.dispose()

    root = Path(__file__).resolve().parents[2]
    cfg = AlembicConfig(str(root / "capacity_migrations" / "alembic.ini"))
    cfg.set_main_option("script_location", str(root / "capacity_migrations"))
    monkeypatch.setenv("LOOM_CAPACITY_DB_URL", isolated_capacity_postgres_url)
    with pytest.raises(RuntimeError, match=error):
        command.downgrade(cfg, "capacity_0015")
