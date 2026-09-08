"""Current membership resolution at the active demand boundary."""

from __future__ import annotations

from uuid import UUID

import pytest
from sqlalchemy import func, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from loom_capacity_manager.membership_current import resolve_current_subject
from loom_capacity_manager.membership_store import CapacityMembershipStore
from loom_capacity_manager.models import (
    CapacityCandidate,
    CapacityDemandReporter,
    CapacityDemandSnapshot,
    CapacityDeploymentGeneration,
    CapacityExecutionEpoch,
    CapacitySubject,
    CapacityWorkerProfile,
)
from loom_capacity_manager.store import ConfigurationConflictError
from tests.capacity_execution_fixtures import (
    execution_acknowledgement,
    execution_policy,
    ready_execution_activation,
    register_execution_executors,
    setup_execution,
)
from tests.capacity_fixtures import demand_snapshot, fleet_with_development_template
from tests.integration.test_capacity_membership import (
    BOB_OWNER_ID,
    BOB_SUBJECT_ID,
    DELEGATE,
    _active_v3,
    _active_v3_with_managed_base,
    _projection,
    _request,
)


async def _execution_epoch(
    capacity_session: AsyncSession, execution_epoch: int
) -> CapacityExecutionEpoch:
    return (
        await capacity_session.execute(
            select(CapacityExecutionEpoch).where(
                CapacityExecutionEpoch.execution_epoch == execution_epoch
            )
        )
    ).scalar_one()


async def _active_v2(capacity_session: AsyncSession):  # type: ignore[no-untyped-def]
    fleet = fleet_with_development_template()
    from tests.capacity_fixtures import subject_configuration

    subject = subject_configuration(fleet)
    policy = execution_policy(
        subject_acknowledgements=(execution_acknowledgement(subject=subject),)
    )
    fixture = await setup_execution(
        capacity_session,
        execution_policy=policy,
        fleet=fleet,
        subjects=(subject,),
    )
    prepared = await fixture.store.prepare_execution_epoch(
        capacity_session,
        fixture.request,
        actor="preparation-operator",
        idempotency_key=UUID(int=22001),
    )
    await register_execution_executors(capacity_session, fixture, prepared)
    activation = await ready_execution_activation(
        capacity_session, fixture.store, fixture.request, prepared
    )
    active = await fixture.store.activate_execution_epoch(
        capacity_session,
        activation,
        actor="activation-operator",
        idempotency_key=UUID(int=22002),
    )
    return fixture, active, subject, policy.subject_acknowledgements[0]


async def test_current_resolver_retains_exact_v2_base_behavior(
    capacity_session: AsyncSession,
) -> None:
    """A V2 active epoch resolves only its immutable materialized base evidence."""

    _fixture, active, expected_subject, expected_acknowledgement = await _active_v2(
        capacity_session
    )
    epoch = await _execution_epoch(capacity_session, active.execution_epoch)

    subject, acknowledgement = await resolve_current_subject(
        capacity_session, epoch, subject_id=expected_subject.subject_id
    )
    report = demand_snapshot(
        subject_id=subject.subject_id,
        subject_incarnation=subject.subject_incarnation,
        configuration_generation=subject.configuration_generation,
        deployment_generation=subject.deployment_generation,
        reporter_incarnation=subject.demand_reporter_incarnation,
    )
    ingested = await _fixture.store.ingest_demand_snapshot(
        capacity_session, report, actor="development"
    )

    assert subject == expected_subject
    assert acknowledgement == expected_acknowledgement
    assert ingested.sequence == 1


async def test_delegated_current_subject_accepts_real_demand(
    capacity_session: AsyncSession,
) -> None:
    """A newly admitted owner is not restricted to prepared base acknowledgements."""

    fixture, active = await _active_v3(capacity_session)
    projection = _projection()
    admitted = await CapacityMembershipStore(fixture.store).apply(
        capacity_session,
        _request(active, projection),
        actor=DELEGATE,
        idempotency_key=UUID(int=22101),
    )
    epoch = await _execution_epoch(capacity_session, active.execution_epoch)

    subject, acknowledgement = await resolve_current_subject(
        capacity_session, epoch, subject_id=BOB_SUBJECT_ID
    )
    report = demand_snapshot(
        subject_id=subject.subject_id,
        subject_incarnation=subject.subject_incarnation,
        configuration_generation=subject.configuration_generation,
        deployment_generation=subject.deployment_generation,
        reporter_incarnation=subject.demand_reporter_incarnation,
    )
    result = await fixture.store.ingest_demand_snapshot(
        capacity_session, report, actor="bob-capacity-agent"
    )

    assert subject == admitted.member.configuration
    assert acknowledgement == admitted.member.acknowledgement
    assert result.sequence == 1


async def test_managed_base_update_never_falls_back_to_prepared_generation(
    capacity_session: AsyncSession,
) -> None:
    """A delegated base ID resolves its latest member, not its prepared acknowledgement."""

    fixture, active, managed = await _active_v3_with_managed_base(capacity_session)
    projection = _projection(
        operation_kind="update",
        operation_epoch=2,
        operation_id=UUID(int=22111),
        subject_id=managed.subject_id,
        subject_incarnation=managed.subject_incarnation,
        owner_id=UUID(hex=managed.account_id.removeprefix("dev-owner-")),
        environment_name=managed.display_name.removeprefix("dev-"),
        expected_configuration_epoch=2,
        reporter_incarnation=UUID(int=22112),
        deployment_generation=2,
    )
    updated = await CapacityMembershipStore(fixture.store).apply(
        capacity_session,
        _request(active, projection),
        actor=DELEGATE,
        idempotency_key=UUID(int=22113),
    )
    epoch = await _execution_epoch(capacity_session, active.execution_epoch)

    current = await resolve_current_subject(capacity_session, epoch, subject_id=managed.subject_id)

    assert current == (updated.member.configuration, updated.member.acknowledgement)
    assert current[0].configuration_generation == 2
    assert current[0].demand_reporter_incarnation == UUID(int=22112)


async def test_other_owner_revision_does_not_invalidate_unchanged_current_subject(
    capacity_session: AsyncSession,
) -> None:
    """Current-target resolution is per subject, not pinned to the global log head."""

    fixture, active = await _active_v3(capacity_session, max_subjects=3)
    membership = CapacityMembershipStore(fixture.store)
    bob = await membership.apply(
        capacity_session,
        _request(active),
        actor=DELEGATE,
        idempotency_key=UUID(int=22201),
    )
    epoch = await _execution_epoch(capacity_session, active.execution_epoch)
    before = await resolve_current_subject(capacity_session, epoch, subject_id=BOB_SUBJECT_ID)
    carol_projection = _projection(
        operation_id=UUID(int=22202),
        subject_id=UUID(int=22203),
        subject_incarnation=UUID(int=22204),
        owner_id=UUID(int=22205),
        environment_name="carol",
        reporter_incarnation=UUID(int=22206),
    ).model_copy(update={"demand_reporter_token_sha256": "7" * 64})
    carol = await membership.apply(
        capacity_session,
        _request(active, carol_projection, expected_revision=bob.revision),
        actor=DELEGATE,
        idempotency_key=UUID(int=22207),
    )

    after = await resolve_current_subject(capacity_session, epoch, subject_id=BOB_SUBJECT_ID)

    assert carol.revision > bob.revision
    assert after == before == (bob.member.configuration, bob.member.acknowledgement)


async def test_disabled_current_member_rejects_resolution_and_demand(
    capacity_session: AsyncSession,
) -> None:
    """A prepared acknowledgement cannot reopen a disabled delegated generation."""

    fixture, active = await _active_v3(capacity_session)
    membership = CapacityMembershipStore(fixture.store)
    created = await membership.apply(
        capacity_session,
        _request(active),
        actor=DELEGATE,
        idempotency_key=UUID(int=22301),
    )
    destroyed_projection = _projection().model_copy(
        update={
            "operation_kind": "destroy",
            "operation_epoch": 2,
            "operation_id": UUID(int=22302),
            "configuration_generation": 2,
        }
    )
    destroyed = await membership.apply(
        capacity_session,
        _request(active, destroyed_projection, expected_revision=created.revision),
        actor=DELEGATE,
        idempotency_key=UUID(int=22303),
    )
    epoch = await _execution_epoch(capacity_session, active.execution_epoch)
    disabled = destroyed.member.configuration

    with pytest.raises(ConfigurationConflictError, match="not active"):
        await resolve_current_subject(capacity_session, epoch, subject_id=disabled.subject_id)
    report = demand_snapshot(
        subject_id=disabled.subject_id,
        subject_incarnation=disabled.subject_incarnation,
        configuration_generation=disabled.configuration_generation,
        deployment_generation=disabled.deployment_generation,
        reporter_incarnation=disabled.demand_reporter_incarnation,
    )
    with pytest.raises(ConfigurationConflictError, match="not active"):
        await fixture.store.ingest_demand_snapshot(
            capacity_session, report, actor="bob-capacity-agent"
        )
    assert (
        await capacity_session.scalar(select(func.count()).select_from(CapacityDemandSnapshot))
    ) == 0


@pytest.mark.parametrize(
    "tamper",
    (
        "payload",
        "scalar",
        "candidate",
        "deployment",
        "profile",
        "reporter-token",
        "installed-cutover",
    ),
)
async def test_current_resolver_rejects_tampered_materialization_or_retained_evidence(
    capacity_session: AsyncSession,
    tamper: str,
) -> None:
    """Current membership is usable only with exact durable materialization and evidence."""

    fixture, active = await _active_v3(capacity_session)
    admitted = await CapacityMembershipStore(fixture.store).apply(
        capacity_session,
        _request(active),
        actor=DELEGATE,
        idempotency_key=UUID(int=22401),
    )
    subject = admitted.member.configuration
    if tamper == "payload":
        changed = subject.model_copy(update={"max_slots": subject.max_slots + 1})
        await capacity_session.execute(
            update(CapacitySubject)
            .where(CapacitySubject.subject_id == subject.subject_id)
            .values(payload=changed.model_dump(mode="json", exclude_none=False))
        )
    elif tamper == "scalar":
        await capacity_session.execute(
            update(CapacitySubject)
            .where(CapacitySubject.subject_id == subject.subject_id)
            .values(max_slots=subject.max_slots + 1)
        )
    elif tamper == "candidate":
        await capacity_session.execute(
            update(CapacityCandidate)
            .where(CapacityCandidate.subject_id == subject.subject_id)
            .values(source_payload={"publication_sha256": "1" * 64})
        )
    elif tamper == "deployment":
        await capacity_session.execute(
            update(CapacityDeploymentGeneration)
            .where(CapacityDeploymentGeneration.subject_id == subject.subject_id)
            .values(readiness_state="pending")
        )
    elif tamper == "reporter-token":
        await capacity_session.execute(update(CapacityDemandReporter).where(
            CapacityDemandReporter.subject_id == subject.subject_id,
        ).values(token_sha256="1" * 64))
    elif tamper == "installed-cutover":
        await capacity_session.execute(update(CapacityDeploymentGeneration).where(
            CapacityDeploymentGeneration.subject_id == subject.subject_id,
        ).values(cutover_payload={"protected_admission_sha256": "1" * 64}))
    else:
        await capacity_session.execute(
            update(CapacityWorkerProfile)
            .where(CapacityWorkerProfile.subject_id == subject.subject_id)
            .values(profile_digest="1" * 64)
        )
    epoch = await _execution_epoch(capacity_session, active.execution_epoch)

    with pytest.raises(ConfigurationConflictError):
        await resolve_current_subject(capacity_session, epoch, subject_id=subject.subject_id)


async def test_current_resolver_rejects_inactive_epoch(
    capacity_session: AsyncSession,
) -> None:
    """Prepared execution evidence alone never authorizes current subject work."""

    _fixture, _active = await _active_v3(capacity_session)
    epoch = await _execution_epoch(capacity_session, 1)
    epoch.state = "prepared"

    with pytest.raises(ConfigurationConflictError, match="not active"):
        await resolve_current_subject(capacity_session, epoch, subject_id=BOB_OWNER_ID)
