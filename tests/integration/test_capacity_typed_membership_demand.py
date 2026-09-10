"""Fresh typed membership at the real demand boundary; activation stays separate."""

from uuid import UUID

import pytest
from sqlalchemy import func, select, update

from loom_capacity_manager.membership_current import resolve_current_subject
from loom_capacity_manager.models import (
    CapacityCandidate,
    CapacityDemandReporter,
    CapacityDemandSnapshot,
    CapacityDeploymentGeneration,
    CapacityExecutionEpoch,
    CapacitySubject,
    CapacityWorkerProfile,
)
from loom_capacity_manager.store import ConfigurationConflictError, UnknownReporterError
from tests.capacity_build_membership_fixtures import (
    application_request,
    build_request,
    typed_sql_execution,
)
from tests.capacity_fixtures import demand_snapshot
from tests.integration.test_capacity_mixed_membership_store import apply, transition


def report(subject, *, sequence=1):
    return demand_snapshot(
        subject_id=subject.subject_id,
        subject_incarnation=subject.subject_incarnation,
        configuration_generation=subject.configuration_generation,
        deployment_generation=subject.deployment_generation,
        reporter_incarnation=subject.demand_reporter_incarnation,
        sequence=sequence,
    )


async def test_two_fresh_typed_owners_publish_demand_without_losing_build_accounts(capacity_session):
    management, preparation, _fleet, execution = await typed_sql_execution(capacity_session)
    epoch = await capacity_session.get(CapacityExecutionEpoch, execution.execution_epoch)
    applications = []
    for index, owner in enumerate((88010, 88011)):
        build = await apply(capacity_session, build_request(preparation, execution, owner=owner, revision=index * 2), key=110000 + index * 2)
        application = await apply(capacity_session, application_request(preparation, execution, owner=owner, revision=index * 2 + 1), key=110001 + index * 2)
        assert build.member.configuration.account_id == application.member.configuration.account_id
        applications.append(application.member)
    assert applications[0].configuration.account_id != applications[1].configuration.account_id
    for member in applications:
        assert await resolve_current_subject(capacity_session, epoch, subject_id=member.configuration.subject_id) == (member.configuration, member.acknowledgement)
        accepted = await management.ingest_demand_snapshot(capacity_session, report(member.configuration), actor="owner-agent")
        assert accepted.sequence == 1
    assert await capacity_session.scalar(select(func.count()).select_from(CapacityDemandSnapshot)) == 2


@pytest.mark.parametrize("operation", ("capacity", "update", "destroy"))
async def test_typed_demand_follows_current_generation_not_original_create(capacity_session, operation):
    management, preparation, _fleet, execution = await typed_sql_execution(capacity_session)
    original = application_request(preparation, execution)
    created = await apply(capacity_session, original)
    changed = await apply(capacity_session, transition(original, operation, revision=1), key=110002)
    epoch = await capacity_session.get(CapacityExecutionEpoch, execution.execution_epoch)
    with pytest.raises(UnknownReporterError):
        await management.ingest_demand_snapshot(capacity_session, report(created.member.configuration), actor="old-owner-agent")
    assert await capacity_session.scalar(select(func.count()).select_from(CapacityDemandSnapshot)) == 0
    if operation == "destroy":
        with pytest.raises(ConfigurationConflictError, match="not active"):
            await resolve_current_subject(capacity_session, epoch, subject_id=changed.member.configuration.subject_id)
        assert await resolve_current_subject(capacity_session, epoch, subject_id=changed.member.configuration.subject_id, allow_disabled=True) == (changed.member.configuration, changed.member.acknowledgement)
    else:
        accepted = await management.ingest_demand_snapshot(capacity_session, report(changed.member.configuration), actor="owner-agent")
        assert accepted.sequence == 1


async def test_pending_typed_build_cannot_publish_executable_demand(capacity_session):
    management, preparation, _fleet, execution = await typed_sql_execution(capacity_session)
    created = await apply(capacity_session, build_request(preparation, execution))
    epoch = await capacity_session.get(CapacityExecutionEpoch, execution.execution_epoch)
    # A pending installation is retained accounting evidence, never admission.
    with pytest.raises(ConfigurationConflictError, match="deployment binding changed"):
        await resolve_current_subject(capacity_session, epoch, subject_id=created.member.configuration.subject_id)
    with pytest.raises(ConfigurationConflictError):
        await management.ingest_demand_snapshot(capacity_session, report(created.member.configuration), actor="build-agent")
    assert await capacity_session.scalar(select(func.count()).select_from(CapacityDemandSnapshot)) == 0


async def test_typed_current_subject_does_not_fall_back_for_unknown_identity(capacity_session):
    _management, preparation, _fleet, execution = await typed_sql_execution(capacity_session)
    await apply(capacity_session, application_request(preparation, execution))
    epoch = await capacity_session.get(CapacityExecutionEpoch, execution.execution_epoch)
    with pytest.raises(ConfigurationConflictError, match="unavailable"):
        await resolve_current_subject(capacity_session, epoch, subject_id=UUID(int=110999))


@pytest.mark.parametrize("changed", ("subject-payload", "subject-scalar", "candidate", "reporter", "deployment", "profile"))
async def test_typed_current_demand_rejects_corrupt_retained_evidence(capacity_session, changed):
    management, preparation, _fleet, execution = await typed_sql_execution(capacity_session)
    admitted = await apply(capacity_session, application_request(preparation, execution))
    subject = admitted.member.configuration
    changes = {
        "subject-payload": (CapacitySubject, {"payload": {}}),
        "subject-scalar": (CapacitySubject, {"max_slots": subject.max_slots + 1}),
        "candidate": (CapacityCandidate, {"source_payload": {"publication_sha256": "f" * 64}}),
        "reporter": (CapacityDemandReporter, {"token_sha256": "f" * 64}),
        "deployment": (CapacityDeploymentGeneration, {"readiness_state": "pending"}),
        "profile": (CapacityWorkerProfile, {"profile_digest": "f" * 64}),
    }
    model, fields = changes[changed]
    await capacity_session.execute(update(model).where(model.subject_id == subject.subject_id).values(**fields))
    with pytest.raises(ConfigurationConflictError):
        await management.ingest_demand_snapshot(capacity_session, report(subject), actor="owner-agent")
    assert await capacity_session.scalar(select(func.count()).select_from(CapacityDemandSnapshot)) == 0


async def test_typed_current_demand_keeps_source_admission_closed(capacity_session):
    from tests.integration.test_capacity_retired_source_graph import seed_active_successor
    from tests.integration.test_capacity_successor_source_verification import successor

    candidate, _exported = await successor(capacity_session)
    _management, candidate, _execution = await seed_active_successor(capacity_session, candidate, epoch=43)
    epoch = await capacity_session.get(CapacityExecutionEpoch, 43)
    subject = candidate.managed_application_origins[0].configuration
    with pytest.raises(ConfigurationConflictError, match="source graph authentication"):
        await resolve_current_subject(capacity_session, epoch, subject_id=subject.subject_id)


async def test_typed_current_demand_requires_activated_execution(capacity_session):
    _management, _preparation, _fleet, execution = await typed_sql_execution(capacity_session, activate=False)
    epoch = await capacity_session.get(CapacityExecutionEpoch, execution.execution_epoch)
    with pytest.raises(ConfigurationConflictError, match="not active"):
        await resolve_current_subject(capacity_session, epoch, subject_id=UUID(int=110999))
