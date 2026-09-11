"""Build-generation facts persist separately from application installation evidence."""

from importlib import import_module
from uuid import UUID

import pytest
from sqlalchemy import func, select

from loom_capacity_manager.models import (
    CapacityCandidate,
    CapacityDemandReporter,
    CapacityDeploymentGeneration,
    CapacityWorkerProfile,
)
from tests.unit.test_capacity_typed_membership_commands import typed_build_mutation


def _store():
    return import_module("loom_capacity_manager.build_generation_store")


def _next(request, *, operation="capacity", **changes):
    projection = request.command.projection.model_copy(update={
        "operation_kind": operation, "operation_epoch": 2, "configuration_generation": 2,
        "operation_id": UUID(int=780), **changes,
    })
    ack = request.command.acknowledgement.model_copy(update={
        "configuration_generation": projection.configuration_generation,
        "deployment_generation": projection.deployment_generation,
        "reporter_incarnation": projection.demand_reporter_incarnation,
    })
    return request.model_copy(update={"expected_revision": 2, "command": request.command.model_copy(update={"projection": projection, "acknowledgement": ack})})


async def test_build_generation_staging_records_native_runtime_not_application_installation(capacity_session):
    module, value, request = typed_build_mutation()
    member = module.derive_build_member(request, value.preparation, value.fleet)
    await _store().stage_build_generation_evidence(capacity_session, request, member, value.preparation, value.fleet)
    candidate = (await capacity_session.scalars(select(CapacityCandidate))).one()
    deployment = (await capacity_session.scalars(select(CapacityDeploymentGeneration))).one()
    reporter = (await capacity_session.scalars(select(CapacityDemandReporter))).one()
    profiles = (await capacity_session.scalars(select(CapacityWorkerProfile))).all()
    assert candidate.candidate_identity_algorithm == "git-sha1"
    assert candidate.candidate_identity == value.preparation.personal_builds.runtime_candidate.identity
    assert len(candidate.candidate_digest) == 64
    assert candidate.launcher_payload["purpose"] == "personal-build-worker"
    assert "capacity_agent_installation_sha256" not in deployment.cutover_payload
    assert "local_activation_sha256" not in candidate.launcher_payload
    assert deployment.readiness_state == "pending"
    assert reporter.token_sha256 == request.command.projection.demand_reporter_token_sha256
    assert {profile.pool_id for profile in profiles} == {"gb10", "oldlab"}


async def test_build_service_rotation_keeps_candidate_and_fences_old_reporter(capacity_session):
    module, value, request = typed_build_mutation()
    first = module.derive_build_member(request, value.preparation, value.fleet)
    await _store().stage_build_generation_evidence(capacity_session, request, first, value.preparation, value.fleet)
    updated = _next(request, operation="update", deployment_generation=2, demand_reporter_incarnation=UUID(int=999), demand_reporter_token_sha256="a" * 64)
    second = module.derive_build_member(updated, value.preparation, value.fleet)
    await _store().stage_build_generation_evidence(capacity_session, updated, second, value.preparation, value.fleet, previous=first, previous_request=request)
    assert await capacity_session.scalar(select(func.count()).select_from(CapacityCandidate)) == 1
    assert await capacity_session.scalar(select(func.count()).select_from(CapacityDeploymentGeneration)) == 2
    rows = (await capacity_session.scalars(select(CapacityDemandReporter))).all()
    assert {row.reporter_incarnation: row.state for row in rows} == {
        first.configuration.demand_reporter_incarnation: "fenced",
        second.configuration.demand_reporter_incarnation: "current",
    }


@pytest.mark.parametrize("operation", ("capacity", "destroy"))
async def test_non_deployment_build_changes_keep_reporter_token_and_high_water(capacity_session, operation):
    module, value, request = typed_build_mutation()
    first = module.derive_build_member(request, value.preparation, value.fleet)
    await _store().stage_build_generation_evidence(capacity_session, request, first, value.preparation, value.fleet)
    reporter = (await capacity_session.scalars(select(CapacityDemandReporter))).one()
    reporter.high_water = 7
    await capacity_session.flush()
    updated = _next(request, operation=operation)
    second = module.derive_build_member(updated, value.preparation, value.fleet)
    await _store().stage_build_generation_evidence(capacity_session, updated, second, value.preparation, value.fleet, previous=first, previous_request=request)
    assert reporter.high_water == 7
    assert reporter.configuration_generation == 2
    assert reporter.token_sha256 == request.command.projection.demand_reporter_token_sha256
    assert await capacity_session.scalar(select(func.count()).select_from(CapacityCandidate)) == 1
    assert await capacity_session.scalar(select(func.count()).select_from(CapacityDeploymentGeneration)) == 1


@pytest.mark.parametrize("tamper", ("candidate", "deployment", "deployment_ready", "profile", "token", "reporter_generation"))
async def test_build_generation_transition_rejects_corrupted_retained_facts(capacity_session, tamper):
    module, value, request = typed_build_mutation()
    first = module.derive_build_member(request, value.preparation, value.fleet)
    await _store().stage_build_generation_evidence(capacity_session, request, first, value.preparation, value.fleet)
    if tamper == "candidate":
        row = (await capacity_session.scalars(select(CapacityCandidate))).one()
        row.artifact_payload = {"candidate_sha256": "a" * 64}
    elif tamper in {"deployment", "deployment_ready"}:
        row = (await capacity_session.scalars(select(CapacityDeploymentGeneration))).one()
        if tamper == "deployment":
            row.cutover_payload = {"purpose": "personal-application"}
        else:
            row.readiness_state = "ready"
    elif tamper == "profile":
        row = (await capacity_session.scalars(select(CapacityWorkerProfile))).first()
        row.shape_catalog = []
    else:
        row = (await capacity_session.scalars(select(CapacityDemandReporter))).one()
        if tamper == "token":
            row.token_sha256 = "b" * 64
        else:
            row.configuration_generation = 9
    await capacity_session.flush()
    updated = _next(request)
    second = module.derive_build_member(updated, value.preparation, value.fleet)
    with pytest.raises(ValueError):
        await _store().stage_build_generation_evidence(capacity_session, updated, second, value.preparation, value.fleet, previous=first, previous_request=request)


async def test_build_generation_staging_participates_in_caller_rollback(capacity_session):
    module, value, request = typed_build_mutation()
    member = module.derive_build_member(request, value.preparation, value.fleet)
    with pytest.raises(RuntimeError, match="abort enclosing membership transaction"):
        async with capacity_session.begin_nested():
            await _store().stage_build_generation_evidence(capacity_session, request, member, value.preparation, value.fleet)
            raise RuntimeError("abort enclosing membership transaction")
    for model in (CapacityCandidate, CapacityDemandReporter, CapacityDeploymentGeneration, CapacityWorkerProfile):
        assert await capacity_session.scalar(select(func.count()).select_from(model)) == 0


async def test_failed_build_rotation_rolls_back_its_partial_deployment_writes(capacity_session):
    module, value, request = typed_build_mutation()
    first = module.derive_build_member(request, value.preparation, value.fleet)
    await _store().stage_build_generation_evidence(capacity_session, request, first, value.preparation, value.fleet)
    profile = first.configuration.profiles[0]
    # A conflicting retained profile forces failure after staging the next deployment.
    capacity_session.add(CapacityWorkerProfile(
        subject_id=first.configuration.subject_id, subject_incarnation=first.configuration.subject_incarnation,
        deployment_generation=2, pool_id=profile.pool_id, pool_generation=profile.pool_generation,
        profile_generation=profile.profile_generation, profile_digest=profile.profile_digest,
        shape_catalog=[], narrowing_constraints={"eligible_resource_domains": list(profile.eligible_resource_domains)},
    ))
    await capacity_session.flush()
    updated = _next(request, operation="update", deployment_generation=2, demand_reporter_incarnation=UUID(int=999), demand_reporter_token_sha256="a" * 64)
    second = module.derive_build_member(updated, value.preparation, value.fleet)
    from loom_capacity_manager.store import ConfigurationConflictError
    with pytest.raises(ConfigurationConflictError):
        await _store().stage_build_generation_evidence(capacity_session, updated, second, value.preparation, value.fleet, previous=first, previous_request=request)
    assert await capacity_session.scalar(select(func.count()).select_from(CapacityDeploymentGeneration)) == 1
    reporter = (await capacity_session.scalars(select(CapacityDemandReporter))).one()
    assert reporter.state == "current"
    assert reporter.reporter_incarnation == first.configuration.demand_reporter_incarnation
