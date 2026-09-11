"""Generation staging consumes pinned inherited bases, never invented requests."""

import json

import pytest
from sqlalchemy import func, select

from loom_capacity_manager.build_generation_store import stage_build_generation_evidence
from loom_capacity_manager.models import (
    CapacityCandidate,
    CapacityDemandReporter,
    CapacityDeploymentGeneration,
)
from loom_capacity_manager.typed_membership_commands import (
    parse_typed_membership_mutation,
    parse_typed_membership_result,
)
from tests.unit.test_capacity_typed_membership_events import _next_build_row, event_row
from tests.unit.test_capacity_typed_successor_history import successor_row


async def seed_base(session, *, operation="capacity"):
    value, request, result, first = event_row()
    await stage_build_generation_evidence(session, request, result.member, value.preparation, value.fleet)
    resized = _next_build_row(first, operation=operation)
    next_request = parse_typed_membership_mutation(json.dumps(resized.request_payload))
    next_result = parse_typed_membership_result(json.dumps(resized.result_payload))
    await stage_build_generation_evidence(session, next_request, next_result.member, value.preparation, value.fleet,
        previous=result.member, previous_request=request)
    return next_result.member


@pytest.mark.parametrize("operation", ("capacity", "destroy", "update"))
async def test_first_successor_build_stages_from_actual_inherited_service(capacity_session, operation):
    old = await seed_base(capacity_session)
    preparation, fleet, row = successor_row(build=True, operation=operation)
    base = preparation.managed_build_origins[0]
    assert base.configuration == old.configuration
    reporter = (await capacity_session.scalars(select(CapacityDemandReporter))).one()
    reporter.high_water = 7
    await capacity_session.flush()
    request = parse_typed_membership_mutation(json.dumps(row.request_payload))
    member = parse_typed_membership_result(json.dumps(row.result_payload)).member
    await stage_build_generation_evidence(capacity_session, request, member, preparation, fleet)
    assert await capacity_session.scalar(select(func.count()).select_from(CapacityDeploymentGeneration)) == (2 if operation == "update" else 1)
    assert await capacity_session.scalar(select(func.count()).select_from(CapacityCandidate)) == (2 if operation == "update" else 1)
    assert reporter.high_water == 7
    if operation == "update":
        assert reporter.state == "fenced"
    else:
        assert reporter.configuration_generation == member.configuration.configuration_generation
        assert reporter.token_sha256 == base.base_projection.demand_reporter_token_sha256


@pytest.mark.parametrize("tamper", ("token", "generation", "fenced", "ready"))
async def test_inherited_build_staging_requires_current_retained_facts(capacity_session, tamper):
    await seed_base(capacity_session)
    reporter = (await capacity_session.scalars(select(CapacityDemandReporter))).one()
    if tamper == "ready":
        deployment = (await capacity_session.scalars(select(CapacityDeploymentGeneration))).one()
        deployment.readiness_state = "ready"
    elif tamper == "token":
        assert reporter.token_sha256 != "a" * 64
        reporter.token_sha256 = "a" * 64
    elif tamper == "generation":
        reporter.configuration_generation += 1
    else:
        reporter.state = "fenced"
    await capacity_session.flush()
    preparation, fleet, row = successor_row(build=True)
    request = parse_typed_membership_mutation(json.dumps(row.request_payload))
    member = parse_typed_membership_result(json.dumps(row.result_payload)).member
    with pytest.raises(ValueError, match="retained build"):
        await stage_build_generation_evidence(capacity_session, request, member, preparation, fleet)
    assert await capacity_session.scalar(select(func.count()).select_from(CapacityDeploymentGeneration)) == 1


@pytest.mark.parametrize("changes", (
    {"demand_reporter_token_sha256": "9" * 64}, {"candidate_generation": 2},
    {"deployment_generation": 2},
))
async def test_inherited_non_deployment_staging_cannot_replace_service(capacity_session, changes):
    await seed_base(capacity_session)
    preparation, fleet, row = successor_row(build=True, **changes)
    request = parse_typed_membership_mutation(json.dumps(row.request_payload))
    member = parse_typed_membership_result(json.dumps(row.result_payload)).member
    with pytest.raises(ValueError, match="retain its service evidence"):
        await stage_build_generation_evidence(capacity_session, request, member, preparation, fleet)


async def test_inherited_disabled_first_create_stays_closed_in_staging(capacity_session):
    from uuid import UUID
    await seed_base(capacity_session, operation="destroy")
    preparation, fleet, row = successor_row(build=True, source_operation="destroy", operation="create",
        subject_incarnation=UUID(int=99920), demand_reporter_incarnation=UUID(int=99921), demand_reporter_token_sha256="9" * 64)
    request = parse_typed_membership_mutation(json.dumps(row.request_payload))
    member = parse_typed_membership_result(json.dumps(row.result_payload)).member
    with pytest.raises(ValueError, match="predecessor release"):
        await stage_build_generation_evidence(capacity_session, request, member, preparation, fleet)
