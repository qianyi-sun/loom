"""Typed service credentials authenticate identity, not runtime readiness."""

import pytest
from sqlalchemy import select, update

from loom_capacity_manager.auth import AuthorizationError
from loom_capacity_manager.membership_auth import authenticate_personal_subject_agent
from loom_capacity_manager.models import CapacityDemandReporter, CapacityDeploymentGeneration
from tests.capacity_build_membership_fixtures import build_request, typed_sql_execution
from tests.integration.test_capacity_typed_membership_store import _apply, _transition


async def test_typed_build_reporter_authenticates_without_promoting_readiness(capacity_session):
    management, preparation, _fleet, execution = await typed_sql_execution(capacity_session)
    request = build_request(preparation, execution)
    result = await _apply(capacity_session, request)
    actor = await authenticate_personal_subject_agent(capacity_session, management,
        token_sha256=request.command.projection.demand_reporter_token_sha256)
    subject = result.member.configuration
    assert actor.subject_id == subject.subject_id
    assert actor.subject_incarnation == subject.subject_incarnation
    assert actor.demand_reporter_incarnation == subject.demand_reporter_incarnation
    assert actor.scopes == frozenset({"capacity:report:demand"})
    assert await capacity_session.scalar(select(CapacityDeploymentGeneration.readiness_state).where(
        CapacityDeploymentGeneration.subject_id == subject.subject_id)) == "pending"


async def test_typed_rotation_keeps_historical_identity_for_store_fenced_cleanup(capacity_session):
    management, preparation, _fleet, execution = await typed_sql_execution(capacity_session)
    initial = build_request(preparation, execution)
    await _apply(capacity_session, initial)
    current = _transition(initial, "update")
    await _apply(capacity_session, current, key=92001)
    for request in (initial, current):
        actor = await authenticate_personal_subject_agent(capacity_session, management,
            token_sha256=request.command.projection.demand_reporter_token_sha256)
        assert actor.demand_reporter_incarnation == request.command.projection.demand_reporter_incarnation
    # A capacity-only update retains the reporter and advances its row generation.
    resized = _transition(current, "capacity", max_slots=0)
    await _apply(capacity_session, resized, key=92002)
    actor = await authenticate_personal_subject_agent(capacity_session, management,
        token_sha256=resized.command.projection.demand_reporter_token_sha256)
    assert actor.demand_reporter_incarnation == current.command.projection.demand_reporter_incarnation


@pytest.mark.parametrize("field,value", (
    ("state", "equivocal"), ("state", "fenced"), ("token_sha256", "f" * 64),
    ("configuration_generation", 999), ("deployment_generation", 999),
))
async def test_typed_reporter_rejects_forged_current_materialization(capacity_session, field, value):
    management, preparation, _fleet, execution = await typed_sql_execution(capacity_session)
    request = build_request(preparation, execution)
    result = await _apply(capacity_session, request)
    await capacity_session.execute(update(CapacityDemandReporter).where(
        CapacityDemandReporter.subject_id == result.member.configuration.subject_id).values(**{field: value}))
    await capacity_session.commit()
    with pytest.raises(AuthorizationError):
        await authenticate_personal_subject_agent(capacity_session, management,
            token_sha256="f" * 64 if field == "token_sha256" else request.command.projection.demand_reporter_token_sha256)
