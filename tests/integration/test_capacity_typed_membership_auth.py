"""Typed service credentials authenticate identity, not runtime readiness."""

from hashlib import sha256
from types import SimpleNamespace
from uuid import UUID

import pytest
from sqlalchemy import select, update

from loom_capacity_manager.auth import AuthorizationError, CapacityPrincipalVerifier
from loom_capacity_manager.membership_auth import authenticate_personal_subject_agent
from loom_capacity_manager.models import CapacityDemandReporter, CapacityDeploymentGeneration
from tests.capacity_build_membership_fixtures import build_request, typed_sql_execution
from tests.integration.test_capacity_membership_agent_auth import _http_client
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


async def test_typed_reporter_http_scope_and_owner_boundaries(capacity_session):
    management, preparation, _fleet, execution = await typed_sql_execution(capacity_session)
    request = build_request(preparation, execution)
    token = "typed-build-reporter-test"
    request = request.model_copy(update={"command": request.command.model_copy(update={
        "projection": request.command.projection.model_copy(update={
            "demand_reporter_token_sha256": sha256(token.encode()).hexdigest()})})})
    result = await _apply(capacity_session, request)
    fixture = SimpleNamespace(store=management, writer=None)
    async with _http_client(capacity_session, fixture, execution, CapacityPrincipalVerifier(())) as (client, _app):
        for path, expected in (
            (f"/v2/subjects/{result.member.configuration.subject_id}/admission-work", 200),
            (f"/v2/subjects/{UUID(int=999)}/admission-work", 403),
            ("/v2/executors/gb10/work", 401), ("/v1/status", 401),
        ):
            response = await client.get(path, headers={"Authorization": f"Bearer {token}"})
            assert response.status_code == expected, (path, response.text)
            if expected == 200:
                assert response.json() is None


async def test_retired_v4_reporter_can_only_replay_exact_closure(capacity_session, monkeypatch):
    from unittest.mock import AsyncMock

    from loom_capacity_manager.executable_contracts import (
        ExecutableAdmissionPlanClosureAcknowledgementV2,
        ExecutableAdmissionPlanClosureV2,
        ExecutableExecutorHeartbeatV2,
        ExecutableIntentCloseV2,
        ExecutablePartialReleaseV2,
        ExecutableProtectedReleaseV2,
    )
    from tests.integration import test_capacity_typed_execution_guards as fixtures
    from tests.integration.test_capacity_manager_execution_epoch import (
        _drain_request,
        _publish_final_safe_evidence,
        _retirement_request,
    )
    from tests.integration.test_capacity_manager_execution_store import _admission_acknowledgement
    from tests.integration.test_capacity_typed_membership_execution import typed_management

    original = fixtures.application_request

    def token_request(*args, **kwargs):
        request = original(*args, **kwargs)
        token = f"typed-owner-{kwargs['owner']}"
        return request.model_copy(update={"command": request.command.model_copy(update={
            "projection": request.command.projection.model_copy(update={
                "demand_reporter_token_sha256": sha256(token.encode()).hexdigest()})})})

    monkeypatch.setattr(fixtures, "application_request", token_request)
    store, executor, preparation, execution, selected, plan = await fixtures.typed_admission_plan(capacity_session)
    management = typed_management(preparation)
    for other in preparation.executors:
        if other.executor_incarnation != executor.executor_incarnation:
            await store.heartbeat_executor(capacity_session, ExecutableExecutorHeartbeatV2(
                execution=execution, executor_id=other.executor_id,
                executor_incarnation=other.executor_incarnation, pool_id=other.pool_id,
                pool_generation=other.pool_generation, heartbeat_sequence=1,
                journal_sequence=0, journal_digest="0" * 64))
    drained = await management.begin_execution_drain(capacity_session, _drain_request(execution),
        actor="test-retirement", idempotency_key=UUID(int=991001))
    close = await store.next_pool_work(capacity_session, executor)
    assert isinstance(close, ExecutableIntentCloseV2)
    await store.begin_intent_close(capacity_session, close)
    await store.acknowledge_protected_release(capacity_session, ExecutableProtectedReleaseV2(
        binding=plan.shapes[0].binding, reporter_incarnation=selected.acknowledgement.reporter_incarnation,
        bootstrap_registration_epoch=1, protected_registration_epoch=2,
        bootstrap_revoked=True, protected_release_sha256="b" * 64),
        actor="owner-agent", idempotency_key=UUID(int=991002))
    release = await store.next_pool_work(capacity_session, executor)
    assert isinstance(release, ExecutablePartialReleaseV2)
    await store.release_shapes(capacity_session, release)
    checkpoints = await _publish_final_safe_evidence(capacity_session, drained,
        bindings=preparation.executors, typed_management=management)
    await management.retire_execution_epoch(capacity_session, _retirement_request(drained, checkpoints),
        actor="test-retirement", idempotency_key=UUID(int=991003))
    token = f"typed-owner-{selected.owner_id.int}"
    headers = {"Authorization": f"Bearer {token}"}
    subject = selected.configuration.subject_id
    fixture = SimpleNamespace(store=management, writer=None)
    async with _http_client(capacity_session, fixture, execution, CapacityPrincipalVerifier(()),
        execution_store=store) as (client, _app):
        response = await client.get(f"/v2/subjects/{subject}/admission-work", headers=headers)
        assert response.status_code == 200, response.text
        closure = ExecutableAdmissionPlanClosureV2.model_validate_json(response.content)
        assert closure.proposal == plan
        with monkeypatch.context() as race:
            race.setattr(store, "next_subject_admission_plan", AsyncMock(return_value=plan))
            assert (await client.get(f"/v2/subjects/{subject}/admission-work", headers=headers)).status_code == 401
        acknowledgement = ExecutableAdmissionPlanClosureAcknowledgementV2(
            closure_id=closure.closure_id, proposal_id=plan.proposal_id,
            proposal_digest=store.contract_digest(plan), plan_id=plan.plan_id,
            admission_incarnation=plan.admission_incarnation, subject_id=subject,
            subject_incarnation=selected.configuration.subject_incarnation,
            reporter_incarnation=selected.acknowledgement.reporter_incarnation,
            protected_admission_sha256=plan.protected_admission_sha256,
            close_reason=closure.close_reason, disposition_kind="never-converged", disposition_digest="e" * 64)
        for replayed in (False, True):
            response = await client.put(f"/v2/subjects/{subject}/admission-closures/{closure.closure_id}/acknowledgements",
                headers={**headers, "Idempotency-Key": str(UUID(int=991004))},
                json=acknowledgement.model_dump(mode="json"))
            assert response.status_code == 200, response.text
            assert response.json()["replayed"] is replayed
        response = await client.put(f"/v2/subjects/{subject}/admission-acknowledgements/{plan.proposal_id}",
            headers={**headers, "Idempotency-Key": str(UUID(int=991005))},
            json=_admission_acknowledgement(plan).model_dump(mode="json"))
        assert response.status_code == 401, response.text


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
