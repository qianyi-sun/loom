"""Real transactional build membership, before executable V4 admission opens."""

from importlib import import_module
from uuid import UUID

import pytest
from sqlalchemy import func, select

from loom_capacity_manager.membership_store import PersonalMembershipRevisionConflictError
from loom_capacity_manager.models import (
    CapacityAccountPolicy,
    CapacityCandidate,
    CapacityDemandReporter,
    CapacityDeploymentGeneration,
    CapacityPersonalMembershipEvent,
    CapacitySubject,
    CapacityWorkerProfile,
)
from loom_capacity_manager.store import ConfigurationConflictError, ExecutionConflictError, IdempotencyConflictError
from tests.capacity_build_membership_fixtures import build_request, typed_sql_execution


def _store():
    return import_module("loom_capacity_manager.typed_membership_store").CapacityTypedMembershipStore()


async def _apply(session, request, *, actor="build-management", key=92000):
    return await _store().apply_build(session, request, actor=actor, idempotency_key=UUID(int=key))


async def _count(session, model, subject_id):
    return await session.scalar(select(func.count()).select_from(model).where(model.subject_id == subject_id))


async def test_typed_store_creates_two_owner_services_and_replays_original_receipt(capacity_session):
    _management, preparation, _fleet, execution = await typed_sql_execution(capacity_session)
    first_request = build_request(preparation, execution)
    first = await _apply(capacity_session, first_request)
    second = await _apply(capacity_session, build_request(preparation, execution, owner=88011, revision=1), key=92001)
    replay = await _apply(capacity_session, first_request)
    assert first.revision == 1 and second.revision == 2
    assert replay == first.model_copy(update={"replayed": True})
    assert first.member.configuration.account_id != second.member.configuration.account_id
    for result in (first, second):
        subject_id = result.member.configuration.subject_id
        assert await _count(capacity_session, CapacityPersonalMembershipEvent, subject_id) == 1
        assert await _count(capacity_session, CapacityCandidate, subject_id) == 1
        assert await _count(capacity_session, CapacityWorkerProfile, subject_id) == 2
        deployment = (await capacity_session.scalars(select(CapacityDeploymentGeneration).where(CapacityDeploymentGeneration.subject_id == subject_id))).one()
        assert deployment.readiness_state == "pending"
        assert deployment.cutover_payload["purpose"] == "personal-build-worker"


@pytest.mark.parametrize("change", ("actor", "execution", "namespace"))
async def test_typed_store_rejects_wrong_authority_before_writing(capacity_session, change):
    _management, preparation, _fleet, execution = await typed_sql_execution(capacity_session)
    request = build_request(preparation, execution)
    if change == "execution":
        request = request.model_copy(update={"execution": execution.model_copy(update={"writer_epoch": 999})})
    elif change == "namespace":
        request = request.model_copy(update={"namespace_id": UUID(int=999)})
    with pytest.raises(ExecutionConflictError):
        await _apply(capacity_session, request, actor="foreign-manager" if change == "actor" else "build-management")
    assert await capacity_session.scalar(select(func.count()).select_from(CapacityPersonalMembershipEvent)) == 0
    assert await _count(capacity_session, CapacitySubject, request.command.acknowledgement.subject_id) == 0


@pytest.mark.parametrize("reuse", ("key", "operation", "changed-input"))
async def test_typed_store_rejects_replay_identity_reuse(capacity_session, reuse):
    _management, preparation, _fleet, execution = await typed_sql_execution(capacity_session)
    first = build_request(preparation, execution)
    await _apply(capacity_session, first)
    request = build_request(preparation, execution, owner=88011, revision=1)
    if reuse == "operation":
        request = request.model_copy(update={"command": request.command.model_copy(update={"projection": request.command.projection.model_copy(update={"operation_id": first.command.projection.operation_id})})})
    elif reuse == "changed-input":
        request = first.model_copy(update={"command": first.command.model_copy(update={"projection": first.command.projection.model_copy(update={"max_slots": 1})})})
    with pytest.raises(IdempotencyConflictError):
        await _apply(capacity_session, request, key=92001 if reuse == "operation" else 92000)
    assert await capacity_session.scalar(select(func.count()).select_from(CapacityPersonalMembershipEvent)) == 1


async def test_typed_store_stale_revision_leaves_no_partial_generation(capacity_session):
    _management, preparation, _fleet, execution = await typed_sql_execution(capacity_session)
    await _apply(capacity_session, build_request(preparation, execution))
    request = build_request(preparation, execution, owner=88011)
    with pytest.raises(PersonalMembershipRevisionConflictError):
        await _apply(capacity_session, request, key=92001)
    for model in (CapacitySubject, CapacityCandidate, CapacityDeploymentGeneration, CapacityWorkerProfile, CapacityDemandReporter):
        assert await _count(capacity_session, model, request.command.acknowledgement.subject_id) == 0


async def test_typed_store_subject_limit_leaves_no_partial_generation(capacity_session):
    _management, preparation, _fleet, execution = await typed_sql_execution(capacity_session, max_subjects=1)
    await _apply(capacity_session, build_request(preparation, execution))
    request = build_request(preparation, execution, owner=88011, revision=1)
    with pytest.raises(ConfigurationConflictError):
        await _apply(capacity_session, request, key=92001)
    for model in (CapacitySubject, CapacityCandidate, CapacityDeploymentGeneration, CapacityWorkerProfile, CapacityDemandReporter):
        assert await _count(capacity_session, model, request.command.acknowledgement.subject_id) == 0


@pytest.mark.parametrize("corrupt", ("subject", "account", "reporter", "candidate", "deployment"))
async def test_typed_store_replay_validates_current_retained_facts(capacity_session, corrupt):
    _management, preparation, _fleet, execution = await typed_sql_execution(capacity_session)
    request = build_request(preparation, execution)
    result = await _apply(capacity_session, request)
    subject_id = result.member.configuration.subject_id
    if corrupt == "account":
        row = (await capacity_session.scalars(select(CapacityAccountPolicy).where(CapacityAccountPolicy.account_id == result.member.configuration.account_id))).one()
        row.max_slots += 1
    elif corrupt == "subject":
        row = (await capacity_session.scalars(select(CapacitySubject).where(CapacitySubject.subject_id == subject_id))).one()
        row.max_slots += 1
    elif corrupt == "reporter":
        row = (await capacity_session.scalars(select(CapacityDemandReporter).where(CapacityDemandReporter.subject_id == subject_id))).one()
        row.token_sha256 = "f" * 64
    elif corrupt == "candidate":
        row = (await capacity_session.scalars(select(CapacityCandidate).where(CapacityCandidate.subject_id == subject_id))).one()
        row.artifact_payload = {}
    else:
        row = (await capacity_session.scalars(select(CapacityDeploymentGeneration).where(CapacityDeploymentGeneration.subject_id == subject_id))).one()
        row.readiness_state = "ready"
    await capacity_session.flush()
    with pytest.raises(ConfigurationConflictError):
        await _apply(capacity_session, request)


async def test_typed_store_preserves_enclosing_transaction_rollback(capacity_session):
    _management, preparation, _fleet, execution = await typed_sql_execution(capacity_session)
    request = build_request(preparation, execution)
    with pytest.raises(RuntimeError, match="abort owner operation"):
        async with capacity_session.begin_nested():
            await _apply(capacity_session, request)
            raise RuntimeError("abort owner operation")
    for model in (CapacityPersonalMembershipEvent, CapacitySubject, CapacityCandidate, CapacityDeploymentGeneration, CapacityWorkerProfile, CapacityDemandReporter):
        assert await _count(capacity_session, model, request.command.acknowledgement.subject_id) == 0
