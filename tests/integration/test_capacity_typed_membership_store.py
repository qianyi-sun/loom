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
from loom_capacity_manager.store import (
    ConfigurationConflictError,
    ExecutionConflictError,
    IdempotencyConflictError,
)
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


@pytest.mark.parametrize("same_owner", (False, True))
async def test_typed_store_concurrent_requests_retry_without_duplicate_or_partial_builds(isolated_capacity_postgres_url, same_owner):
    import asyncio

    from sqlalchemy.exc import DBAPIError
    from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

    from loom_capacity_manager.store import CapacityStoreError

    engine = create_async_engine(isolated_capacity_postgres_url, isolation_level="SERIALIZABLE")
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    try:
        async with sessions() as session, session.begin():
            _management, preparation, _fleet, execution = await typed_sql_execution(session)
        requests = (build_request(preparation, execution), build_request(preparation, execution, owner=88010 if same_owner else 88011))
        barrier = asyncio.Barrier(2)

        async def submit(index):
            try:
                async with sessions() as session, session.begin():
                    # Establish both SERIALIZABLE snapshots before either writer
                    # locks authority; no timing sleeps or mock lock behavior.
                    await session.scalar(select(func.count()).select_from(CapacityPersonalMembershipEvent))
                    await barrier.wait()
                    return await _apply(session, requests[index], key=92000 if same_owner else 92000 + index)
            except (CapacityStoreError, DBAPIError) as exc:
                return exc

        async with asyncio.timeout(30):
            outcomes = await asyncio.gather(submit(0), submit(1))
        assert sum(not isinstance(value, Exception) for value in outcomes) == 1
        failed_index = next(index for index, value in enumerate(outcomes) if isinstance(value, Exception))
        failed = outcomes[failed_index]
        if isinstance(failed, DBAPIError):
            assert failed.orig.sqlstate == "40001"
        else:
            assert isinstance(failed, CapacityStoreError) and "must be retried" in str(failed)
        async with sessions() as session:
            assert await session.scalar(select(func.count()).select_from(CapacityPersonalMembershipEvent)) == 1
        if same_owner:
            async with sessions() as session:
                retried = await _apply(session, requests[failed_index])
            assert retried.replayed and retried.revision == 1
        else:
            async with sessions() as session:
                with pytest.raises(PersonalMembershipRevisionConflictError):
                    await _apply(session, requests[failed_index], key=92000 + failed_index)
            async with sessions() as session:
                retried = await _apply(session, requests[failed_index].model_copy(update={"expected_revision": 1}), key=92000 + failed_index)
            assert not retried.replayed and retried.revision == 2
        async with sessions() as session:
            expected = 1 if same_owner else 2
            assert await session.scalar(select(func.count()).select_from(CapacityPersonalMembershipEvent)) == expected
            subject_ids = [request.command.acknowledgement.subject_id for request in requests]
            for model, multiplier in ((CapacityCandidate, 1), (CapacityDeploymentGeneration, 1), (CapacityWorkerProfile, 2), (CapacityDemandReporter, 1)):
                assert await session.scalar(select(func.count()).select_from(model).where(model.subject_id.in_(subject_ids))) == expected * multiplier
    finally:
        await engine.dispose()


async def test_typed_store_rolls_back_generation_writes_after_sql_event_rejection(capacity_session):
    from sqlalchemy.exc import DBAPIError

    from loom_capacity_manager.contracts import canonical_digest
    from loom_capacity_manager.models import CapacityConfigGeneration
    from loom_capacity_manager.typed_membership_commands import derive_build_member

    _management, preparation, fleet, execution = await typed_sql_execution(capacity_session)
    request = build_request(preparation, execution)
    subject = derive_build_member(request, preparation, fleet).configuration
    capacity_session.add(CapacityConfigGeneration(scope="subject", subject_id=subject.subject_id,
        subject_incarnation=subject.subject_incarnation, scope_generation=subject.configuration_generation,
        digest=canonical_digest(subject), payload=subject.model_dump(mode="json"), state="proposed",
        actor="existing-proposal", idempotency_key=UUID(int=93000)))
    await capacity_session.flush()
    with pytest.raises(DBAPIError, match="identity or membership bound changed"):
        await _apply(capacity_session, request)
    for model in (CapacityPersonalMembershipEvent, CapacitySubject, CapacityCandidate, CapacityDeploymentGeneration, CapacityWorkerProfile, CapacityDemandReporter):
        assert await _count(capacity_session, model, subject.subject_id) == 0
    assert await _count(capacity_session, CapacityConfigGeneration, subject.subject_id) == 1


@pytest.mark.parametrize("model,field,changed", (
    (CapacityCandidate, "artifact_payload", {}),
    (CapacityDeploymentGeneration, "readiness_state", "ready"),
    (CapacityDemandReporter, "token_sha256", "f" * 64),
    (CapacityWorkerProfile, "shape_catalog", []),
))
async def test_typed_store_replay_refreshes_retained_orm_objects(isolated_capacity_postgres_url, model, field, changed):
    from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

    engine = create_async_engine(isolated_capacity_postgres_url, isolation_level="SERIALIZABLE")
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    try:
        async with sessions() as reader:
            async with reader.begin():
                _management, preparation, _fleet, execution = await typed_sql_execution(reader)
                request = build_request(preparation, execution)
                await _apply(reader, request)
                retained = (await reader.scalars(select(model).where(model.subject_id == request.command.acknowledgement.subject_id))).first()
                retained_id = retained.id
            async with sessions() as writer, writer.begin():
                row = await writer.get(model, retained_id)
                setattr(row, field, changed)
            assert getattr(retained, field) != changed
            with pytest.raises(ConfigurationConflictError):
                await _apply(reader, request)
    finally:
        await engine.dispose()


@pytest.mark.parametrize("corrupt", ("candidate-version", "shape-boolean", "account-number"))
async def test_typed_store_replay_rejects_retained_json_type_aliases(capacity_session, corrupt):
    from copy import deepcopy

    _management, preparation, _fleet, execution = await typed_sql_execution(capacity_session)
    request = build_request(preparation, execution)
    result = await _apply(capacity_session, request)
    if corrupt == "account-number":
        row = (await capacity_session.scalars(select(CapacityAccountPolicy).where(CapacityAccountPolicy.account_id == result.member.configuration.account_id))).one()
        payload = deepcopy(row.payload)
        payload["max_slots"] = float(payload["max_slots"])
        row.payload = payload
    elif corrupt == "candidate-version":
        row = (await capacity_session.scalars(select(CapacityCandidate).where(CapacityCandidate.subject_id == result.member.configuration.subject_id))).one()
        payload = deepcopy(row.artifact_payload)
        payload["runtime_candidate"]["schema_version"] = 2.0
        row.artifact_payload = payload
    else:
        row = (await capacity_session.scalars(select(CapacityWorkerProfile).where(CapacityWorkerProfile.subject_id == result.member.configuration.subject_id))).first()
        payload = deepcopy(row.shape_catalog)
        payload[0]["concurrency_slots"] = True
        row.shape_catalog = payload
    # Force the semantically-equal JSON assignment to persist: SQLAlchemy's dirty
    # checker also uses Python equality and otherwise silently drops this change.
    from sqlalchemy.orm.attributes import flag_modified
    flag_modified(row, "payload" if corrupt == "account-number" else "artifact_payload" if corrupt == "candidate-version" else "shape_catalog")
    await capacity_session.flush()
    with pytest.raises(ConfigurationConflictError):
        await _apply(capacity_session, request)


@pytest.mark.parametrize("scope", ("fleet", "subject"))
async def test_typed_store_refreshes_retained_base_and_fleet_documents(isolated_capacity_postgres_url, scope):
    from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

    from loom_capacity_manager.models import CapacityConfigGeneration

    engine = create_async_engine(isolated_capacity_postgres_url, isolation_level="SERIALIZABLE")
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    try:
        async with sessions() as reader:
            async with reader.begin():
                _management, preparation, _fleet, execution = await typed_sql_execution(reader)
                request = build_request(preparation, execution)
                await _apply(reader, request)
                retained = (await reader.scalars(select(CapacityConfigGeneration).where(CapacityConfigGeneration.scope == scope))).first()
                retained_id = retained.id
            async with sessions() as writer, writer.begin():
                row = await writer.get(CapacityConfigGeneration, retained_id)
                row.payload = row.payload | {"schema_version": 99}
            assert retained.payload["schema_version"] == 1
            with pytest.raises(ConfigurationConflictError):
                await _apply(reader, request)
    finally:
        await engine.dispose()


def _transition(request, operation, *, max_slots=1):
    previous = request.command.projection
    generation = previous.configuration_generation + 1
    values = dict(operation_kind=operation, operation_epoch=generation, configuration_generation=generation,
        operation_id=UUID(int=95000 + generation), max_slots=max_slots)
    if operation == "update":
        values.update(deployment_generation=previous.deployment_generation + 1,
            demand_reporter_incarnation=UUID(int=96000 + generation), demand_reporter_token_sha256=f"{96000 + generation:064x}")
    projection = previous.model_copy(update=values)
    acknowledgement = request.command.acknowledgement.model_copy(update={
        "configuration_generation": generation, "deployment_generation": projection.deployment_generation,
        "reporter_incarnation": projection.demand_reporter_incarnation,
    })
    return request.model_copy(update={"expected_revision": request.expected_revision + 1,
        "command": request.command.model_copy(update={"projection": projection, "acknowledgement": acknowledgement})})


async def test_typed_store_complete_pending_service_lifecycle_keeps_cleanup_evidence(capacity_session):
    _management, preparation, _fleet, execution = await typed_sql_execution(capacity_session)
    initial = build_request(preparation, execution)
    created = await _apply(capacity_session, initial)
    updated_request = _transition(initial, "update")
    updated = await _apply(capacity_session, updated_request, key=92001)
    subject_id = created.member.configuration.subject_id
    reporters = list((await capacity_session.scalars(select(CapacityDemandReporter).where(CapacityDemandReporter.subject_id == subject_id))).all())
    assert {row.reporter_incarnation: row.state for row in reporters} == {
        initial.command.projection.demand_reporter_incarnation: "fenced",
        updated_request.command.projection.demand_reporter_incarnation: "current",
    }
    current = next(row for row in reporters if row.state == "current")
    current.high_water = 7
    await capacity_session.flush()
    capacity_request = _transition(updated_request, "capacity", max_slots=0)
    resized = await _apply(capacity_session, capacity_request, key=92002)
    destroy_request = _transition(capacity_request, "destroy")
    destroyed = await _apply(capacity_session, destroy_request, key=92003)
    assert [result.revision for result in (created, updated, resized, destroyed)] == [1, 2, 3, 4]
    assert destroyed.member.configuration.lifecycle_state == "disabled"
    assert destroyed.member.configuration.max_slots == 0
    assert current.high_water == 7 and current.state == "current"
    assert current.configuration_generation == 4
    assert current.token_sha256 == updated_request.command.projection.demand_reporter_token_sha256
    assert await _count(capacity_session, CapacityCandidate, subject_id) == 1
    assert await _count(capacity_session, CapacityDeploymentGeneration, subject_id) == 2
    assert (await _apply(capacity_session, initial)) == created.model_copy(update={"replayed": True})
    assert (await _apply(capacity_session, updated_request, key=92001)) == updated.model_copy(update={"replayed": True})
    assert (await _apply(capacity_session, destroy_request, key=92003)) == destroyed.model_copy(update={"replayed": True})


@pytest.mark.parametrize("operation", ("update", "capacity", "destroy"))
async def test_typed_store_cannot_mutate_destroyed_service(capacity_session, operation):
    _management, preparation, _fleet, execution = await typed_sql_execution(capacity_session)
    initial = build_request(preparation, execution)
    await _apply(capacity_session, initial)
    destroy = _transition(initial, "destroy")
    await _apply(capacity_session, destroy, key=92001)
    with pytest.raises(ConfigurationConflictError):
        await _apply(capacity_session, _transition(destroy, operation), key=92002)
    assert await _count(capacity_session, CapacityPersonalMembershipEvent, initial.command.acknowledgement.subject_id) == 2
