"""One real transaction/history domain for personal applications and build services."""

import asyncio
from uuid import UUID

import pytest
from sqlalchemy import select
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from loom_capacity_manager.membership_store import PersonalMembershipRevisionConflictError
from loom_capacity_manager.models import CapacityCandidate, CapacityDemandReporter
from loom_capacity_manager.store import (
    CapacityStoreError,
    ConfigurationConflictError,
    IdempotencyConflictError,
    WriterFence,
)
from loom_capacity_manager.typed_membership_store import CapacityTypedMembershipStore
from tests.capacity_build_membership_fixtures import (
    application_request,
    build_request,
    typed_sql_execution,
)


async def apply(session, request, *, key=100001):
    return await CapacityTypedMembershipStore().apply(session, request, actor="build-management", idempotency_key=UUID(int=key))


def transition(request, operation, *, revision):
    old = request.command.projection
    generation = old.configuration_generation + 1
    fields = dict(operation_kind=operation, operation_epoch=generation, configuration_generation=generation,
        operation_id=UUID(int=101000 + revision), max_slots=1)
    if operation == "update":
        fields.update(candidate_generation=old.candidate_generation + 1, deployment_generation=old.deployment_generation + 1,
            demand_reporter_incarnation=UUID(int=102000 + revision), demand_reporter_token_sha256=f"{102000 + revision:064x}")
    projection = old.model_copy(update=fields)
    acknowledgement = request.command.acknowledgement.model_copy(update={
        "configuration_generation": generation, "deployment_generation": projection.deployment_generation,
        "reporter_incarnation": projection.demand_reporter_incarnation,
    })
    return request.model_copy(update={"expected_revision": revision,
        "command": request.command.model_copy(update={"projection": projection, "acknowledgement": acknowledgement})})


async def test_mixed_store_uses_one_owner_account_and_shared_revision(capacity_session):
    management, preparation, _fleet, execution = await typed_sql_execution(capacity_session)
    build = await apply(capacity_session, build_request(preparation, execution))
    app_request = application_request(preparation, execution, revision=1)
    app = await apply(capacity_session, app_request, key=100002)
    assert app.member.configuration.account_id == build.member.configuration.account_id
    assert app.member.purpose == "personal-application"
    assert app.revision == 2
    value = await management.load_allocation_input(capacity_session, WriterFence(authority_incarnation=execution.authority_incarnation, writer_epoch=execution.writer_epoch))
    assert value.membership.members == (build.member, app.member)
    assert await apply(capacity_session, app_request, key=100002) == app.model_copy(update={"replayed": True})


async def test_mixed_store_application_capacity_update_destroy_preserves_origin_and_replay(capacity_session):
    _management, preparation, _fleet, execution = await typed_sql_execution(capacity_session)
    original = application_request(preparation, execution)
    created = await apply(capacity_session, original)
    resized_request = transition(original, "capacity", revision=1)
    await apply(capacity_session, resized_request, key=100002)
    updated_request = transition(resized_request, "update", revision=2)
    await apply(capacity_session, updated_request, key=100003)
    destroyed = await apply(capacity_session, transition(updated_request, "destroy", revision=3), key=100004)
    assert destroyed.member.configuration.lifecycle_state == "disabled"
    candidates = (await capacity_session.scalars(select(CapacityCandidate).where(CapacityCandidate.subject_id == created.member.configuration.subject_id))).all()
    assert {row.attestation_payload["operation_id"] for row in candidates} == {str(original.command.projection.operation_id), str(updated_request.command.projection.operation_id)}
    assert (await apply(capacity_session, original)).replayed
    reporters = (await capacity_session.scalars(select(CapacityDemandReporter).where(CapacityDemandReporter.subject_id == created.member.configuration.subject_id))).all()
    assert {row.state for row in reporters} == {"current", "fenced"}
    with pytest.raises(ConfigurationConflictError):
        await apply(capacity_session, transition(updated_request, "capacity", revision=4), key=100005)


@pytest.mark.parametrize("reuse", ("operation", "key"))
async def test_mixed_store_replay_identity_cannot_cross_purpose(capacity_session, reuse):
    _management, preparation, _fleet, execution = await typed_sql_execution(capacity_session)
    build_request_value = build_request(preparation, execution)
    await apply(capacity_session, build_request_value)
    request = application_request(preparation, execution, revision=1)
    if reuse == "operation":
        request = request.model_copy(update={"command": request.command.model_copy(update={"projection": request.command.projection.model_copy(update={"operation_id": build_request_value.command.projection.operation_id})})})
    with pytest.raises(IdempotencyConflictError):
        await apply(capacity_session, request, key=100001 if reuse == "key" else 100002)


async def test_mixed_store_build_and_app_share_owner_live_subject_limit(capacity_session):
    _management, preparation, _fleet, execution = await typed_sql_execution(capacity_session)
    await apply(capacity_session, build_request(preparation, execution))
    await apply(capacity_session, application_request(preparation, execution, revision=1), key=100002)
    request = application_request(preparation, execution, owner=88011, revision=2)
    request = request.model_copy(update={"command": request.command.model_copy(update={
        "projection": request.command.projection.model_copy(update={"owner_id": UUID(int=88010)}),
    })})
    with pytest.raises(ConfigurationConflictError, match="max_live_subjects"):
        await apply(capacity_session, request, key=100003)
    assert (await CapacityTypedMembershipStore().snapshot(capacity_session, execution.execution_epoch)).revision == 2
    assert await capacity_session.scalar(select(CapacityCandidate.id).where(CapacityCandidate.subject_id == request.command.projection.subject_id)) is None


@pytest.mark.parametrize("same_owner", (True, False))
async def test_mixed_store_concurrent_build_and_app_retry_one_shared_revision(isolated_capacity_postgres_url, same_owner):
    engine = create_async_engine(isolated_capacity_postgres_url, isolation_level="SERIALIZABLE")
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    try:
        async with sessions() as session, session.begin():
            _management, preparation, _fleet, execution = await typed_sql_execution(session)
        requests = (build_request(preparation, execution), application_request(preparation, execution, owner=88010 if same_owner else 88011))
        barrier = asyncio.Barrier(2)

        async def submit(index):
            try:
                async with sessions() as session, session.begin():
                    await session.scalar(select(CapacityCandidate.id).limit(1))
                    await barrier.wait()
                    return await apply(session, requests[index], key=100001 + index)
            except (CapacityStoreError, DBAPIError) as exc:
                return exc

        async with asyncio.timeout(30):
            results = await asyncio.gather(submit(0), submit(1))
        assert sum(not isinstance(result, Exception) for result in results) == 1
        loser = next(index for index, result in enumerate(results) if isinstance(result, Exception))
        if isinstance(results[loser], DBAPIError):
            assert results[loser].orig.sqlstate == "40001"
        else:
            assert "must be retried" in str(results[loser])
        async with sessions() as session:
            with pytest.raises(PersonalMembershipRevisionConflictError):
                await apply(session, requests[loser], key=100001 + loser)
        async with sessions() as session:
            await apply(session, requests[loser].model_copy(update={"expected_revision": 1}), key=100001 + loser)
        async with sessions() as session:
            snapshot = await CapacityTypedMembershipStore().snapshot(session, execution.execution_epoch)
            assert {member.purpose for member in snapshot.members} == {"personal-application", "personal-build-worker"}
            assert len({member.configuration.account_id for member in snapshot.members}) == (1 if same_owner else 2)
    finally:
        await engine.dispose()
