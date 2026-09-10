"""Recreation authenticates actual predecessor release, not caller certificates."""

from uuid import UUID

import pytest
from sqlalchemy import func, select
from sqlalchemy.exc import DBAPIError

from loom_capacity_manager.contracts import ObservedCommitmentV1
from loom_capacity_manager.membership_release import predecessor_release_sha256
from loom_capacity_manager.models import CapacityCandidate, CapacityDemandReporter
from loom_capacity_manager.store import ConfigurationConflictError, WriterFence
from loom_capacity_manager.typed_membership_store import CapacityTypedMembershipStore
from tests.capacity_build_membership_fixtures import (
    application_request,
    build_request,
    managed_application_request,
    staged_build_event,
    typed_sql_execution,
)
from tests.integration.test_capacity_build_membership_sql import _reseal
from tests.integration.test_capacity_mixed_membership_store import apply, transition
from tests.integration.test_capacity_typed_managed_base_history import prepared


def recreate(request, *, revision):
    result = transition(request, "create", revision=revision)
    projection = result.command.projection.model_copy(update={
        "subject_incarnation": UUID(int=910000 + revision), "candidate_generation": 1, "deployment_generation": 1,
        "demand_reporter_incarnation": UUID(int=920000 + revision), "demand_reporter_token_sha256": f"{930000 + revision:064x}",
    })
    acknowledgement = result.command.acknowledgement.model_copy(update={
        "subject_incarnation": projection.subject_incarnation, "deployment_generation": 1,
        "reporter_incarnation": projection.demand_reporter_incarnation,
    })
    return result.model_copy(update={"command": result.command.model_copy(update={"projection": projection, "acknowledgement": acknowledgement})})


async def retain_physical_charge(session, management, subject):
    profile = subject.profiles[0]
    shape = profile.worker_shapes[0]
    observed = ObservedCommitmentV1(kind="physical", commitment_id="predecessor-still-running",
        physical_identity="predecessor-still-running", subject_id=subject.subject_id,
        subject_incarnation=subject.subject_incarnation, pool_id=profile.pool_id,
        pool_generation=profile.pool_generation, deployment_generation=subject.deployment_generation,
        profile_id=shape.shape_id, profile_generation=profile.profile_generation,
        profile_digest=profile.profile_digest, shape_id=shape.shape_id, resources=shape.total_resources, state="live")
    await management._upsert_commitment(session, kind="physical", source_incarnation=UUID(int=99100),
        sequence=1, observed=observed, now=await session.scalar(select(func.now())))
    await session.flush()


@pytest.mark.parametrize("kind", ("application", "build", "managed"))
async def test_typed_recreation_generates_release_evidence_and_retains_old_installations(capacity_session, kind):
    management, preparation, _fleet, execution = await (prepared if kind == "managed" else typed_sql_execution)(capacity_session)
    request = {"build": build_request, "application": application_request, "managed": managed_application_request}[kind](preparation, execution)
    created = await apply(capacity_session, request)
    disabled_request = transition(request, "destroy", revision=1)
    disabled = await apply(capacity_session, disabled_request, key=940001)
    old_snapshot = await CapacityTypedMembershipStore().snapshot(capacity_session, execution.execution_epoch)
    recreated_request = recreate(disabled_request, revision=2)
    recreated = await apply(capacity_session, recreated_request, key=940002)
    proof = recreated.member.reincarnation
    assert proof is not None
    assert proof.predecessor == disabled.member.configuration
    assert proof.predecessor_revision == disabled.revision and proof.predecessor_head_sha256 == disabled.head_sha256
    assert proof.origin.subject_incarnation == created.member.configuration.subject_incarnation
    assert proof.release_set_sha256 == await predecessor_release_sha256(capacity_session, disabled.member.configuration)
    resized_request = transition(recreated_request, "capacity", revision=3)
    resized = await apply(capacity_session, resized_request, key=940003)
    assert resized.member.reincarnation == proof
    assert (await apply(capacity_session, recreated_request, key=940002)).replayed
    assert await CapacityTypedMembershipStore().snapshot(capacity_session, execution.execution_epoch, through_revision=2) == old_snapshot
    value = await management.load_allocation_input(capacity_session, WriterFence(authority_incarnation=execution.authority_incarnation, writer_epoch=execution.writer_epoch))
    assert value.membership.members == (resized.member,)
    candidates = (await capacity_session.scalars(select(CapacityCandidate).where(CapacityCandidate.subject_id == created.member.configuration.subject_id))).all()
    assert {(row.subject_incarnation, row.candidate_generation) for row in candidates} == {
        (created.member.configuration.subject_incarnation, 1), (recreated.member.configuration.subject_incarnation, 1)}
    reporters = (await capacity_session.scalars(select(CapacityDemandReporter).where(CapacityDemandReporter.subject_id == created.member.configuration.subject_id))).all()
    assert {row.state for row in reporters} == {"current", "fenced"}
    disabled_again = transition(resized_request, "destroy", revision=4)
    await apply(capacity_session, disabled_again, key=940004)
    second = await apply(capacity_session, recreate(disabled_again, revision=5), key=940005)
    assert second.member.reincarnation.origin == proof.origin
    assert second.member.reincarnation.predecessor.subject_incarnation == recreated.member.configuration.subject_incarnation


@pytest.mark.parametrize("build", (False, True))
async def test_typed_recreation_cannot_replace_an_active_predecessor(capacity_session, build):
    _management, preparation, _fleet, execution = await typed_sql_execution(capacity_session)
    request = (build_request if build else application_request)(preparation, execution)
    await apply(capacity_session, request)
    with pytest.raises(ConfigurationConflictError):
        await apply(capacity_session, recreate(request, revision=1), key=940001)
    assert (await CapacityTypedMembershipStore().snapshot(capacity_session, execution.execution_epoch)).revision == 1


@pytest.mark.parametrize("build", (False, True))
@pytest.mark.parametrize("tamper", (None, "release_set_sha256", "predecessor_revision", "predecessor_head_sha256", "origin", "missing", "charged", "foreign-candidate", "foreign-reporter"))
async def test_sql_recreation_checks_certificate_independently_of_python(capacity_session, build, tamper):
    management, preparation, fleet, execution = await typed_sql_execution(capacity_session)
    request = (build_request if build else application_request)(preparation, execution)
    await apply(capacity_session, request)
    disabled_request = transition(request, "destroy", revision=1)
    disabled = await apply(capacity_session, disabled_request, key=940001)
    recreated_request = recreate(disabled_request, revision=2)
    async with capacity_session.begin_nested() as trial:
        receipt = await apply(capacity_session, recreated_request, key=940002)
        proof = receipt.member.reincarnation
        await trial.rollback()
    row = await staged_build_event(capacity_session, management, preparation, fleet, recreated_request,
        previous_head=disabled.head_sha256, previous=disabled.member, previous_request=disabled_request,
        idempotency_key=UUID(int=940002), reincarnation=proof)
    certificate = row.result_payload["member"]["reincarnation"]
    if tamper in {"foreign-candidate", "foreign-reporter"}:
        model = CapacityCandidate if tamper == "foreign-candidate" else CapacityDemandReporter
        retained = (await capacity_session.scalars(select(model).where(
            model.subject_incarnation == recreated_request.command.projection.subject_incarnation))).one()
        fields = {column.name: getattr(retained, column.name) for column in model.__table__.columns if column.name != "id"}
        fields["subject_id"] = UUID(int=960001)
        if tamper == "foreign-reporter":
            fields.update(reporter_incarnation=UUID(int=960002), token_sha256="e" * 64)
        capacity_session.add(model(**fields))
        await capacity_session.flush()
    elif tamper == "charged":
        await retain_physical_charge(capacity_session, management, disabled.member.configuration)
    elif tamper == "missing":
        row.result_payload["member"]["reincarnation"] = None
    elif tamper == "origin":
        certificate["origin"]["generation"] += 1
    elif tamper == "predecessor_revision":
        certificate[tamper] = 1
    elif tamper is not None:
        certificate[tamper] = "f" * 64
    _reseal(row)
    if tamper is None:
        capacity_session.add(row)
        await capacity_session.flush()
        assert (await CapacityTypedMembershipStore().snapshot(capacity_session, execution.execution_epoch)).revision == 3
    else:
        with pytest.raises(DBAPIError) as failure:
            async with capacity_session.begin_nested():
                capacity_session.add(row)
                await capacity_session.flush()
        assert failure.value.orig.sqlstate == "23514"


@pytest.mark.parametrize("build", (False, True))
async def test_store_recreation_keeps_unreleased_predecessor_charged(capacity_session, build):
    management, preparation, _fleet, execution = await typed_sql_execution(capacity_session)
    request = (build_request if build else application_request)(preparation, execution)
    await apply(capacity_session, request)
    disabled_request = transition(request, "destroy", revision=1)
    disabled = await apply(capacity_session, disabled_request, key=940001)
    await retain_physical_charge(capacity_session, management, disabled.member.configuration)
    with pytest.raises(ConfigurationConflictError, match="unreleased observed commitments"):
        await apply(capacity_session, recreate(disabled_request, revision=2), key=940002)
    value = await management.load_allocation_input(capacity_session,
        WriterFence(authority_incarnation=execution.authority_incarnation, writer_epoch=execution.writer_epoch))
    assert value.membership.revision == 2 and value.membership.members == (disabled.member,)
    assert len(value.observed_commitments) == 1


async def test_retired_import_cannot_flatten_recreation_lineage(capacity_session):
    from tests.integration.test_capacity_retired_application_import import import_apps, retire

    management, preparation, _fleet, execution = await typed_sql_execution(capacity_session)
    request = application_request(preparation, execution)
    await apply(capacity_session, request)
    disabled_request = transition(request, "destroy", revision=1)
    await apply(capacity_session, disabled_request, key=940001)
    await apply(capacity_session, recreate(disabled_request, revision=2), key=940002)
    snapshot = await CapacityTypedMembershipStore().snapshot(capacity_session, execution.execution_epoch)
    await retire(capacity_session, management, preparation, execution)
    with pytest.raises(ConfigurationConflictError, match="recreation lineage"):
        await import_apps(capacity_session, management, execution, snapshot)


@pytest.mark.parametrize("build", (False, True))
async def test_store_cannot_recycle_a_predecessor_incarnation(capacity_session, build):
    _management, preparation, _fleet, execution = await typed_sql_execution(capacity_session)
    request = (build_request if build else application_request)(preparation, execution)
    await apply(capacity_session, request)
    disabled_request = transition(request, "destroy", revision=1)
    await apply(capacity_session, disabled_request, key=940001)
    successor = recreate(disabled_request, revision=2)
    incarnation = request.command.projection.subject_incarnation
    successor = successor.model_copy(update={"command": successor.command.model_copy(update={
        "projection": successor.command.projection.model_copy(update={"subject_incarnation": incarnation}),
        "acknowledgement": successor.command.acknowledgement.model_copy(update={"subject_incarnation": incarnation}),
    })})
    with pytest.raises(ConfigurationConflictError, match="incarnation was already used"):
        await apply(capacity_session, successor, key=940002)


async def test_concurrent_recreation_keeps_only_one_successor(isolated_capacity_postgres_url):
    import asyncio

    from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

    from loom_capacity_manager.models import CapacityPersonalMembershipEvent
    from loom_capacity_manager.store import CapacityStoreError

    engine = create_async_engine(isolated_capacity_postgres_url, isolation_level="SERIALIZABLE")
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    try:
        async with sessions() as session, session.begin():
            _management, preparation, _fleet, execution = await typed_sql_execution(session)
            request = build_request(preparation, execution)
            await apply(session, request)
            disabled_request = transition(request, "destroy", revision=1)
            await apply(session, disabled_request, key=940001)
        first = recreate(disabled_request, revision=2)
        second = first.model_copy(update={"command": first.command.model_copy(update={
            "projection": first.command.projection.model_copy(update={
                "subject_incarnation": UUID(int=950000), "operation_id": UUID(int=950001),
                "demand_reporter_incarnation": UUID(int=950002), "demand_reporter_token_sha256": "f" * 64}),
            "acknowledgement": first.command.acknowledgement.model_copy(update={
                "subject_incarnation": UUID(int=950000), "reporter_incarnation": UUID(int=950002)}),
        })})
        barrier = asyncio.Barrier(2)

        async def submit(index, successor):
            try:
                async with sessions() as session, session.begin():
                    await session.scalar(select(func.count()).select_from(CapacityPersonalMembershipEvent))
                    await barrier.wait()
                    return await apply(session, successor, key=940002 + index)
            except (CapacityStoreError, DBAPIError) as error:
                return error

        async with asyncio.timeout(30):
            outcomes = await asyncio.gather(submit(0, first), submit(1, second))
        assert sum(not isinstance(value, Exception) for value in outcomes) == 1
        failure = next(value for value in outcomes if isinstance(value, Exception))
        if isinstance(failure, DBAPIError):
            assert failure.orig.sqlstate == "40001"
        else:
            assert "must be retried" in str(failure)
        async with sessions() as session:
            snapshot = await CapacityTypedMembershipStore().snapshot(session, execution.execution_epoch)
            assert snapshot.revision == 3 and len(snapshot.members) == 1
            candidates = (await session.scalars(select(CapacityCandidate).where(
                CapacityCandidate.subject_id == request.command.acknowledgement.subject_id))).all()
            assert len(candidates) == 2
    finally:
        await engine.dispose()
