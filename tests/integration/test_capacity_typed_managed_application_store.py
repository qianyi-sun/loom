"""Managed applications adopt pinned origins through the same typed SQL boundary."""

from uuid import UUID

import pytest
from sqlalchemy import select, text
from sqlalchemy.exc import DBAPIError

from loom_capacity_manager.models import CapacityCandidate, CapacityDemandReporter
from loom_capacity_manager.store import WriterFence
from loom_capacity_manager.typed_membership_store import CapacityTypedMembershipStore
from tests.capacity_build_membership_fixtures import (
    build_request,
    managed_application_request,
    staged_build_event,
)
from tests.integration.test_capacity_build_membership_sql import _reseal
from tests.integration.test_capacity_mixed_membership_store import apply, transition
from tests.integration.test_capacity_typed_managed_base_history import prepared


@pytest.mark.parametrize("operation", ("capacity", "update", "destroy"))
async def test_typed_store_adopts_managed_base_without_recreating_it(capacity_session, operation):
    management, preparation, _fleet, execution = await prepared(capacity_session)
    request = managed_application_request(preparation, execution, operation=operation)
    receipt = await apply(capacity_session, request)
    assert receipt.revision == 1 and receipt.member.configuration.configuration_generation == 2
    assert receipt.member.configuration.subject_id == preparation.managed_application_origins[0].configuration.subject_id
    assert await apply(capacity_session, request) == receipt.model_copy(update={"replayed": True})
    value = await management.load_allocation_input(capacity_session,
        WriterFence(authority_incarnation=execution.authority_incarnation, writer_epoch=execution.writer_epoch))
    assert value.membership.members == (receipt.member,)
    assert sum(subject.configuration.subject_id == receipt.member.configuration.subject_id for subject in value.subjects) == 1


async def test_typed_managed_capacity_update_destroy_preserves_original_installation_and_reporter_history(capacity_session):
    _management, preparation, _fleet, execution = await prepared(capacity_session)
    origin = preparation.managed_application_origins[0]
    request = managed_application_request(preparation, execution)
    first = await apply(capacity_session, request)
    updated_request = transition(request, "update", revision=1)
    await apply(capacity_session, updated_request, key=100002)
    await apply(capacity_session, transition(updated_request, "destroy", revision=2), key=100003)
    assert (await CapacityTypedMembershipStore().snapshot(capacity_session, execution.execution_epoch, through_revision=1)).members == (first.member,)
    assert (await apply(capacity_session, request)).replayed
    candidate = (await capacity_session.scalars(select(CapacityCandidate).where(
        CapacityCandidate.subject_id == origin.configuration.subject_id,
        CapacityCandidate.candidate_generation == origin.configuration.candidate_generation))).one()
    assert candidate.attestation_payload["operation_id"] == str(origin.installation_projection.operation_id)
    reporter = (await capacity_session.scalars(select(CapacityDemandReporter).where(
        CapacityDemandReporter.reporter_incarnation == origin.configuration.demand_reporter_incarnation))).one()
    assert reporter.state == "fenced" and reporter.configuration_generation == 2
    assert reporter.token_sha256 == origin.base_projection.demand_reporter_token_sha256


async def test_managed_application_and_build_share_owner_account_and_global_revision(capacity_session):
    management, preparation, _fleet, execution = await prepared(capacity_session)
    owner = preparation.managed_application_origins[0].base_projection.owner_id
    build = await apply(capacity_session, build_request(preparation, execution, owner=owner.int))
    app = await apply(capacity_session, managed_application_request(preparation, execution, revision=1), key=100002)
    assert app.revision == 2 and app.member.configuration.account_id == build.member.configuration.account_id
    value = await management.load_allocation_input(capacity_session,
        WriterFence(authority_incarnation=execution.authority_incarnation, writer_epoch=execution.writer_epoch))
    assert value.membership.members == (build.member, app.member)
    assert sum(subject.configuration.account_id == app.member.configuration.account_id for subject in value.subjects) == 2


async def test_managed_application_cross_epoch_capacity_import_and_reporter_rotation(capacity_session):
    """Real retirement/reprojection and SQL lifecycle; V4 activation is still seeded."""
    from loom_capacity_manager.application_origin_contracts import ManagedApplicationOriginV1
    from loom_capacity_manager.build_membership_contracts import ExecutionPreparationV4
    from loom_capacity_manager.executable_contracts import ExecutableExecutorHeartbeatV2
    from loom_capacity_manager.execution_store import CapacityExecutionStore
    from loom_capacity_manager.models import CapacityExecutionEpoch
    from loom_capacity_manager.store import (
        AuthorityRecoveryError,
        CapacityManagementStore,
        ExecutionConflictError,
    )
    from tests.capacity_build_membership_fixtures import seed_typed_sql_execution
    from tests.capacity_execution_fixtures import PreparedExecutionFixture
    from tests.integration.test_capacity_manager_execution_epoch import (
        _drain_request,
        _publish_final_safe_evidence,
        _retirement_request,
    )

    management, preparation, _fleet, execution = await prepared(capacity_session)
    request = managed_application_request(preparation, execution)
    resized = await apply(capacity_session, request)
    typed = CapacityTypedMembershipStore()
    old_snapshot = await typed.snapshot(capacity_session, execution.execution_epoch)
    writer = WriterFence(authority_incarnation=execution.authority_incarnation, writer_epoch=execution.writer_epoch)
    fixture = PreparedExecutionFixture(store=management, writer=writer, request=preparation)
    executions = CapacityExecutionStore()
    for binding in preparation.executors:
        await executions.heartbeat_executor(capacity_session, ExecutableExecutorHeartbeatV2(
            execution=execution, executor_id=binding.executor_id, executor_incarnation=binding.executor_incarnation,
            pool_id=binding.pool_id, pool_generation=binding.pool_generation, heartbeat_sequence=1,
            journal_sequence=0, journal_digest="0" * 64))
    drained = await management.begin_execution_drain(capacity_session, _drain_request(execution),
        actor="retirement-operator", idempotency_key=UUID(int=88988))
    checkpoints = await _publish_final_safe_evidence(capacity_session, drained)
    await management.retire_execution_epoch(capacity_session, _retirement_request(drained, checkpoints),
        actor="retirement-operator", idempotency_key=UUID(int=88989))

    projected = await management.project_development_subject(capacity_session, request.command.projection,
        actor="configuration-operator", idempotency_key=UUID(int=88990))
    assert projected.subject == resized.member.configuration
    origin = ManagedApplicationOriginV1(configuration=projected.subject,
        installation_projection=preparation.managed_application_origins[0].installation_projection,
        base_projection=request.command.projection, acknowledgement=request.command.acknowledgement)
    assert origin.installation_projection.operation_id != origin.base_projection.operation_id
    successor = ExecutionPreparationV4.model_validate(preparation.model_dump(mode="python") | {
        "configuration_epoch": projected.configuration_epoch,
        "executors": tuple(binding.model_copy(update={"executor_incarnation": UUID(int=88980 + index)})
            for index, binding in enumerate(preparation.executors)),
        "managed_application_origins": (origin,),
        "subject_acknowledgements": tuple(origin.acknowledgement if ack.subject_id == origin.configuration.subject_id else ack
            for ack in preparation.subject_acknowledgements),
    })
    policy = management.execution_policy.model_copy(update={"executors": successor.executors})
    management = CapacityManagementStore(execution_policy=policy)
    fixture = PreparedExecutionFixture(store=management, writer=writer, request=successor)
    active = await seed_typed_sql_execution(capacity_session, fixture, successor, execution_epoch=43)
    next_request = managed_application_request(successor, active)
    await apply(capacity_session, next_request, key=88991)
    updated = await apply(capacity_session, transition(next_request, "update", revision=1), key=88992)
    value = await management.load_allocation_input(capacity_session, writer)
    assert value.membership.members == (updated.member,)
    assert sum(subject.configuration.subject_id == origin.configuration.subject_id for subject in value.subjects) == 1
    assert await typed.snapshot(capacity_session, execution.execution_epoch) == old_snapshot
    old_epoch = await capacity_session.get(CapacityExecutionEpoch, execution.execution_epoch)
    with pytest.raises(AuthorityRecoveryError):
        await typed.verify_snapshot_materialization(capacity_session, old_epoch, old_snapshot)
    with pytest.raises(ExecutionConflictError):
        await apply(capacity_session, request)


@pytest.mark.parametrize("operation", ("capacity", "update", "destroy"))
async def test_sql_admits_exact_first_managed_lifecycle(capacity_session, operation):
    management, preparation, fleet, execution = await prepared(capacity_session)
    row = await staged_build_event(capacity_session, management, preparation, fleet,
        managed_application_request(preparation, execution, operation=operation), idempotency_key=UUID(int=88901))
    capacity_session.add(row)
    await capacity_session.flush()


@pytest.mark.parametrize("changed", ("configuration_generation=99", "token_sha256=repeat('1',64)", "state='current'"))
async def test_sql_first_managed_update_requires_exact_fenced_base_reporter(capacity_session, changed):
    management, preparation, fleet, execution = await prepared(capacity_session)
    row = await staged_build_event(capacity_session, management, preparation, fleet,
        managed_application_request(preparation, execution, operation="update"), idempotency_key=UUID(int=88901))
    reporter = preparation.managed_application_origins[0].configuration.demand_reporter_incarnation
    await capacity_session.execute(text(f"UPDATE capacity_demand_reporters SET {changed} WHERE reporter_incarnation=:reporter"), {"reporter": reporter})
    with pytest.raises(DBAPIError) as error:
        async with capacity_session.begin_nested():
            capacity_session.add(row)
            await capacity_session.flush()
    assert error.value.orig.sqlstate == "23514"


@pytest.mark.parametrize("target", ("candidate-operation", "candidate-number", "deployment-number", "profile-number", "base-number"))
async def test_sql_managed_update_checks_original_installation_before_new_candidate(capacity_session, target):
    management, preparation, fleet, execution = await prepared(capacity_session)
    row = await staged_build_event(capacity_session, management, preparation, fleet,
        managed_application_request(preparation, execution, operation="update"), idempotency_key=UUID(int=88901))
    statements = {
        "candidate-operation": "UPDATE capacity_candidates SET attestation_payload=jsonb_set(attestation_payload,'{operation_id}','\"00000000-0000-0000-0000-000000000999\"') WHERE subject_id=:subject AND candidate_generation=1",
        "candidate-number": "UPDATE capacity_candidates SET attestation_payload=jsonb_set(attestation_payload,'{operation_epoch}','1.0') WHERE subject_id=:subject AND candidate_generation=1",
        "deployment-number": "UPDATE capacity_deployment_generations SET required_profiles=jsonb_set(required_profiles,'{0,worker_shapes,0,concurrency_slots}','1.0') WHERE subject_id=:subject AND deployment_generation=1",
        "profile-number": "UPDATE capacity_worker_profiles SET shape_catalog=jsonb_set(shape_catalog,'{0,concurrency_slots}','1.0') WHERE subject_id=:subject AND deployment_generation=1",
        "base-number": "UPDATE capacity_config_generations SET payload=jsonb_set(payload,'{max_slots}','2.0') WHERE subject_id=:subject",
    }
    await capacity_session.execute(text(statements[target]), {"subject": row.subject_id})
    with pytest.raises(DBAPIError) as error:
        async with capacity_session.begin_nested():
            capacity_session.add(row)
            await capacity_session.flush()
    assert error.value.orig.sqlstate == "23514"


async def test_sql_managed_origin_operation_identity_is_reserved_without_prior_event(capacity_session):
    management, preparation, fleet, execution = await prepared(capacity_session)
    row = await staged_build_event(capacity_session, management, preparation, fleet,
        managed_application_request(preparation, execution), idempotency_key=UUID(int=88901))
    row.operation_id = preparation.managed_application_origins[0].installation_projection.operation_id
    row.request_payload["command"]["projection"]["operation_id"] = str(row.operation_id)
    _reseal(row)
    with pytest.raises(DBAPIError) as error:
        async with capacity_session.begin_nested():
            capacity_session.add(row)
            await capacity_session.flush()
    assert error.value.orig.sqlstate == "23514"


async def test_sql_application_installation_helper_is_private_and_search_path_pinned(capacity_session):
    row = (await capacity_session.execute(text(
        "SELECT prosecdef, proconfig, EXISTS (SELECT 1 FROM aclexplode(coalesce(proacl, acldefault('f', proowner))) "
        "WHERE grantee=0 AND privilege_type='EXECUTE') FROM pg_proc "
        "WHERE oid='public.capacity_personal_application_installation_matches(jsonb,jsonb)'::regprocedure"
    ))).one()
    assert not row[0]
    assert "search_path=pg_catalog" in row[1]
    assert not row[2]
