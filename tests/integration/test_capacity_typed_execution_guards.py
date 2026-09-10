"""Typed SQL execution reads must preserve real mixed owner allocation history."""

import hashlib
import json
from copy import deepcopy
from datetime import UTC, datetime, timedelta
from uuid import UUID

import pytest
from sqlalchemy import select, text, update
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import async_sessionmaker

from loom_capacity_manager.executable_contracts import (
    ExecutableAdmissionPlanClosureV2,
    ExecutableAdmissionPlanProposalV2,
    ExecutableBootstrapAcknowledgementV2,
    ExecutableBootstrapProposalV2,
    ExecutableExecutorHeartbeatV2,
    ExecutableIntentBindingV2,
    ExecutableReservationAcceptanceV2,
    ExecutableReservationProposalV2,
    ExecutionFenceV2,
)
from loom_capacity_manager.execution_store import CapacityExecutionStore
from loom_capacity_manager.membership_digest import canonical_membership_event_head
from loom_capacity_manager.models import (
    CapacityAccountPolicy,
    CapacityAllocationEpoch,
    CapacityAuthorityState,
    CapacityCandidate,
    CapacityDemandReporter,
    CapacityDeploymentGeneration,
    CapacityPersonalMembershipEvent,
)
from loom_capacity_manager.reconciler import reconcile_shadow_once
from loom_capacity_manager.store import ExecutionConflictError, ReportEquivocationError, WriterFence
from loom_capacity_manager.typed_inventory_contracts import ExecutableExecutorInventoryV3
from loom_capacity_manager.typed_membership_commands import (
    PersonalMembershipResultV2,
)
from tests.capacity_build_membership_fixtures import (
    application_request,
    build_request,
    typed_sql_execution,
)
from tests.capacity_fixtures import development_projection, pool_observation
from tests.integration.test_capacity_mixed_membership_store import apply, transition
from tests.integration.test_capacity_typed_membership_demand import report
from tests.integration.test_capacity_typed_membership_execution import typed_management


async def sealed_owners(session, *, owner_rate=8):
    _legacy, preparation, _fleet, execution = await typed_sql_execution(session,
        owner_submission_rate_per_minute=owner_rate)
    management = typed_management(preparation)
    authority = await session.get(CapacityAuthorityState, 1)
    authority.increase_freeze = False
    authority.increase_freeze_reason = None
    members = []
    for index, owner in enumerate((88010, 88011)):
        build = await apply(session, build_request(preparation, execution,
            owner=owner, revision=index * 2), key=121000 + index * 2)
        application = await apply(session, application_request(preparation, execution,
            owner=owner, revision=index * 2 + 1), key=121001 + index * 2)
        members.extend((build.member, application.member))
        demand = report(application.member.configuration)
        demand = demand.model_copy(update={"pending_unassigned": tuple(
            item.model_copy(update={"attempt_ids": (str(UUID(int=owner * 100 + offset)),)})
            for offset, item in enumerate(demand.pending_unassigned))})
        await management.ingest_demand_snapshot(session, demand,
            actor="owner-agent")
    allocation = await seal_allocation(session, management, execution)
    return preparation, execution, allocation, members


async def seal_allocation(session, management, execution):
    authority = await session.get(CapacityAuthorityState, 1)
    authority.increase_freeze = False
    authority.increase_freeze_reason = None
    for pool in ("gb10", "oldlab"):
        await management.ingest_pool_observation(session,
            pool_observation(sequence=1, pool_id=pool), actor=f"{pool}-reporter")
    sessions = async_sessionmaker(bind=session.bind, expire_on_commit=False,
        join_transaction_mode="create_savepoint")
    result = await reconcile_shadow_once(sessions,
        WriterFence(authority_incarnation=execution.authority_incarnation,
            writer_epoch=execution.writer_epoch), store=management)
    assert result.status == "committed", result
    allocation = (await session.scalars(select(CapacityAllocationEpoch))).one()
    assert allocation.sealed and allocation.complete_payload["schema_version"] == 4
    return allocation


async def test_typed_sql_pinned_and_current_reads_preserve_application_and_build_purposes(capacity_session):
    _preparation, _execution, allocation, members = await sealed_owners(capacity_session)
    for member in members:
        parameters = {"allocation": allocation.allocation_epoch,
            "subject": member.configuration.subject_id,
            "incarnation": member.configuration.subject_incarnation}
        pinned = await capacity_session.scalar(text("""
            SELECT public.capacity_membership_pinned_subject(:allocation, :subject, :incarnation)
        """), parameters)
        assert pinned["configuration"] == member.configuration.model_dump(mode="json")
        assert pinned["acknowledgement"] == member.acknowledgement.model_dump(mode="json")
        assert pinned["purpose"] == (
            "application-worker" if member.purpose == "personal-application" else "personal-build-worker")
        query = text("SELECT public.capacity_membership_target_current(:allocation, :subject, :incarnation)")
        if member.purpose == "personal-application":
            assert await capacity_session.scalar(query, parameters) is True
        else:
            # A retained pending build service is not an executable worker.
            with pytest.raises(DBAPIError):
                async with capacity_session.begin_nested():
                    await capacity_session.scalar(query, parameters)


@pytest.mark.parametrize("change", ("equivocal", "update", "destroy", "candidate"))
async def test_typed_sql_currentness_isolates_owner_changes_without_swallowing_corruption(capacity_session, change):
    preparation, execution, allocation, members = await sealed_owners(capacity_session)
    selected, other = members[1], members[3]
    if change == "equivocal":
        with pytest.raises(ReportEquivocationError):
            await typed_management(preparation).ingest_demand_snapshot(capacity_session,
                report(selected.configuration).model_copy(update={"pending_unassigned": ()}), actor="owner-a")
    elif change == "candidate":
        await capacity_session.execute(update(CapacityCandidate).where(
            CapacityCandidate.subject_id == selected.configuration.subject_id).values(
                source_payload={"publication_sha256": "f" * 64}))
    else:
        original = application_request(preparation, execution, owner=88010, revision=1)
        await apply(capacity_session, transition(original, change, revision=4), key=121010)
    query = text("SELECT public.capacity_membership_target_current(:allocation, :subject, :incarnation)")
    parameters = {"allocation": allocation.allocation_epoch, "subject": selected.configuration.subject_id,
        "incarnation": selected.configuration.subject_incarnation}
    if change == "candidate":
        with pytest.raises(DBAPIError, match="candidate evidence changed"):
            async with capacity_session.begin_nested():
                await capacity_session.scalar(query, parameters)
        return
    assert await capacity_session.scalar(query, parameters) is False
    assert await capacity_session.scalar(query, {**parameters,
        "subject": other.configuration.subject_id,
        "incarnation": other.configuration.subject_incarnation}) is True
    if change == "update":
        from loom_capacity_manager.membership_launch_authority import (
            resolve_allocation_launch_subject,
        )
        from loom_capacity_manager.models import CapacityExecutionEpoch
        from tests.unit.test_capacity_executor_typed_launch_renderer import typed_context

        epoch = await capacity_session.get(CapacityExecutionEpoch, execution.execution_epoch)
        resolved = await resolve_allocation_launch_subject(capacity_session, epoch, allocation,
            subject_id=selected.configuration.subject_id, require_current=False)
        context = typed_context(purpose="application-worker", resolved=resolved,
            execution=ExecutionFenceV2.model_validate_json(json.dumps(allocation.complete_payload["execution"])))
        assert await capacity_session.scalar(text(
            "SELECT public.capacity_membership_cleanup_reporter(CAST(:binding AS jsonb), :reporter)"),
            {"binding": json.dumps(context.binding.model_dump(mode="json")),
             "reporter": selected.acknowledgement.reporter_incarnation}) is True


async def reservation_ready(capacity_session, *, owner_rate=8):
    preparation, execution, allocation, members = await sealed_owners(capacity_session, owner_rate=owner_rate)
    first = allocation.complete_payload["hypothetical_launch_rank"][0]
    executor = next(item for item in preparation.executors if item.pool_id == first["pool_id"])
    store = CapacityExecutionStore()
    common = dict(execution=execution, executor_id=executor.executor_id,
        executor_incarnation=executor.executor_incarnation, pool_id=executor.pool_id,
        pool_generation=executor.pool_generation, journal_sequence=0, journal_digest="0" * 64)
    await store.heartbeat_executor(capacity_session, ExecutableExecutorHeartbeatV2(**common, heartbeat_sequence=1))
    await store.ingest_typed_executor_inventory(capacity_session,
        ExecutableExecutorInventoryV3(**common, inventory_sequence=1), management=typed_management(preparation))
    authority = await capacity_session.get(CapacityAuthorityState, 1)
    assert authority.global_submission_rate_ceiling > 0
    for member in members:
        account = await capacity_session.scalar(select(CapacityAccountPolicy).where(
            CapacityAccountPolicy.configuration_epoch == execution.configuration_epoch,
            CapacityAccountPolicy.account_id == member.configuration.account_id))
        assert account is not None, member.configuration.account_id
        assert account.submission_rate_per_minute == owner_rate
    return store, executor, preparation, execution, members


@pytest.mark.parametrize("owner_rate", (0, 8))
async def test_typed_manager_can_create_real_application_reservation_without_build_admission(capacity_session, owner_rate):
    store, executor, _preparation, _execution, members = await reservation_ready(capacity_session, owner_rate=owner_rate)
    if owner_rate == 0:
        with pytest.raises(ExecutionConflictError, match="launch rate is exhausted"):
            await store.next_pool_work(capacity_session, executor)
        return
    proposal = await store.next_pool_work(capacity_session, executor)
    assert isinstance(proposal, ExecutableReservationProposalV2)
    assert proposal.subject_id in {item.configuration.subject_id for item in members
        if item.purpose == "personal-application"}


@pytest.mark.parametrize("tamper", (None, "artifact", "architecture", "launcher", "protocol", "attestation", "token", "cutover"))
async def test_typed_sql_managed_base_checks_pinned_installation_before_first_event(capacity_session, tamper):
    _legacy, preparation, _fleet, execution = await typed_sql_execution(capacity_session,
        managed_projection=development_projection(expected_configuration_epoch=1))
    management = typed_management(preparation)
    subject = preparation.managed_application_origins[0].configuration
    await management.ingest_demand_snapshot(capacity_session, report(subject), actor="owner-agent")
    allocation = await seal_allocation(capacity_session, management, execution)
    parameters = {"allocation": allocation.allocation_epoch, "subject": subject.subject_id,
        "incarnation": subject.subject_incarnation}
    query = text("SELECT public.capacity_membership_target_current(:allocation, :subject, :incarnation)")
    assert await capacity_session.scalar(query, parameters) is True
    if tamper is None:
        return
    if tamper == "token":
        statement = update(CapacityDemandReporter).where(
            CapacityDemandReporter.subject_id == subject.subject_id).values(token_sha256="f" * 64)
    elif tamper == "cutover":
        statement = update(CapacityDeploymentGeneration).where(
            CapacityDeploymentGeneration.subject_id == subject.subject_id).values(cutover_payload={})
    else:
        statement = update(CapacityCandidate).where(CapacityCandidate.subject_id == subject.subject_id).values(
            **{f"{tamper}_payload": {}})
    await capacity_session.execute(statement)
    with pytest.raises(DBAPIError, match="evidence changed"):
        async with capacity_session.begin_nested():
            await capacity_session.scalar(query, parameters)


@pytest.mark.parametrize("purpose", ("application", "build"))
@pytest.mark.parametrize("operation", ("update", "capacity"))
async def test_typed_sql_prefix_authenticates_lifecycle_after_rehashed_corruption(capacity_session, purpose, operation):
    _legacy, preparation, _fleet, execution = await typed_sql_execution(capacity_session)
    factory = application_request if purpose == "application" else build_request
    original = factory(preparation, execution)
    await apply(capacity_session, original)
    await apply(capacity_session, transition(original, operation, revision=1), key=100002)
    row = await capacity_session.scalar(select(CapacityPersonalMembershipEvent).where(
        CapacityPersonalMembershipEvent.execution_epoch == execution.execution_epoch,
        CapacityPersonalMembershipEvent.revision == 2))
    request, result = deepcopy(row.request_payload), deepcopy(row.result_payload)
    if operation == "update":
        # Builds may redeploy the same runtime; applications must advance source generation.
        request["command"]["projection"]["candidate_generation"] = 1
        result["member"]["configuration"]["candidate_generation"] = 1
    else:
        request["command"]["projection"]["demand_reporter_token_sha256"] = "f" * 64
    digest = hashlib.sha256(json.dumps(request, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode()).hexdigest()
    member = PersonalMembershipResultV2.model_validate_json(json.dumps(result)).member
    head = canonical_membership_event_head(actor=row.actor, execution_epoch=row.execution_epoch,
        idempotency_key=row.idempotency_key, operation_id=row.operation_id,
        previous_sha256=row.previous_sha256, request_digest=digest, request_payload=request,
        member=member, revision=row.revision)
    result["head_sha256"] = head
    await capacity_session.execute(text("ALTER TABLE capacity_personal_membership_events DISABLE TRIGGER USER"))
    await capacity_session.execute(update(CapacityPersonalMembershipEvent).where(
        CapacityPersonalMembershipEvent.id == row.id).values(request_payload=request,
        result_payload=result, request_digest=digest, head_sha256=head))
    await capacity_session.execute(text("ALTER TABLE capacity_personal_membership_events ENABLE TRIGGER USER"))
    query = text("SELECT public.capacity_membership_event_prefix(:epoch, 2)")
    if purpose == "build" and operation == "update":
        assert await capacity_session.scalar(query, {"epoch": execution.execution_epoch})
    else:
        with pytest.raises(DBAPIError, match="lifecycle"):
            async with capacity_session.begin_nested():
                await capacity_session.scalar(query, {"epoch": execution.execution_epoch})


async def typed_admission_plan(capacity_session):
    store, executor, preparation, execution, members = await reservation_ready(capacity_session)
    proposal = await store.next_pool_work(capacity_session, executor)
    assert isinstance(proposal, ExecutableReservationProposalV2)
    await store.accept_reservation(capacity_session, ExecutableReservationAcceptanceV2(
        execution=proposal.execution, tranche_id=proposal.tranche_id, proposal_digest=store.contract_digest(proposal),
        pool_id=executor.pool_id, pool_generation=executor.pool_generation,
        executor_id=executor.executor_id, executor_incarnation=executor.executor_incarnation, command_sequence=1))
    binding = await store.next_pool_work(capacity_session, executor)
    assert isinstance(binding, ExecutableIntentBindingV2)
    selected = next(member for member in members if member.configuration.subject_id == proposal.subject_id)
    bootstrap = ExecutableBootstrapProposalV2(binding=binding, command_sequence=2, proposal_epoch=1,
        bootstrap_sha256="7" * 64, expires_at=datetime.now(UTC) + timedelta(minutes=1))
    await store.propose_bootstrap(capacity_session, bootstrap)
    await store.acknowledge_bootstrap(capacity_session, ExecutableBootstrapAcknowledgementV2(
        binding=binding, proposal_epoch=1, proposal_digest=store.contract_digest(bootstrap),
        reporter_incarnation=selected.acknowledgement.reporter_incarnation, bootstrap_registration_epoch=1,
        bootstrap_evidence_sha256="8" * 64, protected_admission_sha256=selected.acknowledgement.protected_admission_sha256),
        actor="owner-agent", idempotency_key=UUID(int=122001))
    identity = dict(subject_id=selected.configuration.subject_id, subject_incarnation=selected.configuration.subject_incarnation,
        reporter_incarnation=selected.acknowledgement.reporter_incarnation)
    plan = await store.next_subject_admission_plan(capacity_session, **identity)
    assert isinstance(plan, ExecutableAdmissionPlanProposalV2)
    return store, executor, preparation, execution, selected, plan


async def test_typed_sql_admission_closure_accepts_membership_only_supersession(capacity_session):
    from tests.integration.test_capacity_manager_execution_store import (
        _insert_direct_admission_closure_acknowledgement,
        _manager_closed_admission_closure_acknowledgement,
    )

    store, _executor, preparation, execution, selected, plan = await typed_admission_plan(capacity_session)
    identity = dict(subject_id=selected.configuration.subject_id, subject_incarnation=selected.configuration.subject_incarnation,
        reporter_incarnation=selected.acknowledgement.reporter_incarnation)
    original = application_request(preparation, execution, owner=selected.owner_id.int, revision=selected.revision - 1)
    await apply(capacity_session, transition(original, "capacity", revision=4), key=122002)
    closure = await store.next_subject_admission_plan(capacity_session, **identity)
    assert isinstance(closure, ExecutableAdmissionPlanClosureV2)
    assert closure.close_reason == "allocation-superseded"
    acknowledgement = _manager_closed_admission_closure_acknowledgement(plan).model_copy(update={
        "closure_id": closure.closure_id, "close_reason": closure.close_reason})
    with pytest.raises(DBAPIError):
        async with capacity_session.begin_nested():
            await _insert_direct_admission_closure_acknowledgement(capacity_session,
                acknowledgement.model_copy(update={"reporter_incarnation": UUID(int=122003)}))
    await _insert_direct_admission_closure_acknowledgement(capacity_session, acknowledgement)


@pytest.mark.parametrize("retained", (False, True))
async def test_typed_execution_reader_downgrade_preserves_intents_and_restores_legacy_functions(capacity_session, retained):
    from alembic import command

    from tests.integration.test_capacity_build_membership_sql import _config

    if retained:
        store, executor, _preparation, _execution, _members = await reservation_ready(capacity_session)
        assert isinstance(await store.next_pool_work(capacity_session, executor), ExecutableReservationProposalV2)
        with pytest.raises(RuntimeError, match="retained typed intents"):
            async with capacity_session.begin_nested():
                connection = await capacity_session.connection()
                await connection.run_sync(lambda sync: command.downgrade(_config(sync), "capacity_0019"))
        assert await capacity_session.scalar(text("SELECT version_num FROM alembic_version")) == "capacity_0022"
    else:
        original = await capacity_session.scalar(text("SELECT pg_get_functiondef('public.capacity_0020_legacy_target_current(bigint,uuid,uuid)'::regprocedure)"))
        connection = await capacity_session.connection()
        await connection.run_sync(lambda sync: command.downgrade(_config(sync), "capacity_0019"))
        restored = await capacity_session.scalar(text("SELECT pg_get_functiondef('public.capacity_membership_target_current(bigint,uuid,uuid)'::regprocedure)"))
        assert restored == original.replace("FUNCTION public.capacity_0020_legacy_target_current(", "FUNCTION public.capacity_membership_target_current(")
        await connection.run_sync(lambda sync: command.upgrade(_config(sync), "capacity_0020"))
    permissions = (await capacity_session.execute(text("""
        SELECT proname, proconfig, EXISTS (SELECT 1 FROM aclexplode(coalesce(proacl, acldefault('f', proowner)))
          WHERE grantee=0 AND privilege_type='EXECUTE') AS public_execute
        FROM pg_proc WHERE pronamespace='public'::regnamespace
          AND (proname LIKE 'capacity_typed_membership_%' OR proname LIKE 'capacity_0020_%')
    """))).all()
    assert permissions
    assert all("search_path=pg_catalog" in row.proconfig and not row.public_execute for row in permissions)


@pytest.mark.parametrize("operation", ("capacity", "update", "destroy"))
async def test_typed_sql_managed_lifecycle_keeps_original_installation_and_supersedes_pin(capacity_session, operation):
    from tests.capacity_build_membership_fixtures import managed_application_request

    _legacy, preparation, _fleet, execution = await typed_sql_execution(capacity_session,
        managed_projection=development_projection(expected_configuration_epoch=1))
    management = typed_management(preparation)
    subject = preparation.managed_application_origins[0].configuration
    await management.ingest_demand_snapshot(capacity_session, report(subject), actor="owner-agent")
    allocation = await seal_allocation(capacity_session, management, execution)
    await apply(capacity_session, managed_application_request(preparation, execution, operation=operation))
    assert await capacity_session.scalar(text(
        "SELECT public.capacity_membership_target_current(:allocation, :subject, :incarnation)"),
        {"allocation": allocation.allocation_epoch, "subject": subject.subject_id,
         "incarnation": subject.subject_incarnation}) is False


@pytest.mark.parametrize("build", (False, True))
async def test_typed_sql_prefix_keeps_same_purpose_recreation_and_original_root(capacity_session, build):
    from tests.integration.test_capacity_typed_recreation_store import recreate

    _legacy, preparation, _fleet, execution = await typed_sql_execution(capacity_session)
    original = (build_request if build else application_request)(preparation, execution)
    created = await apply(capacity_session, original)
    disabled = transition(original, "destroy", revision=1)
    await apply(capacity_session, disabled, key=100002)
    successor = await apply(capacity_session, recreate(disabled, revision=2), key=100003)
    prefix = await capacity_session.scalar(text("SELECT public.capacity_membership_event_prefix(:epoch, 3)"),
        {"epoch": execution.execution_epoch})
    assert prefix["members"][str(created.member.configuration.subject_id)] == successor.member.model_dump(mode="json")
    assert successor.member.reincarnation.origin.subject_incarnation == created.member.configuration.subject_incarnation
