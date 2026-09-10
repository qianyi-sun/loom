"""New-owner demand traverses the real migrated executable admission ledger."""

from datetime import UTC, datetime, timedelta
from uuid import UUID

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from loom_capacity_manager.executable_contracts import (
    ExecutableAdmissionPlanClosureAcknowledgementV2,
    ExecutableAdmissionPlanClosureV2,
    ExecutableAdmissionPlanProposalV2,
    ExecutableBootstrapAcknowledgementV2,
    ExecutableBootstrapProposalV2,
    ExecutableIntentBindingV2,
    ExecutableIntentCloseV2,
    ExecutableLaunchPermitV2,
    ExecutablePartialReleaseV2,
    ExecutablePermitConsumptionV2,
    ExecutableProtectedReleaseV2,
    ExecutableReservationAcceptanceV2,
    ExecutableReservationProposalV2,
)
from loom_capacity_manager.execution_store import CapacityExecutionStore
from loom_capacity_manager.membership_store import CapacityMembershipStore
from loom_capacity_manager.models import CapacityAllocationEpoch, CapacityExecutableIntent
from loom_capacity_manager.reconciler import reconcile_shadow_once
from loom_capacity_manager.store import ExecutionConflictError
from tests.capacity_execution_fixtures import executor_binding
from tests.capacity_fixtures import demand_snapshot, pool_observation
from tests.integration.test_capacity_manager_execution_store import (
    _admission_acknowledgement,
    _heartbeat,
)
from tests.integration.test_capacity_membership import DELEGATE, _active_v3, _request


async def _personal_plan(  # type: ignore[no-untyped-def]
    session: AsyncSession,
    *,
    base_pending: bool = False,
    reporter_token_sha256: str | None = None,
):
    fixture, active = await _active_v3(session, owner_submission_rate_per_minute=8)
    request = _request(active)
    if reporter_token_sha256 is not None:
        request = request.model_copy(
            update={
                "projection": request.projection.model_copy(
                    update={"demand_reporter_token_sha256": reporter_token_sha256},
                )
            }
        )
    await CapacityMembershipStore(fixture.store).apply(
        session, request, actor=DELEGATE, idempotency_key=UUID(int=22400)
    )
    base_report = demand_snapshot(
        pending_attempt_ids=(str(UUID(int=22499)),) if base_pending else ()
    )
    base_report = base_report.model_copy(
        update={
            "pending_unassigned": tuple(
                item.model_copy(update={"eligible_pool_ids": ("gb10",)})
                for item in base_report.pending_unassigned
            ),
        }
    )
    await fixture.store.ingest_demand_snapshot(session, base_report, actor="development")
    projection = request.projection
    report = demand_snapshot(
        subject_id=projection.subject_id,
        subject_incarnation=projection.subject_incarnation,
        configuration_generation=projection.configuration_generation,
        deployment_generation=projection.deployment_generation,
        reporter_incarnation=projection.demand_reporter_incarnation,
        pending_attempt_ids=(str(UUID(int=22401)),),
    )
    report = report.model_copy(
        update={
            "pending_unassigned": tuple(
                item.model_copy(update={"eligible_pool_ids": ("gb10",)})
                for item in report.pending_unassigned
            ),
        }
    )
    await fixture.store.ingest_demand_snapshot(session, report, actor="bob-capacity-agent")
    for pool in ("gb10", "oldlab"):
        await fixture.store.ingest_pool_observation(
            session, pool_observation(pool_id=pool), actor=f"{pool}-reporter"
        )
    sessions = async_sessionmaker(
        bind=session.bind, expire_on_commit=False, join_transaction_mode="create_savepoint"
    )
    result = await reconcile_shadow_once(sessions, fixture.writer, store=fixture.store)
    assert result.status == "committed"
    return fixture, active, request


async def _personal_bootstrap(  # type: ignore[no-untyped-def]
    session: AsyncSession,
    *,
    reporter_token_sha256: str | None = None,
):
    fixture, active, request = await _personal_plan(
        session, reporter_token_sha256=reporter_token_sha256
    )
    store = CapacityExecutionStore()
    executor = executor_binding("gb10")
    await _heartbeat(store, session, active, pool_id="gb10")
    proposal = await store.next_pool_work(session, executor)
    assert isinstance(proposal, ExecutableReservationProposalV2)
    assert proposal.subject_id == request.projection.subject_id
    await store.accept_reservation(
        session,
        ExecutableReservationAcceptanceV2(
            execution=proposal.execution,
            tranche_id=proposal.tranche_id,
            proposal_digest=store.contract_digest(proposal),
            pool_id=executor.pool_id,
            pool_generation=executor.pool_generation,
            executor_id=executor.executor_id,
            executor_incarnation=executor.executor_incarnation,
            command_sequence=1,
        ),
    )
    binding = await store.next_pool_work(session, executor)
    assert isinstance(binding, ExecutableIntentBindingV2)
    assert binding.account_id == f"dev-owner-{request.projection.owner_id.hex}"
    bootstrap = ExecutableBootstrapProposalV2(
        binding=binding,
        command_sequence=2,
        proposal_epoch=1,
        bootstrap_sha256="7" * 64,
        expires_at=datetime.now(UTC) + timedelta(minutes=1),
    )
    await store.propose_bootstrap(session, bootstrap)
    return fixture, active, request, store, executor, bootstrap


async def test_cleanup_only_poll_preserves_proposals_without_admitting_new_work(capacity_session):
    _fixture, active, _request = await _personal_plan(capacity_session, base_pending=True)
    store = CapacityExecutionStore()
    executor = executor_binding("gb10")
    await _heartbeat(store, capacity_session, active, pool_id="gb10")
    assert await store.next_pool_work(capacity_session, executor, cleanup_only=True) is None
    assert (await capacity_session.scalars(select(CapacityExecutableIntent))).all() == []
    proposal = await store.next_pool_work(capacity_session, executor)
    assert isinstance(proposal, ExecutableReservationProposalV2)
    for _ in range(2):
        assert await store.next_pool_work(capacity_session, executor, cleanup_only=True) is None
    assert await store.next_pool_work(capacity_session, executor) == proposal
    await store.accept_reservation(capacity_session, ExecutableReservationAcceptanceV2(
        execution=proposal.execution, tranche_id=proposal.tranche_id,
        proposal_digest=store.contract_digest(proposal), pool_id=executor.pool_id,
        pool_generation=executor.pool_generation, executor_id=executor.executor_id,
        executor_incarnation=executor.executor_incarnation, command_sequence=1))
    # Even an accepted intent must not cause the pressure poll to issue a new
    # bootstrap/proposal/permit. Its reservation stays intact for a later retry.
    assert await store.next_pool_work(capacity_session, executor, cleanup_only=True) is None
    rows = (await capacity_session.scalars(select(CapacityExecutableIntent))).all()
    assert rows and all(row.state == "accepted" for row in rows)


def _bootstrap_ack(request, bootstrap):  # type: ignore[no-untyped-def]
    return ExecutableBootstrapAcknowledgementV2(
        binding=bootstrap.binding,
        proposal_epoch=bootstrap.proposal_epoch,
        proposal_digest=CapacityExecutionStore.contract_digest(bootstrap),
        reporter_incarnation=request.acknowledgement.reporter_incarnation,
        bootstrap_registration_epoch=1,
        bootstrap_evidence_sha256="8" * 64,
        protected_admission_sha256=request.acknowledgement.protected_admission_sha256,
    )


async def test_new_owner_reaches_protected_admission_and_launch_permit(
    capacity_session: AsyncSession,
) -> None:
    _, _, request, store, executor, bootstrap = await _personal_bootstrap(capacity_session)
    identity = dict(
        subject_id=request.projection.subject_id,
        subject_incarnation=request.projection.subject_incarnation,
        reporter_incarnation=request.acknowledgement.reporter_incarnation,
    )
    assert await store.next_subject_bootstrap(capacity_session, **identity) == bootstrap
    await store.acknowledge_bootstrap(
        capacity_session,
        _bootstrap_ack(request, bootstrap),
        actor="bob-capacity-agent",
        idempotency_key=UUID(int=22402),
    )
    plan = await store.next_subject_admission_plan(capacity_session, **identity)
    assert isinstance(plan, ExecutableAdmissionPlanProposalV2)
    await store.acknowledge_admission_plan(
        capacity_session,
        _admission_acknowledgement(plan),
        actor="bob-capacity-agent",
        idempotency_key=UUID(int=22403),
    )
    permit = await store.next_pool_work(capacity_session, executor)
    assert isinstance(permit, ExecutableLaunchPermitV2)
    assert permit.binding.subject_id == request.projection.subject_id


async def _supersede(session, fixture, active, request, kind):  # type: ignore[no-untyped-def]
    changed = request.projection.model_copy(
        update={
            "operation_kind": kind,
            "operation_epoch": 2,
            "configuration_generation": 2,
            "operation_id": UUID(int=22411),
            "max_slots": 1,
        }
        | (
            {
                "candidate_generation": 2,
                "deployment_generation": 2,
                "demand_reporter_incarnation": UUID(int=22412),
                "demand_reporter_token_sha256": "2" * 64,
            }
            if kind == "update"
            else {}
        )
    )
    return await CapacityMembershipStore(fixture.store).apply(
        session,
        _request(active, changed, expected_revision=1),
        actor=DELEGATE,
        idempotency_key=UUID(int=22413),
    )


@pytest.mark.parametrize("kind", ("capacity", "update", "destroy"))
async def test_membership_only_supersession_allows_bootstrap_cleanup_but_no_admission(
    capacity_session: AsyncSession,
    kind: str,
) -> None:
    fixture, active, request, store, executor, bootstrap = await _personal_bootstrap(
        capacity_session
    )
    await _supersede(capacity_session, fixture, active, request, kind)
    # No successor allocation was committed. The exact original reporter can still
    # finish bootstrap-before-release, but it must not produce a new admission plan.
    await store.acknowledge_bootstrap(
        capacity_session,
        _bootstrap_ack(request, bootstrap),
        actor="bob-capacity-agent",
        idempotency_key=UUID(int=22414),
    )
    assert (
        await store.next_subject_admission_plan(
            capacity_session,
            subject_id=request.projection.subject_id,
            subject_incarnation=request.projection.subject_incarnation,
            reporter_incarnation=request.acknowledgement.reporter_incarnation,
        )
        is None
    )
    intent = await capacity_session.scalar(
        select(CapacityExecutableIntent).where(
            CapacityExecutableIntent.intent_id == bootstrap.binding.intent_id,
        )
    )
    assert intent is not None and intent.state == "bootstrap-acknowledged"
    close = await store.next_pool_work(capacity_session, executor)
    assert isinstance(close, ExecutableIntentCloseV2)
    await store.begin_intent_close(capacity_session, close)
    assert intent.state == "closing" and intent.released_at is None
    await store.acknowledge_protected_release(
        capacity_session,
        ExecutableProtectedReleaseV2(
            binding=bootstrap.binding,
            reporter_incarnation=request.acknowledgement.reporter_incarnation,
            bootstrap_registration_epoch=1,
            protected_registration_epoch=2,
            bootstrap_revoked=True,
            protected_release_sha256="b" * 64,
        ),
        actor="bob-capacity-agent",
        idempotency_key=UUID(int=22415),
    )
    release = await store.next_pool_work(capacity_session, executor)
    assert isinstance(release, ExecutablePartialReleaseV2)
    await store.release_shapes(capacity_session, release)
    assert intent.state == "released" and intent.released_at is not None


async def test_unproposed_superseded_owner_does_not_block_unchanged_owner(
    capacity_session: AsyncSession,
) -> None:
    fixture, active, request = await _personal_plan(capacity_session, base_pending=True)
    allocation = await capacity_session.scalar(select(CapacityAllocationEpoch))
    assert allocation is not None
    ranks = allocation.complete_payload["hypothetical_launch_rank"]
    assert len(ranks) == 2
    assert ranks[0]["subject_id"] == str(request.projection.subject_id)
    await _supersede(capacity_session, fixture, active, request, "destroy")
    store = CapacityExecutionStore()
    await _heartbeat(store, capacity_session, active, pool_id="gb10")
    work = await store.next_pool_work(capacity_session, executor_binding("gb10"))
    assert isinstance(work, ExecutableReservationProposalV2)
    assert work.subject_id == fixture.request.subject_acknowledgements[0].subject_id
    executor = executor_binding("gb10")
    await store.accept_reservation(
        capacity_session,
        ExecutableReservationAcceptanceV2(
            execution=work.execution,
            tranche_id=work.tranche_id,
            proposal_digest=store.contract_digest(work),
            pool_id=executor.pool_id,
            pool_generation=executor.pool_generation,
            executor_id=executor.executor_id,
            executor_incarnation=executor.executor_incarnation,
            command_sequence=1,
        ),
    )
    binding = await store.next_pool_work(capacity_session, executor)
    assert isinstance(binding, ExecutableIntentBindingV2)
    bootstrap = ExecutableBootstrapProposalV2(
        binding=binding,
        command_sequence=2,
        proposal_epoch=1,
        bootstrap_sha256="7" * 64,
        expires_at=datetime.now(UTC) + timedelta(minutes=1),
    )
    await store.propose_bootstrap(capacity_session, bootstrap)
    base_ack = fixture.request.subject_acknowledgements[0]
    await store.acknowledge_bootstrap(
        capacity_session,
        _bootstrap_ack(request.model_copy(update={"acknowledgement": base_ack}), bootstrap),
        actor="development",
        idempotency_key=UUID(int=22431),
    )
    plan = await store.next_subject_admission_plan(
        capacity_session,
        subject_id=base_ack.subject_id,
        subject_incarnation=base_ack.subject_incarnation,
        reporter_incarnation=base_ack.reporter_incarnation,
    )
    assert isinstance(plan, ExecutableAdmissionPlanProposalV2)
    await store.acknowledge_admission_plan(
        capacity_session,
        _admission_acknowledgement(plan),
        actor="development",
        idempotency_key=UUID(int=22432),
    )
    permit = await store.next_pool_work(capacity_session, executor)
    assert isinstance(permit, ExecutableLaunchPermitV2)
    assert permit.binding.subject_id == base_ack.subject_id


@pytest.mark.parametrize("kind", ("capacity", "update", "destroy"))
async def test_delivered_plan_becomes_durable_closure_without_next_allocation(
    capacity_session: AsyncSession,
    kind: str,
) -> None:
    fixture, active, request, store, _, bootstrap = await _personal_bootstrap(capacity_session)
    await store.acknowledge_bootstrap(
        capacity_session,
        _bootstrap_ack(request, bootstrap),
        actor="bob-capacity-agent",
        idempotency_key=UUID(int=22421),
    )
    identity = dict(
        subject_id=request.projection.subject_id,
        subject_incarnation=request.projection.subject_incarnation,
        reporter_incarnation=request.acknowledgement.reporter_incarnation,
    )
    plan = await store.next_subject_admission_plan(capacity_session, **identity)
    assert isinstance(plan, ExecutableAdmissionPlanProposalV2)
    await _supersede(capacity_session, fixture, active, request, kind)
    closure = await store.next_subject_admission_plan(capacity_session, **identity)
    assert isinstance(closure, ExecutableAdmissionPlanClosureV2)
    assert closure.proposal == plan and closure.close_reason == "allocation-superseded"
    with pytest.raises(ExecutionConflictError):
        await store.acknowledge_admission_plan(
            capacity_session,
            _admission_acknowledgement(plan),
            actor="bob-capacity-agent",
            idempotency_key=UUID(int=22422),
        )
    await store.acknowledge_admission_plan_closure(
        capacity_session,
        ExecutableAdmissionPlanClosureAcknowledgementV2(
            closure_id=closure.closure_id,
            proposal_id=plan.proposal_id,
            proposal_digest=store.contract_digest(plan),
            plan_id=plan.plan_id,
            admission_incarnation=plan.admission_incarnation,
            **identity,
            protected_admission_sha256=plan.protected_admission_sha256,
            close_reason=closure.close_reason,
            disposition_kind="never-converged",
            disposition_digest="e" * 64,
        ),
        actor="bob-capacity-agent",
        idempotency_key=UUID(int=22423),
    )
    assert await store.next_subject_admission_plan(capacity_session, **identity) is None


@pytest.mark.parametrize("kind", ("capacity", "update", "destroy"))
async def test_superseded_cached_permit_is_neither_reissued_nor_consumed(
    capacity_session: AsyncSession,
    kind: str,
) -> None:
    fixture, active, request, store, executor, bootstrap = await _personal_bootstrap(
        capacity_session
    )
    await store.acknowledge_bootstrap(
        capacity_session,
        _bootstrap_ack(request, bootstrap),
        actor="bob-capacity-agent",
        idempotency_key=UUID(int=22441),
    )
    plan = await store.next_subject_admission_plan(
        capacity_session,
        subject_id=request.projection.subject_id,
        subject_incarnation=request.projection.subject_incarnation,
        reporter_incarnation=request.acknowledgement.reporter_incarnation,
    )
    assert isinstance(plan, ExecutableAdmissionPlanProposalV2)
    await store.acknowledge_admission_plan(
        capacity_session,
        _admission_acknowledgement(plan),
        actor="bob-capacity-agent",
        idempotency_key=UUID(int=22442),
    )
    permit = await store.next_pool_work(capacity_session, executor)
    assert isinstance(permit, ExecutableLaunchPermitV2)
    await _supersede(capacity_session, fixture, active, request, kind)
    assert isinstance(
        await store.next_pool_work(capacity_session, executor), ExecutableIntentCloseV2
    )
    with pytest.raises(ExecutionConflictError):
        await store.consume_launch_permit(
            capacity_session,
            ExecutablePermitConsumptionV2(
                permit_id=permit.permit_id,
                permit_digest=store.contract_digest(permit),
                binding=permit.binding,
                command_sequence=3,
            ),
        )
