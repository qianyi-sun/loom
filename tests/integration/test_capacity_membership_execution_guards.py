"""Installed SQL authority checks for allocation-pinned personal membership."""

import copy
from pathlib import Path
from typing import Any
from uuid import UUID

import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import create_engine, select, text, update
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from loom_capacity_manager.executable_contracts import (
    ExecutableAdmissionPlanClosureV2,
    ExecutableAdmissionPlanProposalV2,
    ExecutableLaunchPermitV2,
    ExecutableReservationProposalV2,
)
from loom_capacity_manager.execution_store import CapacityExecutionStore
from loom_capacity_manager.membership_store import CapacityMembershipStore
from loom_capacity_manager.models import (
    CapacityAllocationEpoch,
    CapacityCandidate,
    CapacityDemandReporter,
    CapacityDeploymentGeneration,
    CapacityExecutableIntent,
    CapacitySubject,
    CapacityWorkerProfile,
)
from loom_capacity_manager.reconciler import reconcile_shadow_once
from tests.capacity_execution_fixtures import executor_binding
from tests.capacity_fixtures import (
    SUBJECT_ID,
    SUBJECT_INCARNATION,
    demand_snapshot,
    pool_observation,
)
from tests.integration.test_capacity_manager_execution_store import (
    _active_plan,
    _admission_acknowledgement,
    _heartbeat,
    _insert_direct_admission_acknowledgement,
    _insert_direct_admission_closure_acknowledgement,
    _insert_direct_bootstrap_acknowledgement,
    _manager_closed_admission_closure_acknowledgement,
)
from tests.integration.test_capacity_membership import (
    BOB_INCARNATION,
    BOB_SUBJECT_ID,
    DELEGATE,
    _active_v3,
    _projection,
    _request,
)
from tests.integration.test_capacity_membership_admission import (
    _bootstrap_ack,
    _personal_bootstrap,
    _personal_plan,
    _supersede,
)

GUARDS = (
    "capacity_executable_bootstrap_ack_insert_guard",
    "capacity_executable_admission_proposal_insert_guard",
    "capacity_executable_admission_ack_insert_guard",
    "capacity_executable_intent_protected_bootstrap_guard",
    "capacity_executable_admission_closure_ack_insert_guard",
    "capacity_executable_protected_release_insert_guard",
)


@pytest.mark.parametrize(
    "tamper",
    (
        "candidate-publication",
        "candidate-artifact",
        "candidate-architecture",
        "candidate-launcher",
        "candidate-protocol",
        "reporter-token",
        "reporter-configuration",
        "reporter-deployment",
        "deployment-cutover",
        "deployment-readiness",
        "worker-profile",
    ),
)
async def test_sql_currentness_and_admission_reject_changed_retained_evidence(
    capacity_session: AsyncSession,
    tamper: str,
) -> None:
    _, _, request, store, _, bootstrap = await _personal_bootstrap(capacity_session)
    await store.acknowledge_bootstrap(
        capacity_session,
        _bootstrap_ack(request, bootstrap),
        actor="bob-capacity-agent",
        idempotency_key=UUID(int=24000),
    )
    plan = await store.next_subject_admission_plan(
        capacity_session,
        subject_id=request.projection.subject_id,
        subject_incarnation=request.projection.subject_incarnation,
        reporter_incarnation=request.acknowledgement.reporter_incarnation,
    )
    assert isinstance(plan, ExecutableAdmissionPlanProposalV2)
    mutations = {
        "candidate-publication": (
            CapacityCandidate,
            {"source_payload": {"publication_sha256": "1" * 64}},
        ),
        "candidate-artifact": (
            CapacityCandidate,
            {"artifact_payload": {"candidate_sha256": "1" * 64}},
        ),
        "candidate-architecture": (CapacityCandidate, {"architecture_payload": {}}),
        "candidate-launcher": (CapacityCandidate, {"launcher_payload": {}}),
        "candidate-protocol": (CapacityCandidate, {"protocol_payload": {}}),
        "reporter-token": (CapacityDemandReporter, {"token_sha256": "1" * 64}),
        "reporter-configuration": (CapacityDemandReporter, {"configuration_generation": 99}),
        "reporter-deployment": (CapacityDemandReporter, {"deployment_generation": 2}),
        "deployment-cutover": (
            CapacityDeploymentGeneration,
            {"cutover_payload": {"protected_admission_sha256": "1" * 64}},
        ),
        "deployment-readiness": (CapacityDeploymentGeneration, {"readiness_state": "pending"}),
        "worker-profile": (CapacityWorkerProfile, {"profile_digest": "1" * 64}),
    }
    model, values = mutations[tamper]
    await capacity_session.execute(
        update(model).where(model.subject_id == request.projection.subject_id).values(**values)
    )
    with pytest.raises(DBAPIError, match=r"membership current.*evidence changed"):
        async with capacity_session.begin_nested():
            await _resolve(
                capacity_session, bootstrap.binding.execution.allocation_epoch, current=True
            )
    with pytest.raises(
        DBAPIError, match=r"membership current.*evidence changed|proposal payload is not exact"
    ):
        async with capacity_session.begin_nested():
            await _insert_direct_admission_acknowledgement(
                capacity_session, _admission_acknowledgement(plan)
            )


@pytest.mark.parametrize("fenced", (False, True))
@pytest.mark.parametrize("generation", ("configuration_generation", "deployment_generation"))
async def test_sql_cleanup_rejects_unrecorded_reporter_generations(
    capacity_session: AsyncSession,
    fenced: bool,
    generation: str,
) -> None:
    fixture, active, request, _, _, bootstrap = await _personal_bootstrap(capacity_session)
    if fenced:
        await _supersede(capacity_session, fixture, active, request, "update")
    await capacity_session.execute(
        update(CapacityDemandReporter)
        .where(
            CapacityDemandReporter.reporter_incarnation
            == request.acknowledgement.reporter_incarnation,
        )
        .values(**{generation: 99})
    )
    with pytest.raises(DBAPIError, match="reporter changed"):
        async with capacity_session.begin_nested():
            await _insert_direct_bootstrap_acknowledgement(
                capacity_session, _bootstrap_ack(request, bootstrap)
            )


@pytest.mark.parametrize("fenced", (False, True))
async def test_sql_cleanup_accepts_recorded_generation_later_than_allocation_pin(
    capacity_session: AsyncSession,
    fenced: bool,
) -> None:
    fixture, active, request, _, _, bootstrap = await _personal_bootstrap(capacity_session)
    await _supersede(capacity_session, fixture, active, request, "capacity")
    if fenced:
        changed = request.projection.model_copy(
            update={
                "operation_kind": "update",
                "operation_epoch": 3,
                "configuration_generation": 3,
                "operation_id": UUID(int=24010),
                "candidate_generation": 2,
                "deployment_generation": 2,
                "demand_reporter_incarnation": UUID(int=24011),
                "demand_reporter_token_sha256": "3" * 64,
            }
        )
        await CapacityMembershipStore(fixture.store).apply(
            capacity_session,
            _request(active, changed, expected_revision=2),
            actor=DELEGATE,
            idempotency_key=UUID(int=24012),
        )
    reporter = await capacity_session.scalar(
        select(CapacityDemandReporter).where(
            CapacityDemandReporter.reporter_incarnation
            == request.acknowledgement.reporter_incarnation,
        )
    )
    assert reporter is not None and reporter.configuration_generation == 2
    assert reporter.state == ("fenced" if fenced else "current")
    await _insert_direct_bootstrap_acknowledgement(
        capacity_session, _bootstrap_ack(request, bootstrap)
    )


async def test_sql_supersession_does_not_hide_corrupt_current_retained_evidence(
    capacity_session: AsyncSession,
) -> None:
    fixture, active, request, _, _, bootstrap = await _personal_bootstrap(capacity_session)
    await _supersede(capacity_session, fixture, active, request, "update")
    await capacity_session.execute(
        update(CapacityDemandReporter)
        .where(
            CapacityDemandReporter.reporter_incarnation == UUID(int=22412),
        )
        .values(token_sha256="4" * 64)
    )
    with pytest.raises(DBAPIError, match="membership current retained evidence changed"):
        async with capacity_session.begin_nested():
            await _resolve(
                capacity_session, bootstrap.binding.execution.allocation_epoch, current=True
            )


@pytest.mark.parametrize("edge", ("insert", "accept"))
async def test_direct_sql_pristine_proposal_and_acceptance_reject_superseded_target(
    capacity_session: AsyncSession,
    edge: str,
) -> None:
    fixture, active, request = await _personal_plan(capacity_session)
    store = CapacityExecutionStore()
    await _heartbeat(store, capacity_session, active, pool_id="gb10")
    work = await store.next_pool_work(capacity_session, executor_binding("gb10"))
    assert isinstance(work, ExecutableReservationProposalV2)
    await _supersede(capacity_session, fixture, active, request, "capacity")
    with pytest.raises(DBAPIError, match="target superseded"):
        async with capacity_session.begin_nested():
            if edge == "insert":
                await capacity_session.execute(
                    text(
                        "INSERT INTO public.capacity_executable_intents SELECT * FROM public.capacity_executable_intents"
                    )
                )
            else:
                await capacity_session.execute(
                    update(CapacityExecutableIntent)
                    .where(CapacityExecutableIntent.tranche_id == work.tranche_id)
                    .values(state="accepted", accepted_at=text("clock_timestamp()"))
                )


@pytest.mark.parametrize("edge", ("renew", "consume"))
async def test_direct_sql_cached_permit_cannot_increase_superseded_target(
    capacity_session: AsyncSession,
    edge: str,
) -> None:
    fixture, active, request, store, executor, bootstrap = await _personal_bootstrap(
        capacity_session
    )
    await store.acknowledge_bootstrap(
        capacity_session,
        _bootstrap_ack(request, bootstrap),
        actor="bob-capacity-agent",
        idempotency_key=UUID(int=22630),
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
        idempotency_key=UUID(int=22631),
    )
    permit = await store.next_pool_work(capacity_session, executor)
    assert isinstance(permit, ExecutableLaunchPermitV2)
    await _supersede(capacity_session, fixture, active, request, "capacity")
    if edge == "renew":
        renewed = permit.model_copy(
            update={"permit_epoch": permit.permit_epoch + 1, "permit_id": UUID(int=22632)}
        )
        statement = update(CapacityExecutableIntent).values(
            permit_id=renewed.permit_id,
            permit_epoch=renewed.permit_epoch,
            permit_digest=store.contract_digest(renewed),
            permit_payload=renewed.model_dump(mode="json"),
        )
    else:
        statement = update(CapacityExecutableIntent).values(
            state="submitting-unknown", permit_consumed_at=text("clock_timestamp()")
        )
    with pytest.raises(DBAPIError, match="target superseded"):
        async with capacity_session.begin_nested():
            await capacity_session.execute(
                statement.where(CapacityExecutableIntent.intent_id == bootstrap.binding.intent_id)
            )


def _migration_config() -> Config:
    root = Path(__file__).resolve().parents[2]
    config = Config(str(root / "capacity_migrations" / "alembic.ini"))
    config.set_main_option("script_location", str(root / "capacity_migrations"))
    return config


def test_membership_guard_migration_restores_exact_installed_functions_and_acl(
    isolated_capacity_postgres_url: str,
) -> None:
    engine = create_engine(isolated_capacity_postgres_url)
    config = _migration_config()
    query = text(
        "SELECT proname, pg_get_functiondef(oid), proacl::text FROM pg_proc WHERE pronamespace = 'public'::regnamespace AND proname = ANY(:names) ORDER BY proname"
    )
    try:
        with engine.begin() as connection:
            config.attributes["connection"] = connection
            command.downgrade(config, "capacity_0016")
            before = connection.execute(query, {"names": list(GUARDS)}).all()
            assert len(before) == 6
            command.upgrade(config, "capacity_0017")
            after = connection.execute(query, {"names": list(GUARDS)}).all()
            assert all(old[1] != new[1] for old, new in zip(before, after, strict=True))
            assert all(old[2] == new[2] for old, new in zip(before, after, strict=True))
            helpers = connection.execute(
                text(
                    "SELECT proname, prosecdef, proconfig, EXISTS (SELECT 1 FROM aclexplode(coalesce(proacl, acldefault('f', proowner))) WHERE grantee = 0 AND privilege_type = 'EXECUTE') FROM pg_proc WHERE pronamespace = 'public'::regnamespace AND (proname LIKE 'capacity_membership_%' OR proname LIKE 'capacity_0017_prior_guard_%')"
                )
            ).all()
            assert len(helpers) == 13
            assert all(
                not row[1] and "search_path=pg_catalog" in row[2] and not row[3] for row in helpers
            )
            command.downgrade(config, "capacity_0016")
            assert connection.execute(query, {"names": list(GUARDS)}).all() == before
            assert (
                connection.scalar(
                    text(
                        "SELECT to_regprocedure('public.capacity_membership_pinned_subject(bigint,uuid,uuid)')"
                    )
                )
                is None
            )
            command.upgrade(config, "capacity_0017")
    finally:
        engine.dispose()


def test_membership_guard_upgrade_refuses_drift_without_partial_installation(
    isolated_capacity_postgres_url: str,
) -> None:
    engine = create_engine(isolated_capacity_postgres_url)
    config = _migration_config()
    try:
        with engine.begin() as connection:
            config.attributes["connection"] = connection
            command.downgrade(config, "capacity_0016")
            definition = connection.scalar(
                text(
                    "SELECT pg_get_functiondef('public.capacity_executable_bootstrap_ack_insert_guard()'::regprocedure)"
                )
            )
            assert definition is not None
            connection.execute(
                text(
                    definition.replace(
                        "AND reporter.state = 'current'", "AND reporter.state IN ('current')"
                    )
                )
            )
        with engine.connect() as connection:
            config.attributes["connection"] = connection
            with pytest.raises(RuntimeError, match="migration drift"):
                with connection.begin():
                    command.upgrade(config, "capacity_0017")
            assert (
                connection.scalar(text("SELECT version_num FROM alembic_version"))
                == "capacity_0016"
            )
            assert (
                connection.scalar(
                    text(
                        "SELECT to_regprocedure('public.capacity_membership_pinned_subject(bigint,uuid,uuid)')"
                    )
                )
                is None
            )
    finally:
        engine.dispose()


async def test_membership_guard_downgrade_refuses_retained_v3_allocations(
    capacity_session: AsyncSession,
) -> None:
    await _sealed_v3(capacity_session)
    config = _migration_config()

    def downgrade(connection):  # type: ignore[no-untyped-def]
        config.attributes["connection"] = connection
        command.downgrade(config, "capacity_0016")

    with pytest.raises(RuntimeError, match="delegated allocations exist"):
        async with capacity_session.begin_nested():
            await (await capacity_session.connection()).run_sync(downgrade)
    assert (
        await capacity_session.scalar(text("SELECT version_num FROM alembic_version"))
        == "capacity_0021"
    )


@pytest.mark.parametrize("kind", ("capacity", "update", "destroy"))
async def test_direct_sql_old_admission_ack_fails_after_membership_only_supersession(
    capacity_session: AsyncSession,
    kind: str,
) -> None:
    fixture, active, request, store, _, bootstrap = await _personal_bootstrap(capacity_session)
    await store.acknowledge_bootstrap(
        capacity_session,
        _bootstrap_ack(request, bootstrap),
        actor="bob-capacity-agent",
        idempotency_key=UUID(int=22600),
    )
    plan = await store.next_subject_admission_plan(
        capacity_session,
        subject_id=request.projection.subject_id,
        subject_incarnation=request.projection.subject_incarnation,
        reporter_incarnation=request.acknowledgement.reporter_incarnation,
    )
    assert isinstance(plan, ExecutableAdmissionPlanProposalV2)
    acknowledgement = _admission_acknowledgement(plan)
    await _supersede(capacity_session, fixture, active, request, kind)
    with pytest.raises(DBAPIError, match=r"target superseded|reporter changed"):
        async with capacity_session.begin_nested():
            await _insert_direct_admission_acknowledgement(capacity_session, acknowledgement)


async def test_direct_sql_admission_proposal_rejects_capacity_only_supersession(
    capacity_session: AsyncSession,
) -> None:
    fixture, active, request, store, _, bootstrap = await _personal_bootstrap(capacity_session)
    await store.acknowledge_bootstrap(
        capacity_session,
        _bootstrap_ack(request, bootstrap),
        actor="bob-capacity-agent",
        idempotency_key=UUID(int=22640),
    )
    await _supersede(capacity_session, fixture, active, request, "capacity")
    # Reinsert exact immutable proposal bytes: BEFORE INSERT must reject target
    # authority before the duplicate-key constraint, without Python mediation.
    with pytest.raises(DBAPIError, match="protected authority changed"):
        async with capacity_session.begin_nested():
            await capacity_session.execute(
                text(
                    "INSERT INTO public.capacity_executable_admission_proposals SELECT * FROM public.capacity_executable_admission_proposals"
                )
            )


@pytest.mark.parametrize("kind", ("capacity", "update", "destroy"))
async def test_direct_sql_launch_ready_fails_after_membership_only_supersession(
    capacity_session: AsyncSession,
    kind: str,
) -> None:
    fixture, active, request, store, _, bootstrap = await _personal_bootstrap(capacity_session)
    await store.acknowledge_bootstrap(
        capacity_session,
        _bootstrap_ack(request, bootstrap),
        actor="bob-capacity-agent",
        idempotency_key=UUID(int=22601),
    )
    plan = await store.next_subject_admission_plan(
        capacity_session,
        subject_id=request.projection.subject_id,
        subject_incarnation=request.projection.subject_incarnation,
        reporter_incarnation=request.acknowledgement.reporter_incarnation,
    )
    assert isinstance(plan, ExecutableAdmissionPlanProposalV2)
    # Install exact acknowledgement while current, without Python's state transition.
    await _insert_direct_admission_acknowledgement(
        capacity_session, _admission_acknowledgement(plan)
    )
    await _supersede(capacity_session, fixture, active, request, kind)
    with pytest.raises(DBAPIError, match=r"target superseded|admission acknowledgement"):
        async with capacity_session.begin_nested():
            await capacity_session.execute(
                update(CapacityExecutableIntent)
                .where(CapacityExecutableIntent.intent_id == bootstrap.binding.intent_id)
                .values(state="launch-ready", launch_ready_at=text("clock_timestamp()"))
            )


@pytest.mark.parametrize("kind", ("capacity", "update", "destroy"))
async def test_direct_sql_bootstrap_allows_only_exact_retained_reporter(
    capacity_session: AsyncSession,
    kind: str,
) -> None:
    fixture, active, request, _, _, bootstrap = await _personal_bootstrap(capacity_session)
    changed = await _supersede(capacity_session, fixture, active, request, kind)
    acknowledgement = _bootstrap_ack(request, bootstrap)
    wrong = acknowledgement.model_copy(update={"reporter_incarnation": UUID(int=22605)})
    with pytest.raises(DBAPIError, match="reporter changed"):
        async with capacity_session.begin_nested():
            await _insert_direct_bootstrap_acknowledgement(capacity_session, wrong)
    if kind == "update":
        successor = acknowledgement.model_copy(
            update={"reporter_incarnation": changed.member.acknowledgement.reporter_incarnation}
        )
        with pytest.raises(DBAPIError, match="reporter changed"):
            async with capacity_session.begin_nested():
                await _insert_direct_bootstrap_acknowledgement(capacity_session, successor)
    await _insert_direct_bootstrap_acknowledgement(capacity_session, acknowledgement)


@pytest.mark.parametrize("state", ("fenced", "equivocal", "destroy-then-fenced"))
async def test_direct_sql_bootstrap_rejects_reporter_without_legitimate_rollover(
    capacity_session: AsyncSession,
    state: str,
) -> None:
    fixture, active, request, _, _, bootstrap = await _personal_bootstrap(capacity_session)
    if state == "destroy-then-fenced":
        await _supersede(capacity_session, fixture, active, request, "destroy")
        state = "fenced"
    await capacity_session.execute(
        update(CapacityDemandReporter)
        .where(
            CapacityDemandReporter.reporter_incarnation
            == request.acknowledgement.reporter_incarnation
        )
        .values(state=state)
    )
    with pytest.raises(DBAPIError, match="reporter changed"):
        async with capacity_session.begin_nested():
            await _insert_direct_bootstrap_acknowledgement(
                capacity_session, _bootstrap_ack(request, bootstrap)
            )


async def test_direct_sql_historical_rotated_reporter_survives_successor_destroy(
    capacity_session: AsyncSession,
) -> None:
    fixture, active, request, _, _, bootstrap = await _personal_bootstrap(capacity_session)
    await _supersede(capacity_session, fixture, active, request, "update")
    disabled = request.projection.model_copy(
        update={
            "operation_kind": "destroy",
            "operation_epoch": 3,
            "configuration_generation": 3,
            "operation_id": UUID(int=22641),
            "candidate_generation": 2,
            "deployment_generation": 2,
            "demand_reporter_incarnation": UUID(int=22412),
            "demand_reporter_token_sha256": "2" * 64,
        }
    )
    await CapacityMembershipStore(fixture.store).apply(
        capacity_session,
        _request(active, disabled, expected_revision=2),
        actor=DELEGATE,
        idempotency_key=UUID(int=22642),
    )
    await _insert_direct_bootstrap_acknowledgement(
        capacity_session, _bootstrap_ack(request, bootstrap)
    )


async def test_sql_two_recreations_preserve_exact_old_pin_and_authenticated_latest_chain(
    capacity_session: AsyncSession,
) -> None:
    fixture, active, _, row = await _sealed_v3(capacity_session)
    original = await _resolve(capacity_session, row.allocation_epoch)
    projection = _projection()
    for generation in (2, 4):
        disabled = projection.model_copy(
            update={
                "operation_kind": "destroy",
                "operation_id": UUID(int=22700 + generation),
                "operation_epoch": generation,
                "configuration_generation": generation,
            }
        )
        await CapacityMembershipStore(fixture.store).apply(
            capacity_session,
            _request(active, disabled, expected_revision=generation - 1),
            actor=DELEGATE,
            idempotency_key=UUID(int=22710 + generation),
        )
        projection = projection.model_copy(
            update={
                "operation_kind": "create",
                "operation_id": UUID(int=22700 + generation + 1),
                "operation_epoch": generation + 1,
                "configuration_generation": generation + 1,
                "subject_incarnation": UUID(int=22720 + generation),
                "demand_reporter_incarnation": UUID(int=22730 + generation),
                "demand_reporter_token_sha256": str(generation) * 64,
            }
        )
        recreated = await CapacityMembershipStore(fixture.store).apply(
            capacity_session,
            _request(active, projection, expected_revision=generation),
            actor=DELEGATE,
            idempotency_key=UUID(int=22710 + generation + 1),
        )
        prefix = await capacity_session.scalar(
            text("SELECT public.capacity_membership_event_prefix(:epoch, :revision)"),
            {"epoch": active.execution_epoch, "revision": generation + 1},
        )
        assert prefix["members"][str(BOB_SUBJECT_ID)] == recreated.member.model_dump(mode="json")
        assert await _resolve(capacity_session, row.allocation_epoch) == original
        assert await _resolve(capacity_session, row.allocation_epoch, current=True) is False


@pytest.mark.parametrize("kind", ("capacity", "update", "destroy"))
async def test_direct_sql_closure_accepts_membership_only_supersession(
    capacity_session: AsyncSession,
    kind: str,
) -> None:
    fixture, active, request, store, _, bootstrap = await _personal_bootstrap(capacity_session)
    await store.acknowledge_bootstrap(
        capacity_session,
        _bootstrap_ack(request, bootstrap),
        actor="bob-capacity-agent",
        idempotency_key=UUID(int=22610),
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
    assert closure.close_reason == "allocation-superseded"
    acknowledgement = _manager_closed_admission_closure_acknowledgement(plan).model_copy(
        update={
            "closure_id": closure.closure_id,
            "close_reason": closure.close_reason,
        }
    )
    wrong = acknowledgement.model_copy(update={"reporter_incarnation": UUID(int=22611)})
    with pytest.raises(DBAPIError, match="closure acknowledgement binding changed"):
        async with capacity_session.begin_nested():
            await _insert_direct_admission_closure_acknowledgement(capacity_session, wrong)
    await _insert_direct_admission_closure_acknowledgement(capacity_session, acknowledgement)


async def test_sql_unrelated_later_member_preserves_pinned_member_currentness(
    capacity_session: AsyncSession,
) -> None:
    fixture, active, _, row = await _sealed_v3(capacity_session)
    original = await _resolve(capacity_session, row.allocation_epoch)
    projection = _projection(
        subject_id=UUID(int=22621),
        subject_incarnation=UUID(int=22622),
        reporter_incarnation=UUID(int=22623),
        owner_id=UUID(int=22624),
        operation_id=UUID(int=22625),
        environment_name="charlie",
    ).model_copy(update={"demand_reporter_token_sha256": "d" * 64})
    await CapacityMembershipStore(fixture.store).apply(
        capacity_session,
        _request(active, projection, expected_revision=1),
        actor=DELEGATE,
        idempotency_key=UUID(int=22626),
    )
    assert await _resolve(capacity_session, row.allocation_epoch) == original
    assert await _resolve(capacity_session, row.allocation_epoch, current=True) is True


async def _sealed_v3(session: AsyncSession, *, member: bool = True):  # type: ignore[no-untyped-def]
    fixture, active = await _active_v3(session)
    admitted = None
    if member:
        admitted = await CapacityMembershipStore(fixture.store).apply(
            session, _request(active), actor=DELEGATE, idempotency_key=UUID(int=22500)
        )
    await fixture.store.ingest_demand_snapshot(
        session, demand_snapshot(pending_attempt_ids=(str(UUID(int=22501)),)), actor="development"
    )
    for pool in ("gb10", "oldlab"):
        await fixture.store.ingest_pool_observation(
            session, pool_observation(pool_id=pool), actor=f"{pool}-reporter"
        )
    sessions = async_sessionmaker(
        bind=session.bind, expire_on_commit=False, join_transaction_mode="create_savepoint"
    )
    result = await reconcile_shadow_once(sessions, fixture.writer, store=fixture.store)
    assert result.status == "committed"
    row = (await session.scalars(select(CapacityAllocationEpoch))).one()
    return fixture, active, admitted, row


async def _resolve(
    session: AsyncSession,
    allocation: int,
    subject: UUID = BOB_SUBJECT_ID,
    incarnation: UUID = BOB_INCARNATION,
    *,
    current: bool = False,
) -> Any:
    function = "target_current" if current else "pinned_subject"
    return (
        await session.execute(
            text(
                f"SELECT public.capacity_membership_{function}(:allocation, :subject, :incarnation)"
            ),
            {"allocation": allocation, "subject": subject, "incarnation": incarnation},
        )
    ).scalar_one()


async def _corrupt_allocation(
    session: AsyncSession, row: CapacityAllocationEpoch, payload: dict[str, Any]
) -> None:
    """Inject impossible sealed evidence to exercise fail-closed SQL readers."""
    guards = (
        "capacity_executable_allocation_seal_guard",
        "capacity_allocation_epoch_binding_guard",
    )
    for guard in guards:
        await session.execute(
            text(f"ALTER TABLE public.capacity_allocation_epochs DISABLE TRIGGER {guard}")
        )
    await session.execute(
        update(CapacityAllocationEpoch)
        .where(CapacityAllocationEpoch.allocation_epoch == row.allocation_epoch)
        .values(complete_payload=payload)
    )
    for guard in reversed(guards):
        await session.execute(
            text(f"ALTER TABLE public.capacity_allocation_epochs ENABLE TRIGGER {guard}")
        )


@pytest.mark.parametrize("member", (False, True))
async def test_sql_pinned_v3_resolves_exact_base_and_delegated_member(
    capacity_session: AsyncSession,
    member: bool,
) -> None:
    _, _, admitted, row = await _sealed_v3(capacity_session, member=member)
    base = await _resolve(capacity_session, row.allocation_epoch, SUBJECT_ID, SUBJECT_INCARNATION)
    assert base["delegated"] is True
    assert row.complete_payload["membership"]["revision"] == int(member)
    if admitted is not None:
        resolved = await _resolve(capacity_session, row.allocation_epoch)
        assert resolved["configuration"] == admitted.member.configuration.model_dump(mode="json")
        assert resolved["acknowledgement"] == admitted.member.acknowledgement.model_dump(
            mode="json"
        )
        assert await _resolve(capacity_session, row.allocation_epoch, current=True) is True


@pytest.mark.parametrize("operation", ("capacity", "update", "destroy"))
async def test_sql_target_supersession_does_not_fence_unchanged_base_in_same_allocation(
    capacity_session: AsyncSession,
    operation: str,
) -> None:
    fixture, active, admitted, row = await _sealed_v3(capacity_session)
    original = await _resolve(capacity_session, row.allocation_epoch)
    assert admitted is not None
    projection = _projection(
        operation_kind=operation,
        operation_epoch=2,
        operation_id=UUID(int=22502),
        reporter_incarnation=(
            UUID(int=22503) if operation == "update" else _projection().demand_reporter_incarnation
        ),
        deployment_generation=2 if operation == "update" else 1,
        max_slots=0 if operation == "destroy" else 1,
    )
    if operation != "update":
        projection = projection.model_copy(
            update={
                "demand_reporter_token_sha256": _projection().demand_reporter_token_sha256,
            }
        )
    await CapacityMembershipStore(fixture.store).apply(
        capacity_session,
        _request(active, projection, expected_revision=1),
        actor=DELEGATE,
        idempotency_key=UUID(int=22504),
    )
    assert await _resolve(capacity_session, row.allocation_epoch) == original
    assert await _resolve(capacity_session, row.allocation_epoch, current=True) is False
    assert (
        await _resolve(
            capacity_session, row.allocation_epoch, SUBJECT_ID, SUBJECT_INCARNATION, current=True
        )
        is True
    )
    assert (
        await capacity_session.scalars(select(CapacityAllocationEpoch))
    ).one().allocation_epoch == row.allocation_epoch


@pytest.mark.parametrize(
    "corruption",
    (
        "missing",
        "v2",
        "head",
        "omitted",
        "revision",
        "string-revision",
        "fractional-revision",
        "configuration",
        "acknowledgement",
        "namespace",
    ),
)
async def test_sql_pinned_v3_rejects_tampered_or_missing_snapshot(
    capacity_session: AsyncSession,
    corruption: str,
) -> None:
    _, _, _, row = await _sealed_v3(capacity_session)
    payload = copy.deepcopy(row.complete_payload)
    snapshot = payload["membership"]
    if corruption == "missing":
        del payload["membership"]
    elif corruption == "v2":
        payload["schema_version"] = 2
        del payload["membership"]
    elif corruption == "head":
        snapshot["head_sha256"] = "0" * 64
    elif corruption == "omitted":
        snapshot["members"] = []
    elif corruption == "revision":
        snapshot["revision"] = 2
    elif corruption == "string-revision":
        snapshot["revision"] = "1"
    elif corruption == "fractional-revision":
        snapshot["revision"] = 1.0
    elif corruption == "namespace":
        snapshot["namespace_id"] = str(UUID(int=22509))
    elif corruption == "configuration":
        snapshot["members"][0]["configuration"]["max_slots"] += 1
    else:
        snapshot["members"][0]["acknowledgement"]["protected_admission_sha256"] = "0" * 64
    await _corrupt_allocation(capacity_session, row, payload)
    with pytest.raises(DBAPIError, match="membership"):
        async with capacity_session.begin_nested():
            await _resolve(capacity_session, row.allocation_epoch)


@pytest.mark.parametrize(
    "field",
    (
        "writer_epoch",
        "configuration_epoch",
        "authority_incarnation",
        "trusted_fleet_release_sha256",
    ),
)
async def test_sql_pinned_v3_rejects_tampered_execution_fence(
    capacity_session: AsyncSession, field: str
) -> None:
    _, _, _, row = await _sealed_v3(capacity_session)
    payload = copy.deepcopy(row.complete_payload)
    original = payload["execution"][field]
    payload["execution"][field] = (
        original + 1
        if isinstance(original, int)
        else (str(UUID(int=22800)) if field == "authority_incarnation" else "0" * 64)
    )
    await _corrupt_allocation(capacity_session, row, payload)
    with pytest.raises(DBAPIError, match="membership allocation authority changed"):
        async with capacity_session.begin_nested():
            await _resolve(capacity_session, row.allocation_epoch)


async def test_sql_pinned_subject_preserves_v2_base_authority(
    capacity_session: AsyncSession,
) -> None:
    _, allocation_epoch = await _active_plan(capacity_session)
    subject = (await capacity_session.scalars(select(CapacitySubject))).first()
    assert subject is not None
    parameters = {
        "allocation": allocation_epoch,
        "subject": subject.subject_id,
        "incarnation": subject.subject_incarnation,
    }
    resolved = (
        await capacity_session.execute(
            text(
                "SELECT public.capacity_membership_pinned_subject("
                ":allocation, :subject, :incarnation)"
            ),
            parameters,
        )
    ).scalar_one()
    assert resolved["configuration"] == subject.payload
    assert resolved["acknowledgement"]["subject_id"] == str(subject.subject_id)
    assert resolved["delegated"] is False
    assert (
        await capacity_session.execute(
            text(
                "SELECT public.capacity_membership_target_current("
                ":allocation, :subject, :incarnation)"
            ),
            parameters,
        )
    ).scalar_one() is True
