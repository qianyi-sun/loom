from __future__ import annotations

from datetime import UTC, datetime, timedelta
from uuid import UUID, uuid4

import pytest
from fastapi import FastAPI, HTTPException
from sqlalchemy import delete, func, select, update
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from starlette.requests import Request

from loom.auth import AuthContext
from loom.db.schema import (
    ServiceExecutionCommand,
    ServiceExecutionEvent,
    ServiceExecutionLease,
    ServiceExecutionLeaseHistory,
    ServiceExecutionTarget,
    Task,
    Team,
    Trial,
)
from loom.execution_contract import (
    NEBIUS_CPU_EXECUTION_CLASS_V1,
    ExecutionTargetV1,
    ImageMaterialization,
    IsolationLevel,
    NetworkAccess,
    VerifierTopology,
    WorkloadRequirementsV1,
)
from loom_control_plane.service_execution import (
    ServiceExecutionConflict,
    ServiceExecutionFenceError,
    acknowledge_execution_command,
    claim_execution_commands,
    enqueue_execution_transition,
    persist_execution_catalog,
    record_execution_event,
    reserve_trial_execution,
    set_execution_target_health,
    verify_trial_execution_fence,
)
from loom_llm_gateway.execution_attempt_dispatch import authorize_trial_execution_dispatch


@pytest.fixture(autouse=True)
async def _cleanup_service_execution_test_rows(postgres_url: str):  # type: ignore[no-untyped-def]
    """Keep the session Postgres fixture compatible with legacy broad cleanup fixtures."""

    yield
    engine = create_async_engine(postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    try:
        async with sessions() as session, session.begin():
            owned_trials = select(Trial.id).join(Team).where(Team.name.like("service-execution-%"))
            await session.execute(
                delete(ServiceExecutionLease).where(
                    ServiceExecutionLease.trial_id.in_(owned_trials)
                )
            )
            await session.execute(delete(Trial).where(Trial.id.in_(owned_trials)))
            await session.execute(delete(Task).where(Task.id.like("service-execution/%")))
            await session.execute(
                delete(ServiceExecutionTarget).where(
                    ServiceExecutionTarget.id.like("nebius-staging-%")
                )
            )
            await session.execute(delete(Team).where(Team.name.like("service-execution-%")))
    finally:
        await engine.dispose()


def _target(suffix: str) -> ExecutionTargetV1:
    return ExecutionTargetV1(
        target_id=f"nebius-staging-{suffix}",
        logical_pool_id="nebius-cpu",
        execution_class_id=NEBIUS_CPU_EXECUTION_CLASS_V1.class_id,
        environment="staging",
        provider="nebius",
        region="eu-north1",
        failure_domain=f"eu-north1-{suffix}",
        data_residency="eu",
        health_role="primary",
        health_check_id=f"nebius-staging-health-{suffix}",
        health_check_interval_seconds=10,
        health_stale_after_seconds=60,
    )


def _requirements() -> WorkloadRequirementsV1:
    return WorkloadRequirementsV1(
        operating_system="linux",
        cpu_architecture="x86_64",
        gpu_vendor="none",
        gpu_count=0,
        cpu_millis=1000,
        memory_mib=1024,
        ephemeral_storage_mib=2048,
        isolation_level=IsolationLevel.SANDBOXED_RUNTIME,
        network_access=NetworkAccess.GATEWAY_ONLY,
        image_materialization=ImageMaterialization.IMMUTABLE_OCI,
        image_ref="registry.example/loom/task@sha256:" + "a" * 64,
        sidecar_count=0,
        verifier_topology=VerifierTopology.IN_ATTEMPT,
        custom_dns=False,
        extra_hosts=False,
        tmpfs=True,
        privileged=False,
        host_path=False,
        host_network=False,
        nested_containers=False,
        host_devices=False,
        host_specialized=False,
    )


async def _seed_ready_trial(
    session: AsyncSession,
    *,
    now: datetime,
) -> tuple[UUID, ExecutionTargetV1]:
    suffix = uuid4().hex[:12]
    team_id = uuid4()
    trial_id = uuid4()
    task_id = f"service-execution/{suffix}"
    target = _target(suffix)
    session.add_all(
        (
            Team(id=team_id, name=f"service-execution-{suffix}"),
            Task(
                id=task_id,
                checksum="b" * 64,
                config={"schema_version": "1", "task": {"id": task_id}},
            ),
            Trial(
                id=trial_id,
                team_id=team_id,
                task_id=task_id,
                config={"agent": {"name": "test"}},
                requires_caps={"os": "linux", "cpu_arch": "x86_64"},
                state="queued",
                attempt_count=0,
            ),
        )
    )
    await persist_execution_catalog(
        session,
        execution_class=NEBIUS_CPU_EXECUTION_CLASS_V1,
        targets=(target,),
    )
    await set_execution_target_health(
        session,
        target_id=target.target_id,
        desired_state="active",
        observed_state="ready",
        health_status="healthy",
        observed_at=now,
    )
    return trial_id, target


async def _reserve(
    session: AsyncSession,
    *,
    trial_id: UUID,
    target: ExecutionTargetV1,
    now: datetime,
    request_id: UUID | None = None,
) -> ServiceExecutionLease:
    return await reserve_trial_execution(
        session,
        request_id=request_id or uuid4(),
        trial_id=trial_id,
        execution_class_id=NEBIUS_CPU_EXECUTION_CLASS_V1.class_id,
        target_id=target.target_id,
        requirements=_requirements(),
        deadline_at=now + timedelta(hours=1),
        now=now,
    )


async def test_reservation_persists_trial_lease_command_and_history_atomically(
    postgres_url: str,
) -> None:
    engine = create_async_engine(postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    now = datetime.now(UTC)
    request_id = uuid4()
    try:
        async with sessions() as session:
            trial_id, target = await _seed_ready_trial(session, now=now)
            lease = await _reserve(
                session,
                trial_id=trial_id,
                target=target,
                now=now,
                request_id=request_id,
            )
            await session.commit()
            lease_id = lease.id

        async with sessions() as session:
            trial = await session.get(Trial, trial_id)
            persisted = await session.get(ServiceExecutionLease, lease_id)
            commands = (
                (
                    await session.execute(
                        select(ServiceExecutionCommand).where(
                            ServiceExecutionCommand.lease_id == lease_id
                        )
                    )
                )
                .scalars()
                .all()
            )
            history_count = await session.scalar(
                select(func.count(ServiceExecutionLeaseHistory.id)).where(
                    ServiceExecutionLeaseHistory.lease_id == lease_id
                )
            )
            assert trial is not None
            assert (trial.state, trial.attempt_count) == ("claimed", 1)
            assert persisted is not None
            assert (persisted.attempt, persisted.generation) == (1, 1)
            assert persisted.last_event_ordinal == 0
            assert len(commands) == 1
            assert (commands[0].command_type, commands[0].state) == ("create", "pending")
            assert history_count == 1

            replay = await _reserve(
                session,
                trial_id=trial_id,
                target=target,
                now=now,
                request_id=request_id,
            )
            assert replay.id == lease_id
    finally:
        await engine.dispose()


async def test_deferred_outbox_constraint_rolls_back_trial_reservation(
    postgres_url: str,
) -> None:
    engine = create_async_engine(postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    now = datetime.now(UTC)
    try:
        async with sessions() as session:
            trial_id, target = await _seed_ready_trial(session, now=now)
            await session.commit()

        async with sessions() as session:
            lease = await _reserve(session, trial_id=trial_id, target=target, now=now)
            await session.execute(
                delete(ServiceExecutionCommand).where(ServiceExecutionCommand.lease_id == lease.id)
            )
            with pytest.raises(DBAPIError, match=r"lacks durable .* command"):
                await session.commit()
            await session.rollback()

        async with sessions() as session:
            trial = await session.get(Trial, trial_id)
            lease_count = await session.scalar(
                select(func.count(ServiceExecutionLease.id)).where(
                    ServiceExecutionLease.trial_id == trial_id
                )
            )
            assert trial is not None
            assert (trial.state, trial.attempt_count) == ("queued", 0)
            assert lease_count == 0
    finally:
        await engine.dispose()


async def test_command_redelivery_and_acknowledgement_are_replay_safe(
    postgres_url: str,
) -> None:
    engine = create_async_engine(postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    now = datetime.now(UTC)
    try:
        async with sessions() as session:
            trial_id, target = await _seed_ready_trial(session, now=now)
            lease = await _reserve(session, trial_id=trial_id, target=target, now=now)
            await session.commit()

        async with sessions() as session:
            first = await claim_execution_commands(
                session, consumer_id="actuator-a", limit=1, lease_seconds=5, now=now
            )
            await session.commit()
            assert len(first) == 1
            assert first[0].delivery_count == 1

        async with sessions() as session:
            second = await claim_execution_commands(
                session,
                consumer_id="actuator-b",
                limit=1,
                lease_seconds=5,
                now=now + timedelta(seconds=6),
            )
            assert len(second) == 1
            assert second[0].id == first[0].id
            assert second[0].delivery_count == 2
            command = await acknowledge_execution_command(
                session,
                command_id=second[0].id,
                consumer_id="actuator-b",
                acknowledgement={"provider_action": "created", "lease_id": str(lease.id)},
                now=now + timedelta(seconds=7),
            )
            await session.commit()
            assert command.state == "acknowledged"

        async with sessions() as session:
            replay = await acknowledge_execution_command(
                session,
                command_id=second[0].id,
                consumer_id="actuator-b",
                acknowledgement={"provider_action": "created", "lease_id": str(lease.id)},
            )
            assert replay.id == second[0].id
            with pytest.raises(ServiceExecutionConflict, match="replay changed"):
                await acknowledge_execution_command(
                    session,
                    command_id=second[0].id,
                    consumer_id="actuator-b",
                    acknowledgement={"provider_action": "different"},
                )
    finally:
        await engine.dispose()


async def test_events_are_replay_safe_and_lower_ordinals_do_not_regress_projection(
    postgres_url: str,
) -> None:
    engine = create_async_engine(postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    now = datetime.now(UTC)
    try:
        async with sessions() as session:
            trial_id, target = await _seed_ready_trial(session, now=now)
            lease = await _reserve(session, trial_id=trial_id, target=target, now=now)
            await session.commit()

        async with sessions() as session:
            await enqueue_execution_transition(
                session,
                lease_id=lease.id,
                expected_generation=1,
                desired_state="start",
                now=now + timedelta(seconds=1),
            )
            started, duplicate = await record_execution_event(
                session,
                lease_id=lease.id,
                generation=1,
                ordinal=2,
                event_kind="started",
                payload={"runtime": "pod"},
                observed_at=now + timedelta(seconds=2),
            )
            assert not duplicate
            await record_execution_event(
                session,
                lease_id=lease.id,
                generation=1,
                ordinal=1,
                event_kind="created",
                payload={"runtime": "pod"},
                observed_at=now + timedelta(seconds=1),
            )
            await session.commit()

        async with sessions() as session:
            persisted = await session.get(ServiceExecutionLease, lease.id)
            event_count = await session.scalar(
                select(func.count(ServiceExecutionEvent.id)).where(
                    ServiceExecutionEvent.lease_id == lease.id
                )
            )
            assert persisted is not None
            assert (persisted.observed_state, persisted.last_event_ordinal) == ("running", 2)
            assert event_count == 2
            replay, duplicate = await record_execution_event(
                session,
                lease_id=lease.id,
                generation=1,
                ordinal=2,
                event_kind="started",
                payload={"runtime": "pod"},
                observed_at=now + timedelta(seconds=2),
            )
            assert duplicate
            assert replay.id == started.id
            with pytest.raises(ServiceExecutionConflict, match="replay changed"):
                await record_execution_event(
                    session,
                    lease_id=lease.id,
                    generation=1,
                    ordinal=2,
                    event_kind="started",
                    payload={"runtime": "changed"},
                    observed_at=now + timedelta(seconds=2),
                )
    finally:
        await engine.dispose()


async def test_revocation_fences_old_generation_and_database_rejects_generation_skip(
    postgres_url: str,
) -> None:
    engine = create_async_engine(postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    now = datetime.now(UTC)
    try:
        async with sessions() as session:
            trial_id, target = await _seed_ready_trial(session, now=now)
            lease = await _reserve(session, trial_id=trial_id, target=target, now=now)
            await session.commit()

        async with sessions() as session:
            cancel = await enqueue_execution_transition(
                session,
                lease_id=lease.id,
                expected_generation=1,
                desired_state="cancel",
                now=now + timedelta(seconds=1),
            )
            await session.commit()
            assert cancel.generation == 2

        async with sessions() as session:
            for surface in (
                "gateway",
                "artifact",
                "trajectory",
                "usage",
                "heartbeat",
                "result",
            ):
                with pytest.raises(ServiceExecutionFenceError):
                    await verify_trial_execution_fence(
                        session,
                        trial_id=trial_id,
                        lease_id=lease.id,
                        generation=1,
                        surface=surface,
                    )
            with pytest.raises(ServiceExecutionFenceError):
                await verify_trial_execution_fence(
                    session,
                    trial_id=trial_id,
                    lease_id=lease.id,
                    generation=2,
                    surface="heartbeat",
                )
            terminal = await verify_trial_execution_fence(
                session,
                trial_id=trial_id,
                lease_id=lease.id,
                generation=2,
                surface="cancelled",
                allow_terminal_event=True,
            )
            assert terminal is not None

            with pytest.raises(DBAPIError, match="advance monotonically by one"):
                await session.execute(
                    update(ServiceExecutionLease)
                    .where(ServiceExecutionLease.id == lease.id)
                    .values(generation=4)
                )
            await session.rollback()

        async with sessions() as session:
            persisted = await session.get(ServiceExecutionLease, lease.id)
            assert persisted is not None
            assert persisted.generation == 2
            assert persisted.revoked_at is not None
            assert persisted.cleanup_requested_at == now + timedelta(seconds=1)
            assert persisted.cleanup_deadline_at == now + timedelta(minutes=5, seconds=1)
    finally:
        await engine.dispose()


async def test_retry_creates_a_new_attempt_and_finalization_is_idempotent(
    postgres_url: str,
) -> None:
    engine = create_async_engine(postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    now = datetime.now(UTC)
    try:
        async with sessions() as session:
            trial_id, target = await _seed_ready_trial(session, now=now)
            first = await _reserve(session, trial_id=trial_id, target=target, now=now)
            await session.commit()

        async with sessions() as session:
            await enqueue_execution_transition(
                session,
                lease_id=first.id,
                expected_generation=1,
                desired_state="retry",
                now=now + timedelta(seconds=1),
            )
            await session.commit()

        async with sessions() as session:
            with pytest.raises(ServiceExecutionConflict, match="cleanup is not complete"):
                await _reserve(
                    session,
                    trial_id=trial_id,
                    target=target,
                    now=now + timedelta(seconds=2),
                )
            await session.rollback()

        async with sessions() as session:
            await record_execution_event(
                session,
                lease_id=first.id,
                generation=2,
                ordinal=1,
                event_kind="deleted",
                payload={"resource_release": "complete"},
                observed_at=now + timedelta(seconds=2),
            )
            await session.commit()

        async with sessions() as session:
            second = await _reserve(
                session,
                trial_id=trial_id,
                target=target,
                now=now + timedelta(seconds=3),
            )
            await session.commit()
            assert (second.attempt, second.generation) == (2, 1)
            assert second.id != first.id

        async with sessions() as session:
            await enqueue_execution_transition(
                session,
                lease_id=second.id,
                expected_generation=1,
                desired_state="start",
                now=now + timedelta(seconds=3),
            )
            await enqueue_execution_transition(
                session,
                lease_id=second.id,
                expected_generation=1,
                desired_state="finalize",
                now=now + timedelta(seconds=4),
            )
            await session.commit()

        final_payload = {"trial_state": "succeeded", "result": {"reward": 1.0}}
        async with sessions() as session:
            event, duplicate = await record_execution_event(
                session,
                lease_id=second.id,
                generation=1,
                ordinal=1,
                event_kind="finalized",
                payload=final_payload,
                observed_at=now + timedelta(seconds=5),
            )
            assert not duplicate
            await session.commit()

        async with sessions() as session:
            replay, duplicate = await record_execution_event(
                session,
                lease_id=second.id,
                generation=1,
                ordinal=1,
                event_kind="finalized",
                payload=final_payload,
                observed_at=now + timedelta(seconds=5),
            )
            assert duplicate
            assert replay.id == event.id
            trial = await session.get(Trial, trial_id)
            assert trial is not None
            assert trial.state == "succeeded"
            assert trial.result == {"reward": 1.0}
            assert trial.finished_at == now + timedelta(seconds=5)
    finally:
        await engine.dispose()


async def test_gateway_dispatch_is_rejected_immediately_after_generation_revocation(
    postgres_url: str,
) -> None:
    engine = create_async_engine(postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    now = datetime.now(UTC)
    try:
        async with sessions() as session:
            trial_id, target = await _seed_ready_trial(session, now=now)
            lease = await _reserve(session, trial_id=trial_id, target=target, now=now)
            await record_execution_event(
                session,
                lease_id=lease.id,
                generation=1,
                ordinal=1,
                event_kind="created",
                payload={"provider_action": "created"},
                observed_at=now + timedelta(seconds=1),
            )
            await session.commit()

        ctx = AuthContext(
            token_hash=b"",
            type="step_session",
            scopes=["llm:call"],
            team_id=lease.team_id,
            expires_at=now + timedelta(minutes=10),
            trial_id=trial_id,
            step_id="main",
            service_execution_lease_id=lease.id,
            service_execution_generation=1,
        )
        async with sessions() as session:
            await authorize_trial_execution_dispatch(session, ctx)
            await enqueue_execution_transition(
                session,
                lease_id=lease.id,
                expected_generation=1,
                desired_state="cancel",
                now=now + timedelta(seconds=2),
            )
            await session.commit()

        async with sessions() as session:
            with pytest.raises(
                HTTPException,
                match="service execution dispatch forbidden",
            ) as exc_info:
                await authorize_trial_execution_dispatch(session, ctx)
            assert exc_info.value.status_code == 403
    finally:
        await engine.dispose()


async def test_invalid_state_edges_event_bounds_and_payload_bounds_fail_closed(
    postgres_url: str,
) -> None:
    engine = create_async_engine(postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    now = datetime.now(UTC)
    try:
        async with sessions() as session:
            trial_id, target = await _seed_ready_trial(session, now=now)
            lease = await _reserve(session, trial_id=trial_id, target=target, now=now)
            await session.commit()

        async with sessions() as session:
            with pytest.raises(ServiceExecutionConflict, match="create -> finalize"):
                await enqueue_execution_transition(
                    session,
                    lease_id=lease.id,
                    expected_generation=1,
                    desired_state="finalize",
                )
            with pytest.raises(ServiceExecutionConflict, match="invalid for desired state"):
                await record_execution_event(
                    session,
                    lease_id=lease.id,
                    generation=1,
                    ordinal=1,
                    event_kind="started",
                    payload={},
                    observed_at=now,
                )
            with pytest.raises(ServiceExecutionConflict, match="between 1 and 10000"):
                await record_execution_event(
                    session,
                    lease_id=lease.id,
                    generation=1,
                    ordinal=10_001,
                    event_kind="created",
                    payload={},
                    observed_at=now,
                )

        async with sessions() as session:
            with pytest.raises(DBAPIError, match="payload_bound"):
                await enqueue_execution_transition(
                    session,
                    lease_id=lease.id,
                    expected_generation=1,
                    desired_state="start",
                    payload={"oversized": "x" * 70_000},
                )
            await session.rollback()
    finally:
        await engine.dispose()


async def test_operator_projection_is_team_isolated(
    postgres_url: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from loom_control_plane.routes import service_executions

    engine = create_async_engine(postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    now = datetime.now(UTC)
    try:
        async with sessions() as session:
            trial_id, target = await _seed_ready_trial(session, now=now)
            lease = await _reserve(session, trial_id=trial_id, target=target, now=now)
            await session.commit()

        app = FastAPI()
        app.state.session_factory = sessions
        request = Request({"type": "http", "app": app, "headers": []})

        async def wrong_team_auth(request: Request, authorization: str | None) -> AuthContext:
            del request, authorization
            return AuthContext(
                token_hash=b"x",
                type="team",
                scopes=["read:own"],
                team_id=uuid4(),
                expires_at=None,
            )

        monkeypatch.setattr(service_executions, "_auth", wrong_team_auth)
        with pytest.raises(HTTPException) as hidden:
            await service_executions.get_trial_execution(trial_id, request, None)
        assert (hidden.value.status_code, hidden.value.detail) == (404, "trial not found")

        async def owning_team_auth(request: Request, authorization: str | None) -> AuthContext:
            del request, authorization
            return AuthContext(
                token_hash=b"x",
                type="team",
                scopes=["read:own"],
                team_id=lease.team_id,
                expires_at=None,
            )

        monkeypatch.setattr(service_executions, "_auth", owning_team_auth)
        projection = await service_executions.get_trial_execution(trial_id, request, None)
        assert projection["trial_id"] == str(trial_id)
        assert projection["execution"]["lease_id"] == str(lease.id)
        assert len(projection["commands"]) == 1
        assert projection["events"] == []
        assert projection["history"]
    finally:
        await engine.dispose()
