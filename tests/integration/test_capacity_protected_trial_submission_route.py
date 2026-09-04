"""Production trial submission through the protected runtime boundary."""

from __future__ import annotations

import asyncio
import hashlib
from datetime import UTC, datetime
from pathlib import Path
from uuid import UUID, uuid4

import pytest
from alembic import command
from alembic.config import Config as AlembicConfig
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, insert, text
from sqlalchemy.engine import make_url
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from loom import model_switch_store
from loom.db.schema import Task, Team, TeamQuota, Token, User
from loom_capacity_agent.contracts import AgentRegistrationV1, AtomicTrialSubmissionV1
from loom_capacity_agent.store import (
    CapacityAgentStore,
    capture_lifecycle_demand_observation,
)
from loom_capacity_guard.contracts import (
    GuardFenceV1,
    SealedRequirementsV1,
    canonical_digest,
)
from loom_capacity_guard.store import CapacityGuardStore
from loom_control_plane.app import create_app
from loom_control_plane.config import ControlPlaneSettings
from loom_control_plane.protected_worker_session import (
    ProtectedTrialSubmissionError,
    ProtectedWorkerSessionStore,
)
from loom_control_plane.routes import trials as trial_routes


def _value(database: dict[str, object], key: str) -> str:
    value = database[key]
    assert isinstance(value, str)
    return value


async def _initialize_guard(database: dict[str, object]) -> AgentRegistrationV1:
    fence = GuardFenceV1(
        environment_id="staging",
        subject_id=uuid4(),
        subject_incarnation=uuid4(),
        authority_incarnation=uuid4(),
        reporter_incarnation=uuid4(),
        deployment_generation=7,
        configuration_generation=11,
        candidate_digest="a" * 64,
    )
    registration = AgentRegistrationV1(
        environment_id=fence.environment_id,
        subject_id=fence.subject_id,
        subject_incarnation=fence.subject_incarnation,
        authority_incarnation=fence.authority_incarnation,
        agent_incarnation=uuid4(),
        reporter_incarnation=fence.reporter_incarnation,
        candidate_digest=fence.candidate_digest,
        deployment_generation=fence.deployment_generation,
        configuration_generation=fence.configuration_generation,
    )
    engine = create_async_engine(
        make_url(_value(database, "migrator_url")),
        isolation_level="SERIALIZABLE",
    )
    factory = async_sessionmaker(engine, expire_on_commit=False)
    quoted_owner = engine.sync_engine.dialect.identifier_preparer.quote(
        _value(database, "owner_role")
    )
    try:
        async with factory() as session, session.begin():
            await session.execute(text(f"SET LOCAL ROLE {quoted_owner}"))
            await CapacityGuardStore(
                session,
                expected_owner_role=_value(database, "owner_role"),
            ).initialize_disabled_authority(fence)
            await CapacityAgentStore(
                session,
                expected_owner_role=_value(database, "owner_role"),
                expected_agent_role=_value(database, "agent_role"),
            ).register_agent(registration)
    finally:
        await engine.dispose()
    return registration


def _write_runtime_url(path: Path, database: dict[str, object]) -> None:
    path.write_text(_value(database, "runtime_url"), encoding="ascii")
    path.chmod(0o600)


@pytest.mark.asyncio
async def test_runtime_submission_is_hidden_until_guard_verified_readiness(
    capacity_guard_database: dict[str, object],
) -> None:
    registration = await _initialize_guard(capacity_guard_database)
    team_id = uuid4()
    task_id = f"protected-readiness-{uuid4().hex}"
    admin_engine = create_engine(_value(capacity_guard_database, "admin_url"))
    try:
        with admin_engine.begin() as connection:
            connection.execute(insert(Team).values(id=team_id, name=f"ready-{team_id}"))
            connection.execute(insert(TeamQuota).values(team_id=team_id))
            connection.execute(
                insert(Task).values(
                    id=task_id,
                    checksum="0" * 64,
                    config={
                        "schema_version": "1",
                        "task": {"id": task_id, "name": task_id},
                        "environment": {
                            "os": "linux",
                            "docker_image": "alpine",
                            "baseline_network_policy": {"kind": "gateway-only"},
                            "network_policies_supported": ["gateway-only"],
                        },
                        "agent": {"name": "oracle"},
                        "verifier": {"name": "pytest"},
                        "steps": [{"name": "main"}],
                    },
                )
            )

        requirements = SealedRequirementsV1(
            os="linux",
            cpu_arch="x86_64",
            gpu_vendor="none",
            network_policies=("gateway-only",),
        )
        submission = AtomicTrialSubmissionV1(
            **registration.model_dump(mode="python"),
            trial_id=uuid4(),
            protected_attempt_id=uuid4(),
            execution_generation=registration.deployment_generation,
            requirements=requirements,
            requirements_digest=canonical_digest(requirements),
            team_id=team_id,
            task_id=task_id,
            config={"agent_name": "oracle", "agent_model": None},
            submit_priority=100,
            idempotency_key=f"protected-ready-{uuid4().hex}",
        )
        public_requires_caps = {
            "backend": "docker",
            "os": "linux",
            "cpu_arch": "x86_64",
            "gpu_vendor": "none",
            "network_policies": ["gateway-only"],
            "terminus2_model_switch": False,
        }
        runtime_engine = create_async_engine(
            make_url(_value(capacity_guard_database, "runtime_url")),
            isolation_level="SERIALIZABLE",
        )
        agent_engine = create_async_engine(
            make_url(_value(capacity_guard_database, "agent_url")),
            isolation_level="SERIALIZABLE",
        )
        try:
            runtime_store = ProtectedWorkerSessionStore(
                async_sessionmaker(runtime_engine, expire_on_commit=False)
            )
            receipt = await runtime_store.submit_trial(
                registration=registration,
                submission=submission,
                public_requires_caps=public_requires_caps,
            )
            async with async_sessionmaker(
                agent_engine, expire_on_commit=False
            )() as session, session.begin():
                hidden = await capture_lifecycle_demand_observation(
                    session,
                    registration=registration,
                    expected_high_water=0,
                    max_attempts=100,
                )
            assert hidden.attempts == ()

            readiness = await runtime_store.publish_trial_readiness(
                trial_id=receipt.trial_id,
                protected_attempt_id=receipt.protected_attempt_id,
            )
            assert readiness.replayed is False
            async with async_sessionmaker(
                agent_engine, expire_on_commit=False
            )() as session, session.begin():
                visible = await capture_lifecycle_demand_observation(
                    session,
                    registration=registration,
                    expected_high_water=1,
                    max_attempts=100,
                )
        finally:
            await agent_engine.dispose()
            await runtime_engine.dispose()

        assert [item.protected_attempt_id for item in visible.attempts] == [
            receipt.protected_attempt_id
        ]
        with admin_engine.connect() as connection:
            row = (
                connection.execute(
                    text(
                        "SELECT trial.state, trial.requires_caps, head.executable, "
                        "readiness.public_requires_caps_digest "
                        "FROM public.trials AS trial "
                        "JOIN loom_capacity_guard.attempt_lifecycle_heads AS head "
                        "ON head.protected_attempt_id = :protected_attempt_id "
                        "JOIN loom_capacity_guard.protected_runtime_trial_readiness AS readiness "
                        "ON readiness.trial_id = trial.id "
                        "WHERE trial.id = :trial_id"
                    ),
                    {
                        "trial_id": receipt.trial_id,
                        "protected_attempt_id": receipt.protected_attempt_id,
                    },
                )
                .mappings()
                .one()
            )
        assert row["state"] == "protected-pending"
        assert row["requires_caps"] == public_requires_caps
        assert row["executable"] is False
        assert row["public_requires_caps_digest"] == readiness.public_requires_caps_digest

        append_only_statements = (
            "UPDATE loom_capacity_guard.protected_runtime_trial_submissions "
            "SET public_requires_caps_digest = public_requires_caps_digest",
            "DELETE FROM loom_capacity_guard.protected_runtime_trial_readiness",
            "TRUNCATE loom_capacity_guard.protected_runtime_trial_readiness",
        )
        for statement in append_only_statements:
            with pytest.raises(DBAPIError, match="append-only"):
                with admin_engine.begin() as connection:
                    connection.execute(text(statement))

        with admin_engine.begin() as connection:
            connection.execute(
                text(
                    "UPDATE public.trials SET requires_caps = "
                    "jsonb_set(requires_caps, '{terminus2_model_switch}', 'true'::jsonb) "
                    "WHERE id = :trial_id"
                ),
                {"trial_id": receipt.trial_id},
            )
        drift_engine = create_async_engine(
            make_url(_value(capacity_guard_database, "agent_url")),
            isolation_level="SERIALIZABLE",
        )
        try:
            with pytest.raises(DBAPIError, match="public trial binding drifted"):
                async with async_sessionmaker(
                    drift_engine, expire_on_commit=False
                )() as session, session.begin():
                    await capture_lifecycle_demand_observation(
                        session,
                        registration=registration,
                        expected_high_water=2,
                        max_attempts=100,
                    )
        finally:
            await drift_engine.dispose()
    finally:
        admin_engine.dispose()


@pytest.mark.asyncio
async def test_runtime_submission_rejects_missing_logical_pool_binding(
    capacity_guard_database: dict[str, object],
) -> None:
    registration = await _initialize_guard(capacity_guard_database)
    team_id = uuid4()
    task_id = f"protected-pool-{uuid4().hex}"
    admin_engine = create_engine(_value(capacity_guard_database, "admin_url"))
    try:
        with admin_engine.begin() as connection:
            connection.execute(insert(Team).values(id=team_id, name=f"pool-{team_id}"))
            connection.execute(insert(TeamQuota).values(team_id=team_id))
            connection.execute(
                insert(Task).values(
                    id=task_id,
                    checksum="5" * 64,
                    config={
                        "schema_version": "1",
                        "task": {"id": task_id, "name": task_id},
                        "environment": {"os": "linux", "docker_image": "alpine"},
                        "agent": {"name": "oracle"},
                        "verifier": {"name": "pytest"},
                        "steps": [{"name": "main"}],
                    },
                )
            )
        requirements = SealedRequirementsV1(
            os="linux",
            cpu_arch="x86_64",
            gpu_vendor="none",
            network_policies=("public",),
            required_pool="oldlab",
        )
        submission = AtomicTrialSubmissionV1(
            **registration.model_dump(mode="python"),
            trial_id=uuid4(),
            protected_attempt_id=uuid4(),
            execution_generation=registration.deployment_generation,
            requirements=requirements,
            requirements_digest=canonical_digest(requirements),
            team_id=team_id,
            task_id=task_id,
            config={"agent_name": "oracle", "agent_model": None},
            submit_priority=100,
            idempotency_key=f"protected-pool-{uuid4().hex}",
        )
        runtime_engine = create_async_engine(
            make_url(_value(capacity_guard_database, "runtime_url")),
            isolation_level="SERIALIZABLE",
        )
        try:
            store = ProtectedWorkerSessionStore(
                async_sessionmaker(runtime_engine, expire_on_commit=False)
            )
            with pytest.raises(ProtectedTrialSubmissionError):
                await store.submit_trial(
                    registration=registration,
                    submission=submission,
                    public_requires_caps={
                        "backend": "docker",
                        "os": "linux",
                        "cpu_arch": "x86_64",
                        "gpu_vendor": "none",
                        "network_policies": ["public"],
                        "terminus2_model_switch": False,
                    },
                )
        finally:
            await runtime_engine.dispose()
        with admin_engine.connect() as connection:
            assert connection.execute(
                text(
                    "SELECT count(*) FROM public.trials "
                    "WHERE idempotency_key = :idempotency_key"
                ),
                {"idempotency_key": submission.idempotency_key},
            ).scalar_one() == 0
    finally:
        admin_engine.dispose()


@pytest.mark.asyncio
async def test_runtime_readiness_refuses_incomplete_task_image_prerequisites(
    capacity_guard_database: dict[str, object],
) -> None:
    registration = await _initialize_guard(capacity_guard_database)
    team_id = uuid4()
    task_id = f"protected-incomplete-image-{uuid4().hex}"
    admin_engine = create_engine(_value(capacity_guard_database, "admin_url"))
    try:
        with admin_engine.begin() as connection:
            connection.execute(insert(Team).values(id=team_id, name=f"image-{team_id}"))
            connection.execute(insert(TeamQuota).values(team_id=team_id))
            connection.execute(
                insert(Task).values(
                    id=task_id,
                    checksum="6" * 64,
                    source="s3://loom-tasks/protected-incomplete-image",
                    config={
                        "schema_version": "1",
                        "task": {"id": task_id, "name": task_id},
                        "environment": {
                            "os": "linux",
                            "cpu_arch": "x86_64",
                            "dockerfile": "environment/Dockerfile",
                        },
                        "agent": {"name": "oracle"},
                        "verifier": {"name": "pytest"},
                        "steps": [{"name": "main"}],
                    },
                )
            )
        requirements = SealedRequirementsV1(
            os="linux",
            cpu_arch="x86_64",
            gpu_vendor="none",
            network_policies=("public",),
        )
        submission = AtomicTrialSubmissionV1(
            **registration.model_dump(mode="python"),
            trial_id=uuid4(),
            protected_attempt_id=uuid4(),
            execution_generation=registration.deployment_generation,
            requirements=requirements,
            requirements_digest=canonical_digest(requirements),
            team_id=team_id,
            task_id=task_id,
            config={"agent_name": "oracle", "agent_model": None},
            submit_priority=100,
            idempotency_key=f"protected-incomplete-image-{uuid4().hex}",
        )
        runtime_engine = create_async_engine(
            make_url(_value(capacity_guard_database, "runtime_url")),
            isolation_level="SERIALIZABLE",
        )
        agent_engine = create_async_engine(
            make_url(_value(capacity_guard_database, "agent_url")),
            isolation_level="SERIALIZABLE",
        )
        try:
            store = ProtectedWorkerSessionStore(
                async_sessionmaker(runtime_engine, expire_on_commit=False)
            )
            receipt = await store.submit_trial(
                registration=registration,
                submission=submission,
                public_requires_caps={
                    "backend": "docker",
                    "os": "linux",
                    "cpu_arch": "x86_64",
                    "gpu_vendor": "none",
                    "network_policies": ["public"],
                    "terminus2_model_switch": False,
                },
            )
            with pytest.raises(ProtectedTrialSubmissionError):
                await store.publish_trial_readiness(
                    trial_id=receipt.trial_id,
                    protected_attempt_id=receipt.protected_attempt_id,
                )
            async with async_sessionmaker(
                agent_engine, expire_on_commit=False
            )() as session, session.begin():
                hidden = await capture_lifecycle_demand_observation(
                    session,
                    registration=registration,
                    expected_high_water=0,
                    max_attempts=100,
                )
            assert hidden.attempts == ()
        finally:
            await agent_engine.dispose()
            await runtime_engine.dispose()
        with admin_engine.connect() as connection:
            assert connection.execute(
                text(
                    "SELECT count(*) FROM "
                    "loom_capacity_guard.protected_runtime_trial_readiness "
                    "WHERE trial_id = :trial_id"
                ),
                {"trial_id": submission.trial_id},
            ).scalar_one() == 0
    finally:
        admin_engine.dispose()


@pytest.mark.asyncio
async def test_guard_0023_downgrade_refuses_persisted_runtime_origin(
    capacity_guard_database: dict[str, object],
    monkeypatch,
) -> None:  # type: ignore[no-untyped-def]
    registration = await _initialize_guard(capacity_guard_database)
    team_id = uuid4()
    task_id = f"protected-downgrade-{uuid4().hex}"
    admin_engine = create_engine(_value(capacity_guard_database, "admin_url"))
    try:
        with admin_engine.begin() as connection:
            connection.execute(
                insert(Team).values(id=team_id, name=f"downgrade-{team_id}")
            )
            connection.execute(insert(TeamQuota).values(team_id=team_id))
            connection.execute(
                insert(Task).values(
                    id=task_id,
                    checksum="7" * 64,
                    config={
                        "schema_version": "1",
                        "task": {"id": task_id, "name": task_id},
                        "environment": {"os": "linux", "docker_image": "alpine"},
                        "agent": {"name": "oracle"},
                        "verifier": {"name": "pytest"},
                        "steps": [{"name": "main"}],
                    },
                )
            )
        requirements = SealedRequirementsV1(
            os="linux",
            cpu_arch="x86_64",
            gpu_vendor="none",
            network_policies=("public",),
        )
        submission = AtomicTrialSubmissionV1(
            **registration.model_dump(mode="python"),
            trial_id=uuid4(),
            protected_attempt_id=uuid4(),
            execution_generation=registration.deployment_generation,
            requirements=requirements,
            requirements_digest=canonical_digest(requirements),
            team_id=team_id,
            task_id=task_id,
            config={"agent_name": "oracle", "agent_model": None},
            submit_priority=100,
            idempotency_key=f"protected-downgrade-{uuid4().hex}",
        )
        runtime_engine = create_async_engine(
            make_url(_value(capacity_guard_database, "runtime_url")),
            isolation_level="SERIALIZABLE",
        )
        try:
            store = ProtectedWorkerSessionStore(
                async_sessionmaker(runtime_engine, expire_on_commit=False)
            )
            await store.submit_trial(
                registration=registration,
                submission=submission,
                public_requires_caps={
                    "backend": "docker",
                    "os": "linux",
                    "cpu_arch": "x86_64",
                    "gpu_vendor": "none",
                    "network_policies": ["public"],
                    "terminus2_model_switch": False,
                },
            )
        finally:
            await runtime_engine.dispose()

        root = Path(__file__).resolve().parents[2]
        cfg = AlembicConfig(str(root / "capacity_guard_migrations" / "alembic.ini"))
        cfg.set_main_option("script_location", str(root / "capacity_guard_migrations"))
        for environment_name, database_key in {
            "LOOM_CAPACITY_GUARD_DB_URL": "migrator_url",
            "LOOM_CAPACITY_GUARD_OWNER_ROLE": "owner_role",
            "LOOM_CAPACITY_GUARD_AGENT_ROLE": "agent_role",
            "LOOM_CAPACITY_GUARD_EXECUTOR_ROLE": "executor_role",
            "LOOM_CAPACITY_GUARD_OBSERVER_ROLE": "observer_role",
            "LOOM_CAPACITY_GUARD_RUNTIME_ROLE": "runtime_role",
        }.items():
            monkeypatch.setenv(
                environment_name,
                _value(capacity_guard_database, database_key),
            )
        with pytest.raises(
            RuntimeError,
            match="cannot downgrade guard_0023 while protected runtime submissions exist",
        ):
            command.downgrade(cfg, "guard_0022")
        with admin_engine.connect() as connection:
            assert connection.execute(
                text(
                    "SELECT version_num FROM "
                    "loom_capacity_guard.capacity_guard_alembic_version"
                )
            ).scalar_one() == "guard_0026"
            assert connection.execute(
                text(
                    "SELECT count(*) FROM "
                    "loom_capacity_guard.protected_runtime_trial_submissions "
                    "WHERE trial_id = :trial_id"
                ),
                {"trial_id": submission.trial_id},
            ).scalar_one() == 1
    finally:
        admin_engine.dispose()


def test_protected_mode_submit_creates_only_inert_guarded_demand(
    capacity_guard_database: dict[str, object],
    tmp_path: Path,
    monkeypatch,
) -> None:  # type: ignore[no-untyped-def]
    """Removing the protected route branch must leave a detectable public-only row."""

    asyncio.run(_initialize_guard(capacity_guard_database))
    team_id = uuid4()
    user_id = uuid4()
    raw_token = f"loom_team_{uuid4().hex}"
    task_id = f"protected-submit-{uuid4().hex}"
    admin_engine = create_engine(_value(capacity_guard_database, "admin_url"))
    try:
        with admin_engine.begin() as connection:
            connection.execute(insert(Team).values(id=team_id, name=f"sub-{team_id}"))
            connection.execute(
                insert(User).values(
                    id=user_id,
                    username=f"protected-{user_id.hex[:8]}",
                    username_normalized=f"protected-{user_id.hex[:8]}",
                    status="active",
                    is_platform_admin=False,
                )
            )
            connection.execute(insert(TeamQuota).values(team_id=team_id))
            connection.execute(
                insert(Token).values(
                    token_hash=hashlib.sha256(raw_token.encode()).digest(),
                    type="team",
                    scopes=["submit"],
                    team_id=team_id,
                    created_by_user_id=user_id,
                    issued_at=datetime.now(UTC),
                    expires_at=None,
                )
            )
            connection.execute(
                insert(Task).values(
                    id=task_id,
                    checksum="0" * 64,
                    config={
                        "schema_version": "1",
                        "task": {"id": task_id, "name": task_id},
                        "environment": {"os": "linux", "docker_image": "alpine"},
                        "agent": {"name": "oracle"},
                        "verifier": {"name": "pytest"},
                        "steps": [{"name": "main"}],
                    },
                )
            )

        runtime_url_file = tmp_path / "protected-runtime-db-url"
        _write_runtime_url(runtime_url_file, capacity_guard_database)
        for key, value in {
            "LOOM_CP_DB_URL": _value(capacity_guard_database, "admin_url"),
            "LOOM_CP_MINIO_ENDPOINT": "http://minio:9000",
            "LOOM_CP_MINIO_ACCESS_KEY": "x",
            "LOOM_CP_MINIO_SECRET_KEY": "x",
            "LOOM_CP_LLM_GATEWAY_URL": "http://gw:9100/",
            "LOOM_CP_PROTECTED_WORKER_RUNTIME_DB_URL_FILE": str(runtime_url_file),
        }.items():
            monkeypatch.setenv(key, value)
        app = create_app(ControlPlaneSettings(_env_file=None))

        with TestClient(app) as client:
            response = client.post(
                "/trials",
                headers={"Authorization": f"Bearer {raw_token}"},
                json={
                    "task_id": task_id,
                    "config": {"agent_name": "oracle", "agent_model": None},
                },
            )

        assert response.status_code == 201, response.text
        trial_id = UUID(response.json()["trial_id"])
        assert response.json()["state"] == "queued"
        with admin_engine.connect() as connection:
            row = (
                connection.execute(
                    text(
                        "SELECT trial.state, trial.worker_id, trial.attempt_count, "
                        "submission.protected_attempt_id, attempt.claim_state, "
                        "head.lifecycle_state, head.executable "
                        "FROM public.trials AS trial "
                        "JOIN loom_capacity_guard.atomic_trial_submissions AS submission "
                        "ON submission.trial_id = trial.id "
                        "JOIN loom_capacity_guard.trial_attempts AS attempt "
                        "ON attempt.protected_attempt_id = submission.protected_attempt_id "
                        "JOIN loom_capacity_guard.attempt_lifecycle_heads AS head "
                        "ON head.protected_attempt_id = attempt.protected_attempt_id "
                        "WHERE trial.id = :trial_id"
                    ),
                    {"trial_id": trial_id},
                )
                .mappings()
                .one()
            )
        assert dict(row) == {
            "state": "protected-pending",
            "worker_id": None,
            "attempt_count": 0,
            "protected_attempt_id": row["protected_attempt_id"],
            "claim_state": "queued",
            "lifecycle_state": "pending-unassigned",
            "executable": False,
        }
    finally:
        admin_engine.dispose()


def test_protected_idempotency_replay_is_exact_and_conflicts_are_inert(
    capacity_guard_database: dict[str, object],
    tmp_path: Path,
    monkeypatch,
) -> None:  # type: ignore[no-untyped-def]
    asyncio.run(_initialize_guard(capacity_guard_database))
    team_id = uuid4()
    user_id = uuid4()
    raw_token = f"loom_team_{uuid4().hex}"
    idempotency_key = f"protected-exact-{uuid4().hex}"
    task_id = f"protected-exact-{uuid4().hex}"
    other_task_id = f"protected-exact-other-{uuid4().hex}"
    admin_engine = create_engine(_value(capacity_guard_database, "admin_url"))
    try:
        task_config = {
            "schema_version": "1",
            "environment": {"os": "linux", "docker_image": "alpine"},
            "agent": {"name": "oracle"},
            "verifier": {"name": "pytest"},
            "steps": [{"name": "main"}],
        }
        with admin_engine.begin() as connection:
            connection.execute(insert(Team).values(id=team_id, name=f"exact-{team_id}"))
            connection.execute(
                insert(User).values(
                    id=user_id,
                    username=f"exact-{user_id.hex[:8]}",
                    username_normalized=f"exact-{user_id.hex[:8]}",
                    status="active",
                    is_platform_admin=False,
                )
            )
            connection.execute(insert(TeamQuota).values(team_id=team_id))
            connection.execute(
                insert(Token).values(
                    token_hash=hashlib.sha256(raw_token.encode()).digest(),
                    type="team",
                    scopes=["submit"],
                    team_id=team_id,
                    created_by_user_id=user_id,
                    issued_at=datetime.now(UTC),
                    expires_at=None,
                )
            )
            for seeded_task_id in (task_id, other_task_id):
                connection.execute(
                    insert(Task).values(
                        id=seeded_task_id,
                        checksum=("3" if seeded_task_id == task_id else "4") * 64,
                        config={
                            **task_config,
                            "task": {"id": seeded_task_id, "name": seeded_task_id},
                        },
                    )
                )

        runtime_url_file = tmp_path / "protected-runtime-exact-db-url"
        _write_runtime_url(runtime_url_file, capacity_guard_database)
        for key, value in {
            "LOOM_CP_DB_URL": _value(capacity_guard_database, "admin_url"),
            "LOOM_CP_MINIO_ENDPOINT": "http://minio:9000",
            "LOOM_CP_MINIO_ACCESS_KEY": "x",
            "LOOM_CP_MINIO_SECRET_KEY": "x",
            "LOOM_CP_LLM_GATEWAY_URL": "http://gw:9100/",
            "LOOM_CP_PROTECTED_WORKER_RUNTIME_DB_URL_FILE": str(runtime_url_file),
        }.items():
            monkeypatch.setenv(key, value)
        app = create_app(ControlPlaneSettings(_env_file=None))
        base_payload = {
            "task_id": task_id,
            "idempotency_key": idempotency_key,
            "config": {"agent_name": "oracle", "agent_model": None},
        }
        conflicting_payloads = (
            {
                **base_payload,
                "task_id": other_task_id,
            },
            {
                **base_payload,
                "config": {
                    "agent_name": "oracle",
                    "agent_model": None,
                    "submit_priority": 101,
                },
            },
            {
                **base_payload,
                "required_worker_pool": "behavior-cpu-data",
            },
        )
        headers = {"Authorization": f"Bearer {raw_token}"}
        with TestClient(app, raise_server_exceptions=False) as client:
            first = client.post("/trials", headers=headers, json=base_payload)
            replay = client.post("/trials", headers=headers, json=base_payload)
            conflicts = tuple(
                client.post("/trials", headers=headers, json=payload)
                for payload in conflicting_payloads
            )

        assert first.status_code == 201, first.text
        assert replay.status_code == 201, replay.text
        assert replay.json() == first.json()
        assert [response.status_code for response in conflicts] == [409, 409, 409]

        trial_id = UUID(first.json()["trial_id"])
        with admin_engine.connect() as connection:
            counts = (
                connection.execute(
                    text(
                        "SELECT "
                        "(SELECT count(*) FROM public.trials "
                        "WHERE idempotency_key = :idempotency_key) AS trials, "
                        "(SELECT count(*) FROM loom_capacity_guard.atomic_trial_submissions "
                        "WHERE idempotency_key = :idempotency_key) AS submissions, "
                        "(SELECT count(*) FROM loom_capacity_guard.trial_attempts "
                        "WHERE trial_id = :trial_id) AS attempts, "
                        "(SELECT count(*) FROM "
                        "loom_capacity_guard.protected_runtime_trial_submissions "
                        "WHERE trial_id = :trial_id) AS origins, "
                        "(SELECT count(*) FROM "
                        "loom_capacity_guard.protected_runtime_trial_readiness "
                        "WHERE trial_id = :trial_id) AS readiness"
                    ),
                    {"idempotency_key": idempotency_key, "trial_id": trial_id},
                )
                .mappings()
                .one()
            )
        assert dict(counts) == {
            "trials": 1,
            "submissions": 1,
            "attempts": 1,
            "origins": 1,
            "readiness": 1,
        }
    finally:
        admin_engine.dispose()


def test_protected_idempotency_replay_repairs_task_images_and_readiness(
    capacity_guard_database: dict[str, object],
    tmp_path: Path,
    monkeypatch,
) -> None:  # type: ignore[no-untyped-def]
    registration = asyncio.run(_initialize_guard(capacity_guard_database))
    team_id = uuid4()
    user_id = uuid4()
    raw_token = f"loom_team_{uuid4().hex}"
    idempotency_key = f"protected-repair-{uuid4().hex}"
    task_id = f"protected-repair-{uuid4().hex}"
    admin_engine = create_engine(_value(capacity_guard_database, "admin_url"))
    try:
        with admin_engine.begin() as connection:
            connection.execute(insert(Team).values(id=team_id, name=f"repair-{team_id}"))
            connection.execute(
                insert(User).values(
                    id=user_id,
                    username=f"repair-{user_id.hex[:8]}",
                    username_normalized=f"repair-{user_id.hex[:8]}",
                    status="active",
                    is_platform_admin=False,
                )
            )
            connection.execute(insert(TeamQuota).values(team_id=team_id))
            connection.execute(
                insert(Token).values(
                    token_hash=hashlib.sha256(raw_token.encode()).digest(),
                    type="team",
                    scopes=["submit"],
                    team_id=team_id,
                    created_by_user_id=user_id,
                    issued_at=datetime.now(UTC),
                    expires_at=None,
                )
            )
            connection.execute(
                insert(Task).values(
                    id=task_id,
                    checksum="1" * 64,
                    source="s3://loom-tasks/protected-repair",
                    config={
                        "schema_version": "1",
                        "task": {"id": task_id, "name": task_id},
                        "environment": {
                            "os": "linux",
                            "cpu_arch": "x86_64",
                            "dockerfile": "environment/Dockerfile",
                        },
                        "agent": {"name": "oracle"},
                        "verifier": {"name": "pytest"},
                        "steps": [{"name": "main"}],
                    },
                )
            )

        runtime_url_file = tmp_path / "protected-runtime-repair-db-url"
        _write_runtime_url(runtime_url_file, capacity_guard_database)
        for key, value in {
            "LOOM_CP_DB_URL": _value(capacity_guard_database, "admin_url"),
            "LOOM_CP_MINIO_ENDPOINT": "http://minio:9000",
            "LOOM_CP_MINIO_ACCESS_KEY": "x",
            "LOOM_CP_MINIO_SECRET_KEY": "x",
            "LOOM_CP_LLM_GATEWAY_URL": "http://gw:9100/",
            "LOOM_CP_PROTECTED_WORKER_RUNTIME_DB_URL_FILE": str(runtime_url_file),
        }.items():
            monkeypatch.setenv(key, value)
        app = create_app(ControlPlaneSettings(_env_file=None))
        original_ensure = trial_routes._ensure_trial_task_image_links
        failed = False

        async def fail_once(*args, **kwargs) -> None:  # type: ignore[no-untyped-def]
            nonlocal failed
            if not failed:
                failed = True
                raise RuntimeError("simulated crash before task-image prerequisites")
            await original_ensure(*args, **kwargs)

        monkeypatch.setattr(trial_routes, "_ensure_trial_task_image_links", fail_once)
        request_payload = {
            "task_id": task_id,
            "idempotency_key": idempotency_key,
            "config": {"agent_name": "oracle", "agent_model": None},
        }
        with TestClient(app, raise_server_exceptions=False) as client:
            failed_response = client.post(
                "/trials",
                headers={"Authorization": f"Bearer {raw_token}"},
                json=request_payload,
            )
            replay_response = client.post(
                "/trials",
                headers={"Authorization": f"Bearer {raw_token}"},
                json=request_payload,
            )

        assert failed_response.status_code == 500
        assert replay_response.status_code == 201, replay_response.text
        assert replay_response.json()["state"] == "queued"
        trial_id = UUID(replay_response.json()["trial_id"])
        with admin_engine.connect() as connection:
            row = (
                connection.execute(
                    text(
                        "SELECT "
                        "(SELECT count(*) FROM public.trial_task_image_materializations "
                        "WHERE trial_id = :trial_id) AS task_image_links, "
                        "(SELECT count(*) FROM "
                        "loom_capacity_guard.protected_runtime_trial_readiness "
                        "WHERE trial_id = :trial_id) AS readiness"
                    ),
                    {"trial_id": trial_id},
                )
                .mappings()
                .one()
            )
        assert dict(row) == {"task_image_links": 1, "readiness": 1}

        async def capture(expected_high_water: int) -> tuple[UUID, ...]:
            engine = create_async_engine(
                make_url(_value(capacity_guard_database, "agent_url")),
                isolation_level="SERIALIZABLE",
            )
            try:
                async with async_sessionmaker(
                    engine, expire_on_commit=False
                )() as session, session.begin():
                    observation = await capture_lifecycle_demand_observation(
                        session,
                        registration=registration,
                        expected_high_water=expected_high_water,
                        max_attempts=100,
                    )
                return tuple(item.protected_attempt_id for item in observation.attempts)
            finally:
                await engine.dispose()

        with admin_engine.connect() as connection:
            protected_attempt_id = connection.execute(
                text(
                    "SELECT protected_attempt_id FROM "
                    "loom_capacity_guard.atomic_trial_submissions WHERE trial_id = :trial_id"
                ),
                {"trial_id": trial_id},
            ).scalar_one()
        assert asyncio.run(capture(0)) == (protected_attempt_id,)
        with admin_engine.begin() as connection:
            connection.execute(
                text(
                    "UPDATE public.task_image_materializations "
                    "SET materialization_key = repeat('f', 64) "
                    "WHERE id IN ("
                    "SELECT materialization_id FROM "
                    "public.trial_task_image_materializations "
                    "WHERE trial_id = :trial_id)"
                ),
                {"trial_id": trial_id},
            )
        with pytest.raises(DBAPIError, match="readiness prerequisites drifted"):
            asyncio.run(capture(1))
    finally:
        admin_engine.dispose()


def test_protected_idempotency_replay_repairs_model_switch_and_readiness(
    capacity_guard_database: dict[str, object],
    tmp_path: Path,
    monkeypatch,
) -> None:  # type: ignore[no-untyped-def]
    registration = asyncio.run(_initialize_guard(capacity_guard_database))
    team_id = uuid4()
    user_id = uuid4()
    raw_token = f"loom_team_{uuid4().hex}"
    idempotency_key = f"protected-model-repair-{uuid4().hex}"
    task_id = f"protected-model-repair-{uuid4().hex}"
    admin_engine = create_engine(_value(capacity_guard_database, "admin_url"))
    try:
        with admin_engine.begin() as connection:
            connection.execute(insert(Team).values(id=team_id, name=f"model-{team_id}"))
            connection.execute(
                insert(User).values(
                    id=user_id,
                    username=f"model-{user_id.hex[:8]}",
                    username_normalized=f"model-{user_id.hex[:8]}",
                    status="active",
                    is_platform_admin=False,
                )
            )
            connection.execute(insert(TeamQuota).values(team_id=team_id))
            connection.execute(
                insert(Token).values(
                    token_hash=hashlib.sha256(raw_token.encode()).digest(),
                    type="team",
                    scopes=["submit"],
                    team_id=team_id,
                    created_by_user_id=user_id,
                    issued_at=datetime.now(UTC),
                    expires_at=None,
                )
            )
            connection.execute(
                insert(Task).values(
                    id=task_id,
                    checksum="2" * 64,
                    config={
                        "schema_version": "1",
                        "task": {"id": task_id, "name": task_id},
                        "environment": {"os": "linux", "docker_image": "alpine"},
                        "agent": {"name": "terminus-2"},
                        "verifier": {"name": "pytest"},
                        "steps": [{"name": "main"}],
                    },
                )
            )

        runtime_url_file = tmp_path / "protected-runtime-model-repair-db-url"
        _write_runtime_url(runtime_url_file, capacity_guard_database)
        for key, value in {
            "LOOM_CP_DB_URL": _value(capacity_guard_database, "admin_url"),
            "LOOM_CP_MINIO_ENDPOINT": "http://minio:9000",
            "LOOM_CP_MINIO_ACCESS_KEY": "x",
            "LOOM_CP_MINIO_SECRET_KEY": "x",
            "LOOM_CP_LLM_GATEWAY_URL": "http://gw:9100/",
            "LOOM_CP_PROTECTED_WORKER_RUNTIME_DB_URL_FILE": str(runtime_url_file),
        }.items():
            monkeypatch.setenv(key, value)
        app = create_app(ControlPlaneSettings(_env_file=None))
        original_persist = model_switch_store.persist_model_switch_plan
        failed = False

        async def fail_once(*args, **kwargs):  # type: ignore[no-untyped-def]
            nonlocal failed
            if not failed:
                failed = True
                raise RuntimeError("simulated crash before model-switch prerequisite")
            return await original_persist(*args, **kwargs)

        monkeypatch.setattr(model_switch_store, "persist_model_switch_plan", fail_once)
        request_payload = {
            "task_id": task_id,
            "idempotency_key": idempotency_key,
            "config": {
                "agent_name": "terminus-2",
                "agent_model": {
                    "provider": "openai",
                    "name": "primary-model",
                    "source": "api",
                },
                "multi_model": {
                    "enabled": True,
                    "policy": "student_teacher_student",
                    "secondary_model": {
                        "provider": "openai",
                        "name": "teacher-model",
                        "source": "api",
                    },
                    "switch_episode": 2,
                    "teacher_episodes": 2,
                },
            },
        }
        with TestClient(app, raise_server_exceptions=False) as client:
            failed_response = client.post(
                "/trials",
                headers={"Authorization": f"Bearer {raw_token}"},
                json=request_payload,
            )
            replay_response = client.post(
                "/trials",
                headers={"Authorization": f"Bearer {raw_token}"},
                json=request_payload,
            )

        assert failed_response.status_code == 500
        assert replay_response.status_code == 201, replay_response.text
        assert replay_response.json()["state"] == "queued"
        trial_id = UUID(replay_response.json()["trial_id"])
        with admin_engine.connect() as connection:
            row = (
                connection.execute(
                    text(
                        "SELECT "
                        "(SELECT count(*) FROM public.model_switch_plans "
                        "WHERE trial_id = :trial_id) AS model_switch_plans, "
                        "(SELECT count(*) FROM "
                        "loom_capacity_guard.protected_runtime_trial_readiness "
                        "WHERE trial_id = :trial_id) AS readiness"
                    ),
                    {"trial_id": trial_id},
                )
                .mappings()
                .one()
            )
        assert dict(row) == {"model_switch_plans": 1, "readiness": 1}

        async def capture(expected_high_water: int) -> tuple[UUID, ...]:
            engine = create_async_engine(
                make_url(_value(capacity_guard_database, "agent_url")),
                isolation_level="SERIALIZABLE",
            )
            try:
                async with async_sessionmaker(
                    engine, expire_on_commit=False
                )() as session, session.begin():
                    observation = await capture_lifecycle_demand_observation(
                        session,
                        registration=registration,
                        expected_high_water=expected_high_water,
                        max_attempts=100,
                    )
                return tuple(item.protected_attempt_id for item in observation.attempts)
            finally:
                await engine.dispose()

        with admin_engine.connect() as connection:
            protected_attempt_id = connection.execute(
                text(
                    "SELECT protected_attempt_id FROM "
                    "loom_capacity_guard.atomic_trial_submissions "
                    "WHERE trial_id = :trial_id"
                ),
                {"trial_id": trial_id},
            ).scalar_one()
        assert asyncio.run(capture(0)) == (protected_attempt_id,)
        with admin_engine.begin() as connection:
            connection.execute(
                text(
                    "UPDATE public.model_switch_plans SET seed = seed || '-drift' "
                    "WHERE trial_id = :trial_id"
                ),
                {"trial_id": trial_id},
            )
        with pytest.raises(DBAPIError, match="readiness prerequisites drifted"):
            asyncio.run(capture(1))
    finally:
        admin_engine.dispose()
