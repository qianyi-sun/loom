"""Authenticated operator/user transport for the original queued trial IDs."""

import asyncio
import hashlib
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from loom.db.schema import DataLifecycleAuthority, Task, Team, Token, Trial, User
from loom_capacity_agent.store import capture_lifecycle_demand_observation
from loom_control_plane.app import create_app
from loom_control_plane.config import ControlPlaneSettings
from tests.integration.test_capacity_agent_store import _value
from tests.integration.test_capacity_protected_trial_submission_route import (
    _initialize_guard,
    _write_runtime_url,
)
from tests.integration.test_capacity_submission_store import _seed_trial_inputs
from tests.integration.test_capacity_trial_writer_fence import _freeze, _initialize


@pytest.mark.parametrize("capture_demand", [False, True])
def test_team_authenticated_adoption_replays_original_id_and_publishes_readiness(
    capacity_guard_database, tmp_path, monkeypatch, capture_demand,
):
    database = capacity_guard_database
    registration = asyncio.run(_initialize_guard(database))
    team_id, task_id = _seed_trial_inputs(database)
    user_id, trial_id, lifecycle_id, operation_id = (uuid4() for _ in range(4))
    token = f"loom_team_{uuid4().hex}"
    foreign_token = f"loom_team_{uuid4().hex}"
    foreign_team = uuid4()
    submitted_at = datetime.now(UTC) - timedelta(days=2)
    engine = create_engine(_value(database, "admin_url"))
    try:
        with engine.begin() as connection:
            connection.execute(User.__table__.insert().values(
                id=user_id, username=f"adopt-{user_id.hex}", username_normalized=f"adopt-{user_id.hex}",
                status="active", is_platform_admin=False,
            ))
            connection.execute(Token.__table__.insert().values(
                token_hash=hashlib.sha256(token.encode()).digest(), type="team", scopes=["submit"],
                team_id=team_id, created_by_user_id=user_id, issued_at=datetime.now(UTC),
            ))
            connection.execute(Team.__table__.insert().values(id=foreign_team, name=f"foreign-{foreign_team}"))
            connection.execute(Token.__table__.insert().values(
                token_hash=hashlib.sha256(foreign_token.encode()).digest(), type="team", scopes=["submit"],
                team_id=foreign_team, created_by_user_id=user_id, issued_at=datetime.now(UTC),
            ))
            connection.execute(Task.__table__.update().where(Task.id == task_id).values(config={
                "schema_version": "1", "task": {"id": task_id, "name": task_id},
                "environment": {"os": "linux", "docker_image": "alpine"},
                "agent": {"name": "oracle"}, "verifier": {"name": "pytest"}, "steps": [{"name": "main"}],
            }))
            scope = connection.execute(text("SELECT lifecycle_environment,lifecycle_namespace FROM loom_capacity_guard.authority_state")).one()
            connection.execute(DataLifecycleAuthority.__table__.insert().values(
                id=lifecycle_id, environment=scope[0], namespace=scope[1], team_id=team_id,
                data_class="trial", owner_kind="trial", owner_id=str(trial_id), created_at=submitted_at,
                expires_at=submitted_at + timedelta(days=7), pinned=False, state="active",
            ))
            connection.execute(Trial.__table__.insert().values(
                id=trial_id, team_id=team_id, task_id=task_id,
                config={"agent_name": "oracle", "agent_model": None}, state="queued",
                requires_caps={"backend": "docker", "os": "linux", "cpu_arch": "any", "gpu_vendor": "none",
                               "network_policies": ["public"], "terminus2_model_switch": False},
                submitted_at=submitted_at, lifecycle_authority_id=lifecycle_id,
                submitted_by_user_id=user_id,
            ))
        writer = asyncio.run(_initialize(database, registration=registration))
        asyncio.run(_freeze(database, writer["writer_incarnation"], uuid4()))
        runtime_file = tmp_path / "runtime-url"
        _write_runtime_url(runtime_file, database)
        for key, value in {
            "LOOM_CP_DB_URL": _value(database, "admin_url"), "LOOM_CP_MINIO_ENDPOINT": "http://minio:9000",
            "LOOM_CP_MINIO_ACCESS_KEY": "x", "LOOM_CP_MINIO_SECRET_KEY": "x",
            "LOOM_CP_LLM_GATEWAY_URL": "http://gw:9100/",
            "LOOM_CP_PROTECTED_WORKER_RUNTIME_DB_URL_FILE": str(runtime_file),
        }.items():
            monkeypatch.setenv(key, value)
        with TestClient(create_app(ControlPlaneSettings(_env_file=None))) as client:
            path = f"/trials/{trial_id}/adopt-protected"
            body = {"operation_id": str(operation_id)}
            assert client.post(path, json=body).status_code == 401
            assert client.post(path, headers={"Authorization": f"Bearer {foreign_token}"}, json=body).status_code == 403
            with engine.connect() as connection:
                assert connection.execute(text("SELECT count(*) FROM loom_capacity_guard.trial_adoptions")).scalar_one() == 0
            response = client.post(path, headers={"Authorization": f"Bearer {token}"}, json=body)
            assert response.status_code == 200, response.text
            replay = client.post(path, headers={"Authorization": f"Bearer {token}"}, json=body)
            assert replay.status_code == 200, replay.text
            assert response.json()["trial_id"] == replay.json()["trial_id"] == str(trial_id)
            assert response.json()["state"] == "protected-pending"
            assert response.json()["ready"] is True
        if capture_demand:
            async def capture():
                agent = create_async_engine(_value(database, "agent_url"), isolation_level="SERIALIZABLE")
                try:
                    async with async_sessionmaker(agent)() as session, session.begin():
                        return await capture_lifecycle_demand_observation(
                            session, registration=registration, expected_high_water=0, max_attempts=100,
                        )
                finally:
                    await agent.dispose()
            observed = asyncio.run(capture())
            assert [str(attempt.protected_attempt_id) for attempt in observed.attempts] == [response.json()["protected_attempt_id"]]
        with engine.connect() as connection:
            actual = connection.execute(text(
                "SELECT submitted_at,lifecycle_authority_id,state FROM public.trials WHERE id=:trial"
            ), {"trial": trial_id}).one()
            assert actual == (submitted_at, lifecycle_id, "protected-pending")
            assert connection.execute(text(
                "SELECT count(*) FROM loom_capacity_guard.protected_runtime_trial_readiness WHERE trial_id=:trial"
            ), {"trial": trial_id}).scalar_one() == 1
    finally:
        engine.dispose()
