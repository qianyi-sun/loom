"""License metadata is informational at trial submit.

Tasks submit regardless of source license. Team quota license allowlists remain
legacy metadata and must not block research evaluation.
"""

import hashlib
from collections.abc import Iterator
from datetime import UTC, datetime
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, delete, insert, update
from sqlalchemy.orm import sessionmaker

from loom.db.schema import Task, Team, TeamQuota, Token, User
from loom_control_plane.app import create_app
from loom_control_plane.config import ControlPlaneSettings


@pytest.fixture
def seed(postgres_url: str) -> Iterator[dict]:
    engine = create_engine(postgres_url)
    session_local = sessionmaker(engine)
    team_id = uuid4()
    user_id = uuid4()
    raw = f"team_{uuid4().hex}"
    now = datetime.now(UTC)
    with session_local() as s:
        s.execute(insert(Team).values(id=team_id, name=f"t-{team_id}"))
        s.execute(insert(User).values(
            id=user_id,
            username=f"LicensePolicyUser-{user_id.hex[:8]}",
            username_normalized=f"license-policy-user-{user_id.hex[:8]}",
            status="active",
            is_platform_admin=False,
        ))
        # The DB still carries the legacy license_allowlist default, but it is
        # informational only and must not affect submit.
        s.execute(insert(TeamQuota).values(team_id=team_id))
        s.execute(insert(Token).values(
            token_hash=hashlib.sha256(raw.encode()).digest(),
            type="team", scopes=["submit"], team_id=team_id,
            created_by_user_id=user_id,
            issued_at=now, expires_at=None,
        ))
        # Three tasks: MIT, GPL-3.0, no license.
        s.execute(insert(Task).values(
            id="mit-task", checksum="0" * 64,
            config={
                "schema_version": "1",
                "task": {"id": "mit-task", "name": "mit-task"},
                "environment": {"os": "linux", "docker_image": "alpine"},
                "agent": {"name": "oracle"},
                "verifier": {"name": "pytest"},
                "steps": [{"name": "main"}],
            },
            license="MIT",
        ))
        s.execute(insert(Task).values(
            id="gpl-task", checksum="0" * 64,
            config={
                "schema_version": "1",
                "task": {"id": "gpl-task", "name": "gpl-task"},
                "environment": {"os": "linux", "docker_image": "alpine"},
                "agent": {"name": "oracle"},
                "verifier": {"name": "pytest"},
                "steps": [{"name": "main"}],
            },
            license="GPL-3.0-only",
        ))
        s.execute(insert(Task).values(
            id="no-license-task", checksum="0" * 64,
            config={
                "schema_version": "1",
                "task": {"id": "no-license-task", "name": "no-license-task"},
                "environment": {"os": "linux", "docker_image": "alpine"},
                "agent": {"name": "oracle"},
                "verifier": {"name": "pytest"},
                "steps": [{"name": "main"}],
            },
            license=None,
        ))
        # Empty-string license — a misconfigured importer might write this,
        # but license metadata must still not block submit.
        s.execute(insert(Task).values(
            id="empty-license-task", checksum="0" * 64,
            config={
                "schema_version": "1",
                "task": {"id": "empty-license-task", "name": "empty"},
                "environment": {"os": "linux", "docker_image": "alpine"},
                "agent": {"name": "oracle"},
                "verifier": {"name": "pytest"},
                "steps": [{"name": "main"}],
            },
            license="",
        ))
        s.commit()
    try:
        yield {"team_id": team_id, "token": raw}
    finally:
        with session_local() as s:
            from loom.db.schema import Trial
            s.execute(delete(Trial))
            s.execute(delete(Token))
            s.execute(delete(Task))
            s.execute(delete(TeamQuota))
            s.execute(delete(User).where(User.id == user_id))
            s.execute(delete(Team))
            s.commit()
        engine.dispose()


@pytest.fixture
def app(monkeypatch: pytest.MonkeyPatch, postgres_url: str, seed: dict):
    for k, v in {
        "LOOM_CP_DB_URL": postgres_url,
        "LOOM_CP_MINIO_ENDPOINT": "http://minio:9000",
        "LOOM_CP_MINIO_ACCESS_KEY": "x",
        "LOOM_CP_MINIO_SECRET_KEY": "y",
        "LOOM_CP_LLM_GATEWAY_URL": "http://gw:9100/",
    }.items():
        monkeypatch.setenv(k, v)
    return create_app(ControlPlaneSettings(_env_file=None))


def test_mit_task_accepted(app, seed):  # type: ignore[no-untyped-def]
    with TestClient(app) as client:
        r = client.post(
            "/trials",
            headers={"Authorization": f"Bearer {seed['token']}"},
            json={"task_id": "mit-task", "config": {"agent_name": "oracle", "agent_model": None}},
        )
        assert r.status_code == 201, r.text


def test_gpl_task_accepted(app, seed):  # type: ignore[no-untyped-def]
    with TestClient(app) as client:
        r = client.post(
            "/trials",
            headers={"Authorization": f"Bearer {seed['token']}"},
            json={"task_id": "gpl-task", "config": {"agent_name": "oracle", "agent_model": None}},
        )
        assert r.status_code == 201, r.text


def test_no_license_task_accepted(app, seed):  # type: ignore[no-untyped-def]
    with TestClient(app) as client:
        r = client.post(
            "/trials",
            headers={"Authorization": f"Bearer {seed['token']}"},
            json={"task_id": "no-license-task", "config": {"agent_name": "oracle", "agent_model": None}},
        )
        assert r.status_code == 201, r.text


def test_empty_string_license_accepted(app, seed):  # type: ignore[no-untyped-def]
    with TestClient(app) as client:
        r = client.post(
            "/trials",
            headers={"Authorization": f"Bearer {seed['token']}"},
            json={"task_id": "empty-license-task", "config": {"agent_name": "oracle", "agent_model": None}},
        )
        assert r.status_code == 201, r.text


def test_tightened_allowlist_still_accepts_mit(app, seed, postgres_url):  # type: ignore[no-untyped-def]
    """Operator-tightened legacy allowlists do not affect submit."""
    engine = create_engine(postgres_url)
    with engine.begin() as conn:
        conn.execute(
            update(TeamQuota)
            .where(TeamQuota.team_id == seed["team_id"])
            .values(license_allowlist=["Apache-2.0"]),
        )
    engine.dispose()

    with TestClient(app) as client:
        r = client.post(
            "/trials",
            headers={"Authorization": f"Bearer {seed['token']}"},
            json={"task_id": "mit-task", "config": {"agent_name": "oracle", "agent_model": None}},
        )
        assert r.status_code == 201, r.text
