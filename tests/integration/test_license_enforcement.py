"""Plan 13 Task 4: license-allowlist enforcement at trial submit.

A task with a license not in the team's allowlist 403s. A task with no
license tag passes (hand-authored tasks). A task with an allowlisted
license submits cleanly."""

import hashlib
from collections.abc import Iterator
from datetime import UTC, datetime
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, delete, insert, update
from sqlalchemy.orm import sessionmaker

from loom.db.schema import Task, Team, TeamQuota, Token
from loom_control_plane.app import create_app
from loom_control_plane.config import ControlPlaneSettings


@pytest.fixture
def seed(postgres_url: str) -> Iterator[dict]:
    engine = create_engine(postgres_url)
    session_local = sessionmaker(engine)
    team_id = uuid4()
    raw = f"team_{uuid4().hex}"
    now = datetime.now(UTC)
    with session_local() as s:
        s.execute(insert(Team).values(id=team_id, name=f"t-{team_id}"))
        # Default allowlist will come via the DB default (MIT, Apache-2.0,
        # BSD-3-Clause, CC-BY-4.0).
        s.execute(insert(TeamQuota).values(team_id=team_id))
        s.execute(insert(Token).values(
            token_hash=hashlib.sha256(raw.encode()).digest(),
            type="team", scopes=["submit"], team_id=team_id,
            issued_at=now, expires_at=None,
        ))
        # Three tasks: MIT (allowed), GPL-3.0 (NOT in default), no license
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
        # Empty-string license — a misconfigured importer might write
        # this. It must NOT bypass enforcement (audit M3).
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
            json={"task_id": "mit-task", "config": {}},
        )
        assert r.status_code == 201, r.text


def test_gpl_task_rejected_with_403(app, seed):  # type: ignore[no-untyped-def]
    with TestClient(app) as client:
        r = client.post(
            "/trials",
            headers={"Authorization": f"Bearer {seed['token']}"},
            json={"task_id": "gpl-task", "config": {}},
        )
        assert r.status_code == 403
        assert "GPL-3.0-only" in r.json()["detail"]


def test_no_license_task_accepted(app, seed):  # type: ignore[no-untyped-def]
    """Hand-authored tasks have license=NULL — those pass through
    unchecked (allowlist enforcement only applies to tagged tasks)."""
    with TestClient(app) as client:
        r = client.post(
            "/trials",
            headers={"Authorization": f"Bearer {seed['token']}"},
            json={"task_id": "no-license-task", "config": {}},
        )
        assert r.status_code == 201, r.text


def test_empty_string_license_rejected(app, seed):  # type: ignore[no-untyped-def]
    """A task with license='' (empty string from a buggy importer)
    must 403, not bypass — empty-string is NOT None and shouldn't be
    in any sane allowlist (audit M3)."""
    with TestClient(app) as client:
        r = client.post(
            "/trials",
            headers={"Authorization": f"Bearer {seed['token']}"},
            json={"task_id": "empty-license-task", "config": {}},
        )
        assert r.status_code == 403


def test_tightened_allowlist_rejects_mit(app, seed, postgres_url):  # type: ignore[no-untyped-def]
    """Operator-tightened allowlist: shrinking the array means previously-
    allowed tasks now 403."""
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
            json={"task_id": "mit-task", "config": {}},
        )
        assert r.status_code == 403
        assert "MIT" in r.json()["detail"]
