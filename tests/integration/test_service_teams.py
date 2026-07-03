"""GET /api/v1/teams/{team_id} (Plan 20 Task 3)."""

from __future__ import annotations

import hashlib
from collections.abc import AsyncIterator
from datetime import UTC, datetime
from uuid import UUID, uuid4

import boto3
import httpx
import pytest
from botocore.config import Config
from fastapi import FastAPI
from sqlalchemy import create_engine, delete, insert
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.orm import sessionmaker

from loom.admin_secret import AdminSecretVerifier
from loom.db.schema import (
    AdminAuditEvent,
    Team,
    TeamMembership,
    TeamQuota,
    Token,
    User,
)
from loom_service.app import create_app
from loom_service.config import LoomServiceSettings

RAW_ADMIN_TOKEN = "loom_admin_" + "T" * 43


@pytest.fixture
async def teams_setup(
    monkeypatch: pytest.MonkeyPatch, postgres_url: str,
) -> AsyncIterator[tuple[FastAPI, str, str, UUID, UUID]]:
    for k, v in {
        "LOOM_SVC_DB_URL": postgres_url,
        "LOOM_SVC_MINIO_ENDPOINT": "http://minio:9000",
        "LOOM_SVC_MINIO_ACCESS_KEY": "x",
        "LOOM_SVC_MINIO_SECRET_KEY": "y",
        "LOOM_SVC_CONTROL_PLANE_URL": "http://cp:8080/",
        "LOOM_SVC_GATEWAY_URL": "http://gw:9100/",
    }.items():
        monkeypatch.setenv(k, v)
    settings = LoomServiceSettings(_env_file=None)
    app = create_app(settings)
    engine = create_async_engine(str(settings.db_url))
    app.state.settings = settings
    app.state.session_factory = async_sessionmaker(
        engine, expire_on_commit=False,
    )
    app.state.admin_secret_verifier = AdminSecretVerifier.from_token(
        RAW_ADMIN_TOKEN,
    )
    app.state.minio_client = boto3.client(
        "s3",
        endpoint_url=settings.minio_endpoint,
        aws_access_key_id=settings.minio_access_key.get_secret_value(),
        aws_secret_access_key=settings.minio_secret_key.get_secret_value(),
        region_name=settings.minio_region,
        config=Config(signature_version="s3v4"),
    )
    app.state.http_client = httpx.AsyncClient(base_url="http://cp")
    app.state.gateway_client = httpx.AsyncClient(base_url="http://gw")

    team_a, team_b = uuid4(), uuid4()
    team_a_raw = f"loom_team_{uuid4().hex}"
    sync_engine = create_engine(postgres_url)
    sl = sessionmaker(sync_engine)
    with sl() as s:
        s.execute(insert(Team).values(id=team_a, name=f"A-{team_a}"))
        s.execute(insert(Team).values(id=team_b, name=f"B-{team_b}"))
        # TeamQuota for team A only — team B has no quota to test the
        # quota=None branch.
        s.execute(insert(TeamQuota).values(team_id=team_a))
        owner_id, member_id = uuid4(), uuid4()
        now = datetime.now(UTC)
        s.execute(insert(User).values(
            id=owner_id,
            email="owner@example.com",
            display_name="Owner Example",
            is_platform_admin=False,
            created_at=now,
        ))
        s.execute(insert(User).values(
            id=member_id,
            email="member@example.com",
            display_name=None,
            is_platform_admin=False,
            created_at=now,
        ))
        s.execute(insert(TeamMembership).values(
            team_id=team_a, user_id=owner_id, role="owner", created_at=now,
        ))
        s.execute(insert(TeamMembership).values(
            team_id=team_a, user_id=member_id, role="member", created_at=now,
        ))
        s.execute(insert(Token).values(
            token_hash=hashlib.sha256(team_a_raw.encode()).digest(),
            type="team", scopes=["read:own"], team_id=team_a,
            issued_at=now,
        ))
        s.execute(insert(Token).values(
            token_hash=hashlib.sha256(b"member-2").digest(),
            type="team", scopes=["submit"], team_id=team_a,
            issued_at=now,
        ))
        s.commit()
    try:
        yield app, team_a_raw, str(team_a), team_a, team_b
    finally:
        await app.state.gateway_client.aclose()
        await app.state.http_client.aclose()
        await engine.dispose()
        with sl() as s:
            s.execute(delete(AdminAuditEvent))
            s.execute(delete(Token))
            s.execute(delete(TeamMembership))
            s.execute(delete(User))
            s.execute(delete(TeamQuota))
            s.execute(delete(Team))
            s.commit()
        sync_engine.dispose()


def _admin_headers(actor: str | None = "ops-admin") -> dict[str, str]:
    headers = {"Authorization": f"Bearer {RAW_ADMIN_TOKEN}"}
    if actor is not None:
        headers["X-Loom-Admin-Actor"] = actor
    return headers


def _counter_value(
    metric_name: str,
    sample_name: str,
    labels: dict[str, str],
) -> float:
    from prometheus_client import REGISTRY

    for metric in REGISTRY.collect():
        if metric.name != metric_name:
            continue
        for sample in metric.samples:
            if sample.name == sample_name and all(
                sample.labels.get(k) == v for k, v in labels.items()
            ):
                return float(sample.value)
    return 0.0


async def test_get_own_team_with_quota_and_members(
    teams_setup: tuple[FastAPI, str, str, UUID, UUID],
) -> None:
    app, raw, team_a_str, _, _ = teams_setup
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport, base_url="http://svc",
    ) as ac:
        r = await ac.get(
            f"/api/v1/teams/{team_a_str}",
            headers={"Authorization": f"Bearer {raw}"},
        )
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["id"] == team_a_str
    assert "A-" in body["name"]
    # Quota row exists for team A.
    quota = body["quota"]
    assert quota is not None
    assert quota["fair_share_weight"] == 1.0
    assert quota["max_attempts_ceiling"] == 3
    assert "MIT" in quota["license_allowlist"]
    # Both tokens (the caller + member-2) appear in members.
    assert len(body["members"]) == 2
    scopes_seen = {tuple(m["scopes"]) for m in body["members"]}
    assert ("read:own",) in scopes_seen
    assert ("submit",) in scopes_seen
    # No raw secret leaks — only an 8-char hex prefix.
    for m in body["members"]:
        assert len(m["token_hash_prefix"]) == 8
    assert body["user_members"] == [
        {
            "user_id": body["user_members"][0]["user_id"],
            "email": "member@example.com",
            "display_name": None,
            "role": "member",
            "joined_at": body["user_members"][0]["joined_at"],
        },
        {
            "user_id": body["user_members"][1]["user_id"],
            "email": "owner@example.com",
            "display_name": "Owner Example",
            "role": "owner",
            "joined_at": body["user_members"][1]["joined_at"],
        },
    ]


async def test_team_b_has_no_quota(
    teams_setup: tuple[FastAPI, str, str, UUID, UUID],
    postgres_url: str,
) -> None:
    """Team without a TeamQuota row returns `quota: null`."""
    app, _raw_a, _team_a_str, _, team_b = teams_setup
    # Mint a token for team B so we can read it.
    raw_b = f"loom_team_{uuid4().hex}"
    sync_engine = create_engine(postgres_url)
    sl = sessionmaker(sync_engine)
    with sl() as s:
        s.execute(insert(Token).values(
            token_hash=hashlib.sha256(raw_b.encode()).digest(),
            type="team", scopes=["read:own"], team_id=team_b,
            issued_at=datetime.now(UTC),
        ))
        s.commit()
    sync_engine.dispose()
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport, base_url="http://svc",
    ) as ac:
        r = await ac.get(
            f"/api/v1/teams/{team_b}",
            headers={"Authorization": f"Bearer {raw_b}"},
        )
    assert r.status_code == 200
    assert r.json()["quota"] is None


async def test_missing_auth_records_auth_failure_metric(
    teams_setup: tuple[FastAPI, str, str, UUID, UUID],
) -> None:
    app, _raw, team_a_str, _team_a, _team_b = teams_setup
    before = _counter_value(
        "loom_svc_auth_failures",
        "loom_svc_auth_failures_total",
        {"auth_kind": "anonymous", "reason": "missing_or_invalid"},
    )
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport, base_url="http://svc",
    ) as ac:
        r = await ac.get(f"/api/v1/teams/{team_a_str}")

    assert r.status_code == 401
    after = _counter_value(
        "loom_svc_auth_failures",
        "loom_svc_auth_failures_total",
        {"auth_kind": "anonymous", "reason": "missing_or_invalid"},
    )
    assert after == before + 1


async def test_admin_team_emergency_controls_update_state_and_audit(
    teams_setup: tuple[FastAPI, str, str, UUID, UUID],
) -> None:
    app, raw, _team_a_str, team_a, _team_b = teams_setup
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport, base_url="http://svc",
    ) as ac:
        paused = await ac.post(
            f"/api/v1/admin/teams/{team_a}/pause-submissions",
            headers=_admin_headers("incident-commander"),
            json={"reason": "provider incident"},
        )
        paused_detail = await ac.get(
            f"/api/v1/teams/{team_a}",
            headers=_admin_headers(),
        )
        resumed = await ac.post(
            f"/api/v1/admin/teams/{team_a}/resume-submissions",
            headers=_admin_headers("incident-commander"),
            json={"reason": "provider restored"},
        )
        disabled = await ac.post(
            f"/api/v1/admin/teams/{team_a}/disable",
            headers=_admin_headers("incident-commander"),
            json={"reason": "suspected token leak"},
        )
        blocked = await ac.get(
            f"/api/v1/teams/{team_a}",
            headers={"Authorization": f"Bearer {raw}"},
        )
        enabled = await ac.post(
            f"/api/v1/admin/teams/{team_a}/enable",
            headers=_admin_headers("incident-commander"),
            json={"reason": "leak contained"},
        )
        restored = await ac.get(
            f"/api/v1/teams/{team_a}",
            headers={"Authorization": f"Bearer {raw}"},
        )
        audit = await ac.get(
            "/api/v1/admin/audit-events?limit=10",
            headers=_admin_headers(),
        )

    assert paused.status_code == 200, paused.text
    assert paused_detail.status_code == 200, paused_detail.text
    paused_body = paused_detail.json()
    assert paused_body["submissions_paused_at"] is not None
    assert paused_body["submissions_paused_reason"] == "provider incident"
    assert resumed.status_code == 200, resumed.text
    assert disabled.status_code == 200, disabled.text
    assert blocked.status_code == 403, blocked.text
    assert "disabled" in blocked.json()["detail"]
    assert enabled.status_code == 200, enabled.text
    assert restored.status_code == 200, restored.text

    actions = [
        event["action"] for event in audit.json()["items"]
        if event["target_type"] == "team" and event["target_id"] == str(team_a)
    ]
    assert actions[:4] == [
        "team.enable",
        "team.disable",
        "team.submissions.resume",
        "team.submissions.pause",
    ]
    for event in audit.json()["items"]:
        assert "provider incident" not in str(event["metadata"])
        assert event["actor"] == "incident-commander" or event["actor"] == "ops-admin"


async def test_admin_can_list_create_and_rename_internal_teams(
    teams_setup: tuple[FastAPI, str, str, UUID, UUID],
) -> None:
    app, _raw, _team_a_str, _team_a, _team_b = teams_setup
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport, base_url="http://svc",
    ) as ac:
        initial = await ac.get("/api/v1/admin/teams", headers=_admin_headers())
        created = await ac.post(
            "/api/v1/admin/teams",
            headers=_admin_headers("ops-admin"),
            json={"name": "Research Platform"},
        )
        team_id = created.json().get("id")
        renamed = await ac.patch(
            f"/api/v1/admin/teams/{team_id}",
            headers=_admin_headers("ops-admin"),
            json={"name": "Research Platform Core"},
        )
        listed = await ac.get("/api/v1/admin/teams", headers=_admin_headers())
        duplicate = await ac.post(
            "/api/v1/admin/teams",
            headers=_admin_headers("ops-admin"),
            json={"name": "research platform core"},
        )
        audit = await ac.get(
            "/api/v1/admin/audit-events?limit=10",
            headers=_admin_headers(),
        )

    assert initial.status_code == 200, initial.text
    assert created.status_code == 201, created.text
    assert created.json()["name"] == "Research Platform"
    assert created.json()["quota"] is not None
    assert renamed.status_code == 200, renamed.text
    assert renamed.json()["name"] == "Research Platform Core"
    assert duplicate.status_code == 409, duplicate.text
    names = [item["name"] for item in listed.json()["items"]]
    assert "Research Platform Core" in names
    actions = {item["action"] for item in audit.json()["items"]}
    assert {"team.create", "team.update"}.issubset(actions)


async def test_cross_team_forbidden(
    teams_setup: tuple[FastAPI, str, str, UUID, UUID],
) -> None:
    """Team-A token cannot fetch team B."""
    app, raw, _team_a_str, _, team_b = teams_setup
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport, base_url="http://svc",
    ) as ac:
        r = await ac.get(
            f"/api/v1/teams/{team_b}",
            headers={"Authorization": f"Bearer {raw}"},
        )
    assert r.status_code == 403


async def test_unknown_team_for_team_caller_is_403(
    teams_setup: tuple[FastAPI, str, str, UUID, UUID],
) -> None:
    """Cross-team check fires BEFORE the not-found probe so a team
    caller can't enumerate which UUIDs exist."""
    app, raw, _team_a_str, _, _ = teams_setup
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport, base_url="http://svc",
    ) as ac:
        r = await ac.get(
            f"/api/v1/teams/{uuid4()}",
            headers={"Authorization": f"Bearer {raw}"},
        )
    assert r.status_code == 403


async def test_admin_can_get_any_team(
    teams_setup: tuple[FastAPI, str, str, UUID, UUID],
) -> None:
    """Admin tokens bypass the same-team check."""
    app, _raw_a, _team_a_str, _, team_b = teams_setup
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport, base_url="http://svc",
    ) as ac:
        r = await ac.get(
            f"/api/v1/teams/{team_b}",
            headers={"Authorization": f"Bearer {RAW_ADMIN_TOKEN}"},
        )
    assert r.status_code == 200
