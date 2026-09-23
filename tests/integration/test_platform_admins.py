"""Real Postgres and service HTTP coverage of the #802 authority boundary."""

from __future__ import annotations

import asyncio
import json
import os
import socket
import sys
from datetime import UTC, datetime
from uuid import uuid4

import httpx
import pytest
import uvicorn
from sqlalchemy import delete, func, select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from loom.admin_secret import AdminSecretVerifier
from loom.db.schema import AdminAuditEvent, Team, TeamMembership, Token, User, UserSession
from loom_service.app import create_app
from loom_service.config import LoomServiceSettings
from loom_service.password_auth import hash_password

PASSWORD = "issue-802-disposable-passphrase"
OPERATOR_SECRET = "loom_admin_" + "T" * 43


@pytest.fixture
async def admin_setup(monkeypatch, postgres_url):
    for key, value in {
        "LOOM_SVC_DB_URL": postgres_url,
        "LOOM_SVC_MINIO_ENDPOINT": "http://minio:9000",
        "LOOM_SVC_MINIO_ACCESS_KEY": "test",
        "LOOM_SVC_MINIO_SECRET_KEY": "test",
        "LOOM_SVC_CONTROL_PLANE_URL": "http://cp:8080/",
        "LOOM_SVC_GATEWAY_URL": "http://gw:9100/",
    }.items():
        monkeypatch.setenv(key, value)
    settings = LoomServiceSettings(_env_file=None)
    app = create_app(settings)
    engine = create_async_engine(postgres_url)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    app.state.settings = settings
    app.state.session_factory = factory
    app.state.admin_secret_verifier = AdminSecretVerifier.from_token(OPERATOR_SECRET)
    ids = {name: uuid4() for name in ("actor", "target", "other", "team", "second_team")}
    async with factory() as session:
        admin_team = (await session.execute(
            select(Team).where(func.lower(Team.name) == "admin"),
        )).scalar_one_or_none()
        created_admin_team = admin_team is None
        if admin_team is None:
            admin_team = Team(id=uuid4(), name="admin")
            session.add(admin_team)
        ids["admin_team"] = admin_team.id
        for index, name in enumerate(("team", "second_team")):
            session.add(Team(id=ids[name], name=f"issue802-{index}-{ids[name]}"))
        for name in ("actor", "target", "other"):
            session.add(User(
                id=ids[name], username=f"issue802-{name}", username_normalized=f"issue802-{name}",
                # Deliberately ambiguous display names: only UUID selects a target.
                display_name="Same Name", email=None, status="active",
                password_hash=hash_password(PASSWORD), password_set_at=datetime.now(UTC),
                is_platform_admin=name == "actor",
            ))
        await session.flush()
        for name in ("actor", "target", "other"):
            session.add(TeamMembership(user_id=ids[name], team_id=ids["team"], role="owner"))
        session.add(TeamMembership(user_id=ids["target"], team_id=ids["second_team"], role="viewer"))
        await session.commit()
    try:
        yield app, factory, ids
    finally:
        async with factory() as session:
            user_ids = [ids[name] for name in ("actor", "target", "other")]
            await session.execute(delete(AdminAuditEvent))
            await session.execute(delete(Token).where(Token.created_by_user_id.in_(user_ids)))
            await session.execute(delete(User).where(User.id.in_(user_ids)))
            team_ids = [ids["team"], ids["second_team"]]
            if created_admin_team:
                team_ids.append(ids["admin_team"])
            await session.execute(delete(Team).where(Team.id.in_(team_ids)))
            await session.commit()
        await engine.dispose()


async def _login(client, name):
    response = await client.post("/api/v1/auth/login", json={
        "username": f"issue802-{name}", "password": PASSWORD,
    })
    assert response.status_code == 200, response.text
    client.headers["X-Loom-CSRF"] = response.json()["csrf_token"]
    return response.json()


async def _mint(client, team_id=None):
    body = {"name": "issue802", "type": "team", "scopes": ["read:own"], "expires_in_days": 1}
    if team_id:
        body["team_id"] = str(team_id)
    response = await client.post("/api/v1/tokens", json=body, headers={"X-Loom-Admin-Actor": "test"})
    assert response.status_code == 201, response.text
    return response.json()["token"]


def _path(ids, operation):
    return f"/api/v1/admin/users/{ids['target']}/platform-admin/{operation}"


@pytest.mark.parametrize("actor_kind", ["session", "user_token", "operator"])
async def test_grant_revoke_sessions_tokens_and_idempotency(admin_setup, actor_kind):
    app, factory, ids = admin_setup
    transport = httpx.ASGITransport(app=app)
    async with (
        httpx.AsyncClient(transport=transport, base_url="http://svc") as actor,
        httpx.AsyncClient(transport=transport, base_url="http://svc") as target,
        httpx.AsyncClient(transport=transport, base_url="http://svc") as other,
    ):
        await _login(actor, "actor")
        await _login(target, "target")
        await _login(other, "other")
        raw_session = target.cookies.get("loom_session")
        raw_token = await _mint(target)
        token_headers = {"Authorization": f"Bearer {raw_token}"}
        assert (await target.post(_path(ids, "grant"), json={})).status_code == 403
        assert (await target.get("/api/v1/admin/audit-events", headers=token_headers)).status_code == 403
        if actor_kind == "user_token":
            actor.headers["Authorization"] = f"Bearer {await _mint(actor)}"
            actor.cookies.clear()
        elif actor_kind == "operator":
            actor.headers["Authorization"] = f"Bearer {OPERATOR_SECRET}"
            actor.cookies.clear()
            assert (await actor.post(_path(ids, "grant"), json={})).status_code == 400
        actor.headers["X-Loom-Admin-Actor"] = "operator-802"
        actor.headers["X-Request-ID"] = "issue-802-functional-test"
        granted = await actor.post(_path(ids, "grant"), json={})
        assert granted.status_code == 200, granted.text
        assert granted.json()["before"] == {"is_platform_admin": False, "admin_team_role": None}
        assert granted.json()["after"] == {"is_platform_admin": True, "admin_team_role": "owner"}
        repeated = await actor.post(_path(ids, "grant"), json={})
        assert repeated.status_code == 200 and repeated.json()["changed"] is False
        assert (await target.get("/api/v1/admin/audit-events")).status_code == 200
        assert (await target.get("/api/v1/admin/audit-events", headers=token_headers)).status_code == 200
        # A platform admin can issue cross-team credentials, which also must be revoked.
        cross_team_token = await _mint(target, ids["admin_team"])
        missing_policy = await actor.post(_path(ids, "revoke"), json={})
        assert missing_policy.status_code == 422
        revoked = await actor.post(_path(ids, "revoke"), json={"credential_policy": "revoke_all"})
        assert revoked.status_code == 200, revoked.text
        assert revoked.json()["after"] == {"is_platform_admin": False, "admin_team_role": None}
        assert revoked.json()["revoked_sessions"] == 1
        assert revoked.json()["revoked_user_tokens"] == 2
        repeated = await actor.post(_path(ids, "revoke"), json={"credential_policy": "revoke_all"})
        assert repeated.status_code == 200 and repeated.json()["changed"] is False
        assert repeated.json()["revoked_sessions"] == repeated.json()["revoked_user_tokens"] == 0
        assert (await target.get("/api/v1/auth/me")).status_code == 401
        for token in (raw_token, cross_team_token):
            assert (await target.get("/api/v1/admin/audit-events", headers={
                "Authorization": f"Bearer {token}",
            })).status_code == 401
        assert (await other.get("/api/v1/auth/me")).status_code == 200
        me = await _login(target, "target")
        assert me["is_platform_admin"] is False
        assert (await target.get("/api/v1/admin/audit-events")).status_code == 403
        ordinary_token = await _mint(target)
        ordinary_headers = {"Authorization": f"Bearer {ordinary_token}"}
        assert (await target.get("/api/v1/tokens", headers=ordinary_headers)).status_code == 200
        assert (await target.post(_path(ids, "grant"), json={}, headers=ordinary_headers)).status_code == 403
        events = (await actor.get("/api/v1/admin/audit-events")).json()["items"]
        events = [e for e in events if e["target_id"] == str(ids["target"])]
        assert len(events) == 4
        assert {e["actor"] for e in events} == {
            "operator-802" if actor_kind == "operator" else "user:issue802-actor",
        }
        for event in events:
            assert event["request_id"] == "issue-802-functional-test"
            assert "before" in event["metadata"] and "after" in event["metadata"]
        serialized = json.dumps(events)
        for secret in (PASSWORD, OPERATOR_SECRET, raw_session, raw_token, cross_team_token):
            assert secret not in serialized
    async with factory() as session:
        memberships = dict((await session.execute(select(
            TeamMembership.team_id, TeamMembership.role,
        ).where(TeamMembership.user_id == ids["target"]))).all())
        assert memberships == {ids["team"]: "owner", ids["second_team"]: "viewer"}
        assert not (await session.get(User, ids["other"])).is_platform_admin


@pytest.mark.parametrize("operation", ["grant", "revoke"])
async def test_audit_failure_rolls_back_all_mutations(admin_setup, monkeypatch, operation):
    app, factory, ids = admin_setup
    transport = httpx.ASGITransport(app=app, raise_app_exceptions=False)
    async with (
        httpx.AsyncClient(transport=transport, base_url="http://svc") as actor,
        httpx.AsyncClient(transport=transport, base_url="http://svc") as target,
    ):
        await _login(actor, "actor")
        await _login(target, "target")
        token = await _mint(target)
        if operation == "revoke":
            assert (await actor.post(_path(ids, "grant"), json={})).status_code == 200

        async def fail_audit(*args, **kwargs):
            raise RuntimeError("simulated audit storage failure")

        monkeypatch.setattr("loom_service.routes.platform_admins.write_admin_audit_event", fail_audit)
        body = {} if operation == "grant" else {"credential_policy": "revoke_all"}
        response = await actor.post(_path(ids, operation), json=body)
        assert response.status_code == 500
        assert (await target.get("/api/v1/auth/me")).status_code == 200
        assert (await target.get("/api/v1/tokens", headers={"Authorization": f"Bearer {token}"})).status_code == 200
    async with factory() as session:
        assert (await session.get(User, ids["target"])).is_platform_admin == (operation == "revoke")
        membership = await session.get(TeamMembership, (ids["admin_team"], ids["target"]))
        assert (membership is not None) == (operation == "revoke")
        assert (await session.execute(select(func.count()).select_from(UserSession).where(
            UserSession.user_id == ids["target"], UserSession.revoked_at.is_not(None),
        ))).scalar_one() == 0
        assert (await session.execute(select(func.count()).select_from(AdminAuditEvent).where(
            AdminAuditEvent.target_id == str(ids["target"]),
        ))).scalar_one() == (1 if operation == "revoke" else 0)


@pytest.mark.parametrize("operation", ["grant", "revoke"])
async def test_fail_closed_targets_and_authorization(admin_setup, operation):
    app, factory, ids = admin_setup
    transport = httpx.ASGITransport(app=app)
    body = {} if operation == "grant" else {"credential_policy": "revoke_all"}
    async with httpx.AsyncClient(transport=transport, base_url="http://svc") as actor:
        assert (await actor.post(_path(ids, operation), json=body)).status_code == 401
        await _login(actor, "actor")
        csrf = actor.headers.pop("X-Loom-CSRF")
        assert (await actor.post(_path(ids, operation), json=body)).status_code == 403
        actor.headers["X-Loom-CSRF"] = csrf
        for selector in ("Same Name", "same@example.test", str(ids["target"])[:8]):
            assert (await actor.post(
                f"/api/v1/admin/users/{selector}/platform-admin/{operation}", json=body,
            )).status_code == 422
        assert (await actor.post(_path({"target": uuid4()}, operation), json=body)).status_code == 404
        assert (await actor.post(_path(ids, operation), json={**body, "email": "same@example.test"})).status_code == 422
        for status, disabled in (("pending_setup", None), ("active", datetime.now(UTC))):
            async with factory() as session:
                user = await session.get(User, ids["target"])
                user.status, user.disabled_at = status, disabled
                await session.commit()
            assert (await actor.post(_path(ids, operation), json=body)).status_code == 409
        async with factory() as session:
            user = await session.get(User, ids["actor"])
            user.disabled_at = datetime.now(UTC)
            await session.commit()
        assert (await actor.post(_path(ids, operation), json=body)).status_code in {401, 403}


@pytest.mark.parametrize("team_state", ["absent", "disabled", "ambiguous"])
async def test_reserved_team_fails_closed(admin_setup, team_state):
    app, factory, ids = admin_setup
    extra_id = uuid4()
    async with factory() as session:
        team = await session.get(Team, ids["admin_team"])
        original_name = team.name
        if team_state == "absent":
            team.name = f"renamed-{extra_id}"
        elif team_state == "disabled":
            team.disabled_at = datetime.now(UTC)
        else:
            session.add(Team(id=extra_id, name="ADMIN"))
        await session.commit()
    try:
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://svc") as actor:
            await _login(actor, "actor")
            response = await actor.post(_path(ids, "grant"), json={})
            assert response.status_code == 409, response.text
        async with factory() as session:
            assert not (await session.get(User, ids["target"])).is_platform_admin
    finally:
        async with factory() as session:
            await session.execute(delete(Team).where(Team.id == extra_id))
            team = await session.get(Team, ids["admin_team"])
            team.name, team.disabled_at = original_name, None
            await session.commit()


async def test_optional_membership_and_self_revocation(admin_setup):
    app, _factory, ids = admin_setup
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://svc") as actor:
        await _login(actor, "actor")
        response = await actor.post(_path(ids, "grant"), json={"ensure_admin_team": False})
        assert response.status_code == 200
        assert response.json()["after"] == {"is_platform_admin": True, "admin_team_role": None}
        # Self-revocation returns its audit result, then invalidates the calling session.
        response = await actor.post(_path({"target": ids["actor"]}, "revoke"), json={
            "credential_policy": "revoke_all",
        })
        assert response.status_code == 200
        assert (await actor.get("/api/v1/admin/audit-events")).status_code == 401


async def test_cli_against_real_http_and_postgres(admin_setup, tmp_path):
    """Exercise the shipped CLI subprocess and real loopback HTTP, no transport mocks."""
    app, _factory, ids = admin_setup
    listener = socket.socket()
    listener.bind(("127.0.0.1", 0))
    port = listener.getsockname()[1]
    server = uvicorn.Server(uvicorn.Config(app, lifespan="off", log_level="error"))
    serving = asyncio.create_task(server.serve(sockets=[listener]))
    env = {**os.environ, "XDG_CONFIG_HOME": str(tmp_path), "ISSUE802_PASSWORD": PASSWORD}

    async def cli(*args):
        process = await asyncio.create_subprocess_exec(
            sys.executable, "-m", "loom_cli", *args, env=env,
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await asyncio.wait_for(process.communicate(), timeout=20)
        assert PASSWORD.encode() not in stdout + stderr
        return process.returncode, stdout.decode(), stderr.decode()

    try:
        async with asyncio.timeout(10):
            while not server.started:
                if serving.done():
                    await serving
                await asyncio.sleep(0.01)
        code, _out, err = await cli("auth", "login", "--server", f"http://127.0.0.1:{port}",
                                    "--username", "issue802-actor", "--password", "env:ISSUE802_PASSWORD")
        assert code == 0, err
        code, out, err = await cli("admin", "platform-admin", "grant", "--user-id", str(ids["target"]))
        assert code == 0, err
        assert json.loads(out)["after"]["is_platform_admin"] is True
        code, _out, err = await cli("admin", "platform-admin", "grant", "--user-id", str(uuid4()))
        assert code == 1 and "404" in err
        code, out, err = await cli("admin", "platform-admin", "revoke", "--user-id", str(ids["target"]),
                                   "--credential-policy", "revoke_all")
        assert code == 0, err
        assert json.loads(out)["after"]["is_platform_admin"] is False
    finally:
        server.should_exit = True
        await asyncio.wait_for(serving, timeout=10)
        listener.close()
