"""Real shared-DB login proofs stay within their originating application."""

from __future__ import annotations

import json
from contextlib import AsyncExitStack
from uuid import uuid4

import httpx
import pytest
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from loom.db.schema import Team, TeamMembership, User
from loom_service.app import create_app
from loom_service.config import LoomServiceSettings
from loom_service.password_auth import hash_password

ALICE = {
    "schema_version": "loom.application-session-audience.v1",
    "application_id": "c18a28fe-22e1-4aeb-ab82-fd496c38309e",
    "origin": "https://alice.dev.example.com",
    "access_generation": 1,
}
COOKIE = "__Host-loom_session"
PASSWORD = "shared-development-test-passphrase"


@pytest.fixture
async def audience_apps(isolated_migration_postgres_url):
    """Use real routes/transactions, without starting unrelated background loops."""
    engine = create_async_engine(isolated_migration_postgres_url)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    team_a, team_b, user_id = uuid4(), uuid4(), uuid4()
    try:
        async with factory() as session:
            session.add_all([
                Team(id=team_a, name="Audience A"), Team(id=team_b, name="Audience B"),
                User(id=user_id, username="owner", username_normalized="owner",
                     email="owner@example.com", status="active", password_hash=hash_password(PASSWORD)),
            ])
            await session.flush()
            session.add_all([
                TeamMembership(team_id=team_a, user_id=user_id, role="owner"),
                TeamMembership(team_id=team_b, user_id=user_id, role="owner"),
            ])
            await session.commit()
        async with AsyncExitStack() as clients:
            async def client(audience):
                origin = audience["origin"] if audience else ALICE["origin"]
                settings = LoomServiceSettings(
                    _env_file=None, db_url=isolated_migration_postgres_url,
                    minio_access_key="x", minio_secret_key="y", public_base_url=origin,
                    auth_local_http=False, auth_return_login_token=True,
                    auth_session_audience_json=json.dumps(audience) if audience else None,
                )
                app = create_app(settings)
                app.state.settings = settings
                app.state.session_factory = factory
                return await clients.enter_async_context(httpx.AsyncClient(
                    transport=httpx.ASGITransport(app=app), base_url=origin,
                ))
            yield client, team_a, team_b
    finally:
        await engine.dispose()


async def _challenge(client):
    response = await client.post("/api/v1/auth/login/start", json={"email": "owner@example.com"})
    assert response.status_code == 200, response.text
    return response.json()["login_token"]


async def _password_login(client):
    response = await client.post("/api/v1/auth/login", json={"username": "owner", "password": PASSWORD})
    assert response.status_code == 200, response.text
    return response


def _cookie_headers(raw):
    # Explicitly copy the secret: a browser cookie jar's domain filtering must
    # not be what makes the server-side audience test pass.
    return {"Cookie": f"{COOKIE}={raw}"}


@pytest.mark.parametrize("other", [
    ALICE | {"application_id": "13721134-1a0e-446b-848c-fc7505f993e1"},
    ALICE | {"origin": "https://bob.dev.example.com"},
    ALICE | {"access_generation": 2},
    None,
])
async def test_wrong_audience_cannot_consume_challenge_or_use_cookie(audience_apps, other):
    make_client, *_ = audience_apps
    alice, foreign = await make_client(ALICE), await make_client(other)
    token = await _challenge(alice)
    denied = await foreign.post("/api/v1/auth/login/complete", json={"token": token})
    assert denied.status_code == 400, denied.text
    complete = await alice.post("/api/v1/auth/login/complete", json={"token": token})
    assert complete.status_code == 200, complete.text
    assert (await alice.post("/api/v1/auth/login/complete", json={"token": token})).status_code == 400
    cookie = alice.cookies.get(COOKIE)
    assert (await alice.get("/api/v1/auth/me")).status_code == 200
    rejected = await foreign.get("/api/v1/auth/me", headers=_cookie_headers(cookie))
    assert rejected.status_code == 401, rejected.text
    # Neither proxy headers nor using the original Host changes process identity.
    spoofed = await foreign.get("/api/v1/auth/me", headers=_cookie_headers(cookie) | {
        "Host": "alice.dev.example.com", "X-Forwarded-Host": "alice.dev.example.com",
    })
    assert spoofed.status_code == 401, spoofed.text
    # Wrong-audience lookups did not revoke the legitimate user's session.
    assert (await alice.get("/api/v1/auth/me")).status_code == 200


async def test_legacy_challenge_and_cookie_have_no_scoped_fallback(audience_apps):
    make_client, *_ = audience_apps
    legacy, scoped = await make_client(None), await make_client(ALICE)
    token = await _challenge(legacy)
    assert (await scoped.post("/api/v1/auth/login/complete", json={"token": token})).status_code == 400
    response = await legacy.post("/api/v1/auth/login/complete", json={"token": token})
    assert response.status_code == 200, response.text
    rejected = await scoped.get("/api/v1/auth/me", headers=_cookie_headers(legacy.cookies.get(COOKIE)))
    assert rejected.status_code == 401, rejected.text


async def test_password_team_switch_refresh_and_logout_remain_application_local(audience_apps):
    make_client, _team_a, team_b = audience_apps
    alice = await make_client(ALICE)
    bob = await make_client(ALICE | {"origin": "https://bob.dev.example.com"})
    login = await _password_login(alice)
    await _password_login(bob)
    original_cookie = alice.cookies.get(COOKIE)
    rejected = await bob.get("/api/v1/auth/me", headers=_cookie_headers(original_cookie))
    assert rejected.status_code == 401, rejected.text
    switched = await alice.post("/api/v1/auth/team", json={"team_id": str(team_b)},
                                headers={"X-Loom-CSRF": login.json()["csrf_token"]})
    assert switched.status_code == 200, switched.text
    assert switched.json()["current_team"]["id"] == str(team_b)
    refreshed = await alice.post("/api/v1/auth/refresh", headers={
        "X-Loom-CSRF": switched.json()["csrf_token"],
    })
    assert refreshed.status_code == 200, refreshed.text
    rotated = alice.cookies.get(COOKIE)
    assert rotated != original_cookie
    assert (await alice.get("/api/v1/auth/me", headers=_cookie_headers(original_cookie))).status_code == 401
    assert (await bob.get("/api/v1/auth/me", headers=_cookie_headers(rotated))).status_code == 401
    logout = await alice.post("/api/v1/auth/logout", headers={
        "X-Loom-CSRF": refreshed.json()["csrf_token"],
    })
    assert logout.status_code == 204, logout.text
    assert (await alice.get("/api/v1/auth/me", headers=_cookie_headers(rotated))).status_code == 401
    assert (await bob.get("/api/v1/auth/me")).status_code == 200


async def test_invite_session_is_scoped_and_foreign_cookie_cannot_select_identity(audience_apps):
    make_client, team_a, _team_b = audience_apps
    alice = await make_client(ALICE)
    bob = await make_client(ALICE | {"origin": "https://bob.dev.example.com"})
    owner = await _password_login(alice)
    invite = await alice.post("/api/v1/invites", json={
        "team_id": str(team_a), "role": "member", "email": "invited@example.com",
    }, headers={"X-Loom-CSRF": owner.json()["csrf_token"]})
    assert invite.status_code == 201, invite.text
    accepted = await bob.post("/api/v1/invites/accept", json={
        "code": invite.json()["invite_code"], "email": "invited@example.com",
    }, headers=_cookie_headers(alice.cookies.get(COOKIE)))
    # If Bob accepts Alice's session, the owner email conflicts with the invite.
    assert accepted.status_code == 200, accepted.text
    assert (await bob.get("/api/v1/auth/me")).status_code == 200
    rejected = await alice.get("/api/v1/auth/me", headers=_cookie_headers(bob.cookies.get(COOKIE)))
    assert rejected.status_code == 401, rejected.text
