from __future__ import annotations

import asyncio
import base64
import hashlib
import json
import logging
import warnings
from collections.abc import AsyncIterator
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4

import pytest
from fastapi import FastAPI
from fastapi.routing import APIRoute
from sqlalchemy import delete, select
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from starlette.exceptions import StarletteDeprecationWarning

with warnings.catch_warnings():
    warnings.simplefilter("ignore", StarletteDeprecationWarning)
    from starlette.testclient import TestClient

from loom.db.schema import (
    Secret,
    TaskImageBuildContainmentAttestation,
    TaskImageBuildGrant,
    TaskImageBuildGrantEvent,
    TaskImageBuildProjection,
    TaskImageBuildProjectionEvent,
)
from loom_task_image_authority import api
from loom_task_image_authority.api import create_app
from loom_task_image_authority.auth import TaskImagePrincipalVerifier
from loom_task_image_authority.config import TaskImageAuthoritySettings
from loom_task_image_authority.contracts import (
    MAX_CONTRACT_BYTES,
    TaskImageProjectionChallengeV1,
    TaskImageProjectionReceiptV1,
)
from tests.integration.test_task_image_projection_store import (
    ATTESTATION_ID,
    CHALLENGE_NONCE,
    GRANT_ID,
    NOW,
    _attestation,
    _exchange,
    _proof,
    _release_grant,
    _request,
    _revocation,
)

_BEARER = "phase2b1-api-test-node-bearer"
_BOOTSTRAP = "loom_tibp_" + "A" * 64
_SESSION = "loom_tibs_" + "B" * 64
_HEADERS = {"Authorization": f"Bearer {_BEARER}"}


def _owner_file(path: Path, payload: str | bytes) -> Path:
    path.write_bytes(payload.encode("utf-8") if isinstance(payload, str) else payload)
    path.chmod(0o600)
    return path


def _principal_document(
    *,
    bearer: str = _BEARER,
    principal_id: str = "gb10-trt-gb10-1",
    cluster: str = "gb10",
    node: str = "trt-gb10-1",
    scopes: tuple[str, ...] = ("task-image:attest", "task-image:project"),
) -> dict[str, object]:
    return {
        "schema_version": 1,
        "principals": [
            {
                "principal_id": principal_id,
                "token_sha256": hashlib.sha256(bearer.encode("utf-8")).hexdigest(),
                "slurm_cluster_id": cluster,
                "node_name": node,
                "scopes": list(scopes),
            }
        ],
    }


def _settings(
    tmp_path: Path,
    database_url: str,
    *,
    principal_document: dict[str, object] | None = None,
    keyring_document: dict[str, object] | None = None,
    **changes: object,
) -> TaskImageAuthoritySettings:
    principals = _owner_file(
        tmp_path / f"principals-{uuid4()}.json",
        json.dumps(principal_document or _principal_document()),
    )
    keyring = _owner_file(
        tmp_path / f"keyring-{uuid4()}.json",
        json.dumps(
            keyring_document
            or {
                "schema_version": 1,
                "primary": {
                    "version": 1,
                    "key_base64": base64.b64encode(b"k" * 32).decode("ascii"),
                },
                "fallbacks": [],
            }
        ),
    )
    values: dict[str, object] = {
        "principals_file": principals,
        "db_url_file": _owner_file(
            tmp_path / f"database-url-{uuid4()}",
            database_url,
        ),
        "secret_store_keyring_file": keyring,
        "tls_cert_file": _owner_file(tmp_path / f"server-{uuid4()}.pem", "test"),
        "tls_key_file": _owner_file(tmp_path / f"server-key-{uuid4()}.pem", "test"),
        "tls_client_ca_file": _owner_file(
            tmp_path / f"client-ca-{uuid4()}.pem", "test"
        ),
    }
    values.update(changes)
    return TaskImageAuthoritySettings(**values)


async def _clear_authority_rows(database_url: str) -> None:
    engine = create_async_engine(database_url)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    try:
        async with factory() as session:
            await session.execute(delete(TaskImageBuildContainmentAttestation))
            await session.execute(delete(TaskImageBuildProjectionEvent))
            await session.execute(delete(TaskImageBuildProjection))
            await session.execute(delete(TaskImageBuildGrantEvent))
            await session.execute(delete(TaskImageBuildGrant))
            await session.execute(
                delete(Secret).where(Secret.ref.like("loom://task-image-%"))
            )
            await session.commit()
    finally:
        await engine.dispose()


async def _seed_released_grant(database_url: str) -> None:
    engine = create_async_engine(database_url)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    try:
        async with factory() as session:
            await _release_grant(session)
            await session.commit()
    finally:
        await engine.dispose()


@dataclass
class _ApiContext:
    client: TestClient
    app: FastAPI
    now: list[datetime]
    settings: TaskImageAuthoritySettings


@pytest.fixture
async def authority_api(
    tmp_path: Path,
    postgres_url: str,
) -> AsyncIterator[_ApiContext]:
    await _clear_authority_rows(postgres_url)
    await _seed_released_grant(postgres_url)
    settings = _settings(tmp_path, postgres_url)
    current_now = [NOW + timedelta(seconds=4)]
    app = create_app(
        settings,
        verifier=TaskImagePrincipalVerifier.from_file(settings.principals_file),
        now_factory=lambda: current_now[0],
        challenge_nonce_factory=lambda: CHALLENGE_NONCE,
        bootstrap_token_factory=lambda: _BOOTSTRAP,
        session_token_factory=lambda: _SESSION,
    )
    try:
        with TestClient(app) as client:
            yield _ApiContext(client=client, app=app, now=current_now, settings=settings)
    finally:
        await _clear_authority_rows(postgres_url)


def _put(
    context: _ApiContext,
    path: str,
    model: object,
    *,
    headers: dict[str, str] | None = None,
):
    return context.client.put(
        path,
        headers=_HEADERS if headers is None else headers,
        json=model.model_dump(mode="json"),  # type: ignore[attr-defined]
    )


async def test_authority_routes_drive_the_exact_projection_lifecycle_and_replays(
    authority_api: _ApiContext,
) -> None:
    context = authority_api
    health = context.client.get("/healthz")
    assert health.status_code == 200
    assert health.json() == {"status": "ok"}

    route_paths = {
        route.path for route in context.app.routes if isinstance(route, APIRoute)
    }
    assert route_paths == {
        "/healthz",
        "/metrics",
        "/v1/projections/{grant_id}/challenge",
        "/v1/projections/{grant_id}/attachment",
        "/v1/projections/{grant_id}/exchange",
        "/v1/projections/{grant_id}/attestations/{generation}",
        "/v1/projections/{grant_id}/revocation",
    }
    for disabled in ("/openapi.json", "/docs", "/redoc"):
        assert context.client.get(disabled).status_code == 404

    challenge_response = _put(
        context,
        f"/v1/projections/{GRANT_ID}/challenge",
        _request(),
    )
    assert challenge_response.status_code == 200
    assert challenge_response.headers["content-type"] == "application/json"
    challenge = TaskImageProjectionChallengeV1.model_validate_json(
        challenge_response.content
    )
    assert challenge.challenge_nonce == CHALLENGE_NONCE
    challenge_replay = _put(
        context,
        f"/v1/projections/{GRANT_ID}/challenge",
        _request(),
    )
    assert challenge_replay.json() == challenge_response.json()

    context.now[0] = NOW + timedelta(seconds=6)
    receipt_response = _put(
        context,
        f"/v1/projections/{GRANT_ID}/attachment",
        _proof(),
    )
    assert receipt_response.status_code == 200
    receipt = TaskImageProjectionReceiptV1.model_validate_json(receipt_response.content)
    assert receipt.bootstrap_token == _BOOTSTRAP
    receipt_replay = _put(
        context,
        f"/v1/projections/{GRANT_ID}/attachment",
        _proof(),
    )
    assert receipt_replay.json() == receipt_response.json()

    context.now[0] = NOW + timedelta(seconds=8)
    exchange_response = _put(
        context,
        f"/v1/projections/{GRANT_ID}/exchange",
        _exchange(receipt),
    )
    assert exchange_response.status_code == 200
    assert exchange_response.json()["session_token"] == _SESSION
    exchange_replay = _put(
        context,
        f"/v1/projections/{GRANT_ID}/exchange",
        _exchange(receipt),
    )
    assert exchange_replay.json() == exchange_response.json()

    context.now[0] = NOW + timedelta(seconds=13)
    attestation = _attestation(_proof(), generation=2)
    attestation_response = _put(
        context,
        f"/v1/projections/{GRANT_ID}/attestations/2",
        attestation,
    )
    assert attestation_response.status_code == 200
    assert attestation_response.json()["attestation_id"] == str(ATTESTATION_ID)
    attestation_replay = _put(
        context,
        f"/v1/projections/{GRANT_ID}/attestations/2",
        attestation,
    )
    assert attestation_replay.json() == attestation_response.json()

    context.now[0] = NOW + timedelta(seconds=14)
    revocation_response = _put(
        context,
        f"/v1/projections/{GRANT_ID}/revocation",
        _revocation(observed_at=NOW + timedelta(seconds=13)),
    )
    assert revocation_response.status_code == 204
    assert revocation_response.content == b""
    revocation_replay = _put(
        context,
        f"/v1/projections/{GRANT_ID}/revocation",
        _revocation(observed_at=NOW + timedelta(seconds=13)),
    )
    assert revocation_replay.status_code == 204
    assert revocation_replay.content == b""


@pytest.mark.parametrize(
    ("path", "model"),
    [
        (f"/v1/projections/{uuid4()}/challenge", _request()),
        (f"/v1/projections/{uuid4()}/attachment", _proof()),
        (
            f"/v1/projections/{uuid4()}/exchange",
            _exchange(
                TaskImageProjectionReceiptV1(
                    grant_id=GRANT_ID,
                    proof_id=UUID("44444444-4444-4444-4444-444444444444"),
                    proof_sha256="b" * 64,
                    bootstrap_token=_BOOTSTRAP,
                    issued_at=NOW + timedelta(seconds=6),
                    expires_at=NOW + timedelta(seconds=40),
                )
            ),
        ),
        (f"/v1/projections/{uuid4()}/revocation", _revocation()),
        (f"/v1/projections/{GRANT_ID}/attestations/3", _attestation(_proof(), generation=2)),
    ],
)
async def test_path_and_body_identity_mismatches_are_one_bounded_conflict(
    authority_api: _ApiContext,
    path: str,
    model: object,
) -> None:
    response = _put(authority_api, path, model)

    assert response.status_code == 409
    assert response.json() == {"detail": "task-image authority conflict"}


async def test_contract_and_body_limits_are_bounded(authority_api: _ApiContext) -> None:
    path = f"/v1/projections/{GRANT_ID}/challenge"
    unknown = _request().model_dump(mode="json") | {"raw_secret": "must-not-echo"}
    response = authority_api.client.put(path, headers=_HEADERS, json=unknown)
    assert response.status_code == 422
    assert response.json() == {"detail": "invalid task-image authority contract"}
    assert "must-not-echo" not in response.text

    malformed = authority_api.client.put(
        path,
        headers={**_HEADERS, "content-type": "application/json"},
        content=b"{" + b" " * (MAX_CONTRACT_BYTES - 1),
    )
    assert malformed.status_code == 422
    assert malformed.json() == {"detail": "invalid task-image authority contract"}

    oversized = authority_api.client.put(
        path,
        headers={**_HEADERS, "content-type": "application/json"},
        content=b"{" + b" " * MAX_CONTRACT_BYTES,
    )
    assert oversized.status_code == 413
    assert oversized.json() == {"detail": "task-image authority request too large"}


async def test_authentication_scope_and_store_failures_use_bounded_responses(
    authority_api: _ApiContext,
    tmp_path: Path,
) -> None:
    path = f"/v1/projections/{GRANT_ID}/challenge"
    payload = _request().model_dump(mode="json")
    for headers in ({}, {"Authorization": "Bearer wrong-private-bearer"}):
        response = authority_api.client.put(path, headers=headers, json=payload)
        assert response.status_code == 401
        assert response.json() == {"detail": "invalid task-image authority credentials"}
        assert "wrong-private-bearer" not in response.text

    duplicate = authority_api.client.put(
        path,
        headers=[
            ("Authorization", f"Bearer {_BEARER}"),
            ("Authorization", "Bearer duplicate-private-bearer"),
        ],
        json=payload,
    )
    assert duplicate.status_code == 401
    assert duplicate.json() == {
        "detail": "invalid task-image authority credentials"
    }
    assert "duplicate-private-bearer" not in duplicate.text

    unauthenticated_malformed = authority_api.client.put(
        path,
        headers={"content-type": "application/json"},
        content=b"{",
    )
    assert unauthenticated_malformed.status_code == 401
    assert unauthenticated_malformed.json() == {
        "detail": "invalid task-image authority credentials"
    }

    attest_only_bearer = "attest-only-private-bearer"
    attest_only = _owner_file(
        tmp_path / "attest-only-principals.json",
        json.dumps(
            _principal_document(
                bearer=attest_only_bearer,
                scopes=("task-image:attest",),
            )
        ),
    )
    scoped_app = create_app(
        authority_api.settings,
        verifier=TaskImagePrincipalVerifier.from_file(attest_only),
        now_factory=lambda: NOW + timedelta(seconds=4),
    )
    with TestClient(scoped_app) as scoped_client:
        forbidden = scoped_client.put(
            path,
            headers={"Authorization": f"Bearer {attest_only_bearer}"},
            json=payload,
        )
    assert forbidden.status_code == 403
    assert forbidden.json() == {"detail": "task-image authority forbidden"}
    assert attest_only_bearer not in forbidden.text

    project_only_bearer = "project-only-private-bearer"
    project_only = _owner_file(
        tmp_path / "project-only-principals.json",
        json.dumps(
            _principal_document(
                bearer=project_only_bearer,
                scopes=("task-image:project",),
            )
        ),
    )
    project_app = create_app(
        authority_api.settings,
        verifier=TaskImagePrincipalVerifier.from_file(project_only),
        now_factory=lambda: NOW + timedelta(seconds=13),
    )
    with TestClient(project_app) as project_client:
        forbidden_attestation = project_client.put(
            f"/v1/projections/{GRANT_ID}/attestations/2",
            headers={"Authorization": f"Bearer {project_only_bearer}"},
            json=_attestation(_proof(), generation=2).model_dump(mode="json"),
        )
    assert forbidden_attestation.status_code == 403
    assert forbidden_attestation.json() == {
        "detail": "task-image authority forbidden"
    }
    assert project_only_bearer not in forbidden_attestation.text

    first = authority_api.client.put(path, headers=_HEADERS, json=payload)
    assert first.status_code == 200
    changed = _request(cgroup_inode=987655)
    conflict = _put(authority_api, path, changed)
    assert conflict.status_code == 409
    assert conflict.json() == {"detail": "task-image authority conflict"}

    authority_api.now[0] = NOW + timedelta(seconds=65)
    expired = _put(authority_api, path, _request())
    assert expired.status_code == 403
    assert expired.json() == {"detail": "task-image authority rejected"}


@pytest.mark.parametrize(
    ("bearer", "principal_document"),
    [
        (
            "wrong-node-private-bearer",
            _principal_document(
                bearer="wrong-node-private-bearer",
                principal_id="gb10-trt-gb10-2",
                node="trt-gb10-2",
            ),
        ),
        (
            "wrong-cluster-private-bearer",
            _principal_document(
                bearer="wrong-cluster-private-bearer",
                principal_id="oldlab-trt-eai-oldlab-3",
                cluster="oldlab",
                node="trt-eai-oldlab-3",
            ),
        ),
    ],
)
async def test_exchange_rejects_wrong_node_or_cluster_at_the_http_boundary(
    authority_api: _ApiContext,
    tmp_path: Path,
    bearer: str,
    principal_document: dict[str, object],
) -> None:
    challenge = _put(
        authority_api,
        f"/v1/projections/{GRANT_ID}/challenge",
        _request(),
    )
    assert challenge.status_code == 200
    authority_api.now[0] = NOW + timedelta(seconds=6)
    receipt_response = _put(
        authority_api,
        f"/v1/projections/{GRANT_ID}/attachment",
        _proof(),
    )
    receipt = TaskImageProjectionReceiptV1.model_validate_json(receipt_response.content)

    registry = _owner_file(
        tmp_path / f"wrong-principal-{uuid4()}.json",
        json.dumps(principal_document),
    )
    wrong_app = create_app(
        authority_api.settings,
        verifier=TaskImagePrincipalVerifier.from_file(registry),
        now_factory=lambda: NOW + timedelta(seconds=8),
    )
    with TestClient(wrong_app) as client:
        response = client.put(
            f"/v1/projections/{GRANT_ID}/exchange",
            headers={"Authorization": f"Bearer {bearer}"},
            json=_exchange(receipt).model_dump(mode="json"),
        )

    assert response.status_code == 403
    assert response.json() == {"detail": "task-image authority rejected"}
    assert bearer not in response.text


async def test_streamed_body_limit_cannot_be_bypassed_without_content_length() -> None:
    downstream_called = False

    async def downstream(scope: Any, receive: Any, send: Any) -> None:
        del scope, receive, send
        nonlocal downstream_called
        downstream_called = True

    middleware = api.RequestBodyLimitMiddleware(
        downstream,
        maximum_bytes=MAX_CONTRACT_BYTES,
    )
    chunks = [
        {
            "type": "http.request",
            "body": b"x" * (MAX_CONTRACT_BYTES // 2),
            "more_body": True,
        },
        {
            "type": "http.request",
            "body": b"x" * (MAX_CONTRACT_BYTES // 2 + 1),
            "more_body": False,
        },
    ]
    sent: list[dict[str, object]] = []

    async def receive() -> dict[str, object]:
        return chunks.pop(0)

    async def send(message: dict[str, object]) -> None:
        sent.append(message)

    await middleware(
        {"type": "http", "headers": [], "path": "/v1/projections/x/challenge"},
        receive,
        send,
    )

    assert downstream_called is False
    assert sent[0]["status"] == 413
    assert sent[1]["body"] == b'{"detail":"task-image authority request too large"}'


async def test_concurrency_limiter_rejects_work_instead_of_queueing_unboundedly() -> None:
    entered = asyncio.Event()
    release = asyncio.Event()

    async def downstream(scope: Any, receive: Any, send: Any) -> None:
        del scope, receive
        entered.set()
        await release.wait()
        await send({"type": "http.response.start", "status": 204, "headers": []})
        await send({"type": "http.response.body", "body": b""})

    middleware = api.AuthorityTrafficLimitMiddleware(
        downstream,
        requests_per_second=10,
        concurrency=1,
    )
    scope = {"type": "http", "path": "/v1/projections/x/challenge"}

    async def receive() -> dict[str, object]:
        return {"type": "http.request", "body": b"", "more_body": False}

    first_messages: list[dict[str, object]] = []
    second_messages: list[dict[str, object]] = []

    async def first_send(message: dict[str, object]) -> None:
        first_messages.append(message)

    async def second_send(message: dict[str, object]) -> None:
        second_messages.append(message)

    first = asyncio.create_task(middleware(scope, receive, first_send))
    await entered.wait()
    await middleware(scope, receive, second_send)
    release.set()
    await first

    assert first_messages[0]["status"] == 204
    assert second_messages[0]["status"] == 503
    assert second_messages[1]["body"] == (
        b'{"detail":"task-image authority concurrency exhausted"}'
    )


async def test_rate_limiter_bounds_mutations_per_process(
    authority_api: _ApiContext,
) -> None:
    settings = authority_api.settings.model_copy(
        update={"request_rate_limit_per_second": 1}
    )
    limited_app = create_app(settings)
    payload = _request().model_dump(mode="json")
    path = f"/v1/projections/{GRANT_ID}/challenge"
    with TestClient(limited_app) as client:
        first = client.put(
            path,
            headers={"Authorization": "Bearer wrong-first-private-token"},
            json=payload,
        )
        second = client.put(
            path,
            headers={"Authorization": "Bearer wrong-second-private-token"},
            json=payload,
        )

    assert first.status_code == 401
    assert second.status_code == 429
    assert second.json() == {"detail": "task-image authority rate limited"}
    assert "wrong-second-private-token" not in second.text


async def test_attestation_equivocation_commits_quarantine_before_bounded_conflict(
    authority_api: _ApiContext,
    postgres_url: str,
) -> None:
    assert _put(
        authority_api,
        f"/v1/projections/{GRANT_ID}/challenge",
        _request(),
    ).status_code == 200
    authority_api.now[0] = NOW + timedelta(seconds=6)
    receipt_response = _put(
        authority_api,
        f"/v1/projections/{GRANT_ID}/attachment",
        _proof(),
    )
    receipt = TaskImageProjectionReceiptV1.model_validate_json(receipt_response.content)
    authority_api.now[0] = NOW + timedelta(seconds=8)
    assert _put(
        authority_api,
        f"/v1/projections/{GRANT_ID}/exchange",
        _exchange(receipt),
    ).status_code == 200

    authority_api.now[0] = NOW + timedelta(seconds=9)
    equivocation = _attestation(
        _proof(),
        generation=1,
        attestation_id=uuid4(),
    )
    response = _put(
        authority_api,
        f"/v1/projections/{GRANT_ID}/attestations/1",
        equivocation,
    )
    assert response.status_code == 409
    assert response.json() == {"detail": "task-image authority conflict"}

    engine = create_async_engine(postgres_url)
    try:
        async with AsyncSession(engine) as session:
            row = await session.scalar(
                select(TaskImageBuildProjection).where(
                    TaskImageBuildProjection.grant_id == GRANT_ID
                )
            )
            assert row is not None
            assert row.state == "revoked"
            assert row.revoke_reason == "attestation_equivocation"
            events = list(
                (
                    await session.scalars(
                        select(TaskImageBuildProjectionEvent).where(
                            TaskImageBuildProjectionEvent.grant_id == GRANT_ID,
                            TaskImageBuildProjectionEvent.event_type == "revoked",
                        )
                    )
                ).all()
            )
            assert len(events) == 1
    finally:
        await engine.dispose()


async def test_secret_store_failure_rolls_back_projection_and_redacts_failure(
    authority_api: _ApiContext,
    postgres_url: str,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    challenge = _put(
        authority_api,
        f"/v1/projections/{GRANT_ID}/challenge",
        _request(),
    )
    assert challenge.status_code == 200

    async def fail_put(*args: object, **kwargs: object) -> str:
        del args, kwargs
        raise RuntimeError("synthetic-private-keyring-failure")

    monkeypatch.setattr(api.LocalEncryptedSecretStore, "put", fail_put)
    authority_api.now[0] = NOW + timedelta(seconds=6)
    with caplog.at_level(logging.INFO):
        response = _put(
            authority_api,
            f"/v1/projections/{GRANT_ID}/attachment",
            _proof(),
        )
    assert response.status_code == 503
    assert response.json() == {"detail": "task-image authority unavailable"}
    assert "synthetic-private-keyring-failure" not in response.text
    assert "synthetic-private-keyring-failure" not in caplog.text

    engine = create_async_engine(postgres_url)
    try:
        async with AsyncSession(engine) as session:
            row = await session.scalar(
                select(TaskImageBuildProjection).where(
                    TaskImageBuildProjection.grant_id == GRANT_ID
                )
            )
            assert row is not None
            assert row.state == "challenged"
            assert row.proof_id is None
            secret_count = len(
                list(
                    (
                        await session.scalars(
                            select(Secret).where(Secret.ref.like("loom://task-image-%"))
                        )
                    ).all()
                )
            )
            assert secret_count == 0
    finally:
        await engine.dispose()


async def test_metrics_are_aggregate_only_and_never_expose_authority_inputs(
    authority_api: _ApiContext,
) -> None:
    response = _put(
        authority_api,
        f"/v1/projections/{GRANT_ID}/challenge",
        _request(),
    )
    assert response.status_code == 200
    rejected = authority_api.client.put(
        f"/v1/projections/{GRANT_ID}/challenge",
        headers={"Authorization": "Bearer metrics-private-wrong-token"},
        json=_request().model_dump(mode="json"),
    )
    assert rejected.status_code == 401

    metrics = authority_api.client.get("/metrics")
    assert metrics.status_code == 200
    assert metrics.headers["content-type"].startswith("text/plain")
    assert "loom_task_image_authority_ready 1.0" in metrics.text
    assert (
        'loom_task_image_authority_requests_total{outcome="success",route="challenge"} 1.0'
        in metrics.text
    )
    assert (
        'loom_task_image_authority_requests_total{outcome="rejected",route="challenge"} 1.0'
        in metrics.text
    )
    for private in (
        _BEARER,
        str(GRANT_ID),
        "trt-gb10-1",
        "gb10-trt-gb10-1",
        "guard_attestation_lost",
        _BOOTSTRAP,
        _SESSION,
        "metrics-private-wrong-token",
    ):
        assert private not in metrics.text


async def test_startup_fails_closed_for_invalid_keyring_and_schema(
    tmp_path: Path,
    postgres_url: str,
) -> None:
    invalid_keyring = {
        "schema_version": 1,
        "primary": {"version": 1, "key_base64": "not-base64"},
        "fallbacks": [],
    }
    keyring_settings = _settings(
        tmp_path,
        postgres_url,
        keyring_document=invalid_keyring,
    )
    keyring_app = create_app(keyring_settings)
    with TestClient(keyring_app) as client:
        response = client.get("/healthz")
        assert response.status_code == 503
        assert response.json() == {"detail": "task-image authority not ready"}
        assert keyring_app.state.ready is False

    empty_database_url = make_url(postgres_url).set(database="postgres").render_as_string(
        hide_password=False
    )
    schema_settings = _settings(tmp_path, empty_database_url)
    schema_app = create_app(schema_settings)
    with TestClient(schema_app) as client:
        response = client.get("/healthz")
        assert response.status_code == 503
        assert response.json() == {"detail": "task-image authority not ready"}
        assert schema_app.state.ready is False
