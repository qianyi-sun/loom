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
from sqlalchemy import delete, null, select, update
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
    TaskImageBuildSessionGeneration,
    TaskImageMaterialization,
    TaskImageMaterializationAttempt,
    TaskImageMaterializationOperationEvent,
    TaskImagePublicationEvidence,
)
from loom.task_image_materialization import task_image_materialization_key
from loom_task_image_authority import api
from loom_task_image_authority.api import create_app
from loom_task_image_authority.auth import TaskImagePrincipalVerifier
from loom_task_image_authority.bundle_capability import (
    TaskImageBundleCapabilityProvider,
    TaskImageBundleCapabilityV1,
    TaskImageBundleObject,
)
from loom_task_image_authority.config import TaskImageAuthoritySettings
from loom_task_image_authority.contracts import (
    MAX_CONTRACT_BYTES,
    TaskImageBuildSessionV2,
    TaskImageMaterializationClaimRequestV1,
    TaskImageMaterializationFailureRequestV1,
    TaskImageMaterializationOperationRequestV1,
    TaskImageProjectionChallengeV1,
    TaskImageProjectionReceiptV1,
    TaskImageSessionRenewalV1,
)
from loom_task_image_authority.http_contracts import (
    TaskImageMaterializationClaimResponseV1,
    TaskImageMaterializationOperationResponseV1,
)
from tests.integration.test_task_image_projection_store import (
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
_NEXT_SESSION = "loom_tibs_" + "C" * 64
_HEADERS = {"Authorization": f"Bearer {_BEARER}"}
_NEXT_SESSION_ID = UUID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")
_CLAIM_ID = UUID("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb")
_START_ID = UUID("cccccccc-cccc-cccc-cccc-cccccccccccc")
_HEARTBEAT_ID = UUID("dddddddd-dddd-dddd-dddd-dddddddddddd")
_RELEASE_ID = UUID("eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee")


class _FakeBundleBackend:
    def __init__(self) -> None:
        self.list_calls = 0
        self.presign_calls = 0

    def list_objects(
        self,
        *,
        bucket: str,
        prefix: str,
        maximum_objects: int,
    ) -> tuple[TaskImageBundleObject, ...]:
        assert bucket == "loom-bundles"
        assert prefix == "phase2c/session-bound/"
        assert maximum_objects == 2_001
        self.list_calls += 1
        return (
            TaskImageBundleObject(key=f"{prefix}task.toml", size_bytes=20),
            TaskImageBundleObject(
                key=f"{prefix}.loom-bundle-file-metadata.json",
                size_bytes=42,
            ),
        )

    def presign_get(
        self,
        *,
        bucket: str,
        key: str,
        expires_in_seconds: int,
    ) -> str:
        assert bucket == "loom-bundles"
        assert 0 < expires_in_seconds <= 600
        self.presign_calls += 1
        return (
            f"https://objects.example/{key}"
            f"?X-Amz-Date=20260902T140000Z&X-Amz-Expires={expires_in_seconds}"
            "&X-Amz-Signature=secret"
        )


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
        "tls_client_ca_file": _owner_file(tmp_path / f"client-ca-{uuid4()}.pem", "test"),
    }
    values.update(changes)
    return TaskImageAuthoritySettings(**values)


async def _clear_authority_rows(database_url: str) -> None:
    engine = create_async_engine(database_url)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    try:
        async with factory() as session:
            await session.execute(delete(TaskImageMaterializationOperationEvent))
            await session.execute(delete(TaskImagePublicationEvidence))
            await session.execute(delete(TaskImageMaterializationAttempt))
            await session.execute(delete(TaskImageMaterialization))
            await session.execute(delete(TaskImageBuildProjectionEvent))
            await session.execute(
                update(TaskImageBuildProjection)
                .where(TaskImageBuildProjection.session_id.is_not(None))
                .values(
                    state="projected",
                    exchange_id=None,
                    exchange_json=null(),
                    exchange_sha256=None,
                    session_id=None,
                    session_generation=None,
                    session_token_hash=None,
                    session_secret_ref=None,
                    session_json=null(),
                    session_sha256=None,
                    session_issued_at=None,
                    session_expires_at=None,
                    revoked_at=None,
                    revoke_reason=None,
                    expired_at=None,
                )
            )
            await session.execute(delete(TaskImageBuildSessionGeneration))
            await session.execute(delete(TaskImageBuildContainmentAttestation))
            await session.execute(delete(TaskImageBuildProjection))
            await session.execute(delete(TaskImageBuildGrantEvent))
            await session.execute(delete(TaskImageBuildGrant))
            await session.execute(delete(Secret).where(Secret.ref.like("loom://task-image-%")))
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


async def _seed_materialization(
    database_url: str,
    *,
    task_config: dict[str, object] | None = None,
) -> UUID:
    engine = create_async_engine(database_url)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    materialization_id = uuid4()
    try:
        async with factory() as session:
            checksum = "4" * 64
            session.add(
                TaskImageMaterialization(
                    id=materialization_id,
                    materialization_key=task_image_materialization_key(
                        task_id="phase2c/session-bound",
                        task_checksum=checksum,
                        cpu_arch="arm64",
                    ),
                    task_id="phase2c/session-bound",
                    task_checksum=checksum,
                    cpu_arch="arm64",
                    task_config=task_config
                    or {
                        "schema_version": "1",
                        "task": {
                            "id": "phase2c/session-bound",
                            "name": "session-bound",
                        },
                        "environment": {
                            "os": "linux",
                            "cpu_arch": "arm64",
                            "dockerfile": "environment/Dockerfile",
                            "build_timeout_sec": 600.0,
                        },
                        "agent": {"name": "oracle"},
                        "verifier": {"name": "pytest"},
                    },
                    task_source="s3://loom-bundles/phase2c/session-bound/",
                    task_source_provenance={
                        "bundle_file_metadata_sha256": "sha256:" + "5" * 64,
                    },
                )
            )
            await session.commit()
    finally:
        await engine.dispose()
    return materialization_id


@dataclass
class _ApiContext:
    client: TestClient
    app: FastAPI
    now: list[datetime]
    settings: TaskImageAuthoritySettings
    bundle_backend: _FakeBundleBackend


@pytest.fixture
async def authority_api(
    tmp_path: Path,
    postgres_url: str,
) -> AsyncIterator[_ApiContext]:
    await _clear_authority_rows(postgres_url)
    await _seed_released_grant(postgres_url)
    settings = _settings(
        tmp_path,
        postgres_url,
        bundle_public_https_origin="https://objects.example",
        bundle_expected_bucket="loom-bundles",
        bundle_url_expiry_seconds=600,
    )
    current_now = [NOW + timedelta(seconds=4)]
    session_tokens = iter((_SESSION, _NEXT_SESSION))
    bundle_backend = _FakeBundleBackend()
    capability_ids = iter(
        (
            UUID("ffffffff-ffff-ffff-ffff-ffffffffffff"),
            UUID("fefefefe-fefe-fefe-fefe-fefefefefefe"),
        )
    )
    capability_provider = TaskImageBundleCapabilityProvider(
        backend=bundle_backend,
        public_https_origin=settings.bundle_public_https_origin or "",
        expected_bucket=settings.bundle_expected_bucket or "",
        maximum_objects=settings.bundle_maximum_objects,
        maximum_bytes=settings.bundle_maximum_bytes,
        url_expiry_seconds=settings.bundle_url_expiry_seconds,
        capability_id_factory=lambda: next(capability_ids),
    )
    app = create_app(
        settings,
        verifier=TaskImagePrincipalVerifier.from_file(settings.principals_file),
        now_factory=lambda: current_now[0],
        challenge_nonce_factory=lambda: CHALLENGE_NONCE,
        bootstrap_token_factory=lambda: _BOOTSTRAP,
        session_token_factory=lambda: next(session_tokens),
        session_id_factory=lambda: _NEXT_SESSION_ID,
        bundle_capability_provider=capability_provider,
    )
    try:
        with TestClient(app) as client:
            yield _ApiContext(
                client=client,
                app=app,
                now=current_now,
                settings=settings,
                bundle_backend=bundle_backend,
            )
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


def _post(
    context: _ApiContext,
    path: str,
    model: object,
    *,
    headers: dict[str, str] | None = None,
):
    return context.client.post(
        path,
        headers=_HEADERS if headers is None else headers,
        json=model.model_dump(mode="json"),  # type: ignore[attr-defined]
    )


def _renewed_session(context: _ApiContext) -> TaskImageBuildSessionV2:
    assert (
        _put(
            context,
            f"/v1/projections/{GRANT_ID}/challenge",
            _request(),
        ).status_code
        == 200
    )
    context.now[0] = NOW + timedelta(seconds=6)
    receipt_response = _put(
        context,
        f"/v1/projections/{GRANT_ID}/attachment",
        _proof(),
    )
    receipt = TaskImageProjectionReceiptV1.model_validate_json(receipt_response.content)
    context.now[0] = NOW + timedelta(seconds=8)
    exchange_response = _put(
        context,
        f"/v1/projections/{GRANT_ID}/exchange",
        _exchange(receipt),
    )
    current = TaskImageBuildSessionV2.model_validate_json(exchange_response.content)
    context.now[0] = NOW + timedelta(seconds=13)
    renewal = TaskImageSessionRenewalV1(
        renewal_id=UUID("99999999-9999-9999-9999-999999999999"),
        grant_id=GRANT_ID,
        session_id=current.session_id,
        session_generation=current.generation,
        session_token=current.session_token,
        attestation=_attestation(_proof(), generation=2),
        observed_at=NOW + timedelta(seconds=12),
    )
    response = _put(
        context,
        f"/v1/projections/{GRANT_ID}/sessions/1/renew",
        renewal,
    )
    assert response.status_code == 200
    return TaskImageBuildSessionV2.model_validate_json(response.content)


def _claim_request(
    build_session: TaskImageBuildSessionV2,
    *,
    claim_id: UUID = _CLAIM_ID,
) -> TaskImageMaterializationClaimRequestV1:
    return TaskImageMaterializationClaimRequestV1(
        claim_id=claim_id,
        grant_id=GRANT_ID,
        session_id=build_session.session_id,
        session_generation=build_session.generation,
        session_token=build_session.session_token,
    )


def _operation_request(
    build_session: TaskImageBuildSessionV2,
    receipt: TaskImageMaterializationClaimResponseV1,
    *,
    operation_id: UUID,
) -> TaskImageMaterializationOperationRequestV1:
    return TaskImageMaterializationOperationRequestV1(
        operation_id=operation_id,
        grant_id=GRANT_ID,
        session_id=build_session.session_id,
        session_generation=build_session.generation,
        session_token=build_session.session_token,
        materialization_id=receipt.materialization_id,
        attempt_id=receipt.attempt_id,
        lease_epoch=receipt.lease_epoch,
    )


async def test_authority_routes_drive_the_exact_projection_lifecycle_and_replays(
    authority_api: _ApiContext,
) -> None:
    context = authority_api
    health = context.client.get("/healthz")
    assert health.status_code == 200
    assert health.json() == {"status": "ok"}

    route_paths = {route.path for route in context.app.routes if isinstance(route, APIRoute)}
    assert route_paths == {
        "/healthz",
        "/metrics",
        "/v1/projections/{grant_id}/challenge",
        "/v1/projections/{grant_id}/attachment",
        "/v1/projections/{grant_id}/exchange",
        "/v1/projections/{grant_id}/attestations/{generation}",
        "/v1/projections/{grant_id}/sessions/{generation}/renew",
        "/v1/projections/{grant_id}/materializations/claim",
        "/v1/projections/{grant_id}/materializations/{materialization_id}/start",
        "/v1/projections/{grant_id}/materializations/{materialization_id}/heartbeat",
        "/v1/projections/{grant_id}/materializations/{materialization_id}/release",
        "/v1/projections/{grant_id}/materializations/{materialization_id}/fail",
        "/v1/projections/{grant_id}/materializations/{materialization_id}/bundle",
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
    challenge = TaskImageProjectionChallengeV1.model_validate_json(challenge_response.content)
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
    initial_session = TaskImageBuildSessionV2.model_validate_json(exchange_response.content)
    exchange_replay = _put(
        context,
        f"/v1/projections/{GRANT_ID}/exchange",
        _exchange(receipt),
    )
    assert exchange_replay.json() == exchange_response.json()

    context.now[0] = NOW + timedelta(seconds=13)
    renewal = TaskImageSessionRenewalV1(
        renewal_id=UUID("99999999-9999-9999-9999-999999999999"),
        grant_id=GRANT_ID,
        session_id=initial_session.session_id,
        session_generation=initial_session.generation,
        session_token=initial_session.session_token,
        attestation=_attestation(_proof(), generation=2),
        observed_at=NOW + timedelta(seconds=12),
    )
    renewal_response = _put(
        context,
        f"/v1/projections/{GRANT_ID}/sessions/1/renew",
        renewal,
    )
    assert renewal_response.status_code == 200
    renewed_session = TaskImageBuildSessionV2.model_validate_json(renewal_response.content)
    assert renewed_session.generation == 2
    assert renewed_session.session_id == _NEXT_SESSION_ID
    assert renewed_session.session_token == _NEXT_SESSION
    renewal_replay = _put(
        context,
        f"/v1/projections/{GRANT_ID}/sessions/1/renew",
        renewal,
    )
    assert renewal_replay.json() == renewal_response.json()

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


async def test_session_routes_drive_claim_bundle_and_lease_operations(
    authority_api: _ApiContext,
    postgres_url: str,
) -> None:
    materialization_id = await _seed_materialization(postgres_url)
    build_session = _renewed_session(authority_api)

    authority_api.now[0] = NOW + timedelta(seconds=14)
    claim_request = _claim_request(build_session)
    claim_response = _post(
        authority_api,
        f"/v1/projections/{GRANT_ID}/materializations/claim",
        claim_request,
    )
    assert claim_response.status_code == 200
    claim = TaskImageMaterializationClaimResponseV1.model_validate_json(claim_response.content)
    assert claim.claim_id == _CLAIM_ID
    assert claim.materialization_id == materialization_id
    assert claim.state == "claimed"
    assert claim.deterministic_failure_count == 0
    assert claim.plan.materialization_id == materialization_id
    assert claim.plan.session_id == build_session.session_id
    assert claim.plan.session_generation == build_session.generation
    assert claim.plan.builder_id == f"rootless:{build_session.session_id.hex}"
    replay = _post(
        authority_api,
        f"/v1/projections/{GRANT_ID}/materializations/claim",
        claim_request,
    )
    assert replay.status_code == 200
    assert replay.json() == claim_response.json()

    authority_api.now[0] = NOW + timedelta(seconds=15)
    start_request = _operation_request(
        build_session,
        claim,
        operation_id=_START_ID,
    )
    start_response = _put(
        authority_api,
        f"/v1/projections/{GRANT_ID}/materializations/{materialization_id}/start",
        start_request,
    )
    start = TaskImageMaterializationOperationResponseV1.model_validate_json(start_response.content)
    assert start_response.status_code == 200
    assert start.operation == "start"
    assert start.state == "running"
    assert start.attempt_id == claim.attempt_id

    authority_api.now[0] = NOW + timedelta(seconds=16)
    bundle_request = _operation_request(
        build_session,
        claim,
        operation_id=UUID("12121212-1212-1212-1212-121212121212"),
    )
    bundle_response = _put(
        authority_api,
        f"/v1/projections/{GRANT_ID}/materializations/{materialization_id}/bundle",
        bundle_request,
    )
    assert bundle_response.status_code == 200
    capability = TaskImageBundleCapabilityV1.model_validate_json(bundle_response.content)
    assert capability.materialization_id == materialization_id
    assert capability.session_id == build_session.session_id
    assert capability.expires_at <= build_session.expires_at
    assert all(item.url.startswith("https://objects.example/") for item in capability.objects)
    bundle_replay = _put(
        authority_api,
        f"/v1/projections/{GRANT_ID}/materializations/{materialization_id}/bundle",
        bundle_request,
    )
    assert bundle_replay.status_code == 200
    assert bundle_replay.json() == bundle_response.json()
    assert authority_api.bundle_backend.list_calls == 1
    assert authority_api.bundle_backend.presign_calls == len(capability.objects)

    authority_api.now[0] = NOW + timedelta(seconds=17)
    heartbeat_request = _operation_request(
        build_session,
        claim,
        operation_id=_HEARTBEAT_ID,
    )
    heartbeat_response = _put(
        authority_api,
        f"/v1/projections/{GRANT_ID}/materializations/{materialization_id}/heartbeat",
        heartbeat_request,
    )
    heartbeat = TaskImageMaterializationOperationResponseV1.model_validate_json(
        heartbeat_response.content
    )
    assert heartbeat_response.status_code == 200
    assert heartbeat.operation == "heartbeat"
    assert heartbeat.state == "running"

    authority_api.now[0] = NOW + timedelta(seconds=18)
    release_request = _operation_request(
        build_session,
        claim,
        operation_id=_RELEASE_ID,
    )
    release_response = _put(
        authority_api,
        f"/v1/projections/{GRANT_ID}/materializations/{materialization_id}/release",
        release_request,
    )
    release = TaskImageMaterializationOperationResponseV1.model_validate_json(
        release_response.content
    )
    assert release_response.status_code == 200
    assert release.operation == "release"
    assert release.state == "queued"
    assert release.deterministic_failure_count == 0
    assert release.lease_expires_at is None
    release_replay = _put(
        authority_api,
        f"/v1/projections/{GRANT_ID}/materializations/{materialization_id}/release",
        release_request,
    )
    assert release_replay.json() == release_response.json()
    heartbeat_replay_after_release = _put(
        authority_api,
        f"/v1/projections/{GRANT_ID}/materializations/{materialization_id}/heartbeat",
        heartbeat_request,
    )
    assert heartbeat_replay_after_release.status_code == 200
    assert heartbeat_replay_after_release.json() == heartbeat_response.json()
    claim_replay_after_release = _post(
        authority_api,
        f"/v1/projections/{GRANT_ID}/materializations/claim",
        claim_request,
    )
    assert claim_replay_after_release.status_code == 200
    assert claim_replay_after_release.json() == claim_response.json()

    engine = create_async_engine(postgres_url)
    try:
        async with engine.begin() as connection:
            await connection.execute(
                update(TaskImageMaterialization)
                .where(TaskImageMaterialization.id == materialization_id)
                .values(next_attempt_at=NOW + timedelta(seconds=19))
            )
    finally:
        await engine.dispose()

    authority_api.now[0] = NOW + timedelta(seconds=19)
    second_claim_response = _post(
        authority_api,
        f"/v1/projections/{GRANT_ID}/materializations/claim",
        _claim_request(build_session, claim_id=uuid4()),
    )
    second_claim = TaskImageMaterializationClaimResponseV1.model_validate_json(
        second_claim_response.content
    )
    failure_request = TaskImageMaterializationFailureRequestV1(
        **_operation_request(
            build_session,
            second_claim,
            operation_id=uuid4(),
        ).model_dump(mode="python"),
        failure_kind="deterministic",
    )
    authority_api.now[0] = NOW + timedelta(seconds=20)
    failure_response = _put(
        authority_api,
        f"/v1/projections/{GRANT_ID}/materializations/{materialization_id}/fail",
        failure_request,
    )
    failure = TaskImageMaterializationOperationResponseV1.model_validate_json(
        failure_response.content
    )
    assert failure_response.status_code == 200
    assert failure.operation == "deterministic_fail"
    assert failure.state == "queued"
    assert failure.deterministic_failure_count == 1


async def test_materialization_routes_bind_session_path_attempt_and_guard_identity(
    authority_api: _ApiContext,
    postgres_url: str,
    tmp_path: Path,
) -> None:
    materialization_id = await _seed_materialization(postgres_url)
    build_session = _renewed_session(authority_api)
    path = f"/v1/projections/{GRANT_ID}/materializations/claim"
    valid_claim = _claim_request(build_session)

    for field, value in (
        ("session_id", uuid4()),
        ("session_generation", 1),
        ("session_token", "loom_tibs_" + "PRIVATE_WRONG_" + "D" * 64),
    ):
        payload = valid_claim.model_dump(mode="python")
        payload[field] = value
        changed = TaskImageMaterializationClaimRequestV1.model_validate(payload)
        response = _post(authority_api, path, changed)
        assert response.status_code == 403
        assert response.json() == {"detail": "task-image authority rejected"}
        assert str(value) not in response.text

    wrong_bearer = "wrong-node-materialization-private-bearer"
    wrong_principals = _owner_file(
        tmp_path / "wrong-node-materialization-principals.json",
        json.dumps(
            _principal_document(
                bearer=wrong_bearer,
                principal_id="gb10-trt-gb10-2",
                node="trt-gb10-2",
            )
        ),
    )
    wrong_app = create_app(
        authority_api.settings,
        verifier=TaskImagePrincipalVerifier.from_file(wrong_principals),
        now_factory=lambda: NOW + timedelta(seconds=14),
    )
    with TestClient(wrong_app) as wrong_client:
        response = wrong_client.post(
            path,
            headers={"Authorization": f"Bearer {wrong_bearer}"},
            json=valid_claim.model_dump(mode="json"),
        )
    assert response.status_code == 403
    assert response.json() == {"detail": "task-image authority rejected"}
    assert wrong_bearer not in response.text

    claim_response = _post(authority_api, path, valid_claim)
    claim = TaskImageMaterializationClaimResponseV1.model_validate_json(claim_response.content)
    operation = _operation_request(build_session, claim, operation_id=_START_ID)
    wrong_materialization = authority_api.client.put(
        f"/v1/projections/{GRANT_ID}/materializations/{uuid4()}/start",
        headers=_HEADERS,
        json=operation.model_dump(mode="json"),
    )
    assert wrong_materialization.status_code == 409
    assert wrong_materialization.json() == {"detail": "task-image authority conflict"}
    wrong_grant = authority_api.client.put(
        f"/v1/projections/{uuid4()}/materializations/{materialization_id}/start",
        headers=_HEADERS,
        json=operation.model_dump(mode="json"),
    )
    assert wrong_grant.status_code == 409

    for field, value in (("attempt_id", uuid4()), ("lease_epoch", 2)):
        payload = operation.model_dump(mode="python")
        payload[field] = value
        changed = TaskImageMaterializationOperationRequestV1.model_validate(payload)
        response = _put(
            authority_api,
            f"/v1/projections/{GRANT_ID}/materializations/{materialization_id}/start",
            changed,
        )
        assert response.status_code == 409
        assert response.json() == {"detail": "task-image authority conflict"}

    unknown = operation.model_dump(mode="json") | {"private_task_config": "must-not-echo"}
    response = authority_api.client.put(
        f"/v1/projections/{GRANT_ID}/materializations/{materialization_id}/start",
        headers=_HEADERS,
        json=unknown,
    )
    assert response.status_code == 422
    assert response.json() == {"detail": "invalid task-image authority contract"}
    assert "must-not-echo" not in response.text


async def test_bundle_route_fails_closed_without_provider_and_redacts_backend_errors(
    authority_api: _ApiContext,
    postgres_url: str,
) -> None:
    materialization_id = await _seed_materialization(postgres_url)
    build_session = _renewed_session(authority_api)
    claim_response = _post(
        authority_api,
        f"/v1/projections/{GRANT_ID}/materializations/claim",
        _claim_request(build_session),
    )
    claim = TaskImageMaterializationClaimResponseV1.model_validate_json(claim_response.content)
    request = _operation_request(
        build_session,
        claim,
        operation_id=uuid4(),
    )
    path = f"/v1/projections/{GRANT_ID}/materializations/{materialization_id}/bundle"

    unavailable_app = create_app(
        authority_api.settings,
        verifier=TaskImagePrincipalVerifier.from_file(authority_api.settings.principals_file),
        now_factory=lambda: NOW + timedelta(seconds=16),
    )
    with TestClient(unavailable_app) as client:
        unavailable = client.put(
            path,
            headers=_HEADERS,
            json=request.model_dump(mode="json"),
        )
    assert unavailable.status_code == 503
    assert unavailable.json() == {"detail": "task-image authority unavailable"}

    class FailingBackend:
        def list_objects(self, **kwargs: object) -> tuple[TaskImageBundleObject, ...]:
            del kwargs
            raise RuntimeError("private-backend-error s3://loom-bundles/private-source-key")

        def presign_get(self, **kwargs: object) -> str:
            del kwargs
            raise AssertionError("presign must not follow failed listing")

    failing_provider = TaskImageBundleCapabilityProvider(
        backend=FailingBackend(),
        public_https_origin="https://objects.example",
        expected_bucket="loom-bundles",
        maximum_objects=2_000,
        maximum_bytes=512 * 1024 * 1024,
        url_expiry_seconds=600,
    )
    failing_app = create_app(
        authority_api.settings,
        verifier=TaskImagePrincipalVerifier.from_file(authority_api.settings.principals_file),
        now_factory=lambda: NOW + timedelta(seconds=16),
        bundle_capability_provider=failing_provider,
    )
    with TestClient(failing_app) as client:
        failed = client.put(
            path,
            headers=_HEADERS,
            json=request.model_dump(mode="json"),
        )
        metrics = client.get("/metrics")
    assert failed.status_code == 503
    assert failed.json() == {"detail": "task-image authority unavailable"}
    for private in (
        "private-backend-error",
        "private-source-key",
        build_session.session_token,
        "X-Amz-Signature",
    ):
        assert private not in failed.text
        assert private not in metrics.text


async def test_oversized_claim_response_is_rejected_before_a_lease_is_committed(
    authority_api: _ApiContext,
    postgres_url: str,
) -> None:
    oversized_config: dict[str, object] = {
        "schema_version": "1",
        "task": {"id": "phase2c/session-bound", "name": "session-bound"},
        "environment": {
            "os": "linux",
            "cpu_arch": "arm64",
            "dockerfile": "environment/Dockerfile",
            "build_timeout_sec": 600.0,
            "sidecars": [
                {
                    "name": f"component-{index:03d}",
                    "dockerfile": f"component-{index:03d}/" + "x" * 1024,
                }
                for index in range(127)
            ],
        },
        "agent": {"name": "oracle"},
        "verifier": {"name": "pytest"},
    }
    materialization_id = await _seed_materialization(
        postgres_url,
        task_config=oversized_config,
    )
    build_session = _renewed_session(authority_api)

    response = _post(
        authority_api,
        f"/v1/projections/{GRANT_ID}/materializations/claim",
        _claim_request(build_session),
    )

    assert response.status_code == 503
    assert response.json() == {"detail": "task-image authority unavailable"}
    assert "component-126" not in response.text
    engine = create_async_engine(postgres_url)
    try:
        async with AsyncSession(engine) as session:
            row = await session.get(TaskImageMaterialization, materialization_id)
            assert row is not None
            assert row.state == "queued"
            assert row.claimed_by is None
            assert (
                await session.scalar(
                    select(TaskImageMaterializationAttempt).where(
                        TaskImageMaterializationAttempt.materialization_id == materialization_id
                    )
                )
                is None
            )
    finally:
        await engine.dispose()


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
    assert duplicate.json() == {"detail": "invalid task-image authority credentials"}
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
    assert forbidden_attestation.json() == {"detail": "task-image authority forbidden"}
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


async def test_streamed_body_is_replayed_as_one_bounded_message() -> None:
    received_downstream: list[dict[str, object]] = []

    async def downstream(scope: Any, receive: Any, send: Any) -> None:
        del scope
        received_downstream.append(await receive())
        received_downstream.append(await receive())
        await send({"type": "http.response.start", "status": 204, "headers": []})
        await send({"type": "http.response.body", "body": b""})

    middleware = api.RequestBodyLimitMiddleware(downstream, maximum_bytes=4)
    chunks = [
        {"type": "http.request", "body": b"a", "more_body": True},
        {"type": "http.request", "body": b"bc", "more_body": True},
        {"type": "http.request", "body": b"d", "more_body": False},
    ]

    async def receive() -> dict[str, object]:
        return chunks.pop(0)

    async def send(message: dict[str, object]) -> None:
        del message

    await middleware(
        {"type": "http", "headers": [], "path": "/v1/projections/x/challenge"},
        receive,
        send,
    )

    assert received_downstream == [
        {"type": "http.request", "body": b"abcd", "more_body": False},
        {"type": "http.disconnect"},
    ]


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
    settings = authority_api.settings.model_copy(update={"request_rate_limit_per_second": 1})
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
    assert (
        _put(
            authority_api,
            f"/v1/projections/{GRANT_ID}/challenge",
            _request(),
        ).status_code
        == 200
    )
    authority_api.now[0] = NOW + timedelta(seconds=6)
    receipt_response = _put(
        authority_api,
        f"/v1/projections/{GRANT_ID}/attachment",
        _proof(),
    )
    receipt = TaskImageProjectionReceiptV1.model_validate_json(receipt_response.content)
    authority_api.now[0] = NOW + timedelta(seconds=8)
    assert (
        _put(
            authority_api,
            f"/v1/projections/{GRANT_ID}/exchange",
            _exchange(receipt),
        ).status_code
        == 200
    )

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

    empty_database_url = (
        make_url(postgres_url).set(database="postgres").render_as_string(hide_password=False)
    )
    schema_settings = _settings(tmp_path, empty_database_url)
    schema_app = create_app(schema_settings)
    with TestClient(schema_app) as client:
        response = client.get("/healthz")
        assert response.status_code == 503
        assert response.json() == {"detail": "task-image authority not ready"}
        assert schema_app.state.ready is False
