"""Authenticated and bounded capacity-manager HTTP surface tests."""

from __future__ import annotations

import hashlib
import json
import ssl
from collections.abc import AsyncIterator, Iterator
from contextlib import contextmanager
from pathlib import Path
from threading import Event, Thread
from uuid import UUID, uuid4

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy import delete, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from loom_capacity_manager.allocator import allocate_shadow
from loom_capacity_manager.api import RequestBodyLimitMiddleware, create_app
from loom_capacity_manager.auth import CapacityPrincipalVerifier
from loom_capacity_manager.config import CapacityManagerSettings, build_uvicorn_kwargs
from loom_capacity_manager.contracts import MAX_CONTRACT_BYTES
from loom_capacity_manager.models import Base, CapacityAuthorityState
from loom_capacity_manager.store import CapacityManagementStore
from tests.capacity_fixtures import (
    AUTHORITY_ID,
    DEVELOPMENT_REPORTER_INCARNATION,
    DEVELOPMENT_SUBJECT_ID,
    DEVELOPMENT_SUBJECT_INCARNATION,
    POOL_REPORTER_GB10_ID,
    POOL_REPORTER_OLDLAB_ID,
    SUBJECT_ID,
    SUBJECT_INCARNATION,
    demand_snapshot,
    development_projection,
    fleet_manifest,
    fleet_with_development_template,
    pool_observation,
    subject_configuration,
)

OPERATOR_TOKEN = "operator-api-secret"
DEMAND_TOKEN = "demand-api-secret"
GB10_TOKEN = "gb10-api-secret"
OLDLAB_TOKEN = "oldlab-api-secret"
DYNAMIC_DEMAND_TOKEN = "dynamic-demand-api-secret"


def _hash(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def _principal(
    principal_id: str,
    token: str,
    scopes: list[str],
    *,
    subject_id: UUID | None = None,
    subject_incarnation: UUID | None = None,
    demand_reporter_incarnation: UUID | None = None,
    pool_id: str | None = None,
    pool_reporter_incarnation: UUID | None = None,
) -> dict[str, object]:
    return {
        "principal_id": principal_id,
        "token_sha256": _hash(token),
        "scopes": scopes,
        "subject_id": None if subject_id is None else str(subject_id),
        "subject_incarnation": (None if subject_incarnation is None else str(subject_incarnation)),
        "demand_reporter_incarnation": (
            None if demand_reporter_incarnation is None else str(demand_reporter_incarnation)
        ),
        "pool_id": pool_id,
        "pool_reporter_incarnation": (
            None if pool_reporter_incarnation is None else str(pool_reporter_incarnation)
        ),
    }


def _owner_file(path: Path, value: str) -> Path:
    path.write_text(value, encoding="utf-8")
    path.chmod(0o600)
    return path


async def _reset_capacity_database(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    async with session_factory() as session, session.begin():
        for table in reversed(Base.metadata.sorted_tables):
            if table.name != CapacityAuthorityState.__tablename__:
                await session.execute(delete(table))
        await session.execute(
            update(CapacityAuthorityState)
            .where(CapacityAuthorityState.singleton_id == 1)
            .values(
                authority_incarnation=AUTHORITY_ID,
                writer_epoch=0,
                recovery_state="shadow",
                increase_freeze=True,
                increase_freeze_reason="initial_shadow_freeze",
                executable_new_capacity_ceiling=0,
                global_pending_slot_ceiling=0,
                global_pending_job_ceiling=0,
                global_submission_rate_ceiling=0,
            )
        )


class BlockingAllocator:
    def __init__(self) -> None:
        self.started = Event()
        self.release = Event()

    def __call__(self, value):  # type: ignore[no-untyped-def]
        self.started.set()
        if not self.release.wait(timeout=5):
            raise TimeoutError("test allocator release timed out")
        return allocate_shadow(value)


@pytest.fixture
def operator_headers() -> dict[str, str]:
    return {"Authorization": f"Bearer {OPERATOR_TOKEN}"}


@pytest.fixture
def reporter_headers() -> dict[str, str]:
    return {"Authorization": f"Bearer {DEMAND_TOKEN}"}


@pytest.fixture
async def api_context(
    tmp_path: Path,
    capacity_postgres_url: str,
    capacity_session_factory: async_sessionmaker[AsyncSession],
) -> AsyncIterator[tuple[TestClient, FastAPI, CapacityManagerSettings, BlockingAllocator]]:
    await _reset_capacity_database(capacity_session_factory)
    registry_path = _owner_file(
        tmp_path / "principals.json",
        json.dumps(
            {
                "schema_version": 1,
                "principals": [
                    _principal(
                        "fleet-operator",
                        OPERATOR_TOKEN,
                        [
                            "capacity:configure:fleet",
                            "capacity:configure:subject",
                            "capacity:configure:activate",
                            "capacity:project:development",
                            "capacity:reconcile",
                            "capacity:read",
                        ],
                    ),
                    _principal(
                        "dev-reporter",
                        DEMAND_TOKEN,
                        ["capacity:report:demand"],
                        subject_id=SUBJECT_ID,
                        subject_incarnation=SUBJECT_INCARNATION,
                        demand_reporter_incarnation=subject_configuration().demand_reporter_incarnation,
                    ),
                    _principal(
                        "gb10-reporter",
                        GB10_TOKEN,
                        ["capacity:report:pool"],
                        pool_id="gb10",
                        pool_reporter_incarnation=POOL_REPORTER_GB10_ID,
                    ),
                    _principal(
                        "oldlab-reporter",
                        OLDLAB_TOKEN,
                        ["capacity:report:pool"],
                        pool_id="oldlab",
                        pool_reporter_incarnation=POOL_REPORTER_OLDLAB_ID,
                    ),
                ],
            }
        ),
    )
    db_url_path = _owner_file(tmp_path / "database-url", capacity_postgres_url)
    dummy_cert = _owner_file(tmp_path / "server.crt", "test")
    dummy_key = _owner_file(tmp_path / "server.key", "test")
    dummy_ca = _owner_file(tmp_path / "client-ca.crt", "test")
    settings = CapacityManagerSettings(
        principals_file=registry_path,
        db_url_file=db_url_path,
        expected_authority_incarnation=AUTHORITY_ID,
        tls_cert_file=dummy_cert,
        tls_key_file=dummy_key,
        tls_client_ca_file=dummy_ca,
        allocation_timeout_seconds=5,
    )
    allocator = BlockingAllocator()
    app = create_app(
        settings,
        verifier=CapacityPrincipalVerifier.from_file(registry_path),
        allocator=allocator,
    )
    with TestClient(app) as client:
        assert client.get("/healthz").status_code == 200
        fleet = fleet_with_development_template()
        subject = subject_configuration(fleet)
        fleet_response = client.put(
            "/v1/config-proposals/fleet",
            headers={
                "Authorization": f"Bearer {OPERATOR_TOKEN}",
                "Idempotency-Key": str(uuid4()),
            },
            json=fleet.model_dump(mode="json"),
        )
        assert fleet_response.status_code == 200, fleet_response.text
        subject_response = client.put(
            f"/v1/config-proposals/subjects/{SUBJECT_ID}",
            headers={
                "Authorization": f"Bearer {OPERATOR_TOKEN}",
                "Idempotency-Key": str(uuid4()),
            },
            json=subject.model_dump(mode="json"),
        )
        assert subject_response.status_code == 200, subject_response.text
        fleet_proposal = fleet_response.json()
        subject_proposal = subject_response.json()
        activation = {
            "schema_version": 1,
            "expected_configuration_epoch": 0,
            "fleet": {
                "schema_version": 1,
                "scope": "fleet",
                "generation": fleet_proposal["generation"],
                "digest": fleet_proposal["digest"],
                "subject_id": None,
                "subject_incarnation": None,
            },
            "subjects": [
                {
                    "schema_version": 1,
                    "scope": "subject",
                    "generation": subject_proposal["generation"],
                    "digest": subject_proposal["digest"],
                    "subject_id": str(SUBJECT_ID),
                    "subject_incarnation": str(SUBJECT_INCARNATION),
                }
            ],
        }
        activation_response = client.post(
            "/v1/config-activations",
            headers={
                "Authorization": f"Bearer {OPERATOR_TOKEN}",
                "Idempotency-Key": str(uuid4()),
            },
            json=activation,
        )
        assert activation_response.status_code == 200, activation_response.text
        assert (
            client.put(
                f"/v1/reports/demand/{SUBJECT_ID}",
                headers={"Authorization": f"Bearer {DEMAND_TOKEN}"},
                json=demand_snapshot(sequence=1).model_dump(mode="json"),
            ).status_code
            == 200
        )
        assert (
            client.put(
                "/v1/reports/pools/gb10",
                headers={"Authorization": f"Bearer {GB10_TOKEN}"},
                json=pool_observation(sequence=1, pool_id="gb10").model_dump(mode="json"),
            ).status_code
            == 200
        )
        assert (
            client.put(
                "/v1/reports/pools/oldlab",
                headers={"Authorization": f"Bearer {OLDLAB_TOKEN}"},
                json=pool_observation(sequence=1, pool_id="oldlab").model_dump(mode="json"),
            ).status_code
            == 200
        )
        yield client, app, settings, allocator
    await _reset_capacity_database(capacity_session_factory)


@contextmanager
def hold_reconciliation_open(
    client: TestClient,
    allocator: BlockingAllocator,
    operator_headers: dict[str, str],
) -> Iterator[None]:
    responses: list[object] = []

    def trigger() -> None:
        responses.append(client.post("/v1/shadow-reconciliations", headers=operator_headers))

    thread = Thread(target=trigger, daemon=True)
    thread.start()
    assert allocator.started.wait(timeout=2)
    try:
        yield
    finally:
        allocator.release.set()
        thread.join(timeout=5)
        assert not thread.is_alive()
        assert responses and responses[0].status_code == 200  # type: ignore[union-attr]


def test_shadow_api_exposes_exactly_the_approved_routes(
    api_context: tuple[TestClient, FastAPI, CapacityManagerSettings, BlockingAllocator],
) -> None:
    _client, app, _settings, _allocator = api_context
    routes = {(route.path, tuple(sorted(route.methods or ()))) for route in app.routes}
    assert routes == {
        ("/healthz", ("GET",)),
        ("/v1/config-proposals/fleet", ("PUT",)),
        ("/v1/config-proposals/subjects/{subject_id}", ("PUT",)),
        ("/v1/config-activations", ("POST",)),
        ("/v1/development-projections/{subject_id}", ("PUT",)),
        ("/v1/reports/demand/{subject_id}", ("PUT",)),
        ("/v1/reports/pools/{pool_id}", ("PUT",)),
        ("/v1/shadow-reconciliations", ("POST",)),
        ("/v1/status", ("GET",)),
        ("/v1/status/subjects", ("GET",)),
        ("/v1/status/pools", ("GET",)),
        ("/v1/shadow-epochs/{allocation_epoch}", ("GET",)),
        ("/v1/shadow-epochs/{allocation_epoch}/allocations", ("GET",)),
        ("/v1/audit-events", ("GET",)),
        ("/metrics", ("GET",)),
    }


def test_lifecycle_can_project_and_authenticate_a_personal_demand_reporter(
    api_context: tuple[TestClient, FastAPI, CapacityManagerSettings, BlockingAllocator],
    operator_headers: dict[str, str],
) -> None:
    client, _app, _settings, _allocator = api_context
    projection = development_projection().model_copy(
        update={"demand_reporter_token_sha256": _hash(DYNAMIC_DEMAND_TOKEN)}
    )
    response = client.put(
        f"/v1/development-projections/{DEVELOPMENT_SUBJECT_ID}",
        headers=operator_headers | {"Idempotency-Key": str(uuid4())},
        json=projection.model_dump(mode="json"),
    )
    assert response.status_code == 200, response.text
    assert response.json()["subject"]["display_name"] == "dev-alice"
    assert response.json()["subject"]["min_slots"] == 0

    report = demand_snapshot(
        subject_id=DEVELOPMENT_SUBJECT_ID,
        subject_incarnation=DEVELOPMENT_SUBJECT_INCARNATION,
        reporter_incarnation=DEVELOPMENT_REPORTER_INCARNATION,
    )
    accepted = client.put(
        f"/v1/reports/demand/{DEVELOPMENT_SUBJECT_ID}",
        headers={"Authorization": f"Bearer {DYNAMIC_DEMAND_TOKEN}"},
        json=report.model_dump(mode="json"),
    )
    assert accepted.status_code == 200, accepted.text

    rejected = client.put(
        f"/v1/reports/demand/{DEVELOPMENT_SUBJECT_ID}",
        headers={"Authorization": "Bearer wrong-dynamic-token"},
        json=report.model_copy(update={"sequence": 2}).model_dump(mode="json"),
    )
    assert rejected.status_code == 401


def test_lifecycle_can_retire_personal_subject_and_fence_its_reporter(
    api_context: tuple[TestClient, FastAPI, CapacityManagerSettings, BlockingAllocator],
    operator_headers: dict[str, str],
) -> None:
    client, _app, _settings, _allocator = api_context
    create = development_projection().model_copy(
        update={"demand_reporter_token_sha256": _hash(DYNAMIC_DEMAND_TOKEN)}
    )
    created = client.put(
        f"/v1/development-projections/{DEVELOPMENT_SUBJECT_ID}",
        headers=operator_headers | {"Idempotency-Key": str(uuid4())},
        json=create.model_dump(mode="json"),
    )
    assert created.status_code == 200, created.text

    retirement = create.model_copy(
        update={
            "expected_configuration_epoch": 2,
            "operation_kind": "destroy",
            "operation_id": uuid4(),
            "operation_epoch": 2,
            "configuration_generation": 2,
        }
    )
    forged = retirement.model_copy(
        update={
            "operation_id": uuid4(),
            "candidate_publication_sha256": "0" * 64,
        }
    )
    rejected_forgery = client.put(
        f"/v1/development-projections/{DEVELOPMENT_SUBJECT_ID}",
        headers=operator_headers | {"Idempotency-Key": str(uuid4())},
        json=forged.model_dump(mode="json"),
    )
    assert rejected_forgery.status_code == 409

    retirement_key = uuid4()
    retired = client.put(
        f"/v1/development-projections/{DEVELOPMENT_SUBJECT_ID}",
        headers=operator_headers | {"Idempotency-Key": str(retirement_key)},
        json=retirement.model_dump(mode="json"),
    )
    assert retired.status_code == 200, retired.text
    assert retired.json()["subject"]["lifecycle_state"] == "disabled"
    assert retired.json()["subject"]["min_slots"] == 0
    assert retired.json()["subject"]["max_slots"] == 0

    replay = client.put(
        f"/v1/development-projections/{DEVELOPMENT_SUBJECT_ID}",
        headers=operator_headers | {"Idempotency-Key": str(retirement_key)},
        json=retirement.model_dump(mode="json"),
    )
    assert replay.status_code == 200, replay.text
    assert replay.json()["configuration_epoch"] == retired.json()["configuration_epoch"]
    assert replay.json()["replayed"] is True

    report = demand_snapshot(
        subject_id=DEVELOPMENT_SUBJECT_ID,
        subject_incarnation=DEVELOPMENT_SUBJECT_INCARNATION,
        reporter_incarnation=DEVELOPMENT_REPORTER_INCARNATION,
    ).model_copy(update={"configuration_generation": 2})
    rejected = client.put(
        f"/v1/reports/demand/{DEVELOPMENT_SUBJECT_ID}",
        headers={"Authorization": f"Bearer {DYNAMIC_DEMAND_TOKEN}"},
        json=report.model_dump(mode="json"),
    )
    assert rejected.status_code == 401

    subjects = client.get("/v1/status/subjects", headers=operator_headers)
    assert subjects.status_code == 200
    assert all(
        item["subject_id"] != str(DEVELOPMENT_SUBJECT_ID)
        for item in subjects.json()["items"]
    )


@pytest.mark.parametrize("operation_kind", ["update", "capacity"])
def test_lifecycle_can_restore_an_unregistered_personal_subject_from_full_evidence(
    operation_kind: str,
    api_context: tuple[TestClient, FastAPI, CapacityManagerSettings, BlockingAllocator],
    operator_headers: dict[str, str],
) -> None:
    client, _app, _settings, _allocator = api_context
    projection = development_projection().model_copy(
        update={
            "operation_kind": operation_kind,
            "demand_reporter_token_sha256": _hash(DYNAMIC_DEMAND_TOKEN),
        }
    )
    response = client.put(
        f"/v1/development-projections/{DEVELOPMENT_SUBJECT_ID}",
        headers=operator_headers | {"Idempotency-Key": str(uuid4())},
        json=projection.model_dump(mode="json"),
    )
    assert response.status_code == 200, response.text
    assert response.json()["subject"]["configuration_generation"] == (
        projection.configuration_generation
    )


async def test_streaming_body_limit_rejects_oversized_body_without_content_length() -> None:
    downstream_called = False
    sent: list[dict[str, object]] = []
    incoming = iter(
        [
            {
                "type": "http.request",
                "body": b"x" * MAX_CONTRACT_BYTES,
                "more_body": True,
            },
            {"type": "http.request", "body": b"x", "more_body": False},
        ]
    )

    async def downstream(_scope, _receive, _send):  # type: ignore[no-untyped-def]
        nonlocal downstream_called
        downstream_called = True

    async def receive() -> dict[str, object]:
        return next(incoming)

    async def send(message: dict[str, object]) -> None:
        sent.append(message)

    middleware = RequestBodyLimitMiddleware(downstream, maximum_bytes=MAX_CONTRACT_BYTES)
    await middleware({"type": "http", "headers": []}, receive, send)

    assert not downstream_called
    assert sent[0]["status"] == 413
    assert sent[1]["body"] == b'{"detail":"request too large"}'


def test_reporter_cannot_publish_config_or_impersonate_subject(
    api_context: tuple[TestClient, FastAPI, CapacityManagerSettings, BlockingAllocator],
    reporter_headers: dict[str, str],
) -> None:
    client, _app, _settings, _allocator = api_context
    assert (
        client.put(
            "/v1/config-proposals/fleet",
            headers=reporter_headers | {"Idempotency-Key": str(uuid4())},
            json=fleet_manifest().model_dump(mode="json"),
        ).status_code
        == 403
    )
    other = uuid4()
    impersonated = demand_snapshot(sequence=2).model_copy(update={"subject_id": other})
    response = client.put(
        f"/v1/reports/demand/{other}",
        headers=reporter_headers,
        json=impersonated.model_dump(mode="json"),
    )
    assert response.status_code == 403
    assert response.json() == {"detail": "forbidden"}


def test_real_server_requires_mutual_tls(
    api_context: tuple[TestClient, FastAPI, CapacityManagerSettings, BlockingAllocator],
) -> None:
    _client, _app, settings, _allocator = api_context
    options = build_uvicorn_kwargs(settings)
    assert options["ssl_cert_reqs"] == ssl.CERT_REQUIRED
    assert options["ssl_ca_certs"] == str(settings.tls_client_ca_file)


def test_concurrent_reconciliation_trigger_is_rejected(
    api_context: tuple[TestClient, FastAPI, CapacityManagerSettings, BlockingAllocator],
    operator_headers: dict[str, str],
) -> None:
    client, _app, _settings, allocator = api_context
    with hold_reconciliation_open(client, allocator, operator_headers):
        response = client.post(
            "/v1/shadow-reconciliations",
            headers=operator_headers,
        )
    assert response.status_code == 409
    assert response.json() == {"detail": "shadow reconciliation already running"}


def test_body_limit_and_status_pagination_are_bounded(
    api_context: tuple[TestClient, FastAPI, CapacityManagerSettings, BlockingAllocator],
    operator_headers: dict[str, str],
) -> None:
    client, _app, _settings, _allocator = api_context
    oversized = client.put(
        "/v1/config-proposals/fleet",
        headers=operator_headers
        | {
            "Idempotency-Key": str(uuid4()),
            "Content-Type": "application/json",
        },
        content=b"x" * (MAX_CONTRACT_BYTES + 1),
    )
    assert oversized.status_code == 413
    assert client.get("/v1/status/subjects?limit=501", headers=operator_headers).status_code == 422
    assert client.get("/v1/status", headers=operator_headers).status_code == 200


def test_metrics_have_no_subject_or_environment_labels(
    api_context: tuple[TestClient, FastAPI, CapacityManagerSettings, BlockingAllocator],
    operator_headers: dict[str, str],
) -> None:
    client, _app, _settings, _allocator = api_context
    response = client.get("/metrics", headers=operator_headers)
    assert response.status_code == 200
    body = response.text
    assert "loom_capacity_manager_ready" in body
    assert "subject_id=" not in body
    assert "environment=" not in body
    assert str(SUBJECT_ID) not in body


async def test_health_fails_closed_after_another_process_takes_writer_fence(
    api_context: tuple[TestClient, FastAPI, CapacityManagerSettings, BlockingAllocator],
    capacity_session_factory: async_sessionmaker[AsyncSession],
) -> None:
    client, _app, _settings, _allocator = api_context
    async with capacity_session_factory() as session:
        await CapacityManagementStore().register_writer(
            session,
            AUTHORITY_ID,
            expected_epoch=1,
        )
        await session.commit()

    response = client.get("/healthz")
    assert response.status_code == 503
    assert response.json() == {
        "status": "not-ready",
        "executable_new_capacity_ceiling": 0,
    }
