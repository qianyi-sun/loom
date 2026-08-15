"""Authenticated and bounded capacity-manager HTTP surface tests."""

from __future__ import annotations

import asyncio
import base64
import hashlib
import json
import ssl
from collections.abc import AsyncIterator, Iterator
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from threading import Event, Thread
from uuid import UUID, uuid4

import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy import delete, event, select, text, update
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from loom_capacity_manager.allocator import ShadowAllocatorError, allocate_shadow
from loom_capacity_manager.api import (
    RequestBodyLimitMiddleware,
    _health_payload,
    _writer_matches_authority,
    create_app,
)
from loom_capacity_manager.auth import CapacityPrincipalVerifier
from loom_capacity_manager.config import CapacityManagerSettings, build_uvicorn_kwargs
from loom_capacity_manager.contracts import MAX_CONTRACT_BYTES, canonical_digest
from loom_capacity_manager.executable_contracts import ExecutionActivationV2
from loom_capacity_manager.models import (
    Base,
    CapacityAllocation,
    CapacityAllocationEpoch,
    CapacityAuditEvent,
    CapacityAuthorityState,
)
from loom_capacity_manager.ownership import public_key_fingerprint
from loom_capacity_manager.reconciler import reconcile_shadow_once
from loom_capacity_manager.store import CapacityManagementStore, StaleWriterError, WriterFence
from tests.capacity_execution_fixtures import (
    execution_policy,
    register_execution_executors,
    setup_execution,
)
from tests.capacity_fixtures import (
    AUTHORITY_ID,
    DEMAND_REPORTER_ID,
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
OLDLAB_EXECUTOR_TOKEN = "oldlab-executor-secret"
OLDLAB_V2_EXECUTOR_TOKEN = "oldlab-v2-executor-secret"
OLDLAB_EXECUTOR_INCARNATION = UUID("00000000-0000-4000-8000-000000000601")
OLDLAB_OWNERSHIP_PRIVATE_KEY = Ed25519PrivateKey.from_private_bytes(b"\x03" * 32)


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
    executor_id: str | None = None,
    executor_incarnation: UUID | None = None,
    executor_pool_generation: int | None = None,
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
        "executor_id": executor_id,
        "executor_incarnation": (
            None if executor_incarnation is None else str(executor_incarnation)
        ),
        "executor_pool_generation": executor_pool_generation,
    }


def _owner_file(path: Path, value: str) -> Path:
    path.write_text(value, encoding="utf-8")
    path.chmod(0o600)
    return path


async def _reset_capacity_database(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    async with session_factory() as session, session.begin():
        for table_name in (
            "capacity_allocations",
            "capacity_allocation_epochs",
            "capacity_execution_executors",
            "capacity_execution_epochs",
        ):
            await session.execute(
                text(f"ALTER TABLE {table_name} DISABLE TRIGGER USER")
            )
        await session.execute(
            text(
                "ALTER TABLE capacity_authority_state DISABLE TRIGGER "
                "capacity_authority_execution_transition_guard"
            )
        )
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
                execution_epoch=0,
                execution_state="shadow",
                execution_manifest_sha256=None,
                global_pending_slot_ceiling=0,
                global_pending_job_ceiling=0,
                global_submission_rate_ceiling=0,
            )
        )
        for table in reversed(Base.metadata.sorted_tables):
            if table.name != CapacityAuthorityState.__tablename__:
                await session.execute(delete(table))
        await session.execute(
            text(
                "ALTER TABLE capacity_authority_state ENABLE TRIGGER "
                "capacity_authority_execution_transition_guard"
            )
        )
        for table_name in reversed(
            (
                "capacity_allocations",
                "capacity_allocation_epochs",
                "capacity_execution_executors",
                "capacity_execution_epochs",
            )
        ):
            await session.execute(
                text(f"ALTER TABLE {table_name} ENABLE TRIGGER USER")
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


class ActiveTimeoutAllocator:
    async def __call__(self, value):  # type: ignore[no-untyped-def]
        del value
        await asyncio.sleep(5)


class ActiveInvalidAllocator:
    def __call__(self, value):  # type: ignore[no-untyped-def]
        del value
        raise ShadowAllocatorError("synthetic invalid allocation")


class ActiveUnexpectedAllocator:
    def __call__(self, value):  # type: ignore[no-untyped-def]
        del value
        raise RuntimeError("synthetic unexpected allocation failure")


class ActiveCommitFailureStore(CapacityManagementStore):
    def __init__(self) -> None:
        super().__init__(execution_policy=execution_policy())
        self._input_loads = 0

    async def load_allocation_input(self, session, writer):  # type: ignore[no-untyped-def]
        self._input_loads += 1
        if self._input_loads == 2:
            raise RuntimeError("synthetic executable transaction failure")
        return await super().load_allocation_input(session, writer)


class AuthorityResolutionFailureStore(CapacityManagementStore):
    def __init__(self) -> None:
        super().__init__(execution_policy=execution_policy())
        self.reconcile_failure_records = 0

    async def execution_authority(self, session):  # type: ignore[no-untyped-def]
        del session
        raise RuntimeError("synthetic authority resolution failure")

    async def record_reconcile_failure(self, *args, **kwargs):  # type: ignore[no-untyped-def]
        self.reconcile_failure_records += 1
        return await super().record_reconcile_failure(*args, **kwargs)


class CommitAndRecorderFailureStore(ActiveCommitFailureStore):
    async def record_reconcile_failure(self, *args, **kwargs):  # type: ignore[no-untyped-def]
        del args, kwargs
        raise RuntimeError("synthetic recorder failure")


class FailureInputDriftStore(CapacityManagementStore):
    def __init__(self) -> None:
        super().__init__(execution_policy=execution_policy())
        self._input_loads = 0
        self.expected_digest: str | None = None
        self.observed_digest: str | None = None

    async def load_allocation_input(self, session, writer):  # type: ignore[no-untyped-def]
        value = await super().load_allocation_input(session, writer)
        self._input_loads += 1
        if self._input_loads == 1:
            self.expected_digest = canonical_digest(value)
            return value
        drifted = value.model_copy(
            update={"existing_pending_slots": value.existing_pending_slots + 1}
        )
        self.observed_digest = canonical_digest(drifted)
        return drifted


class ExpiredFreshnessFenceStore(CapacityManagementStore):
    def __init__(self) -> None:
        super().__init__(execution_policy=execution_policy())

    def allocation_input_valid_until(self, value):  # type: ignore[no-untyped-def]
        del value
        return datetime(2000, 1, 1, tzinfo=UTC)


class CrossingFreshnessFenceStore(CapacityManagementStore):
    def __init__(self) -> None:
        super().__init__(execution_policy=execution_policy())
        self.validity_checks = 0

    def allocation_input_valid_until(self, value):  # type: ignore[no-untyped-def]
        del value
        self.validity_checks += 1
        year = 2999 if self.validity_checks == 1 else 2000
        return datetime(year, 1, 1, tzinfo=UTC)


class HostileConstraintSearchPathStore(CapacityManagementStore):
    def __init__(self) -> None:
        super().__init__(execution_policy=execution_policy())
        self._input_loads = 0

    async def load_allocation_input(self, session, writer):  # type: ignore[no-untyped-def]
        value = await super().load_allocation_input(session, writer)
        self._input_loads += 1
        if self._input_loads == 2:
            await session.execute(
                text("SET LOCAL search_path TO capacity_constraint_decoy, public")
            )
        return value


class ActivateBeforeShadowCommitStore(CapacityManagementStore):
    def __init__(self, activation: ExecutionActivationV2) -> None:
        super().__init__(execution_policy=execution_policy())
        self._activation = activation

    async def commit_shadow_epoch(self, session, writer, epoch):  # type: ignore[no-untyped-def]
        await self.activate_execution_epoch(
            session,
            self._activation,
            actor="activation-operator",
            idempotency_key=UUID(int=882),
        )
        return await super().commit_shadow_epoch(session, writer, epoch)


async def _ingest_fresh_execution_inputs(
    session: AsyncSession,
    store: CapacityManagementStore,
) -> None:
    await store.ingest_demand_snapshot(
        session,
        demand_snapshot(sequence=1, pending_attempt_ids=("attempt-pending",)),
        actor="development",
    )
    for pool_id in ("gb10", "oldlab"):
        await store.ingest_pool_observation(
            session,
            pool_observation(sequence=1, pool_id=pool_id),
            actor=f"{pool_id}-reporter",
        )


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
                            "capacity:grant:manage",
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
                    _principal(
                        "oldlab-executor",
                        OLDLAB_EXECUTOR_TOKEN,
                        ["capacity:execute:pool"],
                        pool_id="oldlab",
                        executor_id="oldlab-executor",
                        executor_incarnation=OLDLAB_EXECUTOR_INCARNATION,
                    ),
                ],
            }
        ),
    )
    db_url_path = _owner_file(tmp_path / "database-url", capacity_postgres_url)
    dummy_cert = _owner_file(tmp_path / "server.crt", "test")
    dummy_key = _owner_file(tmp_path / "server.key", "test")
    dummy_ca = _owner_file(tmp_path / "client-ca.crt", "test")
    ownership_keys = _owner_file(
        tmp_path / "ownership-public-keys.json",
        json.dumps(
            {
                "schema_version": 1,
                "keys": [
                    {
                        "signing_key_id": "oldlab-key-1",
                        "public_key_base64": base64.b64encode(
                            OLDLAB_OWNERSHIP_PRIVATE_KEY.public_key().public_bytes(
                                encoding=serialization.Encoding.Raw,
                                format=serialization.PublicFormat.Raw,
                            )
                        ).decode("ascii"),
                    }
                ],
            }
        ),
    )
    settings = CapacityManagerSettings(
        principals_file=registry_path,
        db_url_file=db_url_path,
        expected_authority_incarnation=AUTHORITY_ID,
        tls_cert_file=dummy_cert,
        tls_key_file=dummy_key,
        tls_client_ca_file=dummy_ca,
        ownership_public_keys_file=ownership_keys,
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


@pytest.fixture
async def api_context_v2_executor_generation(
    tmp_path: Path,
    capacity_postgres_url: str,
    capacity_session_factory: async_sessionmaker[AsyncSession],
) -> AsyncIterator[tuple[TestClient, FastAPI, CapacityManagerSettings, BlockingAllocator]]:
    await _reset_capacity_database(capacity_session_factory)
    registry_path = _owner_file(
        tmp_path / "principals-v2.json",
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
                            "capacity:grant:manage",
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
                    _principal(
                        "oldlab-executor-v2",
                        OLDLAB_V2_EXECUTOR_TOKEN,
                        ["capacity:execute:pool"],
                        pool_id="oldlab",
                        executor_id="oldlab-executor",
                        executor_incarnation=OLDLAB_EXECUTOR_INCARNATION,
                        executor_pool_generation=1,
                    ),
                ],
            }
        ),
    )
    db_url_path = _owner_file(tmp_path / "database-url-v2", capacity_postgres_url)
    dummy_cert = _owner_file(tmp_path / "server-v2.crt", "test")
    dummy_key = _owner_file(tmp_path / "server-v2.key", "test")
    dummy_ca = _owner_file(tmp_path / "client-ca-v2.crt", "test")
    ownership_keys = _owner_file(
        tmp_path / "ownership-public-keys-v2.json",
        json.dumps(
            {
                "schema_version": 1,
                "keys": [
                    {
                        "signing_key_id": "oldlab-key-1",
                        "public_key_base64": base64.b64encode(
                            OLDLAB_OWNERSHIP_PRIVATE_KEY.public_key().public_bytes(
                                encoding=serialization.Encoding.Raw,
                                format=serialization.PublicFormat.Raw,
                            )
                        ).decode("ascii"),
                    }
                ],
            }
        ),
    )
    settings = CapacityManagerSettings(
        principals_file=registry_path,
        db_url_file=db_url_path,
        expected_authority_incarnation=AUTHORITY_ID,
        tls_cert_file=dummy_cert,
        tls_key_file=dummy_key,
        tls_client_ca_file=dummy_ca,
        ownership_public_keys_file=ownership_keys,
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
        (
            "/v1/reports/protected-releases/{subject_id}/{shape_instance_id}",
            ("PUT",),
        ),
        ("/v1/reports/pools/{pool_id}", ("PUT",)),
        ("/v1/executors/{pool_id}/registration", ("PUT",)),
        ("/v1/executors/{pool_id}/heartbeat", ("PUT",)),
        ("/v1/executors/{pool_id}/checkpoint", ("GET",)),
        ("/v1/executors/{pool_id}/inventory", ("PUT",)),
        ("/v1/grants/reservations/{tranche_id}", ("PUT",)),
        (
            "/v1/executors/{pool_id}/reservations/{tranche_id}/accept",
            ("POST",),
        ),
        ("/v1/executors/{pool_id}/intents/{intent_id}/bootstrap", ("POST",)),
        ("/v1/grants/launch-permits/{permit_id}", ("PUT",)),
        ("/v1/executors/{pool_id}/permits/{permit_id}/consume", ("POST",)),
        ("/v1/executors/{pool_id}/intents/{intent_id}/close", ("POST",)),
        (
            "/v1/executors/{pool_id}/reservations/{tranche_id}/release",
            ("POST",),
        ),
        ("/v2/executors/{pool_id}/heartbeat", ("PUT",)),
        ("/v2/executors/{pool_id}/checkpoint", ("GET",)),
        ("/v2/executors/{pool_id}/work", ("GET",)),
        ("/v2/executors/{pool_id}/inventory", ("PUT",)),
        (
            "/v2/executors/{pool_id}/reservations/{tranche_id}/accept",
            ("POST",),
        ),
        ("/v2/executors/{pool_id}/intents/{intent_id}/bootstrap", ("POST",)),
        ("/v2/executors/{pool_id}/permits/{permit_id}/consume", ("POST",)),
        ("/v2/executors/{pool_id}/permits/{permit_id}/recover", ("POST",)),
        ("/v2/executors/{pool_id}/intents/{intent_id}/close", ("POST",)),
        (
            "/v2/executors/{pool_id}/reservations/{tranche_id}/release",
            ("POST",),
        ),
        (
            "/v2/reports/protected-releases/{subject_id}/{shape_instance_id}",
            ("PUT",),
        ),
        ("/v1/shadow-reconciliations", ("POST",)),
        ("/v1/status", ("GET",)),
        ("/v1/status/subjects", ("GET",)),
        ("/v1/status/pools", ("GET",)),
        ("/v1/status/executors", ("GET",)),
        ("/v1/status/reservations", ("GET",)),
        ("/v1/shadow-epochs/{allocation_epoch}", ("GET",)),
        ("/v1/shadow-epochs/{allocation_epoch}/allocations", ("GET",)),
        ("/v1/audit-events", ("GET",)),
        ("/metrics", ("GET",)),
    }


def test_v2_executor_work_route_rejects_legacy_v1_principal_without_generation(
    api_context: tuple[TestClient, FastAPI, CapacityManagerSettings, BlockingAllocator],
) -> None:
    client, _app, _settings, _allocator = api_context
    headers = {"Authorization": f"Bearer {OLDLAB_EXECUTOR_TOKEN}"}

    own_pool = client.get("/v2/executors/oldlab/work", headers=headers)
    crossed_pool = client.get("/v2/executors/gb10/work", headers=headers)

    assert own_pool.status_code == 403
    assert crossed_pool.status_code == 403


def test_v2_executor_routes_require_exact_positive_generation(
    api_context_v2_executor_generation: tuple[
        TestClient, FastAPI, CapacityManagerSettings, BlockingAllocator
    ],
) -> None:
    client, app, _settings, _allocator = api_context_v2_executor_generation
    headers = {"Authorization": f"Bearer {OLDLAB_V2_EXECUTOR_TOKEN}"}

    work = client.get("/v2/executors/oldlab/work", headers=headers)
    assert work.status_code == 200
    assert work.json() is None

    wrong_generation = client.put(
        "/v2/executors/oldlab/heartbeat",
        headers=headers,
        json={
            "schema_version": 2,
            "execution": {
                "schema_version": 2,
                "authority_incarnation": str(AUTHORITY_ID),
                "writer_epoch": app.state.writer.writer_epoch,
                "configuration_epoch": 1,
                "execution_epoch": 1,
                "execution_manifest_sha256": "0" * 64,
                "execution_state": "active",
                "executable_new_capacity_ceiling": 1,
                "executable_new_capacity_rate_per_minute": 1,
                "trusted_fleet_release_sha256": "1" * 64,
            },
            "executor_id": "oldlab-executor",
            "executor_incarnation": str(OLDLAB_EXECUTOR_INCARNATION),
            "pool_id": "oldlab",
            "pool_generation": 2,
            "heartbeat_sequence": 1,
            "journal_sequence": 0,
            "journal_digest": "0" * 64,
            "journal_checkpoint_sequence": 0,
            "journal_checkpoint_digest": "0" * 64,
            "executable": True,
        },
    )
    assert wrong_generation.status_code == 403, wrong_generation.text


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
        item["subject_id"] != str(DEVELOPMENT_SUBJECT_ID) for item in subjects.json()["items"]
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

    protected_release = {
        "schema_version": 1,
        "authority_incarnation": str(AUTHORITY_ID),
        "writer_epoch": 1,
        "configuration_epoch": 1,
        "allocation_epoch": 1,
        "tranche_id": str(uuid4()),
        "shape_instance_id": "shape-0001",
        "intent_id": str(uuid4()),
        "subject_id": str(other),
        "subject_incarnation": str(SUBJECT_INCARNATION),
        "reporter_incarnation": str(DEMAND_REPORTER_ID),
        "deployment_generation": 1,
        "pool_id": "gb10",
        "pool_generation": 1,
        "bootstrap_registration_epoch": 0,
        "protected_registration_epoch": 1,
        "bootstrap_revoked": True,
        "protected_release_sha256": "a" * 64,
        "executable": False,
    }
    response = client.put(
        f"/v1/reports/protected-releases/{other}/shape-0001",
        headers=reporter_headers | {"Idempotency-Key": str(uuid4())},
        json=protected_release,
    )
    assert response.status_code == 403
    assert response.json() == {"detail": "forbidden"}


def test_pool_executor_registration_heartbeat_inventory_and_cross_pool_rbac(
    api_context: tuple[TestClient, FastAPI, CapacityManagerSettings, BlockingAllocator],
    operator_headers: dict[str, str],
) -> None:
    client, app, _settings, _allocator = api_context
    registration = {
        "schema_version": 1,
        "executor_id": "oldlab-executor",
        "executor_incarnation": str(OLDLAB_EXECUTOR_INCARNATION),
        "pool_id": "oldlab",
        "pool_generation": 1,
        "signing_key_id": "oldlab-key-1",
        "signing_key_sha256": public_key_fingerprint(OLDLAB_OWNERSHIP_PRIVATE_KEY.public_key()),
        "local_authority_sha256": "b" * 64,
        "executable": False,
    }
    registered = client.put(
        "/v1/executors/oldlab/registration",
        headers=operator_headers | {"Idempotency-Key": str(uuid4())},
        json=registration,
    )
    assert registered.status_code == 200, registered.text
    heartbeat = {
        "schema_version": 1,
        "authority_incarnation": str(AUTHORITY_ID),
        "writer_epoch": app.state.writer.writer_epoch,
        "executor_id": "oldlab-executor",
        "executor_incarnation": str(OLDLAB_EXECUTOR_INCARNATION),
        "pool_id": "oldlab",
        "pool_generation": 1,
        "heartbeat_sequence": 1,
        "journal_sequence": 0,
        "journal_digest": "0" * 64,
        "executable": False,
    }
    executor_headers = {"Authorization": f"Bearer {OLDLAB_EXECUTOR_TOKEN}"}

    checkpoint = client.get(
        "/v1/executors/oldlab/checkpoint",
        headers=executor_headers,
    )
    assert checkpoint.status_code == 200, checkpoint.text
    assert checkpoint.json()["journal_sequence"] == 0
    assert checkpoint.json()["journal_digest"] == "0" * 64
    assert checkpoint.json()["executable"] is False
    assert (
        client.get(
            "/v1/executors/gb10/checkpoint",
            headers=executor_headers,
        ).status_code
        == 403
    )

    response = client.put(
        "/v1/executors/oldlab/heartbeat",
        headers=executor_headers,
        json=heartbeat,
    )
    assert response.status_code == 200, response.text
    assert response.json()["heartbeat_sequence"] == 1
    inventory = {
        "schema_version": 1,
        "authority_incarnation": str(AUTHORITY_ID),
        "writer_epoch": app.state.writer.writer_epoch,
        "executor_id": "oldlab-executor",
        "executor_incarnation": str(OLDLAB_EXECUTOR_INCARNATION),
        "pool_id": "oldlab",
        "pool_generation": 1,
        "inventory_sequence": 1,
        "journal_sequence": 0,
        "journal_digest": "0" * 64,
        "journal_checkpoint_sequence": 0,
        "journal_checkpoint_digest": "0" * 64,
        "complete": True,
        "records": [],
        "executable": False,
    }
    inventory_response = client.put(
        "/v1/executors/oldlab/inventory",
        headers=executor_headers,
        json=inventory,
    )
    assert inventory_response.status_code == 200, inventory_response.text
    assert inventory_response.json()["inventory_sequence"] == 1
    assert (
        client.put(
            "/v1/executors/gb10/heartbeat",
            headers=executor_headers,
            json=heartbeat | {"pool_id": "gb10"},
        ).status_code
        == 403
    )


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


async def test_allocation_reconcile_keeps_shadow_execution_bindings_null(
    api_context: tuple[TestClient, FastAPI, CapacityManagerSettings, BlockingAllocator],
    operator_headers: dict[str, str],
    capacity_session_factory: async_sessionmaker[AsyncSession],
) -> None:
    client, _app, _settings, allocator = api_context
    allocator.release.set()

    response = client.post("/v1/shadow-reconciliations", headers=operator_headers)

    assert response.status_code == 200, response.text
    async with capacity_session_factory() as session:
        epoch = (await session.execute(select(CapacityAllocationEpoch))).scalar_one()
        allocations = (await session.execute(select(CapacityAllocation))).scalars().all()
    assert epoch.status == "shadow"
    assert epoch.executable is False
    assert epoch.execution_epoch is None
    assert epoch.execution_manifest_sha256 is None
    assert allocations
    assert all(allocation.mode == "shadow" for allocation in allocations)
    assert all(allocation.executable is False for allocation in allocations)
    assert all(allocation.execution_epoch is None for allocation in allocations)
    assert all(allocation.execution_manifest_sha256 is None for allocation in allocations)
    async with capacity_session_factory() as session:
        with pytest.raises(DBAPIError):
            async with session.begin_nested():
                await session.execute(update(CapacityAllocation).values(mode="executable"))


async def test_allocation_shadow_commit_preserves_prepared_execution_freeze(
    capacity_session: AsyncSession,
) -> None:
    fixture = await setup_execution(capacity_session, execution_policy=execution_policy())
    await fixture.store.prepare_execution_epoch(
        capacity_session,
        fixture.request,
        actor="activation-operator",
        idempotency_key=UUID(int=879),
    )
    session_factory = async_sessionmaker(
        bind=capacity_session.bind,
        expire_on_commit=False,
        join_transaction_mode="create_savepoint",
    )

    result = await reconcile_shadow_once(
        session_factory,
        fixture.writer,
        store=fixture.store,
    )

    assert result.status == "committed"
    epoch = (await capacity_session.execute(select(CapacityAllocationEpoch))).scalar_one()
    authority = (await capacity_session.execute(select(CapacityAuthorityState))).scalar_one()
    assert epoch.status == "shadow"
    assert epoch.execution_epoch is None
    assert epoch.execution_manifest_sha256 is None
    assert authority.execution_state == "prepared"
    assert authority.increase_freeze is True
    assert authority.increase_freeze_reason == "execution_epoch_prepared"


async def test_allocation_activation_during_shadow_commit_retries_as_executable(
    capacity_session: AsyncSession,
) -> None:
    fixture = await setup_execution(capacity_session, execution_policy=execution_policy())
    await _ingest_fresh_execution_inputs(capacity_session, fixture.store)
    prepared = await fixture.store.prepare_execution_epoch(
        capacity_session,
        fixture.request,
        actor="activation-operator",
        idempotency_key=UUID(int=883),
    )
    await register_execution_executors(capacity_session, fixture, prepared)
    activation = ExecutionActivationV2(
        authority_incarnation=AUTHORITY_ID,
        expected_writer_epoch=fixture.writer.writer_epoch,
        execution_epoch=prepared.execution_epoch,
        execution_manifest_sha256=prepared.execution_manifest_sha256,
        executable_new_capacity_ceiling=1,
        executable_new_capacity_rate_per_minute=1,
    )
    session_factory = async_sessionmaker(
        bind=capacity_session.bind,
        expire_on_commit=False,
        join_transaction_mode="create_savepoint",
    )

    store = ActivateBeforeShadowCommitStore(activation)
    result = await reconcile_shadow_once(
        session_factory,
        fixture.writer,
        store=store,
    )

    assert result.status == "committed"
    assert result.attempt_count == 2
    epoch = (await capacity_session.execute(select(CapacityAllocationEpoch))).scalar_one()
    assert epoch.status == "executable"
    assert epoch.execution_epoch == prepared.execution_epoch
    assert epoch.execution_manifest_sha256 == prepared.execution_manifest_sha256


async def test_allocation_reconcile_commits_fresh_plan_under_active_execution_fence(
    capacity_session: AsyncSession,
) -> None:
    fixture = await setup_execution(capacity_session, execution_policy=execution_policy())
    await _ingest_fresh_execution_inputs(capacity_session, fixture.store)
    prepared = await fixture.store.prepare_execution_epoch(
        capacity_session,
        fixture.request,
        actor="activation-operator",
        idempotency_key=UUID(int=880),
    )
    await register_execution_executors(capacity_session, fixture, prepared)
    active = await fixture.store.activate_execution_epoch(
        capacity_session,
        ExecutionActivationV2(
            authority_incarnation=AUTHORITY_ID,
            expected_writer_epoch=fixture.writer.writer_epoch,
            execution_epoch=prepared.execution_epoch,
            execution_manifest_sha256=prepared.execution_manifest_sha256,
            executable_new_capacity_ceiling=1,
            executable_new_capacity_rate_per_minute=1,
        ),
        actor="activation-operator",
        idempotency_key=UUID(int=881),
    )
    session_factory = async_sessionmaker(
        bind=capacity_session.bind,
        expire_on_commit=False,
        join_transaction_mode="create_savepoint",
    )

    result = await reconcile_shadow_once(
        session_factory,
        fixture.writer,
        store=fixture.store,
    )

    assert result.status == "committed"
    epoch = (await capacity_session.execute(select(CapacityAllocationEpoch))).scalar_one()
    allocations = (await capacity_session.execute(select(CapacityAllocation))).scalars().all()
    current_input = await fixture.store.load_allocation_input(capacity_session, fixture.writer)
    assert epoch.status == "executable"
    assert epoch.executable is True
    assert epoch.execution_epoch == active.execution_epoch
    assert epoch.execution_manifest_sha256 == active.execution_manifest_sha256
    assert epoch.input_valid_until == fixture.store.allocation_input_valid_until(current_input)
    assert epoch.sealed is True
    assert epoch.allocation_count == len(allocations)
    assert epoch.complete_payload["execution"]["allocation_epoch"] == epoch.allocation_epoch
    assert epoch.complete_payload["executable_new_capacity_ceiling"] == 1
    assert allocations
    assert all(allocation.mode == "executable" for allocation in allocations)
    assert all(allocation.executable is True for allocation in allocations)
    assert all(allocation.execution_epoch == active.execution_epoch for allocation in allocations)
    assert all(
        allocation.execution_manifest_sha256 == active.execution_manifest_sha256
        for allocation in allocations
    )
    with pytest.raises(DBAPIError):
        async with capacity_session.begin_nested():
            await capacity_session.execute(update(CapacityAllocation).values(mode="shadow"))

    shadow_parent = CapacityAllocationEpoch(
        writer_epoch=fixture.writer.writer_epoch,
        configuration_epoch=active.configuration_epoch,
        input_digest="9" * 64,
        status="shadow",
        failure_reason=None,
        complete_payload={"schema_version": 1},
        executable=False,
        execution_epoch=None,
        execution_manifest_sha256=None,
        committed_at=None,
    )
    capacity_session.add(shadow_parent)
    await capacity_session.flush()
    shadow_child = CapacityAllocation(
        allocation_epoch=shadow_parent.allocation_epoch,
        subject_id=uuid4(),
        subject_incarnation=uuid4(),
        deployment_generation=1,
        pool_id="gb10",
        desired_shapes=[],
        desired_resources={},
        commitments=[],
        drains=[],
        allowances=[],
        witness={},
        mode="shadow",
        executable=False,
        execution_epoch=None,
        execution_manifest_sha256=None,
    )
    capacity_session.add(shadow_child)
    await capacity_session.flush()
    with pytest.raises(DBAPIError, match="allocation epoch mode binding is immutable"):
        async with capacity_session.begin_nested():
            await capacity_session.execute(
                update(CapacityAllocationEpoch)
                .where(CapacityAllocationEpoch.allocation_epoch == shadow_parent.allocation_epoch)
                .values(
                    status="executable",
                    executable=True,
                    execution_epoch=active.execution_epoch,
                    execution_manifest_sha256=active.execution_manifest_sha256,
                )
            )
            await capacity_session.execute(
                update(CapacityAllocation)
                .where(CapacityAllocation.id == shadow_child.id)
                .values(
                    mode="executable",
                    executable=True,
                    execution_epoch=active.execution_epoch,
                    execution_manifest_sha256=active.execution_manifest_sha256,
                )
            )

    capacity_session.add(
        CapacityAllocation(
            allocation_epoch=shadow_parent.allocation_epoch,
            subject_id=uuid4(),
            subject_incarnation=uuid4(),
            deployment_generation=1,
            pool_id="oldlab",
            desired_shapes=[],
            desired_resources={},
            commitments=[],
            drains=[],
            allowances=[],
            witness={},
            mode="executable",
            executable=True,
            execution_epoch=active.execution_epoch,
            execution_manifest_sha256=active.execution_manifest_sha256,
        )
    )
    with pytest.raises(DBAPIError, match="allocation binding must match its parent epoch"):
        async with capacity_session.begin_nested():
            await capacity_session.flush()


async def test_executable_allocation_evidence_is_immutable_and_not_reparentable(
    capacity_session: AsyncSession,
) -> None:
    fixture, active = await _active_execution_fixture(capacity_session)
    session_factory = async_sessionmaker(
        bind=capacity_session.bind,
        expire_on_commit=False,
        join_transaction_mode="create_savepoint",
    )
    result = await reconcile_shadow_once(
        session_factory,
        fixture.writer,
        store=fixture.store,
    )
    assert result.status == "committed"
    parent = (await capacity_session.execute(select(CapacityAllocationEpoch))).scalar_one()
    child = (
        (await capacity_session.execute(select(CapacityAllocation))).scalars().first()
    )
    assert child is not None
    second_parent = CapacityAllocationEpoch(
        writer_epoch=fixture.writer.writer_epoch,
        configuration_epoch=active.configuration_epoch,
        input_digest="8" * 64,
        status="executable",
        failure_reason=None,
        complete_payload={"schema_version": 2, "allocations": []},
        executable=True,
        execution_epoch=active.execution_epoch,
        execution_manifest_sha256=active.execution_manifest_sha256,
        input_valid_until=datetime.now(UTC),
        sealed=True,
        allocation_count=0,
        committed_at=parent.committed_at,
    )
    capacity_session.add(second_parent)
    await capacity_session.flush()

    with pytest.raises(DBAPIError, match="executable allocation epoch is immutable"):
        async with capacity_session.begin_nested():
            await capacity_session.execute(
                update(CapacityAllocationEpoch)
                .where(CapacityAllocationEpoch.allocation_epoch == parent.allocation_epoch)
                .values(complete_payload={"tampered": True})
            )
    with pytest.raises(DBAPIError, match="executable allocation is immutable"):
        async with capacity_session.begin_nested():
            await capacity_session.execute(
                update(CapacityAllocation)
                .where(CapacityAllocation.id == child.id)
                .values(desired_shapes=[{"tampered": True}])
            )
    with pytest.raises(DBAPIError, match="executable allocation is immutable"):
        async with capacity_session.begin_nested():
            await capacity_session.execute(
                update(CapacityAllocation)
                .where(CapacityAllocation.id == child.id)
                .values(allocation_epoch=second_parent.allocation_epoch)
            )
    with pytest.raises(DBAPIError, match="executable allocation is append-only"):
        async with capacity_session.begin_nested():
            await capacity_session.execute(
                delete(CapacityAllocation).where(CapacityAllocation.id == child.id)
            )
    with pytest.raises(DBAPIError, match="executable allocation epoch is append-only"):
        async with capacity_session.begin_nested():
            await capacity_session.execute(
                delete(CapacityAllocationEpoch).where(
                    CapacityAllocationEpoch.allocation_epoch
                    == second_parent.allocation_epoch
                )
            )
    with pytest.raises(DBAPIError, match="executable allocation epoch is sealed"):
        async with capacity_session.begin_nested():
            capacity_session.add(
                CapacityAllocation(
                    allocation_epoch=parent.allocation_epoch,
                    subject_id=uuid4(),
                    subject_incarnation=uuid4(),
                    deployment_generation=1,
                    pool_id="oldlab",
                    desired_shapes=[],
                    desired_resources={},
                    commitments=[],
                    drains=[],
                    allowances=[],
                    witness={},
                    mode="executable",
                    executable=True,
                    execution_epoch=parent.execution_epoch,
                    execution_manifest_sha256=parent.execution_manifest_sha256,
                )
            )
            await capacity_session.flush()


async def _active_execution_fixture(capacity_session: AsyncSession):  # type: ignore[no-untyped-def]
    fixture = await setup_execution(capacity_session, execution_policy=execution_policy())
    await _ingest_fresh_execution_inputs(capacity_session, fixture.store)
    prepared = await fixture.store.prepare_execution_epoch(
        capacity_session,
        fixture.request,
        actor="activation-operator",
        idempotency_key=uuid4(),
    )
    await register_execution_executors(capacity_session, fixture, prepared)
    active = await fixture.store.activate_execution_epoch(
        capacity_session,
        ExecutionActivationV2(
            authority_incarnation=AUTHORITY_ID,
            expected_writer_epoch=fixture.writer.writer_epoch,
            execution_epoch=prepared.execution_epoch,
            execution_manifest_sha256=prepared.execution_manifest_sha256,
            executable_new_capacity_ceiling=1,
            executable_new_capacity_rate_per_minute=1,
        ),
        actor="activation-operator",
        idempotency_key=uuid4(),
    )
    return fixture, active


@pytest.mark.parametrize(
    "allocator, timeout, store_factory, expected_event",
    [
        (
            ActiveTimeoutAllocator(),
            0.01,
            None,
            "shadow_allocation_timeout",
        ),
        (
            ActiveInvalidAllocator(),
            1.0,
            None,
            "shadow_allocation_invalid",
        ),
        (
            ActiveUnexpectedAllocator(),
            1.0,
            None,
            "shadow_allocation_failure",
        ),
        (
            allocate_shadow,
            1.0,
            ActiveCommitFailureStore,
            "shadow_allocation_failure",
        ),
    ],
)
async def test_allocation_active_failure_records_false_null_evidence_and_freezes(
    capacity_session: AsyncSession,
    allocator,  # type: ignore[no-untyped-def]
    timeout: float,
    store_factory,  # type: ignore[no-untyped-def]
    expected_event: str,
) -> None:
    fixture, active = await _active_execution_fixture(capacity_session)
    store = fixture.store if store_factory is None else store_factory()
    session_factory = async_sessionmaker(
        bind=capacity_session.bind,
        expire_on_commit=False,
        join_transaction_mode="create_savepoint",
    )

    result = await reconcile_shadow_once(
        session_factory,
        fixture.writer,
        allocator=allocator,
        allocation_timeout_seconds=timeout,
        store=store,
    )

    assert result.status == "failed"
    failed = (await capacity_session.execute(select(CapacityAllocationEpoch))).scalar_one()
    authority = (await capacity_session.execute(select(CapacityAuthorityState))).scalar_one()
    audit = (
        (
            await capacity_session.execute(
                select(CapacityAuditEvent).order_by(CapacityAuditEvent.id.desc())
            )
        )
        .scalars()
        .first()
    )
    assert failed.status == "failed"
    assert failed.executable is False
    assert failed.execution_epoch is None
    assert failed.execution_manifest_sha256 is None
    assert authority.execution_epoch == active.execution_epoch
    assert authority.execution_manifest_sha256 == active.execution_manifest_sha256
    assert authority.increase_freeze is True
    assert authority.increase_freeze_reason == expected_event
    assert audit is not None and audit.event_kind == expected_event


async def test_allocation_authority_resolution_failure_uses_locked_durable_authority(
    capacity_session: AsyncSession,
) -> None:
    fixture, active = await _active_execution_fixture(capacity_session)
    store = AuthorityResolutionFailureStore()
    session_factory = async_sessionmaker(
        bind=capacity_session.bind,
        expire_on_commit=False,
        join_transaction_mode="create_savepoint",
    )

    result = await reconcile_shadow_once(
        session_factory,
        fixture.writer,
        store=store,
    )

    assert result.status == "failed"
    assert store.reconcile_failure_records == 1
    failed = (await capacity_session.execute(select(CapacityAllocationEpoch))).scalar_one()
    authority = (await capacity_session.execute(select(CapacityAuthorityState))).scalar_one()
    audit = (
        (
            await capacity_session.execute(
                select(CapacityAuditEvent).order_by(CapacityAuditEvent.id.desc())
            )
        )
        .scalars()
        .first()
    )
    assert failed.status == "failed"
    assert failed.writer_epoch == fixture.writer.writer_epoch
    assert failed.configuration_epoch == active.configuration_epoch
    assert failed.executable is False
    assert failed.execution_epoch is None
    assert failed.execution_manifest_sha256 is None
    assert authority.increase_freeze is True
    assert authority.increase_freeze_reason == "shadow_allocation_invalid"
    assert audit is not None
    assert audit.object_binding == {
        "allocation_epoch": failed.allocation_epoch,
        "configuration_epoch": active.configuration_epoch,
        "execution_epoch": active.execution_epoch,
        "execution_manifest_sha256": active.execution_manifest_sha256,
        "writer_epoch": fixture.writer.writer_epoch,
    }


async def test_allocation_commit_and_failure_recorder_errors_propagate_hard_failure(
    capacity_session: AsyncSession,
) -> None:
    fixture, _active = await _active_execution_fixture(capacity_session)
    session_factory = async_sessionmaker(
        bind=capacity_session.bind,
        expire_on_commit=False,
        join_transaction_mode="create_savepoint",
    )

    with pytest.raises(RuntimeError, match="failed to persist reconciliation failure") as caught:
        await reconcile_shadow_once(
            session_factory,
            fixture.writer,
            store=CommitAndRecorderFailureStore(),
        )

    assert caught.value.__cause__ is not None
    assert "synthetic recorder failure" in str(caught.value.__cause__)


async def test_allocation_input_drift_during_failure_record_still_freezes(
    capacity_session: AsyncSession,
) -> None:
    fixture, active = await _active_execution_fixture(capacity_session)
    store = FailureInputDriftStore()
    session_factory = async_sessionmaker(
        bind=capacity_session.bind,
        expire_on_commit=False,
        join_transaction_mode="create_savepoint",
    )

    result = await reconcile_shadow_once(
        session_factory,
        fixture.writer,
        allocator=ActiveInvalidAllocator(),
        store=store,
    )

    assert result.status == "failed"
    assert store.expected_digest is not None
    assert store.observed_digest is not None
    assert store.expected_digest != store.observed_digest
    assert (
        await capacity_session.execute(select(CapacityAllocationEpoch))
    ).scalar_one_or_none() is None
    authority = (await capacity_session.execute(select(CapacityAuthorityState))).scalar_one()
    audit = (
        (
            await capacity_session.execute(
                select(CapacityAuditEvent).order_by(CapacityAuditEvent.id.desc())
            )
        )
        .scalars()
        .first()
    )
    assert authority.execution_epoch == active.execution_epoch
    assert authority.increase_freeze is True
    assert authority.increase_freeze_reason == "shadow_allocation_invalid"
    assert audit is not None
    assert audit.detail["input_digest"] == store.expected_digest
    assert audit.detail["observed_input_digest"] == store.observed_digest
    assert audit.detail["input_drifted"] is True


async def test_allocation_commit_rejects_an_expired_freshness_fence(
    capacity_session: AsyncSession,
) -> None:
    fixture, _active = await _active_execution_fixture(capacity_session)
    store = ExpiredFreshnessFenceStore()
    session_factory = async_sessionmaker(
        bind=capacity_session.bind,
        expire_on_commit=False,
        join_transaction_mode="create_savepoint",
    )

    result = await reconcile_shadow_once(
        session_factory,
        fixture.writer,
        max_attempts=1,
        store=store,
    )

    assert result.status == "input-contention"
    assert (
        await capacity_session.execute(select(CapacityAllocationEpoch))
    ).scalar_one_or_none() is None
    authority = (await capacity_session.execute(select(CapacityAuthorityState))).scalar_one()
    assert authority.increase_freeze is True
    assert authority.increase_freeze_reason == "shadow_allocation_input_contention"


async def test_allocation_commit_rechecks_freshness_after_persistence(
    capacity_session: AsyncSession,
) -> None:
    fixture, _active = await _active_execution_fixture(capacity_session)
    store = CrossingFreshnessFenceStore()
    session_factory = async_sessionmaker(
        bind=capacity_session.bind,
        expire_on_commit=False,
        join_transaction_mode="create_savepoint",
    )

    result = await reconcile_shadow_once(
        session_factory,
        fixture.writer,
        max_attempts=1,
        store=store,
    )

    assert store.validity_checks == 2
    assert result.status == "input-contention"
    assert (
        await capacity_session.execute(select(CapacityAllocationEpoch))
    ).scalar_one_or_none() is None
    authority = (await capacity_session.execute(select(CapacityAuthorityState))).scalar_one()
    assert authority.increase_freeze is True
    assert authority.increase_freeze_reason == "shadow_allocation_input_contention"


async def test_allocation_commit_schema_qualifies_seal_guard_under_hostile_search_path(
    capacity_session: AsyncSession,
) -> None:
    fixture, _active = await _active_execution_fixture(capacity_session)
    await capacity_session.execute(text("CREATE SCHEMA capacity_constraint_decoy"))
    await capacity_session.execute(
        text(
            "CREATE TABLE capacity_constraint_decoy.guard_decoy ("
            "value integer CONSTRAINT capacity_executable_allocation_seal_guard "
            "UNIQUE DEFERRABLE INITIALLY DEFERRED)"
        )
    )
    await capacity_session.execute(
        text(
            """
            CREATE OR REPLACE FUNCTION public.capacity_executable_allocation_seal_guard()
            RETURNS trigger
            LANGUAGE plpgsql
            SET search_path = pg_catalog
            AS $function$
            BEGIN
              IF NEW.status = 'executable' THEN
                RAISE EXCEPTION 'synthetic executable seal failure'
                  USING ERRCODE = '23514';
              END IF;
              RETURN NULL;
            END;
            $function$
            """
        )
    )
    session_factory = async_sessionmaker(
        bind=capacity_session.bind,
        expire_on_commit=False,
        join_transaction_mode="create_savepoint",
    )

    result = await reconcile_shadow_once(
        session_factory,
        fixture.writer,
        max_attempts=1,
        store=HostileConstraintSearchPathStore(),
    )

    assert result.status == "failed"
    failed_epoch = (
        await capacity_session.execute(select(CapacityAllocationEpoch))
    ).scalar_one()
    assert failed_epoch.status == "failed"
    assert failed_epoch.executable is False
    authority = (await capacity_session.execute(select(CapacityAuthorityState))).scalar_one()
    assert authority.increase_freeze is True
    assert authority.increase_freeze_reason == "shadow_allocation_failure"


async def test_allocation_commit_samples_final_time_after_seal_validation(
    capacity_session: AsyncSession,
) -> None:
    fixture, _active = await _active_execution_fixture(capacity_session)
    session_factory = async_sessionmaker(
        bind=capacity_session.bind,
        expire_on_commit=False,
        join_transaction_mode="create_savepoint",
    )
    statements: list[str] = []

    def capture_statement(
        _connection,  # type: ignore[no-untyped-def]
        _cursor,  # type: ignore[no-untyped-def]
        statement: str,
        _parameters,  # type: ignore[no-untyped-def]
        _context,  # type: ignore[no-untyped-def]
        _executemany: bool,
    ) -> None:
        statements.append(statement)

    engine = capacity_session.bind
    assert engine is not None
    event.listen(engine.sync_engine, "before_cursor_execute", capture_statement)
    try:
        result = await reconcile_shadow_once(
            session_factory,
            fixture.writer,
            max_attempts=1,
            store=fixture.store,
        )
    finally:
        event.remove(engine.sync_engine, "before_cursor_execute", capture_statement)

    assert result.status == "committed"
    clock_samples = [
        index for index, statement in enumerate(statements) if "clock_timestamp" in statement
    ]
    assert clock_samples
    seal_validation = statements.index(
        "SET CONSTRAINTS public.capacity_executable_allocation_seal_guard IMMEDIATE"
    )
    assert seal_validation < clock_samples[-1]


async def test_allocation_active_failure_rejects_a_stale_authority_fence(
    capacity_session: AsyncSession,
) -> None:
    fixture, _active = await _active_execution_fixture(capacity_session)
    successor = await fixture.store.register_writer(
        capacity_session,
        AUTHORITY_ID,
        expected_epoch=fixture.writer.writer_epoch,
    )

    with pytest.raises(StaleWriterError):
        await fixture.store.record_reconcile_failure(
            capacity_session,
            fixture.writer,
            event_kind="shadow_allocation_failure",
            reason="synthetic stale active failure",
            expected_input_digest=None,
        )

    assert successor.writer_epoch > fixture.writer.writer_epoch
    assert (
        await capacity_session.execute(select(CapacityAllocationEpoch))
    ).scalar_one_or_none() is None


async def test_allocation_active_failure_freeze_blocks_later_promotion(
    capacity_session: AsyncSession,
) -> None:
    fixture, _active = await _active_execution_fixture(capacity_session)
    await fixture.store.record_reconcile_failure(
        capacity_session,
        fixture.writer,
        event_kind="shadow_allocation_failure",
        reason="synthetic active failure",
        expected_input_digest=None,
    )
    session_factory = async_sessionmaker(
        bind=capacity_session.bind,
        expire_on_commit=False,
        join_transaction_mode="create_savepoint",
    )

    result = await reconcile_shadow_once(
        session_factory,
        fixture.writer,
        max_attempts=1,
        store=fixture.store,
    )

    assert result.status == "failed"
    statuses = (await capacity_session.execute(select(CapacityAllocationEpoch.status))).scalars()
    assert set(statuses) == {"failed"}


async def test_shadow_read_routes_hide_executable_epochs(
    api_context: tuple[TestClient, FastAPI, CapacityManagerSettings, BlockingAllocator],
    operator_headers: dict[str, str],
    capacity_session_factory: async_sessionmaker[AsyncSession],
) -> None:
    client, _app, _settings, _allocator = api_context
    async with capacity_session_factory() as session:
        fixture, _active = await _active_execution_fixture(session)
        await session.commit()
    result = await reconcile_shadow_once(
        capacity_session_factory,
        fixture.writer,
        store=fixture.store,
    )
    assert result.status == "committed"
    assert result.allocation_epoch is not None

    epoch_response = client.get(
        f"/v1/shadow-epochs/{result.allocation_epoch}",
        headers=operator_headers,
    )
    allocations_response = client.get(
        f"/v1/shadow-epochs/{result.allocation_epoch}/allocations",
        headers=operator_headers,
    )

    assert epoch_response.status_code == 404
    assert epoch_response.json() == {"detail": "shadow epoch not found"}
    assert allocations_response.status_code == 404
    assert allocations_response.json() == {"detail": "shadow epoch not found"}


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
    status_response = client.get("/v1/status", headers=operator_headers)
    assert status_response.status_code == 200
    status_body = status_response.json()
    assert status_body["execution_state"] == "shadow"
    assert status_body["execution_epoch"] == 0
    assert status_body["execution_manifest_sha256"] is None
    assert status_body["executable_new_capacity_ceiling"] == 0
    assert (
        client.post(
            "/v1/execution-activations",
            headers=operator_headers | {"Idempotency-Key": str(uuid4())},
            json={},
        ).status_code
        == 404
    )


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


def test_active_execution_ceiling_does_not_fence_current_writer_health() -> None:
    writer = WriterFence(authority_incarnation=AUTHORITY_ID, writer_epoch=3)
    authority = CapacityAuthorityState(
        authority_incarnation=AUTHORITY_ID,
        writer_epoch=3,
        executable_new_capacity_ceiling=1,
    )

    assert _writer_matches_authority(writer, authority)
    assert json.loads(_health_payload(ready=True, executable_new_capacity_ceiling=1)) == {
        "status": "ready",
        "executable_new_capacity_ceiling": 1,
    }
