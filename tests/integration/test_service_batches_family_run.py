"""Integration test: POST /batches with trial_config.family_run enabled
seeds batch_family_state + records the resolved spec (#672 PR-3).

Verifies the batches route wire:

1. ``trial_config.family_run`` override is validated + merged with any
   catalog defaults into a ResolvedFamilyRunSpec.
2. ``batches.family_run_spec`` is populated with the resolved dict.
3. One ``batch_family_state`` row is inserted per family, with
   ``task_sequence`` reflecting the sequencer's ordering.

The state_backend is stubbed so this test does not touch MinIO. The
end-to-end scheduler + orchestrator loop is covered by the family_run
scheduler tests (tests/integration/test_family_run_scheduler.py
family) landed in PR-1/PR-2.
"""

from __future__ import annotations

import hashlib
from collections.abc import AsyncIterator
from datetime import UTC, datetime
from typing import Any
from uuid import UUID, uuid4

import boto3
import httpx
import pytest
from botocore.config import Config
from fastapi import FastAPI
from sqlalchemy import create_engine, delete, insert, select, text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.orm import sessionmaker

from loom.admin_secret import AdminSecretVerifier
from loom.db.schema import (
    Batch,
    ProviderConnection,
    ProviderConnectionShare,
    Task,
    Team,
    TeamMembership,
    TeamQuota,
    Token,
    Trial,
    User,
    Worker,
)
from loom_service.app import create_app
from loom_service.config import LoomServiceSettings
from tests.integration.gateway_db import delete_lifecycle_authorities

RAW_ADMIN_TOKEN = "loom_admin_" + "A" * 43


def _family_run_override(
    adapter_params: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "enabled": True,
        "family_key_extractor": {
            "name": "instance_id_prefix",
            "params": {"depth": 2},
        },
        "sequencer": {"name": "submitted_order"},
        "advance_predicate": {"name": "always_on_terminal"},
        "adapter": {
            "name": "skill_patcher_llm",
            "params": adapter_params or {},
        },
        "failure_policy": {"name": "skip_and_advance"},
        "state_backend": {"name": "s3_artifacts"},
        "mount_path": "/root/.skills",
    }


async def _post_family_batch(
    app: FastAPI,
    raw_token: str,
    *,
    name: str,
    adapter_params: dict[str, Any] | None = None,
) -> httpx.Response:
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://svc") as ac:
        return await ac.post(
            "/api/v1/batches",
            headers={"Authorization": f"Bearer {raw_token}"},
            json={
                "name": name,
                "task_filter": {
                    "task_ids": [
                        "benchmarks/skillflow-iterative/family-a/task-1",
                    ],
                },
                "trial_config": {
                    "agent_name": "oracle",
                    "agent_model": None,
                    "family_run": _family_run_override(adapter_params),
                },
            },
        )


def _insert_provider_connection(
    postgres_url: str,
    owner_team_id: UUID,
    *,
    display_name: str,
    ownership: _FakeStateBackend,
) -> UUID:
    connection_id = uuid4()
    sync_engine = create_engine(postgres_url)
    sl = sessionmaker(sync_engine)
    try:
        with sl() as s:
            s.execute(
                insert(ProviderConnection).values(
                    id=connection_id,
                    team_id=owner_team_id,
                    provider_type="openai-compatible",
                    display_name=display_name,
                    base_url="https://provider.invalid/v1",
                    upstream_host="provider.invalid",
                    encrypted_api_key_ref=(f"loom://test/{owner_team_id}/{connection_id}"),
                    created_by="test",
                )
            )
            s.commit()
    finally:
        sync_engine.dispose()
    ownership.owned_connection_ids.add(connection_id)
    return connection_id


def _insert_team(
    postgres_url: str,
    *,
    name: str,
    ownership: _FakeStateBackend,
) -> UUID:
    team_id = uuid4()
    sync_engine = create_engine(postgres_url)
    sl = sessionmaker(sync_engine)
    try:
        with sl() as s:
            s.execute(insert(Team).values(id=team_id, name=name))
            s.commit()
    finally:
        sync_engine.dispose()
    ownership.owned_team_ids.add(team_id)
    return team_id


def _task_config(task_id: str) -> dict[str, object]:
    return {
        "schema_version": "1",
        "task": {"id": task_id, "name": task_id},
        "environment": {"os": "linux", "docker_image": "alpine"},
        "agent": {"name": "oracle"},
        "verifier": {"name": "pytest"},
        "steps": [{"name": "main"}],
    }


class _FakeStateBackend:
    """Records initialize() calls and returns deterministic URIs so the
    test can assert without standing up MinIO."""

    def __init__(self) -> None:
        self.initialized: list[tuple[UUID, str]] = []
        self.owned_team_ids: set[UUID] = set()
        self.owned_connection_ids: set[UUID] = set()

    async def initialize(
        self,
        *,
        batch_id: UUID,
        family_key: str,
        params: dict[str, Any],
    ) -> str:
        self.initialized.append((batch_id, family_key))
        return f"s3://fake/family-state/{batch_id}/{family_key}/state.tar.gz"


@pytest.fixture
async def camp_setup(
    monkeypatch: pytest.MonkeyPatch,
    postgres_url: str,
) -> AsyncIterator[tuple[FastAPI, str, UUID, _FakeStateBackend]]:
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
    app.state.session_factory = async_sessionmaker(engine, expire_on_commit=False)
    app.state.admin_secret_verifier = AdminSecretVerifier.from_token(RAW_ADMIN_TOKEN)
    app.state.minio_client = boto3.client(
        "s3",
        endpoint_url=settings.minio_endpoint,
        aws_access_key_id=settings.minio_access_key.get_secret_value(),
        aws_secret_access_key=settings.minio_secret_key.get_secret_value(),
        region_name=settings.minio_region,
        config=Config(signature_version="s3v4"),
    )
    app.state.http_client = httpx.AsyncClient(base_url="http://cp")

    # Stub the state backend so the family-run seed path exercises the
    # resolver / seeder + insert without touching MinIO.
    fake_backend = _FakeStateBackend()
    monkeypatch.setattr(
        "loom_service.routes.batches._build_service_state_backend",
        lambda request: fake_backend,
    )

    team_id = uuid4()
    fake_backend.owned_team_ids.add(team_id)
    username = f"FamilyRunOwner-{team_id.hex[:8]}"
    user_id = uuid4()
    raw = f"loom_team_{uuid4().hex}"
    worker_id = uuid4()
    owned_task_ids = (
        "benchmarks/skillflow-iterative/family-a/task-1",
        "benchmarks/skillflow-iterative/family-a/task-2",
    )
    sync_engine = create_engine(postgres_url)
    sl = sessionmaker(sync_engine)
    with sl() as s:
        s.execute(insert(Team).values(id=team_id, name=f"t-{team_id}"))
        s.execute(
            insert(User).values(
                id=user_id,
                username=username,
                username_normalized=username.casefold(),
                status="active",
                is_platform_admin=False,
            )
        )
        s.execute(
            insert(Token).values(
                token_hash=hashlib.sha256(raw.encode()).digest(),
                type="team",
                scopes=["submit", "read:own"],
                team_id=team_id,
                created_by_user_id=user_id,
                issued_at=datetime.now(UTC),
            )
        )
        s.execute(
            insert(TeamMembership).values(
                team_id=team_id,
                user_id=user_id,
                role="owner",
            )
        )
        # Two tasks in a shared family under a distinct benchmark path.
        for slug in owned_task_ids:
            s.execute(
                insert(Task).values(
                    id=slug,
                    checksum="x" * 64,
                    config=_task_config(slug),
                    source="local",
                    license="Apache-2.0",
                )
            )
        # Fake worker so POST /batches passes the no-worker check.
        s.execute(
            insert(Worker).values(
                id=worker_id,
                hostname="fixture-worker",
                version="test",
                capabilities=[{"backend": "docker"}, {"backend": "fake"}],
                registered_at=datetime.now(UTC),
                last_seen_at=datetime.now(UTC),
                status="active",
            )
        )
        s.commit()
    try:
        yield app, raw, team_id, fake_backend
    finally:
        await app.state.http_client.aclose()
        await engine.dispose()
        with sl() as s:
            owned_team_ids = tuple(fake_backend.owned_team_ids)
            trial_rows = tuple(
                s.execute(
                    select(Trial.id, Trial.team_id).where(Trial.team_id.in_(owned_team_ids))
                ).all()
            )
            batch_rows = tuple(
                s.execute(
                    select(Batch.id, Batch.team_id).where(Batch.team_id.in_(owned_team_ids))
                ).all()
            )
            # Cascade cleanup: batch_family_state.batch_id has ON DELETE
            # CASCADE, so deleting Batch is enough.
            s.execute(delete(Trial).where(Trial.team_id.in_(owned_team_ids)))
            s.execute(delete(Batch).where(Batch.team_id.in_(owned_team_ids)))
            s.execute(
                delete(ProviderConnection).where(
                    ProviderConnection.id.in_(tuple(fake_backend.owned_connection_ids))
                )
            )
            s.execute(delete(Token).where(Token.team_id.in_(owned_team_ids)))
            s.execute(delete(Task).where(Task.id.in_(owned_task_ids)))
            s.execute(delete(Worker).where(Worker.id == worker_id))
            delete_lifecycle_authorities(
                s,
                bindings=(
                    *(("trial", "trial", str(row.id), row.team_id) for row in trial_rows),
                    *(("event", "trial", str(row.id), row.team_id) for row in trial_rows),
                    *(("run", "batch", str(row.id), row.team_id) for row in batch_rows),
                ),
            )
            s.execute(delete(TeamQuota).where(TeamQuota.team_id.in_(owned_team_ids)))
            s.execute(delete(TeamMembership).where(TeamMembership.team_id.in_(owned_team_ids)))
            s.execute(delete(User).where(User.id == user_id))
            s.execute(delete(Team).where(Team.id.in_(owned_team_ids)))
            s.commit()
        sync_engine.dispose()


async def test_post_batch_with_family_run_seeds_state(
    camp_setup: tuple[FastAPI, str, UUID, _FakeStateBackend],
    postgres_url: str,
) -> None:
    """Full happy path: trial_config carries a full family_run override,
    the batch route seeds batch_family_state, and the resolved spec is
    persisted on the batch row so the scheduler + orchestrator can read
    it back."""
    app, raw, _team_id, fake_backend = camp_setup
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://svc") as ac:
        r = await ac.post(
            "/api/v1/batches",
            headers={"Authorization": f"Bearer {raw}"},
            json={
                "name": "skillflow-iterative",
                "description": "family-run seed integration test",
                "task_filter": {
                    "task_ids": [
                        "benchmarks/skillflow-iterative/family-a/task-1",
                        "benchmarks/skillflow-iterative/family-a/task-2",
                    ],
                },
                "trial_config": {
                    "agent_name": "oracle",
                    "agent_model": None,
                    "family_run": {
                        "enabled": True,
                        "family_key_extractor": {
                            "name": "instance_id_prefix",
                            "params": {"depth": 2},
                        },
                        "sequencer": {"name": "submitted_order"},
                        "advance_predicate": {"name": "always_on_terminal"},
                        "adapter": {"name": "noop"},
                        "failure_policy": {"name": "skip_and_advance"},
                        "state_backend": {"name": "s3_artifacts"},
                        "mount_path": "/root/.skills",
                    },
                },
            },
        )
    assert r.status_code == 201, r.text
    batch_id = UUID(r.json()["batch_id"])

    # Fake backend saw one initialize per family (depth=2 groups by
    # first-two path segments — one family for both tasks).
    assert len(fake_backend.initialized) == 1
    assert fake_backend.initialized[0][0] == batch_id

    # Verify batch_family_state + batches.family_run_spec were written.
    sync_engine = create_engine(postgres_url)
    sl = sessionmaker(sync_engine)
    try:
        with sl() as s:
            spec_row = s.execute(
                text("SELECT family_run_spec FROM batches WHERE id = :bid"),
                {"bid": str(batch_id)},
            ).one()
            assert spec_row.family_run_spec is not None
            spec = spec_row.family_run_spec
            assert spec["enabled"] is True
            assert spec["adapter"]["name"] == "noop"
            assert spec["mount_path"] == "/root/.skills"

            family_rows = s.execute(
                text(
                    "SELECT family_key, task_sequence, state, state_uri "
                    "FROM batch_family_state WHERE batch_id = :bid",
                ),
                {"bid": str(batch_id)},
            ).all()
            assert len(family_rows) == 1
            row = family_rows[0]
            assert row.state == "pending"
            assert row.state_uri.startswith("s3://fake/family-state/")
            assert set(row.task_sequence) == {
                "benchmarks/skillflow-iterative/family-a/task-1",
                "benchmarks/skillflow-iterative/family-a/task-2",
            }
    finally:
        sync_engine.dispose()


async def test_post_batch_family_run_partial_spec_returns_400(
    camp_setup: tuple[FastAPI, str, UUID, _FakeStateBackend],
) -> None:
    """A partial family_run spec (missing required roles) must 400 at
    submit time so operators see the failure immediately."""
    app, raw, _team_id, _fake_backend = camp_setup
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://svc") as ac:
        r = await ac.post(
            "/api/v1/batches",
            headers={"Authorization": f"Bearer {raw}"},
            json={
                "name": "partial-spec",
                "task_filter": {
                    "task_ids": [
                        "benchmarks/skillflow-iterative/family-a/task-1",
                    ],
                },
                "trial_config": {
                    "agent_name": "oracle",
                    "agent_model": None,
                    "family_run": {
                        "enabled": True,
                        # Missing sequencer / advance_predicate /
                        # adapter / failure_policy / state_backend.
                    },
                },
            },
        )
    assert r.status_code == 400
    assert "family_run" in r.text or "missing required role" in r.text


async def test_post_batch_no_family_run_stays_classic(
    camp_setup: tuple[FastAPI, str, UUID, _FakeStateBackend],
    postgres_url: str,
) -> None:
    """Backward-compat: omitting family_run leaves the batch classic;
    no batch_family_state rows and family_run_spec stays NULL."""
    app, raw, _team_id, fake_backend = camp_setup
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://svc") as ac:
        r = await ac.post(
            "/api/v1/batches",
            headers={"Authorization": f"Bearer {raw}"},
            json={
                "name": "classic",
                "task_filter": {
                    "task_ids": [
                        "benchmarks/skillflow-iterative/family-a/task-1",
                    ],
                },
                "trial_config": {
                    "agent_name": "oracle",
                    "agent_model": None,
                },
            },
        )
    assert r.status_code == 201, r.text
    batch_id = UUID(r.json()["batch_id"])
    assert fake_backend.initialized == []

    sync_engine = create_engine(postgres_url)
    sl = sessionmaker(sync_engine)
    try:
        with sl() as s:
            row = s.execute(
                text("SELECT family_run_spec FROM batches WHERE id = :bid"),
                {"bid": str(batch_id)},
            ).one()
            assert row.family_run_spec is None
            count = s.execute(
                text("SELECT COUNT(*) FROM batch_family_state WHERE batch_id = :bid"),
                {"bid": str(batch_id)},
            ).scalar_one()
            assert count == 0
    finally:
        sync_engine.dispose()


async def test_family_evolver_provider_owned_by_submission_team_is_canonicalized(
    camp_setup: tuple[FastAPI, str, UUID, _FakeStateBackend],
    postgres_url: str,
) -> None:
    app, raw, team_id, fake_backend = camp_setup
    connection_id = _insert_provider_connection(
        postgres_url,
        team_id,
        display_name="family-evolver-owner",
        ownership=fake_backend,
    )

    r = await _post_family_batch(
        app,
        raw,
        name="family-evolver-owner",
        adapter_params={"provider_connection_id": str(connection_id).upper()},
    )

    assert r.status_code == 201, r.text
    assert len(fake_backend.initialized) == 1
    sync_engine = create_engine(postgres_url)
    try:
        with sync_engine.connect() as conn:
            spec = conn.execute(
                text("SELECT family_run_spec FROM batches WHERE id = :bid"),
                {"bid": r.json()["batch_id"]},
            ).scalar_one()
    finally:
        sync_engine.dispose()
    assert spec["adapter"]["params"]["provider_connection_id"] == str(
        connection_id,
    )


async def test_family_evolver_provider_shared_with_submission_team_is_accepted(
    camp_setup: tuple[FastAPI, str, UUID, _FakeStateBackend],
    postgres_url: str,
) -> None:
    app, raw, team_id, fake_backend = camp_setup
    owner_team_id = _insert_team(
        postgres_url,
        name=f"family-provider-owner-{uuid4().hex[:8]}",
        ownership=fake_backend,
    )
    connection_id = _insert_provider_connection(
        postgres_url,
        owner_team_id,
        display_name="family-evolver-shared",
        ownership=fake_backend,
    )
    sync_engine = create_engine(postgres_url)
    sl = sessionmaker(sync_engine)
    try:
        with sl() as s:
            s.execute(
                insert(ProviderConnectionShare).values(
                    provider_connection_id=connection_id,
                    target_team_id=team_id,
                    created_by_actor="test",
                )
            )
            s.commit()
    finally:
        sync_engine.dispose()

    r = await _post_family_batch(
        app,
        raw,
        name="family-evolver-shared",
        adapter_params={"provider_connection_id": str(connection_id).upper()},
    )

    assert r.status_code == 201, r.text
    assert len(fake_backend.initialized) == 1


@pytest.mark.parametrize("provider_connection_id", ["", "not-a-uuid"])
async def test_family_evolver_provider_must_be_non_empty_uuid_before_state_seed(
    camp_setup: tuple[FastAPI, str, UUID, _FakeStateBackend],
    provider_connection_id: str,
) -> None:
    app, raw, _team_id, fake_backend = camp_setup

    r = await _post_family_batch(
        app,
        raw,
        name="family-evolver-invalid-provider",
        adapter_params={"provider_connection_id": provider_connection_id},
    )

    assert r.status_code == 400
    assert "provider_connection_id" in r.text
    if provider_connection_id:
        assert provider_connection_id not in r.text
    assert fake_backend.initialized == []


async def test_family_evolver_unshared_provider_is_hidden_before_state_seed(
    camp_setup: tuple[FastAPI, str, UUID, _FakeStateBackend],
    postgres_url: str,
) -> None:
    app, raw, _team_id, fake_backend = camp_setup
    owner_team_id = _insert_team(
        postgres_url,
        name=f"family-provider-other-{uuid4().hex[:8]}",
        ownership=fake_backend,
    )
    connection_id = _insert_provider_connection(
        postgres_url,
        owner_team_id,
        display_name="family-evolver-unshared",
        ownership=fake_backend,
    )

    r = await _post_family_batch(
        app,
        raw,
        name="family-evolver-unshared",
        adapter_params={"provider_connection_id": str(connection_id)},
    )

    assert r.status_code == 404
    assert str(connection_id) not in r.text
    assert fake_backend.initialized == []


@pytest.mark.parametrize(
    "adapter_params",
    [
        {"api_key": "DO-NOT-ECHO"},
        {"request_options": {"authorization": "DO-NOT-ECHO"}},
        {"authToken": "DO-NOT-ECHO"},
    ],
)
async def test_family_evolver_rejects_secret_like_param_keys_without_value_echo(
    camp_setup: tuple[FastAPI, str, UUID, _FakeStateBackend],
    adapter_params: dict[str, Any],
) -> None:
    app, raw, _team_id, fake_backend = camp_setup

    r = await _post_family_batch(
        app,
        raw,
        name="family-evolver-secret-param",
        adapter_params=adapter_params,
    )

    assert r.status_code == 400
    assert "secret-like key" in r.text
    assert "DO-NOT-ECHO" not in r.text
    assert fake_backend.initialized == []


async def test_family_evolver_without_provider_keeps_platform_route_compatible(
    camp_setup: tuple[FastAPI, str, UUID, _FakeStateBackend],
) -> None:
    app, raw, _team_id, fake_backend = camp_setup

    r = await _post_family_batch(
        app,
        raw,
        name="family-evolver-no-provider",
        adapter_params={
            "model": "anthropic/claude-sonnet-4-6",
            "max_tokens": 256,
        },
    )

    assert r.status_code == 201, r.text
    assert len(fake_backend.initialized) == 1
