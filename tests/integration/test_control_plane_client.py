"""Integration: HttpControlPlaneClient against a real Plan 5 app over
ASGITransport (no network). ASGITransport doesn't run lifespan, so we
populate app.state manually — same pattern as test_http_gateway_client.py
in Plan 4."""

import hashlib
from collections.abc import AsyncIterator
from datetime import UTC, datetime
from uuid import UUID, uuid4

import boto3
import httpx
import pytest
from botocore.config import Config
from sqlalchemy import create_engine, delete, insert, select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.orm import sessionmaker

from loom.db.schema import (
    Artifact,
    ArtifactLineageEdge,
    DataLifecycleAuthority,
    DataLifecycleGcItem,
    DataLifecycleGcRun,
    DataLifecycleObject,
    Task,
    Team,
    TeamQuota,
    Token,
    Trial,
    Worker,
)
from loom_control_plane.app import create_app
from loom_control_plane.config import ControlPlaneSettings
from loom_worker.control_plane_client import HttpControlPlaneClient

_CAPS = [{
    "os": "linux",
    "gpu_vendor": "none",
    "network_policies": ["public"],
    "dynamic_network_policy": True,
    "mounted_fs": True,
    "resource_modes": ["auto"],
}]


@pytest.fixture
async def cp_setup(
    monkeypatch: pytest.MonkeyPatch, postgres_url: str,
) -> AsyncIterator[tuple[object, str]]:
    """Returns (app, raw_token) with state populated and a fresh worker token."""
    for k, v in {
        "LOOM_CP_DB_URL": postgres_url,
        "LOOM_CP_MINIO_ENDPOINT": "http://minio:9000",
        "LOOM_CP_MINIO_ACCESS_KEY": "x",
        "LOOM_CP_MINIO_SECRET_KEY": "y",
        "LOOM_CP_LLM_GATEWAY_URL": "http://gw:9100/",
    }.items():
        monkeypatch.setenv(k, v)
    settings = ControlPlaneSettings(_env_file=None)
    app = create_app(settings)

    async_engine = create_async_engine(str(settings.db_url))
    app.state.settings = settings
    app.state.session_factory = async_sessionmaker(async_engine, expire_on_commit=False)
    app.state.minio_client = boto3.client(
        "s3",
        endpoint_url=settings.minio_endpoint,
        aws_access_key_id=settings.minio_access_key.get_secret_value(),
        aws_secret_access_key=settings.minio_secret_key.get_secret_value(),
        region_name=settings.minio_region,
        config=Config(signature_version="s3v4"),
    )

    raw = f"loom_w_{uuid4().hex}"
    sync_engine = create_engine(postgres_url)
    session_local = sessionmaker(sync_engine)
    with session_local() as s:
        s.execute(delete(ArtifactLineageEdge))
        s.execute(delete(Artifact))
        s.execute(delete(Trial))
        s.execute(delete(Worker))
        s.execute(delete(Token))
        s.execute(delete(TeamQuota))
        s.execute(delete(DataLifecycleGcItem))
        s.execute(delete(DataLifecycleGcRun))
        s.execute(delete(DataLifecycleObject))
        s.execute(delete(DataLifecycleAuthority))
        s.execute(delete(Team))
        s.execute(delete(Task))
        s.execute(insert(Token).values(
            token_hash=hashlib.sha256(raw.encode()).digest(),
            type="worker",
            scopes=["worker:claim", "worker:report", "worker:index"],
            team_id=None,
            issued_at=datetime.now(UTC), expires_at=None,
        ))
        s.commit()
    try:
        yield app, raw
    finally:
        await async_engine.dispose()
        with session_local() as s:
            s.execute(delete(ArtifactLineageEdge))
            s.execute(delete(Artifact))
            s.execute(delete(Trial))
            s.execute(delete(Worker))
            s.execute(delete(Token))
            s.execute(delete(TeamQuota))
            s.execute(delete(DataLifecycleGcItem))
            s.execute(delete(DataLifecycleGcRun))
            s.execute(delete(DataLifecycleObject))
            s.execute(delete(DataLifecycleAuthority))
            s.execute(delete(Team))
            s.execute(delete(Task))
            s.commit()
        sync_engine.dispose()


async def _client(app: object, raw: str) -> tuple[HttpControlPlaneClient, httpx.AsyncClient]:
    transport = httpx.ASGITransport(app=app)  # type: ignore[arg-type]
    http = httpx.AsyncClient(transport=transport, base_url="http://cp")
    cp = HttpControlPlaneClient(base_url="http://cp", token=raw, _client=http)
    return cp, http


async def test_register_returns_worker_id(cp_setup):  # type: ignore[no-untyped-def]
    app, raw = cp_setup
    cp, http = await _client(app, raw)
    try:
        info = await cp.register(hostname="h", version="v", capabilities=_CAPS)
        assert "worker_id" in info
        UUID(info["worker_id"])
        assert info["heartbeat_interval_sec"] > 0
    finally:
        await http.aclose()


async def test_claim_returns_none_when_empty(cp_setup):  # type: ignore[no-untyped-def]
    app, raw = cp_setup
    cp, http = await _client(app, raw)
    try:
        info = await cp.register(hostname="h", version="v", capabilities=_CAPS)
        wid = UUID(info["worker_id"])
        assert await cp.claim(worker_id=wid, caps=_CAPS) is None
    finally:
        await http.aclose()


async def test_heartbeat(cp_setup, postgres_url):  # type: ignore[no-untyped-def]
    app, raw = cp_setup
    cp, http = await _client(app, raw)
    try:
        info = await cp.register(hostname="h", version="v", capabilities=_CAPS)
        wid = UUID(info["worker_id"])
        await cp.heartbeat(wid)
        await cp.heartbeat(wid, status="idle-exit")

        engine = create_engine(postgres_url)
        with engine.connect() as conn:
            status = conn.execute(
                select(Worker.status).where(Worker.id == wid),
            ).scalar_one()
        engine.dispose()
        assert status == "idle-exit"
    finally:
        await http.aclose()


async def test_patch_state_returns_true_when_owner(  # type: ignore[no-untyped-def]
    cp_setup, postgres_url,
):
    app, raw = cp_setup
    cp, http = await _client(app, raw)
    try:
        info = await cp.register(hostname="h", version="v", capabilities=_CAPS)
        wid = UUID(info["worker_id"])

        team_id = uuid4()
        trial_id = uuid4()
        engine = create_engine(postgres_url)
        with engine.begin() as conn:
            conn.execute(insert(Team).values(id=team_id, name=f"t-{team_id}"))
            conn.execute(insert(TeamQuota).values(team_id=team_id))
            conn.execute(insert(Task).values(
                id="t", checksum="0" * 64, config={},
            ))
            conn.execute(insert(Trial).values(
                id=trial_id, team_id=team_id, task_id="t",
                config={}, requires_caps={}, state="claimed",
                worker_id=wid,
            ))
        engine.dispose()

        assert await cp.patch_state(
            trial_id=trial_id, worker_id=wid, state="running",
        ) is True
    finally:
        await http.aclose()


async def test_patch_state_returns_false_when_fenced(  # type: ignore[no-untyped-def]
    cp_setup, postgres_url,
):
    """Trial belongs to a different worker → 409 → False."""
    app, raw = cp_setup
    cp, http = await _client(app, raw)
    try:
        info = await cp.register(hostname="h", version="v", capabilities=_CAPS)
        wid = UUID(info["worker_id"])

        team_id = uuid4()
        trial_id = uuid4()
        other_worker = uuid4()
        engine = create_engine(postgres_url)
        with engine.begin() as conn:
            conn.execute(insert(Team).values(id=team_id, name=f"t-{team_id}"))
            conn.execute(insert(TeamQuota).values(team_id=team_id))
            conn.execute(insert(Worker).values(
                id=other_worker, hostname="o", version="v", capabilities=[],
                registered_at=datetime.now(UTC),
                last_seen_at=datetime.now(UTC), status="active",
            ))
            conn.execute(insert(Task).values(
                id="t", checksum="0" * 64, config={},
            ))
            conn.execute(insert(Trial).values(
                id=trial_id, team_id=team_id, task_id="t",
                config={}, requires_caps={}, state="claimed",
                worker_id=other_worker,
            ))
        engine.dispose()

        assert await cp.patch_state(
            trial_id=trial_id, worker_id=wid, state="running",
        ) is False
    finally:
        await http.aclose()


async def test_requeue_trial_retry_moves_prestart_claim_back_to_queue(  # type: ignore[no-untyped-def]
    cp_setup, postgres_url,
):
    app, raw = cp_setup
    cp, http = await _client(app, raw)
    try:
        info = await cp.register(hostname="h", version="v", capabilities=_CAPS)
        wid = UUID(info["worker_id"])

        team_id = uuid4()
        trial_id = uuid4()
        engine = create_engine(postgres_url)
        with engine.begin() as conn:
            conn.execute(insert(Team).values(id=team_id, name=f"t-{team_id}"))
            conn.execute(insert(TeamQuota).values(team_id=team_id))
            conn.execute(insert(Task).values(
                id="t", checksum="0" * 64, config={},
            ))
            conn.execute(insert(Trial).values(
                id=trial_id, team_id=team_id, task_id="t",
                config={}, requires_caps={}, state="claimed",
                worker_id=wid,
            ))

        assert await cp.requeue_trial_retry(
            trial_id=trial_id,
            worker_id=wid,
            failure_reason="provider_transport_disconnect",
            failure_message="Server disconnected without sending a response.",
            retry_after_sec=0.0,
        ) is True

        with engine.connect() as conn:
            row = conn.execute(
                select(
                    Trial.state,
                    Trial.worker_id,
                    Trial.failure_reason,
                    Trial.failure_message,
                    Trial.next_attempt_at,
                    Trial.finished_at,
                ).where(Trial.id == trial_id)
            ).one()
        engine.dispose()

        assert row.state == "queued"
        assert row.worker_id is None
        assert row.failure_reason == "provider_transport_disconnect"
        assert row.failure_message == "Server disconnected without sending a response."
        assert row.next_attempt_at is not None
        assert row.finished_at is None
    finally:
        await http.aclose()


async def test_requeue_trial_retry_returns_false_after_trial_started(  # type: ignore[no-untyped-def]
    cp_setup, postgres_url,
):
    app, raw = cp_setup
    cp, http = await _client(app, raw)
    try:
        info = await cp.register(hostname="h", version="v", capabilities=_CAPS)
        wid = UUID(info["worker_id"])

        team_id = uuid4()
        trial_id = uuid4()
        started_at = datetime.now(UTC)
        engine = create_engine(postgres_url)
        with engine.begin() as conn:
            conn.execute(insert(Team).values(id=team_id, name=f"t-{team_id}"))
            conn.execute(insert(TeamQuota).values(team_id=team_id))
            conn.execute(insert(Task).values(
                id="t", checksum="0" * 64, config={},
            ))
            conn.execute(insert(Trial).values(
                id=trial_id, team_id=team_id, task_id="t",
                config={}, requires_caps={}, state="claimed",
                worker_id=wid, started_at=started_at,
            ))

        assert await cp.requeue_trial_retry(
            trial_id=trial_id,
            worker_id=wid,
            failure_reason="provider_transport_disconnect",
            failure_message="Server disconnected without sending a response.",
            retry_after_sec=0.0,
        ) is False

        with engine.connect() as conn:
            row = conn.execute(
                select(Trial.state, Trial.worker_id, Trial.started_at).where(
                    Trial.id == trial_id,
                )
            ).one()
        engine.dispose()

        assert row.state == "claimed"
        assert row.worker_id == wid
        assert row.started_at == started_at
    finally:
        await http.aclose()


async def test_requeue_trial_retry_returns_false_when_team_attempt_budget_exhausted(  # type: ignore[no-untyped-def]
    cp_setup, postgres_url,
):
    app, raw = cp_setup
    cp, http = await _client(app, raw)
    try:
        info = await cp.register(hostname="h", version="v", capabilities=_CAPS)
        wid = UUID(info["worker_id"])

        team_id = uuid4()
        trial_id = uuid4()
        engine = create_engine(postgres_url)
        with engine.begin() as conn:
            conn.execute(insert(Team).values(id=team_id, name=f"t-{team_id}"))
            conn.execute(insert(TeamQuota).values(team_id=team_id, max_attempts_ceiling=1))
            conn.execute(insert(Task).values(
                id="t", checksum="0" * 64, config={},
            ))
            conn.execute(insert(Trial).values(
                id=trial_id, team_id=team_id, task_id="t",
                config={}, requires_caps={}, state="claimed",
                worker_id=wid, attempt_count=1,
            ))

        assert await cp.requeue_trial_retry(
            trial_id=trial_id,
            worker_id=wid,
            failure_reason="provider_transport_disconnect",
            failure_message="Server disconnected without sending a response.",
            retry_after_sec=0.0,
        ) is False

        with engine.connect() as conn:
            row = conn.execute(
                select(
                    Trial.state,
                    Trial.worker_id,
                    Trial.attempt_count,
                    Trial.failure_reason,
                    Trial.next_attempt_at,
                ).where(Trial.id == trial_id)
            ).one()
        engine.dispose()

        assert row.state == "claimed"
        assert row.worker_id == wid
        assert row.attempt_count == 1
        assert row.failure_reason is None
        assert row.next_attempt_at is None
    finally:
        await http.aclose()


async def test_get_task_bundle_returns_config(  # type: ignore[no-untyped-def]
    cp_setup, postgres_url,
):
    app, raw = cp_setup
    cp, http = await _client(app, raw)
    try:
        engine = create_engine(postgres_url)
        with engine.begin() as conn:
            conn.execute(insert(Task).values(
                id="t-bundle", checksum="ab" * 32,
                config={
                    "schema_version": "1",
                    "task": {"id": "t-bundle", "name": "t-bundle"},
                    "environment": {"os": "linux", "docker_image": "alpine"},
                    "agent": {"name": "oracle"},
                    "verifier": {"name": "pytest"},
                    "steps": [{"name": "main"}],
                },
                source="fixture://t-bundle",
            ))
        engine.dispose()

        bundle = await cp.get_task_bundle("t-bundle")
        assert bundle["id"] == "t-bundle"
        assert bundle["checksum"] == "ab" * 32
        assert bundle["config"]["task"]["name"] == "t-bundle"
        assert bundle["source"] == "fixture://t-bundle"
    finally:
        await http.aclose()


async def test_get_task_bundle_returns_slash_task_id_config(  # type: ignore[no-untyped-def]
    cp_setup, postgres_url,
):
    app, raw = cp_setup
    cp, http = await _client(app, raw)
    try:
        task_id = "humaneval/HumanEval/26"
        engine = create_engine(postgres_url)
        with engine.begin() as conn:
            conn.execute(insert(Task).values(
                id=task_id,
                checksum="cd" * 32,
                config={
                    "schema_version": "1",
                    "task": {"id": task_id, "name": "HumanEval/26"},
                    "environment": {
                        "os": "linux",
                        "docker_image": "alpine",
                    },
                    "agent": {"name": "oracle"},
                    "verifier": {"name": "pytest"},
                    "steps": [{"name": "main"}],
                },
                source="fixture://humaneval/HumanEval/26",
            ))
        engine.dispose()

        bundle = await cp.get_task_bundle(task_id)
        assert bundle["id"] == task_id
        assert bundle["checksum"] == "cd" * 32
        assert bundle["config"]["task"]["id"] == task_id
        assert bundle["source"] == "fixture://humaneval/HumanEval/26"
    finally:
        await http.aclose()


async def test_mint_step_token_returns_loom_step_jwt(  # type: ignore[no-untyped-def]
    cp_setup, postgres_url,
):
    """A11.1: worker mints step tokens via the new endpoint."""
    app, raw = cp_setup
    cp, http = await _client(app, raw)
    try:
        # Seed a team + trial the worker can mint a token for.
        team_id = uuid4()
        trial_id = uuid4()
        engine = create_engine(postgres_url)
        with engine.begin() as conn:
            conn.execute(insert(Team).values(id=team_id, name=f"t-{team_id}"))
            conn.execute(insert(TeamQuota).values(team_id=team_id))
            conn.execute(insert(Task).values(
                id="t", checksum="0" * 64, config={},
            ))
            conn.execute(insert(Trial).values(
                id=trial_id, team_id=team_id, task_id="t",
                config={}, requires_caps={}, state="running",
            ))
        engine.dispose()

        token = await cp.mint_step_token(
            team_id=team_id, trial_id=trial_id,
            step_id="main", ttl_sec=60,
        )
        assert token.startswith("loom_step_")
    finally:
        await http.aclose()


async def test_get_trial_llm_calls_returns_items(  # type: ignore[no-untyped-def]
    cp_setup, postgres_url,
):
    """A11.1: worker reads llm_calls rows at finalize."""
    from decimal import Decimal

    from loom.db.schema import LlmCall

    app, raw = cp_setup
    cp, http = await _client(app, raw)
    try:
        team_id = uuid4()
        trial_id = uuid4()
        engine = create_engine(postgres_url)
        with engine.begin() as conn:
            conn.execute(insert(Team).values(id=team_id, name=f"t-{team_id}"))
            conn.execute(insert(TeamQuota).values(team_id=team_id))
            conn.execute(insert(Task).values(
                id="t", checksum="0" * 64, config={},
            ))
            conn.execute(insert(Trial).values(
                id=trial_id, team_id=team_id, task_id="t",
                config={}, requires_caps={}, state="running",
            ))
            conn.execute(insert(LlmCall).values(
                team_id=team_id, trial_id=trial_id, step_id="main",
                dialect="anthropic", model="claude-opus-4-7",
                input_tokens=100, output_tokens=50,
                provider_extras={},
                cost_usd=Decimal("0.001"), rate_card_hash="abc",
            ))
        engine.dispose()

        items = await cp.get_trial_llm_calls(trial_id)
        assert len(items) == 1
        assert items[0]["input_tokens"] == 100
        assert items[0]["dialect"] == "anthropic"
    finally:
        await http.aclose()


async def test_patch_trajectory_index_returns_true_when_owner(  # type: ignore[no-untyped-def]
    cp_setup, postgres_url,
):
    app, raw = cp_setup
    cp, http = await _client(app, raw)
    try:
        info = await cp.register(hostname="h", version="v", capabilities=_CAPS)
        wid = UUID(info["worker_id"])

        team_id = uuid4()
        trial_id = uuid4()
        engine = create_engine(postgres_url)
        with engine.begin() as conn:
            conn.execute(insert(Team).values(id=team_id, name=f"t-{team_id}"))
            conn.execute(insert(TeamQuota).values(team_id=team_id))
            conn.execute(insert(Task).values(
                id="t", checksum="0" * 64, config={},
            ))
            conn.execute(insert(Trial).values(
                id=trial_id, team_id=team_id, task_id="t",
                config={}, requires_caps={}, state="running",
                worker_id=wid,
            ))
        engine.dispose()

        assert await cp.patch_trajectory_index(
            trial_id=trial_id, worker_id=wid,
            trajectory_uri="s3://trajectories/x.jsonl",
            trajectory_size_bytes=42,
            trajectory_sha256="1" * 64,
        ) is True
    finally:
        await http.aclose()


async def test_patch_trajectory_index_persists_result_projection(  # type: ignore[no-untyped-def]
    cp_setup, postgres_url,
):
    app, raw = cp_setup
    cp, http = await _client(app, raw)
    try:
        info = await cp.register(hostname="h", version="v", capabilities=_CAPS)
        wid = UUID(info["worker_id"])

        team_id = uuid4()
        trial_id = uuid4()
        engine = create_engine(postgres_url)
        with engine.begin() as conn:
            conn.execute(insert(Team).values(id=team_id, name=f"t-{team_id}"))
            conn.execute(insert(TeamQuota).values(team_id=team_id))
            conn.execute(insert(Task).values(
                id="t", checksum="0" * 64, config={},
            ))
            conn.execute(insert(Trial).values(
                id=trial_id, team_id=team_id, task_id="t",
                config={}, requires_caps={}, state="succeeded",
                worker_id=wid, result={},
            ))

        result_payload = {
            "schema_version": "1",
            "state": "succeeded",
            "aggregate_reward": 1.0,
        }
        assert await cp.patch_trajectory_index(
            trial_id=trial_id,
            worker_id=wid,
            result=result_payload,
            trajectory_uri=f"s3://trajectories/{team_id}/{trial_id}/events.jsonl",
            trajectory_size_bytes=42,
            trajectory_sha256="2" * 64,
            atif_uri=f"s3://trajectories/{team_id}/{trial_id}/atif.json",
            atif_size_bytes=84,
            atif_sha256="3" * 64,
            artifacts=[{
                "step_name": "main",
                "bucket": "artifacts",
                "key": f"{team_id}/{trial_id}/main/result.txt",
                "size": 5,
                "content_hash": "sha256:" + ("4" * 64),
            }],
        ) is True

        with engine.begin() as conn:
            row = conn.execute(
                select(Trial.result, Trial.trajectory_index).where(
                    Trial.id == trial_id,
                ),
            ).one()
        engine.dispose()

        assert row.result == result_payload
        assert row.trajectory_index["trajectory_uri"].endswith("/events.jsonl")
        assert row.trajectory_index["artifacts"][0]["key"].endswith(
            "/main/result.txt",
        )
    finally:
        await http.aclose()


async def test_patch_output_projection_accepts_index_with_trial_id(  # type: ignore[no-untyped-def]
    cp_setup, postgres_url,
):
    """Worker-built trajectory indexes include trial_id/team_id/task_id.

    The higher-level output projection client must not expand those metadata
    fields as duplicate Python kwargs before it reaches the CP endpoint.
    """

    app, raw = cp_setup
    cp, http = await _client(app, raw)
    try:
        info = await cp.register(hostname="h", version="v", capabilities=_CAPS)
        wid = UUID(info["worker_id"])

        team_id = uuid4()
        trial_id = uuid4()
        engine = create_engine(postgres_url)
        with engine.begin() as conn:
            conn.execute(insert(Team).values(id=team_id, name=f"t-{team_id}"))
            conn.execute(insert(TeamQuota).values(team_id=team_id))
            conn.execute(insert(Task).values(
                id="t", checksum="0" * 64, config={},
            ))
            conn.execute(insert(Trial).values(
                id=trial_id, team_id=team_id, task_id="t",
                config={}, requires_caps={}, state="succeeded",
                worker_id=wid, result={},
            ))

        result_payload = {
            "schema_version": "1",
            "state": "succeeded",
            "aggregate_reward": 1.0,
        }
        trajectory_index = {
            "schema_version": "1",
            "trial_id": str(trial_id),
            "team_id": str(team_id),
            "task_id": "t",
            "trajectory_uri": f"s3://trajectories/{team_id}/{trial_id}/events.jsonl",
            "trajectory_size_bytes": 42,
            "trajectory_sha256": "5" * 64,
            "atif_uri": f"s3://trajectories/{team_id}/{trial_id}/atif.json",
            "atif_size_bytes": 84,
            "atif_sha256": "6" * 64,
            "atif_schema_version": "1.7",
            "artifacts": [{
                "step_name": "main",
                "bucket": "artifacts",
                "key": f"{team_id}/{trial_id}/main/result.txt",
                "size": 5,
                "content_hash": "sha256:" + ("7" * 64),
            }],
        }
        assert await cp.patch_output_projection(
            trial_id=trial_id,
            worker_id=wid,
            result=result_payload,
            trajectory_index=trajectory_index,
        ) is True

        with engine.begin() as conn:
            row = conn.execute(
                select(Trial.result, Trial.trajectory_index).where(
                    Trial.id == trial_id,
                ),
            ).one()
        engine.dispose()

        assert row.result == result_payload
        assert row.trajectory_index["trial_id"] == str(trial_id)
        assert row.trajectory_index["artifacts"][0]["key"].endswith(
            "/main/result.txt",
        )
    finally:
        await http.aclose()
