"""Org-wide Run Library and shared artifact behavior (#336)."""

from __future__ import annotations

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
from testcontainers.minio import MinioContainer

from loom.db.schema import (
    Batch,
    ProviderConnection,
    Task,
    Team,
    TeamQuota,
    Token,
    Trial,
)
from loom_service.app import create_app
from loom_service.config import LoomServiceSettings


@pytest.fixture
async def run_library_setup(
    monkeypatch: pytest.MonkeyPatch,
    postgres_url: str,
    shared_minio: MinioContainer,
) -> AsyncIterator[dict[str, object]]:
    cfg = shared_minio.get_config()
    endpoint = f"http://{cfg['endpoint']}"
    for k, v in {
        "LOOM_SVC_DB_URL": postgres_url,
        "LOOM_SVC_MINIO_ENDPOINT": endpoint,
        "LOOM_SVC_MINIO_ACCESS_KEY": cfg["access_key"],
        "LOOM_SVC_MINIO_SECRET_KEY": cfg["secret_key"],
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
    app.state.minio_client = boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=cfg["access_key"],
        aws_secret_access_key=cfg["secret_key"],
        region_name=settings.minio_region,
        config=Config(signature_version="s3v4"),
    )
    app.state.http_client = httpx.AsyncClient(
        transport=httpx.MockTransport(lambda _req: httpx.Response(404)),
        base_url=str(settings.control_plane_url),
    )

    team_a = uuid4()
    team_b = uuid4()
    raw_a = f"loom_team_{uuid4().hex}"
    raw_b = f"loom_team_{uuid4().hex}"
    task_id = f"local/task-{uuid4().hex[:8]}"
    batch_shared = uuid4()
    batch_default = uuid4()
    batch_private = uuid4()
    batch_running = uuid4()
    batch_b = uuid4()
    trial_shared = uuid4()
    trial_private = uuid4()
    conn_a = uuid4()
    conn_b = uuid4()
    now = datetime.now(UTC)

    sync_engine = create_engine(postgres_url)
    sl = sessionmaker(sync_engine)
    safe_key = f"{team_a}/{trial_shared}/main/report.json"
    private_key = f"{team_a}/{trial_private}/main/report.json"
    blocked_key = f"{team_a}/{trial_shared}/main/debug.log"

    with sl() as s:
        s.execute(insert(Team).values(id=team_a, name="Alpha Research"))
        s.execute(insert(Team).values(id=team_b, name="Beta Apps"))
        s.execute(insert(TeamQuota).values(team_id=team_a))
        s.execute(insert(TeamQuota).values(team_id=team_b))
        for raw, team_id in ((raw_a, team_a), (raw_b, team_b)):
            s.execute(insert(Token).values(
                token_hash=hashlib.sha256(raw.encode()).digest(),
                type="team",
                scopes=["read:own", "submit", "providers:manage"],
                team_id=team_id,
                issued_at=now,
                expires_at=None,
            ))
        s.execute(insert(Task).values(
            id=task_id,
            checksum="1" * 64,
            config={"benchmark_id": "humaneval"},
            source="local",
        ))
        for conn_id, team_id, name in (
            (conn_a, team_a, "alpha-provider"),
            (conn_b, team_b, "beta-provider"),
        ):
            s.execute(insert(ProviderConnection).values(
                id=conn_id,
                team_id=team_id,
                provider_type="openai-compatible",
                display_name=name,
                base_url="https://api.example.test/v1",
                upstream_host="api.example.test",
                encrypted_api_key_ref=f"env:{name.upper().replace('-', '_')}",
                created_by="test",
                status="valid",
            ))
        common_config = {
            "agent_name": "litellm",
            "agent_model": {
                "provider": "openai",
                "name": "gpt-4o-mini",
            },
        }
        for batch_id, team_id, name, state, visibility, share_status in (
            (batch_shared, team_a, "shared alpha run", "finished", "org", "shared"),
            (batch_private, team_a, "private alpha run", "finished", "private", "shared"),
            (batch_running, team_a, "running alpha run", "running", "org", "shared"),
            (batch_b, team_b, "beta team run", "finished", "team", "pending_scan"),
        ):
            s.execute(insert(Batch).values(
                id=batch_id,
                team_id=team_id,
                name=name,
                description=None,
                task_filter={"subset_kind": "explicit", "task_ids": [task_id]},
                trial_config=common_config,
                state=state,
                result_status="succeeded" if state == "finished" else None,
                created_at=now,
                finished_at=now if state == "finished" else None,
                created_by_token_prefix="test:web",
                expected_trial_count=1,
                n_per_task=1,
                backend="docker",
                combinations=[],
                provider_connection_id=conn_a if team_id == team_a else conn_b,
                provider_model_id="gpt-4o-mini",
                visibility=visibility,
                share_status=share_status,
            ))
        s.execute(insert(Batch).values(
            id=batch_default,
            team_id=team_a,
            name="default shared alpha run",
            description=None,
            task_filter={"subset_kind": "explicit", "task_ids": [task_id]},
            trial_config=common_config,
            state="finished",
            result_status="succeeded",
            created_at=now,
            finished_at=now,
            created_by_token_prefix="test:web",
            expected_trial_count=1,
            n_per_task=1,
            backend="docker",
            combinations=[],
            provider_connection_id=conn_a,
            provider_model_id="gpt-4o-mini",
        ))
        s.execute(insert(Trial).values(
            id=trial_shared,
            task_id=task_id,
            team_id=team_a,
            batch_id=batch_shared,
            state="succeeded",
            config=common_config,
            requires_caps={},
            submitted_at=now,
            started_at=now,
            finished_at=now,
            result={"aggregate_reward": 1.0, "cost_usd": 0.02},
            visibility="org",
            share_status="shared",
            trajectory_index={
                "artifacts": [
                    {
                        "step_name": "main",
                        "bucket": settings.artifacts_bucket,
                        "key": safe_key,
                        "size": 17,
                        "role": "report",
                        "share_status": "shared",
                    },
                    {
                        "step_name": "main",
                        "bucket": settings.artifacts_bucket,
                        "key": blocked_key,
                        "size": 21,
                        "role": "raw_diagnostics",
                        "share_status": "blocked",
                        "blocked_reason": "secret-like content detected",
                    },
                ],
            },
        ))
        s.execute(insert(Trial).values(
            id=trial_private,
            task_id=task_id,
            team_id=team_a,
            batch_id=batch_private,
            state="succeeded",
            config=common_config,
            requires_caps={},
            submitted_at=now,
            started_at=now,
            finished_at=now,
            result={"aggregate_reward": 0.5, "cost_usd": 0.01},
            visibility="org",
            share_status="shared",
            trajectory_index={
                "artifacts": [
                    {
                        "step_name": "main",
                        "bucket": settings.artifacts_bucket,
                        "key": private_key,
                        "size": 17,
                        "role": "report",
                        "share_status": "shared",
                    },
                ],
            },
        ))
        s.commit()

    existing = {
        bucket["Name"]
        for bucket in app.state.minio_client.list_buckets()["Buckets"]
    }
    if settings.artifacts_bucket not in existing:
        app.state.minio_client.create_bucket(Bucket=settings.artifacts_bucket)
    app.state.minio_client.put_object(
        Bucket=settings.artifacts_bucket,
        Key=safe_key,
        Body=b'{"ok": true}\n',
    )
    app.state.minio_client.put_object(
        Bucket=settings.artifacts_bucket,
        Key=blocked_key,
        Body=b"sk-test-should-not-leak",
    )

    try:
        yield {
            "app": app,
            "raw_a": raw_a,
            "raw_b": raw_b,
            "team_a": team_a,
            "team_b": team_b,
            "batch_shared": batch_shared,
            "batch_default": batch_default,
            "batch_private": batch_private,
            "batch_running": batch_running,
            "batch_b": batch_b,
            "trial_shared": trial_shared,
            "trial_private": trial_private,
            "conn_a": conn_a,
            "conn_b": conn_b,
            "safe_key": safe_key,
            "private_key": private_key,
            "blocked_key": blocked_key,
            "postgres_url": postgres_url,
        }
    finally:
        await app.state.http_client.aclose()
        await engine.dispose()
        with sl() as s:
            s.execute(delete(Trial))
            s.execute(delete(Batch))
            s.execute(delete(ProviderConnection))
            s.execute(delete(Token))
            s.execute(delete(Task))
            s.execute(delete(TeamQuota))
            s.execute(delete(Team))
            s.commit()
        sync_engine.dispose()


async def test_run_library_defaults_to_my_team_and_all_teams_shows_shared_owner(
    run_library_setup: dict[str, object],
) -> None:
    app = run_library_setup["app"]
    raw_b = run_library_setup["raw_b"]
    team_a = run_library_setup["team_a"]
    batch_shared = run_library_setup["batch_shared"]
    batch_default = run_library_setup["batch_default"]
    batch_private = run_library_setup["batch_private"]
    batch_running = run_library_setup["batch_running"]
    batch_b = run_library_setup["batch_b"]

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport, base_url="http://svc",
    ) as ac:
        mine = await ac.get(
            "/api/v1/run-library/batches",
            headers={"Authorization": f"Bearer {raw_b}"},
        )
        all_teams = await ac.get(
            "/api/v1/run-library/batches?scope=all",
            headers={"Authorization": f"Bearer {raw_b}"},
        )

    assert mine.status_code == 200, mine.text
    assert [item["id"] for item in mine.json()["items"]] == [str(batch_b)]

    assert all_teams.status_code == 200, all_teams.text
    ids = {item["id"] for item in all_teams.json()["items"]}
    assert str(batch_b) in ids
    assert str(batch_shared) in ids
    assert str(batch_default) in ids
    assert str(batch_private) not in ids
    assert str(batch_running) not in ids

    shared = next(
        item for item in all_teams.json()["items"]
        if item["id"] == str(batch_shared)
    )
    assert shared["owner_team"] == {
        "id": str(team_a),
        "name": "Alpha Research",
    }
    assert shared["visibility"] == "org"
    assert shared["share_status"] == "shared"
    assert shared["artifact_summary"]["reports"] == 1
    assert shared["artifact_summary"]["raw_diagnostics"] == 1

    default_shared = next(
        item for item in all_teams.json()["items"]
        if item["id"] == str(batch_default)
    )
    assert default_shared["visibility"] == "org"
    assert default_shared["share_status"] == "shared"


async def test_cross_team_shared_artifact_downloads_and_blocked_denials(
    run_library_setup: dict[str, object],
) -> None:
    app = run_library_setup["app"]
    raw_b = run_library_setup["raw_b"]
    trial_id = run_library_setup["trial_shared"]
    private_trial_id = run_library_setup["trial_private"]
    safe_key = run_library_setup["safe_key"]
    private_key = run_library_setup["private_key"]
    blocked_key = run_library_setup["blocked_key"]

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport, base_url="http://svc",
        follow_redirects=False,
    ) as ac:
        direct = await ac.get(
            f"/api/v1/trials/{trial_id}/artifacts/download",
            params={"key": safe_key},
            headers={"Authorization": f"Bearer {raw_b}"},
        )
        shared = await ac.get(
            f"/api/v1/run-library/trials/{trial_id}/artifacts/download",
            params={"key": safe_key},
            headers={"Authorization": f"Bearer {raw_b}"},
        )
        blocked = await ac.get(
            f"/api/v1/run-library/trials/{trial_id}/artifacts/download",
            params={"key": blocked_key},
            headers={"Authorization": f"Bearer {raw_b}"},
        )
        private_parent = await ac.get(
            f"/api/v1/run-library/trials/{private_trial_id}/artifacts/download",
            params={"key": private_key},
            headers={"Authorization": f"Bearer {raw_b}"},
        )

    assert direct.status_code == 403
    assert shared.status_code == 200
    assert "location" not in shared.headers
    assert shared.content == b'{"ok": true}\n'
    assert blocked.status_code == 403
    assert blocked.json()["detail"] == "secret-like content detected"
    assert private_parent.status_code == 403


async def test_clone_config_uses_destination_provider_and_records_provenance(
    run_library_setup: dict[str, object],
) -> None:
    app = run_library_setup["app"]
    raw_b = run_library_setup["raw_b"]
    batch_shared = run_library_setup["batch_shared"]
    conn_a = run_library_setup["conn_a"]
    conn_b = run_library_setup["conn_b"]
    team_b = run_library_setup["team_b"]
    postgres_url = run_library_setup["postgres_url"]

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport, base_url="http://svc",
    ) as ac:
        cloned = await ac.post(
            f"/api/v1/run-library/batches/{batch_shared}/clone-config",
            json={
                "name": "beta clone from alpha",
                "provider_connection_id": str(conn_b),
            },
            headers={"Authorization": f"Bearer {raw_b}"},
        )
        mutation = await ac.post(
            f"/api/v1/batches/{batch_shared}/cancel",
            headers={"Authorization": f"Bearer {raw_b}"},
        )

    assert cloned.status_code == 201, cloned.text
    body = cloned.json()
    assert body["cloned_from_batch_id"] == str(batch_shared)
    assert body["provider_connection_id"] == str(conn_b)
    assert body["provider_connection_id"] != str(conn_a)
    assert body["source_provenance"][0]["source_batch_id"] == str(batch_shared)
    assert mutation.status_code == 403

    sync_engine = create_engine(str(postgres_url))
    sl = sessionmaker(sync_engine)
    with sl() as s:
        row = s.execute(
            select(Batch).where(Batch.id == UUID(body["batch_id"])),
        ).scalar_one()
        assert row.team_id == team_b
        assert row.provider_connection_id == conn_b
        assert row.provider_connection_id != conn_a
        assert row.source_provenance[0]["source_batch_id"] == str(batch_shared)
    sync_engine.dispose()


async def test_reuse_shared_artifact_creates_provenance_and_blocks_raw(
    run_library_setup: dict[str, object],
) -> None:
    app = run_library_setup["app"]
    raw_b = run_library_setup["raw_b"]
    trial_id = run_library_setup["trial_shared"]
    safe_key = run_library_setup["safe_key"]
    blocked_key = run_library_setup["blocked_key"]

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport, base_url="http://svc",
    ) as ac:
        reused = await ac.post(
            f"/api/v1/run-library/trials/{trial_id}/artifacts/reuse",
            json={"key": safe_key, "name": "beta reuse of alpha report"},
            headers={"Authorization": f"Bearer {raw_b}"},
        )
        blocked = await ac.post(
            f"/api/v1/run-library/trials/{trial_id}/artifacts/reuse",
            json={"key": blocked_key, "name": "blocked reuse"},
            headers={"Authorization": f"Bearer {raw_b}"},
        )

    assert reused.status_code == 201, reused.text
    body = reused.json()
    assert body["source_artifact"]["trial_id"] == str(trial_id)
    assert body["source_artifact"]["key"] == safe_key
    assert body["source_provenance"][0]["source_trial_id"] == str(trial_id)
    assert body["source_provenance"][0]["source_artifact_key"] == safe_key
    assert blocked.status_code == 403
    assert blocked.json()["detail"] == "secret-like content detected"
