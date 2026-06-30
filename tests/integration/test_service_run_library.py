"""Org-wide Run Library and shared artifact behavior (#336)."""

from __future__ import annotations

import hashlib
import json
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
    Artifact,
    ArtifactLineageEdge,
    Batch,
    ProviderConnection,
    Task,
    Team,
    TeamQuota,
    Token,
    Trial,
    User,
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
    legacy_raw_b = f"loom_team_{uuid4().hex}"
    user_a = uuid4()
    user_b = uuid4()
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
    safe_artifact_id = uuid4()
    blocked_artifact_id = uuid4()
    parent_artifact_id = uuid4()

    with sl() as s:
        s.execute(insert(Team).values(id=team_a, name="Alpha Research"))
        s.execute(insert(Team).values(id=team_b, name="Beta Apps"))
        s.execute(insert(User).values(
            id=user_a,
            username="AlphaOwner",
            username_normalized="alphaowner",
            status="active",
            is_platform_admin=False,
        ))
        s.execute(insert(User).values(
            id=user_b,
            username="BetaOwner",
            username_normalized="betaowner",
            status="active",
            is_platform_admin=False,
        ))
        s.execute(insert(TeamQuota).values(team_id=team_a))
        s.execute(insert(TeamQuota).values(team_id=team_b))
        for raw, team_id, user_id in (
            (raw_a, team_a, user_a),
            (raw_b, team_b, user_b),
        ):
            s.execute(insert(Token).values(
                token_hash=hashlib.sha256(raw.encode()).digest(),
                type="team",
                scopes=["read:own", "submit", "providers:manage"],
                team_id=team_id,
                created_by_user_id=user_id,
                issued_at=now,
                expires_at=None,
            ))
        s.execute(insert(Token).values(
            token_hash=hashlib.sha256(legacy_raw_b.encode()).digest(),
            type="team",
            scopes=["read:own", "submit", "providers:manage"],
            team_id=team_b,
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
                        "share_status": "shared",
                        "blocked_reason": "legacy field is stale",
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
        s.execute(insert(Artifact).values(
            id=safe_artifact_id,
            artifact_type="metric_table",
            artifact_schema_version="1.0",
            name="Alpha aggregate metrics",
            team_id=team_a,
            batch_id=batch_shared,
            trial_id=trial_shared,
            created_by={
                "kind": "trial",
                "batch_id": str(batch_shared),
                "trial_id": str(trial_shared),
            },
            content_hash="sha256:" + ("a" * 64),
            storage={
                "backend": "object_store",
                "bucket": settings.artifacts_bucket,
                "key": safe_key,
                "media_type": "application/json",
                "size_bytes": 17,
            },
            visibility="org",
            share_status="shared",
            redaction_state="redacted",
            safety_state="safe",
            blocked_reason=None,
            retention={"class": "shared_reusable", "expires_at": None},
            provenance={
                "batch_id": str(batch_shared),
                "trial_id": str(trial_shared),
                "source_trial_ids": [str(trial_shared)],
                "agent": "litellm",
                "model": "gpt-4o-mini",
                "relation": "produced_from",
            },
            artifact_metadata={"metric_name": "aggregate_reward"},
        ))
        s.execute(insert(Artifact).values(
            id=parent_artifact_id,
            artifact_type="task_set",
            artifact_schema_version="1.0",
            name="Parent task set",
            team_id=team_a,
            created_by={"kind": "manual_import"},
            content_hash="sha256:" + ("d" * 64),
            storage={
                "backend": "object_store",
                "bucket": settings.artifacts_bucket,
                "key": f"{team_a}/parents/task-set.json",
                "media_type": "application/json",
                "size_bytes": 12,
            },
            visibility="org",
            share_status="shared",
            redaction_state="redacted",
            safety_state="safe",
            blocked_reason=None,
            retention={"class": "shared_reusable", "expires_at": None},
            provenance={},
            artifact_metadata={"task_count": 1},
        ))
        s.execute(insert(ArtifactLineageEdge).values(
            child_artifact_id=safe_artifact_id,
            parent_artifact_id=parent_artifact_id,
            relation="produced_from",
            edge_metadata={"source": "test"},
        ))
        s.execute(insert(Artifact).values(
            id=blocked_artifact_id,
            artifact_type="debug_bundle",
            artifact_schema_version="1.0",
            name="Unsafe debug bundle",
            team_id=team_a,
            batch_id=batch_shared,
            trial_id=trial_shared,
            created_by={
                "kind": "trial",
                "batch_id": str(batch_shared),
                "trial_id": str(trial_shared),
            },
            content_hash="sha256:" + ("b" * 64),
            storage={
                "backend": "object_store",
                "bucket": settings.artifacts_bucket,
                "key": blocked_key,
                "media_type": "text/plain",
                "size_bytes": 21,
            },
            visibility="org",
            share_status="shared",
            redaction_state="blocked",
            safety_state="unsafe",
            blocked_reason="secret-like content detected",
            retention={"class": "owner_only_debug", "expires_at": None},
            provenance={
                "batch_id": str(batch_shared),
                "trial_id": str(trial_shared),
                "source_trial_ids": [str(trial_shared)],
                "relation": "produced_from",
            },
            artifact_metadata={"debug_kind": "raw_log"},
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
            "legacy_raw_b": legacy_raw_b,
            "team_a": team_a,
            "team_b": team_b,
            "user_b": user_b,
            "batch_shared": batch_shared,
            "batch_default": batch_default,
            "batch_private": batch_private,
            "batch_running": batch_running,
            "batch_b": batch_b,
            "trial_shared": trial_shared,
            "trial_private": trial_private,
            "conn_a": conn_a,
            "conn_b": conn_b,
            "task_id": task_id,
            "safe_key": safe_key,
            "private_key": private_key,
            "blocked_key": blocked_key,
            "safe_artifact_id": safe_artifact_id,
            "blocked_artifact_id": blocked_artifact_id,
            "parent_artifact_id": parent_artifact_id,
            "postgres_url": postgres_url,
        }
    finally:
        await app.state.http_client.aclose()
        await engine.dispose()
        with sl() as s:
            s.execute(delete(ArtifactLineageEdge))
            s.execute(delete(Artifact))
            s.execute(delete(Trial))
            s.execute(delete(Batch))
            s.execute(delete(ProviderConnection))
            s.execute(delete(Token))
            s.execute(delete(Task))
            s.execute(delete(TeamQuota))
            s.execute(delete(User).where(User.id.in_([user_a, user_b])))
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


async def test_run_library_filters_by_structured_batch_fields(
    run_library_setup: dict[str, object],
) -> None:
    app = run_library_setup["app"]
    raw_b = run_library_setup["raw_b"]
    team_a = run_library_setup["team_a"]
    conn_a = run_library_setup["conn_a"]
    postgres_url = run_library_setup["postgres_url"]
    wanted_id = uuid4()
    wrong_id = uuid4()
    now = datetime.now(UTC)

    sync_engine = create_engine(str(postgres_url))
    with sync_engine.begin() as conn:
        conn.execute(
            insert(Batch),
            [
                {
                    "id": wanted_id,
                    "team_id": team_a,
                    "name": "skilllearnbench codex qwen generated",
                    "description": "structured library needle",
                    "task_filter": {
                        "subset_kind": "random_n",
                        "benchmark_ids": ["skilllearnbench"],
                        "n": 5,
                    },
                    "trial_config": {},
                    "state": "finished",
                    "result_status": "succeeded",
                    "created_at": now,
                    "finished_at": now,
                    "created_by_token_prefix": "test:web",
                    "expected_trial_count": 1,
                    "backend": "docker",
                    "combinations": [
                        {
                            "agent_name": "codex",
                            "agent_model": {
                                "provider": "openai",
                                "name": "qwen3.6-35b-a3b",
                            },
                            "n_per_task": 1,
                        }
                    ],
                    "provider_connection_id": conn_a,
                    "provider_model_id": "qwen3.6-35b-a3b",
                    "visibility": "org",
                    "share_status": "shared",
                },
                {
                    "id": wrong_id,
                    "team_id": team_a,
                    "name": "skilllearnbench codex claude generated",
                    "description": "structured library needle",
                    "task_filter": {
                        "subset_kind": "random_n",
                        "benchmark_ids": ["skilllearnbench"],
                        "n": 5,
                    },
                    "trial_config": {},
                    "state": "finished",
                    "result_status": "succeeded",
                    "created_at": now,
                    "finished_at": now,
                    "created_by_token_prefix": "test:web",
                    "expected_trial_count": 1,
                    "backend": "docker",
                    "combinations": [
                        {
                            "agent_name": "codex",
                            "agent_model": {
                                "provider": "anthropic",
                                "name": "claude-sonnet-4-6",
                            },
                            "n_per_task": 1,
                        }
                    ],
                    "provider_connection_id": conn_a,
                    "provider_model_id": "claude-sonnet-4-6",
                    "visibility": "org",
                    "share_status": "shared",
                },
            ],
        )
    sync_engine.dispose()

    params = (
        "scope=all&q=needle&benchmark_id=skilllearnbench&agent_name=codex"
        "&model_provider=openai&model_name=qwen3.6-35b-a3b"
        f"&provider_connection_id={conn_a}&provider_model_id=qwen3.6-35b-a3b"
    )
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport, base_url="http://svc",
    ) as ac:
        r = await ac.get(
            f"/api/v1/run-library/batches?{params}",
            headers={"Authorization": f"Bearer {raw_b}"},
        )

    assert r.status_code == 200, r.text
    assert [item["id"] for item in r.json()["items"]] == [str(wanted_id)]


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


async def test_typed_registry_filters_detail_inventory_and_metadata_export(
    run_library_setup: dict[str, object],
) -> None:
    app = run_library_setup["app"]
    raw_b = run_library_setup["raw_b"]
    team_a = run_library_setup["team_a"]
    batch_shared = run_library_setup["batch_shared"]
    raw_a = run_library_setup["raw_a"]
    trial_id = run_library_setup["trial_shared"]
    safe_key = run_library_setup["safe_key"]
    safe_artifact_id = run_library_setup["safe_artifact_id"]
    blocked_artifact_id = run_library_setup["blocked_artifact_id"]
    parent_artifact_id = run_library_setup["parent_artifact_id"]

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport, base_url="http://svc",
    ) as ac:
        batches = await ac.get(
            "/api/v1/run-library/batches",
            params={
                "scope": "all",
                "artifact_type": "metric_table",
                "owner_team_id": str(team_a),
                "source_trial_id": str(trial_id),
                "safety_state": "safe",
                "provenance_relation": "produced_from",
            },
            headers={"Authorization": f"Bearer {raw_b}"},
        )
        artifacts = await ac.get(
            "/api/v1/run-library/artifacts",
            params={
                "scope": "all",
                "artifact_type": "metric_table",
                "owner_team_id": str(team_a),
                "source_trial_id": str(trial_id),
                "safety_state": "safe",
                "provenance_relation": "produced_from",
            },
            headers={"Authorization": f"Bearer {raw_b}"},
        )
        detail = await ac.get(
            f"/api/v1/run-library/batches/{batch_shared}",
            headers={"Authorization": f"Bearer {raw_b}"},
        )
        exported = await ac.get(
            "/api/v1/run-library/artifacts/export",
            params={
                "scope": "all",
                "artifact_type": "metric_table",
                "format": "jsonl",
            },
            headers={"Authorization": f"Bearer {raw_b}"},
        )
        owner_detail = await ac.get(
            f"/api/v1/run-library/batches/{batch_shared}",
            headers={"Authorization": f"Bearer {raw_a}"},
        )

    assert batches.status_code == 200, batches.text
    assert [item["id"] for item in batches.json()["items"]] == [str(batch_shared)]

    assert artifacts.status_code == 200, artifacts.text
    artifact_items = artifacts.json()["items"]
    assert [item["id"] for item in artifact_items] == [str(safe_artifact_id)]
    artifact = artifact_items[0]
    assert artifact["artifact_type"] == "metric_table"
    assert artifact["artifact_type_label"] == "Metric table"
    assert artifact["owner_team"] == {
        "id": str(team_a),
        "name": "Alpha Research",
    }
    assert artifact["source"] == {
        "kind": "trial",
        "batch_id": str(batch_shared),
        "trial_id": str(trial_id),
    }
    assert artifact["storage"]["key"] == safe_key
    assert artifact["safety_state"] == "safe"
    assert artifact["redaction_state"] == "redacted"
    assert artifact["content_hash"] == "sha256:" + ("a" * 64)
    assert artifact["metadata"] == {"metric_name": "aggregate_reward"}
    assert artifact["parents"] == [{
        "artifact_id": str(parent_artifact_id),
        "relation": "produced_from",
        "metadata": {"source": "test"},
    }]
    assert artifact["download_url"].endswith(
        f"/api/v1/run-library/trials/{trial_id}/artifacts/download?key="
        f"{safe_key.replace('/', '%2F')}",
    )

    assert detail.status_code == 200, detail.text
    detail_json = detail.json()
    assert detail_json["owner_team"] == {
        "id": str(team_a),
        "name": "Alpha Research",
    }
    inventory = detail_json["artifact_inventory"]
    metric = next(
        item for item in inventory["reports"]
        if item["id"] == str(safe_artifact_id)
    )
    assert metric["artifact_type_label"] == "Metric table"
    assert metric["owner_team"]["name"] == "Alpha Research"
    assert metric["source"]["trial_id"] == str(trial_id)
    assert metric["safety_state"] == "safe"
    unsafe = next(
        item for item in inventory["raw_diagnostics"]
        if item["id"] == str(blocked_artifact_id)
    )
    assert unsafe["artifact_type"] == "debug_bundle"
    assert unsafe["safety_state"] == "unsafe"
    assert unsafe["share_status"] == "shared"
    assert unsafe["blocked_reason"] == "secret-like content detected"
    assert unsafe["key"].startswith("redacted-artifact:")
    assert unsafe["storage"] is None
    assert unsafe["download_url"] is None
    assert unsafe["metadata"] == {}
    assert "debug.log" not in json.dumps(unsafe)

    assert owner_detail.status_code == 200, owner_detail.text
    owner_unsafe = next(
        item for item in owner_detail.json()["artifact_inventory"]["raw_diagnostics"]
        if item["id"] == str(blocked_artifact_id)
    )
    assert owner_unsafe["storage"]["key"].endswith("/debug.log")
    assert owner_unsafe["download_url"].endswith(
        f"/api/v1/run-library/trials/{trial_id}/artifacts/download?key="
        f"{run_library_setup['blocked_key'].replace('/', '%2F')}",
    )

    assert exported.status_code == 200, exported.text
    assert exported.headers["content-type"].startswith("application/x-ndjson")
    lines = [json.loads(line) for line in exported.text.splitlines()]
    assert [item["id"] for item in lines] == [str(safe_artifact_id)]
    assert str(blocked_artifact_id) not in exported.text
    assert "sk-test-should-not-leak" not in exported.text


async def test_artifact_filter_is_applied_before_batch_limit(
    run_library_setup: dict[str, object],
) -> None:
    app = run_library_setup["app"]
    raw_b = run_library_setup["raw_b"]
    team_a = run_library_setup["team_a"]
    conn_a = run_library_setup["conn_a"]
    task_id = run_library_setup["task_id"]
    postgres_url = run_library_setup["postgres_url"]
    older_batch = uuid4()
    older_trial = uuid4()
    newer_batch = uuid4()
    newer_trial = uuid4()
    metric_artifact = uuid4()
    now = datetime.now(UTC)

    sync_engine = create_engine(str(postgres_url))
    with sync_engine.begin() as conn:
        for batch_id, trial_id, created_offset, name in (
            (older_batch, older_trial, 0, "older metric batch"),
            (newer_batch, newer_trial, 1, "newer nonmatching batch"),
        ):
            created = now.replace(microsecond=created_offset)
            conn.execute(insert(Batch).values(
                id=batch_id,
                team_id=team_a,
                name=name,
                description=None,
                task_filter={"subset_kind": "explicit", "task_ids": ["t"]},
                trial_config={},
                state="finished",
                result_status="succeeded",
                created_at=created,
                finished_at=created,
                created_by_token_prefix="test:web",
                expected_trial_count=1,
                backend="docker",
                combinations=[],
                provider_connection_id=conn_a,
                provider_model_id="gpt-4o-mini",
                visibility="org",
                share_status="shared",
            ))
            conn.execute(insert(Trial).values(
                id=trial_id,
                team_id=team_a,
                batch_id=batch_id,
                task_id=task_id,
                config={},
                requires_caps={},
                state="succeeded",
                submitted_at=created,
                started_at=created,
                finished_at=created,
                result={"aggregate_reward": 1.0},
                visibility="org",
                share_status="shared",
            ))
        conn.execute(insert(Artifact).values(
            id=metric_artifact,
            artifact_type="training_data_export",
            artifact_schema_version="1.0",
            name="older export",
            team_id=team_a,
            batch_id=older_batch,
            trial_id=older_trial,
            created_by={"kind": "trial", "trial_id": str(older_trial)},
            content_hash="sha256:" + ("c" * 64),
            storage={
                "backend": "object_store",
                "bucket": "artifacts",
                "key": f"{team_a}/{older_trial}/main/export.jsonl",
                "media_type": "application/json",
                "size_bytes": 2,
            },
            visibility="org",
            share_status="shared",
            redaction_state="redacted",
            safety_state="safe",
            retention={"class": "shared_reusable"},
            provenance={
                "batch_id": str(older_batch),
                "trial_id": str(older_trial),
                "relation": "produced_from",
            },
            artifact_metadata={},
            created_at=now,
        ))
    sync_engine.dispose()

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport, base_url="http://svc",
    ) as ac:
        r = await ac.get(
            "/api/v1/run-library/batches",
            params={
                "scope": "all",
                "artifact_type": "training_data_export",
                "limit": "1",
            },
            headers={"Authorization": f"Bearer {raw_b}"},
        )

    assert r.status_code == 200, r.text
    assert [item["id"] for item in r.json()["items"]] == [str(older_batch)]


async def test_typed_registry_policy_controls_reuse_and_records_provenance(
    run_library_setup: dict[str, object],
) -> None:
    app = run_library_setup["app"]
    raw_b = run_library_setup["raw_b"]
    trial_id = run_library_setup["trial_shared"]
    safe_key = run_library_setup["safe_key"]
    blocked_key = run_library_setup["blocked_key"]
    safe_artifact_id = run_library_setup["safe_artifact_id"]
    conn_a = run_library_setup["conn_a"]
    postgres_url = run_library_setup["postgres_url"]
    user_b = run_library_setup["user_b"]

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport, base_url="http://svc",
    ) as ac:
        reused = await ac.post(
            f"/api/v1/run-library/trials/{trial_id}/artifacts/reuse",
            json={"key": safe_key, "name": "beta reuse of typed metrics"},
            headers={"Authorization": f"Bearer {raw_b}"},
        )
        blocked = await ac.post(
            f"/api/v1/run-library/trials/{trial_id}/artifacts/reuse",
            json={"key": blocked_key, "name": "blocked typed debug reuse"},
            headers={"Authorization": f"Bearer {raw_b}"},
        )

    assert reused.status_code == 201, reused.text
    body = reused.json()
    assert body["source_artifact"]["id"] == str(safe_artifact_id)
    assert body["source_artifact"]["artifact_type"] == "metric_table"
    assert body["source_artifact"]["content_hash"] == "sha256:" + ("a" * 64)
    assert body["source_artifact"]["artifact_schema_version"] == "1.0"
    provenance = body["source_provenance"][0]
    assert provenance["kind"] == "reused_artifact"
    assert provenance["relation"] == "reused_as_input"
    assert provenance["source_artifact_id"] == str(safe_artifact_id)
    assert provenance["source_artifact_key"] == safe_key
    assert provenance["source_content_hash"] == "sha256:" + ("a" * 64)
    assert provenance["source_artifact_schema_version"] == "1.0"
    assert "provider_connection_id" not in provenance

    assert blocked.status_code == 403
    assert blocked.json()["detail"] == "secret-like content detected"

    sync_engine = create_engine(str(postgres_url))
    sl = sessionmaker(sync_engine)
    with sl() as s:
        row = s.execute(
            select(Batch).where(Batch.id == UUID(body["batch_id"])),
        ).scalar_one()
        assert row.provider_connection_id is None
        assert row.provider_connection_id != conn_a
        assert row.submitted_by_user_id == user_b
        assert row.source_provenance[0]["source_artifact_id"] == str(safe_artifact_id)
    sync_engine.dispose()


async def test_clone_config_uses_destination_provider_and_records_provenance(
    run_library_setup: dict[str, object],
) -> None:
    app = run_library_setup["app"]
    raw_b = run_library_setup["raw_b"]
    batch_shared = run_library_setup["batch_shared"]
    conn_a = run_library_setup["conn_a"]
    conn_b = run_library_setup["conn_b"]
    team_b = run_library_setup["team_b"]
    user_b = run_library_setup["user_b"]
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
        assert row.submitted_by_user_id == user_b
        assert row.provider_connection_id == conn_b
        assert row.provider_connection_id != conn_a
        assert row.source_provenance[0]["source_batch_id"] == str(batch_shared)
    sync_engine.dispose()


async def test_legacy_team_token_cannot_clone_run_library_batch(
    run_library_setup: dict[str, object],
) -> None:
    app = run_library_setup["app"]
    legacy_raw_b = run_library_setup["legacy_raw_b"]
    batch_shared = run_library_setup["batch_shared"]
    conn_b = run_library_setup["conn_b"]

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport, base_url="http://svc",
    ) as ac:
        cloned = await ac.post(
            f"/api/v1/run-library/batches/{batch_shared}/clone-config",
            json={
                "name": "legacy clone from alpha",
                "provider_connection_id": str(conn_b),
            },
            headers={"Authorization": f"Bearer {legacy_raw_b}"},
        )

    assert cloned.status_code == 403
    assert "legacy team token" in cloned.json()["detail"]


async def test_legacy_team_token_cannot_reuse_run_library_artifact(
    run_library_setup: dict[str, object],
) -> None:
    app = run_library_setup["app"]
    legacy_raw_b = run_library_setup["legacy_raw_b"]
    trial_id = run_library_setup["trial_shared"]
    safe_key = run_library_setup["safe_key"]

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport, base_url="http://svc",
    ) as ac:
        reused = await ac.post(
            f"/api/v1/run-library/trials/{trial_id}/artifacts/reuse",
            json={"key": safe_key, "name": "legacy reuse of alpha report"},
            headers={"Authorization": f"Bearer {legacy_raw_b}"},
        )

    assert reused.status_code == 403
    assert "legacy team token" in reused.json()["detail"]


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
