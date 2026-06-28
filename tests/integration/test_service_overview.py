"""Home overview summary API (#455).

The overview endpoint is the SPA's first-run control surface: it should
summarize what the signed-in team can do now, what needs attention, and
which operator-owned prerequisites are missing.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta
from uuid import UUID, uuid4

import httpx
import pytest
from fastapi import FastAPI
from sqlalchemy import create_engine, delete, insert, update
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.orm import sessionmaker

from loom.admin_secret import AdminSecretVerifier
from loom.db.schema import (
    Batch,
    Benchmark,
    ProviderConnection,
    Task,
    Team,
    TeamMembership,
    TeamQuota,
    Trial,
    User,
    UserSession,
    Worker,
    WorkerPoolAutoscalerPolicy,
)
from loom_service.app import create_app
from loom_service.config import LoomServiceSettings

RAW_ADMIN_TOKEN = "loom_admin_" + "O" * 43


def _valid_task_config(task_id: str) -> dict[str, object]:
    return {
        "schema_version": "1",
        "task": {"id": task_id, "name": task_id},
        "environment": {"os": "linux", "docker_image": "alpine"},
        "agent": {"name": "oracle"},
        "verifier": {"name": "pytest"},
        "steps": [{"name": "main"}],
    }


@pytest.fixture
async def overview_setup(
    monkeypatch: pytest.MonkeyPatch,
    postgres_url: str,
) -> AsyncIterator[tuple[FastAPI, UUID, UUID]]:
    for k, v in {
        "LOOM_SVC_DB_URL": postgres_url,
        "LOOM_SVC_MINIO_ENDPOINT": "http://minio:9000",
        "LOOM_SVC_MINIO_ACCESS_KEY": "x",
        "LOOM_SVC_MINIO_SECRET_KEY": "y",
        "LOOM_SVC_CONTROL_PLANE_URL": "http://cp:8080/",
        "LOOM_SVC_GATEWAY_URL": "http://gw:9100/",
        "LOOM_SVC_AUTH_RETURN_LOGIN_TOKEN": "1",
    }.items():
        monkeypatch.setenv(k, v)
    settings = LoomServiceSettings(_env_file=None)
    app = create_app(settings)
    engine = create_async_engine(str(settings.db_url))
    app.state.settings = settings
    app.state.session_factory = async_sessionmaker(
        engine,
        expire_on_commit=False,
    )
    app.state.admin_secret_verifier = AdminSecretVerifier.from_token(
        RAW_ADMIN_TOKEN,
    )
    app.state.http_client = httpx.AsyncClient(base_url="http://cp")

    team_id = uuid4()
    other_team_id = uuid4()
    user_id = uuid4()
    now = datetime.now(UTC)
    task_id = "humaneval/HumanEval/0"
    latest_batch_id = uuid4()
    old_batch_id = uuid4()
    other_batch_id = uuid4()
    stale_worker_id = uuid4()

    sync_engine = create_engine(postgres_url)
    sl = sessionmaker(sync_engine)
    with sl() as s:
        s.execute(insert(Team).values(id=team_id, name="EAI"))
        s.execute(insert(Team).values(id=other_team_id, name="Other Team"))
        s.execute(insert(User).values(
            id=user_id,
            username="Owner",
            username_normalized="owner",
            email="owner@example.com",
            display_name="Owner Example",
            status="active",
            is_platform_admin=False,
            created_at=now,
        ))
        s.execute(insert(TeamMembership).values(
            team_id=team_id,
            user_id=user_id,
            role="owner",
        ))
        s.execute(insert(Benchmark).values(
            id="humaneval",
            display_name="HumanEval",
            upstream_kind="huggingface",
            upstream_locator="openai/openai_humaneval",
            upstream_revision="",
            license_spdx="MIT",
            license_url="https://example.test/humaneval",
            splits=["test"],
        ))
        s.execute(insert(Benchmark).values(
            id="gaia",
            display_name="GAIA",
            upstream_kind="huggingface",
            upstream_locator="gaia-benchmark/GAIA",
            upstream_revision="",
            license_spdx="CC-BY-SA-4.0",
            license_url="https://example.test/gaia",
            splits=["validation"],
        ))
        s.execute(insert(Task).values(
            id=task_id,
            benchmark_id="humaneval",
            config=_valid_task_config(task_id),
            checksum="1" * 64,
            source="s3://bucket/humaneval/",
            license="MIT",
            registered_at=now,
        ))
        for name, status, err, deleted in (
            ("ready-provider", "valid", None, None),
            ("needs-fix", "invalid", "timeout after 5s", None),
            ("deleted", "valid", None, now),
        ):
            s.execute(insert(ProviderConnection).values(
                id=uuid4(),
                team_id=team_id,
                provider_type="openai-compatible",
                display_name=name,
                base_url="https://api.example.test/v1",
                upstream_host="api.example.test",
                encrypted_api_key_ref=f"env:{name.upper().replace('-', '_')}",
                created_by="test",
                status=status,
                last_validated_at=now if status != "pending" else None,
                last_validation_error=err,
                deleted_at=deleted,
            ))
        s.execute(insert(ProviderConnection).values(
            id=uuid4(),
            team_id=other_team_id,
            provider_type="openai-compatible",
            display_name="other-provider",
            base_url="https://api.example.test/v1",
            upstream_host="api.example.test",
            encrypted_api_key_ref="env:OTHER",
            created_by="test",
            status="valid",
        ))
        s.execute(insert(Worker).values(
            id=uuid4(),
            hostname="trt-gb10-1",
            version="test",
            capabilities=[{"backend": "docker", "cpu_arch": "arm64"}],
            max_concurrent=10,
            pool_name="gb10-arm64",
            registered_at=now,
            last_seen_at=now,
            status="active",
        ))
        fresh_x86_worker_id = uuid4()
        s.execute(insert(Worker).values(
            id=fresh_x86_worker_id,
            hostname="public-beta-x86-1",
            version="test",
            capabilities=[
                {"backend": "docker", "cpu_arch": "x86_64"},
                {"backend": "fake", "cpu_arch": "x86_64"},
            ],
            max_concurrent=2,
            pool_name="public-beta-x86",
            registered_at=now,
            last_seen_at=now,
            status="active",
        ))
        s.execute(insert(Worker).values(
            id=stale_worker_id,
            hostname="stale-worker",
            version="test",
            capabilities=[{"backend": "modal", "cpu_arch": "x86_64"}],
            max_concurrent=99,
            pool_name="stale-pool",
            registered_at=now - timedelta(minutes=10),
            last_seen_at=now - timedelta(minutes=10),
            status="active",
        ))
        s.execute(insert(WorkerPoolAutoscalerPolicy).values(
            environment="production",
            pool_name="gb10-arm64",
            actuator="slurm",
            enabled=True,
            min_slots=0,
            max_slots=150,
            scale_up_threshold_slots=1,
            scale_down_idle_seconds=600,
            scale_up_cooldown_seconds=60,
            scale_down_cooldown_seconds=300,
            drain_timeout_seconds=600,
            actuator_config={
                "backend": "docker",
                "cpu_arch": "arm64",
                "partition": "gb10",
                "allowed_nodes": ["trt-gb10-1"],
                "env_file": "/secure/.env.gb10-worker",
                "repo_dir": "/shared_work/qianyi/loom-remote-worker",
                "requested_cpus": 20,
                "requested_memory_mib": 115000,
                "requested_concurrency": 10,
            },
            last_decision="noop",
            last_decision_reason="at_target",
            last_desired_slots=150,
            last_actual_slots=10,
            last_pending_slots=0,
            last_draining_slots=0,
            last_occupied_slots=0,
            last_queued_slots=1,
        ))
        s.execute(insert(WorkerPoolAutoscalerPolicy).values(
            environment="production",
            pool_name="public-beta-x86",
            actuator="slurm",
            enabled=True,
            min_slots=0,
            max_slots=12,
            scale_up_threshold_slots=1,
            scale_down_idle_seconds=600,
            scale_up_cooldown_seconds=60,
            scale_down_cooldown_seconds=300,
            drain_timeout_seconds=600,
            actuator_config={"backend": "docker", "cpu_arch": "x86_64"},
            last_decision="blocked",
            last_decision_reason="queued_deficit",
            last_desired_slots=6,
            last_actual_slots=2,
            last_pending_slots=6,
            last_draining_slots=0,
            last_occupied_slots=2,
            last_queued_slots=1,
            last_blocked_reason="pending_cap",
        ))
        s.execute(insert(Batch).values(
            id=old_batch_id,
            team_id=team_id,
            name="older submitted",
            task_filter={"benchmark_ids": ["humaneval"]},
            trial_config={"agent_name": "oracle"},
            state="submitted",
            created_at=now - timedelta(hours=1),
            created_by_token_prefix="test:web",
            expected_trial_count=1,
            backend="docker",
        ))
        s.execute(insert(Batch).values(
            id=latest_batch_id,
            team_id=team_id,
            name="latest running",
            task_filter={"benchmark_ids": ["humaneval"]},
            trial_config={"agent_name": "oracle"},
            state="running",
            created_at=now,
            created_by_token_prefix="test:web",
            expected_trial_count=2,
            backend="docker",
        ))
        s.execute(insert(Batch).values(
            id=other_batch_id,
            team_id=other_team_id,
            name="other submitted",
            task_filter={"benchmark_ids": ["humaneval"]},
            trial_config={"agent_name": "oracle"},
            state="submitted",
            created_at=now,
            created_by_token_prefix="test:web",
            expected_trial_count=100,
            backend="docker",
        ))
        for state, batch_id, worker_id in (
            ("queued", old_batch_id, None),
            ("claimed", latest_batch_id, fresh_x86_worker_id),
            ("running", latest_batch_id, fresh_x86_worker_id),
            ("succeeded", latest_batch_id, None),
        ):
            s.execute(insert(Trial).values(
                id=uuid4(),
                team_id=team_id,
                task_id=task_id,
                batch_id=batch_id,
                config={"agent_name": "oracle"},
                requires_caps={},
                state=state,
                worker_id=worker_id,
                result={"aggregate_reward": 1.0} if state == "succeeded" else None,
                submitted_at=now,
            ))
        s.execute(insert(Trial).values(
            id=uuid4(),
            team_id=other_team_id,
            task_id=task_id,
            batch_id=other_batch_id,
            config={"agent_name": "oracle"},
            requires_caps={},
            state="queued",
            submitted_at=now,
        ))
        s.commit()
    try:
        yield app, team_id, latest_batch_id
    finally:
        await app.state.http_client.aclose()
        await engine.dispose()
        with sl() as s:
            from loom.db.schema import LoginChallenge

            s.execute(delete(Trial))
            s.execute(delete(Batch))
            s.execute(delete(ProviderConnection))
            s.execute(delete(Task))
            s.execute(delete(Benchmark))
            s.execute(delete(WorkerPoolAutoscalerPolicy))
            s.execute(delete(Worker))
            s.execute(delete(UserSession))
            s.execute(delete(LoginChallenge))
            s.execute(delete(TeamMembership))
            s.execute(delete(TeamQuota))
            s.execute(delete(User))
            s.execute(delete(Team))
            s.commit()
        sync_engine.dispose()


async def _login(ac: httpx.AsyncClient) -> dict[str, object]:
    start = await ac.post(
        "/api/v1/auth/login/start",
        json={"email": "owner@example.com"},
    )
    assert start.status_code == 200, start.text
    login_token = start.json().get("login_token")
    assert isinstance(login_token, str)
    complete = await ac.post(
        "/api/v1/auth/login/complete",
        json={"token": login_token},
    )
    assert complete.status_code == 200, complete.text
    return complete.json()


async def test_overview_summarizes_signed_in_team_readiness(
    overview_setup: tuple[FastAPI, UUID, UUID],
) -> None:
    app, team_id, latest_batch_id = overview_setup
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://svc",
    ) as ac:
        await _login(ac)
        r = await ac.get("/api/v1/overview")

    assert r.status_code == 200, r.text
    body = r.json()
    assert body["status"] == "ready"
    assert body["team_context"] == {
        "team_id": str(team_id),
        "team_name": "EAI",
        "role": "owner",
        "scopes": [
            "read:own",
            "submit",
            "tokens:manage",
            "providers:manage",
            "team:manage",
        ],
        "is_platform_admin": False,
        "submissions_paused": False,
    }
    assert body["capabilities"] == {
        "can_read": True,
        "can_submit": True,
        "can_manage_providers": True,
        "can_manage_team": True,
    }
    assert body["provider_health"] | {
        "total": 2,
        "ready": 1,
        "needs_attention": 1,
        "untested": 0,
    } == body["provider_health"]
    assert [p["name"] for p in body["provider_health"]["latest"]] == [
        "needs-fix",
        "ready-provider",
    ]
    assert body["benchmark_readiness"] | {
        "total": 2,
        "runnable": 1,
        "needs_attention": 1,
    } == body["benchmark_readiness"]
    assert body["benchmark_readiness"]["blocked"][0]["id"] == "gaia"
    assert body["worker_health"] == {
        "active": 2,
        "available_backends": ["docker", "fake"],
        "has_default_backend": True,
    }
    assert body["run_activity"]["batches"] == {
        "submitted": 1,
        "running": 1,
        "finished": 0,
        "cancelled": 0,
    }
    assert body["run_activity"]["trials"] == {
        "queued": 1,
        "claimed": 1,
        "running": 1,
        "succeeded": 1,
        "failed": 0,
        "cancelled": 0,
    }
    assert body["run_activity"]["latest_batch"] | {
        "id": str(latest_batch_id),
        "name": "latest running",
        "state": "running",
        "expected_trial_count": 2,
    } == body["run_activity"]["latest_batch"]
    action_ids = {item["id"] for item in body["next_actions"]}
    assert {"create_batch", "repair_provider"} <= action_ids
    assert "start_worker" not in action_ids


async def test_monitor_summary_scopes_state_counts_and_worker_capacity(
    overview_setup: tuple[FastAPI, UUID, UUID],
) -> None:
    app, team_id, _latest_batch_id = overview_setup
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://svc",
    ) as ac:
        await _login(ac)
        r = await ac.get(
            "/api/v1/monitor/summary",
            params={
                "view": "trials",
                "team_id": str(team_id),
                "benchmark_id": "humaneval",
                "agent": "oracle",
                "state": "failed",
            },
        )

    assert r.status_code == 200, r.text
    body = r.json()
    assert body["scope"] == {
        "view": "trials",
        "team_id": str(team_id),
        "q": None,
        "benchmark_id": "humaneval",
        "agent_name": "oracle",
        "model_provider": None,
        "model_name": None,
        "provider_connection_id": None,
        "provider_model_id": None,
        "batch_id": None,
        "state": "failed",
    }
    assert body["state_counts"]["batches"] == {
        "submitted": 1,
        "running": 1,
        "finished": 0,
        "cancelled": 0,
    }
    assert body["state_counts"]["trials"] == {
        "queued": 1,
        "claimed": 1,
        "running": 1,
        "succeeded": 1,
        "failed": 0,
        "cancelled": 0,
    }
    assert body["queue"] == {
        "queued": 1,
        "claimed": 1,
        "running": 1,
        "waiting": 2,
        "active_workers": 2,
        "available_backends": ["docker", "fake"],
        "has_default_backend": True,
        "status": "waiting",
    }
    assert body["resources"]["aggregate"] == {
        "desired_slots": 156,
        "pending_slots": 6,
        "current_active_slots": 12,
        "max_slots": 162,
        "ceiling_slots": 162,
        "active_workers": 2,
        "draining_workers": 0,
        "total_slots": 12,
        "draining_slots": 0,
        "occupied_slots": 2,
        "free_slots": 10,
        "running_tasks": 1,
        "starting_tasks": 1,
        "queued_tasks": 1,
    }
    assert body["resources"]["pools"] == [
        {
            "pool_name": "gb10-arm64",
            "backend": "docker",
            "cpu_arch": "arm64",
            "autoscaler_environment": "production",
            "autoscaler_actuator": "slurm",
            "autoscaler_enabled": True,
            "autoscaler_idle_since_at": None,
            "autoscaler_idle_seconds": None,
            "desired_slots": 150,
            "pending_slots": 0,
            "current_active_slots": 10,
            "max_slots": 150,
            "ceiling_slots": 150,
            "active_workers": 1,
            "draining_workers": 0,
            "total_slots": 10,
            "draining_slots": 0,
            "occupied_slots": 0,
            "free_slots": 10,
            "running_tasks": 0,
            "starting_tasks": 0,
            "queued_tasks": 1,
            "last_autoscaler_decision": "noop",
            "last_autoscaler_reason": "at_target",
            "decision_reason": "at_target",
            "last_autoscaler_blocked_reason": None,
            "blocked_reason": None,
            "last_autoscaler_error": None,
        },
        {
            "pool_name": "public-beta-x86",
            "backend": "docker",
            "cpu_arch": "x86_64",
            "autoscaler_environment": "production",
            "autoscaler_actuator": "slurm",
            "autoscaler_enabled": True,
            "autoscaler_idle_since_at": None,
            "autoscaler_idle_seconds": None,
            "desired_slots": 6,
            "pending_slots": 6,
            "current_active_slots": 2,
            "max_slots": 12,
            "ceiling_slots": 12,
            "active_workers": 1,
            "draining_workers": 0,
            "total_slots": 2,
            "draining_slots": 0,
            "occupied_slots": 2,
            "free_slots": 0,
            "running_tasks": 1,
            "starting_tasks": 1,
            "queued_tasks": 1,
            "last_autoscaler_decision": "blocked",
            "last_autoscaler_reason": "queued_deficit",
            "decision_reason": "queued_deficit",
            "last_autoscaler_blocked_reason": "pending_cap",
            "blocked_reason": "pending_cap",
            "last_autoscaler_error": None,
        },
    ]


async def test_monitor_summary_search_scopes_batch_identity_counts(
    overview_setup: tuple[FastAPI, UUID, UUID],
) -> None:
    app, team_id, _latest_batch_id = overview_setup
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://svc",
    ) as ac:
        await _login(ac)
        r = await ac.get(
            "/api/v1/monitor/summary",
            params={
                "view": "batches",
                "team_id": str(team_id),
                "q": "latest",
            },
        )

    assert r.status_code == 200, r.text
    body = r.json()
    assert body["scope"]["q"] == "latest"
    assert body["state_counts"]["batches"] == {
        "submitted": 0,
        "running": 1,
        "finished": 0,
        "cancelled": 0,
    }
    assert body["state_counts"]["trials"] == {
        "queued": 0,
        "claimed": 1,
        "running": 1,
        "succeeded": 1,
        "failed": 0,
        "cancelled": 0,
    }
    assert body["queue"] | {
        "queued": 0,
        "claimed": 1,
        "running": 1,
        "waiting": 1,
    } == body["queue"]


async def test_overview_marks_operator_prerequisites_separately(
    overview_setup: tuple[FastAPI, UUID, UUID],
    postgres_url: str,
) -> None:
    app, _team_id, _latest_batch_id = overview_setup
    sync_engine = create_engine(postgres_url)
    sl = sessionmaker(sync_engine)
    with sl() as s:
        s.execute(update(Trial).values(worker_id=None))
        s.execute(delete(Worker))
        s.execute(update(Task).values(benchmark_id=None))
        s.commit()
    sync_engine.dispose()

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://svc",
    ) as ac:
        await _login(ac)
        r = await ac.get("/api/v1/overview")

    assert r.status_code == 200, r.text
    body = r.json()
    assert body["status"] == "needs_setup"
    assert body["worker_health"] == {
        "active": 0,
        "available_backends": [],
        "has_default_backend": False,
    }
    assert body["benchmark_readiness"]["runnable"] == 0
    operator_actions = [
        action for action in body["next_actions"]
        if action["kind"] == "operator"
    ]
    assert {action["id"] for action in operator_actions} == {
        "publish_benchmarks",
        "start_worker",
    }
