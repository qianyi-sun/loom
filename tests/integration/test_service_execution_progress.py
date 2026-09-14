"""Native observations keep ordinary Trial progress and counters in sync."""

import hashlib
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import httpx
import pytest
from sqlalchemy import delete
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from loom.db.schema import Batch, Token, Trial, User
from loom_control_plane.service_execution import record_execution_event
from loom_service.app import create_app
from loom_service.config import LoomServiceSettings
from tests.integration import test_service_execution_leases as fixtures
from tests.integration.test_service_execution_leases import (
    _cleanup_service_execution_test_rows,  # noqa: F401
)


async def test_native_start_reaches_public_trial_list_detail_and_batch(postgres_url: str) -> None:
    engine = create_async_engine(postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    now = datetime.now(UTC)
    actual_start = now + timedelta(seconds=8)
    user_id, batch_id = uuid4(), uuid4()
    raw_token = f"test-progress-{uuid4()}"
    token_hash = hashlib.sha256(raw_token.encode()).digest()
    settings = LoomServiceSettings(
        _env_file=None, db_url=postgres_url, minio_endpoint="http://minio:9000",
        minio_access_key="test", minio_secret_key="test",
        control_plane_url="http://cp/", gateway_url="http://gw/",
    )
    app = create_app(settings)
    app.state.settings = settings
    app.state.session_factory = sessions
    try:
        async with sessions() as session:
            trial_id, target = await fixtures._seed_ready_trial(session, now=now)
            lease = await fixtures._reserve(session, trial_id=trial_id, target=target, now=now)
            trial = await session.get(Trial, trial_id)
            assert trial is not None
            session.add(User(
                id=user_id, username=f"progress-{user_id}",
                username_normalized=f"progress-{user_id}", status="active",
                is_platform_admin=False,
            ))
            session.add(Batch(
                id=batch_id, team_id=trial.team_id, name="native progress",
                task_filter={}, trial_config={}, expected_trial_count=1,
                created_by_token_prefix="test", state="running", backend="nebius",
            ))
            await session.flush()
            session.add(Token(
                token_hash=token_hash, type="team", scopes=["read:own"],
                team_id=trial.team_id, created_by_user_id=user_id, issued_at=now,
            ))
            trial.batch_id = batch_id
            for ordinal, state, started in [(1, "pending", None), (2, "running", actual_start)]:
                await record_execution_event(
                    session, lease_id=lease.id, generation=lease.generation,
                    ordinal=ordinal, event_kind="kubernetes_observed",
                    payload={
                        "normalized_state": state, "job_uid": "job-progress",
                        "pod_uid": "pod-progress", "resource_version": str(ordinal),
                        "scheduled_at": (now + timedelta(seconds=2)).isoformat(),
                        "started_at": started.isoformat() if started else None,
                    },
                    observed_at=now + timedelta(seconds=10 * ordinal),
                )
                if state == "pending":
                    assert trial.state == "claimed" and trial.started_at is None
            await session.commit()
            assert trial.state == "running"
            assert trial.started_at == actual_start

        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://service",
            headers={"Authorization": f"Bearer {raw_token}"},
        ) as client:
            detail = await client.get(f"/api/v1/trials/{trial_id}")
            listing = await client.get("/api/v1/trials", params={"batch_id": str(batch_id)})
            batch = await client.get(f"/api/v1/batches/{batch_id}")
        for response in (detail, listing, batch):
            assert response.status_code == 200, response.text
        assert detail.json()["state"] == "running"
        assert datetime.fromisoformat(detail.json()["started_at"]) == actual_start
        listed = next(t for t in listing.json()["items"] if t["id"] == str(trial_id))
        assert listed["state"] == "running"
        assert listed["started_at"] == detail.json()["started_at"]
        assert batch.json()["service_execution_summary"]["lifecycle_stages"]["running"] == 1
    finally:
        async with sessions() as session:
            await session.execute(delete(Token).where(Token.token_hash == token_hash))
            await session.execute(delete(User).where(User.id == user_id))
            await session.commit()
        await engine.dispose()


@pytest.mark.parametrize("state", ["cancelled", "failed", "materializing"])
async def test_late_native_start_preserves_terminal_or_materializing_trial(
    postgres_url: str, state: str,
) -> None:
    engine = create_async_engine(postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    now = datetime.now(UTC)
    try:
        async with sessions() as session:
            trial_id, target = await fixtures._seed_ready_trial(session, now=now)
            lease = await fixtures._reserve(session, trial_id=trial_id, target=target, now=now)
            trial = await session.get(Trial, trial_id)
            assert trial is not None
            trial.state = state
            trial.finished_at = None if state == "materializing" else now
            await session.flush()
            await record_execution_event(
                session, lease_id=lease.id, generation=lease.generation,
                ordinal=1, event_kind="kubernetes_observed",
                payload={"normalized_state": "running", "started_at": now.isoformat()},
                observed_at=now + timedelta(seconds=1),
            )
            assert trial.state == state
            assert trial.finished_at == (None if state == "materializing" else now)
    finally:
        await engine.dispose()


async def test_native_start_replay_and_old_attempt_do_not_rewrite_progress(postgres_url: str) -> None:
    engine = create_async_engine(postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    now = datetime.now(UTC)
    try:
        async with sessions() as session:
            trial_id, target = await fixtures._seed_ready_trial(session, now=now)
            lease = await fixtures._reserve(session, trial_id=trial_id, target=target, now=now)
            trial = await session.get(Trial, trial_id)
            assert trial is not None
            event = dict(
                lease_id=lease.id, generation=lease.generation, ordinal=2,
                event_kind="kubernetes_observed",
                payload={"normalized_state": "running", "started_at": now.isoformat()},
                observed_at=now + timedelta(seconds=1),
            )
            await record_execution_event(session, **event)
            _, duplicate = await record_execution_event(session, **event)
            assert duplicate and trial.state == "running" and trial.started_at == now
            await record_execution_event(
                session, lease_id=lease.id, generation=lease.generation, ordinal=1,
                event_kind="kubernetes_observed",
                payload={"normalized_state": "pending"}, observed_at=now,
            )
            assert trial.state == "running" and trial.started_at == now
            trial.attempt_count += 1
            trial.state = "claimed"
            trial.started_at = None
            await record_execution_event(
                session, lease_id=lease.id, generation=lease.generation, ordinal=3,
                event_kind="kubernetes_observed",
                payload={"normalized_state": "running", "started_at": now.isoformat()},
                observed_at=now + timedelta(seconds=2),
            )
            assert trial.state == "claimed" and trial.started_at is None
    finally:
        await engine.dispose()
