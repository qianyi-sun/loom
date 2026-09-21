"""Exercise the real Postgres boundary between dispatch and an idle rollout."""
from datetime import UTC, datetime

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine

from loom.nebius_rollout_guard import acquire, admission_open, release
from loom_control_plane.execution_capacity import ExecutionProvisioningBlockedError
from loom_control_plane.task_image_materializations import claim_task_image_materialization
from tests.integration import test_service_execution_leases as execution


@pytest.mark.asyncio
async def test_idle_check_excludes_inflight_admission_and_persists_across_connections(isolated_migration_postgres_url):
    engine = create_async_engine(isolated_migration_postgres_url)
    try:
        async with AsyncSession(engine) as scheduler, AsyncSession(engine) as deploy:
            async with scheduler.begin():
                assert await admission_open(scheduler)
                async with deploy.begin():
                    result = await acquire(deploy, owner="test-rollout", candidate="a" * 40)
                    assert result == {"status": "skipped_busy", "reason": "admission_in_progress"}
            async with deploy.begin():
                assert (await acquire(deploy, owner="test-rollout", candidate="a" * 40))["status"] == "acquired"
                async with scheduler.begin():
                    assert not await admission_open(scheduler)
        # The deploy connection is gone; rollout or runner restarts cannot lift the pause.
        async with AsyncSession(engine) as session:
            async with session.begin():
                assert not await admission_open(session)
                assert await claim_task_image_materialization(session, builder_id="test", cpu_arch="x86_64") is None
                assert (await acquire(session, owner="other", candidate="b" * 40))["status"] == "skipped_locked"
            with pytest.raises(ValueError, match="owner"):
                async with session.begin():
                    await release(session, owner="other")
            async with session.begin():
                await release(session, owner="test-rollout")
            async with session.begin():
                assert await admission_open(session)
    finally:
        async with engine.begin() as connection:
            await connection.execute(text("DELETE FROM nebius_rollout_guard WHERE owner = 'test-rollout'"))
        await engine.dispose()


@pytest.mark.asyncio
async def test_queued_work_does_not_block_but_reservations_do(isolated_migration_postgres_url):
    engine = create_async_engine(isolated_migration_postgres_url)
    now = datetime.now(UTC)
    try:
        # Roll this fixture back; it must not leave active work in the shared DB.
        async with AsyncSession(engine) as session:
            trial_id, target = await execution._seed_ready_trial(session, now=now)
            assert (await acquire(session, owner="test-queued", candidate="a" * 40))["status"] == "acquired"
            with pytest.raises(ExecutionProvisioningBlockedError, match="platform_deploying"):
                await execution._reserve(session, trial_id=trial_id, target=target, now=now)
            await release(session, owner="test-queued")
            await execution._reserve(session, trial_id=trial_id, target=target, now=now)
            result = await acquire(session, owner="test-queued", candidate="a" * 40)
            assert result["status"] == "skipped_busy"
            assert result["active"]["executions"] == 1
            await session.rollback()
    finally:
        await engine.dispose()


def test_operator_cli_acquires_and_releases(isolated_migration_postgres_url, monkeypatch):
    import json
    import subprocess
    import sys

    monkeypatch.setenv("LOOM_CP_DB_URL", isolated_migration_postgres_url)
    monkeypatch.setenv("LOOM_CP_MINIO_ACCESS_KEY", "test-access")
    monkeypatch.setenv("LOOM_CP_MINIO_SECRET_KEY", "test-secret")
    command = [sys.executable, "-m", "loom.nebius_rollout_guard"]
    result = subprocess.run([*command, "acquire", "--owner", "cli-test", "--candidate", "a" * 40],
                            capture_output=True, text=True)
    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout)["status"] == "acquired"
    result = subprocess.run([*command, "release", "--owner", "cli-test"], capture_output=True, text=True)
    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout)["status"] == "released"
