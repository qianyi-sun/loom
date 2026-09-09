from datetime import UTC, datetime
from types import SimpleNamespace
from uuid import uuid4

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine

from loom.db.schema import Task, Team, Trial, TrialTaskImageMaterialization
from loom.task_image_materialization import ensure_task_image_materializations
from loom_control_plane.task_image_materializations import _durable_reference_exists
from loom_task_image_authority.retirement_store import _pins
from tests.integration.test_task_image_materialization_store import _task_values

KEY = "bundle_content_manifest_sha256"


async def test_ensure_versions_strong_rows_and_preserves_exact_historical_trial_pins(
    isolated_migration_postgres_url,
):
    engine = create_async_engine(isolated_migration_postgres_url)
    try:
        async with AsyncSession(engine, expire_on_commit=False) as session:
            task = Task(**_task_values(task_id="manifest-store", checksum="a" * 64))
            session.add(task)
            await session.flush()
            legacy = await ensure_task_image_materializations(session, task_row=task)
            await session.commit()
            for row in legacy:
                row.state = "ready"
                row.registry_images = {"task": "registry.example/task@sha256:" + "d" * 64}
            await session.commit()
            task.source_provenance = {**task.source_provenance, KEY: "b" * 64}
            first = await ensure_task_image_materializations(session, task_row=task)
            repeated = await ensure_task_image_materializations(session, task_row=task)
            await session.commit()
            assert [row.id for row in repeated] == [row.id for row in first]
            assert all(
                row.state == "queued" and row.bundle_content_manifest_sha256 == "b" * 64
                for row in first
            )
            assert not {row.id for row in legacy} & {row.id for row in first}
            task.source_provenance = {**task.source_provenance, KEY: "c" * 64}
            second = await ensure_task_image_materializations(session, task_row=task)
            await session.commit()
            assert len({row.materialization_key for row in (*legacy, *first, *second)}) == 6
            # Catalog retention follows the full identity, not the old checksum.
            for rows, expected in ((legacy, False), (first, False), (second, True)):
                for row in rows:
                    assert (
                        bool(await session.scalar(select(_durable_reference_exists(row))))
                        is expected
                    )
                    pins = await _pins(
                        session,
                        row,
                        SimpleNamespace(),
                        None,
                        current_ready=True,
                        now=datetime.now(UTC),
                    )
                    assert ("current_task" in pins) is expected
            team = Team(id=uuid4(), name="manifest-reference")
            session.add(team)
            await session.flush()
            trial = Trial(
                id=uuid4(),
                team_id=team.id,
                task_id=task.id,
                config={},
                requires_caps={},
                state="queued",
            )
            session.add(trial)
            await session.flush()
            session.add(
                TrialTaskImageMaterialization(trial_id=trial.id, materialization_id=legacy[0].id)
            )
            await session.commit()
            assert await session.scalar(select(_durable_reference_exists(legacy[0])))
            assert not await session.scalar(select(_durable_reference_exists(legacy[1])))
            assert legacy[0].state == "ready"
    finally:
        await engine.dispose()


@pytest.mark.parametrize("changed", ["config", "source", "provenance"])
async def test_strong_ensure_rejects_different_frozen_snapshot(
    isolated_migration_postgres_url, changed
):
    engine = create_async_engine(isolated_migration_postgres_url)
    try:
        async with AsyncSession(engine, expire_on_commit=False) as session:
            task = Task(**_task_values(task_id="manifest-store", checksum="a" * 64))
            task.source_provenance = {KEY: "b" * 64}
            original = await ensure_task_image_materializations(session, task_row=task)
            await session.commit()
            if changed == "config":
                task.config = {**task.config, "task": {**task.config["task"], "name": "changed"}}
            elif changed == "source":
                task.source = "s3://another-bucket/another-prefix/"
            else:
                task.source_provenance = {**task.source_provenance, "extra": "changed"}
            with pytest.raises(ValueError, match="frozen.*snapshot"):
                await ensure_task_image_materializations(session, task_row=task)
            assert all(row.task_config["task"]["name"] == "manifest-store" for row in original)
    finally:
        await engine.dispose()
