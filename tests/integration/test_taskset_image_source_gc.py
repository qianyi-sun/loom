"""Generated inputs remain available for frozen task-image consumers."""

from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
from sqlalchemy import delete

from loom.db.schema import (
    Task,
    TaskImageMaterialization,
    TaskSet,
    TaskSetMaterializationJob,
    Trial,
    TrialTaskImageMaterialization,
)
from loom_service import taskset_gc
from tests.integration.test_taskset_materialization import (
    _object_keys,
    materialization_minio,  # noqa: F401
    materialization_setup,  # noqa: F401
)


@pytest.mark.parametrize("reference", ["current_version", "live_trial", "ready_cache", "active_build"])
@pytest.mark.parametrize("late_reference", [False, True], ids=["initial-snapshot", "final-recheck"])
async def test_generation_gc_keeps_image_sources_until_references_and_cache_retire(
    materialization_setup, monkeypatch: pytest.MonkeyPatch,  # noqa: F811
    reference: str, late_reference: bool,
) -> None:
    app, _tokens, teams = materialization_setup
    team_id = teams["team_a"]
    job_id, image_id, trial_id = uuid4(), uuid4(), uuid4()
    slug = "image-gc"
    task_set_id = f"ts/{team_id}/{slug}"
    task_id = f"{task_set_id}/tasks/example"
    prefix = f"tasksets/user/{team_id}/{slug}/materializations/{job_id}/1/"
    source = f"s3://{app.state.settings.artifacts_bucket}/{prefix}tasks/example/"
    old_key = f"{prefix}tasks/example/instruction.md"
    app.state.minio_client.put_object(
        Bucket=app.state.settings.artifacts_bucket, Key=old_key, Body=b"frozen input",
    )
    try:
        async with app.state.session_factory() as session:
            session.add(TaskSet(
                id=task_set_id, owning_team_id=team_id, slug=slug, display_name="Image GC",
                visibility="private", status="ready", intents=["trajectory_generation"],
                manifest_blob_uri="s3://artifacts/image-gc/manifest.yaml",
            ))
            await session.flush()
            session.add(TaskSetMaterializationJob(
                id=job_id, task_set_id=task_set_id, owning_team_id=team_id,
                state="succeeded", lease_epoch=1,
            ))
            session.add(Task(
                id=task_id, task_set_id=task_set_id, config={},
                checksum="1" * 64 if reference == "current_version" else "2" * 64,
                source=f"s3://artifacts/tasksets/user/{team_id}/{slug}/new-generation/",
            ))
            session.add(TaskImageMaterialization(
                id=image_id, task_id=task_id, materialization_key="a" * 64,
                task_checksum="1" * 64, cpu_arch="x86_64", task_config={},
                task_source=source,
                state=("ready" if reference == "ready_cache" else
                       "running" if reference == "active_build" else "queued"),
                lease_expires_at=datetime.now(UTC) + timedelta(minutes=5),
            ))
            await session.flush()
            if reference == "live_trial":
                session.add(Trial(
                    id=trial_id, team_id=team_id, task_id=task_id,
                    state="queued", config={}, requires_caps={},
                ))
                await session.flush()
                session.add(TrialTaskImageMaterialization(
                    trial_id=trial_id, materialization_id=image_id,
                ))
            await session.commit()

        if late_reference:
            original = taskset_gc._protected_task_image_sources
            calls = 0

            async def miss_initial_snapshot(*args, **kwargs):
                nonlocal calls
                calls += 1
                if calls == 1:
                    return []
                return await original(*args, **kwargs)

            monkeypatch.setattr(taskset_gc, "_protected_task_image_sources", miss_initial_snapshot)

        async with app.state.session_factory() as session:
            result = await taskset_gc.purge_abandoned_materialization_generations(
                session, minio_client=app.state.minio_client,
                artifacts_bucket=app.state.settings.artifacts_bucket,
            )
        assert result.deleted_objects == 0
        assert result.protected_generations == 1
        assert old_key in _object_keys(app, prefix=prefix)

        async with app.state.session_factory() as session:
            task = await session.get(Task, task_id)
            image = await session.get(TaskImageMaterialization, image_id)
            assert task is not None and image is not None
            task.checksum = "3" * 64
            image.state = "retired"
            image.lease_expires_at = None
            trial = await session.get(Trial, trial_id)
            if trial is not None:
                trial.state = "failed"
            await session.commit()
            result = await taskset_gc.purge_abandoned_materialization_generations(
                session, minio_client=app.state.minio_client,
                artifacts_bucket=app.state.settings.artifacts_bucket,
            )
        assert result.deleted_objects == 1
        assert _object_keys(app, prefix=prefix) == set()
    finally:
        async with app.state.session_factory() as session:
            await session.execute(delete(Trial).where(Trial.id == trial_id))
            await session.execute(delete(TaskImageMaterialization).where(
                TaskImageMaterialization.id == image_id,
            ))
            await session.commit()
