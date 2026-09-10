"""Build and execution admission must retain the exact registered task inputs."""

from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
from sqlalchemy import select

from loom.db.schema import (
    TaskBundleSourceReference,
    TaskImageMaterialization,
    Team,
    Trial,
    TrialTaskImageMaterialization,
)
from loom.task_image_materialization import (
    ensure_task_image_materializations,
    get_trial_task_image_execution_grant,
)
from loom_control_plane.task_image_materializations import (
    claim_task_image_materialization,
    start_task_image_materialization,
)
from tests.integration.test_task_bundle_source_admission import _task
from tests.integration.test_task_bundle_source_admission import journal as journal
from tests.integration.test_task_bundle_source_journal import (
    NOW,
    _module,
    _publish,
    _receipts,
    _spec,
    _upload,
)


@pytest.mark.parametrize("operation", ["claim", "start", "execution-grant"])
@pytest.mark.parametrize("retired", [False, True])
async def test_build_and_execution_admission_require_available_pinned_source(
    journal, tmp_path, operation, retired,
):
    spec = _spec(tmp_path)
    ticket = await _upload(journal, spec)
    await _receipts(journal, ticket)
    await _publish(journal, ticket)
    expected_state = {"claim": "queued", "start": "claimed", "execution-grant": "ready"}[operation]
    trial_id = uuid4()
    async with journal.begin() as session:
        task = _task(spec)
        session.add(task)
        image = (await ensure_task_image_materializations(session, task_row=task))[0]
        image_id = image.id
        image.state = expected_state
        if operation == "start":
            image.claimed_by = "source-admission-test"
            image.lease_epoch = 1
            image.lease_expires_at = datetime.now(UTC) + timedelta(minutes=5)
        if operation == "execution-grant":
            image.registry_images = {"task": "registry.example/task@sha256:" + "a" * 64}
            team = Team(id=uuid4(), name="source-execution-" + uuid4().hex)
            session.add(team)
            await session.flush()
            session.add(Trial(
                id=trial_id, team_id=team.id, task_id=task.id, config={}, requires_caps={},
                state="queued", submitted_at=NOW,
            ))
            await session.flush()
            session.add(TrialTaskImageMaterialization(
                trial_id=trial_id, materialization_id=image_id,
            ))
    async with journal.begin() as session:
        # Model an owning lifecycle release; admission must not trust a digest
        # or stale pin without rechecking the serialized source state.
        await session.scalar(select(TaskImageMaterialization).where(
            TaskImageMaterialization.id == image_id,
        ).with_for_update())
        await _module().release_task_bundle_reference(
            session, source_id=spec.id, reference_kind="materialization", owner_id=str(image_id),
        )
        if retired:
            await _module().release_task_bundle_reference(
                session, source_id=spec.id, reference_kind="catalog", owner_id="catalog",
            )
            assert await _module().retire_task_bundle_source(
                session, incarnation_id=ticket.incarnation_id, now=NOW,
            )

    async def admit(session):
        if operation == "claim":
            return await claim_task_image_materialization(
                session, builder_id="source-admission-test", cpu_arch="x86_64",
            )
        if operation == "start":
            return await start_task_image_materialization(
                session, materialization_id=image_id, builder_id="source-admission-test",
                lease_epoch=1,
            )
        return await get_trial_task_image_execution_grant(
            session, trial_id=trial_id, cpu_arches=["x86_64"],
        )

    async with journal() as session:
        if retired:
            with pytest.raises((ValueError, RuntimeError), match="available"):
                await admit(session)
            await session.rollback()
        else:
            assert await admit(session) is not None
            await session.commit()
    async with journal() as session:
        if retired:
            assert (await session.get(TaskImageMaterialization, image_id)).state == expected_state
        else:
            assert await session.get(TaskBundleSourceReference, (
                spec.id, "materialization", str(image_id),
            )) is not None
