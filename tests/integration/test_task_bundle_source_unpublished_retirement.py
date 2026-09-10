"""Unpublished image owners need bounded retirement independent of registry GC."""

import importlib
from datetime import timedelta

import pytest
from sqlalchemy import delete

from loom.db.schema import Task, TaskBundleSourceReference, TaskImageMaterialization
from loom.task_image_materialization import ensure_task_image_materializations
from loom_control_plane.task_image_materializations import (
    claim_task_image_materialization,
    fail_task_image_materialization,
)
from tests.integration.test_task_bundle_source_admission import _task
from tests.integration.test_task_bundle_source_admission import journal as journal
from tests.integration.test_task_bundle_source_journal import (
    NOW,
    _publish,
    _receipts,
    _spec,
    _upload,
)

INSTANT = NOW + timedelta(days=2)


async def _image(journal, tmp_path):
    spec = _spec(tmp_path)
    ticket = await _upload(journal, spec)
    await _receipts(journal, ticket)
    await _publish(journal, ticket)
    async with journal.begin() as session:
        image = (await ensure_task_image_materializations(session, task_row=_task(spec)))[0]
        image_id = image.id
    return spec, image_id


async def _observe(journal, image_id, now=INSTANT):
    module = importlib.import_module("loom_control_plane.task_image_materializations")
    return await module.observe_unpublished_task_image_retirement(
        journal.kw["bind"], materialization_id=image_id, now=now,
        grace=timedelta(hours=24),
    )


@pytest.mark.parametrize("failed", [False, True])
async def test_never_pushed_image_retires_after_observed_grace(journal, tmp_path, failed):
    spec, image_id = await _image(journal, tmp_path)
    if failed:
        async with journal.begin() as session:
            image = await claim_task_image_materialization(
                session, builder_id="builder", cpu_arch="x86_64",
            )
            await fail_task_image_materialization(
                session, materialization_id=image_id, builder_id="builder",
                lease_epoch=image.lease_epoch, retryable=False,
                failure_reason="build_failed", failure_message="no publication", registry_images={},
            )
    assert await _observe(journal, image_id) == "observing"
    assert await _observe(journal, image_id, INSTANT + timedelta(hours=23)) == "observing"
    async with journal() as session:
        before = await session.get(TaskImageMaterialization, image_id)
        epoch = before.lease_epoch
        assert await session.get(TaskBundleSourceReference, (
            spec.id, "materialization", str(image_id),
        )) is not None
    assert await _observe(journal, image_id, INSTANT + timedelta(hours=24)) == "retired"
    assert await _observe(journal, image_id, INSTANT + timedelta(hours=25)) == "retired"
    async with journal() as session:
        image = await session.get(TaskImageMaterialization, image_id)
        assert image.state == "retired" and image.lease_epoch == epoch + 1
        assert await session.get(TaskBundleSourceReference, (
            spec.id, "materialization", str(image_id),
        )) is None
        assert await session.get(TaskBundleSourceReference, (spec.id, "catalog", "catalog"))


async def test_reference_resets_unpublished_retirement_grace(journal, tmp_path):
    spec, image_id = await _image(journal, tmp_path)
    assert await _observe(journal, image_id) == "observing"
    async with journal.begin() as session:
        task = _task(spec)
        session.add(task)
        await ensure_task_image_materializations(session, task_row=task)
    assert await _observe(journal, image_id, INSTANT + timedelta(hours=25)) == "pinned"
    async with journal.begin() as session:
        await session.execute(delete(Task).where(Task.id == spec.catalog_task_id))
    assert await _observe(journal, image_id, INSTANT + timedelta(hours=26)) == "observing"
    assert await _observe(journal, image_id, INSTANT + timedelta(hours=49)) == "observing"
    assert await _observe(journal, image_id, INSTANT + timedelta(hours=50)) == "retired"


@pytest.mark.parametrize("state", ["claimed", "running", "ready", "retiring"])
async def test_other_image_lifecycles_cannot_retire_as_unpublished(journal, tmp_path, state):
    spec, image_id = await _image(journal, tmp_path)
    async with journal.begin() as session:
        image = await session.get(TaskImageMaterialization, image_id)
        image.state = state
        image.unreferenced_at = INSTANT - timedelta(days=3)
    assert await _observe(journal, image_id) == "ineligible"
    async with journal() as session:
        assert (await session.get(TaskImageMaterialization, image_id)).state == state
        assert await session.get(TaskBundleSourceReference, (
            spec.id, "materialization", str(image_id),
        )) is not None


@pytest.mark.parametrize("field", ["registry_images", "registry_image_history"])
async def test_recorded_publication_stays_with_registry_cleanup(journal, tmp_path, field):
    spec, image_id = await _image(journal, tmp_path)
    async with journal.begin() as session:
        image = await session.get(TaskImageMaterialization, image_id)
        image.state = "failed"
        image.unreferenced_at = INSTANT - timedelta(days=3)
        setattr(image, field, {"task": "registry/image@sha256:" + "a" * 64}
                if field == "registry_images" else [{"component": "task"}])
    assert await _observe(journal, image_id) == "ineligible"
    async with journal() as session:
        assert await session.get(TaskBundleSourceReference, (
            spec.id, "materialization", str(image_id),
        )) is not None
