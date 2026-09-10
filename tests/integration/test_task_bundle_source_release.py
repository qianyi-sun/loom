"""Image cleanup releases only the input pins whose owners actually retire."""

import pytest

from loom.db.schema import TaskBundleSourceReference
from loom.task_image_materialization import ensure_task_image_materializations
from loom_control_plane.task_image_materializations import (
    claim_task_image_registry_gc,
    complete_task_image_registry_gc,
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


@pytest.mark.parametrize("raced_reference", [False, True])
async def test_completed_image_gc_releases_only_retired_image_source_pin(
    journal, tmp_path, raced_reference,
):
    spec = _spec(tmp_path)
    ticket = await _upload(journal, spec)
    await _receipts(journal, ticket)
    await _publish(journal, ticket)
    async with journal.begin() as session:
        image = (await ensure_task_image_materializations(session, task_row=_task(spec)))[0]
        image.state = "ready"
        image.registry_images = {"task": "registry.example/task@sha256:" + "a" * 64}
        image_id = image.id
    async with journal.begin() as session:
        image = await claim_task_image_registry_gc(session, gc_id="gc", grace_hours=0)
        assert image.id == image_id and image.state == "retiring"
        epoch = image.lease_epoch
    async with journal.begin() as session:
        assert await session.get(TaskBundleSourceReference, (
            spec.id, "materialization", str(image_id),
        )) is not None
        if raced_reference:
            session.add(_task(spec))
    async with journal.begin() as session:
        image = await complete_task_image_registry_gc(
            session, materialization_id=image_id, gc_id="gc", lease_epoch=epoch,
        )
        assert image.state == ("queued" if raced_reference else "retired")
    async with journal.begin() as session:
        retained = await session.get(TaskBundleSourceReference, (
            spec.id, "materialization", str(image_id),
        ))
        assert (retained is not None) is raced_reference
        # Catalog ownership is separate from the image being deleted.
        assert await session.get(TaskBundleSourceReference, (spec.id, "catalog", "catalog"))
        await _module().release_task_bundle_reference(
            session, source_id=spec.id, reference_kind="catalog", owner_id="catalog",
        )
        assert await _module().retire_task_bundle_source(
            session, incarnation_id=ticket.incarnation_id, now=NOW,
        ) is (not raced_reference)
