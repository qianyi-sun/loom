"""Verified producer bytes and out-of-prefix manifests preserve Phase 1 reads."""

import pytest

from loom.task_image_bundle_manifest import capture_task_image_bundle_manifest
from loom.trajectory.storage import BUNDLE_FILE_METADATA_NAME, FakeObjectStore
from loom_benchmark_tool.upload import upload_task_dir


def _source(tmp_path):
    root = tmp_path / "source"
    root.mkdir()
    (root / "a.txt").write_bytes(b"hello")
    (root / "run.sh").write_bytes(b"#!/bin/sh\n")
    (root / "run.sh").chmod(0o755)
    return root, capture_task_image_bundle_manifest(root)


async def test_upload_publishes_exact_manifest_outside_legacy_prefix(tmp_path):
    from loom.task_image_bundle_manifest import task_image_bundle_manifest_key

    root, manifest = _source(tmp_path)
    store = FakeObjectStore()
    prefix = f"bench/revision/{manifest.digest}/"
    count = await upload_task_dir(store=store, bucket="loom-bundles", prefix=prefix, task_dir=root, content_manifest=manifest)
    assert count == 2
    assert store.objects[("loom-bundles", task_image_bundle_manifest_key(manifest.digest))] == manifest.canonical_bytes
    assert store.objects[("loom-bundles", prefix + BUNDLE_FILE_METADATA_NAME)] == manifest.mode_metadata_bytes
    assert await store.download_prefix(bucket="loom-bundles", prefix=prefix, out_dir=tmp_path / "downloaded") == 2
    assert capture_task_image_bundle_manifest(tmp_path / "downloaded") == manifest


@pytest.mark.parametrize("change", ["content", "mode", "symlink", "extra", "unbound_prefix"])
async def test_upload_refuses_changed_source_before_publishing_anything(tmp_path, change):
    root, manifest = _source(tmp_path)
    prefix = f"bench/revision/{manifest.digest}/"
    if change == "content":
        (root / "a.txt").write_bytes(b"other")
    elif change == "mode":
        (root / "run.sh").chmod(0o644)
    elif change == "symlink":
        (root / "a.txt").unlink()
        (root / "a.txt").symlink_to(root / "run.sh")
    elif change == "extra":
        (root / "extra").write_bytes(b"extra")
    else:
        prefix = "bench/legacy-prefix/"
    store = FakeObjectStore()
    with pytest.raises(ValueError):
        await upload_task_dir(store=store, bucket="loom-bundles", prefix=prefix, task_dir=root, content_manifest=manifest)
    assert not store.objects


async def test_upload_rechecks_bytes_after_await_and_never_publishes_manifest_on_drift(tmp_path):
    from loom.task_image_bundle_manifest import task_image_bundle_manifest_key

    root, manifest = _source(tmp_path)
    prefix = f"bench/revision/{manifest.digest}/"

    class MutatingStore(FakeObjectStore):
        async def put_object(self, *, bucket, key, body):
            await super().put_object(bucket=bucket, key=key, body=body)
            (root / "run.sh").write_bytes(b"changed during upload")

    store = MutatingStore()
    with pytest.raises(ValueError):
        await upload_task_dir(store=store, bucket="loom-bundles", prefix=prefix, task_dir=root, content_manifest=manifest)
    assert ("loom-bundles", prefix + "a.txt") in store.objects
    assert ("loom-bundles", prefix + "run.sh") not in store.objects
    assert ("loom-bundles", task_image_bundle_manifest_key(manifest.digest)) not in store.objects
