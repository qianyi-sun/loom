"""Stable source inventory and bounded preparation work."""

import pytest

from loom.task_bundle_registration import prepare_task_bundle_registration
from loom.task_bundle_source import TaskBundleSourceSpecV1
from tests.unit.test_task_bundle_registration import _bundle


def test_source_reader_caches_only_immutable_inventory_not_authored_bytes(tmp_path, monkeypatch):
    task_dir = _bundle(tmp_path)
    spec = TaskBundleSourceSpecV1.from_registration(
        prepare_task_bundle_registration(task_dir, task_id="benchmark/task"), bucket="task-sources"
    )
    # Capture does not cache authored bytes: each read still verifies the descriptor.
    original = TaskBundleSourceSpecV1.service_manifest.fget
    calls = 0

    def count(self):
        nonlocal calls
        calls += 1
        return original(self)

    monkeypatch.setattr(TaskBundleSourceSpecV1, "service_manifest", property(count))
    expected = {
        item.object_key: spec.read_object(task_dir, item.object_key) for item in spec.objects
    }
    for key, body in expected.items():
        assert spec.read_object(task_dir, key) == body
    assert calls <= 1, "preparation rebuilds the whole manifest per authored file"
    with pytest.raises(TypeError):
        spec.transport_bodies["unexpected"] = b"not part of the inventory"
    (task_dir / "task.toml").write_text("drifted")
    with pytest.raises(ValueError):
        spec.read_object(task_dir, spec.data_prefix + "task.toml")


def test_source_namespace_keeps_benchmark_membership_scoped(tmp_path):
    from loom.task_bundle_source import task_bundle_catalog_prefix

    root = _bundle(tmp_path)
    specs = [
        TaskBundleSourceSpecV1.from_registration(
            prepare_task_bundle_registration(root, task_id=task_id), bucket="task-sources"
        )
        for task_id in ("first/a", "first/b", "second/a")
    ]
    prefix = task_bundle_catalog_prefix("first")
    assert all(spec.data_prefix.startswith(prefix) for spec in specs[:2])
    assert not specs[2].data_prefix.startswith(prefix)
    assert len({spec.source_uri for spec in specs}) == 3
