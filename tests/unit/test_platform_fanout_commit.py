from __future__ import annotations

from uuid import UUID, uuid5

import pytest

from loom.pipeline.platform_fanout_commit import synthesize_fanout_manifest


def test_zero_one_many_fanout_is_sorted_and_platform_assigns_ids() -> None:
    namespace = UUID("83829df2-6321-42c3-986d-8903d50ab6b1")
    empty = synthesize_fanout_manifest([], namespace=namespace, item_binding_name="item")
    assert empty == {"schema_version": "loom.fanout-manifest.v1", "items": []}
    many = synthesize_fanout_manifest(
        [
            ("z", "out_z", "behavior.task-instance.v1"),
            ("a", "out_a", "behavior.task-instance.v1"),
        ],
        namespace=namespace,
        item_binding_name="item",
    )
    assert [item["shard_key"] for item in many["items"]] == ["a", "z"]
    assert many["items"][0]["artifact_bindings"][0]["artifact_id"] == str(
        uuid5(namespace, "a\x00out_a\x00behavior.task-instance.v1")
    )


def test_fanout_manifest_can_use_preallocated_commit_ids() -> None:
    namespace = UUID("12345678-1234-5678-1234-567812345678")
    artifact_id = UUID("aaaaaaaa-aaaa-4aaa-aaaa-aaaaaaaaaaaa")
    value = synthesize_fanout_manifest(
        [("slot-1", "item_1", "example.item.v1")],
        namespace=namespace,
        item_binding_name="item",
        artifact_ids_by_output={"item_1": artifact_id},
    )
    items = value["items"]
    assert isinstance(items, list)
    assert items[0]["artifact_bindings"][0]["artifact_id"] == str(artifact_id)

    with pytest.raises(ValueError, match="no preallocated Artifact"):
        synthesize_fanout_manifest(
            [("slot-1", "item_1", "example.item.v1")],
            namespace=namespace,
            item_binding_name="item",
            artifact_ids_by_output={},
        )
