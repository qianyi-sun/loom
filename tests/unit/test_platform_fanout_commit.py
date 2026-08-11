from __future__ import annotations

from uuid import UUID, uuid5

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
