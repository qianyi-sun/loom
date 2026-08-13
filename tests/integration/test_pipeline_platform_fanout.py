from loom.pipeline.core_fixture import (
    PIPELINE_CORE_FIXTURE_IMAGE_REPOSITORY,
    build_pipeline_core_fixture_graph,
)
from loom.pipeline.spec import RecipeIdentityV1


def test_fixture_uses_platform_owned_bounded_fanout() -> None:
    graph = build_pipeline_core_fixture_graph(
        RecipeIdentityV1(
            name="pipeline-core-fixture",
            version=1,
            digest="sha256:" + "1" * 64,
        ),
        {},
        image=PIPELINE_CORE_FIXTURE_IMAGE_REPOSITORY + "@sha256:" + "2" * 64,
    )
    nodes = {node.node_key: node for node in graph.nodes}
    produce = nodes["produce_index"]
    transform = nodes["transform"]
    assert produce.fanout_commit is not None
    assert produce.fanout_commit.max_items == 2
    manifest = next(output for output in produce.outputs if output.name == "manifest")
    assert (manifest.producer, manifest.role) == ("platform", "fanout_manifest")
    assert transform.fanout is not None
    assert transform.fanout.source == "stage_output"
    assert transform.fanout.manifest_stage_key == "produce_index"
