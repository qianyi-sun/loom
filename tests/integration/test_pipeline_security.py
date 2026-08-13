from loom.pipeline.core_fixture import (
    PIPELINE_CORE_FIXTURE_IMAGE_REPOSITORY,
    build_pipeline_core_fixture_graph,
)
from loom.pipeline.spec import RecipeIdentityV1

TEST_IMAGE = PIPELINE_CORE_FIXTURE_IMAGE_REPOSITORY + "@sha256:" + "2" * 64


def test_core_fixture_is_digest_pinned_network_closed_and_gpu_free() -> None:
    graph = build_pipeline_core_fixture_graph(
        RecipeIdentityV1(
            name="pipeline-core-fixture",
            version=1,
            digest="sha256:" + "1" * 64,
        ),
        {},
        image=TEST_IMAGE,
    )
    containers = [node for node in graph.nodes if node.node_kind == "container"]
    assert "@sha256:" in TEST_IMAGE
    assert containers
    assert all(node.image == TEST_IMAGE for node in containers)
    assert all(node.network_profile == "none" for node in containers)
    assert graph.budget.max_gpu_seconds == 0
    assert not graph.inputs and not graph.parameters
