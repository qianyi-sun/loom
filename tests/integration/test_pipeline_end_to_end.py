from pathlib import Path

import pytest

from loom.pipeline.core_acceptance import run_fault_scenario
from loom.pipeline.core_fixture import (
    PIPELINE_CORE_FIXTURE_IMAGE,
    PIPELINE_CORE_FIXTURE_IMAGE_REPOSITORY,
    build_pipeline_core_fixture_graph,
    builtin_pipeline_core_fixture_registry,
)
from loom.pipeline.spec import RecipeIdentityV1
from loom.pipeline.state import PipelineRunResult

TEST_IMAGE = PIPELINE_CORE_FIXTURE_IMAGE_REPOSITORY + "@sha256:" + "2" * 64


def _graph():
    return build_pipeline_core_fixture_graph(
        RecipeIdentityV1(
            name="pipeline-core-fixture",
            version=1,
            digest="sha256:" + "1" * 64,
        ),
        {},
        image=TEST_IMAGE,
    )


def test_cpu_fixture_graph_executes_two_item_success_program() -> None:
    graph = _graph()
    assert [node.node_key for node in graph.nodes] == [
        "seed_set",
        "produce_index",
        "transform",
        "aggregate",
        "outcome_gate",
        "local_artifact_readback",
    ]
    assert all(getattr(node, "image", TEST_IMAGE) == TEST_IMAGE for node in graph.nodes)
    assert graph.budget.max_gpu_seconds == 0
    assert {getattr(node, "resource_profile", None) for node in graph.nodes} <= {
        None,
        "pipeline-test-cpu-none@1",
    }
    produce = graph.nodes[1]
    assert produce.fanout_commit is not None
    assert produce.fanout_commit.max_items == 2
    transform = graph.nodes[2]
    assert transform.fanout is not None
    assert transform.fanout.source == "stage_output"
    assert graph.nodes[-1].network_profile == "none"
    actual = run_fault_scenario(1)
    assert actual.result is PipelineRunResult.SUCCEEDED
    assert actual.stages.total == 7
    assert actual.attempts.total == 6
    assert actual.committed_artifacts[-1] == "transform-001"


def test_published_fixture_rejects_parameters_and_is_publicly_registered() -> None:
    identity = RecipeIdentityV1(
        name="pipeline-core-fixture",
        version=1,
        digest="sha256:" + "1" * 64,
    )
    with pytest.raises(ValueError, match="accepts no parameters"):
        build_pipeline_core_fixture_graph(identity, {"image": "override"}, image=TEST_IMAGE)
    registry = builtin_pipeline_core_fixture_registry(repo_root=Path.cwd())
    graph = registry.resolve_ordinary("pipeline-core-fixture", 1, {})
    containers = [node for node in graph.nodes if node.node_kind == "container"]
    assert containers
    assert {node.image for node in containers} == {PIPELINE_CORE_FIXTURE_IMAGE}
    with pytest.raises(KeyError, match="unknown official Recipe"):
        registry.resolve_ordinary("caller-graph", 1, {})
