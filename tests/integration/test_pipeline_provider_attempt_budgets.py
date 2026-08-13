from loom.pipeline.core_fixture import (
    PIPELINE_CORE_FIXTURE_IMAGE_REPOSITORY,
    build_pipeline_core_fixture_graph,
)
from loom.pipeline.spec import RecipeIdentityV1


def test_cpu_fixture_closes_provider_and_attempt_budgets() -> None:
    graph = build_pipeline_core_fixture_graph(
        RecipeIdentityV1(
            name="pipeline-core-fixture",
            version=1,
            digest="sha256:" + "1" * 64,
        ),
        {},
        image=PIPELINE_CORE_FIXTURE_IMAGE_REPOSITORY + "@sha256:" + "2" * 64,
    )
    assert graph.budget.max_provider_cost_usd == "0"
    assert graph.budget.max_gpu_seconds == 0
    assert graph.budget.max_stage_runs == 7
    assert graph.budget.max_attempts_total == 21
    assert all(node.max_attempts == 3 for node in graph.nodes if node.node_kind == "container")
