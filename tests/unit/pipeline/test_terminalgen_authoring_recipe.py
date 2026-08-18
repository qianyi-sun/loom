from __future__ import annotations

from loom.integrations.terminalgen.contracts import AuthoringImageLockV1
from loom.integrations.terminalgen.recipe import (
    TerminalGenRendererLocksV1,
    build_terminalgen_authoring_graph,
)
from loom.pipeline.spec import RecipeIdentityV1, declared_stage_run_upper_bound

DIGEST = "sha256:" + "a" * 64
IMAGE = "registry.example.invalid/loom/terminalgen@sha256:" + "b" * 64


def _graph(*, slots_per_card: int = 500):
    return build_terminalgen_authoring_graph(
        RecipeIdentityV1(name="terminalgen-authoring", version=1, digest=DIGEST),
        {
            "slots_per_card": slots_per_card,
            "difficulty": "mixed",
            "random_seed": 7,
            "dynamic_validation_repetitions": 2,
            "package_format": "tar.zst",
        },
        images=AuthoringImageLockV1(
            schema_version="terminalgen.image-lock.v1",
            planner=IMAGE,
            generator=IMAGE,
            static_validator=IMAGE,
            dynamic_validator=IMAGE,
            task_base=IMAGE,
            dependency_resolver=IMAGE,
            packager=IMAGE,
        ),
        renderers=TerminalGenRendererLocksV1(
            plan_audit=DIGEST,
            card_finalize=DIGEST,
            global_finalize=DIGEST,
            authoring_package=DIGEST,
            runtime_package=DIGEST,
        ),
    )


def test_complete_graph_fits_static_node_and_stage_run_bounds() -> None:
    graph = _graph()
    by_key = {node.node_key: node for node in graph.nodes}

    assert len(graph.nodes) == 116
    assert declared_stage_run_upper_bound(graph) == 27_062
    assert len(by_key) == 116
    assert len([key for key in by_key if key.startswith("generate_card_")]) == 18
    assert len([key for key in by_key if key.startswith("validate_card_")]) == 18
    assert graph.budget.max_stage_runs == 50_000
    assert by_key["publish_boundary"].needs == ["package_authoring", "package_runtime"]


def test_each_card_is_an_independent_bounded_fanout_and_validation_lineage() -> None:
    graph = _graph()
    by_key = {node.node_key: node for node in graph.nodes}

    for ordinal in range(18):
        suffix = f"{ordinal:02d}"
        plan = by_key[f"plan_card_{suffix}"]
        generate = by_key[f"generate_card_{suffix}"]
        validate = by_key[f"validate_card_{suffix}"]
        finalize = by_key[f"finalize_card_{suffix}"]
        assert plan.fanout_commit is not None
        assert plan.fanout_commit.max_items == 500
        assert generate.fanout is not None and generate.fanout.max_items == 500
        assert validate.fanout is not None and validate.fanout.max_items == 500
        assert generate.network_profile == "gateway"
        assert validate.network_profile == "none"
        assert finalize.fanout is None


def test_graph_factory_rejects_local_concurrency_and_scales_bound_with_test_quota() -> None:
    graph = _graph(slots_per_card=7)
    assert declared_stage_run_upper_bound(graph) == 440

    parameters = graph.parameters | {"workers": 150}
    try:
        build_terminalgen_authoring_graph(
            graph.recipe,
            parameters,
            images=AuthoringImageLockV1(
                schema_version="terminalgen.image-lock.v1",
                planner=IMAGE,
                generator=IMAGE,
                static_validator=IMAGE,
                dynamic_validator=IMAGE,
                task_base=IMAGE,
                dependency_resolver=IMAGE,
                packager=IMAGE,
            ),
            renderers=TerminalGenRendererLocksV1(
                plan_audit=DIGEST,
                card_finalize=DIGEST,
                global_finalize=DIGEST,
                authoring_package=DIGEST,
                runtime_package=DIGEST,
            ),
        )
    except ValueError as exc:
        assert "Extra inputs are not permitted" in str(exc)
    else:  # pragma: no cover - an accidental permissive schema is a contract break
        raise AssertionError("workers bypass was accepted")
