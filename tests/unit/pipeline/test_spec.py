from __future__ import annotations

from copy import deepcopy
from typing import Any
from uuid import UUID

import pytest
from pydantic import ValidationError

from loom.pipeline.keys import canonical_digest, canonical_document
from loom.pipeline.spec import (
    MAX_SAFE_INTEGER,
    CheckpointPolicyV1,
    ContainerNodeV1,
    FanoutArtifactBindingV1,
    FanoutManifestItemV1,
    FanoutManifestV1,
    GraphInputV1,
    PlatformFanoutIndexV1,
    ProviderAttemptLimitsV1,
    RecipeIdentityV1,
    RequestRendererLockV1,
    RunBudgetV1,
    RunGraphSpecV1,
    StageBudgetV1,
    StageOutputFanoutV1,
    declared_stage_run_upper_bound,
    validate_fanout_manifest,
    validate_shard_key,
)

DIGEST_0 = "sha256:" + "0" * 64
DIGEST_1 = "sha256:" + "1" * 64
IMAGE = "registry.example.com/loom/pipeline@sha256:" + "2" * 64


def output(
    name: str,
    artifact_type: str,
    *,
    required: bool = True,
    role: str = "artifact",
    producer: str = "container",
    max_bytes: int = 100,
) -> dict[str, Any]:
    return {
        "name": name,
        "artifact_type": artifact_type,
        "required": required,
        "role": role,
        "producer": producer,
        "max_bytes": max_bytes,
    }


def container(
    node_key: str,
    *,
    needs: list[str] | None = None,
    inputs: list[dict[str, Any]] | None = None,
    outputs: list[dict[str, Any]] | None = None,
    fanout: dict[str, Any] | None = None,
    fanout_commit: dict[str, Any] | None = None,
    checkpoint: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "node_kind": "container",
        "node_key": node_key,
        "image": IMAGE,
        "argv": ["python", "-m", f"pipeline.{node_key}"],
        "workdir": "/workspace",
        "resource_profile": "cpu_small@1",
        "network_profile": "none",
        "needs": needs or [],
        "inputs": inputs or [],
        "outputs": outputs or [],
        "request_renderer": None,
        "checkpoint": checkpoint,
        "fanout": fanout,
        "fanout_commit": fanout_commit,
        "timeout_seconds": 120,
        "max_attempts": 2,
        "failure_policy": "fail_run",
    }


def golden_graph_value() -> dict[str, Any]:
    prepare = container(
        "prepare",
        outputs=[
            output("fanout_index", "loom.platform-fanout-index.v1", max_bytes=2_048),
            output(
                "fanout_manifest",
                "loom.fanout-manifest.v1",
                role="fanout_manifest",
                producer="platform",
                max_bytes=16_384,
            ),
            output("case", "behavior.case.v1", required=False, max_bytes=4_096),
        ],
        fanout_commit={
            "index_output_name": "fanout_index",
            "manifest_output_name": "fanout_manifest",
            "items_pointer": "/items",
            "item_binding_name": "case",
            "max_items": 3,
        },
    )
    fanout = {
        "source": "stage_output",
        "manifest_stage_key": "prepare",
        "manifest_output_name": "fanout_manifest",
        "items_pointer": "/items",
        "shard_key_pointer": "/shard_key",
        "item_binding_name": "case",
        "item_artifact_type": "behavior.case.v1",
        "max_items": 3,
    }
    judge = container(
        "judge",
        needs=["prepare"],
        inputs=[
            {
                "source": "fanout_item",
                "binding_name": "current_case",
                "artifact_type": "behavior.case.v1",
            }
        ],
        outputs=[
            output("verdict", "behavior.verdict.v1"),
            output("diagnosis", "behavior.diagnosis.v1", required=False),
        ],
        fanout=fanout,
    )
    gate = {
        "node_kind": "gate",
        "gate_kind": "outcome",
        "node_key": "route_recovery",
        "shard_mode": "subject",
        "needs": ["judge"],
        "subject_stage_key": "judge",
        "match_outcomes": ["needs_recovery"],
        "matched_targets": ["recover"],
        "unmatched_targets": [],
    }
    recover = container(
        "recover",
        needs=["prepare", "judge", "route_recovery"],
        inputs=[
            {
                "source": "fanout_item",
                "binding_name": "current_case",
                "artifact_type": "behavior.case.v1",
            },
            {
                "source": "stage_output",
                "binding_name": "diagnosis",
                "artifact_type": "behavior.diagnosis.v1",
                "stage_key": "judge",
                "output_name": "diagnosis",
                "shard_selection": "same_shard",
                "match_outcomes": ["needs_recovery"],
            },
        ],
        outputs=[output("recovered", "behavior.recovered.v1")],
        fanout=fanout,
    )
    return {
        "schema_version": "loom.run-graph.v1",
        "recipe": {"name": "behavior-recovery", "version": 1, "digest": DIGEST_0},
        "inputs": [],
        "parameters": {"mode": "quick"},
        "budget": {
            "max_provider_cost_usd": "12.500000",
            "max_gpu_seconds": 0,
            "max_wall_seconds": 600,
            "max_artifact_bytes": 1_000_000,
            "max_stage_runs": 16,
            "max_attempts_total": 32,
        },
        "nodes": [prepare, judge, gate, recover],
    }


def test_golden_graph_round_trips_and_has_stable_digest() -> None:
    graph = RunGraphSpecV1.model_validate(golden_graph_value())

    assert graph.model_dump(mode="json", exclude_none=False) == golden_graph_value()
    assert canonical_document(graph) == canonical_document(golden_graph_value())
    assert canonical_digest(graph) == (
        "sha256:23a2f793354481c4c6ca89264495d9f4eb6ab560f0d8edc1e2e13968bb36acda"
    )
    assert declared_stage_run_upper_bound(graph) == 10


@pytest.mark.parametrize("model", [RecipeIdentityV1, GraphInputV1, RunBudgetV1])
def test_closed_models_reject_extra_fields(model: type[Any]) -> None:
    values = {
        RecipeIdentityV1: {"name": "recipe", "version": 1, "digest": DIGEST_0},
        GraphInputV1: {"name": "cases", "artifact_type": "behavior.cases.v1"},
        RunBudgetV1: golden_graph_value()["budget"],
    }[model]

    with pytest.raises(ValidationError, match="Extra inputs are not permitted"):
        model.model_validate({**values, "unexpected": True})


@pytest.mark.parametrize(
    ("model", "values", "missing"),
    [
        (RecipeIdentityV1, {"name": "recipe", "version": 1}, "digest"),
        (GraphInputV1, {"name": "cases"}, "artifact_type"),
        (
            RunBudgetV1,
            {
                key: value
                for key, value in golden_graph_value()["budget"].items()
                if key != "max_wall_seconds"
            },
            "max_wall_seconds",
        ),
    ],
)
def test_closed_models_reject_missing_fields(
    model: type[Any], values: dict[str, Any], missing: str
) -> None:
    with pytest.raises(ValidationError, match=missing):
        model.model_validate(values)


def test_strict_models_do_not_coerce_integer_or_boolean_fields() -> None:
    budget = golden_graph_value()["budget"]
    with pytest.raises(ValidationError):
        RunBudgetV1.model_validate({**budget, "max_wall_seconds": "600"})
    with pytest.raises(ValidationError):
        GraphInputV1.model_validate(
            {"name": "cases", "artifact_type": "behavior.cases.v1", "required": 1}
        )


def test_free_form_argv_strings_preserve_unicode_code_points() -> None:
    value = container("stage")
    value["argv"] = ["render", "e\u0301"]

    node = ContainerNodeV1.model_validate(value)

    assert node.argv == ["render", "e\u0301"]
    assert node.argv[1] != "\u00e9"


def test_graph_rejects_unknown_dependency_cycle_and_duplicate_binding() -> None:
    unknown = golden_graph_value()
    unknown["nodes"][3]["needs"].append("missing")
    with pytest.raises(ValidationError, match="unknown dependencies"):
        RunGraphSpecV1.model_validate(unknown)

    cyclic = golden_graph_value()
    cyclic["nodes"][0]["needs"] = ["recover"]
    with pytest.raises(ValidationError, match="cycle"):
        RunGraphSpecV1.model_validate(cyclic)

    duplicate = golden_graph_value()
    duplicate["nodes"][3]["inputs"].append(deepcopy(duplicate["nodes"][3]["inputs"][0]))
    with pytest.raises(ValidationError, match="input binding names must be unique"):
        RunGraphSpecV1.model_validate(duplicate)


def test_conditional_output_requires_exact_gate_contract() -> None:
    missing_outcomes = golden_graph_value()
    missing_outcomes["nodes"][3]["inputs"][1]["match_outcomes"] = None
    with pytest.raises(
        ValidationError, match="optional outputs require conditional scalar binding"
    ):
        RunGraphSpecV1.model_validate(missing_outcomes)

    wrong_gate = golden_graph_value()
    wrong_gate["nodes"][2]["match_outcomes"] = ["authored"]
    with pytest.raises(ValidationError, match="matching outcome gate"):
        RunGraphSpecV1.model_validate(wrong_gate)

    missing_gate_need = golden_graph_value()
    missing_gate_need["nodes"][3]["needs"].remove("route_recovery")
    with pytest.raises(ValidationError, match="every gate target must list the gate"):
        RunGraphSpecV1.model_validate(missing_gate_need)


def test_stage_output_fanout_rejects_manifest_or_item_contract_drift() -> None:
    manifest_drift = golden_graph_value()
    manifest_drift["nodes"][1]["fanout"]["manifest_output_name"] = "other_manifest"
    with pytest.raises(ValidationError, match="manifest name drift"):
        RunGraphSpecV1.model_validate(manifest_drift)

    item_drift = golden_graph_value()
    for node_index in (1, 3):
        item_drift["nodes"][node_index]["fanout"]["item_artifact_type"] = "behavior.other.v1"
        item_drift["nodes"][node_index]["inputs"][0]["artifact_type"] = "behavior.other.v1"
    with pytest.raises(ValidationError, match="item contract drift"):
        RunGraphSpecV1.model_validate(item_drift)


def test_fanout_index_requires_sorted_unique_safe_items() -> None:
    value = {
        "schema_version": "loom.platform-fanout-index.v1",
        "items": [
            {"shard_key": "case-001", "output_name": "item_001"},
            {"shard_key": "case-002", "output_name": "item_002"},
        ],
    }
    assert PlatformFanoutIndexV1.model_validate(value).model_dump(mode="json") == value

    with pytest.raises(ValidationError, match="bytewise sorted"):
        PlatformFanoutIndexV1.model_validate({**value, "items": list(reversed(value["items"]))})
    duplicated = deepcopy(value)
    duplicated["items"][1]["output_name"] = "item_001"
    with pytest.raises(ValidationError, match="output names must be unique"):
        PlatformFanoutIndexV1.model_validate(duplicated)


def test_fanout_manifest_is_closed_against_stage_output_contract() -> None:
    fanout = StageOutputFanoutV1.model_validate(golden_graph_value()["nodes"][1]["fanout"])
    manifest = FanoutManifestV1(
        schema_version="loom.fanout-manifest.v1",
        items=[
            FanoutManifestItemV1(
                artifact_bindings=[
                    FanoutArtifactBindingV1(
                        artifact_id=UUID("00000000-0000-0000-0000-000000000001"),
                        artifact_type="behavior.case.v1",
                        name="case",
                    )
                ],
                parameters={},
                shard_key="case-001",
            )
        ],
    )
    assert validate_fanout_manifest(manifest, fanout) is manifest

    parameter_drift = manifest.model_copy(
        update={"items": [manifest.items[0].model_copy(update={"parameters": {"x": 1}})]}
    )
    with pytest.raises(ValueError, match="parameters must be empty"):
        validate_fanout_manifest(parameter_drift, fanout)
    binding_drift = manifest.model_copy(
        update={
            "items": [
                manifest.items[0].model_copy(
                    update={
                        "artifact_bindings": [
                            manifest.items[0]
                            .artifact_bindings[0]
                            .model_copy(update={"name": "other"})
                        ]
                    }
                )
            ]
        }
    )
    with pytest.raises(ValueError, match="binding drift"):
        validate_fanout_manifest(binding_drift, fanout)


@pytest.mark.parametrize("value", ["", ".", "..", "singleton", "a/b", "a\\b", "a\x00b"])
def test_expanded_shard_keys_reject_reserved_or_path_values(value: str) -> None:
    with pytest.raises(ValueError, match="expanded shard_key is invalid"):
        validate_shard_key(value, allow_singleton=False)


def test_renderer_lock_requires_sorted_safe_unique_repo_paths() -> None:
    lock = {
        "name": "stage_request",
        "version": 1,
        "entrypoint": "loom.renderers.stage:render",
        "files": [
            {"repo_path": "loom/renderers/helper.py", "sha256": DIGEST_0},
            {"repo_path": "loom/renderers/schema.py", "sha256": DIGEST_1},
        ],
    }
    assert RequestRendererLockV1.model_validate(lock).model_dump(mode="json") == lock

    with pytest.raises(ValidationError, match="bytewise sorted"):
        RequestRendererLockV1.model_validate({**lock, "files": list(reversed(lock["files"]))})
    with pytest.raises(ValidationError, match="invalid component"):
        RequestRendererLockV1.model_validate(
            {**lock, "files": [{"repo_path": "loom/../secret.py", "sha256": DIGEST_0}]}
        )


def test_stage_budget_matches_attempt_reservation_ceiling() -> None:
    node = ContainerNodeV1.model_validate(
        container(
            "prepare",
            outputs=[
                output("fanout_index", "loom.platform-fanout-index.v1", max_bytes=2_000),
                output(
                    "fanout_manifest",
                    "loom.fanout-manifest.v1",
                    role="fanout_manifest",
                    producer="platform",
                    max_bytes=3_000,
                ),
                output("case", "behavior.case.v1", required=False, max_bytes=4_000),
            ],
            fanout_commit={
                "index_output_name": "fanout_index",
                "manifest_output_name": "fanout_manifest",
                "items_pointer": "/items",
                "item_binding_name": "case",
                "max_items": 3,
            },
            checkpoint={
                "max_bytes": 5_000,
                "min_interval_seconds": 5,
                "max_committed_per_attempt": 4,
            },
        )
    )
    provider = ProviderAttemptLimitsV1(
        provider_request_limit_per_attempt=8,
        provider_cost_limit_microusd_per_attempt=900_000,
        per_call_timeout_seconds=30,
    )

    zero_gpu = StageBudgetV1.for_node(node, gpu_count_exact=0, provider=provider)
    gpu = StageBudgetV1.for_node(node, gpu_count_exact=2, provider=provider)

    assert zero_gpu.gpu_seconds_limit == 0
    assert gpu.gpu_seconds_limit == 2 * (node.timeout_seconds + 35)
    assert gpu.final_output_bytes_limit == 2_000 + 3_000 + 3 * 4_000
    assert gpu.checkpoint_bytes_limit == 5_000
    assert gpu.timeout_seconds == node.timeout_seconds
    assert gpu.max_attempts == node.max_attempts
    assert gpu.provider == provider
    with pytest.raises(ValueError, match="cannot be negative"):
        StageBudgetV1.for_node(node, gpu_count_exact=-1)


def test_stage_budget_and_checkpoint_enforce_safe_positive_limits() -> None:
    with pytest.raises(ValidationError):
        CheckpointPolicyV1(
            max_bytes=0,
            min_interval_seconds=5,
            max_committed_per_attempt=1,
        )
    with pytest.raises(ValidationError):
        StageBudgetV1(
            provider=None,
            gpu_seconds_limit=MAX_SAFE_INTEGER + 1,
            final_output_bytes_limit=0,
            checkpoint_bytes_limit=0,
            timeout_seconds=1,
            max_attempts=1,
        )


def test_nested_stage_output_fanout_multiplies_the_source_cardinality() -> None:
    commit_outputs = [
        output("fanout_index", "loom.platform-fanout-index.v1"),
        output(
            "fanout_manifest",
            "loom.fanout-manifest.v1",
            role="fanout_manifest",
            producer="platform",
            max_bytes=16_384,
        ),
        output("item", "behavior.case.v1", required=False),
    ]
    first = container(
        "first",
        inputs=[
            {
                "source": "fanout_item",
                "binding_name": "item",
                "artifact_type": "behavior.case.v1",
            }
        ],
        outputs=commit_outputs,
        fanout={
            "source": "run_input",
            "manifest_input_name": "seed",
            "items_pointer": "/items",
            "shard_key_pointer": "/shard_key",
            "item_binding_name": "item",
            "item_artifact_type": "behavior.case.v1",
            "parameters_contract": {"name": "params", "version": 1, "digest": DIGEST_0},
            "max_items": 5_000,
        },
        fanout_commit={
            "index_output_name": "fanout_index",
            "manifest_output_name": "fanout_manifest",
            "items_pointer": "/items",
            "item_binding_name": "item",
            "max_items": 5_000,
        },
    )
    second = container(
        "second",
        needs=["first"],
        inputs=[
            {
                "source": "fanout_item",
                "binding_name": "item",
                "artifact_type": "behavior.case.v1",
            }
        ],
        fanout={
            "source": "stage_output",
            "manifest_stage_key": "first",
            "manifest_output_name": "fanout_manifest",
            "items_pointer": "/items",
            "shard_key_pointer": "/shard_key",
            "item_binding_name": "item",
            "item_artifact_type": "behavior.case.v1",
            "max_items": 5_000,
        },
    )
    value = {
        "schema_version": "loom.run-graph.v1",
        "recipe": {"name": "nested", "version": 1, "digest": DIGEST_0},
        "inputs": [
            {"name": "seed", "artifact_type": "loom.fanout-manifest.v1", "required": True}
        ],
        "parameters": {},
        "budget": golden_graph_value()["budget"],
        "nodes": [first, second],
    }

    with pytest.raises(ValidationError, match="exceeds 50,000"):
        RunGraphSpecV1.model_validate(value)
