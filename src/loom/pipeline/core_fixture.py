"""Official CPU-only generic Pipeline repository-acceptance fixture."""

from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path
from typing import Any

from loom.pipeline.keys import canonical_digest
from loom.pipeline.recipes import (
    ConditionalOutputContract,
    OfficialRecipeRegistration,
    OfficialRecipeRegistry,
)
from loom.pipeline.spec import (
    RecipeIdentityV1,
    RequestRendererLockFileV1,
    RequestRendererLockV1,
    RunGraphSpecV1,
)

PIPELINE_CORE_FIXTURE_IMAGE_REPOSITORY = "ghcr.io/qianyi-sun/loom-pipeline-core-fixture"
PIPELINE_CORE_FIXTURE_IMAGE_DIGEST = (
    "sha256:f207eb8a709584b29cb24d174327b3d7c89261896c9a985bb083a91a94c7fa0b"
)
PIPELINE_CORE_FIXTURE_IMAGE = (
    f"{PIPELINE_CORE_FIXTURE_IMAGE_REPOSITORY}@{PIPELINE_CORE_FIXTURE_IMAGE_DIGEST}"
)
PIPELINE_CORE_FIXTURE_IMAGE_SOURCE_COMMIT = "dcb570bfec15d5579ddc4e4cfe630bc52e0d5f5f"
_RENDERER_PATH = "src/loom/pipeline/renderers/core_fixture_aggregate.py"
_RENDERER_FILE_SHA256 = "sha256:e04d9caa726b3af290a1f532147611b45e6c7605afac55c6aa6d0c6c9dd16161"
_FIXTURE_MODULE_SHA256 = "sha256:0b23592f86d0128c95b8be3f8a648b356d1eddc7af7f2006c916e35cc849c932"
_FIXTURE_DOCKERFILE_SHA256 = (
    "sha256:8dcbda1d93499850a84bc4d6d4431d8472b7d488fbd7a5cbf369a139bdcad64a"
)


def _renderer_lock() -> RequestRendererLockV1:
    return RequestRendererLockV1(
        name="pipeline_core_aggregate",
        version=1,
        entrypoint="loom.pipeline.renderers.core_fixture_aggregate:render",
        files=[
            RequestRendererLockFileV1(
                repo_path=_RENDERER_PATH,
                sha256=_RENDERER_FILE_SHA256,
            )
        ],
    )


def _output(
    name: str,
    artifact_type: str,
    *,
    required: bool = True,
    producer: str = "container",
    role: str = "artifact",
    max_bytes: int = 1_048_576,
) -> dict[str, object]:
    return {
        "name": name,
        "artifact_type": artifact_type,
        "required": required,
        "role": role,
        "producer": producer,
        "max_bytes": max_bytes,
    }


def _container(
    node_key: str,
    *,
    image: str,
    needs: list[str],
    inputs: list[dict[str, object]],
    outputs: list[dict[str, object]],
    resource_profile: str = "pipeline-test-cpu-none@1",
    request_renderer: dict[str, object] | None = None,
    fanout: dict[str, object] | None = None,
    fanout_commit: dict[str, object] | None = None,
) -> dict[str, object]:
    return {
        "node_kind": "container",
        "node_key": node_key,
        "image": image,
        "argv": [node_key],
        "workdir": "/workspace",
        "resource_profile": resource_profile,
        "network_profile": "none" if resource_profile.endswith("none@1") else "gateway",
        "needs": needs,
        "inputs": inputs,
        "outputs": outputs,
        "request_renderer": request_renderer,
        "checkpoint": None,
        "fanout": fanout,
        "fanout_commit": fanout_commit,
        "timeout_seconds": 60,
        "max_attempts": 3,
        "failure_policy": "fail_run",
    }


def build_pipeline_core_fixture_graph(
    identity: RecipeIdentityV1,
    parameters: Mapping[str, Any],
    *,
    image: str,
) -> RunGraphSpecV1:
    """Return the fixed two-item fan-out/fan-in graph; no raw parameter surface exists."""

    if parameters:
        raise ValueError("pipeline-core-fixture@1 accepts no parameters")
    lock = _renderer_lock()
    renderer_digest = canonical_digest(lock)
    nodes: list[dict[str, object]] = [
        _container(
            "seed_set",
            image=image,
            needs=[],
            inputs=[],
            outputs=[_output("seed", "loom.pipeline-core-seed.v1")],
        ),
        _container(
            "produce_index",
            image=image,
            needs=["seed_set"],
            inputs=[
                {
                    "source": "stage_output",
                    "binding_name": "seed",
                    "artifact_type": "loom.pipeline-core-seed.v1",
                    "stage_key": "seed_set",
                    "output_name": "seed",
                    "shard_selection": "singleton",
                    "match_outcomes": None,
                }
            ],
            outputs=[
                _output("index", "loom.platform-fanout-index.v1"),
                _output("item", "loom.pipeline-core-item.v1", required=False),
                _output(
                    "manifest",
                    "loom.fanout-manifest.v1",
                    producer="platform",
                    role="fanout_manifest",
                    max_bytes=16_777_216,
                ),
            ],
            fanout_commit={
                "index_output_name": "index",
                "manifest_output_name": "manifest",
                "items_pointer": "/items",
                "item_binding_name": "item",
                "max_items": 2,
            },
        ),
        _container(
            "transform",
            image=image,
            needs=["produce_index"],
            inputs=[
                {
                    "source": "fanout_item",
                    "binding_name": "item",
                    "artifact_type": "loom.pipeline-core-item.v1",
                }
            ],
            outputs=[_output("transformed", "loom.pipeline-core-transformed.v1")],
            fanout={
                "source": "stage_output",
                "manifest_stage_key": "produce_index",
                "manifest_output_name": "manifest",
                "items_pointer": "/items",
                "shard_key_pointer": "/shard_key",
                "item_binding_name": "item",
                "item_artifact_type": "loom.pipeline-core-item.v1",
                "max_items": 2,
            },
        ),
        _container(
            "aggregate",
            image=image,
            needs=["transform"],
            inputs=[
                {
                    "source": "terminal_outputs",
                    "binding_name": "transforms",
                    "artifact_type": "loom.pipeline-core-transformed.v1",
                    "stage_keys": ["transform"],
                    "output_name": "transformed",
                    "match_outcomes": ["transformed"],
                }
            ],
            outputs=[_output("aggregate", "loom.pipeline-core-aggregate.v1", required=False)],
            request_renderer={
                "name": "pipeline_core_aggregate",
                "version": 1,
                "digest": renderer_digest,
                "max_bytes": 65_536,
                "terminal_stage_keys": ["transform"],
            },
        ),
        {
            "node_kind": "gate",
            "gate_kind": "outcome",
            "node_key": "outcome_gate",
            "shard_mode": "subject",
            "needs": ["aggregate"],
            "subject_stage_key": "aggregate",
            "match_outcomes": ["pass"],
            "matched_targets": ["local_artifact_readback"],
            "unmatched_targets": [],
        },
        _container(
            "local_artifact_readback",
            image=image,
            needs=["aggregate", "outcome_gate"],
            inputs=[
                {
                    "source": "stage_output",
                    "binding_name": "aggregate",
                    "artifact_type": "loom.pipeline-core-aggregate.v1",
                    "stage_key": "aggregate",
                    "output_name": "aggregate",
                    "shard_selection": "singleton",
                    "match_outcomes": ["pass"],
                }
            ],
            outputs=[_output("receipt", "loom.pipeline-core-receipt.v1")],
        ),
    ]
    return RunGraphSpecV1.model_validate(
        {
            "schema_version": "loom.run-graph.v1",
            "recipe": identity.model_dump(mode="json"),
            "inputs": [],
            "parameters": {},
            "budget": {
                "max_provider_cost_usd": "0",
                "max_gpu_seconds": 0,
                "max_wall_seconds": 600,
                "max_artifact_bytes": 64 * 1_048_576,
                "max_stage_runs": 7,
                "max_attempts_total": 21,
            },
            "nodes": nodes,
        }
    )


def pipeline_core_fixture_registration() -> OfficialRecipeRegistration:
    """Return the ordinary, immutable registration for the published CPU fixture."""

    parameter_contract = {
        "additionalProperties": False,
        "properties": {},
        "type": "object",
    }
    source_lock = {
        "dockerfile": {
            "path": "deploy/Dockerfile.pipeline-core-fixture",
            "sha256": _FIXTURE_DOCKERFILE_SHA256,
        },
        "fixture_module": {
            "path": "src/loom/pipeline_fixture.py",
            "sha256": _FIXTURE_MODULE_SHA256,
        },
        "image": PIPELINE_CORE_FIXTURE_IMAGE,
        "image_source_commit": PIPELINE_CORE_FIXTURE_IMAGE_SOURCE_COMMIT,
        "renderer_lock": canonical_digest(_renderer_lock()),
    }

    def factory(identity: RecipeIdentityV1, parameters: Mapping[str, Any]) -> RunGraphSpecV1:
        return build_pipeline_core_fixture_graph(
            identity,
            parameters,
            image=PIPELINE_CORE_FIXTURE_IMAGE,
        )

    return OfficialRecipeRegistration(
        name="pipeline-core-fixture",
        version=1,
        submission_policy="ordinary",
        factory=factory,
        parameter_contract_digest=canonical_digest(parameter_contract),
        source_lock_digest=canonical_digest(source_lock),
        renderer_locks=(_renderer_lock(),),
        conditional_output_contracts=(
            ConditionalOutputContract(
                stage_key="transform",
                outcomes=(("transformed", ("transformed",)),),
            ),
            ConditionalOutputContract(
                stage_key="aggregate",
                outcomes=(("pass", ("aggregate",)),),
            ),
        ),
    )


def builtin_pipeline_core_fixture_registry(*, repo_root: Path) -> OfficialRecipeRegistry:
    """Build the startup registry only after renderer source-lock verification."""

    return OfficialRecipeRegistry(
        (pipeline_core_fixture_registration(),),
        repo_root=repo_root,
    )


__all__ = [
    "PIPELINE_CORE_FIXTURE_IMAGE",
    "PIPELINE_CORE_FIXTURE_IMAGE_DIGEST",
    "PIPELINE_CORE_FIXTURE_IMAGE_REPOSITORY",
    "PIPELINE_CORE_FIXTURE_IMAGE_SOURCE_COMMIT",
    "build_pipeline_core_fixture_graph",
    "builtin_pipeline_core_fixture_registry",
    "pipeline_core_fixture_registration",
]
