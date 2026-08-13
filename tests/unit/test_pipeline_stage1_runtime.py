from __future__ import annotations

from datetime import UTC, datetime
from uuid import UUID

import pytest

from loom.pipeline.gpu_backend import PipelineRunGpuBackendSelectionV1
from loom.pipeline.image_runtime import ImageRuntimeRecord, ImageRuntimeRegistry
from loom.pipeline.keys import canonical_digest
from loom.pipeline.resource_profiles import ResourceProfileRegistry
from loom.pipeline.stage1_smoke import build_stage1_smoke_graph
from loom.pipeline.work_protocol import ImageRuntimeContractV1
from loom_pipeline_orchestrator.repository import ReadinessCandidate
from loom_pipeline_orchestrator.stage1_runtime import (
    Stage1ReadinessResolver,
    Stage1RequestRenderer,
    Stage1RuntimeError,
)
from tests.unit.test_pipeline_stage1_smoke import DIGEST, REPO_ROOT, _candidate


def _readiness(*, reverse_inputs: bool = False) -> tuple[ReadinessCandidate, object]:
    candidate = _candidate()
    graph = build_stage1_smoke_graph(candidate, repo_root=REPO_ROOT)
    run_id = UUID("10000000-0000-4000-8000-000000000001")
    stage_id = UUID("20000000-0000-4000-8000-000000000002")
    selection = PipelineRunGpuBackendSelectionV1(
        pipeline_run_id=run_id,
        scope="all_gpu_nodes",
        variant_id=candidate.backend_variant_id,
        policy_id=candidate.policy_id,
        selection_source="acceptance_authority",
        selected_at=datetime(2026, 8, 13, 16, tzinfo=UTC),
    )
    inputs = [
        {
            "input_name": item.name,
            "artifact_id": str(item.artifact_id),
            "artifact_type": item.artifact_type,
            "content_sha256": item.content_sha256,
            "manifest_sha256": item.manifest_sha256,
            "stored_size_bytes": item.stored_size_bytes,
            "unpacked_size_bytes": item.unpacked_size_bytes,
            "file_count": item.file_count,
        }
        for item in candidate.inputs
    ]
    if reverse_inputs:
        inputs.reverse()
    return (
        ReadinessCandidate(
            pipeline_run_id=run_id,
            stage_run_id=stage_id,
            node_key="rollout",
            shard_key="singleton",
            state="blocked",
            attempt_count=0,
            graph_spec_json=graph.model_dump(mode="json"),
            resolved_input_bindings_json=None,
            resolved_input_bindings_digest=None,
            resolved_execution_spec_json=None,
            resolved_execution_spec_bytes=None,
            execution_spec_digest=None,
            resource_profile_json=None,
            resource_profile_digest=None,
            image_runtime_contract_json=None,
            image_runtime_contract_digest=None,
            recipe_digest=candidate.recipe_digest,
            graph_spec_digest=canonical_digest(graph),
            parameters_json=candidate.parameters,
            resolved_inputs_json=inputs,
            official_submission_kind="behavior_stage1_smoke_v1",
            authority_candidate_json=candidate.model_dump(mode="json"),
            gpu_backend_selection_json=selection.model_dump(mode="json"),
            gpu_backend_selection_digest=selection.gpu_backend_selection_sha256,
        ),
        candidate,
    )


def _images(candidate: object) -> ImageRuntimeRegistry:
    contract = ImageRuntimeContractV1.model_validate(
        {
            "image_index_digest": candidate.image_index_digest,
            "platform": candidate.platform,
            "platform_manifest_digest": candidate.platform_child_digest,
            "cpu_arch": "x86_64",
            "gpu_vendor": "nvidia",
            "cuda_userspace_version": "13.0",
            "min_nvidia_driver_version": "580.12.0",
            "application_features": ["isaac-sim-5.1", "omnigibson-3.8"],
            "provider_assets": [],
            "preflight_argv": ["/opt/behavior/bin/behavior-gpu-preflight"],
            "preflight_digest": DIGEST,
            "sbom_digest": DIGEST,
            "attestation_digest": DIGEST,
        }
    )
    return ImageRuntimeRegistry(
        {
            (candidate.image_index_digest, candidate.platform): ImageRuntimeRecord(
                contract=contract,
                snapshot_sha256=candidate.image_runtime_contract_sha256,
            )
        }
    )


async def test_stage1_readiness_renders_selected_platform_child_provenance() -> None:
    readiness, candidate = _readiness()
    resolver = Stage1ReadinessResolver(
        repo_root=REPO_ROOT,
        resource_profiles=ResourceProfileRegistry.load(),
        image_runtime=_images(candidate),
    )

    frozen = await resolver.resolve(readiness)
    rendered = Stage1RequestRenderer().render(readiness, frozen)

    provenance = rendered.stage_request_json["provenance"]
    assert provenance["image_digest"] == (
        candidate.image_index_digest.rsplit("@", 1)[0]
        + "@"
        + candidate.platform_child_digest
    )
    assert frozen.execution_spec_json["container_node"]["image"] == (
        candidate.image_index_digest
    )
    assert frozen.execution_spec_json["resolved_image_manifest_digest"] == (
        candidate.platform_child_digest
    )


async def test_stage1_readiness_fails_closed_without_published_runtime_lock() -> None:
    readiness, _candidate_value = _readiness()
    resolver = Stage1ReadinessResolver(
        repo_root=REPO_ROOT,
        resource_profiles=ResourceProfileRegistry.load(),
        image_runtime=ImageRuntimeRegistry({}),
    )

    with pytest.raises(ValueError, match="image_contract_mismatch"):
        await resolver.resolve(readiness)


async def test_stage1_readiness_rejects_resolved_input_reordering() -> None:
    readiness, candidate = _readiness(reverse_inputs=True)
    resolver = Stage1ReadinessResolver(
        repo_root=REPO_ROOT,
        resource_profiles=ResourceProfileRegistry.load(),
        image_runtime=_images(candidate),
    )

    with pytest.raises(Stage1RuntimeError, match="input order"):
        await resolver.resolve(readiness)
