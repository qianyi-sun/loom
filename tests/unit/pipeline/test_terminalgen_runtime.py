from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path
from uuid import UUID

import pytest

from loom.integrations.terminalgen.contracts import AuthoringImageLockV1
from loom.integrations.terminalgen.recipe import (
    TerminalGenRendererLocksV1,
    build_terminalgen_authoring_graph,
)
from loom.integrations.terminalgen.renderer_locks import terminalgen_renderer_locks
from loom.integrations.terminalgen.stage_request import TerminalGenStageRequestV1
from loom.pipeline.control_bindings import (
    ControlBindingSnapshotDocumentV1,
    TerminalGenProviderBindingV2,
    control_snapshot_digest,
)
from loom.pipeline.image_runtime import ImageRuntimeRecord, ImageRuntimeRegistry
from loom.pipeline.keys import canonical_digest
from loom.pipeline.resource_profiles import ResourceProfileRegistry
from loom.pipeline.spec import RecipeIdentityV1, RunGraphSpecV1
from loom.pipeline.work_protocol import ImageRuntimeContractV1
from loom_pipeline_orchestrator.repository import ReadinessCandidate
from loom_pipeline_orchestrator.terminalgen_runtime import TerminalGenReadinessRuntime

REPO_ROOT = Path(__file__).resolve().parents[3]
DIGEST = "sha256:" + "a" * 64
IMAGE = "registry.example.com/loom/terminalgen@" + DIGEST
PLATFORM_DIGEST = "sha256:" + "b" * 64
RUN_ID = UUID("10000000-0000-4000-8000-000000000001")
STAGE_ID = UUID("20000000-0000-4000-8000-000000000002")


def _graph() -> RunGraphSpecV1:
    digests = {lock.name: canonical_digest(lock) for lock in terminalgen_renderer_locks()}
    return build_terminalgen_authoring_graph(
        RecipeIdentityV1(name="terminalgen-authoring", version=1, digest=DIGEST),
        {
            "slots_per_card": 1,
            "difficulty": "hard",
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
            stage=digests["terminalgen_stage_request"],
            plan_audit=digests["terminalgen_plan_audit"],
            card_finalize=digests["terminalgen_card_finalize"],
            global_finalize=digests["terminalgen_global_finalize"],
            authoring_package=digests["terminalgen_authoring_package"],
            runtime_package=digests["terminalgen_runtime_package"],
        ),
        dependency_allowlist_digest=DIGEST,
    )


def _images() -> ImageRuntimeRegistry:
    contract = ImageRuntimeContractV1(
        image_index_digest=IMAGE,
        platform="linux/amd64",
        platform_manifest_digest=PLATFORM_DIGEST,
        cpu_arch="x86_64",
        gpu_vendor="none",
        cuda_userspace_version=None,
        min_nvidia_driver_version=None,
        application_features=[
            "terminalgen-generator",
            "terminalgen-packager",
            "terminalgen-planner",
            "terminalgen-validator",
        ],
        provider_assets=[],
        preflight_argv=["/opt/loom/bin/terminalgen-preflight"],
        preflight_digest=DIGEST,
        sbom_digest=DIGEST,
        attestation_digest=DIGEST,
    )
    return ImageRuntimeRegistry(
        {
            (IMAGE, "linux/amd64"): ImageRuntimeRecord(
                contract=contract,
                snapshot_sha256=canonical_digest(contract),
            )
        }
    )


def _candidate() -> ReadinessCandidate:
    graph = _graph()
    return ReadinessCandidate(
        pipeline_run_id=RUN_ID,
        stage_run_id=STAGE_ID,
        node_key="plan_batch",
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
        recipe_digest=DIGEST,
        graph_spec_digest=canonical_digest(graph),
        parameters_json=graph.parameters,
        resolved_inputs_json=[],
        official_submission_kind=None,
        authority_candidate_json=None,
        gpu_backend_selection_json=None,
        gpu_backend_selection_digest=None,
        ordinary_input_bindings_json=[
            {
                "binding_name": "catalog",
                "artifact_type": "terminalgen.authoring-catalog.v1",
                "cardinality": "one",
                "items": [
                    {
                        "artifact_id": "30000000-0000-4000-8000-000000000003",
                        "content_sha256": DIGEST,
                        "file_count": 1,
                        "item_key": "singleton",
                        "manifest_sha256": PLATFORM_DIGEST,
                        "stored_size_bytes": 100,
                        "unpacked_size_bytes": 200,
                    }
                ],
            }
        ],
        control_binding_snapshots_json=[],
    )


def _generate_candidate() -> ReadinessCandidate:
    graph = _graph()
    binding_id = UUID("40000000-0000-4000-8000-000000000004")
    connection_id = UUID("50000000-0000-4000-8000-000000000005")
    actor_id = UUID("60000000-0000-4000-8000-000000000006")
    team_id = UUID("70000000-0000-4000-8000-000000000007")
    now = datetime(2026, 8, 18, 20, tzinfo=UTC)
    snapshot = TerminalGenProviderBindingV2(
        schema_version="loom.recipe-provider-binding.v2",
        binding_id=binding_id,
        logical_name="generate_card_00",
        version=1,
        recipe_name="terminalgen-authoring",
        recipe_version=1,
        node_key="generate_card_00",
        status="active",
        recipe_digest=DIGEST,
        environment="dev",
        provider_connection_id=connection_id,
        provider="openai",
        model="gpt-5.6",
        wire_api="responses",
        runtime_adapter="terminalgen_openai_responses_v1",
        runtime_adapter_sha256=DIGEST,
        runner_lock_sha256=DIGEST,
        adapter_image_digest=IMAGE,
        request_schema_sha256=DIGEST,
        response_schema_sha256=DIGEST,
        provider_request_limit_per_attempt=2,
        provider_cost_limit_microusd_per_attempt=100_000,
        per_call_timeout_seconds=300,
        allowed_team_ids=[team_id],
        created_by=actor_id,
        created_at=now,
        updated_by=actor_id,
        updated_at=now,
    )
    control = ControlBindingSnapshotDocumentV1(
        logical_name="generate_card_00",
        kind="provider",
        node_key="generate_card_00",
        object_id=binding_id,
        version=1,
        snapshot_sha256=control_snapshot_digest(snapshot),
        snapshot=snapshot,
    )
    shard_key = "capability__same-domain-parametric__0001"
    slot_artifact_id = UUID("80000000-0000-4000-8000-000000000008")
    fanout_item = {
        "artifact_bindings": [
            {
                "artifact_id": str(slot_artifact_id),
                "artifact_type": "terminalgen.slot.v1",
                "name": "slot",
            }
        ],
        "parameters": {"slot_ordinal": 0},
        "shard_key": shard_key,
    }
    return ReadinessCandidate(
        pipeline_run_id=RUN_ID,
        stage_run_id=STAGE_ID,
        node_key="generate_card_00",
        shard_key=shard_key,
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
        recipe_digest=DIGEST,
        graph_spec_digest=canonical_digest(graph),
        parameters_json=graph.parameters,
        resolved_inputs_json=[],
        official_submission_kind=None,
        authority_candidate_json=None,
        gpu_backend_selection_json=None,
        gpu_backend_selection_digest=None,
        fanout_item_json=fanout_item,
        fanout_source_manifest_digest=PLATFORM_DIGEST,
        fanout_item_digest=canonical_digest(fanout_item),
        fanout_parameters_json={"slot_ordinal": 0},
        ordinary_input_bindings_json=[
            {
                "binding_name": "slot",
                "artifact_type": "terminalgen.slot.v1",
                "cardinality": "one",
                "items": [
                    {
                        "artifact_id": str(slot_artifact_id),
                        "content_sha256": DIGEST,
                        "file_count": 1,
                        "item_key": "singleton",
                        "manifest_sha256": PLATFORM_DIGEST,
                        "stored_size_bytes": 100,
                        "unpacked_size_bytes": 200,
                    }
                ],
            }
        ],
        control_binding_snapshots_json=[control.model_dump(mode="json")],
    )


@pytest.mark.asyncio
async def test_terminalgen_runtime_freezes_and_renders_offline_singleton() -> None:
    candidate = _candidate()
    runtime = TerminalGenReadinessRuntime(
        repo_root=REPO_ROOT,
        resource_profiles=ResourceProfileRegistry.load(),
        image_runtime=_images(),
    )

    frozen = await runtime.resolve(candidate)
    rendered = runtime.render(candidate, frozen)

    assert frozen.provider_connection_ref is None
    assert frozen.execution_spec_json["execution_variant_id"] == "terminalgen-cpu-x86_64"
    assert frozen.execution_spec_json["resolved_image_manifest_digest"] == PLATFORM_DIGEST
    assert rendered.provider_budget is None
    assert rendered.stage_request_bytes is not None
    request = TerminalGenStageRequestV1.model_validate_json(rendered.stage_request_bytes)
    assert request.node_key == "plan_batch"
    assert request.inputs[0].binding_name == "catalog"
    assert request.provenance.execution_spec_digest == frozen.execution_spec_digest


@pytest.mark.asyncio
async def test_terminalgen_runtime_rejects_graph_digest_drift() -> None:
    candidate = _candidate()
    candidate = replace(candidate, graph_spec_digest=PLATFORM_DIGEST)
    runtime = TerminalGenReadinessRuntime(
        repo_root=REPO_ROOT,
        resource_profiles=ResourceProfileRegistry.load(),
        image_runtime=_images(),
    )

    with pytest.raises(ValueError, match="graph authority drift"):
        await runtime.resolve(candidate)


@pytest.mark.asyncio
async def test_terminalgen_runtime_rejects_singleton_fanout_item() -> None:
    candidate = replace(
        _candidate(),
        fanout_item_json={
            "artifact_bindings": [
                {
                    "artifact_id": "80000000-0000-4000-8000-000000000008",
                    "artifact_type": "terminalgen.slot.v1",
                    "name": "slot",
                }
            ],
            "parameters": {"slot_ordinal": 0},
            "shard_key": "capability__same-domain-parametric__0001",
        },
    )
    runtime = TerminalGenReadinessRuntime(
        repo_root=REPO_ROOT,
        resource_profiles=ResourceProfileRegistry.load(),
        image_runtime=_images(),
    )

    with pytest.raises(ValueError, match="singleton fanout authority drift"):
        await runtime.resolve(candidate)


@pytest.mark.asyncio
async def test_terminalgen_runtime_freezes_generate_provider_budget_and_fanout() -> None:
    candidate = _generate_candidate()
    runtime = TerminalGenReadinessRuntime(
        repo_root=REPO_ROOT,
        resource_profiles=ResourceProfileRegistry.load(),
        image_runtime=_images(),
    )

    frozen = await runtime.resolve(candidate)
    rendered = runtime.render(candidate, frozen)

    assert frozen.provider_connection_ref == UUID("50000000-0000-4000-8000-000000000005")
    assert frozen.execution_spec_json["fanout_item_digest"] == candidate.fanout_item_digest
    assert len(frozen.execution_spec_json["control_binding_snapshots"]) == 1
    assert rendered.provider_budget is not None
    assert rendered.provider_budget.request_limit == 2
    assert rendered.stage_request_bytes is not None
    request = TerminalGenStageRequestV1.model_validate_json(rendered.stage_request_bytes)
    assert request.fanout_item is not None
    assert request.fanout_item.shard_key == candidate.shard_key
    assert request.provenance.control_binding is not None


@pytest.mark.asyncio
async def test_terminalgen_runtime_rejects_fanout_parameter_drift() -> None:
    candidate = replace(_generate_candidate(), fanout_parameters_json={"slot_ordinal": 1})
    runtime = TerminalGenReadinessRuntime(
        repo_root=REPO_ROOT,
        resource_profiles=ResourceProfileRegistry.load(),
        image_runtime=_images(),
    )

    with pytest.raises(ValueError, match="expanded fanout parameters drift"):
        await runtime.resolve(candidate)


@pytest.mark.asyncio
async def test_terminalgen_runtime_rejects_secret_looking_fanout_parameters() -> None:
    candidate = _generate_candidate()
    assert candidate.fanout_item_json is not None
    parameters = {"api_key": "opaque-but-forbidden"}
    fanout_item = {**candidate.fanout_item_json, "parameters": parameters}
    candidate = replace(
        candidate,
        fanout_item_json=fanout_item,
        fanout_item_digest=canonical_digest(fanout_item),
        fanout_parameters_json=parameters,
    )
    runtime = TerminalGenReadinessRuntime(
        repo_root=REPO_ROOT,
        resource_profiles=ResourceProfileRegistry.load(),
        image_runtime=_images(),
    )

    frozen = await runtime.resolve(candidate)
    with pytest.raises(ValueError, match="secret-looking field name"):
        runtime.render(candidate, frozen)
