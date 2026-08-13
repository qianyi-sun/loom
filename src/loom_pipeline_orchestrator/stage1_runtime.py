"""Production readiness and StageRequest rendering for the hidden Stage 1 smoke."""

from __future__ import annotations

from pathlib import Path
from uuid import UUID

from loom.integrations.behavior.contracts import (
    BehaviorRolloutParametersV1,
    BehaviorStage,
    StageRequestBindingSetV1,
    StageRequestProvenanceV1,
)
from loom.pipeline.budget import (
    BudgetKind,
    final_artifact_reservation_key,
    gpu_reservation_key,
    reservation_request_digest,
)
from loom.pipeline.image_runtime import ImageRuntimeRegistry
from loom.pipeline.keys import canonical_digest, canonical_document, canonical_uuid5
from loom.pipeline.renderers.behavior_stage_request import render
from loom.pipeline.renderers.behavior_stage_request_models import (
    BehaviorStageRequestRenderInputV1,
)
from loom.pipeline.resource_profiles import ResourceProfileRegistry
from loom.pipeline.spec import (
    BindingItemV1,
    BindingSetV1,
    ContainerNodeV1,
    ExecutionSpecSnapshotV1,
    RunGraphSpecV1,
    StageBudgetV1,
)
from loom.pipeline.stage1_smoke import (
    Stage1SmokeCandidateV1,
    load_behavior_renderer_lock,
)
from loom_pipeline_orchestrator.reconciler import RenderedAttempt
from loom_pipeline_orchestrator.repository import (
    AttemptReservationSpec,
    FrozenReadiness,
    ReadinessCandidate,
)

_ATTEMPT_NAMESPACE = UUID("c485c5f7-ae4a-4301-9fd1-b76b7357bec7")


class Stage1RuntimeError(ValueError):
    """The persisted authority cannot be rendered without drift."""


class Stage1ReadinessResolver:
    def __init__(
        self,
        *,
        repo_root: Path,
        resource_profiles: ResourceProfileRegistry,
        image_runtime: ImageRuntimeRegistry,
    ) -> None:
        self._repo_root = repo_root
        self._profiles = resource_profiles
        self._images = image_runtime

    @staticmethod
    def supports(candidate: ReadinessCandidate) -> bool:
        return candidate.official_submission_kind == "behavior_stage1_smoke_v1"

    async def resolve(self, candidate: ReadinessCandidate) -> FrozenReadiness:
        if not self.supports(candidate) or candidate.authority_candidate_json is None:
            raise Stage1RuntimeError("unsupported Pipeline readiness authority")
        authority = Stage1SmokeCandidateV1.model_validate_json(
            canonical_document(candidate.authority_candidate_json)
        )
        if candidate.node_key != "rollout" or candidate.shard_key != "singleton":
            raise Stage1RuntimeError("Stage 1 run identity drift")
        graph = RunGraphSpecV1.model_validate_json(canonical_document(candidate.graph_spec_json))
        if (
            canonical_digest(graph) != candidate.graph_spec_digest
            or graph.recipe.digest != candidate.recipe_digest
            or authority.recipe_digest != candidate.recipe_digest
            or candidate.parameters_json != authority.parameters
        ):
            raise Stage1RuntimeError("Stage 1 graph authority drift")
        nodes = [item for item in graph.nodes if isinstance(item, ContainerNodeV1)]
        if len(nodes) != 1 or nodes[0].node_key != candidate.node_key:
            raise Stage1RuntimeError("Stage 1 graph is not the exact one-node graph")
        node = nodes[0]
        profile_record = self._profiles.get(node.resource_profile)
        if profile_record.snapshot_sha256 != authority.resource_profile_sha256:
            raise Stage1RuntimeError("Stage 1 ResourceProfile drift")
        variant = next(
            (
                item
                for item in profile_record.profile.execution_variants
                if item.variant_id == authority.backend_variant_id
            ),
            None,
        )
        if variant is None:
            raise Stage1RuntimeError("Stage 1 execution variant is unavailable")
        image_record = self._images.resolve(
            image_index_digest=authority.image_index_digest,
            cpu_arch=variant.cpu_arch,
            expected_snapshot_sha256=authority.image_runtime_contract_sha256,
        )
        if (
            image_record.contract.platform_manifest_digest != authority.platform_child_digest
            or image_record.contract.platform != authority.platform
        ):
            raise Stage1RuntimeError("Stage 1 image index/child authority drift")
        selection = candidate.gpu_backend_selection_json
        if (
            selection is None
            or candidate.gpu_backend_selection_digest is None
            or canonical_digest(selection) != candidate.gpu_backend_selection_digest
            or selection.get("variant_id") != authority.backend_variant_id
            or selection.get("policy_id") != authority.policy_id
            or selection.get("selection_source") != "acceptance_authority"
        ):
            raise Stage1RuntimeError("Stage 1 GPU backend selection drift")
        bindings = _binding_sets(candidate, authority)
        bindings_json = [item.model_dump(mode="json") for item in bindings]
        bindings_digest = canonical_digest(bindings_json)
        lock = load_behavior_renderer_lock(self._repo_root)
        if canonical_digest(lock) != authority.renderer_lock_sha256:
            raise Stage1RuntimeError("Stage 1 renderer drift")
        spec = ExecutionSpecSnapshotV1(
            schema_version="loom.execution-spec.v1",
            recipe_digest=candidate.recipe_digest,
            run_graph_digest=candidate.graph_spec_digest,
            node_key=node.node_key,
            shard_key=candidate.shard_key,
            container_node=node,
            image_runtime_contract_digest=image_record.snapshot_sha256,
            resource_profile_digest=profile_record.snapshot_sha256,
            execution_variant_id=authority.backend_variant_id,
            gpu_backend_selection_sha256=candidate.gpu_backend_selection_digest,
            resolved_image_manifest_digest=authority.platform_child_digest,
            network_profile="none",
            resolved_input_bindings_digest=bindings_digest,
            fanout_source_manifest_digest=None,
            fanout_item_digest=None,
            fanout_parameters_digest=None,
            request_renderer_lock_digest=authority.renderer_lock_sha256,
            control_binding_snapshots=[],
        )
        spec_json = spec.model_dump(mode="json")
        spec_bytes = canonical_document(spec_json)
        return FrozenReadiness(
            input_bindings_json=bindings_json,
            input_bindings_digest=bindings_digest,
            execution_spec_json=spec_json,
            execution_spec_bytes=spec_bytes,
            execution_spec_digest=canonical_digest(spec_json),
            resource_profile_json=profile_record.profile.model_dump(mode="json"),
            resource_profile_digest=profile_record.snapshot_sha256,
            image_runtime_contract_json=image_record.contract.model_dump(mode="json"),
            image_runtime_contract_digest=image_record.snapshot_sha256,
        )


class Stage1RequestRenderer:
    def render(
        self, candidate: ReadinessCandidate, frozen: FrozenReadiness
    ) -> RenderedAttempt:
        authority = Stage1SmokeCandidateV1.model_validate_json(
            canonical_document(candidate.authority_candidate_json)
        )
        attempt_number = candidate.attempt_count + 1
        attempt_id = canonical_uuid5(
            _ATTEMPT_NAMESPACE,
            {
                "attempt_number": attempt_number,
                "pipeline_run_id": str(candidate.pipeline_run_id),
                "stage_run_id": str(candidate.stage_run_id),
            },
        )
        request = render(
            BehaviorStageRequestRenderInputV1(
                stage=BehaviorStage.ROLLOUT,
                run_id=candidate.pipeline_run_id,
                stage_run_id=candidate.stage_run_id,
                attempt_id=attempt_id,
                inputs=[
                    StageRequestBindingSetV1.model_validate_json(canonical_document(item))
                    for item in frozen.input_bindings_json
                ],
                parameters=BehaviorRolloutParametersV1.model_validate(authority.parameters),
                budget=authority.stage_budget,
                provenance=StageRequestProvenanceV1(
                    recipe_digest=authority.recipe_digest,
                    resolved_input_bindings_digest=frozen.input_bindings_digest,
                    execution_spec_digest=frozen.execution_spec_digest,
                    # The execution spec keeps the multi-platform index as the
                    # scheduling identity. The StageRequest records the exact
                    # selected child that the adapter actually executes and
                    # later returns in StageResult provenance.
                    image_digest=(
                        authority.image_index_digest.rsplit("@", maxsplit=1)[0]
                        + "@"
                        + authority.platform_child_digest
                    ),
                    loom_commit_sha=authority.loom_commit_sha,
                    control_binding=None,
                    compatibility_manifest_sha256=authority.compatibility_manifest_sha256,
                ),
                orchestration=None,
            )
        )
        request_json = request.model_dump(mode="json")
        request_bytes = canonical_document(request_json)
        request_digest = canonical_digest(request_json)
        reservations = _reservations(
            attempt_id=attempt_id,
            stage_budget=authority.stage_budget,
            execution_spec_digest=frozen.execution_spec_digest,
        )
        return RenderedAttempt(
            attempt_id=attempt_id,
            stage_request_json=request_json,
            stage_request_bytes=request_bytes,
            stage_request_digest=request_digest,
            reservations=reservations,
        )


def _binding_sets(
    candidate: ReadinessCandidate, authority: Stage1SmokeCandidateV1
) -> list[BindingSetV1]:
    if len(candidate.resolved_inputs_json) != 3:
        raise Stage1RuntimeError("Stage 1 input descriptor set is not exact")
    observed_names = [str(item.get("input_name")) for item in candidate.resolved_inputs_json]
    expected_names = [item.name for item in authority.inputs]
    if observed_names != expected_names or len(set(observed_names)) != len(observed_names):
        raise Stage1RuntimeError("Stage 1 input order or identity drift")
    by_name = {str(item.get("input_name")): item for item in candidate.resolved_inputs_json}
    result: list[BindingSetV1] = []
    for expected in authority.inputs:
        observed = by_name.get(expected.name)
        if observed is None:
            raise Stage1RuntimeError("Stage 1 input is missing")
        expected_fields = {
            "artifact_id": str(expected.artifact_id),
            "artifact_type": expected.artifact_type,
            "content_sha256": expected.content_sha256,
            "manifest_sha256": expected.manifest_sha256,
            "stored_size_bytes": expected.stored_size_bytes,
            "unpacked_size_bytes": expected.unpacked_size_bytes,
            "file_count": expected.file_count,
        }
        if any(observed.get(name) != value for name, value in expected_fields.items()):
            raise Stage1RuntimeError("Stage 1 input descriptor drift")
        result.append(
            BindingSetV1(
                binding_name=expected.name,
                artifact_type=expected.artifact_type,
                cardinality="one",
                items=[
                    BindingItemV1(
                        artifact_id=expected.artifact_id,
                        content_sha256=expected.content_sha256,
                        file_count=expected.file_count,
                        item_key="singleton",
                        manifest_sha256=expected.manifest_sha256,
                        stored_size_bytes=expected.stored_size_bytes,
                        unpacked_size_bytes=expected.unpacked_size_bytes,
                    )
                ],
            )
        )
    return result


def _reservations(
    *, attempt_id: UUID, stage_budget: StageBudgetV1, execution_spec_digest: str
) -> tuple[AttemptReservationSpec, ...]:
    values: list[AttemptReservationSpec] = []
    for kind, key, amount in (
        (BudgetKind.ARTIFACT, final_artifact_reservation_key(attempt_id), stage_budget.final_output_bytes_limit),
        (BudgetKind.GPU, gpu_reservation_key(attempt_id), stage_budget.gpu_seconds_limit),
    ):
        identity: dict[str, str | int] = {
            "execution_attempt_id": str(attempt_id),
            "purpose": "stage1_smoke",
        }
        values.append(
            AttemptReservationSpec(
                kind=kind,
                reservation_key=key,
                request_digest=reservation_request_digest(
                    kind=kind,
                    reservation_key=key,
                    producer_identity=identity,
                    requested_amount=amount,
                    governing_digest=execution_spec_digest,
                ),
                amount=amount,
                metadata=identity,
            )
        )
    return tuple(values)


__all__ = ["Stage1ReadinessResolver", "Stage1RequestRenderer", "Stage1RuntimeError"]
