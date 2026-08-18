"""Closed readiness and request rendering for TerminalGen authoring."""

from __future__ import annotations

from pathlib import Path
from uuid import UUID

from loom.integrations.terminalgen.authority import (
    TERMINALGEN_RECIPE_NAME,
    TERMINALGEN_RECIPE_VERSION,
)
from loom.integrations.terminalgen.renderer_locks import terminalgen_renderer_locks
from loom.integrations.terminalgen.stage_request import (
    TerminalGenStageRequestProvenanceV1,
    TerminalGenStageRequestV1,
    render,
)
from loom.pipeline.budget import (
    BudgetKind,
    final_artifact_reservation_key,
    gpu_reservation_key,
    reservation_request_digest,
)
from loom.pipeline.control_bindings import (
    ControlBindingSnapshotDocumentV1,
    TerminalGenProviderBindingV2,
)
from loom.pipeline.image_runtime import ImageRuntimeRegistry
from loom.pipeline.keys import canonical_digest, canonical_document, canonical_uuid5
from loom.pipeline.recipes import verify_renderer_lock
from loom.pipeline.resource_profiles import ResourceProfileRegistry
from loom.pipeline.spec import (
    BindingSetV1,
    ContainerNodeV1,
    ControlBindingSnapshotRefV1,
    ExecutionSpecSnapshotV1,
    FanoutManifestItemV1,
    PipelineTerminalSnapshotDocumentV1,
    ProviderAttemptLimitsV1,
    RunGraphSpecV1,
    StageBudgetV1,
)
from loom_pipeline_orchestrator.reconciler import RenderedAttempt
from loom_pipeline_orchestrator.repository import (
    AttemptProviderBudgetSpec,
    AttemptReservationSpec,
    FrozenReadiness,
    ReadinessCandidate,
)

_ATTEMPT_NAMESPACE = UUID("d0f583e4-643b-4924-8b2c-02866be712a4")


class TerminalGenRuntimeError(ValueError):
    """Persisted ordinary authority is absent or has drifted."""


class TerminalGenReadinessRuntime:
    def __init__(
        self,
        *,
        repo_root: Path,
        resource_profiles: ResourceProfileRegistry,
        image_runtime: ImageRuntimeRegistry,
    ) -> None:
        self._profiles = resource_profiles
        self._images = image_runtime
        self._renderer_digests = {
            (lock.name, lock.version): verify_renderer_lock(lock, repo_root)
            for lock in terminalgen_renderer_locks()
        }

    @staticmethod
    def supports(candidate: ReadinessCandidate) -> bool:
        if candidate.official_submission_kind is not None:
            return False
        recipe = candidate.graph_spec_json.get("recipe")
        return isinstance(recipe, dict) and (
            recipe.get("name"), recipe.get("version")
        ) == (TERMINALGEN_RECIPE_NAME, TERMINALGEN_RECIPE_VERSION)

    async def resolve(self, candidate: ReadinessCandidate) -> FrozenReadiness:
        _graph, node = self._graph_node(candidate)
        if candidate.ordinary_input_bindings_json is None:
            raise TerminalGenRuntimeError("ordinary input bindings are unavailable")
        bindings = [
            BindingSetV1.model_validate_json(canonical_document(item))
            for item in candidate.ordinary_input_bindings_json
        ]
        bindings_json = [item.model_dump(mode="json") for item in bindings]
        bindings_digest = canonical_digest(bindings_json)

        profile_record = self._profiles.get(node.resource_profile)
        variants = profile_record.profile.execution_variants
        if len(variants) != 1 or variants[0].gpu_count_exact != 0:
            raise TerminalGenRuntimeError("TerminalGen requires one exact CPU execution variant")
        variant = variants[0]
        image_record = self._images.resolve(
            image_index_digest=node.image,
            cpu_arch=variant.cpu_arch,
        )
        if not set(profile_record.profile.required_image_features).issubset(
            set(image_record.contract.application_features)
        ):
            raise TerminalGenRuntimeError("TerminalGen image feature authority drift")

        if node.request_renderer is None:
            raise TerminalGenRuntimeError("TerminalGen node is missing its request renderer")
        renderer_digest = self._renderer_digests.get(
            (node.request_renderer.name, node.request_renderer.version)
        )
        if renderer_digest != node.request_renderer.digest:
            raise TerminalGenRuntimeError("TerminalGen renderer lock drift")

        control = self._control_binding(candidate, node)
        refs = [] if control is None else [
            ControlBindingSnapshotRefV1(
                logical_name=control.logical_name,
                kind=control.kind,
                object_id=control.object_id,
                version=control.version,
                snapshot_sha256=control.snapshot_sha256,
            )
        ]
        fanout_item = (
            FanoutManifestItemV1.model_validate_json(canonical_document(candidate.fanout_item_json))
            if candidate.fanout_item_json is not None
            else None
        )
        fanout_parameters_digest = (
            canonical_digest(candidate.fanout_parameters_json)
            if candidate.fanout_parameters_json is not None
            else None
        )
        if candidate.shard_key == "singleton":
            if any(
                item is not None
                for item in (
                    candidate.fanout_source_manifest_digest,
                    candidate.fanout_item_digest,
                    fanout_parameters_digest,
                )
            ):
                raise TerminalGenRuntimeError("singleton fanout authority drift")
        elif (
            fanout_item is None
            or candidate.fanout_source_manifest_digest is None
            or candidate.fanout_item_digest != canonical_digest(fanout_item)
            or fanout_parameters_digest is None
        ):
            raise TerminalGenRuntimeError("expanded fanout authority is incomplete")

        spec = ExecutionSpecSnapshotV1(
            schema_version="loom.execution-spec.v1",
            recipe_digest=candidate.recipe_digest,
            run_graph_digest=candidate.graph_spec_digest,
            node_key=node.node_key,
            shard_key=candidate.shard_key,
            container_node=node,
            image_runtime_contract_digest=image_record.snapshot_sha256,
            resource_profile_digest=profile_record.snapshot_sha256,
            execution_variant_id=variant.variant_id,
            gpu_backend_selection_sha256=None,
            resolved_image_manifest_digest=image_record.contract.platform_manifest_digest,
            network_profile=node.network_profile,
            resolved_input_bindings_digest=bindings_digest,
            fanout_source_manifest_digest=candidate.fanout_source_manifest_digest,
            fanout_item_digest=candidate.fanout_item_digest,
            fanout_parameters_digest=fanout_parameters_digest,
            request_renderer_lock_digest=renderer_digest,
            control_binding_snapshots=refs,
        )
        spec_json = spec.model_dump(mode="json")
        provider_connection = (
            control.snapshot.provider_connection_id if control is not None else None
        )
        return FrozenReadiness(
            input_bindings_json=bindings_json,
            input_bindings_digest=bindings_digest,
            execution_spec_json=spec_json,
            execution_spec_bytes=canonical_document(spec_json),
            execution_spec_digest=canonical_digest(spec_json),
            resource_profile_json=profile_record.profile.model_dump(mode="json"),
            resource_profile_digest=profile_record.snapshot_sha256,
            image_runtime_contract_json=image_record.contract.model_dump(mode="json"),
            image_runtime_contract_digest=image_record.snapshot_sha256,
            provider_connection_ref=provider_connection,
            secret_refs=(),
        )

    def render(
        self, candidate: ReadinessCandidate, frozen: FrozenReadiness
    ) -> RenderedAttempt:
        _graph, node = self._graph_node(candidate)
        spec = ExecutionSpecSnapshotV1.model_validate_json(frozen.execution_spec_bytes)
        if spec.request_renderer_lock_digest is None:
            raise TerminalGenRuntimeError("TerminalGen renderer snapshot is missing")
        attempt_number = candidate.attempt_count + 1
        attempt_id = canonical_uuid5(
            _ATTEMPT_NAMESPACE,
            {
                "attempt_number": attempt_number,
                "pipeline_run_id": str(candidate.pipeline_run_id),
                "stage_run_id": str(candidate.stage_run_id),
            },
        )
        control = self._control_binding(candidate, node)
        provider_limits = None
        provider_budget = None
        if control is not None:
            snapshot = control.snapshot
            provider_limits = ProviderAttemptLimitsV1(
                provider_request_limit_per_attempt=snapshot.provider_request_limit_per_attempt,
                provider_cost_limit_microusd_per_attempt=(
                    snapshot.provider_cost_limit_microusd_per_attempt
                ),
                per_call_timeout_seconds=snapshot.per_call_timeout_seconds,
            )
            provider_budget = AttemptProviderBudgetSpec(
                binding_snapshot_sha256=control.snapshot_sha256,
                request_limit=snapshot.provider_request_limit_per_attempt,
                cost_limit_microusd=snapshot.provider_cost_limit_microusd_per_attempt,
                per_call_timeout_seconds=snapshot.per_call_timeout_seconds,
            )
        stage_budget = StageBudgetV1.for_node(
            node,
            gpu_count_exact=0,
            provider=provider_limits,
        )
        terminal = (
            PipelineTerminalSnapshotDocumentV1.model_validate_json(
                candidate.terminal_snapshot.snapshot_bytes
            )
            if candidate.terminal_snapshot is not None
            else None
        )
        if bool(node.request_renderer and node.request_renderer.terminal_stage_keys) != (
            terminal is not None
        ):
            raise TerminalGenRuntimeError("TerminalGen terminal snapshot cardinality drift")
        fanout_item = (
            FanoutManifestItemV1.model_validate_json(
                canonical_document(candidate.fanout_item_json)
            )
            if candidate.fanout_item_json is not None
            else None
        )
        provenance = TerminalGenStageRequestProvenanceV1(
            recipe_digest=candidate.recipe_digest,
            run_graph_digest=candidate.graph_spec_digest,
            resolved_input_bindings_digest=frozen.input_bindings_digest,
            execution_spec_digest=frozen.execution_spec_digest,
            resource_profile_digest=spec.resource_profile_digest,
            image_runtime_contract_digest=spec.image_runtime_contract_digest,
            resolved_image_manifest_digest=spec.resolved_image_manifest_digest,
            request_renderer_digest=spec.request_renderer_lock_digest,
            control_binding=control,
        )
        request_values = {
            "schema_version": "terminalgen.stage-request.v1",
            "run_id": candidate.pipeline_run_id,
            "stage_run_id": candidate.stage_run_id,
            "attempt_id": attempt_id,
            "node_key": candidate.node_key,
            "shard_key": candidate.shard_key,
            "inputs": [
                BindingSetV1.model_validate_json(canonical_document(item))
                for item in frozen.input_bindings_json
            ],
            "parameters": candidate.parameters_json,
            "fanout_item": fanout_item,
            "budget": stage_budget,
            "provenance": provenance,
            "orchestration": terminal,
        }
        request_preimage = {
            **request_values,
            "run_id": str(candidate.pipeline_run_id),
            "stage_run_id": str(candidate.stage_run_id),
            "attempt_id": str(attempt_id),
            "inputs": frozen.input_bindings_json,
            "fanout_item": fanout_item.model_dump(mode="json") if fanout_item else None,
            "budget": stage_budget.model_dump(mode="json"),
            "provenance": provenance.model_dump(mode="json"),
            "orchestration": terminal.model_dump(mode="json") if terminal else None,
        }
        request = TerminalGenStageRequestV1.model_validate(
            request_values
            | {"idempotency_key": canonical_digest(request_preimage, persisted=False)}
        )
        request_bytes = render(request)
        assert node.request_renderer is not None
        if len(request_bytes) > node.request_renderer.max_bytes:
            raise TerminalGenRuntimeError("TerminalGen StageRequest exceeds renderer max_bytes")
        request_digest = canonical_digest(request.model_dump(mode="json"))
        return RenderedAttempt(
            attempt_id=attempt_id,
            stage_request_json=request.model_dump(mode="json"),
            stage_request_bytes=request_bytes,
            stage_request_digest=request_digest,
            reservations=_reservations(
                attempt_id=attempt_id,
                stage_budget=stage_budget,
                execution_spec_digest=frozen.execution_spec_digest,
                node_key=node.node_key,
            ),
            provider_budget=provider_budget,
        )

    @staticmethod
    def _graph_node(
        candidate: ReadinessCandidate,
    ) -> tuple[RunGraphSpecV1, ContainerNodeV1]:
        if not TerminalGenReadinessRuntime.supports(candidate):
            raise TerminalGenRuntimeError("unsupported TerminalGen readiness authority")
        graph = RunGraphSpecV1.model_validate_json(canonical_document(candidate.graph_spec_json))
        if (
            canonical_digest(graph) != candidate.graph_spec_digest
            or graph.recipe.digest != candidate.recipe_digest
            or graph.parameters != candidate.parameters_json
        ):
            raise TerminalGenRuntimeError("TerminalGen graph authority drift")
        matches = [
            item
            for item in graph.nodes
            if isinstance(item, ContainerNodeV1) and item.node_key == candidate.node_key
        ]
        if len(matches) != 1:
            raise TerminalGenRuntimeError("TerminalGen node authority is not exact")
        return graph, matches[0]

    @staticmethod
    def _control_binding(
        candidate: ReadinessCandidate,
        node: ContainerNodeV1,
    ) -> ControlBindingSnapshotDocumentV1 | None:
        values = candidate.control_binding_snapshots_json or []
        documents = [
            ControlBindingSnapshotDocumentV1.model_validate_json(canonical_document(item))
            for item in values
        ]
        selected = [item for item in documents if item.node_key == node.node_key]
        requires_provider = node.network_profile == "gateway"
        if requires_provider != (len(selected) == 1):
            raise TerminalGenRuntimeError("TerminalGen control binding cardinality drift")
        if not selected:
            return None
        control = selected[0]
        if not isinstance(control.snapshot, TerminalGenProviderBindingV2):
            raise TerminalGenRuntimeError("TerminalGen provider binding type drift")
        if (
            control.snapshot.recipe_digest != candidate.recipe_digest
            or control.snapshot.node_key != node.node_key
            or control.snapshot.status != "active"
        ):
            raise TerminalGenRuntimeError("TerminalGen provider binding authority drift")
        return control


def _reservations(
    *,
    attempt_id: UUID,
    stage_budget: StageBudgetV1,
    execution_spec_digest: str,
    node_key: str,
) -> tuple[AttemptReservationSpec, ...]:
    values: list[AttemptReservationSpec] = []
    for kind, key, amount in (
        (
            BudgetKind.ARTIFACT,
            final_artifact_reservation_key(attempt_id),
            stage_budget.final_output_bytes_limit,
        ),
        (BudgetKind.GPU, gpu_reservation_key(attempt_id), stage_budget.gpu_seconds_limit),
    ):
        identity: dict[str, str | int] = {
            "execution_attempt_id": str(attempt_id),
            "node_key": node_key,
            "purpose": "terminalgen_authoring",
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


__all__ = ["TerminalGenReadinessRuntime", "TerminalGenRuntimeError"]
