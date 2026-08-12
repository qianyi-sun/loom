"""Concrete `behavior-recovery@1` implementation of the generic #1214 seam."""

from __future__ import annotations

import hashlib
from collections.abc import Iterator
from typing import Annotated
from uuid import UUID

from pydantic import Field, StringConstraints, field_validator, model_validator

from loom.integrations.behavior.contracts import (
    ArtifactRefV1,
    ArtifactTaskBundleRefV1,
    BehaviorTaskInstanceArtifactV1,
    BehaviorTaskInstancePayloadV1,
    BehaviorTasksetSnapshotArtifactV1,
    BehaviorTasksetSnapshotPayloadV1,
    CompanionInputsV1,
    ControlArtifactProvenanceV1,
    DatasetCompatibilityV1,
    DownstreamLimitsV1,
    EmbeddedTaskInstanceV1,
    MaterializationRefV1,
    MaterializedFanoutRefV1,
    MopBankCompatibilityV1,
    RecipeRefV1,
    SourceTaskSetRefV1,
    TaskInstanceLineageV1,
    TaskSnapshotRowV1,
)
from loom.pipeline.input_materialization import (
    InputMaterializationRequestV1,
    MaterializationDeclarationV1,
    MaterializationOutputV1,
    MaterializationResultBindingV1,
    MaterializationSourceArtifactRefV1,
    MaterializedArtifactDocumentV1,
    RecipeInputMaterializer,
)
from loom.pipeline.keys import canonical_digest, canonical_identity
from loom.pipeline.spec import (
    FanoutArtifactBindingV1,
    FanoutManifestItemV1,
    FanoutManifestV1,
    PipelineModel,
)

CHILD_MAX_BYTES = 1_048_576
FANOUT_MAX_BYTES = 16_777_216
SNAPSHOT_MAX_BYTES = 16_777_216
MAX_TASK_INSTANCES = 200


class BehaviorMaterializationSourceSnapshotV1(PipelineModel):
    source_task_set: SourceTaskSetRefV1
    tasks: Annotated[list[TaskSnapshotRowV1], Field(min_length=1, max_length=200)]
    companion_inputs: CompanionInputsV1
    dataset_compatibility: DatasetCompatibilityV1
    mop_bank_compatibility: MopBankCompatibilityV1
    control_event_id: UUID
    loom_commit_sha: Annotated[str, StringConstraints(pattern=r"^[0-9a-f]{40}$")]
    caller_idempotency_key: Annotated[str, StringConstraints(min_length=1, max_length=128)]

    @field_validator("tasks")
    @classmethod
    def tasks_are_identity_sorted(cls, values: list[TaskSnapshotRowV1]) -> list[TaskSnapshotRowV1]:
        if [item.loom_task_id for item in values] != sorted(
            (item.loom_task_id for item in values), key=lambda item: item.encode("utf-8")
        ):
            raise ValueError("source Task rows must be bytewise loom_task_id sorted")
        if len({item.loom_task_id for item in values}) != len(values) or len(
            {item.behavior_task_id for item in values}
        ) != len(values):
            raise ValueError("source Task identities must be unique")
        return values

    @model_validator(mode="after")
    def semantic_coverage_is_exact(self) -> BehaviorMaterializationSourceSnapshotV1:
        if self.source_task_set.owning_team_id is None:  # defensive, UUID cannot be null
            raise ValueError("TaskSet owning team is required")
        dataset = {
            item.behavior_task_id: item for item in self.dataset_compatibility.test_instance_sets
        }
        cards = {
            item.behavior_task_id: item for item in self.dataset_compatibility.agentic_task_cards
        }
        demos = {
            item.behavior_task_id: item
            for item in self.dataset_compatibility.agentic_demo_video_sets
        }
        coverage = {
            item.behavior_task_id: item for item in self.mop_bank_compatibility.task_coverage
        }
        if (
            self.dataset_compatibility.task_universe_sha256
            != self.mop_bank_compatibility.task_universe_sha256
        ):
            raise ValueError("dataset and MOP task universes differ")
        for task in self.tasks:
            signed = dataset.get(task.behavior_task_id)
            if (
                signed is None
                or task.task_name != signed.task_name
                or task.engine_task_instance_ids != signed.engine_task_instance_ids
                or task.behavior_task_id not in cards
                or task.behavior_task_id not in demos
                or task.behavior_task_id not in coverage
            ):
                raise ValueError("Task row lacks byte-identical dataset/MOP semantic coverage")
            if any(not demos[task.behavior_task_id].episodes for _ in (0,)):
                raise ValueError("Task row has no demo coverage")
        return self


class BehaviorMaterializationParametersV1(PipelineModel):
    episodes_per_instance: Annotated[int, Field(strict=True, ge=1, le=10)] = 1
    seed_base: Annotated[int, Field(strict=True, ge=0, le=4_294_967_295)] = 0


def _source_refs(snapshot: BehaviorMaterializationSourceSnapshotV1) -> list[ArtifactRefV1]:
    refs = [
        ArtifactRefV1(
            artifact_id=item.artifact_id,
            artifact_type=item.artifact_type,
            manifest_sha256=item.manifest_sha256,
        )
        for item in (
            snapshot.companion_inputs.dataset,
            snapshot.companion_inputs.policy,
            snapshot.companion_inputs.mop_bank,
        )
    ]
    refs.extend(
        ArtifactRefV1(
            artifact_id=task.task_bundle.artifact_id,
            artifact_type=task.task_bundle.artifact_type,
            manifest_sha256=task.task_bundle.manifest_sha256,
        )
        for task in snapshot.tasks
        if isinstance(task.task_bundle, ArtifactTaskBundleRefV1)
    )
    by_id = {item.artifact_id: item for item in refs}
    if len(by_id) != len(refs):
        for item in refs:
            if by_id[item.artifact_id] != item:
                raise ValueError("source Artifact UUID is reused with divergent identity")
    return sorted(by_id.values(), key=lambda item: item.artifact_id.bytes)


def _materialization_ref(
    request: InputMaterializationRequestV1,
    snapshot: BehaviorMaterializationSourceSnapshotV1,
    parameters: BehaviorMaterializationParametersV1,
) -> MaterializationRefV1:
    return MaterializationRefV1(
        episodes_per_instance=parameters.episodes_per_instance,
        seed_base=parameters.seed_base,
        request_sha256=canonical_digest(
            {
                "dataset_content_sha256": snapshot.companion_inputs.dataset.content_sha256,
                "episodes_per_instance": parameters.episodes_per_instance,
                "mop_bank_content_sha256": snapshot.companion_inputs.mop_bank.content_sha256,
                "policy_content_sha256": snapshot.companion_inputs.policy.content_sha256,
                "recipe_digest": request.recipe.digest,
                "seed_base": parameters.seed_base,
                "source_task_set_manifest_sha256": snapshot.source_task_set.manifest_sha256,
            },
            persisted=False,
        ),
    )


def _task_instance_payloads(
    request: InputMaterializationRequestV1,
    snapshot: BehaviorMaterializationSourceSnapshotV1,
    parameters: BehaviorMaterializationParametersV1,
) -> list[BehaviorTaskInstancePayloadV1]:
    materialization = _materialization_ref(request, snapshot, parameters)
    recipe = RecipeRefV1(name="behavior-recovery", version=1, digest=request.recipe.digest)
    payloads: list[BehaviorTaskInstancePayloadV1] = []
    for task in snapshot.tasks:
        task_bundle_digest = (
            task.task_bundle.content_sha256
            if isinstance(task.task_bundle, ArtifactTaskBundleRefV1)
            else task.task_bundle.object_sha256
        )
        for selector in task.eligible_eval_instance_ids:
            engine_id = task.engine_task_instance_ids[selector]
            for episode in range(parameters.episodes_per_instance):
                demo_id = task.behavior_task_id * 10_000 + engine_id * 10 + episode
                if demo_id > 4_294_967_295:
                    raise ValueError("demo ID overflows uint32")
                seed_preimage = {
                    "engine_task_instance_id": engine_id,
                    "episode_index": episode,
                    "eval_instance_index": selector,
                    "seed_base": parameters.seed_base,
                    "task_checksum": task.task_checksum,
                }
                seed = int.from_bytes(
                    hashlib.sha256(canonical_identity(seed_preimage)).digest()[:4],
                    "big",
                )
                identity = canonical_digest(
                    {
                        "behavior_task_id": task.behavior_task_id,
                        "demo_id": demo_id,
                        "engine_task_instance_id": engine_id,
                        "episode_index": episode,
                        "eval_instance_index": selector,
                        "recipe_digest": request.recipe.digest,
                        "seed": seed,
                        "task_bundle_digest": task_bundle_digest,
                    },
                    persisted=False,
                ).removeprefix("sha256:")
                payloads.append(
                    BehaviorTaskInstancePayloadV1(
                        source_task_set=snapshot.source_task_set,
                        loom_task_id=task.loom_task_id,
                        behavior_task_id=task.behavior_task_id,
                        task_name=task.task_name,
                        semantic_task_id=task.semantic_task_id,
                        task_checksum=task.task_checksum,
                        task_bundle_digest=task_bundle_digest,
                        task_bundle=task.task_bundle,
                        source_bddl_path=task.source_bddl_path,
                        eval_instance_index=selector,
                        engine_task_instance_id=engine_id,
                        episode_index=episode,
                        demo_id=demo_id,
                        demo_stem=f"episode_{demo_id:08d}",
                        seed=seed,
                        task_instance_identity=identity,
                        materialization=materialization,
                        recipe=recipe,
                        lineage=TaskInstanceLineageV1(
                            source_task_set_manifest_sha256=(
                                snapshot.source_task_set.manifest_sha256
                            ),
                            task_bundle=task.task_bundle,
                            materialization_request_sha256=materialization.request_sha256,
                            dataset_content_sha256=(
                                snapshot.companion_inputs.dataset.content_sha256
                            ),
                            policy_content_sha256=(snapshot.companion_inputs.policy.content_sha256),
                            mop_bank_content_sha256=(
                                snapshot.companion_inputs.mop_bank.content_sha256
                            ),
                        ),
                    )
                )
    payloads.sort(key=lambda item: item.task_instance_identity.encode("utf-8"))
    if not 1 <= len(payloads) <= MAX_TASK_INSTANCES:
        raise ValueError("materialization must produce 1..200 task instances")
    identities = [item.task_instance_identity for item in payloads]
    demos = [
        (item.behavior_task_id, item.engine_task_instance_id, item.episode_index)
        for item in payloads
    ]
    if len(identities) != len(set(identities)) or len(demos) != len(set(demos)):
        raise ValueError("materialized task instance identity/demo tuple collided")
    return payloads


class BehaviorRecipeInputMaterializer:
    """Pure deterministic declaration/render adapter for `behavior-recovery@1`."""

    materializer_kind = "behavior-recovery"
    official_one_kind = "behavior_one_task_instance_v1"

    def _validated(
        self, frozen_request: InputMaterializationRequestV1
    ) -> tuple[
        BehaviorMaterializationSourceSnapshotV1,
        BehaviorMaterializationParametersV1,
        list[BehaviorTaskInstancePayloadV1],
        list[ArtifactRefV1],
    ]:
        if frozen_request.recipe.name != "behavior-recovery" or frozen_request.recipe.version != 1:
            raise ValueError("BEHAVIOR materializer accepts only behavior-recovery@1")
        snapshot = BehaviorMaterializationSourceSnapshotV1.model_validate_json(
            canonical_identity(frozen_request.source_snapshot)
        )
        if snapshot.source_task_set.owning_team_id != frozen_request.team_id:
            raise ValueError("source TaskSet is cross-team")
        parameters = BehaviorMaterializationParametersV1.model_validate(frozen_request.parameters)
        payloads = _task_instance_payloads(frozen_request, snapshot, parameters)
        return snapshot, parameters, payloads, _source_refs(snapshot)

    def declare(
        self, *, frozen_request: InputMaterializationRequestV1
    ) -> MaterializationDeclarationV1:
        snapshot, parameters, payloads, refs = self._validated(frozen_request)
        lineage_ids = [item.artifact_id for item in refs]
        outputs = [
            MaterializationOutputV1(
                logical_name=f"task_instance_{ordinal:012d}",
                artifact_type="behavior_task_instance.v1",
                max_bytes=CHILD_MAX_BYTES,
                lineage_source_ids=lineage_ids,
            )
            for ordinal in range(len(payloads))
        ]
        outputs.extend(
            [
                MaterializationOutputV1(
                    logical_name="task_instances",
                    artifact_type="loom.fanout-manifest.v1",
                    max_bytes=FANOUT_MAX_BYTES,
                    lineage_source_ids=lineage_ids,
                ),
                MaterializationOutputV1(
                    logical_name="task_set",
                    artifact_type="behavior_taskset_snapshot.v1",
                    max_bytes=SNAPSHOT_MAX_BYTES,
                    lineage_source_ids=lineage_ids,
                ),
            ]
        )
        result_bindings = [
            MaterializationResultBindingV1(graph_input_name="task_set", logical_name="task_set"),
            MaterializationResultBindingV1(
                graph_input_name="task_instances", logical_name="task_instances"
            ),
        ]
        source_artifact_refs = [
            MaterializationSourceArtifactRefV1(
                artifact_id=item.artifact_id,
                artifact_type=item.artifact_type,
                manifest_sha256=item.manifest_sha256,
            )
            for item in refs
        ]
        raw = {
            "outputs": outputs,
            "result_bindings": result_bindings,
            "source_artifact_refs": source_artifact_refs,
        }
        materialization_identity = canonical_digest(
            {
                "caller_idempotency_key": snapshot.caller_idempotency_key,
                "downstream_limits": {
                    "max_failure_cases_per_task": 4,
                    "max_failure_cases_total": 800,
                },
                "outputs": outputs,
                "parameters": parameters,
                "recipe": frozen_request.recipe,
                "source_artifact_refs": raw["source_artifact_refs"],
                "source_task_set": snapshot.source_task_set,
                "task_payload_digests": [canonical_digest(item) for item in payloads],
                "team_id": frozen_request.team_id,
            },
            persisted=False,
        )
        declaration = MaterializationDeclarationV1(
            outputs=outputs,
            result_bindings=result_bindings,
            source_artifact_refs=source_artifact_refs,
            materialization_identity_digest=materialization_identity,
        )
        self._render_states[declaration.materialization_identity_digest] = (
            frozen_request,
            snapshot,
            parameters,
            payloads,
            refs,
        )
        return declaration

    def render(
        self,
        *,
        declaration: MaterializationDeclarationV1,
        artifact_ids: dict[str, UUID],
    ) -> Iterator[MaterializedArtifactDocumentV1]:
        # render() is intentionally pure.  The declaration carries no mutable
        # lookup handles, so the frozen request-specific values are cached only
        # for the duration of declare+render on this adapter instance.
        state = self._render_states.get(declaration.materialization_identity_digest)
        if state is None:
            raise ValueError("declare must run on this adapter before render")
        request, snapshot, parameters, payloads, refs = state
        expected_names = [item.logical_name for item in declaration.outputs]
        if set(artifact_ids) != set(expected_names):
            raise ValueError("service UUID map differs from the declaration")
        provenance = ControlArtifactProvenanceV1(
            producer_kind="control",
            loom_commit_sha=snapshot.loom_commit_sha,
            control_event_id=snapshot.control_event_id,
            actor_id=request.actor_user_id,
            recipe_digest=request.recipe.digest,
            source_artifacts=refs,
        )
        embedded: list[EmbeddedTaskInstanceV1] = []
        for ordinal, payload in enumerate(payloads):
            logical_name = f"task_instance_{ordinal:012d}"
            artifact = BehaviorTaskInstanceArtifactV1(
                schema_version="behavior_task_instance.v1",
                payload=payload,
                files=[],
                provenance=provenance,
            )
            embedded.append(
                EmbeddedTaskInstanceV1(
                    artifact_id=artifact_ids[logical_name],
                    payload_sha256=canonical_digest(payload),
                    payload=payload,
                )
            )
            yield MaterializedArtifactDocumentV1(
                logical_name=logical_name, value=artifact.model_dump(mode="json")
            )
        fanout = FanoutManifestV1(
            schema_version="loom.fanout-manifest.v1",
            items=[
                FanoutManifestItemV1(
                    shard_key=payload.task_instance_identity,
                    artifact_bindings=[
                        FanoutArtifactBindingV1(
                            name="task_instance",
                            artifact_type="behavior_task_instance.v1",
                            artifact_id=embedded_item.artifact_id,
                        )
                    ],
                    parameters={
                        "eval_instance_index": payload.eval_instance_index,
                        "episode_index": payload.episode_index,
                        "seed": payload.seed,
                        "record_depth": False,
                        "recording_fps": 30,
                    },
                )
                for payload, embedded_item in zip(payloads, embedded, strict=True)
            ],
        )
        fanout_value = fanout.model_dump(mode="json")
        yield MaterializedArtifactDocumentV1(logical_name="task_instances", value=fanout_value)
        snapshot_payload = BehaviorTasksetSnapshotPayloadV1(
            source_task_set=snapshot.source_task_set,
            tasks=snapshot.tasks,
            companion_inputs=snapshot.companion_inputs,
            materialization=_materialization_ref(request, snapshot, parameters),
            task_instances=embedded,
            task_instances_fanout=MaterializedFanoutRefV1(
                artifact_id=artifact_ids["task_instances"],
                artifact_type="loom.fanout-manifest.v1",
                content_sha256=canonical_digest(fanout_value),
            ),
            downstream_limits=DownstreamLimitsV1(
                max_failure_cases_per_task=4, max_failure_cases_total=800
            ),
            recipe=RecipeRefV1(name="behavior-recovery", version=1, digest=request.recipe.digest),
            loom_commit=snapshot.loom_commit_sha,
            created_by=request.actor_user_id,
        )
        snapshot_artifact = BehaviorTasksetSnapshotArtifactV1(
            schema_version="behavior_taskset_snapshot.v1",
            payload=snapshot_payload,
            files=[],
            provenance=provenance,
        )
        yield MaterializedArtifactDocumentV1(
            logical_name="task_set", value=snapshot_artifact.model_dump(mode="json")
        )

    def __init__(self) -> None:
        self._render_states: dict[
            str,
            tuple[
                InputMaterializationRequestV1,
                BehaviorMaterializationSourceSnapshotV1,
                BehaviorMaterializationParametersV1,
                list[BehaviorTaskInstancePayloadV1],
                list[ArtifactRefV1],
            ],
        ] = {}


class BehaviorOneTaskInstanceMaterializer(BehaviorRecipeInputMaterializer):
    """Internal-only strict N=1 adapter used by the generic official grant."""

    materializer_kind = "behavior_one_task_instance_v1"

    def _validated(
        self, frozen_request: InputMaterializationRequestV1
    ) -> tuple[
        BehaviorMaterializationSourceSnapshotV1,
        BehaviorMaterializationParametersV1,
        list[BehaviorTaskInstancePayloadV1],
        list[ArtifactRefV1],
    ]:
        snapshot, parameters, payloads, refs = super()._validated(frozen_request)
        if (
            len(snapshot.tasks) != 1
            or len(snapshot.tasks[0].eligible_eval_instance_ids) != 1
            or parameters.episodes_per_instance != 1
            or len(payloads) != 1
        ):
            raise ValueError("invalid_one_child_tuple")
        return snapshot, parameters, payloads, refs


def behavior_materializer_registry() -> dict[str, RecipeInputMaterializer]:
    """Return the explicit public/internal registry; there is no fallback key."""

    return {
        "behavior-recovery": BehaviorRecipeInputMaterializer(),
        "behavior_one_task_instance_v1": BehaviorOneTaskInstanceMaterializer(),
    }


__all__ = [
    "BehaviorMaterializationParametersV1",
    "BehaviorMaterializationSourceSnapshotV1",
    "BehaviorOneTaskInstanceMaterializer",
    "BehaviorRecipeInputMaterializer",
    "behavior_materializer_registry",
]
