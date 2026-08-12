"""Loom-native whole-episode predicate-log offline judge adapter.

The adapter treats every model-written byte as untrusted.  It validates the
three immutable input Artifacts, the frozen control snapshot, provider assets,
and the final report/seed pair before the worker-owned AttemptWorkspace may
publish anything.
"""

from __future__ import annotations

import json
import os
import stat
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Protocol, TypeVar, cast

from loom.integrations.behavior.agentic_sweep import validate_sweep_outputs
from loom.integrations.behavior.canonical_json import (
    canonical_digest,
    canonical_document,
    digest_bytes,
)
from loom.integrations.behavior.cli import StageAdapterBinding
from loom.integrations.behavior.contracts import (
    ArtifactFileV1,
    ArtifactRefV1,
    BehaviorDatasetSnapshotArtifactV1,
    BehaviorInspectionReportArtifactV1,
    BehaviorInspectionReportPayloadV1,
    BehaviorRolloutBundleArtifactV1,
    BehaviorStage,
    BehaviorTaskInstanceArtifactV1,
    ContentArtifactRefV1,
    DatasetCompatibilityV1,
    JudgeProfileProvenanceV1,
    OfflineJudgeParametersV1,
    PipelineArtifactProvenanceV1,
    ProviderUsageV1,
    StageRequestV1,
)
from loom.integrations.behavior.errors import BehaviorContractError
from loom.integrations.behavior.offline_judge_assets import (
    ProviderAssetBundle,
    compose_sweep_prompt,
)
from loom.integrations.behavior.stages.rollout import (
    BddlTransitionsDocumentV1,
    RolloutSceneProjectionV1,
)
from loom.pipeline.control_bindings import (
    JudgeExecutionProfileV1,
    control_snapshot_digest,
    validate_registered_judge_adapter,
)
from loom.pipeline.spec import BindingItemV1, BindingSetV1, PipelineModel
from loom.pipeline.state import (
    RetryClass,
    StageResultInputV1,
    StageResultOutputV1,
    StageResultProvenanceV1,
    StageResultV1,
)
from loom_worker.pipeline_attempt_workspace import AttemptWorkspace

INPUT_ROOT = Path("/inputs")
ASSET_ROOT = Path("/opt/behavior/provider-assets/behavior_offline_judge")
JUDGE_OUTPUT_ROOT = Path("/outputs/judge")
CONTROL_BINDING_PATH = Path("/inputs/control-binding.json")
OFFLINE_JUDGE_OUTPUT_DECLARATIONS: Mapping[str, str] = {
    "inspection": "behavior_inspection_report.v1"
}


@dataclass(frozen=True)
class OfflineJudgePaths:
    input_root: Path = INPUT_ROOT
    asset_root: Path = ASSET_ROOT
    judge_output_root: Path = JUDGE_OUTPUT_ROOT
    control_binding_path: Path = CONTROL_BINDING_PATH

    def artifact_root(self, binding_name: str) -> Path:
        return self.input_root / binding_name


_ModelT = TypeVar("_ModelT", bound=PipelineModel)


@dataclass(frozen=True)
class OfflineJudgeBindings:
    task_instance: BindingSetV1
    rollout: BindingSetV1
    dataset: BindingSetV1


@dataclass(frozen=True)
class MountedOfflineJudgeInputs:
    task_instance: BehaviorTaskInstanceArtifactV1
    rollout: BehaviorRolloutBundleArtifactV1
    dataset: BehaviorDatasetSnapshotArtifactV1
    bindings: OfflineJudgeBindings
    task_card: bytes


@dataclass(frozen=True)
class OfflineJudgeAuthority:
    """The full immutable profile, materialized by the worker from the claim."""

    profile: JudgeExecutionProfileV1


@dataclass(frozen=True)
class OfflineJudgeRunRequest:
    request: StageRequestV1
    inputs: MountedOfflineJudgeInputs
    profile: JudgeExecutionProfileV1
    assets: ProviderAssetBundle
    prompt: bytes


@dataclass(frozen=True)
class OfflineJudgeRunResult:
    """Trusted runner result; usage is the loopback shim's settlement total."""

    report: bytes
    seed: bytes
    usage: ProviderUsageV1
    child_exit_code: int = 0


class OfflineJudgeRunner(Protocol):
    def run(self, run: OfflineJudgeRunRequest) -> OfflineJudgeRunResult: ...


class FrozenJudgeAuthorityReader(Protocol):
    def read(self, request: StageRequestV1, path: Path) -> OfflineJudgeAuthority: ...


class MountedFrozenJudgeAuthorityReader:
    """Read the worker-materialized frozen profile before any JWT/network."""

    def read(self, request: StageRequestV1, path: Path) -> OfflineJudgeAuthority:
        raw = _read_canonical_file(path, limit=4 * 1024 * 1024)
        try:
            profile = JudgeExecutionProfileV1.model_validate_json(raw)
            validate_registered_judge_adapter(profile)
        except ValueError as exc:
            raise BehaviorContractError("frozen judge profile is invalid") from exc
        control = request.provenance.control_binding
        if not isinstance(control, JudgeProfileProvenanceV1):
            raise BehaviorContractError("offline judge lacks Judge profile provenance")
        expected = (
            control.object_id,
            control.version,
            control.snapshot_sha256,
            control.provider_asset_manifest_sha256,
            control.judge_profile_id,
            control.judge_profile_version,
            control.agent,
            control.agent_version,
            control.provider,
            control.model,
        )
        actual = (
            profile.profile_id,
            profile.version,
            control_snapshot_digest(profile),
            profile.provider_asset_manifest_sha256,
            profile.profile_id,
            profile.version,
            profile.agent_name,
            profile.agent_version,
            profile.provider,
            profile.model,
        )
        if actual != expected:
            raise BehaviorContractError("frozen judge profile disagrees with StageRequest")
        limits = request.budget.provider
        if limits is None or (
            limits.provider_request_limit_per_attempt,
            limits.provider_cost_limit_microusd_per_attempt,
            limits.per_call_timeout_seconds,
        ) != (
            profile.provider_request_limit_per_attempt,
            profile.provider_cost_limit_microusd_per_attempt,
            profile.per_call_timeout_seconds,
        ):
            raise BehaviorContractError("Stage budget disagrees with the frozen judge profile")
        return OfflineJudgeAuthority(profile=profile)


def validate_offline_judge_request(request: StageRequestV1) -> OfflineJudgeBindings:
    if request.stage is not BehaviorStage.OFFLINE_JUDGE or not isinstance(
        request.parameters, OfflineJudgeParametersV1
    ):
        raise BehaviorContractError("offline judge accepts only predicate-log StageRequestV1")
    if request.parameters.inspection_mode != "whole_episode_predicate_log":
        raise BehaviorContractError("video-only judging is forbidden")
    expected = [
        ("task_instance", "behavior_task_instance.v1"),
        ("rollout", "behavior_rollout_bundle.v1"),
        ("dataset", "behavior_dataset_snapshot.v1"),
    ]
    actual = [(item.binding_name, item.artifact_type) for item in request.inputs]
    if actual != expected:
        raise BehaviorContractError("offline judge requires the exact ordered three bindings")
    if any(
        binding.cardinality != "one"
        or len(binding.items) != 1
        or binding.items[0].item_key != "singleton"
        for binding in request.inputs
    ):
        raise BehaviorContractError("offline judge inputs must be scalar singleton bindings")
    return OfflineJudgeBindings(*request.inputs)


def load_mounted_offline_judge_inputs(
    request: StageRequestV1, paths: OfflineJudgePaths
) -> MountedOfflineJudgeInputs:
    bindings = validate_offline_judge_request(request)
    task = cast(
        BehaviorTaskInstanceArtifactV1,
        _load_mounted_artifact(
            bindings.task_instance,
            paths.artifact_root("task_instance"),
            BehaviorTaskInstanceArtifactV1,
        ),
    )
    rollout = cast(
        BehaviorRolloutBundleArtifactV1,
        _load_mounted_artifact(
            bindings.rollout,
            paths.artifact_root("rollout"),
            BehaviorRolloutBundleArtifactV1,
        ),
    )
    dataset = cast(
        BehaviorDatasetSnapshotArtifactV1,
        _load_mounted_artifact(
            bindings.dataset,
            paths.artifact_root("dataset"),
            BehaviorDatasetSnapshotArtifactV1,
        ),
    )
    task_card = _validate_inputs(request, task, rollout, dataset, bindings, paths)
    return MountedOfflineJudgeInputs(task, rollout, dataset, bindings, task_card)


class OfflineJudgeAdapter:
    def __init__(
        self,
        *,
        runner: OfflineJudgeRunner,
        authority_reader: FrozenJudgeAuthorityReader,
        paths: OfflineJudgePaths | None = None,
    ) -> None:
        self.runner = runner
        self.authority_reader = authority_reader
        self.paths = paths or OfflineJudgePaths()

    def __call__(self, request: StageRequestV1, workspace: AttemptWorkspace) -> StageResultV1:
        inputs = load_mounted_offline_judge_inputs(request, self.paths)
        authority = self.authority_reader.read(request, self.paths.control_binding_path)
        control = cast(JudgeProfileProvenanceV1, request.provenance.control_binding)
        assets = ProviderAssetBundle.load(
            self.paths.asset_root,
            expected_manifest_sha256=control.provider_asset_manifest_sha256,
        )
        _validate_asset_profile(authority.profile, assets)
        task = inputs.task_instance.payload
        prompt = compose_sweep_prompt(assets, inputs.task_card, task.behavior_task_id, task.demo_id)
        outcome = self.runner.run(
            OfflineJudgeRunRequest(request, inputs, authority.profile, assets, prompt)
        )
        limits = request.budget.provider
        assert limits is not None
        if (
            outcome.usage.request_count > limits.provider_request_limit_per_attempt
            or outcome.usage.cost_microusd > limits.provider_cost_limit_microusd_per_attempt
        ):
            raise BehaviorContractError("Gateway settlement exceeds the frozen Attempt budget")
        rollout = inputs.rollout.payload
        validated = validate_sweep_outputs(
            outcome.report,
            outcome.seed,
            task_name=task.task_name,
            engine_task_instance_id=task.engine_task_instance_id,
            task_id=task.behavior_task_id,
            demo_id=task.demo_id,
            n_steps=rollout.step_count,
            rollout_artifact_id=str(bindings_item(inputs.bindings.rollout).artifact_id),
        )
        report_sha = digest_bytes(validated.report)
        seed_sha = digest_bytes(validated.seed)
        workspace.write_payload_bytes("inspection", "payload/report.md", validated.report)
        workspace.write_payload_bytes("inspection", "payload/seed.json", validated.seed)
        artifact = BehaviorInspectionReportArtifactV1(
            schema_version="behavior_inspection_report.v1",
            payload=BehaviorInspectionReportPayloadV1(
                task_instance_identity=task.task_instance_identity,
                task_id=task.behavior_task_id,
                eval_instance_index=task.eval_instance_index,
                engine_task_instance_id=task.engine_task_instance_id,
                episode_index=task.episode_index,
                demo_id=task.demo_id,
                episode=task.demo_id,
                n_steps=rollout.step_count,
                rollout_artifact_id=bindings_item(inputs.bindings.rollout).artifact_id,
                task_card_sha256=digest_bytes(inputs.task_card),
                prompt_sha256=digest_bytes(prompt),
                report_sha256=report_sha,
                seed_sha256=seed_sha,
                judge_profile_id=authority.profile.profile_id,
                judge_profile_version=authority.profile.version,
                judge_profile_sha256=control_snapshot_digest(authority.profile),
                agent=authority.profile.agent_name,
                agent_version=authority.profile.agent_version,
                provider=authority.profile.provider,
                model=authority.profile.model,
                control_binding_sha256=control.snapshot_sha256,
                mcp_server_locks_sha256=assets.runner_lock.mcp_server_locks_sha256,
                usage=outcome.usage,
                seed_count=validated.seed_count,
                learn_count=validated.learn_count,
            ),
            files=[
                ArtifactFileV1(
                    name="inspection_report",
                    relative_path="payload/report.md",
                    sha256=report_sha,
                    size_bytes=len(validated.report),
                    media_type="text/markdown",
                    required=True,
                ),
                ArtifactFileV1(
                    name="judgement_seed",
                    relative_path="payload/seed.json",
                    sha256=seed_sha,
                    size_bytes=len(validated.seed),
                    media_type="application/json",
                    required=True,
                ),
            ],
            provenance=_artifact_provenance(request),
        )
        workspace.write_artifact_json("inspection", artifact)
        return _stage_result(request, rollout.step_count)


def offline_judge_stage_binding(
    *,
    runner: OfflineJudgeRunner,
    authority_reader: FrozenJudgeAuthorityReader,
    paths: OfflineJudgePaths | None = None,
) -> StageAdapterBinding:
    return StageAdapterBinding(
        adapter=OfflineJudgeAdapter(runner=runner, authority_reader=authority_reader, paths=paths),
        output_declarations=OFFLINE_JUDGE_OUTPUT_DECLARATIONS,
    )


def _validate_inputs(
    request: StageRequestV1,
    task: BehaviorTaskInstanceArtifactV1,
    rollout: BehaviorRolloutBundleArtifactV1,
    dataset: BehaviorDatasetSnapshotArtifactV1,
    bindings: OfflineJudgeBindings,
    paths: OfflineJudgePaths,
) -> bytes:
    child = task.payload
    episode = rollout.payload
    dataset_ref = bindings_item(bindings.dataset)
    task_ref = bindings_item(bindings.task_instance)
    expected = (
        child.task_instance_identity,
        child.loom_task_id,
        child.behavior_task_id,
        child.task_name,
        child.task_checksum,
        child.task_bundle_digest,
        child.source_bddl_path,
        child.eval_instance_index,
        child.engine_task_instance_id,
        child.episode_index,
        child.demo_id,
        child.demo_stem,
        child.seed,
    )
    actual = (
        episode.task_instance_identity,
        episode.loom_task_id,
        episode.behavior_task_id,
        episode.task_name,
        episode.task_checksum,
        episode.task_bundle_digest,
        episode.source_bddl_path,
        episode.eval_instance_index,
        episode.engine_task_instance_id,
        episode.episode_index,
        episode.demo_id,
        episode.demo_stem,
        episode.seed,
    )
    if actual != expected or episode.recording_fps != 30:
        raise BehaviorContractError("TaskInstance and rollout signed identities disagree")
    expected_dataset = ContentArtifactRefV1(
        artifact_id=dataset_ref.artifact_id,
        artifact_type=bindings.dataset.artifact_type,
        manifest_sha256=dataset_ref.manifest_sha256,
        content_sha256=dataset_ref.content_sha256,
    )
    if (
        episode.dataset != expected_dataset
        or child.lineage.dataset_content_sha256 != dataset_ref.content_sha256
    ):
        raise BehaviorContractError("dataset lineage disagrees across task and rollout")
    sources = {item.artifact_id: item for item in rollout.provenance.source_artifacts}
    if task_ref.artifact_id not in sources or dataset_ref.artifact_id not in sources:
        raise BehaviorContractError("rollout provenance lacks task/dataset source Artifacts")
    expected_sources = {
        item.artifact_id: ArtifactRefV1(
            artifact_id=item.artifact_id,
            artifact_type=binding.artifact_type,
            manifest_sha256=item.manifest_sha256,
        )
        for binding, item in (
            (bindings.task_instance, task_ref),
            (bindings.dataset, dataset_ref),
        )
    }
    if any(sources[artifact_id] != expected for artifact_id, expected in expected_sources.items()):
        raise BehaviorContractError("rollout source Artifact identity drift")
    compatibility = dataset.payload.compatibility
    if not isinstance(compatibility, DatasetCompatibilityV1):
        raise BehaviorContractError("dataset compatibility branch drift")
    task_rows = [
        row
        for row in compatibility.test_instance_sets
        if row.behavior_task_id == child.behavior_task_id
    ]
    if len(task_rows) != 1 or task_rows[0].task_name != child.task_name:
        raise BehaviorContractError("dataset universe lacks one exact task row")
    row = task_rows[0]
    if child.eval_instance_index >= len(row.engine_task_instance_ids):
        raise BehaviorContractError("eval selector is outside the signed engine ID array")
    if row.engine_task_instance_ids[child.eval_instance_index] != child.engine_task_instance_id:
        raise BehaviorContractError("eval selector does not resolve to the signed engine ID")
    cards = [
        item
        for item in compatibility.agentic_task_cards
        if item.behavior_task_id == child.behavior_task_id
    ]
    if len(cards) != 1 or cards[0].task_name != child.task_name:
        raise BehaviorContractError("dataset has no unique selected task card")
    card = cards[0]
    card_descriptor = _declared_file(dataset, f"payload/{card.relative_path}")
    if (card_descriptor.sha256, card_descriptor.size_bytes) != (card.sha256, card.size_bytes):
        raise BehaviorContractError("task card semantic and file manifests disagree")
    card_bytes = _read_declared(
        paths.artifact_root("dataset"),
        card_descriptor.relative_path,
        card_descriptor.size_bytes,
        card_descriptor.sha256,
    )
    demo_sets = [
        item
        for item in compatibility.agentic_demo_video_sets
        if item.behavior_task_id == child.behavior_task_id
    ]
    if len(demo_sets) != 1:
        raise BehaviorContractError("dataset has no unique selected demo-video set")
    for demo_episode in demo_sets[0].episodes:
        for video in demo_episode.files:
            demo_descriptor = _declared_file(dataset, f"payload/{video.relative_path}")
            if (demo_descriptor.sha256, demo_descriptor.size_bytes) != (
                video.sha256,
                video.size_bytes,
            ):
                raise BehaviorContractError("demo video semantic and file manifests disagree")
            _read_declared(
                paths.artifact_root("dataset"),
                demo_descriptor.relative_path,
                demo_descriptor.size_bytes,
                demo_descriptor.sha256,
            )
    by_role = {item.role: item for item in episode.required_file_descriptors}
    payloads: dict[str, bytes] = {}
    for role in (
        "rollout_hdf5",
        "bddl_transitions",
        "scene_metadata",
        "rgb_head",
        "rgb_left_wrist",
        "rgb_right_wrist",
        "rgb_composite",
    ):
        descriptor = by_role[role]
        payloads[role] = _read_declared(
            paths.artifact_root("rollout"),
            descriptor.relative_path,
            descriptor.size_bytes,
            descriptor.sha256,
        )
    transitions = _parse_model(payloads["bddl_transitions"], BddlTransitionsDocumentV1)
    scene = _parse_model(payloads["scene_metadata"], RolloutSceneProjectionV1)
    if (
        transitions.task_name,
        transitions.instance_id,
        transitions.demo_id,
        transitions.total_steps,
        transitions.success,
    ) != (
        child.task_name,
        child.engine_task_instance_id,
        child.demo_id,
        episode.step_count,
        episode.success,
    ):
        raise BehaviorContractError("BDDL transition identity disagrees with the rollout")
    if (scene.task_name, scene.instance_id, scene.demo_id) != (
        child.task_name,
        child.engine_task_instance_id,
        child.demo_id,
    ):
        raise BehaviorContractError("scene projection identity disagrees with the rollout")
    if request.run_id != rollout.provenance.pipeline_run_id:
        raise BehaviorContractError("rollout is not from this PipelineRun")
    return card_bytes


def _asset_provenance_source(request: StageRequestV1) -> list[ArtifactRefV1]:
    return sorted(
        [
            ArtifactRefV1(
                artifact_id=item.artifact_id,
                artifact_type=binding.artifact_type,
                manifest_sha256=item.manifest_sha256,
            )
            for binding in request.inputs
            for item in binding.items
        ],
        key=lambda item: item.artifact_id.bytes,
    )


def _artifact_provenance(request: StageRequestV1) -> PipelineArtifactProvenanceV1:
    return PipelineArtifactProvenanceV1(
        producer_kind="pipeline",
        loom_commit_sha=request.provenance.loom_commit_sha,
        pipeline_run_id=request.run_id,
        stage_run_id=request.stage_run_id,
        execution_attempt_id=request.attempt_id,
        recipe_digest=request.provenance.recipe_digest,
        execution_spec_digest=request.provenance.execution_spec_digest,
        image_digest=request.provenance.image_digest,
        compatibility_manifest_sha256=request.provenance.compatibility_manifest_sha256,
        control_binding=request.provenance.control_binding,
        source_artifacts=_asset_provenance_source(request),
    )


def _stage_result(request: StageRequestV1, step_count: int) -> StageResultV1:
    return StageResultV1(
        schema_version="loom.stage-result.v1",
        domain_outcome="judged",
        reason_code="inspection_complete",
        retry_class=RetryClass.NONE,
        inputs=[
            StageResultInputV1(
                binding_name=binding.binding_name,
                item_key=item.item_key,
                artifact_id=item.artifact_id,
                artifact_type=binding.artifact_type,
                manifest_sha256=item.manifest_sha256,
            )
            for binding in request.inputs
            for item in binding.items
        ],
        outputs=[
            StageResultOutputV1(name="inspection", artifact_type="behavior_inspection_report.v1")
        ],
        metrics={"step_count": step_count},
        provenance=StageResultProvenanceV1(
            pipeline_run_id=request.run_id,
            stage_run_id=request.stage_run_id,
            execution_attempt_id=request.attempt_id,
            recipe_digest=request.provenance.recipe_digest,
            execution_spec_digest=request.provenance.execution_spec_digest,
            image_digest=request.provenance.image_digest.rsplit("@", 1)[-1],
        ),
        error=None,
    )


def _validate_asset_profile(profile: JudgeExecutionProfileV1, assets: ProviderAssetBundle) -> None:
    if (
        profile.runner_lock_sha256 != assets.digests["runner-lock.json"]
        or profile.provider_asset_manifest_sha256 != assets.manifest_sha256
        or list(profile.mcp_server_locks) != list(assets.mcp_locks)
    ):
        raise BehaviorContractError("frozen profile and provider asset locks disagree")
    prefix = "/opt/behavior/provider-assets/behavior_offline_judge/"
    locked: dict[str, str] = {}
    for item in profile.provider_asset_locks:
        if not item.image_path.startswith(prefix):
            raise BehaviorContractError("frozen profile provider asset root drift")
        relative = item.image_path.removeprefix(prefix)
        if relative in locked:
            raise BehaviorContractError("frozen profile repeats a provider asset path")
        locked[relative] = item.sha256
    if set(locked) != set(assets.digests):
        raise BehaviorContractError("frozen profile provider asset inventory drift")
    for role, digest in assets.digests.items():
        if locked[role] != digest:
            raise BehaviorContractError("frozen profile provider asset digest drift")


def _load_mounted_artifact(
    binding: BindingSetV1, root: Path, model: type[PipelineModel]
) -> PipelineModel:
    raw = _read_canonical_file(root / "artifact.json", limit=67_108_864)
    parsed = model.model_validate_json(raw)
    if canonical_digest(parsed) != bindings_item(binding).content_sha256:
        raise BehaviorContractError(f"{binding.binding_name} semantic content digest drift")
    return parsed


def _read_canonical_file(path: Path, *, limit: int) -> bytes:
    raw = _read_regular(path, limit=limit)
    try:
        value = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise BehaviorContractError("mounted JSON is invalid") from exc
    if canonical_document(value) != raw:
        raise BehaviorContractError("mounted JSON is not canonical JCS+LF")
    return raw


def _parse_model(raw: bytes, model: type[_ModelT]) -> _ModelT:
    value = model.model_validate_json(raw)
    if canonical_document(value) != raw:
        raise BehaviorContractError("mounted semantic document is not canonical JCS+LF")
    return value


def _declared_file(
    artifact: BehaviorDatasetSnapshotArtifactV1, relative_path: str
) -> ArtifactFileV1:
    values = [item for item in artifact.files if item.relative_path == relative_path]
    if len(values) != 1:
        raise BehaviorContractError("dataset declared-file inventory is not unique")
    return values[0]


def _read_declared(root: Path, relative_path: str, size: int, digest: str) -> bytes:
    relative = PurePosixPath(relative_path)
    if relative.is_absolute() or any(part in {"", ".", ".."} for part in relative.parts):
        raise BehaviorContractError("declared input path is unsafe")
    value = _read_regular(root.joinpath(*relative.parts), limit=size)
    if len(value) != size or digest_bytes(value) != digest:
        raise BehaviorContractError("declared input bytes disagree with the manifest")
    return value


def _read_regular(path: Path, *, limit: int) -> bytes:
    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
    try:
        fd = os.open(path, flags)
    except OSError as exc:
        raise BehaviorContractError("declared input cannot be opened") from exc
    try:
        before = os.fstat(fd)
        if not stat.S_ISREG(before.st_mode) or before.st_nlink != 1:
            raise BehaviorContractError("declared input is not one private regular file")
        chunks: list[bytes] = []
        observed = 0
        while observed <= limit:
            chunk = os.read(fd, min(1_048_576, limit + 1 - observed))
            if not chunk:
                break
            chunks.append(chunk)
            observed += len(chunk)
        if observed > limit:
            raise BehaviorContractError("declared input exceeds its signed byte limit")
        after = os.fstat(fd)
        if (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns) != (
            after.st_dev,
            after.st_ino,
            after.st_size,
            after.st_mtime_ns,
        ):
            raise BehaviorContractError("declared input changed while being read")
        return b"".join(chunks)
    finally:
        os.close(fd)


def bindings_item(binding: BindingSetV1) -> BindingItemV1:
    return binding.items[0]


__all__ = [
    "OFFLINE_JUDGE_OUTPUT_DECLARATIONS",
    "FrozenJudgeAuthorityReader",
    "MountedFrozenJudgeAuthorityReader",
    "MountedOfflineJudgeInputs",
    "OfflineJudgeAdapter",
    "OfflineJudgeAuthority",
    "OfflineJudgeBindings",
    "OfflineJudgePaths",
    "OfflineJudgeRunRequest",
    "OfflineJudgeRunResult",
    "OfflineJudgeRunner",
    "load_mounted_offline_judge_inputs",
    "offline_judge_stage_binding",
    "validate_offline_judge_request",
]
