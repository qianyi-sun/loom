"""Closed authority and graph contracts for the BEHAVIOR Stage 1 live smoke.

This module is deliberately independent from the final Pipeline acceptance
``matrix|soak`` protocol.  It defines one controller-owned, single-stage Recipe
and the immutable documents that bind a separately authorized live action to
its preflight, execution, terminal evidence, and cleanup.
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Annotated, Literal
from uuid import UUID

from pydantic import Field, StringConstraints, field_validator, model_validator

from loom.pipeline.keys import canonical_digest, canonical_document, digest_bytes
from loom.pipeline.recipes import verify_renderer_lock
from loom.pipeline.spec import (
    ContainerNodeV1,
    Digest,
    GraphInputV1,
    NonNegativeSafeInt,
    OutputDeclV1,
    PipelineModel,
    PositiveSafeInt,
    RecipeIdentityV1,
    RequestRendererLockV1,
    RequestRendererRefV1,
    RunBudgetV1,
    RunGraphSpecV1,
    RunInputBindingV1,
    StageBudgetV1,
    reject_secret_literals,
)

STAGE1_SMOKE_RECIPE_NAME = "behavior-stage1-smoke"
STAGE1_SMOKE_RECIPE_VERSION = 1
STAGE1_SMOKE_ACTION = "stage1"
STAGE1_SMOKE_NODE_KEY = "rollout"
STAGE1_SMOKE_OUTPUT_NAME = "rollout"
STAGE1_SMOKE_OUTPUT_TYPE = "behavior_rollout_bundle.v1"
STAGE1_SMOKE_RESOURCE_PROFILE = "behavior-sim-local-none@1"
STAGE1_SMOKE_RENDERER_LOCK = Path(
    "src/loom/integrations/behavior/schemas/behavior_stage_request.renderer-lock.v1.json"
)
MAX_STAGE1_SMOKE_CANDIDATE_BYTES = 1_048_576

_IMAGE = Annotated[
    str,
    StringConstraints(
        pattern=(
            r"^[a-z0-9]+(?:[._-][a-z0-9]+)*(?::[0-9]+)?"
            r"(?:/[a-z0-9]+(?:[._-][a-z0-9]+)*)+@sha256:[0-9a-f]{64}$"
        )
    ),
]
_CommitSha = Annotated[str, StringConstraints(pattern=r"^[0-9a-f]{40}$")]
_BoundedIdentity = Annotated[str, StringConstraints(min_length=1, max_length=256)]


class Stage1SmokeInputV1(GraphInputV1):
    artifact_id: UUID
    manifest_sha256: Digest
    content_sha256: Digest
    stored_size_bytes: NonNegativeSafeInt
    unpacked_size_bytes: NonNegativeSafeInt
    file_count: NonNegativeSafeInt


class Stage1SmokePreviewPolicyV1(PipelineModel):
    """The exact #1366 low-rate, ephemeral preview policy."""

    schema_version: Literal["loom.behavior-stage1-preview-policy.v1"]
    min_interval_ms: Literal[500]
    ttl_seconds: Literal[300]
    max_frame_bytes: Literal[524_288]
    max_frames_per_attempt: Literal[64]
    max_total_bytes_per_attempt: Literal[33_554_432]
    width: Literal[672]
    height: Literal[448]
    media_type: Literal["image/jpeg"]
    label: Literal["LIVE / UNVERIFIED"]


class Stage1SmokeOutputV1(PipelineModel):
    name: Literal["rollout"]
    artifact_type: Literal["behavior_rollout_bundle.v1"]
    producer: Literal["container"]
    required: Literal[True]
    max_bytes: PositiveSafeInt


class Stage1SmokeCandidateV1(PipelineModel):
    """Canonical render-only candidate.  It grants no mutation authority."""

    schema_version: Literal["loom.behavior-stage1-smoke-candidate.v1"]
    loom_commit_sha: _CommitSha
    environment: _BoundedIdentity
    team_id: UUID
    operator_user_id: UUID
    backend_variant_id: Literal["oldlab-rtx5080-2gpu", "gb10-shared-1gpu"]
    slurm_cluster_id: Literal["oldlab", "gb10"]
    slurm_cluster_config_sha256: Digest
    policy_id: Literal["behavior-gpu-oldlab", "behavior-gpu-gb10"]
    policy_config_sha256: Digest
    policy_activation_epoch: PositiveSafeInt
    image_index_digest: _IMAGE
    platform: Literal["linux/amd64", "linux/arm64"]
    platform_child_digest: Digest
    image_runtime_contract_sha256: Digest
    resource_profile_sha256: Digest
    renderer_lock_sha256: Digest
    stage_request_schema_sha256: Digest
    compatibility_manifest_sha256: Digest
    recipe_digest: Digest
    inputs: Annotated[list[Stage1SmokeInputV1], Field(min_length=3, max_length=3)]
    parameters: dict[str, object]
    run_budget: RunBudgetV1
    stage_budget: StageBudgetV1
    expected_outputs: Annotated[list[Stage1SmokeOutputV1], Field(min_length=1, max_length=1)]
    expected_domain_outcome: Literal["rollout_success", "rollout_failure"]
    preview_policy: Stage1SmokePreviewPolicyV1
    start_by: datetime
    cleanup_deadline: datetime

    @field_validator("environment")
    @classmethod
    def exact_environment(cls, value: str) -> str:
        if value != value.strip() or any(ord(char) < 32 or ord(char) == 127 for char in value):
            raise ValueError("environment must be exact printable text")
        return value

    @field_validator("start_by", "cleanup_deadline")
    @classmethod
    def aware_timestamp(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("Stage 1 smoke timestamps must include a timezone")
        return value

    @model_validator(mode="after")
    def closed_stage1_authority(self) -> Stage1SmokeCandidateV1:
        expected_backend = {
            "oldlab-rtx5080-2gpu": ("oldlab", "behavior-gpu-oldlab", "linux/amd64"),
            "gb10-shared-1gpu": ("gb10", "behavior-gpu-gb10", "linux/arm64"),
        }[self.backend_variant_id]
        if (self.slurm_cluster_id, self.policy_id, self.platform) != expected_backend:
            raise ValueError("backend, Slurm cluster, policy, and platform do not match")
        if self.cleanup_deadline <= self.start_by:
            raise ValueError("cleanup deadline must be later than start-by")
        if [item.name for item in self.inputs] != ["task_instance", "dataset", "policy"]:
            raise ValueError("Stage 1 smoke inputs must use the exact graph order")
        expected_types = [
            "behavior_task_instance.v1",
            "behavior_dataset_snapshot.v1",
            "behavior_policy_checkpoint.v1",
        ]
        if [item.artifact_type for item in self.inputs] != expected_types:
            raise ValueError("Stage 1 smoke input types drifted")
        if len({item.artifact_id for item in self.inputs}) != 3:
            raise ValueError("Stage 1 smoke input Artifact IDs must be distinct")
        if any(item.file_count < 1 for item in self.inputs):
            raise ValueError("Stage 1 smoke inputs must contain at least one file")
        if self.parameters != {
            "episode_index": self.parameters.get("episode_index"),
            "eval_instance_index": self.parameters.get("eval_instance_index"),
            "record_depth": False,
            "recording_fps": 30,
            "seed": self.parameters.get("seed"),
        }:
            raise ValueError("Stage 1 smoke parameters are not the closed rollout set")
        for name in ("episode_index", "eval_instance_index", "seed"):
            value = self.parameters[name]
            if isinstance(value, bool) or not isinstance(value, int) or not 0 <= value <= 2**32 - 1:
                raise ValueError(f"{name} must be uint32")
        output = self.expected_outputs[0]
        if output.max_bytes != self.stage_budget.final_output_bytes_limit:
            raise ValueError("output declaration and Stage output budget differ")
        if (
            self.stage_budget.provider is not None
            or self.stage_budget.checkpoint_bytes_limit != 0
            or self.stage_budget.timeout_seconds > self.run_budget.max_wall_seconds
            or self.stage_budget.max_attempts > self.run_budget.max_attempts_total
            or self.run_budget.max_provider_cost_usd != "0"
            or self.run_budget.max_stage_runs != 1
        ):
            raise ValueError("Stage 1 smoke budgets are not closed")
        if self.recipe_digest != stage1_smoke_recipe_digest(
            renderer_lock_sha256=self.renderer_lock_sha256,
            stage_request_schema_sha256=self.stage_request_schema_sha256,
            resource_profile_sha256=self.resource_profile_sha256,
            image_index_digest=self.image_index_digest,
            platform_child_digest=self.platform_child_digest,
            compatibility_manifest_sha256=self.compatibility_manifest_sha256,
        ):
            raise ValueError("Stage 1 smoke Recipe digest drifted")
        reject_secret_literals(self)
        if len(self.canonical_bytes) > MAX_STAGE1_SMOKE_CANDIDATE_BYTES:
            raise ValueError("Stage 1 smoke candidate exceeds 1 MiB")
        return self

    @property
    def canonical_bytes(self) -> bytes:
        return canonical_document(self.model_dump(mode="json", exclude_none=False))

    @property
    def candidate_sha256(self) -> Digest:
        return digest_bytes(self.canonical_bytes)


class Stage1SmokeAuthorizationV1(PipelineModel):
    schema_version: Literal["loom.behavior-stage1-smoke-authorization.v1"]
    action: Literal["stage1"]
    authorization_id: UUID
    candidate_sha256: Digest
    operator_user_id: UUID
    team_id: UUID
    environment: _BoundedIdentity
    loom_commit_sha: _CommitSha
    recipe_digest: Digest
    image_index_digest: _IMAGE
    platform: Literal["linux/amd64", "linux/arm64"]
    platform_child_digest: Digest
    backend_variant_id: Literal["oldlab-rtx5080-2gpu", "gb10-shared-1gpu"]
    policy_id: Literal["behavior-gpu-oldlab", "behavior-gpu-gb10"]
    policy_config_sha256: Digest
    policy_activation_epoch: PositiveSafeInt
    input_descriptor_set_sha256: Digest
    run_budget_sha256: Digest
    start_by: datetime
    cleanup_deadline: datetime
    live_mutation_authorized: Literal[True]
    authorized_at: datetime
    expires_at: datetime
    nonce_sha256: Digest

    @field_validator("authorized_at", "expires_at", "start_by", "cleanup_deadline")
    @classmethod
    def aware_timestamp(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("authorization timestamps must include a timezone")
        return value

    @model_validator(mode="after")
    def authorization_window_is_valid(self) -> Stage1SmokeAuthorizationV1:
        if self.expires_at <= self.authorized_at:
            raise ValueError("authorization expiry must be later than issue time")
        if self.cleanup_deadline <= self.start_by:
            raise ValueError("authorization cleanup deadline must follow start-by")
        return self

    @property
    def authorization_sha256(self) -> Digest:
        return canonical_digest(self.model_dump(mode="json", exclude_none=False))


def validate_stage1_smoke_authorization(
    candidate: Stage1SmokeCandidateV1, authorization: Stage1SmokeAuthorizationV1
) -> None:
    expected = {
        "candidate_sha256": candidate.candidate_sha256,
        "operator_user_id": candidate.operator_user_id,
        "team_id": candidate.team_id,
        "environment": candidate.environment,
        "loom_commit_sha": candidate.loom_commit_sha,
        "recipe_digest": candidate.recipe_digest,
        "image_index_digest": candidate.image_index_digest,
        "platform": candidate.platform,
        "platform_child_digest": candidate.platform_child_digest,
        "backend_variant_id": candidate.backend_variant_id,
        "policy_id": candidate.policy_id,
        "policy_config_sha256": candidate.policy_config_sha256,
        "policy_activation_epoch": candidate.policy_activation_epoch,
        "input_descriptor_set_sha256": canonical_digest(candidate.inputs),
        "run_budget_sha256": canonical_digest(candidate.run_budget),
        "start_by": candidate.start_by,
        "cleanup_deadline": candidate.cleanup_deadline,
    }
    observed = authorization.model_dump(mode="python")
    for field, value in expected.items():
        if observed[field] != value:
            raise ValueError(f"Stage 1 authorization {field} drifted")


class Stage1SmokeGpuDeviceV1(PipelineModel):
    logical_index: Annotated[int, Field(strict=True, ge=0, le=1)]
    device_uuid: Annotated[
        str, StringConstraints(pattern=r"^GPU-[A-Za-z0-9][A-Za-z0-9_-]{0,122}$")
    ]
    model: Literal["NVIDIA GeForce RTX 5080", "NVIDIA GB10"]
    role: Literal["sim", "vla", "sim_and_vla"]


class Stage1SmokePreflightV1(PipelineModel):
    schema_version: Literal["loom.behavior-stage1-smoke-preflight.v1"]
    candidate_sha256: Digest
    authorization_id: UUID
    authorization_sha256: Digest
    worker_id: UUID
    worker_lease_epoch: PositiveSafeInt
    worker_capability_snapshot_sha256: Digest
    slurm_allocation_id: _BoundedIdentity
    gpu_devices: Annotated[list[Stage1SmokeGpuDeviceV1], Field(min_length=1, max_length=2)]
    policy_activation_epoch: PositiveSafeInt
    platform_child_digest: Digest
    image_runtime_contract_sha256: Digest
    input_descriptor_set_sha256: Digest
    ancestry_ok: Literal[True]
    image_platform_ok: Literal[True]
    worker_capability_ok: Literal[True]
    slurm_config_ok: Literal[True]
    gpu_topology_ok: Literal[True]
    cas_capacity_ok: Literal[True]
    scratch_capacity_ok: Literal[True]
    input_markers_ok: Literal[True]
    existing_pipeline_runs: Literal[0]
    existing_attempts: Literal[0]
    existing_upload_sessions: Literal[0]
    existing_slurm_jobs: Literal[0]
    observed_at: datetime

    @field_validator("gpu_devices")
    @classmethod
    def exact_gpu_devices(
        cls, values: list[Stage1SmokeGpuDeviceV1]
    ) -> list[Stage1SmokeGpuDeviceV1]:
        if [item.logical_index for item in values] != list(range(len(values))):
            raise ValueError("GPU devices must preserve logical CUDA index order")
        if len({item.device_uuid for item in values}) != len(values):
            raise ValueError("GPU UUIDs must be unique")
        if len(values) == 1:
            if values[0].model != "NVIDIA GB10" or values[0].role != "sim_and_vla":
                raise ValueError("single-GPU preflight must be the GB10 shared-device contract")
        elif (
            [item.model for item in values]
            != ["NVIDIA GeForce RTX 5080", "NVIDIA GeForce RTX 5080"]
            or [item.role for item in values] != ["sim", "vla"]
        ):
            raise ValueError("two-GPU preflight must preserve OLDLAB sim/VLA role order")
        return values

    @field_validator("observed_at")
    @classmethod
    def aware_timestamp(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("preflight timestamp must include a timezone")
        return value


class Stage1SmokeCleanupV1(PipelineModel):
    schema_version: Literal["loom.behavior-stage1-smoke-cleanup.v1"]
    candidate_sha256: Digest
    pipeline_run_id: UUID
    preview_generation_count: Literal[0]
    preview_frame_count: Literal[0]
    active_policy_slots: Literal[0]
    active_upload_sessions: Literal[0]
    active_input_leases: Literal[0]
    active_worker_fences: Literal[0]
    active_slurm_jobs: Literal[0]
    active_allocations: Literal[0]
    unexpected_processes: Literal[0]
    unexpected_mounts: Literal[0]
    cleaned_at: datetime

    @field_validator("cleaned_at")
    @classmethod
    def aware_timestamp(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("cleanup timestamp must include a timezone")
        return value


def load_behavior_renderer_lock(repo_root: Path) -> RequestRendererLockV1:
    path = repo_root / STAGE1_SMOKE_RENDERER_LOCK
    lock = RequestRendererLockV1.model_validate_json(path.read_bytes())
    verify_renderer_lock(lock, repo_root)
    return lock


def stage1_smoke_recipe_digest(
    *,
    renderer_lock_sha256: str,
    stage_request_schema_sha256: str,
    resource_profile_sha256: str,
    image_index_digest: str,
    platform_child_digest: str,
    compatibility_manifest_sha256: str,
) -> Digest:
    return canonical_digest(
        {
            "name": STAGE1_SMOKE_RECIPE_NAME,
            "version": STAGE1_SMOKE_RECIPE_VERSION,
            "renderer_lock_sha256": renderer_lock_sha256,
            "stage_request_schema_sha256": stage_request_schema_sha256,
            "resource_profile_sha256": resource_profile_sha256,
            "image_index_digest": image_index_digest,
            "platform_child_digest": platform_child_digest,
            "compatibility_manifest_sha256": compatibility_manifest_sha256,
        }
    )


def build_stage1_smoke_graph(
    candidate: Stage1SmokeCandidateV1, *, repo_root: Path
) -> RunGraphSpecV1:
    """Resolve the sole internal Stage 1 graph from an immutable candidate."""

    lock = load_behavior_renderer_lock(repo_root)
    renderer_digest = canonical_digest(lock)
    if renderer_digest != candidate.renderer_lock_sha256:
        raise ValueError("Stage 1 renderer lock drifted from the candidate")
    identity = RecipeIdentityV1(
        name=STAGE1_SMOKE_RECIPE_NAME,
        version=STAGE1_SMOKE_RECIPE_VERSION,
        digest=candidate.recipe_digest,
    )
    node = ContainerNodeV1(
        node_kind="container",
        node_key=STAGE1_SMOKE_NODE_KEY,
        image=candidate.image_index_digest,
        argv=[
            "/opt/loom/venv/bin/python",
            "-m",
            "loom.integrations.behavior.cli",
            "run",
            "--request",
            "/inputs/stage-request.json",
            "--output-dir",
            "/outputs",
        ],
        workdir="/opt/loom",
        resource_profile=STAGE1_SMOKE_RESOURCE_PROFILE,
        network_profile="none",
        needs=[],
        inputs=[
            RunInputBindingV1(
                source="run_input",
                binding_name=item.name,
                artifact_type=item.artifact_type,
                input_name=item.name,
            )
            for item in candidate.inputs
        ],
        outputs=[
            OutputDeclV1(
                name="rollout",
                artifact_type="behavior_rollout_bundle.v1",
                required=True,
                role="artifact",
                producer="container",
                max_bytes=candidate.expected_outputs[0].max_bytes,
            )
        ],
        request_renderer=RequestRendererRefV1(
            name=lock.name,
            version=lock.version,
            digest=renderer_digest,
            max_bytes=16_777_216,
            terminal_stage_keys=[],
        ),
        checkpoint=None,
        fanout=None,
        fanout_commit=None,
        timeout_seconds=candidate.stage_budget.timeout_seconds,
        max_attempts=candidate.stage_budget.max_attempts,
        failure_policy="fail_run",
    )
    graph = RunGraphSpecV1(
        schema_version="loom.run-graph.v1",
        recipe=identity,
        inputs=[
            GraphInputV1(name=item.name, artifact_type=item.artifact_type, required=True)
            for item in candidate.inputs
        ],
        parameters=candidate.parameters,
        budget=candidate.run_budget,
        nodes=[node],
    )
    if StageBudgetV1.for_node(
        node,
        gpu_count_exact=2 if candidate.backend_variant_id == "oldlab-rtx5080-2gpu" else 1,
    ) != candidate.stage_budget:
        raise ValueError("candidate Stage budget does not match the resolved graph")
    return graph


__all__ = [
    "MAX_STAGE1_SMOKE_CANDIDATE_BYTES",
    "STAGE1_SMOKE_ACTION",
    "STAGE1_SMOKE_RECIPE_NAME",
    "STAGE1_SMOKE_RECIPE_VERSION",
    "Stage1SmokeAuthorizationV1",
    "Stage1SmokeCandidateV1",
    "Stage1SmokeCleanupV1",
    "Stage1SmokeGpuDeviceV1",
    "Stage1SmokeInputV1",
    "Stage1SmokeOutputV1",
    "Stage1SmokePreflightV1",
    "Stage1SmokePreviewPolicyV1",
    "build_stage1_smoke_graph",
    "load_behavior_renderer_lock",
    "stage1_smoke_recipe_digest",
    "validate_stage1_smoke_authorization",
]
