"""Closed worker-facing Pipeline execution protocol v1 (#8).

The models in this module are transport contracts.  They intentionally contain
no object-store locator, host path, credential value, or mutable image tag.
"""

from __future__ import annotations

import json
import re
import unicodedata
from datetime import datetime
from hashlib import sha256
from pathlib import PurePosixPath
from typing import Annotated, Any, Literal
from uuid import UUID

from pydantic import Field, StringConstraints, field_validator, model_validator

from loom.models.worker_capabilities import (
    SlurmGpuAllocationEvidenceV1,
    WorkerCapabilitySnapshotV1,
)
from loom.pipeline.keys import MAX_SAFE_INTEGER, canonical_digest, canonical_document
from loom.pipeline.spec import (
    IMAGE_PATTERN,
    ArtifactType,
    BindingName,
    BindingSetV1,
    CheckpointPolicyV1,
    Digest,
    ExecutionSpecSnapshotV1,
    NonNegativeSafeInt,
    OutputDeclV1,
    PipelineModel,
    PlatformFanoutCommitV1,
    PositiveSafeInt,
    PositiveVersion,
    reject_secret_literals,
)
from loom.pipeline.state import RetryClass, StageResultV1

WorkKind = Literal["trial", "execution_attempt"]
_OpaqueReference = Annotated[
    str,
    StringConstraints(pattern=r"^(?:loom|k8s-secret)://[A-Za-z0-9._/@:-]+$"),
]
_ImageReference = Annotated[str, StringConstraints(pattern=IMAGE_PATTERN)]
_KebabName = Annotated[
    str,
    StringConstraints(pattern=r"^[a-z][a-z0-9_-]{0,126}$", max_length=127),
]
_BoundedText = Annotated[str, StringConstraints(min_length=1, max_length=500)]
_RuntimeSeconds = Annotated[int, Field(strict=True, ge=0, le=MAX_SAFE_INTEGER)]


def _nfc(value: str) -> str:
    normalized = unicodedata.normalize("NFC", value)
    normalized.encode("utf-8", errors="strict")
    return normalized


def _aware(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("timestamp must include a timezone")
    return value


def _ordered_unique_strings(values: list[str], label: str) -> list[str]:
    if values != sorted(values, key=lambda item: item.encode("utf-8")):
        raise ValueError(f"{label} must be bytewise sorted")
    if len(values) != len(set(values)):
        raise ValueError(f"{label} must be unique")
    return values


def _canonical_sha256(value: object) -> str:
    return "sha256:" + sha256(canonical_document(value)).hexdigest()


class WorkClaimRequestV1(PipelineModel):
    schema_version: Literal["loom.work-claim-request.v1"]
    worker_id: UUID
    capability_snapshot_digest: Digest
    free_slots: PositiveSafeInt
    supported_work_kinds: Annotated[list[WorkKind], Field(min_length=2, max_length=2)]

    @field_validator("supported_work_kinds")
    @classmethod
    def exact_work_kinds(cls, values: list[WorkKind]) -> list[WorkKind]:
        if values != ["trial", "execution_attempt"]:
            raise ValueError("new workers must request trial then execution_attempt")
        return values


class TrialClaimV1(PipelineModel):
    """The existing Trial claim payload, unchanged and wrapped by WorkClaimV1."""

    trial_id: UUID
    team_id: UUID
    task_id: str
    config: dict[str, Any]
    requires_caps: dict[str, Any]
    attempt_count: PositiveSafeInt
    provider_connection_id: UUID | None
    family_key: str | None
    family_state_uri: str | None
    family_run_spec: dict[str, Any] | None
    state: Literal["claimed"]


class StageRequestGrantV1(PipelineModel):
    renderer_name: BindingName
    renderer_version: PositiveVersion
    renderer_digest: Digest
    canonical_jcs_lf: str
    stage_request_sha256: Digest
    size_bytes: PositiveSafeInt

    @model_validator(mode="after")
    def exact_canonical_bytes(self) -> StageRequestGrantV1:
        raw = self.canonical_jcs_lf.encode("utf-8", errors="strict")
        if len(raw) != self.size_bytes:
            raise ValueError("StageRequest byte size does not match its grant")
        if "sha256:" + sha256(raw).hexdigest() != self.stage_request_sha256:
            raise ValueError("StageRequest digest does not match its grant")
        try:
            decoded = json.loads(raw)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ValueError("StageRequest grant is not valid JSON") from exc
        if canonical_document(decoded) != raw:
            raise ValueError("StageRequest grant must contain canonical JCS+LF bytes")
        return self


class AcceptancePreflightGrantV1(PipelineModel):
    authorization_id: UUID
    authorization_snapshot_sha256: Digest
    action: Literal["matrix"]
    candidate_sha256: Digest
    preflight_input_set_id: Literal["S02"]
    prerequisite_pipeline_run_id: UUID
    exclusive_fence_id: UUID
    node_key: str
    backend_variant_id: Literal["oldlab-rtx5080-2gpu", "gb10-shared-1gpu"]
    cache_expectation: Literal["cold_after_eviction", "warm_reuse_only"]
    sealed_input_descriptor_set_sha256: Digest
    policy_id: Literal["behavior-gpu-oldlab", "behavior-gpu-gb10"]
    policy_config_sha256: Digest
    policy_activation_epoch: PositiveSafeInt
    slurm_cluster_id: Literal["oldlab", "gb10"]
    slurm_cluster_config_sha256: Digest
    slurm_allocation_id: str
    image_runtime_contract_digest: Digest
    resource_profile_digest: Digest
    network_profile: Literal["none"]
    renderer_digest: Digest

    @field_validator("node_key", "slurm_allocation_id")
    @classmethod
    def normalize_text(cls, value: str) -> str:
        value = _nfc(value)
        if not value or len(value.encode("utf-8")) > 256 or "\x00" in value:
            raise ValueError("acceptance preflight identity is invalid")
        return value

    @model_validator(mode="after")
    def variant_authorities_are_exact(self) -> AcceptancePreflightGrantV1:
        expected = {
            "oldlab-rtx5080-2gpu": ("behavior-gpu-oldlab", "oldlab"),
            "gb10-shared-1gpu": ("behavior-gpu-gb10", "gb10"),
        }[self.backend_variant_id]
        if (self.policy_id, self.slurm_cluster_id) != expected:
            raise ValueError("acceptance preflight variant/policy/cluster drift")
        phase = "cold" if self.cache_expectation == "cold_after_eviction" else "warm"
        if self.node_key != f"{self.backend_variant_id}_acceptance_preflight_{phase}":
            raise ValueError("acceptance preflight node/cache phase drift")
        return self


class ResourceDeviceRolesV1(PipelineModel):
    sim_gpu_index: NonNegativeSafeInt
    vla_gpu_index: NonNegativeSafeInt


class ResourceExecutionVariantV1(PipelineModel):
    variant_id: _KebabName
    cpu_arch: Literal["x86_64", "arm64"]
    gpu_count_exact: NonNegativeSafeInt
    gpu_vendor: Literal["nvidia"] | None
    allowed_gpu_models: list[str]
    gpu_memory_kind: Literal["dedicated", "unified"] | None
    gpu_memory_mb_min: NonNegativeSafeInt | None
    gpu_unified_memory_mb_min: NonNegativeSafeInt | None
    memory_accounting_kind: Literal["separate", "unified_shared"]
    container_memory_bytes_override: NonNegativeSafeInt | None
    same_gpu_model_required: bool
    pool_class: Literal["behavior-cpu-data", "behavior-gpu-oldlab", "behavior-gpu-gb10"]
    device_roles: ResourceDeviceRolesV1 | None

    @field_validator("allowed_gpu_models")
    @classmethod
    def models_are_canonical(cls, values: list[str]) -> list[str]:
        return _ordered_unique_strings([_nfc(value) for value in values], "GPU models")

    @model_validator(mode="after")
    def gpu_fields_are_coherent(self) -> ResourceExecutionVariantV1:
        if self.gpu_count_exact == 0:
            if (
                self.gpu_vendor is not None
                or self.gpu_memory_kind is not None
                or self.allowed_gpu_models
                or self.gpu_memory_mb_min is not None
                or self.gpu_unified_memory_mb_min is not None
                or self.device_roles is not None
                or self.memory_accounting_kind != "separate"
                or self.container_memory_bytes_override is not None
                or self.same_gpu_model_required
                or self.pool_class != "behavior-cpu-data"
            ):
                raise ValueError("CPU execution variant cannot carry GPU requirements")
        else:
            if (
                self.gpu_vendor != "nvidia"
                or self.gpu_memory_kind is None
                or not self.allowed_gpu_models
                or self.device_roles is None
                or not self.same_gpu_model_required
            ):
                raise ValueError("GPU execution variant is incomplete")
            if max(self.device_roles.sim_gpu_index, self.device_roles.vla_gpu_index) >= (
                self.gpu_count_exact
            ):
                raise ValueError("GPU device role index exceeds the requested device count")
            if self.gpu_memory_kind == "dedicated":
                if (
                    self.gpu_memory_mb_min is None
                    or self.gpu_unified_memory_mb_min is not None
                    or self.memory_accounting_kind != "separate"
                    or self.container_memory_bytes_override is not None
                ):
                    raise ValueError("dedicated GPU memory accounting is invalid")
            elif (
                self.gpu_memory_mb_min is not None
                or self.gpu_unified_memory_mb_min is None
                or self.memory_accounting_kind != "unified_shared"
                or self.container_memory_bytes_override is None
            ):
                raise ValueError("unified GPU memory accounting is invalid")
        return self


class ResourceProfileV1(PipelineModel):
    name: _KebabName
    version: PositiveVersion
    execution_variants: Annotated[list[ResourceExecutionVariantV1], Field(min_length=1)]
    cpu_cores: PositiveSafeInt
    memory_bytes: PositiveSafeInt
    scratch_bytes: PositiveSafeInt
    timeout_seconds_max: PositiveSafeInt
    required_host_runtime_features: list[str]
    required_image_features: list[str]
    network_profile: Literal["none", "gateway"]
    input_cache_capacity_bytes_min: NonNegativeSafeInt

    @field_validator("required_host_runtime_features", "required_image_features")
    @classmethod
    def features_are_canonical(cls, values: list[str]) -> list[str]:
        return _ordered_unique_strings([_nfc(value) for value in values], "runtime features")

    @field_validator("execution_variants")
    @classmethod
    def variants_are_canonical(
        cls, values: list[ResourceExecutionVariantV1]
    ) -> list[ResourceExecutionVariantV1]:
        ids = [item.variant_id for item in values]
        _ordered_unique_strings(ids, "execution variants")
        return values

    @model_validator(mode="after")
    def runtime_and_network_requirements_are_coherent(self) -> ResourceProfileV1:
        has_gpu = any(item.gpu_count_exact > 0 for item in self.execution_variants)
        host_features = set(self.required_host_runtime_features)
        image_features = set(self.required_image_features)
        if self.network_profile == "gateway":
            if "loom-secret-tmpfs-v1" not in host_features:
                raise ValueError("gateway profiles require loom-secret-tmpfs-v1")
        elif "loom-secret-tmpfs-v1" in host_features:
            raise ValueError("network=none profiles cannot request a runtime secret mount")
        if has_gpu:
            if not {"egl", "nvidia-container-runtime"} <= host_features:
                raise ValueError("GPU profiles require NVIDIA runtime and EGL")
            if not {"isaac-sim-5.1", "omnigibson-3.8"} <= image_features:
                raise ValueError("GPU profiles require the attested simulation image features")
        elif image_features != {"behavior-cpu-data"}:
            raise ValueError("zero-GPU profiles require only behavior-cpu-data")
        return self


class ProviderAssetImageEntryV1(PipelineModel):
    logical_name: BindingName
    role: str
    image_path: str
    sha256: Digest

    @field_validator("image_path")
    @classmethod
    def provider_path_is_confined(cls, value: str) -> str:
        value = _nfc(value)
        if not value.startswith("/opt/behavior/provider-assets/"):
            raise ValueError("provider asset must be below the immutable provider-assets root")
        if any(part in {"", ".", ".."} for part in value.split("/")[1:]):
            raise ValueError("provider asset path contains an invalid component")
        return value

    @model_validator(mode="after")
    def path_matches_logical_name(self) -> ProviderAssetImageEntryV1:
        prefix = f"/opt/behavior/provider-assets/{self.logical_name}/"
        if not self.image_path.startswith(prefix) or self.image_path == prefix:
            raise ValueError("provider asset path must be below its logical-name directory")
        return self


class ImageRuntimeContractV1(PipelineModel):
    image_index_digest: _ImageReference
    platform: Literal["linux/amd64", "linux/arm64"]
    platform_manifest_digest: Digest
    cpu_arch: Literal["x86_64", "arm64"]
    gpu_vendor: Literal["none", "nvidia"]
    cuda_userspace_version: str | None
    min_nvidia_driver_version: str | None
    application_features: list[str]
    provider_assets: list[ProviderAssetImageEntryV1]
    preflight_argv: Annotated[list[str], Field(min_length=1, max_length=256)]
    preflight_digest: Digest
    sbom_digest: Digest
    attestation_digest: Digest

    @field_validator("application_features")
    @classmethod
    def application_features_are_canonical(cls, values: list[str]) -> list[str]:
        return _ordered_unique_strings([_nfc(value) for value in values], "image features")

    @field_validator("min_nvidia_driver_version")
    @classmethod
    def driver_version_is_numeric(cls, value: str | None) -> str | None:
        if value is not None and re.fullmatch(r"[0-9]+(?:\.[0-9]+)+", value) is None:
            raise ValueError("minimum NVIDIA driver version must use dotted integers")
        return value

    @field_validator("provider_assets")
    @classmethod
    def provider_assets_are_canonical(
        cls, values: list[ProviderAssetImageEntryV1]
    ) -> list[ProviderAssetImageEntryV1]:
        names = [item.logical_name for item in values]
        _ordered_unique_strings(names, "provider assets")
        return values

    @model_validator(mode="after")
    def platform_and_gpu_fields_are_coherent(self) -> ImageRuntimeContractV1:
        expected_arch = "x86_64" if self.platform == "linux/amd64" else "arm64"
        if self.cpu_arch != expected_arch:
            raise ValueError("image platform and CPU architecture drift")
        if self.gpu_vendor == "none" and (
            self.cuda_userspace_version is not None or self.min_nvidia_driver_version is not None
        ):
            raise ValueError("CPU image cannot carry NVIDIA runtime requirements")
        if self.gpu_vendor == "nvidia" and (
            self.cuda_userspace_version is None or self.min_nvidia_driver_version is None
        ):
            raise ValueError("NVIDIA image must pin CUDA and minimum driver versions")
        return self


class ArtifactInputDescriptorV1(PipelineModel):
    artifact_id: UUID
    artifact_type: ArtifactType
    content_sha256: Digest
    manifest_sha256: Digest
    stored_size_bytes: NonNegativeSafeInt
    unpacked_size_bytes: NonNegativeSafeInt
    file_count: NonNegativeSafeInt


class ExecutionAttemptClaimV1(PipelineModel):
    execution_attempt_id: UUID
    pipeline_run_id: UUID
    stage_run_id: UUID
    team_id: UUID
    node_key: BindingName
    shard_key: str
    attempt_number: Annotated[int, Field(strict=True, ge=1, le=3)]
    claim_id: UUID
    lease_epoch: PositiveSafeInt
    lease_token: Annotated[str, StringConstraints(min_length=32, max_length=512)]
    lease_expires_at: datetime
    recipe_digest: Digest
    run_graph_digest: Digest
    execution_spec_snapshot: ExecutionSpecSnapshotV1
    execution_spec_digest: Digest
    image: _ImageReference
    argv: Annotated[list[str], Field(min_length=1, max_length=256)]
    workdir: str
    resource_profile_snapshot: ResourceProfileV1
    resource_profile_digest: Digest
    network_profile: Literal["none", "gateway"]
    image_runtime_contract_snapshot: ImageRuntimeContractV1
    image_runtime_contract_digest: Digest
    worker_capability_snapshot: WorkerCapabilitySnapshotV1
    worker_capability_snapshot_digest: Digest
    slurm_gpu_allocation_evidence: SlurmGpuAllocationEvidenceV1 | None
    slurm_gpu_allocation_evidence_digest: Digest | None
    input_bindings: Annotated[list[BindingSetV1], Field(max_length=128)]
    outputs: Annotated[list[OutputDeclV1], Field(max_length=64)]
    checkpoint: CheckpointPolicyV1 | None
    fanout_commit: PlatformFanoutCommitV1 | None
    stage_request: StageRequestGrantV1 | None
    acceptance_preflight: AcceptancePreflightGrantV1 | None
    provider_connection_ref: UUID | None
    secret_refs: list[_OpaqueReference]
    resume_checkpoint: ArtifactInputDescriptorV1 | None
    timeout_seconds: PositiveSafeInt
    cancellation_poll_seconds: Literal[5]
    cancellation_grace_seconds: Literal[30]

    _lease_is_aware = field_validator("lease_expires_at")(_aware)

    @field_validator("secret_refs")
    @classmethod
    def secret_references_are_canonical(cls, values: list[str]) -> list[str]:
        return _ordered_unique_strings(values, "secret references")

    @model_validator(mode="after")
    def duplicated_claim_fields_are_exact(self) -> ExecutionAttemptClaimV1:
        spec = self.execution_spec_snapshot
        node = spec.container_node
        if canonical_digest(spec) != self.execution_spec_digest:
            raise ValueError("execution spec bytes and digest drift")
        if (
            self.recipe_digest != spec.recipe_digest
            or self.run_graph_digest != spec.run_graph_digest
            or self.node_key != spec.node_key
            or self.shard_key != spec.shard_key
            or self.image != node.image
            or self.argv != node.argv
            or self.workdir != node.workdir
            or self.network_profile != node.network_profile
            or self.timeout_seconds != node.timeout_seconds
            or self.outputs != node.outputs
            or self.checkpoint != node.checkpoint
            or self.fanout_commit != node.fanout_commit
        ):
            raise ValueError("claim fields drift from the immutable execution spec")
        if self.resource_profile_digest != spec.resource_profile_digest or (
            self.resource_profile_digest != canonical_digest(self.resource_profile_snapshot)
        ):
            raise ValueError("resource profile snapshot/digest drift")
        if self.image_runtime_contract_digest != spec.image_runtime_contract_digest or (
            self.image_runtime_contract_digest
            != canonical_digest(self.image_runtime_contract_snapshot)
        ):
            raise ValueError("image runtime contract snapshot/digest drift")
        expected_profile = (
            f"{self.resource_profile_snapshot.name}@{self.resource_profile_snapshot.version}"
        )
        if node.resource_profile != expected_profile:
            raise ValueError("resource profile identity drift")
        if self.resource_profile_snapshot.network_profile != self.network_profile:
            raise ValueError("resource profile network drift")
        if self.timeout_seconds > self.resource_profile_snapshot.timeout_seconds_max:
            raise ValueError("stage timeout exceeds the ResourceProfile maximum")
        final_output_bytes = sum(output.max_bytes for output in node.outputs)
        if node.fanout_commit is not None:
            item = next(
                output
                for output in node.outputs
                if output.name == node.fanout_commit.item_binding_name
            )
            final_output_bytes += item.max_bytes * (node.fanout_commit.max_items - 1)
        if final_output_bytes > self.resource_profile_snapshot.scratch_bytes:
            raise ValueError("container output maximum exceeds the combined scratch quota")
        if self.image_runtime_contract_snapshot.image_index_digest != self.image:
            raise ValueError("image index identity drift")
        if (
            self.image_runtime_contract_snapshot.platform_manifest_digest
            != spec.resolved_image_manifest_digest
        ):
            raise ValueError("resolved image manifest drift")
        if self.worker_capability_snapshot_digest != canonical_digest(
            self.worker_capability_snapshot
        ):
            raise ValueError("worker capability snapshot/digest drift")
        if (self.slurm_gpu_allocation_evidence is None) != (
            self.slurm_gpu_allocation_evidence_digest is None
        ):
            raise ValueError("Slurm allocation evidence and digest must be present together")
        if self.slurm_gpu_allocation_evidence is not None and (
            self.slurm_gpu_allocation_evidence_digest
            != canonical_digest(self.slurm_gpu_allocation_evidence)
        ):
            raise ValueError("Slurm allocation evidence digest drift")
        variants = {
            item.variant_id: item for item in self.resource_profile_snapshot.execution_variants
        }
        selected_variant_id = spec.execution_variant_id
        try:
            variant = variants[selected_variant_id]
        except KeyError as exc:
            raise ValueError("selected execution variant is absent from the profile") from exc
        capability = self.worker_capability_snapshot
        if (
            capability.cpu_arch != variant.cpu_arch
            or capability.cpu_cores < self.resource_profile_snapshot.cpu_cores
            or capability.memory_bytes < (
                variant.container_memory_bytes_override
                or self.resource_profile_snapshot.memory_bytes
            )
            or capability.scratch_bytes < self.resource_profile_snapshot.scratch_bytes
            or capability.input_cache_capacity_bytes
            < self.resource_profile_snapshot.input_cache_capacity_bytes_min
            or not set(self.resource_profile_snapshot.required_host_runtime_features)
            <= set(capability.container_runtime_features)
            or self.network_profile not in capability.network_profiles
        ):
            raise ValueError("worker capability does not satisfy the ResourceProfile")
        image_contract = self.image_runtime_contract_snapshot
        if (
            image_contract.cpu_arch != variant.cpu_arch
            or not set(self.resource_profile_snapshot.required_image_features)
            <= set(image_contract.application_features)
        ):
            raise ValueError("image runtime contract does not satisfy the selected variant")
        if variant.gpu_count_exact == 0:
            if (
                capability.gpu_devices
                or self.slurm_gpu_allocation_evidence is not None
                or image_contract.gpu_vendor != "none"
                or spec.gpu_backend_selection_sha256 is not None
            ):
                raise ValueError("zero-GPU execution cannot carry GPU allocation evidence")
        else:
            evidence = self.slurm_gpu_allocation_evidence
            devices = capability.gpu_devices
            if evidence is None or len(devices) != variant.gpu_count_exact:
                raise ValueError("GPU execution requires its exact Slurm allocation")
            if evidence.variant_id != variant.variant_id or {
                item.allocation_id for item in devices
            } != {evidence.allocation_id}:
                raise ValueError("GPU variant and Slurm allocation evidence drift")
            if [item.device_uuid for item in devices] != evidence.device_uuids:
                raise ValueError("GPU capability UUIDs and allocation evidence drift")
            if any(item.model not in variant.allowed_gpu_models for item in devices):
                raise ValueError("GPU model is not allowed by the selected variant")
            if variant.same_gpu_model_required and len({item.model for item in devices}) != 1:
                raise ValueError("GPU devices must use the same selected model")
            if variant.gpu_memory_kind == "dedicated":
                if any(
                    item.memory_kind != "dedicated"
                    or item.memory_mb is None
                    or variant.gpu_memory_mb_min is None
                    or item.memory_mb < variant.gpu_memory_mb_min
                    for item in devices
                ):
                    raise ValueError("dedicated GPU memory does not satisfy the variant")
            elif variant.gpu_memory_kind == "unified":
                if any(
                    item.memory_kind != "unified"
                    or item.unified_memory_mb is None
                    or variant.gpu_unified_memory_mb_min is None
                    or item.unified_memory_mb < variant.gpu_unified_memory_mb_min
                    for item in devices
                ):
                    raise ValueError("unified GPU memory does not satisfy the variant")
            else:
                raise ValueError("GPU variant has no closed memory accounting kind")
            expected_cluster = (
                "gb10" if variant.variant_id == "gb10-shared-1gpu" else "oldlab"
            )
            if evidence.slurm_cluster_id != expected_cluster:
                raise ValueError("GPU variant and Slurm cluster drift")
            if spec.gpu_backend_selection_sha256 is None:
                raise ValueError("GPU execution requires frozen backend selection evidence")
            if image_contract.gpu_vendor != "nvidia":
                raise ValueError("GPU variant requires an NVIDIA image runtime contract")
            minimum_driver = image_contract.min_nvidia_driver_version
            if minimum_driver is None:
                raise ValueError("GPU image contract has no minimum NVIDIA driver")
            minimum_parts = tuple(int(part) for part in minimum_driver.split("."))
            for device in devices:
                actual_parts = tuple(
                    int(part) for part in device.nvidia_driver_version.split(".")
                )
                width = max(len(actual_parts), len(minimum_parts))
                if actual_parts + (0,) * (width - len(actual_parts)) < minimum_parts + (
                    0,
                ) * (width - len(minimum_parts)):
                    raise ValueError("NVIDIA driver is below the image runtime minimum")
        if canonical_digest(self.input_bindings) != spec.resolved_input_bindings_digest:
            raise ValueError("input bindings digest drift")
        renderer_digest = self.stage_request.renderer_digest if self.stage_request else None
        if renderer_digest != spec.request_renderer_lock_digest:
            raise ValueError("StageRequest renderer lock drift")
        if self.acceptance_preflight is not None:
            grant = self.acceptance_preflight
            if (
                self.pipeline_run_id != grant.prerequisite_pipeline_run_id
                or grant.node_key != self.node_key
                or grant.resource_profile_digest != self.resource_profile_digest
                or grant.image_runtime_contract_digest != self.image_runtime_contract_digest
                or grant.renderer_digest != renderer_digest
                or self.network_profile != "none"
                or self.provider_connection_ref is not None
                or self.secret_refs
            ):
                raise ValueError("acceptance preflight grant drift")
        if self.network_profile == "none" and (
            self.provider_connection_ref is not None or self.secret_refs
        ):
            raise ValueError("network=none cannot carry provider or secret references")
        return self


class WorkClaimV1(PipelineModel):
    schema_version: Literal["loom.work-claim.v1"]
    work_kind: WorkKind
    payload: TrialClaimV1 | ExecutionAttemptClaimV1

    @model_validator(mode="after")
    def payload_matches_discriminator(self) -> WorkClaimV1:
        expected = "trial" if isinstance(self.payload, TrialClaimV1) else "execution_attempt"
        if self.work_kind != expected:
            raise ValueError("work_kind does not match the claim payload")
        return self


class FencedMutationAuthV1(PipelineModel):
    claim_id: UUID
    lease_epoch: PositiveSafeInt
    lease_token: Annotated[str, StringConstraints(min_length=32, max_length=512)]
    request_id: UUID


class ClaimReadAuthV1(PipelineModel):
    claim_id: UUID
    lease_epoch: PositiveSafeInt
    lease_token: Annotated[str, StringConstraints(min_length=32, max_length=512)]


class WorkerLostCleanupAuthV1(PipelineModel):
    claim_id: UUID
    lease_epoch: PositiveSafeInt
    request_id: UUID


class ExecutionHeartbeatV1(PipelineModel):
    schema_version: Literal["loom.execution-heartbeat.v1"]
    phase: Literal[
        "input_materializing", "container_starting", "running", "output_committing", "cancelling"
    ]
    monotonic_runtime_seconds: _RuntimeSeconds
    active_upload_session_ids: list[UUID]

    @field_validator("active_upload_session_ids")
    @classmethod
    def active_uploads_are_unique(cls, values: list[UUID]) -> list[UUID]:
        if len(values) != len(set(values)):
            raise ValueError("active upload session IDs must be unique")
        return values


class ExecutionControlCommandV1(PipelineModel):
    seq: PositiveSafeInt
    command: Literal["cancel_requested", "rotate_step_jwt", "drain_after_attempt"]


class ExecutionControlResponseV1(PipelineModel):
    commands: list[ExecutionControlCommandV1]
    current_seq: NonNegativeSafeInt

    @model_validator(mode="after")
    def commands_are_ordered(self) -> ExecutionControlResponseV1:
        sequences = [item.seq for item in self.commands]
        if sequences != sorted(sequences) or len(sequences) != len(set(sequences)):
            raise ValueError("control commands must have increasing unique sequences")
        if sequences and sequences[-1] > self.current_seq:
            raise ValueError("control command sequence exceeds current_seq")
        return self


class ExecutionEventV1(PipelineModel):
    local_seq: NonNegativeSafeInt
    timestamp: datetime
    stream: Literal["stdout", "stderr", "worker"]
    level: Literal["debug", "info", "warning", "error"]
    message: str

    _timestamp_is_aware = field_validator("timestamp")(_aware)

    @field_validator("message")
    @classmethod
    def bounded_message(cls, value: str) -> str:
        value = _nfc(value)
        if len(value.encode("utf-8")) > 65_536:
            raise ValueError("event message exceeds 64 KiB")
        reject_secret_literals(value)
        return value


class ExecutionEventsV1(PipelineModel):
    events: Annotated[list[ExecutionEventV1], Field(max_length=100)]

    @model_validator(mode="after")
    def events_are_ordered(self) -> ExecutionEventsV1:
        sequences = [item.local_seq for item in self.events]
        if sequences != sorted(sequences) or len(sequences) != len(set(sequences)):
            raise ValueError("events must have increasing unique local sequences")
        return self


class PipelineInputMaterializationEvidenceReportV1(PipelineModel):
    schema_version: Literal["loom.pipeline-input-materialization-evidence-report.v1"]
    execution_attempt_id: UUID
    worker_id: UUID
    lease_epoch: PositiveSafeInt
    cache_expectation: Literal["cold_after_eviction", "warm_reuse_only"]
    ordered_manifest_sha256s: Annotated[list[Digest], Field(min_length=5, max_length=5)]
    manifest_open_count: NonNegativeSafeInt
    file_open_count: NonNegativeSafeInt
    file_bytes: NonNegativeSafeInt
    archive_extraction_count: NonNegativeSafeInt
    cas_rename_count: NonNegativeSafeInt
    input_view_sha256: Digest


class PipelineInputMaterializationEvidenceV1(PipelineModel):
    schema_version: Literal["loom.pipeline-input-materialization-evidence.v1"]
    execution_attempt_id: UUID
    worker_id: UUID
    lease_epoch: PositiveSafeInt
    cache_expectation: Literal["cold_after_eviction", "warm_reuse_only"]
    ordered_manifest_sha256s: Annotated[list[Digest], Field(min_length=5, max_length=5)]
    manifest_open_count: NonNegativeSafeInt
    file_open_count: NonNegativeSafeInt
    file_bytes: NonNegativeSafeInt
    archive_extraction_count: NonNegativeSafeInt
    cas_rename_count: NonNegativeSafeInt
    input_view_sha256: Digest
    materialized_at: datetime

    _materialized_is_aware = field_validator("materialized_at")(_aware)


class PipelineInputMaterializationEvidenceRefV1(PipelineModel):
    attempt_id: UUID
    worker_id: UUID
    lease_epoch: PositiveSafeInt
    evidence_sha256: Digest


class AcceptanceEvictionGrantV1(PipelineModel):
    schema_version: Literal["loom.acceptance-eviction-grant.v1"]
    command_id: UUID
    authorization_id: UUID
    candidate_sha256: Digest
    worker_id: UUID
    worker_lease_epoch: PositiveSafeInt
    ordered_manifest_sha256s: Annotated[list[Digest], Field(min_length=5, max_length=5)]
    pipeline_run_id: UUID
    exclusive_fence_id: UUID
    authorization_snapshot_sha256: Digest
    backend_variant_id: Literal["oldlab-rtx5080-2gpu", "gb10-shared-1gpu"]
    policy_id: Literal["behavior-gpu-oldlab", "behavior-gpu-gb10"]
    policy_config_sha256: Digest
    policy_activation_epoch: PositiveSafeInt
    slurm_cluster_id: Literal["oldlab", "gb10"]
    slurm_cluster_config_sha256: Digest
    slurm_allocation_id: _BoundedText
    worker_capability_snapshot_digest: Digest
    action: Literal["matrix"]

    @field_validator("ordered_manifest_sha256s")
    @classmethod
    def manifests_are_canonical(cls, values: list[str]) -> list[str]:
        return _ordered_unique_strings(values, "acceptance eviction manifests")

    @model_validator(mode="after")
    def backend_authorities_are_exact(self) -> AcceptanceEvictionGrantV1:
        expected = {
            "oldlab-rtx5080-2gpu": ("behavior-gpu-oldlab", "oldlab"),
            "gb10-shared-1gpu": ("behavior-gpu-gb10", "gb10"),
        }[self.backend_variant_id]
        if (self.policy_id, self.slurm_cluster_id) != expected:
            raise ValueError("acceptance eviction backend authority drift")
        return self


class AcceptanceEvictionEntryV1(PipelineModel):
    manifest_sha256: Digest
    pre_state: Literal["ready", "absent"]
    freed_bytes: NonNegativeSafeInt

    @model_validator(mode="after")
    def absent_frees_nothing(self) -> AcceptanceEvictionEntryV1:
        if self.pre_state == "absent" and self.freed_bytes != 0:
            raise ValueError("absent acceptance entry cannot free bytes")
        return self


class AcceptanceEvictionResultV1(PipelineModel):
    schema_version: Literal["loom.acceptance-eviction-result.v1"]
    authorization_id: UUID
    candidate_sha256: Digest
    worker_id: UUID
    ordered_manifest_sha256s: Annotated[list[Digest], Field(min_length=5, max_length=5)]
    entries: Annotated[list[AcceptanceEvictionEntryV1], Field(min_length=5, max_length=5)]
    evicted_count: Annotated[int, Field(strict=True, ge=0, le=5)]
    status: Literal["already_absent", "evicted"]
    absence_verified: Literal[True]
    finished_at: datetime

    _finished_is_aware = field_validator("finished_at")(_aware)

    @model_validator(mode="after")
    def result_is_exact(self) -> AcceptanceEvictionResultV1:
        manifests = _ordered_unique_strings(
            self.ordered_manifest_sha256s, "acceptance eviction manifests"
        )
        entry_manifests = [entry.manifest_sha256 for entry in self.entries]
        if entry_manifests != sorted(entry_manifests, key=str.encode):
            raise ValueError("acceptance eviction entries must be bytewise sorted")
        if set(entry_manifests) != set(manifests):
            raise ValueError("acceptance eviction entries must cover the request exactly")
        evicted = sum(entry.pre_state == "ready" for entry in self.entries)
        if self.evicted_count != evicted:
            raise ValueError("acceptance eviction count drift")
        expected_status = "already_absent" if evicted == 0 else "evicted"
        if self.status != expected_status:
            raise ValueError("acceptance eviction status drift")
        return self


class ExecutionStartedV1(PipelineModel):
    container_id: _BoundedText
    runtime_started_at: datetime
    input_view_digest: Digest
    step_jwt_id: UUID | None

    _runtime_started_is_aware = field_validator("runtime_started_at")(_aware)


class ExecutionCompleteV1(PipelineModel):
    exit_code: Literal[0]
    stage_result: StageResultV1
    stage_result_sha256: Digest
    final_output_upload_session_id: UUID

    @model_validator(mode="after")
    def result_digest_is_exact(self) -> ExecutionCompleteV1:
        if _canonical_sha256(self.stage_result) != self.stage_result_sha256:
            raise ValueError("StageResult digest drift")
        return self


class FinalOutputInventoryItemV1(PipelineModel):
    output_name: BindingName
    relative_path: str
    size_bytes: NonNegativeSafeInt
    sha256: Digest

    @field_validator("relative_path")
    @classmethod
    def confined_relative_path(cls, value: str) -> str:
        value = _nfc(value)
        path = PurePosixPath(value)
        if (
            not value
            or path.is_absolute()
            or "\\" in value
            or "\x00" in value
            or any(part in {"", ".", ".."} for part in value.split("/"))
        ):
            raise ValueError("final-output path must be a confined relative POSIX path")
        return value


class FinalOutputPrepareRequestV1(PipelineModel):
    schema_version: Literal["loom.final-output-prepare.v1"]
    stage_result: StageResultV1
    stage_result_sha256: Digest
    files: list[FinalOutputInventoryItemV1]

    @model_validator(mode="after")
    def canonical_inventory(self) -> FinalOutputPrepareRequestV1:
        if _canonical_sha256(self.stage_result) != self.stage_result_sha256:
            raise ValueError("StageResult digest drift")
        identities = [(item.output_name, item.relative_path) for item in self.files]
        if identities != sorted(
            identities, key=lambda item: (item[0].encode(), item[1].encode())
        ) or len(identities) != len(set(identities)):
            raise ValueError("final-output inventory must be bytewise sorted and unique")
        return self


class UploadFilePlanV1(PipelineModel):
    file_index: NonNegativeSafeInt
    output_name: BindingName
    relative_path: str
    role: Literal["semantic_document", "payload"]
    archive_format: Literal["none"]
    expected_size_bytes: NonNegativeSafeInt
    expected_sha256: Digest
    part_size_bytes: PositiveSafeInt


class UploadSessionGrantV1(PipelineModel):
    schema_version: Literal["loom.upload-session-grant.v1"]
    upload_session_id: UUID
    state: Literal["uploading"]
    upload_token: Annotated[str, StringConstraints(min_length=32, max_length=512)]
    token_expires_at: datetime
    part_size_bytes: PositiveSafeInt
    files: list[UploadFilePlanV1]

    _token_expiry_is_aware = field_validator("token_expires_at")(_aware)


class UploadTokenRenewV1(PipelineModel):
    schema_version: Literal["loom.upload-token-renew.v1"]


class PartReceiptV1(PipelineModel):
    file_index: NonNegativeSafeInt
    part_number: PositiveSafeInt
    size_bytes: NonNegativeSafeInt
    sha256: Digest


class FinalOutputFileCompleteV1(PipelineModel):
    schema_version: Literal["loom.final-output-file-complete.v1"]
    ordered_parts: list[PartReceiptV1]

    @field_validator("ordered_parts")
    @classmethod
    def parts_are_contiguous(cls, values: list[PartReceiptV1]) -> list[PartReceiptV1]:
        if [item.part_number for item in values] != list(range(1, len(values) + 1)):
            raise ValueError("final-output parts must be contiguous from one")
        return values


class VerifiedFileV1(PipelineModel):
    file_index: NonNegativeSafeInt
    size_bytes: NonNegativeSafeInt
    sha256: Digest
    state: Literal["verified"]


class FinalOutputSessionCommitV1(PipelineModel):
    schema_version: Literal["loom.final-output-session-commit.v1"]


class FinalOutputCommittedReadyV1(PipelineModel):
    schema_version: Literal["loom.final-output-committed-ready.v1"]
    upload_session_id: UUID
    state: Literal["committed_ready"]
    manifest_sha256: Digest
    committed_marker_sha256: Digest


class FinalOutputAbortV1(PipelineModel):
    schema_version: Literal["loom.final-output-abort.v1"]
    reason: _BoundedText

    _normalize_reason = field_validator("reason")(_nfc)


class ExecutionFailedV1(PipelineModel):
    exit_code: int
    retry_class: RetryClass
    reason_code: Annotated[str, StringConstraints(pattern=r"^[a-z][a-z0-9_]{0,127}$")]
    redacted_message: str
    stage_result: StageResultV1 | None
    stage_result_sha256: Digest | None
    teardown_observed: Literal[True]

    @field_validator("exit_code")
    @classmethod
    def exit_is_nonzero(cls, value: int) -> int:
        if isinstance(value, bool) or value == 0:
            raise ValueError("failed execution requires a nonzero integer exit code")
        return value

    @field_validator("redacted_message")
    @classmethod
    def message_is_safe(cls, value: str) -> str:
        value = _nfc(value)
        if len(value.encode("utf-8")) > 4_096:
            raise ValueError("redacted failure message exceeds 4096 UTF-8 bytes")
        reject_secret_literals(value)
        return value

    @model_validator(mode="after")
    def optional_result_group_is_exact(self) -> ExecutionFailedV1:
        if (self.stage_result is None) != (self.stage_result_sha256 is None):
            raise ValueError("StageResult and digest must be both null or both present")
        if self.stage_result is not None:
            assert self.stage_result_sha256 is not None
            if _canonical_sha256(self.stage_result) != self.stage_result_sha256:
                raise ValueError("StageResult digest drift")
            if self.stage_result.retry_class is not self.retry_class:
                raise ValueError("failure retry class drifts from StageResult")
        if self.retry_class is RetryClass.NONE:
            raise ValueError("failed execution cannot use retry_class=none")
        return self


class ExecutionCancelAckV1(PipelineModel):
    outcome: Literal["graceful", "forced"]
    observed_at: datetime
    last_committed_checkpoint_artifact_id: UUID | None
    teardown_observed: Literal[True]

    _observed_is_aware = field_validator("observed_at")(_aware)


class WorkerCleanupProofV1(PipelineModel):
    container_absent: Literal[True]
    cgroup_empty: Literal[True]
    network_absent: Literal[True]
    step_jwt_revoked: Literal[True]
    runtime_secret_mount_absent: Literal[True]
    scratch_absent: Literal[True]
    outputs_absent: Literal[True]
    input_views_absent: Literal[True]
    active_upload_session_ids: Annotated[list[UUID], Field(max_length=0)]


class WorkerLostCleanupAckV1(PipelineModel):
    schema_version: Literal["loom.worker-lost-cleanup-ack.v1"]
    observer_kind: Literal["worker_journal", "slurm_node_reaper"]
    observed_at: datetime
    allocation_id: str | None
    allocation_terminal: Literal[True] | None
    resources: WorkerCleanupProofV1

    _observed_is_aware = field_validator("observed_at")(_aware)

    @model_validator(mode="after")
    def observer_allocation_fields_are_exact(self) -> WorkerLostCleanupAckV1:
        if self.observer_kind == "worker_journal":
            if self.allocation_id is not None or self.allocation_terminal is not None:
                raise ValueError("worker journal cleanup cannot assert allocation state")
        elif (
            self.allocation_id is None
            or not self.allocation_id.strip()
            or self.allocation_terminal is not True
        ):
            raise ValueError("Slurm reaper cleanup requires a terminal allocation identity")
        return self
