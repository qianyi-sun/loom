"""Production Stage 1-only assembly for one claimed Pipeline attempt.

The shared scheduler deliberately injects this callable only into the two
controller-created BEHAVIOR GPU pools.  It turns the immutable claim into the
existing strict worker seams; no user supplied Docker option, filesystem path,
or Artifact locator crosses this boundary.
"""

from __future__ import annotations

import asyncio
import contextlib
import hashlib
import json
import os
import shutil
import stat
import subprocess
import time
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Literal, cast
from uuid import UUID, uuid5

from loom.integrations.behavior.canonical_json import load_canonical_document
from loom.integrations.behavior.contracts import StageRequestV1
from loom.integrations.behavior.stages.rollout import (
    RolloutGpuV1,
    RolloutRuntimeContractV1,
    validate_rollout_request,
)
from loom.pipeline.artifact_commit import (
    CommittedReadySessionV1,
    UploadSessionGrantV1,
    multipart_part_size,
)
from loom.pipeline.keys import canonical_document, digest_bytes
from loom.pipeline.live_preview import is_stage1_live_preview_eligible
from loom.pipeline.state import RetryClass, StageResultV1
from loom.pipeline.work_protocol import (
    ExecutionAttemptClaimV1,
    ExecutionCompleteV1,
    ExecutionFailedV1,
    ExecutionHeartbeatV1,
    ExecutionStartedV1,
    FinalOutputFileCompleteV1,
    FinalOutputInventoryItemV1,
    FinalOutputPrepareRequestV1,
    PartReceiptV1,
    VerifiedFileV1,
    WorkerCleanupProofV1,
)
from loom.security.redaction import redact_text
from loom_worker.artifact_input_journal import ArtifactInputJournal
from loom_worker.artifact_inputs import (
    ArtifactInputMaterializer as CasArtifactInputMaterializer,
)
from loom_worker.artifact_inputs import (
    ArtifactInputReadClient,
    CancellationSignal,
    LinuxReadOnlyViewMounter,
    MaterializedInputSet,
)
from loom_worker.config import WorkerSettings
from loom_worker.control_plane_client import (
    ExecutionAttemptClaimHeaders,
    HttpControlPlaneClient,
)
from loom_worker.pipeline_attempt_workspace import (
    MAX_COMPLETE_BYTES,
    parse_attempt_complete,
)
from loom_worker.pipeline_container_runner import (
    ArtifactCommitResult,
    ArtifactCommitter,
    ArtifactInputMaterializer,
    ContainerProcessResult,
    ExecutionCancellation,
    MaterializedInputView,
    PipelineCancelledError,
    PipelineContainerBackend,
    PipelineContainerRunner,
    PipelineExecutionRequest,
    PipelineProcessFailedError,
    build_pipeline_container_spec,
)
from loom_worker.pipeline_gpu_lifecycle import PipelineGpuLifecycleTracker
from loom_worker.pipeline_gpu_preflight import (
    AttestedGpuExecutionPreflight,
    GpuContainerPreflightObservation,
    GpuContainerPreflightPlan,
    build_gpu_container_preflight_plan,
)
from loom_worker.pipeline_live_preview import PipelineLivePreviewPublisher

_PIPELINE_POOLS = frozenset({"behavior-gpu-oldlab", "behavior-gpu-gb10"})
_WORKER_UID = 65532
_WORKER_GID = 65532
_HEARTBEAT_SECONDS = 20.0
_UPLOAD_CHUNK_BYTES = 64 * 1024 * 1024
_NAMESPACE = UUID("d2683215-91bb-4fe2-b624-eab04bd80e47")
HeartbeatPhase = Literal[
    "input_materializing",
    "container_starting",
    "running",
    "output_committing",
    "cancelling",
]
_STAGE1_INPUTS = (
    ("task_instance", "behavior_task_instance.v1"),
    ("dataset", "behavior_dataset_snapshot.v1"),
    ("policy", "behavior_policy_checkpoint.v1"),
)


def production_pipeline_enabled(settings: WorkerSettings) -> bool:
    """Only controller-created BEHAVIOR GPU workers run this assembly."""

    return bool(
        settings.pool_name in _PIPELINE_POOLS
        and getattr(settings, "require_cgroup_parent", False)
        and str(getattr(settings, "cgroup_parent", "")).strip()
        and str(getattr(settings, "slurm_job_id", "")).strip()
        and str(getattr(settings, "sandbox_identity", "")).strip()
        and str(getattr(settings, "candidate_sha", "")).strip()
        and str(getattr(settings, "compose_project", "")).strip()
    )


def _claim_headers(claim: ExecutionAttemptClaimV1) -> ExecutionAttemptClaimHeaders:
    return ExecutionAttemptClaimHeaders(
        claim_id=claim.claim_id,
        lease_epoch=claim.lease_epoch,
        lease_token=claim.lease_token,
    )


def _request_id(claim: ExecutionAttemptClaimV1, operation: str) -> UUID:
    return uuid5(_NAMESPACE, f"{claim.execution_attempt_id}:{claim.lease_epoch}:{operation}")


def _docker_not_found(exc: BaseException) -> bool:
    import docker

    return isinstance(exc, docker.errors.NotFound)


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _require_stage1_claim(
    claim: ExecutionAttemptClaimV1,
    settings: WorkerSettings,
) -> StageRequestV1:
    """Reject any non-Stage-1 work before creating attempt-local resources."""

    spec = claim.execution_spec_snapshot
    node = spec.container_node
    bindings = claim.input_bindings
    allocation = claim.slurm_gpu_allocation_evidence
    selected_variant = next(
        (
            item
            for item in claim.resource_profile_snapshot.execution_variants
            if item.variant_id == spec.execution_variant_id
        ),
        None,
    )
    expected_pool_cluster = {
        "behavior-gpu-oldlab": "oldlab",
        "behavior-gpu-gb10": "gb10",
    }.get(settings.pool_name)
    if (
        expected_pool_cluster is None
        or not production_pipeline_enabled(settings)
        or allocation is None
        or allocation.slurm_cluster_id != expected_pool_cluster
        or str(settings.slurm_job_id) != allocation.job_id
        or selected_variant is None
        or selected_variant.device_roles is None
        or selected_variant.pool_class != settings.pool_name
        or spec.node_key != "rollout"
        or claim.node_key != "rollout"
        or spec.shard_key != "singleton"
        or node.node_kind != "container"
        or node.resource_profile != "behavior-sim-local-none@1"
        or claim.resource_profile_snapshot.name != "behavior-sim-local-none"
        or claim.resource_profile_snapshot.version != 1
        or claim.network_profile != "none"
        or node.network_profile != "none"
        or claim.provider_connection_ref is not None
        or claim.secret_refs
        or claim.control_binding_snapshot is not None
        or spec.control_binding_snapshots
        or claim.acceptance_preflight is not None
        or claim.stage1_smoke is None
        or claim.resume_checkpoint is not None
        or claim.checkpoint is not None
        or claim.fanout_commit is not None
        or node.fanout is not None
        or node.fanout_commit is not None
        or node.checkpoint is not None
        or claim.stage_request is None
    ):
        raise RuntimeError("pipeline_stage1_claim_not_eligible")
    grant = claim.stage1_smoke
    if (
        grant.pipeline_run_id != claim.pipeline_run_id
        or grant.recipe_digest != claim.recipe_digest
        or grant.platform_child_digest != spec.resolved_image_manifest_digest
        or grant.image_runtime_contract_digest != claim.image_runtime_contract_digest
        or grant.resolved_input_bindings_digest != spec.resolved_input_bindings_digest
        or grant.renderer_digest != claim.stage_request.renderer_digest
    ):
        raise RuntimeError("pipeline_stage1_authority_grant_drift")
    actual_bindings = [(item.binding_name, item.artifact_type) for item in bindings]
    if actual_bindings != list(_STAGE1_INPUTS) or any(
        item.cardinality != "one"
        or len(item.items) != 1
        or item.items[0].item_key != "singleton"
        for item in bindings
    ):
        raise RuntimeError("pipeline_stage1_input_contract_drift")
    node_inputs = [
        (
            value.get("source"),
            value.get("binding_name"),
            value.get("artifact_type"),
            value.get("input_name"),
        )
        for value in (item.model_dump(mode="python") for item in node.inputs)
    ]
    if node_inputs != [
        ("run_input", name, artifact_type, name)
        for name, artifact_type in _STAGE1_INPUTS
    ]:
        raise RuntimeError("pipeline_stage1_graph_input_drift")
    if len(node.outputs) != 1 or (
        node.outputs[0].name,
        node.outputs[0].artifact_type,
        node.outputs[0].required,
        node.outputs[0].producer,
    ) != ("rollout", "behavior_rollout_bundle.v1", True, "container"):
        raise RuntimeError("pipeline_stage1_output_contract_drift")
    request = StageRequestV1.model_validate_json(
        claim.stage_request.canonical_jcs_lf.encode("utf-8")
    )
    validate_rollout_request(request)
    selected_image = (
        claim.image.rsplit("@", maxsplit=1)[0]
        + "@"
        + spec.resolved_image_manifest_digest
    )
    if (
        request.run_id != claim.pipeline_run_id
        or request.stage_run_id != claim.stage_run_id
        or request.attempt_id != claim.execution_attempt_id
        or request.inputs != bindings
        or request.orchestration is not None
        or request.provenance.recipe_digest != claim.recipe_digest
        or request.provenance.resolved_input_bindings_digest
        != spec.resolved_input_bindings_digest
        or request.provenance.execution_spec_digest != claim.execution_spec_digest
        or request.provenance.image_digest != selected_image
        or request.provenance.control_binding is not None
        or request.budget.timeout_seconds != claim.timeout_seconds
        or request.budget.max_attempts != node.max_attempts
    ):
        raise RuntimeError("pipeline_stage1_request_claim_drift")
    return request


@dataclass(slots=True)
class PipelineAttemptPaths:
    root: Path
    inputs: Path
    outputs: Path
    scratch: Path

    @classmethod
    def create(cls, base: Path, attempt_id: UUID) -> PipelineAttemptPaths:
        root = (base / "attempts" / str(attempt_id)).resolve()
        inputs = (base / "input-views" / str(attempt_id)).resolve()
        if root.exists():
            raise RuntimeError("pipeline_attempt_workspace_exists")
        outputs = root / "outputs"
        scratch = root / "scratch"
        for value in (root, outputs, scratch):
            value.mkdir(mode=0o700, parents=value is root, exist_ok=False)
        for value in (outputs, scratch):
            os.chown(value, _WORKER_UID, _WORKER_GID)
        inputs.parent.mkdir(parents=True, mode=0o700, exist_ok=True)
        if inputs.exists():
            raise RuntimeError("pipeline_attempt_input_view_exists")
        return cls(root=root, inputs=inputs, outputs=outputs, scratch=scratch)

    def remove_writable(self) -> None:
        for value in (self.outputs, self.scratch):
            if value.exists():
                shutil.rmtree(value)

    def remove_root_if_empty(self) -> None:
        with contextlib.suppress(FileNotFoundError, OSError):
            self.root.rmdir()


@dataclass(slots=True)
class PipelineCleanupJournal:
    root: Path

    def __post_init__(self) -> None:
        self.root.mkdir(parents=True, mode=0o700, exist_ok=True)

    def record(self, claim: ExecutionAttemptClaimV1, *, container_id: str | None) -> None:
        value = {
            "schema_version": "loom.pipeline-worker-cleanup.v1",
            "attempt_id": str(claim.execution_attempt_id),
            "claim_id": str(claim.claim_id),
            "lease_epoch": claim.lease_epoch,
            "container_id": container_id,
        }
        target = self.root / f"{claim.execution_attempt_id}.json"
        partial = self.root / f".{claim.execution_attempt_id}.tmp"
        partial.write_bytes(canonical_document(value))
        os.chmod(partial, 0o600)
        os.replace(partial, target)
        _fsync_directory(self.root)

    def clear(self, attempt_id: UUID) -> None:
        with contextlib.suppress(FileNotFoundError):
            (self.root / f"{attempt_id}.json").unlink()
            _fsync_directory(self.root)

    def cleanup_orphans(
        self,
        *,
        docker_client: Any,
        attempts_root: Path,
        input_views_root: Path,
    ) -> list[UUID]:
        """Reap only exact journalled attempts left by an exited process."""

        cleaned: list[UUID] = []
        for record in sorted(self.root.glob("*.json"), key=lambda item: item.name):
            try:
                value = load_canonical_document(record, max_bytes=4096)
                attempt_id = UUID(str(value["attempt_id"]))
                claim_id = UUID(str(value["claim_id"]))
                lease_epoch = value["lease_epoch"]
                container_id = value["container_id"]
                if (
                    not isinstance(value, dict)
                    or set(value)
                    != {
                        "schema_version",
                        "attempt_id",
                        "claim_id",
                        "lease_epoch",
                        "container_id",
                    }
                    or value["schema_version"] != "loom.pipeline-worker-cleanup.v1"
                    or record.stem != str(attempt_id)
                    or isinstance(lease_epoch, bool)
                    or not isinstance(lease_epoch, int)
                    or lease_epoch < 1
                    or not isinstance(claim_id, UUID)
                    or not (container_id is None or isinstance(container_id, str))
                ):
                    raise ValueError("cleanup journal record drift")
            except (OSError, ValueError, KeyError, TypeError) as exc:
                raise RuntimeError("pipeline_cleanup_journal_corrupt") from exc
            if isinstance(container_id, str) and container_id:
                try:
                    container = docker_client.containers.get(container_id)
                except Exception as exc:
                    if not _docker_not_found(exc):
                        raise
                else:
                    labels = container.labels or {}
                    if labels.get("loom.execution_attempt_id") != str(attempt_id):
                        raise RuntimeError("pipeline_orphan_container_identity_drift")
                    container.remove(force=True)
            attempt_root = (attempts_root / str(attempt_id)).resolve()
            if attempt_root.parent == attempts_root.resolve() and attempt_root.exists():
                shutil.rmtree(attempt_root)
            input_root = (input_views_root / str(attempt_id)).resolve()
            if input_root.parent == input_views_root.resolve() and input_root.exists():
                _remove_orphan_input_view(input_root)
            cleaned.append(attempt_id)
        return cleaned


def _remove_orphan_input_view(root: Path) -> None:
    """Unmount only descendants of one journal-bound Attempt, then remove it."""

    mounts = sorted(
        _mountpoints_below(root),
        key=lambda item: len(item.parts),
        reverse=True,
    )
    for target in mounts:
        completed = subprocess.run(
            ["umount", str(target)],
            check=False,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        if completed.returncode != 0:
            raise RuntimeError("pipeline_orphan_input_unmount_failed")
    for directory in sorted(
        (item for item in root.rglob("*") if item.is_dir()),
        key=lambda item: len(item.parts),
        reverse=True,
    ):
        os.chmod(directory, 0o700, follow_symlinks=False)
    os.chmod(root, 0o700, follow_symlinks=False)
    shutil.rmtree(root)


def _mountpoints_below(root: Path) -> list[Path]:
    mountinfo = Path("/proc/self/mountinfo")
    if not mountinfo.is_file():
        return [root] if os.path.ismount(root) else []
    result: list[Path] = []
    for line in mountinfo.read_text(encoding="utf-8").splitlines():
        fields = line.split(" ")
        if len(fields) < 5:
            continue
        target = Path(
            fields[4]
            .replace("\\040", " ")
            .replace("\\011", "\t")
            .replace("\\012", "\n")
            .replace("\\134", "\\")
        )
        with contextlib.suppress(ValueError):
            target.relative_to(root)
            result.append(target)
    return result


@dataclass(slots=True)
class AttemptControlCancellation(ExecutionCancellation, CancellationSignal):
    claim: ExecutionAttemptClaimV1
    control_plane: HttpControlPlaneClient
    paths: PipelineAttemptPaths
    backend: DockerPipelineBackend | None = None
    committer: HttpFinalOutputCommitter | None = None
    current_seq: int = 0
    sticky: bool = False
    phase_ref: list[HeartbeatPhase] | None = None
    acknowledged: bool = False

    async def requested(self, *, attempt_id: UUID | None = None) -> bool:
        if attempt_id is not None and attempt_id != self.claim.execution_attempt_id:
            raise RuntimeError("pipeline_attempt_identity_drift")
        if self.sticky:
            return True
        response = await self.control_plane.get_execution_attempt_control(
            attempt_id=self.claim.execution_attempt_id,
            claim=_claim_headers(self.claim),
            after_seq=self.current_seq,
        )
        commands = list(response.get("commands", []))
        self.current_seq = int(response.get("current_seq", self.current_seq))
        self.sticky = any(item.get("command") == "cancel_requested" for item in commands)
        if self.sticky and self.phase_ref is not None:
            self.phase_ref[0] = "cancelling"
        return self.sticky

    async def acknowledge(
        self,
        *,
        attempt_id: UUID,
        forced: bool,
        teardown_observed: bool,
    ) -> None:
        if attempt_id != self.claim.execution_attempt_id or not teardown_observed:
            raise RuntimeError("pipeline_cancellation_cleanup_not_observed")
        self.paths.remove_writable()
        input_absent = not self.paths.inputs.exists()
        container_absent = self.backend is None or self.backend.container_absent
        process_group_present = (
            False
            if self.backend is None
            else await self.backend.expected_process_group_present(attempt_id=attempt_id)
        )
        upload_absent = (
            self.committer is None or self.committer.active_session_id is None
        )
        if (
            self.claim.network_profile != "none"
            or not input_absent
            or not container_absent
            or process_group_present
            or not upload_absent
            or self.paths.outputs.exists()
            or self.paths.scratch.exists()
        ):
            raise RuntimeError("pipeline_cancellation_cleanup_not_observed")
        proof = WorkerCleanupProofV1(
            container_absent=True,
            cgroup_empty=True,
            network_absent=True,
            step_jwt_revoked=True,
            runtime_secret_mount_absent=True,
            scratch_absent=True,
            outputs_absent=True,
            input_views_absent=True,
            active_upload_session_ids=[],
        )
        await self.control_plane.acknowledge_execution_attempt_cancel(
            attempt_id=attempt_id,
            claim=_claim_headers(self.claim),
            request_id=_request_id(self.claim, "cancel-ack"),
            payload={
                "outcome": "forced" if forced else "graceful",
                "observed_at": datetime.now(UTC).isoformat(),
                "last_committed_checkpoint_artifact_id": None,
                "teardown_observed": True,
                "resources": proof.model_dump(mode="json"),
            },
        )
        self.acknowledged = True


@dataclass(slots=True)
class ClaimArtifactMaterializer(ArtifactInputMaterializer):
    claim: ExecutionAttemptClaimV1
    materializer: CasArtifactInputMaterializer
    cancellation: AttemptControlCancellation
    active: MaterializedInputSet | None = None

    async def materialize(
        self,
        *,
        attempt_id: UUID,
        bindings: Sequence[Mapping[str, object]],
        destination: Path,
        stage_request: bytes | None,
    ) -> MaterializedInputView:
        if attempt_id != self.claim.execution_attempt_id:
            raise RuntimeError("pipeline_attempt_identity_drift")
        expected_bindings = [item.model_dump(mode="json") for item in self.claim.input_bindings]
        if list(bindings) != expected_bindings or destination != self.materializer.attempt_input_root.resolve() / str(attempt_id):
            raise RuntimeError("pipeline_input_claim_drift")
        expected_request = (
            self.claim.stage_request.canonical_jcs_lf.encode("utf-8")
            if self.claim.stage_request is not None
            else None
        )
        if stage_request != expected_request:
            raise RuntimeError("pipeline_stage_request_drift")
        active = await self.materializer.materialize_inputs(
            claim=self.claim,
            cancellation=self.cancellation,
        )
        self.active = await active.__aenter__()
        if self.active.root is None or self.active.input_view_digest is None:
            raise RuntimeError("pipeline_input_materialization_incomplete")
        root = self.active.root
        input_view_digest = self.active.input_view_digest
        try:
            _install_runtime_contract(root, self.claim)
        except BaseException:
            await self.active.close()
            self.active = None
            raise
        return MaterializedInputView(
            root=root,
            input_view_digest=input_view_digest,
            stage_request_path=self.active.stage_request_path,
            control_binding_path=self.active.control_binding_path,
        )

    async def release(self, *, attempt_id: UUID, input_view: MaterializedInputView) -> None:
        if attempt_id != self.claim.execution_attempt_id or self.active is None:
            raise RuntimeError("pipeline_input_release_drift")
        await self.active.close()
        self.active = None


def _rollout_runtime_contract(
    claim: ExecutionAttemptClaimV1,
) -> RolloutRuntimeContractV1:
    allocation = claim.slurm_gpu_allocation_evidence
    if allocation is None:
        raise RuntimeError("stage1_gpu_allocation_missing")
    image = claim.image_runtime_contract_snapshot
    capability = claim.worker_capability_snapshot
    expected_image_platform = (
        "linux/amd64" if allocation.slurm_cluster_id == "oldlab" else "linux/arm64"
    )
    if image.platform != expected_image_platform or image.gpu_vendor != "nvidia":
        raise RuntimeError("stage1_runtime_contract_image_drift")
    if [item.device_uuid for item in capability.gpu_devices] != allocation.device_uuids:
        raise RuntimeError("stage1_runtime_contract_device_drift")

    variant = next(
        (
            item
            for item in claim.resource_profile_snapshot.execution_variants
            if item.variant_id == claim.execution_spec_snapshot.execution_variant_id
        ),
        None,
    )
    if variant is None or variant.device_roles is None:
        raise RuntimeError("stage1_runtime_contract_roles_missing")
    model: Literal["RTX 5080", "GB10"] = (
        "RTX 5080" if allocation.slurm_cluster_id == "oldlab" else "GB10"
    )
    expected_capability_model = (
        "NVIDIA GeForce RTX 5080"
        if allocation.slurm_cluster_id == "oldlab"
        else "NVIDIA GB10"
    )
    devices: list[RolloutGpuV1] = []
    for logical_index, device in enumerate(capability.gpu_devices):
        if device.model != expected_capability_model:
            raise RuntimeError("stage1_runtime_contract_device_model_drift")
        roles: list[Literal["sim", "vla"]] = []
        if variant.device_roles.sim_gpu_index == logical_index:
            roles.append("sim")
        if variant.device_roles.vla_gpu_index == logical_index:
            roles.append("vla")
        devices.append(
            RolloutGpuV1(
                logical_index=logical_index,
                model=model,
                roles=roles,
            )
        )
    return RolloutRuntimeContractV1(
        platform=allocation.slurm_cluster_id,
        devices=devices,
        system_env={},
    )


def _install_runtime_contract(root: Path, claim: ExecutionAttemptClaimV1) -> Path:
    """Install the claim-derived Stage 1 control file without following links."""

    target = root / "runtime-contract.json"
    value = canonical_document(_rollout_runtime_contract(claim))
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    descriptor = os.open(target, flags, 0o444)
    try:
        with os.fdopen(descriptor, "wb", closefd=False) as stream:
            stream.write(value)
            stream.flush()
            os.fsync(stream.fileno())
        os.fchmod(descriptor, 0o444)
    finally:
        os.close(descriptor)
    _fsync_directory(root)
    return target


@dataclass(slots=True)
class HttpFinalOutputCommitter(ArtifactCommitter):
    claim: ExecutionAttemptClaimV1
    control_plane: HttpControlPlaneClient
    active_session_id: UUID | None = None
    phase_ref: list[HeartbeatPhase] | None = None
    cancellation: AttemptControlCancellation | None = None

    async def commit(
        self,
        *,
        attempt_id: UUID,
        outputs_dir: Path,
        stage_result: Mapping[str, object],
        stage_result_digest: str,
    ) -> ArtifactCommitResult:
        if attempt_id != self.claim.execution_attempt_id:
            raise RuntimeError("pipeline_attempt_identity_drift")
        if self.phase_ref is not None:
            self.phase_ref[0] = "output_committing"
        if self.cancellation is not None and await self.cancellation.requested():
            raise PipelineCancelledError("Pipeline cancelled before output commit")
        files = _final_output_inventory(outputs_dir)
        prepare = FinalOutputPrepareRequestV1(
            schema_version="loom.final-output-prepare.v1",
            stage_result=StageResultV1.model_validate_json(canonical_document(stage_result)),
            stage_result_sha256=stage_result_digest,
            files=[item[0] for item in files],
        )
        prepare_id = _request_id(self.claim, "final-output-prepare")
        grant = UploadSessionGrantV1.model_validate_json(
            canonical_document(
                await self.control_plane.prepare_final_output(
                    attempt_id=attempt_id,
                    claim=_claim_headers(self.claim),
                    request_id=prepare_id,
                    payload=prepare.model_dump(mode="json"),
                )
            )
        )
        session_id = grant.upload_session_id
        self.active_session_id = session_id
        token = grant.upload_token
        server_plans = sorted(grant.files, key=lambda item: item.file_index)
        plans = [item for item in server_plans if item.producer == "container"]
        if [item.file_index for item in plans] != list(range(len(files))):
            raise RuntimeError("final_output_upload_plan_drift")
        if any(
            item.producer != "platform" or item.file_index < len(files)
            for item in server_plans
            if item.producer != "container"
        ):
            raise RuntimeError("final_output_upload_plan_drift")

        async def refresh_upload_token() -> None:
            nonlocal grant, plans, server_plans, token
            if grant.token_expires_at > datetime.now(UTC) + timedelta(minutes=2):
                return
            renewed = UploadSessionGrantV1.model_validate_json(
                canonical_document(
                    await self.control_plane.renew_final_output_token(
                        attempt_id=attempt_id,
                        session_id=session_id,
                        claim=_claim_headers(self.claim),
                    )
                )
            )
            renewed_server_plans = sorted(renewed.files, key=lambda item: item.file_index)
            renewed_plans = [
                item for item in renewed_server_plans if item.producer == "container"
            ]
            if (
                renewed.upload_session_id != session_id
                or renewed_server_plans != server_plans
                or renewed_plans != plans
            ):
                raise RuntimeError("final_output_token_renewal_drift")
            grant = renewed
            plans = renewed_plans
            server_plans = renewed_server_plans
            token = renewed.upload_token

        session_terminal = False
        try:
            for index, (plan, (descriptor, path)) in enumerate(
                zip(plans, files, strict=True)
            ):
                expected_role = (
                    "semantic_document"
                    if descriptor.relative_path.endswith("/artifact.json")
                    else "payload"
                )
                workspace_prefix = f"artifacts/{descriptor.output_name}/"
                relative_path = descriptor.relative_path.removeprefix(workspace_prefix)
                output_types = {item.name: item.artifact_type for item in prepare.stage_result.outputs}
                if (
                    plan.artifact_name != descriptor.output_name
                    or plan.relative_path != relative_path
                    or plan.expected_size != descriptor.size_bytes
                    or plan.expected_sha256 != descriptor.sha256
                    or plan.artifact_type != output_types.get(descriptor.output_name)
                    or plan.producer != "container"
                    or plan.role != expected_role
                    or plan.archive_format != "none"
                ):
                    raise RuntimeError("final_output_upload_plan_drift")
                if self.cancellation is not None and await self.cancellation.requested():
                    raise PipelineCancelledError("Pipeline cancelled during output commit")
                part_size = multipart_part_size(plan.expected_max_bytes)
                receipts: list[PartReceiptV1] = []
                with path.open("rb") as stream:
                    part_number = 0
                    while True:
                        value = stream.read(part_size)
                        if not value and part_number > 0:
                            break
                        if self.cancellation is not None and await self.cancellation.requested():
                            raise PipelineCancelledError(
                                "Pipeline cancelled during output upload"
                            )
                        part_number += 1
                        value_digest = digest_bytes(value)
                        await refresh_upload_token()
                        receipt = PartReceiptV1.model_validate_json(
                            canonical_document(
                                await self.control_plane.upload_final_output_part(
                                    attempt_id=attempt_id,
                                    session_id=session_id,
                                    file_index=index,
                                    part_number=part_number,
                                    claim=_claim_headers(self.claim),
                                    request_id=_request_id(
                                        self.claim,
                                        f"final-output-part:{index}:{part_number}",
                                    ),
                                    upload_token=token,
                                    content_sha256=value_digest,
                                    content=value,
                                )
                            )
                        )
                        if (
                            receipt.file_index != index
                            or receipt.part_number != part_number
                            or receipt.size_bytes != len(value)
                            or receipt.sha256 != value_digest
                        ):
                            raise RuntimeError("final_output_part_receipt_drift")
                        receipts.append(receipt)
                        if not value:
                            break
                complete_file = FinalOutputFileCompleteV1(
                    schema_version="loom.final-output-file-complete.v1",
                    ordered_parts=receipts,
                )
                await refresh_upload_token()
                verified = VerifiedFileV1.model_validate_json(
                    canonical_document(
                        await self.control_plane.complete_final_output_file(
                            attempt_id=attempt_id,
                            session_id=session_id,
                            file_index=index,
                            claim=_claim_headers(self.claim),
                            request_id=_request_id(
                                self.claim, f"final-output-file:{index}"
                            ),
                            upload_token=token,
                            payload=complete_file.model_dump(mode="json"),
                        )
                    )
                )
                if (
                    verified.file_index != index
                    or verified.size_bytes != descriptor.size_bytes
                    or verified.sha256 != descriptor.sha256
                ):
                    raise RuntimeError("final_output_verified_file_drift")
            await refresh_upload_token()
            committed = CommittedReadySessionV1.model_validate_json(
                canonical_document(
                    await self.control_plane.commit_final_output_session(
                        attempt_id=attempt_id,
                        session_id=session_id,
                        claim=_claim_headers(self.claim),
                        request_id=_request_id(self.claim, "final-output-commit"),
                        upload_token=token,
                    )
                )
            )
            if committed.upload_session_id != session_id:
                raise RuntimeError("final_output_commit_readback_drift")
            session_terminal = True
            return ArtifactCommitResult(
                manifest_digest=committed.manifest_sha256,
                upload_session_id=session_id,
            )
        except BaseException as exc:
            aborted = await self.control_plane.abort_final_output_session(
                attempt_id=attempt_id,
                session_id=session_id,
                claim=_claim_headers(self.claim),
                request_id=_request_id(self.claim, "final-output-abort"),
                reason="worker_output_commit_failed",
            )
            if (
                aborted.get("state") != "aborted"
                or UUID(str(aborted.get("upload_session_id"))) != session_id
            ):
                raise RuntimeError("final_output_abort_readback_drift") from exc
            session_terminal = True
            raise
        finally:
            if session_terminal:
                self.active_session_id = None


def _final_output_inventory(
    outputs_dir: Path,
) -> list[tuple[FinalOutputInventoryItemV1, Path]]:
    artifacts = outputs_dir / "artifacts"
    if artifacts.is_symlink() or not artifacts.is_dir():
        raise RuntimeError("final_output_artifacts_missing")
    result: list[tuple[FinalOutputInventoryItemV1, Path]] = []
    for output in sorted(artifacts.iterdir(), key=lambda item: item.name.encode()):
        if output.is_symlink() or not output.is_dir():
            raise RuntimeError("final_output_tree_invalid")
        for path in sorted(output.rglob("*"), key=lambda item: item.relative_to(outputs_dir).as_posix().encode()):
            details = path.lstat()
            if stat.S_ISLNK(details.st_mode) or not (
                stat.S_ISDIR(details.st_mode) or stat.S_ISREG(details.st_mode)
            ):
                raise RuntimeError("final_output_tree_invalid")
            if not stat.S_ISREG(details.st_mode):
                continue
            digest = hashlib.sha256()
            with path.open("rb") as stream:
                while value := stream.read(_UPLOAD_CHUNK_BYTES):
                    digest.update(value)
            result.append(
                (
                    FinalOutputInventoryItemV1(
                        output_name=output.name,
                        relative_path=path.relative_to(outputs_dir).as_posix(),
                        size_bytes=details.st_size,
                        sha256=f"sha256:{digest.hexdigest()}",
                    ),
                    path,
                )
            )
    if not result:
        raise RuntimeError("final_output_inventory_empty")
    return result


def _validate_complete_marker(outputs_dir: Path, *, stage_result_digest: str) -> None:
    complete_path = outputs_dir / "COMPLETE.json"
    details = complete_path.lstat()
    if stat.S_ISLNK(details.st_mode) or not stat.S_ISREG(details.st_mode):
        raise RuntimeError("complete_marker_not_regular")
    complete_document = load_canonical_document(
        complete_path,
        max_bytes=MAX_COMPLETE_BYTES,
    )
    complete = parse_attempt_complete(complete_document)
    if complete.stage_result_sha256 != stage_result_digest:
        raise RuntimeError("complete_marker_stage_result_digest_mismatch")

    inventory = _final_output_inventory(outputs_dir)
    actual = {
        descriptor.relative_path: descriptor
        for descriptor, _path in inventory
    }
    expected_paths: list[str] = []
    for output in complete.outputs:
        artifact_path = f"artifacts/{output.name}/artifact.json"
        expected_paths.append(artifact_path)
        artifact = actual.get(artifact_path)
        if artifact is None or artifact.sha256 != output.artifact_json_sha256:
            raise RuntimeError("complete_marker_artifact_digest_mismatch")
        for item in output.files:
            relative_path = f"artifacts/{output.name}/{item.relative_path}"
            expected_paths.append(relative_path)
            uploaded = actual.get(relative_path)
            if (
                uploaded is None
                or uploaded.sha256 != item.sha256
                or uploaded.size_bytes != item.size_bytes
            ):
                raise RuntimeError("complete_marker_payload_inventory_mismatch")
    if expected_paths != [item.relative_path for item, _path in inventory]:
        raise RuntimeError("complete_marker_output_inventory_mismatch")


@dataclass(slots=True)
class DockerPipelineBackend(PipelineContainerBackend):
    claim: ExecutionAttemptClaimV1
    control_plane: HttpControlPlaneClient
    paths: PipelineAttemptPaths
    cleanup_journal: PipelineCleanupJournal
    cgroup_parent: str | None
    identity_labels: tuple[tuple[str, str], ...]
    _client: Any | None = None
    _container: Any | None = None
    container_absent: bool = True
    phase_ref: list[HeartbeatPhase] | None = None

    def _docker(self) -> Any:
        if self._client is None:
            import docker

            self._client = docker.from_env()
        return self._client

    def _container_name(self, *, preflight: bool) -> str:
        role = "preflight" if preflight else "stage"
        return f"loom-pipeline-{role}-{self.claim.execution_attempt_id}"

    def _create_kwargs(self, spec: Any, *, preflight: bool = False) -> dict[str, Any]:
        values = dict(spec.docker_create_kwargs())
        labels = dict(self.identity_labels)
        labels.update(
            {
                "loom.execution_attempt_id": str(self.claim.execution_attempt_id),
                "loom.claim_id": str(self.claim.claim_id),
                "loom.lease_epoch": str(self.claim.lease_epoch),
                "loom.pipeline_role": "gpu-preflight" if preflight else "stage",
            }
        )
        values["labels"] = labels
        values["name"] = self._container_name(preflight=preflight)
        if self.cgroup_parent is not None:
            values["cgroup_parent"] = self.cgroup_parent
        return values

    async def _remove_exact_container(self, container: Any) -> None:
        try:
            await asyncio.to_thread(container.remove, force=True)
        except Exception as exc:
            import docker

            if not isinstance(exc, docker.errors.NotFound):
                raise

    async def run(
        self,
        *,
        attempt_id: UUID,
        spec: Any,
        input_view: MaterializedInputView,
    ) -> ContainerProcessResult:
        if attempt_id != self.claim.execution_attempt_id:
            raise RuntimeError("pipeline_attempt_identity_drift")
        self.cleanup_journal.record(
            self.claim,
            container_id=self._container_name(preflight=False),
        )
        container = await asyncio.to_thread(
            self._docker().containers.create, **self._create_kwargs(spec)
        )
        self._container = container
        self.container_absent = False
        await asyncio.to_thread(container.start)
        await self.control_plane.report_execution_attempt_started(
            attempt_id=attempt_id,
            claim=_claim_headers(self.claim),
            request_id=_request_id(self.claim, "started"),
            payload=ExecutionStartedV1(
                container_id=str(container.id),
                runtime_started_at=datetime.now(UTC),
                input_view_digest=input_view.input_view_digest,
                step_jwt_id=None,
            ).model_dump(mode="json"),
        )
        if self.phase_ref is not None:
            self.phase_ref[0] = "running"
        waited = await asyncio.to_thread(container.wait, timeout=self.claim.timeout_seconds)
        exit_code = int(waited.get("StatusCode", waited.get("statusCode", 70)))
        if exit_code != 0:
            return ContainerProcessResult(
                exit_code=exit_code,
                stage_result=None,
                stage_result_digest=None,
            )
        result_path = self.paths.outputs / "stage_result.json"
        raw = result_path.read_bytes()
        result = StageResultV1.model_validate_json(raw)
        if canonical_document(result) != raw:
            raise RuntimeError("stage_result_not_canonical")
        _validate_complete_marker(self.paths.outputs, stage_result_digest=digest_bytes(raw))
        return ContainerProcessResult(
            exit_code=0,
            stage_result=result.model_dump(mode="json"),
            stage_result_digest=digest_bytes(raw),
        )

    async def observe_gpu_preflight(
        self,
        attempt_id: UUID,
        plan: GpuContainerPreflightPlan,
        spec: Any,
        input_view: MaterializedInputView,
    ) -> GpuContainerPreflightObservation:
        del input_view
        values = self._create_kwargs(spec, preflight=True)
        values["command"] = list(plan.preflight_argv)
        self.cleanup_journal.record(
            self.claim,
            container_id=self._container_name(preflight=True),
        )
        container = await asyncio.to_thread(self._docker().containers.create, **values)
        try:
            await asyncio.to_thread(container.start)
            waited = await asyncio.to_thread(container.wait, timeout=plan.timeout_seconds)
            if int(waited.get("StatusCode", 70)) != 0:
                raise RuntimeError("gpu_preflight_failed")
            raw = await asyncio.to_thread(container.logs, stdout=True, stderr=False)
            document = json.loads(raw)
            return GpuContainerPreflightObservation(
                cpu_arch=str(document["cpu_arch"]),
                platform_manifest_digest=str(document["platform_manifest_digest"]),
                preflight_digest=str(document["preflight_digest"]),
                cuda_userspace_version=str(document["cuda_userspace_version"]),
                egl_healthy=document["egl_healthy"] is True,
                visible_device_uuids=tuple(document["visible_device_uuids"]),
                device_models=tuple(document["device_models"]),
                isaac_healthy=document["isaac_healthy"] is True,
                omnigibson_healthy=document["omnigibson_healthy"] is True,
                vla_healthy=document["vla_healthy"] is True,
                concurrent_vla_isaac_healthy=document["concurrent_vla_isaac_healthy"] is True,
            )
        finally:
            await self._remove_exact_container(container)
            self.cleanup_journal.record(self.claim, container_id=None)

    async def terminate(self, *, attempt_id: UUID, grace_seconds: int) -> bool:
        del attempt_id
        if self._container is None:
            return False
        await asyncio.to_thread(self._container.stop, timeout=grace_seconds)
        await asyncio.to_thread(self._container.reload)
        if self._container.status != "running":
            return False
        await asyncio.to_thread(self._container.kill)
        return True

    async def expected_process_group_present(self, *, attempt_id: UUID) -> bool:
        del attempt_id
        if self._container is None:
            return False
        try:
            await asyncio.to_thread(self._container.reload)
        except Exception as exc:
            if _docker_not_found(exc):
                return False
            raise
        return bool(self._container.status == "running")

    async def teardown(self, *, attempt_id: UUID) -> None:
        if attempt_id != self.claim.execution_attempt_id:
            raise RuntimeError("pipeline_attempt_identity_drift")
        if self._container is not None:
            await self._remove_exact_container(self._container)
            self._container = None
        self.container_absent = True


@dataclass(slots=True)
class PipelineWorkerRuntime:
    settings: WorkerSettings
    control_plane: HttpControlPlaneClient
    base: Path = field(init=False)
    journal: ArtifactInputJournal = field(init=False)
    cleanup_journal: PipelineCleanupJournal = field(init=False)
    worker_id: UUID | None = None
    _orphan_cleanup_done: bool = False

    def __post_init__(self) -> None:
        self.base = (self.settings.trajectory_cache_dir.parent / "pipeline").resolve()
        self.base.mkdir(parents=True, mode=0o700, exist_ok=True)
        raw_bytes = shutil.disk_usage(self.base).total
        self.journal = ArtifactInputJournal(
            database_path=self.base / "input-cache.sqlite3",
            cas_root=self.base / "cas",
            capacity_bytes=raw_bytes * 85 // 100,
        )
        self.cleanup_journal = PipelineCleanupJournal(self.base / "cleanup-journal")

    def registration_cache_fields(self) -> dict[str, int]:
        self._cleanup_orphans()
        return self.journal.capacity_snapshot().registration_fields()

    def bind_worker(self, worker_id: UUID) -> None:
        self.worker_id = worker_id
        self._cleanup_orphans()

    def _cleanup_orphans(self) -> None:
        if self._orphan_cleanup_done:
            return
        try:
            import docker

            client = docker.from_env()
            try:
                cleaned = self.cleanup_journal.cleanup_orphans(
                    docker_client=client,
                    attempts_root=(self.base / "attempts").resolve(),
                    input_views_root=(self.base / "input-views").resolve(),
                )
                for attempt_id in cleaned:
                    self.journal.release_attempt(attempt_id)
                    self.cleanup_journal.clear(attempt_id)
            finally:
                client.close()
        except Exception:
            # Registration remains fail closed: leave the journal in place so
            # a later startup can converge without advertising stale capacity.
            raise RuntimeError("pipeline_orphan_cleanup_failed") from None
        self._orphan_cleanup_done = True

    async def run_claim(self, claim: ExecutionAttemptClaimV1) -> None:
        if self.worker_id is None or not self._orphan_cleanup_done:
            raise RuntimeError("pipeline_runtime_not_bound")
        _require_stage1_claim(claim, self.settings)
        self.cleanup_journal.record(claim, container_id=None)
        paths = PipelineAttemptPaths.create(self.base, claim.execution_attempt_id)
        cancellation = AttemptControlCancellation(claim, self.control_plane, paths)
        materializer = ClaimArtifactMaterializer(
            claim,
            CasArtifactInputMaterializer(
                read_client=ArtifactInputReadClient(self.control_plane),
                journal=self.journal,
                attempt_input_root=(self.base / "input-views").resolve(),
                mounter=LinuxReadOnlyViewMounter(),
            ),
            cancellation,
        )
        committer = HttpFinalOutputCommitter(claim, self.control_plane)
        cancellation.committer = committer
        backend = DockerPipelineBackend(
            claim=claim,
            control_plane=self.control_plane,
            paths=paths,
            cleanup_journal=self.cleanup_journal,
            cgroup_parent=_worker_cgroup_parent(self.settings),
            identity_labels=_runtime_identity_labels(self.settings),
        )
        cancellation.backend = backend
        phase: list[HeartbeatPhase] = ["input_materializing"]
        cancellation.phase_ref = phase
        committer.phase_ref = phase
        committer.cancellation = cancellation
        backend.phase_ref = phase
        heartbeat: asyncio.Task[None] | None = None
        terminal_reported = False
        final_output_committed = False
        try:
            spec = _container_spec(claim, paths)
            variant = claim.execution_spec_snapshot.execution_variant_id
            allocation = claim.slurm_gpu_allocation_evidence
            if allocation is None:
                raise RuntimeError("stage1_gpu_allocation_missing")
            plan = build_gpu_container_preflight_plan(
                profile=claim.resource_profile_snapshot,
                variant_id=variant,
                image_contract=claim.image_runtime_contract_snapshot,
                capability=claim.worker_capability_snapshot,
                allocation=allocation,
                requires_vla=True,
            )
            preview = _preview_lifecycle(claim, paths, self.control_plane)
            runner = PipelineContainerRunner(
                spec=spec,
                materializer=materializer,
                committer=committer,
                cancellation=cancellation,
                backend=backend,
                preflight=AttestedGpuExecutionPreflight(
                    plan, backend.observe_gpu_preflight
                ),
                cancellation_grace_seconds=claim.cancellation_grace_seconds,
                cancellation_poll_seconds=claim.cancellation_poll_seconds,
                gpu_cluster=allocation.slurm_cluster_id,
                gpu_lifecycle=PipelineGpuLifecycleTracker(),
                live_preview=preview,
            )
            heartbeat = asyncio.create_task(
                self._heartbeat(claim, phase, committer),
                name=f"pipeline-heartbeat-{claim.execution_attempt_id}",
            )
            phase[0] = "container_starting"
            execution = asyncio.create_task(
                runner.run(
                    PipelineExecutionRequest(
                        attempt_id=claim.execution_attempt_id,
                        bindings=tuple(
                            item.model_dump(mode="json") for item in claim.input_bindings
                        ),
                        stage_request=(
                            claim.stage_request.canonical_jcs_lf.encode("utf-8")
                            if claim.stage_request is not None
                            else None
                        ),
                    )
                ),
                name=f"pipeline-execution-{claim.execution_attempt_id}",
            )
            done, _pending = await asyncio.wait(
                {execution, heartbeat}, return_when=asyncio.FIRST_COMPLETED
            )
            if heartbeat in done:
                heartbeat_error = heartbeat.exception()
                if heartbeat_error is None:
                    raise RuntimeError("pipeline_heartbeat_stopped")
                execution.cancel()
                await asyncio.gather(execution, return_exceptions=True)
                raise RuntimeError("pipeline_heartbeat_failed") from heartbeat_error
            result = await execution
            phase[0] = "output_committing"
            session_id = result.commit.upload_session_id
            if session_id is None:
                raise RuntimeError("final_output_upload_session_missing")
            final_output_committed = True
            complete = ExecutionCompleteV1(
                exit_code=0,
                stage_result=StageResultV1.model_validate(
                    load_canonical_document(paths.outputs / "stage_result.json")
                ),
                stage_result_sha256=result.stage_result_digest,
                final_output_upload_session_id=session_id,
            )
            await self.control_plane.complete_execution_attempt(
                attempt_id=claim.execution_attempt_id,
                claim=_claim_headers(claim),
                request_id=_request_id(claim, "complete"),
                payload=complete.model_dump(mode="json"),
            )
            terminal_reported = True
        except PipelineCancelledError:
            if not cancellation.acknowledged:
                await cancellation.acknowledge(
                    attempt_id=claim.execution_attempt_id,
                    forced=False,
                    teardown_observed=True,
                )
            terminal_reported = True
        except Exception as exc:
            if final_output_committed:
                raise RuntimeError("pipeline_completion_report_failed") from exc
            if heartbeat is not None:
                heartbeat.cancel()
                await asyncio.gather(heartbeat, return_exceptions=True)
            proof = await _observed_cleanup_proof(
                claim=claim,
                paths=paths,
                backend=backend,
                committer=committer,
            )
            failure = _execution_failure(exc, resources=proof)
            await self.control_plane.fail_execution_attempt(
                attempt_id=claim.execution_attempt_id,
                claim=_claim_headers(claim),
                request_id=_request_id(claim, "failed"),
                payload=failure.model_dump(mode="json"),
            )
            terminal_reported = True
            return
        finally:
            if heartbeat is not None:
                heartbeat.cancel()
                await asyncio.gather(heartbeat, return_exceptions=True)
            paths.remove_writable()
            paths.remove_root_if_empty()
            if terminal_reported:
                self.cleanup_journal.clear(claim.execution_attempt_id)

    async def _heartbeat(
        self,
        claim: ExecutionAttemptClaimV1,
        phase: list[HeartbeatPhase],
        committer: HttpFinalOutputCommitter,
    ) -> None:
        started = time.monotonic()
        ordinal = 0
        while True:
            payload = ExecutionHeartbeatV1(
                schema_version="loom.execution-heartbeat.v1",
                phase=phase[0],
                monotonic_runtime_seconds=max(0, int(time.monotonic() - started)),
                active_upload_session_ids=(
                    [committer.active_session_id]
                    if committer.active_session_id is not None
                    else []
                ),
            )
            await self.control_plane.heartbeat_execution_attempt(
                attempt_id=claim.execution_attempt_id,
                claim=_claim_headers(claim),
                request_id=_request_id(claim, f"heartbeat:{ordinal}"),
                payload=payload.model_dump(mode="json"),
            )
            ordinal += 1
            await asyncio.sleep(_HEARTBEAT_SECONDS)


def _container_spec(claim: ExecutionAttemptClaimV1, paths: PipelineAttemptPaths) -> Any:
    profile = claim.resource_profile_snapshot
    variant = next(
        item
        for item in profile.execution_variants
        if item.variant_id == claim.execution_spec_snapshot.execution_variant_id
    )
    return build_pipeline_container_spec(
        image=claim.image,
        argv=claim.argv,
        workdir=claim.workdir,
        uid=_WORKER_UID,
        gid=_WORKER_GID,
        input_dir=paths.inputs,
        outputs_dir=paths.outputs,
        scratch_dir=paths.scratch,
        network_profile=claim.network_profile,
        cpus=profile.cpu_cores,
        memory_bytes=variant.container_memory_bytes_override or profile.memory_bytes,
        pids=4096,
        scratch_bytes=profile.scratch_bytes,
        gpu_device_uuids=(
            claim.slurm_gpu_allocation_evidence.device_uuids
            if claim.slurm_gpu_allocation_evidence is not None
            else ()
        ),
        resume_checkpoint_artifact_id=(
            claim.resume_checkpoint.artifact_id if claim.resume_checkpoint is not None else None
        ),
    )


def _preview_lifecycle(
    claim: ExecutionAttemptClaimV1,
    paths: PipelineAttemptPaths,
    control_plane: HttpControlPlaneClient,
) -> PipelineLivePreviewPublisher | None:
    if not is_stage1_live_preview_eligible(claim.execution_spec_snapshot):
        return None
    if claim.stage_request is None:
        return None
    request = json.loads(claim.stage_request.canonical_jcs_lf)
    timeout = int(request["budget"]["timeout_seconds"])
    fps = int(request["parameters"]["recording_fps"])
    return PipelineLivePreviewPublisher(
        preview_root=paths.scratch / "live-preview",
        attempt_id=claim.execution_attempt_id,
        claim=_claim_headers(claim),
        control_plane=cast(Any, control_plane),
        episode_bound=timeout * fps,
        owner_uid=_WORKER_UID,
    )


async def _observed_cleanup_proof(
    *,
    claim: ExecutionAttemptClaimV1,
    paths: PipelineAttemptPaths,
    backend: DockerPipelineBackend,
    committer: HttpFinalOutputCommitter,
) -> WorkerCleanupProofV1:
    """Return proof only after every attempt-local resource is observed absent."""

    paths.remove_writable()
    process_group_present = await backend.expected_process_group_present(
        attempt_id=claim.execution_attempt_id
    )
    if (
        claim.network_profile != "none"
        or not backend.container_absent
        or process_group_present
        or paths.inputs.exists()
        or paths.outputs.exists()
        or paths.scratch.exists()
        or committer.active_session_id is not None
    ):
        raise RuntimeError("pipeline_cleanup_proof_incomplete")
    return WorkerCleanupProofV1(
        container_absent=True,
        cgroup_empty=True,
        network_absent=True,
        step_jwt_revoked=True,
        runtime_secret_mount_absent=True,
        scratch_absent=True,
        outputs_absent=True,
        input_views_absent=True,
        active_upload_session_ids=[],
    )


def _execution_failure(
    exc: BaseException,
    *,
    resources: WorkerCleanupProofV1,
) -> ExecutionFailedV1:
    exit_code = exc.exit_code if isinstance(exc, PipelineProcessFailedError) else 70
    retry_class = (
        RetryClass.INFRASTRUCTURE_TRANSIENT
        if isinstance(exc, (TimeoutError, ConnectionError))
        else RetryClass.INTERNAL_DEFECT
    )
    reason = "container_exit_nonzero" if isinstance(exc, PipelineProcessFailedError) else "worker_execution_failed"
    return ExecutionFailedV1(
        exit_code=exit_code,
        retry_class=retry_class,
        reason_code=reason,
        redacted_message=redact_text(str(exc))[:4096] or reason,
        stage_result=None,
        stage_result_sha256=None,
        teardown_observed=True,
        resources=resources,
    )


def _worker_cgroup_parent(settings: WorkerSettings) -> str | None:
    value = str(getattr(settings, "cgroup_parent", "")).strip()
    return value or None


def _runtime_identity_labels(settings: WorkerSettings) -> tuple[tuple[str, str], ...]:
    values = (
        ("loom.sandbox", getattr(settings, "sandbox_identity", "")),
        ("loom.candidate_sha", getattr(settings, "candidate_sha", "")),
        ("loom.slurm_job_id", getattr(settings, "slurm_job_id", "")),
        ("loom.compose_project", getattr(settings, "compose_project", "")),
    )
    return tuple((key, value) for key, value in values if value)


__all__ = [
    "PipelineWorkerRuntime",
    "production_pipeline_enabled",
]
