"""Closed container specification and deterministic Pipeline runner seams.

This module intentionally does not expose a generic Docker option bag.  A
Pipeline container is constructed from the immutable claim fields and the four
worker-owned directories only; unsupported runtime features are therefore not
representable at this boundary.
"""

from __future__ import annotations

import asyncio
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path, PurePosixPath
from types import MappingProxyType
from typing import Literal, Protocol, runtime_checkable
from uuid import UUID

from loom_worker.pipeline_gpu_lifecycle import (
    PipelineGpuCluster,
    PipelineGpuLifecycleTracker,
)

IMAGE_DIGEST_RE = re.compile(
    r"^[a-z0-9]+(?:[._-][a-z0-9]+)*(?::[0-9]+)?"
    r"(?:/[a-z0-9]+(?:[._-][a-z0-9]+)*)+@sha256:[0-9a-f]{64}$"
)
SHELL_EXECUTABLES = frozenset({"sh", "bash", "dash", "ash", "zsh", "ksh", "csh", "tcsh", "fish"})
SECRET_ARG_RE = re.compile(
    r"(?:\bBearer\s+[A-Za-z0-9._~+/=-]{8,}|"
    r"(?:api[_-]?key|password|passwd|secret|token|credential)\s*=|"
    r"\b(?:sk|rk|ghp|gho|github_pat)-?[A-Za-z0-9_-]{16,})",
    re.IGNORECASE,
)
GATEWAY_NETWORK_NAME = "loom-pipeline-gateway"
INPUTS_TARGET = PurePosixPath("/inputs")
OUTPUTS_TARGET = PurePosixPath("/outputs")
SCRATCH_TARGET = PurePosixPath("/scratch")
RUNTIME_SECRET_TARGET = PurePosixPath("/run/loom")


class PipelineContainerContractError(ValueError):
    """An immutable claim cannot be represented by the v1 sandbox contract."""


class PipelineExecutionError(RuntimeError):
    """The deterministic Pipeline execution sequence failed."""


class PipelineCancelledError(PipelineExecutionError):
    """The Attempt observed a sticky cancellation before commit."""


class PipelineProcessFailedError(PipelineExecutionError):
    """The container exited non-zero and therefore cannot commit output."""

    def __init__(self, exit_code: int) -> None:
        super().__init__(f"Pipeline container exited with status {exit_code}")
        self.exit_code = exit_code


@dataclass(frozen=True)
class MountSpec:
    """One worker-derived bind mount; arbitrary claim mounts are impossible."""

    source: Path
    target: PurePosixPath
    read_only: bool
    recursive_read_only: bool = False
    nosuid: bool = False
    nodev: bool = False
    noexec: bool = False
    container_mode: int | None = None
    quota_group: str | None = None
    quota_bytes: int | None = None

    def __post_init__(self) -> None:
        source = _closed_host_path(self.source, "mount source")
        object.__setattr__(self, "source", source)
        if not self.target.is_absolute() or ".." in self.target.parts:
            raise PipelineContainerContractError("mount target must be an absolute closed path")
        if self.container_mode is not None and self.container_mode not in {0o500}:
            raise PipelineContainerContractError("unsupported container mount mode")
        if (self.quota_group is None) != (self.quota_bytes is None):
            raise PipelineContainerContractError("mount quota group and byte limit must pair")
        if self.quota_bytes is not None and self.quota_bytes <= 0:
            raise PipelineContainerContractError("mount quota byte limit must be positive")


@dataclass(frozen=True)
class ContainerLimits:
    cpus: float
    memory_bytes: int
    pids: int
    scratch_bytes: int

    def __post_init__(self) -> None:
        if isinstance(self.cpus, bool) or not isinstance(self.cpus, int | float) or self.cpus <= 0:
            raise PipelineContainerContractError("cpus must be positive")
        for label, value in (
            ("memory_bytes", self.memory_bytes),
            ("pids", self.pids),
            ("scratch_bytes", self.scratch_bytes),
        ):
            if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
                raise PipelineContainerContractError(f"{label} must be a positive integer")


@dataclass(frozen=True)
class PipelineContainerSpec:
    """Complete v1 runtime policy for one approved ExecutionAttempt."""

    image: str
    argv: tuple[str, ...]
    workdir: PurePosixPath
    uid: int
    gid: int
    network_profile: Literal["none", "gateway"]
    network_mode: str
    mounts: tuple[MountSpec, ...]
    limits: ContainerLimits
    cap_drop: tuple[str, ...] = ("ALL",)
    security_opt: tuple[str, ...] = ("no-new-privileges:true",)
    read_only_rootfs: bool = True
    seccomp_profile: Literal["default"] = "default"
    privileged: Literal[False] = False
    host_pid: Literal[False] = False
    host_ipc: Literal[False] = False
    devices: tuple[str, ...] = ()
    gpu_device_uuids: tuple[str, ...] = ()
    environment: tuple[tuple[str, str], ...] = ()
    init: Literal[True] = True

    def __post_init__(self) -> None:
        _validate_image(self.image)
        _validate_argv(self.argv)
        _validate_posix_workdir(self.workdir)
        _non_root_id(self.uid, "uid")
        _non_root_id(self.gid, "gid")
        if self.cap_drop != ("ALL",):
            raise PipelineContainerContractError("Pipeline containers must drop every capability")
        names = [name for name, _value in self.environment]
        if names != sorted(names) or len(names) != len(set(names)):
            raise PipelineContainerContractError("Pipeline environment must be sorted and unique")
        if any(
            name not in {"LOOM_RESUME_CHECKPOINT", "LOOM_RESUME_CHECKPOINT_ARTIFACT_ID"}
            for name in names
        ):
            raise PipelineContainerContractError("unsupported Pipeline environment variable")
        if self.environment and set(names) != {
            "LOOM_RESUME_CHECKPOINT",
            "LOOM_RESUME_CHECKPOINT_ARTIFACT_ID",
        }:
            raise PipelineContainerContractError(
                "resume environment must be the exact two-variable set"
            )
        if self.security_opt != ("no-new-privileges:true",):
            raise PipelineContainerContractError("no-new-privileges is mandatory")
        if not self.read_only_rootfs or self.seccomp_profile != "default":
            raise PipelineContainerContractError(
                "read-only rootfs and default seccomp are mandatory"
            )
        if self.privileged or self.host_pid or self.host_ipc or self.devices:
            raise PipelineContainerContractError("privileged host/device access is forbidden")
        if self.gpu_device_uuids:
            if self.gpu_device_uuids != tuple(sorted(self.gpu_device_uuids, key=str.encode)):
                raise PipelineContainerContractError("GPU UUIDs must be bytewise sorted")
            if len(self.gpu_device_uuids) != len(set(self.gpu_device_uuids)) or any(
                re.fullmatch(r"GPU-[A-Za-z0-9][A-Za-z0-9_-]{0,122}", value) is None
                for value in self.gpu_device_uuids
            ):
                raise PipelineContainerContractError("GPU UUID set is invalid")
        expected_targets = [INPUTS_TARGET, OUTPUTS_TARGET, SCRATCH_TARGET]
        if self.network_profile == "gateway":
            expected_targets.append(RUNTIME_SECRET_TARGET)
            if self.network_mode != GATEWAY_NETWORK_NAME:
                raise PipelineContainerContractError(
                    "gateway work must use the fixed gateway network"
                )
        elif self.network_profile == "none":
            if self.network_mode != "none":
                raise PipelineContainerContractError("none work must use Docker network none")
        else:
            raise PipelineContainerContractError("network profile must be none or gateway")
        if [mount.target for mount in self.mounts] != expected_targets:
            raise PipelineContainerContractError("Pipeline mount set or order is not exact")
        if not self.mounts[0].read_only:
            raise PipelineContainerContractError("/inputs must be read-only")
        if self.mounts[1].read_only or self.mounts[2].read_only:
            raise PipelineContainerContractError("/outputs and /scratch must be writable")
        for writable in self.mounts[1:3]:
            if (
                writable.quota_group != "attempt-scratch"
                or writable.quota_bytes != self.limits.scratch_bytes
            ):
                raise PipelineContainerContractError(
                    "/outputs and /scratch must share the exact Attempt quota"
                )
        if self.network_profile == "gateway":
            secret = self.mounts[3]
            if not (
                secret.read_only
                and secret.recursive_read_only
                and secret.nosuid
                and secret.nodev
                and secret.noexec
                and secret.container_mode == 0o500
            ):
                raise PipelineContainerContractError("/run/loom secret mount policy is incomplete")

    def docker_create_kwargs(self) -> Mapping[str, object]:
        """Return a closed docker-py create argument projection.

        Storage quota enforcement is worker/filesystem-owned and represented by
        ``limits.scratch_bytes``; it intentionally is not translated into a
        generic Docker storage option here.
        """

        volumes = {
            str(mount.source): {
                "bind": str(mount.target),
                "mode": "ro" if mount.read_only else "rw",
            }
            for mount in self.mounts
        }
        create_kwargs: dict[str, object] = {
            "image": self.image,
            "command": list(self.argv),
            "working_dir": str(self.workdir),
            "user": f"{self.uid}:{self.gid}",
            "network_mode": self.network_mode,
            "read_only": True,
            "cap_drop": ["ALL"],
            "security_opt": ["no-new-privileges:true"],
            "privileged": False,
            "pid_mode": None,
            "ipc_mode": None,
            "devices": [],
            "pids_limit": self.limits.pids,
            "mem_limit": self.limits.memory_bytes,
            "nano_cpus": int(float(self.limits.cpus) * 1_000_000_000),
            "volumes": volumes,
            "init": True,
            "environment": dict(self.environment),
        }
        if self.gpu_device_uuids:
            create_kwargs["device_requests"] = [
                {
                    "device_ids": list(self.gpu_device_uuids),
                    "capabilities": [["gpu"]],
                }
            ]
        return MappingProxyType(create_kwargs)


def build_pipeline_container_spec(
    *,
    image: str,
    argv: Sequence[str],
    workdir: str | PurePosixPath,
    uid: int,
    gid: int,
    input_dir: Path,
    outputs_dir: Path,
    scratch_dir: Path,
    network_profile: str,
    cpus: float,
    memory_bytes: int,
    pids: int,
    scratch_bytes: int,
    runtime_secret_dir: Path | None = None,
    gpu_device_uuids: Sequence[str] = (),
    resume_checkpoint_artifact_id: UUID | None = None,
) -> PipelineContainerSpec:
    """Build the sole supported container policy from immutable fields."""

    _validate_image(image)
    closed_argv = _validate_argv(argv)
    closed_workdir = PurePosixPath(workdir)
    _validate_posix_workdir(closed_workdir)
    _non_root_id(uid, "uid")
    _non_root_id(gid, "gid")
    if network_profile not in {"none", "gateway"}:
        raise PipelineContainerContractError("network profile must be none or gateway")
    if network_profile == "none" and runtime_secret_dir is not None:
        raise PipelineContainerContractError("network=none must not have /run/loom")
    if network_profile == "gateway" and runtime_secret_dir is None:
        raise PipelineContainerContractError("network=gateway requires its Attempt secret mount")

    mounts = [
        MountSpec(source=input_dir, target=INPUTS_TARGET, read_only=True),
        MountSpec(
            source=outputs_dir,
            target=OUTPUTS_TARGET,
            read_only=False,
            quota_group="attempt-scratch",
            quota_bytes=scratch_bytes,
        ),
        MountSpec(
            source=scratch_dir,
            target=SCRATCH_TARGET,
            read_only=False,
            quota_group="attempt-scratch",
            quota_bytes=scratch_bytes,
        ),
    ]
    if runtime_secret_dir is not None:
        mounts.append(
            MountSpec(
                source=runtime_secret_dir,
                target=RUNTIME_SECRET_TARGET,
                read_only=True,
                recursive_read_only=True,
                nosuid=True,
                nodev=True,
                noexec=True,
                container_mode=0o500,
            )
        )
    return PipelineContainerSpec(
        image=image,
        argv=closed_argv,
        workdir=closed_workdir,
        uid=uid,
        gid=gid,
        network_profile=network_profile,  # type: ignore[arg-type]
        network_mode="none" if network_profile == "none" else GATEWAY_NETWORK_NAME,
        mounts=tuple(mounts),
        limits=ContainerLimits(
            cpus=cpus,
            memory_bytes=memory_bytes,
            pids=pids,
            scratch_bytes=scratch_bytes,
        ),
        gpu_device_uuids=tuple(gpu_device_uuids),
        environment=(
            (
                ("LOOM_RESUME_CHECKPOINT", "/inputs/loom_checkpoint"),
                ("LOOM_RESUME_CHECKPOINT_ARTIFACT_ID", str(resume_checkpoint_artifact_id)),
            )
            if resume_checkpoint_artifact_id is not None
            else ()
        ),
    )


@dataclass(frozen=True)
class MaterializedInputView:
    root: Path
    input_view_digest: str
    stage_request_path: Path | None = None
    control_binding_path: Path | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "root", _closed_host_path(self.root, "input view"))
        _digest(self.input_view_digest, "input_view_digest")
        if self.stage_request_path is not None:
            stage_request = _closed_host_path(self.stage_request_path, "stage request")
            if stage_request != self.root / "stage-request.json":
                raise PipelineContainerContractError(
                    "stage request must be the reserved /inputs/stage-request.json file"
                )
            object.__setattr__(self, "stage_request_path", stage_request)
        if self.control_binding_path is not None:
            control_binding = _closed_host_path(
                self.control_binding_path, "control binding snapshot"
            )
            if control_binding != self.root / "control-binding.json":
                raise PipelineContainerContractError(
                    "control binding must be the reserved /inputs/control-binding.json file"
                )
            object.__setattr__(self, "control_binding_path", control_binding)


@dataclass(frozen=True)
class ContainerProcessResult:
    exit_code: int
    stage_result: Mapping[str, object] | None
    stage_result_digest: str | None

    def __post_init__(self) -> None:
        if isinstance(self.exit_code, bool) or not isinstance(self.exit_code, int):
            raise PipelineContainerContractError("exit_code must be an integer")
        if (self.stage_result is None) != (self.stage_result_digest is None):
            raise PipelineContainerContractError("stage result and digest must be present together")
        if self.stage_result_digest is not None:
            _digest(self.stage_result_digest, "stage_result_digest")


@dataclass(frozen=True)
class ArtifactCommitResult:
    manifest_digest: str
    artifact_ids: tuple[UUID, ...] = ()
    upload_session_id: UUID | None = None

    def __post_init__(self) -> None:
        _digest(self.manifest_digest, "manifest_digest")


@dataclass(frozen=True)
class PipelineRunResult:
    attempt_id: UUID
    input_view_digest: str
    stage_result_digest: str
    commit: ArtifactCommitResult


@runtime_checkable
class ArtifactInputMaterializer(Protocol):
    """Strict seam implemented in production by #1240."""

    async def materialize(
        self,
        *,
        attempt_id: UUID,
        bindings: Sequence[Mapping[str, object]],
        destination: Path,
        stage_request: bytes | None,
    ) -> MaterializedInputView: ...

    async def release(
        self,
        *,
        attempt_id: UUID,
        input_view: MaterializedInputView,
    ) -> None: ...


@runtime_checkable
class ArtifactCommitter(Protocol):
    """Strict seam implemented in production by #1214."""

    async def commit(
        self,
        *,
        attempt_id: UUID,
        outputs_dir: Path,
        stage_result: Mapping[str, object],
        stage_result_digest: str,
    ) -> ArtifactCommitResult: ...


@runtime_checkable
class ExecutionCancellation(Protocol):
    """Sticky cancellation observation/ack seam implemented by #1215."""

    async def requested(self, *, attempt_id: UUID) -> bool: ...

    async def acknowledge(
        self,
        *,
        attempt_id: UUID,
        forced: bool,
        teardown_observed: bool,
    ) -> None: ...


@runtime_checkable
class PipelineContainerBackend(Protocol):
    """Small runtime adapter shared by Docker-backed production and fakes."""

    async def run(
        self,
        *,
        attempt_id: UUID,
        spec: PipelineContainerSpec,
        input_view: MaterializedInputView,
    ) -> ContainerProcessResult: ...

    async def terminate(self, *, attempt_id: UUID, grace_seconds: int) -> bool: ...

    async def expected_process_group_present(self, *, attempt_id: UUID) -> bool: ...

    async def teardown(self, *, attempt_id: UUID) -> None: ...


@runtime_checkable
class PipelineExecutionPreflight(Protocol):
    """Attest the exact container/image/device mapping before stage argv."""

    async def attest(
        self,
        *,
        attempt_id: UUID,
        spec: PipelineContainerSpec,
        input_view: MaterializedInputView,
    ) -> None: ...


@runtime_checkable
class PipelineLivePreviewLifecycle(Protocol):
    """Explicit Stage 1-only seam assembled from the server-owned claim."""

    async def run_until(self, stop: asyncio.Event) -> None: ...

    def stop(self, *, reason: str = "preview_lifecycle_ended") -> object: ...


@dataclass(frozen=True)
class PipelineExecutionRequest:
    attempt_id: UUID
    bindings: tuple[Mapping[str, object], ...]
    stage_request: bytes | None = None


@dataclass
class PipelineContainerRunner:
    """Deterministic materialize → run → commit sequence over strict seams."""

    spec: PipelineContainerSpec
    materializer: ArtifactInputMaterializer
    committer: ArtifactCommitter
    cancellation: ExecutionCancellation
    backend: PipelineContainerBackend
    preflight: PipelineExecutionPreflight | None = None
    cancellation_grace_seconds: int = 30
    cancellation_poll_seconds: int = 5
    gpu_cluster: PipelineGpuCluster | None = None
    gpu_lifecycle: PipelineGpuLifecycleTracker | None = None
    live_preview: PipelineLivePreviewLifecycle | None = None

    async def run(self, request: PipelineExecutionRequest) -> PipelineRunResult:
        if self.cancellation_grace_seconds != 30 or self.cancellation_poll_seconds != 5:
            raise PipelineContainerContractError("Pipeline cancellation cadence is fixed at 5s/30s")
        input_view: MaterializedInputView | None = None
        process_result: ContainerProcessResult | None = None
        backend_started = False
        backend_torn_down = False
        input_released = False
        preview_stop = asyncio.Event()
        preview_task: asyncio.Task[None] | None = None
        try:
            await self._raise_if_cancelled(request.attempt_id)
            input_view = await self.materializer.materialize(
                attempt_id=request.attempt_id,
                bindings=request.bindings,
                destination=self._mount_source(INPUTS_TARGET),
                stage_request=request.stage_request,
            )
            if input_view.root != self._mount_source(INPUTS_TARGET):
                raise PipelineExecutionError("materializer returned a different input root")
            if await self.cancellation.requested(attempt_id=request.attempt_id):
                await self.materializer.release(
                    attempt_id=request.attempt_id,
                    input_view=input_view,
                )
                input_released = True
                await self.cancellation.acknowledge(
                    attempt_id=request.attempt_id,
                    forced=False,
                    teardown_observed=True,
                )
                raise PipelineCancelledError(f"ExecutionAttempt {request.attempt_id} was cancelled")
            if self.preflight is not None:
                await self.preflight.attest(
                    attempt_id=request.attempt_id,
                    spec=self.spec,
                    input_view=input_view,
                )
            if self.spec.gpu_device_uuids and self.gpu_lifecycle is not None:
                if self.gpu_cluster is None:
                    raise PipelineContainerContractError(
                        "GPU lifecycle tracking requires a closed slurm cluster"
                    )
                self.gpu_lifecycle.mark(
                    request.attempt_id,
                    cluster=self.gpu_cluster,
                    reason="pre_start",
                )
            backend_started = True
            if self.live_preview is not None:
                preview_task = asyncio.create_task(
                    self.live_preview.run_until(preview_stop),
                    name=f"pipeline-live-preview-{request.attempt_id}",
                )
            process_result, forced = await self._run_with_cancellation(
                attempt_id=request.attempt_id, input_view=input_view
            )
            if process_result is None:
                if self.spec.gpu_device_uuids and self.gpu_lifecycle is not None:
                    assert self.gpu_cluster is not None
                    self.gpu_lifecycle.mark(
                        request.attempt_id,
                        cluster=self.gpu_cluster,
                        reason="cleanup_pending",
                    )
                try:
                    await self.backend.teardown(attempt_id=request.attempt_id)
                finally:
                    if self.gpu_lifecycle is not None:
                        self.gpu_lifecycle.clear(request.attempt_id)
                backend_started = False
                backend_torn_down = True
                await self.materializer.release(
                    attempt_id=request.attempt_id,
                    input_view=input_view,
                )
                input_released = True
                await self.cancellation.acknowledge(
                    attempt_id=request.attempt_id,
                    forced=forced,
                    teardown_observed=True,
                )
                raise PipelineCancelledError(f"ExecutionAttempt {request.attempt_id} was cancelled")
            if process_result.exit_code != 0:
                raise PipelineProcessFailedError(process_result.exit_code)
            if process_result.stage_result is None or process_result.stage_result_digest is None:
                raise PipelineExecutionError("rc=0 requires a canonical StageResult and digest")
            commit = await self.committer.commit(
                attempt_id=request.attempt_id,
                outputs_dir=self._mount_source(OUTPUTS_TARGET),
                stage_result=process_result.stage_result,
                stage_result_digest=process_result.stage_result_digest,
            )
            return PipelineRunResult(
                attempt_id=request.attempt_id,
                input_view_digest=input_view.input_view_digest,
                stage_result_digest=process_result.stage_result_digest,
                commit=commit,
            )
        finally:
            preview_stop.set()
            if preview_task is not None:
                preview_task.cancel()
                await asyncio.gather(preview_task, return_exceptions=True)
            if self.live_preview is not None:
                self.live_preview.stop()
            if backend_started and not backend_torn_down:
                if self.spec.gpu_device_uuids and self.gpu_lifecycle is not None:
                    assert self.gpu_cluster is not None
                    self.gpu_lifecycle.mark(
                        request.attempt_id,
                        cluster=self.gpu_cluster,
                        reason="cleanup_pending",
                    )
                try:
                    await self.backend.teardown(attempt_id=request.attempt_id)
                finally:
                    if self.gpu_lifecycle is not None:
                        self.gpu_lifecycle.clear(request.attempt_id)
            if input_view is not None and not input_released:
                await self.materializer.release(
                    attempt_id=request.attempt_id,
                    input_view=input_view,
                )

    def _mount_source(self, target: PurePosixPath) -> Path:
        return next(mount.source for mount in self.spec.mounts if mount.target == target)

    async def _raise_if_cancelled(self, attempt_id: UUID) -> None:
        if not await self.cancellation.requested(attempt_id=attempt_id):
            return
        await self.cancellation.acknowledge(
            attempt_id=attempt_id,
            forced=False,
            teardown_observed=True,
        )
        raise PipelineCancelledError(f"ExecutionAttempt {attempt_id} was cancelled")

    async def _run_with_cancellation(
        self,
        *,
        attempt_id: UUID,
        input_view: MaterializedInputView,
    ) -> tuple[ContainerProcessResult | None, bool]:
        process = asyncio.create_task(
            self.backend.run(attempt_id=attempt_id, spec=self.spec, input_view=input_view)
        )
        try:
            while True:
                if self.spec.gpu_device_uuids and self.gpu_lifecycle is not None:
                    assert self.gpu_cluster is not None
                    if await self.backend.expected_process_group_present(attempt_id=attempt_id):
                        self.gpu_lifecycle.process_present(attempt_id)
                    else:
                        self.gpu_lifecycle.mark(
                            attempt_id,
                            cluster=self.gpu_cluster,
                            reason="process_absent",
                        )
                done, _pending = await asyncio.wait(
                    {process}, timeout=self.cancellation_poll_seconds
                )
                if done:
                    result = process.result()
                    if await self.cancellation.requested(attempt_id=attempt_id):
                        forced = await self.backend.terminate(
                            attempt_id=attempt_id,
                            grace_seconds=self.cancellation_grace_seconds,
                        )
                        return None, forced
                    return result, False
                if await self.cancellation.requested(attempt_id=attempt_id):
                    forced = await self.backend.terminate(
                        attempt_id=attempt_id,
                        grace_seconds=self.cancellation_grace_seconds,
                    )
                    process.cancel()
                    await asyncio.gather(process, return_exceptions=True)
                    return None, forced
        finally:
            if not process.done():
                process.cancel()
                await asyncio.gather(process, return_exceptions=True)


# Deterministic fakes used by focused #8 tests.  They deliberately implement
# only the strict Protocol methods so later production I/O cannot leak into the
# worker runner acceptance boundary.


@dataclass
class FakeArtifactInputMaterializer:
    result: MaterializedInputView
    calls: list[tuple[UUID, Path]] = field(default_factory=list)
    releases: list[tuple[UUID, Path]] = field(default_factory=list)

    async def materialize(
        self,
        *,
        attempt_id: UUID,
        bindings: Sequence[Mapping[str, object]],
        destination: Path,
        stage_request: bytes | None,
    ) -> MaterializedInputView:
        del bindings, stage_request
        self.calls.append((attempt_id, destination))
        return self.result

    async def release(
        self,
        *,
        attempt_id: UUID,
        input_view: MaterializedInputView,
    ) -> None:
        self.releases.append((attempt_id, input_view.root))


@dataclass
class FakeArtifactCommitter:
    result: ArtifactCommitResult
    calls: list[tuple[UUID, Path, str]] = field(default_factory=list)

    async def commit(
        self,
        *,
        attempt_id: UUID,
        outputs_dir: Path,
        stage_result: Mapping[str, object],
        stage_result_digest: str,
    ) -> ArtifactCommitResult:
        del stage_result
        self.calls.append((attempt_id, outputs_dir, stage_result_digest))
        return self.result


@dataclass
class FakeExecutionCancellation:
    observations: list[bool] = field(default_factory=lambda: [False])
    acknowledged: list[tuple[UUID, bool, bool]] = field(default_factory=list)

    async def requested(self, *, attempt_id: UUID) -> bool:
        del attempt_id
        if len(self.observations) > 1:
            return self.observations.pop(0)
        return self.observations[0]

    async def acknowledge(
        self,
        *,
        attempt_id: UUID,
        forced: bool,
        teardown_observed: bool,
    ) -> None:
        self.acknowledged.append((attempt_id, forced, teardown_observed))


@dataclass
class FakePipelineContainerBackend:
    result: ContainerProcessResult
    calls: list[str] = field(default_factory=list)
    terminate_forced: bool = False

    async def run(
        self,
        *,
        attempt_id: UUID,
        spec: PipelineContainerSpec,
        input_view: MaterializedInputView,
    ) -> ContainerProcessResult:
        del attempt_id, spec, input_view
        self.calls.append("run")
        return self.result

    async def terminate(self, *, attempt_id: UUID, grace_seconds: int) -> bool:
        del attempt_id, grace_seconds
        self.calls.append("terminate")
        return self.terminate_forced

    async def expected_process_group_present(self, *, attempt_id: UUID) -> bool:
        del attempt_id
        self.calls.append("probe")
        return True

    async def teardown(self, *, attempt_id: UUID) -> None:
        del attempt_id
        self.calls.append("teardown")


def _validate_image(image: str) -> str:
    if not isinstance(image, str) or IMAGE_DIGEST_RE.fullmatch(image) is None:
        raise PipelineContainerContractError(
            "image must be an allowlisted repository@sha256 lowercase digest"
        )
    return image


def _validate_argv(argv: Sequence[str]) -> tuple[str, ...]:
    if isinstance(argv, str | bytes) or not isinstance(argv, Sequence):
        raise PipelineContainerContractError("argv must be a non-empty string array")
    values = tuple(argv)
    if not values or len(values) > 256:
        raise PipelineContainerContractError("argv must contain 1..256 items")
    total = 0
    for item in values:
        if not isinstance(item, str) or not item or "\x00" in item:
            raise PipelineContainerContractError("argv entries must be non-empty NUL-free strings")
        if SECRET_ARG_RE.search(item):
            raise PipelineContainerContractError("argv must not contain a raw secret")
        total += len(item.encode("utf-8"))
    if total > 65_536:
        raise PipelineContainerContractError("argv exceeds the v1 byte limit")
    executable = PurePosixPath(values[0]).name.lower()
    if executable in SHELL_EXECUTABLES:
        raise PipelineContainerContractError("shell entrypoints are forbidden")
    return values


def _validate_posix_workdir(path: PurePosixPath) -> None:
    if not path.is_absolute() or path == PurePosixPath("/") or ".." in path.parts:
        raise PipelineContainerContractError("workdir must be a non-root absolute container path")


def _closed_host_path(path: Path, label: str) -> Path:
    if not isinstance(path, Path) or not path.is_absolute():
        raise PipelineContainerContractError(f"{label} must be an absolute host path")
    if ".." in path.parts:
        raise PipelineContainerContractError(f"{label} contains traversal")
    if path.name in {"docker.sock", "containerd.sock"}:
        raise PipelineContainerContractError(f"{label} cannot expose a runtime socket")
    try:
        if path.is_symlink():
            raise PipelineContainerContractError(f"{label} cannot be a symlink")
    except OSError as exc:
        raise PipelineContainerContractError(f"{label} cannot be inspected") from exc
    return path


def _non_root_id(value: int, label: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or not 1 <= value <= 2_147_483_647:
        raise PipelineContainerContractError(f"{label} must identify a non-root user")
    return value


def _digest(value: str, label: str) -> str:
    if not isinstance(value, str) or re.fullmatch(r"sha256:[0-9a-f]{64}", value) is None:
        raise PipelineContainerContractError(f"{label} must be a lowercase sha256 digest")
    return value
