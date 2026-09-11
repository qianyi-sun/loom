"""Fixed immutable-image/stdin worker composition, not native claim authority.

The caller verifies the CLI executable snapshot, empty operator Docker config,
physical allocation and cgroup before constructing these launch inputs. No
Compose, host environment, image entrypoint, restart or command suffix is used.
Native GPU/pipeline and worker-vLLM modes remain closed until their complete
device/process admission is available. Legacy workers are unaffected.
"""

from __future__ import annotations

import json
import os
import re
import selectors
import subprocess
import time
from dataclasses import dataclass, replace
from pathlib import PurePosixPath
from typing import Annotated, Any
from uuid import UUID

from pydantic import Field, field_validator, model_validator

from loom_capacity_executor.launch_renderer import NativeTaskImageExecutionV2
from loom_capacity_executor.native_worker_bootstrap import NativeWorkerBootstrap
from loom_capacity_manager.executable_contracts import StrictV2Model

_IMAGE_ID = re.compile(r"sha256:[0-9a-f]{64}", re.ASCII)
_CONTAINER_ID = re.compile(r"[0-9a-f]{64}", re.ASCII)
_ENV_NAME = re.compile(r"[A-Za-z_][A-Za-z0-9_]*", re.ASCII)
_SOCKET = "/var/run/docker.sock"
_FIXED_ENV = {"PATH": "/usr/local/bin:/usr/bin:/bin", "LANG": "C.UTF-8",
              "HOME": "/nonexistent", "DOCKER_HOST": "unix:///var/run/docker.sock",
              "PYTHONDONTWRITEBYTECODE": "1"}


class NativeContainerError(RuntimeError):
    """Sanitized native-launch failure; never include CLI output or settings."""


class NativeWorkerContainerPolicyV2(StrictV2Model):
    """Native-only configuration inside the existing digest-pinned trusted config."""

    native_execution: NativeTaskImageExecutionV2
    canonical_worker_settings: Annotated[str, Field(min_length=2, max_length=2048, repr=False)]
    docker_config_directory: Annotated[str, Field(min_length=1, max_length=4096)]
    pids_max: Annotated[int, Field(gt=0, le=1_048_576)]

    @field_validator("docker_config_directory")
    @classmethod
    def _config_directory(cls, value: str) -> str:
        path = PurePosixPath(value)
        if not path.is_absolute() or str(path) != value or path == PurePosixPath("/") or ".." in path.parts or "\0" in value:
            raise ValueError("native Docker config directory is invalid")
        return value

    @model_validator(mode="after")
    def _settings(self) -> NativeWorkerContainerPolicyV2:
        NativeWorkerBootstrap(native_execution=self.native_execution, worker_credential="x" * 43,
            canonical_worker_settings=self.canonical_worker_settings)
        return self


@dataclass(frozen=True, slots=True)
class PreparedNativeImage:
    image_id: str
    platform: str
    inherited_environment: tuple[str, ...]


def prepare_native_image(value: object, *, image_digest: str, platform: str) -> PreparedNativeImage:
    """Check Docker image readback, not labels or a caller's digest assertion."""

    try:
        if not isinstance(value, dict) or platform not in {"linux/amd64", "linux/arm64"}:
            raise ValueError
        image_id = value["Id"]
        config = value["Config"]
        if (
            type(image_id) is not str or _IMAGE_ID.fullmatch(image_id) is None
            or image_digest not in value["RepoDigests"]
            or f"{value['Os']}/{value['Architecture']}" != platform
            or not isinstance(config, dict) or config.get("Volumes") or config.get("OnBuild")
        ):
            raise ValueError
        inherited = config.get("Env") or []
        if not isinstance(inherited, list) or len(inherited) > 256:
            raise ValueError
        names = []
        for item in inherited:
            if type(item) is not str or "=" not in item or len(item) > 8192:
                raise ValueError
            name = item.split("=", 1)[0]
            if _ENV_NAME.fullmatch(name) is None or name in names:
                raise ValueError
            names.append(name)
        return PreparedNativeImage(image_id=image_id, platform=platform,
            inherited_environment=tuple(sorted(names)))
    except (KeyError, TypeError, ValueError):
        raise NativeContainerError("native worker image composition is unverified") from None


@dataclass(frozen=True, slots=True)
class NativeWorkerAllocation:
    intent_id: str
    job_id: str
    cgroup_parent: str
    cpu_millicores: int
    memory_bytes: int
    pids_max: int
    concurrency_slots: int
    scratch_directory: str
    docker_socket_gid: int
    runtime_uid: int
    runtime_gid: int
    pool_id: str
    hostname: str
    candidate_sha: str

    def __post_init__(self) -> None:
        try:
            path = PurePosixPath(self.scratch_directory)
            if (
                str(UUID(self.intent_id)) != self.intent_id
                or self.pool_id not in {"oldlab", "gb10"}
                or re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}", self.hostname) is None
                or re.fullmatch(r"[0-9a-f]{40}", self.candidate_sha) is None
                or re.fullmatch(r"[1-9][0-9]*", self.job_id) is None
                or not self.cgroup_parent or any(char in self.cgroup_parent for char in "\0\n\r,")
                or not path.is_relative_to("/var/lib/loom/native-workers")
                or path == PurePosixPath("/var/lib/loom/native-workers")
                or str(path) != self.scratch_directory or ".." in path.parts
                or any(char in self.scratch_directory for char in "\0\n\r,:")
                or any(type(value) is not int or not 0 < value < (1 << 63) for value in (
                    self.cpu_millicores, self.memory_bytes, self.pids_max, self.concurrency_slots))
                or any(type(value) is not int or not 0 < value < (1 << 31) for value in (
                    self.runtime_uid, self.runtime_gid, self.docker_socket_gid))
            ):
                raise ValueError
        except (TypeError, ValueError):
            raise NativeContainerError("native worker allocation is invalid") from None


def bind_native_settings(bootstrap: NativeWorkerBootstrap, allocation: NativeWorkerAllocation) -> NativeWorkerBootstrap:
    """Bind transport settings to actual allocation facts without ambient fallbacks."""

    settings = bootstrap.worker_settings()
    if any(settings.get(key, False) is not False for key in (
        "enable_worker_vllm", "sandbox_isolation", "pipeline_terminalgen_authoring_enabled",
    )):
        raise NativeContainerError("native worker runtime mode is not admitted")
    pinned = {
        "pool_name": allocation.pool_id,
        "hostname": allocation.hostname, "candidate_sha": allocation.candidate_sha,
        "compose_project": "loom-native-" + allocation.intent_id,
        "cgroup_parent": allocation.cgroup_parent, "require_cgroup_parent": True,
        "slurm_job_id": allocation.job_id, "slurm_allocated_gpus": 0, "slurm_gpu_device_ids": "",
        "docker_socket": _SOCKET, "max_concurrent": allocation.concurrency_slots,
        "container_cpus": allocation.cpu_millicores / 1000,
        "container_memory_mib": allocation.memory_bytes // (1024 * 1024),
        "container_pids": allocation.pids_max,
        "trajectory_cache_dir": allocation.scratch_directory + "/trajectories",
        "benchmark_cache": allocation.scratch_directory + "/benchmarks",
    }
    if any(key in settings and settings[key] != value for key, value in pinned.items()):
        raise NativeContainerError("native worker settings conflict with allocation")
    return replace(bootstrap, canonical_worker_settings=json.dumps(settings | pinned,
        sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False))


def native_create_argv(image: PreparedNativeImage, allocation: NativeWorkerAllocation, *, name: str, ownership: str) -> tuple[str, ...]:
    """Create only the approved worker entrypoint with explicit allocation limits."""

    if re.fullmatch(r"loom-native-[a-z0-9-]{1,100}", name) is None or _CONTAINER_ID.fullmatch(ownership) is None:
        raise NativeContainerError("native worker ownership identity is invalid")
    environment = _FIXED_ENV | {"TMPDIR": allocation.scratch_directory + "/tmp"}
    # A bare --env NAME removes image ENV when NAME is absent from the CLI's
    # empty environment. This also removes loader/config keys before Python -I.
    cleared = tuple(f"--env={name}" for name in image.inherited_environment if name not in environment)
    return (
        "container", "create", "--interactive", "--restart=no", "--read-only", "--no-healthcheck",
        "--cap-drop=ALL", "--security-opt=no-new-privileges", "--network=host",
        "--entrypoint=/usr/local/bin/python", "--workdir=/", "--stop-timeout=30",
        f"--user={allocation.runtime_uid}:{allocation.runtime_gid}", f"--group-add={allocation.docker_socket_gid}",
        f"--name={name}", f"--label=loom.native-launch={ownership}",
        f"--label=loom.intent_id={allocation.intent_id}", f"--label=loom.slurm_job_id={allocation.job_id}",
        f"--platform={image.platform}", f"--cgroup-parent={allocation.cgroup_parent}",
        f"--cpus={allocation.cpu_millicores / 1000:.3f}", f"--memory={allocation.memory_bytes}",
        f"--memory-swap={allocation.memory_bytes}", f"--pids-limit={allocation.pids_max}",
        "--ulimit=core=0:0", "--mount=type=bind,source=/var/run/docker.sock,target=/var/run/docker.sock",
        f"--mount=type=bind,source={allocation.scratch_directory},target={allocation.scratch_directory}",
        *cleared, *(f"--env={key}={value}" for key, value in sorted(environment.items())),
        image.image_id, "-I", "-m", "loom_worker.native_main",
    )


@dataclass(frozen=True, slots=True)
class FixedDockerCLI:
    """Verified Docker snapshot with explicit endpoint/config and an empty ENV."""

    executable: str
    descriptor: int
    config_directory: str

    def argv(self, *arguments: str) -> tuple[str, ...]:
        return (self.executable, f"--config={self.config_directory}",
                "--host=unix:///var/run/docker.sock", *arguments)

    def call(self, *arguments: str, timeout: int = 60) -> bytes:
        try:
            with subprocess.Popen(self.argv(*arguments),
                executable=self.executable, env={}, pass_fds=(self.descriptor,),
                stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.PIPE) as process:
                return capture_native_control_output(process, timeout=timeout)
        except (OSError, subprocess.SubprocessError):
            raise NativeContainerError("native Docker operation failed or has uncertain outcome") from None

    def json(self, *arguments: str, timeout: int = 60) -> Any:
        try:
            return json.loads(self.call(*arguments, timeout=timeout))
        except (TypeError, ValueError):
            raise NativeContainerError("native Docker readback is invalid") from None


def capture_native_control_output(process: subprocess.Popen[bytes], *, timeout: float) -> bytes:
    """Bound both streams while reading; failure leaves daemon outcome uncertain.

    Used only for short control operations. The attached worker lifetime must
    stream output without retaining logs and be supervised separately.
    """

    deadline = time.monotonic() + timeout
    output = bytearray()
    total_bytes = 0
    try:
        if process.stdout is None or process.stderr is None:
            raise NativeContainerError("native Docker output pipes are unavailable")
        with selectors.DefaultSelector() as selector:
            selector.register(process.stdout, selectors.EVENT_READ, True)
            selector.register(process.stderr, selectors.EVENT_READ, False)
            while selector.get_map():
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise NativeContainerError("native Docker operation timed out with uncertain outcome")
                for key, _event in selector.select(remaining):
                    chunk = os.read(key.fd, 65536)
                    if not chunk:
                        selector.unregister(key.fileobj)
                        continue
                    total_bytes += len(chunk)
                    if total_bytes > 1024 * 1024:
                        raise NativeContainerError("native Docker output exceeded its bound; outcome uncertain")
                    if key.data:
                        output.extend(chunk)
            if process.wait(timeout=max(0.001, deadline - time.monotonic())) != 0:
                raise NativeContainerError("native Docker operation failed")
        return bytes(output)
    finally:
        if process.poll() is None:
            process.kill()
            process.wait()


def prefetch_native_image(cli: FixedDockerCLI, *, image_digest: str, platform: str) -> PreparedNativeImage:
    """Pull and inspect immutable content before touching a one-use handoff."""

    cli.call("image", "pull", "--quiet", f"--platform={platform}", image_digest, timeout=1800)
    images = cli.json("image", "inspect", image_digest)
    if not isinstance(images, list) or len(images) != 1:
        raise NativeContainerError("native worker image readback is invalid")
    return prepare_native_image(images[0], image_digest=image_digest, platform=platform)


def remove_native_container(cli: FixedDockerCLI, container_id: str) -> None:
    """Remove an exact known-owned ID and require responsive-daemon absence.

    Only use after successful create returned its exact immutable ID. An absent
    name after an ambiguous create is NOT evidence that delayed creation ended.
    This is worker-container cleanup, not release of native trial runtime pins.
    """

    if _CONTAINER_ID.fullmatch(container_id) is None:
        raise NativeContainerError("native cleanup container identity is invalid")
    try:
        cli.call("container", "rm", "--force", container_id)
    except NativeContainerError:
        # Resolve a lost remove response only by successful exact-ID readback.
        if cli.call("container", "ls", "--all", "--quiet", "--no-trunc", f"--filter=id={container_id}").strip():
            raise NativeContainerError("native worker cleanup is unconfirmed") from None
        return
    if cli.call("container", "ls", "--all", "--quiet", "--no-trunc", f"--filter=id={container_id}").strip():
        raise NativeContainerError("native worker cleanup is unconfirmed")
