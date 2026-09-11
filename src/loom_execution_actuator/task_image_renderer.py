"""Pure Kubernetes Job rendering for isolated native task-image preparation."""

from __future__ import annotations

import copy
import json
import math
import re
import shlex
from dataclasses import dataclass
from pathlib import PurePosixPath
from typing import Any, Literal
from uuid import UUID

from loom.models.task import TaskConfig
from loom.task_image_build_plan import TaskImageBuildComponentV1
from loom_execution_actuator.renderer import ExecutionTargetRuntime

# Same manifest identity as the successful native rootless capability probe.
BUILDKIT_IMAGE = (
    "docker.io/moby/buildkit:v0.33.0-rootless@"
    "sha256:80b15f0735e87bab7bf59ec4d695dfb4a7cfb25521cf56dc75d6f256285b63ef"
)
_IMAGE = re.compile(r"[^\s]+@sha256:[0-9a-f]{64}")
_DNS_NAME = re.compile(r"[a-z0-9](?:[-a-z0-9.]*[a-z0-9])?")
_CLAIM = "/loom/claim/claim.json"
_BUILD = "/loom/build"
_CREDENTIALS = "/var/run/loom-task-build"
MAX_NATIVE_BUILD_COMPONENTS = 8
# One largest supported component needs 0.5 GiB context + 1 GiB cache in
# + 1 GiB cache out + 3 GiB OCI, plus logs. Its native snapshots and the
# publisher's extracted OCI require separate room. Larger multi-component
# builds remain bounded by the explicitly configured aggregate Pod budget.
MIN_TASK_IMAGE_EPHEMERAL_STORAGE_MIB = 16 * 1024
_SCRATCH_DIRECTORIES = "/scratch/state /scratch/tmp /scratch/runtime /scratch/docker-config"


@dataclass(frozen=True)
class TaskImageJobConfig:
    service_image: str
    source_secret_name: str
    registry_secret_name: str
    cache_secret_name: str | None = None
    registry_auth_kind: Literal["docker-config", "nebius"] = "docker-config"
    buildkit_image: str = BUILDKIT_IMAGE
    cpu_millis: int = 1000
    memory_mib: int = 2048
    ephemeral_storage_mib: int = MIN_TASK_IMAGE_EPHEMERAL_STORAGE_MIB
    max_processes: int = 512
    active_deadline_seconds: int = 1800

    def __post_init__(self) -> None:
        for image in (self.service_image, self.buildkit_image):
            if not _IMAGE.fullmatch(image):
                raise ValueError("task-image containers require an immutable image reference")
        for secret in (self.source_secret_name, self.registry_secret_name, self.cache_secret_name):
            if secret is not None and (len(secret) > 253 or not _DNS_NAME.fullmatch(secret)):
                raise ValueError("task-image Secret name is invalid")
        for name in (
            "cpu_millis",
            "memory_mib",
            "ephemeral_storage_mib",
            "max_processes",
            "active_deadline_seconds",
        ):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
                raise ValueError(f"{name} must be a positive integer")
        if self.ephemeral_storage_mib < MIN_TASK_IMAGE_EPHEMERAL_STORAGE_MIB:
            raise ValueError("native task-image builds require at least 16 GiB ephemeral storage")


def task_image_job_name(materialization_id: UUID, lease_epoch: int) -> str:
    if (
        materialization_id.int == 0
        or isinstance(lease_epoch, bool)
        or not isinstance(lease_epoch, int)
        or not 0 < lease_epoch < 2**63
    ):
        raise ValueError(
            "task-image Job identity requires a materialization and positive lease epoch"
        )
    return f"loom-img-{materialization_id.hex}-e{lease_epoch}"


def _build_script(
    components: tuple[TaskImageBuildComponentV1, ...],
    *,
    platform: str,
    max_processes: int,
    cache_enabled: bool,
    build_args: dict[str, str] | None = None,
    build_target: str | None = None,
) -> str:
    lines = [
        "set -eu",
        # Set both limits in the original namespace, before daemonless.sh starts
        # rootlesskit. Activation requires the native process-bound proof.
        f"ulimit -u {max_processes}",
        "mkdir -p /scratch/tmp /scratch/runtime /scratch/docker-config /scratch/state /loom/build/oci /loom/build/cache-out /loom/build/log",
    ]
    for index, component in enumerate(components):
        dockerfile = PurePosixPath(component.dockerfile_path)
        context = PurePosixPath(_BUILD, "context", component.context_path).as_posix()
        dockerfile_dir = PurePosixPath(_BUILD, "context", dockerfile.parent).as_posix()
        cache_in = f"{_BUILD}/cache-in/{index}"
        cache_out = f"{_BUILD}/cache-out/{index}"
        output = f"{_BUILD}/{component.oci_output_path}"
        logfile = f"{_BUILD}/log/{index:04d}.log"
        argv = [
            "buildctl-daemonless.sh",
            "build",
            "--progress",
            "plain",
            "--frontend",
            "dockerfile.v0",
            "--local",
            f"context={context}",
            "--local",
            f"dockerfile={dockerfile_dir}",
            "--opt",
            f"filename={dockerfile.name}",
            "--opt",
            f"platform={platform}",
            "--output",
            f"type=oci,dest={output}",
        ]
        if component.name == "task":
            for key, value in sorted((build_args or {}).items()):
                argv.extend(["--opt", f"build-arg:{key}={value}"])
            if build_target is not None:
                argv.extend(["--opt", f"target={build_target}"])
        lines.append("set --")
        if cache_enabled:
            argv.extend(["--export-cache", f"type=local,dest={cache_out},mode=max"])
            lines.append(
                f"if [ -f {shlex.quote(cache_in + '/index.json')} ]; then set -- --import-cache {shlex.quote('type=local,src=' + cache_in)}; fi"
            )
        lines.extend(
            [
                f'if {shlex.join(argv)} "$@" >{shlex.quote(logfile)} 2>&1; then',
                f"  cat {shlex.quote(logfile)}",
                # The daemonless process has exited. Use the same user mapping to
                # remove snapshots containing private directories owned by subuids;
                # outer UID1000 alone cannot necessarily traverse them. The cleanup
                # namespace has separate temporary state and touches no build output.
                "  mkdir -p /scratch/cleanup",
                f"  TMPDIR=/scratch/cleanup rootlesskit rm -rf -- {_SCRATCH_DIRECTORIES}",
                "  rm -rf -- /scratch/cleanup",
                f"  mkdir -p {_SCRATCH_DIRECTORIES}",
                "else",
                f"  cat {shlex.quote(logfile)}; exit 1",
                "fi",
            ]
        )
    return "\n".join(lines) + "\n"


def render_task_image_job(
    *,
    materialization_id: UUID,
    lease_epoch: int,
    claim: dict[str, Any],
    components: tuple[TaskImageBuildComponentV1, ...],
    target: ExecutionTargetRuntime,
    config: TaskImageJobConfig,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Return an immutable ConfigMap and sequential prepare/build/publish Job.

    ``claim`` is the controller's credential-free frozen snapshot/configuration;
    components come from the existing task-image build-plan derivation. Secret
    values must never be placed in this ConfigMap or the shared build volume.
    """
    name = task_image_job_name(materialization_id, lease_epoch)
    if not 1 <= len(components) <= MAX_NATIVE_BUILD_COMPONENTS:
        raise ValueError("task-image Job requires bounded Dockerfile components")
    checked = tuple(
        TaskImageBuildComponentV1.model_validate(component.model_dump()) for component in components
    )
    if len({component.name for component in checked}) != len(checked) or any(
        component.oci_output_path != f"oci/{index:04d}.tar"
        for index, component in enumerate(checked)
    ):
        raise ValueError("task-image components must retain canonical unique output identities")
    frozen = copy.deepcopy(claim)
    environment = (
        TaskConfig.model_validate(frozen["task_config"]).environment
        if "task_config" in frozen
        else None
    )
    for field, expected in (("id", str(materialization_id)), ("lease_epoch", lease_epoch)):
        if field in frozen and frozen[field] != expected:
            raise ValueError("task-image claim differs from Job identity")
        frozen[field] = expected
    architecture = frozen.get("cpu_arch")
    if architecture not in {"x86_64", "arm64"}:
        raise ValueError("task-image claim requires an explicit supported architecture")
    frozen["components"] = [component.model_dump(mode="json") for component in checked]
    claim_body = json.dumps(frozen, sort_keys=True, separators=(",", ":"), allow_nan=False)
    if len(claim_body.encode()) > 256 * 1024:
        raise ValueError("task-image claim exceeds the ConfigMap limit")
    labels = {
        "app.kubernetes.io/component": "task-image-builder",
        "loom.materialization-id": str(materialization_id),
        "loom.lease-epoch": str(lease_epoch),
    }
    metadata = {"name": name, "namespace": target.namespace, "labels": labels}
    configmap = {
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": copy.deepcopy(metadata),
        "immutable": True,
        "data": {"claim.json": claim_body},
    }
    resources = {
        "cpu": f"{config.cpu_millis}m",
        "memory": f"{config.memory_mib}Mi",
        "ephemeral-storage": f"{config.ephemeral_storage_mib}Mi",
    }
    security = {
        "runAsNonRoot": True,
        "runAsUser": 1000,
        "runAsGroup": 1000,
        "readOnlyRootFilesystem": True,
        "allowPrivilegeEscalation": False,
        "privileged": False,
        "capabilities": {"drop": ["ALL"]},
        "seccompProfile": {"type": "RuntimeDefault"},
        "appArmorProfile": {"type": "RuntimeDefault"},
    }
    claim_mount = {"name": "claim", "mountPath": "/loom/claim", "readOnly": True}
    shared_mount = {"name": "build", "mountPath": _BUILD}

    def phase(phase_name: str) -> dict[str, Any]:
        mounts = [
            copy.deepcopy(claim_mount),
            {**shared_mount, "readOnly": phase_name == "publish"},
            {"name": f"{phase_name}-tmp", "mountPath": "/tmp"},
        ]
        roles = ["source"] if phase_name == "prepare" else ["registry"]
        if config.cache_secret_name is not None:
            roles.append("cache")
        mounts.extend(
            {"name": role, "mountPath": f"{_CREDENTIALS}/{role}", "readOnly": True}
            for role in roles
        )
        return {
            "name": phase_name,
            "image": config.service_image,
            "imagePullPolicy": "IfNotPresent",
            "command": [
                "python",
                "-I",
                "-B",
                "-m",
                "loom_execution_actuator.task_image_runtime",
                phase_name,
                "--claim",
                _CLAIM,
            ],
            "env": [
                {"name": "TMPDIR", "value": "/tmp"},
                {"name": "DOCKER_CONFIG", "value": "/tmp/docker-config"},
            ],
            "resources": {"requests": resources.copy(), "limits": resources.copy()},
            "securityContext": copy.deepcopy(security),
            "volumeMounts": mounts,
            "terminationMessagePath": "/dev/termination-log",
            "terminationMessagePolicy": "File",
        }

    builder = {
        "name": "build",
        "image": config.buildkit_image,
        "imagePullPolicy": "IfNotPresent",
        "command": [
            "sh",
            "-c",
            _build_script(
                checked,
                platform="linux/amd64" if architecture == "x86_64" else "linux/arm64",
                max_processes=config.max_processes,
                cache_enabled=config.cache_secret_name is not None,
                build_args=environment.docker_build_args if environment else None,
                build_target=environment.docker_build_target if environment else None,
            ),
        ],
        "env": [
            {"name": "TMPDIR", "value": "/scratch/tmp"},
            {"name": "DOCKER_CONFIG", "value": "/scratch/docker-config"},
            {"name": "XDG_RUNTIME_DIR", "value": "/scratch/runtime"},
            {
                "name": "BUILDKITD_FLAGS",
                "value": "--root /scratch/state --oci-worker-no-process-sandbox --oci-worker-snapshotter=native",
            },
        ],
        "resources": {"requests": resources.copy(), "limits": resources.copy()},
        "securityContext": {
            **security,
            "allowPrivilegeEscalation": True,
            "capabilities": {"drop": ["ALL"], "add": ["SETUID", "SETGID"]},
            "seccompProfile": {"type": "Unconfined"},
            "appArmorProfile": {"type": "Unconfined"},
        },
        "volumeMounts": [
            shared_mount,
            {"name": "builder-tmp", "mountPath": "/scratch"},
            # rootlesskit also creates bind0 under literal
            # /tmp even when TMPDIR points into /scratch.
            {"name": "builder-tmp", "mountPath": "/tmp"},
        ],
        "terminationMessagePath": "/dev/termination-log",
        "terminationMessagePolicy": "File",
    }
    # emptyDir limits are not reservations. These phases run sequentially;
    # prepare/publish clear their own temporary archives, and each completed
    # builder clears its snapshots. At 16 GiB: shared=7, builder=7, each
    # trusted tmp=4 GiB. Peak shared+builder leaves 2 GiB for container logs;
    # publisher has space for a 3 GiB OCI extraction after builder cleanup.
    phase_mib = config.ephemeral_storage_mib // 4
    build_mib = config.ephemeral_storage_mib * 7 // 16
    volumes: list[dict[str, Any]] = [
        {"name": "claim", "configMap": {"name": name, "defaultMode": 0o444}},
        *(
            {"name": volume, "emptyDir": {"sizeLimit": f"{size}Mi"}}
            for volume, size in (
                ("build", build_mib),
                ("builder-tmp", build_mib),
                ("prepare-tmp", phase_mib),
                ("publish-tmp", phase_mib),
            )
        ),
    ]
    for role, secret_name in (
        ("source", config.source_secret_name),
        ("cache", config.cache_secret_name),
        ("registry", config.registry_secret_name),
    ):
        if secret_name is not None:
            keys = (
                (
                    ("credentials.json",)
                    if config.registry_auth_kind == "nebius"
                    else ("config.json",)
                )
                if role == "registry"
                else ("access-key", "secret-key")
            )
            volumes.append(
                {
                    "name": role,
                    "secret": {
                        "secretName": secret_name,
                        "defaultMode": 0o440,
                        "items": [{"key": key, "path": key} for key in keys],
                    },
                }
            )
    node_selector = dict(target.node_selector or {})
    # Nebius ignores reserved Kubernetes labels in zero-node templates. Translate
    # these two constraints to the provider-supported labels owned by our IaC.
    for standard_key, key, expected in (
        ("kubernetes.io/os", "loom.nebius/node-os", "linux"),
        (
            "kubernetes.io/arch",
            "loom.nebius/node-arch",
            "amd64" if architecture == "x86_64" else "arm64",
        ),
    ):
        for constraint in (standard_key, key):
            if constraint in node_selector and node_selector[constraint] != expected:
                raise ValueError("native task-image architecture conflicts with execution target")
        node_selector.pop(standard_key, None)
        node_selector[key] = expected
    pod: dict[str, Any] = {
        "restartPolicy": "Never",
        "automountServiceAccountToken": False,
        "enableServiceLinks": False,
        "shareProcessNamespace": False,
        "hostNetwork": False,
        "hostPID": False,
        "hostIPC": False,
        "serviceAccountName": target.service_account_name,
        "nodeSelector": node_selector,
        "tolerations": [dict(item) for item in target.tolerations],
        "securityContext": {
            "runAsNonRoot": True,
            "runAsUser": 1000,
            "runAsGroup": 1000,
            "fsGroup": 1000,
            "fsGroupChangePolicy": "OnRootMismatch",
            "seccompProfile": {"type": "RuntimeDefault"},
        },
        "terminationGracePeriodSeconds": 30,
        "volumes": volumes,
        "initContainers": [phase("prepare"), builder],
        "containers": [phase("publish")],
    }
    if target.runtime_class_name is not None:
        pod["runtimeClassName"] = target.runtime_class_name
    job = {
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": copy.deepcopy(metadata),
        "spec": {
            "backoffLimit": 0,
            "parallelism": 1,
            "completions": 1,
            "activeDeadlineSeconds": min(
                config.active_deadline_seconds, math.ceil(environment.build_timeout_sec)
            )
            if environment
            else config.active_deadline_seconds,
            "template": {"metadata": {"labels": copy.deepcopy(labels)}, "spec": pod},
        },
    }
    return configmap, job
