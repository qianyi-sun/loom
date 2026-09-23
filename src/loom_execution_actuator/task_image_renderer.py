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
# Loom-owned rootless Compose builder (#2086). Until the first immutable
# publish of deploy/Dockerfile.task-image-compose-builder, tests and Job
# renders pin the upstream dind-rootless digest used as that Dockerfile's
# FROM; production enablement must retarget this constant to the published
# cr.eu-north1…/…@sha256:… image that includes skopeo.
COMPOSE_BUILDER_IMAGE = (
    "docker.io/library/docker:28-dind-rootless@"
    "sha256:95813f7e06959c7cbd0e5a6e357cb76bf97c20db85ee2d16c57122c340ded385"
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
    builder_engine: Literal["buildkit", "compose"] = "buildkit"
    buildkit_image: str = BUILDKIT_IMAGE
    compose_image: str = COMPOSE_BUILDER_IMAGE
    cpu_millis: int = 1000
    memory_mib: int = 2048
    ephemeral_storage_mib: int = MIN_TASK_IMAGE_EPHEMERAL_STORAGE_MIB
    max_processes: int = 512
    active_deadline_seconds: int = 1800
    snapshotter: Literal["overlayfs", "native"] = "overlayfs"
    export_cache_mode: Literal["max", "min"] = "max"
    oci_export_format: Literal["archive", "directory"] = "archive"

    def __post_init__(self) -> None:
        for image in (self.service_image, self.buildkit_image, self.compose_image):
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
        if self.builder_engine not in {"buildkit", "compose"}:
            raise ValueError("task-image builder_engine must be buildkit or compose")
        if self.snapshotter not in {"overlayfs", "native"}:
            raise ValueError("task-image snapshotter must be overlayfs or native")
        if self.export_cache_mode not in {"max", "min"}:
            raise ValueError("task-image export_cache_mode must be max or min")
        if self.oci_export_format not in {"archive", "directory"}:
            raise ValueError("task-image oci_export_format must be archive or directory")
        if self.builder_engine == "compose":
            if self.oci_export_format != "archive":
                raise ValueError("compose builder v1 only supports oci_export_format=archive")
            if self.snapshotter != "overlayfs":
                raise ValueError("compose builder does not use BuildKit snapshotter")


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
    build_timeout_seconds: int,
    build_args: dict[str, str] | None = None,
    build_target: str | None = None,
    export_cache_mode: Literal["max", "min"] = "max",
    oci_export_format: Literal["archive", "directory"] = "archive",
) -> str:
    lines = [
        "set -eu",
        # Set both limits in the original namespace, before daemonless.sh starts
        # rootlesskit. Activation requires the native process-bound proof.
        f"ulimit -u {max_processes}",
        "mkdir -p /scratch/tmp /scratch/runtime /scratch/docker-config /scratch/state /loom/build/oci /loom/build/cache-out",
    ]
    for index, component in enumerate(components):
        dockerfile = PurePosixPath(component.dockerfile_path)
        context = PurePosixPath(_BUILD, "context", component.context_path).as_posix()
        dockerfile_dir = PurePosixPath(_BUILD, "context", dockerfile.parent).as_posix()
        cache_in = f"{_BUILD}/cache-in/{index}"
        cache_out = f"{_BUILD}/cache-out/{index}"
        if oci_export_format == "directory":
            relative = component.oci_output_path.removesuffix(".tar")
            output = f"{_BUILD}/{relative}"
            output_spec = f"type=oci,dest={output},tar=false"
            bytes_expr = (
                f"$(find {shlex.quote(output)} -type f -exec wc -c {{}} + 2>/dev/null "
                "| awk 'END {print $1+0}')"
            )
        else:
            output = f"{_BUILD}/{component.oci_output_path}"
            output_spec = f"type=oci,dest={output}"
            bytes_expr = f"$(wc -c < {shlex.quote(output)})"
        argv = [
            "timeout", "-s", "TERM", "-k", "10", str(build_timeout_seconds),
            # BusyBox passes through child signal exits; the waiting shell marks
            # only the timer signal as timeout, not a build exiting 137/143.
            "sh", "-c", 'trap "exit 124" TERM; "$@" & wait "$!"', "loom-build",
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
            output_spec,
        ]
        if component.name == "task":
            for key, value in sorted((build_args or {}).items()):
                argv.extend(["--opt", f"build-arg:{key}={value}"])
            if build_target is not None:
                argv.extend(["--opt", f"target={build_target}"])
        lines.append("set --")
        if cache_enabled:
            argv.extend(
                [
                    "--export-cache",
                    f"type=local,dest={cache_out},mode={export_cache_mode}",
                ]
            )
            lines.append(
                f"if [ -f {shlex.quote(cache_in + '/index.json')} ]; then set -- --import-cache {shlex.quote('type=local,src=' + cache_in)}; fi"
            )
        # Stage timing (Phase 2/5): solve covers buildctl LLB solve + OCI write.
        # oci_export records resulting bytes (archive size or directory sum).
        lines.extend(
            [
                (
                    'echo \'{"loom_task_image_stage":"solve","event":"start",'
                    f'"component_index":{index},"budget_seconds":{build_timeout_seconds}}}\''
                ),
                "solve_started=$(date +%s)",
                f'if {shlex.join(argv)} "$@"; then',
                "  solve_ended=$(date +%s)",
                (
                    '  echo \'{"loom_task_image_stage":"solve","event":"end",'
                    f'"component_index":{index},"duration_ms":\'"$(( (solve_ended - solve_started) * 1000 ))"\'}}\''
                ),
                (
                    '  echo \'{"loom_task_image_stage":"oci_export","event":"end",'
                    f'"component_index":{index},"included_in":"solve","bytes":\'"{bytes_expr}"\'}}\''
                ),
                (
                    '  echo \'{"loom_task_image_stage":"cleanup","event":"start",'
                    f'"component_index":{index}}}\''
                ),
                "  cleanup_started=$(date +%s)",
                # The daemonless process has exited. Use the same user mapping to
                # remove snapshots containing private directories owned by subuids;
                # outer UID1000 alone cannot necessarily traverse them. The cleanup
                # namespace has separate temporary state and touches no build output.
                "  mkdir -p /scratch/cleanup",
                f"  TMPDIR=/scratch/cleanup rootlesskit rm -rf -- {_SCRATCH_DIRECTORIES}",
                "  rm -rf -- /scratch/cleanup",
                f"  mkdir -p {_SCRATCH_DIRECTORIES}",
                "  cleanup_ended=$(date +%s)",
                (
                    '  echo \'{"loom_task_image_stage":"cleanup","event":"end",'
                    f'"component_index":{index},"duration_ms":\'"$(( (cleanup_ended - cleanup_started) * 1000 ))"\'}}\''
                ),
                "else",
                "  result=$?",
                "  solve_ended=$(date +%s)",
                (
                    '  echo \'{"loom_task_image_stage":"solve","event":"end",'
                    f'"component_index":{index},"failed":true,"exit":\'"$result"'
                    ',"duration_ms":\'"$(( (solve_ended - solve_started) * 1000 ))"\'}}\''
                ),
                "  case $result in 124) exit 124 ;; *) exit 1 ;; esac",
                "fi",
            ]
        )
    return "\n".join(lines) + "\n"


def _dockerfile_path_in_context(dockerfile_path: str, context_path: str) -> str:
    """Compose `dockerfile:` is resolved relative to the build context."""
    dockerfile = PurePosixPath(dockerfile_path)
    if context_path in {".", ""}:
        return dockerfile.as_posix()
    return dockerfile.relative_to(PurePosixPath(context_path)).as_posix()


def _compose_build_script(
    components: tuple[TaskImageBuildComponentV1, ...],
    *,
    platform: str,
    max_processes: int,
    cache_enabled: bool,
    build_timeout_seconds: int,
    build_args: dict[str, str] | None = None,
    build_target: str | None = None,
    export_cache_mode: Literal["max", "min"] = "max",
) -> str:
    """One rootless dockerd per Job; Compose+buildx → skopeo OCI archive (#2086/#2092)."""
    lines = [
        "set -eu",
        f"ulimit -u {max_processes}",
        "mkdir -p /run/user/1000 /scratch/tmp /scratch/runtime /scratch/docker-config "
        "/scratch/docker-data /scratch/docker-exec /loom/build/oci /loom/build/cache-out",
        "chmod 700 /run/user/1000",
        "export XDG_RUNTIME_DIR=/run/user/1000",
        "export TMPDIR=/scratch/tmp",
        "export DOCKER_CONFIG=/scratch/docker-config",
        "export DOCKER_HOST=unix:///scratch/docker-exec/docker.sock",
        f"export DOCKER_DEFAULT_PLATFORM={shlex.quote(platform)}",
        "command -v docker >/dev/null",
        "command -v rootlesskit >/dev/null",
        "command -v skopeo >/dev/null",
        "docker compose version >/dev/null",
        "docker buildx version >/dev/null",
        "trap 'test -z \"${daemon_pid:-}\" || kill \"$daemon_pid\" 2>/dev/null || true' EXIT",
        'echo \'{"loom_task_image_stage":"dockerd","event":"start"}\'',
        "dockerd_started=$(date +%s)",
        # Nested rootless dockerd (same recipe as Nebius compose probes).
        "/usr/bin/rootlesskit --net=none --detach-netns --port-driver=none "
        "--copy-up=/etc --copy-up=/run sh -ec '"
        "exec dockerd --rootless --host=\"$DOCKER_HOST\" "
        "--iptables=false --ip6tables=false --bridge=none "
        "--ip-forward=false --ip-masq=false --userland-proxy=false "
        "--data-root=/scratch/docker-data --exec-root=/scratch/docker-exec "
        "--pidfile=/scratch/docker.pid"
        "' >/scratch/daemon.log 2>&1 &",
        "daemon_pid=$!",
        "i=0",
        "until docker version >/dev/null 2>&1; do",
        "  i=$((i+1))",
        "  if [ \"$i\" -ge 60 ] || ! kill -0 \"$daemon_pid\" 2>/dev/null; then",
        "    cat /scratch/daemon.log >&2 || true",
        "    exit 1",
        "  fi",
        "  sleep 1",
        "done",
        "dockerd_ended=$(date +%s)",
        (
            'echo \'{"loom_task_image_stage":"dockerd","event":"end",'
            '"duration_ms":\'"$(( (dockerd_ended - dockerd_started) * 1000 ))"\'}\''
        ),
        # Default docker driver often cannot cache_to type=local; use buildx
        # docker-container so S3-backed local cache export is real (#2092).
        "docker buildx rm -f loom-compose >/dev/null 2>&1 || true",
        "docker buildx create --name loom-compose --driver docker-container --use",
        "docker buildx inspect --bootstrap >/dev/null",
    ]
    for index, component in enumerate(components):
        context = PurePosixPath(_BUILD, "context", component.context_path).as_posix()
        dockerfile_rel = _dockerfile_path_in_context(
            component.dockerfile_path, component.context_path
        )
        image_tag = f"loom-compose-build/{index}:local"
        output = f"{_BUILD}/{component.oci_output_path}"
        compose_file = f"/scratch/compose-{index}.yml"
        cache_in = f"{_BUILD}/cache-in/{index}"
        cache_out = f"{_BUILD}/cache-out/{index}"
        build_block = [
            "    build:",
            f"      context: {context}",
            f"      dockerfile: {dockerfile_rel}",
            "      network: host",
        ]
        if component.name == "task":
            if build_target is not None:
                build_block.append(f"      target: {build_target}")
            if build_args:
                build_block.append("      args:")
                for key, value in sorted(build_args.items()):
                    build_block.append(f"        {key}: {json.dumps(value)}")
        # Cache stanza is completed in shell so cache_from is omitted on miss.
        compose_head = "\n".join(
            [
                "services:",
                "  task:",
                f"    image: {image_tag}",
                *build_block,
            ]
        )
        lines.extend(
            [
                (
                    'echo \'{"loom_task_image_stage":"solve","event":"start",'
                    f'"component_index":{index},"budget_seconds":{build_timeout_seconds},'
                    '"builder_engine":"compose"}\''
                ),
                "solve_started=$(date +%s)",
                f"mkdir -p {shlex.quote(cache_out)}",
                f"cat > {shlex.quote(compose_file)} <<'LOOM_COMPOSE_EOF'",
                compose_head,
            ]
        )
        if cache_enabled:
            lines.extend(
                [
                    "LOOM_COMPOSE_EOF",
                    # Append cache_* under build: (same indent as context/dockerfile).
                    f"if [ -f {shlex.quote(cache_in + '/index.json')} ]; then",
                    f"  printf '%s\\n' '      cache_from:' "
                    f"'        - type=local,src={cache_in}' >> {shlex.quote(compose_file)}",
                    "fi",
                    f"printf '%s\\n' '      cache_to:' "
                    f"'        - type=local,dest={cache_out},mode={export_cache_mode}' "
                    f">> {shlex.quote(compose_file)}",
                ]
            )
        else:
            lines.append("LOOM_COMPOSE_EOF")
        lines.extend(
            [
                "set +e",
                (
                    f"timeout -s TERM -k 10 {build_timeout_seconds} "
                    f"docker compose -f {shlex.quote(compose_file)} "
                    "--progress plain --builder loom-compose build --load"
                ),
                "rc=$?",
                "set -e",
                "solve_ended=$(date +%s)",
                "if [ \"$rc\" -ne 0 ]; then",
                (
                    f'  echo \'{{"loom_task_image_stage":"solve","event":"end",'
                    f'"component_index":{index},"failed":true,"exit":\'"$rc"\''
                    f',"duration_ms":\'"$(( (solve_ended - solve_started) * 1000 ))"\'}}\''
                ),
                "  case $rc in 124) exit 124 ;; *) exit 1 ;; esac",
                "fi",
                (
                    f'echo \'{{"loom_task_image_stage":"solve","event":"end",'
                    f'"component_index":{index},"duration_ms":\'"$(( (solve_ended - solve_started) * 1000 ))"\'}}\''
                ),
            ]
        )
        if cache_enabled:
            # Fail closed: Compose may silently ignore unsupported cache_to.
            lines.extend(
                [
                    f"if [ ! -f {shlex.quote(cache_out + '/index.json')} ]; then",
                    (
                        '  echo \'{"loom_task_image_stage":"solve","event":"end",'
                        f'"component_index":{index},"failed":true,'
                        '"reason":"cache_export_missing"}\' >&2'
                    ),
                    "  exit 1",
                    "fi",
                ]
            )
        lines.extend(
            [
                (
                    f'echo \'{{"loom_task_image_stage":"oci_export","event":"start",'
                    f'"component_index":{index}}}\''
                ),
                "export_started=$(date +%s)",
                f"rm -f -- {shlex.quote(output)}",
                (
                    "skopeo copy --quiet "
                    f"docker-daemon:{shlex.quote(image_tag)} "
                    f"oci-archive:{shlex.quote(output)}"
                ),
                f"bytes=$(wc -c < {shlex.quote(output)})",
                "export_ended=$(date +%s)",
                (
                    f'echo \'{{"loom_task_image_stage":"oci_export","event":"end",'
                    f'"component_index":{index},"bytes":\'"$bytes"\''
                    f',"duration_ms":\'"$(( (export_ended - export_started) * 1000 ))"\'}}\''
                ),
                f"docker image rm -f {shlex.quote(image_tag)} >/dev/null 2>&1 || true",
            ]
        )
    lines.append("docker buildx rm -f loom-compose >/dev/null 2>&1 || true")
    lines.append("docker builder prune -af >/dev/null 2>&1 || true")
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
    # Directory export rewrites dests in the Job claim JSON only; plan schema and
    # TaskImageBuildComponentV1 still use oci/NNNN.tar. The build script strips
    # .tar when oci_export_format=directory.
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
    frozen["components"] = []
    for index, component in enumerate(checked):
        payload = component.model_dump(mode="json")
        if config.oci_export_format == "directory":
            payload["oci_output_path"] = f"oci/{index:04d}"
        frozen["components"].append(payload)
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
        # Shared S3 task-build-cache for buildkit and compose (#2092); build
        # container never mounts these secrets — only prepare/publish do.
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

    build_timeout_seconds = (
        math.ceil(environment.build_timeout_sec)
        if environment
        else config.active_deadline_seconds
    )
    build_args = environment.docker_build_args if environment else None
    build_target = environment.docker_build_target if environment else None
    compose = config.builder_engine == "compose"
    if compose:
        builder_command = _compose_build_script(
            checked,
            platform="linux/amd64" if architecture == "x86_64" else "linux/arm64",
            max_processes=config.max_processes,
            cache_enabled=config.cache_secret_name is not None,
            build_timeout_seconds=build_timeout_seconds,
            build_args=build_args,
            build_target=build_target,
            export_cache_mode=config.export_cache_mode,
        )
        builder_image = config.compose_image
        builder_env = [
            {"name": "TMPDIR", "value": "/scratch/tmp"},
            {"name": "DOCKER_CONFIG", "value": "/scratch/docker-config"},
            {"name": "XDG_RUNTIME_DIR", "value": "/run/user/1000"},
            {"name": "DOCKER_HOST", "value": "unix:///scratch/docker-exec/docker.sock"},
        ]
        builder_security = {
            **security,
            "allowPrivilegeEscalation": True,
            "capabilities": {"drop": ["ALL"], "add": ["SETUID", "SETGID"]},
            "seccompProfile": {"type": "Unconfined"},
            "appArmorProfile": {"type": "Unconfined"},
            "procMount": "Unmasked",
        }
        builder_mounts = [
            shared_mount,
            {"name": "builder-tmp", "mountPath": "/scratch"},
            {"name": "builder-tmp", "mountPath": "/tmp"},
            {"name": "run-user", "mountPath": "/run/user/1000"},
        ]
    else:
        builder_command = _build_script(
            checked,
            platform="linux/amd64" if architecture == "x86_64" else "linux/arm64",
            max_processes=config.max_processes,
            cache_enabled=config.cache_secret_name is not None,
            build_timeout_seconds=build_timeout_seconds,
            build_args=build_args,
            build_target=build_target,
            export_cache_mode=config.export_cache_mode,
            oci_export_format=config.oci_export_format,
        )
        builder_image = config.buildkit_image
        builder_env = [
            {"name": "TMPDIR", "value": "/scratch/tmp"},
            {"name": "DOCKER_CONFIG", "value": "/scratch/docker-config"},
            {"name": "XDG_RUNTIME_DIR", "value": "/scratch/runtime"},
            {
                "name": "BUILDKITD_FLAGS",
                "value": (
                    "--root /scratch/state --oci-worker-no-process-sandbox "
                    f"--oci-worker-snapshotter={config.snapshotter}"
                ),
            },
        ]
        builder_security = {
            **security,
            "allowPrivilegeEscalation": True,
            "capabilities": {"drop": ["ALL"], "add": ["SETUID", "SETGID"]},
            "seccompProfile": {"type": "Unconfined"},
            "appArmorProfile": {"type": "Unconfined"},
        }
        builder_mounts = [
            shared_mount,
            {"name": "builder-tmp", "mountPath": "/scratch"},
            # rootlesskit also creates bind0 under literal
            # /tmp even when TMPDIR points into /scratch.
            {"name": "builder-tmp", "mountPath": "/tmp"},
        ]

    builder = {
        "name": "build",
        "image": builder_image,
        "imagePullPolicy": "IfNotPresent",
        "command": ["sh", "-c", builder_command],
        "env": builder_env,
        "resources": {"requests": resources.copy(), "limits": resources.copy()},
        "securityContext": builder_security,
        "volumeMounts": builder_mounts,
        "terminationMessagePath": "/dev/termination-log",
        "terminationMessagePolicy": "File",
    }
    # Separate volumes preserve the trusted prepare/publish boundary. Let the
    # untrusted build use the existing total budget for either scratch or output
    # instead of evicting it at an arbitrary 7/7 GiB split. Kubelet accounts for
    # all disk-backed emptyDirs, writable layers and logs against the unchanged
    # aggregate Pod limit; these volume maxima are not extra reservations.
    phase_mib = config.ephemeral_storage_mib // 4
    build_mib = config.ephemeral_storage_mib
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
    if compose:
        volumes.append({"name": "run-user", "emptyDir": {"medium": "Memory", "sizeLimit": "32Mi"}})
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
    if compose:
        # Nested rootless dockerd requires a user namespace (compose probes).
        pod["hostUsers"] = False
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
            "activeDeadlineSeconds": config.active_deadline_seconds,
            "template": {"metadata": {"labels": copy.deepcopy(labels)}, "spec": pod},
        },
    }
    return configmap, job
