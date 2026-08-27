#!/usr/bin/env python3
"""Operate the isolated oldlab-5 GitHub Actions runner pool.

Mutation commands are dry-run by default. ``--execute`` is required to build a
golden guest, register a JIT runner, start a VM, drain capacity, or delete a
runner registration. Secrets are read from a mode-0600 file and are never
included in plans, state, subprocess arguments, or output.
"""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import os
import re
import shutil
import socket
import stat
import subprocess
import sys
import tomllib
import urllib.error
import urllib.request
import uuid
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Protocol

SCHEMA_VERSION = 2
EXPECTED_REPOSITORY = "qianyi-sun/loom"
EXPECTED_HOSTNAME = "trt-eai-oldlab-5"
EXPECTED_LABELS = (
    "self-hosted",
    "linux",
    "x64",
    "loom-ci",
    "oldlab-5",
    "ephemeral-kvm",
)
ROUTING_VARIABLE = "LOOM_CI_ACCELERATOR_RUNS_ON"
ROUTING_MODE_VARIABLE = "LOOM_CI_ROUTE_MODE"
WORK_CLASS_CONTRACTS = {
    "normal": ("loom-ci-normal", "LOOM_CI_NORMAL_RUNS_ON"),
    "image": ("loom-ci-image", "LOOM_CI_IMAGE_RUNS_ON"),
    "smoke": ("loom-ci-smoke", "LOOM_CI_SMOKE_RUNS_ON"),
}
ROUTING_VARIABLES = (
    ROUTING_MODE_VARIABLE,
    ROUTING_VARIABLE,
    *(contract[1] for contract in WORK_CLASS_CONTRACTS.values()),
)
MAX_SLOTS = 11
HOST_CPU_BUDGET = 22
HOST_MEMORY_BUDGET_MIB = 80 * 1024
MAX_TOKEN_BYTES = 4096
GOLDEN_IMAGE_NAME = "golden.qcow2"
GOLDEN_MANIFEST_NAME = "golden-manifest.json"
STATE_SCHEMA_VERSION = 1
DOCKERHUB_CREDENTIALS_NAME = "dockerhub-credentials.json"
UV_VERSION = "0.11.26"
UV_TARGET = "x86_64-unknown-linux-gnu"
UV_ARCHIVE_NAME = f"uv-{UV_TARGET}.tar.gz"
UV_ARCHIVE_URL = (
    f"https://github.com/astral-sh/uv/releases/download/{UV_VERSION}/{UV_ARCHIVE_NAME}"
)
UV_ARCHIVE_SHA256 = (
    "6426a73c3837e6e2483ee344cbc00f36394d179afcba6183cb77437e67db4af0"
)
UV_ASSET_PORT = 8181
UV_MANIFEST_NAME = "uv.ndjson"
UV_MANIFEST_URL = f"http://127.0.0.1:{UV_ASSET_PORT}/{UV_MANIFEST_NAME}"
GUEST_BASE_IMAGES = (
    "alpine:3.19",
    "alpine:3.20",
    "alpine/socat:1.7.4.4",
    "aquasec/trivy:latest",
    "bitnamilegacy/pgbouncer:1.24.0",
    "busybox:latest",
    "docker:29.7.2-cli",
    "edoburu/pgbouncer:latest",
    "envoyproxy/envoy:v1.30-latest",
    "golang:1.23-alpine",
    "golang:1.25.7-alpine",
    "minio/minio:RELEASE.2022-12-02T19-19-22Z",
    "minio/minio:latest",
    "moby/buildkit:buildx-stable-1",
    "moby/buildkit:rootless",
    "nginxinc/nginx-unprivileged:1.27-alpine",
    "node:20-slim",
    "node:20.19.5-slim",
    "node:22-bookworm-slim",
    "postgres:16",
    "postgres:16-alpine",
    "postgres:17.4",
    "prometheuscommunity/pgbouncer-exporter:v0.12.1",
    "python:3.11-alpine",
    "python:3.11-slim",
    "python:3.12-bookworm",
    "python:3.12.13-slim-bookworm",
    "python:3.12-slim",
    "rancher/k3s:latest",
    "registry:2",
    "testcontainers/ryuk:0.8.1",
    "tonistiigi/binfmt:latest",
)
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_GIT_SHA_RE = re.compile(r"^[0-9a-f]{40}$")
_SAFE_IMAGE_RE = re.compile(r"^loom-ci-runner-qemu:[a-z0-9][a-z0-9._-]{0,63}$")
_SAFE_PREFIX_RE = re.compile(r"^[a-z0-9][a-z0-9-]{2,30}$")
_DOCKERHUB_USERNAME_RE = re.compile(r"^[A-Za-z0-9_.-]{1,128}$")


class PoolConfigError(ValueError):
    """The checked-in pool profile is invalid."""


class PoolOperationError(RuntimeError):
    """A pool operation failed without exposing credential material."""


@dataclass(frozen=True, slots=True)
class WorkClassProfile:
    name: str
    label: str
    slots: int
    routing_variable: str
    hosted_overflow_after_seconds: int

    def validate(self) -> None:
        expected = WORK_CLASS_CONTRACTS.get(self.name)
        if expected is None:
            raise PoolConfigError(f"unknown work class: {self.name}")
        if (self.label, self.routing_variable) != expected:
            raise PoolConfigError(f"work class {self.name} contract is invalid")
        if type(self.slots) is not int or not 0 <= self.slots <= MAX_SLOTS:
            raise PoolConfigError(f"work class {self.name} slots must be in 0..{MAX_SLOTS}")
        if (
            type(self.hosted_overflow_after_seconds) is not int
            or not 60 <= self.hosted_overflow_after_seconds <= 3600
        ):
            raise PoolConfigError(
                f"work class {self.name} hosted overflow must be in 60..3600 seconds",
            )


@dataclass(frozen=True, slots=True)
class PoolProfile:
    schema_version: int
    repository: str
    expected_hostname: str
    state_root: Path
    cache_root: Path
    qemu_image: str
    runner_name_prefix: str
    slots: int
    vcpus_per_slot: int
    memory_mib_per_slot: int
    disk_gib_per_slot: int
    reconcile_seconds: int
    host_cpu_budget: int
    host_memory_budget_mib: int
    labels: tuple[str, ...]
    work_classes: tuple[WorkClassProfile, ...]
    cloud_image_url: str
    cloud_image_sha256: str
    actions_runner_url: str
    actions_runner_sha256: str

    @property
    def total_guest_vcpus(self) -> int:
        return self.slots * self.vcpus_per_slot

    @property
    def total_memory_mib(self) -> int:
        return self.slots * self.memory_mib_per_slot

    @property
    def cloud_image_name(self) -> str:
        return Path(self.cloud_image_url).name

    @property
    def actions_runner_name(self) -> str:
        return Path(self.actions_runner_url).name

    def work_class_for_slot(self, slot: int) -> WorkClassProfile:
        if not 0 <= slot < self.slots:
            raise PoolConfigError(f"slot must be in 0..{self.slots - 1}")
        offset = 0
        for work_class in self.work_classes:
            offset += work_class.slots
            if slot < offset:
                return work_class
        raise PoolConfigError(f"slot {slot} has no work class")

    def labels_for_slot(self, slot: int) -> tuple[str, ...]:
        return (*self.labels, self.work_class_for_slot(slot).label)

    @property
    def golden_image(self) -> Path:
        return self.cache_root / GOLDEN_IMAGE_NAME

    @property
    def golden_manifest(self) -> Path:
        return self.cache_root / GOLDEN_MANIFEST_NAME

    def validate(self) -> None:
        if self.schema_version != SCHEMA_VERSION:
            raise PoolConfigError(f"schema_version must be {SCHEMA_VERSION}")
        if self.repository != EXPECTED_REPOSITORY:
            raise PoolConfigError(f"repository must be {EXPECTED_REPOSITORY}")
        if self.expected_hostname != EXPECTED_HOSTNAME:
            raise PoolConfigError(f"expected_hostname must be {EXPECTED_HOSTNAME}")
        for field, path in (
            ("state_root", self.state_root),
            ("cache_root", self.cache_root),
        ):
            if not path.is_absolute() or ".." in path.parts:
                raise PoolConfigError(f"{field} must be an absolute normalized path")
        if self.state_root == self.cache_root:
            raise PoolConfigError("state_root and cache_root must be distinct")
        if _SAFE_IMAGE_RE.fullmatch(self.qemu_image) is None:
            raise PoolConfigError("qemu_image must be a bounded local Loom tag")
        if _SAFE_PREFIX_RE.fullmatch(self.runner_name_prefix) is None:
            raise PoolConfigError("runner_name_prefix is invalid")
        if not 1 <= self.slots <= MAX_SLOTS:
            raise PoolConfigError(f"slots must be in 1..{MAX_SLOTS}")
        if not 4 <= self.vcpus_per_slot <= 8:
            raise PoolConfigError("vcpus_per_slot must be in 4..8")
        if not 2048 <= self.memory_mib_per_slot <= 8192:
            raise PoolConfigError("memory_mib_per_slot must be in 2048..8192")
        if not 16 <= self.disk_gib_per_slot <= 128:
            raise PoolConfigError("disk_gib_per_slot must be in 16..128")
        if not 10 <= self.reconcile_seconds <= 300:
            raise PoolConfigError("reconcile_seconds must be in 10..300")
        if self.host_cpu_budget != HOST_CPU_BUDGET:
            raise PoolConfigError("host_cpu_budget must remain 22")
        if self.host_memory_budget_mib != HOST_MEMORY_BUDGET_MIB:
            raise PoolConfigError("host_memory_budget_mib must remain 81920")
        if self.total_memory_mib > self.host_memory_budget_mib:
            raise PoolConfigError("aggregate memory budget exceeds 80 GiB")
        if self.labels != EXPECTED_LABELS:
            raise PoolConfigError(f"labels must be exactly {list(EXPECTED_LABELS)}")
        expected_names = tuple(WORK_CLASS_CONTRACTS)
        actual_names = tuple(work_class.name for work_class in self.work_classes)
        if actual_names != expected_names:
            raise PoolConfigError(
                f"work classes must be ordered exactly as {list(expected_names)}",
            )
        for work_class in self.work_classes:
            work_class.validate()
        if sum(work_class.slots for work_class in self.work_classes) != self.slots:
            raise PoolConfigError("work class slots must sum to pool slots")
        for field, value in (
            ("cloud_image_url", self.cloud_image_url),
            ("actions_runner_url", self.actions_runner_url),
        ):
            if not value.startswith("https://") or value != value.strip():
                raise PoolConfigError(f"{field} must be an HTTPS URL")
        if "cloud-images.ubuntu.com/releases/noble/" not in self.cloud_image_url:
            raise PoolConfigError("cloud_image_url must use the Ubuntu noble release feed")
        if "github.com/actions/runner/releases/download/" not in self.actions_runner_url:
            raise PoolConfigError("actions_runner_url must use an official runner release")
        for field, value in (
            ("cloud_image_sha256", self.cloud_image_sha256),
            ("actions_runner_sha256", self.actions_runner_sha256),
        ):
            if _SHA256_RE.fullmatch(value) is None:
                raise PoolConfigError(f"{field} must be a lowercase SHA-256")


@dataclass(frozen=True, slots=True)
class CommandResult:
    returncode: int
    stdout: str = ""
    stderr: str = ""


class CommandRunner(Protocol):
    def run(
        self,
        argv: Sequence[str],
        *,
        timeout: int | None = None,
    ) -> CommandResult: ...


class SubprocessCommandRunner:
    def run(
        self,
        argv: Sequence[str],
        *,
        timeout: int | None = None,
    ) -> CommandResult:
        completed = subprocess.run(
            list(argv),
            check=False,
            capture_output=True,
            text=True,
            timeout=timeout,
        )
        return CommandResult(completed.returncode, completed.stdout, completed.stderr)


class GitHubRunnerAPI(Protocol):
    def list_runners(self) -> list[dict[str, Any]]: ...

    def routing_variable_present(self, name: str) -> bool: ...

    def generate_jit_config(
        self,
        *,
        name: str,
        labels: Sequence[str],
    ) -> tuple[int, str]: ...

    def delete_runner(self, runner_id: int) -> None: ...


class GitHubAPI:
    def __init__(self, *, repository: str, token: str) -> None:
        self.repository = repository
        self._token = token

    def _request(
        self,
        method: str,
        path: str,
        *,
        payload: Mapping[str, object] | None = None,
        allow_not_found: bool = False,
    ) -> Any:
        body = None
        if payload is not None:
            body = json.dumps(payload, separators=(",", ":")).encode()
        request = urllib.request.Request(
            f"https://api.github.com/repos/{self.repository}{path}",
            data=body,
            method=method,
            headers={
                "Accept": "application/vnd.github+json",
                "Authorization": f"Bearer {self._token}",
                "Content-Type": "application/json",
                "User-Agent": "loom-ci-runner-pool/1",
                "X-GitHub-Api-Version": "2022-11-28",
            },
        )
        try:
            with urllib.request.urlopen(request, timeout=20) as response:
                raw = response.read()
        except urllib.error.HTTPError as exc:
            if allow_not_found and exc.code == 404:
                return None
            raise PoolOperationError(
                f"GitHub API {method} {path} failed with HTTP {exc.code}",
            ) from None
        except (OSError, TimeoutError) as exc:
            raise PoolOperationError(f"GitHub API {method} {path} failed") from exc
        if not raw:
            return None
        try:
            return json.loads(raw)
        except json.JSONDecodeError as exc:
            raise PoolOperationError("GitHub API returned invalid JSON") from exc

    def list_runners(self) -> list[dict[str, Any]]:
        payload = self._request("GET", "/actions/runners?per_page=100")
        runners = payload.get("runners") if isinstance(payload, dict) else None
        if not isinstance(runners, list):
            raise PoolOperationError("GitHub runner inventory is malformed")
        return [dict(item) for item in runners if isinstance(item, dict)]

    def routing_variable_present(self, name: str) -> bool:
        if name not in ROUTING_VARIABLES:
            raise PoolOperationError("routing variable is outside the accepted contract")
        payload = self._request(
            "GET",
            f"/actions/variables/{name}",
            allow_not_found=True,
        )
        return payload is not None

    def generate_jit_config(
        self,
        *,
        name: str,
        labels: Sequence[str],
    ) -> tuple[int, str]:
        payload = self._request(
            "POST",
            "/actions/runners/generate-jitconfig",
            payload={
                "name": name,
                "runner_group_id": 1,
                "labels": list(labels),
                "work_folder": "_work",
            },
        )
        if not isinstance(payload, dict):
            raise PoolOperationError("GitHub JIT response is malformed")
        encoded = payload.get("encoded_jit_config")
        runner = payload.get("runner")
        runner_id = runner.get("id") if isinstance(runner, dict) else None
        if type(runner_id) is not int or not isinstance(encoded, str) or not encoded:
            raise PoolOperationError("GitHub JIT response is missing required fields")
        return runner_id, encoded

    def delete_runner(self, runner_id: int) -> None:
        self._request("DELETE", f"/actions/runners/{runner_id}")


@dataclass(frozen=True, slots=True)
class SlotState:
    schema_version: int
    slot: int
    runner_id: int
    runner_name: str
    container_name: str
    created_at: str
    candidate_sha: str


@dataclass(frozen=True, slots=True)
class DockerHubCredentials:
    username: str
    token: str


def load_profile(path: Path) -> PoolProfile:
    try:
        with path.open("rb") as handle:
            raw = tomllib.load(handle)
    except OSError as exc:
        raise PoolConfigError(f"could not read profile: {path}") from exc
    except tomllib.TOMLDecodeError as exc:
        raise PoolConfigError(f"invalid profile TOML: {path}") from exc

    expected = {
        "schema_version",
        "repository",
        "expected_hostname",
        "state_root",
        "cache_root",
        "qemu_image",
        "runner_name_prefix",
        "slots",
        "vcpus_per_slot",
        "memory_mib_per_slot",
        "disk_gib_per_slot",
        "reconcile_seconds",
        "host_cpu_budget",
        "host_memory_budget_mib",
        "labels",
        "work_classes",
        "cloud_image_url",
        "cloud_image_sha256",
        "actions_runner_url",
        "actions_runner_sha256",
    }
    unknown = sorted(set(raw) - expected)
    missing = sorted(expected - set(raw))
    if unknown or missing:
        raise PoolConfigError(
            f"profile fields mismatch: missing={missing}, unknown={unknown}",
        )
    try:
        raw_work_classes = raw["work_classes"]
        if not isinstance(raw_work_classes, list):
            raise PoolConfigError("work_classes must be a TOML array of tables")
        work_classes = tuple(
            WorkClassProfile(
                name=item["name"],
                label=item["label"],
                slots=item["slots"],
                routing_variable=item["routing_variable"],
                hosted_overflow_after_seconds=item[
                    "hosted_overflow_after_seconds"
                ],
            )
            for item in raw_work_classes
            if isinstance(item, dict)
        )
        if len(work_classes) != len(raw_work_classes):
            raise PoolConfigError("work class entries must be tables")
        profile = PoolProfile(
            schema_version=raw["schema_version"],
            repository=raw["repository"],
            expected_hostname=raw["expected_hostname"],
            state_root=Path(raw["state_root"]),
            cache_root=Path(raw["cache_root"]),
            qemu_image=raw["qemu_image"],
            runner_name_prefix=raw["runner_name_prefix"],
            slots=raw["slots"],
            vcpus_per_slot=raw["vcpus_per_slot"],
            memory_mib_per_slot=raw["memory_mib_per_slot"],
            disk_gib_per_slot=raw["disk_gib_per_slot"],
            reconcile_seconds=raw["reconcile_seconds"],
            host_cpu_budget=raw["host_cpu_budget"],
            host_memory_budget_mib=raw["host_memory_budget_mib"],
            labels=tuple(raw["labels"]),
            work_classes=work_classes,
            cloud_image_url=raw["cloud_image_url"],
            cloud_image_sha256=raw["cloud_image_sha256"],
            actions_runner_url=raw["actions_runner_url"],
            actions_runner_sha256=raw["actions_runner_sha256"],
        )
    except (KeyError, TypeError) as exc:
        raise PoolConfigError("profile field types are invalid") from exc
    profile.validate()
    return profile


def read_token(path: Path) -> str:
    try:
        metadata = path.stat()
        raw = path.read_bytes()
    except OSError as exc:
        raise PoolOperationError("could not read GitHub token file") from exc
    if not stat.S_ISREG(metadata.st_mode):
        raise PoolOperationError("GitHub token source must be a regular file")
    if stat.S_IMODE(metadata.st_mode) & 0o077:
        raise PoolOperationError("GitHub token file must not grant group/other access")
    if not raw or len(raw) > MAX_TOKEN_BYTES or b"\x00" in raw:
        raise PoolOperationError("GitHub token file has an invalid size or encoding")
    try:
        token = raw.decode("utf-8").strip()
    except UnicodeDecodeError as exc:
        raise PoolOperationError("GitHub token file is not UTF-8") from exc
    if not token or any(char.isspace() for char in token):
        raise PoolOperationError("GitHub token file must contain one opaque token")
    return token


def read_dockerhub_credentials(path: Path) -> DockerHubCredentials:
    try:
        metadata = path.stat()
        raw = path.read_bytes()
    except OSError as exc:
        raise PoolOperationError("could not read Docker Hub credentials") from exc
    if not stat.S_ISREG(metadata.st_mode):
        raise PoolOperationError("Docker Hub credentials must be a regular file")
    if stat.S_IMODE(metadata.st_mode) & 0o077:
        raise PoolOperationError(
            "Docker Hub credentials must not grant group/other access",
        )
    if not raw or len(raw) > MAX_TOKEN_BYTES or b"\x00" in raw:
        raise PoolOperationError("Docker Hub credentials have an invalid size")
    try:
        payload = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise PoolOperationError("Docker Hub credentials are not valid JSON") from exc
    if not isinstance(payload, dict) or set(payload) != {"username", "token"}:
        raise PoolOperationError("Docker Hub credential fields are invalid")
    username = payload.get("username")
    token = payload.get("token")
    if (
        not isinstance(username, str)
        or _DOCKERHUB_USERNAME_RE.fullmatch(username) is None
        or not isinstance(token, str)
        or not token
        or any(char.isspace() for char in token)
    ):
        raise PoolOperationError("Docker Hub credentials are invalid")
    return DockerHubCredentials(username=username, token=token)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _atomic_json(path: Path, payload: Mapping[str, object], *, mode: int = 0o600) -> None:
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
    temporary.write_text(
        json.dumps(payload, sort_keys=True, indent=2) + "\n",
        encoding="utf-8",
    )
    temporary.chmod(mode)
    os.replace(temporary, path)


def _write_private(path: Path, data: str | bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
    descriptor = os.open(path, flags, 0o600)
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(data.encode() if isinstance(data, str) else data)
    except BaseException:
        path.unlink(missing_ok=True)
        raise


def _download_pinned(url: str, target: Path, expected_sha256: str) -> None:
    if target.exists() and _sha256(target) == expected_sha256:
        return
    target.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    temporary = target.with_name(f".{target.name}.{uuid.uuid4().hex}.download")
    request = urllib.request.Request(url, headers={"User-Agent": "loom-ci-runner-pool/1"})
    try:
        with urllib.request.urlopen(request, timeout=60) as response:
            with temporary.open("xb") as handle:
                shutil.copyfileobj(response, handle, length=1024 * 1024)
        if _sha256(temporary) != expected_sha256:
            raise PoolOperationError(f"download checksum mismatch for {target.name}")
        temporary.chmod(0o600)
        os.replace(temporary, target)
    except BaseException:
        temporary.unlink(missing_ok=True)
        raise


def _require_success(
    runner: CommandRunner,
    argv: Sequence[str],
    *,
    timeout: int | None = None,
    purpose: str,
) -> str:
    result = runner.run(argv, timeout=timeout)
    if result.returncode != 0:
        raise PoolOperationError(f"{purpose} failed")
    return result.stdout


def _docker_utility_command(
    profile: PoolProfile,
    *,
    state_mount: Path,
    args: Sequence[str],
    cache_read_only: bool = True,
) -> tuple[str, ...]:
    cache_mount = f"type=bind,src={profile.cache_root},dst=/cache"
    if cache_read_only:
        cache_mount += ",readonly"
    return (
        "docker",
        "run",
        "--rm",
        "--dns",
        "1.1.1.1",
        "--dns",
        "8.8.8.8",
        "--read-only",
        "--tmpfs",
        "/tmp:rw,nosuid,nodev,noexec,size=256m",
        "--tmpfs",
        "/run:rw,nosuid,nodev,size=16m",
        "--cap-drop",
        "ALL",
        "--cap-add",
        "NET_ADMIN",
        "--cap-add",
        "SETPCAP",
        "--security-opt",
        "no-new-privileges",
        "--mount",
        f"type=bind,src={state_mount},dst=/state",
        "--mount",
        cache_mount,
        profile.qemu_image,
        *args,
    )


def _uv_manifest() -> str:
    return json.dumps(
        {
            "version": UV_VERSION,
            "artifacts": [
                {
                    "platform": UV_TARGET,
                    "variant": "default",
                    "url": f"http://127.0.0.1:{UV_ASSET_PORT}/{UV_ARCHIVE_NAME}",
                    "archive_format": "tar.gz",
                    "sha256": UV_ARCHIVE_SHA256,
                },
            ],
        },
        separators=(",", ":"),
    )


def _qemu_container_command(
    profile: PoolProfile,
    *,
    slot: int,
    slot_root: Path,
    container_name: str,
    detach: bool,
) -> tuple[str, ...]:
    command = [
        "docker",
        "run",
        "-d" if detach else "--rm",
        "--dns",
        "1.1.1.1",
        "--dns",
        "8.8.8.8",
        "--name",
        container_name,
        "--read-only",
        "--tmpfs",
        "/tmp:rw,nosuid,nodev,noexec,size=256m",
        "--tmpfs",
        "/run:rw,nosuid,nodev,size=16m",
        "--device",
        "/dev/kvm",
        "--cgroup-parent",
        "loom-ci-runner-pool.slice",
        "--cap-drop",
        "ALL",
        "--cap-add",
        "NET_ADMIN",
        "--cap-add",
        "SETPCAP",
        "--security-opt",
        "no-new-privileges",
        "--pids-limit",
        "512",
        "--cpu-shares",
        "1024",
        "--memory",
        f"{profile.memory_mib_per_slot + 512}m",
        "--label",
        "loom.ci.runner=true",
        "--label",
        f"loom.ci.slot={slot}",
        "--mount",
        f"type=bind,src={slot_root},dst=/slot",
        "--mount",
        f"type=bind,src={profile.cache_root},dst=/cache,readonly",
        profile.qemu_image,
        "qemu-system-x86_64",
        "-accel",
        "kvm",
        "-cpu",
        "host",
        "-smp",
        str(profile.vcpus_per_slot),
        "-m",
        str(profile.memory_mib_per_slot),
        "-drive",
        "if=virtio,format=qcow2,file=/slot/root.qcow2,cache=none,discard=unmap",
        "-drive",
        "if=virtio,format=raw,file=/slot/seed.iso,readonly=on",
        "-drive",
        "if=virtio,format=raw,file=/slot/jit.iso,readonly=on",
        "-netdev",
        "user,id=net0",
        "-device",
        "virtio-net-pci,netdev=net0",
        "-display",
        "none",
        "-serial",
        "file:/slot/serial.log",
        "-no-reboot",
    ]
    return tuple(command)


def _base_install_script(profile: PoolProfile) -> str:
    runner_sha = profile.actions_runner_sha256
    base_images = " ".join(GUEST_BASE_IMAGES)
    uv_manifest = _uv_manifest()
    return f"""#!/usr/bin/env bash
set -euo pipefail
export DEBIAN_FRONTEND=noninteractive
apt-get update
apt-get install -y --no-install-recommends \
  build-essential \
  ca-certificates \
  curl \
  docker-buildx \
  docker-compose-v2 \
  docker-registry \
  docker.io \
  git \
  jq \
  python-is-python3 \
  skopeo \
  sudo \
  unzip
systemctl enable --now docker
wait_for_registry() {{
  for _attempt in $(seq 1 30); do
    if curl --fail --silent http://127.0.0.1:5000/v2/ >/dev/null; then
      return 0
    fi
    sleep 1
  done
  systemctl status --no-pager docker-registry >&2 || true
  journalctl --no-pager -u docker-registry -n 50 >&2 || true
  return 1
}}
install -d -o docker-registry -g docker-registry -m 0750 \
  /var/lib/loom-ci-registry
cat >/etc/docker/registry/config.yml <<'EOF'
version: 0.1
log:
  fields:
    service: loom-ci-registry
storage:
  filesystem:
    rootdirectory: /var/lib/loom-ci-registry
http:
  addr: 0.0.0.0:5000
EOF
systemctl enable docker-registry
systemctl restart docker-registry
wait_for_registry
install -d -m 0700 /run/loom-ci-registry
install -d -m 0755 /mnt/loom-ci-assets
mount -o ro LABEL=LOOMCIASSETS /mnt/loom-ci-assets
install -m 0600 \
  /mnt/loom-ci-assets/{DOCKERHUB_CREDENTIALS_NAME} \
  /run/loom-ci-registry/{DOCKERHUB_CREDENTIALS_NAME}
dockerhub_username=$(
  python3 -c 'import json; print(json.load(open("/run/loom-ci-registry/{DOCKERHUB_CREDENTIALS_NAME}"))["username"])'
)
cleanup_registry_auth() {{
  docker logout >/dev/null 2>&1 || true
  rm -rf /root/.docker /run/loom-ci-registry
}}
trap cleanup_registry_auth EXIT
python3 -c \
  'import json; print(json.load(open("/run/loom-ci-registry/{DOCKERHUB_CREDENTIALS_NAME}"))["token"])' \
  | docker login --username "${{dockerhub_username}}" --password-stdin >/dev/null
unset dockerhub_username
for image in {base_images}; do
  repository="${{image%:*}}"
  tag="${{image##*:}}"
  if [[ "$repository" != */* ]]; then
    repository="library/${{repository}}"
  fi
  skopeo copy \
    --all \
    --src-authfile /root/.docker/config.json \
    --dest-tls-verify=false \
    "docker://docker.io/${{repository}}:${{tag}}" \
    "docker://127.0.0.1:5000/${{repository}}:${{tag}}" \
    >/dev/null
done
cleanup_registry_auth
trap - EXIT
test ! -e /root/.docker/config.json
cat >/etc/docker/registry/config.yml <<'EOF'
version: 0.1
log:
  fields:
    service: loom-ci-registry
storage:
  filesystem:
    rootdirectory: /var/lib/loom-ci-registry
  maintenance:
    readonly:
      enabled: true
http:
  addr: 0.0.0.0:5000
EOF
systemctl restart docker-registry
wait_for_registry
cat >/etc/docker/daemon.json <<'EOF'
{{
  "insecure-registries": ["127.0.0.1:5000"],
  "registry-mirrors": ["http://127.0.0.1:5000"]
}}
EOF
install -d -m 0755 /etc/buildkit
cat >/etc/buildkit/loom-ci.toml <<'EOF'
[registry."docker.io"]
  mirrors = ["127.0.0.1:5000"]
  http = true
  insecure = true
EOF
systemctl restart docker
for image in {base_images}; do
  repository="${{image%:*}}"
  tag="${{image##*:}}"
  if [[ "$repository" != */* ]]; then
    repository="library/${{repository}}"
  fi
  registry_ref="127.0.0.1:5000/${{repository}}:${{tag}}"
  raw_manifest=$(
    skopeo inspect \
      --raw \
      --tls-verify=false \
      "docker://${{registry_ref}}"
  )
  media_type=$(jq -r '.mediaType // ""' <<<"${{raw_manifest}}")
  case "${{media_type}}" in
    application/vnd.docker.distribution.manifest.list.v2+json|\
    application/vnd.oci.image.index.v1+json)
      platform_digest=$(
        jq -er '
          first(
            .manifests[]
            | select(
                .platform.os == "linux"
                and .platform.architecture == "amd64"
              )
            | .digest
          )
        ' <<<"${{raw_manifest}}"
      )
      source_ref="127.0.0.1:5000/${{repository}}@${{platform_digest}}"
      ;;
    *)
      source_ref="${{registry_ref}}"
      ;;
  esac
  unset raw_manifest media_type platform_digest
  docker pull "${{source_ref}}" >/dev/null
  docker tag "${{source_ref}}" "${{image}}"
  docker image inspect "${{image}}" >/dev/null
done
test "$(systemctl is-active docker-registry)" = active
id runner >/dev/null 2>&1 || useradd --create-home --shell /bin/bash runner
usermod -aG docker runner
printf 'runner ALL=(ALL) NOPASSWD:ALL\\n' >/etc/sudoers.d/runner
chmod 0440 /etc/sudoers.d/runner
mkdir -p /mnt/loom-ci-assets /opt/actions-runner /opt/loom-ci-assets
printf '{runner_sha}  /mnt/loom-ci-assets/{profile.actions_runner_name}\\n' | sha256sum -c -
tar -xzf /mnt/loom-ci-assets/{profile.actions_runner_name} -C /opt/actions-runner
printf '{UV_ARCHIVE_SHA256}  /mnt/loom-ci-assets/{UV_ARCHIVE_NAME}\\n' | sha256sum -c -
install -m 0644 \
  /mnt/loom-ci-assets/{UV_ARCHIVE_NAME} \
  /opt/loom-ci-assets/{UV_ARCHIVE_NAME}
cat >/opt/loom-ci-assets/{UV_MANIFEST_NAME} <<'EOF'
{uv_manifest}
EOF
chmod 0644 /opt/loom-ci-assets/{UV_MANIFEST_NAME}
umount /mnt/loom-ci-assets
cat >/etc/systemd/system/loom-ci-uv-assets.service <<'EOF'
[Unit]
Description=Loom CI pinned uv assets
After=network.target

[Service]
Type=simple
DynamicUser=yes
ExecStart=/usr/bin/python3 -m http.server {UV_ASSET_PORT} --bind 127.0.0.1 --directory /opt/loom-ci-assets
Restart=on-failure
NoNewPrivileges=yes
PrivateTmp=yes
ProtectHome=yes
ProtectSystem=strict
CapabilityBoundingSet=
RestrictAddressFamilies=AF_INET AF_INET6

[Install]
WantedBy=multi-user.target
EOF
systemctl daemon-reload
systemctl enable --now loom-ci-uv-assets.service
uv_assets_ready=false
for _attempt in $(seq 1 30); do
  if curl --fail --silent {UV_MANIFEST_URL} \
    | jq -e --arg version '{UV_VERSION}' '.version == $version' >/dev/null; then
    uv_assets_ready=true
    break
  fi
  sleep 1
done
if [[ "$uv_assets_ready" != true ]]; then
  systemctl status --no-pager loom-ci-uv-assets.service >&2 || true
  journalctl --no-pager -u loom-ci-uv-assets.service -n 50 >&2 || true
  exit 1
fi
unset uv_assets_ready
/opt/actions-runner/bin/installdependencies.sh
chown -R runner:runner /opt/actions-runner
sudo -u runner docker version
sudo -u runner docker buildx version
sudo -u runner docker compose version
sudo -u runner python --version
test -x /usr/bin/cc
install -d -o runner -g runner -m 0700 /home/runner/_work
apt-get clean
rm -rf /var/lib/apt/lists/* /tmp/*
truncate -s 0 /etc/machine-id
rm -f /var/lib/dbus/machine-id
cloud-init clean --logs --seed
sync
echo LOOM_CI_BASE_READY >/dev/ttyS0
poweroff
"""


def _slot_run_script() -> str:
    return """#!/usr/bin/env bash
set -euo pipefail
umask 0022
install -d -m 0700 /run/loom-ci-jit
mount -o ro LABEL=LOOMCIJIT /mnt
install -m 0600 /mnt/jitconfig /run/loom-ci-jit/jitconfig
umount /mnt
install -o runner -g runner -m 0600 /dev/null /var/log/loom-actions-runner.log
systemctl is-active --quiet docker
set +e
sudo -u runner /bin/bash -c '
  set -euo pipefail
  umask 0022
  exec /opt/actions-runner/run.sh --jitconfig "$1"
' loom-ci-runner "$(cat /run/loom-ci-jit/jitconfig)" \
  >/var/log/loom-actions-runner.log 2>&1
runner_rc=$?
set -e
shred -u /run/loom-ci-jit/jitconfig
truncate -s 0 /var/log/loom-actions-runner.log
sync
echo "LOOM_CI_RUNNER_EXIT rc=${runner_rc}" >/dev/ttyS0
poweroff
"""


def _agent_sandbox_benchmark_script(candidate_sha: str, vcpus: int) -> str:
    return f"""#!/usr/bin/env bash
set -euo pipefail
started=$(date +%s)
finish() {{
  rc=$?
  finished=$(date +%s)
  elapsed=$((finished - started))
  echo "LOOM_CI_BENCHMARK_RESULT vcpus={vcpus} seconds=${{elapsed}} rc=${{rc}}" \
    >/dev/ttyS0
  sync
  poweroff
}}
trap finish EXIT
exec >/var/log/loom-ci-agent-sandbox-benchmark.log 2>&1
git clone --filter=blob:none https://github.com/{EXPECTED_REPOSITORY}.git /opt/loom
git -C /opt/loom checkout --detach {candidate_sha}
docker run --privileged --rm tonistiigi/binfmt --install all
buildx_args=(
  docker buildx create
  --name loom-benchmark
  --use
  --bootstrap
)
if [[ -r /etc/buildkit/loom-ci.toml ]]; then
  buildx_args+=(
    --driver-opt network=host
    --buildkitd-config /etc/buildkit/loom-ci.toml
  )
fi
"${{buildx_args[@]}}"
docker buildx build \
  --platform linux/amd64,linux/arm64 \
  --file /opt/loom/deploy/Dockerfile.agent-sandbox \
  --tag loom-ci-agent-sandbox:benchmark \
  --provenance=false \
  --build-arg LOOM_BUILD_SHA={candidate_sha} \
  /opt/loom
"""


def _cloud_config(script_path: str, script: str) -> str:
    payload = {
        "ssh_pwauth": False,
        "disable_root": True,
        "write_files": [
            {
                "path": script_path,
                "permissions": "0700",
                "owner": "root:root",
                "encoding": "b64",
                "content": base64.b64encode(script.encode()).decode(),
            },
        ],
        "runcmd": [[script_path]],
    }
    return "#cloud-config\n" + json.dumps(payload, separators=(",", ":")) + "\n"


def _meta_data(instance_id: str) -> str:
    return json.dumps(
        {"instance-id": instance_id, "local-hostname": instance_id},
        separators=(",", ":"),
    ) + "\n"


def _slot_root(profile: PoolProfile, slot: int) -> Path:
    return profile.state_root / f"slot-{slot:02d}"


def _slot_state_path(profile: PoolProfile, slot: int) -> Path:
    return _slot_root(profile, slot) / "state.json"


def _load_slot_state(path: Path) -> SlotState:
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
        state = SlotState(**raw)
    except (OSError, json.JSONDecodeError, TypeError) as exc:
        raise PoolOperationError(f"invalid slot state: {path.name}") from exc
    if state.schema_version != STATE_SCHEMA_VERSION:
        raise PoolOperationError(f"unsupported slot state: {path.name}")
    return state


def _existing_states(profile: PoolProfile) -> dict[int, SlotState]:
    states: dict[int, SlotState] = {}
    if not profile.state_root.exists():
        return states
    for path in sorted(profile.state_root.glob("slot-*/state.json")):
        state = _load_slot_state(path)
        if not 0 <= state.slot < profile.slots or state.slot in states:
            raise PoolOperationError("slot state identity is invalid or duplicated")
        states[state.slot] = state
    return states


def _container_running(
    runner: CommandRunner,
    container_name: str,
) -> bool:
    result = runner.run(
        ("docker", "inspect", "--format", "{{.State.Running}}", container_name),
        timeout=20,
    )
    return result.returncode == 0 and result.stdout.strip() == "true"


def _remove_container(runner: CommandRunner, container_name: str) -> None:
    result = runner.run(("docker", "rm", "-f", container_name), timeout=60)
    if result.returncode != 0 and "No such container" not in result.stderr:
        raise PoolOperationError("runner container cleanup failed")


def _safe_remove_slot(profile: PoolProfile, slot: int) -> None:
    root = _slot_root(profile, slot)
    if root.parent != profile.state_root or root.name != f"slot-{slot:02d}":
        raise PoolOperationError("refusing unsafe slot cleanup target")
    if root.exists():
        shutil.rmtree(root)


def _runner_inventory(
    api: GitHubRunnerAPI,
    profile: PoolProfile,
) -> dict[int, dict[str, Any]]:
    inventory: dict[int, dict[str, Any]] = {}
    for runner in api.list_runners():
        name = runner.get("name")
        runner_id = runner.get("id")
        if (
            isinstance(name, str)
            and name.startswith(f"{profile.runner_name_prefix}-")
            and type(runner_id) is int
        ):
            inventory[runner_id] = runner
    return inventory


def _runner_labels(record: Mapping[str, Any] | None) -> set[str]:
    if record is None:
        return set()
    raw_labels = record.get("labels")
    if not isinstance(raw_labels, Sequence) or isinstance(raw_labels, (str, bytes)):
        return set()
    labels: set[str] = set()
    for item in raw_labels:
        if isinstance(item, Mapping) and isinstance(item.get("name"), str):
            labels.add(item["name"])
    return labels


def _verify_candidate_sha(candidate_sha: str) -> None:
    if _GIT_SHA_RE.fullmatch(candidate_sha) is None:
        raise PoolConfigError("candidate_sha must be a full lowercase Git SHA")


def _verify_host(profile: PoolProfile) -> None:
    actual = socket.gethostname().split(".", 1)[0].lower()
    if actual != profile.expected_hostname:
        raise PoolOperationError(
            f"execution host must be {profile.expected_hostname}, got {actual}",
        )
    if not Path("/dev/kvm").exists():
        raise PoolOperationError("/dev/kvm is unavailable")


def _verify_qemu_image(
    profile: PoolProfile,
    candidate_sha: str,
    runner: CommandRunner,
) -> None:
    output = _require_success(
        runner,
        (
            "docker",
            "image",
            "inspect",
            "--format",
            '{{ index .Config.Labels "loom.candidate.sha" }}',
            profile.qemu_image,
        ),
        timeout=20,
        purpose="QEMU image inspection",
    ).strip()
    if output != candidate_sha:
        raise PoolOperationError("QEMU image is not bound to the requested candidate")


def prepare_base(
    profile: PoolProfile,
    *,
    candidate_sha: str,
    execute: bool,
    dockerhub_credentials: DockerHubCredentials | None,
    runner: CommandRunner,
) -> dict[str, object]:
    _verify_candidate_sha(candidate_sha)
    plan: dict[str, object] = {
        "operation": "prepare-base",
        "mutation_authorized": execute,
        "cloud_image": profile.cloud_image_name,
        "actions_runner": profile.actions_runner_name,
        "uv_archive": UV_ARCHIVE_NAME,
        "golden_image": str(profile.golden_image),
    }
    if not execute:
        return plan
    if dockerhub_credentials is None:
        raise PoolOperationError(
            "Docker Hub credentials are required to prepare the golden guest",
        )

    _verify_host(profile)
    _verify_qemu_image(profile, candidate_sha, runner)
    profile.cache_root.mkdir(parents=True, exist_ok=True, mode=0o700)
    profile.state_root.mkdir(parents=True, exist_ok=True, mode=0o700)
    cloud_image = profile.cache_root / profile.cloud_image_name
    actions_runner = profile.cache_root / profile.actions_runner_name
    uv_archive = profile.cache_root / UV_ARCHIVE_NAME
    _download_pinned(profile.cloud_image_url, cloud_image, profile.cloud_image_sha256)
    _download_pinned(
        profile.actions_runner_url,
        actions_runner,
        profile.actions_runner_sha256,
    )
    _download_pinned(UV_ARCHIVE_URL, uv_archive, UV_ARCHIVE_SHA256)

    build_root = profile.state_root / f".base-build-{uuid.uuid4().hex}"
    build_root.mkdir(mode=0o700)
    container_name = f"loom-ci-base-build-{uuid.uuid4().hex[:12]}"
    temporary_golden = profile.cache_root / f".{GOLDEN_IMAGE_NAME}.{uuid.uuid4().hex}"
    try:
        _require_success(
            runner,
            _docker_utility_command(
                profile,
                state_mount=build_root,
                cache_read_only=False,
                args=(
                    "qemu-img",
                    "create",
                    "-f",
                    "qcow2",
                    "-F",
                    "qcow2",
                    "-b",
                    f"/cache/{profile.cloud_image_name}",
                    f"/cache/{temporary_golden.name}",
                    f"{profile.disk_gib_per_slot}G",
                ),
            ),
            timeout=60,
            purpose="golden overlay creation",
        )
        user_data = build_root / "user-data"
        meta_data = build_root / "meta-data"
        user_data.write_text(
            _cloud_config(
                "/usr/local/sbin/loom-ci-prepare-base",
                _base_install_script(profile),
            ),
            encoding="utf-8",
        )
        meta_data.write_text(_meta_data("loom-ci-golden"), encoding="utf-8")
        for path in (user_data, meta_data):
            path.chmod(0o600)
        dockerhub_credentials_path = build_root / DOCKERHUB_CREDENTIALS_NAME
        _write_private(
            dockerhub_credentials_path,
            json.dumps(
                {
                    "username": dockerhub_credentials.username,
                    "token": dockerhub_credentials.token,
                },
                separators=(",", ":"),
            ),
        )

        _require_success(
            runner,
            _docker_utility_command(
                profile,
                state_mount=build_root,
                args=(
                    "cloud-localds",
                    "/state/seed.iso",
                    "/state/user-data",
                    "/state/meta-data",
                ),
            ),
            timeout=60,
            purpose="golden cloud-init seed creation",
        )
        _require_success(
            runner,
            _docker_utility_command(
                profile,
                state_mount=build_root,
                args=(
                    "genisoimage",
                    "-quiet",
                    "-graft-points",
                    "-R",
                    "-o",
                    "/state/assets.iso",
                    "-V",
                    "LOOMCIASSETS",
                    (
                        f"{profile.actions_runner_name}="
                        f"/cache/{profile.actions_runner_name}"
                    ),
                    f"{UV_ARCHIVE_NAME}=/cache/{UV_ARCHIVE_NAME}",
                    (
                        f"{DOCKERHUB_CREDENTIALS_NAME}="
                        f"/state/{DOCKERHUB_CREDENTIALS_NAME}"
                    ),
                ),
            ),
            timeout=60,
            purpose="golden runner asset ISO creation",
        )
        qemu = (
            "docker",
            "run",
            "--rm",
            "--dns",
            "1.1.1.1",
            "--dns",
            "8.8.8.8",
            "--name",
            container_name,
            "--read-only",
            "--tmpfs",
            "/tmp:rw,nosuid,nodev,noexec,size=256m",
            "--tmpfs",
            "/run:rw,nosuid,nodev,size=16m",
            "--device",
            "/dev/kvm",
            "--cap-drop",
            "ALL",
            "--cap-add",
            "NET_ADMIN",
            "--cap-add",
            "SETPCAP",
            "--security-opt",
            "no-new-privileges",
            "--cpus",
            "2",
            "--memory",
            "4608m",
            "--mount",
            f"type=bind,src={build_root},dst=/state",
            "--mount",
            f"type=bind,src={profile.cache_root},dst=/cache",
            profile.qemu_image,
            "qemu-system-x86_64",
            "-accel",
            "kvm",
            "-cpu",
            "host",
            "-smp",
            "2",
            "-m",
            "4096",
            "-drive",
            f"if=virtio,format=qcow2,file=/cache/{temporary_golden.name},cache=none",
            "-drive",
            "if=virtio,format=raw,file=/state/seed.iso,readonly=on",
            "-drive",
            "if=virtio,format=raw,file=/state/assets.iso,readonly=on",
            "-netdev",
            "user,id=net0",
            "-device",
            "virtio-net-pci,netdev=net0",
            "-display",
            "none",
            "-serial",
            "file:/state/serial.log",
            "-no-reboot",
        )
        _require_success(
            runner,
            qemu,
            timeout=1800,
            purpose="golden guest preparation",
        )
        serial = (build_root / "serial.log").read_text(
            encoding="utf-8",
            errors="replace",
        )
        if "LOOM_CI_BASE_READY" not in serial:
            raise PoolOperationError("golden guest did not emit readiness evidence")
        _require_success(
            runner,
            _docker_utility_command(
                profile,
                state_mount=build_root,
                cache_read_only=False,
                args=("qemu-img", "check", f"/cache/{temporary_golden.name}"),
            ),
            timeout=300,
            purpose="golden image integrity check",
        )
        golden_sha = _sha256(temporary_golden)
        os.replace(temporary_golden, profile.golden_image)
        _atomic_json(
            profile.golden_manifest,
            {
                "schema_version": 1,
                "candidate_sha": candidate_sha,
                "cloud_image_sha256": profile.cloud_image_sha256,
                "actions_runner_sha256": profile.actions_runner_sha256,
                "uv_version": UV_VERSION,
                "uv_archive_sha256": UV_ARCHIVE_SHA256,
                "base_images": list(GUEST_BASE_IMAGES),
                "golden_sha256": golden_sha,
                "created_at": datetime.now(UTC).isoformat(),
            },
        )
        plan["status"] = "prepared"
        plan["golden_sha256"] = golden_sha
        return plan
    finally:
        _remove_container(runner, container_name)
        temporary_golden.unlink(missing_ok=True)
        if build_root.exists():
            shutil.rmtree(build_root)


def _verify_golden(profile: PoolProfile, candidate_sha: str) -> None:
    try:
        manifest = json.loads(profile.golden_manifest.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise PoolOperationError("golden image manifest is unavailable") from exc
    expected = {
        "candidate_sha": candidate_sha,
        "cloud_image_sha256": profile.cloud_image_sha256,
        "actions_runner_sha256": profile.actions_runner_sha256,
        "uv_version": UV_VERSION,
        "uv_archive_sha256": UV_ARCHIVE_SHA256,
        "base_images": list(GUEST_BASE_IMAGES),
    }
    if any(manifest.get(key) != value for key, value in expected.items()):
        raise PoolOperationError("golden image manifest does not match the candidate")
    golden_sha = manifest.get("golden_sha256")
    if not isinstance(golden_sha, str) or _SHA256_RE.fullmatch(golden_sha) is None:
        raise PoolOperationError("golden image manifest digest is invalid")
    if not profile.golden_image.is_file() or _sha256(profile.golden_image) != golden_sha:
        raise PoolOperationError("golden image bytes do not match the manifest")


def benchmark_agent_sandbox(
    profile: PoolProfile,
    *,
    candidate_sha: str,
    vcpus: int,
    execute: bool,
    runner: CommandRunner,
) -> dict[str, object]:
    _verify_candidate_sha(candidate_sha)
    if vcpus not in (2, 8):
        raise PoolConfigError("benchmark vcpus must be 2 or 8")
    plan: dict[str, object] = {
        "operation": "benchmark-agent-sandbox",
        "mutation_authorized": execute,
        "candidate_sha": candidate_sha,
        "vcpus": vcpus,
        "memory_mib": profile.memory_mib_per_slot,
    }
    if not execute:
        return plan

    _verify_host(profile)
    _verify_qemu_image(profile, candidate_sha, runner)
    _verify_golden(profile, candidate_sha)
    profile.state_root.mkdir(parents=True, exist_ok=True, mode=0o700)
    benchmark_root = profile.state_root / (
        f".benchmark-agent-sandbox-{vcpus}-{uuid.uuid4().hex}"
    )
    benchmark_root.mkdir(mode=0o700)
    container_name = f"loom-ci-benchmark-{vcpus}-{uuid.uuid4().hex[:12]}"
    try:
        _require_success(
            runner,
            _docker_utility_command(
                profile,
                state_mount=benchmark_root,
                args=(
                    "qemu-img",
                    "create",
                    "-f",
                    "qcow2",
                    "-F",
                    "qcow2",
                    "-b",
                    f"/cache/{GOLDEN_IMAGE_NAME}",
                    "/state/root.qcow2",
                    f"{profile.disk_gib_per_slot}G",
                ),
            ),
            timeout=60,
            purpose="benchmark overlay creation",
        )
        (benchmark_root / "user-data").write_text(
            _cloud_config(
                "/usr/local/sbin/loom-ci-benchmark-agent-sandbox",
                _agent_sandbox_benchmark_script(candidate_sha, vcpus),
            ),
            encoding="utf-8",
        )
        (benchmark_root / "meta-data").write_text(
            _meta_data(f"loom-ci-benchmark-{vcpus}"),
            encoding="utf-8",
        )
        for path in (benchmark_root / "user-data", benchmark_root / "meta-data"):
            path.chmod(0o600)
        _require_success(
            runner,
            _docker_utility_command(
                profile,
                state_mount=benchmark_root,
                args=(
                    "cloud-localds",
                    "/state/seed.iso",
                    "/state/user-data",
                    "/state/meta-data",
                ),
            ),
            timeout=60,
            purpose="benchmark cloud-init seed creation",
        )
        qemu = (
            "docker",
            "run",
            "--rm",
            "--dns",
            "1.1.1.1",
            "--dns",
            "8.8.8.8",
            "--name",
            container_name,
            "--read-only",
            "--tmpfs",
            "/tmp:rw,nosuid,nodev,noexec,size=256m",
            "--tmpfs",
            "/run:rw,nosuid,nodev,size=16m",
            "--device",
            "/dev/kvm",
            "--cap-drop",
            "ALL",
            "--cap-add",
            "NET_ADMIN",
            "--cap-add",
            "SETPCAP",
            "--security-opt",
            "no-new-privileges",
            "--cpus",
            str(vcpus),
            "--memory",
            f"{profile.memory_mib_per_slot + 512}m",
            "--mount",
            f"type=bind,src={benchmark_root},dst=/state",
            "--mount",
            f"type=bind,src={profile.cache_root},dst=/cache,readonly",
            profile.qemu_image,
            "qemu-system-x86_64",
            "-accel",
            "kvm",
            "-cpu",
            "host",
            "-smp",
            str(vcpus),
            "-m",
            str(profile.memory_mib_per_slot),
            "-drive",
            "if=virtio,format=qcow2,file=/state/root.qcow2,cache=none",
            "-drive",
            "if=virtio,format=raw,file=/state/seed.iso,readonly=on",
            "-netdev",
            "user,id=net0",
            "-device",
            "virtio-net-pci,netdev=net0",
            "-display",
            "none",
            "-serial",
            "file:/state/serial.log",
            "-no-reboot",
        )
        _require_success(
            runner,
            qemu,
            timeout=2700,
            purpose="agent-sandbox benchmark guest",
        )
        serial = (benchmark_root / "serial.log").read_text(
            encoding="utf-8",
            errors="replace",
        )
        matches = re.findall(
            r"LOOM_CI_BENCHMARK_RESULT vcpus=(\d+) seconds=(\d+) rc=(\d+)",
            serial,
        )
        if len(matches) != 1:
            raise PoolOperationError("benchmark guest did not emit one result")
        measured_vcpus, seconds, returncode = (int(value) for value in matches[0])
        if measured_vcpus != vcpus or returncode != 0:
            raise PoolOperationError("agent-sandbox benchmark failed")
        plan.update({"status": "pass", "seconds": seconds})
        return plan
    finally:
        _remove_container(runner, container_name)
        if benchmark_root.exists():
            shutil.rmtree(benchmark_root)


def _create_slot(
    profile: PoolProfile,
    *,
    slot: int,
    candidate_sha: str,
    api: GitHubRunnerAPI,
    runner: CommandRunner,
) -> SlotState:
    slot_root = _slot_root(profile, slot)
    if slot_root.exists():
        raise PoolOperationError(f"slot {slot} already has local state")
    slot_root.mkdir(parents=True, mode=0o700)
    work_class = profile.work_class_for_slot(slot)
    nonce = uuid.uuid4().hex[:12]
    runner_name = f"{profile.runner_name_prefix}-{work_class.name}-{slot:02d}-{nonce}"
    container_name = f"loom-ci-runner-{slot:02d}-{nonce}"
    runner_id: int | None = None
    try:
        runner_id, jit_config = api.generate_jit_config(
            name=runner_name,
            labels=profile.labels_for_slot(slot),
        )
        _require_success(
            runner,
            _docker_utility_command(
                profile,
                state_mount=slot_root,
                args=(
                    "qemu-img",
                    "create",
                    "-f",
                    "qcow2",
                    "-F",
                    "qcow2",
                    "-b",
                    f"/cache/{GOLDEN_IMAGE_NAME}",
                    "/state/root.qcow2",
                    f"{profile.disk_gib_per_slot}G",
                ),
            ),
            timeout=60,
            purpose="slot overlay creation",
        )
        (slot_root / "user-data").write_text(
            _cloud_config(
                "/usr/local/sbin/loom-ci-run-once",
                _slot_run_script(),
            ),
            encoding="utf-8",
        )
        (slot_root / "meta-data").write_text(
            _meta_data(runner_name),
            encoding="utf-8",
        )
        for path in (slot_root / "user-data", slot_root / "meta-data"):
            path.chmod(0o600)
        jit_path = slot_root / "jitconfig"
        _write_private(jit_path, jit_config)
        _require_success(
            runner,
            _docker_utility_command(
                profile,
                state_mount=slot_root,
                args=(
                    "cloud-localds",
                    "/state/seed.iso",
                    "/state/user-data",
                    "/state/meta-data",
                ),
            ),
            timeout=60,
            purpose="slot cloud-init seed creation",
        )
        _require_success(
            runner,
            _docker_utility_command(
                profile,
                state_mount=slot_root,
                args=(
                    "genisoimage",
                    "-quiet",
                    "-graft-points",
                    "-R",
                    "-o",
                    "/state/jit.iso",
                    "-V",
                    "LOOMCIJIT",
                    "jitconfig=/state/jitconfig",
                ),
            ),
            timeout=60,
            purpose="slot JIT ISO creation",
        )
        jit_path.unlink(missing_ok=True)
        (slot_root / "jit.iso").chmod(0o600)
        state = SlotState(
            schema_version=STATE_SCHEMA_VERSION,
            slot=slot,
            runner_id=runner_id,
            runner_name=runner_name,
            container_name=container_name,
            created_at=datetime.now(UTC).isoformat(),
            candidate_sha=candidate_sha,
        )
        _atomic_json(_slot_state_path(profile, slot), asdict(state))
        _require_success(
            runner,
            _qemu_container_command(
                profile,
                slot=slot,
                slot_root=slot_root,
                container_name=container_name,
                detach=True,
            ),
            timeout=60,
            purpose="slot VM launch",
        )
        return state
    except BaseException:
        (slot_root / "jitconfig").unlink(missing_ok=True)
        _remove_container(runner, container_name)
        if runner_id is not None:
            try:
                api.delete_runner(runner_id)
            except PoolOperationError:
                pass
        _safe_remove_slot(profile, slot)
        raise


def _runner_busy(record: Mapping[str, Any] | None) -> bool:
    return bool(record and record.get("busy") is True)


def reconcile(
    profile: PoolProfile,
    *,
    candidate_sha: str,
    execute: bool,
    api: GitHubRunnerAPI | None,
    runner: CommandRunner,
) -> dict[str, object]:
    _verify_candidate_sha(candidate_sha)
    states = _existing_states(profile)
    plan: dict[str, object] = {
        "operation": "reconcile",
        "mutation_authorized": execute,
        "target_slots": profile.slots,
        "existing_slots": sorted(states),
        "create_slots": [slot for slot in range(profile.slots) if slot not in states],
        "work_classes": {
            work_class.name: {
                "label": work_class.label,
                "target_slots": work_class.slots,
                "routing_variable": work_class.routing_variable,
                "hosted_overflow_after_seconds": (
                    work_class.hosted_overflow_after_seconds
                ),
            }
            for work_class in profile.work_classes
        },
    }
    if not execute:
        return plan
    if api is None:
        raise PoolOperationError("GitHub API credentials are required")
    _verify_host(profile)
    _verify_qemu_image(profile, candidate_sha, runner)
    _verify_golden(profile, candidate_sha)
    profile.state_root.mkdir(parents=True, exist_ok=True, mode=0o700)
    inventory = _runner_inventory(api, profile)

    cleaned: list[int] = []
    for slot, state in list(states.items()):
        if state.candidate_sha != candidate_sha:
            raise PoolOperationError("existing slot belongs to another candidate")
        if _container_running(runner, state.container_name):
            record = inventory.get(state.runner_id)
            if record is not None and record.get("status") == "online":
                expected_label = profile.work_class_for_slot(slot).label
                if expected_label not in _runner_labels(record):
                    if _runner_busy(record):
                        raise PoolOperationError(
                            "busy runner does not match its reserved work class",
                        )
                    _remove_container(runner, state.container_name)
                    api.delete_runner(state.runner_id)
                    _safe_remove_slot(profile, slot)
                    del states[slot]
                    cleaned.append(slot)
                    continue
                (_slot_root(profile, slot) / "jit.iso").unlink(missing_ok=True)
            continue
        record = inventory.get(state.runner_id)
        if _runner_busy(record):
            raise PoolOperationError("dead local slot is still busy in GitHub inventory")
        if record is not None:
            api.delete_runner(state.runner_id)
        _remove_container(runner, state.container_name)
        _safe_remove_slot(profile, slot)
        del states[slot]
        cleaned.append(slot)

    created: list[int] = []
    for slot in range(profile.slots):
        if slot in states:
            continue
        states[slot] = _create_slot(
            profile,
            slot=slot,
            candidate_sha=candidate_sha,
            api=api,
            runner=runner,
        )
        created.append(slot)
    plan.update(
        {
            "status": "reconciled",
            "cleaned_slots": cleaned,
            "created_slots": created,
            "active_slots": sorted(states),
        },
    )
    return plan


def drain(
    profile: PoolProfile,
    *,
    execute: bool,
    api: GitHubRunnerAPI | None,
    runner: CommandRunner,
) -> dict[str, object]:
    states = _existing_states(profile)
    plan: dict[str, object] = {
        "operation": "drain",
        "mutation_authorized": execute,
        "existing_slots": sorted(states),
    }
    if not execute:
        return plan
    if api is None:
        raise PoolOperationError("GitHub API credentials are required")
    active_routing_variables = [
        name for name in ROUTING_VARIABLES if api.routing_variable_present(name)
    ]
    if active_routing_variables:
        raise PoolOperationError(
            "delete repository routing variables before draining: "
            + ", ".join(active_routing_variables),
        )
    inventory = _runner_inventory(api, profile)
    busy: list[int] = []
    removed: list[int] = []
    for slot, state in states.items():
        record = inventory.get(state.runner_id)
        if _runner_busy(record):
            busy.append(slot)
            continue
        _remove_container(runner, state.container_name)
        if record is not None:
            api.delete_runner(state.runner_id)
        _safe_remove_slot(profile, slot)
        removed.append(slot)
    plan.update(
        {
            "status": "draining" if busy else "drained",
            "busy_slots": busy,
            "removed_slots": removed,
        },
    )
    return plan


def status(
    profile: PoolProfile,
    *,
    api: GitHubRunnerAPI | None,
    runner: CommandRunner,
) -> dict[str, object]:
    states = _existing_states(profile)
    inventory = _runner_inventory(api, profile) if api is not None else {}
    work_classes: dict[str, dict[str, object]] = {
        work_class.name: {
            "label": work_class.label,
            "target_slots": work_class.slots,
            "ready_slots": 0,
            "busy_slots": 0,
            "routing_variable": work_class.routing_variable,
            "hosted_overflow_after_seconds": (
                work_class.hosted_overflow_after_seconds
            ),
        }
        for work_class in profile.work_classes
    }
    slots: list[dict[str, object]] = []
    for slot, state in states.items():
        record = inventory.get(state.runner_id)
        work_class = profile.work_class_for_slot(slot)
        container_running = _container_running(runner, state.container_name)
        labels_match = work_class.label in _runner_labels(record)
        ready = bool(
            container_running
            and labels_match
            and record
            and record.get("status") == "online"
        )
        busy = bool(record and record.get("busy") is True)
        if ready:
            work_classes[work_class.name]["ready_slots"] += 1
        if busy:
            work_classes[work_class.name]["busy_slots"] += 1
        slots.append(
            {
                "slot": slot,
                "work_class": work_class.name,
                "work_class_label": work_class.label,
                "runner_name": state.runner_name,
                "container_running": container_running,
                "github_status": record.get("status") if record else "absent",
                "github_busy": busy,
                "labels_match": labels_match,
            },
        )
    route_presence = (
        {name: api.routing_variable_present(name) for name in ROUTING_VARIABLES}
        if api is not None
        else None
    )
    ready = sum(
        1
        for item in slots
        if item["container_running"]
        and item["github_status"] == "online"
        and item["labels_match"]
    )
    return {
        "operation": "status",
        "target_slots": profile.slots,
        "ready_slots": ready,
        "routing_variable_present": (
            route_presence[ROUTING_VARIABLE] if route_presence is not None else None
        ),
        "routing_variables_present": route_presence,
        "work_classes": work_classes,
        "slots": slots,
        "healthy": ready == profile.slots,
    }


def preflight(
    profile: PoolProfile,
    *,
    candidate_sha: str,
    execute: bool,
    runner: CommandRunner,
) -> dict[str, object]:
    _verify_candidate_sha(candidate_sha)
    report: dict[str, object] = {
        "operation": "preflight",
        "mutation_authorized": False,
        "expected_hostname": profile.expected_hostname,
        "target_slots": profile.slots,
        "guest_vcpus_per_slot": profile.vcpus_per_slot,
        "total_guest_vcpus": profile.total_guest_vcpus,
        "host_cpu_budget": profile.host_cpu_budget,
        "total_memory_mib": profile.total_memory_mib,
        "host_memory_budget_mib": profile.host_memory_budget_mib,
    }
    if not execute:
        return report
    _verify_host(profile)
    _require_success(
        runner,
        ("docker", "version", "--format", "{{.Server.Version}}"),
        timeout=20,
        purpose="Docker daemon preflight",
    )
    cgroup_driver = _require_success(
        runner,
        ("docker", "info", "--format", "{{.CgroupDriver}}"),
        timeout=20,
        purpose="Docker cgroup driver preflight",
    ).strip()
    if cgroup_driver != "systemd":
        raise PoolOperationError("Docker must use the systemd cgroup driver")
    _verify_qemu_image(profile, candidate_sha, runner)
    _require_success(
        runner,
        (
            "docker",
            "run",
            "--rm",
            "--device",
            "/dev/kvm",
            "--entrypoint",
            "/usr/bin/test",
            profile.qemu_image,
            "-c",
            "/dev/kvm",
        ),
        timeout=20,
        purpose="container KVM preflight",
    )
    report["status"] = "pass"
    return report


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--profile", type=Path, required=True)
    parser.add_argument("--candidate-sha", default="")
    parser.add_argument("--token-file", type=Path)
    parser.add_argument("--dockerhub-credentials-file", type=Path)
    subparsers = parser.add_subparsers(dest="operation", required=True)
    for name in ("preflight", "prepare-base", "reconcile", "drain"):
        subparser = subparsers.add_parser(name)
        subparser.add_argument("--execute", action="store_true")
    benchmark = subparsers.add_parser("benchmark-agent-sandbox")
    benchmark.add_argument("--vcpus", type=int, required=True)
    benchmark.add_argument("--execute", action="store_true")
    subparsers.add_parser("status")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        profile = load_profile(args.profile)
        runner = SubprocessCommandRunner()
        api: GitHubRunnerAPI | None = None
        if args.token_file is not None:
            api = GitHubAPI(
                repository=profile.repository,
                token=read_token(args.token_file),
            )
        if args.operation == "preflight":
            result = preflight(
                profile,
                candidate_sha=args.candidate_sha,
                execute=args.execute,
                runner=runner,
            )
        elif args.operation == "prepare-base":
            result = prepare_base(
                profile,
                candidate_sha=args.candidate_sha,
                execute=args.execute,
                dockerhub_credentials=(
                    read_dockerhub_credentials(args.dockerhub_credentials_file)
                    if args.dockerhub_credentials_file is not None
                    else None
                ),
                runner=runner,
            )
        elif args.operation == "reconcile":
            result = reconcile(
                profile,
                candidate_sha=args.candidate_sha,
                execute=args.execute,
                api=api,
                runner=runner,
            )
        elif args.operation == "benchmark-agent-sandbox":
            result = benchmark_agent_sandbox(
                profile,
                candidate_sha=args.candidate_sha,
                vcpus=args.vcpus,
                execute=args.execute,
                runner=runner,
            )
        elif args.operation == "drain":
            result = drain(
                profile,
                execute=args.execute,
                api=api,
                runner=runner,
            )
        else:
            result = status(profile, api=api, runner=runner)
    except (PoolConfigError, PoolOperationError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    json.dump(result, sys.stdout, sort_keys=True)
    sys.stdout.write("\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
