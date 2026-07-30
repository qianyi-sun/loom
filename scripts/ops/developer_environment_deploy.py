#!/usr/bin/env python3
"""Converge one registry-owned developer environment on the host.

The registry is the only allocator.  This program accepts an exact environment
and candidate identifier, verifies the root-published registry snapshot, and
uses only resources present in that snapshot.  Every mutating phase is
idempotent and journaled before the corresponding registry phase is advanced.
"""

from __future__ import annotations

import argparse
import base64
import fcntl
import grp
import hashlib
import json
import os
import platform
import pwd
import re
import secrets
import shutil
import stat
import subprocess
import sys
import tempfile
import urllib.error
import urllib.request
from collections.abc import Callable, Iterator, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Final, Protocol, cast

# The installed fixed program runs with Python isolated mode. Admit only the
# root-owned libexec module tree installed by the transactional authority
# installer; repository execution continues to use the normal package import.
_INSTALLED_MODULE_ROOT = Path("/usr/local/libexec")
_INSTALLED_REGISTRY_MODULE = (
    _INSTALLED_MODULE_ROOT / "scripts/ops/developer_environment_registry.py"
)
if __package__ in {None, ""} and _INSTALLED_REGISTRY_MODULE.is_file():
    try:
        module_root = _INSTALLED_MODULE_ROOT.lstat()
        scripts_root = (_INSTALLED_MODULE_ROOT / "scripts").lstat()
        ops_root = (_INSTALLED_MODULE_ROOT / "scripts/ops").lstat()
        registry_module = _INSTALLED_REGISTRY_MODULE.lstat()
    except OSError as exc:
        raise RuntimeError("installed developer environment module tree is unavailable") from exc
    if (
        any(
            not stat.S_ISDIR(metadata.st_mode) or metadata.st_uid != 0 or metadata.st_mode & 0o022
            for metadata in (module_root, scripts_root, ops_root)
        )
        or not stat.S_ISREG(registry_module.st_mode)
        or stat.S_ISLNK(registry_module.st_mode)
        or registry_module.st_uid != 0
        or registry_module.st_nlink != 1
        or registry_module.st_mode & 0o022
    ):
        raise RuntimeError("installed developer environment module tree is unsafe")
    sys.path.insert(0, str(_INSTALLED_MODULE_ROOT))

from scripts.ops import developer_environment_acceptance_probe as acceptance_probe  # noqa: E402
from scripts.ops import developer_environment_runtime_retire as runtime_retire  # noqa: E402
from scripts.ops import shared_capacity_runtime_host as capacity_runtime_host  # noqa: E402
from scripts.ops.developer_environment_registry import (  # noqa: E402
    CANDIDATE_ID_RE,
    DEPLOY_KIND,
    DEPLOY_PHASES,
    ENV_ID_RE,
    RUNTIME_ID_RE,
    SYSTEM_SNAPSHOT,
    DeploymentRecord,
    DeveloperEnvironmentRegistry,
    EnvironmentRecord,
    RegistryError,
    verify_worker_image_archive,
)

JOURNAL_KIND: Final = "loom.developer-environment.deployment-journal"
ROLLBACK_KIND: Final = "loom.developer-environment.rollback-operation"
FINALIZATION_KIND: Final = "loom.developer-environment.finalization-journal"
USABLE_KIND: Final = "loom.developer-environment.usable-receipt"
RETIRE_KIND: Final = "loom.developer-environment.retire-journal"
RETIRE_RECEIPT_KIND: Final = "loom.developer-environment.cleanup-receipt"
REVIVE_KIND: Final = "loom.developer-environment.revive-journal"
ADMISSION_INTENT_KIND: Final = "loom.developer-environment.admission-intent"
MANIFEST_KIND: Final = "loom.developer-environment.host-manifest"
RETIRE_LOCAL_OBJECTS: Final = (
    "local_preflight",
    "postgres_checkpoint",
    "control_plane_stop",
    "minio_stop",
    "compose_project",
    "container_absence",
    "systemd_unit",
    "postgres_volume",
    "minio_volume",
    "compose_network",
    "candidate_tree",
    "runtime_tree",
    "state_tree",
    "privileged_compose_inputs",
)
RETIRE_OBJECT_STATUSES: Final = frozenset(
    {
        "validated",
        "checkpointed",
        "stopped",
        "not-present",
        "removed",
        "disabled",
        "missing-after-authorized-retry",
        "not-applicable",
    }
)
JOURNAL_VERSION: Final = 1
MAX_SNAPSHOT_BYTES: Final = 16 * 1024 * 1024
MAX_BUNDLE_BYTES: Final = 256 * 1024 * 1024
EXPECTED_HOSTNAME: Final = "trt-eai-oldlab-2"
CAPACITY_ROOT: Final = Path("/var/lib/loom-developer-environment-capacity")
CAPACITY_PROGRAM: Final = Path("/usr/local/libexec/loom-developer-environment-capacity-authority")
RUNTIME_ROOT: Final = Path("/var/lib/loom-developer-environment-runtime")
RUNTIME_PROGRAM: Final = Path("/usr/local/libexec/loom-developer-environment-runtime-authority")
NODE_TRANSPORT: Final = Path("/usr/local/libexec/loom-developer-sandbox-node-transport")
DOMAIN_IDENTITY_NODES: Final = {
    "oldlab": ("oldlab-1",),
    "gb10": ("trt-gb10-1",),
}
DOMAIN_RUNTIME_NODES: Final = {
    "oldlab": tuple(f"oldlab-{index}" for index in range(1, 6)),
    "gb10": tuple(f"trt-gb10-{index}" for index in range(1, 16)),
}
COMPOSE_FILE: Final = Path("deploy/docker-compose.dev.yml")
LOOM_SERVICES: Final = (
    "llm-gateway",
    "control-plane",
    "loom-service",
    "worker",
    "egress-xds",
    "egress-proxy",
    "web",
)
ALL_SERVICES: Final = ("postgres", "minio", *LOOM_SERVICES)
LOCAL_BUILD_SERVICES: Final = (
    "llm-gateway",
    "control-plane",
    "loom-service",
    "egress-xds",
)
WORKER_REVISION_LABEL: Final = "org.opencontainers.image.revision"
WORKER_COMMAND: Final = ("python", "-m", "loom_worker")
SECRET_KEYS: Final = (
    "LOOM_DEV_POSTGRES_PASSWORD",
    "LOOM_DEV_MINIO_ROOT_PASSWORD",
    "LOOM_CP_STEP_JWT_SIGNING_KEY",
    "LOOM_SECRET_STORE_MASTER_KEY",
    "LOOM_WORKER_TOKEN",
)
SAFE_ID_RE: Final = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{15,127}$")
TERMINAL_JOB_STATES: Final = {
    "BOOT_FAIL",
    "CANCELLED",
    "COMPLETED",
    "DEADLINE",
    "FAILED",
    "NODE_FAIL",
    "OUT_OF_MEMORY",
    "PREEMPTED",
    "REVOKED",
    "SPECIAL_EXIT",
    "TIMEOUT",
}


class DeploymentError(RuntimeError):
    """A bounded error that never includes command output or secret values."""


class RegistryAuthority(Protocol):
    def reconcile_predeployment_ports(
        self,
        env_id: str,
        *,
        principal_id: str,
        expected_resource_generation: int,
    ) -> EnvironmentRecord: ...

    def registration_idempotency_replay(
        self,
        *,
        principal_id: str,
        idempotency_key: str,
    ) -> bool: ...

    def prepare_deployment_finalization(
        self,
        deployment_id: str,
        *,
        principal_id: str,
        expected_resource_generation: int,
    ) -> DeploymentRecord: ...

    def record_deployment_finalization(
        self,
        deployment_id: str,
        *,
        principal_id: str,
        expected_resource_generation: int,
        evidence: Mapping[str, str],
    ) -> DeploymentRecord: ...

    def record_worker_runtime_bindings(
        self,
        deployment_id: str,
        *,
        principal_id: str,
        expected_resource_generation: int,
        bindings: Mapping[str, Any],
    ) -> DeploymentRecord: ...

    def begin_retirement(
        self,
        env_id: str,
        *,
        principal_id: str,
        expected_resource_generation: int,
    ) -> object: ...

    def snapshot_bytes(self) -> bytes: ...

    def reconcile_snapshot(self) -> bytes: ...

    def begin_deployment(self, payload: Mapping[str, Any]) -> DeploymentRecord: ...

    def advance_deployment(
        self,
        deployment_id: str,
        *,
        principal_id: str,
        expected_phase: str,
        next_phase: str,
        expected_resource_generation: int,
    ) -> DeploymentRecord: ...

    def fail_deployment(
        self,
        deployment_id: str,
        *,
        principal_id: str,
        expected_phase: str,
        expected_resource_generation: int,
    ) -> DeploymentRecord: ...

    def retire_environment(
        self,
        env_id: str,
        *,
        principal_id: str,
        expected_resource_generation: int,
    ) -> object: ...

    def revive_environment(
        self,
        env_id: str,
        *,
        principal_id: str,
        expected_resource_generation: int,
    ) -> object: ...


@dataclass(frozen=True, slots=True)
class CommandResult:
    returncode: int
    stdout: str


class CommandRunner(Protocol):
    def run(
        self,
        argv: Sequence[str],
        *,
        cwd: Path | None = None,
        env: Mapping[str, str] | None = None,
        expected: frozenset[int] = frozenset({0}),
    ) -> CommandResult: ...


class CapacityAuthority(Protocol):
    def abort(self, context: DeploymentContext) -> dict[str, Any]: ...

    def reconcile(self, context: DeploymentContext) -> dict[str, Any]: ...

    def finalize(self, context: DeploymentContext) -> dict[str, Any]: ...

    def finalize_check(self, context: DeploymentContext) -> dict[str, Any]: ...

    def check(self, context: DeploymentContext) -> dict[str, Any]: ...

    def rollback(self, context: DeploymentContext) -> dict[str, Any]: ...

    def retire(self, context: DeploymentContext) -> dict[str, Any]: ...

    def reactivate(self, context: DeploymentContext) -> dict[str, Any]: ...


class DistributedRuntimeAuthority(Protocol):
    def reconcile(self, context: DeploymentContext) -> dict[str, Any]: ...

    def check(self, context: DeploymentContext) -> dict[str, Any]: ...

    def acceptance_probe(self, context: DeploymentContext) -> dict[str, Any]: ...

    def activate(self, context: DeploymentContext) -> dict[str, Any]: ...

    def fence(self, context: DeploymentContext) -> dict[str, Any]: ...

    def rollback(self, context: DeploymentContext) -> dict[str, Any]: ...

    def retire(self, context: DeploymentContext) -> dict[str, Any]: ...


class SubprocessRunner:
    def run(
        self,
        argv: Sequence[str],
        *,
        cwd: Path | None = None,
        env: Mapping[str, str] | None = None,
        expected: frozenset[int] = frozenset({0}),
    ) -> CommandResult:
        try:
            result = subprocess.run(
                list(argv),
                cwd=cwd,
                env=None if env is None else dict(env),
                check=False,
                capture_output=True,
                text=True,
            )
        except OSError as exc:
            raise DeploymentError("host command is unavailable") from exc
        if result.returncode not in expected:
            raise DeploymentError("host command failed safely")
        return CommandResult(result.returncode, result.stdout)


@dataclass(frozen=True, slots=True)
class DeploymentContext:
    snapshot_generation: int
    snapshot_digest: str
    environment: dict[str, Any]
    candidate: dict[str, Any]
    deployment: dict[str, Any]
    host_root: Path | None = None
    effective_candidate: dict[str, Any] | None = None
    effective_deployment: dict[str, Any] | None = None

    def host_path(self, value: str) -> Path:
        path = Path(value)
        if self.host_root is None:
            return path
        return self.host_root.joinpath(*path.parts[1:])

    @property
    def env_id(self) -> str:
        return cast(str, self.environment["env_id"])

    @property
    def principal_id(self) -> str:
        return cast(str, self.environment["principal_id"])

    @property
    def deployment_id(self) -> str:
        return cast(str, self.deployment["deployment_id"])

    @property
    def candidate_id(self) -> str:
        return cast(str, self.candidate["candidate_id"])

    @property
    def candidate_sha(self) -> str:
        return cast(str, self.candidate["candidate_sha"])

    @property
    def candidate_tree(self) -> str:
        return cast(str, self.candidate["candidate_tree"])

    @property
    def resource_generation(self) -> int:
        return cast(int, self.deployment["expected_resource_generation"])

    @property
    def runtime_resource_generation(self) -> int:
        if self.deployment.get("phase") == "committed" or (
            self.deployment.get("phase") == "verified"
            and self.deployment.get("applied_resource_generation") is not None
        ):
            applied = self.deployment.get("applied_resource_generation")
            if (
                not _plain_int(applied, minimum=1)
                or (
                    self.deployment.get("phase") == "committed"
                    and applied != self.environment.get("resource_generation")
                )
                or (
                    self.deployment.get("phase") == "verified"
                    and applied != self.resource_generation + 1
                )
            ):
                raise DeploymentError("deployment resource binding is invalid")
            return cast(int, applied)
        return self.resource_generation

    @property
    def applied_registry_generation(self) -> int:
        if self.deployment.get("phase") == "committed" or (
            self.deployment.get("phase") == "verified"
            and self.deployment.get("applied_registry_generation") is not None
        ):
            applied = self.deployment.get("applied_registry_generation")
            if not _plain_int(applied, minimum=1):
                raise DeploymentError("committed deployment registry binding is invalid")
            return cast(int, applied)
        return self.snapshot_generation

    @property
    def applied_registry_digest(self) -> str:
        if self.deployment.get("phase") == "committed" or (
            self.deployment.get("phase") == "verified"
            and self.deployment.get("applied_registry_payload_sha256") is not None
        ):
            applied = str(self.deployment.get("applied_registry_payload_sha256"))
            if re.fullmatch(r"[0-9a-f]{64}", applied) is None:
                raise DeploymentError("committed deployment registry binding is invalid")
            return applied
        return self.snapshot_digest

    @property
    def state_root(self) -> Path:
        return self.host_path(cast(str, self.environment["state_root"]))

    @property
    def lifecycle_root(self) -> Path:
        return self.host_path(str(RUNTIME_ROOT / "lifecycle" / "environments" / self.env_id))

    @property
    def checkout(self) -> Path:
        return self.host_path(cast(str, self.environment["candidate_root"])) / self.candidate_sha

    @property
    def journal_path(self) -> Path:
        return self.lifecycle_root / "deployment-journal.json"

    @property
    def manifest_path(self) -> Path:
        return self.lifecycle_root / "host-manifest.json"

    @property
    def secrets_path(self) -> Path:
        return self.state_root / "secrets" / "environment.env"

    @property
    def distributed_secrets_path(self) -> Path:
        return self.state_root / "secrets" / "sandbox.env"

    @property
    def compose_env_path(self) -> Path:
        return self.lifecycle_root / "compose.env"

    @property
    def compose_override_path(self) -> Path:
        return self.lifecycle_root / "compose.override.json"

    @property
    def architecture(self) -> str:
        machine = platform.machine().lower()
        if machine in {"x86_64", "amd64"}:
            return "amd64"
        if machine in {"aarch64", "arm64"}:
            return "arm64"
        raise DeploymentError("host architecture is unsupported")

    @property
    def image_digest(self) -> str:
        """Compatibility name for the exact per-architecture loom-worker config ID."""

        return cast(dict[str, str], self.candidate["image_digests"])[self.architecture]

    @property
    def worker_image_id(self) -> str:
        runtime_bindings = self.deployment.get("worker_runtime_bindings")
        domain = "oldlab" if self.architecture == "amd64" else "gb10"
        domains = runtime_bindings.get("domains") if isinstance(runtime_bindings, dict) else None
        binding = domains.get(domain) if isinstance(domains, dict) else None
        runtime_image_id = binding.get("runtime_image_id") if isinstance(binding, dict) else None
        if re.fullmatch(r"sha256:[0-9a-f]{64}", str(runtime_image_id)) is None:
            raise DeploymentError("deployment worker runtime image binding is invalid")
        return str(runtime_image_id)


def _canonical(payload: Mapping[str, Any]) -> bytes:
    try:
        return (
            json.dumps(
                dict(payload),
                sort_keys=True,
                separators=(",", ":"),
                ensure_ascii=True,
            ).encode("ascii")
            + b"\n"
        )
    except (TypeError, UnicodeEncodeError) as exc:
        raise DeploymentError("deployment evidence is not canonical JSON") from exc


def _digest(payload: Mapping[str, Any]) -> str:
    return hashlib.sha256(_canonical(payload)).hexdigest()


def _bound(payload: Mapping[str, Any]) -> dict[str, Any]:
    unsigned = dict(payload)
    return {**unsigned, "payload_sha256": _digest(unsigned)}


def _evidence_digest(payload: Mapping[str, Any]) -> str:
    digest = payload.get("payload_sha256")
    if isinstance(digest, str) and re.fullmatch(r"[0-9a-f]{64}", digest) is not None:
        return digest
    return _digest(payload)


def _resource_owner_labels(context: DeploymentContext) -> dict[str, str]:
    return _environment_owner_labels(context.environment)


def _environment_owner_labels(environment: Mapping[str, Any]) -> dict[str, str]:
    owner = {
        "env_id": environment["env_id"],
        "runtime_id": environment["runtime_id"],
        "compose_project": environment["compose_project"],
        "compose_network": f"{environment['compose_project']}_default",
        "postgres_volume": environment["postgres_volume"],
        "minio_volume": environment["minio_volume"],
        "systemd_instance": environment["systemd_instance"],
    }
    return {
        "loom.developer-environment.env-id": cast(str, environment["env_id"]),
        "loom.developer-environment.runtime-id": cast(str, environment["runtime_id"]),
        "loom.developer-environment.owner-sha256": _digest(owner),
    }


def _plain_int(value: object, *, minimum: int = 0) -> bool:
    return isinstance(value, int) and not isinstance(value, bool) and value >= minimum


def _valid_timestamp(value: object) -> bool:
    if not isinstance(value, str) or not value.endswith("Z"):
        return False
    try:
        parsed = datetime.fromisoformat(value.removesuffix("Z") + "+00:00")
    except ValueError:
        return False
    return parsed.utcoffset() == timedelta(0)


def _read_stable_regular(
    path: Path,
    *,
    limit: int,
    require_root: bool,
    expected_mode: int | None = None,
) -> bytes:
    descriptor = -1
    try:
        lexical = path.lstat()
        descriptor = os.open(
            path,
            os.O_RDONLY | os.O_NOFOLLOW | getattr(os, "O_CLOEXEC", 0),
        )
        opened = os.fstat(descriptor)
        chunks: list[bytes] = []
        total = 0
        while True:
            chunk = os.read(descriptor, min(65_536, limit + 1 - total))
            if not chunk:
                break
            total += len(chunk)
            if total > limit:
                raise DeploymentError("trusted input exceeds its size bound")
            chunks.append(chunk)
        rebound = os.fstat(descriptor)
        current = path.lstat()
    except DeploymentError:
        raise
    except OSError as exc:
        raise DeploymentError("trusted input is unavailable") from exc
    finally:
        if descriptor >= 0:
            os.close(descriptor)
    identities = {
        (
            row.st_dev,
            row.st_ino,
            row.st_mode,
            row.st_uid,
            row.st_gid,
            row.st_nlink,
            row.st_size,
            row.st_mtime_ns,
            row.st_ctime_ns,
        )
        for row in (lexical, opened, rebound, current)
    }
    if (
        not stat.S_ISREG(opened.st_mode)
        or stat.S_ISLNK(lexical.st_mode)
        or opened.st_nlink != 1
        or len(identities) != 1
        or (require_root and (opened.st_uid, opened.st_gid) != (0, 0))
        or (expected_mode is not None and stat.S_IMODE(opened.st_mode) != expected_mode)
    ):
        raise DeploymentError("trusted input metadata is unsafe")
    return b"".join(chunks)


def load_registry_snapshot(
    path: Path = SYSTEM_SNAPSHOT,
    *,
    require_root: bool = True,
) -> dict[str, Any]:
    raw = _read_stable_regular(
        path,
        limit=MAX_SNAPSHOT_BYTES,
        require_root=require_root,
        expected_mode=0o600 if require_root else None,
    )
    try:
        return DeveloperEnvironmentRegistry.verify_snapshot(raw)
    except RegistryError as exc:
        raise DeploymentError("registry snapshot verification failed") from exc


def select_environment(
    snapshot: Mapping[str, Any],
    *,
    principal_id: str | None,
    env_id: str | None,
    root: bool,
) -> dict[str, Any]:
    environments = cast(list[dict[str, Any]], snapshot["environments"])
    if principal_id is not None:
        owned = [row for row in environments if row["principal_id"] == principal_id]
        if len(owned) != 1 or (env_id is not None and owned[0]["env_id"] != env_id):
            raise DeploymentError("developer environment ownership is invalid")
        return owned[0]
    if not root or env_id is None or ENV_ID_RE.fullmatch(env_id) is None:
        raise DeploymentError("root must select one exact developer environment")
    matches = [row for row in environments if row["env_id"] == env_id]
    if len(matches) != 1:
        raise DeploymentError("developer environment is unavailable")
    return matches[0]


def select_runtime_environment(
    snapshot: Mapping[str, Any],
    *,
    runtime_id: str,
    root: bool,
) -> dict[str, Any]:
    if not root or RUNTIME_ID_RE.fullmatch(runtime_id) is None:
        raise DeploymentError("root runtime environment selection is invalid")
    matches = [
        row
        for row in cast(list[dict[str, Any]], snapshot["environments"])
        if row["systemd_instance"] == runtime_id
    ]
    if len(matches) != 1:
        raise DeploymentError("runtime environment is unavailable")
    return matches[0]


def _context(
    snapshot: dict[str, Any],
    environment: dict[str, Any],
    *,
    deployment_id: str | None = None,
    host_root: Path | None = None,
) -> DeploymentContext:
    deployments = [
        row
        for row in cast(list[dict[str, Any]], snapshot["deployments"])
        if row["env_id"] == environment["env_id"] and row["phase"] not in {"committed", "failed"}
    ]
    if deployment_id is not None:
        deployments = [row for row in deployments if row["deployment_id"] == deployment_id]
    if len(deployments) != 1:
        raise DeploymentError("one active developer deployment is required")
    deployment = deployments[0]
    candidates = [
        row
        for row in cast(list[dict[str, Any]], snapshot["candidates"])
        if row["candidate_id"] == deployment["candidate_id"]
    ]
    if len(candidates) != 1:
        raise DeploymentError("developer deployment candidate is unavailable")
    return DeploymentContext(
        snapshot_generation=cast(int, snapshot["generation"]),
        snapshot_digest=cast(str, snapshot["payload_sha256"]),
        environment=environment,
        candidate=candidates[0],
        deployment=deployment,
        host_root=host_root,
    )


def _assert_safe_ancestry(path: Path) -> None:
    absolute = Path(os.path.abspath(path))
    current = absolute
    while True:
        try:
            metadata = current.lstat()
        except FileNotFoundError:
            pass
        except OSError as exc:
            raise DeploymentError("deployment path ancestry is unavailable") from exc
        else:
            if stat.S_ISLNK(metadata.st_mode):
                raise DeploymentError("deployment path ancestry contains a symlink")
        if current.parent == current:
            return
        current = current.parent


def _ensure_directory(
    path: Path,
    *,
    mode: int,
    uid: int,
    gid: int,
    manage_ownership: bool,
) -> None:
    _assert_safe_ancestry(path)
    descriptor = -1
    try:
        path.mkdir(mode=mode, parents=True, exist_ok=True)
        descriptor = os.open(
            path,
            os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | getattr(os, "O_CLOEXEC", 0),
        )
        metadata = os.fstat(descriptor)
        if not stat.S_ISDIR(metadata.st_mode) or stat.S_ISLNK(metadata.st_mode):
            raise DeploymentError("deployment directory is unsafe")
        os.fchmod(descriptor, mode)
        if manage_ownership:
            os.fchown(descriptor, uid, gid)
        rebound = os.fstat(descriptor)
        current = path.lstat()
    except DeploymentError:
        raise
    except OSError as exc:
        raise DeploymentError("deployment directory cannot be converged safely") from exc
    finally:
        if descriptor >= 0:
            os.close(descriptor)
    if (
        (rebound.st_dev, rebound.st_ino) != (current.st_dev, current.st_ino)
        or not stat.S_ISDIR(current.st_mode)
        or stat.S_ISLNK(current.st_mode)
        or stat.S_IMODE(rebound.st_mode) != mode
        or stat.S_IMODE(current.st_mode) != mode
        or (
            manage_ownership
            and (
                (rebound.st_uid, rebound.st_gid) != (uid, gid)
                or (current.st_uid, current.st_gid) != (uid, gid)
            )
        )
    ):
        raise DeploymentError("deployment directory metadata did not converge")


def _atomic_write(
    path: Path,
    raw: bytes,
    *,
    mode: int,
    uid: int,
    gid: int,
    manage_ownership: bool,
) -> None:
    _assert_safe_ancestry(path.parent)
    temporary: Path | None = None
    descriptor = -1
    directory_descriptor = -1
    try:
        descriptor, name = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
        temporary = Path(name)
        os.fchmod(descriptor, mode)
        if manage_ownership:
            os.fchown(descriptor, uid, gid)
        view = memoryview(raw)
        while view:
            written = os.write(descriptor, view)
            if written < 1:
                raise DeploymentError("deployment file write failed")
            view = view[written:]
        os.fsync(descriptor)
        os.close(descriptor)
        descriptor = -1
        os.replace(temporary, path)
        temporary = None
        directory_descriptor = os.open(
            path.parent,
            os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | getattr(os, "O_CLOEXEC", 0),
        )
        os.fsync(directory_descriptor)
    except DeploymentError:
        raise
    except OSError as exc:
        raise DeploymentError("deployment file cannot be published safely") from exc
    finally:
        if descriptor >= 0:
            os.close(descriptor)
        if directory_descriptor >= 0:
            os.close(directory_descriptor)
        if temporary is not None:
            temporary.unlink(missing_ok=True)


def _remove_exact_owned_tree(path: Path, *, allowed_uids: frozenset[int]) -> None:
    """Remove one already registry-bound tree after a complete metadata preflight."""

    if not path.exists():
        return
    if not path.is_absolute() or path == Path("/") or len(path.parts) < 4:
        raise DeploymentError("exact-owned removal path is unsafe")
    _assert_safe_ancestry(path)
    entries = [path]
    try:
        entries.extend(path.rglob("*"))
        for entry in entries:
            metadata = entry.lstat()
            if (
                stat.S_ISLNK(metadata.st_mode)
                or metadata.st_uid not in allowed_uids
                or (not stat.S_ISDIR(metadata.st_mode) and not stat.S_ISREG(metadata.st_mode))
                or (stat.S_ISREG(metadata.st_mode) and metadata.st_nlink != 1)
            ):
                raise DeploymentError("exact-owned removal tree is unsafe")
        for entry in sorted(entries[1:], key=lambda item: len(item.parts), reverse=True):
            if entry.is_dir():
                entry.rmdir()
            else:
                entry.unlink()
        path.rmdir()
    except DeploymentError:
        raise
    except OSError as exc:
        raise DeploymentError("exact-owned removal failed safely") from exc


def _load_bound_json(
    path: Path,
    *,
    kind: str,
    require_root: bool = False,
) -> dict[str, Any] | None:
    try:
        raw = _read_stable_regular(
            path,
            limit=1024 * 1024,
            require_root=require_root,
            expected_mode=0o600 if require_root else None,
        )
    except DeploymentError:
        if not path.exists():
            return None
        raise
    try:
        payload = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise DeploymentError("deployment state is invalid") from exc
    if (
        not isinstance(payload, dict)
        or payload.get("kind") != kind
        or not isinstance(payload.get("payload_sha256"), str)
        or raw != _canonical(payload)
    ):
        raise DeploymentError("deployment state binding is invalid")
    unsigned = {key: value for key, value in payload.items() if key != "payload_sha256"}
    if payload["payload_sha256"] != _digest(unsigned):
        raise DeploymentError("deployment state digest is invalid")
    return payload


class FixedCapacityAuthority:
    """Reconcile both capacity domains through the fixed #827 root producer."""

    def __init__(
        self,
        runner: CommandRunner,
        *,
        root: Path = CAPACITY_ROOT,
        program: Path = CAPACITY_PROGRAM,
        require_root_metadata: bool = True,
    ) -> None:
        self.runner = runner
        self.root = root
        self.program = program
        self.require_root_metadata = require_root_metadata

    def _reconcile(self, context: DeploymentContext, *, action: str) -> dict[str, Any]:
        if action not in {"reconcile", "finalize", "reactivate"}:
            raise DeploymentError("capacity authority action is invalid")
        request_root = self.root / "requests"
        receipt_root = self.root / "receipts"
        for path in (self.root, request_root, receipt_root):
            _ensure_directory(
                path,
                mode=0o700,
                uid=0,
                gid=0,
                manage_ownership=self.require_root_metadata,
            )
        request = _bound(
            {
                "schema_version": 1,
                "kind": "loom.developer-environment.capacity-request",
                "env_id": context.env_id,
                "principal_id": context.principal_id,
                "deployment_id": context.deployment_id,
                "candidate_id": context.candidate_id,
                "candidate_sha": context.candidate_sha,
                "candidate_tree": context.candidate_tree,
                "resource_generation": context.runtime_resource_generation,
                "registry_generation": context.snapshot_generation,
                "registry_snapshot_sha256": context.snapshot_digest,
                "slurm_user": context.environment["slurm_user"],
                "service_group": context.environment["service_group"],
                "slurm_account": context.environment["slurm_account"],
                "slurm_qos": context.environment["slurm_qos"],
                "uid": context.environment["uid"],
                "gid": context.environment["gid"],
                "identity_preflight_nodes": {
                    domain: [nodes[0]] for domain, nodes in DOMAIN_IDENTITY_NODES.items()
                },
            }
        )
        request_path = request_root / f"{context.deployment_id}.json"
        if action != "reactivate":
            _atomic_write(
                request_path,
                _canonical(request),
                mode=0o600,
                uid=0,
                gid=0,
                manage_ownership=self.require_root_metadata,
            )
        self.runner.run(
            (
                str(self.program),
                action,
                "--deployment-id",
                context.deployment_id,
            )
        )
        if action == "reactivate":
            published = _load_bound_json(
                request_path,
                kind="loom.developer-environment.capacity-request",
            )
            if published != request:
                raise DeploymentError("capacity reactivation request binding drifted")
        receipt_path = receipt_root / f"{context.deployment_id}.json"
        raw = _read_stable_regular(
            receipt_path,
            limit=1024 * 1024,
            require_root=self.require_root_metadata,
            expected_mode=0o600,
        )
        try:
            receipt = json.loads(raw)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise DeploymentError("capacity authority receipt is invalid") from exc
        if (
            not isinstance(receipt, dict)
            or set(receipt)
            != {
                "schema_version",
                "kind",
                "status",
                "request_sha256",
                "env_id",
                "principal_id",
                "deployment_id",
                "candidate_id",
                "candidate_sha",
                "candidate_tree",
                "resource_generation",
                "registry_generation",
                "registry_snapshot_sha256",
                "slurm_user",
                "service_group",
                "slurm_account",
                "slurm_qos",
                "uid",
                "gid",
                "domains",
                "payload_sha256",
            }
            or receipt.get("schema_version") != 1
            or receipt.get("kind") != "loom.developer-environment.capacity-receipt"
            or receipt.get("status")
            != {
                "reconcile": "prepared",
                "finalize": "acceptance-prepared",
                "reactivate": "revive-prepared",
            }[action]
            or receipt.get("request_sha256") != request["payload_sha256"]
            or raw != _canonical(receipt)
        ):
            raise DeploymentError("capacity authority receipt binding is invalid")
        unsigned = {key: value for key, value in receipt.items() if key != "payload_sha256"}
        if receipt["payload_sha256"] != _digest(unsigned):
            raise DeploymentError("capacity authority receipt digest is invalid")
        exact = {
            "env_id": context.env_id,
            "principal_id": context.principal_id,
            "deployment_id": context.deployment_id,
            "candidate_id": context.candidate_id,
            "candidate_sha": context.candidate_sha,
            "candidate_tree": context.candidate_tree,
            "resource_generation": context.runtime_resource_generation,
            "registry_generation": context.snapshot_generation,
            "registry_snapshot_sha256": context.snapshot_digest,
            "slurm_user": context.environment["slurm_user"],
            "service_group": context.environment["service_group"],
            "slurm_account": context.environment["slurm_account"],
            "slurm_qos": context.environment["slurm_qos"],
            "uid": context.environment["uid"],
            "gid": context.environment["gid"],
        }
        if any(receipt.get(field) != value for field, value in exact.items()):
            raise DeploymentError("capacity authority receipt identity drifted")
        domains = receipt.get("domains")
        if not isinstance(domains, dict) or set(domains) != {"oldlab", "gb10"}:
            raise DeploymentError("capacity authority domain set is invalid")
        domain_fields = {
            "status",
            "env_id",
            "slurm_user",
            "service_group",
            "uid",
            "gid",
            "slurm_account",
            "slurm_qos",
            "candidate_sha",
            "candidate_tree",
            "registry_snapshot_sha256",
            "policy_generation",
            "policy_sha256",
            "authority_receipt_sha256",
            "cluster",
            "controller",
            "submit_host",
            "identity_preflight",
            "identity_preflight_sha256",
            "identity_convergence",
            "identity_convergence_sha256",
            "slurm_convergence",
            "slurm_convergence_sha256",
            "completed_at",
        }
        routes = {
            "oldlab": {
                "cluster": "trt-oldlab",
                "controller": "TRT-EAI-OLDLAB-1",
                "submit_host": "trt-EAI-OLDLAB-2",
            },
            "gb10": {
                "cluster": "trt-gb10",
                "controller": "trt-gb10-1",
                "submit_host": "trt-gb10-1",
            },
        }
        for domain_name, row in domains.items():
            if (
                not isinstance(row, dict)
                or set(row) != domain_fields
                or row.get("status") != "ready"
                or row.get("env_id") != context.env_id
                or row.get("slurm_user") != context.environment["slurm_user"]
                or row.get("service_group") != context.environment["service_group"]
                or row.get("uid") != context.environment["uid"]
                or row.get("gid") != context.environment["gid"]
                or row.get("slurm_account") != context.environment["slurm_account"]
                or row.get("slurm_qos") != context.environment["slurm_qos"]
                or not self._valid_identity_preflight(row, domain_name)
                or not self._valid_identity_convergence(row, domain_name)
                or not self._valid_slurm_convergence(row, domain_name)
                or row.get("candidate_sha") != context.candidate_sha
                or row.get("candidate_tree") != context.candidate_tree
                or row.get("registry_snapshot_sha256") != context.snapshot_digest
                or row.get("policy_generation") != context.snapshot_generation
                or re.fullmatch(r"[0-9a-f]{64}", str(row.get("policy_sha256"))) is None
                or re.fullmatch(
                    r"[0-9a-f]{64}",
                    str(row.get("authority_receipt_sha256")),
                )
                is None
                or any(row.get(field) != value for field, value in routes[domain_name].items())
                or not _valid_timestamp(row.get("completed_at"))
            ):
                raise DeploymentError("capacity authority domain binding is invalid")
        return receipt

    def reconcile(self, context: DeploymentContext) -> dict[str, Any]:
        return self._reconcile(context, action="reconcile")

    def finalize(self, context: DeploymentContext) -> dict[str, Any]:
        return self._reconcile(context, action="finalize")

    def reactivate(self, context: DeploymentContext) -> dict[str, Any]:
        return self._reconcile(context, action="reactivate")

    def abort(self, context: DeploymentContext) -> dict[str, Any]:
        request = _load_bound_json(
            self.root / "requests" / f"{context.deployment_id}.json",
            kind="loom.developer-environment.capacity-request",
        )
        if request is None:
            return {"status": "absent"}
        self.runner.run(
            (
                str(self.program),
                "abort",
                "--deployment-id",
                context.deployment_id,
            )
        )
        receipt_path = self.root / "receipts" / f"{context.deployment_id}-abort.json"
        raw = _read_stable_regular(
            receipt_path,
            limit=1024 * 1024,
            require_root=self.require_root_metadata,
            expected_mode=0o600,
        )
        try:
            receipt = json.loads(raw)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise DeploymentError("capacity abort receipt is invalid") from exc
        fields = {
            "schema_version",
            "kind",
            "status",
            "request_sha256",
            "env_id",
            "principal_id",
            "deployment_id",
            "candidate_id",
            "candidate_sha",
            "candidate_tree",
            "resource_generation",
            "registry_generation",
            "registry_snapshot_sha256",
            "slurm_user",
            "service_group",
            "slurm_account",
            "slurm_qos",
            "uid",
            "gid",
            "domains",
            "payload_sha256",
        }
        exact = {
            "env_id": context.env_id,
            "principal_id": context.principal_id,
            "deployment_id": context.deployment_id,
            "candidate_id": context.candidate_id,
            "candidate_sha": context.candidate_sha,
            "candidate_tree": context.candidate_tree,
            "resource_generation": context.resource_generation,
            "registry_generation": context.snapshot_generation,
            "registry_snapshot_sha256": context.snapshot_digest,
            "slurm_user": context.environment["slurm_user"],
            "service_group": context.environment["service_group"],
            "slurm_account": context.environment["slurm_account"],
            "slurm_qos": context.environment["slurm_qos"],
            "uid": context.environment["uid"],
            "gid": context.environment["gid"],
        }
        unsigned = (
            {key: value for key, value in receipt.items() if key != "payload_sha256"}
            if isinstance(receipt, dict)
            else {}
        )
        domains = receipt.get("domains") if isinstance(receipt, dict) else None
        routes = {
            "oldlab": ("oldlab-1", "trt-oldlab"),
            "gb10": ("trt-gb10-1", "trt-gb10"),
        }
        if (
            not isinstance(receipt, dict)
            or set(receipt) != fields
            or raw != _canonical(receipt)
            or receipt.get("schema_version") != 1
            or receipt.get("kind") != "loom.developer-environment.capacity-abort-receipt"
            or receipt.get("status") != "retired"
            or any(receipt.get(field) != value for field, value in exact.items())
            or receipt.get("request_sha256") != request["payload_sha256"]
            or not isinstance(domains, dict)
            or set(domains) != set(routes)
            or any(
                not isinstance(domains[domain], dict)
                or set(domains[domain]) != {controller}
                or not self._valid_identity_retire(
                    domains[domain][controller],
                    cluster=cluster,
                    env_id=context.env_id,
                    resource_generation=context.resource_generation,
                )
                for domain, (controller, cluster) in routes.items()
            )
            or receipt.get("payload_sha256") != _digest(unsigned)
        ):
            raise DeploymentError("capacity abort receipt binding is invalid")
        return receipt

    def retire(self, context: DeploymentContext) -> dict[str, Any]:
        request = _bound(
            {
                "schema_version": 1,
                "kind": "loom.developer-environment.capacity-retire-request",
                "env_id": context.env_id,
                "principal_id": context.principal_id,
                "deployment_id": context.deployment_id,
                "candidate_id": context.candidate_id,
                "candidate_sha": context.candidate_sha,
                "candidate_tree": context.candidate_tree,
                "resource_generation": context.runtime_resource_generation,
                "registry_generation": context.snapshot_generation,
                "registry_snapshot_sha256": context.snapshot_digest,
                "slurm_user": context.environment["slurm_user"],
                "service_group": context.environment["service_group"],
                "slurm_account": context.environment["slurm_account"],
                "slurm_qos": context.environment["slurm_qos"],
                "uid": context.environment["uid"],
                "gid": context.environment["gid"],
                "identity_preflight_nodes": {
                    domain: list(nodes) for domain, nodes in DOMAIN_IDENTITY_NODES.items()
                },
            }
        )
        request_path = self.root / "requests" / f"{context.deployment_id}-retire-request.json"
        _ensure_directory(
            request_path.parent,
            mode=0o700,
            uid=0,
            gid=0,
            manage_ownership=self.require_root_metadata,
        )
        _atomic_write(
            request_path,
            _canonical(request),
            mode=0o600,
            uid=0,
            gid=0,
            manage_ownership=self.require_root_metadata,
        )
        self.runner.run(
            (
                str(self.program),
                "retire",
                "--deployment-id",
                context.deployment_id,
            )
        )
        raw = _read_stable_regular(
            self.root / "receipts" / f"{context.deployment_id}-retire.json",
            limit=1024 * 1024,
            require_root=self.require_root_metadata,
            expected_mode=0o600,
        )
        try:
            receipt = json.loads(raw)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise DeploymentError("capacity retire receipt is invalid") from exc
        exact = {
            key: request[key]
            for key in (
                "env_id",
                "principal_id",
                "deployment_id",
                "candidate_id",
                "candidate_sha",
                "candidate_tree",
                "resource_generation",
                "registry_generation",
                "registry_snapshot_sha256",
                "slurm_user",
                "service_group",
                "slurm_account",
                "slurm_qos",
                "uid",
                "gid",
            )
        }
        domains = receipt.get("domains") if isinstance(receipt, dict) else None
        routes = {
            "oldlab": ("oldlab-1", "trt-oldlab"),
            "gb10": ("trt-gb10-1", "trt-gb10"),
        }
        unsigned = (
            {key: value for key, value in receipt.items() if key != "payload_sha256"}
            if isinstance(receipt, dict)
            else {}
        )
        if (
            not isinstance(receipt, dict)
            or raw != _canonical(receipt)
            or receipt.get("schema_version") != 1
            or receipt.get("kind") != "loom.developer-environment.capacity-retire-receipt"
            or receipt.get("status") != "retired"
            or receipt.get("request_sha256") != request["payload_sha256"]
            or any(receipt.get(field) != value for field, value in exact.items())
            or not isinstance(domains, dict)
            or set(domains) != set(routes)
            or any(
                not isinstance(domains[domain], dict)
                or set(domains[domain]) != {controller}
                or not self._valid_identity_retire(
                    domains[domain][controller],
                    cluster=cluster,
                    env_id=context.env_id,
                    resource_generation=context.runtime_resource_generation,
                )
                for domain, (controller, cluster) in routes.items()
            )
            or receipt.get("payload_sha256") != _digest(unsigned)
        ):
            raise DeploymentError("capacity retire receipt binding is invalid")
        return receipt

    @staticmethod
    def _valid_identity_retire(
        proof: object,
        *,
        cluster: str,
        env_id: str,
        resource_generation: int,
    ) -> bool:
        if not isinstance(proof, dict):
            return False
        return (
            set(proof)
            == {
                "request_id",
                "result_sha256",
                "authority_receipt_sha256",
                "completed_at",
                "action",
                "tombstone",
            }
            and proof.get("action") == "slurm-identity-retire"
            and proof.get("tombstone")
            == (
                "/var/lib/loom-developer-sandbox-slurm-policy/"
                f"identity-tombstones/{cluster}/{env_id}/"
                f"{resource_generation}.json"
            )
            and all(
                re.fullmatch(r"[0-9a-f]{64}", str(proof.get(field))) is not None
                for field in (
                    "request_id",
                    "result_sha256",
                    "authority_receipt_sha256",
                )
            )
            and _valid_timestamp(proof.get("completed_at"))
        )

    def rollback(self, context: DeploymentContext) -> dict[str, Any]:
        self.runner.run(
            (
                str(self.program),
                "rollback",
                "--deployment-id",
                context.deployment_id,
            )
        )
        receipt_path = self.root / "receipts" / f"{context.deployment_id}-rollback.json"
        raw = _read_stable_regular(
            receipt_path,
            limit=1024 * 1024,
            require_root=self.require_root_metadata,
            expected_mode=0o600,
        )
        try:
            receipt = json.loads(raw)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise DeploymentError("capacity rollback receipt is invalid") from exc
        effective = context.effective_candidate
        unsigned = (
            {key: value for key, value in receipt.items() if key != "payload_sha256"}
            if isinstance(receipt, dict)
            else {}
        )
        if (
            not isinstance(receipt, dict)
            or set(receipt)
            != {
                "schema_version",
                "kind",
                "status",
                "deployment_id",
                "env_id",
                "failed_candidate_id",
                "failed_candidate_sha",
                "failed_candidate_tree",
                "restored_candidate_id",
                "restored_candidate_sha",
                "restored_candidate_tree",
                "resource_generation",
                "registry_generation",
                "registry_payload_sha256",
                "failed_candidate_projection_present",
                "association_preserved",
                "domains",
                "payload_sha256",
            }
            or receipt.get("schema_version") != 1
            or receipt.get("kind") != "loom.developer-environment.capacity-rollback-receipt"
            or receipt.get("status") != "ready"
            or receipt.get("deployment_id") != context.deployment_id
            or receipt.get("env_id") != context.env_id
            or receipt.get("failed_candidate_id") != context.candidate_id
            or receipt.get("failed_candidate_sha") != context.candidate_sha
            or receipt.get("failed_candidate_tree") != context.candidate_tree
            or effective is None
            or receipt.get("restored_candidate_id") != effective["candidate_id"]
            or receipt.get("restored_candidate_sha") != effective["candidate_sha"]
            or receipt.get("restored_candidate_tree") != effective["candidate_tree"]
            or receipt.get("resource_generation") != context.environment["resource_generation"]
            or receipt.get("registry_generation") != context.snapshot_generation
            or receipt.get("registry_payload_sha256") != context.snapshot_digest
            or receipt.get("failed_candidate_projection_present") is not False
            or receipt.get("association_preserved") is not True
            or not isinstance(receipt.get("domains"), dict)
            or set(receipt["domains"]) != {"oldlab", "gb10"}
            or any(
                not isinstance(receipt["domains"][domain], dict)
                or receipt["domains"][domain].get("status") != "ready"
                for domain in ("oldlab", "gb10")
            )
            or raw != _canonical(receipt)
            or receipt.get("payload_sha256") != _digest(unsigned)
        ):
            raise DeploymentError("capacity rollback receipt binding is invalid")
        return receipt

    def _check(
        self,
        context: DeploymentContext,
        *,
        finalized: bool,
    ) -> dict[str, Any]:
        result = self.runner.run(
            (
                str(self.program),
                "finalize-check" if finalized else "check",
                "--deployment-id",
                context.deployment_id,
            )
        )
        try:
            receipt = json.loads(result.stdout)
            raw = result.stdout.encode("ascii")
        except (UnicodeEncodeError, UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise DeploymentError("capacity check receipt is invalid") from exc
        fields = {
            "schema_version",
            "kind",
            "status",
            "deployment_id",
            "env_id",
            "candidate_id",
            "candidate_sha",
            "candidate_tree",
            "resource_generation",
            "registry_generation",
            "registry_payload_sha256",
            "capacity_receipt_sha256",
            "identity_node_count",
            "domains",
            "checked_at",
            "payload_sha256",
        }
        unsigned = (
            {key: value for key, value in receipt.items() if key != "payload_sha256"}
            if isinstance(receipt, dict)
            else {}
        )
        exact = {
            "deployment_id": context.deployment_id,
            "env_id": context.env_id,
            "candidate_id": context.candidate_id,
            "candidate_sha": context.candidate_sha,
            "candidate_tree": context.candidate_tree,
            "resource_generation": context.runtime_resource_generation,
            "registry_generation": context.snapshot_generation,
            "registry_payload_sha256": context.snapshot_digest,
        }
        if (
            not isinstance(receipt, dict)
            or set(receipt) != fields
            or raw != _canonical(receipt)
            or receipt.get("schema_version") != 1
            or receipt.get("kind")
            != (
                "loom.developer-environment.capacity-finalize-check"
                if finalized
                else "loom.developer-environment.capacity-check"
            )
            or receipt.get("status") != ("acceptance-prepared" if finalized else "activated")
            or any(receipt.get(field) != value for field, value in exact.items())
            or receipt.get("identity_node_count")
            != sum(len(nodes) for nodes in DOMAIN_IDENTITY_NODES.values())
            or receipt.get("domains") != ["oldlab", "gb10"]
            or re.fullmatch(
                r"[0-9a-f]{64}",
                str(receipt.get("capacity_receipt_sha256")),
            )
            is None
            or not _valid_timestamp(receipt.get("checked_at"))
            or receipt.get("payload_sha256") != _digest(unsigned)
        ):
            raise DeploymentError("capacity check receipt binding is invalid")
        return receipt

    def finalize_check(self, context: DeploymentContext) -> dict[str, Any]:
        return self._check(context, finalized=True)

    def check(self, context: DeploymentContext) -> dict[str, Any]:
        return self._check(context, finalized=False)

    @staticmethod
    def _valid_identity_preflight(row: Mapping[str, Any], domain_name: str) -> bool:
        preflight = row.get("identity_preflight")
        if not isinstance(preflight, dict) or set(preflight) != set(
            DOMAIN_IDENTITY_NODES[domain_name]
        ):
            return False
        for proof in preflight.values():
            if (
                not isinstance(proof, dict)
                or set(proof) != {"status", "receipt_sha256"}
                or proof.get("status") not in {"available", "exact-existing"}
                or re.fullmatch(r"[0-9a-f]{64}", str(proof.get("receipt_sha256"))) is None
            ):
                return False
        return row.get("identity_preflight_sha256") == _digest(preflight)

    @staticmethod
    def _valid_identity_convergence(
        row: Mapping[str, Any],
        domain_name: str,
    ) -> bool:
        convergence = row.get("identity_convergence")
        if not isinstance(convergence, dict) or set(convergence) != set(
            DOMAIN_IDENTITY_NODES[domain_name]
        ):
            return False
        fields = {
            "request_id",
            "result_sha256",
            "authority_receipt_sha256",
            "completed_at",
            "status",
            "readback_receipt_sha256",
        }
        for proof in convergence.values():
            if (
                not isinstance(proof, dict)
                or set(proof) != fields
                or proof.get("status") != "exact-existing"
                or any(
                    re.fullmatch(r"[0-9a-f]{64}", str(proof.get(field))) is None
                    for field in (
                        "request_id",
                        "result_sha256",
                        "authority_receipt_sha256",
                        "readback_receipt_sha256",
                    )
                )
                or not _valid_timestamp(proof.get("completed_at"))
            ):
                return False
        return row.get("identity_convergence_sha256") == _digest(convergence)

    @staticmethod
    def _valid_slurm_convergence(
        row: Mapping[str, Any],
        domain_name: str,
    ) -> bool:
        convergence = row.get("slurm_convergence")
        nodes = DOMAIN_IDENTITY_NODES[domain_name]
        if not isinstance(convergence, dict) or set(convergence) != set(nodes):
            return False
        fields = {
            "action",
            "request_id",
            "result_sha256",
            "authority_receipt_sha256",
            "completed_at",
        }
        for proof in convergence.values():
            if (
                not isinstance(proof, dict)
                or set(proof) != fields
                or proof.get("action") != "slurm-identity-converge"
                or any(
                    re.fullmatch(r"[0-9a-f]{64}", str(proof.get(field))) is None
                    for field in (
                        "request_id",
                        "result_sha256",
                        "authority_receipt_sha256",
                    )
                )
                or not _valid_timestamp(proof.get("completed_at"))
            ):
                return False
        return (
            row.get("slurm_convergence_sha256") == _digest(convergence)
            and row.get("policy_sha256")
            == _digest(
                {node: convergence[node]["result_sha256"] for node in nodes},
            )
            and row.get("authority_receipt_sha256")
            == _digest(
                {
                    "identity_convergence": row["identity_convergence"],
                    "slurm_convergence": convergence,
                },
            )
        )


class FixedDistributedRuntimeAuthority:
    """Drive the fixed root cross-domain authority with registry IDs only."""

    def __init__(
        self,
        runner: CommandRunner,
        *,
        root: Path = RUNTIME_ROOT,
        program: Path = RUNTIME_PROGRAM,
        require_root_metadata: bool = True,
    ) -> None:
        self.runner = runner
        self.root = root
        self.program = program
        self.require_root_metadata = require_root_metadata

    def _request(self, context: DeploymentContext, action: str) -> tuple[Path, dict[str, Any]]:
        if action not in {
            "reconcile",
            "check",
            "acceptance-probe",
            "activate",
            "fence",
            "rollback",
            "retire",
        }:
            raise DeploymentError("distributed runtime action is invalid")
        request = _bound(
            {
                "schema_version": 1,
                "kind": "loom.developer-environment.runtime-request",
                "action": action,
                "deployment_id": context.deployment_id,
                "env_id": context.env_id,
                "principal_id": context.principal_id,
                "runtime_id": context.environment["runtime_id"],
                "candidate_id": context.candidate_id,
                "candidate_sha": context.candidate_sha,
                "candidate_tree": context.candidate_tree,
                "resource_generation": context.runtime_resource_generation,
                "registry_generation": context.snapshot_generation,
                "registry_snapshot_sha256": context.snapshot_digest,
            }
        )
        request_root = self.root / "requests"
        _ensure_directory(
            request_root,
            mode=0o700,
            uid=0,
            gid=0,
            manage_ownership=self.require_root_metadata,
        )
        path = request_root / f"{context.deployment_id}-{action}.json"
        _atomic_write(
            path,
            _canonical(request),
            mode=0o600,
            uid=0,
            gid=0,
            manage_ownership=self.require_root_metadata,
        )
        return path, request

    def _invoke(self, context: DeploymentContext, action: str) -> dict[str, Any]:
        _request_path, request = self._request(context, action)
        self.runner.run(
            (
                str(self.program),
                action,
                "--deployment-id",
                context.deployment_id,
            )
        )
        receipt_path = (
            self.root / "acceptance-probes" / context.deployment_id / "combined.json"
            if action == "acceptance-probe"
            else self.root / "receipts" / f"{context.deployment_id}-{action}.json"
        )
        raw = _read_stable_regular(
            receipt_path,
            limit=8 * 1024 * 1024,
            require_root=self.require_root_metadata,
            expected_mode=0o600,
        )
        try:
            receipt = json.loads(raw)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise DeploymentError("distributed runtime receipt is invalid") from exc
        if not isinstance(receipt, dict) or raw != _canonical(receipt):
            raise DeploymentError("distributed runtime receipt is invalid")
        if action == "acceptance-probe":
            try:
                validated = acceptance_probe._validate_combined_receipt(
                    receipt,
                    request=request,
                    environment=context.environment,
                    candidate=context.candidate,
                    deployment=context.deployment,
                )
            except acceptance_probe.AcceptanceProbeError as exc:
                raise DeploymentError(
                    "distributed runtime acceptance receipt binding is invalid"
                ) from exc
            if validated != receipt:
                raise DeploymentError("distributed runtime acceptance receipt binding is invalid")
            return receipt
        unsigned = {key: value for key, value in receipt.items() if key != "payload_sha256"}
        expected_nodes = [
            node for domain in ("oldlab", "gb10") for node in DOMAIN_RUNTIME_NODES[domain]
        ]
        effective_candidate = (
            context.effective_candidate if action == "rollback" else context.candidate
        )
        retired_runtime = action in {"activate", "fence", "retire"} or (
            action == "rollback" and effective_candidate is None
        )
        binding_deployment = (
            context.effective_deployment
            if action == "rollback" and effective_candidate is not None
            else context.deployment
        )
        runtime_bindings = (
            binding_deployment.get("worker_runtime_bindings")
            if isinstance(binding_deployment, dict)
            else None
        )
        domains = runtime_bindings.get("domains") if isinstance(runtime_bindings, dict) else None
        expected_worker_image_ids = (
            {domain: str(domains[domain]["runtime_image_id"]) for domain in ("oldlab", "gb10")}
            if not (action == "rollback" and effective_candidate is None)
            and isinstance(domains, dict)
            and all(
                isinstance(domains.get(domain), dict)
                and re.fullmatch(
                    r"sha256:[0-9a-f]{64}",
                    str(domains[domain].get("runtime_image_id")),
                )
                is not None
                for domain in ("oldlab", "gb10")
            )
            else {}
        )
        if not retired_runtime and set(expected_worker_image_ids) != {"oldlab", "gb10"}:
            raise DeploymentError("distributed runtime worker image binding is invalid")
        shared_capacity = receipt.get("shared_capacity")
        expected_instances = [
            f"{context.environment['runtime_id']}-{domain}" for domain in ("oldlab", "gb10")
        ]
        shared_capacity_valid = isinstance(shared_capacity, dict) and (
            (
                set(shared_capacity)
                == {
                    "schema_version",
                    "status",
                    "runtime_id",
                    "instances",
                }
                and shared_capacity.get("schema_version") == 1
                and shared_capacity.get("status") == "ready"
                and shared_capacity.get("runtime_id") == context.environment["runtime_id"]
                and shared_capacity.get("instances") == expected_instances
            )
            if retired_runtime
            else (
                set(shared_capacity)
                == {
                    "schema_version",
                    "status",
                    "runtime_id",
                    "candidate_id",
                    "candidate_sha",
                    "candidate_tree",
                    "resource_generation",
                    "registry_generation",
                    "registry_payload_sha256",
                    "instances",
                    "activation_status",
                }
                and shared_capacity.get("schema_version") == 1
                and shared_capacity.get("status") in {"prepared", "ready"}
                and shared_capacity.get("runtime_id") == context.environment["runtime_id"]
                and shared_capacity.get("candidate_id")
                == (None if effective_candidate is None else effective_candidate["candidate_id"])
                and shared_capacity.get("candidate_sha")
                == (None if effective_candidate is None else effective_candidate["candidate_sha"])
                and shared_capacity.get("candidate_tree")
                == (None if effective_candidate is None else effective_candidate["candidate_tree"])
                and shared_capacity.get("resource_generation")
                == context.runtime_resource_generation
                and shared_capacity.get("registry_generation") == context.snapshot_generation
                and shared_capacity.get("registry_payload_sha256") == context.snapshot_digest
                and shared_capacity.get("instances") == expected_instances
                and (
                    (
                        shared_capacity.get("status") == "prepared"
                        and shared_capacity.get("activation_status") == "installed"
                    )
                    or (
                        shared_capacity.get("status") == "ready"
                        and shared_capacity.get("activation_status")
                        in {
                            "bootstrap-active",
                            "acceptance-active",
                            "activated",
                        }
                    )
                )
            )
        )
        if (
            set(receipt)
            != {
                "schema_version",
                "kind",
                "status",
                "action",
                "deployment_id",
                "env_id",
                "runtime_id",
                "candidate_id",
                "candidate_sha",
                "candidate_tree",
                "effective_candidate_id",
                "effective_candidate_sha",
                "effective_candidate_tree",
                "resource_generation",
                "registry_generation",
                "registry_snapshot_sha256",
                "request_sha256",
                "domains",
                "worker_image_ids",
                "nodes",
                "remote_link",
                "domain_runtime",
                "shared_capacity",
                "completed_at",
                "payload_sha256",
            }
            or receipt.get("schema_version") != 1
            or receipt.get("kind") != "loom.developer-environment.runtime-receipt"
            or receipt.get("status")
            != (shared_capacity.get("status") if isinstance(shared_capacity, dict) else None)
            or receipt.get("action") != action
            or receipt.get("deployment_id") != context.deployment_id
            or receipt.get("env_id") != context.env_id
            or receipt.get("runtime_id") != context.environment["runtime_id"]
            or receipt.get("candidate_id") != context.candidate_id
            or receipt.get("candidate_sha") != context.candidate_sha
            or receipt.get("candidate_tree") != context.candidate_tree
            or receipt.get("effective_candidate_id")
            != (None if effective_candidate is None else effective_candidate["candidate_id"])
            or receipt.get("effective_candidate_sha")
            != (None if effective_candidate is None else effective_candidate["candidate_sha"])
            or receipt.get("effective_candidate_tree")
            != (None if effective_candidate is None else effective_candidate["candidate_tree"])
            or receipt.get("resource_generation") != context.runtime_resource_generation
            or receipt.get("registry_generation") != context.snapshot_generation
            or receipt.get("registry_snapshot_sha256") != context.snapshot_digest
            or receipt.get("request_sha256") != request["payload_sha256"]
            or receipt.get("domains") != ["oldlab", "gb10"]
            or receipt.get("worker_image_ids") != expected_worker_image_ids
            or receipt.get("nodes") != expected_nodes
            or receipt.get("remote_link") != {"status": "ready"}
            or receipt.get("domain_runtime") != {"status": "ready"}
            or not shared_capacity_valid
            or not _valid_timestamp(receipt.get("completed_at"))
            or receipt.get("payload_sha256") != _digest(unsigned)
        ):
            raise DeploymentError("distributed runtime receipt binding is invalid")
        return receipt

    def reconcile(self, context: DeploymentContext) -> dict[str, Any]:
        return self._invoke(context, "reconcile")

    def check(self, context: DeploymentContext) -> dict[str, Any]:
        return self._invoke(context, "check")

    def acceptance_probe(self, context: DeploymentContext) -> dict[str, Any]:
        return self._invoke(context, "acceptance-probe")

    def activate(self, context: DeploymentContext) -> dict[str, Any]:
        return self._invoke(context, "activate")

    def fence(self, context: DeploymentContext) -> dict[str, Any]:
        return self._invoke(context, "fence")

    def rollback(self, context: DeploymentContext) -> dict[str, Any]:
        return self._invoke(context, "rollback")

    def retire(self, context: DeploymentContext) -> dict[str, Any]:
        return self._invoke(context, "retire")


class DeveloperEnvironmentDeployer:
    def __init__(
        self,
        registry: RegistryAuthority,
        *,
        snapshot_path: Path = SYSTEM_SNAPSHOT,
        runner: CommandRunner | None = None,
        require_root_metadata: bool = True,
        manage_ownership: bool = True,
        expected_hostname: str = EXPECTED_HOSTNAME,
        host_root: Path | None = None,
        capacity_authority: CapacityAuthority | None = None,
        distributed_runtime_authority: DistributedRuntimeAuthority | None = None,
        runtime_retire_executor: Callable[[str, str, str], dict[str, Any]] | None = None,
        environment_admission_fence: Callable[
            [str, str],
            dict[str, Any],
        ]
        | None = None,
    ) -> None:
        self.registry = registry
        self.snapshot_path = snapshot_path
        self.runner = runner or SubprocessRunner()
        self.require_root_metadata = require_root_metadata
        self.manage_ownership = manage_ownership
        self.expected_hostname = expected_hostname
        self.host_root = host_root
        self.capacity_authority = capacity_authority or FixedCapacityAuthority(
            self.runner,
            require_root_metadata=require_root_metadata,
        )
        self.distributed_runtime_authority = (
            distributed_runtime_authority
            or FixedDistributedRuntimeAuthority(
                self.runner,
                require_root_metadata=require_root_metadata,
            )
        )
        self.runtime_retire_executor = (
            runtime_retire_executor
            if runtime_retire_executor is not None
            else lambda deployment_id, env_id, operation_sha256: runtime_retire.execute(
                deployment_id,
                env_id,
                operation_sha256,
                runtime_root=self._global_runtime_path(),
                registry_snapshot=self.snapshot_path,
                require_root_ownership=self.require_root_metadata,
            )
        )
        self.environment_admission_fence = (
            environment_admission_fence
            if environment_admission_fence is not None
            else capacity_runtime_host.fence_registry_environment_intent
        )

    def renew_active(self) -> dict[str, Any]:
        """Renew the complete active registry cohort through the fixed node authority."""

        self._require_host()
        snapshot = self._snapshot()
        candidates = {
            row["candidate_id"]: row for row in cast(list[dict[str, Any]], snapshot["candidates"])
        }
        deployments = cast(list[dict[str, Any]], snapshot["deployments"])
        environment_by_id: dict[str, dict[str, Any]] = {}
        cohort: list[dict[str, Any]] = []
        for environment in cast(list[dict[str, Any]], snapshot["environments"]):
            candidate_id = environment["current_candidate_id"]
            if environment["state"] != "active" or candidate_id is None:
                continue
            committed = [
                row
                for row in deployments
                if row["env_id"] == environment["env_id"]
                and row["phase"] == "committed"
                and row["candidate_id"] == candidate_id
                and row["applied_resource_generation"] == environment["resource_generation"]
            ]
            candidate = candidates.get(candidate_id)
            if not committed or candidate is None:
                raise DeploymentError("active attestation cohort binding is invalid")
            latest = max(
                committed,
                key=lambda row: (
                    row["expected_resource_generation"],
                    row["updated_at"],
                    row["deployment_id"],
                ),
            )
            cohort.append(
                {
                    "env_id": environment["env_id"],
                    "principal_id": environment["principal_id"],
                    "runtime_id": environment["runtime_id"],
                    "resource_generation": environment["resource_generation"],
                    "deployment_id": latest["deployment_id"],
                    "candidate_id": candidate_id,
                    "candidate_sha": candidate["candidate_sha"],
                    "candidate_tree": candidate["candidate_tree"],
                    "slurm_user": environment["slurm_user"],
                    "slurm_account": environment["slurm_account"],
                    "slurm_qos": environment["slurm_qos"],
                }
            )
            environment_by_id[cast(str, environment["env_id"])] = environment
        cohort.sort(key=lambda row: row["env_id"])
        if not cohort:
            raise DeploymentError("active attestation cohort is empty")
        from scripts.ops import developer_sandbox_host as legacy_host

        renewed: list[str] = []
        for binding in cohort:
            environment = environment_by_id[cast(str, binding["env_id"])]
            state_root = Path(cast(str, environment["state_root"]))
            profile = legacy_host.Profile(
                sandbox=cast(str, environment["runtime_id"]),
                compose_project=cast(str, environment["compose_project"]),
                canonical_hostname=EXPECTED_HOSTNAME,
                candidate_root=Path(cast(str, environment["candidate_root"])),
                state_root=state_root,
                cache_root=state_root / "cache",
                evidence_root=Path(cast(str, environment["evidence_root"])),
                runtime_root=Path(cast(str, environment["runtime_root"])),
                ports={
                    str(name): int(port)
                    for name, port in cast(
                        dict[str, int],
                        environment["ports"],
                    ).items()
                },
                env_id=cast(str, environment["env_id"]),
                resource_generation=cast(int, environment["resource_generation"]),
                registry_generation=cast(int, snapshot["generation"]),
                registry_payload_sha256=cast(str, snapshot["payload_sha256"]),
                candidate_id=cast(str, binding["candidate_id"]),
                candidate_tree=cast(str, binding["candidate_tree"]),
            )
            try:
                fleet = legacy_host._collect_and_persist_remote_link_fleet(
                    profile,
                    cast(str, binding["candidate_sha"]),
                    cast(str, binding["candidate_tree"]),
                )
            except legacy_host.HostConvergeError as exc:
                raise DeploymentError("dynamic fleet attestation renewal failed") from exc
            if (
                fleet.get("env_id") != environment["env_id"]
                or fleet.get("resource_generation") != environment["resource_generation"]
                or fleet.get("registry_generation") != snapshot["generation"]
                or fleet.get("registry_payload_sha256") != snapshot["payload_sha256"]
                or fleet.get("candidate_sha") != binding["candidate_sha"]
                or fleet.get("candidate_tree") != binding["candidate_tree"]
                or set(fleet.get("nodes", {}))
                != set(legacy_host.DOMAIN_PEERS["oldlab"]) | set(legacy_host.DOMAIN_PEERS["gb10"])
            ):
                raise DeploymentError("dynamic fleet attestation binding drifted")
            renewed.append(cast(str, environment["env_id"]))
        return {
            "schema_version": 1,
            "kind": "loom.developer-environment.attestation-renewal-result",
            "status": "renewed",
            "registry_generation": snapshot["generation"],
            "registry_snapshot_sha256": snapshot["payload_sha256"],
            "environment_count": len(renewed),
            "env_ids": renewed,
        }

    def _snapshot(self) -> dict[str, Any]:
        if self.require_root_metadata:
            try:
                self.registry.reconcile_snapshot()
            except RegistryError as exc:
                raise DeploymentError("registry snapshot recovery failed safely") from exc
            return load_registry_snapshot(self.snapshot_path, require_root=True)
        try:
            return DeveloperEnvironmentRegistry.verify_snapshot(self.registry.snapshot_bytes())
        except RegistryError as exc:
            raise DeploymentError("registry snapshot verification failed") from exc

    def _environment(
        self,
        *,
        env_id: str | None,
        principal_id: str | None,
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        snapshot = self._snapshot()
        environment = select_environment(
            snapshot,
            principal_id=principal_id,
            env_id=env_id,
            root=principal_id is None and os.geteuid() == 0,
        )
        return snapshot, environment

    @contextmanager
    def _lock(self, environment: Mapping[str, Any]) -> Iterator[None]:
        env_id = cast(str, environment["env_id"])
        lifecycle_root = self._root_lifecycle_directory("environments", env_id)
        path = lifecycle_root / "deployment.lock"
        descriptor = -1
        try:
            descriptor = os.open(
                path,
                os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW | getattr(os, "O_CLOEXEC", 0),
                0o600,
            )
            metadata = os.fstat(descriptor)
            if (
                not stat.S_ISREG(metadata.st_mode)
                or metadata.st_nlink != 1
                or (self.manage_ownership and (metadata.st_uid, metadata.st_gid) != (0, 0))
            ):
                raise DeploymentError("deployment lock is unsafe")
            os.fchmod(descriptor, 0o600)
            if self.manage_ownership:
                os.fchown(descriptor, 0, 0)
            fcntl.flock(descriptor, fcntl.LOCK_EX)
            locked = os.fstat(descriptor)
            current = path.lstat()
            if (
                (locked.st_dev, locked.st_ino) != (current.st_dev, current.st_ino)
                or locked.st_nlink != 1
                or current.st_nlink != 1
                or not stat.S_ISREG(current.st_mode)
                or stat.S_ISLNK(current.st_mode)
                or stat.S_IMODE(locked.st_mode) != 0o600
                or stat.S_IMODE(current.st_mode) != 0o600
                or (
                    self.manage_ownership
                    and (
                        (locked.st_uid, locked.st_gid) != (0, 0)
                        or (current.st_uid, current.st_gid) != (0, 0)
                    )
                )
            ):
                raise DeploymentError("deployment lock identity drifted")
            yield
        except DeploymentError:
            raise
        except OSError as exc:
            raise DeploymentError("deployment lock is unavailable") from exc
        finally:
            if descriptor >= 0:
                try:
                    fcntl.flock(descriptor, fcntl.LOCK_UN)
                except OSError:
                    pass
                os.close(descriptor)

    def _refresh_context(
        self,
        env_id: str,
        principal_id: str,
        deployment_id: str,
    ) -> DeploymentContext:
        snapshot = self._snapshot()
        environment = select_environment(
            snapshot,
            principal_id=principal_id,
            env_id=env_id,
            root=False,
        )
        return _context(
            snapshot,
            environment,
            deployment_id=deployment_id,
            host_root=self.host_root,
        )

    def _refresh_committed_context(
        self,
        env_id: str,
        principal_id: str,
        deployment_id: str,
    ) -> DeploymentContext:
        snapshot = self._snapshot()
        environment = select_environment(
            snapshot,
            principal_id=principal_id,
            env_id=env_id,
            root=False,
        )
        deployments = [
            row
            for row in cast(list[dict[str, Any]], snapshot["deployments"])
            if row["deployment_id"] == deployment_id
            and row["env_id"] == env_id
            and row["phase"] == "committed"
            and row["applied_resource_generation"] == environment["resource_generation"]
        ]
        if len(deployments) != 1:
            raise DeploymentError("committed developer deployment is unavailable")
        candidates = [
            row
            for row in cast(list[dict[str, Any]], snapshot["candidates"])
            if row["candidate_id"] == deployments[0]["candidate_id"]
        ]
        if len(candidates) != 1:
            raise DeploymentError("committed developer candidate is unavailable")
        return DeploymentContext(
            cast(int, snapshot["generation"]),
            cast(str, snapshot["payload_sha256"]),
            environment,
            candidates[0],
            deployments[0],
            self.host_root,
        )

    def _refresh_failed_context(
        self,
        env_id: str,
        principal_id: str,
        deployment_id: str,
    ) -> DeploymentContext:
        snapshot = self._snapshot()
        environment = select_environment(
            snapshot,
            principal_id=principal_id,
            env_id=env_id,
            root=False,
        )
        deployments = [
            row
            for row in cast(list[dict[str, Any]], snapshot["deployments"])
            if row["deployment_id"] == deployment_id
            and row["env_id"] == env_id
            and row["phase"] == "failed"
        ]
        if len(deployments) != 1:
            raise DeploymentError("failed developer deployment is unavailable")
        candidates = [
            row
            for row in cast(list[dict[str, Any]], snapshot["candidates"])
            if row["candidate_id"] == deployments[0]["candidate_id"]
        ]
        if len(candidates) != 1:
            raise DeploymentError("failed developer candidate is unavailable")
        effective_candidate = next(
            (
                row
                for row in cast(list[dict[str, Any]], snapshot["candidates"])
                if row["candidate_id"] == environment["current_candidate_id"]
            ),
            None,
        )
        effective_deployment = (
            max(
                (
                    row
                    for row in cast(list[dict[str, Any]], snapshot["deployments"])
                    if row["env_id"] == env_id
                    and row["candidate_id"] == environment["current_candidate_id"]
                    and row["phase"] == "committed"
                ),
                key=lambda row: (
                    row["applied_resource_generation"],
                    row["deployment_id"],
                ),
                default=None,
            )
            if effective_candidate is not None
            else None
        )
        if (effective_candidate is None) != (effective_deployment is None):
            raise DeploymentError("failed deployment effective runtime binding is invalid")
        return DeploymentContext(
            cast(int, snapshot["generation"]),
            cast(str, snapshot["payload_sha256"]),
            environment,
            candidates[0],
            deployments[0],
            self.host_root,
            effective_candidate,
            effective_deployment,
        )

    def _journal(self, context: DeploymentContext, phase: str) -> dict[str, Any]:
        previous = _load_bound_json(context.journal_path, kind=JOURNAL_KIND)
        if previous is not None:
            old_deployment = previous.get("deployment_id")
            old_phase = previous.get("phase")
            if old_deployment != context.deployment_id and old_phase not in {
                "committed",
                "failed",
                "retired",
            }:
                raise DeploymentError("another deployment journal is active")
        payload = _bound(
            {
                "schema_version": JOURNAL_VERSION,
                "kind": JOURNAL_KIND,
                "env_id": context.env_id,
                "principal_id": context.principal_id,
                "deployment_id": context.deployment_id,
                "candidate_id": context.candidate_id,
                "candidate_sha": context.candidate_sha,
                "candidate_tree": context.candidate_tree,
                "resource_generation": context.runtime_resource_generation,
                "registry_snapshot_sha256": context.snapshot_digest,
                "image_digest": context.image_digest,
                "phase": phase,
            }
        )
        _atomic_write(
            context.journal_path,
            _canonical(payload),
            mode=0o600,
            uid=0,
            gid=0,
            manage_ownership=self.manage_ownership,
        )
        return payload

    def _rollback_operation_path(self, environment: Mapping[str, Any]) -> Path:
        return self._global_runtime_path(
            "lifecycle",
            "environments",
            cast(str, environment["env_id"]),
            "rollback-operation.json",
        )

    def _global_runtime_path(self, *parts: str) -> Path:
        path = RUNTIME_ROOT.joinpath(*parts)
        if self.host_root is None:
            return path
        return self.host_root.joinpath(*path.parts[1:])

    def _root_lifecycle_directory(self, *parts: str) -> Path:
        current = self._global_runtime_path()
        for part in ("lifecycle", *parts):
            _ensure_directory(
                current,
                mode=0o700,
                uid=0,
                gid=0,
                manage_ownership=self.manage_ownership,
            )
            current /= part
        _ensure_directory(
            current,
            mode=0o700,
            uid=0,
            gid=0,
            manage_ownership=self.manage_ownership,
        )
        return current

    def _admission_intent_path(self, runtime_id: str) -> Path:
        if RUNTIME_ID_RE.fullmatch(runtime_id) is None:
            raise DeploymentError("admission intent runtime binding is invalid")
        return self._global_runtime_path(
            "lifecycle",
            "admission",
            f"{runtime_id}.json",
        )

    def _write_admission_intent(
        self,
        snapshot: Mapping[str, Any],
        environment: Mapping[str, Any],
        *,
        operation: str,
        candidate_id: str | None,
        idempotency_key: str,
        phase: str,
        fence_receipt_sha256: str | None = None,
        admission_token: str | None = None,
    ) -> dict[str, Any]:
        if (
            operation not in {"create", "update", "retire"}
            or phase not in {"recorded", "fenced", "registry-transitioned", "activated", "retired"}
            or SAFE_ID_RE.fullmatch(idempotency_key) is None
            or (
                operation != "retire"
                and (candidate_id is None or CANDIDATE_ID_RE.fullmatch(candidate_id) is None)
            )
            or (operation == "retire" and candidate_id is not None)
        ):
            raise DeploymentError("admission intent binding is invalid")
        runtime_id = cast(str, environment["runtime_id"])
        path = self._admission_intent_path(runtime_id)
        _ensure_directory(
            path.parent,
            mode=0o700,
            uid=0,
            gid=0,
            manage_ownership=self.manage_ownership,
        )
        previous = _load_bound_json(path, kind=ADMISSION_INTENT_KIND)
        replacing_previous = previous is not None and (
            previous.get("operation") != operation
            or previous.get("target_candidate_id") != candidate_id
            or previous.get("resource_generation") != environment["resource_generation"]
            or previous.get("idempotency_key") != idempotency_key
        )
        prior_admission_token = (
            previous.get("admission_token")
            if replacing_previous and previous is not None
            else previous.get("prior_admission_token")
            if previous is not None
            else None
        )
        immutable = {
            "env_id": environment["env_id"],
            "principal_id": environment["principal_id"],
            "runtime_id": runtime_id,
            "operation": operation,
            "target_candidate_id": candidate_id,
            "current_candidate_id": environment["current_candidate_id"],
            "resource_generation": environment["resource_generation"],
            "expected_resource_generation": environment["resource_generation"],
            "applied_resource_generation": (cast(int, environment["resource_generation"]) + 1),
            "idempotency_key": idempotency_key,
            "registry_generation": snapshot["generation"],
            "registry_payload_sha256": snapshot["payload_sha256"],
            "prior_admission_token": prior_admission_token,
        }
        replay_core_fields = (
            "env_id",
            "principal_id",
            "runtime_id",
            "operation",
            "target_candidate_id",
            "current_candidate_id",
            "resource_generation",
            "expected_resource_generation",
            "applied_resource_generation",
            "idempotency_key",
        )
        if (
            previous is not None
            and previous.get("phase") not in {"activated", "retired"}
            and all(previous.get(field) == immutable[field] for field in replay_core_fields)
        ):
            # Peer registry changes must not replace an already persisted
            # exact-env intent. Reuse its original snapshot binding and token.
            immutable["registry_generation"] = previous["registry_generation"]
            immutable["registry_payload_sha256"] = previous["registry_payload_sha256"]
            immutable["prior_admission_token"] = previous.get(
                "prior_admission_token",
            )
        intent_sha256 = _digest(immutable)
        phase_order = (
            "recorded",
            "fenced",
            "registry-transitioned",
            "activated",
            "retired",
        )
        if previous is not None:
            previous_immutable = {field: previous.get(field) for field in immutable}
            if (
                previous.get("phase") not in {"activated", "retired"}
                and previous_immutable != immutable
            ):
                raise DeploymentError("another admission intent is active")
            if previous_immutable == immutable:
                previous_phase = str(previous.get("phase"))
                if previous_phase not in phase_order:
                    raise DeploymentError("admission intent phase is invalid")
                if phase_order.index(phase) < phase_order.index(previous_phase):
                    phase = previous_phase
                if fence_receipt_sha256 is None:
                    prior_receipt = previous.get("fence_receipt_sha256")
                    fence_receipt_sha256 = (
                        cast(str, prior_receipt) if isinstance(prior_receipt, str) else None
                    )
                if admission_token is None:
                    prior_current_token = previous.get("admission_token")
                    admission_token = (
                        cast(str, prior_current_token)
                        if isinstance(prior_current_token, str)
                        else None
                    )
        if phase != "recorded" and (
            fence_receipt_sha256 is None
            or re.fullmatch(r"[0-9a-f]{64}", fence_receipt_sha256) is None
            or admission_token is None
            or re.fullmatch(r"[0-9a-f]{32}", admission_token) is None
        ):
            raise DeploymentError("admission fence receipt binding is invalid")
        now = datetime.now(UTC).isoformat().replace("+00:00", "Z")
        payload = _bound(
            {
                "schema_version": 1,
                "kind": ADMISSION_INTENT_KIND,
                **immutable,
                "intent_sha256": intent_sha256,
                "phase": phase,
                "fence_receipt_sha256": fence_receipt_sha256,
                "admission_token": admission_token,
                "created_at": (
                    previous["created_at"]
                    if previous is not None and previous_immutable == immutable
                    else now
                ),
                "updated_at": now,
            }
        )
        _atomic_write(
            path,
            _canonical(payload),
            mode=0o600,
            uid=0,
            gid=0,
            manage_ownership=self.manage_ownership,
        )
        return payload

    def _fence_admission_intent(
        self,
        snapshot: Mapping[str, Any],
        environment: Mapping[str, Any],
        *,
        operation: str,
        candidate_id: str | None,
        idempotency_key: str,
    ) -> dict[str, Any]:
        intent = self._write_admission_intent(
            snapshot,
            environment,
            operation=operation,
            candidate_id=candidate_id,
            idempotency_key=idempotency_key,
            phase="recorded",
        )
        receipt = self.environment_admission_fence(
            cast(str, environment["runtime_id"]),
            cast(str, intent["intent_sha256"]),
        )
        receipt_sha256 = _evidence_digest(receipt)
        admission_token = receipt.get("admission_token")
        if (
            not isinstance(admission_token, str)
            or re.fullmatch(r"[0-9a-f]{32}", admission_token) is None
        ):
            raise DeploymentError("admission fence token binding is invalid")
        return self._write_admission_intent(
            snapshot,
            environment,
            operation=operation,
            candidate_id=candidate_id,
            idempotency_key=idempotency_key,
            phase="fenced",
            fence_receipt_sha256=receipt_sha256,
            admission_token=admission_token,
        )

    def _advance_admission_intent(
        self,
        runtime_id: str,
        *,
        phase: str,
        deployment_id: str | None = None,
        finalization_payload_sha256: str | None = None,
    ) -> dict[str, Any]:
        path = self._admission_intent_path(runtime_id)
        previous = _load_bound_json(path, kind=ADMISSION_INTENT_KIND)
        phases = ("recorded", "fenced", "registry-transitioned", "activated", "retired")
        if (
            previous is None
            or phase not in phases
            or previous.get("phase") not in phases
            or (
                phase != "recorded"
                and re.fullmatch(
                    r"[0-9a-f]{64}",
                    str(previous.get("fence_receipt_sha256")),
                )
                is None
            )
        ):
            raise DeploymentError("admission intent cannot advance")
        if phases.index(phase) < phases.index(cast(str, previous["phase"])):
            phase = cast(str, previous["phase"])
        unsigned = dict(previous)
        unsigned.pop("payload_sha256")
        existing_deployment_id = unsigned.get("deployment_id")
        if deployment_id is None:
            deployment_id = (
                cast(str, existing_deployment_id)
                if isinstance(existing_deployment_id, str)
                else None
            )
        if deployment_id is not None and (
            not deployment_id.startswith("dep-")
            or SAFE_ID_RE.fullmatch(deployment_id) is None
            or (existing_deployment_id is not None and existing_deployment_id != deployment_id)
        ):
            raise DeploymentError("admission deployment binding is invalid")
        existing_finalization = unsigned.get("finalization_payload_sha256")
        if finalization_payload_sha256 is None:
            finalization_payload_sha256 = (
                cast(str, existing_finalization) if isinstance(existing_finalization, str) else None
            )
        if finalization_payload_sha256 is not None and (
            re.fullmatch(r"[0-9a-f]{64}", finalization_payload_sha256) is None
            or (
                existing_finalization is not None
                and existing_finalization != finalization_payload_sha256
            )
        ):
            raise DeploymentError("admission finalization binding is invalid")
        unsigned["deployment_id"] = deployment_id
        unsigned["finalization_payload_sha256"] = finalization_payload_sha256
        unsigned["phase"] = phase
        unsigned["updated_at"] = datetime.now(UTC).isoformat().replace("+00:00", "Z")
        payload = _bound(unsigned)
        _atomic_write(
            path,
            _canonical(payload),
            mode=0o600,
            uid=0,
            gid=0,
            manage_ownership=self.manage_ownership,
        )
        return payload

    def _retire_operation_path(
        self,
        env_id: str,
        *,
        expected_resource_generation: int | None = None,
        idempotency_key: str | None = None,
    ) -> Path:
        if expected_resource_generation is None and idempotency_key is None:
            return self._global_runtime_path("lifecycle", "retire", f"{env_id}.json")
        if (
            type(expected_resource_generation) is not int
            or cast(int, expected_resource_generation) < 1
            or idempotency_key is None
            or SAFE_ID_RE.fullmatch(idempotency_key) is None
        ):
            raise DeploymentError("retirement operation path binding is invalid")
        key_digest = hashlib.sha256(idempotency_key.encode("ascii")).hexdigest()
        return self._global_runtime_path(
            "lifecycle",
            "retire",
            env_id,
            f"{expected_resource_generation}-{key_digest}-journal.json",
        )

    @contextmanager
    def _retirement_lock(self, env_id: str) -> Iterator[None]:
        lock_root = self._root_lifecycle_directory("locks")
        path = lock_root / f"{env_id}.retire.lock"
        descriptor = -1
        try:
            descriptor = os.open(
                path,
                os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW | getattr(os, "O_CLOEXEC", 0),
                0o600,
            )
            metadata = os.fstat(descriptor)
            if (
                not stat.S_ISREG(metadata.st_mode)
                or metadata.st_nlink != 1
                or (self.manage_ownership and (metadata.st_uid, metadata.st_gid) != (0, 0))
            ):
                raise DeploymentError("retirement lock is unsafe")
            os.fchmod(descriptor, 0o600)
            if self.manage_ownership:
                os.fchown(descriptor, 0, 0)
            fcntl.flock(descriptor, fcntl.LOCK_EX)
            locked = os.fstat(descriptor)
            current = path.lstat()
            if (
                (locked.st_dev, locked.st_ino) != (current.st_dev, current.st_ino)
                or locked.st_nlink != 1
                or current.st_nlink != 1
                or not stat.S_ISREG(current.st_mode)
                or stat.S_ISLNK(current.st_mode)
                or stat.S_IMODE(locked.st_mode) != 0o600
                or stat.S_IMODE(current.st_mode) != 0o600
                or (
                    self.manage_ownership
                    and (
                        (locked.st_uid, locked.st_gid) != (0, 0)
                        or (current.st_uid, current.st_gid) != (0, 0)
                    )
                )
            ):
                raise DeploymentError("retirement lock identity drifted")
            yield
        except DeploymentError:
            raise
        except OSError as exc:
            raise DeploymentError("retirement lock is unavailable") from exc
        finally:
            if descriptor >= 0:
                try:
                    fcntl.flock(descriptor, fcntl.LOCK_UN)
                except OSError:
                    pass
                os.close(descriptor)

    def _cleanup_receipt_path(
        self,
        env_id: str,
        *,
        expected_resource_generation: int | None = None,
        idempotency_key: str | None = None,
    ) -> Path:
        if expected_resource_generation is None and idempotency_key is None:
            return self._global_runtime_path(
                "lifecycle",
                "retire",
                f"{env_id}-cleanup.json",
            )
        if (
            type(expected_resource_generation) is not int
            or cast(int, expected_resource_generation) < 1
            or idempotency_key is None
            or SAFE_ID_RE.fullmatch(idempotency_key) is None
        ):
            raise DeploymentError("cleanup receipt path binding is invalid")
        key_digest = hashlib.sha256(idempotency_key.encode("ascii")).hexdigest()
        return self._global_runtime_path(
            "lifecycle",
            "retire",
            env_id,
            f"{expected_resource_generation}-{key_digest}-cleanup.json",
        )

    def _revive_operation_path(
        self,
        env_id: str,
        *,
        new_resource_generation: int | None = None,
        idempotency_key: str | None = None,
    ) -> Path:
        if new_resource_generation is None and idempotency_key is None:
            # Mutable current receipt for fixed Slurm reactivation consumers.
            return self._global_runtime_path("revive", f"{env_id}.json")
        if (
            type(new_resource_generation) is not int
            or cast(int, new_resource_generation) < 1
            or idempotency_key is None
            or SAFE_ID_RE.fullmatch(idempotency_key) is None
        ):
            raise DeploymentError("revival operation path binding is invalid")
        key_digest = hashlib.sha256(idempotency_key.encode("ascii")).hexdigest()
        return self._global_runtime_path(
            "revive",
            env_id,
            f"{new_resource_generation}-{key_digest}.json",
        )

    def _write_retire_operation(
        self,
        environment: Mapping[str, Any],
        *,
        idempotency_key: str,
        phase: str,
        expected_resource_generation: int,
        evidence: Mapping[str, str],
        object_checkpoints: Mapping[str, Mapping[str, Any]] | None = None,
    ) -> dict[str, Any]:
        phases = (
            "intent-recorded",
            "admission-fenced",
            "quarantined",
            "capacity-retired",
            "runtime-retired",
            "local-cleaned",
            "registry-retired",
        )
        if phase not in phases or SAFE_ID_RE.fullmatch(idempotency_key) is None:
            raise DeploymentError("retirement operation binding is invalid")
        checkpoints = {
            str(name): dict(checkpoint) for name, checkpoint in (object_checkpoints or {}).items()
        }
        if not set(checkpoints).issubset(RETIRE_LOCAL_OBJECTS) or any(
            checkpoint.get("kind") != "loom.developer-environment.retire-object-checkpoint"
            or checkpoint.get("object") != name
            or checkpoint.get("status") not in RETIRE_OBJECT_STATUSES
            or re.fullmatch(
                r"[0-9a-f]{64}",
                str(checkpoint.get("payload_sha256")),
            )
            is None
            for name, checkpoint in checkpoints.items()
        ):
            raise DeploymentError("retirement object checkpoints are invalid")
        env_id = cast(str, environment["env_id"])
        path = self._retire_operation_path(
            env_id,
            expected_resource_generation=expected_resource_generation,
            idempotency_key=idempotency_key,
        )
        _ensure_directory(
            path.parent,
            mode=0o700,
            uid=0,
            gid=0,
            manage_ownership=self.manage_ownership,
        )
        previous = _load_bound_json(path, kind=RETIRE_KIND)
        if previous is not None:
            previous_evidence = previous.get("evidence")
            previous_checkpoints = previous.get("object_checkpoints", {})
            if (
                previous.get("env_id") != environment["env_id"]
                or previous.get("principal_id") != environment["principal_id"]
                or previous.get("runtime_id") != environment["runtime_id"]
                or previous.get("idempotency_key") != idempotency_key
                or previous.get("expected_resource_generation") != expected_resource_generation
                or phases.index(cast(str, previous.get("phase"))) > phases.index(phase)
                or not isinstance(previous_evidence, dict)
                or any(evidence.get(key) != value for key, value in previous_evidence.items())
                or not isinstance(previous_checkpoints, dict)
                or any(checkpoints.get(key) != value for key, value in previous_checkpoints.items())
            ):
                raise DeploymentError("retirement operation replay drifted")
        now = datetime.now(UTC).isoformat().replace("+00:00", "Z")
        payload = _bound(
            {
                "schema_version": 1,
                "kind": RETIRE_KIND,
                "phase": phase,
                "env_id": environment["env_id"],
                "principal_id": environment["principal_id"],
                "runtime_id": environment["runtime_id"],
                "uid": environment["uid"],
                "gid": environment["gid"],
                "service_user": environment["service_user"],
                "service_group": environment["service_group"],
                "slurm_user": environment["slurm_user"],
                "slurm_account": environment["slurm_account"],
                "slurm_qos": environment["slurm_qos"],
                "expected_resource_generation": expected_resource_generation,
                "current_candidate_id": environment["current_candidate_id"],
                "idempotency_key": idempotency_key,
                "evidence": dict(evidence),
                "object_checkpoints": checkpoints,
                "created_at": (cast(str, previous["created_at"]) if previous is not None else now),
                "updated_at": now,
            }
        )
        _atomic_write(
            path,
            _canonical(payload),
            mode=0o600,
            uid=0,
            gid=0,
            manage_ownership=self.manage_ownership,
        )
        current_path = self._retire_operation_path(env_id)
        _ensure_directory(
            current_path.parent,
            mode=0o700,
            uid=0,
            gid=0,
            manage_ownership=self.manage_ownership,
        )
        _atomic_write(
            current_path,
            _canonical(payload),
            mode=0o600,
            uid=0,
            gid=0,
            manage_ownership=self.manage_ownership,
        )
        return payload

    @staticmethod
    def _retire_object_checkpoint(
        environment: Mapping[str, Any],
        *,
        object_name: str,
        status: str,
        expected_resource_generation: int,
        details: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        if (
            object_name not in RETIRE_LOCAL_OBJECTS
            or status not in RETIRE_OBJECT_STATUSES
            or (object_name == "local_preflight") != (status == "validated")
        ):
            raise DeploymentError("retirement object checkpoint is invalid")
        payload: dict[str, Any] = {
            "schema_version": 1,
            "kind": "loom.developer-environment.retire-object-checkpoint",
            "env_id": environment["env_id"],
            "runtime_id": environment["runtime_id"],
            "expected_resource_generation": expected_resource_generation,
            "object": object_name,
            "status": status,
        }
        if details is not None:
            payload["details"] = dict(details)
        return _bound(payload)

    def _write_cleanup_receipt(
        self,
        environment: Mapping[str, Any],
        *,
        operation: Mapping[str, Any],
        retired_resource_generation: int,
    ) -> dict[str, Any]:
        if (
            operation.get("phase") != "registry-retired"
            or retired_resource_generation != operation.get("expected_resource_generation", 0) + 1
        ):
            raise DeploymentError("cleanup receipt binding is invalid")
        evidence = operation.get("evidence")
        object_checkpoints = operation.get("object_checkpoints")
        if (
            not isinstance(evidence, dict)
            or set(evidence)
            != {
                "admission_fence",
                "capacity_retire",
                "runtime_retire",
                "local_cleanup",
            }
            or any(re.fullmatch(r"[0-9a-f]{64}", str(value)) is None for value in evidence.values())
            or not isinstance(object_checkpoints, dict)
            or set(object_checkpoints) != set(RETIRE_LOCAL_OBJECTS)
            or any(
                not isinstance(checkpoint, dict)
                or checkpoint.get("object") != name
                or checkpoint.get("status") not in RETIRE_OBJECT_STATUSES
                or re.fullmatch(
                    r"[0-9a-f]{64}",
                    str(checkpoint.get("payload_sha256")),
                )
                is None
                for name, checkpoint in object_checkpoints.items()
            )
        ):
            raise DeploymentError("cleanup receipt evidence is invalid")
        payload = _bound(
            {
                "schema_version": 1,
                "kind": RETIRE_RECEIPT_KIND,
                "status": "retired",
                "env_id": operation["env_id"],
                "principal_id": operation["principal_id"],
                "runtime_id": operation["runtime_id"],
                "uid": operation["uid"],
                "gid": operation["gid"],
                "service_user": operation["service_user"],
                "service_group": operation["service_group"],
                "slurm_user": operation["slurm_user"],
                "slurm_account": operation["slurm_account"],
                "slurm_qos": operation["slurm_qos"],
                "retired_candidate_id": operation["current_candidate_id"],
                "previous_resource_generation": operation["expected_resource_generation"],
                "retired_resource_generation": retired_resource_generation,
                "retire_operation_sha256": operation["payload_sha256"],
                "evidence": evidence,
                "object_checkpoints": object_checkpoints,
                "completed_at": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
            }
        )
        env_id = cast(str, operation["env_id"])
        path = self._cleanup_receipt_path(
            env_id,
            expected_resource_generation=cast(
                int,
                operation["expected_resource_generation"],
            ),
            idempotency_key=cast(str, operation["idempotency_key"]),
        )
        existing = _load_bound_json(path, kind=RETIRE_RECEIPT_KIND)
        if existing is not None:
            stable = {
                key: value
                for key, value in existing.items()
                if key not in {"completed_at", "payload_sha256"}
            }
            rebound = {
                key: value
                for key, value in payload.items()
                if key not in {"completed_at", "payload_sha256"}
            }
            if stable != rebound:
                raise DeploymentError("cleanup receipt replay drifted")
            current_path = self._cleanup_receipt_path(env_id)
            _ensure_directory(
                current_path.parent,
                mode=0o700,
                uid=0,
                gid=0,
                manage_ownership=self.manage_ownership,
            )
            _atomic_write(
                current_path,
                _canonical(existing),
                mode=0o600,
                uid=0,
                gid=0,
                manage_ownership=self.manage_ownership,
            )
            return existing
        _atomic_write(
            path,
            _canonical(payload),
            mode=0o600,
            uid=0,
            gid=0,
            manage_ownership=self.manage_ownership,
        )
        current_path = self._cleanup_receipt_path(env_id)
        _ensure_directory(
            current_path.parent,
            mode=0o700,
            uid=0,
            gid=0,
            manage_ownership=self.manage_ownership,
        )
        _atomic_write(
            current_path,
            _canonical(payload),
            mode=0o600,
            uid=0,
            gid=0,
            manage_ownership=self.manage_ownership,
        )
        return payload

    def _write_revive_operation(
        self,
        environment: Mapping[str, Any],
        *,
        idempotency_key: str,
        cleanup_receipt: Mapping[str, Any],
        registry_generation: int,
        registry_payload_sha256: str,
        registration_idempotency_key: str,
        phase: str = "registered",
    ) -> dict[str, Any]:
        if phase not in {"registered", "capacity-restored"}:
            raise DeploymentError("revival operation phase is invalid")
        previous_generation = cleanup_receipt.get("retired_resource_generation")
        if (
            cleanup_receipt.get("kind") != RETIRE_RECEIPT_KIND
            or cleanup_receipt.get("env_id") != environment["env_id"]
            or cleanup_receipt.get("principal_id") != environment["principal_id"]
            or cleanup_receipt.get("runtime_id") != environment["runtime_id"]
            or cleanup_receipt.get("uid") != environment["uid"]
            or cleanup_receipt.get("gid") != environment["gid"]
            or type(previous_generation) is not int
            or environment["resource_generation"] != previous_generation + 1
            or SAFE_ID_RE.fullmatch(idempotency_key) is None
            or SAFE_ID_RE.fullmatch(registration_idempotency_key) is None
            or not self.registry.registration_idempotency_replay(
                principal_id=cast(str, environment["principal_id"]),
                idempotency_key=registration_idempotency_key,
            )
        ):
            raise DeploymentError("revival cleanup binding is invalid")
        evidence = cast(dict[str, str], cleanup_receipt["evidence"])
        now = datetime.now(UTC).isoformat().replace("+00:00", "Z")
        path = self._revive_operation_path(
            cast(str, environment["env_id"]),
            new_resource_generation=cast(int, environment["resource_generation"]),
            idempotency_key=idempotency_key,
        )
        existing = _load_bound_json(path, kind=REVIVE_KIND)
        payload = _bound(
            {
                "schema_version": 1,
                "kind": REVIVE_KIND,
                "phase": phase,
                "env_id": environment["env_id"],
                "principal_id": environment["principal_id"],
                "runtime_id": environment["runtime_id"],
                "uid": environment["uid"],
                "gid": environment["gid"],
                "service_user": environment["service_user"],
                "service_group": environment["service_group"],
                "slurm_user": environment["slurm_user"],
                "slurm_account": environment["slurm_account"],
                "slurm_qos": environment["slurm_qos"],
                "previous_resource_generation": previous_generation,
                "new_resource_generation": environment["resource_generation"],
                "registry_generation": registry_generation,
                "registry_payload_sha256": registry_payload_sha256,
                "retire_tombstone_sha256": evidence["capacity_retire"],
                "idempotency_key": idempotency_key,
                "registration_idempotency_key": registration_idempotency_key,
                "created_at": (cast(str, existing["created_at"]) if existing is not None else now),
                "updated_at": now,
            }
        )
        if existing is not None and (
            existing.get("env_id") != environment["env_id"]
            or existing.get("principal_id") != environment["principal_id"]
            or existing.get("idempotency_key") != idempotency_key
            or existing.get("previous_resource_generation") != previous_generation
            or existing.get("new_resource_generation") != environment["resource_generation"]
        ):
            raise DeploymentError("revival operation replay drifted")
        _ensure_directory(
            path.parent,
            mode=0o700,
            uid=0,
            gid=0,
            manage_ownership=self.manage_ownership,
        )
        _atomic_write(
            path,
            _canonical(payload),
            mode=0o600,
            uid=0,
            gid=0,
            manage_ownership=self.manage_ownership,
        )
        current_path = self._revive_operation_path(cast(str, environment["env_id"]))
        _ensure_directory(
            current_path.parent,
            mode=0o700,
            uid=0,
            gid=0,
            manage_ownership=self.manage_ownership,
        )
        _atomic_write(
            current_path,
            _canonical(payload),
            mode=0o600,
            uid=0,
            gid=0,
            manage_ownership=self.manage_ownership,
        )
        return payload

    def _write_finalization_operation(
        self,
        context: DeploymentContext,
        *,
        phase: str,
        evidence: Mapping[str, Any],
    ) -> dict[str, Any]:
        if phase not in {"finalization-ready", "committed"}:
            raise DeploymentError("finalization journal phase is invalid")
        prepared_fields = {
            "capacity_finalize",
            "capacity_finalize_check",
            "runtime_reconcile",
            "runtime_check",
            "acceptance_probe",
        }
        required_fields = (
            prepared_fields
            if phase == "finalization-ready"
            else prepared_fields | {"capacity_active_check", "runtime_active_check"}
        )
        if set(evidence) != required_fields or any(
            re.fullmatch(r"[0-9a-f]{64}", str(evidence[field])) is None for field in required_fields
        ):
            raise DeploymentError("finalization evidence shape is invalid")
        path = self._global_runtime_path(
            "lifecycle",
            "finalization",
            f"{context.deployment_id}.json",
        )
        _ensure_directory(
            path.parent,
            mode=0o700,
            uid=0,
            gid=0,
            manage_ownership=self.manage_ownership,
        )
        previous = _load_bound_json(
            path,
            kind=FINALIZATION_KIND,
        )
        if previous is not None and (
            previous.get("deployment_id") != context.deployment_id
            or previous.get("env_id") != context.env_id
            or previous.get("principal_id") != context.principal_id
            or previous.get("candidate_id") != context.candidate_id
            or previous.get("applied_resource_generation") != context.runtime_resource_generation
        ):
            raise DeploymentError("finalization journal binding drifted")
        now = datetime.now(UTC).isoformat().replace("+00:00", "Z")
        payload = _bound(
            {
                "schema_version": 1,
                "kind": FINALIZATION_KIND,
                "phase": phase,
                "deployment_id": context.deployment_id,
                "env_id": context.env_id,
                "principal_id": context.principal_id,
                "runtime_id": context.environment["runtime_id"],
                "candidate_id": context.candidate_id,
                "candidate_sha": context.candidate_sha,
                "candidate_tree": context.candidate_tree,
                "applied_resource_generation": context.runtime_resource_generation,
                "applied_registry_generation": context.applied_registry_generation,
                "applied_registry_payload_sha256": context.applied_registry_digest,
                "capacity_finalize_receipt_sha256": evidence["capacity_finalize"],
                "capacity_finalize_check_receipt_sha256": evidence["capacity_finalize_check"],
                "runtime_reconcile_receipt_sha256": evidence["runtime_reconcile"],
                "runtime_prepare_check_receipt_sha256": evidence["runtime_check"],
                "acceptance_probe_receipt_sha256": evidence["acceptance_probe"],
                "created_at": (previous["created_at"] if previous is not None else now),
                "updated_at": now,
                **(
                    {}
                    if phase == "finalization-ready"
                    else {
                        "capacity_active_check_receipt_sha256": evidence["capacity_active_check"],
                        "runtime_active_check_receipt_sha256": evidence["runtime_active_check"],
                    }
                ),
            }
        )
        _atomic_write(
            path,
            _canonical(payload),
            mode=0o600,
            uid=0,
            gid=0,
            manage_ownership=self.manage_ownership,
        )
        return payload

    def _write_usable_receipt(
        self,
        context: DeploymentContext,
        *,
        finalization: Mapping[str, Any],
    ) -> dict[str, Any]:
        if (
            finalization.get("kind") != FINALIZATION_KIND
            or finalization.get("phase") != "committed"
            or finalization.get("deployment_id") != context.deployment_id
        ):
            raise DeploymentError("usable receipt finalization binding is invalid")
        path = self._global_runtime_path(
            "usable",
            f"{context.deployment_id}.json",
        )
        _ensure_directory(
            path.parent,
            mode=0o700,
            uid=0,
            gid=0,
            manage_ownership=self.manage_ownership,
        )
        payload = _bound(
            {
                "schema_version": 1,
                "kind": USABLE_KIND,
                "status": "activated",
                "admission_status": "general-open",
                "deployment_id": context.deployment_id,
                "env_id": context.env_id,
                "principal_id": context.principal_id,
                "runtime_id": context.environment["runtime_id"],
                "candidate_id": context.candidate_id,
                "candidate_sha": context.candidate_sha,
                "candidate_tree": context.candidate_tree,
                "applied_resource_generation": context.runtime_resource_generation,
                "applied_registry_generation": context.applied_registry_generation,
                "applied_registry_payload_sha256": context.applied_registry_digest,
                "active_registry_generation": context.snapshot_generation,
                "active_registry_payload_sha256": context.snapshot_digest,
                "finalization_journal_sha256": finalization["payload_sha256"],
                "capacity_finalize_receipt_sha256": finalization[
                    "capacity_finalize_receipt_sha256"
                ],
                "capacity_finalize_check_receipt_sha256": finalization[
                    "capacity_finalize_check_receipt_sha256"
                ],
                "runtime_reconcile_receipt_sha256": finalization[
                    "runtime_reconcile_receipt_sha256"
                ],
                "acceptance_probe_receipt_sha256": finalization["acceptance_probe_receipt_sha256"],
                "capacity_active_check_receipt_sha256": finalization[
                    "capacity_active_check_receipt_sha256"
                ],
                "runtime_active_check_receipt_sha256": finalization[
                    "runtime_active_check_receipt_sha256"
                ],
                "completed_at": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
            }
        )
        _atomic_write(
            path,
            _canonical(payload),
            mode=0o600,
            uid=0,
            gid=0,
            manage_ownership=self.manage_ownership,
        )
        return payload

    def _usable_receipt(self, context: DeploymentContext) -> dict[str, Any]:
        receipt = _load_bound_json(
            self._global_runtime_path(
                "usable",
                f"{context.deployment_id}.json",
            ),
            kind=USABLE_KIND,
        )
        exact = {
            "schema_version": 1,
            "kind": USABLE_KIND,
            "status": "activated",
            "admission_status": "general-open",
            "deployment_id": context.deployment_id,
            "env_id": context.env_id,
            "principal_id": context.principal_id,
            "runtime_id": context.environment["runtime_id"],
            "candidate_id": context.candidate_id,
            "candidate_sha": context.candidate_sha,
            "candidate_tree": context.candidate_tree,
            "applied_resource_generation": context.runtime_resource_generation,
            "applied_registry_generation": context.applied_registry_generation,
            "applied_registry_payload_sha256": context.applied_registry_digest,
        }
        digest_fields = {
            "active_registry_payload_sha256",
            "finalization_journal_sha256",
            "capacity_finalize_receipt_sha256",
            "capacity_finalize_check_receipt_sha256",
            "runtime_reconcile_receipt_sha256",
            "acceptance_probe_receipt_sha256",
            "capacity_active_check_receipt_sha256",
            "runtime_active_check_receipt_sha256",
        }
        if (
            receipt is None
            or any(receipt.get(field) != value for field, value in exact.items())
            or not _plain_int(receipt.get("active_registry_generation"), minimum=1)
            or receipt["active_registry_generation"] > context.snapshot_generation
            or any(
                re.fullmatch(r"[0-9a-f]{64}", str(receipt.get(field))) is None
                for field in digest_fields
            )
            or not _valid_timestamp(receipt.get("completed_at"))
        ):
            raise DeploymentError("developer environment is not usable")
        return receipt

    def _rebuild_usable_receipt(
        self,
        context: DeploymentContext,
        *,
        capacity_active_check: Mapping[str, Any],
        runtime_active_check: Mapping[str, Any],
    ) -> dict[str, Any]:
        snapshot = self._snapshot()
        records = [
            row
            for row in cast(
                list[dict[str, Any]],
                snapshot["deployment_finalizations"],
            )
            if row["deployment_id"] == context.deployment_id
            and row["payload_sha256"] == context.deployment["finalization_payload_sha256"]
        ]
        if (
            snapshot["generation"] != context.snapshot_generation
            or snapshot["payload_sha256"] != context.snapshot_digest
            or len(records) != 1
        ):
            raise DeploymentError("usable receipt recovery binding is invalid")
        record = records[0]
        finalization = self._write_finalization_operation(
            context,
            phase="committed",
            evidence={
                "capacity_finalize": record["capacity_finalize_receipt_sha256"],
                "capacity_finalize_check": record["capacity_finalize_check_receipt_sha256"],
                "runtime_reconcile": record["runtime_reconcile_receipt_sha256"],
                "runtime_check": record["runtime_prepare_check_receipt_sha256"],
                "acceptance_probe": record["acceptance_probe_receipt_sha256"],
                "capacity_active_check": _evidence_digest(capacity_active_check),
                "runtime_active_check": _evidence_digest(runtime_active_check),
            },
        )
        return self._write_usable_receipt(
            context,
            finalization=finalization,
        )

    def _write_rollback_operation(
        self,
        context: DeploymentContext,
        *,
        idempotency_key: str,
        phase: str,
    ) -> dict[str, Any]:
        payload = _bound(
            {
                "schema_version": 1,
                "kind": ROLLBACK_KIND,
                "env_id": context.env_id,
                "principal_id": context.principal_id,
                "deployment_id": context.deployment_id,
                "candidate_id": context.candidate_id,
                "effective_candidate_id": context.environment["current_candidate_id"],
                "idempotency_key": idempotency_key,
                "phase": phase,
            }
        )
        _atomic_write(
            self._rollback_operation_path(context.environment),
            _canonical(payload),
            mode=0o600,
            uid=0,
            gid=0,
            manage_ownership=self.manage_ownership,
        )
        return payload

    def _active_committed_context(
        self,
        environment: dict[str, Any],
    ) -> DeploymentContext | None:
        current = environment["current_candidate_id"]
        if current is None:
            return None
        snapshot = self._snapshot()
        deployments = [
            row
            for row in cast(list[dict[str, Any]], snapshot["deployments"])
            if row["env_id"] == environment["env_id"]
            and row["candidate_id"] == current
            and row["phase"] == "committed"
            and row["applied_resource_generation"] == environment["resource_generation"]
        ]
        candidates = [
            row
            for row in cast(list[dict[str, Any]], snapshot["candidates"])
            if row["candidate_id"] == current
        ]
        if not deployments or len(candidates) != 1:
            raise DeploymentError("rollback active candidate binding is unavailable")
        deployment = max(
            deployments,
            key=lambda row: (
                row["expected_resource_generation"],
                row["updated_at"],
                row["deployment_id"],
            ),
        )
        return DeploymentContext(
            cast(int, snapshot["generation"]),
            cast(str, snapshot["payload_sha256"]),
            select_environment(
                snapshot,
                principal_id=cast(str, environment["principal_id"]),
                env_id=cast(str, environment["env_id"]),
                root=False,
            ),
            candidates[0],
            deployment,
            self.host_root,
        )

    def _restore_active_local(self, environment: dict[str, Any]) -> None:
        context = self._active_committed_context(environment)
        if context is None:
            return
        self._enable_recovery_unit(context)
        self._verify_bundle(context)
        self._verify_checkout(context, context.checkout)
        self._rebind_committed_services(context)

    def _manifest_belongs_to(self, context: DeploymentContext) -> bool:
        manifest = _load_bound_json(
            context.manifest_path,
            kind=MANIFEST_KIND,
            require_root=self.require_root_metadata,
        )
        return (
            manifest is not None
            and manifest.get("deployment_id") == context.deployment_id
            and manifest.get("candidate_id") == context.candidate_id
        )

    def _require_host(self) -> None:
        if self.manage_ownership and (os.getuid() != 0 or os.geteuid() != 0):
            raise DeploymentError("developer environment deployment requires root")
        hostname = platform.node().split(".", 1)[0].lower()
        if self.expected_hostname and hostname != self.expected_hostname:
            raise DeploymentError("developer environment deployment host is invalid")

    def _ensure_identity(self, context: DeploymentContext) -> None:
        environment = context.environment
        name = cast(str, environment["service_user"])
        group_name = cast(str, environment["service_group"])
        uid = cast(int, environment["uid"])
        gid = cast(int, environment["gid"])
        try:
            user_by_name = pwd.getpwnam(name)
        except KeyError:
            user_by_name = None
        try:
            user_by_id = pwd.getpwuid(uid)
        except KeyError:
            user_by_id = None
        try:
            group_by_name = grp.getgrnam(group_name)
        except KeyError:
            group_by_name = None
        try:
            group_by_id = grp.getgrgid(gid)
        except KeyError:
            group_by_id = None
        if (group_by_name is None) != (group_by_id is None):
            raise DeploymentError("allocated service group conflicts with host")
        if group_by_name is None:
            self.runner.run(("groupadd", "--system", "--gid", str(gid), group_name))
        elif (
            group_by_name.gr_gid != gid or group_by_id is None or group_by_id.gr_name != group_name
        ):
            raise DeploymentError("allocated service group conflicts with host")
        if (user_by_name is None) != (user_by_id is None):
            raise DeploymentError("allocated service user conflicts with host")
        if user_by_name is None:
            self.runner.run(
                (
                    "useradd",
                    "--system",
                    "--uid",
                    str(uid),
                    "--gid",
                    str(gid),
                    "--no-create-home",
                    "--home-dir",
                    cast(str, environment["runtime_root"]),
                    "--shell",
                    "/usr/sbin/nologin",
                    name,
                )
            )
        elif (
            user_by_name.pw_uid != uid
            or user_by_name.pw_gid != gid
            or user_by_id is None
            or user_by_id.pw_name != name
        ):
            raise DeploymentError("allocated service user conflicts with host")
        try:
            converged_user = pwd.getpwnam(name)
            converged_user_id = pwd.getpwuid(uid)
            converged_group = grp.getgrnam(group_name)
            converged_group_id = grp.getgrgid(gid)
        except (KeyError, OSError) as exc:
            raise DeploymentError("allocated service identity readback failed") from exc
        if (
            converged_user.pw_uid != uid
            or converged_user.pw_gid != gid
            or converged_user_id.pw_name != name
            or converged_group.gr_gid != gid
            or converged_group_id.gr_name != group_name
        ):
            raise DeploymentError("allocated service identity readback failed")

    def _ensure_resources(self, context: DeploymentContext) -> None:
        self._ensure_identity(context)
        uid = cast(int, context.environment["uid"])
        gid = cast(int, context.environment["gid"])
        roots = (
            (
                context.host_path(cast(str, context.environment["candidate_root"])),
                0o750,
                0,
                gid,
            ),
            (
                context.host_path(cast(str, context.environment["runtime_root"])),
                0o700,
                uid,
                gid,
            ),
            (context.state_root, 0o700, uid, gid),
            (
                context.host_path(cast(str, context.environment["evidence_root"])),
                0o700,
                uid,
                gid,
            ),
            (context.state_root / "secrets", 0o700, uid, gid),
        )
        for path, mode, owner_uid, owner_gid in roots:
            _ensure_directory(
                path,
                mode=mode,
                uid=owner_uid,
                gid=owner_gid,
                manage_ownership=self.manage_ownership,
            )
        _ensure_directory(
            context.lifecycle_root,
            mode=0o700,
            uid=0,
            gid=0,
            manage_ownership=self.manage_ownership,
        )
        if not context.secrets_path.exists():
            values = {
                "LOOM_DEV_POSTGRES_PASSWORD": secrets.token_urlsafe(36),
                "LOOM_DEV_MINIO_ROOT_PASSWORD": secrets.token_urlsafe(36),
                "LOOM_CP_STEP_JWT_SIGNING_KEY": secrets.token_urlsafe(48),
                "LOOM_SECRET_STORE_MASTER_KEY": base64.b64encode(os.urandom(32)).decode("ascii"),
                "LOOM_WORKER_TOKEN": f"loom_w_{secrets.token_hex(32)}",
            }
            raw = "".join(f"{key}={values[key]}\n" for key in SECRET_KEYS).encode("ascii")
            _atomic_write(
                context.secrets_path,
                raw,
                mode=0o600,
                uid=uid,
                gid=gid,
                manage_ownership=self.manage_ownership,
            )
        _read_stable_regular(
            context.secrets_path,
            limit=64 * 1024,
            require_root=False,
            expected_mode=0o600,
        )
        _atomic_write(
            context.distributed_secrets_path,
            _read_stable_regular(
                context.secrets_path,
                limit=64 * 1024,
                require_root=False,
                expected_mode=0o600,
            ),
            mode=0o600,
            uid=uid,
            gid=gid,
            manage_ownership=self.manage_ownership,
        )
        admin_secret = context.state_root / "secrets" / "admin.toml"
        if not admin_secret.exists():
            token = "loom_admin_" + secrets.token_urlsafe(36)
            _atomic_write(
                admin_secret,
                f'[admin]\ntoken = "{token}"\n'.encode("ascii"),
                mode=0o600,
                uid=uid,
                gid=gid,
                manage_ownership=self.manage_ownership,
            )
        _read_stable_regular(
            admin_secret,
            limit=64 * 1024,
            require_root=False,
            expected_mode=0o600,
        )

    def _verify_bundle(self, context: DeploymentContext) -> Path:
        bundle = Path(cast(str, context.candidate["bundle_path"]))
        raw = _read_stable_regular(
            bundle,
            limit=MAX_BUNDLE_BYTES,
            require_root=self.require_root_metadata,
            expected_mode=0o600 if self.require_root_metadata else None,
        )
        if (
            len(raw) != context.candidate["bundle_size"]
            or hashlib.sha256(raw).hexdigest() != context.candidate["bundle_sha256"]
        ):
            raise DeploymentError("candidate bundle binding is invalid")
        return bundle

    def _ensure_worker_images(self, context: DeploymentContext) -> DeploymentContext:
        image_digests = context.candidate.get("image_digests")
        image_archives = context.candidate.get("image_archives")
        if (
            not isinstance(image_digests, dict)
            or set(image_digests) != {"amd64", "arm64"}
            or not isinstance(image_archives, dict)
            or set(image_archives) != {"amd64", "arm64"}
        ):
            raise DeploymentError("candidate worker image binding is invalid")
        verified_archives: dict[str, Path] = {}
        for architecture in ("amd64", "arm64"):
            binding = image_archives.get(architecture)
            image_id = image_digests.get(architecture)
            if (
                not isinstance(binding, dict)
                or set(binding)
                != {
                    "path",
                    "sha256",
                    "size",
                    "config_digest",
                    "index_digest",
                    "manifest_digest",
                    "manifest_media_type",
                    "load_descriptor_digest",
                    "load_descriptor_media_type",
                }
                or not isinstance(binding.get("path"), str)
                or binding.get("config_digest") != image_id
            ):
                raise DeploymentError("candidate worker image archive binding is invalid")
            archive = Path(cast(str, binding["path"]))
            try:
                metadata = archive.lstat()
                if self.require_root_metadata and (
                    (metadata.st_uid, metadata.st_gid) != (0, 0)
                    or stat.S_IMODE(metadata.st_mode) != 0o600
                ):
                    raise DeploymentError("candidate worker image archive metadata is unsafe")
                verified = verify_worker_image_archive(
                    archive,
                    architecture=architecture,
                    candidate_sha=context.candidate_sha,
                    image_id=str(image_id),
                    expected_archive_sha256=str(binding["sha256"]),
                    expected_archive_size=cast(int, binding["size"]),
                )
                if any(
                    verified.get(field) != binding.get(field)
                    for field in (
                        "config_digest",
                        "index_digest",
                        "manifest_digest",
                        "manifest_media_type",
                        "load_descriptor_digest",
                        "load_descriptor_media_type",
                    )
                ):
                    raise DeploymentError(
                        "candidate worker image archive descriptor binding is invalid"
                    )
            except DeploymentError:
                raise
            except (OSError, RegistryError, TypeError, ValueError) as exc:
                raise DeploymentError("candidate worker image archive verification failed") from exc
            verified_archives[architecture] = archive

        node_bindings: dict[str, dict[str, Any]] = {}
        domain_bindings: dict[str, dict[str, Any]] = {}
        for domain in ("oldlab", "gb10"):
            architecture = "amd64" if domain == "oldlab" else "arm64"
            binding = cast(dict[str, Any], image_archives[architecture])
            for node in DOMAIN_RUNTIME_NODES[domain]:
                metadata = {
                    "schema_version": 1,
                    "kind": "loom.developer-sandbox.worker-image-load-request",
                    "node": node,
                    "domain": domain,
                    "architecture": architecture,
                    "env_id": context.env_id,
                    "resource_generation": context.resource_generation,
                    "candidate_id": context.candidate_id,
                    "candidate_sha": context.candidate_sha,
                    "candidate_tree": context.candidate_tree,
                    "config_digest": binding["config_digest"],
                    "index_digest": binding["index_digest"],
                    "load_descriptor_digest": binding["load_descriptor_digest"],
                    "load_descriptor_media_type": binding["load_descriptor_media_type"],
                    "archive_sha256": binding["sha256"],
                    "archive_size": binding["size"],
                    "registry_generation": context.snapshot_generation,
                    "registry_payload_sha256": context.snapshot_digest,
                }
                metadata["payload_sha256"] = _digest(metadata)
                result = self.runner.run(
                    (
                        str(NODE_TRANSPORT),
                        "load-image",
                        "--node",
                        node,
                        "--archive",
                        str(verified_archives[architecture]),
                        "--metadata-json",
                        _canonical(metadata).decode("ascii").strip(),
                    )
                )
                try:
                    receipt = json.loads(result.stdout)
                except (UnicodeError, json.JSONDecodeError) as exc:
                    raise DeploymentError("worker image load receipt is invalid") from exc
                unsigned = (
                    {key: value for key, value in receipt.items() if key != "payload_sha256"}
                    if isinstance(receipt, dict)
                    else {}
                )
                if (
                    not isinstance(receipt, dict)
                    or receipt.get("schema_version") != 1
                    or receipt.get("kind") != "loom.developer-sandbox.worker-image-load-receipt"
                    or receipt.get("status") not in {"loaded", "reused"}
                    or receipt.get("node") != node
                    or receipt.get("domain") != domain
                    or receipt.get("architecture") != architecture
                    or receipt.get("candidate_id") != context.candidate_id
                    or receipt.get("candidate_sha") != context.candidate_sha
                    or any(
                        receipt.get(field) != binding[field]
                        for field in (
                            "config_digest",
                            "index_digest",
                            "load_descriptor_digest",
                            "load_descriptor_media_type",
                        )
                    )
                    or receipt.get("archive_sha256") != binding["sha256"]
                    or receipt.get("archive_size") != binding["size"]
                    or receipt.get("registry_generation") != context.snapshot_generation
                    or receipt.get("registry_payload_sha256") != context.snapshot_digest
                    or receipt.get("docker_backend")
                    not in {"classic-overlay2", "containerd-snapshotter-v1"}
                    or (
                        receipt.get("docker_backend") == "classic-overlay2"
                        and (
                            receipt.get("docker_storage_driver") != "overlay2"
                            or receipt.get("runtime_image_id") != binding["config_digest"]
                            or receipt.get("docker_descriptor_digest") is not None
                            or receipt.get("docker_descriptor_media_type") is not None
                        )
                    )
                    or (
                        receipt.get("docker_backend") == "containerd-snapshotter-v1"
                        and (
                            receipt.get("docker_storage_driver") != "overlayfs"
                            or receipt.get("runtime_image_id") != binding["load_descriptor_digest"]
                            or receipt.get("docker_descriptor_digest")
                            != binding["load_descriptor_digest"]
                            or receipt.get("docker_descriptor_media_type")
                            != binding["load_descriptor_media_type"]
                        )
                    )
                    or receipt.get("payload_sha256") != _digest(unsigned)
                    or result.stdout.encode("ascii") != _canonical(receipt)
                ):
                    raise DeploymentError("worker image load receipt binding is invalid")
                node_binding = {
                    "domain": domain,
                    "architecture": architecture,
                    "docker_driver": receipt["docker_storage_driver"],
                    "docker_backend": receipt["docker_backend"],
                    "config_digest": receipt["config_digest"],
                    "load_descriptor_digest": receipt["load_descriptor_digest"],
                    "load_descriptor_media_type": receipt["load_descriptor_media_type"],
                    "runtime_image_id": receipt["runtime_image_id"],
                    "docker_descriptor_digest": receipt["docker_descriptor_digest"],
                    "docker_descriptor_media_type": receipt["docker_descriptor_media_type"],
                    "receipt_sha256": receipt["payload_sha256"],
                }
                node_bindings[node] = node_binding
                domain_binding = {
                    key: value
                    for key, value in node_binding.items()
                    if key
                    not in {
                        "domain",
                        "docker_descriptor_digest",
                        "docker_descriptor_media_type",
                        "receipt_sha256",
                    }
                }
                prior = domain_bindings.setdefault(domain, domain_binding)
                if prior != domain_binding:
                    raise DeploymentError("worker image runtime identity differs within one domain")
        bindings = {"nodes": node_bindings, "domains": domain_bindings}
        persisted = context.deployment.get("worker_runtime_bindings")
        if persisted is not None:
            if not isinstance(persisted, dict):
                raise DeploymentError("persisted worker runtime binding is invalid")
            persisted_nodes = persisted.get("nodes")
            persisted_domains = persisted.get("domains")
            if (
                not isinstance(persisted_nodes, dict)
                or persisted_domains != domain_bindings
                or set(persisted_nodes) != set(node_bindings)
                or any(
                    {
                        key: value
                        for key, value in persisted_nodes[node].items()
                        if key != "receipt_sha256"
                    }
                    != {
                        key: value
                        for key, value in node_bindings[node].items()
                        if key != "receipt_sha256"
                    }
                    for node in node_bindings
                    if isinstance(persisted_nodes.get(node), dict)
                )
                or any(not isinstance(persisted_nodes.get(node), dict) for node in node_bindings)
            ):
                raise DeploymentError("persisted worker runtime binding drifted")
            return context
        self.registry.record_worker_runtime_bindings(
            context.deployment_id,
            principal_id=context.principal_id,
            expected_resource_generation=context.resource_generation,
            bindings=bindings,
        )
        return self._refresh_context(
            context.env_id,
            context.principal_id,
            context.deployment_id,
        )

    def _verify_checkout(self, context: DeploymentContext, path: Path) -> None:
        if path.is_symlink() or not path.is_dir():
            raise DeploymentError("candidate checkout is unsafe")
        before = self._checkout_metadata(path)
        head = self.runner.run(("git", "-C", str(path), "rev-parse", "HEAD")).stdout.strip()
        tree = self.runner.run(("git", "-C", str(path), "rev-parse", "HEAD^{tree}")).stdout.strip()
        dirty = self.runner.run(
            ("git", "-C", str(path), "status", "--porcelain", "--untracked-files=all")
        ).stdout
        after = self._checkout_metadata(path)
        if (
            before != after
            or head != context.candidate_sha
            or tree != context.candidate_tree
            or dirty
        ):
            raise DeploymentError("candidate checkout binding is invalid")

    def _checkout_metadata(self, path: Path) -> tuple[tuple[str, int, ...], ...]:
        try:
            entries = [path, *sorted(path.rglob("*"), key=lambda item: item.as_posix())]
            rows: list[tuple[str, int, ...]] = []
            for entry in entries:
                relative = entry.relative_to(path)
                if relative.parts[:1] == (".git",) and relative.parts != (".git",):
                    # Read-only git commands may refresh private index metadata.
                    # The sealed .git directory identity plus exact HEAD/tree
                    # readback is the trust boundary for repository internals.
                    continue
                metadata = entry.lstat()
                if (
                    stat.S_ISLNK(metadata.st_mode)
                    or (not stat.S_ISDIR(metadata.st_mode) and not stat.S_ISREG(metadata.st_mode))
                    or (stat.S_ISREG(metadata.st_mode) and metadata.st_nlink != 1)
                    or metadata.st_mode & 0o022
                    or (self.require_root_metadata and metadata.st_uid != 0)
                ):
                    raise DeploymentError("candidate checkout metadata is unsafe")
                rows.append(
                    (
                        str(entry.relative_to(path.parent)),
                        metadata.st_dev,
                        metadata.st_ino,
                        metadata.st_mode,
                        metadata.st_uid,
                        metadata.st_gid,
                        metadata.st_nlink,
                        metadata.st_size,
                    )
                )
            return tuple(rows)
        except DeploymentError:
            raise
        except OSError as exc:
            raise DeploymentError("candidate checkout metadata is unavailable") from exc

    def _seal_checkout(self, context: DeploymentContext, path: Path) -> None:
        if not self.manage_ownership:
            return
        gid = cast(int, context.environment["gid"])
        try:
            entries = [path, *sorted(path.rglob("*"), key=lambda item: item.as_posix())]
            for entry in reversed(entries):
                metadata = entry.lstat()
                if stat.S_ISLNK(metadata.st_mode):
                    raise DeploymentError("candidate checkout metadata is unsafe")
                os.chown(entry, 0, gid)
                if stat.S_ISDIR(metadata.st_mode):
                    os.chmod(entry, 0o550)
                elif stat.S_ISREG(metadata.st_mode):
                    os.chmod(entry, 0o550 if metadata.st_mode & 0o111 else 0o440)
                else:
                    raise DeploymentError("candidate checkout metadata is unsafe")
        except DeploymentError:
            raise
        except OSError as exc:
            raise DeploymentError("candidate checkout cannot be sealed") from exc

    def _materialize_candidate(self, context: DeploymentContext) -> None:
        bundle = self._verify_bundle(context)
        if context.checkout.exists():
            self._verify_checkout(context, context.checkout)
            return
        parent = context.checkout.parent
        _assert_safe_ancestry(parent)
        temporary = Path(tempfile.mkdtemp(prefix=".candidate-", dir=parent))
        try:
            self.runner.run(
                (
                    "git",
                    "-c",
                    "protocol.file.allow=always",
                    "clone",
                    "--no-checkout",
                    str(bundle),
                    str(temporary),
                )
            )
            self.runner.run(
                ("git", "-C", str(temporary), "checkout", "--detach", context.candidate_sha)
            )
            self._seal_checkout(context, temporary)
            self._verify_checkout(context, temporary)
            os.replace(temporary, context.checkout)
        except Exception:
            shutil.rmtree(temporary, ignore_errors=True)
            raise
        self._verify_checkout(context, context.checkout)

    def _secret_values(self, context: DeploymentContext) -> dict[str, str]:
        raw = _read_stable_regular(
            context.secrets_path,
            limit=64 * 1024,
            require_root=False,
            expected_mode=0o600,
        )
        values: dict[str, str] = {}
        try:
            text = raw.decode("ascii")
        except UnicodeDecodeError as exc:
            raise DeploymentError("developer environment secrets are invalid") from exc
        for line in text.splitlines():
            key, separator, value = line.partition("=")
            if not separator or key not in SECRET_KEYS or not value or key in values:
                raise DeploymentError("developer environment secrets are invalid")
            values[key] = value
        if set(values) != set(SECRET_KEYS):
            raise DeploymentError("developer environment secrets are incomplete")
        return values

    def _compose_environment(self, context: DeploymentContext) -> dict[str, str]:
        environment = context.environment
        ports = cast(dict[str, int], environment["ports"])
        values = self._secret_values(context)
        public = {
            "LOOM_DEV_BIND_ADDR": "127.0.0.1",
            "LOOM_DEV_POSTGRES_USER": cast(str, environment["service_user"]).replace("-", "_"),
            "LOOM_DEV_POSTGRES_DB": cast(str, environment["database_name"]),
            "LOOM_DEV_POSTGRES_PORT": str(ports["postgres"]),
            "LOOM_DEV_MINIO_ROOT_USER": cast(str, environment["service_user"]),
            "LOOM_DEV_MINIO_PORT": str(ports["minio"]),
            "LOOM_DEV_MINIO_CONSOLE_PORT": str(ports["minio_console"]),
            "LOOM_DEV_CONTROL_PLANE_PORT": str(ports["control_plane"]),
            "LOOM_DEV_LOOM_SERVICE_PORT": str(ports["loom_service"]),
            "LOOM_DEV_LLM_GATEWAY_PORT": str(ports["llm_gateway"]),
            "LOOM_DEV_EGRESS_XDS_PORT": str(ports["egress_xds"]),
            "LOOM_DEV_EGRESS_PROXY_PORT": str(ports["egress_proxy"]),
            "LOOM_DEV_EGRESS_ADMIN_PORT": str(ports["egress_admin"]),
            "LOOM_DEV_WEB_PORT": str(ports["web"]),
            "LOOM_DEV_TASK_BUCKET": cast(str, environment["task_bucket"]),
            "LOOM_DEV_TRAJECTORIES_BUCKET": cast(str, environment["trajectories_bucket"]),
            "LOOM_DEV_ARTIFACTS_BUCKET": cast(str, environment["artifacts_bucket"]),
            "LOOM_DEV_PROVIDER_CONNECTION_NAMESPACE": cast(str, environment["provider_namespace"]),
            "LOOM_DEV_ADMIN_SECRET_FILE": str(context.state_root / "secrets" / "admin.toml"),
            "LOOM_DEV_IMAGE_TAG": context.candidate_sha[:12],
            "LOOM_CANDIDATE_SHA": context.candidate_sha,
            "LOOM_CANDIDATE_TREE": context.candidate_tree,
            "LOOM_CANDIDATE_IMAGE_DIGEST": context.image_digest,
            "LOOM_WORKER_IMAGE_ID": context.worker_image_id,
            "SLURM_ACCOUNT": cast(str, environment["slurm_account"]),
            "SLURM_QOS": cast(str, environment["slurm_qos"]),
            "SLURM_USER": cast(str, environment["slurm_user"]),
        }
        return {**values, **public}

    def _write_compose_assets(self, context: DeploymentContext) -> None:
        values = self._compose_environment(context)
        raw = "".join(f"{key}={values[key]}\n" for key in sorted(values)).encode("ascii")
        _atomic_write(
            context.compose_env_path,
            raw,
            mode=0o600,
            uid=0,
            gid=0,
            manage_ownership=self.manage_ownership,
        )
        labels = {
            "loom.developer-environment.env-id": context.env_id,
            "loom.developer-environment.runtime-id": cast(
                str,
                context.environment["runtime_id"],
            ),
            "loom.developer-environment.compose-project": cast(
                str,
                context.environment["compose_project"],
            ),
            "loom.developer-environment.candidate-id": context.candidate_id,
            "loom.developer-environment.candidate-sha": context.candidate_sha,
            "loom.developer-environment.candidate-tree": context.candidate_tree,
            "loom.developer-environment.resource-generation": str(
                context.runtime_resource_generation
            ),
            "loom.developer-environment.registry-generation": str(
                context.applied_registry_generation
            ),
            "loom.developer-environment.registry-payload-sha256": (context.applied_registry_digest),
        }
        owner_labels = _resource_owner_labels(context)
        service_overrides = {
            service: {
                "labels": (
                    {
                        **labels,
                        "loom.developer-environment.image-digest": context.worker_image_id,
                    }
                    if service == "worker"
                    else labels
                ),
                "cgroup_parent": cast(str, context.environment["cgroup_slice"]),
            }
            for service in ALL_SERVICES
        }
        service_overrides["worker"]["image"] = context.worker_image_id
        override = {
            "services": service_overrides,
            "networks": {
                "default": {
                    "name": f"{context.environment['compose_project']}_default",
                    "labels": owner_labels,
                }
            },
            "volumes": {
                "postgres_data": {
                    "name": context.environment["postgres_volume"],
                    "labels": owner_labels,
                },
                "minio_data": {
                    "name": context.environment["minio_volume"],
                    "labels": owner_labels,
                },
            },
        }
        _atomic_write(
            context.compose_override_path,
            _canonical(override),
            mode=0o600,
            uid=0,
            gid=0,
            manage_ownership=self.manage_ownership,
        )
        compose_env_sha256 = hashlib.sha256(raw).hexdigest()
        compose_override_sha256 = hashlib.sha256(_canonical(override)).hexdigest()
        manifest = _bound(
            {
                "schema_version": 1,
                "kind": MANIFEST_KIND,
                "env_id": context.env_id,
                "principal_id": context.principal_id,
                "deployment_id": context.deployment_id,
                "candidate_id": context.candidate_id,
                "candidate_sha": context.candidate_sha,
                "candidate_tree": context.candidate_tree,
                "image_digest": context.image_digest,
                "resource_generation": context.runtime_resource_generation,
                "applied_registry_generation": context.applied_registry_generation,
                "applied_registry_payload_sha256": context.applied_registry_digest,
                "compose_project": context.environment["compose_project"],
                "compose_network": f"{context.environment['compose_project']}_default",
                "postgres_volume": context.environment["postgres_volume"],
                "minio_volume": context.environment["minio_volume"],
                "provider_namespace": context.environment["provider_namespace"],
                "slurm_user": context.environment["slurm_user"],
                "slurm_account": context.environment["slurm_account"],
                "slurm_qos": context.environment["slurm_qos"],
                "cgroup_slice": context.environment["cgroup_slice"],
                "candidate_checkout": str(context.checkout),
                "runtime_root": context.environment["runtime_root"],
                "compose_env_sha256": compose_env_sha256,
                "compose_override_sha256": compose_override_sha256,
                "resource_owner_sha256": owner_labels["loom.developer-environment.owner-sha256"],
                "systemd_unit": (
                    f"loom-developer-environment@{context.environment['systemd_instance']}.service"
                ),
            }
        )
        _atomic_write(
            context.manifest_path,
            _canonical(manifest),
            mode=0o600,
            uid=0,
            gid=0,
            manage_ownership=self.manage_ownership,
        )

    def _compose_argv(self, context: DeploymentContext) -> tuple[str, ...]:
        self._verify_bundle(context)
        self._verify_checkout(context, context.checkout)
        manifest = _load_bound_json(
            context.manifest_path,
            kind=MANIFEST_KIND,
            require_root=self.require_root_metadata,
        )
        compose_env = _read_stable_regular(
            context.compose_env_path,
            limit=256 * 1024,
            require_root=self.require_root_metadata,
            expected_mode=0o600,
        )
        compose_override = _read_stable_regular(
            context.compose_override_path,
            limit=1024 * 1024,
            require_root=self.require_root_metadata,
            expected_mode=0o600,
        )
        if (
            manifest is None
            or manifest.get("candidate_id") != context.candidate_id
            or manifest.get("candidate_sha") != context.candidate_sha
            or manifest.get("candidate_tree") != context.candidate_tree
            or manifest.get("compose_env_sha256") != hashlib.sha256(compose_env).hexdigest()
            or manifest.get("compose_override_sha256")
            != hashlib.sha256(compose_override).hexdigest()
        ):
            raise DeploymentError("privileged Compose input binding is invalid")
        compose = context.checkout / COMPOSE_FILE
        compose_metadata = compose.lstat()
        if (
            not stat.S_ISREG(compose_metadata.st_mode)
            or stat.S_ISLNK(compose_metadata.st_mode)
            or compose_metadata.st_nlink != 1
            or compose_metadata.st_mode & 0o022
            or (self.require_root_metadata and compose_metadata.st_uid != 0)
        ):
            raise DeploymentError("candidate Compose file is unavailable")
        return (
            "docker",
            "compose",
            "--project-name",
            cast(str, context.environment["compose_project"]),
            "--env-file",
            str(context.compose_env_path),
            "--file",
            str(compose),
            "--file",
            str(context.compose_override_path),
        )

    def _verify_worker_image(self, context: DeploymentContext) -> None:
        result = self.runner.run(
            ("docker", "image", "inspect", context.worker_image_id),
        )
        if not isinstance(result.stdout, str) or len(result.stdout) > 1024 * 1024:
            raise DeploymentError("loom-worker image inspection is invalid")
        try:
            payload = json.loads(result.stdout)
        except json.JSONDecodeError as exc:
            raise DeploymentError("loom-worker image inspection is invalid") from exc
        row = payload[0] if isinstance(payload, list) and len(payload) == 1 else None
        config = row.get("Config") if isinstance(row, dict) else None
        labels = config.get("Labels") if isinstance(config, dict) else None
        runtime_bindings = context.deployment.get("worker_runtime_bindings")
        domains = runtime_bindings.get("domains") if isinstance(runtime_bindings, dict) else None
        domain = "oldlab" if context.architecture == "amd64" else "gb10"
        binding = domains.get(domain) if isinstance(domains, dict) else None
        descriptor = row.get("Descriptor") if isinstance(row, dict) else None
        if (
            not isinstance(row, dict)
            or not isinstance(binding, dict)
            or row.get("Id") != context.worker_image_id
            or row.get("Os") != "linux"
            or row.get("Architecture") != context.architecture
            or not isinstance(labels, dict)
            or labels.get(WORKER_REVISION_LABEL) != context.candidate_sha
            or config.get("Cmd") != list(WORKER_COMMAND)
            or config.get("Entrypoint") not in (None, [])
        ):
            raise DeploymentError("loom-worker image binding is invalid")
        if binding.get("docker_backend") == "classic-overlay2":
            if descriptor not in (None, {}):
                raise DeploymentError("loom-worker descriptor binding is invalid")
        elif (
            binding.get("docker_backend") != "containerd-snapshotter-v1"
            or not isinstance(descriptor, dict)
            or descriptor.get("digest") != binding.get("load_descriptor_digest")
            or descriptor.get("mediaType") != binding.get("load_descriptor_media_type")
        ):
            raise DeploymentError("loom-worker descriptor binding is invalid")

    def _prepare_services(self, context: DeploymentContext) -> None:
        self._verify_worker_image(context)
        self._write_compose_assets(context)
        self.runner.run(
            (*self._compose_argv(context), "config", "--quiet"),
            cwd=context.checkout,
        )
        self.runner.run(
            (*self._compose_argv(context), "up", "--detach", "postgres", "minio"),
            cwd=context.checkout,
        )
        self.runner.run(
            (
                *self._compose_argv(context),
                "run",
                "--rm",
                "--no-deps",
                "--build",
                "control-plane",
                "sh",
                "-eu",
                "-c",
                'export LOOM_DB_URL="$LOOM_CP_DB_URL"; '
                "python -m alembic -c migrations/alembic.ini upgrade head",
            ),
            cwd=context.checkout,
        )
        self.runner.run(
            (*self._compose_argv(context), "build", *LOCAL_BUILD_SERVICES),
            cwd=context.checkout,
        )
        self.runner.run(
            (
                *self._compose_argv(context),
                "up",
                "--detach",
                "--no-build",
                "--remove-orphans",
                "--force-recreate",
            ),
            cwd=context.checkout,
        )

    def _rebind_committed_services(self, context: DeploymentContext) -> None:
        self._verify_worker_image(context)
        self._write_compose_assets(context)
        self.runner.run(
            (*self._compose_argv(context), "config", "--quiet"),
            cwd=context.checkout,
        )
        self.runner.run(
            (
                *self._compose_argv(context),
                "up",
                "--detach",
                "--no-build",
                "--remove-orphans",
                "--force-recreate",
            ),
            cwd=context.checkout,
        )
        self._verify_services(context)
        self._verify_persistence(context)

    def _ensure_committed_services_binding(self, context: DeploymentContext) -> None:
        manifest = _load_bound_json(
            context.manifest_path,
            kind=MANIFEST_KIND,
            require_root=self.require_root_metadata,
        )
        if (
            manifest is not None
            and manifest.get("deployment_id") == context.deployment_id
            and manifest.get("candidate_id") == context.candidate_id
            and manifest.get("image_digest") == context.image_digest
            and manifest.get("resource_generation") == context.runtime_resource_generation
            and manifest.get("applied_registry_generation") == context.applied_registry_generation
            and manifest.get("applied_registry_payload_sha256") == context.applied_registry_digest
        ):
            self._verify_services(context)
            self._verify_persistence(context)
            return
        self._rebind_committed_services(context)

    def _ensure_capacity(self, context: DeploymentContext) -> None:
        revive = _load_bound_json(
            self._revive_operation_path(context.env_id),
            kind=REVIVE_KIND,
        )
        cleanup = _load_bound_json(
            self._cleanup_receipt_path(context.env_id),
            kind=RETIRE_RECEIPT_KIND,
        )
        if revive is not None:
            if cleanup is None:
                raise DeploymentError("revival cleanup receipt is unavailable")
            rebound = self._write_revive_operation(
                context.environment,
                idempotency_key=cast(str, revive["idempotency_key"]),
                cleanup_receipt=cleanup,
                registry_generation=context.snapshot_generation,
                registry_payload_sha256=context.snapshot_digest,
                registration_idempotency_key=cast(
                    str,
                    revive["registration_idempotency_key"],
                ),
            )
            if cleanup.get("retired_candidate_id") is None:
                self.capacity_authority.reconcile(context)
            else:
                self.capacity_authority.reactivate(context)
            self._write_revive_operation(
                context.environment,
                idempotency_key=cast(str, rebound["idempotency_key"]),
                cleanup_receipt=cleanup,
                registry_generation=context.snapshot_generation,
                registry_payload_sha256=context.snapshot_digest,
                registration_idempotency_key=cast(
                    str,
                    rebound["registration_idempotency_key"],
                ),
                phase="capacity-restored",
            )
        else:
            self.capacity_authority.reconcile(context)
        self.distributed_runtime_authority.reconcile(context)

    def _prepare_finalization(self, context: DeploymentContext) -> dict[str, Any]:
        if (
            context.deployment.get("phase") != "verified"
            or context.deployment.get("applied_resource_generation") is None
        ):
            raise DeploymentError("deployment finalization binding is invalid")
        recorded_digest = context.deployment.get("finalization_payload_sha256")
        if recorded_digest is not None:
            snapshot = self._snapshot()
            records = [
                row
                for row in cast(
                    list[dict[str, Any]],
                    snapshot["deployment_finalizations"],
                )
                if row["deployment_id"] == context.deployment_id
                and row["payload_sha256"] == recorded_digest
            ]
            if (
                snapshot["generation"] != context.snapshot_generation
                or snapshot["payload_sha256"] != context.snapshot_digest
                or len(records) != 1
            ):
                raise DeploymentError("recorded deployment finalization binding is invalid")
            record = records[0]
            exact = {
                "env_id": context.env_id,
                "principal_id": context.principal_id,
                "candidate_id": context.candidate_id,
                "candidate_sha": context.candidate_sha,
                "candidate_tree": context.candidate_tree,
                "applied_resource_generation": context.runtime_resource_generation,
                "applied_registry_generation": context.applied_registry_generation,
                "applied_registry_payload_sha256": context.applied_registry_digest,
            }
            if any(record.get(field) != value for field, value in exact.items()):
                raise DeploymentError("recorded deployment finalization binding is invalid")
            # A finalization row is the durable commit-intent boundary.  Resume
            # from its exact evidence and never repeat any mutation or probe.
            return {
                "capacity_finalize": record["capacity_finalize_receipt_sha256"],
                "capacity_finalize_check": record["capacity_finalize_check_receipt_sha256"],
                "runtime_reconcile": record["runtime_reconcile_receipt_sha256"],
                "runtime_check": record["runtime_prepare_check_receipt_sha256"],
                "acceptance_probe": record["acceptance_probe_receipt_sha256"],
            }
        self._rebind_committed_services(context)
        capacity_finalize = self.capacity_authority.finalize(context)
        capacity_finalize_check = self.capacity_authority.finalize_check(context)
        runtime_reconcile = self.distributed_runtime_authority.reconcile(context)
        runtime_check = self.distributed_runtime_authority.check(context)
        acceptance_probe = self.distributed_runtime_authority.acceptance_probe(context)
        if _evidence_digest(acceptance_probe) == _evidence_digest(runtime_check):
            raise DeploymentError("acceptance probe evidence is not independent")
        return {
            "capacity_finalize": _evidence_digest(capacity_finalize),
            "capacity_finalize_check": _evidence_digest(capacity_finalize_check),
            "runtime_reconcile": _evidence_digest(runtime_reconcile),
            "runtime_check": _evidence_digest(runtime_check),
            "acceptance_probe": _evidence_digest(acceptance_probe),
        }

    def _verify_services(self, context: DeploymentContext) -> None:
        command = self._compose_argv(context)
        result = self.runner.run((*command, "ps", "--format", "json"), cwd=context.checkout)
        try:
            rows = json.loads(result.stdout or "[]")
        except json.JSONDecodeError as exc:
            raise DeploymentError("Compose service readback is invalid") from exc
        if isinstance(rows, dict):
            rows = [rows]
        if not isinstance(rows, list):
            raise DeploymentError("Compose service readback is invalid")
        running = {
            row.get("Service")
            for row in rows
            if isinstance(row, dict)
            and row.get("State") == "running"
            and row.get("Service") in ALL_SERVICES
        }
        if running != set(ALL_SERVICES):
            raise DeploymentError("developer environment services are not all running")
        manifest = _load_bound_json(
            context.manifest_path,
            kind=MANIFEST_KIND,
            require_root=self.require_root_metadata,
        )
        if (
            manifest is None
            or manifest.get("candidate_id") != context.candidate_id
            or manifest.get("image_digest") != context.image_digest
            or manifest.get("resource_generation") != context.runtime_resource_generation
            or not _plain_int(manifest.get("applied_registry_generation"), minimum=1)
            or re.fullmatch(
                r"[0-9a-f]{64}",
                str(manifest.get("applied_registry_payload_sha256")),
            )
            is None
        ):
            raise DeploymentError("host manifest binding is invalid")
        bound_containers = self.runner.run(
            (
                "docker",
                "ps",
                "--filter",
                f"label=loom.developer-environment.env-id={context.env_id}",
                "--filter",
                f"label=loom.developer-environment.runtime-id={context.environment['runtime_id']}",
                "--filter",
                "label=loom.developer-environment.compose-project="
                f"{context.environment['compose_project']}",
                "--filter",
                f"label=loom.developer-environment.candidate-id={context.candidate_id}",
                "--filter",
                f"label=loom.developer-environment.candidate-sha={context.candidate_sha}",
                "--filter",
                f"label=loom.developer-environment.candidate-tree={context.candidate_tree}",
                "--filter",
                "label=loom.developer-environment.resource-generation="
                f"{manifest['resource_generation']}",
                "--filter",
                "label=loom.developer-environment.registry-generation="
                f"{manifest['applied_registry_generation']}",
                "--filter",
                "label=loom.developer-environment.registry-payload-sha256="
                f"{manifest['applied_registry_payload_sha256']}",
                "--format={{.ID}}",
            )
        ).stdout.splitlines()
        if len(set(bound_containers)) != len(ALL_SERVICES):
            raise DeploymentError("container candidate and image binding is invalid")
        worker_containers = self.runner.run(
            (
                "docker",
                "ps",
                "--filter",
                f"label=loom.developer-environment.env-id={context.env_id}",
                "--filter",
                "label=com.docker.compose.service=worker",
                "--filter",
                f"label=loom.developer-environment.image-digest={context.worker_image_id}",
                "--format={{.ID}}",
            )
        ).stdout.splitlines()
        if len(worker_containers) != 1:
            raise DeploymentError("loom-worker container binding is invalid")
        actual_worker_image = self.runner.run(
            (
                "docker",
                "inspect",
                "--format={{.Image}}",
                worker_containers[0],
            )
        ).stdout.strip()
        if actual_worker_image != context.worker_image_id:
            raise DeploymentError("loom-worker container image binding is invalid")
        ports = cast(dict[str, int], context.environment["ports"])
        for name, port, path in (
            ("control-plane", ports["control_plane"], "/healthz"),
            ("llm-gateway", ports["llm_gateway"], "/healthz"),
            ("minio", ports["minio"], "/minio/health/live"),
        ):
            try:
                with urllib.request.urlopen(
                    f"http://127.0.0.1:{port}{path}",
                    timeout=5,
                ) as response:
                    status = response.status
            except (OSError, urllib.error.URLError) as exc:
                raise DeploymentError(f"{name} health readback failed") from exc
            if status != 200:
                raise DeploymentError(f"{name} health readback failed")

    @staticmethod
    def _unit(context: DeploymentContext) -> str:
        return f"loom-developer-environment@{context.environment['systemd_instance']}.service"

    def _persistence_properties(self, unit: str) -> dict[str, str]:
        return {
            property_name: self.runner.run(
                (
                    "systemctl",
                    "show",
                    unit,
                    f"--property={property_name}",
                    "--value",
                ),
            ).stdout.strip()
            for property_name in ("LoadState", "FragmentPath", "UnitFileState")
        }

    def _verify_persistence(self, context: DeploymentContext) -> None:
        properties = self._persistence_properties(self._unit(context))
        if properties != {
            "LoadState": "loaded",
            "FragmentPath": "/etc/systemd/system/loom-developer-environment@.service",
            "UnitFileState": "enabled",
        }:
            raise DeploymentError("developer environment reboot persistence is invalid")

    def _enable_recovery_unit(self, context: DeploymentContext) -> None:
        unit = self._unit(context)
        properties = self._persistence_properties(unit)
        if (
            properties["LoadState"] != "loaded"
            or properties["FragmentPath"]
            != "/etc/systemd/system/loom-developer-environment@.service"
            or properties["UnitFileState"] not in {"disabled", "enabled"}
        ):
            raise DeploymentError("developer environment recovery unit is invalid")
        if properties["UnitFileState"] != "enabled":
            self.runner.run(("systemctl", "enable", unit))
        self._verify_persistence(context)

    def _disable_recovery_unit(self, environment: Mapping[str, Any]) -> None:
        unit = f"loom-developer-environment@{environment['systemd_instance']}.service"
        properties = self._persistence_properties(unit)
        if properties["LoadState"] in {"", "not-found"}:
            if properties["FragmentPath"] != "" or properties["UnitFileState"] not in {
                "",
                "disabled",
            }:
                raise DeploymentError("developer environment recovery unit is invalid")
            return
        if (
            properties["LoadState"] != "loaded"
            or properties["FragmentPath"]
            != "/etc/systemd/system/loom-developer-environment@.service"
            or properties["UnitFileState"] not in {"disabled", "enabled"}
        ):
            raise DeploymentError("developer environment recovery unit is invalid")
        if properties["UnitFileState"] == "enabled":
            self.runner.run(("systemctl", "disable", "--now", unit))

    def _verify_services_and_persistence(self, context: DeploymentContext) -> None:
        self._verify_services(context)
        self._verify_persistence(context)

    def converge(
        self,
        *,
        env_id: str | None,
        principal_id: str | None,
        candidate_id: str | None,
        idempotency_key: str | None,
        operation: str,
    ) -> dict[str, Any]:
        self._require_host()
        snapshot, environment = self._environment(env_id=env_id, principal_id=principal_id)
        if environment["state"] == "retired":
            raise DeploymentError("retired developer environment cannot be deployed")
        if environment["state"] == "quarantined":
            operation_journal = _load_bound_json(
                self._retire_operation_path(cast(str, environment["env_id"])),
                kind=RETIRE_KIND,
            )
            if (
                operation_journal is None
                or operation_journal.get("env_id") != environment["env_id"]
                or operation_journal.get("principal_id") != environment["principal_id"]
                or operation_journal.get("expected_resource_generation")
                != environment["resource_generation"]
                or operation_journal.get("phase") == "registry-retired"
                or SAFE_ID_RE.fullmatch(
                    str(operation_journal.get("idempotency_key")),
                )
                is None
            ):
                raise DeploymentError("quarantined environment lacks a resumable retirement WAL")
            self.retire(
                env_id=cast(str, environment["env_id"]),
                principal_id=cast(str, environment["principal_id"]),
                idempotency_key=cast(str, operation_journal["idempotency_key"]),
            )
            raise DeploymentError(
                "quarantined environment retirement resumed; use a new create key"
            )
        with self._lock(environment):
            snapshot, environment = self._environment(env_id=env_id, principal_id=principal_id)
            active = [
                row
                for row in cast(list[dict[str, Any]], snapshot["deployments"])
                if row["env_id"] == environment["env_id"]
                and row["phase"] not in {"committed", "failed"}
            ]
            if not active:
                if candidate_id is None or CANDIDATE_ID_RE.fullmatch(candidate_id) is None:
                    raise DeploymentError("candidate binding is required")
                if environment["current_candidate_id"] == candidate_id:
                    committed = [
                        row
                        for row in cast(list[dict[str, Any]], snapshot["deployments"])
                        if row["env_id"] == environment["env_id"]
                        and row["phase"] == "committed"
                        and row["candidate_id"] == candidate_id
                        and row["applied_resource_generation"] == environment["resource_generation"]
                    ]
                    candidates = [
                        row
                        for row in cast(list[dict[str, Any]], snapshot["candidates"])
                        if row["candidate_id"] == candidate_id
                    ]
                    if not committed or len(candidates) != 1:
                        raise DeploymentError("committed deployment binding is unavailable")
                    deployment = max(
                        committed,
                        key=lambda row: (
                            row["expected_resource_generation"],
                            row["updated_at"],
                            row["deployment_id"],
                        ),
                    )
                    replay = DeploymentContext(
                        cast(int, snapshot["generation"]),
                        cast(str, snapshot["payload_sha256"]),
                        environment,
                        candidates[0],
                        deployment,
                        self.host_root,
                    )
                    replay = self._ensure_worker_images(replay)
                    self._verify_bundle(replay)
                    self._verify_checkout(replay, replay.checkout)
                    self._verify_services(replay)
                    self._verify_persistence(replay)
                    capacity_active = self.capacity_authority.check(replay)
                    self._advance_admission_intent(
                        cast(str, environment["runtime_id"]),
                        phase="registry-transitioned",
                        deployment_id=replay.deployment_id,
                        finalization_payload_sha256=cast(
                            str,
                            replay.deployment["finalization_payload_sha256"],
                        ),
                    )
                    runtime_activation = self.distributed_runtime_authority.activate(replay)
                    runtime_readback = self.distributed_runtime_authority.check(replay)
                    runtime_active = {
                        "activation": _evidence_digest(runtime_activation),
                        "readback": _evidence_digest(runtime_readback),
                    }
                    self._advance_admission_intent(
                        cast(str, environment["runtime_id"]),
                        phase="activated",
                    )
                    usable_path = self._global_runtime_path(
                        "usable",
                        f"{replay.deployment_id}.json",
                    )
                    if usable_path.exists() or usable_path.is_symlink():
                        self._usable_receipt(replay)
                    else:
                        self._rebuild_usable_receipt(
                            replay,
                            capacity_active_check=capacity_active,
                            runtime_active_check=runtime_active,
                        )
                    return {
                        "schema_version": 1,
                        "kind": "loom.developer-environment.deploy-result",
                        "status": "committed",
                        "operation": operation,
                        "env_id": replay.env_id,
                        "deployment_id": replay.deployment_id,
                        "candidate_id": replay.candidate_id,
                        "candidate_sha": replay.candidate_sha,
                        "candidate_tree": replay.candidate_tree,
                        "image_digest": replay.image_digest,
                    }
                if idempotency_key is None or SAFE_ID_RE.fullmatch(idempotency_key) is None:
                    raise DeploymentError("deployment idempotency binding is required")
                if operation == "create" and environment["current_candidate_id"] is not None:
                    raise DeploymentError("developer environment already exists")
                if operation == "update" and environment["current_candidate_id"] is None:
                    raise DeploymentError("developer environment is not active")
                try:
                    recovered = self.registry.reconcile_predeployment_ports(
                        cast(str, environment["env_id"]),
                        principal_id=cast(str, environment["principal_id"]),
                        expected_resource_generation=cast(
                            int,
                            environment["resource_generation"],
                        ),
                    )
                except RegistryError as exc:
                    raise DeploymentError(
                        "pre-deployment port recovery failed safely",
                    ) from exc
                if (
                    recovered.env_id != environment["env_id"]
                    or recovered.principal_id != environment["principal_id"]
                    or recovered.resource_generation < environment["resource_generation"]
                ):
                    raise DeploymentError("pre-deployment port recovery binding is invalid")
                if recovered.resource_generation != environment["resource_generation"]:
                    snapshot, environment = self._environment(
                        env_id=cast(str, environment["env_id"]),
                        principal_id=cast(str, environment["principal_id"]),
                    )
                    if (
                        environment["resource_generation"] != recovered.resource_generation
                        or environment["ports"] != recovered.ports
                    ):
                        raise DeploymentError(
                            "pre-deployment port recovery readback is invalid",
                        )
                intent_environment = dict(environment)
                intent_snapshot = dict(snapshot)
                self._fence_admission_intent(
                    intent_snapshot,
                    intent_environment,
                    operation=operation,
                    candidate_id=candidate_id,
                    idempotency_key=idempotency_key,
                )
                # The exact environment remains active/ready while admission is
                # closed, so no request can slip between drain and transition.
                self._assert_drained(intent_environment)
                new_deployment = self.registry.begin_deployment(
                    {
                        "schema_version": 1,
                        "kind": DEPLOY_KIND,
                        "principal_id": environment["principal_id"],
                        "idempotency_key": idempotency_key,
                        "env_id": environment["env_id"],
                        "candidate_id": candidate_id,
                        "expected_resource_generation": environment["resource_generation"],
                    }
                )
                deployment_id = new_deployment.deployment_id
                snapshot = self._snapshot()
                environment = select_environment(
                    snapshot,
                    principal_id=cast(str, environment["principal_id"]),
                    env_id=cast(str, environment["env_id"]),
                    root=False,
                )
                self._advance_admission_intent(
                    cast(str, environment["runtime_id"]),
                    phase="registry-transitioned",
                    deployment_id=deployment_id,
                )
            elif len(active) == 1:
                deployment_id = cast(str, active[0]["deployment_id"])
                if candidate_id is not None and active[0]["candidate_id"] != candidate_id:
                    raise DeploymentError("active deployment candidate conflicts")
                intent = _load_bound_json(
                    self._admission_intent_path(cast(str, environment["runtime_id"])),
                    kind=ADMISSION_INTENT_KIND,
                )
                if (
                    intent is None
                    or intent.get("env_id") != environment["env_id"]
                    or intent.get("target_candidate_id") != active[0]["candidate_id"]
                    or intent.get("resource_generation")
                    != active[0]["expected_resource_generation"]
                    or intent.get("phase") not in {"fenced", "registry-transitioned"}
                ):
                    raise DeploymentError(
                        "active deployment lacks its admission intent",
                    )
                self._advance_admission_intent(
                    cast(str, environment["runtime_id"]),
                    phase="registry-transitioned",
                    deployment_id=deployment_id,
                )
            else:
                raise DeploymentError("multiple active developer deployments are invalid")
            context = _context(
                snapshot,
                environment,
                deployment_id=deployment_id,
                host_root=self.host_root,
            )
            # Image distribution is additive and candidate-scoped.  Complete
            # it on every runtime node before the first host/resource mutation.
            context = self._ensure_worker_images(context)
            # Once the registry owns an exact deployment, make its boot resume
            # durable before the first host, Docker, or capacity mutation.
            self._enable_recovery_unit(context)
            actions = {
                "resources-verified": self._ensure_resources,
                "candidate-materialized": self._materialize_candidate,
                "services-prepared": self._prepare_services,
                "capacity-ready": self._ensure_capacity,
                "verified": self._verify_services_and_persistence,
            }
            while context.deployment["phase"] != "committed":
                current_phase = cast(str, context.deployment["phase"])
                if current_phase not in DEPLOY_PHASES[:-1]:
                    raise DeploymentError("developer deployment cannot be resumed")
                next_phase = DEPLOY_PHASES[DEPLOY_PHASES.index(current_phase) + 1]
                if current_phase == "verified" and next_phase == "committed":
                    self.registry.prepare_deployment_finalization(
                        context.deployment_id,
                        principal_id=context.principal_id,
                        expected_resource_generation=context.resource_generation,
                    )
                    context = self._refresh_context(
                        context.env_id,
                        context.principal_id,
                        context.deployment_id,
                    )
                finalization_evidence: dict[str, Any] | None = None
                if next_phase == "committed":
                    finalization_evidence = self._prepare_finalization(context)
                    self.registry.record_deployment_finalization(
                        context.deployment_id,
                        principal_id=context.principal_id,
                        expected_resource_generation=context.resource_generation,
                        evidence={
                            "capacity_finalize_receipt_sha256": finalization_evidence[
                                "capacity_finalize"
                            ],
                            "capacity_finalize_check_receipt_sha256": (
                                finalization_evidence["capacity_finalize_check"]
                            ),
                            "runtime_reconcile_receipt_sha256": finalization_evidence[
                                "runtime_reconcile"
                            ],
                            "runtime_prepare_check_receipt_sha256": (
                                finalization_evidence["runtime_check"]
                            ),
                            "acceptance_probe_receipt_sha256": finalization_evidence[
                                "acceptance_probe"
                            ],
                        },
                    )
                    context = self._refresh_context(
                        context.env_id,
                        context.principal_id,
                        context.deployment_id,
                    )
                    self._write_finalization_operation(
                        context,
                        phase="finalization-ready",
                        evidence=finalization_evidence,
                    )
                    self._journal(context, "finalization-ready")
                else:
                    action = actions.get(next_phase)
                    if action is not None:
                        action(context)
                    self._journal(context, next_phase)
                self.registry.advance_deployment(
                    context.deployment_id,
                    principal_id=context.principal_id,
                    expected_phase=current_phase,
                    next_phase=next_phase,
                    expected_resource_generation=context.resource_generation,
                )
                if next_phase == "committed":
                    context = self._refresh_committed_context(
                        context.env_id,
                        context.principal_id,
                        context.deployment_id,
                    )
                    if finalization_evidence is None:
                        raise DeploymentError("finalization evidence is unavailable")
                    finalization_evidence["capacity_active_check"] = _evidence_digest(
                        self.capacity_authority.check(context)
                    )
                    self._advance_admission_intent(
                        cast(str, context.environment["runtime_id"]),
                        phase="registry-transitioned",
                        deployment_id=context.deployment_id,
                        finalization_payload_sha256=cast(
                            str,
                            context.deployment["finalization_payload_sha256"],
                        ),
                    )
                    runtime_activation = self.distributed_runtime_authority.activate(context)
                    runtime_active_check = self.distributed_runtime_authority.check(context)
                    self._advance_admission_intent(
                        cast(str, context.environment["runtime_id"]),
                        phase="activated",
                    )
                    finalization_evidence["runtime_active_check"] = _digest(
                        {
                            "activation": _evidence_digest(runtime_activation),
                            "readback": _evidence_digest(runtime_active_check),
                        }
                    )
                    finalization = self._write_finalization_operation(
                        context,
                        phase="committed",
                        evidence=finalization_evidence,
                    )
                    self._write_usable_receipt(
                        context,
                        finalization=finalization,
                    )
                    self._journal(context, "committed")
                    break
                context = self._refresh_context(
                    context.env_id,
                    context.principal_id,
                    context.deployment_id,
                )
            return {
                "schema_version": 1,
                "kind": "loom.developer-environment.deploy-result",
                "status": "committed",
                "operation": operation,
                "env_id": context.env_id,
                "deployment_id": context.deployment_id,
                "candidate_id": context.candidate_id,
                "candidate_sha": context.candidate_sha,
                "candidate_tree": context.candidate_tree,
                "image_digest": context.image_digest,
            }

    def resume(self, *, runtime_id: str) -> dict[str, Any]:
        self._require_host()
        snapshot = self._snapshot()
        environment = select_runtime_environment(
            snapshot,
            runtime_id=runtime_id,
            root=not self.manage_ownership or os.geteuid() == 0,
        )
        active = [
            row
            for row in cast(list[dict[str, Any]], snapshot["deployments"])
            if row["env_id"] == environment["env_id"]
            and row["phase"] not in {"committed", "failed"}
        ]
        if len(active) > 1:
            raise DeploymentError("multiple active developer deployments are invalid")
        candidate_id = active[0]["candidate_id"] if active else environment["current_candidate_id"]
        if candidate_id is None:
            raise DeploymentError("runtime environment has no deployment to resume")
        return self.converge(
            env_id=cast(str, environment["env_id"]),
            principal_id=cast(str, environment["principal_id"]),
            candidate_id=cast(str, candidate_id),
            idempotency_key=None,
            operation="resume",
        )

    def check(
        self,
        *,
        env_id: str | None,
        principal_id: str | None,
    ) -> dict[str, Any]:
        self._require_host()
        snapshot, environment = self._environment(env_id=env_id, principal_id=principal_id)
        if environment["state"] != "active" or environment["current_candidate_id"] is None:
            raise DeploymentError("developer environment is not active")
        deployments = [
            row
            for row in cast(list[dict[str, Any]], snapshot["deployments"])
            if row["env_id"] == environment["env_id"]
            and row["phase"] == "committed"
            and row["candidate_id"] == environment["current_candidate_id"]
            and row["applied_resource_generation"] == environment["resource_generation"]
        ]
        if not deployments:
            raise DeploymentError("committed developer deployment is unavailable")
        deployment = max(
            deployments,
            key=lambda row: (
                row["expected_resource_generation"],
                row["updated_at"],
                row["deployment_id"],
            ),
        )
        candidate = next(
            row
            for row in cast(list[dict[str, Any]], snapshot["candidates"])
            if row["candidate_id"] == environment["current_candidate_id"]
        )
        context = DeploymentContext(
            cast(int, snapshot["generation"]),
            cast(str, snapshot["payload_sha256"]),
            environment,
            candidate,
            deployment,
            self.host_root,
        )
        with self._lock(environment):
            self._usable_receipt(context)
            self._verify_bundle(context)
            self._verify_checkout(context, context.checkout)
            self._verify_services(context)
            self._verify_persistence(context)
            self.capacity_authority.check(context)
            self.distributed_runtime_authority.check(context)
        return {
            "schema_version": 1,
            "kind": "loom.developer-environment.check-result",
            "status": "verified",
            "env_id": context.env_id,
            "candidate_id": context.candidate_id,
            "candidate_sha": context.candidate_sha,
            "candidate_tree": context.candidate_tree,
            "image_digest": context.image_digest,
        }

    def check_runtime(self, *, runtime_id: str) -> dict[str, Any]:
        snapshot = self._snapshot()
        environment = select_runtime_environment(
            snapshot,
            runtime_id=runtime_id,
            root=not self.manage_ownership or os.geteuid() == 0,
        )
        return self.check(
            env_id=cast(str, environment["env_id"]),
            principal_id=cast(str, environment["principal_id"]),
        )

    def _jobs(self, environment: Mapping[str, Any]) -> list[dict[str, str]]:
        account = cast(str, environment["slurm_account"])
        user = cast(str, environment["slurm_user"])
        rows: dict[str, dict[str, str]] = {}
        for selector in (("--account", account), ("--user", user)):
            result = self.runner.run(
                (
                    "squeue",
                    "--noheader",
                    "--states=all",
                    *selector,
                    "--format=%i|%u|%a|%T|%j",
                ),
                expected=frozenset({0}),
            )
            for line in result.stdout.splitlines():
                fields = line.split("|")
                if len(fields) != 5 or not all(fields):
                    raise DeploymentError("Slurm job inventory is invalid")
                job_id, job_user, job_account, state, name = fields
                rows[job_id] = {
                    "job_id": job_id,
                    "user": job_user,
                    "account": job_account,
                    "state": state.split("+", 1)[0],
                    "name": name,
                }
        return list(rows.values())

    def _assert_drained(self, environment: Mapping[str, Any]) -> None:
        expected_name = f"loom-env-{environment['runtime_id']}-"
        for job in self._jobs(environment):
            owned = (
                job["user"] == environment["slurm_user"]
                and job["account"] == environment["slurm_account"]
                and job["name"].startswith(expected_name)
            )
            if not owned or job["state"] not in TERMINAL_JOB_STATES:
                raise DeploymentError("developer environment has foreign or nonterminal Slurm jobs")

    def _stop_exact_owned(self, context: DeploymentContext) -> None:
        manifest = _load_bound_json(context.manifest_path, kind=MANIFEST_KIND)
        owner_labels = _resource_owner_labels(context)
        unit = f"loom-developer-environment@{context.environment['systemd_instance']}.service"
        if (
            manifest is None
            or manifest.get("env_id") != context.env_id
            or manifest.get("compose_project") != context.environment["compose_project"]
            or manifest.get("candidate_checkout") != str(context.checkout)
            or manifest.get("runtime_root") != context.environment["runtime_root"]
            or manifest.get("postgres_volume") != context.environment["postgres_volume"]
            or manifest.get("minio_volume") != context.environment["minio_volume"]
            or manifest.get("resource_owner_sha256")
            != owner_labels["loom.developer-environment.owner-sha256"]
            or manifest.get("systemd_unit") != unit
        ):
            raise DeploymentError("exact-owned host manifest is unavailable")
        resources = (
            ("volume", cast(str, context.environment["postgres_volume"])),
            ("volume", cast(str, context.environment["minio_volume"])),
            ("network", f"{context.environment['compose_project']}_default"),
        )
        for kind, name in resources:
            result = self.runner.run(
                (
                    "docker",
                    kind,
                    "inspect",
                    "--format={{json .Labels}}",
                    name,
                ),
                expected=frozenset({0, 1}),
            )
            if result.returncode == 1:
                continue
            try:
                labels = json.loads(result.stdout)
            except json.JSONDecodeError as exc:
                raise DeploymentError("exact-owned Docker resource labels are invalid") from exc
            if not isinstance(labels, dict) or any(
                labels.get(key) != value for key, value in owner_labels.items()
            ):
                raise DeploymentError("Docker resource is foreign or unlabeled")
        command = self._compose_argv(context)
        self.runner.run(
            (*command, "down", "--remove-orphans"),
            cwd=context.checkout,
            expected=frozenset({0}),
        )
        load_state = self.runner.run(
            ("systemctl", "show", unit, "--property=LoadState", "--value"),
            expected=frozenset({0, 1}),
        ).stdout.strip()
        if load_state not in {"", "not-found"}:
            fragment = self.runner.run(
                ("systemctl", "show", unit, "--property=FragmentPath", "--value"),
                expected=frozenset({0}),
            ).stdout.strip()
            if (
                load_state != "loaded"
                or fragment != "/etc/systemd/system/loom-developer-environment@.service"
            ):
                raise DeploymentError("systemd instance is foreign or unbound")
            self.runner.run(
                ("systemctl", "disable", "--now", unit),
                expected=frozenset({0}),
            )
        for volume in (
            cast(str, context.environment["postgres_volume"]),
            cast(str, context.environment["minio_volume"]),
        ):
            self.runner.run(
                ("docker", "volume", "rm", volume),
                expected=frozenset({0, 1}),
            )
        self.runner.run(
            ("docker", "network", "rm", f"{context.environment['compose_project']}_default"),
            expected=frozenset({0, 1}),
        )

    def _remove_ready_exact_owned(self, environment: Mapping[str, Any]) -> None:
        """Remove an uncommitted/failed-create environment without a manifest."""

        labels = _environment_owner_labels(environment)
        unit = f"loom-developer-environment@{environment['systemd_instance']}.service"
        load_state = self.runner.run(
            ("systemctl", "show", unit, "--property=LoadState", "--value"),
            expected=frozenset({0, 1}),
        ).stdout.strip()
        if load_state not in {"", "not-found"}:
            fragment = self.runner.run(
                ("systemctl", "show", unit, "--property=FragmentPath", "--value"),
                expected=frozenset({0}),
            ).stdout.strip()
            if (
                load_state != "loaded"
                or fragment != "/etc/systemd/system/loom-developer-environment@.service"
            ):
                raise DeploymentError("systemd instance is foreign or unbound")
            self.runner.run(
                ("systemctl", "disable", "--now", unit),
                expected=frozenset({0}),
            )
        resources = (
            ("volume", cast(str, environment["postgres_volume"])),
            ("volume", cast(str, environment["minio_volume"])),
            ("network", f"{environment['compose_project']}_default"),
        )
        for kind, name in resources:
            inspected = self.runner.run(
                ("docker", kind, "inspect", "--format={{json .Labels}}", name),
                expected=frozenset({0, 1}),
            )
            if inspected.returncode == 1:
                continue
            try:
                observed = json.loads(inspected.stdout)
            except json.JSONDecodeError as exc:
                raise DeploymentError("Docker resource labels are invalid") from exc
            if not isinstance(observed, dict) or any(
                observed.get(key) != value for key, value in labels.items()
            ):
                raise DeploymentError("Docker resource is foreign or unlabeled")
            self.runner.run(
                ("docker", kind, "rm", name),
                expected=frozenset({0, 1}),
            )

    def _preflight_retire_local_objects(
        self,
        environment: Mapping[str, Any],
        context: DeploymentContext | None,
    ) -> None:
        """Validate every extant local object before the first destructive action."""

        owner_labels = (
            _resource_owner_labels(context)
            if context is not None
            else _environment_owner_labels(environment)
        )
        unit = f"loom-developer-environment@{environment['systemd_instance']}.service"
        if context is not None:
            manifest = _load_bound_json(context.manifest_path, kind=MANIFEST_KIND)
            if (
                manifest is None
                or manifest.get("env_id") != context.env_id
                or manifest.get("compose_project") != environment["compose_project"]
                or manifest.get("candidate_checkout") != str(context.checkout)
                or manifest.get("runtime_root") != environment["runtime_root"]
                or manifest.get("postgres_volume") != environment["postgres_volume"]
                or manifest.get("minio_volume") != environment["minio_volume"]
                or manifest.get("resource_owner_sha256")
                != owner_labels["loom.developer-environment.owner-sha256"]
                or manifest.get("systemd_unit") != unit
            ):
                raise DeploymentError("exact-owned host manifest is unavailable")
        load_state = self.runner.run(
            ("systemctl", "show", unit, "--property=LoadState", "--value"),
            expected=frozenset({0, 1}),
        ).stdout.strip()
        if load_state not in {"", "not-found"}:
            fragment = self.runner.run(
                ("systemctl", "show", unit, "--property=FragmentPath", "--value"),
                expected=frozenset({0}),
            ).stdout.strip()
            if (
                load_state != "loaded"
                or fragment != "/etc/systemd/system/loom-developer-environment@.service"
            ):
                raise DeploymentError("systemd instance is foreign or unbound")
        for kind, name in (
            ("volume", cast(str, environment["postgres_volume"])),
            ("volume", cast(str, environment["minio_volume"])),
            ("network", f"{environment['compose_project']}_default"),
        ):
            inspected = self.runner.run(
                ("docker", kind, "inspect", "--format={{json .Labels}}", name),
                expected=frozenset({0, 1}),
            )
            if inspected.returncode == 1:
                continue
            try:
                observed = json.loads(inspected.stdout)
            except json.JSONDecodeError as exc:
                raise DeploymentError("exact-owned Docker resource labels are invalid") from exc
            if not isinstance(observed, dict) or any(
                observed.get(key) != value for key, value in owner_labels.items()
            ):
                raise DeploymentError("Docker resource is foreign or unlabeled")

    @staticmethod
    def _validate_runtime_retire_receipt(
        receipt: Mapping[str, Any],
        *,
        context: DeploymentContext,
        retire_operation_sha256: str,
    ) -> dict[str, Any]:
        unsigned = {key: value for key, value in receipt.items() if key != "payload_sha256"}
        nodes = receipt.get("nodes")
        candidates = receipt.get("candidate_bindings")
        if (
            set(receipt) != runtime_retire.COMBINED_RECEIPT_FIELDS
            or receipt.get("schema_version") != 1
            or receipt.get("kind") != runtime_retire.COMBINED_RECEIPT_KIND
            or receipt.get("status") != "cleaned"
            or receipt.get("action") != runtime_retire.ACTION
            or receipt.get("deployment_id") != context.deployment_id
            or receipt.get("env_id") != context.env_id
            or receipt.get("principal_id") != context.principal_id
            or receipt.get("runtime_id") != context.environment["runtime_id"]
            or receipt.get("resource_generation") != context.runtime_resource_generation
            or receipt.get("registry_generation") != context.snapshot_generation
            or receipt.get("registry_snapshot_sha256") != context.snapshot_digest
            or receipt.get("retire_operation_sha256") != retire_operation_sha256
            or not isinstance(nodes, dict)
            or set(nodes) != set(runtime_retire.NODES)
            or any(re.fullmatch(r"[0-9a-f]{64}", str(value)) is None for value in nodes.values())
            or not isinstance(candidates, list)
            or not any(
                isinstance(candidate, dict)
                and candidate
                == {
                    "candidate_id": context.candidate_id,
                    "candidate_sha": context.candidate_sha,
                    "candidate_tree": context.candidate_tree,
                }
                for candidate in candidates
            )
            or not _valid_timestamp(receipt.get("completed_at"))
            or receipt.get("payload_sha256") != _digest(unsigned)
        ):
            raise DeploymentError("runtime retirement fleet receipt binding is invalid")
        return dict(receipt)

    def _compose_service_states(
        self,
        context: DeploymentContext,
    ) -> dict[str, str]:
        result = self.runner.run(
            (*self._compose_argv(context), "ps", "--all", "--format", "json"),
            cwd=context.checkout,
        )
        try:
            rows = json.loads(result.stdout or "[]")
        except json.JSONDecodeError as exc:
            raise DeploymentError("retirement Compose inventory is invalid") from exc
        if isinstance(rows, dict):
            rows = [rows]
        if not isinstance(rows, list):
            raise DeploymentError("retirement Compose inventory is invalid")
        states: dict[str, str] = {}
        for row in rows:
            if (
                not isinstance(row, dict)
                or row.get("Service") not in ALL_SERVICES
                or not isinstance(row.get("State"), str)
                or row["Service"] in states
            ):
                raise DeploymentError("retirement Compose inventory is invalid")
            states[cast(str, row["Service"])] = cast(str, row["State"])
        return states

    def _retire_postgres_checkpoint(
        self,
        context: DeploymentContext | None,
    ) -> tuple[str, Mapping[str, Any]]:
        if context is None:
            return "not-present", {"service": "postgres", "command": "not-run"}
        state = self._compose_service_states(context).get("postgres")
        if state is None:
            return (
                "missing-after-authorized-retry",
                {"service": "postgres", "observed_state": "absent"},
            )
        if state != "running":
            return (
                "missing-after-authorized-retry",
                {"service": "postgres", "observed_state": state},
            )
        self.runner.run(
            (
                *self._compose_argv(context),
                "exec",
                "-T",
                "postgres",
                "sh",
                "-ec",
                'psql --username="$POSTGRES_USER" --dbname="$POSTGRES_DB" --command="CHECKPOINT"',
            ),
            cwd=context.checkout,
        )
        return (
            "checkpointed",
            {
                "service": "postgres",
                "observed_state": "running",
                "command": "CHECKPOINT",
            },
        )

    def _retire_stateful_service(
        self,
        context: DeploymentContext | None,
        *,
        service: str,
    ) -> tuple[str, Mapping[str, Any]]:
        if context is None:
            return "not-present", {"service": service, "command": "not-run"}
        before = self._compose_service_states(context).get(service)
        if before is None:
            return (
                "missing-after-authorized-retry",
                {"service": service, "observed_state": "absent"},
            )
        if before == "running":
            self.runner.run(
                (
                    *self._compose_argv(context),
                    "stop",
                    "--timeout",
                    "30",
                    service,
                ),
                cwd=context.checkout,
            )
        after = self._compose_service_states(context).get(service)
        if after == "running":
            raise DeploymentError(f"{service} remained running after clean stop")
        return (
            "stopped" if before == "running" else "missing-after-authorized-retry",
            {
                "service": service,
                "observed_state_before": before,
                "observed_state_after": "absent" if after is None else after,
                "timeout_seconds": 30,
            },
        )

    def _retire_compose_project(self, context: DeploymentContext | None) -> str:
        if context is None:
            return "not-applicable"
        self.runner.run(
            (*self._compose_argv(context), "down", "--remove-orphans"),
            cwd=context.checkout,
            expected=frozenset({0}),
        )
        return "removed"

    def _verify_retired_container_absence(
        self,
        context: DeploymentContext | None,
    ) -> tuple[str, Mapping[str, Any]]:
        if context is None:
            return "not-present", {"services": []}
        states = self._compose_service_states(context)
        if states:
            raise DeploymentError("exact-owned Compose containers remain after shutdown")
        return "removed", {"services": [], "container_count": 0}

    def _retire_systemd_unit(self, environment: Mapping[str, Any]) -> str:
        unit = f"loom-developer-environment@{environment['systemd_instance']}.service"
        properties = self._persistence_properties(unit)
        if properties["LoadState"] in {"", "not-found"}:
            if properties["FragmentPath"] != "" or properties["UnitFileState"] not in {
                "",
                "disabled",
            }:
                raise DeploymentError("systemd instance is foreign or unbound")
            return "missing-after-authorized-retry"
        if (
            properties["LoadState"] != "loaded"
            or properties["FragmentPath"]
            != "/etc/systemd/system/loom-developer-environment@.service"
            or properties["UnitFileState"] not in {"disabled", "enabled"}
        ):
            raise DeploymentError("systemd instance is foreign or unbound")
        if properties["UnitFileState"] == "disabled":
            return "missing-after-authorized-retry"
        self.runner.run(
            ("systemctl", "disable", "--now", unit),
            expected=frozenset({0}),
        )
        return "disabled"

    def _retire_docker_object(
        self,
        environment: Mapping[str, Any],
        context: DeploymentContext | None,
        *,
        kind: str,
        name: str,
    ) -> str:
        owner_labels = (
            _resource_owner_labels(context)
            if context is not None
            else _environment_owner_labels(environment)
        )
        inspected = self.runner.run(
            ("docker", kind, "inspect", "--format={{json .Labels}}", name),
            expected=frozenset({0, 1}),
        )
        if inspected.returncode == 1:
            return "missing-after-authorized-retry"
        try:
            observed = json.loads(inspected.stdout)
        except json.JSONDecodeError as exc:
            raise DeploymentError("exact-owned Docker resource labels are invalid") from exc
        if not isinstance(observed, dict) or any(
            observed.get(key) != value for key, value in owner_labels.items()
        ):
            raise DeploymentError("Docker resource is foreign or unlabeled")
        removed = self.runner.run(
            ("docker", kind, "rm", name),
            expected=frozenset({0, 1}),
        )
        if removed.returncode not in {0, 1}:
            raise DeploymentError("exact-owned Docker resource removal failed")
        rebound = self.runner.run(
            ("docker", kind, "inspect", "--format={{json .Labels}}", name),
            expected=frozenset({0, 1}),
        )
        if rebound.returncode != 1:
            raise DeploymentError("exact-owned Docker resource remained after removal")
        return "removed"

    def _retire_tree(
        self,
        environment: Mapping[str, Any],
        *,
        field: str,
    ) -> str:
        raw_path = cast(str, environment[field])
        path = Path(raw_path)
        if self.host_root is not None:
            path = self.host_root.joinpath(*path.parts[1:])
        existed = path.exists()
        allowed_uids = frozenset(
            {0, cast(int, environment["uid"])} if self.manage_ownership else {os.geteuid()}
        )
        _remove_exact_owned_tree(path, allowed_uids=allowed_uids)
        if path.exists():
            raise DeploymentError("exact-owned removal remained after cleanup")
        return "removed" if existed else "missing-after-authorized-retry"

    def _retire_privileged_compose_inputs(
        self,
        context: DeploymentContext | None,
    ) -> tuple[str, Mapping[str, Any]]:
        if context is None:
            return "not-present", {"removed": []}
        removed: list[str] = []
        for path in (context.compose_env_path, context.compose_override_path):
            try:
                _read_stable_regular(
                    path,
                    limit=1024 * 1024,
                    require_root=self.require_root_metadata,
                    expected_mode=0o600,
                )
            except DeploymentError:
                if not path.exists() and not path.is_symlink():
                    continue
                raise
            try:
                path.unlink()
            except OSError as exc:
                raise DeploymentError("privileged Compose input cleanup failed") from exc
            removed.append(path.name)
        try:
            descriptor = os.open(
                context.lifecycle_root,
                os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | getattr(os, "O_CLOEXEC", 0),
            )
            try:
                os.fsync(descriptor)
            finally:
                os.close(descriptor)
        except OSError as exc:
            raise DeploymentError("privileged Compose input cleanup failed") from exc
        return (
            "removed" if removed else "missing-after-authorized-retry",
            {"removed": sorted(removed)},
        )

    def _remove_environment_trees(self, environment: Mapping[str, Any]) -> None:
        allowed_uids = frozenset(
            {0, cast(int, environment["uid"])} if self.manage_ownership else {os.geteuid()}
        )
        for raw_path in (
            cast(str, environment["candidate_root"]),
            cast(str, environment["runtime_root"]),
            cast(str, environment["state_root"]),
        ):
            path = Path(raw_path)
            if self.host_root is not None:
                path = self.host_root.joinpath(*path.parts[1:])
            _remove_exact_owned_tree(path, allowed_uids=allowed_uids)

    def retire(
        self,
        *,
        env_id: str | None,
        principal_id: str | None,
        idempotency_key: str,
    ) -> dict[str, Any]:
        self._require_host()
        if SAFE_ID_RE.fullmatch(idempotency_key) is None:
            raise DeploymentError("retirement idempotency binding is invalid")
        snapshot, environment = self._environment(env_id=env_id, principal_id=principal_id)
        if environment["state"] not in {"ready", "active", "quarantined"}:
            if environment["state"] != "retired":
                raise DeploymentError("developer environment cannot be retired")
        with self._retirement_lock(cast(str, environment["env_id"])):
            snapshot, environment = self._environment(env_id=env_id, principal_id=principal_id)
            operation = _load_bound_json(
                self._retire_operation_path(cast(str, environment["env_id"])),
                kind=RETIRE_KIND,
            )
            cycle_generation = cast(int, environment["resource_generation"]) - (
                1 if environment["state"] == "retired" else 0
            )
            if (
                operation is not None
                and operation.get("expected_resource_generation") != cycle_generation
            ):
                if (
                    operation.get("phase") != "registry-retired"
                    or type(operation.get("expected_resource_generation")) is not int
                    or cast(int, operation["expected_resource_generation"]) >= cycle_generation
                ):
                    raise DeploymentError("retirement current pointer drifted")
                operation = None
            exact_operation = _load_bound_json(
                self._retire_operation_path(
                    cast(str, environment["env_id"]),
                    expected_resource_generation=cycle_generation,
                    idempotency_key=idempotency_key,
                ),
                kind=RETIRE_KIND,
            )
            if exact_operation is not None:
                if operation is not None and operation != exact_operation:
                    raise DeploymentError("retirement current pointer drifted")
                operation = exact_operation
            if operation is not None and operation.get("idempotency_key") != idempotency_key:
                raise DeploymentError("retirement idempotency key conflicts")
            if environment["state"] == "retired":
                if operation is not None and operation.get("phase") == "local-cleaned":
                    operation = self._write_retire_operation(
                        environment,
                        idempotency_key=idempotency_key,
                        phase="registry-retired",
                        expected_resource_generation=cast(
                            int,
                            operation["expected_resource_generation"],
                        ),
                        evidence=cast(dict[str, str], operation["evidence"]),
                        object_checkpoints=cast(
                            dict[str, dict[str, Any]],
                            operation.get("object_checkpoints", {}),
                        ),
                    )
                    self._write_cleanup_receipt(
                        environment,
                        operation=operation,
                        retired_resource_generation=cast(
                            int,
                            environment["resource_generation"],
                        ),
                    )
                receipt = _load_bound_json(
                    self._cleanup_receipt_path(cast(str, environment["env_id"])),
                    kind=RETIRE_RECEIPT_KIND,
                )
                if (
                    operation is None
                    or operation.get("phase") != "registry-retired"
                    or receipt is None
                ):
                    raise DeploymentError("retired environment lacks its cleanup receipt")
                admission_intent = _load_bound_json(
                    self._admission_intent_path(
                        cast(str, environment["runtime_id"]),
                    ),
                    kind=ADMISSION_INTENT_KIND,
                )
                if (
                    admission_intent is not None
                    and admission_intent.get("operation") == "retire"
                    and admission_intent.get("phase") == "registry-transitioned"
                ):
                    self._advance_admission_intent(
                        cast(str, environment["runtime_id"]),
                        phase="retired",
                    )
                return {
                    "schema_version": 1,
                    "kind": "loom.developer-environment.retire-result",
                    "status": "retired",
                    "env_id": environment["env_id"],
                    "cleanup_receipt_sha256": receipt["payload_sha256"],
                }
            active = [
                row
                for row in cast(list[dict[str, Any]], snapshot["deployments"])
                if row["env_id"] == environment["env_id"]
                and row["phase"] not in {"committed", "failed"}
            ]
            if active:
                raise DeploymentError("active deployment must be rolled back before retirement")
            expected_generation = cast(
                int,
                (
                    operation["expected_resource_generation"]
                    if operation is not None
                    else environment["resource_generation"]
                ),
            )
            evidence = (
                cast(dict[str, str], dict(operation["evidence"])) if operation is not None else {}
            )
            object_checkpoints = (
                cast(
                    dict[str, dict[str, Any]],
                    dict(operation.get("object_checkpoints", {})),
                )
                if operation is not None
                else {}
            )
            phase = cast(str, operation["phase"]) if operation is not None else None
            if operation is None:
                operation = self._write_retire_operation(
                    environment,
                    idempotency_key=idempotency_key,
                    phase="intent-recorded",
                    expected_resource_generation=expected_generation,
                    evidence=evidence,
                    object_checkpoints=object_checkpoints,
                )
                phase = "intent-recorded"
            if phase == "intent-recorded":
                admission_intent = self._fence_admission_intent(
                    snapshot,
                    environment,
                    operation="retire",
                    candidate_id=None,
                    idempotency_key=idempotency_key,
                )
                self._assert_drained(environment)
                evidence["admission_fence"] = cast(
                    str,
                    admission_intent["fence_receipt_sha256"],
                )
                operation = self._write_retire_operation(
                    environment,
                    idempotency_key=idempotency_key,
                    phase="admission-fenced",
                    expected_resource_generation=expected_generation,
                    evidence=evidence,
                    object_checkpoints=object_checkpoints,
                )
                phase = "admission-fenced"
            if phase == "admission-fenced" and environment["state"] != "quarantined":
                self.registry.begin_retirement(
                    cast(str, environment["env_id"]),
                    principal_id=cast(str, environment["principal_id"]),
                    expected_resource_generation=cast(
                        int,
                        environment["resource_generation"],
                    ),
                )
                snapshot, environment = self._environment(
                    env_id=env_id,
                    principal_id=principal_id,
                )
                self._advance_admission_intent(
                    cast(str, environment["runtime_id"]),
                    phase="registry-transitioned",
                )
            if phase == "admission-fenced":
                operation = self._write_retire_operation(
                    environment,
                    idempotency_key=idempotency_key,
                    phase="quarantined",
                    expected_resource_generation=expected_generation,
                    evidence=evidence,
                    object_checkpoints=object_checkpoints,
                )
                phase = "quarantined"
            current_candidate = environment["current_candidate_id"]
            context: DeploymentContext | None = None
            if current_candidate is not None:
                deployments = [
                    row
                    for row in cast(list[dict[str, Any]], snapshot["deployments"])
                    if row["env_id"] == environment["env_id"]
                    and row["phase"] == "committed"
                    and row["candidate_id"] == current_candidate
                    and row["applied_resource_generation"] == environment["resource_generation"]
                ]
                candidate = next(
                    row
                    for row in cast(list[dict[str, Any]], snapshot["candidates"])
                    if row["candidate_id"] == current_candidate
                )
                deployment = max(
                    deployments,
                    key=lambda row: (
                        row["expected_resource_generation"],
                        row["updated_at"],
                        row["deployment_id"],
                    ),
                )
                context = DeploymentContext(
                    cast(int, snapshot["generation"]),
                    cast(str, snapshot["payload_sha256"]),
                    environment,
                    candidate,
                    deployment,
                    self.host_root,
                )
            if phase == "quarantined":
                capacity = (
                    self.capacity_authority.retire(context)
                    if context is not None
                    else {"status": "absent"}
                )
                evidence["capacity_retire"] = _evidence_digest(capacity)
                operation = self._write_retire_operation(
                    environment,
                    idempotency_key=idempotency_key,
                    phase="capacity-retired",
                    expected_resource_generation=expected_generation,
                    evidence=evidence,
                    object_checkpoints=object_checkpoints,
                )
                phase = "capacity-retired"
            if phase == "capacity-retired":
                central_runtime = (
                    self.distributed_runtime_authority.retire(context)
                    if context is not None
                    else {"status": "absent"}
                )
                if context is None:
                    fleet_runtime: dict[str, Any] = {"status": "absent"}
                else:
                    fleet_runtime = self._validate_runtime_retire_receipt(
                        self.runtime_retire_executor(
                            context.deployment_id,
                            context.env_id,
                            cast(str, operation["payload_sha256"]),
                        ),
                        context=context,
                        retire_operation_sha256=cast(
                            str,
                            operation["payload_sha256"],
                        ),
                    )
                evidence["runtime_retire"] = _digest(
                    {
                        "central_runtime": _evidence_digest(central_runtime),
                        "fleet_runtime": _evidence_digest(fleet_runtime),
                    }
                )
                operation = self._write_retire_operation(
                    environment,
                    idempotency_key=idempotency_key,
                    phase="runtime-retired",
                    expected_resource_generation=expected_generation,
                    evidence=evidence,
                    object_checkpoints=object_checkpoints,
                )
                phase = "runtime-retired"
            if phase == "runtime-retired":
                if "local_preflight" not in object_checkpoints:
                    self._preflight_retire_local_objects(environment, context)
                    object_checkpoints["local_preflight"] = self._retire_object_checkpoint(
                        environment,
                        object_name="local_preflight",
                        status="validated",
                        expected_resource_generation=expected_generation,
                    )
                    operation = self._write_retire_operation(
                        environment,
                        idempotency_key=idempotency_key,
                        phase="runtime-retired",
                        expected_resource_generation=expected_generation,
                        evidence=evidence,
                        object_checkpoints=object_checkpoints,
                    )
                actions: tuple[tuple[str, Any], ...] = (
                    (
                        "postgres_checkpoint",
                        lambda: self._retire_postgres_checkpoint(context),
                    ),
                    (
                        "control_plane_stop",
                        lambda: self._retire_stateful_service(
                            context,
                            service="control-plane",
                        ),
                    ),
                    (
                        "minio_stop",
                        lambda: self._retire_stateful_service(
                            context,
                            service="minio",
                        ),
                    ),
                    ("compose_project", lambda: self._retire_compose_project(context)),
                    (
                        "container_absence",
                        lambda: self._verify_retired_container_absence(context),
                    ),
                    (
                        "systemd_unit",
                        lambda: self._retire_systemd_unit(environment),
                    ),
                    (
                        "postgres_volume",
                        lambda: self._retire_docker_object(
                            environment,
                            context,
                            kind="volume",
                            name=cast(str, environment["postgres_volume"]),
                        ),
                    ),
                    (
                        "minio_volume",
                        lambda: self._retire_docker_object(
                            environment,
                            context,
                            kind="volume",
                            name=cast(str, environment["minio_volume"]),
                        ),
                    ),
                    (
                        "compose_network",
                        lambda: self._retire_docker_object(
                            environment,
                            context,
                            kind="network",
                            name=f"{environment['compose_project']}_default",
                        ),
                    ),
                    (
                        "candidate_tree",
                        lambda: self._retire_tree(
                            environment,
                            field="candidate_root",
                        ),
                    ),
                    (
                        "runtime_tree",
                        lambda: self._retire_tree(
                            environment,
                            field="runtime_root",
                        ),
                    ),
                    (
                        "state_tree",
                        lambda: self._retire_tree(
                            environment,
                            field="state_root",
                        ),
                    ),
                    (
                        "privileged_compose_inputs",
                        lambda: self._retire_privileged_compose_inputs(context),
                    ),
                )
                for object_name, action in actions:
                    if object_name in object_checkpoints:
                        continue
                    outcome = action()
                    if isinstance(outcome, tuple):
                        status = cast(str, outcome[0])
                        details = cast(Mapping[str, Any], outcome[1])
                    else:
                        status = cast(str, outcome)
                        details = None
                    object_checkpoints[object_name] = self._retire_object_checkpoint(
                        environment,
                        object_name=object_name,
                        status=status,
                        expected_resource_generation=expected_generation,
                        details=details,
                    )
                    operation = self._write_retire_operation(
                        environment,
                        idempotency_key=idempotency_key,
                        phase="runtime-retired",
                        expected_resource_generation=expected_generation,
                        evidence=evidence,
                        object_checkpoints=object_checkpoints,
                    )
                evidence["local_cleanup"] = _digest(
                    {
                        "env_id": environment["env_id"],
                        "resource_generation": expected_generation,
                        "status": "removed",
                        "objects": {
                            name: object_checkpoints[name]["payload_sha256"]
                            for name in RETIRE_LOCAL_OBJECTS
                        },
                    }
                )
                operation = self._write_retire_operation(
                    environment,
                    idempotency_key=idempotency_key,
                    phase="local-cleaned",
                    expected_resource_generation=expected_generation,
                    evidence=evidence,
                    object_checkpoints=object_checkpoints,
                )
                phase = "local-cleaned"
            if phase == "local-cleaned":
                retired = self.registry.retire_environment(
                    cast(str, environment["env_id"]),
                    principal_id=cast(str, environment["principal_id"]),
                    expected_resource_generation=expected_generation,
                )
                operation = self._write_retire_operation(
                    environment,
                    idempotency_key=idempotency_key,
                    phase="registry-retired",
                    expected_resource_generation=expected_generation,
                    evidence=evidence,
                    object_checkpoints=object_checkpoints,
                )
                receipt = self._write_cleanup_receipt(
                    environment,
                    operation=operation,
                    retired_resource_generation=cast(
                        int,
                        retired.resource_generation,
                    ),
                )
                self._advance_admission_intent(
                    cast(str, environment["runtime_id"]),
                    phase="retired",
                )
            else:
                receipt = _load_bound_json(
                    self._cleanup_receipt_path(cast(str, environment["env_id"])),
                    kind=RETIRE_RECEIPT_KIND,
                )
                if receipt is None:
                    raise DeploymentError("retirement cleanup receipt is unavailable")
        return {
            "schema_version": 1,
            "kind": "loom.developer-environment.retire-result",
            "status": "retired",
            "env_id": environment["env_id"],
            "cleanup_receipt_sha256": receipt["payload_sha256"],
        }

    def revive(
        self,
        *,
        env_id: str,
        principal_id: str,
        idempotency_key: str,
        registration_idempotency_key: str,
    ) -> dict[str, Any]:
        """Revive only an exact, durably cleaned retired identity."""

        self._require_host()
        if (
            SAFE_ID_RE.fullmatch(idempotency_key) is None
            or SAFE_ID_RE.fullmatch(registration_idempotency_key) is None
            or not self.registry.registration_idempotency_replay(
                principal_id=principal_id,
                idempotency_key=registration_idempotency_key,
            )
        ):
            raise DeploymentError("revival idempotency binding is invalid")
        snapshot, environment = self._environment(
            env_id=env_id,
            principal_id=principal_id,
        )
        if environment["state"] not in {"retired", "ready"}:
            raise DeploymentError("developer environment is not retired")
        with self._lock(environment):
            snapshot, environment = self._environment(
                env_id=env_id,
                principal_id=principal_id,
            )
            cleanup = _load_bound_json(
                self._cleanup_receipt_path(env_id),
                kind=RETIRE_RECEIPT_KIND,
            )
            if cleanup is None:
                raise DeploymentError("retired environment lacks cleanup evidence")
            expected_retired_generation = cast(
                int,
                cleanup["retired_resource_generation"],
            )
            if environment["state"] == "retired":
                self.registry.revive_environment(
                    env_id,
                    principal_id=principal_id,
                    expected_resource_generation=expected_retired_generation,
                )
            else:
                if environment["resource_generation"] != expected_retired_generation + 1:
                    raise DeploymentError("revival replay generation drifted")
            snapshot = self._snapshot()
            environment = select_environment(
                snapshot,
                principal_id=principal_id,
                env_id=env_id,
                root=False,
            )
            journal = self._write_revive_operation(
                environment,
                idempotency_key=idempotency_key,
                cleanup_receipt=cleanup,
                registry_generation=cast(int, snapshot["generation"]),
                registry_payload_sha256=cast(str, snapshot["payload_sha256"]),
                registration_idempotency_key=registration_idempotency_key,
            )
        return {
            "schema_version": 1,
            "kind": "loom.developer-environment.revive-result",
            "status": "ready",
            "env_id": env_id,
            "resource_generation": environment["resource_generation"],
            "revive_journal_sha256": journal["payload_sha256"],
        }

    def rollback(
        self,
        *,
        env_id: str | None,
        principal_id: str | None,
        idempotency_key: str,
    ) -> dict[str, Any]:
        self._require_host()
        if SAFE_ID_RE.fullmatch(idempotency_key) is None:
            raise DeploymentError("rollback idempotency binding is invalid")
        snapshot, environment = self._environment(env_id=env_id, principal_id=principal_id)
        active = [
            row
            for row in cast(list[dict[str, Any]], snapshot["deployments"])
            if row["env_id"] == environment["env_id"]
            and row["phase"] not in {"committed", "failed"}
        ]
        operation_path = self._rollback_operation_path(environment)
        operation = _load_bound_json(operation_path, kind=ROLLBACK_KIND)
        replay_deployment_id: str | None = None
        if (
            operation is not None
            and operation.get("phase") != "complete"
            and operation.get("idempotency_key") != idempotency_key
        ):
            raise DeploymentError("another rollback operation is active")
        if operation is not None and operation.get("idempotency_key") == idempotency_key:
            if operation.get("phase") == "complete":
                return {
                    "schema_version": 1,
                    "kind": "loom.developer-environment.rollback-result",
                    "status": "committed" if environment["current_candidate_id"] else "ready",
                    "operation": "rollback",
                    "env_id": environment["env_id"],
                    "candidate_id": environment["current_candidate_id"],
                }
            replay_deployment_id = cast(str, operation["deployment_id"])
        if active:
            deployment = active[0]
            if (
                replay_deployment_id is not None
                and deployment["deployment_id"] != replay_deployment_id
            ):
                raise DeploymentError("rollback operation deployment binding drifted")
            candidate = next(
                row
                for row in cast(list[dict[str, Any]], snapshot["candidates"])
                if row["candidate_id"] == deployment["candidate_id"]
            )
            context = DeploymentContext(
                cast(int, snapshot["generation"]),
                cast(str, snapshot["payload_sha256"]),
                environment,
                candidate,
                deployment,
                self.host_root,
            )
            with self._lock(environment):
                self._assert_drained(environment)
                self._write_rollback_operation(
                    context,
                    idempotency_key=idempotency_key,
                    phase="registered",
                )
                if context.deployment["previous_candidate_id"] is None and DEPLOY_PHASES.index(
                    cast(str, context.deployment["phase"])
                ) >= DEPLOY_PHASES.index("services-prepared"):
                    self.capacity_authority.abort(context)
                    self._write_rollback_operation(
                        context,
                        idempotency_key=idempotency_key,
                        phase="capacity-aborted",
                    )
                self.registry.fail_deployment(
                    context.deployment_id,
                    principal_id=context.principal_id,
                    expected_phase=cast(str, context.deployment["phase"]),
                    expected_resource_generation=context.resource_generation,
                )
                failed_context = self._refresh_failed_context(
                    context.env_id,
                    context.principal_id,
                    context.deployment_id,
                )
                self._write_rollback_operation(
                    failed_context,
                    idempotency_key=idempotency_key,
                    phase="runtime-pending",
                )
                post_fail_snapshot, post_fail_environment = self._environment(
                    env_id=context.env_id,
                    principal_id=context.principal_id,
                )
                del post_fail_snapshot
                active_context = self._active_committed_context(post_fail_environment)
                if active_context is not None:
                    self.capacity_authority.rollback(failed_context)
                    self.capacity_authority.check(active_context)
                self.distributed_runtime_authority.rollback(failed_context)
                admission_path = self._admission_intent_path(
                    cast(str, post_fail_environment["runtime_id"]),
                )
                if admission_path.exists():
                    self._advance_admission_intent(
                        cast(str, post_fail_environment["runtime_id"]),
                        phase="activated",
                    )
                if self._manifest_belongs_to(context):
                    self._stop_exact_owned(context)
                if active_context is None:
                    self._disable_recovery_unit(post_fail_environment)
            snapshot, environment = self._environment(env_id=env_id, principal_id=principal_id)
            self._restore_active_local(environment)
            self._write_rollback_operation(
                failed_context,
                idempotency_key=idempotency_key,
                phase="complete",
            )
            return {
                "schema_version": 1,
                "kind": "loom.developer-environment.rollback-result",
                "status": "committed" if environment["current_candidate_id"] else "ready",
                "operation": "rollback",
                "env_id": environment["env_id"],
                "candidate_id": environment["current_candidate_id"],
            }
        if replay_deployment_id is not None:
            failed_context = self._refresh_failed_context(
                cast(str, environment["env_id"]),
                cast(str, environment["principal_id"]),
                replay_deployment_id,
            )
            with self._lock(environment):
                self._assert_drained(environment)
                active_context = self._active_committed_context(environment)
                if active_context is not None:
                    self.capacity_authority.rollback(failed_context)
                    self.capacity_authority.check(active_context)
                self.distributed_runtime_authority.rollback(failed_context)
                admission_path = self._admission_intent_path(
                    cast(str, environment["runtime_id"]),
                )
                if admission_path.exists():
                    self._advance_admission_intent(
                        cast(str, environment["runtime_id"]),
                        phase="activated",
                    )
                if self._manifest_belongs_to(failed_context):
                    self._stop_exact_owned(failed_context)
                if active_context is None:
                    self._disable_recovery_unit(environment)
                snapshot, environment = self._environment(
                    env_id=env_id,
                    principal_id=principal_id,
                )
                self._restore_active_local(environment)
                self._write_rollback_operation(
                    failed_context,
                    idempotency_key=idempotency_key,
                    phase="complete",
                )
            return {
                "schema_version": 1,
                "kind": "loom.developer-environment.rollback-result",
                "status": "committed" if environment["current_candidate_id"] else "ready",
                "operation": "rollback",
                "env_id": environment["env_id"],
                "candidate_id": environment["current_candidate_id"],
            }
        current = environment["current_candidate_id"]
        if current is None:
            return {
                "schema_version": 1,
                "kind": "loom.developer-environment.rollback-result",
                "status": "ready",
                "env_id": environment["env_id"],
                "candidate_id": None,
            }
        committed = [
            row
            for row in cast(list[dict[str, Any]], snapshot["deployments"])
            if row["env_id"] == environment["env_id"]
            and row["phase"] == "committed"
            and row["candidate_id"] == current
            and row["applied_resource_generation"] == environment["resource_generation"]
        ]
        latest = max(
            committed,
            key=lambda row: (
                row["expected_resource_generation"],
                row["updated_at"],
                row["deployment_id"],
            ),
        )
        target = latest["previous_candidate_id"]
        if target is None:
            raise DeploymentError("developer environment has no rollback candidate")
        result = self.converge(
            env_id=cast(str, environment["env_id"]),
            principal_id=cast(str, environment["principal_id"]),
            candidate_id=cast(str, target),
            idempotency_key=idempotency_key,
            operation="update",
        )
        return {
            **result,
            "kind": "loom.developer-environment.rollback-result",
            "operation": "rollback",
        }


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__, allow_abbrev=False)
    parser.add_argument(
        "command",
        choices=(
            "create",
            "update",
            "check",
            "resume",
            "renew-active",
            "rollback",
            "destroy",
            "retire",
        ),
    )
    parser.add_argument("--env-id")
    parser.add_argument("--runtime-id")
    parser.add_argument("--candidate-id")
    parser.add_argument("--idempotency-key")
    parser.add_argument("--execute", action="store_true")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(list(argv) if argv is not None else None)
    selector_invalid = (
        args.command == "renew-active" and (args.env_id is not None or args.runtime_id is not None)
    ) or (
        args.command != "renew-active"
        and (
            (args.command == "resume" and (args.runtime_id is None or args.env_id is not None))
            or (args.command != "resume" and ((args.env_id is None) == (args.runtime_id is None)))
        )
    )
    if selector_invalid:
        sys.stderr.write("error: exact environment selector is required\n")
        return 1
    if os.getuid() != 0 or os.geteuid() != 0:
        sys.stderr.write("error: developer environment deployment requires root\n")
        return 1
    if args.command != "check" and not args.execute:
        sys.stderr.write("error: developer environment mutation requires --execute\n")
        return 1
    try:
        registry = DeveloperEnvironmentRegistry.open_system()
        deployer = DeveloperEnvironmentDeployer(registry)
        if args.command == "renew-active":
            result = deployer.renew_active()
        elif args.command in {"create", "update"}:
            result = deployer.converge(
                env_id=args.env_id,
                principal_id=None,
                candidate_id=args.candidate_id,
                idempotency_key=args.idempotency_key,
                operation=args.command,
            )
        elif args.command == "check":
            result = (
                deployer.check_runtime(runtime_id=args.runtime_id)
                if args.runtime_id is not None
                else deployer.check(env_id=args.env_id, principal_id=None)
            )
        elif args.command == "resume":
            result = deployer.resume(runtime_id=cast(str, args.runtime_id))
        elif args.command == "rollback":
            if args.idempotency_key is None:
                raise DeploymentError("rollback idempotency binding is required")
            result = deployer.rollback(
                env_id=args.env_id,
                principal_id=None,
                idempotency_key=args.idempotency_key,
            )
        else:
            if args.idempotency_key is None:
                raise DeploymentError("retirement idempotency binding is required")
            result = deployer.retire(
                env_id=args.env_id,
                principal_id=None,
                idempotency_key=args.idempotency_key,
            )
        sys.stdout.write(json.dumps(result, sort_keys=True, separators=(",", ":")) + "\n")
        return 0
    except (DeploymentError, RegistryError) as exc:
        sys.stderr.write(f"error: {exc}\n")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
