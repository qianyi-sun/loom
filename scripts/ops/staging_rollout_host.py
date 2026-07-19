#!/usr/bin/env python3
"""Install and verify the fixed platform-dev staging rollout service.

The public CLI intentionally has no repository, ref, host, user, or destination
overrides.  Mutation is isolated behind small filesystem and host-system
adapters so installation behavior can be proven without touching a real host.
"""

from __future__ import annotations

import argparse
import base64
import binascii
import contextlib
import errno
import fcntl
import hashlib
import json
import os
import re
import shlex
import shutil
import stat
import subprocess
import sys
import tempfile
import tomllib
import uuid
from collections.abc import Iterator, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol

try:
    from scripts.ops.staging_rollout_sealed_source import (
        MAX_CUMULATIVE_COMMITS,
        SealedSource,
        SealedSourceError,
        validate_sealed_source,
    )
except ModuleNotFoundError:  # direct execution from scripts/ops
    from staging_rollout_sealed_source import (
        MAX_CUMULATIVE_COMMITS,
        SealedSource,
        SealedSourceError,
        validate_sealed_source,
    )  # type: ignore[import-not-found, no-redef]

REPO_ROOT = Path(__file__).resolve().parents[2]
DEPLOY_ROOT = REPO_ROOT / "deploy" / "staging-rollout"
REMOTE_URL = "https://github.com/qianyi-sun/loom.git"
FETCH_REF = "refs/heads/dev"
SERVICE_USER = "loom-rollout"
SERVICE_GROUP = "loom-rollout"
OPERATOR_GROUP = "loom-staging-operators"
OPERATORS = ("qianyi", "hongjian", "devansh")
SHARED_WORK_CONSUMER = "qianyi"
SHARED_WORK_GROUP = "sharedwork"
SHARED_WORK2_MOUNT_POINT = Path("/shared_work2")
SHARED_WORK2_MOUNT_UNIT = "shared_work2.mount"
SHARED_WORK2_MOUNT_UNIT_PATH = Path("/etc/systemd/system") / SHARED_WORK2_MOUNT_UNIT
SHARED_WORKER_AUTHORITY_ROOT = SHARED_WORK2_MOUNT_POINT / "qianyi/.loom-staging-rollout"
SHARED_WORKER_REPO_ROOT = SHARED_WORKER_AUTHORITY_ROOT / "worker-repos"

RUNNER_ROOT = Path("/opt/loom-staging-runner")
INSTALL_SOURCE = RUNNER_ROOT / "source"
SHARED_WORK2_MOUNT_HELPER = INSTALL_SOURCE / "scripts/ops/staging_rollout_shared_work2.py"
SHARED_WORKER_REPO_HELPER = INSTALL_SOURCE / "scripts/ops/staging_rollout_shared_repo.py"
CANDIDATE_REPO = RUNNER_ROOT / "repo"
VENV = RUNNER_ROOT / "venv"
STATE_ROOT = Path("/var/lib/loom-staging-rollout")
ACTIVE_POINTER = STATE_ROOT / "active.json"
GENERATED_ROOT = STATE_ROOT / "generated"
GENERATED_GB10_ENV_SEED = GENERATED_ROOT / "staging-gb10-worker-staging-bootstrap.env"
LEGACY_GB10_ENV_ROOT = Path("/shared_work/qianyi/loom-worker-capacity")
RUNTIME_ROOT = Path("/run/loom-staging-rollout")
MAINTENANCE_MARKER = RUNTIME_ROOT / "maintenance"
CONFIG_PATH = Path("/etc/loom/staging-rollout.toml")
CLIENT_PATH = Path("/usr/local/bin/loom-staging-rollout")
BROKER_PATH = Path("/usr/local/libexec/loom-staging-rollout-broker")
REHEARSAL_PATH = Path("/usr/local/libexec/loom-staging-rollout-rehearsal")
TRUST_TOOL_PATH = Path("/usr/local/libexec/loom-staging-rollout-gb10-trust")
SUDOERS_PATH = Path("/etc/sudoers.d/loom-staging-rollout")
TMPFILES_PATH = Path("/etc/tmpfiles.d/loom-staging-rollout.conf")
KUBECONFIG_PATH = STATE_ROOT / "kubeconfig"
ROOT_KUBECONFIG = Path("/root/.kube/config")
ROOT_KUBECONFIG_SNAPSHOT_PARENT = Path("/root")
SERVICE_KEY = STATE_ROOT / "gb10-deploy-ed25519"
INSTALL_RECORD = Path("/etc/loom/staging-rollout.install.json")
INSTALL_ATTESTATION = Path("/etc/loom/staging-rollout.install-attestation.json")
TRUST_REVOCATION_LEDGER = Path("/etc/loom/staging-rollout-gb10-trust-revocation.json")
TRUST_REVOCATION_TOMBSTONE = Path("/etc/loom/.staging-rollout-gb10-trust-revocation.finalizing")
TRUST_LIFECYCLE_LOCK = Path("/etc/loom/staging-rollout-gb10-trust.lock")
TRUST_LOCK_FD_ENV = "LOOM_GB10_TRUST_LOCK_FD"
KNOWN_HOSTS_PATH = Path("/etc/loom/staging-rollout-gb10-known-hosts")
SYSTEM_PYTHON = Path("/usr/bin/python3")
UV_BINARY = Path("/usr/local/bin/uv")
SYSTEM_SHELL = Path("/bin/sh")
SYSTEM_GIT = Path("/usr/bin/git")
SEALED_SOURCE_UPLOAD_PACK = (
    f"{SYSTEM_GIT} -c safe.directory={INSTALL_SOURCE / '.git'} upload-pack"
)

PROTECTED_INPUTS = (
    Path("/shared_work/qianyi/loom-worker-capacity/staging-admin-token"),
    Path("/shared_work/qianyi/loom-worker-capacity/staging-service-token"),
    Path("/shared_work/qianyi/loom-worker-capacity/staging-worker-token"),
    Path("/shared_work/qianyi/loom-worker-capacity/staging-taskset-fence-canary-token"),
    Path("/shared_work/qianyi/loom-worker-capacity/staging-catalog-provisioning.env"),
)
DATA_DIRECTORIES = (
    Path("/data/loom-staging/rollouts"),
    Path("/data/loom-staging/postgres"),
    Path("/data/loom-staging/minio"),
    Path("/data/loom-staging/backups"),
    Path("/data/loom-staging/environment-state"),
)

_FINGERPRINT_TOKEN = "__ADMIN_TOKEN_FINGERPRINT__"
_TEAM_TOKEN = "__SMOKE_ON_BEHALF_TEAM_ID__"
_SHA_RE = re.compile(r"^[0-9a-f]{40}$")
_MAX_PROTECTED_INPUT_BYTES = 4 << 20
_MAX_KUBECONFIG_BYTES = 1 << 20
_MAX_WORKER_ENV_BYTES = 1 << 20
_ROOT_PATH = "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"
_ROLLOUT_UNIT_RE = re.compile(r"^loom-staging-rollout-[A-Za-z0-9_.@:-]+-[1-9][0-9]*[.]service$")
_SYSTEMD_STATE_TOKEN_RE = re.compile(r"^[a-z0-9-]+$")
_RUNTIME_IMPORT_RENDER = (
    "import loom_cli.rollout.operator.broker; "
    "from loom_cli.cluster_cmd import render_manifests; "
    "from loom_cli.cluster_config import ClusterConfig; "
    "rendered = render_manifests(ClusterConfig())"
)
_PACKAGE_RUNTIME_PROBE = _RUNTIME_IMPORT_RENDER + "; raise SystemExit(0 if rendered.strip() else 1)"
_BROKER_RUNTIME_PROBE = (
    _RUNTIME_IMPORT_RENDER
    + "; from loom_cli.rollout.operator.config import OperatorConfig"
    + "; from loom_cli.rollout.operator.envelope import fixed_operator_config_path"
    + "; OperatorConfig.load(fixed_operator_config_path())"
    + "; raise SystemExit(0 if rendered.strip() else 1)"
)
_ACL_PERMISSIONS_RE = re.compile(r"^[r-][w-][x-]$")
_ACL_ENTRY_RE = re.compile(
    r"(?:(default):)?(user|group|mask|other):([^:]*):([rwx-]{3})"
    r"(?:\s+#effective:([rwx-]{3}))?"
)
_ACL_SNAPSHOT_ENTRY_RE = re.compile(r"(user|group|mask|other):([^:]*):([rwx-]{3})")
_ACL_ENTRY_ORDER = {"user": 0, "group": 1, "mask": 2, "other": 3}
_GB10_ENV_NAME_RE = re.compile(r"^staging-gb10-worker-staging-[A-Za-z0-9._-]+[.]env$")
_REQUIRED_GB10_ENV_KEYS = frozenset(
    {
        "LOOM_WORKER_CONTROL_PLANE_URL",
        "LOOM_WORKER_GATEWAY_URL",
        "LOOM_WORKER_TOKEN",
        "LOOM_WORKER_MINIO_ENDPOINT",
        "LOOM_WORKER_MINIO_ACCESS_KEY",
        "LOOM_WORKER_MINIO_SECRET_KEY",
    }
)


class InstallError(RuntimeError):
    """Fail-closed installation or convergence error."""


_INSTALL_ATTESTATION_ASSETS = frozenset(
    {
        "broker",
        "client",
        "config",
        "gb10-known-hosts",
        "gb10-trust-tool",
        "rehearsal-helper",
        "shared-work2-mount-unit",
        "tmpfiles",
    }
)


def _runner_install_attestation_payload(
    record: dict[str, object],
    assets: dict[str, bytes],
) -> bytes:
    """Render the minimal root-issued statement readable by the service broker."""
    if (
        record.get("installation_state") != "ready"
        or record.get("admission_enabled") is not True
        or record.get("maintenance_enabled") is not False
        or set(assets) != _INSTALL_ATTESTATION_ASSETS
    ):
        raise InstallError("runner install attestation inputs are incomplete")
    source_sha = record.get("source_sha")
    source_mode = record.get("source_mode", "merged-dev")
    if not isinstance(source_sha, str) or _SHA_RE.fullmatch(source_sha) is None:
        raise InstallError("runner install attestation source SHA is invalid")
    if source_mode == "sealed-cumulative":
        source_tree_sha = record.get("source_tree_sha")
        source_base_sha = record.get("source_base_sha")
        if (
            not isinstance(source_tree_sha, str)
            or _SHA_RE.fullmatch(source_tree_sha) is None
            or not isinstance(source_base_sha, str)
            or _SHA_RE.fullmatch(source_base_sha) is None
        ):
            raise InstallError("runner install attestation sealed identity is invalid")
    elif source_mode == "merged-dev":
        source_tree_sha = "none"
        source_base_sha = "none"
    else:
        raise InstallError("runner install attestation source mode is invalid")
    record_payload = (json.dumps(record, sort_keys=True) + "\n").encode()
    value = {
        "asset_sha256": {
            label: hashlib.sha256(payload).hexdigest() for label, payload in sorted(assets.items())
        },
        "install_record_sha256": hashlib.sha256(record_payload).hexdigest(),
        "schema_version": 1,
        "source_base_sha": source_base_sha,
        "source_mode": source_mode,
        "source_sha": source_sha,
        "source_tree_sha": source_tree_sha,
    }
    return (json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n").encode()


def _validate_gb10_env_payload(payload: bytes) -> None:
    if not payload or len(payload) > _MAX_WORKER_ENV_BYTES:
        raise InstallError("GB10 worker env template payload is invalid")
    try:
        text = payload.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise InstallError("GB10 worker env template is not UTF-8") from exc
    keys: set[str] = set()
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if "=" not in line:
            raise InstallError("GB10 worker env template contains a malformed entry")
        key, value = line.split("=", 1)
        key = key.strip()
        if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", key) is None or key in keys:
            raise InstallError("GB10 worker env template contains an invalid key")
        if key in _REQUIRED_GB10_ENV_KEYS:
            if "${" in value:
                raise InstallError(
                    "GB10 worker env template required values cannot use interpolation"
                )
            try:
                semantic_parts = shlex.split(value, comments=True, posix=True)
            except ValueError as exc:
                raise InstallError("GB10 worker env template contains a malformed entry") from exc
            if not any(part.strip() for part in semantic_parts):
                raise InstallError("GB10 worker env template contains an empty value")
        keys.add(key)
    if not _REQUIRED_GB10_ENV_KEYS.issubset(keys):
        raise InstallError("GB10 worker env template is missing required settings")


def _acl_snapshot_map(
    snapshot: Sequence[str],
    *,
    allow_empty: bool,
) -> dict[tuple[str, str], str]:
    entries: dict[tuple[str, str], str] = {}
    for raw_entry in snapshot:
        if not isinstance(raw_entry, str):
            raise InstallError("install record ACL snapshot is invalid")
        match = _ACL_SNAPSHOT_ENTRY_RE.fullmatch(raw_entry)
        if match is None:
            raise InstallError("install record ACL snapshot is invalid")
        tag, qualifier, permissions = match.groups()
        if tag in {"mask", "other"} and qualifier:
            raise InstallError("install record ACL snapshot is invalid")
        key = (tag, qualifier)
        if key in entries:
            raise InstallError("install record ACL snapshot contains duplicate entries")
        entries[key] = permissions
    if not entries:
        if allow_empty:
            return entries
        raise InstallError("install record ACL snapshot is empty")
    required = {("user", ""), ("group", ""), ("other", "")}
    if not required.issubset(entries):
        raise InstallError("install record ACL snapshot has invalid base entries")
    named = any(qualifier and tag in {"user", "group"} for tag, qualifier in entries)
    if named and ("mask", "") not in entries:
        raise InstallError("install record ACL snapshot has named entries without a mask")
    return entries


def _canonical_acl_snapshot(entries: dict[tuple[str, str], str]) -> tuple[str, ...]:
    return tuple(
        f"{tag}:{qualifier}:{permissions}"
        for (tag, qualifier), permissions in sorted(
            entries.items(),
            key=lambda item: (_ACL_ENTRY_ORDER[item[0][0]], item[0][1]),
        )
    )


@dataclass(frozen=True, slots=True)
class CommandResult:
    returncode: int
    stdout: str = ""
    stderr: str = ""


@dataclass(frozen=True, slots=True)
class AclGrant:
    path: Path
    default: bool = False

    def to_dict(self) -> dict[str, object]:
        return {"path": str(self.path), "default": self.default}

    @classmethod
    def from_dict(cls, value: object) -> AclGrant:
        if not isinstance(value, dict) or set(value) != {"path", "default"}:
            raise InstallError("install record contains an invalid ACL grant")
        raw_path = value["path"]
        default = value["default"]
        if not isinstance(raw_path, str) or type(default) is not bool:
            raise InstallError("install record contains an invalid ACL grant")
        path = Path(raw_path)
        if not path.is_absolute() or ".." in path.parts:
            raise InstallError("install record contains an unsafe ACL path")
        return cls(path=path, default=default)


@dataclass(frozen=True, slots=True)
class AclMaskAdjustment:
    path: Path
    default: bool
    before_mask: str | None
    after_mask: str
    before_acl: tuple[str, ...]
    after_acl: tuple[str, ...]

    def to_dict(self) -> dict[str, object]:
        return {
            "path": str(self.path),
            "default": self.default,
            "before_mask": self.before_mask,
            "after_mask": self.after_mask,
            "before_acl": list(self.before_acl),
            "after_acl": list(self.after_acl),
        }

    @classmethod
    def from_dict(cls, value: object) -> AclMaskAdjustment:
        if not isinstance(value, dict) or set(value) != {
            "path",
            "default",
            "before_mask",
            "after_mask",
            "before_acl",
            "after_acl",
        }:
            raise InstallError("install record contains an invalid ACL mask adjustment")
        raw_path = value["path"]
        default = value["default"]
        before_mask = value["before_mask"]
        after_mask = value["after_mask"]
        raw_before_acl = value["before_acl"]
        raw_after_acl = value["after_acl"]
        if (
            not isinstance(raw_path, str)
            or type(default) is not bool
            or (before_mask is not None and not isinstance(before_mask, str))
            or not isinstance(after_mask, str)
            or (before_mask is not None and _ACL_PERMISSIONS_RE.fullmatch(before_mask) is None)
            or _ACL_PERMISSIONS_RE.fullmatch(after_mask) is None
            or not isinstance(raw_before_acl, list)
            or not isinstance(raw_after_acl, list)
        ):
            raise InstallError("install record contains an invalid ACL mask adjustment")
        path = Path(raw_path)
        if not path.is_absolute() or ".." in path.parts:
            raise InstallError("install record contains an unsafe ACL mask path")
        before_acl = tuple(raw_before_acl)
        after_acl = tuple(raw_after_acl)
        before_entries = _acl_snapshot_map(before_acl, allow_empty=default)
        after_entries = _acl_snapshot_map(after_acl, allow_empty=False)
        if (
            before_entries.get(("mask", "")) != before_mask
            or after_entries.get(("mask", "")) != after_mask
        ):
            raise InstallError("install record ACL mask snapshot is inconsistent")
        if before_mask is not None and (
            before_mask == after_mask
            or not all(
                wanted == "-" or after_mask[index] == wanted
                for index, wanted in enumerate(before_mask)
            )
        ):
            raise InstallError("install record ACL mask adjustment is not monotonic")
        service_key = ("user", SERVICE_USER)
        if service_key not in after_entries:
            raise InstallError("install record ACL snapshot omits the service entry")
        if service_key in before_entries and (
            before_entries[service_key] != after_entries[service_key]
        ):
            raise InstallError("install record ACL snapshot changes a pre-existing service entry")
        before_stable = {
            key: permissions
            for key, permissions in before_entries.items()
            if key not in {service_key, ("mask", "")}
        }
        after_stable = {
            key: permissions
            for key, permissions in after_entries.items()
            if key not in {service_key, ("mask", "")}
        }
        if before_entries:
            if before_stable != after_stable:
                raise InstallError("install record ACL snapshot changes unrelated entries")
        elif not default or set(after_stable) != {
            ("user", ""),
            ("group", ""),
            ("other", ""),
        }:
            raise InstallError("install record default ACL initialization is invalid")
        if before_acl == after_acl:
            raise InstallError("install record ACL mask adjustment is empty")
        return cls(
            path=path,
            default=default,
            before_mask=before_mask,
            after_mask=after_mask,
            before_acl=before_acl,
            after_acl=after_acl,
        )


@dataclass(frozen=True, slots=True)
class ParsedAclEntry:
    default: bool
    tag: str
    qualifier: str
    permissions: str
    effective: str


@dataclass(frozen=True, slots=True)
class AclPlan:
    grant: AclGrant
    permissions: str
    adds_service_entry: bool = True
    before_acl: tuple[str, ...] = ()
    after_acl: tuple[str, ...] = ()
    mask_adjustment: AclMaskAdjustment | None = None


class Runner(Protocol):
    def run(
        self,
        argv: Sequence[str],
        *,
        input_text: str | None = None,
        check: bool = True,
        env: dict[str, str] | None = None,
        pass_fds: tuple[int, ...] = (),
    ) -> CommandResult: ...


class SubprocessRunner:
    def run(
        self,
        argv: Sequence[str],
        *,
        input_text: str | None = None,
        check: bool = True,
        env: dict[str, str] | None = None,
        pass_fds: tuple[int, ...] = (),
    ) -> CommandResult:
        completed = subprocess.run(
            list(argv),
            input=input_text,
            capture_output=True,
            text=True,
            check=False,
            pass_fds=pass_fds,
            env=env
            or {
                "PATH": _ROOT_PATH,
                "LANG": "C.UTF-8",
                "LC_ALL": "C.UTF-8",
            },
        )
        result = CommandResult(completed.returncode, completed.stdout, completed.stderr)
        if check and result.returncode != 0:
            raise InstallError(f"command failed safely: {Path(argv[0]).name}")
        return result


@dataclass(slots=True)
class LocalFilesystem:
    """Filesystem adapter whose optional root supports isolated tests."""

    root: Path = Path("/")

    def path(self, absolute: Path) -> Path:
        if not absolute.is_absolute() or ".." in absolute.parts:
            raise InstallError("installer path must be absolute and normalized")
        if self.root == Path("/"):
            return absolute
        return self.root.joinpath(*absolute.parts[1:])

    def exists(self, absolute: Path) -> bool:
        return self.path(absolute).exists()

    def is_safe_directory(self, absolute: Path) -> bool:
        path = self.path(absolute)
        return path.is_dir() and not path.is_symlink()

    def read_bytes(self, absolute: Path, *, limit: int = 1 << 20) -> bytes:
        path = self.path(absolute)
        flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
        try:
            fd = os.open(path, flags)
        except OSError as exc:
            raise InstallError(f"required protected file is unavailable: {absolute}") from exc
        try:
            metadata = os.fstat(fd)
            if not stat.S_ISREG(metadata.st_mode) or metadata.st_size > limit:
                raise InstallError(f"required protected file is invalid: {absolute}")
            payload = os.read(fd, limit + 1)
            if len(payload) > limit:
                raise InstallError(f"required protected file is too large: {absolute}")
            return payload
        finally:
            os.close(fd)

    def _gb10_env_candidates(
        self,
        root: Path,
        *,
        require_private_mode: bool,
    ) -> tuple[tuple[Path, os.stat_result, bytes], ...]:
        directory = self.path(root)
        if not directory.exists():
            return ()
        try:
            directory_metadata = os.lstat(directory)
        except OSError as exc:
            raise InstallError("GB10 worker env template directory is unavailable") from exc
        if not stat.S_ISDIR(directory_metadata.st_mode) or stat.S_ISLNK(directory_metadata.st_mode):
            raise InstallError("GB10 worker env template directory is unsafe")
        candidates: list[tuple[Path, os.stat_result, bytes]] = []
        try:
            entries = list(os.scandir(directory))
        except OSError as exc:
            raise InstallError("GB10 worker env template directory is unreadable") from exc
        for entry in entries:
            if _GB10_ENV_NAME_RE.fullmatch(entry.name) is None:
                continue
            absolute = root / entry.name
            flags = (
                os.O_RDONLY
                | getattr(os, "O_CLOEXEC", 0)
                | getattr(
                    os,
                    "O_NOFOLLOW",
                    0,
                )
            )
            try:
                fd = os.open(entry.path, flags)
            except OSError as exc:
                if exc.errno == errno.ELOOP:
                    raise InstallError("GB10 worker env template metadata is unsafe") from exc
                raise InstallError("GB10 worker env template is unavailable") from exc
            try:
                metadata = os.fstat(fd)
                expected_mode = 0o600 if require_private_mode else stat.S_IMODE(metadata.st_mode)
                if (
                    not stat.S_ISREG(metadata.st_mode)
                    or metadata.st_nlink != 1
                    or metadata.st_size <= 0
                    or metadata.st_size > _MAX_WORKER_ENV_BYTES
                    or stat.S_IMODE(metadata.st_mode) != expected_mode
                    or (not require_private_mode and stat.S_IMODE(metadata.st_mode) & 0o022)
                ):
                    raise InstallError("GB10 worker env template metadata is unsafe")
                payload = os.read(fd, _MAX_WORKER_ENV_BYTES + 1)
                if len(payload) != metadata.st_size:
                    raise InstallError("GB10 worker env template changed during validation")
            finally:
                os.close(fd)
            _validate_gb10_env_payload(payload)
            current = os.lstat(entry.path)
            if (current.st_dev, current.st_ino, current.st_size, current.st_mtime_ns) != (
                metadata.st_dev,
                metadata.st_ino,
                metadata.st_size,
                metadata.st_mtime_ns,
            ):
                raise InstallError("GB10 worker env template changed during validation")
            candidates.append((absolute, metadata, payload))
        return tuple(candidates)

    def generated_gb10_env_templates(self) -> tuple[Path, ...]:
        return tuple(
            path
            for path, _, _ in self._gb10_env_candidates(
                GENERATED_ROOT,
                require_private_mode=True,
            )
        )

    def legacy_gb10_env_template_payload(self) -> bytes:
        candidates = self._gb10_env_candidates(
            LEGACY_GB10_ENV_ROOT,
            require_private_mode=False,
        )
        if not candidates:
            raise InstallError("legacy GB10 worker env template is unavailable")
        _, _, payload = max(
            candidates,
            key=lambda item: (item[1].st_mtime_ns, item[0].name),
        )
        return payload

    def ensure_directory(self, absolute: Path, mode: int) -> bool:
        path = self.path(absolute)
        if path.exists() and (path.is_symlink() or not path.is_dir()):
            raise InstallError(f"installation directory is unsafe: {absolute}")
        changed = not path.exists()
        path.mkdir(parents=True, exist_ok=True)
        current = stat.S_IMODE(path.stat().st_mode)
        if current != mode:
            path.chmod(mode)
            changed = True
        return changed

    @staticmethod
    def _fsync_directory(path: Path) -> None:
        flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_DIRECTORY", 0)
        directory_fd = os.open(path, flags)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)

    def atomic_write(
        self,
        absolute: Path,
        payload: bytes,
        mode: int,
        *,
        expected_nlink: int | None = None,
    ) -> bool:
        path = self.path(absolute)
        if path.exists() or path.is_symlink():
            metadata = os.lstat(path)
            if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISREG(metadata.st_mode):
                raise InstallError(f"installation destination is unsafe: {absolute}")
            if (
                stat.S_IMODE(metadata.st_mode) == mode
                and (expected_nlink is None or metadata.st_nlink == expected_nlink)
                and metadata.st_size == len(payload)
                and path.read_bytes() == payload
            ):
                self._fsync_directory(path.parent)
                return False
        path.parent.mkdir(parents=True, exist_ok=True)
        fd, temporary = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
        try:
            os.fchmod(fd, mode)
            with os.fdopen(fd, "wb", closefd=True) as handle:
                handle.write(payload)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temporary, path)
            if expected_nlink is not None and os.lstat(path).st_nlink != expected_nlink:
                raise InstallError(f"installation destination link count is unsafe: {absolute}")
            self._fsync_directory(path.parent)
        except BaseException:
            try:
                os.close(fd)
            except OSError:
                pass
            Path(temporary).unlink(missing_ok=True)
            raise
        return True

    def remove(self, absolute: Path) -> bool:
        path = self.path(absolute)
        if not path.exists() and not path.is_symlink():
            return False
        if path.is_dir() and not path.is_symlink():
            raise InstallError(f"refusing recursive installer removal: {absolute}")
        path.unlink()
        return True

    def _validated_trust_ledger(
        self,
        *,
        expected_fingerprint: str,
        absolute: Path = TRUST_REVOCATION_LEDGER,
    ) -> tuple[Path, os.stat_result]:
        if absolute not in {TRUST_REVOCATION_LEDGER, TRUST_REVOCATION_TOMBSTONE}:
            raise InstallError("GB10 trust revocation ledger finalization path is invalid")
        path = self.path(absolute)
        if not path.exists() and not path.is_symlink():
            raise InstallError("GB10 trust revocation ledger disappeared before finalization")
        parent = path.parent
        expected_uid = 0 if self.root == Path("/") else os.geteuid()
        expected_gid = 0 if self.root == Path("/") else os.getegid()
        try:
            parent_metadata = os.lstat(parent)
        except OSError as exc:
            raise InstallError("GB10 trust revocation ledger directory is unavailable") from exc
        if (
            not stat.S_ISDIR(parent_metadata.st_mode)
            or stat.S_ISLNK(parent_metadata.st_mode)
            or parent_metadata.st_uid != expected_uid
            or parent_metadata.st_gid != expected_gid
            or stat.S_IMODE(parent_metadata.st_mode) != 0o755
        ):
            raise InstallError("GB10 trust revocation ledger directory is unsafe")
        flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
        try:
            fd = os.open(path, flags)
        except OSError as exc:
            raise InstallError("GB10 trust revocation ledger is unavailable") from exc
        try:
            metadata = os.fstat(fd)
            if (
                not stat.S_ISREG(metadata.st_mode)
                or metadata.st_uid != expected_uid
                or metadata.st_gid != expected_gid
                or stat.S_IMODE(metadata.st_mode) != 0o600
                or metadata.st_size <= 0
                or metadata.st_size > 64 * 1024
            ):
                raise InstallError("GB10 trust revocation ledger metadata is unsafe")
            try:
                payload = os.read(fd, 64 * 1024 + 1)
            except OSError as exc:
                raise InstallError("GB10 trust revocation ledger could not be read") from exc
            if len(payload) != metadata.st_size:
                raise InstallError("GB10 trust revocation ledger changed while it was read")
        finally:
            os.close(fd)
        try:
            raw = json.loads(payload)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise InstallError("GB10 trust revocation ledger is invalid") from exc
        topology_sha256 = raw.get("topology_sha256") if isinstance(raw, dict) else None
        if (
            not isinstance(raw, dict)
            or set(raw)
            != {
                "active_policy_sha256",
                "key_fingerprint",
                "revocation_hosts",
                "schema_version",
                "topology_sha256",
            }
            or type(raw.get("schema_version")) is not int
            or raw.get("schema_version") != 2
            or raw.get("key_fingerprint") != expected_fingerprint
            or raw.get("revocation_hosts") != []
            or not isinstance(topology_sha256, str)
            or re.fullmatch(r"[0-9a-f]{64}", topology_sha256) is None
            or not isinstance(raw.get("active_policy_sha256"), str)
            or re.fullmatch(r"[0-9a-f]{64}", str(raw.get("active_policy_sha256"))) is None
        ):
            raise InstallError("GB10 trust revocation ledger is not safe to finalize")
        try:
            current = os.lstat(path)
        except OSError as exc:
            raise InstallError("GB10 trust revocation ledger changed before finalization") from exc
        if (current.st_dev, current.st_ino) != (metadata.st_dev, metadata.st_ino):
            raise InstallError("GB10 trust revocation ledger changed before finalization")
        return path, metadata

    def validate_trust_ledger_for_removal(self, *, expected_fingerprint: str) -> None:
        ledger = self.path(TRUST_REVOCATION_LEDGER)
        tombstone = self.path(TRUST_REVOCATION_TOMBSTONE)
        ledger_present = ledger.exists() or ledger.is_symlink()
        tombstone_present = tombstone.exists() or tombstone.is_symlink()
        if ledger_present == tombstone_present:
            raise InstallError("GB10 trust revocation ledger finalization state is ambiguous")
        self._validated_trust_ledger(
            expected_fingerprint=expected_fingerprint,
            absolute=(TRUST_REVOCATION_LEDGER if ledger_present else TRUST_REVOCATION_TOMBSTONE),
        )

    def remove_validated_trust_ledger(self, *, expected_fingerprint: str) -> bool:
        path = self.path(TRUST_REVOCATION_LEDGER)
        tombstone = self.path(TRUST_REVOCATION_TOMBSTONE)
        path_present = path.exists() or path.is_symlink()
        tombstone_present = tombstone.exists() or tombstone.is_symlink()
        if path_present and tombstone_present:
            raise InstallError("GB10 trust revocation ledger finalization state is ambiguous")
        if tombstone_present:
            path, metadata = self._validated_trust_ledger(
                expected_fingerprint=expected_fingerprint,
                absolute=TRUST_REVOCATION_TOMBSTONE,
            )
        else:
            path, metadata = self._validated_trust_ledger(
                expected_fingerprint=expected_fingerprint,
                absolute=TRUST_REVOCATION_LEDGER,
            )
            try:
                os.replace(path, tombstone)
            except OSError as exc:
                raise InstallError("GB10 trust revocation ledger could not be isolated") from exc
            path = tombstone
            isolated_path, isolated_metadata = self._validated_trust_ledger(
                expected_fingerprint=expected_fingerprint,
                absolute=TRUST_REVOCATION_TOMBSTONE,
            )
            if isolated_path != path or (
                isolated_metadata.st_dev,
                isolated_metadata.st_ino,
            ) != (metadata.st_dev, metadata.st_ino):
                raise InstallError("GB10 trust revocation ledger compare-and-swap failed")
        try:
            current = os.lstat(path)
        except OSError as exc:
            raise InstallError("GB10 trust revocation ledger changed before removal") from exc
        if (current.st_dev, current.st_ino) != (metadata.st_dev, metadata.st_ino):
            raise InstallError("GB10 trust revocation ledger changed before removal")
        original = self.path(TRUST_REVOCATION_LEDGER)
        if original.exists() or original.is_symlink():
            raise InstallError("GB10 trust revocation ledger reappeared during removal")
        try:
            path.unlink()
        except OSError as exc:
            raise InstallError("GB10 trust revocation ledger could not be removed") from exc
        try:
            directory_fd = os.open(path.parent, os.O_RDONLY | getattr(os, "O_CLOEXEC", 0))
        except OSError as exc:
            raise InstallError("GB10 trust revocation ledger removal is not durable") from exc
        try:
            try:
                os.fsync(directory_fd)
            except OSError as exc:
                raise InstallError("GB10 trust revocation ledger removal is not durable") from exc
        finally:
            os.close(directory_fd)
        return True

    def remove_tree(self, absolute: Path) -> bool:
        if absolute not in {GENERATED_ROOT, RUNTIME_ROOT}:
            raise InstallError("refusing recursive removal outside service-generated state")
        path = self.path(absolute)
        if not path.exists():
            return False
        if path.is_symlink() or not path.is_dir():
            raise InstallError("service-generated state is unsafe")
        for directory, directories, filenames in os.walk(path, topdown=False, followlinks=False):
            current = Path(directory)
            for name in filenames:
                child = current / name
                if child.is_symlink() or not child.is_file():
                    raise InstallError("service-generated state contains an unsafe entry")
                child.unlink()
            for name in directories:
                child = current / name
                if child.is_symlink() or not child.is_dir():
                    raise InstallError("service-generated state contains an unsafe entry")
                child.rmdir()
        path.rmdir()
        return True

    def load_install_record(self) -> dict[str, object] | None:
        path = self.path(INSTALL_RECORD)
        if not path.exists():
            return None
        payload = self.read_bytes(INSTALL_RECORD, limit=1 << 20)
        try:
            parsed = json.loads(payload)
        except json.JSONDecodeError as exc:
            raise InstallError("install record is invalid") from exc
        if not isinstance(parsed, dict):
            raise InstallError("install record is invalid")
        record: dict[str, object] = {}
        for key, value in parsed.items():
            if not isinstance(key, str):
                raise InstallError("install record is invalid")
            record[key] = value
        schema_version = record.get("schema_version")
        if type(schema_version) is not int or schema_version not in {1, 2, 3, 4}:
            raise InstallError("install record is invalid")
        return record

    def file_matches(
        self,
        absolute: Path,
        payload: bytes,
        mode: int,
        *,
        expected_nlink: int | None = None,
    ) -> bool:
        path = self.path(absolute)
        try:
            metadata = os.lstat(path)
            return bool(
                stat.S_ISREG(metadata.st_mode)
                and not stat.S_ISLNK(metadata.st_mode)
                and stat.S_IMODE(metadata.st_mode) == mode
                and (expected_nlink is None or metadata.st_nlink == expected_nlink)
                and metadata.st_size == len(payload)
                and path.read_bytes() == payload
            )
        except OSError:
            return False


def _validate_owned_tree(
    root: Path,
    *,
    expected_uid: int,
    expected_gid: int,
    allowed_external_symlink_targets: tuple[Path, ...] = (),
    require_nonwritable: bool = True,
) -> None:
    """Reject writable or replaceable descendants in an executable authority tree."""
    try:
        root_metadata = os.lstat(root)
    except OSError as exc:
        raise InstallError(f"authority tree is unavailable: {root}") from exc
    if not stat.S_ISDIR(root_metadata.st_mode) or stat.S_ISLNK(root_metadata.st_mode):
        raise InstallError(f"authority tree root is unsafe: {root}")
    root_resolved = root.resolve()
    try:
        allowed = {path.resolve(strict=False) for path in allowed_external_symlink_targets}
    except OSError as exc:
        raise InstallError(f"authority tree external target is unavailable: {root}") from exc

    for directory, directories, filenames in os.walk(root, followlinks=False):
        for name in [".", *directories, *filenames]:
            path = Path(directory) if name == "." else Path(directory) / name
            try:
                metadata = os.lstat(path)
            except OSError as exc:
                raise InstallError(f"authority tree changed during validation: {root}") from exc
            if metadata.st_uid != expected_uid or metadata.st_gid != expected_gid:
                raise InstallError(f"authority tree ownership is unsafe: {root}")
            if stat.S_ISLNK(metadata.st_mode):
                try:
                    target = (path.parent / os.readlink(path)).resolve(strict=False)
                except OSError as exc:
                    raise InstallError(f"authority tree symlink is unreadable: {root}") from exc
                if not target.is_relative_to(root_resolved) and target not in allowed:
                    raise InstallError(f"authority tree symlink escapes its trust root: {root}")
                continue
            if not (stat.S_ISDIR(metadata.st_mode) or stat.S_ISREG(metadata.st_mode)):
                raise InstallError(f"authority tree contains a special file: {root}")
            if require_nonwritable and stat.S_IMODE(metadata.st_mode) & 0o022:
                raise InstallError(f"authority tree is group/world writable: {root}")


def _harden_owned_tree(
    root: Path,
    *,
    expected_uid: int,
    expected_gid: int,
) -> bool:
    """Remove group/world write bits from an otherwise safe owned tree."""
    _validate_owned_tree(
        root,
        expected_uid=expected_uid,
        expected_gid=expected_gid,
        require_nonwritable=False,
    )
    changed = False
    visited: set[Path] = set()
    for directory, directories, filenames in os.walk(root, followlinks=False):
        current = Path(directory)
        paths = [
            current,
            *(current / name for name in directories),
            *(current / name for name in filenames),
        ]
        for path in paths:
            if path in visited:
                continue
            visited.add(path)
            try:
                before = os.lstat(path)
            except OSError as exc:
                raise InstallError(f"authority tree changed during hardening: {root}") from exc
            if stat.S_ISLNK(before.st_mode):
                continue
            flags = (
                os.O_RDONLY
                | getattr(os, "O_CLOEXEC", 0)
                | getattr(os, "O_NOFOLLOW", 0)
                | getattr(os, "O_NONBLOCK", 0)
            )
            if stat.S_ISDIR(before.st_mode):
                flags |= getattr(os, "O_DIRECTORY", 0)
            try:
                fd = os.open(path, flags)
            except OSError as exc:
                raise InstallError(f"authority tree changed during hardening: {root}") from exc
            try:
                opened = os.fstat(fd)
                if (
                    (opened.st_dev, opened.st_ino) != (before.st_dev, before.st_ino)
                    or opened.st_uid != expected_uid
                    or opened.st_gid != expected_gid
                    or not (stat.S_ISDIR(opened.st_mode) or stat.S_ISREG(opened.st_mode))
                ):
                    raise InstallError(f"authority tree changed during hardening: {root}")
                mode = stat.S_IMODE(opened.st_mode)
                hardened = mode & ~0o022
                if hardened != mode:
                    os.fchmod(fd, hardened)
                    changed = True
            finally:
                os.close(fd)
    _validate_owned_tree(root, expected_uid=expected_uid, expected_gid=expected_gid)
    return changed


def _validate_git_checkout_tree(
    root: Path,
    *,
    expected_uid: int,
    expected_gid: int,
    require_nonwritable: bool = True,
) -> None:
    _validate_owned_tree(
        root,
        expected_uid=expected_uid,
        expected_gid=expected_gid,
        require_nonwritable=require_nonwritable,
    )
    try:
        git_dir = os.lstat(root / ".git")
    except OSError as exc:
        raise InstallError(f"Git authority directory is unavailable: {root}") from exc
    if (
        not stat.S_ISDIR(git_dir.st_mode)
        or stat.S_ISLNK(git_dir.st_mode)
        or git_dir.st_uid != expected_uid
        or git_dir.st_gid != expected_gid
    ):
        raise InstallError(f"Git authority directory is unsafe: {root}")


def _validate_root_authority_parent_chain(path: Path) -> None:
    """Require every replaceable parent of a root authority to be root-controlled."""
    for parent in path.parents:
        try:
            metadata = os.lstat(parent)
        except OSError as exc:
            raise InstallError(f"root authority parent is unavailable: {path}") from exc
        if (
            not stat.S_ISDIR(metadata.st_mode)
            or stat.S_ISLNK(metadata.st_mode)
            or metadata.st_uid != 0
            or metadata.st_gid != 0
            or stat.S_IMODE(metadata.st_mode) & 0o022
        ):
            raise InstallError(f"root authority parent is unsafe: {path}")


def _safe_root_executable(path: Path, *, label: str) -> Path:
    """Resolve one fixed root-owned executable without trusting caller PATH state."""
    try:
        resolved = path.resolve(strict=True)
        metadata = resolved.stat()
    except OSError as exc:
        raise InstallError(f"installer requires fixed {label} at {path}") from exc
    if (
        not resolved.is_relative_to(Path("/usr"))
        or not stat.S_ISREG(metadata.st_mode)
        or metadata.st_uid != 0
        or metadata.st_gid != 0
        or stat.S_IMODE(metadata.st_mode) & 0o022
        or not os.access(resolved, os.X_OK)
    ):
        raise InstallError(f"fixed {label} authority is unsafe")
    return resolved


def _runtime_identity(root: Path, *, service_uid: int, service_gid: int) -> None:
    try:
        metadata = os.lstat(root)
    except OSError as exc:
        raise InstallError("service runtime directory is unavailable") from exc
    if (
        not stat.S_ISDIR(metadata.st_mode)
        or stat.S_ISLNK(metadata.st_mode)
        or metadata.st_uid != service_uid
        or metadata.st_gid != service_gid
        or stat.S_IMODE(metadata.st_mode) != 0o700
    ):
        raise InstallError("service runtime directory is unsafe")


def _read_root_kubeconfig_source() -> bytes:
    """Read the fixed root kubeconfig through one authority-checked descriptor."""
    try:
        for parent in ROOT_KUBECONFIG.parents:
            metadata = os.lstat(parent)
            if (
                not stat.S_ISDIR(metadata.st_mode)
                or stat.S_ISLNK(metadata.st_mode)
                or metadata.st_uid != 0
                or metadata.st_gid != 0
                or stat.S_IMODE(metadata.st_mode) & 0o022
            ):
                raise InstallError("root kubeconfig parent authority is unsafe")
    except OSError as exc:
        raise InstallError("root kubeconfig parent authority is unavailable") from exc

    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        fd = os.open(ROOT_KUBECONFIG, flags)
    except OSError as exc:
        raise InstallError("root kubeconfig source is unavailable") from exc

    failure: BaseException | None = None
    payload = b""
    try:
        before = os.fstat(fd)
        if (
            not stat.S_ISREG(before.st_mode)
            or before.st_uid != 0
            or before.st_gid != 0
            or stat.S_IMODE(before.st_mode) != 0o600
            or before.st_nlink != 1
            or before.st_size <= 0
            or before.st_size > _MAX_KUBECONFIG_BYTES
        ):
            raise InstallError("root kubeconfig source metadata is unsafe")
        chunks: list[bytes] = []
        remaining = _MAX_KUBECONFIG_BYTES + 1
        while remaining:
            chunk = os.read(fd, min(65536, remaining))
            if not chunk:
                break
            chunks.append(chunk)
            remaining -= len(chunk)
        payload = b"".join(chunks)
        after = os.fstat(fd)
        if (
            len(payload) != before.st_size
            or len(payload) > _MAX_KUBECONFIG_BYTES
            or (after.st_dev, after.st_ino, after.st_size)
            != (before.st_dev, before.st_ino, before.st_size)
        ):
            raise InstallError("root kubeconfig source changed during read")
    except OSError as exc:
        failure = InstallError("root kubeconfig source read failed")
        failure.__cause__ = exc
    except InstallError as exc:
        failure = exc
    finally:
        try:
            os.close(fd)
        except OSError as exc:
            if failure is None:
                failure = InstallError("root kubeconfig source close failed")
                failure.__cause__ = exc
    if failure is not None:
        raise failure
    return payload


def _maintenance_marker(
    root: Path,
    *,
    service_uid: int,
    service_gid: int,
    enabled: bool,
    authority_uid: int = 0,
    authority_gid: int = 0,
) -> None:
    """Change the root-owned admission marker while holding the broker launch lock."""
    _runtime_identity(root, service_uid=service_uid, service_gid=service_gid)
    lock_path = root / "launch.lock"
    existed = os.path.lexists(lock_path)
    flags = os.O_RDWR | os.O_CREAT | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        fd = os.open(lock_path, flags, 0o600)
    except OSError as exc:
        raise InstallError("protected launch lock is unavailable") from exc
    try:
        if not existed:
            os.fchown(fd, service_uid, service_gid)
            os.fchmod(fd, 0o600)
        metadata = os.fstat(fd)
        if (
            not stat.S_ISREG(metadata.st_mode)
            or metadata.st_uid != service_uid
            or metadata.st_gid != service_gid
            or stat.S_IMODE(metadata.st_mode) != 0o600
        ):
            raise InstallError("protected launch lock metadata is unsafe")
        fcntl.flock(fd, fcntl.LOCK_EX)
        marker = root / "maintenance"
        if enabled:
            if os.path.lexists(marker):
                marker_metadata = os.lstat(marker)
                if (
                    not stat.S_ISREG(marker_metadata.st_mode)
                    or marker_metadata.st_uid != authority_uid
                    or marker_metadata.st_gid != authority_gid
                    or stat.S_IMODE(marker_metadata.st_mode) != 0o600
                ):
                    raise InstallError("maintenance marker metadata is unsafe")
            else:
                marker_fd = os.open(
                    marker,
                    os.O_WRONLY
                    | os.O_CREAT
                    | os.O_EXCL
                    | getattr(os, "O_CLOEXEC", 0)
                    | getattr(os, "O_NOFOLLOW", 0),
                    0o600,
                )
                try:
                    os.fchown(marker_fd, authority_uid, authority_gid)
                    os.fchmod(marker_fd, 0o600)
                    os.fsync(marker_fd)
                finally:
                    os.close(marker_fd)
        elif os.path.lexists(marker):
            marker_metadata = os.lstat(marker)
            if (
                not stat.S_ISREG(marker_metadata.st_mode)
                or marker_metadata.st_uid != authority_uid
                or marker_metadata.st_gid != authority_gid
                or stat.S_IMODE(marker_metadata.st_mode) != 0o600
            ):
                raise InstallError("maintenance marker metadata is unsafe")
            marker.unlink()
        directory_fd = os.open(root, os.O_RDONLY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    except OSError as exc:
        raise InstallError("maintenance admission transition failed") from exc
    finally:
        try:
            fcntl.flock(fd, fcntl.LOCK_UN)
        finally:
            os.close(fd)


class HostSystem:
    """Command-backed adapter for accounts, ACLs, Git, systemd, and kubeconfig."""

    def __init__(self, runner: Runner) -> None:
        self.runner = runner
        self._trust_lock_fd: int | None = None

    @contextlib.contextmanager
    def trust_lifecycle_lock(self) -> Iterator[None]:
        if self._trust_lock_fd is not None:
            yield
            return
        try:
            parent = os.lstat(TRUST_LIFECYCLE_LOCK.parent)
        except OSError as exc:
            raise InstallError("GB10 trust lifecycle lock directory is unavailable") from exc
        if (
            not stat.S_ISDIR(parent.st_mode)
            or stat.S_ISLNK(parent.st_mode)
            or parent.st_uid != 0
            or parent.st_gid != 0
            or stat.S_IMODE(parent.st_mode) != 0o755
        ):
            raise InstallError("GB10 trust lifecycle lock directory is unsafe")
        flags = os.O_RDWR | os.O_CREAT | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
        try:
            fd = os.open(TRUST_LIFECYCLE_LOCK, flags, 0o600)
        except OSError as exc:
            raise InstallError("GB10 trust lifecycle lock is unavailable") from exc
        try:
            metadata = os.fstat(fd)
            current = os.lstat(TRUST_LIFECYCLE_LOCK)
            if (
                not stat.S_ISREG(metadata.st_mode)
                or metadata.st_uid != 0
                or metadata.st_gid != 0
                or stat.S_IMODE(metadata.st_mode) != 0o600
                or (current.st_dev, current.st_ino) != (metadata.st_dev, metadata.st_ino)
            ):
                raise InstallError("GB10 trust lifecycle lock metadata is unsafe")
            fcntl.flock(fd, fcntl.LOCK_EX)
            self._trust_lock_fd = fd
            yield
        except OSError as exc:
            raise InstallError("GB10 trust lifecycle lock failed") from exc
        finally:
            self._trust_lock_fd = None
            try:
                fcntl.flock(fd, fcntl.LOCK_UN)
            finally:
                os.close(fd)

    def _trust_command_kwargs(self) -> dict[str, Any]:
        environment = {
            "PATH": _ROOT_PATH,
            "LANG": "C.UTF-8",
            "LC_ALL": "C.UTF-8",
            "HOME": str(STATE_ROOT),
            "USER": SERVICE_USER,
            "LOGNAME": SERVICE_USER,
        }
        if self._trust_lock_fd is None:
            return {"env": environment}
        environment[TRUST_LOCK_FD_ENV] = str(self._trust_lock_fd)
        return {"env": environment, "pass_fds": (self._trust_lock_fd,)}

    def _probe(self, argv: Sequence[str]) -> CommandResult:
        return self.runner.run(argv, check=False)

    def _validate_system_python_version(self, python: Path) -> None:
        result = self._probe(
            [
                str(python),
                "-I",
                "-S",
                "-c",
                "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')",
            ]
        )
        match = re.fullmatch(r"([0-9]+)\.([0-9]+)\n?", result.stdout)
        if result.returncode != 0 or match is None:
            raise InstallError("fixed system Python version probe failed")
        version = (int(match.group(1)), int(match.group(2)))
        if version[0] != 3 or version < (3, 11):
            raise InstallError("installer requires system Python 3.11 or newer")

    def validate_prerequisites(self) -> None:
        required = (
            "bash",
            "docker",
            "getfacl",
            "git",
            "kubectl",
            "loginctl",
            "setfacl",
            "ssh",
            "ssh-keygen",
            "systemctl",
            "visudo",
        )
        missing = [name for name in required if shutil.which(name, path=_ROOT_PATH) is None]
        if missing:
            raise InstallError(f"missing required host tools: {', '.join(missing)}")
        os_release = Path("/etc/os-release")
        if not os_release.is_file() or "ID=ubuntu" not in os_release.read_text(encoding="utf-8"):
            raise InstallError("installer requires Ubuntu")
        if not Path("/run/systemd/system").is_dir():
            raise InstallError("installer requires systemd")
        python = _safe_root_executable(SYSTEM_PYTHON, label="system Python")
        _safe_root_executable(UV_BINARY, label="uv")
        _safe_root_executable(SYSTEM_SHELL, label="system shell")
        _safe_root_executable(SYSTEM_GIT, label="system Git")
        self._validate_system_python_version(python)

    def _validate_repo_contract(self, repo: Path, *, root_owned: bool) -> None:
        if root_owned:
            _validate_root_authority_parent_chain(repo)
            _validate_git_checkout_tree(repo, expected_uid=0, expected_gid=0)
        remotes = self.runner.run(["git", "-C", str(repo), "remote"]).stdout.splitlines()
        urls = self.runner.run(
            ["git", "-C", str(repo), "config", "--get-all", "remote.origin.url"]
        ).stdout.splitlines()
        pushurl = self._probe(
            ["git", "-C", str(repo), "config", "--get-all", "remote.origin.pushurl"]
        )
        dirty = self.runner.run(
            ["git", "-C", str(repo), "status", "--porcelain=v1", "--untracked-files=all"]
        ).stdout
        if remotes != ["origin"] or urls != [REMOTE_URL] or pushurl.returncode == 0 or dirty:
            raise InstallError("Git source is dirty or does not use the single approved origin")

    def validate_invocation_checkout(self) -> str:
        """Detect bootstrap checkout drift before installer-managed mutation.

        The operator must establish this root-owned checkout before Python is
        executed. This in-process check is defense in depth, not a bootstrap
        trust boundary against an already malicious checkout.
        """
        self._validate_repo_contract(REPO_ROOT, root_owned=True)
        head = self.runner.run(["git", "-C", str(REPO_ROOT), "rev-parse", "HEAD"]).stdout.strip()
        if _SHA_RE.fullmatch(head) is None:
            raise InstallError("installer checkout HEAD is invalid")
        return head

    def validate_invocation_merged(self, invocation_head: str, source_sha: str) -> None:
        if _SHA_RE.fullmatch(invocation_head) is None or _SHA_RE.fullmatch(source_sha) is None:
            raise InstallError("installer source binding is invalid")
        self.runner.run(
            [
                "git",
                "-C",
                str(INSTALL_SOURCE),
                "merge-base",
                "--is-ancestor",
                invocation_head,
                source_sha,
            ]
        )

    def prepare_install_source(self) -> tuple[Path, str]:
        self.ensure_root_directory(RUNNER_ROOT, mode=0o755)
        cloned = False
        if self._probe(["test", "-d", str(INSTALL_SOURCE / ".git")]).returncode != 0:
            if self._probe(["test", "-e", str(INSTALL_SOURCE)]).returncode == 0:
                raise InstallError("root installation source path is occupied")
            self.runner.run(["git", "clone", "--origin", "origin", REMOTE_URL, str(INSTALL_SOURCE)])
            cloned = True
        self._validate_repo_contract(INSTALL_SOURCE, root_owned=True)
        remote = self.runner.run(
            ["git", "ls-remote", "--exit-code", REMOTE_URL, FETCH_REF]
        ).stdout.splitlines()
        if len(remote) != 1:
            raise InstallError("fresh origin/dev source SHA is unavailable")
        fields = remote[0].split()
        sha = fields[0] if len(fields) == 2 and fields[1] == FETCH_REF else ""
        if _SHA_RE.fullmatch(sha) is None:
            raise InstallError("fresh origin/dev source SHA is invalid")
        head = self._probe(["git", "-C", str(INSTALL_SOURCE), "rev-parse", "HEAD"])
        if head.returncode != 0 or head.stdout.strip() != sha:
            self.runner.run(
                [
                    "git",
                    "-C",
                    str(INSTALL_SOURCE),
                    "fetch",
                    "--prune",
                    "origin",
                    f"{FETCH_REF}:refs/remotes/origin/dev",
                ]
            )
            if cloned:
                self.runner.run(["git", "-C", str(INSTALL_SOURCE), "checkout", "--detach", sha])
        self._validate_repo_contract(INSTALL_SOURCE, root_owned=True)
        return INSTALL_SOURCE, sha

    def prepare_sealed_install_source(self, source: SealedSource) -> tuple[Path, str]:
        """Import one validated local commit without resolving any remote ref."""
        if source.path != REPO_ROOT:
            raise InstallError("sealed source must be the exact installer checkout")
        try:
            validate_sealed_source(source, run=lambda argv: self.runner.run(argv, check=False))
        except SealedSourceError as exc:
            raise InstallError(str(exc)) from exc
        self.ensure_root_directory(RUNNER_ROOT, mode=0o755)
        if self._probe(["test", "-d", str(INSTALL_SOURCE / ".git")]).returncode != 0:
            if self._probe(["test", "-e", str(INSTALL_SOURCE)]).returncode == 0:
                raise InstallError("root installation source path is occupied")
            self.runner.run(["git", "init", str(INSTALL_SOURCE)])
            self.runner.run(
                ["git", "-C", str(INSTALL_SOURCE), "remote", "add", "origin", REMOTE_URL]
            )
        self._validate_repo_contract(INSTALL_SOURCE, root_owned=True)
        object_type = self._probe(
            ["git", "-C", str(INSTALL_SOURCE), "cat-file", "-t", source.commit_sha]
        )
        if object_type.returncode != 0 or object_type.stdout.strip() != "commit":
            self.runner.run(
                [
                    "git",
                    "-C",
                    str(INSTALL_SOURCE),
                    "fetch",
                    "--no-tags",
                    "--no-recurse-submodules",
                    str(source.path),
                    source.commit_sha,
                ]
            )
        expected = {"commit": source.commit_sha, "tree": source.tree_sha}
        observed = {
            "commit": self.runner.run(
                ["git", "-C", str(INSTALL_SOURCE), "rev-parse", f"{source.commit_sha}^{{commit}}"]
            ).stdout.strip(),
            "tree": self.runner.run(
                ["git", "-C", str(INSTALL_SOURCE), "rev-parse", f"{source.commit_sha}^{{tree}}"]
            ).stdout.strip(),
        }
        if observed != expected:
            raise InstallError("imported sealed source identity does not match")
        merge_base = self.runner.run(
            [
                "git",
                "-C",
                str(INSTALL_SOURCE),
                "merge-base",
                source.base_sha,
                source.commit_sha,
            ]
        ).stdout.strip()
        history = self.runner.run(
            [
                "git",
                "-C",
                str(INSTALL_SOURCE),
                "rev-list",
                "--reverse",
                "--parents",
                f"{source.base_sha}..{source.commit_sha}",
            ]
        ).stdout.splitlines()
        expected_parent = source.base_sha
        for line in history:
            fields = line.split()
            if len(fields) != 2 or fields[1] != expected_parent:
                raise InstallError("imported sealed source history is not linear")
            expected_parent = fields[0]
        if (
            merge_base != source.base_sha
            or not 1 <= len(history) <= MAX_CUMULATIVE_COMMITS
            or expected_parent != source.commit_sha
        ):
            raise InstallError("imported sealed source history does not match")
        self._validate_repo_contract(INSTALL_SOURCE, root_owned=True)
        return INSTALL_SOURCE, source.commit_sha

    def install_source_ready(self, expected_sha: str) -> bool:
        if _SHA_RE.fullmatch(expected_sha) is None:
            raise InstallError("root installation source SHA is invalid")
        head = self._probe(["git", "-C", str(INSTALL_SOURCE), "rev-parse", "HEAD"])
        return head.returncode == 0 and head.stdout.strip() == expected_sha

    def ensure_install_source_checkout(self, expected_sha: str) -> bool:
        if self.install_source_ready(expected_sha):
            return False
        self._validate_repo_contract(INSTALL_SOURCE, root_owned=True)
        self.runner.run(["git", "-C", str(INSTALL_SOURCE), "checkout", "--detach", expected_sha])
        self._validate_repo_contract(INSTALL_SOURCE, root_owned=True)
        if not self.install_source_ready(expected_sha):
            raise InstallError("root installation source did not converge to the selected SHA")
        return True

    def validate_assets(self, source_root: Path, source_sha: str) -> None:
        known_hosts = self.source_file(
            source_root,
            source_sha,
            "deploy/worker-pools/gb10/known_hosts",
        )
        _validate_known_hosts_authority(known_hosts)
        with tempfile.TemporaryDirectory(prefix="loom-staging-install-assets-") as raw_directory:
            directory = Path(raw_directory)
            assets = {
                name: self.source_file(
                    source_root,
                    source_sha,
                    f"deploy/staging-rollout/{name}",
                )
                for name in (
                    "loom-staging-rollout",
                    "loom-staging-rollout-broker",
                    "loom-staging-rollout-rehearsal",
                    "loom-staging-rollout.sudoers",
                    SHARED_WORK2_MOUNT_UNIT,
                )
            }
            for name, payload in assets.items():
                (directory / name).write_bytes(payload)
            shared_repo_helper = directory / "staging_rollout_shared_repo.py"
            shared_repo_helper.write_bytes(
                self.source_file(
                    source_root,
                    source_sha,
                    "scripts/ops/staging_rollout_shared_repo.py",
                )
            )
            shared_work2_helper = directory / "staging_rollout_shared_work2.py"
            shared_work2_helper.write_bytes(
                self.source_file(
                    source_root,
                    source_sha,
                    "scripts/ops/staging_rollout_shared_work2.py",
                )
            )
            export_helper = directory / "staging_rollout_shared_work2_export.py"
            export_helper.write_bytes(
                self.source_file(
                    source_root,
                    source_sha,
                    "scripts/ops/staging_rollout_shared_work2_export.py",
                )
            )
            sealed_source_helper = directory / "staging_rollout_sealed_source.py"
            sealed_source_helper.write_bytes(
                self.source_file(
                    source_root,
                    source_sha,
                    "scripts/ops/staging_rollout_sealed_source.py",
                )
            )
            self.runner.run(["bash", "-n", str(directory / "loom-staging-rollout")])
            self.runner.run(["bash", "-n", str(directory / "loom-staging-rollout-broker")])
            self.runner.run(["bash", "-n", str(directory / "loom-staging-rollout-rehearsal")])
            self.runner.run(["visudo", "-cf", str(directory / "loom-staging-rollout.sudoers")])
            self.runner.run([str(SYSTEM_PYTHON), "-m", "py_compile", str(shared_repo_helper)])
            self.runner.run([str(SYSTEM_PYTHON), "-m", "py_compile", str(shared_work2_helper)])
            self.runner.run(
                [str(SYSTEM_PYTHON), "-m", "py_compile", str(sealed_source_helper)]
            )
            self.runner.run([str(SYSTEM_PYTHON), "-m", "py_compile", str(export_helper)])

    def source_file(self, source_root: Path, source_sha: str, relative_path: str) -> bytes:
        if source_root != INSTALL_SOURCE or _SHA_RE.fullmatch(source_sha) is None:
            raise InstallError("root installation source binding is invalid")
        if relative_path.startswith("/") or ".." in Path(relative_path).parts:
            raise InstallError("root installation source path is unsafe")
        result = self.runner.run(
            ["git", "-C", str(source_root), "show", f"{source_sha}:{relative_path}"]
        )
        return result.stdout.encode("utf-8")

    def validate_installed_source(
        self,
        source_sha: str,
        *,
        require_checkout: bool,
        source_tree_sha: str | None = None,
        source_base_sha: str | None = None,
    ) -> None:
        if _SHA_RE.fullmatch(source_sha) is None:
            raise InstallError("install record source SHA is invalid")
        self._validate_repo_contract(INSTALL_SOURCE, root_owned=True)
        object_type = self._probe(["git", "-C", str(INSTALL_SOURCE), "cat-file", "-t", source_sha])
        if object_type.returncode != 0 or object_type.stdout.strip() != "commit":
            raise InstallError("install record source commit is unavailable")
        head = self.runner.run(
            ["git", "-C", str(INSTALL_SOURCE), "rev-parse", "HEAD"]
        ).stdout.strip()
        if require_checkout and head != source_sha:
            raise InstallError("root installation source drifted from the install record")
        if (source_tree_sha is None) != (source_base_sha is None):
            raise InstallError("install record sealed source binding is incomplete")
        if source_tree_sha is not None and source_base_sha is not None:
            if _SHA_RE.fullmatch(source_tree_sha) is None or _SHA_RE.fullmatch(source_base_sha) is None:
                raise InstallError("install record sealed source binding is invalid")
            observed_tree = self.runner.run(
                ["git", "-C", str(INSTALL_SOURCE), "rev-parse", f"{source_sha}^{{tree}}"]
            ).stdout.strip()
            observed_base = self.runner.run(
                ["git", "-C", str(INSTALL_SOURCE), "merge-base", source_base_sha, source_sha]
            ).stdout.strip()
            history = self.runner.run(
                [
                    "git",
                    "-C",
                    str(INSTALL_SOURCE),
                    "rev-list",
                    "--reverse",
                    "--parents",
                    f"{source_base_sha}..{source_sha}",
                ]
            ).stdout.splitlines()
            expected_parent = source_base_sha
            for line in history:
                fields = line.split()
                if len(fields) != 2 or fields[1] != expected_parent:
                    raise InstallError("installed sealed source history is not linear")
                expected_parent = fields[0]
            if (
                (observed_tree, observed_base) != (source_tree_sha, source_base_sha)
                or not 1 <= len(history) <= MAX_CUMULATIVE_COMMITS
                or expected_parent != source_sha
            ):
                raise InstallError("installed sealed source identity drifted")

    def ensure_group(self, name: str) -> bool:
        if self.group_present(name):
            return False
        self.runner.run(["groupadd", "--system", name])
        return True

    def group_present(self, name: str) -> bool:
        return self._probe(["getent", "group", name]).returncode == 0

    def _service_ids(self) -> tuple[int, int]:
        passwd_result = self._probe(["getent", "passwd", SERVICE_USER])
        group_result = self._probe(["getent", "group", SERVICE_GROUP])
        uid_result = self._probe(["id", "-u", SERVICE_USER])
        gid_result = self._probe(["id", "-g", SERVICE_USER])
        passwd_fields = passwd_result.stdout.strip().split(":")
        group_fields = group_result.stdout.strip().split(":")
        rendered_uid = uid_result.stdout.strip()
        rendered_gid = gid_result.stdout.strip()
        if (
            passwd_result.returncode != 0
            or group_result.returncode != 0
            or uid_result.returncode != 0
            or gid_result.returncode != 0
            or len(passwd_fields) < 4
            or len(group_fields) < 3
            or not passwd_fields[2].isdigit()
            or not passwd_fields[3].isdigit()
            or not group_fields[2].isdigit()
            or not rendered_uid.isdigit()
            or not rendered_gid.isdigit()
        ):
            raise InstallError("service account identity is unavailable")
        uids = {int(passwd_fields[2]), int(rendered_uid)}
        gids = {int(passwd_fields[3]), int(group_fields[2]), int(rendered_gid)}
        if len(uids) != 1 or 0 in uids:
            raise InstallError("service account UID is inconsistent")
        if len(gids) != 1 or 0 in gids:
            raise InstallError("service account primary group is inconsistent")
        return uids.pop(), gids.pop()

    def ensure_operator_membership(self, username: str) -> bool:
        if self.operator_membership_present(username):
            return False
        self.runner.run(["usermod", "-a", "-G", OPERATOR_GROUP, username])
        return True

    def operator_membership_present(self, username: str) -> bool:
        groups = self.runner.run(["id", "-nG", username]).stdout.split()
        return OPERATOR_GROUP in groups

    def ensure_service_user(self) -> bool:
        if not self.service_user_present():
            self.runner.run(
                [
                    "useradd",
                    "--system",
                    "--user-group",
                    "--create-home",
                    "--home-dir",
                    str(STATE_ROOT),
                    "--shell",
                    "/usr/sbin/nologin",
                    SERVICE_USER,
                ]
            )
            if not self.service_user_present():  # pragma: no cover - convergence invariant
                raise InstallError("service account creation did not converge")
            return True
        return False

    def service_user_present(self) -> bool:
        result = self._probe(["getent", "passwd", SERVICE_USER])
        if result.returncode != 0:
            return False
        fields = result.stdout.strip().split(":")
        if (
            len(fields) < 7
            or not fields[2].isdigit()
            or int(fields[2]) == 0
            or fields[5] != str(STATE_ROOT)
            or fields[6] != "/usr/sbin/nologin"
        ):
            raise InstallError("existing service account has unexpected identity, home, or shell")
        self._service_ids()
        return True

    def ensure_docker_membership(self) -> bool:
        if self.docker_membership_present():
            return False
        self.runner.run(["usermod", "-a", "-G", "docker", SERVICE_USER])
        return True

    def docker_membership_present(self) -> bool:
        result = self._probe(["id", "-nG", SERVICE_USER])
        if result.returncode != 0:
            return False
        groups = result.stdout.split()
        unexpected = sorted(set(groups) - {SERVICE_USER, "docker"})
        if unexpected:
            raise InstallError("service account has unexpected supplementary groups")
        return "docker" in groups

    def shared_worker_repo_identity(self) -> dict[str, object]:
        service_uid, service_gid = self._service_ids()
        consumer = self._probe(["getent", "passwd", SHARED_WORK_CONSUMER])
        consumer_uid = self._probe(["id", "-u", SHARED_WORK_CONSUMER])
        shared_group = self._probe(["getent", "group", SHARED_WORK_GROUP])
        consumer_groups = self._probe(["id", "-nG", SHARED_WORK_CONSUMER])
        consumer_fields = consumer.stdout.strip().split(":")
        group_fields = shared_group.stdout.strip().split(":")
        rendered_uid = consumer_uid.stdout.strip()
        if (
            consumer.returncode != 0
            or consumer_uid.returncode != 0
            or shared_group.returncode != 0
            or consumer_groups.returncode != 0
            or len(consumer_fields) < 4
            or len(group_fields) < 3
            or not consumer_fields[2].isdigit()
            or not rendered_uid.isdigit()
            or not group_fields[2].isdigit()
            or int(consumer_fields[2]) != int(rendered_uid)
            or int(consumer_fields[2]) == 0
            or int(group_fields[2]) == 0
        ):
            raise InstallError("shared worker repository identity is unavailable or inconsistent")
        if SHARED_WORK_GROUP not in consumer_groups.stdout.split():
            raise InstallError("shared worker repository consumer lacks sharedwork membership")
        identity: dict[str, object] = {
            "root": str(SHARED_WORKER_REPO_ROOT),
            "service_user": SERVICE_USER,
            "service_uid": service_uid,
            "service_primary_group": SERVICE_GROUP,
            "service_primary_gid": service_gid,
            "consumer_user": SHARED_WORK_CONSUMER,
            "consumer_uid": int(rendered_uid),
            "shared_group": SHARED_WORK_GROUP,
            "shared_gid": int(group_fields[2]),
        }

        report = self._shared_worker_repo_helper("check")
        if report is None:
            return identity
        for key, value in identity.items():
            if report.get(key) != value:
                raise InstallError("shared worker repository helper identity drifted")
        return report

    @staticmethod
    def _validate_shared_work2_mount_report(payload: object) -> dict[str, object]:
        if not isinstance(payload, dict):
            raise InstallError("shared_work2 mount helper report is invalid")
        expected: dict[str, object] = {
            "schema_version": 1,
            "mount_point": str(SHARED_WORK2_MOUNT_POINT),
            "source": "192.168.20.12:/shared_work2",
            "filesystem_type": "nfs4",
            "mount_options": ["nodev", "noexec", "nosuid", "rw"],
            "super_options": [
                "hard",
                "proto=tcp",
                "retrans=2",
                "rw",
                "sec=sys",
                "timeo=600",
                "vers=4.2",
            ],
        }
        if any(payload.get(key) != value for key, value in expected.items()):
            raise InstallError("shared_work2 mount helper report is invalid")
        for key in ("mount_id", "parent_id", "device_major", "device_minor"):
            if type(payload.get(key)) is not int or int(payload[key]) < 0:
                raise InstallError("shared_work2 mount helper report is invalid")
        return dict(payload)

    def shared_work2_mount_identity(self) -> dict[str, object]:
        result = self._probe([str(SYSTEM_PYTHON), str(SHARED_WORK2_MOUNT_HELPER), "check"])
        if result.returncode != 0:
            raise InstallError("shared_work2 mount helper failed safely")
        try:
            payload = json.loads(result.stdout)
        except (TypeError, ValueError) as exc:
            raise InstallError("shared_work2 mount helper report is invalid") from exc
        return self._validate_shared_work2_mount_report(payload)

    def shared_work2_mount_ready(self) -> bool:
        try:
            self.shared_work2_mount_identity()
        except InstallError:
            return False
        return True

    def ensure_shared_work2_mount(self) -> bool:
        if self.shared_work2_mount_ready():
            return False
        self.runner.run(["systemctl", "daemon-reload"])
        self.runner.run(["systemctl", "enable", "--now", SHARED_WORK2_MOUNT_UNIT])
        self.shared_work2_mount_identity()
        return True

    def disable_shared_work2_mount(self) -> None:
        self.runner.run(["systemctl", "disable", "--now", SHARED_WORK2_MOUNT_UNIT])

    def reload_systemd(self) -> None:
        self.runner.run(["systemctl", "daemon-reload"])

    @staticmethod
    def _validate_shared_worker_repo_report(payload: object) -> dict[str, object]:
        if not isinstance(payload, dict):
            raise InstallError("shared worker repository helper report is invalid")
        expected_strings = {
            "root": str(SHARED_WORKER_REPO_ROOT),
            "service_user": SERVICE_USER,
            "service_primary_group": SERVICE_GROUP,
            "consumer_user": SHARED_WORK_CONSUMER,
            "shared_group": SHARED_WORK_GROUP,
            "parent_mode": "2775",
            "authority_mode": "2750",
            "repository_mode": "2750",
            "service_capability": "parent-not-writable;repository-writable-searchable",
            "consumer_capability": "repository-readable-searchable-not-writable",
            "publication_capability": "private-mkdir-publish-verified",
        }
        expected_integers = {
            "service_uid",
            "service_primary_gid",
            "consumer_uid",
            "shared_gid",
            "parent_device",
            "parent_inode",
            "authority_device",
            "authority_inode",
            "repository_device",
            "repository_inode",
        }
        if payload.get("schema_version") != 1 or any(
            payload.get(key) != value for key, value in expected_strings.items()
        ):
            raise InstallError("shared worker repository helper report is invalid")
        if any(
            type(payload.get(key)) is not int or int(payload[key]) < 0 for key in expected_integers
        ):
            raise InstallError("shared worker repository helper report is invalid")
        created = payload.get("created")
        if (
            not isinstance(created, list)
            or any(
                value not in {"consumer-parent", "authority-root", "repository-root"}
                for value in created
            )
            or len(set(created)) != len(created)
        ):
            raise InstallError("shared worker repository helper report is invalid")
        mount = payload.get("mount")
        self_mount = HostSystem._validate_shared_work2_mount_report(mount)
        payload = dict(payload)
        payload["mount"] = {
            key: self_mount[key]
            for key in (
                "schema_version",
                "mount_point",
                "source",
                "filesystem_type",
                "mount_options",
                "super_options",
            )
        }
        return payload

    def _shared_worker_repo_helper(self, command: str) -> dict[str, object] | None:
        if command not in {"check", "ensure"}:
            raise InstallError("shared worker repository helper command is invalid")
        result = self._probe([str(SYSTEM_PYTHON), str(SHARED_WORKER_REPO_HELPER), command])
        if result.returncode == 2 and command == "check":
            return None
        if result.returncode != 0:
            raise InstallError("shared worker repository helper failed safely")
        try:
            payload = json.loads(result.stdout)
        except (TypeError, ValueError) as exc:
            raise InstallError("shared worker repository helper report is invalid") from exc
        return self._validate_shared_worker_repo_report(payload)

    def shared_worker_repo_root_ready(self) -> bool:
        return self._shared_worker_repo_helper("check") is not None

    def ensure_shared_worker_repo_root(self) -> bool:
        if self.shared_worker_repo_root_ready():
            return False
        report = self._shared_worker_repo_helper("ensure")
        if report is None:  # pragma: no cover - ensure never returns absent
            raise InstallError("shared worker repository authority did not converge safely")
        return bool(report["created"])

    def ensure_root_directory(self, path: Path, *, mode: int) -> bool:
        expected = f"directory:root:root:{mode:o}"
        current = self._probe(["stat", "-c", "%F:%U:%G:%a", str(path)]).stdout.strip()
        if current == expected:
            return False
        if self._probe(["test", "-L", str(path)]).returncode == 0:
            raise InstallError("root authority directory path is a symlink")
        self.runner.run(
            ["install", "-d", "-o", "root", "-g", "root", "-m", f"{mode:04o}", str(path)]
        )
        confirmed = self.runner.run(["stat", "-c", "%F:%U:%G:%a", str(path)]).stdout.strip()
        if confirmed != expected:
            raise InstallError("root authority directory did not converge safely")
        return True

    def validate_install_record_authority(self, *, allow_absent: bool) -> None:
        parent = self._probe(
            ["stat", "-c", "%F:%U:%G:%a", str(INSTALL_RECORD.parent)]
        ).stdout.strip()
        if parent != "directory:root:root:755":
            raise InstallError("install record authority directory is unsafe")
        record = self._probe(["stat", "-c", "%F:%U:%G:%a", str(INSTALL_RECORD)])
        if record.returncode != 0 and allow_absent:
            return
        if record.returncode != 0 or record.stdout.strip() != "regular file:root:root:600":
            raise InstallError("install record authority is unsafe")

    def ensure_owned_directory(self, path: Path, *, owner: str, mode: int) -> bool:
        if self.owned_directory_ready(path, owner=owner, mode=mode):
            return False
        if self._probe(["test", "-L", str(path)]).returncode == 0:
            raise InstallError("service directory path is a symlink")
        self.runner.run(["install", "-d", "-o", owner, "-g", owner, "-m", f"{mode:04o}", str(path)])
        return True

    def owned_directory_ready(self, path: Path, *, owner: str, mode: int) -> bool:
        expected = f"directory:{owner}:{owner}:{mode:o}"
        current = self._probe(["stat", "-c", "%F:%U:%G:%a", str(path)]).stdout.strip()
        return current == expected

    def create_runtime_directory(self) -> bool:
        if self.runtime_directory_ready():
            return False
        self.runner.run(["systemd-tmpfiles", "--create", str(TMPFILES_PATH)])
        expected = f"directory:{SERVICE_USER}:{SERVICE_GROUP}:700"
        confirmed = self.runner.run(["stat", "-c", "%F:%U:%G:%a", str(RUNTIME_ROOT)]).stdout.strip()
        if confirmed != expected:
            raise InstallError("service runtime directory did not converge safely")
        return True

    def runtime_directory_ready(self) -> bool:
        expected = f"directory:{SERVICE_USER}:{SERVICE_GROUP}:700"
        current = self._probe(["stat", "-c", "%F:%U:%G:%a", str(RUNTIME_ROOT)]).stdout.strip()
        return current == expected

    def _service_git(self, *arguments: str, check: bool = True) -> CommandResult:
        return self.runner.run(
            [
                "sudo",
                "-n",
                "-u",
                SERVICE_USER,
                "--",
                "/usr/bin/env",
                "-i",
                f"HOME={STATE_ROOT}",
                "PATH=/usr/bin:/bin",
                "GIT_CONFIG_NOSYSTEM=1",
                "GIT_CONFIG_GLOBAL=/dev/null",
                "GIT_TERMINAL_PROMPT=0",
                str(SYSTEM_SHELL),
                "-c",
                'umask 077; exec "$@"',
                "loom-staging-git",
                str(SYSTEM_GIT),
                *arguments,
            ],
            check=check,
        )

    def _validate_candidate_sealed_identity(
        self,
        expected_sha: str,
        *,
        source_tree_sha: str,
        source_base_sha: str,
    ) -> None:
        if (
            _SHA_RE.fullmatch(source_tree_sha) is None
            or _SHA_RE.fullmatch(source_base_sha) is None
        ):
            raise InstallError("candidate sealed source binding is invalid")
        observed_tree = self._service_git(
            "-C",
            str(CANDIDATE_REPO),
            "rev-parse",
            f"{expected_sha}^{{tree}}",
        ).stdout.strip()
        merge_base = self._service_git(
            "-C",
            str(CANDIDATE_REPO),
            "merge-base",
            source_base_sha,
            expected_sha,
        ).stdout.strip()
        history = self._service_git(
            "-C",
            str(CANDIDATE_REPO),
            "rev-list",
            "--reverse",
            "--parents",
            f"{source_base_sha}..{expected_sha}",
        ).stdout.splitlines()
        expected_parent = source_base_sha
        for line in history:
            fields = line.split()
            if len(fields) != 2 or fields[1] != expected_parent:
                raise InstallError("candidate sealed source history is not linear")
            expected_parent = fields[0]
        if (
            observed_tree != source_tree_sha
            or merge_base != source_base_sha
            or not 1 <= len(history) <= MAX_CUMULATIVE_COMMITS
            or expected_parent != expected_sha
        ):
            raise InstallError("candidate sealed source identity does not match")

    def ensure_candidate(
        self,
        expected_sha: str,
        *,
        refresh: bool,
        source_tree_sha: str | None = None,
        source_base_sha: str | None = None,
    ) -> bool:
        if _SHA_RE.fullmatch(expected_sha) is None:
            raise InstallError("candidate checkout SHA is invalid")
        sealed = source_tree_sha is not None or source_base_sha is not None
        if sealed and (source_tree_sha is None or source_base_sha is None):
            raise InstallError("candidate sealed source binding is incomplete")
        changed = False
        service_uid, service_gid = self._service_ids()
        if self._probe(["test", "-d", str(CANDIDATE_REPO / ".git")]).returncode != 0:
            if sealed:
                self._service_git("init", str(CANDIDATE_REPO))
                self._service_git(
                    "-C",
                    str(CANDIDATE_REPO),
                    "remote",
                    "add",
                    "origin",
                    REMOTE_URL,
                )
            else:
                self._service_git(
                    "clone", "--origin", "origin", REMOTE_URL, str(CANDIDATE_REPO)
                )
            refresh = True
            changed = True
        _validate_git_checkout_tree(
            CANDIDATE_REPO,
            expected_uid=service_uid,
            expected_gid=service_gid,
            require_nonwritable=False,
        )
        changed = (
            _harden_owned_tree(
                CANDIDATE_REPO,
                expected_uid=service_uid,
                expected_gid=service_gid,
            )
            or changed
        )
        remotes = self._service_git("-C", str(CANDIDATE_REPO), "remote").stdout.splitlines()
        urls = self._service_git(
            "-C", str(CANDIDATE_REPO), "config", "--get-all", "remote.origin.url"
        ).stdout.splitlines()
        if remotes != ["origin"] or urls != [REMOTE_URL]:
            raise InstallError("candidate checkout origin is not approved")
        if (
            self._service_git(
                "-C",
                str(CANDIDATE_REPO),
                "config",
                "--get-all",
                "remote.origin.pushurl",
                check=False,
            ).returncode
            == 0
        ):
            raise InstallError("candidate checkout must not define a push URL")
        dirty = self._service_git(
            "-C",
            str(CANDIDATE_REPO),
            "status",
            "--porcelain=v1",
            "--untracked-files=all",
        ).stdout
        if dirty:
            raise InstallError("candidate checkout is dirty")
        head_result = self._service_git(
            "-C", str(CANDIDATE_REPO), "rev-parse", "HEAD", check=False
        )
        head = head_result.stdout.strip() if head_result.returncode == 0 else ""
        if refresh or head != expected_sha:
            if sealed:
                self._service_git(
                    "-C",
                    str(CANDIDATE_REPO),
                    "fetch",
                    "--no-tags",
                    "--no-recurse-submodules",
                    f"--upload-pack={SEALED_SOURCE_UPLOAD_PACK}",
                    str(INSTALL_SOURCE),
                    expected_sha,
                )
            else:
                self._service_git(
                    "-C",
                    str(CANDIDATE_REPO),
                    "fetch",
                    "--prune",
                    "origin",
                    f"{FETCH_REF}:refs/remotes/origin/dev",
                )
            self._service_git(
                "-C",
                str(CANDIDATE_REPO),
                "checkout",
                "--detach",
                expected_sha,
            )
            changed = True
        changed = (
            _harden_owned_tree(
                CANDIDATE_REPO,
                expected_uid=service_uid,
                expected_gid=service_gid,
            )
            or changed
        )
        head = self._service_git("-C", str(CANDIDATE_REPO), "rev-parse", "HEAD").stdout.strip()
        if head != expected_sha:
            raise InstallError("candidate checkout did not converge to the installed source SHA")
        if sealed:
            if source_tree_sha is None or source_base_sha is None:  # pragma: no cover
                raise InstallError("candidate sealed source binding is incomplete")
            self._validate_candidate_sealed_identity(
                expected_sha,
                source_tree_sha=source_tree_sha,
                source_base_sha=source_base_sha,
            )
        return changed

    def candidate_ready(
        self,
        expected_sha: str,
        *,
        source_tree_sha: str | None = None,
        source_base_sha: str | None = None,
    ) -> bool:
        sealed = source_tree_sha is not None or source_base_sha is not None
        if sealed and (source_tree_sha is None or source_base_sha is None):
            return False
        if self._probe(["test", "-d", str(CANDIDATE_REPO / ".git")]).returncode != 0:
            return False
        try:
            service_uid, service_gid = self._service_ids()
            _validate_git_checkout_tree(
                CANDIDATE_REPO,
                expected_uid=service_uid,
                expected_gid=service_gid,
            )
        except InstallError:
            return False
        remotes = self._service_git(
            "-C", str(CANDIDATE_REPO), "remote", check=False
        ).stdout.splitlines()
        urls = self._service_git(
            "-C",
            str(CANDIDATE_REPO),
            "config",
            "--get-all",
            "remote.origin.url",
            check=False,
        ).stdout.splitlines()
        pushurl = self._service_git(
            "-C",
            str(CANDIDATE_REPO),
            "config",
            "--get-all",
            "remote.origin.pushurl",
            check=False,
        )
        dirty = self._service_git(
            "-C",
            str(CANDIDATE_REPO),
            "status",
            "--porcelain=v1",
            "--untracked-files=all",
            check=False,
        )
        head = self._service_git("-C", str(CANDIDATE_REPO), "rev-parse", "HEAD", check=False)
        ready = bool(
            remotes == ["origin"]
            and urls == [REMOTE_URL]
            and pushurl.returncode != 0
            and dirty.returncode == 0
            and not dirty.stdout
            and head.returncode == 0
            and head.stdout.strip() == expected_sha
        )
        if not ready:
            return False
        if sealed:
            try:
                if source_tree_sha is None or source_base_sha is None:  # pragma: no cover
                    return False
                self._validate_candidate_sealed_identity(
                    expected_sha,
                    source_tree_sha=source_tree_sha,
                    source_base_sha=source_base_sha,
                )
            except InstallError:
                return False
        return True

    def venv_ready(self) -> bool:
        if not os.path.lexists(VENV):
            return False
        system_python = _safe_root_executable(SYSTEM_PYTHON, label="system Python")
        self._validate_system_python_version(system_python)
        metadata = self._probe(["stat", "-c", "%F:%U:%G:%a", str(VENV)])
        if metadata.stdout.strip() != "directory:root:root:755":
            raise InstallError("root venv metadata is unsafe")
        python = VENV / "bin/python"
        allowed_external_targets: tuple[Path, ...] = ()
        if os.path.lexists(python) and python.is_symlink():
            try:
                target = (python.parent / os.readlink(python)).resolve(strict=False)
            except OSError as exc:
                raise InstallError("root venv Python link is unreadable") from exc
            if (
                target.parent != Path("/usr/bin")
                or re.fullmatch(r"python3(?:\.[0-9]+)?", target.name) is None
            ):
                raise InstallError("root venv Python link is unsafe")
            allowed_external_targets = (target,)
        _validate_owned_tree(
            VENV,
            expected_uid=0,
            expected_gid=0,
            allowed_external_symlink_targets=allowed_external_targets,
        )
        if not os.path.lexists(python):
            return False
        try:
            resolved_python = _safe_root_executable(python, label="root venv Python")
        except InstallError:
            try:
                python.resolve(strict=True)
            except OSError:
                return False
            raise
        if resolved_python != system_python:
            return False
        if not os.access(python, os.X_OK):
            raise InstallError("root venv Python authority is unsafe")
        return True

    def _venv_lock_metadata(self, *, allow_absent: bool) -> os.stat_result | None:
        """Return safe uv lock metadata without accepting a replaceable venv root."""
        try:
            venv_metadata = os.lstat(VENV)
        except FileNotFoundError:
            if allow_absent:
                return None
            raise InstallError("root venv lock is unavailable") from None
        except OSError as exc:
            raise InstallError("root venv metadata is unavailable") from exc
        if (
            not stat.S_ISDIR(venv_metadata.st_mode)
            or stat.S_ISLNK(venv_metadata.st_mode)
            or venv_metadata.st_uid != 0
            or venv_metadata.st_gid != 0
            or stat.S_IMODE(venv_metadata.st_mode) != 0o755
        ):
            raise InstallError("root venv metadata is unsafe")

        lock_path = VENV / ".lock"
        try:
            path_metadata = os.lstat(lock_path)
        except FileNotFoundError:
            if allow_absent:
                return None
            raise InstallError("root venv lock is unavailable") from None
        except OSError as exc:
            raise InstallError("root venv lock is unavailable") from exc
        if (
            not stat.S_ISREG(path_metadata.st_mode)
            or stat.S_ISLNK(path_metadata.st_mode)
            or path_metadata.st_uid != 0
            or path_metadata.st_gid != 0
        ):
            raise InstallError("root venv lock authority is unsafe")
        return path_metadata

    def venv_lock_requires_hardening(self) -> bool:
        """Detect the one safe, installer-owned venv drift that may be repaired."""
        metadata = self._venv_lock_metadata(allow_absent=True)
        return bool(metadata is not None and stat.S_IMODE(metadata.st_mode) != 0o600)

    def harden_venv_lock(self) -> None:
        """Converge uv's synchronization lock before validating venv authority."""
        path_metadata = self._venv_lock_metadata(allow_absent=False)
        if path_metadata is None:  # pragma: no cover - allow_absent=False owns this invariant
            raise InstallError("root venv lock is unavailable")

        lock_path = VENV / ".lock"
        flags = (
            os.O_RDONLY
            | getattr(os, "O_CLOEXEC", 0)
            | getattr(os, "O_NOFOLLOW", 0)
            | getattr(os, "O_NONBLOCK", 0)
        )
        try:
            lock_fd = os.open(lock_path, flags)
        except OSError as exc:
            raise InstallError("root venv lock authority is unsafe") from exc
        close_error: OSError | None = None
        try:
            opened_metadata = os.fstat(lock_fd)
            if (
                not stat.S_ISREG(opened_metadata.st_mode)
                or opened_metadata.st_uid != 0
                or opened_metadata.st_gid != 0
                or (opened_metadata.st_dev, opened_metadata.st_ino)
                != (path_metadata.st_dev, path_metadata.st_ino)
            ):
                raise InstallError("root venv lock authority is unsafe")
            if stat.S_IMODE(opened_metadata.st_mode) != 0o600:
                os.fchmod(lock_fd, 0o600)
            hardened_metadata = os.fstat(lock_fd)
            if (
                not stat.S_ISREG(hardened_metadata.st_mode)
                or hardened_metadata.st_uid != 0
                or hardened_metadata.st_gid != 0
                or stat.S_IMODE(hardened_metadata.st_mode) != 0o600
                or (hardened_metadata.st_dev, hardened_metadata.st_ino)
                != (path_metadata.st_dev, path_metadata.st_ino)
            ):
                raise InstallError("root venv lock hardening did not converge")
        except OSError as exc:
            raise InstallError("root venv lock hardening failed") from exc
        finally:
            try:
                os.close(lock_fd)
            except OSError as exc:
                close_error = exc
        if close_error is not None:
            raise InstallError("root venv lock close failed") from close_error

    def sync_venv(self, source_root: Path) -> None:
        system_python = _safe_root_executable(SYSTEM_PYTHON, label="system Python")
        uv = _safe_root_executable(UV_BINARY, label="uv")
        self._validate_system_python_version(system_python)
        environment = {"PATH": _ROOT_PATH, "LANG": "C.UTF-8", "LC_ALL": "C.UTF-8"}
        environment["UV_PROJECT_ENVIRONMENT"] = str(VENV)
        self.runner.run(
            [
                str(uv),
                "sync",
                "--project",
                str(source_root),
                "--no-editable",
                "--extra",
                "cluster",
                "--extra",
                "rollout",
                "--reinstall-package",
                "loom",
                "--python",
                str(system_python),
            ],
            env=environment,
        )
        self.harden_venv_lock()
        if not self.venv_ready():  # pragma: no cover - venv_ready either succeeds or raises
            raise InstallError("root venv installation did not converge")
        if not self.package_runtime_ready():
            raise InstallError("root venv broker import probe failed")

    def _runtime_probe(self, program: str) -> bool:
        service_uid, _service_gid = self._service_ids()
        result = self.runner.run(
            [
                "sudo",
                "-n",
                "-u",
                SERVICE_USER,
                "--",
                "/usr/bin/env",
                "-i",
                f"HOME={STATE_ROOT}",
                f"USER={SERVICE_USER}",
                f"LOGNAME={SERVICE_USER}",
                f"PATH={VENV / 'bin'}:{_ROOT_PATH}",
                "LANG=C.UTF-8",
                "LC_ALL=C.UTF-8",
                f"XDG_RUNTIME_DIR=/run/user/{service_uid}",
                f"DBUS_SESSION_BUS_ADDRESS=unix:path=/run/user/{service_uid}/bus",
                f"KUBECONFIG={KUBECONFIG_PATH}",
                f"LOOM_STAGING_ROLLOUT_CONFIG={CONFIG_PATH}",
                str(VENV / "bin/python"),
                "-I",
                "-B",
                "-c",
                program,
            ],
            check=False,
        )
        return result.returncode == 0

    def package_runtime_ready(self) -> bool:
        """Import the broker and render packaged assets as the service user."""
        return self._runtime_probe(_PACKAGE_RUNTIME_PROBE)

    def broker_runtime_ready(self) -> bool:
        """Exercise packaged assets and load the protected broker config."""
        return self._runtime_probe(_BROKER_RUNTIME_PROBE)

    def ensure_service_key(self) -> bool:
        if self.service_key_present():
            return False
        self.runner.run(
            [
                "sudo",
                "-n",
                "-u",
                SERVICE_USER,
                "--",
                "ssh-keygen",
                "-q",
                "-t",
                "ed25519",
                "-N",
                "",
                "-C",
                "loom-staging-rollout-service",
                "-f",
                str(SERVICE_KEY),
            ]
        )
        self._validate_service_key_pair()
        return True

    def service_key_present(self) -> bool:
        public_key = Path(str(SERVICE_KEY) + ".pub")
        private_present = self._probe(["test", "-e", str(SERVICE_KEY)]).returncode == 0
        public_present = self._probe(["test", "-e", str(public_key)]).returncode == 0
        if not private_present and not public_present:
            return False
        if not private_present or not public_present:
            raise InstallError("service deploy key pair is incomplete")
        self._validate_service_key_pair()
        return True

    def _validate_service_key_pair(self) -> None:
        expected = (
            (SERVICE_KEY, "regular file:loom-rollout:loom-rollout:600"),
            (Path(str(SERVICE_KEY) + ".pub"), "regular file:loom-rollout:loom-rollout:644"),
        )
        for path, wanted in expected:
            if self._probe(["test", "-L", str(path)]).returncode == 0:
                raise InstallError("service deploy key pair contains a symlink")
            metadata = self.runner.run(["stat", "-c", "%F:%U:%G:%a", str(path)]).stdout.strip()
            if metadata != wanted:
                raise InstallError("service deploy key pair metadata is unsafe")
        private_public = self.runner.run(
            ["ssh-keygen", "-y", "-f", str(SERVICE_KEY)]
        ).stdout.strip()
        if not private_public.startswith("ssh-ed25519 "):
            raise InstallError("service deploy private key is invalid")
        private_fields = self.runner.run(
            ["ssh-keygen", "-lf", "-"],
            input_text=private_public + "\n",
        ).stdout.split()
        public_fields = self.runner.run(
            ["ssh-keygen", "-lf", str(SERVICE_KEY) + ".pub"]
        ).stdout.split()
        private_fingerprint = self._validated_ssh_fingerprint(private_fields)
        public_fingerprint = self._validated_ssh_fingerprint(public_fields)
        if private_fingerprint != public_fingerprint:
            raise InstallError("service deploy private/public key fingerprints do not match")

    @staticmethod
    def _validated_ssh_fingerprint(fields: Sequence[str]) -> str:
        if len(fields) < 2 or re.fullmatch(r"SHA256:[A-Za-z0-9+/]{43}", fields[1]) is None:
            raise InstallError("service deploy public-key fingerprint is invalid")
        return fields[1]

    def public_key_fingerprint(self) -> str:
        self._validate_service_key_pair()
        fields = self.runner.run(["ssh-keygen", "-lf", str(SERVICE_KEY) + ".pub"]).stdout.split()
        return self._validated_ssh_fingerprint(fields)

    def validate_service_key_continuity(self, expected_fingerprint: str) -> None:
        if not self.service_key_present():
            raise InstallError("existing GB10 trust authority requires its service key pair")
        if self.public_key_fingerprint() != expected_fingerprint:
            raise InstallError("service deploy key fingerprint drifted from the install record")

    @staticmethod
    def _permissions_allow(actual: str, required: str) -> bool:
        return len(actual) == 3 and all(
            wanted == "-" or actual[index] == wanted for index, wanted in enumerate(required)
        )

    @staticmethod
    def _permission_union(left: str, right: str) -> str:
        return "".join(left[index] if left[index] != "-" else right[index] for index in range(3))

    @staticmethod
    def _permission_intersection(left: str, right: str) -> str:
        return "".join(
            left[index] if left[index] != "-" and right[index] != "-" else "-" for index in range(3)
        )

    def _acl_entries(self, path: Path) -> tuple[ParsedAclEntry, ...]:
        lines = self.runner.run(["getfacl", "-cp", str(path)]).stdout.splitlines()
        entries: list[ParsedAclEntry] = []
        for line in lines:
            stripped = line.strip()
            if not stripped or stripped.startswith("#") or stripped.startswith("flags:"):
                continue
            match = _ACL_ENTRY_RE.fullmatch(stripped)
            if match is None:
                if stripped.startswith(("user:", "group:", "mask:", "other:", "default:")):
                    raise InstallError("host ACL output is invalid")
                continue
            entries.append(
                ParsedAclEntry(
                    default=match.group(1) is not None,
                    tag=match.group(2),
                    qualifier=match.group(3),
                    permissions=match.group(4),
                    effective=match.group(5) or match.group(4),
                )
            )
        parsed = tuple(entries)
        self._acl_snapshot(parsed, default=False)
        self._acl_snapshot(parsed, default=True)
        return parsed

    def _acl_snapshot(
        self,
        entries: Sequence[ParsedAclEntry],
        *,
        default: bool,
    ) -> tuple[str, ...]:
        selected = [entry for entry in entries if entry.default == default]
        raw = {(entry.tag, entry.qualifier): entry.permissions for entry in selected}
        if len(raw) != len(selected):
            raise InstallError("host ACL output contains duplicate entries")
        snapshot = _canonical_acl_snapshot(raw)
        try:
            validated = _acl_snapshot_map(snapshot, allow_empty=default)
        except InstallError as exc:
            raise InstallError("host ACL output is invalid") from exc
        mask = validated.get(("mask", ""))
        for entry in selected:
            masked = (entry.tag == "user" and bool(entry.qualifier)) or entry.tag == "group"
            expected = (
                self._permission_intersection(entry.permissions, mask)
                if masked and mask is not None
                else entry.permissions
            )
            if entry.effective != expected:
                raise InstallError("host ACL effective permissions are inconsistent")
        return snapshot

    @staticmethod
    def _acl_entry_from(
        entries: Sequence[ParsedAclEntry], *, default: bool
    ) -> tuple[str, str] | None:
        matches = [
            entry
            for entry in entries
            if entry.default == default and entry.tag == "user" and entry.qualifier == SERVICE_USER
        ]
        if len(matches) > 1:
            raise InstallError("service ACL entry is duplicated")
        if matches:
            return matches[0].permissions, matches[0].effective
        return None

    def _declared_operator_acl_user(self, qualifier: str) -> bool:
        if qualifier == "2012":
            return False
        numeric = qualifier.isdigit()
        if not numeric and qualifier not in OPERATORS:
            return False
        result = self._probe(["getent", "passwd", qualifier])
        fields = result.stdout.strip().split(":")
        return (
            result.returncode == 0
            and len(fields) >= 3
            and fields[0] in OPERATORS
            and fields[2].isdigit()
            and fields[2] != "2012"
            and (fields[2] == qualifier if numeric else fields[0] == qualifier)
        )

    def _effective_acl(
        self,
        snapshot: Sequence[str],
        *,
        allow_empty: bool,
    ) -> dict[tuple[str, str], str]:
        entries = _acl_snapshot_map(snapshot, allow_empty=allow_empty)
        mask = entries.get(("mask", ""))
        effective: dict[tuple[str, str], str] = {}
        for key, permissions in entries.items():
            tag, qualifier = key
            if tag == "mask":
                continue
            masked = (tag == "user" and bool(qualifier)) or tag == "group"
            effective[key] = (
                self._permission_intersection(permissions, mask)
                if masked and mask is not None
                else permissions
            )
        return effective

    def _assert_acl_transition_safe(
        self,
        before_acl: Sequence[str],
        after_acl: Sequence[str],
        *,
        default: bool,
    ) -> None:
        before = self._effective_acl(before_acl, allow_empty=default)
        after = self._effective_acl(after_acl, allow_empty=False)
        service_key = ("user", SERVICE_USER)
        for key, permissions_after in after.items():
            if key == service_key:
                continue
            if key not in before:
                if (
                    default
                    and not before_acl
                    and key
                    in {
                        ("user", ""),
                        ("group", ""),
                        ("other", ""),
                    }
                ):
                    continue
                raise InstallError("ACL transition would add an undeclared principal")
            permissions_before = before[key]
            gained = any(
                permissions_before[index] == "-" and permissions_after[index] != "-"
                for index in range(3)
            )
            if not gained:
                continue
            tag, qualifier = key
            if tag == "user" and qualifier and self._declared_operator_acl_user(qualifier):
                continue
            raise InstallError("ACL mask transition would change an undeclared principal")

    def _planned_acl_snapshots(
        self,
        entries: Sequence[ParsedAclEntry],
        *,
        default: bool,
        permissions: str,
        existing: tuple[str, str] | None,
    ) -> tuple[tuple[str, ...], tuple[str, ...], str | None, str]:
        before_acl = self._acl_snapshot(entries, default=default)
        before = _acl_snapshot_map(before_acl, allow_empty=default)
        after = dict(before)
        if default and not after:
            access_acl = self._acl_snapshot(entries, default=False)
            access = self._effective_acl(
                access_acl,
                allow_empty=False,
            )
            after = {key: access[key] for key in (("user", ""), ("group", ""), ("other", ""))}
        service_key = ("user", SERVICE_USER)
        after[service_key] = existing[0] if existing is not None else permissions
        before_mask = before.get(("mask", ""))
        mask_baseline = before_mask or after[("group", "")]
        after_mask = self._permission_union(mask_baseline, permissions)
        after[("mask", "")] = after_mask
        after_acl = _canonical_acl_snapshot(after)
        self._assert_acl_transition_safe(before_acl, after_acl, default=default)
        effective = self._effective_acl(after_acl, allow_empty=False)[service_key]
        if not self._permissions_allow(effective, permissions):
            raise InstallError("planned service ACL would remain ineffective")
        return before_acl, after_acl, before_mask, after_mask

    def _acl_entry(self, path: Path, *, default: bool) -> tuple[str, str] | None:
        return self._acl_entry_from(self._acl_entries(path), default=default)

    def _plan_acl(self, path: Path, *, permissions: str, default: bool) -> AclPlan | None:
        entries = self._acl_entries(path)
        existing = self._acl_entry_from(entries, default=default)
        if existing is not None:
            if all(self._permissions_allow(value, permissions) for value in existing):
                return None
            if not self._permissions_allow(existing[0], permissions):
                raise InstallError("pre-existing service ACL is insufficient")
        before_acl, after_acl, before_mask, after_mask = self._planned_acl_snapshots(
            entries,
            default=default,
            permissions=permissions,
            existing=existing,
        )
        adjustment = None
        if before_mask != after_mask:
            adjustment = AclMaskAdjustment(
                path=path,
                default=default,
                before_mask=before_mask,
                after_mask=after_mask,
                before_acl=before_acl,
                after_acl=after_acl,
            )
        return AclPlan(
            AclGrant(path=path, default=default),
            permissions,
            adds_service_entry=existing is None,
            before_acl=before_acl,
            after_acl=after_acl,
            mask_adjustment=adjustment,
        )

    @staticmethod
    def _acl_spec(
        key: tuple[str, str],
        *,
        default: bool,
        permissions: str | None,
    ) -> str:
        tag, qualifier = key
        short_tag = {"user": "u", "group": "g", "mask": "m", "other": "o"}[tag]
        prefix = "d:" if default else ""
        if permissions is None:
            return f"{prefix}{short_tag}:{qualifier}" if qualifier else f"{prefix}{short_tag}"
        return f"{prefix}{short_tag}:{qualifier}:{permissions}"

    def _apply_acl_transition(
        self,
        path: Path,
        *,
        default: bool,
        before_acl: Sequence[str],
        after_acl: Sequence[str],
    ) -> None:
        before = _acl_snapshot_map(before_acl, allow_empty=default)
        after = _acl_snapshot_map(after_acl, allow_empty=default)
        if before == after:
            return
        if default and not after:
            self.runner.run(["setfacl", "-k", str(path)])
            return
        removed = [key for key in before if key not in after]
        modified = [key for key, value in after.items() if before.get(key) != value]
        argv = ["setfacl", "-n"]
        if modified:
            argv.extend(
                [
                    "-m",
                    ",".join(
                        self._acl_spec(
                            key,
                            default=default,
                            permissions=after[key],
                        )
                        for key in modified
                    ),
                ]
            )
        if removed:
            argv.extend(
                [
                    "-x",
                    ",".join(
                        self._acl_spec(key, default=default, permissions=None) for key in removed
                    ),
                ]
            )
        argv.append(str(path))
        self.runner.run(argv)

    def apply_acl(self, plan: AclPlan) -> AclGrant:
        path = plan.grant.path
        default = plan.grant.default
        entries = self._acl_entries(path)
        current_acl = self._acl_snapshot(entries, default=default)
        if current_acl != plan.before_acl:
            raise InstallError("ACL changed before convergence")
        self._apply_acl_transition(
            path,
            default=default,
            before_acl=plan.before_acl,
            after_acl=plan.after_acl,
        )
        confirmed_entries = self._acl_entries(path)
        if self._acl_snapshot(confirmed_entries, default=default) != plan.after_acl:
            raise InstallError("service ACL transition did not converge")
        confirmed = self._acl_entry_from(confirmed_entries, default=default)
        if confirmed is None or not all(
            self._permissions_allow(value, plan.permissions) for value in confirmed
        ):
            raise InstallError("service ACL did not become effective")
        return plan.grant

    def acl_adjustment_state(self, adjustment: AclMaskAdjustment) -> str:
        self._assert_acl_transition_safe(
            adjustment.before_acl,
            adjustment.after_acl,
            default=adjustment.default,
        )
        current = self._acl_snapshot(
            self._acl_entries(adjustment.path),
            default=adjustment.default,
        )
        if current == adjustment.before_acl:
            return "before"
        if current == adjustment.after_acl:
            return "after"
        return "drift"

    def plan_input_acl(self, path: Path) -> tuple[AclPlan, ...]:
        plans: list[AclPlan] = []
        for parent in path.parents:
            if parent == Path("/"):
                continue
            plan = self._plan_acl(parent, permissions="--x", default=False)
            if plan is not None:
                plans.append(plan)
            if parent == Path("/shared_work"):
                break
        plan = self._plan_acl(path, permissions="r--", default=False)
        if plan is not None:
            plans.append(plan)
        return tuple(plans)

    def plan_data_acl(self, path: Path) -> tuple[AclPlan, ...]:
        plans = [
            plan
            for plan in (
                self._plan_acl(path, permissions="rwx", default=False),
                self._plan_acl(path, permissions="rwx", default=True),
            )
            if plan is not None
        ]
        return tuple(plans)

    def ensure_input_acl(self, path: Path) -> tuple[AclGrant, ...]:
        return tuple(self.apply_acl(plan) for plan in self.plan_input_acl(path))

    def ensure_data_acl(self, path: Path) -> tuple[AclGrant, ...]:
        return tuple(self.apply_acl(plan) for plan in self.plan_data_acl(path))

    def remove_acl(
        self,
        grant: AclGrant,
        mask_adjustment: AclMaskAdjustment | None = None,
        *,
        remove_service_entry: bool = True,
    ) -> None:
        entries = self._acl_entries(grant.path)
        current_acl = self._acl_snapshot(entries, default=grant.default)
        service_key = ("user", SERVICE_USER)
        if mask_adjustment is not None:
            if (mask_adjustment.path, mask_adjustment.default) != (
                grant.path,
                grant.default,
            ):
                raise InstallError("ACL mask ledger scope is inconsistent")
            before = _acl_snapshot_map(
                mask_adjustment.before_acl,
                allow_empty=grant.default,
            )
            target = dict(before)
            if remove_service_entry:
                target.pop(service_key, None)
            target_acl = _canonical_acl_snapshot(target)
            if current_acl == target_acl:
                return
            if current_acl not in {
                mask_adjustment.before_acl,
                mask_adjustment.after_acl,
            }:
                raise InstallError("service ACL changed before removal")
        else:
            current = _acl_snapshot_map(current_acl, allow_empty=grant.default)
            if not remove_service_entry or service_key not in current:
                return
            target = dict(current)
            target.pop(service_key)
            target_acl = _canonical_acl_snapshot(target)
        self._apply_acl_transition(
            grant.path,
            default=grant.default,
            before_acl=current_acl,
            after_acl=target_acl,
        )
        restored = self._acl_snapshot(
            self._acl_entries(grant.path),
            default=grant.default,
        )
        if restored != target_acl:
            raise InstallError("service ACL restoration did not converge")

    def export_kubeconfig(self) -> bytes:
        source_payload = _read_root_kubeconfig_source()
        with tempfile.TemporaryDirectory(
            prefix="loom-staging-kubeconfig-",
            dir=ROOT_KUBECONFIG_SNAPSHOT_PARENT,
        ) as raw_directory:
            snapshot_fd, raw_snapshot = tempfile.mkstemp(dir=raw_directory)
            snapshot = Path(raw_snapshot)
            try:
                os.fchmod(snapshot_fd, 0o600)
                with os.fdopen(snapshot_fd, "wb", closefd=True) as handle:
                    handle.write(source_payload)
                    handle.flush()
                    os.fsync(handle.fileno())
            except OSError as exc:
                try:
                    os.close(snapshot_fd)
                except OSError:
                    pass
                raise InstallError("private kubeconfig snapshot failed") from exc
            result = self.runner.run(
                [
                    "kubectl",
                    "--kubeconfig",
                    str(snapshot),
                    "config",
                    "view",
                    "--raw",
                    "--minify",
                    "--context",
                    "loom-staging",
                ]
            )
        if not result.stdout.strip():
            raise InstallError("loom-staging kubeconfig export is empty")
        return result.stdout.encode("utf-8")

    def ensure_linger(self) -> bool:
        if self.linger_enabled():
            return False
        self.runner.run(["loginctl", "enable-linger", SERVICE_USER])
        return True

    def linger_enabled(self) -> bool:
        return Path(f"/var/lib/systemd/linger/{SERVICE_USER}").is_file()

    def verify_user_manager(self) -> None:
        uid = self.runner.run(["id", "-u", SERVICE_USER]).stdout.strip()
        if not uid.isdigit():
            raise InstallError("service UID is unavailable")
        manager_version = self.runner.run(
            [
                "sudo",
                "-n",
                "-u",
                SERVICE_USER,
                "--",
                "/usr/bin/env",
                "-i",
                f"XDG_RUNTIME_DIR=/run/user/{uid}",
                f"DBUS_SESSION_BUS_ADDRESS=unix:path=/run/user/{uid}/bus",
                "PATH=/usr/bin:/bin",
                "/usr/bin/systemctl",
                "--user",
                "show",
                "--property=Version",
                "--value",
            ]
        ).stdout.strip()
        if re.fullmatch(r"[0-9]+(?:[.][0-9]+)*(?:[-+~.A-Za-z0-9]*)?", manager_version) is None:
            raise InstallError("service user manager version is invalid")

    def install_owner(
        self,
        path: Path,
        owner: str,
        mode: int,
        *,
        group: str | None = None,
    ) -> bool:
        target_group = owner if group is None else group
        if self.file_owner_ready(path, owner=owner, group=target_group, mode=mode):
            return False
        self.runner.run(["chown", f"{owner}:{target_group}", str(path)])
        self.runner.run(["chmod", f"{mode:04o}", str(path)])
        return True

    def file_owner_ready(
        self,
        path: Path,
        *,
        owner: str,
        mode: int,
        group: str | None = None,
        nlink: int | None = None,
    ) -> bool:
        target_group = owner if group is None else group
        expected = f"{owner}:{target_group}:{mode:o}"
        stat_format = "%U:%G:%a"
        if nlink is not None:
            stat_format += ":%h"
            expected += f":{nlink}"
        current = self._probe(["stat", "-c", stat_format, str(path)]).stdout.strip()
        return current == expected

    def gb10_trust_ready(self) -> bool:
        result = self.runner.run(
            [
                str(VENV / "bin/python"),
                str(TRUST_TOOL_PATH),
                "check",
            ],
            check=False,
            **self._trust_command_kwargs(),
        )
        return result.returncode == 0

    def prepare_gb10_trust_ledger(
        self,
        source_root: Path,
        *,
        mode: str,
        previous_source_sha: str | None,
    ) -> None:
        operations = {
            "fresh": "initialize-ledger",
            "legacy": "register-legacy-ledger",
            "existing": "migrate-active-policy",
        }
        operation = operations.get(mode)
        if operation is None:
            raise InstallError("GB10 trust ledger preparation mode is invalid")
        system_python = _safe_root_executable(SYSTEM_PYTHON, label="system Python")
        self._validate_system_python_version(system_python)
        if mode == "legacy":
            if previous_source_sha is None or _SHA_RE.fullmatch(previous_source_sha) is None:
                raise InstallError("legacy GB10 trust source binding is unavailable")
            previous_config = self.source_file(
                INSTALL_SOURCE,
                previous_source_sha,
                "deploy/worker-pools/gb10/ssh_config",
            )
            with tempfile.TemporaryDirectory(prefix="loom-gb10-legacy-topology-") as raw:
                path = Path(raw) / "ssh_config"
                path.write_bytes(previous_config)
                path.chmod(0o600)
                self.runner.run(
                    [
                        str(system_python),
                        "-I",
                        str(source_root / "scripts/ops/staging_rollout_gb10_trust.py"),
                        "validate-legacy-topology",
                        "--previous-config",
                        str(path),
                    ],
                    **self._trust_command_kwargs(),
                )
        self.runner.run(
            [
                str(system_python),
                "-I",
                str(source_root / "scripts/ops/staging_rollout_gb10_trust.py"),
                operation,
            ],
            **self._trust_command_kwargs(),
        )

    def require_gb10_revocation_complete(self) -> None:
        self.runner.run(
            [
                str(VENV / "bin/python"),
                str(TRUST_TOOL_PATH),
                "finalize-check",
            ],
            **self._trust_command_kwargs(),
        )

    def run_post_install_dry_run(self) -> None:
        self.runner.run(
            [
                "sudo",
                "-n",
                "-u",
                "qianyi",
                "--",
                str(CLIENT_PATH),
                "start",
                "--dry-run",
            ]
        )

    def check_runtime(
        self,
        expected_sha: str,
        *,
        source_tree_sha: str | None = None,
        source_base_sha: str | None = None,
    ) -> list[str]:
        failures: list[str] = []
        operator_group = self._probe(["getent", "group", OPERATOR_GROUP])
        if operator_group.returncode != 0:
            failures.append("operator-group")
        else:
            fields = operator_group.stdout.strip().split(":")
            members = set(fields[3].split(",")) if len(fields) >= 4 else set()
            if not set(OPERATORS).issubset(members):
                failures.append("operator-membership")
        service = self._probe(["getent", "passwd", SERVICE_USER])
        fields = service.stdout.strip().split(":")
        service_account_ready = not (
            service.returncode != 0
            or len(fields) < 7
            or not fields[2].isdigit()
            or int(fields[2]) == 0
            or fields[5] != str(STATE_ROOT)
            or fields[6] != "/usr/sbin/nologin"
        )
        if not service_account_ready:
            failures.append("service-account")
        else:
            try:
                self._service_ids()
            except InstallError:
                service_account_ready = False
                failures.append("service-primary-group")
        service_groups = set(self._probe(["id", "-nG", SERVICE_USER]).stdout.split())
        if service_groups != {SERVICE_USER, "docker"}:
            failures.append("service-groups")
        if not self.shared_work2_mount_ready():
            failures.append("shared-work2-mount")
        try:
            if not self.shared_worker_repo_root_ready():
                failures.append("shared-worker-repo-root")
        except InstallError:
            failures.append("shared-worker-repo-root")
        if not service_account_ready or not self.candidate_ready(
            expected_sha,
            source_tree_sha=source_tree_sha,
            source_base_sha=source_base_sha,
        ):
            failures.append("candidate-checkout")
        for path in PROTECTED_INPUTS:
            try:
                leaf = self._acl_entry(path, default=False)
                parent_entries: list[tuple[str, str] | None] = []
                for parent in path.parents:
                    if parent == Path("/"):
                        continue
                    parent_entries.append(self._acl_entry(parent, default=False))
                    if parent == Path("/shared_work"):
                        break
            except InstallError:
                leaf = None
                parent_entries = []
            if leaf is None or not all(self._permissions_allow(value, "r--") for value in leaf):
                failures.append(f"input-acl:{path}")
            if (
                any(
                    entry is None
                    or not all(self._permissions_allow(value, "--x") for value in entry)
                    for entry in parent_entries
                )
                or not parent_entries
            ):
                failures.append(f"input-traverse-acl:{path}")
        for path in DATA_DIRECTORIES:
            try:
                entries = (
                    self._acl_entry(path, default=False),
                    self._acl_entry(path, default=True),
                )
            except InstallError:
                entries = (None, None)
            if any(
                entry is None or not all(self._permissions_allow(value, "rwx") for value in entry)
                for entry in entries
            ):
                failures.append(f"data-acl:{path}")
        authority = {
            CLIENT_PATH: "regular file:root:root:755",
            BROKER_PATH: "regular file:root:root:755",
            REHEARSAL_PATH: "regular file:root:root:755",
            SUDOERS_PATH: "regular file:root:root:440",
            TMPFILES_PATH: "regular file:root:root:644",
            CONFIG_PATH: "regular file:root:loom-rollout:640",
            INSTALL_RECORD: "regular file:root:root:600",
            INSTALL_ATTESTATION: "regular file:root:loom-rollout:640",
            TRUST_REVOCATION_LEDGER: "regular file:root:root:600",
            TRUST_TOOL_PATH: "regular file:root:root:755",
            KNOWN_HOSTS_PATH: "regular file:root:root:644",
            SHARED_WORK2_MOUNT_UNIT_PATH: "regular file:root:root:644",
            KUBECONFIG_PATH: "regular file:loom-rollout:loom-rollout:600",
            RUNTIME_ROOT: "directory:loom-rollout:loom-rollout:700",
            Path("/etc/loom"): "directory:root:root:755",
            RUNNER_ROOT: "directory:root:root:755",
            Path("/usr/local/libexec"): "directory:root:root:755",
            Path("/usr/local/bin"): "directory:root:root:755",
            SUDOERS_PATH.parent: "directory:root:root:755",
            TMPFILES_PATH.parent: "directory:root:root:755",
            SHARED_WORK2_MOUNT_UNIT_PATH.parent: "directory:root:root:755",
        }
        for path, expected in authority.items():
            actual = self._probe(["stat", "-c", "%F:%U:%G:%a", str(path)])
            if actual.returncode != 0 or actual.stdout.strip() != expected:
                failures.append(f"metadata:{path}")
        config_links = self._probe(["stat", "-c", "%h", str(CONFIG_PATH)])
        if config_links.returncode != 0 or config_links.stdout.strip() != "1":
            failures.append(f"metadata-links:{CONFIG_PATH}")
        venv_ready = self.venv_ready()
        if not venv_ready:
            failures.append("root-venv")
        elif not service_account_ready:
            failures.append("broker-runtime")
        else:
            try:
                broker_ready = self.broker_runtime_ready()
            except InstallError:
                broker_ready = False
            if not broker_ready:
                failures.append("broker-runtime")
        try:
            self._validate_service_key_pair()
        except InstallError:
            failures.append("service-key")
        if not Path(f"/var/lib/systemd/linger/{SERVICE_USER}").is_file():
            failures.append("linger")
        if not self.gb10_trust_ready():
            failures.append("gb10-trust")
        return failures

    def active_status(self) -> str:
        """Prove inactivity without importing the runtime being replaced.

        Install and uninstall call this only after ``begin_maintenance`` has
        acquired the broker launch lock and published the root-owned admission
        marker. A safe active pointer or any active rollout unit blocks the
        operation; malformed or unreadable state returns ``unknown``.
        """
        try:
            service_uid, service_gid = self._service_ids()
            marker = os.lstat(MAINTENANCE_MARKER)
            state_root = os.lstat(STATE_ROOT)
        except (InstallError, OSError):
            return "unknown"

        if (
            not stat.S_ISREG(marker.st_mode)
            or marker.st_uid != 0
            or marker.st_gid != 0
            or stat.S_IMODE(marker.st_mode) != 0o600
            or not stat.S_ISDIR(state_root.st_mode)
            or state_root.st_uid != service_uid
            or state_root.st_gid != service_gid
            or stat.S_IMODE(state_root.st_mode) != 0o700
        ):
            return "unknown"

        try:
            pointer = os.lstat(ACTIVE_POINTER)
        except FileNotFoundError:
            pass
        except OSError:
            return "unknown"
        else:
            if (
                not stat.S_ISREG(pointer.st_mode)
                or pointer.st_uid != service_uid
                or pointer.st_gid != service_gid
                or stat.S_IMODE(pointer.st_mode) != 0o600
            ):
                return "unknown"
            return "busy"

        result = self.runner.run(
            [
                "sudo",
                "-n",
                "-u",
                SERVICE_USER,
                "--",
                "/usr/bin/env",
                "-i",
                f"XDG_RUNTIME_DIR=/run/user/{service_uid}",
                f"DBUS_SESSION_BUS_ADDRESS=unix:path=/run/user/{service_uid}/bus",
                "PATH=/usr/bin:/bin",
                "LANG=C.UTF-8",
                "LC_ALL=C.UTF-8",
                "/usr/bin/systemctl",
                "--user",
                "list-units",
                "--all",
                "--plain",
                "--full",
                "--type=service",
                "--no-legend",
                "--no-pager",
                "loom-staging-rollout-*.service",
            ],
            check=False,
        )
        if result.returncode != 0 or result.stderr.strip():
            return "unknown"
        for line in result.stdout.splitlines():
            fields = line.split(maxsplit=4)
            if len(fields) < 4:
                return "unknown"
            unit_name, load_state, active_state, sub_state = fields[:4]
            if (
                len(unit_name) > 255
                or _ROLLOUT_UNIT_RE.fullmatch(unit_name) is None
                or load_state != "loaded"
                or _SYSTEMD_STATE_TOKEN_RE.fullmatch(active_state) is None
                or _SYSTEMD_STATE_TOKEN_RE.fullmatch(sub_state) is None
            ):
                return "unknown"
            if active_state not in {"inactive", "failed"}:
                return "busy"
        return "idle"

    def begin_maintenance(self) -> None:
        uid, gid = self._service_ids()
        _maintenance_marker(
            RUNTIME_ROOT,
            service_uid=uid,
            service_gid=gid,
            enabled=True,
        )

    def end_maintenance(self) -> None:
        uid, gid = self._service_ids()
        _maintenance_marker(
            RUNTIME_ROOT,
            service_uid=uid,
            service_gid=gid,
            enabled=False,
        )

    def remove_operator_membership(self, username: str) -> None:
        groups = self.runner.run(["id", "-nG", username]).stdout.split()
        if OPERATOR_GROUP in groups:
            self.runner.run(["gpasswd", "-d", username, OPERATOR_GROUP])

    def remove_docker_membership(self) -> None:
        result = self._probe(["id", "-nG", SERVICE_USER])
        if result.returncode != 0:
            return
        groups = result.stdout.split()
        if "docker" in groups:
            self.runner.run(["gpasswd", "-d", SERVICE_USER, "docker"])

    def disable_linger(self) -> None:
        if Path(f"/var/lib/systemd/linger/{SERVICE_USER}").exists():
            self.runner.run(["loginctl", "disable-linger", SERVICE_USER])

    def revoke_gb10_trust(self) -> None:
        self.runner.run(
            [
                str(VENV / "bin/python"),
                str(TRUST_TOOL_PATH),
                "revoke",
            ],
            **self._trust_command_kwargs(),
        )


def _token_fingerprint(payload: bytes) -> str:
    value = payload.strip()
    if not value:
        raise InstallError("admin token source is empty")
    return f"sha256:{hashlib.sha256(value).hexdigest()[:12]} len={len(value)}"


def _validate_known_hosts_authority(payload: bytes) -> None:
    try:
        lines = payload.decode("ascii").splitlines()
    except UnicodeDecodeError as exc:
        raise InstallError("GB10 known-hosts authority must be ASCII") from exc
    entries = [line for line in lines if line and not line.startswith("#")]
    expected_hosts = (
        "[207.35.188.227]:2221,trt-gb10-1",
        *(f"192.168.20.{number + 10},trt-gb10-{number}" for number in range(2, 16)),
    )
    if len(entries) != len(expected_hosts):
        raise InstallError("GB10 known-hosts authority must contain exactly 15 hosts")
    observed: list[str] = []
    for line in entries:
        fields = line.split()
        if len(fields) != 3 or fields[1] != "ssh-ed25519":
            raise InstallError("GB10 known-hosts authority contains an invalid entry")
        try:
            blob = base64.b64decode(fields[2], validate=True)
        except (ValueError, binascii.Error) as exc:
            raise InstallError("GB10 known-hosts authority contains an invalid key") from exc
        algorithm = b"ssh-ed25519"
        prefix = len(algorithm).to_bytes(4, "big") + algorithm
        offset = len(prefix)
        if (
            not blob.startswith(prefix)
            or len(blob) < offset + 4
            or int.from_bytes(blob[offset : offset + 4], "big") != 32
            or len(blob) != offset + 4 + 32
        ):
            raise InstallError("GB10 known-hosts authority contains an invalid Ed25519 key")
        observed.append(fields[0])
    if tuple(observed) != expected_hosts:
        raise InstallError("GB10 known-hosts authority host coverage is invalid")


def _validate_team_id(value: str) -> str:
    try:
        parsed = uuid.UUID(value)
    except ValueError as exc:
        raise InstallError("smoke team ID must be a UUID") from exc
    if str(parsed) != value.lower():
        raise InstallError("smoke team ID must use canonical UUID form")
    return str(parsed)


@dataclass(slots=True)
class HostInstaller:
    filesystem: LocalFilesystem
    system: HostSystem
    euid: int
    source_root: Path | None = None
    source_sha: str | None = None
    source_mode: str = "merged-dev"
    source_tree_sha: str | None = None
    source_base_sha: str | None = None

    def _source_file(self, relative_path: str) -> bytes:
        if self.source_root is None or self.source_sha is None:
            raise InstallError("root installation source is not bound")
        return self.system.source_file(self.source_root, self.source_sha, relative_path)

    def _asset(self, name: str) -> bytes:
        return self._source_file(f"deploy/staging-rollout/{name}")

    def _render_config(self, team_id: str, admin_token: bytes) -> bytes:
        template = self._asset("staging-rollout.toml").decode("utf-8")
        if template.count(_FINGERPRINT_TOKEN) != 1 or template.count(_TEAM_TOKEN) != 1:
            raise InstallError("staging rollout config template markers are invalid")
        rendered = template.replace(_FINGERPRINT_TOKEN, _token_fingerprint(admin_token))
        rendered = rendered.replace(_TEAM_TOKEN, _validate_team_id(team_id))
        if self.source_mode == "sealed-cumulative":
            if self.source_sha is None or self.source_tree_sha is None or self.source_base_sha is None:
                raise InstallError("sealed source binding is unavailable for config rendering")
            rendered = rendered.replace("schema_version = 1", "schema_version = 2", 1)
            rendered += (
                '\nsource_mode = "sealed-cumulative"\n'
                f'source_commit_sha = "{self.source_sha}"\n'
                f'source_tree_sha = "{self.source_tree_sha}"\n'
                f'source_base_sha = "{self.source_base_sha}"\n'
            )
        payload = rendered.encode("utf-8")
        self._validate_rendered_config(
            payload,
            team_id,
            source_sha=self.source_sha if self.source_mode == "sealed-cumulative" else None,
            source_tree_sha=self.source_tree_sha,
            source_base_sha=self.source_base_sha,
        )
        return payload

    @staticmethod
    def _validate_rendered_config(
        payload: bytes,
        team_id: str,
        *,
        source_sha: str | None = None,
        source_tree_sha: str | None = None,
        source_base_sha: str | None = None,
    ) -> None:
        try:
            raw = tomllib.loads(payload.decode("utf-8"))
        except (UnicodeDecodeError, tomllib.TOMLDecodeError) as exc:
            raise InstallError("rendered staging config is invalid") from exc
        required = {
            "schema_version",
            "service_user",
            "operator_group",
            "remote_url",
            "target_ref",
            "runner_repo",
            "state_root",
            "runtime_root",
            "rollout_root",
            "kubeconfig_path",
            "cluster_config_path",
            "admin_token_source",
            "worker_token_source",
            "service_token_source",
            "expect_admin_token_fingerprint",
            "cluster_name",
            "namespace",
            "environment",
            "cp_url",
            "smoke_on_behalf_username",
            "smoke_on_behalf_team_id",
            "scope",
            "gb10_prep_concurrency",
            "backup_max_objects",
            "backup_max_entries",
        }
        sealed = raw.get("schema_version") == 2
        if sealed:
            required.update(
                {"source_mode", "source_commit_sha", "source_tree_sha", "source_base_sha"}
            )
        if set(raw) != required:
            raise InstallError("rendered staging config keys are invalid")
        literals: dict[str, object] = {
            "schema_version": 2 if sealed else 1,
            "service_user": SERVICE_USER,
            "operator_group": OPERATOR_GROUP,
            "remote_url": REMOTE_URL,
            "target_ref": FETCH_REF,
            "runner_repo": str(CANDIDATE_REPO),
            "state_root": str(STATE_ROOT),
            "runtime_root": str(RUNTIME_ROOT),
            "rollout_root": "/data/loom-staging",
            "kubeconfig_path": str(KUBECONFIG_PATH),
            "cluster_config_path": str(CANDIDATE_REPO / "deploy/environments/staging.cluster.toml"),
            "admin_token_source": f"file:{PROTECTED_INPUTS[0]}",
            "worker_token_source": f"file:{PROTECTED_INPUTS[2]}",
            "service_token_source": f"file:{PROTECTED_INPUTS[1]}",
            "cluster_name": "loom-staging",
            "namespace": "loom-staging",
            "environment": "staging",
            "cp_url": "http://127.0.0.1:18081",
            "smoke_on_behalf_username": "devansh",
            "smoke_on_behalf_team_id": team_id,
            "scope": "current-gb10",
            "gb10_prep_concurrency": 8,
            "backup_max_objects": 1_000_000,
            "backup_max_entries": 16_000_000,
        }
        if any(raw.get(key) != value for key, value in literals.items()):
            raise InstallError("rendered staging config policy is invalid")
        if sealed and (
            source_sha is None
            or source_tree_sha is None
            or source_base_sha is None
            or raw.get("source_mode") != "sealed-cumulative"
            or raw.get("source_commit_sha") != source_sha
            or raw.get("source_tree_sha") != source_tree_sha
            or raw.get("source_base_sha") != source_base_sha
        ):
            raise InstallError("rendered sealed source policy is invalid")
        fingerprint = raw.get("expect_admin_token_fingerprint")
        if (
            not isinstance(fingerprint, str)
            or re.fullmatch(r"sha256:[0-9a-f]{12} len=[1-9][0-9]*", fingerprint) is None
        ):
            raise InstallError("rendered staging config fingerprint is invalid")

    @staticmethod
    def _managed_acl_scope() -> set[AclGrant]:
        scope: set[AclGrant] = set()
        for path in PROTECTED_INPUTS:
            for parent in path.parents:
                if parent == Path("/"):
                    continue
                scope.add(AclGrant(parent))
                if parent == Path("/shared_work"):
                    break
            scope.add(AclGrant(path))
        for path in DATA_DIRECTORIES:
            scope.update({AclGrant(path), AclGrant(path, default=True)})
        return scope

    @classmethod
    def _record_grants(cls, record: dict[str, object] | None) -> set[AclGrant]:
        if record is None:
            return set()
        raw = record.get("added_acls", [])
        if not isinstance(raw, list):
            raise InstallError("install record ACL ledger is invalid")
        parsed = [AclGrant.from_dict(value) for value in raw]
        grants = set(parsed)
        if len(grants) != len(parsed) or not grants.issubset(cls._managed_acl_scope()):
            raise InstallError("install record ACL ledger is invalid")
        return grants

    @classmethod
    def _record_mask_adjustments(
        cls,
        record: dict[str, object] | None,
    ) -> dict[AclGrant, AclMaskAdjustment]:
        if record is None:
            return {}
        if record.get("schema_version") in {1, 2} and "acl_mask_adjustments" in record:
            raise InstallError("legacy install record contains an unsupported ACL mask ledger")
        raw = record.get("acl_mask_adjustments", [])
        if not isinstance(raw, list):
            raise InstallError("install record ACL mask ledger is invalid")
        adjustments: dict[AclGrant, AclMaskAdjustment] = {}
        for value in raw:
            adjustment = AclMaskAdjustment.from_dict(value)
            grant = AclGrant(adjustment.path, default=adjustment.default)
            if grant in adjustments or grant not in cls._managed_acl_scope():
                raise InstallError("install record ACL mask ledger is invalid")
            adjustments[grant] = adjustment
        return adjustments

    @staticmethod
    def _validate_acl_ledgers(
        grants: set[AclGrant],
        adjustments: dict[AclGrant, AclMaskAdjustment],
    ) -> None:
        service_key = ("user", SERVICE_USER)
        for grant, adjustment in adjustments.items():
            before = _acl_snapshot_map(
                adjustment.before_acl,
                allow_empty=grant.default,
            )
            if service_key not in before and grant not in grants:
                raise InstallError("install record ACL ledgers are inconsistent")

    @staticmethod
    def _record_operator_memberships(record: dict[str, object] | None) -> set[str]:
        if record is None:
            return set()
        raw = record.get("added_operator_memberships", [])
        if (
            not isinstance(raw, list)
            or any(not isinstance(value, str) for value in raw)
            or not set(raw).issubset(OPERATORS)
        ):
            raise InstallError("install record operator-membership ledger is invalid")
        return set(raw)

    @staticmethod
    def _record_flag(record: dict[str, object] | None, key: str) -> bool:
        if record is None or key not in record:
            return False
        value = record[key]
        if type(value) is not bool:
            raise InstallError(f"install record {key} ledger is invalid")
        return value

    @staticmethod
    def _record_service_key_fingerprint(record: dict[str, object]) -> str:
        value = record.get("service_key_fingerprint")
        if not isinstance(value, str) or re.fullmatch(r"SHA256:[A-Za-z0-9+/]{43}", value) is None:
            raise InstallError("install record service-key fingerprint is invalid")
        return value

    @staticmethod
    def _record_legacy_trust_source_sha(
        record: dict[str, object] | None,
        *,
        trust_ledger_migrated: bool,
        trust_requires_revocation: bool,
    ) -> str | None:
        if record is None:
            return None
        schema_version = record.get("schema_version")
        field = "trust_legacy_source_sha"
        if schema_version == 1 and any(
            key in record for key in ("trust_ledger_migrated", "trust_ledger_removed", field)
        ):
            raise InstallError("legacy v1 trust-ledger state is invalid")
        if schema_version == 2 and field in record:
            raise InstallError("trust-ledger v2 record contains an unsupported legacy source")
        if trust_ledger_migrated or not trust_requires_revocation:
            if field in record:
                raise InstallError("install record contains a stale legacy trust source")
            return None
        if schema_version == 1:
            value = record.get("source_sha")
        elif schema_version in {3, 4}:
            if field not in record:
                raise InstallError("install record legacy trust source is unavailable")
            value = record[field]
        else:
            raise InstallError(
                "interrupted trust-ledger v2 migration lost its legacy source binding"
            )
        if not isinstance(value, str) or _SHA_RE.fullmatch(value) is None:
            raise InstallError("install record legacy trust source is invalid")
        return value

    def _bind_existing_source(self, record: dict[str, object]) -> None:
        sha = record.get("source_sha")
        if not isinstance(sha, str):
            raise InstallError("install record source SHA is invalid")
        installation_state = record.get("installation_state")
        if installation_state not in {"installing", "ready", "uninstalling"}:
            raise InstallError("install record installation state is invalid")
        source_mode = record.get("source_mode", "merged-dev")
        if source_mode not in {"merged-dev", "sealed-cumulative"}:
            raise InstallError("install record source mode is invalid")
        tree = record.get("source_tree_sha") if source_mode == "sealed-cumulative" else None
        base = record.get("source_base_sha") if source_mode == "sealed-cumulative" else None
        if source_mode == "sealed-cumulative" and (
            not isinstance(tree, str) or not isinstance(base, str)
        ):
            raise InstallError("install record sealed source binding is invalid")
        if source_mode == "sealed-cumulative":
            self.system.validate_installed_source(
                sha,
                require_checkout=installation_state == "ready",
                source_tree_sha=tree if isinstance(tree, str) else None,
                source_base_sha=base if isinstance(base, str) else None,
            )
        else:
            self.system.validate_installed_source(
                sha,
                require_checkout=installation_state == "ready",
            )
        self.source_root = INSTALL_SOURCE
        self.source_sha = sha
        self.source_mode = source_mode
        self.source_tree_sha = tree if isinstance(tree, str) else None
        self.source_base_sha = base if isinstance(base, str) else None

    def plan(self) -> dict[str, object]:
        return {
            "remote_url": REMOTE_URL,
            "target_ref": FETCH_REF,
            "service_user": SERVICE_USER,
            "operator_group": OPERATOR_GROUP,
            "operators": list(OPERATORS),
            "shared_worker_repo_root": str(SHARED_WORKER_REPO_ROOT),
            "shared_worker_repo_consumer": SHARED_WORK_CONSUMER,
            "shared_worker_repo_group": SHARED_WORK_GROUP,
            "shared_work2_mount_point": str(SHARED_WORK2_MOUNT_POINT),
            "shared_work2_mount_source": "192.168.20.12:/shared_work2",
            "protected_inputs": [str(path) for path in PROTECTED_INPUTS],
            "data_directories": [str(path) for path in DATA_DIRECTORIES],
            "preserves": [str(STATE_ROOT), "/data/loom-staging/rollouts"],
        }

    def install(
        self,
        team_id: str,
        *,
        sealed_source: SealedSource | None = None,
        _lock_held: bool = False,
    ) -> dict[str, object]:
        if not _lock_held:
            if self.euid != 0:
                raise InstallError("install requires root")
            self.system.ensure_root_directory(TRUST_LIFECYCLE_LOCK.parent, mode=0o755)
            with self.system.trust_lifecycle_lock():
                return self.install(team_id, sealed_source=sealed_source, _lock_held=True)
        if self.euid != 0:
            raise InstallError("install requires root")
        team_id = _validate_team_id(team_id)
        self.system.validate_prerequisites()
        invocation_head = self.system.validate_invocation_checkout()
        if sealed_source is None:
            self.source_mode = "merged-dev"
            self.source_tree_sha = None
            self.source_base_sha = None
            self.source_root, self.source_sha = self.system.prepare_install_source()
        else:
            if invocation_head != sealed_source.commit_sha:
                raise InstallError("installer checkout does not match sealed source commit")
            self.source_mode = "sealed-cumulative"
            self.source_tree_sha = sealed_source.tree_sha
            self.source_base_sha = sealed_source.base_sha
            self.source_root, self.source_sha = self.system.prepare_sealed_install_source(
                sealed_source
            )
        source_sha = self.source_sha
        if source_sha is None:  # pragma: no cover - prepare_install_source owns this
            raise InstallError("root installation source SHA is unavailable")
        if sealed_source is None:
            self.system.validate_invocation_merged(invocation_head, source_sha)
        self.system.validate_assets(self.source_root, source_sha)
        changes: list[str] = []
        root_directories = (
            Path("/etc/loom"),
            RUNNER_ROOT,
            Path("/usr/local/libexec"),
            Path("/usr/local/bin"),
            SUDOERS_PATH.parent,
            TMPFILES_PATH.parent,
            SHARED_WORK2_MOUNT_UNIT_PATH.parent,
        )
        for directory in root_directories:
            if self.system.ensure_root_directory(directory, mode=0o755):
                changes.append(f"directory:{directory}")
        self.system.validate_install_record_authority(allow_absent=True)
        previous_record = self.filesystem.load_install_record()
        if (
            previous_record is not None
            and previous_record.get("installation_state") == "uninstalling"
        ):
            raise InstallError("complete the interrupted uninstall before installing")
        continuity_required = bool(
            previous_record is not None
            and (
                previous_record.get("installation_state") == "ready"
                or self._record_flag(previous_record, "admission_enabled")
                or self._record_flag(previous_record, "trust_requires_revocation")
            )
        )
        if continuity_required:
            if previous_record is None:  # pragma: no cover - guarded above
                raise InstallError("existing GB10 trust authority record is unavailable")
            self.system.validate_service_key_continuity(
                self._record_service_key_fingerprint(previous_record)
            )
        protected_payloads = [
            self.filesystem.read_bytes(path, limit=_MAX_PROTECTED_INPUT_BYTES)
            for path in PROTECTED_INPUTS
        ]
        if any(not payload.strip() for payload in protected_payloads):
            raise InstallError("protected staging input is empty")
        unsafe_data = [
            path for path in DATA_DIRECTORIES if not self.filesystem.is_safe_directory(path)
        ]
        if unsafe_data:
            raise InstallError("required staging data directory is unavailable or unsafe")
        config = self._render_config(team_id, protected_payloads[0])
        kubeconfig = self.system.export_kubeconfig()
        refresh_runtime = bool(
            previous_record is None
            or previous_record.get("source_sha") != source_sha
            or previous_record.get("installation_state") != "ready"
        )

        installed_files = (
            (CLIENT_PATH, self._asset("loom-staging-rollout"), 0o755, "root", "root"),
            (BROKER_PATH, self._asset("loom-staging-rollout-broker"), 0o755, "root", "root"),
            (
                REHEARSAL_PATH,
                self._asset("loom-staging-rollout-rehearsal"),
                0o755,
                "root",
                "root",
            ),
            (
                TRUST_TOOL_PATH,
                self._source_file("scripts/ops/staging_rollout_gb10_trust.py"),
                0o755,
                "root",
                "root",
            ),
            (
                TMPFILES_PATH,
                self._asset("loom-staging-rollout.tmpfiles"),
                0o644,
                "root",
                "root",
            ),
            (
                KNOWN_HOSTS_PATH,
                self._source_file("deploy/worker-pools/gb10/known_hosts"),
                0o644,
                "root",
                "root",
            ),
            (
                SHARED_WORK2_MOUNT_UNIT_PATH,
                self._asset(SHARED_WORK2_MOUNT_UNIT),
                0o644,
                "root",
                "root",
            ),
            (CONFIG_PATH, config, 0o640, "root", SERVICE_GROUP),
        )
        sudoers = self._asset("loom-staging-rollout.sudoers")
        attestation_assets = {
            "broker": installed_files[1][1],
            "client": installed_files[0][1],
            "config": config,
            "gb10-known-hosts": installed_files[5][1],
            "gb10-trust-tool": installed_files[3][1],
            "rehearsal-helper": installed_files[2][1],
            "shared-work2-mount-unit": installed_files[6][1],
            "tmpfiles": installed_files[4][1],
        }

        added_operator_memberships = self._record_operator_memberships(previous_record)
        missing_operators = {
            username
            for username in OPERATORS
            if not self.system.operator_membership_present(username)
        }
        added_operator_memberships.update(missing_operators)
        added_docker_membership = self._record_flag(previous_record, "added_docker_membership")
        docker_missing = not self.system.docker_membership_present()
        added_docker_membership = added_docker_membership or docker_missing
        enabled_linger = self._record_flag(previous_record, "enabled_linger")
        linger_missing = not self.system.linger_enabled()
        enabled_linger = enabled_linger or linger_missing
        created_service_key = self._record_flag(previous_record, "created_service_key")
        service_key_missing = not self.system.service_key_present()
        created_service_key = created_service_key or service_key_missing
        generated_env_error: InstallError | None = None
        try:
            generated_env_templates = self.filesystem.generated_gb10_env_templates()
        except InstallError as exc:
            generated_env_templates = ()
            generated_env_error = exc
        generated_env_templates_ready = bool(generated_env_templates) and all(
            self.system.file_owner_ready(
                path,
                owner=SERVICE_USER,
                mode=0o600,
                nlink=1,
            )
            for path in generated_env_templates
        )
        raw_acl_plans = [
            plan for path in PROTECTED_INPUTS for plan in self.system.plan_input_acl(path)
        ]
        raw_acl_plans.extend(
            plan for path in DATA_DIRECTORIES for plan in self.system.plan_data_acl(path)
        )
        acl_plan_by_grant: dict[AclGrant, AclPlan] = {}
        for plan in raw_acl_plans:
            previous_plan = acl_plan_by_grant.get(plan.grant)
            if previous_plan is not None and previous_plan != plan:
                raise InstallError("conflicting ACL convergence plans")
            acl_plan_by_grant[plan.grant] = plan
        acl_plans = list(acl_plan_by_grant.values())
        grants = self._record_grants(previous_record)
        mask_adjustments = self._record_mask_adjustments(previous_record)
        self._validate_acl_ledgers(grants, mask_adjustments)
        grants.update(plan.grant for plan in acl_plans if plan.adds_service_entry)
        for plan in acl_plans:
            adjustment = plan.mask_adjustment
            if adjustment is None:
                continue
            previous_adjustment = mask_adjustments.get(plan.grant)
            if previous_adjustment is not None and previous_adjustment != adjustment:
                raise InstallError("ACL mask ledger conflicts with the live baseline")
            mask_adjustments[plan.grant] = adjustment
        for grant, adjustment in mask_adjustments.items():
            state = self.system.acl_adjustment_state(adjustment)
            live_plan = acl_plan_by_grant.get(grant)
            if state == "drift" or (
                state == "before" and (live_plan is None or live_plan.mask_adjustment != adjustment)
            ):
                raise InstallError("ACL mask ledger does not match the live ACL")
        previous_fingerprint = (
            previous_record.get("service_key_fingerprint") if previous_record is not None else None
        )
        fingerprint = previous_fingerprint if isinstance(previous_fingerprint, str) else "pending"
        if not service_key_missing:
            fingerprint = self.system.public_key_fingerprint()
        admission_enabled = self._record_flag(previous_record, "admission_enabled")
        maintenance_enabled = self._record_flag(previous_record, "maintenance_enabled")
        trust_requires_revocation = self._record_flag(
            previous_record, "trust_requires_revocation"
        ) or bool(
            previous_record is not None
            and (previous_record.get("installation_state") == "ready" or admission_enabled)
        )
        trust_ledger_migrated = bool(
            previous_record is not None
            and previous_record.get("schema_version") in {2, 3, 4}
            and self._record_flag(previous_record, "trust_ledger_migrated")
        )
        legacy_trust_source_sha = self._record_legacy_trust_source_sha(
            previous_record,
            trust_ledger_migrated=trust_ledger_migrated,
            trust_requires_revocation=trust_requires_revocation,
        )
        group_missing = not self.system.group_present(OPERATOR_GROUP)
        service_user_missing = not self.system.service_user_present()
        shared_work2_mount_ready = self.system.shared_work2_mount_ready()
        shared_worker_repo_identity = (
            None
            if service_user_missing or not shared_work2_mount_ready
            else self.system.shared_worker_repo_identity()
        )

        def record_value(
            state: str,
            *,
            admission: bool,
            maintenance: bool,
        ) -> dict[str, object]:
            value: dict[str, object] = {
                "schema_version": 4 if self.source_mode == "sealed-cumulative" else 3,
                "installation_state": state,
                "admission_enabled": admission,
                "maintenance_enabled": maintenance,
                "trust_requires_revocation": trust_requires_revocation,
                "trust_ledger_migrated": trust_ledger_migrated,
                "source_sha": source_sha,
                "smoke_on_behalf_team_id": team_id,
                "service_key_fingerprint": fingerprint,
                "added_operator_memberships": sorted(added_operator_memberships),
                "added_docker_membership": added_docker_membership,
                "enabled_linger": enabled_linger,
                "created_service_key": created_service_key,
                "added_acls": [
                    grant.to_dict()
                    for grant in sorted(
                        grants,
                        key=lambda item: (str(item.path), item.default),
                    )
                ],
            }
            if self.source_mode == "sealed-cumulative":
                if self.source_tree_sha is None or self.source_base_sha is None:
                    raise InstallError("sealed source record binding is unavailable")
                value.update(
                    {
                        "source_mode": "sealed-cumulative",
                        "source_tree_sha": self.source_tree_sha,
                        "source_base_sha": self.source_base_sha,
                    }
                )
            if mask_adjustments:
                value["acl_mask_adjustments"] = [
                    adjustment.to_dict()
                    for _, adjustment in sorted(
                        mask_adjustments.items(),
                        key=lambda item: (str(item[0].path), item[0].default),
                    )
                ]
            if legacy_trust_source_sha is not None:
                value["trust_legacy_source_sha"] = legacy_trust_source_sha
            if shared_worker_repo_identity is not None:
                value["shared_worker_repo"] = shared_worker_repo_identity
            return value

        def persist_record(
            state: str,
            *,
            admission: bool,
            maintenance: bool,
        ) -> bool:
            payload = (
                json.dumps(
                    record_value(
                        state,
                        admission=admission,
                        maintenance=maintenance,
                    ),
                    sort_keys=True,
                )
                + "\n"
            ).encode("utf-8")
            changed = self.filesystem.atomic_write(INSTALL_RECORD, payload, 0o600)
            owner_changed = self.system.install_owner(INSTALL_RECORD, "root", 0o600)
            return changed or owner_changed

        install_source_ready = self.system.install_source_ready(source_sha)
        service_directories_ready = not service_user_missing and all(
            self.system.owned_directory_ready(directory, owner=SERVICE_USER, mode=mode)
            for directory, mode in (
                (STATE_ROOT, 0o700),
                (GENERATED_ROOT, 0o700),
                (CANDIDATE_REPO, 0o700),
            )
        )
        if self.source_mode == "sealed-cumulative":
            candidate_ready = service_directories_ready and self.system.candidate_ready(
                source_sha,
                source_tree_sha=self.source_tree_sha,
                source_base_sha=self.source_base_sha,
            )
        else:
            candidate_ready = service_directories_ready and self.system.candidate_ready(source_sha)
        shared_worker_repo_ready = (
            not service_user_missing
            and shared_work2_mount_ready
            and self.system.shared_worker_repo_root_ready()
        )
        venv_lock_requires_hardening = self.system.venv_lock_requires_hardening()
        venv_ready = not venv_lock_requires_hardening and self.system.venv_ready()
        package_runtime_ready = (
            not service_user_missing and venv_ready and self.system.package_runtime_ready()
        )
        installed_files_ready = all(
            self.filesystem.file_matches(
                destination,
                payload,
                mode,
                expected_nlink=1 if destination == CONFIG_PATH else None,
            )
            and self.system.file_owner_ready(
                destination,
                owner=owner,
                group=group,
                mode=mode,
                nlink=1 if destination == CONFIG_PATH else None,
            )
            for destination, payload, mode, owner, group in installed_files
        )
        broker_runtime_ready = (
            package_runtime_ready and installed_files_ready and self.system.broker_runtime_ready()
        )
        runtime_ready = not service_user_missing and self.system.runtime_directory_ready()
        kubeconfig_ready = (
            not service_user_missing
            and self.filesystem.file_matches(KUBECONFIG_PATH, kubeconfig, 0o600)
            and self.system.file_owner_ready(
                KUBECONFIG_PATH,
                owner=SERVICE_USER,
                mode=0o600,
            )
        )
        sudoers_present = self.filesystem.exists(SUDOERS_PATH)
        existing_sudoers = (
            self.filesystem.read_bytes(SUDOERS_PATH, limit=1 << 20) if sudoers_present else None
        )
        sudoers_authority_safe = bool(
            existing_sudoers is not None
            and self.filesystem.file_matches(SUDOERS_PATH, existing_sudoers, 0o440)
            and self.system.file_owner_ready(SUDOERS_PATH, owner="root", mode=0o440)
        )
        sudoers_ready = sudoers_authority_safe and existing_sudoers == sudoers
        if sudoers_present and not sudoers_authority_safe:
            raise InstallError("existing staging rollout admission authority is unsafe")
        desired_ready_record = record_value(
            "ready",
            admission=True,
            maintenance=False,
        )
        install_attestation_payload = _runner_install_attestation_payload(
            desired_ready_record,
            attestation_assets,
        )
        install_attestation_ready = bool(
            self.filesystem.file_matches(
                INSTALL_ATTESTATION,
                install_attestation_payload,
                0o640,
                expected_nlink=1,
            )
            and self.system.file_owner_ready(
                INSTALL_ATTESTATION,
                owner="root",
                group=SERVICE_GROUP,
                mode=0o640,
                nlink=1,
            )
        )

        def restore_admission() -> None:
            if existing_sudoers is None:  # pragma: no cover - caller owns this invariant
                raise InstallError("previous staging rollout admission authority is unavailable")
            self.filesystem.atomic_write(SUDOERS_PATH, existing_sudoers, 0o440)
            self.system.install_owner(SUDOERS_PATH, "root", 0o440)

        transaction_active = (
            previous_record
            != record_value(
                "ready",
                admission=True,
                maintenance=False,
            )
            or not install_attestation_ready
        )
        requires_mutation = bool(
            changes
            or transaction_active
            or not install_source_ready
            or group_missing
            or service_user_missing
            or missing_operators
            or docker_missing
            or linger_missing
            or service_key_missing
            or acl_plans
            or not service_directories_ready
            or not shared_work2_mount_ready
            or not shared_worker_repo_ready
            or generated_env_error is not None
            or not generated_env_templates_ready
            or not candidate_ready
            or venv_lock_requires_hardening
            or not venv_ready
            or not package_runtime_ready
            or not broker_runtime_ready
            or not installed_files_ready
            or not runtime_ready
            or not kubeconfig_ready
            or not sudoers_ready
            or not install_attestation_ready
        )
        transaction_active = transaction_active or requires_mutation

        if requires_mutation and (admission_enabled or sudoers_present):
            admission_was_present = self.filesystem.remove(SUDOERS_PATH)
            try:
                self.system.begin_maintenance()
            except Exception:
                if admission_was_present:
                    restore_admission()
                raise
            status = self.system.active_status()
            if status in {"pending", "running", "cancel_requested", "busy", "unknown"}:
                if admission_was_present:
                    restore_admission()
                self.system.end_maintenance()
                if status == "unknown":
                    raise InstallError("cannot prove staging rollout is inactive")
                raise InstallError("refusing update while a staging rollout is active")
            admission_enabled = False
            maintenance_enabled = True
        elif requires_mutation and maintenance_enabled:
            self.filesystem.remove(SUDOERS_PATH)
            self.system.begin_maintenance()
            admission_enabled = False

        if requires_mutation:
            persist_record(
                "installing",
                admission=admission_enabled,
                maintenance=maintenance_enabled,
            )
            if self.system.ensure_install_source_checkout(source_sha):
                changes.append("install-source")

        if self.system.ensure_group(OPERATOR_GROUP):
            changes.append(f"group:{OPERATOR_GROUP}")
        if self.system.ensure_service_user():
            changes.append(f"user:{SERVICE_USER}")
        for username in OPERATORS:
            if self.system.ensure_operator_membership(username):
                added_operator_memberships.add(username)
                changes.append(f"operator:{username}")
        if self.system.ensure_docker_membership():
            added_docker_membership = True
            changes.append("service-group:docker")

        if not self.system.shared_work2_mount_ready():
            if self.system.ensure_root_directory(SHARED_WORK2_MOUNT_POINT, mode=0o755):
                changes.append(f"directory:{SHARED_WORK2_MOUNT_POINT}")
        mount_payload = self._asset(SHARED_WORK2_MOUNT_UNIT)
        if self.filesystem.atomic_write(SHARED_WORK2_MOUNT_UNIT_PATH, mount_payload, 0o644):
            changes.append(f"file:{SHARED_WORK2_MOUNT_UNIT_PATH}")
        if self.system.install_owner(SHARED_WORK2_MOUNT_UNIT_PATH, "root", 0o644, group="root"):
            changes.append(f"ownership:{SHARED_WORK2_MOUNT_UNIT_PATH}")
        if self.system.ensure_shared_work2_mount():
            changes.append(f"mount:{SHARED_WORK2_MOUNT_POINT}")
        shared_work2_mount_ready = True

        if self.system.ensure_shared_worker_repo_root():
            changes.append(f"directory:{SHARED_WORKER_REPO_ROOT}")
        shared_worker_repo_identity = self.system.shared_worker_repo_identity()

        for directory, mode in (
            (STATE_ROOT, 0o700),
            (GENERATED_ROOT, 0o700),
            (CANDIDATE_REPO, 0o700),
        ):
            if self.system.ensure_owned_directory(directory, owner=SERVICE_USER, mode=mode):
                changes.append(f"directory:{directory}")

        if generated_env_error is not None:
            raise generated_env_error
        if not generated_env_templates:
            worker_env_payload = self.filesystem.legacy_gb10_env_template_payload()
            if self.filesystem.atomic_write(
                GENERATED_GB10_ENV_SEED,
                worker_env_payload,
                0o600,
                expected_nlink=1,
            ):
                changes.append(f"worker-env-template:{GENERATED_GB10_ENV_SEED}")
            generated_env_templates = (GENERATED_GB10_ENV_SEED,)
        for path in generated_env_templates:
            if self.system.install_owner(path, SERVICE_USER, 0o600):
                changes.append(f"ownership:{path}")
        if not all(
            self.system.file_owner_ready(
                path,
                owner=SERVICE_USER,
                mode=0o600,
                nlink=1,
            )
            for path in self.filesystem.generated_gb10_env_templates()
        ):
            raise InstallError("generated GB10 worker env template authority is unsafe")

        if self.source_mode == "sealed-cumulative":
            candidate_changed = self.system.ensure_candidate(
                source_sha,
                refresh=refresh_runtime,
                source_tree_sha=self.source_tree_sha,
                source_base_sha=self.source_base_sha,
            )
        else:
            candidate_changed = self.system.ensure_candidate(
                source_sha,
                refresh=refresh_runtime,
            )
        if candidate_changed:
            changes.append("candidate-checkout")
        if venv_lock_requires_hardening:
            self.system.harden_venv_lock()
            changes.append("venv-lock")
        if (
            refresh_runtime
            or not self.system.venv_ready()
            or not self.system.package_runtime_ready()
        ):
            self.system.sync_venv(self.source_root)
            changes.append("venv")
        if self.system.ensure_service_key():
            created_service_key = True
            changes.append("service-key")
        if self.system.ensure_linger():
            enabled_linger = True
            changes.append("linger")
        fingerprint = self.system.public_key_fingerprint()
        ledger_prepared = not trust_ledger_migrated
        if trust_ledger_migrated:
            ledger_mode = "existing"
        elif trust_requires_revocation:
            ledger_mode = "legacy"
        else:
            ledger_mode = "fresh"
        if self.source_root is None:  # pragma: no cover - prepare_install_source owns this
            raise InstallError("root installation source is unavailable")
        previous_source_sha = legacy_trust_source_sha if ledger_mode == "legacy" else None
        if ledger_mode == "legacy" and previous_source_sha is None:
            raise InstallError("legacy GB10 trust source binding is unavailable")
        self.system.prepare_gb10_trust_ledger(
            self.source_root,
            mode=ledger_mode,
            previous_source_sha=previous_source_sha,
        )
        trust_ledger_migrated = True
        legacy_trust_source_sha = None
        if ledger_prepared:
            changes.append("trust-ledger")
            persist_record(
                "installing",
                admission=admission_enabled,
                maintenance=maintenance_enabled,
            )

        for destination, payload, mode, owner, group in installed_files:
            if self.filesystem.atomic_write(
                destination,
                payload,
                mode,
                expected_nlink=1 if destination == CONFIG_PATH else None,
            ):
                changes.append(f"file:{destination}")
            if self.system.install_owner(destination, owner, mode, group=group):
                changes.append(f"ownership:{destination}")
        if not self.system.broker_runtime_ready():
            raise InstallError("installed broker config probe failed")
        if self.system.create_runtime_directory():
            changes.append(f"directory:{RUNTIME_ROOT}")

        if self.filesystem.atomic_write(KUBECONFIG_PATH, kubeconfig, 0o600):
            changes.append("kubeconfig")
        if self.system.install_owner(KUBECONFIG_PATH, SERVICE_USER, 0o600):
            changes.append("ownership:kubeconfig")

        self.system.verify_user_manager()

        if acl_plans:
            for plan in acl_plans:
                self.system.apply_acl(plan)
            changes.append("acls")

        transaction_active = transaction_active or bool(changes)
        if transaction_active:
            if not maintenance_enabled:
                self.system.begin_maintenance()
                maintenance_enabled = True
            persist_record(
                "installing",
                admission=False,
                maintenance=maintenance_enabled,
            )
        trust_requires_revocation = True
        ready_record = record_value(
            "ready",
            admission=True,
            maintenance=False,
        )
        install_attestation_payload = _runner_install_attestation_payload(
            ready_record,
            attestation_assets,
        )
        if self.filesystem.atomic_write(
            INSTALL_ATTESTATION,
            install_attestation_payload,
            0o640,
            expected_nlink=1,
        ):
            changes.append("install-attestation")
        if self.system.install_owner(
            INSTALL_ATTESTATION,
            "root",
            0o640,
            group=SERVICE_GROUP,
        ):
            changes.append("ownership:install-attestation")
        if persist_record(
            "ready",
            admission=True,
            maintenance=False,
        ):
            changes.append("install-record")
        if self.filesystem.atomic_write(SUDOERS_PATH, sudoers, 0o440):
            changes.append(f"file:{SUDOERS_PATH}")
        if self.system.install_owner(SUDOERS_PATH, "root", 0o440):
            changes.append(f"ownership:{SUDOERS_PATH}")
        if transaction_active:
            self.system.end_maintenance()
            maintenance_enabled = False
        trust_ready = self.system.gb10_trust_ready()
        if trust_ready and changes:
            self.system.run_post_install_dry_run()
        return {
            "ok": True,
            "changed": changes,
            "service_key_fingerprint": fingerprint,
            "post_install_check": "passed" if trust_ready else "awaiting-gb10-trust",
        }

    def check(self, *, _lock_held: bool = False) -> dict[str, object]:
        if not _lock_held:
            with self.system.trust_lifecycle_lock():
                return self.check(_lock_held=True)
        self.system.validate_install_record_authority(allow_absent=True)
        record = self.filesystem.load_install_record()
        if record is None:
            return {"ok": False, "failures": [str(INSTALL_RECORD)]}
        if (
            record.get("installation_state") != "ready"
            or not self._record_flag(record, "admission_enabled")
            or self._record_flag(record, "maintenance_enabled")
        ):
            return {"ok": False, "failures": ["installation-incomplete"]}
        trust_requires_revocation = self._record_flag(record, "trust_requires_revocation")
        trust_ledger_migrated = bool(
            record.get("schema_version") in {2, 3, 4}
            and self._record_flag(record, "trust_ledger_migrated")
        )
        self._record_legacy_trust_source_sha(
            record,
            trust_ledger_migrated=trust_ledger_migrated,
            trust_requires_revocation=trust_requires_revocation,
        )
        if not trust_requires_revocation or not trust_ledger_migrated:
            return {"ok": False, "failures": ["trust-ledger-incomplete"]}
        mask_adjustments = self._record_mask_adjustments(record)
        grants = self._record_grants(record)
        self._validate_acl_ledgers(grants, mask_adjustments)
        self._bind_existing_source(record)
        expected = (
            (CLIENT_PATH, self._asset("loom-staging-rollout"), 0o755),
            (BROKER_PATH, self._asset("loom-staging-rollout-broker"), 0o755),
            (REHEARSAL_PATH, self._asset("loom-staging-rollout-rehearsal"), 0o755),
            (
                TRUST_TOOL_PATH,
                self._source_file("scripts/ops/staging_rollout_gb10_trust.py"),
                0o755,
            ),
            (SUDOERS_PATH, self._asset("loom-staging-rollout.sudoers"), 0o440),
            (TMPFILES_PATH, self._asset("loom-staging-rollout.tmpfiles"), 0o644),
            (
                KNOWN_HOSTS_PATH,
                self._source_file("deploy/worker-pools/gb10/known_hosts"),
                0o644,
            ),
            (
                SHARED_WORK2_MOUNT_UNIT_PATH,
                self._asset(SHARED_WORK2_MOUNT_UNIT),
                0o644,
            ),
        )
        failures = [
            str(path)
            for path, payload, mode in expected
            if not self.filesystem.file_matches(path, payload, mode)
        ]
        config_payload: bytes | None = None
        if not self.filesystem.exists(CONFIG_PATH):
            failures.append(str(CONFIG_PATH))
        else:
            team_id = record.get("smoke_on_behalf_team_id")
            try:
                if not isinstance(team_id, str):
                    raise InstallError("install record team ID is invalid")
                config_payload = self.filesystem.read_bytes(CONFIG_PATH, limit=1 << 20)
                record_source_sha = record.get("source_sha")
                record_source_tree_sha = record.get("source_tree_sha")
                record_source_base_sha = record.get("source_base_sha")
                self._validate_rendered_config(
                    config_payload,
                    team_id,
                    source_sha=(
                        record_source_sha
                        if record.get("source_mode") == "sealed-cumulative"
                        and isinstance(record_source_sha, str)
                        else None
                    ),
                    source_tree_sha=(
                        record_source_tree_sha
                        if isinstance(record_source_tree_sha, str)
                        else None
                    ),
                    source_base_sha=(
                        record_source_base_sha
                        if isinstance(record_source_base_sha, str)
                        else None
                    ),
                )
            except InstallError:
                failures.append("rendered-config")
        if config_payload is None:
            failures.append(str(INSTALL_ATTESTATION))
        else:
            attestation_assets = {
                "broker": self._asset("loom-staging-rollout-broker"),
                "client": self._asset("loom-staging-rollout"),
                "config": config_payload,
                "gb10-known-hosts": self._source_file("deploy/worker-pools/gb10/known_hosts"),
                "gb10-trust-tool": self._source_file("scripts/ops/staging_rollout_gb10_trust.py"),
                "rehearsal-helper": self._asset("loom-staging-rollout-rehearsal"),
                "shared-work2-mount-unit": self._asset(SHARED_WORK2_MOUNT_UNIT),
                "tmpfiles": self._asset("loom-staging-rollout.tmpfiles"),
            }
            expected_attestation = _runner_install_attestation_payload(
                record,
                attestation_assets,
            )
            if not (
                self.filesystem.file_matches(
                    INSTALL_ATTESTATION,
                    expected_attestation,
                    0o640,
                    expected_nlink=1,
                )
                and self.system.file_owner_ready(
                    INSTALL_ATTESTATION,
                    owner="root",
                    group=SERVICE_GROUP,
                    mode=0o640,
                    nlink=1,
                )
            ):
                failures.append(str(INSTALL_ATTESTATION))
        if not self.filesystem.exists(KUBECONFIG_PATH):
            failures.append(str(KUBECONFIG_PATH))
        if not self.filesystem.exists(SERVICE_KEY):
            failures.append(str(SERVICE_KEY))
        generated_env_templates = self.filesystem.generated_gb10_env_templates()
        if not generated_env_templates or not all(
            self.system.file_owner_ready(
                path,
                owner=SERVICE_USER,
                mode=0o600,
                nlink=1,
            )
            for path in generated_env_templates
        ):
            failures.append("generated-gb10-worker-env-template")
        source_sha = record.get("source_sha")
        if not isinstance(source_sha, str):  # _bind_existing_source has already validated this
            raise InstallError("install record source SHA is invalid")
        if record.get("source_mode") == "sealed-cumulative":
            source_tree_sha = record.get("source_tree_sha")
            source_base_sha = record.get("source_base_sha")
            if not isinstance(source_tree_sha, str) or not isinstance(source_base_sha, str):
                raise InstallError("install record sealed source binding is invalid")
            failures.extend(
                self.system.check_runtime(
                    source_sha,
                    source_tree_sha=source_tree_sha,
                    source_base_sha=source_base_sha,
                )
            )
        else:
            failures.extend(self.system.check_runtime(source_sha))
        for grant, adjustment in mask_adjustments.items():
            if self.system.acl_adjustment_state(adjustment) != "after":
                namespace = "default" if grant.default else "access"
                failures.append(f"acl-mask:{namespace}:{grant.path}")
        return {"ok": not failures, "failures": failures}

    def uninstall(
        self,
        *,
        retain_ledger: bool,
        _lock_held: bool = False,
    ) -> dict[str, object]:
        if not _lock_held:
            if self.euid != 0:
                raise InstallError("uninstall requires root")
            with self.system.trust_lifecycle_lock():
                return self.uninstall(retain_ledger=retain_ledger, _lock_held=True)
        if self.euid != 0:
            raise InstallError("uninstall requires root")
        if not retain_ledger:
            raise InstallError("uninstall requires --retain-ledger")
        self.system.validate_install_record_authority(allow_absent=False)
        record = self.filesystem.load_install_record()
        if record is None:
            raise InstallError("uninstall requires a valid install record")
        self._bind_existing_source(record)
        grants = self._record_grants(record)
        mask_adjustments = self._record_mask_adjustments(record)
        self._validate_acl_ledgers(grants, mask_adjustments)
        enabled_linger = self._record_flag(record, "enabled_linger")
        added_operator_memberships = self._record_operator_memberships(record)
        added_docker_membership = self._record_flag(record, "added_docker_membership")
        created_service_key = self._record_flag(record, "created_service_key")
        maintenance_enabled = self._record_flag(record, "maintenance_enabled")

        resuming_uninstall = record.get("installation_state") == "uninstalling"
        admission_enabled = self._record_flag(record, "admission_enabled")
        recorded_trust_revocation = self._record_flag(record, "trust_requires_revocation")
        trust_requires_revocation = recorded_trust_revocation or bool(
            not resuming_uninstall
            and (record.get("installation_state") == "ready" or admission_enabled)
        )
        trust_ledger_migrated = bool(
            record.get("schema_version") in {2, 3, 4}
            and self._record_flag(record, "trust_ledger_migrated")
        )
        trust_ledger_removed = (
            self._record_flag(record, "trust_ledger_removed")
            if "trust_ledger_removed" in record
            else False
        )
        self._record_legacy_trust_source_sha(
            record,
            trust_ledger_migrated=trust_ledger_migrated,
            trust_requires_revocation=trust_requires_revocation,
        )
        uninstall_state_fields = {
            "admission_enabled",
            "maintenance_enabled",
            "trust_ledger_migrated",
            "trust_ledger_removed",
            "trust_requires_revocation",
        }
        if resuming_uninstall and (
            record.get("schema_version") not in {2, 3, 4}
            or not uninstall_state_fields.issubset(record)
            or admission_enabled
            or trust_requires_revocation
            or (not trust_ledger_migrated and not trust_ledger_removed)
        ):
            raise InstallError("interrupted uninstall record is invalid")
        if not resuming_uninstall and trust_ledger_removed:
            raise InstallError("install record trust-ledger state is invalid")
        if trust_requires_revocation and not trust_ledger_migrated:
            raise InstallError("uninstall requires a completed GB10 trust ledger migration")
        expected_fingerprint = (
            self._record_service_key_fingerprint(record)
            if trust_ledger_migrated or trust_requires_revocation
            else None
        )
        if trust_ledger_migrated and not trust_ledger_removed and expected_fingerprint is None:
            raise InstallError("install record service-key fingerprint is invalid")
        if trust_requires_revocation:
            if expected_fingerprint is None:  # pragma: no cover - validated above
                raise InstallError("install record service-key fingerprint is invalid")
            self.system.validate_service_key_continuity(expected_fingerprint)
        admission_was_present = self.filesystem.remove(SUDOERS_PATH)
        if admission_was_present or admission_enabled:
            try:
                self.system.begin_maintenance()
            except Exception:
                if admission_was_present:
                    sudoers = self._asset("loom-staging-rollout.sudoers")
                    self.filesystem.atomic_write(SUDOERS_PATH, sudoers, 0o440)
                    self.system.install_owner(SUDOERS_PATH, "root", 0o440)
                raise
            status = self.system.active_status()
            if status in {"pending", "running", "cancel_requested", "busy", "unknown"}:
                if admission_was_present:
                    sudoers = self._asset("loom-staging-rollout.sudoers")
                    self.filesystem.atomic_write(SUDOERS_PATH, sudoers, 0o440)
                    self.system.install_owner(SUDOERS_PATH, "root", 0o440)
                self.system.end_maintenance()
                if status == "unknown":
                    raise InstallError("cannot prove staging rollout is inactive")
                raise InstallError("refusing uninstall while a rollout is active")
            maintenance_enabled = True
        if trust_requires_revocation:
            self.system.revoke_gb10_trust()
            self.system.require_gb10_revocation_complete()
        ledger_path = self.filesystem.path(TRUST_REVOCATION_LEDGER)
        ledger_tombstone = self.filesystem.path(TRUST_REVOCATION_TOMBSTONE)
        ledger_present = ledger_path.exists() or ledger_path.is_symlink()
        tombstone_present = ledger_tombstone.exists() or ledger_tombstone.is_symlink()
        ledger_artifact_present = ledger_present or tombstone_present
        if trust_ledger_removed and ledger_artifact_present:
            raise InstallError("finalized GB10 trust revocation ledger reappeared")
        if trust_ledger_migrated and not trust_ledger_removed and ledger_artifact_present:
            if expected_fingerprint is None:  # pragma: no cover - validated above
                raise InstallError("install record service-key fingerprint is invalid")
            self.filesystem.validate_trust_ledger_for_removal(
                expected_fingerprint=expected_fingerprint
            )
        elif trust_ledger_migrated and not trust_ledger_removed and not resuming_uninstall:
            raise InstallError("GB10 trust revocation ledger disappeared before finalization")

        def persist_uninstall_record() -> None:
            payload = (json.dumps(record, sort_keys=True) + "\n").encode("utf-8")
            self.filesystem.atomic_write(INSTALL_RECORD, payload, 0o600)
            self.system.install_owner(INSTALL_RECORD, "root", 0o600)

        if not resuming_uninstall:
            record = dict(record)
            record.update(
                {
                    "schema_version": (
                        4 if record.get("source_mode") == "sealed-cumulative" else 3
                    ),
                    "installation_state": "uninstalling",
                    "admission_enabled": False,
                    "maintenance_enabled": maintenance_enabled,
                    "trust_requires_revocation": False,
                    "trust_ledger_migrated": trust_ledger_migrated,
                    "trust_ledger_removed": not trust_ledger_migrated,
                }
            )
            trust_ledger_removed = not trust_ledger_migrated
            persist_uninstall_record()

        acl_scopes = grants | set(mask_adjustments)
        removed: list[str] = []
        for grant in reversed(
            sorted(
                acl_scopes,
                key=lambda item: (len(item.path.parts), item.default),
            )
        ):
            self.system.remove_acl(
                grant,
                mask_adjustments.get(grant),
                remove_service_entry=grant in grants,
            )
        if enabled_linger:
            self.system.disable_linger()
        for username in OPERATORS:
            if username not in added_operator_memberships:
                continue
            self.system.remove_operator_membership(username)
        if added_docker_membership:
            self.system.remove_docker_membership()
        mount_unit_present = self.filesystem.exists(SHARED_WORK2_MOUNT_UNIT_PATH)
        if mount_unit_present:
            self.system.disable_shared_work2_mount()
        removable_files = [
            CLIENT_PATH,
            BROKER_PATH,
            REHEARSAL_PATH,
            TRUST_TOOL_PATH,
            CONFIG_PATH,
            INSTALL_ATTESTATION,
            KUBECONFIG_PATH,
            TMPFILES_PATH,
            KNOWN_HOSTS_PATH,
            SHARED_WORK2_MOUNT_UNIT_PATH,
        ]
        if created_service_key:
            removable_files.extend((SERVICE_KEY, Path(str(SERVICE_KEY) + ".pub")))
        for path in removable_files:
            if self.filesystem.remove(path):
                removed.append(str(path))
        if mount_unit_present:
            self.system.reload_systemd()
        if self.filesystem.remove_tree(GENERATED_ROOT):
            removed.append(str(GENERATED_ROOT))
        if self.filesystem.remove_tree(RUNTIME_ROOT):
            removed.append(str(RUNTIME_ROOT))
        if trust_ledger_migrated and not trust_ledger_removed:
            if expected_fingerprint is None:  # pragma: no cover - validated above
                raise InstallError("install record service-key fingerprint is invalid")
            ledger_artifact_present = (
                ledger_path.exists()
                or ledger_path.is_symlink()
                or ledger_tombstone.exists()
                or ledger_tombstone.is_symlink()
            )
            if ledger_artifact_present and self.filesystem.remove_validated_trust_ledger(
                expected_fingerprint=expected_fingerprint
            ):
                removed.append(str(TRUST_REVOCATION_LEDGER))
            trust_ledger_removed = True
            record["trust_ledger_removed"] = True
            persist_uninstall_record()
        if self.filesystem.remove(INSTALL_RECORD):
            removed.append(str(INSTALL_RECORD))
        return {
            "ok": True,
            "removed": removed,
            "retained": [str(STATE_ROOT), "/data/loom-staging/rollouts"],
        }


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="staging_rollout_host.py", allow_abbrev=False)
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("plan", allow_abbrev=False)
    install = commands.add_parser("install", allow_abbrev=False)
    install.add_argument("--smoke-on-behalf-team-id", required=True)
    install.add_argument(
        "--source-mode",
        choices=("merged-dev", "sealed-cumulative"),
        default="merged-dev",
    )
    install.add_argument("--sealed-source-sha")
    install.add_argument("--sealed-source-tree")
    install.add_argument("--sealed-approved-base-sha")
    check = commands.add_parser("check", allow_abbrev=False)
    check.add_argument("--format", choices=("json", "text"), default="text")
    uninstall = commands.add_parser("uninstall", allow_abbrev=False)
    uninstall.add_argument("--retain-ledger", action="store_true", required=True)
    return parser


def main(
    argv: Sequence[str] | None = None,
    *,
    installer: HostInstaller | None = None,
) -> int:
    try:
        args = _parser().parse_args(list(argv) if argv is not None else None)
        active = installer or HostInstaller(
            filesystem=LocalFilesystem(),
            system=HostSystem(SubprocessRunner()),
            euid=os.geteuid(),
        )
        if args.command == "plan":
            result = active.plan()
        elif args.command == "install":
            sealed_values = (
                args.sealed_source_sha,
                args.sealed_source_tree,
                args.sealed_approved_base_sha,
            )
            if args.source_mode == "merged-dev":
                if any(value is not None for value in sealed_values):
                    raise InstallError("merged-dev install rejects sealed source arguments")
                sealed_source = None
            else:
                if any(value is None for value in sealed_values):
                    raise InstallError("sealed-cumulative install requires exact SHA/tree/base")
                sealed_source = SealedSource(
                    path=REPO_ROOT,
                    commit_sha=args.sealed_source_sha,
                    tree_sha=args.sealed_source_tree,
                    base_sha=args.sealed_approved_base_sha,
                )
            result = active.install(
                args.smoke_on_behalf_team_id,
                sealed_source=sealed_source,
            )
        elif args.command == "check":
            result = active.check()
        elif args.command == "uninstall":
            result = active.uninstall(retain_ledger=bool(args.retain_ledger))
        else:  # pragma: no cover - argparse owns the command set
            return 2
        if args.command == "check" and args.format == "text":
            sys.stdout.write("ok\n" if result["ok"] else "failed\n")
        else:
            sys.stdout.write(json.dumps(result, sort_keys=True) + "\n")
        return 0 if result.get("ok", True) else 1
    except (InstallError, SealedSourceError) as exc:
        sys.stderr.write(json.dumps({"error": str(exc)}, sort_keys=True) + "\n")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
