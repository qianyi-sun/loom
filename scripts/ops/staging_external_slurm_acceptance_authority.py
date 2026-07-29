#!/bin/false
"""Root-installed prepare/probe/activate authority for staging GB10 Slurm.

This program is intentionally independent from the candidate environment-state
profile. The profile can require the authority, but only this fixed program,
fixed configuration, and persistently bootstrapped root-owned Ed25519 key may
produce acceptance.
"""

from __future__ import annotations

import argparse
import base64
import binascii
import ctypes
import fcntl
import grp
import hashlib
import importlib.util
import json
import os
import pwd
import re
import secrets
import socket
import stat
import subprocess
import sys
from collections.abc import Sequence
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Any, cast

if TYPE_CHECKING:
    from loom_cli.external_slurm_acceptance import ExternalSlurmAuthorityConfig

_FIXED_INSTALL_ROOT = Path("/usr/local/lib/loom-staging-external-slurm-authority")
_FIXED_SOURCE_SCRIPT = _FIXED_INSTALL_ROOT / "staging_external_slurm_acceptance_authority.py"
_FIXED_CONSUMER_ROOT = _FIXED_INSTALL_ROOT
_FIXED_CONSUMER_MODULE = _FIXED_CONSUMER_ROOT / "loom_cli/external_slurm_acceptance.py"
_INSTALLED_PROGRAM = Path("/usr/local/libexec/loom-staging-external-slurm-authority")
FIXED_CANDIDATE_RUNTIME_TEMPLATE = (
    "/opt/loom-staging-runner/candidates/{candidate_sha}/venv/bin/python"
)
REQUIRED_INSTALLATION_ASSETS = (
    "scripts/ops/staging_external_slurm_acceptance_authority.py",
    "src/loom_cli/external_slurm_acceptance.py",
    "deploy/developer-sandboxes/staging-external-slurm-authority.toml",
    "deploy/developer-sandboxes/loom-staging-external-slurm-authority.service",
    "deploy/developer-sandboxes/loom-staging-external-slurm-authority.sudoers",
    "deploy/developer-sandboxes/loom-staging-external-slurm-authority.wrapper",
    r"deploy/developer-sandboxes/srv-loom-staging\x2dshared.mount",
    "deploy/developer-sandboxes/loom-staging-shared.tmpfiles.conf",
)

# The candidate-bound root wrapper invokes this source with the exact candidate
# venv's Python in isolated mode. Direct execution is disabled by the shebang.
# The compatibility branch below supports an already-installed legacy copy only
# long enough for its digest to be checked against the fixed source.
_RUNNING_FIXED_SOURCE = Path(__file__).resolve() == _FIXED_SOURCE_SCRIPT
if _RUNNING_FIXED_SOURCE:
    for fixed_path in (_FIXED_SOURCE_SCRIPT, _FIXED_CONSUMER_MODULE):
        metadata = fixed_path.lstat()
        if (
            not stat.S_ISREG(metadata.st_mode)
            or stat.S_ISLNK(metadata.st_mode)
            or metadata.st_uid != 0
            or metadata.st_gid != 0
            or metadata.st_nlink != 1
            or metadata.st_mode & 0o022
        ):
            raise RuntimeError("fixed staging authority source runtime is unsafe")
        for parent in fixed_path.parents:
            if parent == Path("/"):
                break
            parent_metadata = parent.lstat()
            if (
                not stat.S_ISDIR(parent_metadata.st_mode)
                or stat.S_ISLNK(parent_metadata.st_mode)
                or parent_metadata.st_uid != 0
                or parent_metadata.st_gid != 0
                or parent_metadata.st_mode & 0o022
            ):
                raise RuntimeError("fixed staging authority source parents are unsafe")
    if not _INSTALLED_PROGRAM.is_file() or _INSTALLED_PROGRAM.is_symlink():
        raise RuntimeError("installed staging authority wrapper is unsafe")

from cryptography.exceptions import InvalidSignature  # noqa: E402
from cryptography.hazmat.primitives import serialization  # noqa: E402
from cryptography.hazmat.primitives.asymmetric.ed25519 import (  # noqa: E402
    Ed25519PrivateKey,
    Ed25519PublicKey,
)

if _RUNNING_FIXED_SOURCE:
    _consumer_spec = importlib.util.spec_from_file_location(
        "_loom_fixed_external_slurm_acceptance",
        _FIXED_CONSUMER_MODULE,
    )
    if _consumer_spec is None or _consumer_spec.loader is None:
        raise RuntimeError("fixed staging authority consumer cannot be loaded")
    _consumer = importlib.util.module_from_spec(_consumer_spec)
    sys.modules[_consumer_spec.name] = _consumer
    _consumer_spec.loader.exec_module(_consumer)
else:
    from loom_cli import external_slurm_acceptance as _consumer

DEFAULT_CONFIG_PATH = _consumer.DEFAULT_CONFIG_PATH
ExternalSlurmAcceptanceError = _consumer.ExternalSlurmAcceptanceError
authority_paths = _consumer.authority_paths
canonical_json_bytes = _consumer.canonical_json_bytes
load_authority_config = _consumer.load_authority_config
validate_authority_payload = _consumer.validate_authority_payload
verify_authority = _consumer.verify_authority

_MAX_PROBE_BYTES = 2 * 1024 * 1024
_MAX_KEY_BYTES = 16 * 1024
_OBJECT_ID_RE = re.compile(r"[0-9a-f]{40}\Z")
_DIGEST_RE = re.compile(r"[0-9a-f]{64}\Z")
_INFRASTRUCTURE_RECEIPT_ROOT = Path(
    "/var/lib/loom-developer-sandbox-node-authority/staging-infrastructure"
)
_INFRASTRUCTURE_MAX_AGE_SECONDS = 3600
_INFRASTRUCTURE_CONVERGE_TRANSPORT_TIMEOUT_SECONDS = 3660
_INFRASTRUCTURE_SOURCE_CONTROLLER = "oldlab-2"
_INFRASTRUCTURE_SOURCE_CONTROLLER_HOST = "trt-eai-oldlab-2"
_INFRASTRUCTURE_FIELDS = frozenset(
    {
        "schema_version",
        "kind",
        "candidate_sha",
        "candidate_tree",
        "generation",
        "convergence_id",
        "requested_at",
        "request_sha256",
        "source_controller",
        "source_controller_host",
        "created_at",
        "expires_at",
        "source_bootstrap",
        "accounting",
        "node_bootstraps",
        "mount_contract",
        "result",
    }
)
_INFRASTRUCTURE_TRANSPORT_FIELDS = frozenset(
    {
        "schema_version",
        "request_id",
        "action",
        "node",
        "domain",
        "sandbox",
        "candidate_sha",
        "candidate_tree",
        "payload_sha256",
        "result_sha256",
        "inner_receipt",
        "completed_at",
        "status",
    }
)
_INFRASTRUCTURE_MOUNT_FIELDS = frozenset(
    {
        "source",
        "target",
        "filesystem_type",
        "repository_root",
        "worker_env_root",
        "result_root",
        "root_uid",
        "root_gid",
        "root_mode",
        "repository_root_mode",
        "worker_env_root_mode",
        "result_uid",
        "result_gid",
        "result_root_mode",
    }
)
_MOUNT_UNIT_PATH = Path(
    r"/etc/systemd/system/srv-loom-staging\x2dshared.mount",
)
_MOUNT_UNIT = b"""[Unit]
Description=Loom staging shared candidate and result namespace
After=network-online.target
Wants=network-online.target
Before=loom-staging-external-slurm-authority.service

[Mount]
What=192.168.20.12:/shared_work2/loom/staging
Where=/srv/loom/staging-shared
Type=nfs4
Options=rw,hard,nosuid,nodev,noexec,_netdev
TimeoutSec=30

[Install]
WantedBy=multi-user.target
"""
_PREPARED_FIELDS = frozenset(
    {
        "schema_version",
        "kind",
        "candidate_sha",
        "candidate_tree",
        "image_tag",
        "profile_sha256",
        "repository",
        "worker_env",
        "service_identity",
        "supervisor",
        "infrastructure_sha256",
        "prepared_at",
    }
)


def _timestamp(value: datetime | None = None) -> str:
    return (value or datetime.now(UTC)).astimezone(UTC).isoformat().replace("+00:00", "Z")


def _run(
    argv: Sequence[str],
    *,
    timeout: int = 60,
    input_text: str | None = None,
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        list(argv),
        check=False,
        capture_output=True,
        text=True,
        input=input_text,
        timeout=timeout,
        env={
            "PATH": "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
            "GIT_CONFIG_NOSYSTEM": "1",
            "GIT_CONFIG_GLOBAL": "/dev/null",
            "GIT_OPTIONAL_LOCKS": "0",
            "GIT_TERMINAL_PROMPT": "0",
        },
    )


def _require_root() -> None:
    if os.geteuid() != 0:
        raise ExternalSlurmAcceptanceError("authority mutation requires root")


def _require_source_host(config: ExternalSlurmAuthorityConfig) -> None:
    observed = {socket.gethostname().lower(), socket.getfqdn().lower()}
    observed |= {value.split(".", 1)[0] for value in tuple(observed)}
    expected = {
        config.source_host.lower(),
        config.source_host.lower().split(".", 1)[0],
    }
    if not observed & expected:
        raise ExternalSlurmAcceptanceError("authority must run on its fixed source host")


def _sha256_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _candidate_paths(
    config: ExternalSlurmAuthorityConfig,
    *,
    image_tag: str,
) -> tuple[Path, Path]:
    if re.fullmatch(r"staging-[0-9a-f]{7}", image_tag) is None:
        raise ExternalSlurmAcceptanceError("image_tag is invalid")
    repository = Path(config.repository_template.format(image_tag=image_tag))
    worker_env = Path(config.worker_env_template.format(image_tag=image_tag))
    return repository, worker_env


def _producer_candidate_paths(
    config: ExternalSlurmAuthorityConfig,
    *,
    image_tag: str,
) -> tuple[Path, Path]:
    if re.fullmatch(r"staging-[0-9a-f]{7}", image_tag) is None:
        raise ExternalSlurmAcceptanceError("image_tag is invalid")
    return (
        Path(config.producer_repository_template.format(image_tag=image_tag)),
        Path(config.producer_worker_env_template.format(image_tag=image_tag)),
    )


def _validate_candidate_identity(
    *,
    candidate_sha: str,
    candidate_tree: str,
    image_tag: str,
) -> None:
    if (
        _OBJECT_ID_RE.fullmatch(candidate_sha) is None
        or _OBJECT_ID_RE.fullmatch(candidate_tree) is None
        or image_tag != f"staging-{candidate_sha[:7]}"
    ):
        raise ExternalSlurmAcceptanceError("candidate SHA, tree, and image tag binding is invalid")


def _verify_identity(
    *,
    user_name: str,
    group_name: str,
    uid: int,
    gid: int,
    home: Path,
    shell: Path,
    supplementary_groups: Sequence[str] = (),
    require_exact_groups: bool = False,
) -> dict[str, Any]:
    try:
        user = pwd.getpwnam(user_name)
        primary_group = grp.getgrnam(group_name)
    except KeyError as exc:
        raise ExternalSlurmAcceptanceError("fixed service identity is not installed") from exc
    if (
        user.pw_uid != uid
        or user.pw_gid != gid
        or primary_group.gr_gid != gid
        or user.pw_dir != str(home)
        or user.pw_shell != str(shell)
    ):
        raise ExternalSlurmAcceptanceError("fixed service UID/GID/home/shell does not match config")
    groups = sorted(
        group.gr_name
        for group in grp.getgrall()
        if user_name in group.gr_mem or group.gr_gid == user.pw_gid
    )
    expected_groups = {group_name, *supplementary_groups}
    if (require_exact_groups and set(groups) != expected_groups) or not expected_groups.issubset(
        groups
    ):
        raise ExternalSlurmAcceptanceError("fixed service identity group set mismatch")
    return {
        "username": user_name,
        "group": group_name,
        "uid": uid,
        "gid": gid,
        "home": str(home),
        "shell": str(shell),
        "supplementary_groups": list(supplementary_groups),
    }


def _verify_producer_identity(config: ExternalSlurmAuthorityConfig) -> dict[str, Any]:
    return _verify_identity(
        user_name=config.producer_user,
        group_name=config.producer_group,
        uid=config.producer_uid,
        gid=config.producer_gid,
        home=config.producer_home,
        shell=config.producer_shell,
    )


def _verify_batch_identity(config: ExternalSlurmAuthorityConfig) -> dict[str, Any]:
    return _verify_identity(
        user_name=config.batch_user,
        group_name=config.batch_group,
        uid=config.batch_uid,
        gid=config.batch_gid,
        home=config.batch_home,
        shell=config.batch_shell,
        supplementary_groups=config.batch_supplementary_groups,
        require_exact_groups=True,
    )


def _verify_private_leaf(
    path: Path,
    *,
    uid: int,
    gid: int,
    label: str,
    mode: int = 0o600,
) -> None:
    try:
        metadata = path.lstat()
    except OSError as exc:
        raise ExternalSlurmAcceptanceError(f"{label} is unavailable") from exc
    if (
        not stat.S_ISREG(metadata.st_mode)
        or stat.S_IMODE(metadata.st_mode) != mode
        or metadata.st_uid != uid
        or metadata.st_gid != gid
        or metadata.st_nlink != 1
    ):
        raise ExternalSlurmAcceptanceError(
            f"{label} must be a {mode:04o} single-link service-owned regular file"
        )


def _verify_root_parent_chain(path: Path, *, label: str) -> None:
    for parent in path.parents:
        if parent == Path("/"):
            break
        try:
            metadata = parent.lstat()
        except OSError as exc:
            raise ExternalSlurmAcceptanceError(f"{label} parent is unavailable") from exc
        if (
            not stat.S_ISDIR(metadata.st_mode)
            or stat.S_ISLNK(metadata.st_mode)
            or metadata.st_uid != 0
            or metadata.st_gid != 0
            or metadata.st_mode & 0o022
        ):
            raise ExternalSlurmAcceptanceError(
                f"{label} parents must be root-owned and non-writable"
            )


def _verify_candidate_repo(
    repository: Path,
    *,
    candidate_sha: str,
    candidate_tree: str,
) -> None:
    for arguments, expected, label in (
        (["rev-parse", "HEAD"], candidate_sha, "repository HEAD"),
        (["rev-parse", "HEAD^{tree}"], candidate_tree, "repository tree"),
    ):
        completed = _run(
            [
                "git",
                "-c",
                f"safe.directory={repository}",
                "-C",
                str(repository),
                *arguments,
            ]
        )
        if completed.returncode != 0 or completed.stdout.strip() != expected:
            raise ExternalSlurmAcceptanceError(f"{label} mismatch")
    clean = _run(
        [
            "git",
            "-c",
            f"safe.directory={repository}",
            "-C",
            str(repository),
            "status",
            "--porcelain=v1",
            "--untracked-files=all",
            "--ignored=matching",
        ]
    )
    if clean.returncode != 0 or clean.stdout:
        raise ExternalSlurmAcceptanceError("candidate repository is not exactly clean")


def _supervisor_state(config: ExternalSlurmAuthorityConfig) -> dict[str, Any]:
    prefix = ["systemctl", "--user", f"--machine={config.producer_user}@"]
    active = _run([*prefix, "is-active", config.supervisor_timer])
    enabled = _run([*prefix, "is-enabled", config.supervisor_timer])
    if active.stdout.strip() not in {"inactive", "failed", "unknown"}:
        raise ExternalSlurmAcceptanceError(
            "staging supervisor must remain stopped during prepare/probe"
        )
    if enabled.stdout.strip() not in {"disabled", "static", "indirect", "not-found"}:
        raise ExternalSlurmAcceptanceError(
            "staging supervisor must remain disabled during prepare/probe"
        )
    return {
        "service": config.supervisor_service,
        "timer": config.supervisor_timer,
        "enabled": False,
        "active": False,
    }


def _state_dir(config: ExternalSlurmAuthorityConfig) -> Path:
    return config.artifact_root / "state"


def _prepared_path(config: ExternalSlurmAuthorityConfig, candidate_sha: str) -> Path:
    return _state_dir(config) / f"prepared-{candidate_sha}.json"


def _safe_private_dir(path: Path) -> None:
    path.mkdir(mode=0o700, parents=True, exist_ok=True)
    metadata = path.lstat()
    if (
        not stat.S_ISDIR(metadata.st_mode)
        or stat.S_IMODE(metadata.st_mode) != 0o700
        or metadata.st_uid != 0
        or metadata.st_gid != 0
    ):
        raise ExternalSlurmAcceptanceError("authority state directories must be root:root 0700")


def _write_root_file(path: Path, payload: bytes, *, no_replace: bool = False) -> None:
    _safe_private_dir(path.parent)
    temporary = path.with_name(f".{path.name}.tmp-{os.getpid()}")
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_CLOEXEC", 0)
    descriptor = os.open(temporary, flags, 0o600)
    try:
        view = memoryview(payload)
        while view:
            written = os.write(descriptor, view)
            if written <= 0:
                raise ExternalSlurmAcceptanceError("authority state write failed safely")
            view = view[written:]
        os.fchown(descriptor, 0, 0)
        os.fchmod(descriptor, 0o600)
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    if no_replace:
        try:
            _rename_noreplace(temporary, path)
        except FileExistsError:
            temporary.unlink()
            try:
                metadata = path.lstat()
                existing = path.read_bytes()
            except OSError as exc:
                raise ExternalSlurmAcceptanceError(
                    "authority immutable target is unavailable"
                ) from exc
            if (
                not stat.S_ISREG(metadata.st_mode)
                or stat.S_ISLNK(metadata.st_mode)
                or metadata.st_uid != 0
                or metadata.st_gid != 0
                or metadata.st_nlink != 1
                or stat.S_IMODE(metadata.st_mode) != 0o600
                or existing != payload
            ):
                raise ExternalSlurmAcceptanceError(
                    "authority immutable target already exists"
                ) from None
    else:
        os.replace(temporary, path)
    os.chmod(path, 0o600)
    directory = os.open(path.parent, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try:
        os.fsync(directory)
    finally:
        os.close(directory)


def _fsync_directory(path: Path) -> None:
    flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_CLOEXEC", 0)
    descriptor = os.open(path, flags)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _install_key_file_no_replace(path: Path, payload: bytes, *, mode: int) -> bool:
    """Persist one root key leaf without ever replacing an existing inode."""
    temporary = path.with_name(
        f".{path.name}.tmp-{os.getpid()}-{secrets.token_hex(8)}"
    )
    flags = (
        os.O_WRONLY
        | os.O_CREAT
        | os.O_EXCL
        | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_NOFOLLOW", 0)
    )
    try:
        descriptor = os.open(temporary, flags, 0o600)
    except OSError as exc:
        raise ExternalSlurmAcceptanceError(
            "authority key staging failed safely"
        ) from exc
    try:
        try:
            view = memoryview(payload)
            while view:
                written = os.write(descriptor, view)
                if written <= 0:
                    raise ExternalSlurmAcceptanceError(
                        "authority key write failed safely"
                    )
                view = view[written:]
            os.fchown(descriptor, 0, 0)
            os.fchmod(descriptor, mode)
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
        try:
            _rename_noreplace(temporary, path)
        except FileExistsError:
            temporary.unlink()
            _fsync_directory(path.parent)
            return False
        _fsync_directory(path.parent)
        return True
    except Exception:
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass
        raise


def _key_leaf_exists(path: Path, *, label: str) -> bool:
    try:
        path.lstat()
    except FileNotFoundError:
        return False
    except OSError as exc:
        raise ExternalSlurmAcceptanceError(f"{label} is unavailable") from exc
    return True


def _read_key_leaf(
    path: Path,
    *,
    label: str,
    mode: int,
    uid: int = 0,
    gid: int = 0,
) -> bytes:
    _verify_private_leaf(
        path,
        uid=uid,
        gid=gid,
        label=label,
        mode=mode,
    )
    flags = (
        os.O_RDONLY
        | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_NOFOLLOW", 0)
    )
    try:
        descriptor = os.open(path, flags)
    except OSError as exc:
        raise ExternalSlurmAcceptanceError(f"{label} is unavailable") from exc
    try:
        initial = os.fstat(descriptor)
        if (
            not stat.S_ISREG(initial.st_mode)
            or stat.S_IMODE(initial.st_mode) != mode
            or initial.st_uid != uid
            or initial.st_gid != gid
            or initial.st_nlink != 1
            or initial.st_size > _MAX_KEY_BYTES
        ):
            raise ExternalSlurmAcceptanceError(
                f"{label} must be a {mode:04o} single-link service-owned regular file"
            )
        chunks: list[bytes] = []
        total = 0
        while True:
            chunk = os.read(descriptor, min(4096, _MAX_KEY_BYTES + 1 - total))
            if not chunk:
                break
            chunks.append(chunk)
            total += len(chunk)
            if total > _MAX_KEY_BYTES:
                raise ExternalSlurmAcceptanceError(f"{label} exceeds its size limit")
        final = os.fstat(descriptor)
        if (
            final.st_dev != initial.st_dev
            or final.st_ino != initial.st_ino
            or final.st_mode != initial.st_mode
            or final.st_uid != initial.st_uid
            or final.st_gid != initial.st_gid
            or final.st_nlink != initial.st_nlink
            or final.st_size != initial.st_size
            or total != initial.st_size
        ):
            raise ExternalSlurmAcceptanceError(f"{label} changed while being read")
        return b"".join(chunks)
    except OSError as exc:
        raise ExternalSlurmAcceptanceError(f"{label} cannot be read") from exc
    finally:
        os.close(descriptor)


def _private_key_pem(private: Ed25519PrivateKey) -> bytes:
    return private.private_bytes(
        serialization.Encoding.PEM,
        serialization.PrivateFormat.PKCS8,
        serialization.NoEncryption(),
    )


def _public_key_pem(private: Ed25519PrivateKey) -> bytes:
    return private.public_key().public_bytes(
        serialization.Encoding.PEM,
        serialization.PublicFormat.SubjectPublicKeyInfo,
    )


def _converge_signing_keypair(
    config: ExternalSlurmAuthorityConfig,
) -> tuple[Ed25519PrivateKey, str]:
    """Converge the fixed key pair and cryptographically read it back."""
    for path, label in (
        (config.private_key, "authority private key"),
        (config.public_key, "authority public key"),
    ):
        _verify_root_parent_chain(path, label=label)
    private_exists = _key_leaf_exists(
        config.private_key,
        label="authority private key",
    )
    public_exists = _key_leaf_exists(
        config.public_key,
        label="authority public key",
    )
    if public_exists and not private_exists:
        raise ExternalSlurmAcceptanceError(
            "authority public key exists without its private key"
        )
    if not private_exists:
        generated = Ed25519PrivateKey.generate()
        installed = _install_key_file_no_replace(
            config.private_key,
            _private_key_pem(generated),
            mode=0o600,
        )
        private = generated if installed else _load_private_key(config)
    else:
        private = _load_private_key(config)
    if not public_exists:
        _install_key_file_no_replace(
            config.public_key,
            _public_key_pem(private),
            mode=0o644,
        )
    return _verified_signing_key(config)


def _install_root_asset(path: Path, payload: bytes, *, mode: int) -> None:
    path.parent.mkdir(mode=0o755, parents=True, exist_ok=True)
    parent = path.parent.lstat()
    if (
        not stat.S_ISDIR(parent.st_mode)
        or parent.st_uid != 0
        or parent.st_gid != 0
        or stat.S_IMODE(parent.st_mode) != 0o755
    ):
        raise ExternalSlurmAcceptanceError("system asset parent is unsafe")
    if path.exists() or path.is_symlink():
        current = path.lstat()
        if (
            not stat.S_ISREG(current.st_mode)
            or current.st_uid != 0
            or current.st_gid != 0
            or current.st_nlink != 1
        ):
            raise ExternalSlurmAcceptanceError("system asset target is unsafe")
        if path.read_bytes() == payload and stat.S_IMODE(current.st_mode) == mode:
            return
    temporary = path.with_name(f".{path.name}.tmp-{os.getpid()}")
    descriptor = os.open(
        temporary,
        os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
        0o600,
    )
    try:
        os.write(descriptor, payload)
        os.fchown(descriptor, 0, 0)
        os.fchmod(descriptor, mode)
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    os.replace(temporary, path)
    directory = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(directory)
    finally:
        os.close(directory)


def _validate_producer_tree(
    directory_fd: int,
    *,
    uid: int,
    gid: int,
) -> None:
    root = os.fstat(directory_fd)
    if (
        not stat.S_ISDIR(root.st_mode)
        or root.st_uid != uid
        or root.st_gid != gid
        or root.st_mode & 0o022
    ):
        raise ExternalSlurmAcceptanceError("producer candidate directory metadata is unsafe")
    for name in sorted(os.listdir(directory_fd)):
        metadata = os.stat(name, dir_fd=directory_fd, follow_symlinks=False)
        if metadata.st_uid != uid or metadata.st_gid != gid or metadata.st_mode & 0o022:
            raise ExternalSlurmAcceptanceError(
                "producer candidate contains foreign or writable content"
            )
        if stat.S_ISDIR(metadata.st_mode):
            child = os.open(
                name,
                os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW,
                dir_fd=directory_fd,
            )
            try:
                _validate_producer_tree(child, uid=uid, gid=gid)
            finally:
                os.close(child)
        elif stat.S_ISREG(metadata.st_mode):
            if metadata.st_nlink != 1:
                raise ExternalSlurmAcceptanceError("producer candidate contains a hard-linked file")
            descriptor = os.open(
                name,
                os.O_RDONLY | os.O_NOFOLLOW,
                dir_fd=directory_fd,
            )
            os.close(descriptor)
        elif stat.S_ISLNK(metadata.st_mode):
            link = os.readlink(name, dir_fd=directory_fd)
            if not link or "\x00" in link:
                raise ExternalSlurmAcceptanceError("producer candidate symlink is invalid")
        else:
            raise ExternalSlurmAcceptanceError(
                "producer candidate contains an unsupported file type"
            )


def _copy_frozen_tree(
    source_fd: int,
    target_fd: int,
    *,
    gid: int,
) -> None:
    for name in sorted(os.listdir(source_fd)):
        metadata = os.stat(name, dir_fd=source_fd, follow_symlinks=False)
        if stat.S_ISDIR(metadata.st_mode):
            os.mkdir(name, 0o500, dir_fd=target_fd)
            child_source = os.open(
                name,
                os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW,
                dir_fd=source_fd,
            )
            child_target = os.open(
                name,
                os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW,
                dir_fd=target_fd,
            )
            try:
                _copy_frozen_tree(
                    child_source,
                    child_target,
                    gid=gid,
                )
                os.fchown(child_target, 0, gid)
                os.fchmod(child_target, 0o550)
            finally:
                os.close(child_target)
                os.close(child_source)
        elif stat.S_ISREG(metadata.st_mode):
            source = os.open(
                name,
                os.O_RDONLY | os.O_NOFOLLOW,
                dir_fd=source_fd,
            )
            target = os.open(
                name,
                os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
                0o400,
                dir_fd=target_fd,
            )
            try:
                while True:
                    chunk = os.read(source, 1024 * 1024)
                    if not chunk:
                        break
                    view = memoryview(chunk)
                    while view:
                        written = os.write(target, view)
                        if written <= 0:
                            raise ExternalSlurmAcceptanceError(
                                "candidate publication write failed safely"
                            )
                        view = view[written:]
                os.fchown(target, 0, gid)
                os.fchmod(target, 0o550 if metadata.st_mode & 0o111 else 0o440)
                os.fsync(target)
            finally:
                os.close(target)
                os.close(source)
        elif stat.S_ISLNK(metadata.st_mode):
            link = os.readlink(name, dir_fd=source_fd)
            if not link or "\x00" in link:
                raise ExternalSlurmAcceptanceError("producer candidate symlink is invalid")
            os.symlink(link, name, dir_fd=target_fd)
            os.chown(
                name,
                0,
                gid,
                dir_fd=target_fd,
                follow_symlinks=False,
            )
        else:
            raise ExternalSlurmAcceptanceError(
                "producer candidate contains an unsupported file type"
            )
    os.fsync(target_fd)


def _verify_published_tree(
    directory_fd: int,
    *,
    gid: int,
) -> None:
    root = os.fstat(directory_fd)
    if (
        not stat.S_ISDIR(root.st_mode)
        or (root.st_uid, root.st_gid) != (0, gid)
        or stat.S_IMODE(root.st_mode) != 0o550
    ):
        raise ExternalSlurmAcceptanceError("published candidate directory metadata drifted")
    for name in sorted(os.listdir(directory_fd)):
        metadata = os.stat(name, dir_fd=directory_fd, follow_symlinks=False)
        if (metadata.st_uid, metadata.st_gid) != (0, gid):
            raise ExternalSlurmAcceptanceError("published candidate contains foreign content")
        if stat.S_ISDIR(metadata.st_mode):
            if stat.S_IMODE(metadata.st_mode) != 0o550:
                raise ExternalSlurmAcceptanceError("published candidate directory mode drifted")
            child = os.open(
                name,
                os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW,
                dir_fd=directory_fd,
            )
            try:
                _verify_published_tree(child, gid=gid)
            finally:
                os.close(child)
        elif stat.S_ISREG(metadata.st_mode):
            expected_mode = 0o550 if metadata.st_mode & 0o111 else 0o440
            if metadata.st_nlink != 1 or stat.S_IMODE(metadata.st_mode) != expected_mode:
                raise ExternalSlurmAcceptanceError("published candidate file metadata drifted")
        elif stat.S_ISLNK(metadata.st_mode):
            link = os.readlink(name, dir_fd=directory_fd)
            if not link or "\x00" in link:
                raise ExternalSlurmAcceptanceError("published candidate symlink is invalid")
        else:
            raise ExternalSlurmAcceptanceError(
                "published candidate contains an unsupported file type"
            )


def _rename_noreplace(source: Path, target: Path) -> None:
    if target.exists() or target.is_symlink():
        raise FileExistsError(target)
    libc = ctypes.CDLL(None, use_errno=True)
    renameat2 = getattr(libc, "renameat2", None)
    if renameat2 is None:
        raise ExternalSlurmAcceptanceError(
            "kernel renameat2 support is required for candidate publication"
        )
    result = renameat2(
        -100,
        os.fsencode(source),
        -100,
        os.fsencode(target),
        1,
    )
    if result != 0:
        error = ctypes.get_errno()
        if error == 17:
            raise FileExistsError(target)
        raise ExternalSlurmAcceptanceError(
            "candidate no-replace publication failed safely"
        ) from OSError(error, os.strerror(error))


def _publisher_journal_path(config: ExternalSlurmAuthorityConfig) -> Path:
    return _state_dir(config) / "candidate-publication.json"


def _publisher_lock(config: ExternalSlurmAuthorityConfig) -> int:
    path = _state_dir(config) / "authority.lock"
    _safe_private_dir(path.parent)
    try:
        descriptor = os.open(
            path,
            os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW | getattr(os, "O_CLOEXEC", 0),
            0o600,
        )
        metadata = os.fstat(descriptor)
    except OSError as exc:
        raise ExternalSlurmAcceptanceError("candidate publication lock is unavailable") from exc
    if (
        not stat.S_ISREG(metadata.st_mode)
        or metadata.st_uid != 0
        or metadata.st_gid != 0
        or metadata.st_nlink != 1
        or stat.S_IMODE(metadata.st_mode) != 0o600
    ):
        os.close(descriptor)
        raise ExternalSlurmAcceptanceError("candidate publication lock metadata is unsafe")
    fcntl.flock(descriptor, fcntl.LOCK_EX)
    return descriptor


def _remove_publication_stage(path: Path, *, allowed_uids: set[int]) -> None:
    metadata = path.lstat()
    if (
        not stat.S_ISDIR(metadata.st_mode)
        or stat.S_ISLNK(metadata.st_mode)
        or metadata.st_uid not in allowed_uids
    ):
        raise ExternalSlurmAcceptanceError("candidate publication stage is foreign")

    def remove_contents(directory_fd: int) -> None:
        for name in os.listdir(directory_fd):
            child = os.stat(name, dir_fd=directory_fd, follow_symlinks=False)
            if child.st_uid not in allowed_uids:
                raise ExternalSlurmAcceptanceError(
                    "candidate publication stage contains foreign content"
                )
            if stat.S_ISDIR(child.st_mode):
                child_fd = os.open(
                    name,
                    os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW,
                    dir_fd=directory_fd,
                )
                try:
                    remove_contents(child_fd)
                finally:
                    os.close(child_fd)
                os.rmdir(name, dir_fd=directory_fd)
            elif stat.S_ISREG(child.st_mode) or stat.S_ISLNK(child.st_mode):
                os.unlink(name, dir_fd=directory_fd)
            else:
                raise ExternalSlurmAcceptanceError(
                    "candidate publication stage contains unsupported content"
                )

    descriptor = os.open(path, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        remove_contents(descriptor)
    finally:
        os.close(descriptor)
    path.rmdir()


def _publish_candidate_locked(
    config: ExternalSlurmAuthorityConfig,
    *,
    candidate_sha: str,
    candidate_tree: str,
    image_tag: str,
) -> tuple[Path, Path]:
    source_repo, source_env = _producer_candidate_paths(
        config,
        image_tag=image_tag,
    )
    target_repo, target_env = _candidate_paths(config, image_tag=image_tag)
    stage_repo = target_repo.parent / f".publish-{candidate_sha}"
    stage_env = target_env.parent / f".publish-{candidate_sha}.env"
    _verify_candidate_repo(
        source_repo,
        candidate_sha=candidate_sha,
        candidate_tree=candidate_tree,
    )
    _verify_private_leaf(
        source_env,
        uid=config.producer_uid,
        gid=config.producer_gid,
        label="private producer worker env",
    )
    journal = _publisher_journal_path(config)
    journal_payload = {
        "schema_version": 1,
        "kind": "staging_external_slurm_candidate_publication",
        "candidate_sha": candidate_sha,
        "candidate_tree": candidate_tree,
        "image_tag": image_tag,
        "source_repository": str(source_repo),
        "source_worker_env": str(source_env),
        "target_repository": str(target_repo),
        "target_worker_env": str(target_env),
        "stage_repository": str(stage_repo),
        "stage_worker_env": str(stage_env),
        "phase": "prepared",
    }
    recovering = journal.exists()
    if recovering:
        try:
            existing = json.loads(journal.read_bytes())
        except (OSError, json.JSONDecodeError) as exc:
            raise ExternalSlurmAcceptanceError("candidate publication journal is invalid") from exc
        if not isinstance(existing, dict) or {
            key: value for key, value in existing.items() if key != "phase"
        } != {key: value for key, value in journal_payload.items() if key != "phase"}:
            raise ExternalSlurmAcceptanceError(
                "another candidate publication transaction is active"
            )
    else:
        if (
            stage_repo.exists()
            or stage_repo.is_symlink()
            or stage_env.exists()
            or stage_env.is_symlink()
        ):
            raise ExternalSlurmAcceptanceError("foreign candidate publication stage already exists")
        _write_root_file(journal, canonical_json_bytes(journal_payload))
    if recovering and (stage_repo.exists() or stage_repo.is_symlink()):
        _remove_publication_stage(
            stage_repo,
            allowed_uids={0},
        )
    if recovering and (stage_env.exists() or stage_env.is_symlink()):
        metadata = stage_env.lstat()
        if not stat.S_ISREG(metadata.st_mode) or metadata.st_uid != 0 or metadata.st_nlink != 1:
            raise ExternalSlurmAcceptanceError("candidate env publication stage is foreign")
        stage_env.unlink()

    if not target_repo.exists():
        source_fd = os.open(
            source_repo,
            os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW,
        )
        try:
            _validate_producer_tree(
                source_fd,
                uid=config.producer_uid,
                gid=config.producer_gid,
            )
            os.mkdir(stage_repo, 0o500)
            stage_fd = os.open(
                stage_repo,
                os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW,
            )
            try:
                _copy_frozen_tree(
                    source_fd,
                    stage_fd,
                    gid=config.batch_gid,
                )
                os.fchown(stage_fd, 0, config.batch_gid)
                os.fchmod(stage_fd, 0o550)
                os.fsync(stage_fd)
            finally:
                os.close(stage_fd)
            journal_payload["phase"] = "repository-staged"
            _write_root_file(journal, canonical_json_bytes(journal_payload))
            _rename_noreplace(stage_repo, target_repo)
            parent = os.open(target_repo.parent, os.O_RDONLY | os.O_DIRECTORY)
            try:
                os.fsync(parent)
            finally:
                os.close(parent)
        finally:
            os.close(source_fd)
    _verify_candidate_repo(
        target_repo,
        candidate_sha=candidate_sha,
        candidate_tree=candidate_tree,
    )
    published_descriptor = os.open(
        target_repo,
        os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW,
    )
    try:
        _verify_published_tree(published_descriptor, gid=config.batch_gid)
    finally:
        os.close(published_descriptor)

    source_descriptor = os.open(source_env, os.O_RDONLY | os.O_NOFOLLOW)
    try:
        source_metadata = os.fstat(source_descriptor)
        if (
            not stat.S_ISREG(source_metadata.st_mode)
            or source_metadata.st_nlink != 1
            or source_metadata.st_size > 1024 * 1024
            or (source_metadata.st_uid, source_metadata.st_gid)
            != (config.producer_uid, config.producer_gid)
            or stat.S_IMODE(source_metadata.st_mode) != 0o600
        ):
            raise ExternalSlurmAcceptanceError("private producer worker env is unsafe")
        env_payload = b""
        while len(env_payload) <= 1024 * 1024:
            chunk = os.read(source_descriptor, 1024 * 1024)
            if not chunk:
                break
            env_payload += chunk
    finally:
        os.close(source_descriptor)
    if not target_env.exists():
        descriptor = os.open(
            stage_env,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
            0o400,
        )
        try:
            os.write(descriptor, env_payload)
            os.fchown(descriptor, 0, config.batch_gid)
            os.fchmod(descriptor, 0o440)
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
        journal_payload["phase"] = "environment-staged"
        _write_root_file(journal, canonical_json_bytes(journal_payload))
        _rename_noreplace(stage_env, target_env)
    target_payload = target_env.read_bytes()
    if target_payload != env_payload:
        raise ExternalSlurmAcceptanceError("published worker env digest mismatch")
    journal_payload["phase"] = "verified"
    _write_root_file(journal, canonical_json_bytes(journal_payload))
    journal.unlink()
    directory = os.open(journal.parent, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(directory)
    finally:
        os.close(directory)
    return target_repo, target_env


def _publish_candidate(
    config: ExternalSlurmAuthorityConfig,
    *,
    candidate_sha: str,
    candidate_tree: str,
    image_tag: str,
) -> tuple[Path, Path]:
    _validate_candidate_identity(
        candidate_sha=candidate_sha,
        candidate_tree=candidate_tree,
        image_tag=image_tag,
    )
    descriptor = _publisher_lock(config)
    try:
        return _publish_candidate_locked(
            config,
            candidate_sha=candidate_sha,
            candidate_tree=candidate_tree,
            image_tag=image_tag,
        )
    finally:
        os.close(descriptor)


def _converge_source_batch_identity(
    config: ExternalSlurmAuthorityConfig,
) -> dict[str, Any]:
    try:
        account = pwd.getpwnam(config.batch_user)
    except KeyError:
        try:
            occupied = pwd.getpwuid(config.batch_uid)
        except KeyError:
            occupied = None
        if occupied is not None:
            raise ExternalSlurmAcceptanceError("fixed batch UID is already occupied") from None
        try:
            group = grp.getgrnam(config.batch_group)
        except KeyError:
            try:
                occupied_group = grp.getgrgid(config.batch_gid)
            except KeyError:
                occupied_group = None
            if occupied_group is not None:
                raise ExternalSlurmAcceptanceError("fixed batch GID is already occupied") from None
            created = _run(
                [
                    "groupadd",
                    "--system",
                    "--gid",
                    str(config.batch_gid),
                    config.batch_group,
                ]
            )
            if created.returncode != 0:
                raise ExternalSlurmAcceptanceError("fixed batch group creation failed") from None
        else:
            if group.gr_gid != config.batch_gid:
                raise ExternalSlurmAcceptanceError("fixed batch group GID mismatch")
        created = _run(
            [
                "useradd",
                "--system",
                "--uid",
                str(config.batch_uid),
                "--gid",
                str(config.batch_gid),
                "--home-dir",
                str(config.batch_home),
                "--no-create-home",
                "--shell",
                str(config.batch_shell),
                config.batch_user,
            ]
        )
        if created.returncode != 0:
            raise ExternalSlurmAcceptanceError("fixed batch user creation failed") from None
    else:
        if account.pw_uid != config.batch_uid:
            raise ExternalSlurmAcceptanceError("fixed batch username is already occupied")
    groups = _run(
        [
            "usermod",
            "--groups",
            ",".join(config.batch_supplementary_groups),
            config.batch_user,
        ]
    )
    if groups.returncode != 0:
        raise ExternalSlurmAcceptanceError("fixed batch group convergence failed")
    return _verify_batch_identity(config)


def _shared_mount_readback(
    config: ExternalSlurmAuthorityConfig,
) -> dict[str, Any]:
    completed = _run(
        [
            "findmnt",
            "--noheadings",
            "--raw",
            "--mountpoint",
            str(config.shared_mount_target),
            "--output",
            "SOURCE,FSTYPE,TARGET",
        ]
    )
    if completed.returncode != 0:
        raise ExternalSlurmAcceptanceError("fixed staging shared mount is unavailable")
    fields = completed.stdout.strip().split()
    if fields != [
        config.shared_mount_source,
        config.shared_mount_filesystem_type,
        str(config.shared_mount_target),
    ]:
        raise ExternalSlurmAcceptanceError("fixed staging shared mount binding mismatch")
    root = config.shared_mount_target.lstat()
    if not stat.S_ISDIR(root.st_mode) or stat.S_ISLNK(root.st_mode):
        raise ExternalSlurmAcceptanceError("fixed staging shared mount root is unsafe")
    return {
        "source": fields[0],
        "filesystem_type": fields[1],
        "target": fields[2],
        "device": root.st_dev,
        "inode": root.st_ino,
    }


def bootstrap(
    config: ExternalSlurmAuthorityConfig,
    *,
    candidate_sha: str,
    candidate_tree: str,
) -> dict[str, Any]:
    _require_root()
    _require_source_host(config)
    if (
        _OBJECT_ID_RE.fullmatch(candidate_sha) is None
        or _OBJECT_ID_RE.fullmatch(candidate_tree) is None
    ):
        raise ExternalSlurmAcceptanceError(
            "bootstrap requires the exact release candidate identity"
        )
    infrastructure, infrastructure_sha256 = _load_infrastructure_receipt(
        config,
        candidate_sha=candidate_sha,
        candidate_tree=candidate_tree,
    )
    _verify_producer_identity(config)
    for path, label in (
        (DEFAULT_CONFIG_PATH, "authority config"),
        (config.public_key, "authority public key"),
        (config.private_key, "authority private key"),
        (config.artifact_root, "authority artifact root"),
    ):
        _verify_root_parent_chain(path, label=label)
    _converge_signing_keypair(config)
    identity = _converge_source_batch_identity(config)
    _install_root_asset(_MOUNT_UNIT_PATH, _MOUNT_UNIT, mode=0o644)
    for path in (config.shared_mount_target.parent, config.shared_mount_target):
        path.mkdir(mode=0o755, parents=True, exist_ok=True)
        metadata = path.lstat()
        if (
            not stat.S_ISDIR(metadata.st_mode)
            or stat.S_ISLNK(metadata.st_mode)
            or metadata.st_uid != 0
            or metadata.st_gid != 0
        ):
            raise ExternalSlurmAcceptanceError("fixed mountpoint parent is unsafe")
        os.chmod(path, 0o755)
    for argv in (
        ["systemctl", "daemon-reload"],
        ["systemctl", "enable", "--now", config.shared_mount_unit],
    ):
        completed = _run(argv)
        if completed.returncode != 0:
            raise ExternalSlurmAcceptanceError("fixed staging mount activation failed")
    mount = _shared_mount_readback(config)
    namespace_modes = (
        (config.shared_mount_target, 0, config.batch_gid, 0o750),
        (config.repository_root, 0, config.batch_gid, 0o750),
        (config.worker_env_root, 0, config.batch_gid, 0o750),
        (config.result_root, config.batch_uid, config.batch_gid, 0o2770),
    )
    for path, uid, gid, mode in namespace_modes:
        path.mkdir(mode=mode, exist_ok=True)
        os.chown(path, uid, gid)
        os.chmod(path, mode)
        metadata = path.lstat()
        if (
            not stat.S_ISDIR(metadata.st_mode)
            or stat.S_ISLNK(metadata.st_mode)
            or metadata.st_uid != uid
            or metadata.st_gid != gid
            or stat.S_IMODE(metadata.st_mode) != mode
        ):
            raise ExternalSlurmAcceptanceError("fixed shared staging directory drifted")
    return {
        "schema_version": 1,
        "kind": "staging_external_slurm_source_bootstrap",
        "source_host": config.source_host,
        "candidate_sha": candidate_sha,
        "candidate_tree": candidate_tree,
        "infrastructure_receipt": _infrastructure_summary(
            infrastructure,
            payload_sha256=infrastructure_sha256,
        ),
        "service_identity": identity,
        "shared_mount": mount,
        "repository_root": str(config.repository_root),
        "worker_env_root": str(config.worker_env_root),
        "result_root": str(config.result_root),
        "status": "converged",
    }


def prepare(
    config: ExternalSlurmAuthorityConfig,
    *,
    candidate_sha: str,
    candidate_tree: str,
    image_tag: str,
) -> dict[str, Any]:
    _require_root()
    _require_source_host(config)
    _validate_candidate_identity(
        candidate_sha=candidate_sha,
        candidate_tree=candidate_tree,
        image_tag=image_tag,
    )
    _verify_producer_identity(config)
    identity = _verify_batch_identity(config)
    infrastructure, infrastructure_sha256 = _load_infrastructure_receipt(
        config,
        candidate_sha=candidate_sha,
        candidate_tree=candidate_tree,
    )
    _shared_mount_readback(config)
    if infrastructure["mount_contract"] != _expected_infrastructure_mount_contract(config):
        raise ExternalSlurmAcceptanceError(
            "fixed infrastructure receipt mount contract changed during prepare"
        )
    repository, worker_env = _publish_candidate(
        config,
        candidate_sha=candidate_sha,
        candidate_tree=candidate_tree,
        image_tag=image_tag,
    )
    _verify_private_leaf(
        worker_env,
        uid=0,
        gid=config.batch_gid,
        label="candidate worker env",
        mode=0o440,
    )
    _verify_private_leaf(
        config.environment_state_profile,
        uid=config.producer_uid,
        gid=config.producer_gid,
        label="materialized environment-state profile",
    )
    supervisor = _supervisor_state(config)
    payload = {
        "schema_version": 1,
        "kind": "staging_external_slurm_prepare",
        "candidate_sha": candidate_sha,
        "candidate_tree": candidate_tree,
        "image_tag": image_tag,
        "profile_sha256": _sha256_file(config.environment_state_profile),
        "repository": str(repository),
        "worker_env": str(worker_env),
        "service_identity": identity,
        "supervisor": supervisor,
        "infrastructure_sha256": infrastructure_sha256,
        "prepared_at": _timestamp(),
    }
    _write_root_file(
        _prepared_path(config, candidate_sha),
        canonical_json_bytes(payload),
    )
    return payload


def _load_prepared(
    config: ExternalSlurmAuthorityConfig,
    *,
    candidate_sha: str,
    candidate_tree: str,
    image_tag: str,
) -> dict[str, Any]:
    path = _prepared_path(config, candidate_sha)
    try:
        payload = json.loads(path.read_bytes())
    except (OSError, json.JSONDecodeError) as exc:
        raise ExternalSlurmAcceptanceError("prepared authority state is unavailable") from exc
    if not isinstance(payload, dict) or set(payload) != _PREPARED_FIELDS:
        raise ExternalSlurmAcceptanceError("prepared authority state is invalid")
    expected = {
        "schema_version": 1,
        "kind": "staging_external_slurm_prepare",
        "candidate_sha": candidate_sha,
        "candidate_tree": candidate_tree,
        "image_tag": image_tag,
    }
    if any(payload.get(field) != value for field, value in expected.items()):
        raise ExternalSlurmAcceptanceError("prepared authority candidate mismatch")
    if canonical_json_bytes(payload) != path.read_bytes():
        raise ExternalSlurmAcceptanceError("prepared authority state is not canonical")
    return payload


def _probe_argv(
    config: ExternalSlurmAuthorityConfig,
) -> list[str]:
    return _transport_argv(config, node=config.broker_node)


def _transport_argv(
    config: ExternalSlurmAuthorityConfig,
    *,
    node: str,
) -> list[str]:
    return [
        str(config.broker_transport),
        "invoke",
        "--node",
        node,
        "--verb",
        "transact",
    ]


def _parse_utc(value: object, *, label: str) -> datetime:
    if not isinstance(value, str) or not value.endswith("Z"):
        raise ExternalSlurmAcceptanceError(f"{label} must be a canonical UTC timestamp")
    try:
        parsed = datetime.fromisoformat(value[:-1] + "+00:00")
    except ValueError as exc:
        raise ExternalSlurmAcceptanceError(f"{label} must be a canonical UTC timestamp") from exc
    parsed = parsed.astimezone(UTC)
    if _timestamp(parsed) != value:
        raise ExternalSlurmAcceptanceError(f"{label} must be a canonical UTC timestamp")
    return parsed


def _infrastructure_receipt_path(candidate_sha: str) -> Path:
    if _OBJECT_ID_RE.fullmatch(candidate_sha) is None:
        raise ExternalSlurmAcceptanceError("infrastructure candidate SHA is invalid")
    return _INFRASTRUCTURE_RECEIPT_ROOT / f"{candidate_sha}.json"


def _expected_infrastructure_mount_contract(
    config: ExternalSlurmAuthorityConfig,
) -> dict[str, Any]:
    return {
        "source": config.shared_mount_source,
        "target": str(config.shared_mount_target),
        "filesystem_type": config.shared_mount_filesystem_type,
        "repository_root": str(config.repository_root),
        "worker_env_root": str(config.worker_env_root),
        "result_root": str(config.result_root),
        "root_uid": 0,
        "root_gid": config.batch_gid,
        "root_mode": "0o750",
        "repository_root_mode": "0o750",
        "worker_env_root_mode": "0o750",
        "result_uid": config.batch_uid,
        "result_gid": config.batch_gid,
        "result_root_mode": "0o2770",
    }


def _expected_infrastructure_request(
    config: ExternalSlurmAuthorityConfig,
    *,
    action: str,
    node: str,
    candidate_sha: str,
    candidate_tree: str,
    convergence_id: str,
    requested_at: str,
) -> tuple[bytes, str | None]:
    if action not in {
        "staging-shared-source-bootstrap",
        "staging-allocation-bootstrap",
        "staging-slurm-accounting-converge",
    }:
        raise ExternalSlurmAcceptanceError("infrastructure receipt action is unsupported")
    inner_unsigned = {
        "schema_version": 1,
        "kind": "loom.staging-external-slurm.infrastructure-operation-request",
        "action": action,
        "node": node,
        "candidate_sha": candidate_sha,
        "candidate_tree": candidate_tree,
        "convergence_id": convergence_id,
        "requested_at": requested_at,
    }
    inner_request_id = hashlib.sha256(canonical_json_bytes(inner_unsigned)).hexdigest()
    payload = canonical_json_bytes({**inner_unsigned, "request_id": inner_request_id})
    payload_kind = "staging-infrastructure-operation-request"
    unsigned = {
        "schema_version": 1,
        "action": action,
        "node": node,
        "domain": config.broker_domain,
        "sandbox": config.broker_sandbox,
        "candidate_sha": candidate_sha,
        "candidate_tree": candidate_tree,
        "payload_kind": payload_kind,
        "payload_sha256": hashlib.sha256(payload).hexdigest(),
        "payload_base64": base64.b64encode(payload).decode("ascii"),
        "prior_request_id": None,
    }
    return (
        canonical_json_bytes(
            {
                **unsigned,
                "request_id": hashlib.sha256(canonical_json_bytes(unsigned)).hexdigest(),
            }
        ),
        inner_request_id,
    )


def _validate_infrastructure_transport_receipt(
    config: ExternalSlurmAuthorityConfig,
    receipt: object,
    *,
    action: str,
    node: str,
    candidate_sha: str,
    candidate_tree: str,
    convergence_id: str,
    requested_at: str,
) -> datetime:
    envelope, inner_request_id = _expected_infrastructure_request(
        config,
        action=action,
        node=node,
        candidate_sha=candidate_sha,
        candidate_tree=candidate_tree,
        convergence_id=convergence_id,
        requested_at=requested_at,
    )
    outer = json.loads(envelope)
    expected_inner = (
        f"staging-accounting/v1/{inner_request_id}"
        if action == "staging-slurm-accounting-converge"
        else None
    )
    if (
        not isinstance(receipt, dict)
        or set(receipt) != _INFRASTRUCTURE_TRANSPORT_FIELDS
        or type(receipt.get("schema_version")) is not int
        or receipt["schema_version"] != 1
        or receipt.get("request_id") != outer["request_id"]
        or receipt.get("action") != action
        or receipt.get("node") != node
        or receipt.get("domain") != config.broker_domain
        or receipt.get("sandbox") != config.broker_sandbox
        or receipt.get("candidate_sha") != candidate_sha
        or receipt.get("candidate_tree") != candidate_tree
        or receipt.get("payload_sha256") != outer["payload_sha256"]
        or _DIGEST_RE.fullmatch(str(receipt.get("result_sha256"))) is None
        or receipt.get("inner_receipt") != expected_inner
        or not isinstance(receipt.get("completed_at"), str)
        or receipt.get("status") != "succeeded"
    ):
        raise ExternalSlurmAcceptanceError(
            f"infrastructure receipt {action}@{node} binding mismatch"
        )
    return _parse_utc(
        receipt["completed_at"],
        label=f"infrastructure receipt {action}@{node} completed_at",
    )


def _read_infrastructure_receipt(
    path: Path,
    *,
    enforce_root_security: bool,
) -> bytes:
    try:
        metadata = path.lstat()
    except OSError as exc:
        raise ExternalSlurmAcceptanceError("fixed infrastructure receipt is unavailable") from exc
    if (
        not stat.S_ISREG(metadata.st_mode)
        or stat.S_ISLNK(metadata.st_mode)
        or metadata.st_nlink != 1
        or metadata.st_size > _MAX_PROBE_BYTES
        or (
            enforce_root_security
            and (
                metadata.st_uid != 0
                or metadata.st_gid != 0
                or stat.S_IMODE(metadata.st_mode) != 0o600
            )
        )
    ):
        raise ExternalSlurmAcceptanceError("fixed infrastructure receipt metadata is unsafe")
    if enforce_root_security:
        _verify_root_parent_chain(path, label="fixed infrastructure receipt")
    try:
        descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    except OSError as exc:
        raise ExternalSlurmAcceptanceError(
            "fixed infrastructure receipt cannot be opened safely"
        ) from exc
    try:
        opened = os.fstat(descriptor)
        if (opened.st_dev, opened.st_ino) != (metadata.st_dev, metadata.st_ino):
            raise ExternalSlurmAcceptanceError("fixed infrastructure receipt changed while opening")
        chunks: list[bytes] = []
        size = 0
        while True:
            chunk = os.read(descriptor, min(65536, _MAX_PROBE_BYTES + 1 - size))
            if not chunk:
                break
            chunks.append(chunk)
            size += len(chunk)
            if size > _MAX_PROBE_BYTES:
                raise ExternalSlurmAcceptanceError(
                    "fixed infrastructure receipt exceeds its size limit"
                )
        return b"".join(chunks)
    finally:
        os.close(descriptor)


def _load_infrastructure_receipt(
    config: ExternalSlurmAuthorityConfig,
    *,
    candidate_sha: str,
    candidate_tree: str,
    now: datetime | None = None,
    enforce_root_security: bool = True,
) -> tuple[dict[str, Any], str]:
    if (
        _OBJECT_ID_RE.fullmatch(candidate_sha) is None
        or _OBJECT_ID_RE.fullmatch(candidate_tree) is None
    ):
        raise ExternalSlurmAcceptanceError(
            "infrastructure receipt requires the exact candidate identity"
        )
    path = _infrastructure_receipt_path(candidate_sha)
    raw = _read_infrastructure_receipt(
        path,
        enforce_root_security=enforce_root_security,
    )
    try:
        payload = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ExternalSlurmAcceptanceError("fixed infrastructure receipt is invalid JSON") from exc
    try:
        is_canonical = isinstance(payload, dict) and canonical_json_bytes(payload) == raw
    except (TypeError, ValueError) as exc:
        raise ExternalSlurmAcceptanceError(
            "fixed infrastructure receipt contains unsupported JSON values"
        ) from exc
    expected_converge_request = {
        "schema_version": 1,
        "kind": "loom.staging-external-slurm.infrastructure-converge-request",
        "candidate_sha": candidate_sha,
        "candidate_tree": candidate_tree,
        "convergence_id": payload.get("convergence_id") if isinstance(payload, dict) else None,
        "requested_at": payload.get("requested_at") if isinstance(payload, dict) else None,
    }
    if (
        not isinstance(payload, dict)
        or set(payload) != _INFRASTRUCTURE_FIELDS
        or not is_canonical
        or type(payload.get("schema_version")) is not int
        or payload["schema_version"] != 1
        or payload.get("kind") != "loom.staging-external-slurm.infrastructure-receipt"
        or payload.get("candidate_sha") != candidate_sha
        or payload.get("candidate_tree") != candidate_tree
        or type(payload.get("generation")) is not int
        or payload["generation"] < 1
        or _DIGEST_RE.fullmatch(str(payload.get("convergence_id"))) is None
        or _DIGEST_RE.fullmatch(str(payload.get("request_sha256"))) is None
        or payload.get("request_sha256")
        != hashlib.sha256(canonical_json_bytes(expected_converge_request)).hexdigest()
        or _parse_utc(
            payload.get("requested_at"),
            label="fixed infrastructure receipt requested_at",
        )
        is None
        or payload.get("source_controller") != _INFRASTRUCTURE_SOURCE_CONTROLLER
        or payload.get("source_controller_host") != _INFRASTRUCTURE_SOURCE_CONTROLLER_HOST
        or payload.get("result") != "pass"
    ):
        raise ExternalSlurmAcceptanceError(
            "fixed infrastructure receipt candidate or controller binding mismatch"
        )
    mount_contract = payload.get("mount_contract")
    if (
        not isinstance(mount_contract, dict)
        or set(mount_contract) != _INFRASTRUCTURE_MOUNT_FIELDS
        or canonical_json_bytes(mount_contract)
        != canonical_json_bytes(_expected_infrastructure_mount_contract(config))
    ):
        raise ExternalSlurmAcceptanceError("fixed infrastructure receipt mount contract mismatch")
    source_completed = _validate_infrastructure_transport_receipt(
        config,
        payload.get("source_bootstrap"),
        action="staging-shared-source-bootstrap",
        node="trt-gb10-2",
        candidate_sha=candidate_sha,
        candidate_tree=candidate_tree,
        convergence_id=payload["convergence_id"],
        requested_at=payload["requested_at"],
    )
    accounting_completed = _validate_infrastructure_transport_receipt(
        config,
        payload.get("accounting"),
        action="staging-slurm-accounting-converge",
        node=config.controller,
        candidate_sha=candidate_sha,
        candidate_tree=candidate_tree,
        convergence_id=payload["convergence_id"],
        requested_at=payload["requested_at"],
    )
    node_bootstraps = payload.get("node_bootstraps")
    if not isinstance(node_bootstraps, list) or len(node_bootstraps) != len(config.allowed_nodes):
        raise ExternalSlurmAcceptanceError("fixed infrastructure receipt node set mismatch")
    node_completed = [
        _validate_infrastructure_transport_receipt(
            config,
            receipt,
            action="staging-allocation-bootstrap",
            node=node,
            candidate_sha=candidate_sha,
            candidate_tree=candidate_tree,
            convergence_id=payload["convergence_id"],
            requested_at=payload["requested_at"],
        )
        for node, receipt in zip(config.allowed_nodes, node_bootstraps, strict=True)
    ]
    created_at = _parse_utc(
        payload.get("created_at"),
        label="fixed infrastructure receipt created_at",
    )
    expires_at = _parse_utc(
        payload.get("expires_at"),
        label="fixed infrastructure receipt expires_at",
    )
    observed_at = (now or datetime.now(UTC)).astimezone(UTC)
    requested_at = _parse_utc(
        payload["requested_at"],
        label="fixed infrastructure receipt requested_at",
    )
    completion_order = [source_completed, accounting_completed, *node_completed, created_at]
    if completion_order != sorted(completion_order):
        raise ExternalSlurmAcceptanceError(
            "fixed infrastructure receipt completion order is invalid"
        )
    if (
        requested_at > source_completed
        or created_at > observed_at + timedelta(seconds=30)
        or expires_at <= observed_at
        or expires_at <= created_at
        or expires_at - created_at > timedelta(seconds=_INFRASTRUCTURE_MAX_AGE_SECONDS)
        or created_at - source_completed > timedelta(seconds=_INFRASTRUCTURE_MAX_AGE_SECONDS)
    ):
        raise ExternalSlurmAcceptanceError(
            "fixed infrastructure receipt is stale or has an invalid lifetime"
        )
    return payload, hashlib.sha256(raw).hexdigest()


def _infrastructure_transport_summary(receipt: dict[str, Any]) -> dict[str, Any]:
    return {
        field: receipt[field]
        for field in (
            "action",
            "node",
            "request_id",
            "payload_sha256",
            "result_sha256",
            "inner_receipt",
            "completed_at",
            "status",
        )
    }


def _infrastructure_summary(
    payload: dict[str, Any],
    *,
    payload_sha256: str,
) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "kind": "staging_external_slurm_infrastructure_verification",
        "candidate_sha": payload["candidate_sha"],
        "candidate_tree": payload["candidate_tree"],
        "generation": payload["generation"],
        "convergence_id": payload["convergence_id"],
        "requested_at": payload["requested_at"],
        "request_sha256": payload["request_sha256"],
        "receipt_path": str(_infrastructure_receipt_path(payload["candidate_sha"])),
        "payload_sha256": payload_sha256,
        "source_controller": payload["source_controller"],
        "source_controller_host": payload["source_controller_host"],
        "created_at": payload["created_at"],
        "expires_at": payload["expires_at"],
        "source_bootstrap": _infrastructure_transport_summary(payload["source_bootstrap"]),
        "accounting": _infrastructure_transport_summary(payload["accounting"]),
        "node_bootstraps": [
            _infrastructure_transport_summary(receipt) for receipt in payload["node_bootstraps"]
        ],
        "mount_contract": payload["mount_contract"],
        "node_count": len(payload["node_bootstraps"]),
        "result": "pass",
    }


def verify_infrastructure(
    config: ExternalSlurmAuthorityConfig,
    *,
    candidate_sha: str,
    candidate_tree: str,
) -> dict[str, Any]:
    _require_root()
    _require_source_host(config)
    payload, payload_sha256 = _load_infrastructure_receipt(
        config,
        candidate_sha=candidate_sha,
        candidate_tree=candidate_tree,
    )
    return _infrastructure_summary(payload, payload_sha256=payload_sha256)


def _infrastructure_convergence_envelope(
    *,
    candidate_sha: str,
    candidate_tree: str,
    convergence_id: str,
    requested_at: str,
) -> bytes:
    inner = canonical_json_bytes(
        {
            "schema_version": 1,
            "kind": "loom.staging-external-slurm.infrastructure-converge-request",
            "candidate_sha": candidate_sha,
            "candidate_tree": candidate_tree,
            "convergence_id": convergence_id,
            "requested_at": requested_at,
        }
    )
    unsigned = {
        "schema_version": 1,
        "action": "staging-infrastructure-converge",
        "node": "oldlab-2",
        "domain": "oldlab",
        "sandbox": "staging",
        "candidate_sha": candidate_sha,
        "candidate_tree": candidate_tree,
        "payload_kind": "staging-infrastructure-converge-request",
        "payload_sha256": hashlib.sha256(inner).hexdigest(),
        "payload_base64": base64.b64encode(inner).decode("ascii"),
        "prior_request_id": None,
    }
    return cast(
        bytes,
        canonical_json_bytes(
            {
                **unsigned,
                "request_id": hashlib.sha256(canonical_json_bytes(unsigned)).hexdigest(),
            }
        ),
    )


def _commit_infrastructure_convergence_journal(
    journal_path: Path,
    journal: dict[str, Any],
) -> None:
    metadata = journal_path.lstat()
    if (
        not stat.S_ISREG(metadata.st_mode)
        or stat.S_ISLNK(metadata.st_mode)
        or metadata.st_uid != 0
        or metadata.st_gid != 0
        or metadata.st_nlink != 1
        or stat.S_IMODE(metadata.st_mode) != 0o600
        or journal_path.read_bytes() != canonical_json_bytes(journal)
    ):
        raise ExternalSlurmAcceptanceError(
            "infrastructure convergence journal changed before commit"
        )
    journal_path.unlink()
    directory = os.open(
        journal_path.parent,
        os.O_RDONLY | getattr(os, "O_DIRECTORY", 0),
    )
    try:
        os.fsync(directory)
    finally:
        os.close(directory)


def converge_infrastructure(
    config: ExternalSlurmAuthorityConfig,
    *,
    candidate_sha: str,
    candidate_tree: str,
) -> dict[str, Any]:
    _require_root()
    _require_source_host(config)
    if (
        _OBJECT_ID_RE.fullmatch(candidate_sha) is None
        or _OBJECT_ID_RE.fullmatch(candidate_tree) is None
    ):
        raise ExternalSlurmAcceptanceError(
            "infrastructure convergence requires the exact candidate identity"
        )
    descriptor = _publisher_lock(config)
    try:
        journal_path = _state_dir(config) / f"infrastructure-convergence-{candidate_sha}.json"
        journal_fields = {
            "schema_version",
            "candidate_sha",
            "candidate_tree",
            "convergence_id",
            "requested_at",
        }
        if journal_path.exists():
            try:
                journal = json.loads(journal_path.read_bytes())
            except (OSError, json.JSONDecodeError) as exc:
                raise ExternalSlurmAcceptanceError(
                    "infrastructure convergence journal is invalid"
                ) from exc
            if (
                not isinstance(journal, dict)
                or set(journal) != journal_fields
                or canonical_json_bytes(journal) != journal_path.read_bytes()
                or journal.get("candidate_sha") != candidate_sha
                or journal.get("candidate_tree") != candidate_tree
                or _DIGEST_RE.fullmatch(str(journal.get("convergence_id"))) is None
            ):
                raise ExternalSlurmAcceptanceError(
                    "infrastructure convergence journal binding mismatch"
                )
            _parse_utc(
                journal.get("requested_at"),
                label="infrastructure convergence requested_at",
            )
        else:
            requested_at = _timestamp()
            convergence_id = hashlib.sha256(
                canonical_json_bytes(
                    {
                        "candidate_sha": candidate_sha,
                        "candidate_tree": candidate_tree,
                        "requested_at": requested_at,
                        "nonce": secrets.token_hex(32),
                    }
                )
            ).hexdigest()
            journal = {
                "schema_version": 1,
                "candidate_sha": candidate_sha,
                "candidate_tree": candidate_tree,
                "convergence_id": convergence_id,
                "requested_at": requested_at,
            }
            _write_root_file(
                journal_path,
                canonical_json_bytes(journal),
                no_replace=True,
            )
        envelope = _infrastructure_convergence_envelope(
            candidate_sha=candidate_sha,
            candidate_tree=candidate_tree,
            convergence_id=str(journal["convergence_id"]),
            requested_at=str(journal["requested_at"]),
        )
        expected = json.loads(envelope)
        completed = _run(
            _transport_argv(config, node="oldlab-2"),
            timeout=_INFRASTRUCTURE_CONVERGE_TRANSPORT_TIMEOUT_SECONDS,
            input_text=envelope.decode("ascii"),
        )
        if completed.returncode != 0 or completed.stderr:
            raise ExternalSlurmAcceptanceError(
                "fixed oldlab-2 infrastructure convergence failed safely"
            )
        try:
            transport_receipt = json.loads(completed.stdout)
        except json.JSONDecodeError as exc:
            raise ExternalSlurmAcceptanceError(
                "fixed oldlab-2 infrastructure convergence returned invalid JSON"
            ) from exc
        if (
            not isinstance(transport_receipt, dict)
            or set(transport_receipt) != _INFRASTRUCTURE_TRANSPORT_FIELDS
            or any(
                transport_receipt.get(field) != expected[field]
                for field in (
                    "schema_version",
                    "request_id",
                    "action",
                    "node",
                    "domain",
                    "sandbox",
                    "candidate_sha",
                    "candidate_tree",
                    "payload_sha256",
                )
            )
            or _DIGEST_RE.fullmatch(str(transport_receipt.get("result_sha256"))) is None
            or transport_receipt.get("inner_receipt")
            != f"staging-infrastructure/v1/{journal['convergence_id']}"
            or transport_receipt.get("status") != "succeeded"
            or _parse_utc(
                transport_receipt.get("completed_at"),
                label="infrastructure convergence transport completed_at",
            )
            is None
        ):
            raise ExternalSlurmAcceptanceError(
                "fixed oldlab-2 infrastructure convergence receipt mismatch"
            )
        verify_infrastructure(
            config,
            candidate_sha=candidate_sha,
            candidate_tree=candidate_tree,
        )
        bootstrap(
            config,
            candidate_sha=candidate_sha,
            candidate_tree=candidate_tree,
        )
        verification = verify_infrastructure(
            config,
            candidate_sha=candidate_sha,
            candidate_tree=candidate_tree,
        )
        _commit_infrastructure_convergence_journal(journal_path, journal)
    finally:
        os.close(descriptor)
    return {
        "schema_version": 1,
        "kind": "staging_external_slurm_infrastructure_convergence",
        "candidate_sha": candidate_sha,
        "candidate_tree": candidate_tree,
        "convergence_id": journal["convergence_id"],
        "requested_at": journal["requested_at"],
        "generation": verification["generation"],
        "receipt_path": verification["receipt_path"],
        "receipt_sha256": verification["payload_sha256"],
        "node_count": verification["node_count"],
        "source_controller": verification["source_controller"],
        "source_controller_host": verification["source_controller_host"],
        "bootstrap_status": "converged",
        "result": "pass",
    }


def _probe_envelope(
    config: ExternalSlurmAuthorityConfig,
    prepared: dict[str, Any],
    *,
    probe_id: str,
) -> bytes:
    inner = canonical_json_bytes(
        {
            "schema_version": 1,
            "kind": "staging_external_slurm_allocation_probe_request",
            "request_id": probe_id,
            "candidate_sha": prepared["candidate_sha"],
            "candidate_tree": prepared["candidate_tree"],
        }
    )
    unsigned = {
        "schema_version": 1,
        "action": config.probe_action,
        "node": config.broker_node,
        "domain": config.broker_domain,
        "sandbox": config.broker_sandbox,
        "candidate_sha": prepared["candidate_sha"],
        "candidate_tree": prepared["candidate_tree"],
        "payload_kind": "staging-allocation-probe-request",
        "payload_sha256": hashlib.sha256(inner).hexdigest(),
        "payload_base64": base64.b64encode(inner).decode("ascii"),
        "prior_request_id": None,
    }
    return cast(
        bytes,
        canonical_json_bytes(
            {
                **unsigned,
                "request_id": hashlib.sha256(canonical_json_bytes(unsigned)).hexdigest(),
            }
        ),
    )


def _load_probe_result(
    config: ExternalSlurmAuthorityConfig,
    *,
    probe_id: str,
    inner_receipt: str,
    result_sha256: str,
) -> dict[str, Any]:
    expected = config.probe_result_root / probe_id / "probe.json"
    if inner_receipt != f"staging-probe/v1/{probe_id}":
        raise ExternalSlurmAcceptanceError("fixed allocation probe receipt binding mismatch")
    payload = _bounded_probe_read(expected)
    if hashlib.sha256(payload.encode("utf-8")).hexdigest() != result_sha256:
        raise ExternalSlurmAcceptanceError("fixed allocation probe artifact digest mismatch")
    try:
        parsed = json.loads(payload)
    except json.JSONDecodeError as exc:
        raise ExternalSlurmAcceptanceError(
            "fixed allocation probe artifact is invalid JSON"
        ) from exc
    if not isinstance(parsed, dict) or canonical_json_bytes(parsed) != payload.encode("utf-8"):
        raise ExternalSlurmAcceptanceError("fixed allocation probe artifact is not canonical")
    return parsed


def _bounded_probe_read(path: Path) -> str:
    try:
        metadata = path.lstat()
    except OSError as exc:
        raise ExternalSlurmAcceptanceError(
            "fixed allocation probe artifact is unavailable"
        ) from exc
    if (
        not stat.S_ISREG(metadata.st_mode)
        or stat.S_ISLNK(metadata.st_mode)
        or metadata.st_nlink != 1
        or metadata.st_size > _MAX_PROBE_BYTES
    ):
        raise ExternalSlurmAcceptanceError("fixed allocation probe artifact is unsafe")
    try:
        payload = path.read_text(encoding="utf-8")
    except (OSError, UnicodeDecodeError) as exc:
        raise ExternalSlurmAcceptanceError(
            "fixed allocation probe artifact cannot be read"
        ) from exc
    if len(payload.encode("utf-8")) > _MAX_PROBE_BYTES:
        raise ExternalSlurmAcceptanceError("fixed allocation probe artifact exceeds its size limit")
    return payload


def _load_private_key(config: ExternalSlurmAuthorityConfig) -> Ed25519PrivateKey:
    private_bytes = _read_key_leaf(
        config.private_key,
        label="authority private key",
        mode=0o600,
    )
    try:
        key = serialization.load_pem_private_key(
            private_bytes,
            password=None,
        )
    except (TypeError, ValueError) as exc:
        raise ExternalSlurmAcceptanceError("authority private key is invalid") from exc
    if not isinstance(key, Ed25519PrivateKey):
        raise ExternalSlurmAcceptanceError("authority private key must be Ed25519")
    return key


def _verified_signing_key(
    config: ExternalSlurmAuthorityConfig,
) -> tuple[Ed25519PrivateKey, str]:
    public_bytes = _read_key_leaf(
        config.public_key,
        label="authority public key",
        mode=0o644,
    )
    try:
        public = serialization.load_pem_public_key(public_bytes)
    except ValueError as exc:
        raise ExternalSlurmAcceptanceError("authority public key is invalid") from exc
    if not isinstance(public, Ed25519PublicKey):
        raise ExternalSlurmAcceptanceError("authority public key must be Ed25519")
    private = _load_private_key(config)
    private_public = private.public_key().public_bytes(
        serialization.Encoding.Raw,
        serialization.PublicFormat.Raw,
    )
    installed_public = public.public_bytes(
        serialization.Encoding.Raw,
        serialization.PublicFormat.Raw,
    )
    if private_public != installed_public:
        raise ExternalSlurmAcceptanceError("authority private/public key pair does not match")
    challenge = (
        b"loom-staging-external-slurm-authority-key-readback-v1\0"
        + secrets.token_bytes(32)
    )
    try:
        public.verify(private.sign(challenge), challenge)
    except InvalidSignature as exc:
        raise ExternalSlurmAcceptanceError(
            "authority private/public key signing readback failed"
        ) from exc
    return private, hashlib.sha256(public_bytes).hexdigest()


def _next_generation(config: ExternalSlurmAuthorityConfig) -> int:
    current_path = config.artifact_root / "current.json"
    high_water_path = _state_dir(config) / "generation-high-water.json"
    current = _load_generation_record(
        current_path,
        label="authority current pointer",
        include_kind=False,
        missing_ok=True,
    )
    high_water = _load_generation_record(
        high_water_path,
        label="authority generation high-water",
        include_kind=True,
        missing_ok=True,
    )
    if high_water is None:
        if current is not None:
            raise ExternalSlurmAcceptanceError(
                "authority current pointer exists without its monotonic high-water"
            )
        return 1
    pointer = {key: value for key, value in high_water.items() if key != "kind"}
    artifact_path, signature_path = authority_paths(
        config,
        str(pointer["candidate_sha"]),
        str(pointer["generation_id"]),
    )
    artifact = _root_generation_file(
        artifact_path,
        label="high-water authority artifact",
        maximum=2 * 1024 * 1024,
    )
    signature_encoded = _root_generation_file(
        signature_path,
        label="high-water authority signature",
        maximum=512,
    )
    if not signature_encoded.endswith(b"\n") or signature_encoded.count(b"\n") != 1:
        raise ExternalSlurmAcceptanceError("high-water authority signature is invalid")
    try:
        raw_signature = base64.b64decode(
            signature_encoded[:-1],
            validate=True,
        )
    except (binascii.Error, ValueError) as exc:
        raise ExternalSlurmAcceptanceError("high-water authority signature is invalid") from exc
    if (
        hashlib.sha256(artifact).hexdigest() != pointer["artifact_sha256"]
        or hashlib.sha256(raw_signature).hexdigest() != pointer["signature_sha256"]
    ):
        raise ExternalSlurmAcceptanceError(
            "authority generation high-water immutable content drifted"
        )
    _verify_private_leaf(
        config.public_key,
        uid=0,
        gid=0,
        label="authority public key",
        mode=0o644,
    )
    public_bytes = config.public_key.read_bytes()
    try:
        public = serialization.load_pem_public_key(public_bytes)
    except ValueError as exc:
        raise ExternalSlurmAcceptanceError("authority public key is invalid") from exc
    if (
        not isinstance(public, Ed25519PublicKey)
        or hashlib.sha256(public_bytes).hexdigest() != pointer["key_id"]
    ):
        raise ExternalSlurmAcceptanceError("authority generation high-water key binding drifted")
    try:
        public.verify(raw_signature, artifact)
    except InvalidSignature as exc:
        raise ExternalSlurmAcceptanceError(
            "authority generation high-water signature verification failed"
        ) from exc
    try:
        artifact_payload = json.loads(artifact)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ExternalSlurmAcceptanceError(
            "authority generation high-water artifact is invalid"
        ) from exc
    if (
        not isinstance(artifact_payload, dict)
        or canonical_json_bytes(artifact_payload) != artifact
        or any(
            artifact_payload.get(field) != pointer[field]
            for field in (
                "candidate_sha",
                "candidate_tree",
                "generation",
                "generation_id",
                "created_at",
                "expires_at",
            )
        )
    ):
        raise ExternalSlurmAcceptanceError(
            "authority generation high-water payload binding drifted"
        )
    if current is None or current["generation"] < pointer["generation"]:
        _write_root_file(current_path, canonical_json_bytes(pointer))
    elif current != pointer:
        raise ExternalSlurmAcceptanceError(
            "authority current pointer conflicts with its monotonic high-water"
        )
    return int(pointer["generation"]) + 1


def _load_generation_record(
    path: Path,
    *,
    label: str,
    include_kind: bool,
    missing_ok: bool,
) -> dict[str, Any] | None:
    try:
        metadata = path.lstat()
    except FileNotFoundError:
        if missing_ok:
            return None
        raise ExternalSlurmAcceptanceError(f"{label} is unavailable") from None
    except OSError as exc:
        raise ExternalSlurmAcceptanceError(f"{label} is unavailable") from exc
    if (
        not stat.S_ISREG(metadata.st_mode)
        or stat.S_ISLNK(metadata.st_mode)
        or metadata.st_uid != 0
        or metadata.st_gid != 0
        or metadata.st_nlink != 1
        or stat.S_IMODE(metadata.st_mode) != 0o600
    ):
        raise ExternalSlurmAcceptanceError(f"{label} metadata is unsafe")
    raw = path.read_bytes()
    try:
        pointer = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ExternalSlurmAcceptanceError(f"{label} is invalid") from exc
    expected_fields = {
        "schema_version",
        "candidate_sha",
        "candidate_tree",
        "generation",
        "generation_id",
        "artifact_sha256",
        "signature_sha256",
        "key_id",
        "created_at",
        "expires_at",
    }
    if include_kind:
        expected_fields.add("kind")
    if (
        not isinstance(pointer, dict)
        or set(pointer) != expected_fields
        or canonical_json_bytes(pointer) != raw
        or pointer.get("schema_version") != 1
        or (
            include_kind
            and pointer.get("kind") != "staging_external_slurm_acceptance_generation_high_water"
        )
        or isinstance(pointer.get("generation"), bool)
        or not isinstance(pointer.get("generation"), int)
        or pointer["generation"] < 1
        or _OBJECT_ID_RE.fullmatch(str(pointer.get("candidate_sha"))) is None
        or _OBJECT_ID_RE.fullmatch(str(pointer.get("candidate_tree"))) is None
        or any(
            _DIGEST_RE.fullmatch(str(pointer.get(field))) is None
            for field in (
                "generation_id",
                "artifact_sha256",
                "signature_sha256",
                "key_id",
            )
        )
        or not isinstance(pointer.get("created_at"), str)
        or not isinstance(pointer.get("expires_at"), str)
    ):
        raise ExternalSlurmAcceptanceError(f"{label} binding is invalid")
    return pointer


def _root_generation_file(
    path: Path,
    *,
    label: str,
    maximum: int,
) -> bytes:
    try:
        metadata = path.lstat()
        payload = path.read_bytes()
    except OSError as exc:
        raise ExternalSlurmAcceptanceError(f"{label} is unavailable") from exc
    if (
        not stat.S_ISREG(metadata.st_mode)
        or stat.S_ISLNK(metadata.st_mode)
        or metadata.st_uid != 0
        or metadata.st_gid != 0
        or metadata.st_nlink != 1
        or stat.S_IMODE(metadata.st_mode) != 0o600
        or len(payload) > maximum
    ):
        raise ExternalSlurmAcceptanceError(f"{label} metadata is unsafe")
    return payload


def _publish_generation(
    config: ExternalSlurmAuthorityConfig,
    *,
    candidate_sha: str,
    generation_id: str,
    artifact: bytes,
    signature: bytes,
    pointer: dict[str, Any],
) -> None:
    artifact_path, _signature_path = authority_paths(
        config,
        candidate_sha,
        generation_id,
    )
    final = artifact_path.parent
    generations = final.parent
    _safe_private_dir(generations)
    stage = generations / f".generation-{generation_id}.tmp-{os.getpid()}"
    try:
        stage.mkdir(mode=0o700)
    except FileExistsError as exc:
        raise ExternalSlurmAcceptanceError("authority generation stage already exists") from exc
    try:
        _write_root_file(stage / "acceptance.json", artifact, no_replace=True)
        _write_root_file(stage / "acceptance.sig", signature, no_replace=True)
        directory = os.open(stage, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
        _rename_noreplace(stage, final)
        directory = os.open(generations, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    except Exception:
        if stage.exists():
            for child in (stage / "acceptance.json", stage / "acceptance.sig"):
                try:
                    child.unlink()
                except FileNotFoundError:
                    pass
            try:
                stage.rmdir()
            except FileNotFoundError:
                pass
        raise
    high_water = {
        **pointer,
        "kind": "staging_external_slurm_acceptance_generation_high_water",
    }
    _write_root_file(
        _state_dir(config) / "generation-high-water.json",
        canonical_json_bytes(high_water),
    )
    _write_root_file(
        config.artifact_root / "current.json",
        canonical_json_bytes(pointer),
    )


def probe(
    config: ExternalSlurmAuthorityConfig,
    *,
    candidate_sha: str,
    candidate_tree: str,
    image_tag: str,
) -> dict[str, Any]:
    _require_root()
    _require_source_host(config)
    prepared = _load_prepared(
        config,
        candidate_sha=candidate_sha,
        candidate_tree=candidate_tree,
        image_tag=image_tag,
    )
    if _sha256_file(config.environment_state_profile) != prepared["profile_sha256"]:
        raise ExternalSlurmAcceptanceError("environment-state profile changed after prepare")
    if _supervisor_state(config) != prepared["supervisor"]:
        raise ExternalSlurmAcceptanceError("supervisor state changed after prepare")
    probe_id = secrets.token_hex(32)
    envelope = _probe_envelope(config, prepared, probe_id=probe_id)
    outer_request_id = str(json.loads(envelope)["request_id"])
    completed = _run(
        _probe_argv(config),
        timeout=config.max_age_seconds,
        input_text=envelope.decode("ascii"),
    )
    if completed.returncode != 0 or len(completed.stdout.encode("utf-8")) > _MAX_PROBE_BYTES:
        raise ExternalSlurmAcceptanceError("fixed allocation probe failed")
    try:
        transport_receipt = json.loads(completed.stdout)
    except json.JSONDecodeError as exc:
        raise ExternalSlurmAcceptanceError(
            "fixed allocation probe transport returned invalid JSON"
        ) from exc
    receipt_fields = {
        "schema_version",
        "request_id",
        "action",
        "node",
        "domain",
        "sandbox",
        "candidate_sha",
        "candidate_tree",
        "payload_sha256",
        "result_sha256",
        "inner_receipt",
        "completed_at",
        "status",
    }
    if (
        not isinstance(transport_receipt, dict)
        or set(transport_receipt) != receipt_fields
        or transport_receipt.get("schema_version") != 1
        or transport_receipt.get("request_id") != outer_request_id
        or transport_receipt.get("action") != config.probe_action
        or transport_receipt.get("node") != config.broker_node
        or transport_receipt.get("domain") != config.broker_domain
        or transport_receipt.get("sandbox") != config.broker_sandbox
        or transport_receipt.get("candidate_sha") != candidate_sha
        or transport_receipt.get("candidate_tree") != candidate_tree
        or transport_receipt.get("payload_sha256")
        != hashlib.sha256(
            base64.b64decode(json.loads(envelope)["payload_base64"], validate=True)
        ).hexdigest()
        or transport_receipt.get("status") != "succeeded"
        or not isinstance(transport_receipt.get("inner_receipt"), str)
        or not isinstance(transport_receipt.get("completed_at"), str)
        or re.fullmatch(r"[0-9a-f]{64}", str(transport_receipt.get("result_sha256"))) is None
    ):
        raise ExternalSlurmAcceptanceError("fixed allocation probe transport receipt mismatch")
    probe_payload = _load_probe_result(
        config,
        probe_id=probe_id,
        inner_receipt=str(transport_receipt["inner_receipt"]),
        result_sha256=str(transport_receipt["result_sha256"]),
    )
    expected_probe = {
        "schema_version": 1,
        "kind": "staging_external_slurm_allocation_probe",
        "request_id": probe_id,
        "candidate_sha": candidate_sha,
        "candidate_tree": candidate_tree,
        "cluster": config.cluster,
        "pool": config.pool,
        "submit_host": config.submit_host,
        "controller": config.controller,
        "service_identity": prepared["service_identity"],
        "namespace": probe_payload.get("namespace"),
        "slurm_account": config.slurm_account,
        "qos": config.qos,
        "allowed_nodes": list(config.allowed_nodes),
        "repository": prepared["repository"],
        "worker_env": prepared["worker_env"],
        "nodes": probe_payload.get("nodes"),
        "result": "pass",
    }
    namespace = probe_payload.get("namespace")
    if (
        not isinstance(namespace, dict)
        or set(namespace)
        != {
            "root",
            "mount_source",
            "mount_fstype",
            "mount_device",
            "mount_inode",
            "repository_root",
            "worker_env_root",
            "result_root",
            "service_uid",
            "service_gid",
            "root_mode",
            "repository_root_mode",
            "worker_env_root_mode",
            "result_root_mode",
        }
        or namespace.get("root") != str(config.shared_mount_target)
        or namespace.get("mount_source") != config.shared_mount_source
        or namespace.get("mount_fstype") != config.shared_mount_filesystem_type
        or not isinstance(namespace.get("mount_device"), int)
        or not isinstance(namespace.get("mount_inode"), int)
        or namespace.get("repository_root") != str(config.repository_root)
        or namespace.get("worker_env_root") != str(config.worker_env_root)
        or namespace.get("result_root") != str(config.result_root)
        or namespace.get("service_uid") != config.batch_uid
        or namespace.get("service_gid") != config.batch_gid
        or namespace.get("root_mode") != "0o750"
        or namespace.get("repository_root_mode") != "0o750"
        or namespace.get("worker_env_root_mode") != "0o750"
        or namespace.get("result_root_mode") != "0o2770"
    ):
        raise ExternalSlurmAcceptanceError("fixed allocation probe namespace binding mismatch")
    if probe_payload != expected_probe:
        raise ExternalSlurmAcceptanceError(
            "fixed allocation probe returned a mismatched closed receipt"
        )
    created_at = datetime.now(UTC)
    expires_at = created_at + timedelta(seconds=config.max_age_seconds)
    generation_id = probe_id
    descriptor = _publisher_lock(config)
    try:
        generation = _next_generation(config)
        payload = {
            "schema_version": 1,
            "kind": "staging_external_slurm_acceptance",
            "generation": generation,
            "generation_id": generation_id,
            "environment": config.environment,
            "pool": config.pool,
            "candidate_sha": candidate_sha,
            "candidate_tree": candidate_tree,
            "profile_sha256": prepared["profile_sha256"],
            "source_host": config.source_host,
            "created_at": _timestamp(created_at),
            "expires_at": _timestamp(expires_at),
            "service_identity": prepared["service_identity"],
            "cluster": config.cluster,
            "controller": config.controller,
            "submit_host": config.submit_host,
            "partition": config.partition,
            "slurm_account": config.slurm_account,
            "qos": config.qos,
            "allowed_nodes": list(config.allowed_nodes),
            "repository": prepared["repository"],
            "worker_env": prepared["worker_env"],
            "supervisor": prepared["supervisor"],
            "nodes": probe_payload.get("nodes"),
            "result": "pass",
        }
        validate_authority_payload(
            payload,
            config=config,
            candidate_sha=candidate_sha,
            candidate_tree=candidate_tree,
            profile_sha256=str(prepared["profile_sha256"]),
            now=created_at,
        )
        artifact = canonical_json_bytes(payload)
        private_key, key_id = _verified_signing_key(config)
        raw_signature = private_key.sign(artifact)
        signature = base64.b64encode(raw_signature) + b"\n"
        current = {
            "schema_version": 1,
            "candidate_sha": candidate_sha,
            "candidate_tree": candidate_tree,
            "generation": generation,
            "generation_id": generation_id,
            "artifact_sha256": hashlib.sha256(artifact).hexdigest(),
            "signature_sha256": hashlib.sha256(raw_signature).hexdigest(),
            "key_id": key_id,
            "created_at": payload["created_at"],
            "expires_at": payload["expires_at"],
        }
        _publish_generation(
            config,
            candidate_sha=candidate_sha,
            generation_id=generation_id,
            artifact=artifact,
            signature=signature,
            pointer=current,
        )
    finally:
        os.close(descriptor)
    verify_authority(
        config=config,
        candidate_sha=candidate_sha,
        candidate_tree=candidate_tree,
        profile_sha256=str(prepared["profile_sha256"]),
        now=created_at,
    )
    return payload


def activate(
    config: ExternalSlurmAuthorityConfig,
    *,
    candidate_sha: str,
    candidate_tree: str | None = None,
) -> dict[str, Any]:
    _require_source_host(config)
    profile_sha256 = _sha256_file(config.environment_state_profile)
    verified = verify_authority(
        config=config,
        candidate_sha=candidate_sha,
        candidate_tree=candidate_tree,
        profile_sha256=profile_sha256,
    )
    return {
        "result": "pass",
        "candidate_sha": verified.payload["candidate_sha"],
        "candidate_tree": verified.payload["candidate_tree"],
        "profile_sha256": verified.payload["profile_sha256"],
        "artifact_sha256": verified.artifact_sha256,
        "signature_sha256": verified.signature_sha256,
        "key_id": verified.key_id,
        "node_count": len(verified.payload["nodes"]),
        "expires_at": verified.payload["expires_at"],
    }


def verify_current(config: ExternalSlurmAuthorityConfig) -> dict[str, Any]:
    current_path = config.artifact_root / "current.json"
    try:
        current = json.loads(current_path.read_bytes())
    except (OSError, json.JSONDecodeError) as exc:
        raise ExternalSlurmAcceptanceError("current authority pointer is unavailable") from exc
    if not isinstance(current, dict):
        raise ExternalSlurmAcceptanceError("current authority pointer is invalid")
    result = activate(
        config,
        candidate_sha=str(current.get("candidate_sha") or ""),
        candidate_tree=str(current.get("candidate_tree") or ""),
    )
    if result["artifact_sha256"] != current.get("artifact_sha256"):
        raise ExternalSlurmAcceptanceError("current authority pointer digest mismatch")
    return result


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(allow_abbrev=False)
    subparsers = parser.add_subparsers(dest="command", required=True)
    bootstrap_parser = subparsers.add_parser("bootstrap", allow_abbrev=False)
    bootstrap_parser.add_argument("--candidate-sha", required=True)
    bootstrap_parser.add_argument("--candidate-tree", required=True)
    bootstrap_parser.add_argument("--execute", action="store_true")
    for command in ("prepare", "probe"):
        child = subparsers.add_parser(command, allow_abbrev=False)
        child.add_argument("--candidate-sha", required=True)
        child.add_argument("--candidate-tree", required=True)
        child.add_argument("--image-tag", required=True)
        child.add_argument("--execute", action="store_true")
    for command in ("activate", "verify"):
        child = subparsers.add_parser(command, allow_abbrev=False)
        child.add_argument("--candidate-sha", required=True)
        child.add_argument("--candidate-tree")
    infrastructure_parser = subparsers.add_parser(
        "verify-infrastructure",
        allow_abbrev=False,
    )
    infrastructure_parser.add_argument("--candidate-sha", required=True)
    infrastructure_parser.add_argument("--candidate-tree", required=True)
    converge_parser = subparsers.add_parser(
        "converge-infrastructure",
        allow_abbrev=False,
    )
    converge_parser.add_argument("--candidate-sha", required=True)
    converge_parser.add_argument("--candidate-tree", required=True)
    subparsers.add_parser("verify-current")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        config = load_authority_config(DEFAULT_CONFIG_PATH)
        if args.command in {"bootstrap", "prepare", "probe"} and not args.execute:
            raise ExternalSlurmAcceptanceError(
                f"{args.command} is a plan unless --execute is supplied"
            )
        if args.command == "bootstrap":
            result = bootstrap(
                config,
                candidate_sha=args.candidate_sha,
                candidate_tree=args.candidate_tree,
            )
        elif args.command == "prepare":
            result = prepare(
                config,
                candidate_sha=args.candidate_sha,
                candidate_tree=args.candidate_tree,
                image_tag=args.image_tag,
            )
        elif args.command == "probe":
            result = probe(
                config,
                candidate_sha=args.candidate_sha,
                candidate_tree=args.candidate_tree,
                image_tag=args.image_tag,
            )
        elif args.command in {"activate", "verify"}:
            result = activate(
                config,
                candidate_sha=args.candidate_sha,
                candidate_tree=args.candidate_tree,
            )
        elif args.command == "verify-infrastructure":
            result = verify_infrastructure(
                config,
                candidate_sha=args.candidate_sha,
                candidate_tree=args.candidate_tree,
            )
        elif args.command == "converge-infrastructure":
            result = converge_infrastructure(
                config,
                candidate_sha=args.candidate_sha,
                candidate_tree=args.candidate_tree,
            )
        else:
            result = verify_current(config)
    except ExternalSlurmAcceptanceError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    sys.stdout.buffer.write(canonical_json_bytes(result))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
