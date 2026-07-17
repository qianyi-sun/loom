"""Step 11 — environment-state apply + desired-state check (#340, #593).

Applies the release environment-state profile (from cluster-config's
declared path) and records an immediate check. Pure GB10 node-status drift is
deferred because GB10 prep now starts after desired state is written; final
node convergence is checked again by release-gate. The #331 fix to
environment-state apply ensures negative desired states (enabled=false /
active=false) actually stop and disable supervisors.
"""

from __future__ import annotations

import ctypes
import errno
import glob
import grp
import hashlib
import io
import json
import os
import pwd
import re
import secrets
import shlex
import socket
import stat
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
from collections.abc import Callable, Collection, Iterator, Sequence
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path, PurePosixPath
from typing import IO, Any
from urllib.parse import urlsplit, urlunsplit

from dotenv import dotenv_values

from loom.security.redaction import (
    is_sensitive_environment_key,
)
from loom.worker_token import DEFAULT_WORKER_TOKEN_ENV_KEY, worker_token_fingerprint
from loom_cli.rollout.context import RolloutContext
from loom_cli.rollout.evidence import StepDir
from loom_cli.rollout.operator.redaction import (
    redact_rollout_text,
    rollout_redaction_scope,
)
from loom_cli.rollout.steps.base import BaseStep, RunResult
from loom_cli.rollout.steps.candidate_source import (
    CandidateToolingError,
    candidate_loom_argv,
    candidate_loom_cwd,
    candidate_loom_env,
    candidate_relative_path,
    candidate_worktree,
    validate_candidate_loom_source,
)
from loom_cli.rollout.steps.subprocess_util import run_captured
from loom_cli.secret_source import SecretSourceError, resolve_secret_source


class ExternalSlurmPrereqMaterializationError(RuntimeError):
    """Raised when rollout cannot converge external Slurm runner prerequisites."""


class CatalogProvisioningError(RuntimeError):
    """Raised when rollout cannot safely run required catalog provisioning."""


class ControlPlaneReadinessError(RuntimeError):
    """Raised when the private control-plane URL does not become usable."""


_CONTROL_PLANE_READY_TIMEOUT_SECONDS = 60.0
_CONTROL_PLANE_READY_INTERVAL_SECONDS = 0.5
_MAX_CATALOG_SOURCE_BYTES = 1024 * 1024
_MAX_PORT_FORWARD_LOG_CHARS = 64 * 1024
_SHARED_WORKER_REPO_ROOT = Path("/shared_work/qianyi/.loom-staging-rollout/worker-repos")
_SHARED_WORKER_REPO_CONSUMER = PurePosixPath("scripts/ops/staging_rollout_shared_repo_consumer.py")
_GIT_OBJECT_ID_RE = re.compile(r"[0-9a-f]{40}\Z")
_CANONICAL_SHARED_REPO_GIT_CONFIG = (
    b"[core]\n"
    b"\trepositoryformatversion = 0\n"
    b"\tfilemode = true\n"
    b"\tbare = false\n"
    b"\tlogallrefupdates = true\n"
)
_TEST_RENAME_NOREPLACE_BACKEND: Callable[[int, str, int, str], None] | None = None
_OVERSIZED_PORT_FORWARD_OUTPUT = "[REDACTED:oversized-port-forward-output]\n"
_PORT_FORWARD_ENV_KEYS = frozenset(
    {
        "DBUS_SESSION_BUS_ADDRESS",
        "HOME",
        "KUBECONFIG",
        "LANG",
        "LC_ALL",
        "LOGNAME",
        "PATH",
        "USER",
        "XDG_RUNTIME_DIR",
    }
)


def _safe_text(value: object, *, known_values: Sequence[str] = ()) -> str:
    return redact_rollout_text(str(value), known_secrets=known_values)


def _write_safe_text(
    path: Path,
    value: object,
    *,
    known_values: Sequence[str] = (),
) -> None:
    path.write_text(
        _safe_text(value, known_values=known_values),
        encoding="utf-8",
    )


def _write_safe_json(
    path: Path,
    value: object,
    *,
    known_values: Sequence[str] = (),
) -> None:
    _write_safe_text(
        path,
        json.dumps(value, indent=2, sort_keys=True) + "\n",
        known_values=known_values,
    )


@dataclass(frozen=True)
class CatalogKubernetesPortForward:
    namespace: str
    postgres_service: str
    postgres_remote_port: int
    minio_service: str
    minio_remote_port: int


@dataclass
class _BoundedRedactedCapture:
    """Drain a child pipe without ever redacting a structurally truncated value."""

    stream: IO[str]
    known_values: tuple[str, ...] = field(repr=False)
    limit: int = _MAX_PORT_FORWARD_LOG_CHARS
    _buffer: str = field(default="", init=False, repr=False)
    _oversized: bool = field(default=False, init=False, repr=False)
    _lock: threading.Lock = field(
        default_factory=threading.Lock,
        init=False,
        repr=False,
    )
    _thread: threading.Thread = field(init=False, repr=False)

    def __post_init__(self) -> None:
        self._thread = threading.Thread(target=self._drain, daemon=True)

    def start(self) -> None:
        self._thread.start()

    def _drain(self) -> None:
        while True:
            chunk = self.stream.read(8192)
            if not chunk:
                return
            with self._lock:
                if self._oversized:
                    continue
                if len(self._buffer) + len(chunk) > self.limit:
                    self._buffer = ""
                    self._oversized = True
                    continue
                self._buffer += chunk

    def rendered(self) -> str:
        self._thread.join(timeout=5)
        with self._lock:
            if self._oversized or self._thread.is_alive():
                return _OVERSIZED_PORT_FORWARD_OUTPUT
            raw = self._buffer
        return _safe_text(raw, known_values=self.known_values)


@dataclass(frozen=True)
class CatalogPortForwardHandle:
    name: str
    namespace: str
    resource: str
    remote_port: int
    local_port: int
    stdout_log: Path
    stderr_log: Path
    process: subprocess.Popen[str] | None = None
    stdout_handle: Any = None
    stderr_handle: Any = None
    known_values: tuple[str, ...] = field(default=(), repr=False)
    stdout_capture: _BoundedRedactedCapture | None = field(
        default=None,
        repr=False,
    )
    stderr_capture: _BoundedRedactedCapture | None = field(
        default=None,
        repr=False,
    )


@dataclass(frozen=True)
class CatalogProvisioningPlan:
    command: str = field(repr=False)
    env: dict[str, str] = field(repr=False)
    required_env: list[str]
    env_file: dict[str, Any] | None
    env_sources: dict[str, str]
    kubernetes_port_forward: CatalogKubernetesPortForward | None
    protected_values: tuple[str, ...] = field(default=(), repr=False)


def _is_gb10_node_status_drift_only(stdout: str) -> bool:
    try:
        payload = json.loads(stdout)
    except json.JSONDecodeError:
        return False
    autoscaler_blockers = payload.get("autoscaler_blockers", [])
    if not isinstance(autoscaler_blockers, list) or autoscaler_blockers:
        return False
    drift = payload.get("drift")
    if not isinstance(drift, list) or not drift:
        return False
    for item in drift:
        if not isinstance(item, dict):
            return False
        path = item.get("path")
        if not isinstance(path, str):
            return False
        if not path.startswith("gb10_worker_node_status["):
            return False
    return True


def _broker_mutation_args(ctx: RolloutContext) -> list[str]:
    if ctx.request_envelope_path is None:
        return []
    return [
        "--rollout-request-envelope",
        str(ctx.request_envelope_path),
    ]


def environment_state_check_argv(
    ctx: RolloutContext,
    step_dir: StepDir,
    *,
    profile_path: Path | None = None,
) -> Sequence[str] | None:
    if profile_path is None:
        profile = _profile_path_for(ctx)
        if profile is None:
            return None
        profile_path = candidate_relative_path(Path(profile), step_dir)
    release_vars = [
        "--var",
        f"IMAGE_TAG={ctx.image_tag}",
        "--var",
        f"ENV_CONFIG_VERSION={ctx.image_tag}",
        "--var",
        f"GIT_SHA={ctx.resolved_sha}",
    ]
    admin_args = [
        "--admin-token",
        ctx.admin_token_source,
    ]
    if ctx.expect_admin_token_fingerprint:
        admin_args.extend(
            [
                "--expect-admin-token-fingerprint",
                ctx.expect_admin_token_fingerprint,
            ]
        )
    worker_check_args: list[str] = []
    if ctx.worker_token_source:
        worker_check_args.extend(
            [
                "--worker-token",
                ctx.worker_token_source,
            ]
        )
    return candidate_loom_argv(
        "admin",
        "environment-state",
        "check",
        "--cp-url",
        ctx.cp_url,
        *admin_args,
        "--file",
        str(profile_path),
        "--environment",
        ctx.environment,
        *_broker_mutation_args(ctx),
        *release_vars,
        *worker_check_args,
        "--format",
        "json",
    )


def _profile_path_for(ctx: RolloutContext, config_path: Path | None = None) -> str | None:
    """Locate the environment-state TOML for the target scope.

    Convention: cluster-config declares ``env_state_profile`` (a path
    resolved relative to cluster-config's own dir). If unset, returns
    None → the step is a no-op.
    """
    from loom_cli.cluster_config import load_cluster_config

    source_config_path = config_path or ctx.cluster_config_path
    try:
        cfg = load_cluster_config(source_config_path)
    except Exception:
        return None
    profile = getattr(cfg, "env_state_profile", None)
    if not profile:
        return None
    profile_path = Path(str(profile)).expanduser()
    if not profile_path.is_absolute():
        profile_path = source_config_path.parent / profile_path
    return str(profile_path.resolve(strict=False))


def _candidate_profile_path(ctx: RolloutContext, step_dir: StepDir) -> Path | None:
    """Resolve env-state input only from the pinned candidate cluster config."""
    from loom_cli.cluster_config import load_cluster_config

    candidate_root = candidate_worktree(step_dir).resolve(strict=False)
    mapped_config = candidate_relative_path(ctx.cluster_config_path, step_dir)
    candidate_config = mapped_config
    if not candidate_config.is_absolute():
        candidate_config = candidate_root / candidate_config
    candidate_config = Path(os.path.normpath(candidate_config))
    try:
        candidate_config.relative_to(candidate_root)
    except ValueError as exc:
        raise CandidateToolingError(
            "pinned candidate cluster config is outside the candidate worktree"
        ) from exc
    try:
        config_metadata = candidate_config.lstat()
    except (OSError, ValueError) as exc:
        raise CandidateToolingError(
            "pinned candidate cluster config is unavailable or outside the candidate worktree"
        ) from exc
    if not stat.S_ISREG(config_metadata.st_mode):
        raise CandidateToolingError(
            "pinned candidate cluster config must be a regular file, not a symlink"
        )
    candidate_config = candidate_config.resolve(strict=False)
    try:
        candidate_config.relative_to(candidate_root)
        config = load_cluster_config(candidate_config)
    except (OSError, ValueError) as exc:
        raise CandidateToolingError(
            "pinned candidate cluster config is invalid or outside the candidate worktree"
        ) from exc

    profile_value = getattr(config, "env_state_profile", None)
    if not profile_value:
        return None
    profile_path = Path(str(profile_value)).expanduser()
    if not profile_path.is_absolute():
        profile_path = candidate_config.parent / profile_path
    profile_path = Path(os.path.normpath(profile_path))
    try:
        profile_path.relative_to(candidate_root)
    except ValueError as exc:
        raise CandidateToolingError(
            "pinned candidate environment-state profile is outside the candidate worktree"
        ) from exc
    try:
        profile_metadata = profile_path.lstat()
    except (OSError, ValueError) as exc:
        raise CandidateToolingError(
            "pinned candidate environment-state profile is unavailable or outside "
            "the candidate worktree"
        ) from exc
    if not stat.S_ISREG(profile_metadata.st_mode):
        raise CandidateToolingError(
            "pinned candidate environment-state profile must be a regular file, not a symlink"
        )
    resolved_profile = profile_path.resolve(strict=False)
    try:
        resolved_profile.relative_to(candidate_root)
    except ValueError as exc:
        raise CandidateToolingError(
            "pinned candidate environment-state profile is outside the candidate worktree"
        ) from exc
    return resolved_profile


def _release_vars(ctx: RolloutContext) -> dict[str, str]:
    return {
        "IMAGE_TAG": ctx.image_tag,
        "ENV_CONFIG_VERSION": ctx.image_tag,
        "GIT_SHA": ctx.resolved_sha,
    }


def _sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _materialize_rollout_root_profile(
    ctx: RolloutContext,
    *,
    source_profile: Path,
    step_dir: StepDir,
) -> dict[str, Any]:
    target = ctx.rollout_root / "environment-state" / f"{ctx.environment}.toml"
    source_bytes = source_profile.read_bytes()
    target.parent.mkdir(parents=True, exist_ok=True)
    changed = True
    existing_profile_readable: bool | None = None
    try:
        target_metadata = target.lstat()
    except FileNotFoundError:
        target_metadata = None
    except OSError as exc:
        raise CandidateToolingError(
            "rollout-root environment-state profile cannot be inspected safely"
        ) from exc
    if target_metadata is not None:
        if not stat.S_ISREG(target_metadata.st_mode):
            raise CandidateToolingError(
                "rollout-root environment-state profile must be a regular file, "
                "not a symlink or other non-regular entry"
            )
        try:
            changed = target.read_bytes() != source_bytes
        except PermissionError:
            # A pre-broker operator-owned 0600 leaf may be unreadable even
            # though the reviewed directory ACL permits this service account
            # to create and atomically replace entries. Treat that legacy leaf
            # as stale; do not broaden its ACL or skip candidate materialization.
            existing_profile_readable = False
        else:
            existing_profile_readable = True
    if changed:
        tmp = target.with_name(f".{target.name}.tmp")
        tmp.write_bytes(source_bytes)
        tmp.chmod(0o600)
        tmp.replace(target)
    else:
        target.chmod(0o600)
    evidence = {
        "changed": changed,
        "existing_profile_readable": existing_profile_readable,
        "mode": oct(target.stat().st_mode & 0o777),
        "source_path": str(source_profile),
        "source_sha256": _sha256_bytes(source_bytes),
        "target_path": str(target),
        "target_sha256": _sha256_bytes(target.read_bytes()),
    }
    _write_safe_json(
        step_dir.artifact_path("environment-state-profile-materialization.json"),
        evidence,
    )
    return evidence


def _string_list(value: object, field: str) -> list[str]:
    if value is None:
        return []
    if not isinstance(value, list):
        raise CatalogProvisioningError(f"{field} must be an array")
    out: list[str] = []
    for idx, item in enumerate(value):
        if not isinstance(item, str) or not item.strip():
            raise CatalogProvisioningError(f"{field}[{idx}] must be a non-empty string")
        out.append(item.strip())
    return out


def _catalog_source_identity(value: str) -> str:
    return "sha256:" + hashlib.sha256(value.encode("utf-8")).hexdigest()


def _read_private_catalog_source(path: Path, *, label: str) -> str:
    if not path.is_absolute() or ".." in path.parts:
        raise CatalogProvisioningError(f"{label} must be an absolute protected path")
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(path, flags)
    except OSError:
        raise CatalogProvisioningError(f"{label} is unavailable or unsafe") from None
    try:
        metadata = os.fstat(descriptor)
        mode = stat.S_IMODE(metadata.st_mode)
        if not stat.S_ISREG(metadata.st_mode):
            raise CatalogProvisioningError(f"{label} must be a regular file")
        if mode & 0o027:
            raise CatalogProvisioningError(
                f"{label} must not be group-writable/executable or world-accessible"
            )
        if metadata.st_size > _MAX_CATALOG_SOURCE_BYTES:
            raise CatalogProvisioningError(f"{label} exceeds the bounded size limit")
        payload = bytearray()
        while len(payload) <= _MAX_CATALOG_SOURCE_BYTES:
            chunk = os.read(
                descriptor,
                min(65536, _MAX_CATALOG_SOURCE_BYTES + 1 - len(payload)),
            )
            if not chunk:
                break
            payload.extend(chunk)
        if len(payload) > _MAX_CATALOG_SOURCE_BYTES:
            raise CatalogProvisioningError(f"{label} exceeds the bounded size limit")
        after = os.fstat(descriptor)
        if (
            metadata.st_dev,
            metadata.st_ino,
            metadata.st_size,
            metadata.st_mtime_ns,
        ) != (
            after.st_dev,
            after.st_ino,
            after.st_size,
            after.st_mtime_ns,
        ):
            raise CatalogProvisioningError(f"{label} changed while it was read")
        try:
            return bytes(payload).decode("utf-8")
        except UnicodeDecodeError:
            raise CatalogProvisioningError(f"{label} must be valid UTF-8") from None
    finally:
        os.close(descriptor)


def _catalog_env_file(
    catalog: dict[str, Any],
) -> tuple[dict[str, str], dict[str, Any] | None, tuple[str, ...]]:
    raw_path = catalog.get("env_file")
    if raw_path is None:
        return {}, None, ()
    if not isinstance(raw_path, str) or not raw_path.strip():
        raise CatalogProvisioningError("catalog_provisioning.env_file must be a non-empty string")
    path = Path(raw_path).expanduser()
    decoded = _read_private_catalog_source(
        path,
        label="catalog provisioning env_file",
    )
    values = {
        key: str(value)
        for key, value in dotenv_values(stream=io.StringIO(decoded)).items()
        if key and value is not None
    }
    evidence = {
        "source_identity": _catalog_source_identity(str(path)),
        "key_count": len(values),
        "keys": sorted(values),
    }
    return values, evidence, (str(path), f"file:{path}")


def _catalog_env_sources(
    catalog: dict[str, Any],
) -> tuple[dict[str, str], dict[str, str], tuple[str, ...]]:
    raw_sources = catalog.get("env_sources", {})
    if raw_sources is None:
        return {}, {}, ()
    if not isinstance(raw_sources, dict):
        raise CatalogProvisioningError("catalog_provisioning.env_sources must be a table")
    resolved: dict[str, str] = {}
    evidence: dict[str, str] = {}
    protected: list[str] = []
    for raw_key, raw_source in raw_sources.items():
        key = str(raw_key).strip()
        if not key:
            raise CatalogProvisioningError(
                "catalog_provisioning.env_sources keys must be non-empty",
            )
        if not isinstance(raw_source, str) or not raw_source.strip():
            raise CatalogProvisioningError(
                f"catalog_provisioning.env_sources.{key} must be a non-empty string",
            )
        source = raw_source.strip()
        if source == "-":
            raise CatalogProvisioningError(
                f"catalog_provisioning.env_sources.{key} cannot use stdin '-' "
                "during unattended rollout; use env:VAR or file:PATH",
            )
        try:
            if source.startswith("file:"):
                rendered_path = source.removeprefix("file:")
                value = _read_private_catalog_source(
                    Path(rendered_path).expanduser(),
                    label=f"catalog_provisioning.env_sources.{key}",
                ).strip()
                protected.append(rendered_path)
            elif source.startswith("env:"):
                value = resolve_secret_source(
                    source,
                    flag_name=f"catalog_provisioning.env_sources.{key}",
                )
            else:
                raise CatalogProvisioningError(
                    f"catalog_provisioning.env_sources.{key} must use env:VAR or file:PATH"
                )
        except (CatalogProvisioningError, SecretSourceError):
            raise CatalogProvisioningError(
                f"catalog_provisioning.env_sources.{key} could not be resolved safely"
            ) from None
        if not value:
            raise CatalogProvisioningError(
                f"catalog_provisioning.env_sources.{key} resolved to an empty value"
            )
        resolved[key] = value
        protected.extend((source, value))
        evidence[key] = _catalog_source_identity(source)
    return resolved, evidence, tuple(protected)


def _catalog_literal_env(catalog: dict[str, Any]) -> dict[str, str]:
    raw_env = catalog.get("env", {})
    if raw_env is None:
        return {}
    if not isinstance(raw_env, dict):
        raise CatalogProvisioningError("catalog_provisioning.env must be a table")
    out: dict[str, str] = {}
    for raw_key, raw_value in raw_env.items():
        key = str(raw_key).strip()
        if not key:
            raise CatalogProvisioningError("catalog_provisioning.env keys must be non-empty")
        if is_sensitive_environment_key(key):
            raise CatalogProvisioningError(
                f"catalog_provisioning.env.{key} is sensitive; use env_file or env_sources",
            )
        if not isinstance(raw_value, str) or not raw_value.strip():
            raise CatalogProvisioningError(
                f"catalog_provisioning.env.{key} must be a non-empty string",
            )
        out[key] = raw_value.strip()
    return out


def _catalog_port_forward_int(
    raw: object,
    *,
    field: str,
    default: int,
) -> int:
    value = default if raw is None else raw
    if not isinstance(value, int) or value <= 0 or value > 65535:
        raise CatalogProvisioningError(
            f"catalog_provisioning.kubernetes_port_forward.{field} must be a TCP port number",
        )
    return value


def _catalog_port_forward_resource(
    raw: object,
    *,
    field: str,
    default: str,
) -> str:
    value = default if raw is None else raw
    if not isinstance(value, str) or not value.strip():
        raise CatalogProvisioningError(
            f"catalog_provisioning.kubernetes_port_forward.{field} must be a non-empty string",
        )
    return value.strip()


def _catalog_kubernetes_port_forward(
    ctx: RolloutContext,
    catalog: dict[str, Any],
) -> CatalogKubernetesPortForward | None:
    raw = catalog.get("kubernetes_port_forward")
    if raw is None:
        return None
    if not isinstance(raw, dict):
        raise CatalogProvisioningError(
            "catalog_provisioning.kubernetes_port_forward must be a table",
        )
    if raw.get("enabled") is not True:
        return None
    return CatalogKubernetesPortForward(
        namespace=str(raw.get("namespace") or ctx.namespace),
        postgres_service=_catalog_port_forward_resource(
            raw.get("postgres_service"),
            field="postgres_service",
            default="service/loom-postgres",
        ),
        postgres_remote_port=_catalog_port_forward_int(
            raw.get("postgres_remote_port"),
            field="postgres_remote_port",
            default=5432,
        ),
        minio_service=_catalog_port_forward_resource(
            raw.get("minio_service"),
            field="minio_service",
            default="service/loom-minio",
        ),
        minio_remote_port=_catalog_port_forward_int(
            raw.get("minio_remote_port"),
            field="minio_remote_port",
            default=9000,
        ),
    )


def _catalog_provisioning_plan(
    ctx: RolloutContext,
    profile_path: Path | str,
    base_env: dict[str, str],
) -> CatalogProvisioningPlan | None:
    from loom_cli.environment_state import load_environment_state_profile

    try:
        profile = load_environment_state_profile(
            profile_path,
            variables=_release_vars(ctx),
            expected_environment=ctx.environment,
        )
    except Exception:
        raise CatalogProvisioningError("catalog provisioning profile is invalid") from None

    catalog = profile.catalog_provisioning
    if catalog.get("required") is not True:
        return None
    command = catalog.get("command")
    if not isinstance(command, str) or not command.strip():
        raise CatalogProvisioningError(
            "catalog_provisioning.required=true requires a non-empty command",
        )

    env = dict(base_env)
    env_file_values, env_file_evidence, env_file_protected = _catalog_env_file(catalog)
    env.update(env_file_values)
    env_sources, env_source_evidence, env_source_protected = _catalog_env_sources(catalog)
    env.update(env_sources)
    env.update(_catalog_literal_env(catalog))

    required_env = _string_list(catalog.get("required_env"), "catalog_provisioning.required_env")
    missing = [name for name in required_env if not env.get(name)]
    if missing:
        raise CatalogProvisioningError(
            "catalog provisioning missing required env: " + ", ".join(missing),
        )
    return CatalogProvisioningPlan(
        command=command.strip(),
        env=env,
        required_env=required_env,
        env_file=env_file_evidence,
        env_sources=env_source_evidence,
        kubernetes_port_forward=_catalog_kubernetes_port_forward(ctx, catalog),
        protected_values=(
            command.strip(),
            *env_file_protected,
            *env_source_protected,
            *tuple(value for value in env.values() if value),
        ),
    )


def _redact_catalog_output(
    text: str,
    *,
    env: dict[str, str],
    required_env: Sequence[str],
) -> str:
    del required_env
    redacted = text
    values = sorted(
        ((name, value) for name, value in env.items() if value),
        key=lambda item: len(item[1]),
        reverse=True,
    )
    for name, value in values:
        redacted = redacted.replace(value, f"[REDACTED:{name}]")
    return redact_rollout_text(redacted, known_secrets=env.values())


def _reserve_local_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _bounded_port_forward_env(source: dict[str, str]) -> dict[str, str]:
    return {key: source[key] for key in sorted(_PORT_FORWARD_ENV_KEYS) if source.get(key)}


def _flush_port_forward_output(
    handle: CatalogPortForwardHandle,
) -> tuple[str, str]:
    stdout = handle.stdout_capture.rendered() if handle.stdout_capture else ""
    stderr = handle.stderr_capture.rendered() if handle.stderr_capture else ""
    _write_safe_text(
        handle.stdout_log,
        stdout,
        known_values=handle.known_values,
    )
    _write_safe_text(
        handle.stderr_log,
        stderr,
        known_values=handle.known_values,
    )
    return stdout, stderr


def _terminate_port_forward(process: subprocess.Popen[str]) -> None:
    if process.poll() is None:
        process.terminate()
    try:
        process.wait(timeout=5)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(timeout=5)


def _wait_for_local_tcp(
    *,
    handle: CatalogPortForwardHandle,
    local_port: int,
    timeout_seconds: float = 10.0,
) -> None:
    process = handle.process
    if process is None:
        raise CatalogProvisioningError("kubectl port-forward did not start")
    deadline = time.monotonic() + timeout_seconds
    last_error = ""
    while time.monotonic() < deadline:
        if process.poll() is not None:
            process.wait(timeout=1)
            stdout, stderr = _flush_port_forward_output(handle)
            safe_tail = _safe_text(
                stderr or stdout or f"exit={process.returncode}",
                known_values=handle.known_values,
            )[-500:]
            raise CatalogProvisioningError(
                "kubectl port-forward exited before becoming ready: " + safe_tail,
            )
        try:
            with socket.create_connection(("127.0.0.1", local_port), timeout=0.2):
                return
        except OSError as exc:
            last_error = str(exc)
            time.sleep(0.1)
    raise CatalogProvisioningError(
        f"kubectl port-forward did not open 127.0.0.1:{local_port}: {last_error}",
    )


def _start_catalog_port_forward(
    *,
    namespace: str,
    resource: str,
    remote_port: int,
    local_port: int,
    step_dir: StepDir,
    name: str,
    child_env: dict[str, str],
    known_values: Sequence[str],
) -> CatalogPortForwardHandle:
    stdout_log = step_dir.artifact_path(f"catalog-port-forward-{name}.stdout")
    stderr_log = step_dir.artifact_path(f"catalog-port-forward-{name}.stderr")
    process: subprocess.Popen[str] | None = None
    handle: CatalogPortForwardHandle | None = None
    try:
        process = subprocess.Popen(
            [
                "kubectl",
                "-n",
                namespace,
                "port-forward",
                resource,
                f"{local_port}:{remote_port}",
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            env=dict(child_env),
        )
        if process.stdout is None or process.stderr is None:
            raise CatalogProvisioningError("kubectl port-forward capture pipes are unavailable")
        stdout_capture = _BoundedRedactedCapture(
            process.stdout,
            tuple(known_values),
        )
        stderr_capture = _BoundedRedactedCapture(
            process.stderr,
            tuple(known_values),
        )
        stdout_capture.start()
        stderr_capture.start()
        handle = CatalogPortForwardHandle(
            name=name,
            namespace=namespace,
            resource=resource,
            remote_port=remote_port,
            local_port=local_port,
            stdout_log=stdout_log,
            stderr_log=stderr_log,
            process=process,
            known_values=tuple(known_values),
            stdout_capture=stdout_capture,
            stderr_capture=stderr_capture,
        )
        _wait_for_local_tcp(
            handle=handle,
            local_port=local_port,
        )
        return handle
    except CatalogProvisioningError:
        if process is not None:
            _terminate_port_forward(process)
        if handle is not None:
            _flush_port_forward_output(handle)
        else:
            _write_safe_text(stdout_log, "", known_values=known_values)
            _write_safe_text(stderr_log, "", known_values=known_values)
        raise
    except Exception:
        if process is not None:
            _terminate_port_forward(process)
        if handle is not None:
            _flush_port_forward_output(handle)
        else:
            _write_safe_text(stdout_log, "", known_values=known_values)
            _write_safe_text(stderr_log, "", known_values=known_values)
        raise CatalogProvisioningError("kubectl port-forward could not start safely") from None


def _stop_catalog_port_forward(handle: CatalogPortForwardHandle) -> None:
    process = handle.process
    if process is None:
        return
    _terminate_port_forward(process)
    _flush_port_forward_output(handle)


def _replace_url_host_port(value: str, *, host: str, port: int) -> str:
    has_scheme = "://" in value
    candidate = value if has_scheme else "http://" + value
    parsed = urlsplit(candidate)
    userinfo = ""
    if "@" in parsed.netloc:
        userinfo = parsed.netloc.rsplit("@", 1)[0] + "@"
    replaced = urlunsplit(
        (
            parsed.scheme,
            f"{userinfo}{host}:{port}",
            parsed.path,
            parsed.query,
            parsed.fragment,
        )
    )
    return replaced if has_scheme else replaced.removeprefix("http://")


def _control_plane_health_url(cp_url: str) -> str:
    parsed = urlsplit(cp_url)
    if not parsed.scheme or not parsed.netloc:
        raise ControlPlaneReadinessError(
            f"control-plane URL must include scheme and host: {_safe_text(cp_url)}",
        )
    return urlunsplit((parsed.scheme, parsed.netloc, "/healthz", "", ""))


def _wait_for_control_plane(
    ctx: RolloutContext,
    step_dir: StepDir,
    *,
    timeout_seconds: float = _CONTROL_PLANE_READY_TIMEOUT_SECONDS,
    interval_seconds: float = _CONTROL_PLANE_READY_INTERVAL_SECONDS,
) -> None:
    health_url = _control_plane_health_url(ctx.cp_url)
    deadline = time.monotonic() + timeout_seconds
    started = time.monotonic()
    attempts = 0
    last_error = ""
    status: int | None = None
    body_len = 0
    while time.monotonic() < deadline:
        attempts += 1
        try:
            with urllib.request.urlopen(health_url, timeout=2.0) as response:
                status = int(response.status)
                body = response.read(256)
                body_len = len(body.strip())
                if status == 200 and body_len > 0:
                    _write_safe_json(
                        step_dir.artifact_path("control-plane-readiness.json"),
                        {
                            "attempts": attempts,
                            "duration_seconds": round(time.monotonic() - started, 3),
                            "health_url": _safe_text(health_url),
                            "ready": True,
                            "status": status,
                        },
                    )
                    return
                last_error = f"HTTP {status} with empty health body"
        except urllib.error.HTTPError as exc:
            status = int(exc.code)
            last_error = f"HTTP {exc.code}: {exc.reason}"
        except OSError as exc:
            last_error = str(exc)
        time.sleep(interval_seconds)

    evidence = {
        "attempts": attempts,
        "duration_seconds": round(time.monotonic() - started, 3),
        "health_url": _safe_text(health_url),
        "last_error": _safe_text(last_error),
        "ready": False,
        "status": status,
    }
    _write_safe_json(
        step_dir.artifact_path("control-plane-readiness.json"),
        evidence,
    )
    raise ControlPlaneReadinessError(
        "control-plane did not become ready at "
        f"{_safe_text(health_url)} within {timeout_seconds:g}s: {_safe_text(last_error)}",
    )


def _catalog_port_forward_evidence(
    handle: CatalogPortForwardHandle,
) -> dict[str, Any]:
    return {
        "name": handle.name,
        "namespace": handle.namespace,
        "resource": handle.resource,
        "remote_port": handle.remote_port,
        "local_host": "127.0.0.1",
        "local_port": handle.local_port,
        "stdout_log": str(handle.stdout_log),
        "stderr_log": str(handle.stderr_log),
    }


@contextmanager
def _catalog_effective_env(
    plan: CatalogProvisioningPlan,
    *,
    step_dir: StepDir,
) -> Iterator[tuple[dict[str, str], dict[str, Any]]]:
    env = dict(plan.env)
    config = plan.kubernetes_port_forward
    if config is None:
        yield env, {"enabled": False}
        return

    handles: list[CatalogPortForwardHandle] = []
    known_values = (
        *plan.protected_values,
        *tuple(value for value in plan.env.values() if value),
    )
    port_forward_env = _bounded_port_forward_env(plan.env)
    try:
        postgres = _start_catalog_port_forward(
            namespace=config.namespace,
            resource=config.postgres_service,
            remote_port=config.postgres_remote_port,
            local_port=_reserve_local_port(),
            step_dir=step_dir,
            name="postgres",
            child_env=port_forward_env,
            known_values=known_values,
        )
        handles.append(postgres)
        minio = _start_catalog_port_forward(
            namespace=config.namespace,
            resource=config.minio_service,
            remote_port=config.minio_remote_port,
            local_port=_reserve_local_port(),
            step_dir=step_dir,
            name="minio",
            child_env=port_forward_env,
            known_values=known_values,
        )
        handles.append(minio)
        for name in ("LOOM_DB_URL", "LOOM_SVC_DB_URL"):
            if env.get(name):
                env[name] = _replace_url_host_port(
                    env[name],
                    host="127.0.0.1",
                    port=postgres.local_port,
                )
        for name in ("LOOM_MINIO_ENDPOINT", "LOOM_SVC_MINIO_ENDPOINT"):
            if env.get(name):
                env[name] = _replace_url_host_port(
                    env[name],
                    host="127.0.0.1",
                    port=minio.local_port,
                )
        yield (
            env,
            {
                "enabled": True,
                "namespace": config.namespace,
                "forwards": [_catalog_port_forward_evidence(handle) for handle in handles],
            },
        )
    finally:
        for handle in reversed(handles):
            _stop_catalog_port_forward(handle)


def _catalog_known_values(
    plan: CatalogProvisioningPlan,
    effective_env: dict[str, str] | None = None,
) -> tuple[str, ...]:
    values = [
        *plan.protected_values,
        *tuple(value for value in plan.env.values() if value),
    ]
    if plan.env_file is not None:
        values.extend(
            value
            for key, value in plan.env_file.items()
            if "path" in key and isinstance(value, str) and value
        )
    values.extend(
        value for value in plan.env_sources.values() if value and not value.startswith("sha256:")
    )
    if effective_env is not None:
        values.extend(value for value in effective_env.values() if value)
    try:
        command_tokens = shlex.split(plan.command)
    except ValueError:
        command_tokens = []
    values.extend(token for token in command_tokens if token.startswith(("/", "file:", "env:")))
    return tuple(dict.fromkeys(value for value in values if value))


def _run_catalog_provisioning(
    plan: CatalogProvisioningPlan,
    *,
    cwd: Path,
    step_dir: StepDir,
) -> RunResult | None:
    known_values = _catalog_known_values(plan)
    try:
        with rollout_redaction_scope(known_values):
            with _catalog_effective_env(plan, step_dir=step_dir) as (
                effective_env,
                port_forward_evidence,
            ):
                effective_known_values = _catalog_known_values(plan, effective_env)
                with rollout_redaction_scope(effective_known_values):
                    result = run_captured(
                        ["bash", "-euo", "pipefail", "-c", plan.command],
                        cwd=cwd,
                        env=effective_env,
                    )
    except Exception as exc:
        message = _safe_text(exc, known_values=known_values)
        stdout_log = step_dir.artifact_path("catalog-provisioning.stdout")
        stderr_log = step_dir.artifact_path("catalog-provisioning.stderr")
        _write_safe_text(stdout_log, "", known_values=known_values)
        _write_safe_text(
            stderr_log,
            message + "\n",
            known_values=known_values,
        )
        evidence_path = step_dir.artifact_path("catalog-provisioning.json")
        _write_safe_json(
            evidence_path,
            {
                "required": True,
                "command_sha256": _sha256_bytes(plan.command.encode("utf-8")),
                "returncode": 1,
                "required_env": plan.required_env,
                "env_file": plan.env_file,
                "env_sources": plan.env_sources,
                "kubernetes_port_forward": {"enabled": True, "error": message},
                "stdout_log": str(stdout_log),
                "stderr_log": str(stderr_log),
            },
            known_values=known_values,
        )
        return RunResult(
            exit_code=1,
            error=f"catalog provisioning failed: {message[:200]}",
            artifacts={"catalog_provisioning": str(evidence_path)},
        )
    redacted_stdout = _redact_catalog_output(
        result.stdout,
        env=effective_env,
        required_env=plan.required_env,
    )
    redacted_stderr = _redact_catalog_output(
        result.stderr,
        env=effective_env,
        required_env=plan.required_env,
    )
    stdout_log = step_dir.artifact_path("catalog-provisioning.stdout")
    stderr_log = step_dir.artifact_path("catalog-provisioning.stderr")
    known_values = _catalog_known_values(plan, effective_env)
    _write_safe_text(stdout_log, redacted_stdout, known_values=known_values)
    _write_safe_text(stderr_log, redacted_stderr, known_values=known_values)
    evidence = {
        "required": True,
        "command_sha256": _sha256_bytes(plan.command.encode("utf-8")),
        "returncode": result.returncode,
        "required_env": plan.required_env,
        "env_file": plan.env_file,
        "env_sources": plan.env_sources,
        "kubernetes_port_forward": port_forward_evidence,
        "environment_keys": sorted(effective_env),
        "stdout_log": str(stdout_log),
        "stderr_log": str(stderr_log),
    }
    evidence_path = step_dir.artifact_path("catalog-provisioning.json")
    _write_safe_json(
        evidence_path,
        evidence,
        known_values=known_values,
    )
    if result.returncode == 0:
        return None
    message = (redacted_stderr or redacted_stdout).strip() or (
        f"catalog provisioning exited {result.returncode}"
    )
    message = _safe_text(message, known_values=known_values)
    return RunResult(
        exit_code=result.returncode,
        error=f"catalog provisioning failed: {message[:200]}",
        artifacts={"catalog_provisioning": str(evidence_path)},
    )


def _secret_safe_value(value: str) -> str:
    if "\n" in value or "\r" in value:
        raise ExternalSlurmPrereqMaterializationError(
            "external runner env values must be single-line",
        )
    return value


def _update_env_text(existing: str, updates: dict[str, str]) -> str:
    out: list[str] = []
    seen: set[str] = set()
    for line in existing.splitlines():
        if "=" not in line or line.lstrip().startswith("#"):
            out.append(line)
            continue
        key = line.split("=", 1)[0].strip()
        if key in updates:
            if key not in seen:
                out.append(f"{key}={_secret_safe_value(updates[key])}")
                seen.add(key)
            continue
        out.append(line)
    for key, value in updates.items():
        if key not in seen:
            out.append(f"{key}={_secret_safe_value(value)}")
    return "\n".join(out).rstrip() + "\n"


def _select_env_template(
    *,
    target: Path,
    settings: dict[str, Any],
) -> Path:
    template = settings.get("env_template")
    if isinstance(template, str) and template.strip():
        path = Path(template).expanduser()
        if not _env_source_is_safe(path):
            raise ExternalSlurmPrereqMaterializationError(
                f"external runner env template is unavailable or unsafe: {path}",
            )
        return path

    pattern = settings.get("env_template_glob")
    if not isinstance(pattern, str) or not pattern.strip():
        raise ExternalSlurmPrereqMaterializationError(
            f"external runner env file {target} is missing and "
            "external_slurm_runner_prerequisites.env_template_glob is not set",
        )
    matched = [Path(path) for path in glob.glob(str(Path(pattern).expanduser()))]
    unsafe = [path for path in matched if path != target and not _env_source_is_safe(path)]
    if unsafe:
        raise ExternalSlurmPrereqMaterializationError(
            f"external runner env template match is unsafe: {unsafe[0]}",
        )
    candidates = [path for path in matched if path != target]
    if not candidates:
        raise ExternalSlurmPrereqMaterializationError(
            f"external runner env file {target} is missing and no template matched {pattern}",
        )
    return max(candidates, key=lambda path: path.stat().st_mtime_ns)


_MAX_EXTERNAL_RUNNER_ENV_BYTES = 1 << 20
_REQUIRED_EXTERNAL_RUNNER_ENV_KEYS = frozenset(
    {
        "LOOM_WORKER_CONTROL_PLANE_URL",
        "LOOM_WORKER_GATEWAY_URL",
        "LOOM_WORKER_TOKEN",
        "LOOM_WORKER_MINIO_ENDPOINT",
        "LOOM_WORKER_MINIO_ACCESS_KEY",
        "LOOM_WORKER_MINIO_SECRET_KEY",
    }
)


def _env_source_is_safe(path: Path) -> bool:
    try:
        metadata = path.lstat()
    except OSError:
        return False
    return bool(
        stat.S_ISREG(metadata.st_mode)
        and not stat.S_ISLNK(metadata.st_mode)
        and stat.S_IMODE(metadata.st_mode) == 0o600
        and metadata.st_nlink == 1
        and 0 < metadata.st_size <= _MAX_EXTERNAL_RUNNER_ENV_BYTES
    )


def _read_env_source(path: Path) -> str:
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        fd = os.open(path, flags)
    except OSError as exc:
        raise ExternalSlurmPrereqMaterializationError(
            f"external runner env source is unavailable: {path}",
        ) from exc
    try:
        metadata = os.fstat(fd)
        if not (
            stat.S_ISREG(metadata.st_mode)
            and stat.S_IMODE(metadata.st_mode) == 0o600
            and metadata.st_nlink == 1
            and 0 < metadata.st_size <= _MAX_EXTERNAL_RUNNER_ENV_BYTES
        ):
            raise ExternalSlurmPrereqMaterializationError(
                f"external runner env source is unsafe: {path}",
            )
        payload = os.read(fd, _MAX_EXTERNAL_RUNNER_ENV_BYTES + 1)
        if len(payload) != metadata.st_size:
            raise ExternalSlurmPrereqMaterializationError(
                f"external runner env source changed while it was read: {path}",
            )
    finally:
        os.close(fd)
    try:
        text = payload.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ExternalSlurmPrereqMaterializationError(
            f"external runner env source is not UTF-8: {path}",
        ) from exc
    keys: set[str] = set()
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if "=" not in line:
            raise ExternalSlurmPrereqMaterializationError(
                f"external runner env source contains a malformed entry: {path}",
            )
        key, value = line.split("=", 1)
        key = key.strip()
        if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", key) is None or key in keys:
            raise ExternalSlurmPrereqMaterializationError(
                f"external runner env source contains an invalid key: {path}",
            )
        if key in _REQUIRED_EXTERNAL_RUNNER_ENV_KEYS:
            if "${" in value:
                raise ExternalSlurmPrereqMaterializationError(
                    f"external runner env source required values cannot use interpolation: {path}",
                )
            try:
                semantic_parts = shlex.split(value, comments=True, posix=True)
            except ValueError as exc:
                raise ExternalSlurmPrereqMaterializationError(
                    f"external runner env source contains a malformed entry: {path}",
                ) from exc
            if not any(part.strip() for part in semantic_parts):
                raise ExternalSlurmPrereqMaterializationError(
                    f"external runner env source contains an empty required value: {path}",
                )
        keys.add(key)
    if not _REQUIRED_EXTERNAL_RUNNER_ENV_KEYS.issubset(keys):
        raise ExternalSlurmPrereqMaterializationError(
            f"external runner env source is missing required settings: {path}",
        )
    return text


def _materialize_env_file(
    *,
    env_file: Path,
    settings: dict[str, Any],
    image_tag: str,
    pool_name: str,
    requested_concurrency: object,
    worker_token: str | None,
    worker_token_env_key: str,
) -> dict[str, Any]:
    if os.path.lexists(env_file):
        if not _env_source_is_safe(env_file):
            raise ExternalSlurmPrereqMaterializationError(
                f"external runner env file is unsafe: {env_file}",
            )
        source = env_file
    else:
        source = _select_env_template(
            target=env_file,
            settings=settings,
        )
    existing = _read_env_source(source)
    updates = {
        "IMAGE_TAG": image_tag,
        "ENV_CONFIG_VERSION": image_tag,
        "LOOM_IMAGE_TAG": image_tag,
        "LOOM_WORKER_ENV_CONFIG_VERSION": image_tag,
        "LOOM_WORKER_POOL_NAME": pool_name,
    }
    if requested_concurrency is not None:
        updates["LOOM_WORKER_MAX_CONCURRENT"] = str(requested_concurrency)
    if worker_token is not None:
        updates[worker_token_env_key] = worker_token

    env_file.parent.mkdir(parents=True, exist_ok=True)
    rendered = _update_env_text(existing, updates)
    tmp = env_file.with_name(f".{env_file.name}.tmp-{os.getpid()}")
    tmp.write_text(rendered, encoding="utf-8")
    os.chmod(tmp, 0o600)
    os.replace(tmp, env_file)
    os.chmod(env_file, 0o600)
    token_fingerprint = worker_token_fingerprint(worker_token) if worker_token is not None else None
    return {
        "env_file": str(env_file),
        "env_action": "updated" if source == env_file else "created",
        "env_template": None if source == env_file else str(source),
        "env_mode": oct(env_file.stat().st_mode & 0o777),
        "worker_token_key": worker_token_env_key if worker_token is not None else None,
        "worker_token": "[REDACTED]" if worker_token is not None else None,
        "worker_token_fingerprint": token_fingerprint,
    }


def _git_stdout(argv: list[str]) -> str:
    result = run_captured(argv)
    if result.returncode != 0:
        raw_message = (result.stderr or result.stdout).strip() or (
            f"{' '.join(argv)} exited {result.returncode}"
        )
        raise ExternalSlurmPrereqMaterializationError(_safe_text(raw_message))
    return result.stdout.strip()


def _shared_repo_git(repo_dir: Path, *arguments: str) -> str:
    command = [
        "/usr/bin/git",
        "--git-dir",
        str(repo_dir / ".git"),
        "--work-tree",
        str(repo_dir),
    ]
    for key, value in (
        ("safe.directory", str(repo_dir)),
        ("core.worktree", str(repo_dir)),
        ("core.bare", "false"),
        ("core.fsmonitor", "false"),
        ("core.hooksPath", "/dev/null"),
        ("core.attributesFile", "/dev/null"),
        ("core.excludesFile", "/dev/null"),
        ("core.untrackedCache", "false"),
        ("submodule.recurse", "false"),
        ("fetch.recurseSubmodules", "false"),
        ("protocol.file.allow", "never"),
        ("credential.helper", ""),
        ("core.sshCommand", "/usr/bin/false"),
    ):
        command.extend(("-c", f"{key}={value}"))
    command.extend(arguments)
    result = run_captured(
        command,
        env={
            "GIT_CONFIG_NOSYSTEM": "1",
            "GIT_NO_REPLACE_OBJECTS": "1",
            "GIT_CONFIG_SYSTEM": "/dev/null",
            "GIT_CONFIG_GLOBAL": "/dev/null",
            "GIT_ATTR_NOSYSTEM": "1",
            "GIT_OPTIONAL_LOCKS": "0",
            "GIT_TERMINAL_PROMPT": "0",
            "GIT_PROTOCOL_FROM_USER": "0",
            "GIT_PAGER": "cat",
            "GIT_EXTERNAL_DIFF": "/usr/bin/false",
            "GIT_SSH_COMMAND": "/usr/bin/false",
            "HOME": "/nonexistent",
            "XDG_CONFIG_HOME": "/nonexistent",
            "LANG": "C",
            "LC_ALL": "C",
            "PATH": "/usr/bin:/bin",
        },
    )
    if result.returncode != 0 or result.stderr:
        raise ExternalSlurmPrereqMaterializationError(
            "external runner repository Git verification failed safely",
        )
    return result.stdout


@dataclass(frozen=True)
class _BoundDirectory:
    """An opened directory whose identity must remain stable for one operation."""

    path: Path
    fd: int
    identity: os.stat_result

    def assert_stable(self) -> None:
        current = os.fstat(self.fd)
        try:
            lexical = self.path.stat(follow_symlinks=False)
        except OSError as exc:
            raise ExternalSlurmPrereqMaterializationError(
                "external runner repository authority changed during materialization",
            ) from exc
        expected = (self.identity.st_dev, self.identity.st_ino)
        if (
            not stat.S_ISDIR(current.st_mode)
            or stat.S_ISLNK(lexical.st_mode)
            or not stat.S_ISDIR(lexical.st_mode)
            or (current.st_dev, current.st_ino) != expected
            or (lexical.st_dev, lexical.st_ino) != expected
        ):
            raise ExternalSlurmPrereqMaterializationError(
                "external runner repository authority changed during materialization",
            )


def _open_bound_directory(path: Path) -> _BoundDirectory:
    if not path.is_absolute() or ".." in path.parts:
        raise ExternalSlurmPrereqMaterializationError(
            "external runner repository authority path is unsafe",
        )
    flags = (
        os.O_RDONLY | os.O_DIRECTORY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    )
    fd = os.open("/", flags)
    try:
        for component in path.parts[1:]:
            next_fd = os.open(component, flags, dir_fd=fd)
            os.close(fd)
            fd = next_fd
        identity = os.fstat(fd)
        binding = _BoundDirectory(path=path, fd=fd, identity=identity)
        binding.assert_stable()
        return binding
    except Exception:
        os.close(fd)
        raise


def _validate_repo_root(repo_dir: Path, expected_ref: str) -> _BoundDirectory:
    expected_name = f"loom-remote-worker-{expected_ref.strip()}"
    if (
        not repo_dir.is_absolute()
        or ".." in repo_dir.parts
        or repo_dir.name != expected_name
        or repo_dir.parent != _SHARED_WORKER_REPO_ROOT
    ):
        raise ExternalSlurmPrereqMaterializationError(
            "external runner repository destination is outside its candidate-bound root",
        )

    try:
        root = _open_bound_directory(repo_dir.parent)
    except OSError as exc:
        raise ExternalSlurmPrereqMaterializationError(
            "external runner repository authority is unavailable",
        ) from exc
    mode = stat.S_IMODE(root.identity.st_mode)
    try:
        shared_work_gid = grp.getgrnam("sharedwork").gr_gid
    except KeyError as exc:
        os.close(root.fd)
        raise ExternalSlurmPrereqMaterializationError(
            "sharedwork group is unavailable for external runner materialization",
        ) from exc
    if (
        root.identity.st_uid != os.geteuid()
        or root.identity.st_gid != shared_work_gid
        or mode != 0o2750
    ):
        os.close(root.fd)
        raise ExternalSlurmPrereqMaterializationError(
            "external runner repository root has unsafe owner or mode",
        )
    return root


def _open_child_directory(parent_fd: int, name: str) -> int:
    return os.open(
        name,
        os.O_RDONLY | os.O_DIRECTORY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0),
        dir_fd=parent_fd,
    )


def _index_entries(repo_dir: Path) -> dict[str, tuple[str, str]]:
    raw = _shared_repo_git(repo_dir, "ls-files", "--stage", "-z")
    entries: dict[str, tuple[str, str]] = {}
    for raw_entry in raw.split("\0"):
        if not raw_entry:
            continue
        metadata, separator, relative = raw_entry.partition("\t")
        fields = metadata.split()
        if separator != "\t" or len(fields) != 3 or fields[2] != "0":
            raise ExternalSlurmPrereqMaterializationError(
                "external runner repository index is invalid",
            )
        mode, object_id = fields[:2]
        relative_path = PurePosixPath(relative)
        if (
            mode not in {"100644", "100755", "120000"}
            or _GIT_OBJECT_ID_RE.fullmatch(object_id) is None
            or not relative
            or relative_path.is_absolute()
            or ".." in relative_path.parts
            or relative_path.parts[0] == ".git"
            or relative in entries
        ):
            raise ExternalSlurmPrereqMaterializationError(
                "external runner repository index contains an unsupported entry",
            )
        entries[relative] = (mode, object_id)
    if not entries:
        raise ExternalSlurmPrereqMaterializationError(
            "external runner repository index is empty",
        )
    return entries


def _commit_tree_entries(repo_dir: Path, resolved_sha: str) -> dict[str, tuple[str, str]]:
    raw = _shared_repo_git(repo_dir, "ls-tree", "-r", "-z", "--full-tree", resolved_sha)
    entries: dict[str, tuple[str, str]] = {}
    for raw_entry in raw.split("\0"):
        if not raw_entry:
            continue
        metadata, separator, relative = raw_entry.partition("\t")
        fields = metadata.split()
        relative_path = PurePosixPath(relative)
        if (
            separator != "\t"
            or len(fields) != 3
            or fields[0] not in {"100644", "100755", "120000"}
            or fields[1] != "blob"
            or _GIT_OBJECT_ID_RE.fullmatch(fields[2]) is None
            or not relative
            or relative_path.is_absolute()
            or ".." in relative_path.parts
            or relative_path.parts[0] == ".git"
            or relative in entries
        ):
            raise ExternalSlurmPrereqMaterializationError(
                "external runner repository commit tree is invalid",
            )
        entries[relative] = (fields[0], fields[2])
    if not entries:
        raise ExternalSlurmPrereqMaterializationError(
            "external runner repository commit tree is empty",
        )
    return entries


def _normalize_git_metadata(directory_fd: int, *, uid: int, gid: int) -> None:
    for name in os.listdir(directory_fd):
        metadata = os.stat(name, dir_fd=directory_fd, follow_symlinks=False)
        if metadata.st_uid != uid or metadata.st_gid != gid:
            raise ExternalSlurmPrereqMaterializationError(
                "fresh external runner checkout contains foreign-owned git metadata",
            )
        if stat.S_ISLNK(metadata.st_mode) or not (
            stat.S_ISDIR(metadata.st_mode) or stat.S_ISREG(metadata.st_mode)
        ):
            raise ExternalSlurmPrereqMaterializationError(
                "fresh external runner checkout contains unsafe git metadata",
            )
        if stat.S_ISDIR(metadata.st_mode):
            child_fd = _open_child_directory(directory_fd, name)
            try:
                _normalize_git_metadata(child_fd, uid=uid, gid=gid)
                os.fchmod(child_fd, 0o750)
            finally:
                os.close(child_fd)
        else:
            if metadata.st_nlink != 1:
                raise ExternalSlurmPrereqMaterializationError(
                    "fresh external runner checkout contains linked git metadata",
                )
            os.chmod(name, 0o640, dir_fd=directory_fd, follow_symlinks=False)


def _write_canonical_git_config(directory_fd: int, *, uid: int, gid: int) -> None:
    flags = os.O_RDWR | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    config_fd = os.open("config", flags, dir_fd=directory_fd)
    try:
        metadata = os.fstat(config_fd)
        if (
            not stat.S_ISREG(metadata.st_mode)
            or metadata.st_uid != uid
            or metadata.st_gid != gid
            or metadata.st_nlink != 1
        ):
            raise ExternalSlurmPrereqMaterializationError(
                "fresh external runner checkout Git configuration is unsafe",
            )
        os.ftruncate(config_fd, 0)
        written = 0
        while written < len(_CANONICAL_SHARED_REPO_GIT_CONFIG):
            written += os.write(config_fd, _CANONICAL_SHARED_REPO_GIT_CONFIG[written:])
        os.fchmod(config_fd, 0o640)
        os.fsync(config_fd)
        after = os.fstat(config_fd)
        if after.st_size != len(_CANONICAL_SHARED_REPO_GIT_CONFIG):
            raise ExternalSlurmPrereqMaterializationError(
                "fresh external runner checkout Git configuration did not converge",
            )
    finally:
        os.close(config_fd)


def _validate_canonical_git_config(directory_fd: int, *, uid: int, gid: int) -> None:
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    config_fd = os.open("config", flags, dir_fd=directory_fd)
    try:
        before = os.fstat(config_fd)
        if (
            not stat.S_ISREG(before.st_mode)
            or before.st_uid != uid
            or before.st_gid != gid
            or before.st_nlink != 1
            or stat.S_IMODE(before.st_mode) != 0o640
            or before.st_size != len(_CANONICAL_SHARED_REPO_GIT_CONFIG)
        ):
            raise ExternalSlurmPrereqMaterializationError(
                "external runner repository Git configuration authority drifted",
            )
        payload = os.read(config_fd, len(_CANONICAL_SHARED_REPO_GIT_CONFIG) + 1)
        after = os.fstat(config_fd)
        if payload != _CANONICAL_SHARED_REPO_GIT_CONFIG or (
            after.st_dev,
            after.st_ino,
            after.st_mtime_ns,
            after.st_size,
        ) != (before.st_dev, before.st_ino, before.st_mtime_ns, before.st_size):
            raise ExternalSlurmPrereqMaterializationError(
                "external runner repository Git configuration drifted",
            )
    finally:
        os.close(config_fd)


def _validate_physical_metadata(
    directory_fd: int,
    *,
    uid: int,
    gid: int,
    top_level: bool = True,
) -> None:
    for name in os.listdir(directory_fd):
        if top_level and name == ".git":
            continue
        metadata = os.stat(name, dir_fd=directory_fd, follow_symlinks=False)
        if metadata.st_uid != uid or metadata.st_gid != gid:
            raise ExternalSlurmPrereqMaterializationError(
                "external runner repository contains foreign-owned content",
            )
        if stat.S_ISDIR(metadata.st_mode):
            if stat.S_IMODE(metadata.st_mode) != 0o750:
                raise ExternalSlurmPrereqMaterializationError(
                    "external runner repository directory mode drifted",
                )
            child_fd = _open_child_directory(directory_fd, name)
            try:
                _validate_physical_metadata(
                    child_fd,
                    uid=uid,
                    gid=gid,
                    top_level=False,
                )
            finally:
                os.close(child_fd)
        elif stat.S_ISREG(metadata.st_mode):
            if metadata.st_nlink != 1 or stat.S_IMODE(metadata.st_mode) not in {0o640, 0o750}:
                raise ExternalSlurmPrereqMaterializationError(
                    "external runner repository file mode drifted",
                )
        elif stat.S_ISLNK(metadata.st_mode):
            if metadata.st_nlink != 1:
                raise ExternalSlurmPrereqMaterializationError(
                    "external runner repository contains a linked symlink",
                )
        else:
            raise ExternalSlurmPrereqMaterializationError(
                "external runner repository contains an unsafe entry",
            )


def _git_blob_id(data: bytes) -> str:
    digest = hashlib.sha1(usedforsecurity=False)
    digest.update(f"blob {len(data)}\0".encode("ascii"))
    digest.update(data)
    return digest.hexdigest()


def _regular_git_blob_id(
    directory_fd: int,
    name: str,
    *,
    metadata: os.stat_result,
) -> str:
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    file_fd = os.open(name, flags, dir_fd=directory_fd)
    try:
        before = os.fstat(file_fd)
        if (before.st_dev, before.st_ino) != (metadata.st_dev, metadata.st_ino):
            raise ExternalSlurmPrereqMaterializationError(
                "external runner repository file binding drifted",
            )
        digest = hashlib.sha1(usedforsecurity=False)
        digest.update(f"blob {before.st_size}\0".encode("ascii"))
        size = 0
        while chunk := os.read(file_fd, 1024 * 1024):
            digest.update(chunk)
            size += len(chunk)
        after = os.fstat(file_fd)
        if size != before.st_size or (
            after.st_dev,
            after.st_ino,
            after.st_mtime_ns,
            after.st_size,
        ) != (before.st_dev, before.st_ino, before.st_mtime_ns, before.st_size):
            raise ExternalSlurmPrereqMaterializationError(
                "external runner repository file changed while read",
            )
        return digest.hexdigest()
    finally:
        os.close(file_fd)


def _normalize_worktree(
    directory_fd: int,
    *,
    index_modes: dict[str, str],
    expected_directories: set[str],
    uid: int,
    gid: int,
    prefix: str = "",
) -> tuple[set[str], set[str]]:
    materialized: set[str] = set()
    directories: set[str] = set()
    for name in os.listdir(directory_fd):
        if not prefix and name == ".git":
            continue
        relative = f"{prefix}/{name}" if prefix else name
        metadata = os.stat(name, dir_fd=directory_fd, follow_symlinks=False)
        if metadata.st_uid != uid or metadata.st_gid != gid:
            raise ExternalSlurmPrereqMaterializationError(
                "fresh external runner checkout contains foreign-owned content",
            )
        if stat.S_ISDIR(metadata.st_mode):
            if relative not in expected_directories:
                raise ExternalSlurmPrereqMaterializationError(
                    "fresh external runner checkout contains an untracked directory",
                )
            directories.add(relative)
            child_fd = _open_child_directory(directory_fd, name)
            try:
                child_files, child_directories = _normalize_worktree(
                    child_fd,
                    index_modes=index_modes,
                    expected_directories=expected_directories,
                    uid=uid,
                    gid=gid,
                    prefix=relative,
                )
                materialized.update(child_files)
                directories.update(child_directories)
                os.fchmod(child_fd, 0o750)
            finally:
                os.close(child_fd)
            continue
        expected = index_modes.get(relative)
        if stat.S_ISLNK(metadata.st_mode):
            if expected != "120000":
                raise ExternalSlurmPrereqMaterializationError(
                    "fresh external runner checkout contains an untracked symlink",
                )
        elif stat.S_ISREG(metadata.st_mode):
            if metadata.st_nlink != 1 or expected not in {"100644", "100755"}:
                raise ExternalSlurmPrereqMaterializationError(
                    "fresh external runner checkout contains an unsafe file",
                )
            os.chmod(
                name,
                0o750 if expected == "100755" else 0o640,
                dir_fd=directory_fd,
                follow_symlinks=False,
            )
        else:
            raise ExternalSlurmPrereqMaterializationError(
                "fresh external runner checkout contains an unsafe entry",
            )
        materialized.add(relative)
    return materialized, directories


def _validate_git_metadata(directory_fd: int, *, uid: int, gid: int) -> None:
    for name in os.listdir(directory_fd):
        metadata = os.stat(name, dir_fd=directory_fd, follow_symlinks=False)
        if metadata.st_uid != uid or metadata.st_gid != gid:
            raise ExternalSlurmPrereqMaterializationError(
                "external runner repository contains foreign-owned git metadata",
            )
        if stat.S_ISDIR(metadata.st_mode):
            if stat.S_IMODE(metadata.st_mode) != 0o750:
                raise ExternalSlurmPrereqMaterializationError(
                    "external runner repository git metadata mode drifted",
                )
            child_fd = _open_child_directory(directory_fd, name)
            try:
                _validate_git_metadata(child_fd, uid=uid, gid=gid)
            finally:
                os.close(child_fd)
        elif stat.S_ISREG(metadata.st_mode):
            if metadata.st_nlink != 1 or stat.S_IMODE(metadata.st_mode) != 0o640:
                raise ExternalSlurmPrereqMaterializationError(
                    "external runner repository git metadata is unsafe",
                )
        else:
            raise ExternalSlurmPrereqMaterializationError(
                "external runner repository contains unsafe git metadata",
            )


def _validate_single_git_authority(directory_fd: int, *, uid: int, gid: int) -> None:
    entries = set(os.listdir(directory_fd))
    if entries & {"commondir", "config.worktree"}:
        raise ExternalSlurmPrereqMaterializationError(
            "external runner repository Git common authority redirection is forbidden",
        )
    for name in ("objects", "refs"):
        child_fd = _open_child_directory(directory_fd, name)
        try:
            held = os.fstat(child_fd)
            lexical = os.stat(name, dir_fd=directory_fd, follow_symlinks=False)
            if (
                held.st_uid != uid
                or held.st_gid != gid
                or stat.S_IMODE(held.st_mode) != 0o750
                or not stat.S_ISDIR(lexical.st_mode)
                or (held.st_dev, held.st_ino) != (lexical.st_dev, lexical.st_ino)
            ):
                raise ExternalSlurmPrereqMaterializationError(
                    "external runner repository Git object or ref authority drifted",
                )
        finally:
            os.close(child_fd)
    try:
        git_info_fd = _open_child_directory(directory_fd, "info")
    except FileNotFoundError:
        pass
    else:
        try:
            if "grafts" in set(os.listdir(git_info_fd)):
                raise ExternalSlurmPrereqMaterializationError(
                    "external runner repository Git legacy graft authority is forbidden",
                )
        finally:
            os.close(git_info_fd)
    objects_fd = _open_child_directory(directory_fd, "objects")
    try:
        try:
            info_fd = _open_child_directory(objects_fd, "info")
        except FileNotFoundError:
            return
        try:
            if set(os.listdir(info_fd)) & {"alternates", "http-alternates"}:
                raise ExternalSlurmPrereqMaterializationError(
                    "external runner repository Git object authority redirection is forbidden",
                )
        finally:
            os.close(info_fd)
    finally:
        os.close(objects_fd)


def _validate_worktree(
    directory_fd: int,
    *,
    index_entries: dict[str, tuple[str, str]],
    expected_directories: set[str],
    uid: int,
    gid: int,
    prefix: str = "",
) -> tuple[set[str], set[str]]:
    materialized: set[str] = set()
    directories: set[str] = set()
    for name in os.listdir(directory_fd):
        if not prefix and name == ".git":
            continue
        relative = f"{prefix}/{name}" if prefix else name
        metadata = os.stat(name, dir_fd=directory_fd, follow_symlinks=False)
        if metadata.st_uid != uid or metadata.st_gid != gid:
            raise ExternalSlurmPrereqMaterializationError(
                "external runner repository contains foreign-owned content",
            )
        if stat.S_ISDIR(metadata.st_mode):
            if relative not in expected_directories:
                raise ExternalSlurmPrereqMaterializationError(
                    "external runner repository contains an untracked directory",
                )
            if stat.S_IMODE(metadata.st_mode) != 0o750:
                raise ExternalSlurmPrereqMaterializationError(
                    "external runner repository directory mode drifted",
                )
            directories.add(relative)
            child_fd = _open_child_directory(directory_fd, name)
            try:
                child_files, child_directories = _validate_worktree(
                    child_fd,
                    index_entries=index_entries,
                    expected_directories=expected_directories,
                    uid=uid,
                    gid=gid,
                    prefix=relative,
                )
                materialized.update(child_files)
                directories.update(child_directories)
            finally:
                os.close(child_fd)
            continue
        expected = index_entries.get(relative)
        if stat.S_ISLNK(metadata.st_mode):
            object_id = _git_blob_id(os.fsencode(os.readlink(name, dir_fd=directory_fd)))
            if expected is None or expected[0] != "120000" or expected[1] != object_id:
                raise ExternalSlurmPrereqMaterializationError(
                    "external runner repository symlink content drifted",
                )
        elif stat.S_ISREG(metadata.st_mode):
            expected_mode = 0o750 if expected is not None and expected[0] == "100755" else 0o640
            if (
                expected is None
                or expected[0] not in {"100644", "100755"}
                or metadata.st_nlink != 1
                or stat.S_IMODE(metadata.st_mode) != expected_mode
            ):
                raise ExternalSlurmPrereqMaterializationError(
                    "external runner repository file mode drifted",
                )
            if _regular_git_blob_id(directory_fd, name, metadata=metadata) != expected[1]:
                raise ExternalSlurmPrereqMaterializationError(
                    "external runner repository file content drifted",
                )
        else:
            raise ExternalSlurmPrereqMaterializationError(
                "external runner repository contains an unsafe entry",
            )
        materialized.add(relative)
    return materialized, directories


def _expected_worktree_directories(index_entries: Collection[str]) -> set[str]:
    return {
        str(parent)
        for relative in index_entries
        for parent in PurePosixPath(relative).parents
        if str(parent) != "."
    }


def _validate_repo_tree(
    repo_dir: Path,
    *,
    root: _BoundDirectory,
    resolved_sha: str,
) -> None:
    root.assert_stable()
    try:
        repo_fd = _open_child_directory(root.fd, repo_dir.name)
    except FileNotFoundError:
        raise
    try:
        top = os.fstat(repo_fd)
        if (
            top.st_uid != os.geteuid()
            or top.st_gid != root.identity.st_gid
            or stat.S_IMODE(top.st_mode) != 0o750
        ):
            raise ExternalSlurmPrereqMaterializationError(
                "existing external runner repository has unsafe authority",
            )
        git_fd = _open_child_directory(repo_fd, ".git")
        try:
            git_top = os.fstat(git_fd)
            lexical_git = os.stat(".git", dir_fd=repo_fd, follow_symlinks=False)
            if (
                git_top.st_uid != os.geteuid()
                or git_top.st_gid != root.identity.st_gid
                or stat.S_IMODE(git_top.st_mode) != 0o750
                or not stat.S_ISDIR(lexical_git.st_mode)
                or (lexical_git.st_dev, lexical_git.st_ino) != (git_top.st_dev, git_top.st_ino)
            ):
                raise ExternalSlurmPrereqMaterializationError(
                    "external runner repository git metadata authority drifted",
                )
            _validate_single_git_authority(
                git_fd,
                uid=os.geteuid(),
                gid=root.identity.st_gid,
            )
            _validate_canonical_git_config(
                git_fd,
                uid=os.geteuid(),
                gid=root.identity.st_gid,
            )
            _validate_git_metadata(
                git_fd,
                uid=os.geteuid(),
                gid=root.identity.st_gid,
            )
            _validate_physical_metadata(
                repo_fd,
                uid=os.geteuid(),
                gid=root.identity.st_gid,
            )

            object_format = _shared_repo_git(repo_dir, "rev-parse", "--show-object-format").strip()
            if object_format != "sha1":
                raise ExternalSlurmPrereqMaterializationError(
                    "external runner repository object format is unsupported",
                )
            head = _shared_repo_git(repo_dir, "rev-parse", "HEAD^{commit}").strip()
            if head != resolved_sha:
                raise ExternalSlurmPrereqMaterializationError(
                    "external runner repository does not exactly match the resolved candidate",
                )
            index_entries = _index_entries(repo_dir)
            tree_entries = _commit_tree_entries(repo_dir, resolved_sha)
            if index_entries != tree_entries:
                raise ExternalSlurmPrereqMaterializationError(
                    "external runner repository index does not match its commit tree",
                )
            expected_directories = _expected_worktree_directories(index_entries)
            materialized, directories = _validate_worktree(
                repo_fd,
                index_entries=index_entries,
                expected_directories=expected_directories,
                uid=os.geteuid(),
                gid=root.identity.st_gid,
            )
            if materialized != set(index_entries) or directories != expected_directories:
                raise ExternalSlurmPrereqMaterializationError(
                    "external runner repository does not match its exact commit tree",
                )

            _validate_canonical_git_config(
                git_fd,
                uid=os.geteuid(),
                gid=root.identity.st_gid,
            )
            _validate_single_git_authority(
                git_fd,
                uid=os.geteuid(),
                gid=root.identity.st_gid,
            )
            _validate_git_metadata(
                git_fd,
                uid=os.geteuid(),
                gid=root.identity.st_gid,
            )
            lexical_git_after = os.stat(".git", dir_fd=repo_fd, follow_symlinks=False)
            git_after = os.fstat(git_fd)
            if (
                not stat.S_ISDIR(lexical_git_after.st_mode)
                or (lexical_git_after.st_dev, lexical_git_after.st_ino)
                != (git_top.st_dev, git_top.st_ino)
                or (git_after.st_dev, git_after.st_ino) != (git_top.st_dev, git_top.st_ino)
            ):
                raise ExternalSlurmPrereqMaterializationError(
                    "external runner repository Git authority changed during verification",
                )
        finally:
            os.close(git_fd)
    finally:
        os.close(repo_fd)
    root.assert_stable()


def _normalize_repo_tree(
    repo_dir: Path,
    *,
    root: _BoundDirectory,
    resolved_sha: str,
) -> None:
    root.assert_stable()
    repo_fd = _open_child_directory(root.fd, repo_dir.name)
    try:
        git_fd = _open_child_directory(repo_fd, ".git")
        try:
            _write_canonical_git_config(
                git_fd,
                uid=os.geteuid(),
                gid=root.identity.st_gid,
            )
            _normalize_git_metadata(
                git_fd,
                uid=os.geteuid(),
                gid=root.identity.st_gid,
            )
            os.fchmod(git_fd, 0o750)
            _validate_single_git_authority(
                git_fd,
                uid=os.geteuid(),
                gid=root.identity.st_gid,
            )
            _validate_canonical_git_config(
                git_fd,
                uid=os.geteuid(),
                gid=root.identity.st_gid,
            )
        finally:
            os.close(git_fd)
        if _shared_repo_git(repo_dir, "rev-parse", "--show-object-format").strip() != "sha1":
            raise ExternalSlurmPrereqMaterializationError(
                "fresh external runner checkout object format is unsupported",
            )
        head = _shared_repo_git(repo_dir, "rev-parse", "HEAD^{commit}").strip()
        if head != resolved_sha:
            raise ExternalSlurmPrereqMaterializationError(
                "fresh external runner checkout does not match the resolved candidate",
            )
        index_entries = _index_entries(repo_dir)
        if index_entries != _commit_tree_entries(repo_dir, resolved_sha):
            raise ExternalSlurmPrereqMaterializationError(
                "fresh external runner checkout index does not match its commit tree",
            )
        index_modes = {relative: entry[0] for relative, entry in index_entries.items()}
        expected_directories = _expected_worktree_directories(index_entries)
        materialized, directories = _normalize_worktree(
            repo_fd,
            index_modes=index_modes,
            expected_directories=expected_directories,
            uid=os.geteuid(),
            gid=root.identity.st_gid,
        )
        os.fchmod(repo_fd, 0o750)
        if materialized != set(index_modes) or directories != expected_directories:
            raise ExternalSlurmPrereqMaterializationError(
                "fresh external runner checkout does not match its exact index",
            )
    finally:
        os.close(repo_fd)
    _validate_repo_tree(repo_dir, root=root, resolved_sha=resolved_sha)


def _repo_matches(
    repo_dir: Path,
    resolved_sha: str,
    *,
    root: _BoundDirectory,
) -> dict[str, Any] | None:
    try:
        _validate_repo_tree(repo_dir, root=root, resolved_sha=resolved_sha)
    except FileNotFoundError:
        return None
    except OSError as exc:
        raise ExternalSlurmPrereqMaterializationError(
            "existing external runner repository has unsafe authority",
        ) from exc
    return {
        "repo_dir": str(repo_dir),
        "repo_action": "matched",
        "repo_head": resolved_sha,
        "repo_status": "clean",
        "repo_group_id": root.identity.st_gid,
        "repo_mode": "0750",
    }


def _clone_repo_checkout(
    *,
    source_repo: Path,
    tmp_dir: Path,
    resolved_sha: str,
) -> None:
    _git_stdout(
        [
            "git",
            "--no-replace-objects",
            "clone",
            "--quiet",
            "--no-hardlinks",
            str(source_repo),
            str(tmp_dir),
        ]
    )
    _git_stdout(
        [
            "git",
            "--no-replace-objects",
            "-C",
            str(tmp_dir),
            "checkout",
            "--detach",
            resolved_sha,
        ]
    )


def _prepare_temp_root(
    temp_root: Path,
    *,
    root: _BoundDirectory,
) -> os.stat_result:
    temp_identity = temp_root.lstat()
    if (
        temp_identity.st_gid != root.identity.st_gid
        or temp_identity.st_uid != os.geteuid()
        or not stat.S_ISDIR(temp_identity.st_mode)
        or stat.S_ISLNK(temp_identity.st_mode)
    ):
        raise ExternalSlurmPrereqMaterializationError(
            "external runner temporary checkout did not inherit root authority",
        )
    if not temp_identity.st_mode & stat.S_ISGID:
        process_groups = {*os.getgroups(), os.getgid()}
        if root.identity.st_gid not in process_groups:
            raise ExternalSlurmPrereqMaterializationError(
                "external runner temporary checkout did not inherit setgid authority",
            )
        os.chmod(temp_root, 0o2700)
        temp_identity = temp_root.lstat()
    if stat.S_IMODE(temp_identity.st_mode) != 0o2700:
        raise ExternalSlurmPrereqMaterializationError(
            "external runner temporary checkout mode is unsafe",
        )
    return temp_identity


def _remove_directory_at(parent_fd: int, name: str, *, expected: os.stat_result) -> None:
    directory_fd = _open_child_directory(parent_fd, name)
    try:
        current = os.fstat(directory_fd)
        if (current.st_dev, current.st_ino) != (expected.st_dev, expected.st_ino):
            raise ExternalSlurmPrereqMaterializationError(
                "external runner temporary checkout authority changed during materialization",
            )
        for child in os.listdir(directory_fd):
            metadata = os.stat(child, dir_fd=directory_fd, follow_symlinks=False)
            if stat.S_ISDIR(metadata.st_mode):
                _remove_directory_at(directory_fd, child, expected=metadata)
            else:
                os.unlink(child, dir_fd=directory_fd)
    finally:
        os.close(directory_fd)
    os.rmdir(name, dir_fd=parent_fd)


def _entry_exists(directory_fd: int, name: str) -> bool:
    try:
        os.stat(name, dir_fd=directory_fd, follow_symlinks=False)
    except FileNotFoundError:
        return False
    return True


def _rename_directory_noreplace(
    source_fd: int,
    source_name: str,
    destination_fd: int,
    destination_name: str,
) -> None:
    """Publish exactly once without a check-then-replace race."""
    if _TEST_RENAME_NOREPLACE_BACKEND is not None:
        _TEST_RENAME_NOREPLACE_BACKEND(
            source_fd,
            source_name,
            destination_fd,
            destination_name,
        )
        return
    if not sys.platform.startswith("linux"):
        raise ExternalSlurmPrereqMaterializationError(
            "atomic external runner repository publication is unavailable",
        )
    libc = ctypes.CDLL(None, use_errno=True)
    encoded_source = os.fsencode(source_name)
    encoded_destination = os.fsencode(destination_name)
    try:
        rename = libc.renameat2
    except AttributeError as exc:
        raise ExternalSlurmPrereqMaterializationError(
            "atomic external runner repository publication is unavailable",
        ) from exc
    rename.argtypes = [
        ctypes.c_int,
        ctypes.c_char_p,
        ctypes.c_int,
        ctypes.c_char_p,
        ctypes.c_uint,
    ]
    rename.restype = ctypes.c_int
    result = rename(
        source_fd,
        encoded_source,
        destination_fd,
        encoded_destination,
        1,  # RENAME_NOREPLACE
    )
    if result == 0:
        return
    error_number = ctypes.get_errno()
    if error_number in {errno.EEXIST, errno.ENOTEMPTY}:
        raise ExternalSlurmPrereqMaterializationError(
            "external runner repository destination appeared during materialization",
        )
    raise ExternalSlurmPrereqMaterializationError(
        "atomic external runner repository publication failed safely",
    )


def _materialize_repo_dir(
    *,
    repo_dir: Path,
    source_repo: Path,
    resolved_sha: str,
    expected_ref: str,
) -> dict[str, Any]:
    root = _validate_repo_root(repo_dir, expected_ref)
    temp_name: str | None = None
    temp_identity: os.stat_result | None = None
    try:
        try:
            matched = _repo_matches(repo_dir, resolved_sha, root=root)
        except FileNotFoundError:
            matched = None
        if matched is not None:
            return matched
        if _entry_exists(root.fd, repo_dir.name):
            raise ExternalSlurmPrereqMaterializationError(
                "existing external runner repository does not exactly match the candidate",
            )

        temp_name = f".{repo_dir.name}.tmp-{secrets.token_hex(16)}"
        os.mkdir(temp_name, 0o700, dir_fd=root.fd)
        temp_root = repo_dir.parent / temp_name
        temp_identity = os.stat(temp_name, dir_fd=root.fd, follow_symlinks=False)
        checkout = temp_root / "checkout"
        temp_fd = _open_child_directory(root.fd, temp_name)
        try:
            temp_identity = _prepare_temp_root(temp_root, root=root)
            root.assert_stable()
            _clone_repo_checkout(
                source_repo=source_repo,
                tmp_dir=checkout,
                resolved_sha=resolved_sha,
            )
            root.assert_stable()
            temp_current = os.fstat(temp_fd)
            if (temp_current.st_dev, temp_current.st_ino) != (
                temp_identity.st_dev,
                temp_identity.st_ino,
            ):
                raise ExternalSlurmPrereqMaterializationError(
                    "external runner temporary checkout authority changed during materialization",
                )
            _normalize_repo_tree(
                checkout,
                root=_BoundDirectory(temp_root, temp_fd, temp_identity),
                resolved_sha=resolved_sha,
            )
            _rename_directory_noreplace(temp_fd, "checkout", root.fd, repo_dir.name)
        finally:
            os.close(temp_fd)

        _validate_repo_tree(repo_dir, root=root, resolved_sha=resolved_sha)
        return {
            "repo_dir": str(repo_dir),
            "repo_action": "created",
            "repo_head": resolved_sha,
            "repo_status": "clean",
            "repo_group_id": root.identity.st_gid,
            "repo_mode": "0750",
        }
    finally:
        try:
            if temp_name is not None and temp_identity is not None:
                try:
                    current = os.stat(temp_name, dir_fd=root.fd, follow_symlinks=False)
                except FileNotFoundError:
                    current = None
                if current is not None:
                    _remove_directory_at(root.fd, temp_name, expected=temp_identity)
        finally:
            os.close(root.fd)


def _materialize_external_slurm_runner_prerequisites(
    ctx: RolloutContext,
    profile_path: Path | str,
    step_dir: StepDir,
) -> list[dict[str, Any]]:
    from loom_cli.environment_state import (
        _external_slurm_policies,
        load_environment_state_profile,
    )

    try:
        profile = load_environment_state_profile(
            profile_path,
            variables=_release_vars(ctx),
            expected_environment=ctx.environment,
        )
    except Exception:
        return []

    settings = profile.external_slurm_runner_prerequisites
    if not settings or settings.get("materialize") is not True:
        return []

    configured_pools = settings.get("pools")
    checked_pools = set(configured_pools) if isinstance(configured_pools, list) else None
    require_worker_token_parity = bool(
        settings.get("require_worker_token_parity", False),
    )
    worker_token_env_key = str(
        settings.get("worker_token_env_key") or DEFAULT_WORKER_TOKEN_ENV_KEY,
    )
    worker_token: str | None = None
    if require_worker_token_parity:
        if not ctx.worker_token_source:
            raise ExternalSlurmPrereqMaterializationError(
                "external runner materialization requires --worker-token "
                "because require_worker_token_parity=true",
            )
        try:
            worker_token = resolve_secret_source(
                ctx.worker_token_source,
                flag_name="--worker-token",
            )
        except SecretSourceError as exc:
            raise ExternalSlurmPrereqMaterializationError(_safe_text(exc)) from None

    records: list[dict[str, Any]] = []
    source_repo = candidate_worktree(step_dir)
    expected_repo_ref = str(settings.get("expected_repo_ref") or ctx.image_tag)
    for policy in _external_slurm_policies(profile):
        pool_name = str(policy["pool_name"])
        if checked_pools is not None and pool_name not in checked_pools:
            continue
        actuator_config = policy.get("actuator_config", {})
        if not isinstance(actuator_config, dict):
            continue
        env_file = actuator_config.get("env_file")
        repo_dir = actuator_config.get("repo_dir")
        if not isinstance(env_file, str) or not isinstance(repo_dir, str):
            continue
        record = {
            "environment": policy["environment"],
            "pool_name": pool_name,
        }
        record.update(
            _materialize_env_file(
                env_file=Path(env_file).expanduser(),
                settings=settings,
                image_tag=ctx.image_tag,
                pool_name=pool_name,
                requested_concurrency=actuator_config.get("requested_concurrency"),
                worker_token=worker_token,
                worker_token_env_key=worker_token_env_key,
            )
        )
        record.update(
            _materialize_repo_dir(
                repo_dir=Path(repo_dir).expanduser(),
                source_repo=source_repo,
                resolved_sha=ctx.resolved_sha,
                expected_ref=expected_repo_ref,
            )
        )
        records.append(record)

    if records:
        _write_safe_json(
            step_dir.artifact_path("external-slurm-runner-prerequisites.json"),
            {"records": records},
        )
    return records


def _verify_external_slurm_runner_consumers(
    ctx: RolloutContext,
    step_dir: StepDir,
    records: list[dict[str, Any]],
) -> dict[str, Any] | None:
    """Verify the immutable target as qianyi on every fixed GB10 node."""
    if not records:
        return None
    if len(records) != 1 or ctx.scope != "current-gb10":
        raise ExternalSlurmPrereqMaterializationError(
            "external runner consumer verification requires one current-GB10 target",
        )

    from loom_cli.rollout.operator.preflight import ACTIVE_GB10_HOSTS
    from loom_cli.rollout.steps.s04_gb10_prep import (
        _gb10_prep_config_paths,
        _ssh,
        gb10_hosts_for,
    )

    record = records[0]
    repo_value = record.get("repo_dir")
    if (
        not isinstance(repo_value, str)
        or Path(repo_value).parent != _SHARED_WORKER_REPO_ROOT
        or record.get("repo_head") != ctx.resolved_sha
    ):
        raise ExternalSlurmPrereqMaterializationError(
            "external runner consumer target is not candidate-bound",
        )
    repo = Path(repo_value)
    try:
        consumer = pwd.getpwnam("qianyi")
        shared_gid = grp.getgrnam("sharedwork").gr_gid
        consumer_groups = {*os.getgrouplist(consumer.pw_name, consumer.pw_gid)}
    except (KeyError, OSError) as exc:
        raise ExternalSlurmPrereqMaterializationError(
            "external runner consumer identity is unavailable",
        ) from exc
    owner_uid = os.geteuid()
    if (
        consumer.pw_uid <= 0
        or shared_gid <= 0
        or shared_gid not in consumer_groups
        or record.get("repo_group_id") != shared_gid
    ):
        raise ExternalSlurmPrereqMaterializationError(
            "external runner consumer identity is not authorized",
        )

    try:
        _candidate_config, materialized_config = _gb10_prep_config_paths(ctx, step_dir)
        hosts = gb10_hosts_for(ctx, config_path=materialized_config)
    except CandidateToolingError as exc:
        raise ExternalSlurmPrereqMaterializationError(
            "external runner consumer SSH inputs are not candidate-bound",
        ) from exc
    if tuple(host.ssh_target for host in hosts) != ACTIVE_GB10_HOSTS:
        raise ExternalSlurmPrereqMaterializationError(
            "external runner consumer host set is not exact",
        )

    try:
        candidate_root = validate_candidate_loom_source(step_dir).resolve(strict=True)
        consumer_script = candidate_root / _SHARED_WORKER_REPO_CONSUMER
        consumer_script.relative_to(candidate_root)
        flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
        verifier_fd = os.open(consumer_script, flags)
        try:
            verifier_before = os.fstat(verifier_fd)
            if (
                not stat.S_ISREG(verifier_before.st_mode)
                or verifier_before.st_uid != owner_uid
                or stat.S_IMODE(verifier_before.st_mode) & 0o022
                or verifier_before.st_nlink != 1
                or not 0 < verifier_before.st_size <= 1024 * 1024
            ):
                raise ExternalSlurmPrereqMaterializationError(
                    "external runner consumer verifier is unsafe",
                )
            verifier_bytes = os.read(verifier_fd, verifier_before.st_size + 1)
            verifier_after = os.fstat(verifier_fd)
            if len(verifier_bytes) != verifier_before.st_size or (
                verifier_after.st_dev,
                verifier_after.st_ino,
                verifier_after.st_mtime_ns,
                verifier_after.st_size,
            ) != (
                verifier_before.st_dev,
                verifier_before.st_ino,
                verifier_before.st_mtime_ns,
                verifier_before.st_size,
            ):
                raise ExternalSlurmPrereqMaterializationError(
                    "external runner consumer verifier changed while read",
                )
        finally:
            os.close(verifier_fd)
        verifier_text = verifier_bytes.decode("utf-8")
    except (OSError, UnicodeError, ValueError) as exc:
        raise ExternalSlurmPrereqMaterializationError(
            "external runner consumer verifier is unavailable",
        ) from exc
    verifier_sha256 = hashlib.sha256(verifier_bytes).hexdigest()
    command = " ".join(
        shlex.quote(value)
        for value in (
            "/usr/bin/python3",
            "-",
            "--root",
            str(_SHARED_WORKER_REPO_ROOT),
            "--repo",
            str(repo),
            "--sha",
            ctx.resolved_sha,
            "--owner-uid",
            str(owner_uid),
            "--shared-gid",
            str(shared_gid),
            "--consumer-uid",
            str(consumer.pw_uid),
        )
    )
    artifact = step_dir.artifact_path("external-slurm-runner-consumer-verification.json")
    evidence_hash = hashlib.sha256()
    node_evidence: list[dict[str, object]] = []
    expected_keys = {
        "head",
        "index_sha256",
        "probe_file_sha256",
        "root_device",
        "root_inode",
        "target_device",
        "target_inode",
        "tree_content_sha256",
        "tracked_entries",
    }
    content_identity: tuple[str, str, str, int] | None = None
    for host in hosts:
        result = _ssh(host, command, stdin_text=verifier_text)
        safe = False
        parsed: dict[str, object] | None = None
        if (
            result.returncode == 0
            and not result.stderr
            and 0 < len(result.stdout) <= 1024
            and result.stdout.count("\n") <= 1
        ):
            try:
                candidate = json.loads(result.stdout)
            except (TypeError, ValueError):
                candidate = None
            if isinstance(candidate, dict) and set(candidate) == expected_keys:
                parsed = candidate
                numeric_keys = {
                    "root_device",
                    "root_inode",
                    "target_device",
                    "target_inode",
                    "tracked_entries",
                }
                safe = bool(
                    candidate.get("head") == ctx.resolved_sha
                    and all(
                        isinstance(candidate.get(key), str)
                        and re.fullmatch(r"[0-9a-f]{64}", str(candidate[key]))
                        for key in {
                            "index_sha256",
                            "probe_file_sha256",
                            "tree_content_sha256",
                        }
                    )
                    and all(
                        type(candidate.get(key)) is int and int(candidate[key]) > 0
                        for key in numeric_keys
                    )
                )
                if safe:
                    candidate_identity = (
                        str(candidate["index_sha256"]),
                        str(candidate["probe_file_sha256"]),
                        str(candidate["tree_content_sha256"]),
                        int(candidate["tracked_entries"]),
                    )
                    if content_identity is None:
                        content_identity = candidate_identity
                    elif candidate_identity != content_identity:
                        safe = False
        if not safe or parsed is None:
            evidence_hash.update(host.ssh_target.encode("ascii"))
            evidence_hash.update(b"\0failed\0")
            _write_safe_json(
                artifact,
                {
                    "evidence_sha256": evidence_hash.hexdigest(),
                    "expected_host_count": len(ACTIVE_GB10_HOSTS),
                    "host_count": len(node_evidence),
                    "nodes": node_evidence,
                    "passed": False,
                    "resolved_sha": ctx.resolved_sha,
                    "verifier_sha256": verifier_sha256,
                },
            )
            raise ExternalSlurmPrereqMaterializationError(
                "external runner consumer verification failed safely",
            )
        evidence_hash.update(host.ssh_target.encode("ascii"))
        evidence_hash.update(b"\0")
        evidence_hash.update(result.stdout.strip().encode("ascii"))
        evidence_hash.update(b"\0")
        node_evidence.append(
            {
                "host": host.ssh_target,
                "root_device": parsed["root_device"],
                "root_inode": parsed["root_inode"],
                "target_device": parsed["target_device"],
                "target_inode": parsed["target_inode"],
            }
        )

    if content_identity is None:
        raise ExternalSlurmPrereqMaterializationError(
            "external runner consumer evidence is incomplete",
        )
    evidence = {
        "evidence_sha256": evidence_hash.hexdigest(),
        "expected_host_count": len(ACTIVE_GB10_HOSTS),
        "host_count": len(node_evidence),
        "index_sha256": content_identity[0],
        "nodes": node_evidence,
        "passed": True,
        "probe_file_sha256": content_identity[1],
        "resolved_sha": ctx.resolved_sha,
        "tree_content_sha256": content_identity[2],
        "tracked_entries": content_identity[3],
        "verifier_sha256": verifier_sha256,
    }
    _write_safe_json(artifact, evidence)
    return evidence


class EnvStateStep(BaseStep):
    number = 11
    name = "env-state"

    def _run_impl(self, ctx: RolloutContext, step_dir: StepDir) -> RunResult:
        try:
            cwd = candidate_loom_cwd(step_dir)
            env = candidate_loom_env(step_dir)
            profile_path = _candidate_profile_path(ctx, step_dir)
        except CandidateToolingError as exc:
            message = _safe_text(exc)
            _write_safe_text(step_dir.stderr_path(), message + "\n")
            return RunResult(exit_code=2, error=message)

        if profile_path is None:
            _write_safe_text(
                step_dir.stdout_path(),
                "no env_state_profile declared in candidate cluster-config; skipping.\n",
            )
            return RunResult(
                exit_code=0,
                summary="no env-state profile; step is a no-op",
            )

        try:
            catalog_plan = _catalog_provisioning_plan(ctx, profile_path, env)
        except CatalogProvisioningError as exc:
            message = _safe_text(exc)
            _write_safe_text(step_dir.stderr_path(), message + "\n")
            return RunResult(exit_code=2, error=message)
        _materialize_rollout_root_profile(
            ctx,
            source_profile=profile_path,
            step_dir=step_dir,
        )

        release_vars = [
            "--var",
            f"IMAGE_TAG={ctx.image_tag}",
            "--var",
            f"ENV_CONFIG_VERSION={ctx.image_tag}",
            "--var",
            f"GIT_SHA={ctx.resolved_sha}",
        ]
        admin_args = [
            "--admin-token",
            ctx.admin_token_source,
        ]
        if ctx.expect_admin_token_fingerprint:
            admin_args.extend(
                [
                    "--expect-admin-token-fingerprint",
                    ctx.expect_admin_token_fingerprint,
                ]
            )
        try:
            materialized = _materialize_external_slurm_runner_prerequisites(
                ctx,
                profile_path,
                step_dir,
            )
            consumer_evidence = _verify_external_slurm_runner_consumers(
                ctx,
                step_dir,
                materialized,
            )
        except ExternalSlurmPrereqMaterializationError as exc:
            message = _safe_text(exc)
            _write_safe_text(step_dir.stderr_path(), message + "\n")
            consumer_artifact = step_dir.artifact_path(
                "external-slurm-runner-consumer-verification.json"
            )
            return RunResult(
                exit_code=2,
                error=message,
                artifacts=(
                    {"external_runner_consumer_verification": str(consumer_artifact)}
                    if consumer_artifact.exists()
                    else {}
                ),
            )
        try:
            _wait_for_control_plane(ctx, step_dir)
        except ControlPlaneReadinessError as exc:
            message = _safe_text(exc)[:200]
            _write_safe_text(
                step_dir.stdout_path(),
                "# external-slurm-runner-prerequisites\n"
                f"materialized {len(materialized)} external runner prerequisite set(s)\n"
                "# control-plane-readiness\n",
            )
            _write_safe_text(
                step_dir.stderr_path(),
                f"# control-plane-readiness\n{message}\n",
            )
            return RunResult(
                exit_code=2,
                error=f"control-plane readiness failed: {message}",
                artifacts={
                    "control_plane_readiness": str(
                        step_dir.artifact_path("control-plane-readiness.json")
                    ),
                    "environment_state_profile_materialization": str(
                        step_dir.artifact_path("environment-state-profile-materialization.json")
                    ),
                },
            )
        apply_ = run_captured(
            candidate_loom_argv(
                "admin",
                "environment-state",
                "apply",
                "--cp-url",
                ctx.cp_url,
                *admin_args,
                "--file",
                str(profile_path),
                "--environment",
                ctx.environment,
                *_broker_mutation_args(ctx),
                *release_vars,
            ),
            cwd=cwd,
            env=env,
        )
        if apply_.returncode != 0:
            _write_safe_text(
                step_dir.stdout_path(),
                "# external-slurm-runner-prerequisites\n"
                f"materialized {len(materialized)} external runner prerequisite set(s)\n"
                f"# apply\n{apply_.stdout}\n",
            )
            _write_safe_text(
                step_dir.stderr_path(),
                f"# apply\n{apply_.stderr}\n",
            )
            return RunResult(
                exit_code=apply_.returncode,
                error=f"env-state apply failed: {_safe_text(apply_.stderr)[:200].strip()}",
            )

        catalog_summary = "catalog provisioning not required"
        catalog_stdout = ""
        catalog_stderr = ""
        catalog_artifact: str | None = None
        if catalog_plan is not None:
            catalog_result = _run_catalog_provisioning(
                catalog_plan,
                cwd=cwd,
                step_dir=step_dir,
            )
            catalog_stdout = step_dir.artifact_path(
                "catalog-provisioning.stdout",
            ).read_text(encoding="utf-8")
            catalog_stderr = step_dir.artifact_path(
                "catalog-provisioning.stderr",
            ).read_text(encoding="utf-8")
            catalog_artifact = str(step_dir.artifact_path("catalog-provisioning.json"))
            if catalog_result is not None:
                _write_safe_text(
                    step_dir.stdout_path(),
                    "# external-slurm-runner-prerequisites\n"
                    f"materialized {len(materialized)} external runner prerequisite set(s)\n"
                    f"# apply\n{apply_.stdout}\n"
                    f"# catalog-provisioning\n{catalog_stdout}\n",
                )
                _write_safe_text(
                    step_dir.stderr_path(),
                    f"# apply\n{apply_.stderr}\n# catalog-provisioning\n{catalog_stderr}\n",
                )
                return catalog_result
            catalog_summary = "catalog provisioning exited 0"

        check_argv = environment_state_check_argv(
            ctx,
            step_dir,
            profile_path=profile_path,
        )
        assert check_argv is not None
        check = run_captured(check_argv, cwd=cwd, env=env)
        _write_safe_text(
            step_dir.artifact_path("environment-state-check-attempt-1.json"),
            check.stdout,
        )
        deferred_gb10_status = check.returncode != 0 and _is_gb10_node_status_drift_only(
            check.stdout
        )
        check_log = ""
        if deferred_gb10_status:
            check_log = (
                "gb10 node-status drift deferred to release-gate; "
                "gb10-prep runs after env-state and starts node-agent apply\n"
            )
        retry_log = step_dir.artifact_path("environment-state-check.retries.log")
        _write_safe_text(retry_log, check_log)
        _write_safe_text(
            step_dir.artifact_path("environment-state-check.json"),
            check.stdout,
        )
        _write_safe_text(
            step_dir.stdout_path(),
            "# external-slurm-runner-prerequisites\n"
            f"materialized {len(materialized)} external runner prerequisite set(s)\n"
            f"# apply\n{apply_.stdout}\n"
            f"# catalog-provisioning\n{catalog_stdout}\n"
            f"# check\n{check.stdout}\n",
        )
        _write_safe_text(
            step_dir.stderr_path(),
            f"# apply\n{apply_.stderr}\n"
            f"# catalog-provisioning\n{catalog_stderr}\n"
            f"# check\n{check.stderr}\n",
        )
        artifacts = {
            "environment_state_check": str(step_dir.artifact_path("environment-state-check.json")),
            "environment_state_profile_materialization": str(
                step_dir.artifact_path("environment-state-profile-materialization.json")
            ),
        }
        if consumer_evidence is not None:
            artifacts["external_runner_consumer_verification"] = str(
                step_dir.artifact_path("external-slurm-runner-consumer-verification.json")
            )
        if catalog_artifact is not None:
            artifacts["catalog_provisioning"] = catalog_artifact
        if check.returncode != 0:
            if deferred_gb10_status:
                return RunResult(
                    exit_code=0,
                    summary=(
                        f"env-state apply clean; {catalog_summary}; "
                        "GB10 node-status convergence deferred to release-gate"
                    ),
                    artifacts=artifacts,
                )
            return RunResult(
                exit_code=check.returncode,
                error=("env-state check reported drift: " + _safe_text(check.stdout).strip()[:200]),
            )
        return RunResult(
            exit_code=0,
            summary=f"env-state apply + check clean; {catalog_summary}",
            artifacts=artifacts,
        )
