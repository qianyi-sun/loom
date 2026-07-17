"""Step 11 — environment-state apply + desired-state check (#340, #593).

Applies the release environment-state profile (from cluster-config's
declared path) and records an immediate check. Pure GB10 node-status drift is
deferred because GB10 prep now starts after desired state is written; final
node convergence is checked again by release-gate. The #331 fix to
environment-state apply ensures negative desired states (enabled=false /
active=false) actually stop and disable supervisors.
"""

from __future__ import annotations

import glob
import grp
import hashlib
import io
import json
import os
import re
import secrets
import shlex
import shutil
import socket
import stat
import subprocess
import tempfile
import threading
import time
import urllib.error
import urllib.request
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
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
_SHARED_WORKER_REPO_ROOT = Path(
    "/shared_work/qianyi/.loom-staging-rollout/worker-repos"
)
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


def _validate_repo_root(repo_dir: Path, expected_ref: str) -> os.stat_result:
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

    current = Path("/")
    for component in repo_dir.parent.parts[1:]:
        current /= component
        try:
            metadata = current.lstat()
        except OSError as exc:
            raise ExternalSlurmPrereqMaterializationError(
                f"external runner repository authority is unavailable: {current}",
            ) from exc
        if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISDIR(metadata.st_mode):
            raise ExternalSlurmPrereqMaterializationError(
                f"external runner repository authority is not a real directory: {current}",
            )

    root = repo_dir.parent.lstat()
    mode = stat.S_IMODE(root.st_mode)
    try:
        shared_work_gid = grp.getgrnam("sharedwork").gr_gid
    except KeyError as exc:
        raise ExternalSlurmPrereqMaterializationError(
            "sharedwork group is unavailable for external runner materialization",
        ) from exc
    if (
        root.st_uid != os.geteuid()
        or root.st_gid != shared_work_gid
        or mode != 0o2750
    ):
        raise ExternalSlurmPrereqMaterializationError(
            "external runner repository root has unsafe owner or mode",
        )
    return root


def _validate_repo_tree(repo_dir: Path, *, root: os.stat_result) -> None:
    try:
        top = repo_dir.lstat()
    except FileNotFoundError:
        return
    if (
        stat.S_ISLNK(top.st_mode)
        or not stat.S_ISDIR(top.st_mode)
        or top.st_uid != os.geteuid()
        or top.st_gid != root.st_gid
        or stat.S_IMODE(top.st_mode) != 0o750
    ):
        raise ExternalSlurmPrereqMaterializationError(
            "existing external runner repository has unsafe authority",
        )
    for directory, names, files in os.walk(repo_dir, followlinks=False):
        for name in [*names, *files]:
            path = Path(directory, name)
            metadata = path.lstat()
            if metadata.st_uid != os.geteuid() or metadata.st_gid != root.st_gid:
                raise ExternalSlurmPrereqMaterializationError(
                    "external runner repository contains foreign-owned content",
                )
            if stat.S_ISLNK(metadata.st_mode):
                continue
            if stat.S_ISDIR(metadata.st_mode):
                if stat.S_IMODE(metadata.st_mode) & 0o022:
                    raise ExternalSlurmPrereqMaterializationError(
                        "external runner repository contains a writable directory",
                    )
                continue
            if not stat.S_ISREG(metadata.st_mode) or metadata.st_nlink != 1:
                raise ExternalSlurmPrereqMaterializationError(
                    "external runner repository contains an unsafe file",
                )
            if stat.S_IMODE(metadata.st_mode) & 0o022:
                raise ExternalSlurmPrereqMaterializationError(
                    "external runner repository contains a writable file",
                )


def _normalize_repo_tree(repo_dir: Path, *, root: os.stat_result) -> None:
    for directory, names, files in os.walk(repo_dir, topdown=False, followlinks=False):
        for name in files:
            path = Path(directory, name)
            metadata = path.lstat()
            if stat.S_ISLNK(metadata.st_mode):
                continue
            if not stat.S_ISREG(metadata.st_mode) or metadata.st_nlink != 1:
                raise ExternalSlurmPrereqMaterializationError(
                    "fresh external runner checkout contains an unsafe file",
                )
            os.chmod(path, 0o750 if metadata.st_mode & stat.S_IXUSR else 0o640)
        for name in names:
            path = Path(directory, name)
            metadata = path.lstat()
            if stat.S_ISLNK(metadata.st_mode):
                continue
            if not stat.S_ISDIR(metadata.st_mode):
                raise ExternalSlurmPrereqMaterializationError(
                    "fresh external runner checkout contains an unsafe entry",
                )
            os.chmod(path, 0o750)
        os.chmod(directory, 0o750)
    _validate_repo_tree(repo_dir, root=root)


def _repo_status(
    repo_dir: Path,
    *,
    root: os.stat_result,
) -> tuple[str, str] | None:
    try:
        _validate_repo_tree(repo_dir, root=root)
    except FileNotFoundError:
        return None
    git_dir = repo_dir / ".git"
    try:
        git_metadata = git_dir.lstat()
    except FileNotFoundError:
        return None
    if stat.S_ISLNK(git_metadata.st_mode) or not stat.S_ISDIR(git_metadata.st_mode):
        return None
    try:
        head = _git_stdout(["git", "-C", str(repo_dir), "rev-parse", "HEAD"])
        status = _git_stdout(
            [
                "git",
                "-C",
                str(repo_dir),
                "status",
                "--short",
                "--untracked-files=no",
            ]
        )
    except ExternalSlurmPrereqMaterializationError:
        return None
    return head, status


def _repo_matches(
    repo_dir: Path,
    resolved_sha: str,
    *,
    root: os.stat_result,
) -> dict[str, Any] | None:
    status = _repo_status(repo_dir, root=root)
    if status is None:
        return None
    head, dirty = status
    if dirty:
        return None
    if head != resolved_sha:
        return None
    return {
        "repo_dir": str(repo_dir),
        "repo_action": "matched",
        "repo_head": head,
        "repo_status": "clean",
        "repo_group_id": root.st_gid,
        "repo_mode": "0750",
    }


def _clone_repo_checkout(
    *,
    source_repo: Path,
    tmp_dir: Path,
    resolved_sha: str,
) -> None:
    origin_result = run_captured(
        [
            "git",
            "-C",
            str(source_repo),
            "config",
            "--get",
            "remote.origin.url",
        ]
    )
    source_url = (
        origin_result.stdout.strip()
        if origin_result.returncode == 0 and origin_result.stdout.strip()
        else str(source_repo)
    )
    try:
        _git_stdout(
            ["git", "clone", "--quiet", "--no-hardlinks", source_url, str(tmp_dir)]
        )
        _git_stdout(["git", "-C", str(tmp_dir), "checkout", "--detach", resolved_sha])
    except ExternalSlurmPrereqMaterializationError:
        if tmp_dir.exists():
            shutil.rmtree(tmp_dir)
        _git_stdout(
            [
                "git",
                "clone",
                "--quiet",
                "--no-hardlinks",
                str(source_repo),
                str(tmp_dir),
            ]
        )
        _git_stdout(["git", "-C", str(tmp_dir), "checkout", "--detach", resolved_sha])
    dirty = _git_stdout(
        [
            "git",
            "-C",
            str(tmp_dir),
            "status",
            "--short",
            "--untracked-files=no",
        ]
    )
    if dirty:
        raise ExternalSlurmPrereqMaterializationError(
            f"fresh external runner checkout is dirty: {tmp_dir}",
        )


def _prepare_temp_root(
    temp_root: Path,
    *,
    root: os.stat_result,
) -> os.stat_result:
    temp_identity = temp_root.lstat()
    if (
        temp_identity.st_gid != root.st_gid
        or temp_identity.st_uid != os.geteuid()
        or not stat.S_ISDIR(temp_identity.st_mode)
        or stat.S_ISLNK(temp_identity.st_mode)
    ):
        raise ExternalSlurmPrereqMaterializationError(
            "external runner temporary checkout did not inherit root authority",
        )
    if not temp_identity.st_mode & stat.S_ISGID:
        process_groups = {*os.getgroups(), os.getgid()}
        if root.st_gid not in process_groups:
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


def _materialize_repo_dir(
    *,
    repo_dir: Path,
    source_repo: Path,
    resolved_sha: str,
    expected_ref: str,
) -> dict[str, Any]:
    root = _validate_repo_root(repo_dir, expected_ref)
    matched = _repo_matches(repo_dir, resolved_sha, root=root)
    if matched is not None:
        return matched

    temp_root = Path(
        tempfile.mkdtemp(prefix=f".{repo_dir.name}.tmp-", dir=repo_dir.parent)
    )
    temp_identity = temp_root.lstat()
    checkout = temp_root / "checkout"
    try:
        temp_identity = _prepare_temp_root(temp_root, root=root)
        _clone_repo_checkout(
            source_repo=source_repo,
            tmp_dir=checkout,
            resolved_sha=resolved_sha,
        )
        _normalize_repo_tree(checkout, root=root)

        action = "created"
        previous: Path | None = None
        if repo_dir.exists() or repo_dir.is_symlink():
            _validate_repo_tree(repo_dir, root=root)
            previous = repo_dir.with_name(
                f".{repo_dir.name}.previous-{secrets.token_hex(16)}",
            )
            if previous.exists() or previous.is_symlink():
                raise ExternalSlurmPrereqMaterializationError(
                    "external runner previous-checkout destination already exists",
                )
            repo_dir.rename(previous)
            action = "replaced"
        checkout.rename(repo_dir)
        _validate_repo_tree(repo_dir, root=root)
        head = _git_stdout(["git", "-C", str(repo_dir), "rev-parse", "HEAD"])
        if head != resolved_sha:
            raise ExternalSlurmPrereqMaterializationError(
                "published external runner checkout does not match the resolved candidate",
            )
        result = {
            "repo_dir": str(repo_dir),
            "repo_action": action,
            "repo_head": head,
            "repo_status": "clean",
            "repo_group_id": root.st_gid,
            "repo_mode": "0750",
        }
        if previous is not None:
            result["repo_previous"] = str(previous)
        return result
    finally:
        try:
            current = temp_root.lstat()
        except FileNotFoundError:
            current = None
        if current is not None:
            if (
                not stat.S_ISDIR(current.st_mode)
                or stat.S_ISLNK(current.st_mode)
                or current.st_uid != os.geteuid()
                or current.st_dev != temp_identity.st_dev
                or current.st_ino != temp_identity.st_ino
            ):
                raise ExternalSlurmPrereqMaterializationError(
                    "external runner temporary checkout authority changed during materialization",
                )
            shutil.rmtree(temp_root)


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
        except ExternalSlurmPrereqMaterializationError as exc:
            message = _safe_text(exc)
            _write_safe_text(step_dir.stderr_path(), message + "\n")
            return RunResult(exit_code=2, error=message)
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
