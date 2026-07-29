#!/usr/bin/env python3
"""Produce trusted #896 platform-health and containment observations.

The public ``collect`` command accepts only an existing #1023 acceptance
session identity and one fixed checkpoint name.  Candidate identities, phase
completion, node inventory, Kubernetes objects, Slurm identities, Docker
containers, cgroups, and storage counters are read from fixed root authorities;
callers cannot supply evidence JSON, paths, commands, namespaces, or pass
results.

``observe-node`` is an internal, stdin-only node-authority action.  It returns
one sanitized closed-world observation for the node on which it runs.
"""

from __future__ import annotations

import argparse
import base64
import fcntl
import hashlib
import json
import os
import re
import socket
import stat
import subprocess
import sys
import tomllib
import uuid
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path, PurePosixPath
from typing import Any, Final

SCHEMA_VERSION: Final = 1
PLATFORM_HEALTH_EVIDENCE_TTL: Final = timedelta(minutes=15)
REPO_ROOT: Final = Path(__file__).resolve().parents[2]
DEFAULT_CONFIG: Final = REPO_ROOT / "deploy/developer-sandboxes/platform-health-authority.toml"
INSTALLED_CONFIG: Final = Path(
    "/opt/loom-developer-sandbox-node-authority/source/"
    "deploy/developer-sandboxes/platform-health-authority.toml",
)
CHECKPOINTS: Final = (
    "baseline",
    "mixed_non_loom",
    "cancel_cleanup",
    "worker_crash",
    "final_drain",
)
CHECKPOINT_GROUPS: Final = {
    "baseline": "baseline",
    "mixed_non_loom": "during",
    "cancel_cleanup": "during",
    "worker_crash": "during",
    "final_drain": "after",
}
SANDBOXES: Final = ("qianyi", "hongjian", "devansh")
POOLS: Final = ("oldlab", "gb10")
ROLES: Final = ("worker", "trial", "verifier", "sidecar")
EXCLUDED_NODES: Final = frozenset({"trt-gb10-7"})
EXPECTED_HOST_ALIASES: Final = {
    **{f"oldlab-{index}": f"trt-eai-oldlab-{index}" for index in range(1, 6)},
    "trt-gb10-1": "gx10-01c7",
    "trt-gb10-2": "gx10-0fca",
    "trt-gb10-3": "gx10-0f0d",
    "trt-gb10-4": "gx10-0d93",
    "trt-gb10-5": "gx10-1036",
    "trt-gb10-6": "gx10-1000",
    "trt-gb10-8": "gx10-db22",
    "trt-gb10-9": "gx10-16f6",
    "trt-gb10-10": "gx10-0f82",
    "trt-gb10-11": "gx10-c38b",
    "trt-gb10-12": "gx10-e45f",
    "trt-gb10-13": "gx10-fc5d",
    "trt-gb10-14": "gx10-0a49",
    "trt-gb10-15": "gx10-0152",
}
SESSION_RE: Final = re.compile(r"^[0-9a-f]{32}$")
SHA_RE: Final = re.compile(r"^[0-9a-f]{40}$")
DIGEST_RE: Final = re.compile(r"^[0-9a-f]{64}$")
JOB_ID_RE: Final = re.compile(r"^[1-9][0-9]*(?:_[0-9]+)?$")
CONTAINER_ID_RE: Final = re.compile(r"^[0-9a-f]{12,64}$")
SAFE_NAME_RE: Final = re.compile(r"^[A-Za-z0-9_.-]{1,128}$")
MAX_FILE_BYTES: Final = 16 * 1024 * 1024
MAX_COMMAND_BYTES: Final = 8 * 1024 * 1024
ROOT_UID: Final = 0
ROOT_GID: Final = 0
NODE_REQUEST_FIELDS: Final = {
    "schema_version",
    "kind",
    "session_id",
    "checkpoint",
    "checkpoint_group",
    "expected_node",
    "expected_host",
    "since_at",
    "candidates",
}
NODE_RESULT_FIELDS: Final = {
    "schema_version",
    "kind",
    "session_id",
    "checkpoint",
    "checkpoint_group",
    "node",
    "host",
    "pool",
    "observed_at",
    "capacity",
    "io",
    "active_jobs",
    "terminal_jobs",
    "orphan_container_ids",
}
Run = Callable[..., subprocess.CompletedProcess[Any]]
Clock = Callable[[], datetime]
Transport = Callable[[str, bytes], dict[str, Any]]


class PlatformHealthError(RuntimeError):
    """A secret-safe platform-health authority failure."""


@dataclass(frozen=True, slots=True)
class Config:
    collector_host: str
    namespace: str
    longhorn_namespace: str
    kubeconfig: Path
    acceptance_state_root: Path
    authority_state_root: Path
    node_transport: Path
    minio_statefulset: str
    minio_pdb: str
    max_checkpoint_seconds: int
    max_clock_skew_seconds: int
    minimum_oldlab_free_cpu_cores: int
    minimum_oldlab_free_memory_bytes: int
    maximum_cpu_busy_ratio: float
    oldlab_nodes: tuple[str, ...]
    gb10_nodes: tuple[str, ...]
    host_aliases: Mapping[str, str]

    @property
    def nodes(self) -> tuple[str, ...]:
        return (*self.oldlab_nodes, *self.gb10_nodes)


def _canonical(value: Any) -> bytes:
    return (json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n").encode()


def _digest(value: Any) -> str:
    return hashlib.sha256(_canonical(value)).hexdigest()


def _now() -> datetime:
    return datetime.now(UTC)


def _iso(value: datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _timestamp(value: object, *, label: str) -> datetime:
    if not isinstance(value, str):
        raise PlatformHealthError(f"{label} timestamp is invalid")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise PlatformHealthError(f"{label} timestamp is invalid") from exc
    if parsed.tzinfo is None:
        raise PlatformHealthError(f"{label} timestamp is invalid")
    return parsed.astimezone(UTC)


def _host() -> str:
    return socket.gethostname().split(".", 1)[0].rstrip(".").lower()


def _require_root() -> None:
    if os.getuid() != ROOT_UID or os.geteuid() != ROOT_UID:
        raise PlatformHealthError("platform-health authority requires root")


def load_config(path: Path) -> Config:
    try:
        raw = path.read_bytes()
        payload = tomllib.loads(raw.decode())
    except (OSError, UnicodeDecodeError, tomllib.TOMLDecodeError) as exc:
        raise PlatformHealthError("platform-health authority config is invalid") from exc
    fields = {
        "schema_version",
        "collector_host",
        "namespace",
        "longhorn_namespace",
        "kubeconfig",
        "acceptance_state_root",
        "authority_state_root",
        "node_transport",
        "minio_statefulset",
        "minio_pdb",
        "max_checkpoint_seconds",
        "max_clock_skew_seconds",
        "minimum_oldlab_free_cpu_cores",
        "minimum_oldlab_free_memory_bytes",
        "maximum_cpu_busy_ratio",
        "oldlab_nodes",
        "gb10_nodes",
        "host_aliases",
    }
    if not isinstance(payload, dict) or set(payload) != fields:
        raise PlatformHealthError("platform-health authority config has an invalid shape")
    oldlab = tuple(payload["oldlab_nodes"])
    gb10 = tuple(payload["gb10_nodes"])
    aliases = payload["host_aliases"]
    nodes = (*oldlab, *gb10)
    if (
        payload["schema_version"] != SCHEMA_VERSION
        or payload["collector_host"] != "trt-eai-oldlab-2"
        or payload["namespace"] != "loom-staging"
        or payload["longhorn_namespace"] != "longhorn-system"
        or not isinstance(aliases, dict)
        or tuple(aliases) != nodes
        or aliases != EXPECTED_HOST_ALIASES
        or len(set(aliases.values())) != len(aliases)
        or len(nodes) != 19
        or len(set(nodes)) != len(nodes)
        or EXCLUDED_NODES & set(nodes)
        or len(oldlab) != 5
        or len(gb10) != 14
        or any(not isinstance(node, str) or not SAFE_NAME_RE.fullmatch(node) for node in nodes)
        or any(
            not isinstance(host, str) or not SAFE_NAME_RE.fullmatch(host)
            for host in aliases.values()
        )
        or any(
            not isinstance(payload[field], int)
            or isinstance(payload[field], bool)
            or payload[field] < 1
            for field in (
                "max_checkpoint_seconds",
                "max_clock_skew_seconds",
                "minimum_oldlab_free_cpu_cores",
                "minimum_oldlab_free_memory_bytes",
            )
        )
        or not isinstance(payload["maximum_cpu_busy_ratio"], float)
        or not 0 < payload["maximum_cpu_busy_ratio"] < 1
    ):
        raise PlatformHealthError("platform-health authority config binding is invalid")
    absolute_paths = (
        Path(payload["kubeconfig"]),
        Path(payload["acceptance_state_root"]),
        Path(payload["authority_state_root"]),
        Path(payload["node_transport"]),
    )
    if any(not item.is_absolute() for item in absolute_paths):
        raise PlatformHealthError("platform-health authority config path is invalid")
    return Config(
        collector_host=payload["collector_host"],
        namespace=payload["namespace"],
        longhorn_namespace=payload["longhorn_namespace"],
        kubeconfig=absolute_paths[0],
        acceptance_state_root=absolute_paths[1],
        authority_state_root=absolute_paths[2],
        node_transport=absolute_paths[3],
        minio_statefulset=payload["minio_statefulset"],
        minio_pdb=payload["minio_pdb"],
        max_checkpoint_seconds=payload["max_checkpoint_seconds"],
        max_clock_skew_seconds=payload["max_clock_skew_seconds"],
        minimum_oldlab_free_cpu_cores=payload["minimum_oldlab_free_cpu_cores"],
        minimum_oldlab_free_memory_bytes=payload["minimum_oldlab_free_memory_bytes"],
        maximum_cpu_busy_ratio=payload["maximum_cpu_busy_ratio"],
        oldlab_nodes=oldlab,
        gb10_nodes=gb10,
        host_aliases=dict(aliases),
    )


def _read_secure_bytes(
    path: Path,
    *,
    label: str,
    mode: int = 0o600,
    require_root: bool = True,
) -> bytes:
    if not path.is_absolute():
        raise PlatformHealthError(f"{label} path is invalid")
    parent_descriptor = os.open("/", os.O_RDONLY | os.O_DIRECTORY)
    try:
        for part in path.parts[1:-1]:
            child = os.open(
                part,
                os.O_RDONLY
                | os.O_DIRECTORY
                | getattr(os, "O_CLOEXEC", 0)
                | getattr(os, "O_NOFOLLOW", 0),
                dir_fd=parent_descriptor,
            )
            os.close(parent_descriptor)
            parent_descriptor = child
            metadata = os.fstat(parent_descriptor)
            if (
                not stat.S_ISDIR(metadata.st_mode)
                or (require_root and (metadata.st_uid, metadata.st_gid) != (ROOT_UID, ROOT_GID))
                or stat.S_IMODE(metadata.st_mode) & 0o022
            ):
                raise PlatformHealthError(f"{label} parent directory is unsafe")
        before = os.stat(path.name, dir_fd=parent_descriptor, follow_symlinks=False)
        descriptor = os.open(
            path.name,
            os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0),
            dir_fd=parent_descriptor,
        )
        try:
            opened = os.fstat(descriptor)
            if (
                not stat.S_ISREG(opened.st_mode)
                or opened.st_nlink != 1
                or (require_root and (opened.st_uid, opened.st_gid) != (ROOT_UID, ROOT_GID))
                or stat.S_IMODE(opened.st_mode) != mode
                or (before.st_dev, before.st_ino) != (opened.st_dev, opened.st_ino)
                or opened.st_size > MAX_FILE_BYTES
            ):
                raise PlatformHealthError(f"{label} file is unsafe")
            chunks: list[bytes] = []
            remaining = opened.st_size
            while remaining:
                chunk = os.read(descriptor, min(remaining, 65536))
                if not chunk:
                    raise PlatformHealthError(f"{label} file changed while read")
                chunks.append(chunk)
                remaining -= len(chunk)
            raw = b"".join(chunks)
            after = os.stat(path.name, dir_fd=parent_descriptor, follow_symlinks=False)
            if (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns) != (
                opened.st_dev,
                opened.st_ino,
                opened.st_size,
                opened.st_mtime_ns,
            ):
                raise PlatformHealthError(f"{label} file changed while read")
            return raw
        finally:
            os.close(descriptor)
    except PlatformHealthError:
        raise
    except OSError as exc:
        raise PlatformHealthError(f"{label} file is unavailable") from exc
    finally:
        os.close(parent_descriptor)


def _secure_json(path: Path, *, label: str) -> tuple[Any, bytes]:
    raw = _read_secure_bytes(path, label=label)
    try:
        payload = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise PlatformHealthError(f"{label} JSON is invalid") from exc
    if raw != _canonical(payload):
        raise PlatformHealthError(f"{label} JSON is not canonical")
    return payload, raw


def _ensure_directory(path: Path) -> None:
    path.mkdir(mode=0o700, parents=True, exist_ok=True)
    metadata = path.lstat()
    if (
        not stat.S_ISDIR(metadata.st_mode)
        or stat.S_ISLNK(metadata.st_mode)
        or (metadata.st_uid, metadata.st_gid) != (ROOT_UID, ROOT_GID)
        or stat.S_IMODE(metadata.st_mode) != 0o700
    ):
        raise PlatformHealthError("platform-health state directory is unsafe")


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _atomic_replace(path: Path, payload: Any) -> None:
    _ensure_directory(path.parent)
    temporary = path.with_name(f".{path.name}.tmp-{os.getpid()}-{uuid.uuid4().hex}")
    descriptor = os.open(
        temporary,
        os.O_WRONLY
        | os.O_CREAT
        | os.O_EXCL
        | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_NOFOLLOW", 0),
        0o600,
    )
    try:
        raw = _canonical(payload)
        os.write(descriptor, raw)
        os.fchmod(descriptor, 0o600)
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    os.replace(temporary, path)
    _fsync_directory(path.parent)


def _write_or_verify(path: Path, payload: Any) -> None:
    _ensure_directory(path.parent)
    raw = _canonical(payload)
    try:
        descriptor = os.open(
            path,
            os.O_WRONLY
            | os.O_CREAT
            | os.O_EXCL
            | getattr(os, "O_CLOEXEC", 0)
            | getattr(os, "O_NOFOLLOW", 0),
            0o600,
        )
    except FileExistsError:
        if _read_secure_bytes(path, label="platform-health receipt") != raw:
            raise PlatformHealthError("immutable platform-health receipt conflicts") from None
        return
    try:
        os.write(descriptor, raw)
        os.fchmod(descriptor, 0o600)
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    _fsync_directory(path.parent)


def _open_lock(config: Config, session_id: str) -> tuple[int, Path]:
    root = config.authority_state_root
    _ensure_directory(root)
    _ensure_directory(root / "sessions")
    session_root = root / "sessions" / session_id
    _ensure_directory(session_root)
    _ensure_directory(session_root / "receipts")
    lock_path = session_root / "authority.lock"
    descriptor = os.open(
        lock_path,
        os.O_RDWR | os.O_CREAT | getattr(os, "O_CLOEXEC", 0) | os.O_NOFOLLOW,
        0o600,
    )
    metadata = os.fstat(descriptor)
    if (
        not stat.S_ISREG(metadata.st_mode)
        or (metadata.st_uid, metadata.st_gid) != (ROOT_UID, ROOT_GID)
        or metadata.st_nlink != 1
        or stat.S_IMODE(metadata.st_mode) != 0o600
    ):
        os.close(descriptor)
        raise PlatformHealthError("platform-health authority lock is unsafe")
    fcntl.flock(descriptor, fcntl.LOCK_EX)
    return descriptor, session_root


def _acceptance_state(config: Config, session_id: str) -> dict[str, Any]:
    if SESSION_RE.fullmatch(session_id) is None:
        raise PlatformHealthError("acceptance session identity is invalid")
    state, _raw = _secure_json(
        config.acceptance_state_root / "sessions" / session_id / "state.json",
        label="acceptance session state",
    )
    candidates = state.get("candidates") if isinstance(state, dict) else None
    if (
        not isinstance(state, dict)
        or state.get("schema_version") != 2
        or state.get("session_id") != session_id
        or state.get("submit_host") != config.collector_host
        or state.get("status") not in {"running", "complete"}
        or not isinstance(candidates, dict)
        or tuple(candidates) != SANDBOXES
        or any(
            not isinstance(candidates[sandbox], dict)
            or set(candidates[sandbox]) != {"sha", "tree"}
            or SHA_RE.fullmatch(str(candidates[sandbox]["sha"])) is None
            or SHA_RE.fullmatch(str(candidates[sandbox]["tree"])) is None
            for sandbox in SANDBOXES
        )
        or len({candidates[sandbox]["sha"] for sandbox in SANDBOXES}) != len(SANDBOXES)
        or not isinstance(state.get("completed_phases"), list)
    ):
        raise PlatformHealthError("acceptance session state binding is invalid")
    return state


def _require_acceptance_phase(
    config: Config,
    state: Mapping[str, Any],
    checkpoint: str,
) -> dict[str, str]:
    completed = state["completed_phases"]
    expected = [f"{sandbox}:{checkpoint}" for sandbox in SANDBOXES]
    indexes: list[int] = []
    for item in expected:
        try:
            indexes.append(completed.index(item))
        except ValueError as exc:
            raise PlatformHealthError(
                "acceptance phase is not completely checkpointed",
            ) from exc
    if indexes != sorted(indexes):
        raise PlatformHealthError("acceptance checkpoint order is invalid")
    recorded: dict[str, str] = {}
    phase_order = (
        "preflight",
        "baseline",
        "multi_candidate_overlap",
        "large_batch_burst",
        "fairness_contention",
        "mixed_non_loom",
        "cancel_cleanup",
        "ttl_cleanup",
        "submit_host_restart",
        "worker_crash",
        "final_drain",
    )
    phase_index = phase_order.index(checkpoint)
    for sandbox_index, sandbox in enumerate(SANDBOXES):
        checkpoint_index = phase_index * len(SANDBOXES) + sandbox_index
        path = (
            config.acceptance_state_root
            / "sessions"
            / str(state["session_id"])
            / "checkpoints"
            / f"{checkpoint_index:02d}-{sandbox}-{checkpoint}.json"
        )
        payload, _raw = _secure_json(path, label="acceptance phase checkpoint")
        candidate = state["candidates"][sandbox]
        if (
            not isinstance(payload, dict)
            or payload.get("schema_version") != 2
            or payload.get("session_id") != state["session_id"]
            or payload.get("sandbox") != sandbox
            or payload.get("phase") != checkpoint
            or payload.get("candidate_sha") != candidate["sha"]
            or payload.get("candidate_tree") != candidate["tree"]
            or payload.get("status") != "pass"
            or not isinstance(payload.get("recorded_at"), str)
            or DIGEST_RE.fullmatch(str(payload.get("evidence_sha256"))) is None
        ):
            raise PlatformHealthError("acceptance phase checkpoint binding is invalid")
        _timestamp(payload["recorded_at"], label="acceptance checkpoint")
        recorded[sandbox] = payload["recorded_at"]
    return recorded


def _run_bounded(
    argv: Sequence[str],
    *,
    run: Run = subprocess.run,
    input_bytes: bytes | None = None,
    timeout: float = 30,
) -> bytes:
    try:
        completed = run(
            tuple(argv),
            input=input_bytes,
            check=False,
            capture_output=True,
            timeout=timeout,
            env={"PATH": "/usr/sbin:/usr/bin:/sbin:/bin", "LANG": "C", "LC_ALL": "C"},
        )
    except (OSError, subprocess.SubprocessError) as exc:
        raise PlatformHealthError("fixed platform-health command failed safely") from exc
    stdout = completed.stdout.encode() if isinstance(completed.stdout, str) else completed.stdout
    stderr = completed.stderr.encode() if isinstance(completed.stderr, str) else completed.stderr
    if completed.returncode != 0 or stderr or len(stdout) > MAX_COMMAND_BYTES:
        raise PlatformHealthError("fixed platform-health command failed safely")
    return bytes(stdout)


def _json_command(argv: Sequence[str], *, run: Run = subprocess.run) -> Any:
    raw = _run_bounded(argv, run=run)
    try:
        return json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise PlatformHealthError("fixed command returned invalid JSON") from exc


def _kube_json(config: Config, *args: str, run: Run = subprocess.run) -> Any:
    return _json_command(
        (
            "/usr/local/bin/kubectl",
            "--kubeconfig",
            str(config.kubeconfig),
            *args,
            "-o",
            "json",
        ),
        run=run,
    )


def _condition_true(conditions: Any, condition_type: str) -> bool:
    return isinstance(conditions, list) and any(
        isinstance(item, dict)
        and item.get("type") == condition_type
        and item.get("status") == "True"
        for item in conditions
    )


def _platform_health(config: Config, *, run: Run = subprocess.run) -> dict[str, Any]:
    ready = (
        _run_bounded(
            (
                "/usr/local/bin/kubectl",
                "--kubeconfig",
                str(config.kubeconfig),
                "get",
                "--raw=/readyz",
            ),
            run=run,
        )
        .decode(errors="strict")
        .strip()
    )
    nodes = _kube_json(config, "get", "nodes", run=run)
    minio = _kube_json(
        config,
        "-n",
        config.namespace,
        "get",
        "statefulset",
        config.minio_statefulset,
        run=run,
    )
    pdb = _kube_json(
        config,
        "-n",
        config.namespace,
        "get",
        "pdb",
        config.minio_pdb,
        run=run,
    )
    volumes = _kube_json(
        config,
        "-n",
        config.longhorn_namespace,
        "get",
        "volumes.longhorn.io",
        run=run,
    )
    longhorn_pods = _kube_json(
        config,
        "-n",
        config.longhorn_namespace,
        "get",
        "pods",
        run=run,
    )
    node_items = nodes.get("items") if isinstance(nodes, dict) else None
    volume_items = volumes.get("items") if isinstance(volumes, dict) else None
    pod_items = longhorn_pods.get("items") if isinstance(longhorn_pods, dict) else None
    minio_spec = minio.get("spec") if isinstance(minio, dict) else None
    minio_status = minio.get("status") if isinstance(minio, dict) else None
    pdb_status = pdb.get("status") if isinstance(pdb, dict) else None
    if (
        ready != "ok"
        or not isinstance(node_items, list)
        or not node_items
        or not all(
            isinstance(item, dict)
            and _condition_true(item.get("status", {}).get("conditions"), "Ready")
            for item in node_items
        )
        or not isinstance(minio_spec, dict)
        or not isinstance(minio_status, dict)
        or not isinstance(minio_spec.get("replicas"), int)
        or minio_spec["replicas"] < 1
        or minio_status.get("readyReplicas") != minio_spec["replicas"]
        or minio_status.get("currentReplicas") != minio_spec["replicas"]
        or not isinstance(pdb_status, dict)
        or not isinstance(pdb_status.get("expectedPods"), int)
        or pdb_status["expectedPods"] < 1
        or pdb_status.get("currentHealthy", 0) < pdb_status.get("desiredHealthy", 1)
        or not isinstance(volume_items, list)
        or not volume_items
        or not all(
            isinstance(item, dict) and item.get("status", {}).get("robustness") == "healthy"
            for item in volume_items
        )
        or not isinstance(pod_items, list)
        or not pod_items
        or not all(
            isinstance(item, dict)
            and item.get("status", {}).get("phase") == "Running"
            and _condition_true(item.get("status", {}).get("conditions"), "Ready")
            for item in pod_items
        )
    ):
        raise PlatformHealthError("platform health readback is not fully healthy")
    return {
        "k3s": {
            "readyz": True,
            "node_count": len(node_items),
            "ready_node_count": len(node_items),
        },
        "minio": {
            "replicas": minio_spec["replicas"],
            "ready_replicas": minio_status["readyReplicas"],
            "quorum_healthy": True,
            "pdb_expected_pods": pdb_status["expectedPods"],
            "pdb_current_healthy": pdb_status["currentHealthy"],
            "pdb_desired_healthy": pdb_status["desiredHealthy"],
            "pdb_disruptions_allowed": pdb_status.get("disruptionsAllowed", 0),
        },
        "longhorn": {
            "volume_count": len(volume_items),
            "healthy_volume_count": len(volume_items),
            "pod_count": len(pod_items),
            "ready_pod_count": len(pod_items),
        },
    }


def _node_request(
    config: Config,
    *,
    state: Mapping[str, Any],
    checkpoint: str,
    node: str,
    since_at: str,
) -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "kind": "loom.developer-sandbox.platform-health-node-request",
        "session_id": state["session_id"],
        "checkpoint": checkpoint,
        "checkpoint_group": CHECKPOINT_GROUPS[checkpoint],
        "expected_node": node,
        "expected_host": config.host_aliases[node],
        "since_at": since_at,
        "candidates": state["candidates"],
    }


def _request_envelope(
    request: Mapping[str, Any],
    *,
    node: str,
) -> bytes:
    payload = _canonical(request)
    candidate = request["candidates"]["qianyi"]
    body: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "action": "observe-platform-health-node",
        "node": node,
        "domain": "oldlab" if node.startswith("oldlab-") else "gb10",
        "sandbox": "qianyi",
        "candidate_sha": candidate["sha"],
        "candidate_tree": candidate["tree"],
        "payload_kind": "platform-health-node-json",
        "payload_sha256": hashlib.sha256(payload).hexdigest(),
        "payload_base64": base64.b64encode(payload).decode(),
        "prior_request_id": None,
    }
    body["request_id"] = hashlib.sha256(_canonical(body)).hexdigest()
    return _canonical(body)


def _transport_observation(
    config: Config,
    node: str,
    envelope: bytes,
    *,
    run: Run = subprocess.run,
) -> dict[str, Any]:
    raw = _run_bounded(
        (str(config.node_transport), "invoke", "--node", node, "--verb", "check"),
        run=run,
        input_bytes=envelope,
        timeout=120,
    )
    try:
        response = json.loads(raw)
        request_id = json.loads(envelope)["request_id"]
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise PlatformHealthError("node authority response is invalid") from exc
    if (
        not isinstance(response, dict)
        or set(response) != {"schema_version", "request_id", "status", "result"}
        or response.get("schema_version") != SCHEMA_VERSION
        or response.get("request_id") != request_id
        or response.get("status") != "succeeded"
        or not isinstance(response.get("result"), dict)
    ):
        raise PlatformHealthError("node authority response binding is invalid")
    return dict(response["result"])


def _parse_meminfo(raw: str) -> tuple[int, int]:
    values: dict[str, int] = {}
    for line in raw.splitlines():
        match = re.fullmatch(r"(MemTotal|MemAvailable):\s+([0-9]+)\s+kB", line)
        if match:
            values[match.group(1)] = int(match.group(2)) * 1024
    if set(values) != {"MemTotal", "MemAvailable"} or values["MemAvailable"] <= 0:
        raise PlatformHealthError("node memory readback is invalid")
    return values["MemTotal"], values["MemAvailable"]


def _parse_cpu_stat(raw: str) -> tuple[int, int]:
    first = raw.splitlines()[0].split()
    if len(first) < 9 or first[0] != "cpu" or any(not item.isdigit() for item in first[1:]):
        raise PlatformHealthError("node CPU readback is invalid")
    values = [int(item) for item in first[1:]]
    total = sum(values)
    idle = values[3] + values[4]
    if total <= 0 or idle < 0 or idle > total:
        raise PlatformHealthError("node CPU readback is invalid")
    return total, idle


def _parse_diskstats(raw: str) -> tuple[int, int]:
    read_sectors = 0
    written_sectors = 0
    rows = 0
    for line in raw.splitlines():
        fields = line.split()
        if len(fields) < 14 or not all(item.isdigit() for item in fields[:3]):
            continue
        name = fields[2]
        if name.startswith(("loop", "ram", "zram", "dm-")) or re.search(
            r"(?:p|[a-z])?[0-9]+$", name
        ):
            continue
        if not fields[5].isdigit() or not fields[9].isdigit():
            raise PlatformHealthError("node disk counter readback is invalid")
        read_sectors += int(fields[5])
        written_sectors += int(fields[9])
        rows += 1
    if rows < 1:
        raise PlatformHealthError("node disk counter readback is empty")
    return read_sectors * 512, written_sectors * 512


def _docker_container_ids(
    candidates: Mapping[str, Mapping[str, str]],
    *,
    run: Run,
) -> tuple[str, ...]:
    found: set[str] = set()
    for sandbox in SANDBOXES:
        raw = _run_bounded(
            (
                "/usr/bin/docker",
                "ps",
                "-aq",
                "--filter",
                f"label=loom.sandbox={sandbox}",
                "--filter",
                f"label=loom.candidate_sha={candidates[sandbox]['sha']}",
            ),
            run=run,
        )
        for line in raw.decode(errors="strict").splitlines():
            container_id = line.strip().lower()
            if not CONTAINER_ID_RE.fullmatch(container_id):
                raise PlatformHealthError("Docker returned an invalid container identity")
            found.add(container_id)
    if len(found) > 64:
        raise PlatformHealthError("candidate container set exceeds its closed bound")
    return tuple(sorted(found))


def _container_role(labels: Mapping[str, str]) -> str:
    service = labels.get("com.docker.compose.service")
    if service == "worker":
        return "worker"
    if service == "sandbox-link":
        return "sidecar"
    if labels.get("loom.task_sidecar") == "verifier":
        return "verifier"
    if labels.get("loom.trial_id") and labels.get("loom.task-sidecar") != "true":
        return "trial"
    raise PlatformHealthError("candidate container role is outside the acceptance contract")


def _proc_cgroup(pid: int) -> str:
    try:
        raw = Path(f"/proc/{pid}/cgroup").read_text(encoding="ascii")
    except (OSError, UnicodeDecodeError) as exc:
        raise PlatformHealthError("container cgroup readback is unavailable") from exc
    rows = [line.split(":", 2) for line in raw.splitlines()]
    paths = [parts[2] for parts in rows if len(parts) == 3 and parts[0] == "0"]
    if len(paths) != 1:
        raise PlatformHealthError("container cgroup readback is ambiguous")
    path = paths[0]
    pure = PurePosixPath(path)
    if not pure.is_absolute() or any(part in {".", ".."} for part in pure.parts):
        raise PlatformHealthError("container cgroup readback is invalid")
    return path


def _strict_descendant(child: str, parent: str) -> bool:
    child_path = PurePosixPath(child)
    parent_path = PurePosixPath(parent)
    return (
        child_path.is_absolute()
        and parent_path.is_absolute()
        and child_path != parent_path
        and parent_path != PurePosixPath("/")
        and parent_path in child_path.parents
    )


def _read_cgroup_limit(job_path: str, name: str) -> str:
    if not re.fullmatch(r"[a-z.]+", name):
        raise PlatformHealthError("cgroup control name is invalid")
    path = Path("/sys/fs/cgroup") / job_path.lstrip("/") / name
    try:
        value = path.read_text(encoding="ascii").strip()
    except (OSError, UnicodeDecodeError) as exc:
        raise PlatformHealthError("Slurm job cgroup control is unavailable") from exc
    if not value or len(value) > 4096:
        raise PlatformHealthError("Slurm job cgroup control is invalid")
    return value


def _cpu_limit(job_path: str) -> float:
    fields = _read_cgroup_limit(job_path, "cpu.max").split()
    if (
        len(fields) != 2
        or fields[0] == "max"
        or not all(item.isdigit() for item in fields)
        or int(fields[0]) < 1
        or int(fields[1]) < 1
    ):
        raise PlatformHealthError("Slurm job CPU cgroup limit is not finite")
    return int(fields[0]) / int(fields[1])


def _integer_limit(job_path: str, name: str) -> int:
    value = _read_cgroup_limit(job_path, name)
    if value == "max" or not value.isdigit() or int(value) < 1:
        raise PlatformHealthError("Slurm job cgroup limit is not finite")
    return int(value)


def _parse_scontrol(raw: str) -> dict[str, str]:
    matches = list(re.finditer(r"(?:^|\s)([A-Za-z][A-Za-z0-9_]*)=", raw))
    if not matches:
        raise PlatformHealthError("Slurm job readback is malformed")
    values: dict[str, str] = {}
    for index, match in enumerate(matches):
        key = match.group(1)
        if key in values:
            raise PlatformHealthError("Slurm job readback is ambiguous")
        end = matches[index + 1].start() if index + 1 < len(matches) else len(raw)
        values[key] = raw[match.end() : end].strip()
    return values


def _memory_bytes(value: str) -> int:
    match = re.fullmatch(r"([1-9][0-9]*)([KMGTP]?)", value, re.IGNORECASE)
    if match is None:
        raise PlatformHealthError("Slurm memory allocation is invalid")
    powers: dict[str, int] = {"": 0, "K": 1, "M": 2, "G": 3, "T": 4, "P": 5}
    power = powers[match.group(2).upper()]
    count: int = int(str(match.group(1)))
    multiplier: int = 1024**power
    return count * multiplier


def _gpu_count(tres: str) -> int:
    return sum(
        int(value) for value in re.findall(r"(?:^|,)gres/gpu(?:[^=,]*)=([0-9]+)(?:,|$)", tres)
    )


def _job_readback(
    *,
    sandbox: str,
    candidate_sha: str,
    job_id: str,
    expected_node: str,
    run: Run,
) -> dict[str, Any]:
    raw = (
        _run_bounded(
            (
                "/usr/bin/scontrol",
                "show",
                "job",
                "--oneliner",
                "--details",
                job_id,
            ),
            run=run,
        )
        .decode(errors="strict")
        .strip()
    )
    values = _parse_scontrol(raw)
    user = values.get("UserId", "").split("(", 1)[0]
    job_name = values.get("JobName", "")
    comment = values.get("Comment", "")
    pids_match = re.fullmatch(r"loom-cgroup-v1:pids=([1-9][0-9]*)", comment)
    shared = values.get("Shared", values.get("OverSubscribe", ""))
    if (
        values.get("JobId") != job_id
        or not job_name.startswith(f"loom-{sandbox}-{candidate_sha[:12]}-")
        or user != f"loom-sandbox-{sandbox}"
        or values.get("Account") != f"loom-dev-{sandbox}"
        or values.get("JobState") != "RUNNING"
        or values.get("NodeList", "").lower() != expected_node.lower()
        or values.get("NumNodes") != "1"
        or not values.get("NumCPUs", "").isdigit()
        or pids_match is None
        or shared not in {"OK", "YES", "USER", "1"}
    ):
        raise PlatformHealthError("Slurm job readback does not match candidate identity")
    tres = values.get("AllocTRES", "")
    return {
        "job_id": job_id,
        "job_name": job_name,
        "sandbox": sandbox,
        "candidate_sha": candidate_sha,
        "account": values["Account"],
        "user": user,
        "node": expected_node,
        "state": "RUNNING",
        "allocation": {
            "cpu_cores": int(values["NumCPUs"]),
            "memory_bytes": _memory_bytes(values.get("MinMemoryNode", "")),
            "pids": int(pids_match.group(1)),
            "gpu_count": _gpu_count(tres),
            "tres": tres,
            "exclusive": False,
        },
    }


def _container_observations(
    candidates: Mapping[str, Mapping[str, str]],
    *,
    expected_node: str,
    checkpoint: str,
    run: Run,
) -> tuple[list[dict[str, Any]], list[str]]:
    ids = _docker_container_ids(candidates, run=run)
    containers: list[dict[str, Any]] = []
    orphans: list[str] = []
    for container_id in ids:
        inspected = _json_command(("/usr/bin/docker", "inspect", container_id), run=run)
        if (
            not isinstance(inspected, list)
            or len(inspected) != 1
            or not isinstance(
                inspected[0],
                dict,
            )
        ):
            raise PlatformHealthError("Docker inspect result is invalid")
        item = inspected[0]
        raw_config = item.get("Config")
        raw_host_config = item.get("HostConfig")
        raw_state = item.get("State")
        if (
            not isinstance(raw_config, dict)
            or not isinstance(raw_host_config, dict)
            or not isinstance(raw_state, dict)
        ):
            raise PlatformHealthError("Docker inspect result is incomplete")
        container_config: dict[str, Any] = raw_config
        host_config: dict[str, Any] = raw_host_config
        state: dict[str, Any] = raw_state
        labels = container_config.get("Labels")
        if not isinstance(labels, dict) or not all(
            isinstance(key, str) and isinstance(value, str) for key, value in labels.items()
        ):
            raise PlatformHealthError("Docker labels are invalid")
        sandbox = labels.get("loom.sandbox", "")
        candidate_sha = labels.get("loom.candidate_sha", "")
        job_id = labels.get("loom.slurm_job_id", "")
        compose_project = labels.get("loom.compose_project", "")
        if (
            sandbox not in SANDBOXES
            or candidate_sha != candidates[sandbox]["sha"]
            or JOB_ID_RE.fullmatch(job_id) is None
            or SAFE_NAME_RE.fullmatch(compose_project) is None
        ):
            raise PlatformHealthError("Docker candidate labels are invalid")
        if state.get("Status") != "running":
            orphans.append(container_id)
            continue
        pid = state.get("Pid")
        if not isinstance(pid, int) or isinstance(pid, bool) or pid < 1:
            raise PlatformHealthError("Docker container PID is invalid")
        cgroup_parent = host_config.get("CgroupParent")
        if not isinstance(cgroup_parent, str) or not cgroup_parent.startswith("/"):
            raise PlatformHealthError("Docker cgroup parent is invalid")
        observed_path = _proc_cgroup(pid)
        if not _strict_descendant(observed_path, cgroup_parent):
            raise PlatformHealthError("Docker container escaped its Slurm job cgroup")
        nano_cpus = host_config.get("NanoCpus")
        memory = host_config.get("Memory")
        pids = host_config.get("PidsLimit")
        if (
            not isinstance(nano_cpus, int)
            or isinstance(nano_cpus, bool)
            or nano_cpus < 1
            or not isinstance(memory, int)
            or isinstance(memory, bool)
            or memory < 1
            or not isinstance(pids, int)
            or isinstance(pids, bool)
            or pids < 1
        ):
            raise PlatformHealthError("Docker container limits are not finite")
        gpu_ids: list[str] = []
        gpu_count = 0
        requests = host_config.get("DeviceRequests") or []
        if not isinstance(requests, list):
            raise PlatformHealthError("Docker GPU request shape is invalid")
        for request in requests:
            if not isinstance(request, dict):
                raise PlatformHealthError("Docker GPU request shape is invalid")
            capabilities = request.get("Capabilities") or []
            if any("gpu" in row for row in capabilities if isinstance(row, list)):
                device_ids = request.get("DeviceIDs") or []
                if not isinstance(device_ids, list) or not all(
                    isinstance(value, str) and SAFE_NAME_RE.fullmatch(value) for value in device_ids
                ):
                    raise PlatformHealthError("Docker GPU device identity is invalid")
                count = request.get("Count", 0)
                if not isinstance(count, int) or isinstance(count, bool):
                    raise PlatformHealthError("Docker GPU limit is invalid")
                gpu_ids.extend(device_ids)
                gpu_count += len(device_ids) if device_ids else max(count, 0)
        containers.append(
            {
                "container_id": container_id,
                "role": _container_role(labels),
                "sandbox": sandbox,
                "candidate_sha": candidate_sha,
                "job_id": job_id,
                "compose_project": compose_project,
                "pid": pid,
                "cgroup_parent": cgroup_parent,
                "observed_cgroup_path": observed_path,
                "limits": {
                    "cpu_cores": nano_cpus / 1_000_000_000,
                    "memory_bytes": memory,
                    "pids": pids,
                    "gpu_count": gpu_count,
                    "gpu_ids": sorted(gpu_ids),
                },
            },
        )
    if checkpoint in {"baseline", "final_drain"} and (containers or orphans):
        raise PlatformHealthError("drained checkpoint contains candidate containers")
    by_job: dict[str, list[dict[str, Any]]] = {}
    for container in containers:
        by_job.setdefault(container["job_id"], []).append(container)
    jobs: list[dict[str, Any]] = []
    for job_id, job_containers in sorted(by_job.items()):
        identities = {
            (
                item["sandbox"],
                item["candidate_sha"],
                item["compose_project"],
                item["cgroup_parent"],
            )
            for item in job_containers
        }
        roles = [item["role"] for item in job_containers]
        if len(identities) != 1 or tuple(sorted(roles)) != tuple(sorted(ROLES)):
            raise PlatformHealthError("candidate job container set is not closed")
        sandbox, candidate_sha, compose_project, job_path = next(iter(identities))
        slurm = _job_readback(
            sandbox=sandbox,
            candidate_sha=candidate_sha,
            job_id=job_id,
            expected_node=expected_node,
            run=run,
        )
        controllers = sorted(_read_cgroup_limit(job_path, "cgroup.controllers").split())
        if not {"cpu", "memory", "pids"}.issubset(controllers):
            raise PlatformHealthError("Slurm job cgroup controllers are incomplete")
        cgroup = {
            "job_path": job_path,
            "controllers": controllers,
            "cpu_cores_max": _cpu_limit(job_path),
            "memory_bytes_max": _integer_limit(job_path, "memory.max"),
            "pids_max": _integer_limit(job_path, "pids.max"),
        }
        totals = {
            "cpu_cores": sum(item["limits"]["cpu_cores"] for item in job_containers),
            "memory_bytes": sum(item["limits"]["memory_bytes"] for item in job_containers),
            "pids": sum(item["limits"]["pids"] for item in job_containers),
            "gpu_count": sum(item["limits"]["gpu_count"] for item in job_containers),
        }
        allocation = slurm["allocation"]
        if (
            totals["cpu_cores"] > cgroup["cpu_cores_max"]
            or totals["cpu_cores"] > allocation["cpu_cores"]
            or totals["memory_bytes"] > cgroup["memory_bytes_max"]
            or totals["memory_bytes"] > allocation["memory_bytes"]
            or totals["pids"] > cgroup["pids_max"]
            or totals["pids"] > allocation["pids"]
            or totals["gpu_count"] > allocation["gpu_count"]
            or cgroup["cpu_cores_max"] > allocation["cpu_cores"]
            or cgroup["memory_bytes_max"] > allocation["memory_bytes"]
            or cgroup["pids_max"] > allocation["pids"]
        ):
            raise PlatformHealthError("candidate job container aggregate exceeds allocation")
        jobs.append(
            {
                **slurm,
                "compose_project": compose_project,
                "cgroup": cgroup,
                "containers": sorted(job_containers, key=lambda item: item["role"]),
                "aggregate_limits": totals,
            },
        )
    return jobs, sorted(orphans)


def _terminal_jobs(
    candidates: Mapping[str, Mapping[str, str]],
    *,
    expected_node: str,
    since_at: str,
    run: Run,
) -> list[dict[str, str]]:
    since = _timestamp(since_at, label="node observation lower bound")
    raw = _run_bounded(
        (
            "/usr/bin/sacct",
            "--starttime",
            since.strftime("%Y-%m-%dT%H:%M:%S"),
            "--allocations",
            "--noheader",
            "--parsable2",
            "--state=CANCELLED,FAILED,NODE_FAIL,OUT_OF_MEMORY,TIMEOUT,COMPLETED",
            "--format=JobIDRaw,JobName,State,NodeList,Account,User",
        ),
        run=run,
    ).decode(errors="strict")
    rows: list[dict[str, str]] = []
    seen: set[str] = set()
    for line in raw.splitlines():
        fields = line.split("|")
        if len(fields) != 7 or fields[-1] != "":
            raise PlatformHealthError("Slurm accounting readback is malformed")
        job_id, name, state, node, account, user = fields[:-1]
        matches = [
            sandbox
            for sandbox in SANDBOXES
            if name.startswith(f"loom-{sandbox}-{candidates[sandbox]['sha'][:12]}-")
        ]
        if not matches:
            continue
        sandbox = matches[0]
        if (
            JOB_ID_RE.fullmatch(job_id) is None
            or node.lower() != expected_node.lower()
            or account != f"loom-dev-{sandbox}"
            or user != f"loom-sandbox-{sandbox}"
            or job_id in seen
            or state.split("+", 1)[0]
            not in {"CANCELLED", "FAILED", "NODE_FAIL", "OUT_OF_MEMORY", "TIMEOUT", "COMPLETED"}
        ):
            raise PlatformHealthError("Slurm accounting identity is invalid")
        seen.add(job_id)
        rows.append(
            {
                "job_id": job_id,
                "job_name": name,
                "state": state.split("+", 1)[0],
                "node": expected_node,
                "sandbox": sandbox,
                "candidate_sha": candidates[sandbox]["sha"],
            },
        )
    return sorted(rows, key=lambda item: (item["job_id"], item["state"]))


def observe_node(
    raw: bytes,
    *,
    run: Run = subprocess.run,
    clock: Clock = _now,
    hostname: Callable[[], str] = _host,
) -> dict[str, Any]:
    """Return one exact sanitized node observation."""

    _require_root()
    try:
        request = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise PlatformHealthError("platform-health node request is invalid") from exc
    candidates = request.get("candidates") if isinstance(request, dict) else None
    if (
        not isinstance(request, dict)
        or set(request) != NODE_REQUEST_FIELDS
        or raw != _canonical(request)
        or request.get("schema_version") != SCHEMA_VERSION
        or request.get("kind") != "loom.developer-sandbox.platform-health-node-request"
        or SESSION_RE.fullmatch(str(request.get("session_id"))) is None
        or request.get("checkpoint") not in CHECKPOINTS
        or request.get("checkpoint_group") != CHECKPOINT_GROUPS.get(str(request.get("checkpoint")))
        or not isinstance(request.get("expected_node"), str)
        or request.get("expected_node") in EXCLUDED_NODES
        or not isinstance(request.get("expected_host"), str)
        or hostname() != request.get("expected_host")
        or not isinstance(candidates, dict)
        or tuple(candidates) != SANDBOXES
        or any(
            not isinstance(candidates[sandbox], dict)
            or set(candidates[sandbox]) != {"sha", "tree"}
            or SHA_RE.fullmatch(str(candidates[sandbox]["sha"])) is None
            or SHA_RE.fullmatch(str(candidates[sandbox]["tree"])) is None
            for sandbox in SANDBOXES
        )
    ):
        raise PlatformHealthError("platform-health node request binding is invalid")
    _timestamp(request["since_at"], label="node observation lower bound")
    mem_total, mem_available = _parse_meminfo(
        Path("/proc/meminfo").read_text(encoding="ascii"),
    )
    cpu_total, cpu_idle = _parse_cpu_stat(Path("/proc/stat").read_text(encoding="ascii"))
    read_bytes, write_bytes = _parse_diskstats(
        Path("/proc/diskstats").read_text(encoding="ascii"),
    )
    cpu_count = os.cpu_count()
    if not isinstance(cpu_count, int) or cpu_count < 1:
        raise PlatformHealthError("node CPU count is invalid")
    jobs, orphans = _container_observations(
        candidates,
        expected_node=request["expected_node"],
        checkpoint=request["checkpoint"],
        run=run,
    )
    terminal = _terminal_jobs(
        candidates,
        expected_node=request["expected_node"],
        since_at=request["since_at"],
        run=run,
    )
    pool = "oldlab" if str(request["expected_node"]).startswith("oldlab-") else "gb10"
    return {
        "schema_version": SCHEMA_VERSION,
        "kind": "loom.developer-sandbox.platform-health-node-observation",
        "session_id": request["session_id"],
        "checkpoint": request["checkpoint"],
        "checkpoint_group": request["checkpoint_group"],
        "node": request["expected_node"],
        "host": request["expected_host"],
        "pool": pool,
        "observed_at": _iso(clock()),
        "capacity": {
            "cpu_cores_total": cpu_count,
            "cpu_ticks_total": cpu_total,
            "cpu_ticks_idle": cpu_idle,
            "memory_bytes_total": mem_total,
            "memory_bytes_available": mem_available,
        },
        "io": {
            "read_bytes_total": read_bytes,
            "write_bytes_total": write_bytes,
        },
        "active_jobs": jobs,
        "terminal_jobs": terminal,
        "orphan_container_ids": orphans,
    }


def _validate_node_result(
    result: Mapping[str, Any],
    *,
    request: Mapping[str, Any],
    config: Config,
) -> None:
    if (
        set(result) != NODE_RESULT_FIELDS
        or result.get("schema_version") != SCHEMA_VERSION
        or result.get("kind") != "loom.developer-sandbox.platform-health-node-observation"
        or result.get("session_id") != request["session_id"]
        or result.get("checkpoint") != request["checkpoint"]
        or result.get("checkpoint_group") != request["checkpoint_group"]
        or result.get("node") != request["expected_node"]
        or result.get("host") != request["expected_host"]
        or result.get("pool")
        != ("oldlab" if str(request["expected_node"]).startswith("oldlab-") else "gb10")
        or not isinstance(result.get("capacity"), dict)
        or not isinstance(result.get("io"), dict)
        or not isinstance(result.get("active_jobs"), list)
        or not isinstance(result.get("terminal_jobs"), list)
        or not isinstance(result.get("orphan_container_ids"), list)
        or result["orphan_container_ids"]
    ):
        raise PlatformHealthError("node observation binding is invalid")
    observed = _timestamp(result["observed_at"], label="node observation")
    now = _now()
    if observed > now + timedelta(seconds=config.max_clock_skew_seconds):
        raise PlatformHealthError("node observation is from the future")


def _verify_checkpoints(
    config: Config,
    receipts: Sequence[Mapping[str, Any]],
    *,
    require_complete: bool,
) -> dict[str, Any] | None:
    expected = CHECKPOINTS[: len(receipts)]
    if (
        len(receipts) > len(CHECKPOINTS)
        or tuple(item.get("checkpoint") for item in receipts) != expected
    ):
        raise PlatformHealthError("platform-health checkpoint sequence is invalid")
    if require_complete and len(receipts) != len(CHECKPOINTS):
        raise PlatformHealthError("platform-health checkpoint sequence is incomplete")
    prior_time: datetime | None = None
    prior_candidates: Mapping[str, Mapping[str, str]] | None = None
    by_checkpoint: dict[str, Mapping[str, Any]] = {}
    for sequence, receipt in enumerate(receipts, start=1):
        candidates = receipt.get("candidates")
        if (
            receipt.get("schema_version") != SCHEMA_VERSION
            or receipt.get("kind") != "loom.developer-sandbox.platform-health-checkpoint"
            or receipt.get("sequence") != sequence
            or receipt.get("checkpoint_group")
            != CHECKPOINT_GROUPS.get(str(receipt.get("checkpoint")))
            or tuple(receipt.get("nodes", {})) != config.nodes
            or receipt.get("excluded_nodes") != sorted(EXCLUDED_NODES)
            or not isinstance(receipt.get("platform_health"), dict)
            or not isinstance(candidates, dict)
            or tuple(candidates) != SANDBOXES
            or any(
                not isinstance(candidates[sandbox], dict)
                or set(candidates[sandbox]) != {"sha", "tree"}
                or SHA_RE.fullmatch(str(candidates[sandbox]["sha"])) is None
                or SHA_RE.fullmatch(str(candidates[sandbox]["tree"])) is None
                for sandbox in SANDBOXES
            )
            or len({candidates[sandbox]["sha"] for sandbox in SANDBOXES}) != len(SANDBOXES)
            or (prior_candidates is not None and candidates != prior_candidates)
        ):
            raise PlatformHealthError("platform-health checkpoint receipt is invalid")
        prior_candidates = candidates
        current = _timestamp(receipt.get("observed_at"), label="platform-health checkpoint")
        if prior_time is not None and current <= prior_time:
            raise PlatformHealthError("platform-health checkpoint time did not advance")
        prior_time = current
        by_checkpoint[str(receipt["checkpoint"])] = receipt
    if not require_complete:
        return None
    baseline = by_checkpoint["baseline"]
    mixed = by_checkpoint["mixed_non_loom"]
    final = by_checkpoint["final_drain"]
    active_mixed: list[Mapping[str, Any]] = []
    for node in config.nodes:
        base_node = baseline["nodes"][node]
        mixed_node = mixed["nodes"][node]
        final_node = final["nodes"][node]
        for key in ("cpu_cores_total", "memory_bytes_total"):
            if (
                base_node["capacity"][key] != mixed_node["capacity"][key]
                or base_node["capacity"][key] != final_node["capacity"][key]
            ):
                raise PlatformHealthError("node physical capacity drifted across checkpoints")
        total_delta = (
            mixed_node["capacity"]["cpu_ticks_total"] - base_node["capacity"]["cpu_ticks_total"]
        )
        idle_delta = (
            mixed_node["capacity"]["cpu_ticks_idle"] - base_node["capacity"]["cpu_ticks_idle"]
        )
        if total_delta <= 0 or idle_delta < 0 or idle_delta > total_delta:
            raise PlatformHealthError("node CPU interval is invalid")
        busy_ratio = 1 - idle_delta / total_delta
        if node in config.oldlab_nodes and (
            busy_ratio > config.maximum_cpu_busy_ratio
            or mixed_node["capacity"]["memory_bytes_available"]
            < config.minimum_oldlab_free_memory_bytes
        ):
            raise PlatformHealthError("OLDLAB mixed-workload headroom is below policy")
        if (
            final_node["io"]["read_bytes_total"] < base_node["io"]["read_bytes_total"]
            or final_node["io"]["write_bytes_total"] < base_node["io"]["write_bytes_total"]
        ):
            raise PlatformHealthError("node I/O counters regressed")
        if base_node["active_jobs"] or final_node["active_jobs"]:
            raise PlatformHealthError("baseline or final drain retains acceptance jobs")
        active_mixed.extend(mixed_node["active_jobs"])
    combinations = {
        (job["sandbox"], "oldlab" if job["node"] in config.oldlab_nodes else "gb10")
        for job in active_mixed
    }
    expected_combinations = {(sandbox, pool) for sandbox in SANDBOXES for pool in POOLS}
    if combinations != expected_combinations or len(active_mixed) != len(expected_combinations):
        raise PlatformHealthError("mixed workload does not contain one exact job per sandbox/pool")
    cancel_rows = [
        item
        for node in config.nodes
        for item in by_checkpoint["cancel_cleanup"]["nodes"][node]["terminal_jobs"]
        if item["state"] == "CANCELLED"
    ]
    crash_rows = [
        item
        for node in config.nodes
        for item in by_checkpoint["worker_crash"]["nodes"][node]["terminal_jobs"]
        if item["state"] in {"FAILED", "NODE_FAIL", "OUT_OF_MEMORY", "TIMEOUT"}
    ]
    if not cancel_rows or not crash_rows:
        raise PlatformHealthError("cancel or crash cleanup lacks root accounting evidence")
    if any(
        baseline["nodes"][node]["capacity"]["cpu_cores_total"] < 20
        or baseline["nodes"][node]["capacity"]["memory_bytes_total"] < 115000 * 1024**2
        for node in config.gb10_nodes
    ):
        raise PlatformHealthError("GB10 reviewed shared slice lacks positive node headroom")
    completed_at = _timestamp(final["observed_at"], label="platform-health final checkpoint")
    final_payload = {
        "schema_version": SCHEMA_VERSION,
        "kind": "loom.developer-sandbox.platform-health-evidence",
        "session_id": baseline["session_id"],
        "candidates": baseline["candidates"],
        "collector_host": config.collector_host,
        "checkpoints": [
            {
                "sequence": receipt["sequence"],
                "checkpoint": receipt["checkpoint"],
                "checkpoint_group": receipt["checkpoint_group"],
                "observed_at": receipt["observed_at"],
                "payload_sha256": receipt["payload_sha256"],
            }
            for receipt in receipts
        ],
        "mixed_jobs": active_mixed,
        "cancelled_jobs": cancel_rows,
        "crashed_jobs": crash_rows,
        "node_intervals": {
            node: {
                "cpu_busy_ratio": 1
                - (
                    mixed["nodes"][node]["capacity"]["cpu_ticks_idle"]
                    - baseline["nodes"][node]["capacity"]["cpu_ticks_idle"]
                )
                / (
                    mixed["nodes"][node]["capacity"]["cpu_ticks_total"]
                    - baseline["nodes"][node]["capacity"]["cpu_ticks_total"]
                ),
                "minimum_memory_bytes_available": min(
                    receipt["nodes"][node]["capacity"]["memory_bytes_available"]
                    for receipt in receipts
                ),
                "read_bytes": (
                    final["nodes"][node]["io"]["read_bytes_total"]
                    - baseline["nodes"][node]["io"]["read_bytes_total"]
                ),
                "write_bytes": (
                    final["nodes"][node]["io"]["write_bytes_total"]
                    - baseline["nodes"][node]["io"]["write_bytes_total"]
                ),
            }
            for node in config.nodes
        },
        "policy_capacity": {
            "oldlab": {
                "max_slots": 20,
                "requested_cpus": 8,
                "requested_memory_mib": 32000,
                "requested_concurrency": 4,
                "container_cpus": 2,
                "container_memory_mib": 8000,
                "minimum_node_cpu_cores": min(
                    baseline["nodes"][node]["capacity"]["cpu_cores_total"]
                    for node in config.oldlab_nodes
                ),
                "minimum_node_memory_bytes": min(
                    baseline["nodes"][node]["capacity"]["memory_bytes_total"]
                    for node in config.oldlab_nodes
                ),
            },
            "gb10": {
                "max_slots": 112,
                "requested_cpus": 16,
                "requested_memory_mib": 92000,
                "requested_concurrency": 8,
                "container_cpus": 2,
                "container_memory_mib": 11500,
                "reserved_cpu_cores_per_node": 4,
                "reserved_memory_mib_per_node": 23000,
                "minimum_node_cpu_cores": min(
                    baseline["nodes"][node]["capacity"]["cpu_cores_total"]
                    for node in config.gb10_nodes
                ),
                "minimum_node_memory_bytes": min(
                    baseline["nodes"][node]["capacity"]["memory_bytes_total"]
                    for node in config.gb10_nodes
                ),
            },
        },
        "zero_orphans": True,
        "completed_at": _iso(completed_at),
        "expires_at": _iso(completed_at + PLATFORM_HEALTH_EVIDENCE_TTL),
    }
    final_payload["payload_sha256"] = _digest(final_payload)
    return final_payload


def _load_receipts(session_root: Path) -> list[dict[str, Any]]:
    receipts: list[dict[str, Any]] = []
    for sequence, checkpoint in enumerate(CHECKPOINTS, start=1):
        path = session_root / "receipts" / f"{sequence:02d}-{checkpoint}.json"
        if not path.exists() and not path.is_symlink():
            break
        payload, raw = _secure_json(path, label="platform-health checkpoint receipt")
        if (
            not isinstance(payload, dict)
            or payload.get("payload_sha256")
            != hashlib.sha256(
                _canonical({k: v for k, v in payload.items() if k != "payload_sha256"})
            ).hexdigest()
            or raw != _canonical(payload)
        ):
            raise PlatformHealthError("platform-health checkpoint receipt digest is invalid")
        receipts.append(payload)
    if any(
        (session_root / "receipts" / f"{index:02d}-{checkpoint}.json").exists()
        for index, checkpoint in enumerate(CHECKPOINTS[len(receipts) :], start=len(receipts) + 1)
    ):
        raise PlatformHealthError("platform-health checkpoint receipt set has a gap")
    return receipts


def _recover_transaction(config: Config, session_root: Path, session_id: str) -> None:
    journal_path = session_root / "journal.json"
    if not journal_path.exists() and not journal_path.is_symlink():
        return
    payload, _raw = _secure_json(journal_path, label="platform-health transaction")
    fields = {
        "schema_version",
        "kind",
        "session_id",
        "sequence",
        "checkpoint",
        "receipt_path",
        "receipt_sha256",
        "receipt",
        "phase",
    }
    receipt = payload.get("receipt") if isinstance(payload, dict) else None
    sequence = payload.get("sequence") if isinstance(payload, dict) else None
    checkpoint = payload.get("checkpoint") if isinstance(payload, dict) else None
    if (
        not isinstance(payload, dict)
        or set(payload) != fields
        or payload.get("schema_version") != SCHEMA_VERSION
        or payload.get("kind") != "loom.developer-sandbox.platform-health-transaction"
        or payload.get("session_id") != session_id
        or not isinstance(sequence, int)
        or isinstance(sequence, bool)
        or sequence < 1
        or sequence > len(CHECKPOINTS)
        or checkpoint != CHECKPOINTS[sequence - 1]
        or payload.get("receipt_path")
        != str(session_root / "receipts" / f"{sequence:02d}-{checkpoint}.json")
        or not isinstance(receipt, dict)
        or receipt.get("payload_sha256") != payload.get("receipt_sha256")
        or _digest({key: value for key, value in receipt.items() if key != "payload_sha256"})
        != payload.get("receipt_sha256")
        or payload.get("phase") not in {"prepared", "receipt-written", "committed"}
    ):
        raise PlatformHealthError("platform-health transaction recovery binding is invalid")
    if payload["phase"] == "prepared":
        _write_or_verify(Path(payload["receipt_path"]), receipt)
        payload["phase"] = "receipt-written"
        _atomic_replace(journal_path, payload)
    if payload["phase"] == "receipt-written":
        receipts = _load_receipts(session_root)
        if len(receipts) != sequence:
            raise PlatformHealthError("platform-health transaction receipt set drifted")
        final = _verify_checkpoints(
            config,
            receipts,
            require_complete=sequence == len(CHECKPOINTS),
        )
        if final is not None:
            _write_or_verify(session_root / "evidence.json", final)
            _atomic_replace(
                config.authority_state_root / "current.json",
                {
                    "schema_version": SCHEMA_VERSION,
                    "session_id": session_id,
                    "evidence_path": str(session_root / "evidence.json"),
                    "payload_sha256": final["payload_sha256"],
                },
            )
        payload["phase"] = "committed"
        _atomic_replace(journal_path, payload)


def collect(
    config: Config,
    session_id: str,
    checkpoint: str,
    *,
    execute: bool,
    transport: Transport | None = None,
    platform_run: Run = subprocess.run,
    clock: Clock = _now,
    hostname: Callable[[], str] = _host,
) -> dict[str, Any]:
    """Collect one monotonic checkpoint and publish final evidence when complete."""

    _require_root()
    if (
        not execute
        or hostname() != config.collector_host
        or checkpoint not in CHECKPOINTS
        or SESSION_RE.fullmatch(session_id) is None
    ):
        raise PlatformHealthError("platform-health collection authority is invalid")
    state = _acceptance_state(config, session_id)
    acceptance_times = _require_acceptance_phase(config, state, checkpoint)
    lock, session_root = _open_lock(config, session_id)
    try:
        _recover_transaction(config, session_root, session_id)
        receipts = _load_receipts(session_root)
        expected_sequence = len(receipts) + 1
        expected_checkpoint = (
            CHECKPOINTS[len(receipts)] if len(receipts) < len(CHECKPOINTS) else None
        )
        if checkpoint != expected_checkpoint:
            if checkpoint in CHECKPOINTS[: len(receipts)]:
                return receipts[CHECKPOINTS.index(checkpoint)]
            raise PlatformHealthError("platform-health checkpoint is not the exact next phase")
        started = clock().astimezone(UTC)
        baseline_time = (
            receipts[0]["observed_at"]
            if receipts
            else min(acceptance_times.values(), key=lambda value: _timestamp(value, label="phase"))
        )
        nodes: dict[str, Any] = {}
        for node in config.nodes:
            request = _node_request(
                config,
                state=state,
                checkpoint=checkpoint,
                node=node,
                since_at=baseline_time,
            )
            if transport is None:
                result = _transport_observation(
                    config,
                    node,
                    _request_envelope(request, node=node),
                )
            else:
                result = transport(node, _canonical(request))
            _validate_node_result(result, request=request, config=config)
            nodes[node] = result
        health = _platform_health(config, run=platform_run)
        finished = clock().astimezone(UTC)
        if finished < started or finished - started > timedelta(
            seconds=config.max_checkpoint_seconds
        ):
            raise PlatformHealthError("platform-health checkpoint exceeded its bounded window")
        receipt: dict[str, Any] = {
            "schema_version": SCHEMA_VERSION,
            "kind": "loom.developer-sandbox.platform-health-checkpoint",
            "session_id": session_id,
            "sequence": expected_sequence,
            "checkpoint": checkpoint,
            "checkpoint_group": CHECKPOINT_GROUPS[checkpoint],
            "candidates": state["candidates"],
            "collector_host": config.collector_host,
            "acceptance_checkpoint_times": acceptance_times,
            "collection_started_at": _iso(started),
            "observed_at": _iso(finished),
            "excluded_nodes": sorted(EXCLUDED_NODES),
            "nodes": nodes,
            "platform_health": health,
        }
        receipt["payload_sha256"] = _digest(receipt)
        journal_path = session_root / "journal.json"
        journal = {
            "schema_version": SCHEMA_VERSION,
            "kind": "loom.developer-sandbox.platform-health-transaction",
            "session_id": session_id,
            "sequence": expected_sequence,
            "checkpoint": checkpoint,
            "receipt_path": str(
                session_root / "receipts" / f"{expected_sequence:02d}-{checkpoint}.json",
            ),
            "receipt_sha256": receipt["payload_sha256"],
            "receipt": receipt,
            "phase": "prepared",
        }
        _atomic_replace(journal_path, journal)
        destination = Path(journal["receipt_path"])
        _write_or_verify(destination, receipt)
        journal["phase"] = "receipt-written"
        _atomic_replace(journal_path, journal)
        receipts.append(receipt)
        final = _verify_checkpoints(
            config,
            receipts,
            require_complete=len(receipts) == len(CHECKPOINTS),
        )
        if final is not None:
            _write_or_verify(session_root / "evidence.json", final)
            _atomic_replace(
                config.authority_state_root / "current.json",
                {
                    "schema_version": SCHEMA_VERSION,
                    "session_id": session_id,
                    "evidence_path": str(session_root / "evidence.json"),
                    "payload_sha256": final["payload_sha256"],
                },
            )
        journal["phase"] = "committed"
        _atomic_replace(journal_path, journal)
        return {
            "schema_version": SCHEMA_VERSION,
            "kind": "loom.developer-sandbox.platform-health-result",
            "session_id": session_id,
            "checkpoint": checkpoint,
            "sequence": expected_sequence,
            "receipt_path": str(destination),
            "receipt_sha256": receipt["payload_sha256"],
            "evidence_path": str(session_root / "evidence.json") if final else None,
            "evidence_sha256": final["payload_sha256"] if final else None,
        }
    finally:
        os.close(lock)


def verify(config: Config, session_id: str) -> dict[str, Any]:
    """Verify the immutable complete platform-health evidence."""

    _require_root()
    lock, session_root = _open_lock(config, session_id)
    try:
        _recover_transaction(config, session_root, session_id)
        state = _acceptance_state(config, session_id)
        receipts = _load_receipts(session_root)
        final = _verify_checkpoints(config, receipts, require_complete=True)
        if final is None:  # pragma: no cover - require_complete owns this invariant
            raise PlatformHealthError("platform-health evidence is incomplete")
        existing, raw = _secure_json(
            session_root / "evidence.json",
            label="platform-health final evidence",
        )
        if (
            existing != final
            or hashlib.sha256(
                _canonical(
                    {key: value for key, value in existing.items() if key != "payload_sha256"}
                ),
            ).hexdigest()
            != existing.get("payload_sha256")
            or existing.get("candidates") != state["candidates"]
        ):
            raise PlatformHealthError("platform-health final evidence drifted")
        return {
            "schema_version": SCHEMA_VERSION,
            "kind": "loom.developer-sandbox.platform-health-verification",
            "session_id": session_id,
            "path": str(session_root / "evidence.json"),
            "payload_sha256": existing["payload_sha256"],
            "file_sha256": hashlib.sha256(raw).hexdigest(),
            "status": "pass",
        }
    finally:
        os.close(lock)


def verify_current(config: Config) -> dict[str, Any]:
    """Verify only the exact root-owned current evidence pointer."""

    _require_root()
    current, _raw = _secure_json(
        config.authority_state_root / "current.json",
        label="platform-health current pointer",
    )
    if (
        not isinstance(current, dict)
        or set(current) != {"schema_version", "session_id", "evidence_path", "payload_sha256"}
        or current.get("schema_version") != SCHEMA_VERSION
        or SESSION_RE.fullmatch(str(current.get("session_id"))) is None
        or current.get("evidence_path")
        != str(
            config.authority_state_root
            / "sessions"
            / str(current.get("session_id"))
            / "evidence.json"
        )
        or DIGEST_RE.fullmatch(str(current.get("payload_sha256"))) is None
    ):
        raise PlatformHealthError("platform-health current pointer is invalid")
    result = verify(config, str(current["session_id"]))
    if result["payload_sha256"] != current["payload_sha256"]:
        raise PlatformHealthError("platform-health current pointer drifted")
    return result


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__, allow_abbrev=False)
    subparsers = parser.add_subparsers(dest="command", required=True)
    collect_parser = subparsers.add_parser("collect", allow_abbrev=False)
    collect_parser.add_argument("--session-id", required=True)
    collect_parser.add_argument("--checkpoint", choices=CHECKPOINTS, required=True)
    collect_parser.add_argument("--execute", action="store_true")
    verify_parser = subparsers.add_parser("verify", allow_abbrev=False)
    verify_parser.add_argument("--session-id", required=True)
    subparsers.add_parser("verify-current", allow_abbrev=False)
    subparsers.add_parser("observe-node", allow_abbrev=False)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(list(argv) if argv is not None else None)
    try:
        if args.command == "observe-node":
            raw = sys.stdin.buffer.read(MAX_FILE_BYTES + 1)
            if len(raw) > MAX_FILE_BYTES:
                raise PlatformHealthError("platform-health node request is too large")
            result = observe_node(raw)
        else:
            config = load_config(INSTALLED_CONFIG)
            if args.command == "collect":
                result = collect(
                    config,
                    args.session_id,
                    args.checkpoint,
                    execute=args.execute,
                )
            elif args.command == "verify":
                result = verify(config, args.session_id)
            else:
                result = verify_current(config)
        sys.stdout.buffer.write(_canonical(result))
        return 0
    except PlatformHealthError as exc:
        sys.stderr.write(f"error: {exc}\n")
        return 1
    except OSError:
        sys.stderr.write("error: platform-health authority failed safely\n")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
