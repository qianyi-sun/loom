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

REPO_ROOT: Final = Path(__file__).resolve().parents[2]
IMPORT_ROOT: Final = (
    REPO_ROOT
    if (REPO_ROOT / "scripts/ops/developer_sandbox_capacity_contract.py").is_file()
    else Path(__file__).resolve().parent
)
if str(IMPORT_ROOT) not in sys.path:
    sys.path.insert(0, str(IMPORT_ROOT))

from scripts.ops import developer_environment_registry as environment_registry  # noqa: E402
from scripts.ops import developer_sandbox_live_acceptance as live_acceptance  # noqa: E402
from scripts.ops.developer_sandbox_capacity_contract import (  # noqa: E402
    CAPACITY_POLICY_SOURCES,
    CapacityContractError,
    load_capacity_policy,
)

SCHEMA_VERSION: Final = 1
PLATFORM_HEALTH_EVIDENCE_TTL: Final = timedelta(minutes=15)
DEFAULT_CONFIG: Final = REPO_ROOT / "deploy/developer-sandboxes/platform-health-authority.toml"
INSTALLED_CONFIG: Final = Path(
    "/opt/loom-developer-sandbox-node-authority/source/"
    "deploy/developer-sandboxes/platform-health-authority.toml",
)
CAPACITY_SOURCE_ROOT: Final = (
    REPO_ROOT
    if (REPO_ROOT / CAPACITY_POLICY_SOURCES["oldlab"]).is_file()
    else INSTALLED_CONFIG.parents[2]
)
CHECKPOINTS: Final = (
    "baseline",
    "mixed_non_loom",
    "cancel_cleanup",
    "ttl_cleanup",
    "submit_host_restart",
    "worker_crash",
    "final_drain",
)
CHECKPOINT_GROUPS: Final = {
    "baseline": "baseline",
    "mixed_non_loom": "during",
    "cancel_cleanup": "during",
    "ttl_cleanup": "during",
    "submit_host_restart": "during",
    "worker_crash": "during",
    "final_drain": "after",
}
POOLS: Final = ("oldlab", "gb10")
ROLES: Final = ("worker", "trial", "verifier", "sidecar")
GATE6_WORKLOADS: Final = ("loom", "non_loom_slurm", "kubernetes", "minio", "longhorn")
GATE6_CLEANUP_EVENTS: Final = (
    "cancellation",
    "ttl_expiry",
    "worker_crash",
    "submit_host_restart",
)
SOAK_REQUIRED_DURATION_SECONDS: Final = 14_400
SOAK_REQUIRED_SAMPLE_COUNT: Final = 120
SOAK_MINIMUM_TRIAL_SUCCESS_RATIO: Final = 0.95
CLEANUP_MAX_SECONDS: Final = 300
GATE6_MINIMUM_FREE_CPU_CORES: Final = 4
GATE6_MINIMUM_FREE_MEMORY_BYTES: Final = 16_000_000_000
GATE6_MAXIMUM_PID_USAGE_RATIO: Final = 0.7
EXCLUDED_NODES: Final[frozenset[str]] = frozenset()
EXPECTED_HOST_ALIASES: Final = {
    **{f"oldlab-{index}": f"trt-eai-oldlab-{index}" for index in range(1, 6)},
    "trt-gb10-1": "gx10-01c7",
    "trt-gb10-2": "gx10-0fca",
    "trt-gb10-3": "gx10-0f0d",
    "trt-gb10-4": "gx10-0d93",
    "trt-gb10-5": "gx10-1036",
    "trt-gb10-6": "gx10-1000",
    "trt-gb10-7": "gx10-0faf",
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
UUID_RE: Final = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$",
)
JOB_ID_RE: Final = re.compile(r"^[1-9][0-9]*(?:_[0-9]+)?$")
CONTAINER_ID_RE: Final = re.compile(r"^[0-9a-f]{12,64}$")
SAFE_NAME_RE: Final = re.compile(r"^[A-Za-z0-9_.-]{1,128}$")
ENV_ID_RE: Final = re.compile(r"^denv-[a-z0-9-]{8,64}$")
CANDIDATE_ID_RE: Final = re.compile(r"^cand-[0-9a-f]{40}$")
SLURM_START_TIME_RE: Final = re.compile(
    r"^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}$",
)
SYSTEMD_SLICE_RE: Final = re.compile(
    r"^loom-job-[1-9][0-9]*-[0-9a-f]{40}\.slice$",
)
SYSTEMD_SLICE_RECEIPT_ROOT: Final = Path(
    "/run/loom-developer-sandbox-slurm-policy/systemd-slices",
)
SYSTEMD_UNIT_ROOT: Final = Path("/run/systemd/system")
SYSTEMD_SLICE_RECEIPT_FIELDS: Final = {
    "schema_version",
    "kind",
    "systemd_slice",
    "slice_identity_sha256",
    "unit_sha256",
    "job_id",
    "job_start_time",
    "cluster",
    "node_list",
    "account",
    "env_id",
    "resource_generation",
    "runtime_id",
    "candidate_id",
    "candidate_sha",
    "candidate_tree",
    "cpu_max",
    "memory_max",
    "memory_swap_max_source",
    "memory_swap_max_effective",
    "pids_max",
    "cpuset_cpus",
    "cpuset_mems",
    "gpu_tres",
    "gpu_detail",
    "payload_sha256",
}
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
    "expected_slurm_node",
    "expected_host",
    "since_at",
    "registry_snapshot",
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
    "non_loom_slurm",
    "orphan_container_ids",
}
TERMINAL_JOB_FIELDS: Final = {
    "job_id",
    "job_name",
    "state",
    "node",
    "sandbox",
    "candidate_sha",
    "ended_at",
    "elapsed_seconds",
}
TRIAL_OUTCOME_FIELDS: Final = {
    "trial_id",
    "batch_id",
    "batch_created_at",
    "expected_trial_count",
    "state",
    "attempt_count",
    "retry_count",
    "finished_at",
    "worker_id",
    "slurm_job_id",
    "sandbox",
    "pool",
    "candidate_sha",
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
    registry_snapshot: Path
    minio_statefulset: str
    minio_pdb: str
    max_checkpoint_seconds: int
    max_clock_skew_seconds: int
    minimum_oldlab_free_cpu_cores: int
    minimum_oldlab_free_memory_bytes: int
    maximum_cpu_busy_ratio: float
    capacity_policy_sources: Mapping[str, str]
    oldlab_nodes: tuple[str, ...]
    gb10_nodes: tuple[str, ...]
    host_aliases: Mapping[str, str]

    @property
    def nodes(self) -> tuple[str, ...]:
        return (*self.oldlab_nodes, *self.gb10_nodes)

    @property
    def capacity_gb10_nodes(self) -> tuple[str, ...]:
        """Return production-capacity-eligible GB10 nodes."""

        return self.gb10_nodes


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
        "registry_snapshot",
        "minio_statefulset",
        "minio_pdb",
        "max_checkpoint_seconds",
        "max_clock_skew_seconds",
        "minimum_oldlab_free_cpu_cores",
        "minimum_oldlab_free_memory_bytes",
        "maximum_cpu_busy_ratio",
        "capacity_policy_sources",
        "oldlab_nodes",
        "gb10_nodes",
        "host_aliases",
    }
    if not isinstance(payload, dict) or set(payload) != fields:
        raise PlatformHealthError("platform-health authority config has an invalid shape")
    oldlab = tuple(payload["oldlab_nodes"])
    gb10 = tuple(payload["gb10_nodes"])
    aliases = payload["host_aliases"]
    policy_sources = payload["capacity_policy_sources"]
    nodes = (*oldlab, *gb10)
    if (
        payload["schema_version"] != SCHEMA_VERSION
        or payload["collector_host"] != "trt-eai-oldlab-2"
        or payload["namespace"] != "loom-staging"
        or payload["longhorn_namespace"] != "longhorn-system"
        or not isinstance(aliases, dict)
        or policy_sources != CAPACITY_POLICY_SOURCES
        or tuple(aliases) != nodes
        or aliases != EXPECTED_HOST_ALIASES
        or len(set(aliases.values())) != len(aliases)
        or len(nodes) != 20
        or len(set(nodes)) != len(nodes)
        or not EXCLUDED_NODES.issubset(set(nodes))
        or len(oldlab) != 5
        or len(gb10) != 15
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
        Path(payload["registry_snapshot"]),
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
        registry_snapshot=absolute_paths[4],
        minio_statefulset=payload["minio_statefulset"],
        minio_pdb=payload["minio_pdb"],
        max_checkpoint_seconds=payload["max_checkpoint_seconds"],
        max_clock_skew_seconds=payload["max_clock_skew_seconds"],
        minimum_oldlab_free_cpu_cores=payload["minimum_oldlab_free_cpu_cores"],
        minimum_oldlab_free_memory_bytes=payload["minimum_oldlab_free_memory_bytes"],
        maximum_cpu_busy_ratio=payload["maximum_cpu_busy_ratio"],
        capacity_policy_sources=dict(policy_sources),
        oldlab_nodes=oldlab,
        gb10_nodes=gb10,
        host_aliases=dict(aliases),
    )


def _load_capacity_policy(pool: str) -> dict[str, Any]:
    """Load one exact checked-in policy from the installed candidate source.

    The recommendation intentionally derives from these installed bytes instead
    of duplicating capacity constants in the evidence producer.
    """

    expected_nodes = (
        tuple(
            value for value in EXPECTED_HOST_ALIASES.values() if value.startswith("trt-eai-oldlab-")
        )
        if pool == "oldlab"
        else tuple(node for node in EXPECTED_HOST_ALIASES if node.startswith("trt-gb10-"))
    )
    try:
        contract = load_capacity_policy(
            CAPACITY_SOURCE_ROOT,
            pool,
            expected_nodes=expected_nodes,
        )
    except CapacityContractError as exc:
        raise PlatformHealthError(str(exc)) from exc
    return {
        "schema_version": SCHEMA_VERSION,
        "pool": contract.pool,
        "source": contract.source,
        "source_sha256": contract.source_sha256,
        "values": dict(contract.values),
    }


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


def _validated_registry_snapshot(value: object) -> dict[str, Any]:
    try:
        return live_acceptance._validated_registry_snapshot(value)
    except live_acceptance.AcceptanceError as exc:
        raise PlatformHealthError("acceptance registry snapshot is invalid") from exc


def _current_registry_snapshot(config: Config) -> dict[str, Any]:
    raw = _read_secure_bytes(
        config.registry_snapshot,
        label="developer environment registry snapshot",
    )
    try:
        source = environment_registry.DeveloperEnvironmentRegistry.verify_snapshot(raw)
        projected = live_acceptance._acceptance_registry_snapshot(source)
    except (
        environment_registry.RegistryError,
        live_acceptance.AcceptanceError,
    ) as exc:
        raise PlatformHealthError("developer environment registry snapshot is invalid") from exc
    return _validated_registry_snapshot(projected)


def _registry_environments(snapshot: Mapping[str, Any]) -> dict[str, dict[str, Any]]:
    projection = _validated_registry_snapshot(snapshot)
    source = projection["source_registry"]
    candidates = {
        str(row["candidate_id"]): row for row in source["candidates"] if isinstance(row, Mapping)
    }
    result: dict[str, dict[str, Any]] = {}
    for row in source["environments"]:
        if not isinstance(row, Mapping) or row.get("state") != "active":
            continue
        candidate = candidates.get(str(row.get("current_candidate_id")))
        if not isinstance(candidate, Mapping):
            raise PlatformHealthError("registry environment candidate is invalid")
        environment = {
            **dict(row),
            "candidate_id": candidate["candidate_id"],
            "candidate_sha": candidate["candidate_sha"],
            "candidate_tree": candidate["candidate_tree"],
        }
        result[str(row["runtime_id"])] = environment
    projected_ids = {str(row["runtime_id"]) for row in projection["environments"]}
    if set(result) != projected_ids:
        raise PlatformHealthError("registry environment projection is incomplete")
    return result


def _sandboxes(snapshot: Mapping[str, Any]) -> tuple[str, ...]:
    return tuple(_registry_environments(snapshot))


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
    _ensure_directory(session_root / "samples")
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
    registry_snapshot = state.get("registry_snapshot") if isinstance(state, dict) else None
    validated_registry = _validated_registry_snapshot(registry_snapshot)
    current_registry = _current_registry_snapshot(config)
    sandboxes = _sandboxes(validated_registry)
    registry_environments = _registry_environments(validated_registry)
    if (
        not isinstance(state, dict)
        or state.get("schema_version") != 2
        or state.get("session_id") != session_id
        or state.get("submit_host") != config.collector_host
        or state.get("status") not in {"running", "complete"}
        or validated_registry != current_registry
        or not isinstance(candidates, dict)
        or set(candidates) != set(sandboxes)
        or any(
            not isinstance(candidates[sandbox], dict)
            or set(candidates[sandbox]) != {"sha", "tree"}
            or SHA_RE.fullmatch(str(candidates[sandbox]["sha"])) is None
            or SHA_RE.fullmatch(str(candidates[sandbox]["tree"])) is None
            or candidates[sandbox]["sha"] != registry_environments[sandbox]["candidate_sha"]
            or candidates[sandbox]["tree"] != registry_environments[sandbox]["candidate_tree"]
            for sandbox in sandboxes
        )
        or not isinstance(state.get("completed_phases"), list)
    ):
        raise PlatformHealthError("acceptance session state binding is invalid")
    return state


def _require_acceptance_phase(
    config: Config,
    state: Mapping[str, Any],
    checkpoint: str,
) -> dict[str, str]:
    sandboxes = _sandboxes(state["registry_snapshot"])
    completed = state["completed_phases"]
    expected = [f"{sandbox}:{checkpoint}" for sandbox in sandboxes]
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
    for sandbox_index, sandbox in enumerate(sandboxes):
        checkpoint_index = phase_index * len(sandboxes) + sandbox_index
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


def _soak_trial_batch_manifest(
    config: Config,
    state: Mapping[str, Any],
) -> list[dict[str, str]]:
    sandboxes = _sandboxes(state["registry_snapshot"])
    phase_index = (
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
    ).index("mixed_non_loom") * len(sandboxes)
    manifest: list[dict[str, str]] = []
    for sandbox_index, sandbox in enumerate(sandboxes):
        checkpoint = (
            config.acceptance_state_root
            / "sessions"
            / str(state["session_id"])
            / "checkpoints"
            / f"{phase_index + sandbox_index:02d}-{sandbox}-mixed_non_loom.json"
        )
        payload, raw = _secure_json(
            checkpoint,
            label="mixed-workload trial-batch checkpoint",
        )
        trial_batches = payload.get("trial_batches") if isinstance(payload, dict) else None
        candidate = state["candidates"][sandbox]
        if (
            not isinstance(trial_batches, dict)
            or raw != _canonical(payload)
            or set(payload)
            != {
                "schema_version",
                "session_id",
                "sandbox",
                "candidate_sha",
                "candidate_tree",
                "phase",
                "phase_started_at",
                "recorded_at",
                "status",
                "evidence_sha256",
                "trial_batches",
            }
            or payload.get("schema_version") != 2
            or payload.get("session_id") != state["session_id"]
            or set(trial_batches) != set(POOLS)
            or any(UUID_RE.fullmatch(str(trial_batches[pool])) is None for pool in POOLS)
            or payload.get("sandbox") != sandbox
            or payload.get("phase") != "mixed_non_loom"
            or payload.get("candidate_sha") != candidate["sha"]
            or payload.get("candidate_tree") != candidate["tree"]
            or payload.get("status") != "pass"
            or DIGEST_RE.fullmatch(str(payload.get("evidence_sha256"))) is None
        ):
            raise PlatformHealthError("mixed-workload trial-batch manifest is invalid")
        phase_started = _timestamp(
            payload["phase_started_at"],
            label="mixed-workload phase start",
        )
        recorded_at = _timestamp(
            payload["recorded_at"],
            label="mixed-workload phase completion",
        )
        if recorded_at < phase_started:
            raise PlatformHealthError("mixed-workload trial-batch window is invalid")
        manifest.extend(
            {
                "sandbox": sandbox,
                "pool": pool,
                "batch_id": str(trial_batches[pool]),
                "candidate_sha": candidate["sha"],
                "candidate_tree": candidate["tree"],
                "phase_started_at": _iso(phase_started),
                "phase_completed_at": _iso(recorded_at),
            }
            for pool in POOLS
        )
    if len({row["batch_id"] for row in manifest}) != len(manifest):
        raise PlatformHealthError("mixed-workload trial-batch manifest is duplicated")
    return manifest


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
            env={
                "PATH": "/usr/sbin:/usr/bin:/sbin:/bin",
                "LANG": "C",
                "LC_ALL": "C",
                "TZ": "UTC",
            },
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


_TRIAL_OUTCOME_SQL: Final = """
SELECT json_build_object(
  'trial_id', t.id::text,
  'batch_id', t.batch_id::text,
  'batch_created_at', to_char(b.created_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'),
  'expected_trial_count', b.expected_trial_count,
  'state', t.state,
  'attempt_count', t.attempt_count,
  'retry_count', greatest(t.attempt_count - 1, 0),
  'finished_at', to_char(t.finished_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'),
  'worker_id', t.worker_id::text,
  'slurm_job_id', j.job_id,
  'sandbox', j.sandbox_identity,
  'pool', j.pool_name,
  'candidate_sha', j.candidate_sha
)::text
FROM trials AS t
JOIN batches AS b ON b.id = t.batch_id
LEFT JOIN workers AS w ON w.id = t.worker_id
LEFT JOIN slurm_worker_jobs AS j ON j.worker_id = w.id
WHERE t.batch_id IN (
    :'oldlab_batch_id'::uuid,
    :'gb10_batch_id'::uuid
  )
ORDER BY t.finished_at, t.id
""".strip()


def _sandbox_postgres_container(
    sandbox: str,
    candidate: Mapping[str, str],
    environment: Mapping[str, Any],
    *,
    run: Run,
) -> tuple[str, dict[str, str]]:
    manifest, _raw = _secure_json(
        Path(str(environment["state_root"])) / "host-manifest.json",
        label="developer environment host manifest",
    )
    unsigned = (
        {key: value for key, value in manifest.items() if key != "payload_sha256"}
        if isinstance(manifest, dict)
        else {}
    )
    if (
        not isinstance(manifest, dict)
        or manifest.get("schema_version") != 1
        or manifest.get("kind") != "loom.developer-environment.host-manifest"
        or manifest.get("env_id") != environment["env_id"]
        or manifest.get("candidate_id") != environment["candidate_id"]
        or manifest.get("candidate_sha") != candidate["sha"]
        or manifest.get("candidate_tree") != candidate["tree"]
        or manifest.get("compose_project") != environment["compose_project"]
        or manifest.get("slurm_user") != environment["slurm_user"]
        or manifest.get("slurm_account") != environment["slurm_account"]
        or manifest.get("slurm_qos") != environment["slurm_qos"]
        or manifest.get("cgroup_slice") != environment["cgroup_slice"]
        or manifest.get("candidate_checkout")
        != str(Path(str(environment["candidate_root"])) / candidate["sha"])
        or manifest.get("runtime_root") != environment["runtime_root"]
        or manifest.get("payload_sha256") != _digest(unsigned)
    ):
        raise PlatformHealthError("developer environment host manifest is invalid")
    raw = _run_bounded(
        (
            "/usr/bin/docker",
            "ps",
            "--quiet",
            "--filter",
            f"label=com.docker.compose.project={environment['compose_project']}",
            "--filter",
            f"label=loom.developer-environment.env-id={environment['env_id']}",
            "--filter",
            f"label=loom.developer-environment.candidate-id={environment['candidate_id']}",
            "--filter",
            "label=com.docker.compose.service=postgres",
            "--filter",
            "status=running",
        ),
        run=run,
    )
    try:
        container_ids = raw.decode("ascii").splitlines()
    except UnicodeDecodeError as exc:
        raise PlatformHealthError("sandbox Postgres container identity is invalid") from exc
    if len(container_ids) != 1 or CONTAINER_ID_RE.fullmatch(container_ids[0]) is None:
        raise PlatformHealthError("sandbox Postgres container set is not closed")
    container_id = container_ids[0]
    inspected = _json_command(("/usr/bin/docker", "inspect", container_id), run=run)
    entry = inspected[0] if isinstance(inspected, list) and len(inspected) == 1 else None
    labels = entry.get("Config", {}).get("Labels") if isinstance(entry, dict) else None
    state = entry.get("State") if isinstance(entry, dict) else None
    full_container_id = entry.get("Id") if isinstance(entry, dict) else None
    expected_candidate_root = str(Path(str(environment["candidate_root"])) / candidate["sha"])
    expected_compose_root = str(Path(expected_candidate_root) / "deploy")
    compose_config = (
        labels.get("com.docker.compose.project.config_files") if isinstance(labels, dict) else None
    )
    compose_hash = (
        labels.get("com.docker.compose.config-hash") if isinstance(labels, dict) else None
    )
    if (
        not isinstance(labels, dict)
        or not isinstance(full_container_id, str)
        or CONTAINER_ID_RE.fullmatch(full_container_id) is None
        or not full_container_id.startswith(container_id)
        or labels.get("com.docker.compose.project") != environment["compose_project"]
        or labels.get("loom.developer-environment.env-id") != environment["env_id"]
        or labels.get("loom.developer-environment.candidate-id") != environment["candidate_id"]
        or labels.get("com.docker.compose.service") != "postgres"
        or labels.get("com.docker.compose.project.working_dir") != expected_compose_root
        or compose_config != str(Path(expected_candidate_root) / "deploy/docker-compose.dev.yml")
        or not isinstance(compose_hash, str)
        or DIGEST_RE.fullmatch(compose_hash) is None
        or not isinstance(state, dict)
        or state.get("Running") is not True
        or state.get("Health", {}).get("Status") != "healthy"
    ):
        raise PlatformHealthError("sandbox Postgres container binding is invalid")
    assert isinstance(entry, dict)
    assert isinstance(state, dict)
    created_at = _timestamp(entry.get("Created"), label="sandbox Postgres created_at")
    started_at = _timestamp(state.get("StartedAt"), label="sandbox Postgres started_at")
    if created_at > started_at:
        raise PlatformHealthError("sandbox Postgres generation is not current")
    return container_id, {
        "sandbox": sandbox,
        "candidate_sha": candidate["sha"],
        "candidate_tree": candidate["tree"],
        "compose_project": str(environment["compose_project"]),
        "container_id": full_container_id,
        "compose_config_sha256": compose_hash,
        "created_at": _iso(created_at),
        "started_at": _iso(started_at),
        "lifecycle_updated_at": _iso(started_at),
        "desired_sha256": str(manifest["payload_sha256"]),
        "lifecycle_sha256": str(manifest["payload_sha256"]),
        "combined_receipt_sha256": str(manifest["payload_sha256"]),
    }


def _trial_outcomes(
    candidates: Mapping[str, Mapping[str, str]],
    registry_snapshot: Mapping[str, Any],
    trial_batches: Sequence[Mapping[str, str]],
    *,
    run: Run = subprocess.run,
) -> tuple[list[dict[str, Any]], list[dict[str, str]]]:
    outcomes: list[dict[str, Any]] = []
    authorities: list[dict[str, str]] = []
    environments = _registry_environments(registry_snapshot)
    for sandbox in candidates:
        candidate = candidates[sandbox]
        sandbox_batches = {
            row["pool"]: row["batch_id"] for row in trial_batches if row["sandbox"] == sandbox
        }
        if set(sandbox_batches) != set(POOLS):
            raise PlatformHealthError("sandbox trial-batch manifest is incomplete")
        container_id, authority = _sandbox_postgres_container(
            sandbox,
            candidate,
            environments[sandbox],
            run=run,
        )
        authorities.append(authority)
        raw = _run_bounded(
            (
                "/usr/bin/docker",
                "exec",
                container_id,
                "/bin/sh",
                "-ceu",
                (
                    'PGPASSWORD="$POSTGRES_PASSWORD" exec psql '
                    '--host=127.0.0.1 --username="$POSTGRES_USER" '
                    '--dbname="$POSTGRES_DB" --no-psqlrc --tuples-only --no-align '
                    '--set=ON_ERROR_STOP=1 --set=oldlab_batch_id="$1" '
                    '--set=gb10_batch_id="$2" --command="$3"'
                ),
                "loom-platform-health-trial-readback",
                sandbox_batches["oldlab"],
                sandbox_batches["gb10"],
                _TRIAL_OUTCOME_SQL,
            ),
            run=run,
        )
        try:
            rows = [json.loads(line) for line in raw.decode("utf-8").splitlines() if line.strip()]
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise PlatformHealthError("sandbox trial outcome readback is invalid") from exc
        if any(not isinstance(row, dict) for row in rows):
            raise PlatformHealthError("sandbox trial outcome readback is invalid")
        outcomes.extend(rows)
    ordered = sorted(
        outcomes, key=lambda row: (str(row.get("finished_at")), str(row.get("trial_id")))
    )
    if len({str(row.get("trial_id")) for row in ordered}) != len(ordered):
        raise PlatformHealthError("sandbox trial outcome attribution is duplicated")
    return ordered, authorities


def _probe_command(argv: Sequence[str], *, run: Run) -> tuple[bool, str]:
    """Run one fixed, non-mutating device probe and retain only sanitized stdout."""

    try:
        completed = run(
            tuple(argv),
            check=False,
            capture_output=True,
            timeout=30,
            env={
                "PATH": "/usr/sbin:/usr/bin:/sbin:/bin",
                "LANG": "C",
                "LC_ALL": "C",
                "TZ": "UTC",
            },
        )
    except (OSError, subprocess.SubprocessError) as exc:
        raise PlatformHealthError("fixed device probe failed safely") from exc
    stdout = completed.stdout.encode() if isinstance(completed.stdout, str) else completed.stdout
    stderr = completed.stderr.encode() if isinstance(completed.stderr, str) else completed.stderr
    if len(stdout) + len(stderr) > 64 * 1024:
        raise PlatformHealthError("fixed device probe output is oversized")
    try:
        sanitized = bytes(stdout).decode("ascii").strip()
    except UnicodeDecodeError as exc:
        raise PlatformHealthError("fixed device probe output is invalid") from exc
    if sanitized and any(SAFE_NAME_RE.fullmatch(row) is None for row in sanitized.splitlines()):
        raise PlatformHealthError("fixed device probe output is invalid")
    return completed.returncode == 0, sanitized


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


def _positive_int(value: object) -> bool:
    return isinstance(value, int) and not isinstance(value, bool) and value > 0


def _nonnegative_int(value: object) -> bool:
    return isinstance(value, int) and not isinstance(value, bool) and value >= 0


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


def _validate_platform_health_observation(value: object) -> None:
    if not isinstance(value, dict) or set(value) != {"k3s", "minio", "longhorn"}:
        raise PlatformHealthError("platform health observation has an invalid shape")
    k3s = value["k3s"]
    minio = value["minio"]
    longhorn = value["longhorn"]
    if (
        not isinstance(k3s, dict)
        or set(k3s) != {"readyz", "node_count", "ready_node_count"}
        or k3s["readyz"] is not True
        or not _positive_int(k3s["node_count"])
        or k3s["ready_node_count"] != k3s["node_count"]
        or not isinstance(minio, dict)
        or set(minio)
        != {
            "replicas",
            "ready_replicas",
            "quorum_healthy",
            "pdb_expected_pods",
            "pdb_current_healthy",
            "pdb_desired_healthy",
            "pdb_disruptions_allowed",
        }
        or minio["quorum_healthy"] is not True
        or not _positive_int(minio["replicas"])
        or minio["ready_replicas"] != minio["replicas"]
        or not _positive_int(minio["pdb_expected_pods"])
        or not _nonnegative_int(minio["pdb_current_healthy"])
        or not _positive_int(minio["pdb_desired_healthy"])
        or not _nonnegative_int(minio["pdb_disruptions_allowed"])
        or minio["pdb_current_healthy"] < minio["pdb_desired_healthy"]
        or not isinstance(longhorn, dict)
        or set(longhorn)
        != {
            "volume_count",
            "healthy_volume_count",
            "pod_count",
            "ready_pod_count",
        }
        or not _positive_int(longhorn["volume_count"])
        or longhorn["healthy_volume_count"] != longhorn["volume_count"]
        or not _positive_int(longhorn["pod_count"])
        or longhorn["ready_pod_count"] != longhorn["pod_count"]
    ):
        raise PlatformHealthError("platform health observation is not fully healthy")


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
        "expected_slurm_node": config.host_aliases[node] if node.startswith("oldlab-") else node,
        "expected_host": config.host_aliases[node],
        "since_at": since_at,
        "registry_snapshot": state["registry_snapshot"],
        "candidates": state["candidates"],
    }


def _request_envelope(
    request: Mapping[str, Any],
    *,
    node: str,
) -> bytes:
    payload = _canonical(request)
    sandbox = next(iter(request["candidates"]))
    candidate = request["candidates"][sandbox]
    body: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "action": "observe-platform-health-node",
        "node": node,
        "domain": "oldlab" if node.startswith("oldlab-") else "gb10",
        "sandbox": sandbox,
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
    for sandbox in candidates:
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


def _cgroup_binds_slurm_job(job_path: str, job_id: str) -> bool:
    """Bind one Slurm job root to the independently queried Slurm JobId.

    Docker's caller-controlled ``CgroupParent`` is never sufficient authority.
    Accepted roots must contain Slurm's exact ``job_<JobId>`` identity as one
    path component; this makes a cross-job swap fail even when both roots have
    finite controllers.
    """

    path = PurePosixPath(job_path)
    return (
        path.is_absolute()
        and path != PurePosixPath("/")
        and all(part not in {".", ".."} for part in path.parts)
        and f"job_{job_id}" in path.parts
    )


def _slurm_job_root(paths: Sequence[str], job_id: str) -> str:
    """Derive one exact Slurm job root from independently indexed job PIDs."""

    marker = f"job_{job_id}"
    roots: set[PurePosixPath] = set()
    for raw in paths:
        path = PurePosixPath(raw)
        indexes = [index for index, part in enumerate(path.parts) if part == marker]
        if (
            not path.is_absolute()
            or path == PurePosixPath("/")
            or len(indexes) != 1
            or any(part in {".", ".."} for part in path.parts)
        ):
            raise PlatformHealthError("Slurm job PID cgroup identity is invalid")
        root = PurePosixPath(*path.parts[: indexes[0] + 1])
        if not _strict_descendant(str(path), str(root)):
            raise PlatformHealthError("Slurm job PID is outside its job cgroup")
        roots.add(root)
    if len(roots) != 1:
        raise PlatformHealthError("Slurm job PID cgroup roots are ambiguous")
    root = str(next(iter(roots)))
    if not _cgroup_binds_slurm_job(root, job_id):
        raise PlatformHealthError("Slurm job cgroup is not bound to its JobId")
    return root


def _container_cgroup_layout(observed_path: str, cgroup_parent: str) -> tuple[str, str]:
    """Validate one Docker cgroup parent and return layout plus live parent path."""

    if cgroup_parent.startswith("/"):
        if not _strict_descendant(observed_path, cgroup_parent):
            raise PlatformHealthError("Docker container escaped its Slurm job cgroup")
        return "cgroupfs-job-v1", cgroup_parent
    if SYSTEMD_SLICE_RE.fullmatch(cgroup_parent) is None:
        raise PlatformHealthError("Docker cgroup parent is invalid")
    job_id_match = re.fullmatch(
        r"loom-job-([1-9][0-9]*)-[0-9a-f]{40}\.slice",
        cgroup_parent,
    )
    if job_id_match is None:
        raise PlatformHealthError("Docker systemd slice identity is invalid")
    canonical_slice_path = PurePosixPath(
        "/",
        "loom.slice",
        "loom-job.slice",
        f"loom-job-{job_id_match.group(1)}.slice",
        cgroup_parent,
    )
    observed = PurePosixPath(observed_path)
    indexes = [index for index, part in enumerate(observed.parts) if part == cgroup_parent]
    if (
        not observed.is_absolute()
        or observed == PurePosixPath("/")
        or len(indexes) != 1
        or any(part in {".", ".."} for part in observed.parts)
    ):
        raise PlatformHealthError("Docker systemd slice cgroup is invalid")
    slice_path = PurePosixPath(*observed.parts[: indexes[0] + 1])
    if slice_path != canonical_slice_path or not _strict_descendant(str(observed), str(slice_path)):
        raise PlatformHealthError("Docker container escaped its systemd slice")
    return "systemd-mirror-v1", str(slice_path)


def _cpuset_values(value: str) -> set[int]:
    result: set[int] = set()
    for item in value.split(","):
        start_text, separator, end_text = item.partition("-")
        if (
            not start_text.isdecimal()
            or (separator and not end_text.isdecimal())
            or (not separator and end_text)
        ):
            raise PlatformHealthError("cgroup cpuset is invalid")
        start = int(start_text)
        end = int(end_text) if separator else start
        if end < start or end - start > 1_000_000:
            raise PlatformHealthError("cgroup cpuset is invalid")
        result.update(range(start, end + 1))
    if not result:
        raise PlatformHealthError("cgroup cpuset is empty")
    return result


def _cpu_quota(value: str) -> tuple[int, int]:
    fields = value.split()
    if (
        len(fields) != 2
        or fields[0] == "max"
        or not all(item.isdecimal() for item in fields)
        or int(fields[0]) < 1
        or int(fields[1]) < 1
    ):
        raise PlatformHealthError("cgroup CPU limit is not finite")
    return int(fields[0]), int(fields[1])


def _finite_integer_value(value: str, *, allow_zero: bool = False) -> int:
    if value == "max" or not value.isdecimal():
        raise PlatformHealthError("cgroup integer limit is not finite")
    parsed = int(value)
    if parsed < (0 if allow_zero else 1):
        raise PlatformHealthError("cgroup integer limit is not finite")
    return parsed


def _systemd_slice_identity(identity: Mapping[str, Any]) -> tuple[str, str]:
    expected_fields = {
        "cluster",
        "node",
        "job_id",
        "job_start_time",
        "account",
        "env_id",
        "resource_generation",
        "runtime_id",
        "candidate_id",
        "candidate_sha",
        "candidate_tree",
    }
    if (
        set(identity) != expected_fields
        or not isinstance(identity.get("cluster"), str)
        or SAFE_NAME_RE.fullmatch(str(identity["cluster"])) is None
        or not isinstance(identity.get("node"), str)
        or SAFE_NAME_RE.fullmatch(str(identity["node"])) is None
        or re.fullmatch(r"[1-9][0-9]*", str(identity.get("job_id"))) is None
        or SLURM_START_TIME_RE.fullmatch(str(identity.get("job_start_time"))) is None
        or SAFE_NAME_RE.fullmatch(str(identity.get("account"))) is None
        or ENV_ID_RE.fullmatch(str(identity.get("env_id"))) is None
        or type(identity.get("resource_generation")) is not int
        or int(identity["resource_generation"]) < 1
        or SAFE_NAME_RE.fullmatch(str(identity.get("runtime_id"))) is None
        or CANDIDATE_ID_RE.fullmatch(str(identity.get("candidate_id"))) is None
        or SHA_RE.fullmatch(str(identity.get("candidate_sha"))) is None
        or SHA_RE.fullmatch(str(identity.get("candidate_tree"))) is None
    ):
        raise PlatformHealthError("systemd slice identity is invalid")
    digest = hashlib.sha256(
        json.dumps(identity, sort_keys=True, separators=(",", ":")).encode("ascii"),
    ).hexdigest()
    unit = f"loom-job-{identity['job_id']}-{digest[:40]}.slice"
    if SYSTEMD_SLICE_RE.fullmatch(unit) is None:
        raise PlatformHealthError("systemd slice identity is invalid")
    return unit, digest


def _validated_systemd_slice_receipt_payload(
    receipt: object,
    *,
    identity: Mapping[str, Any],
    limits: Mapping[str, str],
    expected_gpu_tres: str,
    expected_gpu_detail: str,
) -> dict[str, Any]:
    if not isinstance(receipt, dict):
        raise PlatformHealthError("systemd slice receipt is invalid")
    unit, identity_digest = _systemd_slice_identity(identity)
    unsigned = {key: value for key, value in receipt.items() if key != "payload_sha256"}
    expected = {
        "systemd_slice": unit,
        "slice_identity_sha256": identity_digest,
        "job_id": identity["job_id"],
        "job_start_time": identity["job_start_time"],
        "cluster": identity["cluster"],
        "node_list": identity["node"],
        "account": identity["account"],
        "env_id": identity["env_id"],
        "resource_generation": identity["resource_generation"],
        "runtime_id": identity["runtime_id"],
        "candidate_id": identity["candidate_id"],
        "candidate_sha": identity["candidate_sha"],
        "candidate_tree": identity["candidate_tree"],
        **dict(limits),
        "gpu_tres": expected_gpu_tres,
        "gpu_detail": expected_gpu_detail,
    }
    source_swap = str(receipt.get("memory_swap_max_source"))
    if source_swap != "max":
        _finite_integer_value(source_swap, allow_zero=True)
    expected_swap = "0"
    if (
        set(receipt) != SYSTEMD_SLICE_RECEIPT_FIELDS
        or receipt.get("schema_version") != 1
        or receipt.get("kind") != "loom.slurm-systemd-slice-receipt"
        or any(receipt.get(key) != value for key, value in expected.items())
        or receipt.get("memory_swap_max_effective") != expected_swap
        or DIGEST_RE.fullmatch(str(receipt.get("unit_sha256"))) is None
        or receipt.get("payload_sha256")
        != hashlib.sha256(
            json.dumps(unsigned, sort_keys=True, separators=(",", ":")).encode("ascii"),
        ).hexdigest()
    ):
        raise PlatformHealthError("systemd slice receipt binding is invalid")
    _cpu_quota(str(receipt["cpu_max"]))
    _finite_integer_value(str(receipt["memory_max"]))
    _finite_integer_value(str(receipt["pids_max"]))
    _cpuset_values(str(receipt["cpuset_cpus"]))
    _cpuset_values(str(receipt["cpuset_mems"]))
    return dict(receipt)


def _load_systemd_slice_receipt(
    unit: str,
    *,
    identity: Mapping[str, Any],
    limits: Mapping[str, str],
    expected_gpu_tres: str,
    expected_gpu_detail: str,
) -> dict[str, Any]:
    raw = _read_secure_bytes(
        SYSTEMD_SLICE_RECEIPT_ROOT / f"{unit}.json",
        label="systemd slice receipt",
        mode=0o444,
    )
    try:
        receipt = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise PlatformHealthError("systemd slice receipt JSON is invalid") from exc
    if raw != _canonical(receipt):
        raise PlatformHealthError("systemd slice receipt is not canonical")
    validated = _validated_systemd_slice_receipt_payload(
        receipt,
        identity=identity,
        limits=limits,
        expected_gpu_tres=expected_gpu_tres,
        expected_gpu_detail=expected_gpu_detail,
    )
    if validated["systemd_slice"] != unit:
        raise PlatformHealthError("systemd slice receipt reached a foreign unit")
    unit_bytes = _read_secure_bytes(
        SYSTEMD_UNIT_ROOT / unit,
        label="systemd slice unit",
        mode=0o644,
    )
    if hashlib.sha256(unit_bytes).hexdigest() != validated["unit_sha256"]:
        raise PlatformHealthError("systemd slice unit digest drifted")
    return validated


def _systemd_slice_live_evidence(
    slice_path: str,
    *,
    receipt: Mapping[str, Any],
) -> dict[str, Any]:
    live_cpu = _read_cgroup_limit(slice_path, "cpu.max")
    live_memory = _read_cgroup_limit(slice_path, "memory.max")
    live_swap = _read_cgroup_limit(slice_path, "memory.swap.max")
    live_pids = _read_cgroup_limit(slice_path, "pids.max")
    live_cpus = _read_cgroup_limit(slice_path, "cpuset.cpus.effective")
    live_mems = _read_cgroup_limit(slice_path, "cpuset.mems.effective")
    live_quota, live_period = _cpu_quota(live_cpu)
    source_quota, source_period = _cpu_quota(str(receipt["cpu_max"]))
    memory = _finite_integer_value(live_memory)
    swap = _finite_integer_value(live_swap, allow_zero=True)
    pids = _finite_integer_value(live_pids)
    if (
        live_quota * source_period > source_quota * live_period
        or memory > _finite_integer_value(str(receipt["memory_max"]))
        or swap
        > _finite_integer_value(
            str(receipt["memory_swap_max_effective"]),
            allow_zero=True,
        )
        or pids > _finite_integer_value(str(receipt["pids_max"]))
        or not _cpuset_values(live_cpus).issubset(
            _cpuset_values(str(receipt["cpuset_cpus"])),
        )
        or not _cpuset_values(live_mems).issubset(
            _cpuset_values(str(receipt["cpuset_mems"])),
        )
    ):
        raise PlatformHealthError("systemd slice live limits are weaker than Slurm")
    return {
        "path": slice_path,
        "cpu_cores_max": live_quota / live_period,
        "memory_bytes_max": memory,
        "memory_swap_bytes_max": swap,
        "pids_max": pids,
        "cpuset_cpus": live_cpus,
        "cpuset_mems": live_mems,
    }


def _slurm_job_pid_cgroups(job_id: str, *, run: Run) -> tuple[str, ...]:
    """Return cgroups for PIDs selected by Slurm's own JobId index."""

    raw = (
        _run_bounded(
            ("/usr/bin/scontrol", "listpids", job_id),
            run=run,
        )
        .decode(errors="strict")
        .splitlines()
    )
    if not raw or raw[0].split() != ["PID", "JOBID", "STEPID", "LOCALID", "GLOBALID"]:
        raise PlatformHealthError("Slurm job PID readback header is invalid")
    pids: list[int] = []
    for line in raw[1:]:
        fields = line.split()
        if len(fields) != 5:
            raise PlatformHealthError("Slurm job PID readback is malformed")
        pid, observed_job_id, step_id, local_id, global_id = fields
        if (
            not pid.isdigit()
            or int(pid) < 1
            or observed_job_id != job_id
            or SAFE_NAME_RE.fullmatch(step_id) is None
            or any(value != "-" and not value.isdigit() for value in (local_id, global_id))
        ):
            raise PlatformHealthError("Slurm job PID identity is invalid")
        pids.append(int(pid))
    if not pids or len(pids) != len(set(pids)) or len(pids) > 4096:
        raise PlatformHealthError("Slurm job PID set is invalid")
    return tuple(_proc_cgroup(pid) for pid in pids)


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


def _gpu_detail_ids(value: str) -> set[str]:
    """Parse the exact physical GPU indexes authorized by Slurm GresDetail."""

    if (
        not isinstance(value, str)
        or value in {"", "(null)", "None"}
        or "mig" in value.lower()
        or len(value) > 4096
    ):
        raise PlatformHealthError("Slurm GPU detail identity is unavailable")
    result: set[str] = set()
    for descriptor in value.split(","):
        match = re.fullmatch(
            r"gpu(?::[A-Za-z0-9_.-]+)*\(IDX:([0-9]+(?:-[0-9]+)?)\)",
            descriptor,
        )
        if match is None:
            raise PlatformHealthError("Slurm GPU detail identity is ambiguous")
        start_text, separator, end_text = match.group(1).partition("-")
        start = int(start_text)
        end = int(end_text) if separator else start
        if start > 4095 or end < start or end > 4095:
            raise PlatformHealthError("Slurm GPU detail identity is out of range")
        for index in range(start, end + 1):
            identity = str(index)
            if identity in result:
                raise PlatformHealthError("Slurm GPU detail identity is duplicated")
            result.add(identity)
    if not result:
        raise PlatformHealthError("Slurm GPU detail identity is empty")
    return result


def _job_readback(
    *,
    sandbox: str,
    candidate_sha: str,
    environment: Mapping[str, Any],
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
    job_start_time = values.get("StartTime", "")
    pids_match = re.fullmatch(r"loom-cgroup-v1:pids=([1-9][0-9]*)", comment)
    shared = values.get("Shared", values.get("OverSubscribe", ""))
    if (
        values.get("JobId") != job_id
        or not job_name.startswith(f"loom-{sandbox}-{candidate_sha[:12]}-")
        or user != environment["slurm_user"]
        or values.get("Account") != environment["slurm_account"]
        or values.get("QOS") != environment["slurm_qos"]
        or values.get("JobState") != "RUNNING"
        or values.get("NodeList", "").lower() != expected_node.lower()
        or values.get("NumNodes") != "1"
        or not values.get("NumCPUs", "").isdigit()
        or SLURM_START_TIME_RE.fullmatch(job_start_time) is None
        or pids_match is None
        or shared not in {"OK", "YES", "USER", "1"}
    ):
        raise PlatformHealthError("Slurm job readback does not match candidate identity")
    tres = values.get("AllocTRES", "")
    return {
        "job_id": job_id,
        "job_start_time": job_start_time,
        "job_name": job_name,
        "sandbox": sandbox,
        "candidate_sha": candidate_sha,
        "account": values["Account"],
        "qos": values["QOS"],
        "user": user,
        "node": expected_node,
        "state": "RUNNING",
        "allocation": {
            "cpu_cores": int(values["NumCPUs"]),
            "memory_bytes": _memory_bytes(values.get("MinMemoryNode", "")),
            "pids": int(pids_match.group(1)),
            "gpu_count": _gpu_count(tres),
            "tres": tres,
            "gpu_detail": values.get("GresDetail", ""),
            "exclusive": False,
        },
    }


def _container_observations(
    candidates: Mapping[str, Mapping[str, str]],
    registry_snapshot: Mapping[str, Any],
    *,
    expected_node: str,
    expected_host: str,
    checkpoint: str,
    policy: Mapping[str, Any],
    run: Run,
) -> tuple[list[dict[str, Any]], list[str]]:
    environments = _registry_environments(registry_snapshot)
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
        raw_name = item.get("Name")
        raw_config = item.get("Config")
        raw_host_config = item.get("HostConfig")
        raw_state = item.get("State")
        raw_network_settings = item.get("NetworkSettings")
        if (
            not isinstance(raw_config, dict)
            or not isinstance(raw_host_config, dict)
            or not isinstance(raw_state, dict)
            or not isinstance(raw_network_settings, dict)
            or not isinstance(raw_name, str)
            or not raw_name.startswith("/")
            or SAFE_NAME_RE.fullmatch(raw_name[1:]) is None
        ):
            raise PlatformHealthError("Docker inspect result is incomplete")
        container_config: dict[str, Any] = raw_config
        host_config: dict[str, Any] = raw_host_config
        state: dict[str, Any] = raw_state
        network_settings: dict[str, Any] = raw_network_settings
        labels = container_config.get("Labels")
        if not isinstance(labels, dict) or not all(
            isinstance(key, str) and isinstance(value, str) for key, value in labels.items()
        ):
            raise PlatformHealthError("Docker labels are invalid")
        sandbox = labels.get("loom.sandbox", "")
        candidate_sha = labels.get("loom.candidate_sha", "")
        job_id = labels.get("loom.slurm_job_id", "")
        compose_project = labels.get("loom.compose_project", "")
        environment = environments.get(sandbox, {})
        registry_generation = str(registry_snapshot["generation"])
        registry_payload_sha256 = str(registry_snapshot["payload_sha256"])
        compose_label = labels.get("com.docker.compose.project", "")
        networks = network_settings.get("Networks")
        if (
            sandbox not in candidates
            or candidate_sha != candidates[sandbox]["sha"]
            or labels.get("loom.env_id") != environment.get("env_id")
            or labels.get("loom.resource_generation") != str(environment.get("resource_generation"))
            or labels.get("loom.candidate_id") != environment.get("candidate_id")
            or labels.get("loom.candidate_tree") != candidates[sandbox]["tree"]
            or labels.get("loom.registry_generation") != registry_generation
            or labels.get("loom.registry_payload_sha256") != registry_payload_sha256
            or JOB_ID_RE.fullmatch(job_id) is None
            or SAFE_NAME_RE.fullmatch(compose_project) is None
            or compose_label != compose_project
            or not isinstance(networks, dict)
            or not networks
            or any(
                not isinstance(name, str)
                or SAFE_NAME_RE.fullmatch(name) is None
                or not name.startswith(f"{compose_project}_")
                for name in networks
            )
        ):
            raise PlatformHealthError("Docker candidate labels are invalid")
        if state.get("Status") != "running":
            orphans.append(container_id)
            continue
        pid = state.get("Pid")
        if not isinstance(pid, int) or isinstance(pid, bool) or pid < 1:
            raise PlatformHealthError("Docker container PID is invalid")
        cgroup_parent = host_config.get("CgroupParent")
        if not isinstance(cgroup_parent, str):
            raise PlatformHealthError("Docker cgroup parent is invalid")
        observed_path = _proc_cgroup(pid)
        _container_cgroup_layout(observed_path, cgroup_parent)
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
                "name": raw_name[1:],
                "role": _container_role(labels),
                "sandbox": sandbox,
                "candidate_sha": candidate_sha,
                "job_id": job_id,
                "compose_project": compose_project,
                "identity_labels": {
                    "loom.sandbox": sandbox,
                    "loom.candidate_sha": candidate_sha,
                    "loom.slurm_job_id": job_id,
                    "loom.compose_project": compose_project,
                    "loom.env_id": str(environment["env_id"]),
                    "loom.resource_generation": str(environment["resource_generation"]),
                    "loom.candidate_id": str(environment["candidate_id"]),
                    "loom.candidate_tree": str(candidates[sandbox]["tree"]),
                    "loom.registry_generation": registry_generation,
                    "loom.registry_payload_sha256": registry_payload_sha256,
                },
                "compose_networks": sorted(networks),
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
            environment=environments[sandbox],
            job_id=job_id,
            expected_node=expected_node,
            run=run,
        )
        environment = environments[sandbox]
        slurm.update(
            {
                "env_id": environment["env_id"],
                "resource_generation": environment["resource_generation"],
                "candidate_id": environment["candidate_id"],
                "candidate_tree": candidates[sandbox]["tree"],
                "registry_generation": registry_snapshot["generation"],
                "registry_payload_sha256": registry_snapshot["payload_sha256"],
            },
        )
        slurm_pid_paths = _slurm_job_pid_cgroups(job_id, run=run)
        container_layouts = {
            _container_cgroup_layout(
                str(item["observed_cgroup_path"]),
                str(item["cgroup_parent"]),
            )
            for item in job_containers
        }
        if len(container_layouts) != 1:
            raise PlatformHealthError("candidate job container cgroup layout is not closed")
        layout_version, live_container_parent = next(iter(container_layouts))
        if layout_version == "cgroupfs-job-v1":
            job_path = str(job_containers[0]["cgroup_parent"])
        else:
            job_path = _slurm_job_root(slurm_pid_paths, job_id)
        if not _cgroup_binds_slurm_job(job_path, job_id) or any(
            not _strict_descendant(path, job_path) for path in slurm_pid_paths
        ):
            raise PlatformHealthError("Slurm job cgroup is not bound to its JobId")
        network_sets = [set(item["compose_networks"]) for item in job_containers]
        if any(not networks for networks in network_sets):
            raise PlatformHealthError("candidate compose network set is empty")
        compose_networks = sorted(set().union(*network_sets))
        controllers = sorted(_read_cgroup_limit(job_path, "cgroup.controllers").split())
        delegated_controllers = sorted(
            value.lstrip("+")
            for value in _read_cgroup_limit(job_path, "cgroup.subtree_control").split()
        )
        if not {"cpu", "memory", "pids"}.issubset(controllers) or not {
            "cpu",
            "memory",
            "pids",
        }.issubset(delegated_controllers):
            raise PlatformHealthError("Slurm job cgroup controllers are incomplete")
        raw_limits = {
            "cpu_max": _read_cgroup_limit(job_path, "cpu.max"),
            "memory_max": _read_cgroup_limit(job_path, "memory.max"),
            "memory_swap_max_source": _read_cgroup_limit(
                job_path,
                "memory.swap.max",
            ),
            "pids_max": _read_cgroup_limit(job_path, "pids.max"),
            "cpuset_cpus": _read_cgroup_limit(
                job_path,
                "cpuset.cpus.effective",
            ),
            "cpuset_mems": _read_cgroup_limit(
                job_path,
                "cpuset.mems.effective",
            ),
        }
        allocation = slurm["allocation"]
        if layout_version == "systemd-mirror-v1":
            expected_cluster = "trt-oldlab" if "oldlab" in expected_node.lower() else "trt-gb10"
            identity = {
                "cluster": expected_cluster,
                "node": expected_node.lower(),
                "job_id": job_id,
                "job_start_time": slurm["job_start_time"],
                "account": slurm["account"],
                "env_id": environment["env_id"],
                "resource_generation": environment["resource_generation"],
                "runtime_id": sandbox,
                "candidate_id": environment["candidate_id"],
                "candidate_sha": candidate_sha,
                "candidate_tree": candidates[sandbox]["tree"],
            }
            systemd_receipt = _load_systemd_slice_receipt(
                str(job_containers[0]["cgroup_parent"]),
                identity=identity,
                limits=raw_limits,
                expected_gpu_tres=(
                    str(allocation["tres"]) if policy["gpu_tres"] else "not-required"
                ),
                expected_gpu_detail=(
                    str(allocation["gpu_detail"]) if policy["gpu_tres"] else "not-required"
                ),
            )
            systemd_live = _systemd_slice_live_evidence(
                live_container_parent,
                receipt=systemd_receipt,
            )
        else:
            systemd_receipt = None
            systemd_live = None
        cgroup = {
            "layout_version": layout_version,
            "job_path": job_path,
            "container_parent": str(job_containers[0]["cgroup_parent"]),
            "slurm_job_id": job_id,
            "slurm_pid_cgroup_paths": sorted(slurm_pid_paths),
            "controllers": controllers,
            "delegated_controllers": delegated_controllers,
            "delegated": True,
            "cpu_cores_max": _cpu_limit(job_path),
            "memory_bytes_max": _integer_limit(job_path, "memory.max"),
            "pids_max": _integer_limit(job_path, "pids.max"),
            "pids_current": _integer_limit(job_path, "pids.current"),
            "systemd_slice_receipt": systemd_receipt,
            "systemd_slice_live": systemd_live,
        }
        totals = {
            "cpu_cores": sum(item["limits"]["cpu_cores"] for item in job_containers),
            "memory_bytes": sum(item["limits"]["memory_bytes"] for item in job_containers),
            "pids": sum(item["limits"]["pids"] for item in job_containers),
            "gpu_count": sum(item["limits"]["gpu_count"] for item in job_containers),
        }
        expected_gpu_count = 1 if policy["gpu_tres"] else 0
        if (
            allocation["cpu_cores"] != policy["requested_cpus"]
            or allocation["memory_bytes"] != policy["requested_memory_mib"] * 1024**2
            or allocation["pids"] != policy["job_pids_max"]
            or allocation["gpu_count"] != expected_gpu_count
            or any(
                item["limits"]["cpu_cores"] != policy["container_cpus"]
                or item["limits"]["memory_bytes"] != policy["container_memory_mib"] * 1024**2
                or item["limits"]["pids"] != policy["container_pids"]
                for item in job_containers
            )
            or cgroup["cpu_cores_max"] != allocation["cpu_cores"]
            or cgroup["memory_bytes_max"] != allocation["memory_bytes"]
            or cgroup["pids_max"] != allocation["pids"]
            or totals["cpu_cores"] > cgroup["cpu_cores_max"]
            or totals["cpu_cores"] > allocation["cpu_cores"]
            or totals["memory_bytes"] > cgroup["memory_bytes_max"]
            or totals["memory_bytes"] > allocation["memory_bytes"]
            or totals["pids"] > cgroup["pids_max"]
            or totals["pids"] > allocation["pids"]
            or totals["gpu_count"] > allocation["gpu_count"]
            or cgroup["cpu_cores_max"] > allocation["cpu_cores"]
            or cgroup["memory_bytes_max"] > allocation["memory_bytes"]
            or cgroup["pids_max"] > allocation["pids"]
            or cgroup["pids_current"] > cgroup["pids_max"]
        ):
            raise PlatformHealthError("candidate job container aggregate exceeds allocation")
        allocated_containers = [item for item in job_containers if item["limits"]["gpu_ids"]]
        denial_containers = [item for item in job_containers if not item["limits"]["gpu_ids"]]
        allocated_ids = sorted(
            {device for item in allocated_containers for device in item["limits"]["gpu_ids"]},
        )
        if policy["gpu_tres"]:
            slurm_allocated_ids = _gpu_detail_ids(str(allocation["gpu_detail"]))
            if (
                len(allocated_ids) != expected_gpu_count
                or not allocated_containers
                or not denial_containers
                or set(allocated_ids) != slurm_allocated_ids
            ):
                raise PlatformHealthError("GB10 GPU device assignment is not closed")
            gpu_query = "index" if all(device.isdigit() for device in allocated_ids) else "uuid"
            for container in allocated_containers:
                usable, observed_ids = _probe_command(
                    (
                        "/usr/bin/docker",
                        "exec",
                        container["container_id"],
                        "nvidia-smi",
                        f"--query-gpu={gpu_query}",
                        "--format=csv,noheader",
                    ),
                    run=run,
                )
                if not usable or sorted(observed_ids.splitlines()) != allocated_ids:
                    raise PlatformHealthError("allocated GB10 GPU is not usable")
            method = "docker-nvidia-smi-and-device-denial-v1"
        else:
            if (
                allocated_containers
                or allocated_ids
                or allocation["gpu_detail"] not in {"", "(null)", "None"}
            ):
                raise PlatformHealthError("OLDLAB unexpectedly exposes a GPU device")
            method = "docker-no-device-exposure-v1"
        for container in denial_containers:
            denied, _output = _probe_command(
                (
                    "/usr/bin/docker",
                    "exec",
                    container["container_id"],
                    "test",
                    "!",
                    "-e",
                    "/dev/nvidiactl",
                ),
                run=run,
            )
            if not denied:
                raise PlatformHealthError("unallocated GPU device exposure was not denied")
        jobs.append(
            {
                **slurm,
                "host": expected_host,
                "compose_project": compose_project,
                "compose_networks": compose_networks,
                "cgroup": cgroup,
                "containers": sorted(job_containers, key=lambda item: item["role"]),
                "aggregate_limits": totals,
                "device_probe": {
                    "method": method,
                    "allocated_ids": allocated_ids,
                    "all_allocated_usable": True,
                    "unallocated_denied": True,
                    "allocated_probe_container_ids": sorted(
                        item["container_id"] for item in allocated_containers
                    ),
                    "denial_probe_container_ids": sorted(
                        item["container_id"] for item in denial_containers
                    ),
                },
            },
        )
    for index, job in enumerate(jobs):
        for other in jobs[index + 1 :]:
            if job["compose_project"] == other["compose_project"] or set(
                job["compose_networks"]
            ) & set(other["compose_networks"]):
                raise PlatformHealthError("candidate compose identity is reused across jobs")
    return jobs, sorted(orphans)


def _terminal_jobs(
    candidates: Mapping[str, Mapping[str, str]],
    registry_snapshot: Mapping[str, Any],
    *,
    expected_node: str,
    since_at: str,
    run: Run,
) -> list[dict[str, Any]]:
    environments = _registry_environments(registry_snapshot)
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
            "--format=JobIDRaw,JobName,State,NodeList,Account,User,End,ElapsedRaw",
        ),
        run=run,
    ).decode(errors="strict")
    rows: list[dict[str, Any]] = []
    seen: set[str] = set()
    for line in raw.splitlines():
        fields = line.split("|")
        if len(fields) != 9 or fields[-1] != "":
            raise PlatformHealthError("Slurm accounting readback is malformed")
        job_id, name, state, node, account, user, ended_at, elapsed_raw = fields[:-1]
        if re.fullmatch(r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}", ended_at):
            ended_at += "Z"
        matches = [
            sandbox
            for sandbox in candidates
            if name.startswith(f"loom-{sandbox}-{candidates[sandbox]['sha'][:12]}-")
        ]
        if not matches:
            continue
        sandbox = matches[0]
        if (
            JOB_ID_RE.fullmatch(job_id) is None
            or node.lower() != expected_node.lower()
            or account != environments[sandbox]["slurm_account"]
            or user != environments[sandbox]["slurm_user"]
            or job_id in seen
            or not elapsed_raw.isdigit()
            or int(elapsed_raw) < 0
            or state.split("+", 1)[0]
            not in {"CANCELLED", "FAILED", "NODE_FAIL", "OUT_OF_MEMORY", "TIMEOUT", "COMPLETED"}
        ):
            raise PlatformHealthError("Slurm accounting identity is invalid")
        _timestamp(ended_at, label="Slurm terminal job end")
        seen.add(job_id)
        rows.append(
            {
                "job_id": job_id,
                "job_name": name,
                "state": state.split("+", 1)[0],
                "node": expected_node,
                "sandbox": sandbox,
                "candidate_sha": candidates[sandbox]["sha"],
                "ended_at": ended_at,
                "elapsed_seconds": int(elapsed_raw),
            },
        )
    return sorted(rows, key=lambda item: (item["job_id"], item["state"]))


def _non_loom_slurm_health(expected_node: str, *, run: Run) -> dict[str, Any]:
    raw = _run_bounded(
        (
            "/usr/bin/squeue",
            "--noheader",
            "--nodes",
            expected_node,
            "--states=RUNNING",
            "--format=%i|%u|%T",
        ),
        run=run,
    ).decode(errors="strict")
    job_ids: list[str] = []
    for line in raw.splitlines():
        fields = line.split("|")
        if len(fields) != 3:
            raise PlatformHealthError("non-Loom Slurm readback is malformed")
        job_id, user, state = fields
        if (
            JOB_ID_RE.fullmatch(job_id) is None
            or SAFE_NAME_RE.fullmatch(user) is None
            or state != "RUNNING"
        ):
            raise PlatformHealthError("non-Loom Slurm readback is invalid")
        if not user.startswith("loom-sandbox-"):
            job_ids.append(job_id)
    return {
        "controller_healthy": True,
        "running_job_count": len(job_ids),
        "running_job_ids": sorted(job_ids),
    }


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
    registry_snapshot = (
        _validated_registry_snapshot(request.get("registry_snapshot"))
        if isinstance(request, dict)
        else {}
    )
    sandboxes = _sandboxes(registry_snapshot)
    environments = _registry_environments(registry_snapshot)
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
        or not isinstance(request.get("expected_slurm_node"), str)
        or request.get("expected_slurm_node")
        != (
            request.get("expected_host")
            if str(request.get("expected_node")).startswith("oldlab-")
            else request.get("expected_node")
        )
        or not isinstance(request.get("expected_host"), str)
        or hostname() != request.get("expected_host")
        or not isinstance(candidates, dict)
        or set(candidates) != set(sandboxes)
        or any(
            not isinstance(candidates[sandbox], dict)
            or set(candidates[sandbox]) != {"sha", "tree"}
            or SHA_RE.fullmatch(str(candidates[sandbox]["sha"])) is None
            or SHA_RE.fullmatch(str(candidates[sandbox]["tree"])) is None
            or candidates[sandbox]["sha"] != environments[sandbox]["candidate_sha"]
            or candidates[sandbox]["tree"] != environments[sandbox]["candidate_tree"]
            for sandbox in sandboxes
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
    pool = "oldlab" if str(request["expected_node"]).startswith("oldlab-") else "gb10"
    policy = _load_capacity_policy(pool)
    jobs, orphans = _container_observations(
        candidates,
        registry_snapshot,
        expected_node=request["expected_slurm_node"],
        expected_host=request["expected_host"],
        checkpoint=request["checkpoint"],
        policy=policy["values"],
        run=run,
    )
    terminal = _terminal_jobs(
        candidates,
        registry_snapshot,
        expected_node=request["expected_slurm_node"],
        since_at=request["since_at"],
        run=run,
    )
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
        "non_loom_slurm": _non_loom_slurm_health(
            request["expected_slurm_node"],
            run=run,
        ),
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
        or any(
            not isinstance(job, dict)
            or set(job) != TERMINAL_JOB_FIELDS
            or JOB_ID_RE.fullmatch(str(job.get("job_id"))) is None
            or job.get("state")
            not in {"CANCELLED", "FAILED", "NODE_FAIL", "OUT_OF_MEMORY", "TIMEOUT", "COMPLETED"}
            or job.get("node") != request["expected_slurm_node"]
            or job.get("sandbox") not in request["candidates"]
            or job.get("candidate_sha")
            != request["candidates"].get(str(job.get("sandbox")), {}).get("sha")
            or not str(job.get("job_name", "")).startswith(
                f"loom-{job.get('sandbox')}-{str(job.get('candidate_sha', ''))[:12]}-",
            )
            or not isinstance(job.get("elapsed_seconds"), int)
            or isinstance(job.get("elapsed_seconds"), bool)
            or job["elapsed_seconds"] < 0
            for job in result["terminal_jobs"]
        )
        or len(
            {
                (job["job_id"], job["state"])
                for job in result["terminal_jobs"]
                if isinstance(job, dict)
            },
        )
        != len(result["terminal_jobs"])
        or not isinstance(result.get("non_loom_slurm"), dict)
        or set(result["non_loom_slurm"])
        != {"controller_healthy", "running_job_count", "running_job_ids"}
        or result["non_loom_slurm"]["controller_healthy"] is not True
        or not isinstance(result["non_loom_slurm"]["running_job_count"], int)
        or isinstance(result["non_loom_slurm"]["running_job_count"], bool)
        or result["non_loom_slurm"]["running_job_count"] < 0
        or not isinstance(result["non_loom_slurm"]["running_job_ids"], list)
        or len(result["non_loom_slurm"]["running_job_ids"])
        != result["non_loom_slurm"]["running_job_count"]
        or any(
            JOB_ID_RE.fullmatch(str(job_id)) is None
            for job_id in result["non_loom_slurm"]["running_job_ids"]
        )
        or not isinstance(result.get("orphan_container_ids"), list)
        or result["orphan_container_ids"]
    ):
        raise PlatformHealthError("node observation binding is invalid")
    observed = _timestamp(result["observed_at"], label="node observation")
    if any(
        _timestamp(job["ended_at"], label="terminal job end") > observed
        for job in result["terminal_jobs"]
    ):
        raise PlatformHealthError("terminal job observation is from the future")
    now = _now()
    if observed > now + timedelta(seconds=config.max_clock_skew_seconds):
        raise PlatformHealthError("node observation is from the future")


def _validate_mixed_job_cgroup(
    job: Mapping[str, Any],
    cgroup: Mapping[str, Any],
    containers: Sequence[Mapping[str, Any]],
    *,
    policy: Mapping[str, Any],
) -> None:
    expected_fields = {
        "layout_version",
        "job_path",
        "container_parent",
        "slurm_job_id",
        "slurm_pid_cgroup_paths",
        "controllers",
        "delegated_controllers",
        "delegated",
        "cpu_cores_max",
        "memory_bytes_max",
        "pids_max",
        "pids_current",
        "systemd_slice_receipt",
        "systemd_slice_live",
    }
    job_id = str(job.get("job_id"))
    job_path = str(cgroup.get("job_path"))
    layout = cgroup.get("layout_version")
    allocation = job.get("allocation")
    pid_paths = cgroup.get("slurm_pid_cgroup_paths")
    if (
        set(cgroup) != expected_fields
        or not isinstance(allocation, dict)
        or layout not in {"cgroupfs-job-v1", "systemd-mirror-v1"}
        or cgroup.get("slurm_job_id") != job_id
        or not _cgroup_binds_slurm_job(job_path, job_id)
        or cgroup.get("delegated") is not True
        or not {"cpu", "memory", "pids"}.issubset(
            set(cgroup.get("delegated_controllers", [])),
        )
        or not isinstance(pid_paths, list)
        or not pid_paths
        or any(
            not isinstance(path, str) or not _strict_descendant(path, job_path)
            for path in pid_paths
        )
        or cgroup.get("cpu_cores_max") != allocation.get("cpu_cores")
        or cgroup.get("memory_bytes_max") != allocation.get("memory_bytes")
        or cgroup.get("pids_max") != allocation.get("pids")
        or not isinstance(cgroup.get("pids_current"), int)
        or isinstance(cgroup.get("pids_current"), bool)
        or int(cgroup["pids_current"]) < 0
        or int(cgroup["pids_current"]) > int(cgroup["pids_max"])
    ):
        raise PlatformHealthError("mixed job cgroup evidence is invalid")
    if layout == "cgroupfs-job-v1":
        if (
            cgroup.get("container_parent") != job_path
            or cgroup.get("systemd_slice_receipt") is not None
            or cgroup.get("systemd_slice_live") is not None
            or any(
                _container_cgroup_layout(
                    str(item.get("observed_cgroup_path")),
                    str(item.get("cgroup_parent")),
                )
                != ("cgroupfs-job-v1", job_path)
                for item in containers
            )
        ):
            raise PlatformHealthError("cgroupfs job containment evidence is invalid")
        return

    receipt = cgroup.get("systemd_slice_receipt")
    live = cgroup.get("systemd_slice_live")
    expected_cluster = "trt-oldlab" if "oldlab" in str(job.get("node")).lower() else "trt-gb10"
    identity = {
        "cluster": expected_cluster,
        "node": str(job.get("node")).lower(),
        "job_id": job_id,
        "job_start_time": job.get("job_start_time"),
        "account": job.get("account"),
        "env_id": job.get("env_id"),
        "resource_generation": job.get("resource_generation"),
        "runtime_id": job.get("sandbox"),
        "candidate_id": job.get("candidate_id"),
        "candidate_sha": job.get("candidate_sha"),
        "candidate_tree": job.get("candidate_tree"),
    }
    if not isinstance(receipt, dict) or not isinstance(live, dict):
        raise PlatformHealthError("systemd mirror evidence is incomplete")
    receipt_limits = {
        key: str(receipt.get(key))
        for key in (
            "cpu_max",
            "memory_max",
            "memory_swap_max_source",
            "pids_max",
            "cpuset_cpus",
            "cpuset_mems",
        )
    }
    validated_receipt = _validated_systemd_slice_receipt_payload(
        receipt,
        identity=identity,
        limits=receipt_limits,
        expected_gpu_tres=(str(allocation.get("tres")) if policy["gpu_tres"] else "not-required"),
        expected_gpu_detail=(
            str(allocation.get("gpu_detail")) if policy["gpu_tres"] else "not-required"
        ),
    )
    expected_live_fields = {
        "path",
        "cpu_cores_max",
        "memory_bytes_max",
        "memory_swap_bytes_max",
        "pids_max",
        "cpuset_cpus",
        "cpuset_mems",
    }
    source_quota, source_period = _cpu_quota(str(validated_receipt["cpu_max"]))
    live_path = str(live.get("path"))
    if (
        set(live) != expected_live_fields
        or cgroup.get("container_parent") != validated_receipt["systemd_slice"]
        or not isinstance(live.get("cpu_cores_max"), int | float)
        or isinstance(live.get("cpu_cores_max"), bool)
        or float(live["cpu_cores_max"]) <= 0
        or float(live["cpu_cores_max"]) > source_quota / source_period
        or source_quota / source_period != float(cgroup["cpu_cores_max"])
        or not isinstance(live.get("memory_bytes_max"), int)
        or isinstance(live.get("memory_bytes_max"), bool)
        or int(live["memory_bytes_max"]) < 1
        or int(live["memory_bytes_max"]) > int(validated_receipt["memory_max"])
        or int(validated_receipt["memory_max"]) != int(cgroup["memory_bytes_max"])
        or not isinstance(live.get("memory_swap_bytes_max"), int)
        or isinstance(live.get("memory_swap_bytes_max"), bool)
        or int(live["memory_swap_bytes_max"]) < 0
        or int(live["memory_swap_bytes_max"]) > int(validated_receipt["memory_swap_max_effective"])
        or not isinstance(live.get("pids_max"), int)
        or isinstance(live.get("pids_max"), bool)
        or int(live["pids_max"]) < 1
        or int(live["pids_max"]) > int(validated_receipt["pids_max"])
        or int(validated_receipt["pids_max"]) != int(cgroup["pids_max"])
        or not isinstance(live.get("cpuset_cpus"), str)
        or not _cpuset_values(str(live["cpuset_cpus"])).issubset(
            _cpuset_values(str(validated_receipt["cpuset_cpus"])),
        )
        or not isinstance(live.get("cpuset_mems"), str)
        or not _cpuset_values(str(live["cpuset_mems"])).issubset(
            _cpuset_values(str(validated_receipt["cpuset_mems"])),
        )
        or any(
            _container_cgroup_layout(
                str(item.get("observed_cgroup_path")),
                str(item.get("cgroup_parent")),
            )
            != ("systemd-mirror-v1", live_path)
            for item in containers
        )
    ):
        raise PlatformHealthError("systemd mirror containment evidence is invalid")


def _verify_mixed_job_policy(
    job: Mapping[str, Any],
    *,
    policy: Mapping[str, Any],
) -> None:
    allocation = job.get("allocation")
    cgroup = job.get("cgroup")
    containers = job.get("containers")
    job_id = job.get("job_id")
    container_ids = (
        {str(item.get("container_id")) for item in containers if isinstance(item, dict)}
        if isinstance(containers, list)
        else set()
    )
    allocated_container_ids = (
        {
            str(item.get("container_id"))
            for item in containers
            if isinstance(item, dict) and item.get("limits", {}).get("gpu_ids")
        }
        if isinstance(containers, list)
        else set()
    )
    allocated_device_ids = (
        {
            str(device_id)
            for item in containers
            if isinstance(item, dict)
            for device_id in item.get("limits", {}).get("gpu_ids", [])
        }
        if isinstance(containers, list)
        else set()
    )
    if isinstance(cgroup, dict) and isinstance(containers, list):
        _validate_mixed_job_cgroup(
            job,
            cgroup,
            containers,
            policy=policy,
        )
    if (
        set(job)
        != {
            "job_id",
            "job_start_time",
            "job_name",
            "sandbox",
            "env_id",
            "resource_generation",
            "candidate_id",
            "candidate_sha",
            "candidate_tree",
            "registry_generation",
            "registry_payload_sha256",
            "account",
            "qos",
            "user",
            "node",
            "host",
            "state",
            "allocation",
            "compose_project",
            "compose_networks",
            "cgroup",
            "containers",
            "aggregate_limits",
            "device_probe",
        }
        or not isinstance(allocation, dict)
        or not isinstance(cgroup, dict)
        or not isinstance(containers, list)
        or JOB_ID_RE.fullmatch(str(job_id)) is None
        or SLURM_START_TIME_RE.fullmatch(str(job.get("job_start_time"))) is None
        or SAFE_NAME_RE.fullmatch(str(job.get("host"))) is None
        or allocation.get("cpu_cores") != policy["requested_cpus"]
        or allocation.get("memory_bytes") != policy["requested_memory_mib"] * 1024**2
        or allocation.get("pids") != policy["job_pids_max"]
        or allocation.get("gpu_count") != (1 if policy["gpu_tres"] else 0)
        or allocation.get("exclusive") is not False
        or len(containers) != len(ROLES)
        or {item.get("role") for item in containers if isinstance(item, dict)} != set(ROLES)
        or any(
            not isinstance(item, dict)
            or set(item)
            != {
                "container_id",
                "name",
                "role",
                "sandbox",
                "candidate_sha",
                "job_id",
                "compose_project",
                "identity_labels",
                "compose_networks",
                "pid",
                "cgroup_parent",
                "observed_cgroup_path",
                "limits",
            }
            or SAFE_NAME_RE.fullmatch(str(item.get("name"))) is None
            or item.get("identity_labels")
            != {
                "loom.sandbox": job.get("sandbox"),
                "loom.candidate_sha": job.get("candidate_sha"),
                "loom.slurm_job_id": job_id,
                "loom.compose_project": job.get("compose_project"),
                "loom.env_id": job.get("env_id"),
                "loom.resource_generation": str(job.get("resource_generation")),
                "loom.candidate_id": job.get("candidate_id"),
                "loom.candidate_tree": job.get("candidate_tree"),
                "loom.registry_generation": str(job.get("registry_generation")),
                "loom.registry_payload_sha256": job.get("registry_payload_sha256"),
            }
            or item.get("cgroup_parent") != cgroup["container_parent"]
            or item.get("limits", {}).get("cpu_cores") != policy["container_cpus"]
            or item.get("limits", {}).get("memory_bytes")
            != policy["container_memory_mib"] * 1024**2
            or item.get("limits", {}).get("pids") != policy["container_pids"]
            for item in containers
        )
        or not isinstance(job.get("device_probe"), dict)
        or set(job["device_probe"])
        != {
            "method",
            "allocated_ids",
            "all_allocated_usable",
            "unallocated_denied",
            "allocated_probe_container_ids",
            "denial_probe_container_ids",
        }
        or job["device_probe"]["method"]
        != (
            "docker-nvidia-smi-and-device-denial-v1"
            if policy["gpu_tres"]
            else "docker-no-device-exposure-v1"
        )
        or job["device_probe"]["all_allocated_usable"] is not True
        or job["device_probe"]["unallocated_denied"] is not True
        or not isinstance(job["device_probe"]["allocated_ids"], list)
        or set(job["device_probe"]["allocated_ids"]) != allocated_device_ids
        or (
            policy["gpu_tres"]
            and _gpu_detail_ids(str(allocation.get("gpu_detail"))) != allocated_device_ids
        )
        or (not policy["gpu_tres"] and allocation.get("gpu_detail") not in {"", "(null)", "None"})
        or len(job["device_probe"]["allocated_ids"]) != (1 if policy["gpu_tres"] else 0)
        or set(job["device_probe"]["allocated_probe_container_ids"]) != allocated_container_ids
        or set(job["device_probe"]["denial_probe_container_ids"])
        != container_ids - allocated_container_ids
        or len(job["device_probe"]["denial_probe_container_ids"])
        != (len(ROLES) - 1 if policy["gpu_tres"] else len(ROLES))
    ):
        raise PlatformHealthError("mixed job does not match its checked-in capacity policy")


def _exact_active_jobs(
    config: Config,
    nodes: Mapping[str, Any],
    candidates: Mapping[str, Any],
) -> list[Mapping[str, Any]]:
    """Verify and return one exact job per dynamic environment/pool pair."""

    if set(nodes) != set(config.nodes):
        raise PlatformHealthError("soak node inventory is incomplete")
    policies = {pool: _load_capacity_policy(pool)["values"] for pool in POOLS}
    oldlab_slurm_nodes = {config.host_aliases[node].lower() for node in config.oldlab_nodes}
    gb10_slurm_nodes = {node.lower() for node in config.capacity_gb10_nodes}
    jobs_with_node = [(node, job) for node in config.nodes for job in nodes[node]["active_jobs"]]
    jobs = [job for _node, job in jobs_with_node]
    combinations: set[tuple[str, str]] = set()
    for authority_node, job in jobs_with_node:
        observed_node = str(job.get("node")).lower()
        if observed_node in oldlab_slurm_nodes:
            pool = "oldlab"
        elif observed_node in gb10_slurm_nodes:
            pool = "gb10"
        else:
            raise PlatformHealthError("soak job uses an undeclared Slurm node")
        expected_slurm_node = (
            config.host_aliases[authority_node]
            if authority_node.startswith("oldlab-")
            else authority_node
        )
        if (
            observed_node != expected_slurm_node.lower()
            or job.get("host") != config.host_aliases[authority_node]
        ):
            raise PlatformHealthError("soak job is attached to the wrong node authority")
        _verify_mixed_job_policy(job, policy=policies[pool])
        combination = (str(job.get("sandbox")), pool)
        if combination in combinations:
            raise PlatformHealthError("soak repeats a sandbox/pool job")
        combinations.add(combination)
    expected = {(sandbox, pool) for sandbox in candidates for pool in POOLS}
    if combinations != expected or len(jobs) != len(expected):
        raise PlatformHealthError("soak does not contain the exact active environment jobs")
    return jobs


def _validate_trial_outcome(
    outcome: object,
    *,
    candidates: Mapping[str, Any],
    trial_batches: Mapping[tuple[str, str], str],
    observed_at: datetime,
) -> None:
    if not isinstance(outcome, dict):
        raise PlatformHealthError("platform-health trial outcome is invalid")
    sandbox = outcome.get("sandbox")
    attempt_count = outcome.get("attempt_count")
    expected_trial_count = outcome.get("expected_trial_count")
    state = outcome.get("state")
    terminal = state in {"succeeded", "failed", "cancelled"}
    batch_id = str(outcome.get("batch_id"))
    expected_pair = next(
        (pair for pair, expected_batch in trial_batches.items() if expected_batch == batch_id),
        None,
    )
    if (
        set(outcome) != TRIAL_OUTCOME_FIELDS
        or UUID_RE.fullmatch(str(outcome.get("trial_id"))) is None
        or UUID_RE.fullmatch(batch_id) is None
        or expected_pair is None
        or state not in {"queued", "claimed", "running", "succeeded", "failed", "cancelled"}
        or not isinstance(attempt_count, int)
        or isinstance(attempt_count, bool)
        or attempt_count < 0
        or not isinstance(expected_trial_count, int)
        or isinstance(expected_trial_count, bool)
        or expected_trial_count <= 0
        or outcome.get("retry_count") != max(attempt_count - 1, 0)
        or (
            terminal
            and (
                UUID_RE.fullmatch(str(outcome.get("worker_id"))) is None
                or JOB_ID_RE.fullmatch(str(outcome.get("slurm_job_id"))) is None
                or (sandbox, outcome.get("pool")) != expected_pair
                or outcome.get("candidate_sha") != candidates.get(str(sandbox), {}).get("sha")
            )
        )
    ):
        raise PlatformHealthError("platform-health trial outcome binding is invalid")
    batch_created_at = _timestamp(
        outcome["batch_created_at"],
        label="trial batch created_at",
    )
    if terminal:
        finished_at = _timestamp(outcome["finished_at"], label="trial outcome finished_at")
    elif outcome.get("finished_at") is not None:
        raise PlatformHealthError("nonterminal trial unexpectedly has a finish time")
    else:
        finished_at = None
    if batch_created_at > observed_at or (
        finished_at is not None and (finished_at < batch_created_at or finished_at > observed_at)
    ):
        raise PlatformHealthError("platform-health trial outcome is from the future")


def _validate_soak_sample(
    config: Config,
    sample: Mapping[str, Any],
    *,
    sequence: int,
    previous_sha256: str | None,
    candidates: Mapping[str, Any],
) -> None:
    fields = {
        "schema_version",
        "kind",
        "session_id",
        "sequence",
        "previous_sha256",
        "registry_snapshot",
        "candidates",
        "collector_host",
        "collection_started_at",
        "observed_at",
        "excluded_nodes",
        "nodes",
        "platform_health",
        "trial_batches",
        "trial_database_authorities",
        "trial_outcomes",
        "payload_sha256",
    }
    registry_snapshot = _validated_registry_snapshot(sample.get("registry_snapshot"))
    sandboxes = _sandboxes(registry_snapshot)
    environments = _registry_environments(registry_snapshot)
    if (
        set(sample) != fields
        or sample.get("schema_version") != SCHEMA_VERSION
        or sample.get("kind") != "loom.developer-sandbox.platform-health-soak-sample"
        or sample.get("sequence") != sequence
        or sample.get("previous_sha256") != previous_sha256
        or sample.get("candidates") != candidates
        or set(candidates) != set(sandboxes)
        or any(
            candidates[sandbox].get("sha") != environments[sandbox]["candidate_sha"]
            or candidates[sandbox].get("tree") != environments[sandbox]["candidate_tree"]
            for sandbox in sandboxes
        )
        or sample.get("collector_host") != config.collector_host
        or sample.get("excluded_nodes") != []
        or set(sample.get("nodes", {})) != set(config.nodes)
        or not isinstance(sample.get("platform_health"), dict)
        or not isinstance(sample.get("trial_batches"), list)
        or not isinstance(sample.get("trial_database_authorities"), list)
        or not isinstance(sample.get("trial_outcomes"), list)
        or sample.get("payload_sha256")
        != _digest({key: value for key, value in sample.items() if key != "payload_sha256"})
    ):
        raise PlatformHealthError("platform-health soak sample is invalid")
    _validate_platform_health_observation(sample["platform_health"])
    started = _timestamp(sample["collection_started_at"], label="soak sample start")
    observed = _timestamp(sample["observed_at"], label="soak sample")
    if observed < started or observed - started > timedelta(seconds=config.max_checkpoint_seconds):
        raise PlatformHealthError("platform-health soak sample window is invalid")
    trial_batch_rows = sample["trial_batches"]
    expected_pairs = {(sandbox, pool) for sandbox in sandboxes for pool in POOLS}
    if (
        len(trial_batch_rows) != len(expected_pairs)
        or any(
            not isinstance(row, dict)
            or set(row)
            != {
                "sandbox",
                "pool",
                "batch_id",
                "candidate_sha",
                "candidate_tree",
                "phase_started_at",
                "phase_completed_at",
            }
            or (row.get("sandbox"), row.get("pool")) not in expected_pairs
            or UUID_RE.fullmatch(str(row.get("batch_id"))) is None
            or row.get("candidate_sha") != candidates.get(str(row.get("sandbox")), {}).get("sha")
            or row.get("candidate_tree") != candidates.get(str(row.get("sandbox")), {}).get("tree")
            for row in trial_batch_rows
        )
        or {(row["sandbox"], row["pool"]) for row in trial_batch_rows if isinstance(row, dict)}
        != expected_pairs
        or len({row["batch_id"] for row in trial_batch_rows}) != len(trial_batch_rows)
    ):
        raise PlatformHealthError("platform-health soak trial-batch manifest is invalid")
    trial_batch_map = {
        (str(row["sandbox"]), str(row["pool"])): str(row["batch_id"]) for row in trial_batch_rows
    }
    trial_batch_windows = {
        str(row["batch_id"]): (
            _timestamp(row["phase_started_at"], label="soak batch phase start"),
            _timestamp(row["phase_completed_at"], label="soak batch phase completion"),
        )
        for row in trial_batch_rows
    }
    if any(
        not started_at <= completed_at for started_at, completed_at in trial_batch_windows.values()
    ):
        raise PlatformHealthError("platform-health soak trial-batch window is invalid")
    database_authorities = sample["trial_database_authorities"]
    if (
        len(database_authorities) != len(sandboxes)
        or {row.get("sandbox") for row in database_authorities if isinstance(row, dict)}
        != set(sandboxes)
        or any(
            not isinstance(row, dict)
            or set(row)
            != {
                "sandbox",
                "candidate_sha",
                "candidate_tree",
                "compose_project",
                "container_id",
                "compose_config_sha256",
                "created_at",
                "started_at",
                "lifecycle_updated_at",
                "desired_sha256",
                "lifecycle_sha256",
                "combined_receipt_sha256",
            }
            or row.get("candidate_sha") != candidates.get(str(row.get("sandbox")), {}).get("sha")
            or row.get("candidate_tree") != candidates.get(str(row.get("sandbox")), {}).get("tree")
            or row.get("compose_project")
            != environments[str(row.get("sandbox"))]["compose_project"]
            or CONTAINER_ID_RE.fullmatch(str(row.get("container_id"))) is None
            or any(
                DIGEST_RE.fullmatch(str(row.get(field))) is None
                for field in (
                    "compose_config_sha256",
                    "desired_sha256",
                    "lifecycle_sha256",
                    "combined_receipt_sha256",
                )
            )
            for row in database_authorities
        )
    ):
        raise PlatformHealthError("platform-health trial database authority is invalid")
    for row in database_authorities:
        database_created = _timestamp(
            row["created_at"],
            label="trial database created_at",
        )
        database_started = _timestamp(
            row["started_at"],
            label="trial database started_at",
        )
        lifecycle_updated = _timestamp(
            row["lifecycle_updated_at"],
            label="trial database lifecycle_updated_at",
        )
        if (
            database_created > database_started
            or database_started > lifecycle_updated
            or lifecycle_updated > observed
        ):
            raise PlatformHealthError("platform-health trial database generation is invalid")
    trial_ids: set[str] = set()
    for outcome in sample["trial_outcomes"]:
        _validate_trial_outcome(
            outcome,
            candidates=candidates,
            trial_batches=trial_batch_map,
            observed_at=observed,
        )
        batch_window = trial_batch_windows[str(outcome["batch_id"])]
        batch_created_at = _timestamp(
            outcome["batch_created_at"],
            label="trial batch created_at",
        )
        if not batch_window[0] <= batch_created_at <= batch_window[1]:
            raise PlatformHealthError("trial batch is outside the mixed-workload phase")
        trial_id = str(outcome["trial_id"])
        if trial_id in trial_ids:
            raise PlatformHealthError("platform-health trial outcome is duplicated")
        trial_ids.add(trial_id)
    for batch_id in trial_batch_windows:
        batch_outcomes = [
            outcome for outcome in sample["trial_outcomes"] if str(outcome["batch_id"]) == batch_id
        ]
        expected_counts = {int(outcome["expected_trial_count"]) for outcome in batch_outcomes}
        if len(expected_counts) > 1 or (
            expected_counts and len(batch_outcomes) > next(iter(expected_counts))
        ):
            raise PlatformHealthError("soak trial batch census is invalid")
    for node in config.nodes:
        request = {
            "session_id": sample["session_id"],
            "checkpoint": "mixed_non_loom",
            "checkpoint_group": "during",
            "expected_node": node,
            "expected_slurm_node": (
                config.host_aliases[node] if node.startswith("oldlab-") else node
            ),
            "expected_host": config.host_aliases[node],
            "registry_snapshot": registry_snapshot,
            "candidates": candidates,
        }
        _validate_node_result(sample["nodes"][node], request=request, config=config)
        node_observed = _timestamp(
            sample["nodes"][node]["observed_at"],
            label="soak node observation",
        )
        if (
            not started
            <= node_observed
            <= observed
            + timedelta(
                seconds=config.max_clock_skew_seconds,
            )
        ):
            raise PlatformHealthError("soak node observation is outside its sample window")
    _exact_active_jobs(config, sample["nodes"], candidates)


def _load_samples(
    config: Config,
    session_root: Path,
    *,
    session_id: str,
    candidates: Mapping[str, Any],
) -> list[dict[str, Any]]:
    samples: list[dict[str, Any]] = []
    previous: str | None = None
    sequence = 1
    while True:
        path = session_root / "samples" / f"{sequence:04d}.json"
        if not path.exists() and not path.is_symlink():
            break
        payload, raw = _secure_json(path, label="platform-health soak sample")
        if not isinstance(payload, dict) or raw != _canonical(payload):
            raise PlatformHealthError("platform-health soak sample encoding is invalid")
        _validate_soak_sample(
            config,
            payload,
            sequence=sequence,
            previous_sha256=previous,
            candidates=candidates,
        )
        samples.append(payload)
        previous = payload["payload_sha256"]
        sequence += 1
    if any(
        path.name.endswith(".json") and path.name != f"{index:04d}.json"
        for index, path in enumerate(
            sorted((session_root / "samples").glob("*.json")),
            start=1,
        )
    ):
        raise PlatformHealthError("platform-health soak sample set has a gap")
    return samples


def _verify_soak_samples(
    config: Config,
    samples: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    if len(samples) < SOAK_REQUIRED_SAMPLE_COUNT:
        raise PlatformHealthError("platform-health soak has too few samples")
    candidates = samples[0].get("candidates")
    if not isinstance(candidates, dict):
        raise PlatformHealthError("platform-health soak candidates are invalid")
    registry_snapshot = _validated_registry_snapshot(samples[0].get("registry_snapshot"))
    previous: str | None = None
    prior_observed: datetime | None = None
    trial_batches = samples[0].get("trial_batches")
    if not isinstance(trial_batches, list):
        raise PlatformHealthError("platform-health soak trial batches are invalid")
    for sequence, sample in enumerate(samples, start=1):
        _validate_soak_sample(
            config,
            sample,
            sequence=sequence,
            previous_sha256=previous,
            candidates=candidates,
        )
        previous = str(sample["payload_sha256"])
        current_observed = _timestamp(sample["observed_at"], label="soak sample")
        if sample.get("trial_batches") != trial_batches:
            raise PlatformHealthError("platform-health soak trial-batch manifest drifted")
        if sample.get("registry_snapshot") != registry_snapshot:
            raise PlatformHealthError("platform-health soak registry snapshot drifted")
        if prior_observed is not None and current_observed <= prior_observed:
            raise PlatformHealthError("platform-health soak sample time did not advance")
        prior_observed = current_observed
    started = _timestamp(samples[0]["observed_at"], label="soak first sample")
    completed = _timestamp(samples[-1]["observed_at"], label="soak final sample")
    duration = int((completed - started).total_seconds())
    if duration < SOAK_REQUIRED_DURATION_SECONDS:
        raise PlatformHealthError("platform-health soak is too short")
    pair_headroom: list[dict[str, Any]] = []
    sample_jobs = [_exact_active_jobs(config, sample["nodes"], candidates) for sample in samples]
    prior_outcomes: dict[str, Mapping[str, Any]] = {}
    for sample_index, sample in enumerate(samples):
        current_outcomes = {
            str(outcome["trial_id"]): outcome for outcome in sample["trial_outcomes"]
        }
        if any(
            trial_id not in current_outcomes
            or (
                outcome["state"] in {"succeeded", "failed", "cancelled"}
                and current_outcomes[trial_id] != outcome
            )
            for trial_id, outcome in prior_outcomes.items()
        ):
            raise PlatformHealthError("soak trial outcome accounting is not monotonic")
        active_before_outcome = {
            (
                str(job["sandbox"]),
                "oldlab" if "oldlab-" in str(job["node"]).lower() else "gb10",
                str(job["job_id"]),
            )
            for jobs in sample_jobs[: sample_index + 1]
            for job in jobs
        }
        for trial_id, outcome in current_outcomes.items():
            prior = prior_outcomes.get(trial_id)
            if outcome["state"] not in {
                "succeeded",
                "failed",
                "cancelled",
            } or (prior is not None and prior["state"] in {"succeeded", "failed", "cancelled"}):
                continue
            binding = (
                str(outcome["sandbox"]),
                str(outcome["pool"]),
                str(outcome["slurm_job_id"]),
            )
            if binding not in active_before_outcome:
                raise PlatformHealthError(
                    "soak trial outcome is not bound to an observed candidate job",
                )
        prior_outcomes = current_outcomes
    final_trial_outcomes = list(prior_outcomes.values())
    if any(
        outcome["state"] not in {"succeeded", "failed", "cancelled"}
        or outcome["finished_at"] is None
        for outcome in final_trial_outcomes
    ):
        raise PlatformHealthError("soak trial batch is not terminal-complete")
    final_batch_ids = {str(row["batch_id"]) for row in trial_batches if isinstance(row, dict)}
    for batch_id in final_batch_ids:
        batch_outcomes = [
            outcome for outcome in final_trial_outcomes if str(outcome["batch_id"]) == batch_id
        ]
        expected_counts = {int(outcome["expected_trial_count"]) for outcome in batch_outcomes}
        if len(expected_counts) != 1 or len(batch_outcomes) != next(iter(expected_counts)):
            raise PlatformHealthError("soak trial batch census is incomplete")
    trial_outcome_summaries: list[dict[str, Any]] = []
    for sandbox in candidates:
        for pool in POOLS:
            jobs = [
                job
                for jobs_in_sample in sample_jobs
                for job in jobs_in_sample
                if job["sandbox"] == sandbox
                and (("oldlab" if "oldlab-" in str(job["node"]).lower() else "gb10") == pool)
            ]
            if len(jobs) != len(samples):
                raise PlatformHealthError("soak pair sampling is incomplete")
            pair_outcomes = [
                outcome
                for outcome in final_trial_outcomes
                if outcome["sandbox"] == sandbox and outcome["pool"] == pool
            ]
            terminal_trial_count = len(pair_outcomes)
            if terminal_trial_count == 0:
                raise PlatformHealthError("soak trial success ratio has a zero denominator")
            succeeded_trial_count = sum(
                outcome["state"] == "succeeded" for outcome in pair_outcomes
            )
            failed_trial_count = sum(outcome["state"] == "failed" for outcome in pair_outcomes)
            cancelled_trial_count = sum(
                outcome["state"] == "cancelled" for outcome in pair_outcomes
            )
            pair_success_ratio = succeeded_trial_count / terminal_trial_count
            trial_outcome_summaries.append(
                {
                    "sandbox": sandbox,
                    "pool": pool,
                    "candidate_sha": candidates[sandbox]["sha"],
                    "candidate_tree": candidates[sandbox]["tree"],
                    "terminal_trial_count": terminal_trial_count,
                    "succeeded_trial_count": succeeded_trial_count,
                    "failed_trial_count": failed_trial_count,
                    "cancelled_trial_count": cancelled_trial_count,
                    "retried_trial_count": sum(
                        outcome["retry_count"] > 0 for outcome in pair_outcomes
                    ),
                    "retry_attempt_count": sum(outcome["retry_count"] for outcome in pair_outcomes),
                    "success_ratio": pair_success_ratio,
                },
            )
            node_by_sample = []
            for job in jobs:
                slurm_node = str(job["node"]).lower()
                node_by_sample.append(
                    next(
                        node
                        for node in config.nodes
                        if config.host_aliases[node].lower() == slurm_node
                        or node.lower() == slurm_node
                    ),
                )
            free_cpu_samples: list[float] = []
            for index, node in enumerate(node_by_sample[1:], start=1):
                current_capacity = samples[index]["nodes"][node]["capacity"]
                prior_capacity = samples[index - 1]["nodes"][node]["capacity"]
                total_delta = (
                    current_capacity["cpu_ticks_total"] - prior_capacity["cpu_ticks_total"]
                )
                idle_delta = current_capacity["cpu_ticks_idle"] - prior_capacity["cpu_ticks_idle"]
                if total_delta <= 0 or idle_delta < 0 or idle_delta > total_delta:
                    raise PlatformHealthError("soak CPU interval is invalid")
                free_cpu_samples.append(
                    current_capacity["cpu_cores_total"] * idle_delta / total_delta,
                )
            min_free_cpu = min(free_cpu_samples)
            min_free_memory = min(
                sample["nodes"][node]["capacity"]["memory_bytes_available"]
                for sample, node in zip(samples, node_by_sample, strict=True)
            )
            max_pid_ratio = max(
                job["cgroup"]["pids_current"] / job["cgroup"]["pids_max"] for job in jobs
            )
            if (
                min_free_cpu < GATE6_MINIMUM_FREE_CPU_CORES
                or min_free_memory < GATE6_MINIMUM_FREE_MEMORY_BYTES
                or max_pid_ratio > GATE6_MAXIMUM_PID_USAGE_RATIO
            ):
                raise PlatformHealthError("soak pair headroom left its reviewed envelope")
            pair_headroom.append(
                {
                    "sandbox": sandbox,
                    "pool": pool,
                    "min_free_cpu_cores": min_free_cpu,
                    "min_free_memory_bytes": min_free_memory,
                    "max_pid_usage_ratio": max_pid_ratio,
                    "observed_peak_concurrency": 1,
                    "within_reviewed_envelope": True,
                },
            )
    trial_success_numerator = sum(row["succeeded_trial_count"] for row in trial_outcome_summaries)
    trial_success_denominator = sum(row["terminal_trial_count"] for row in trial_outcome_summaries)
    if trial_success_denominator == 0:
        raise PlatformHealthError("soak trial success ratio has a zero denominator")
    trial_success_ratio = trial_success_numerator / trial_success_denominator
    if trial_success_ratio < SOAK_MINIMUM_TRIAL_SUCCESS_RATIO or any(
        row["success_ratio"] < SOAK_MINIMUM_TRIAL_SUCCESS_RATIO for row in trial_outcome_summaries
    ):
        raise PlatformHealthError("platform-health soak trial success ratio is below policy")
    return {
        "started_at": _iso(started),
        "completed_at": _iso(completed),
        "duration_seconds": duration,
        "sample_count": len(samples),
        "required_duration_seconds": SOAK_REQUIRED_DURATION_SECONDS,
        "required_sample_count": SOAK_REQUIRED_SAMPLE_COUNT,
        "workloads": list(GATE6_WORKLOADS),
        "trial_success_numerator": trial_success_numerator,
        "trial_success_denominator": trial_success_denominator,
        "trial_success_ratio": trial_success_ratio,
        "minimum_trial_success_ratio": SOAK_MINIMUM_TRIAL_SUCCESS_RATIO,
        "trial_outcomes": trial_outcome_summaries,
        "resource_envelope_breaches": 0,
        "kube_api_healthy": True,
        "minio_quorum_healthy": True,
        "longhorn_healthy": True,
        "non_loom_slurm_healthy": True,
        "pair_headroom": pair_headroom,
    }


def _device_isolation_rows(
    jobs: Sequence[Mapping[str, Any]],
    *,
    observed_at: str,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for job in jobs:
        pool = "oldlab" if "oldlab-" in str(job["node"]).lower() else "gb10"
        proof = job["device_probe"]
        rows.append(
            {
                "sandbox": job["sandbox"],
                "pool": pool,
                "job_id": job["job_id"],
                "node": job["node"],
                "host": job["host"],
                "allocated_ids": proof["allocated_ids"],
                "all_allocated_usable": proof["all_allocated_usable"],
                "unallocated_denied": proof["unallocated_denied"],
                "proof": {
                    "method": proof["method"],
                    "allocated_probe_container_ids": proof["allocated_probe_container_ids"],
                    "denial_probe_container_ids": proof["denial_probe_container_ids"],
                    "observed_at": observed_at,
                },
            },
        )
    return sorted(rows, key=lambda row: (row["sandbox"], row["pool"]))


def _verify_checkpoints(
    config: Config,
    receipts: Sequence[Mapping[str, Any]],
    *,
    require_complete: bool,
    samples: Sequence[Mapping[str, Any]] = (),
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
    prior_registry_snapshot: Mapping[str, Any] | None = None
    by_checkpoint: dict[str, Mapping[str, Any]] = {}
    receipt_fields = {
        "schema_version",
        "kind",
        "session_id",
        "sequence",
        "checkpoint",
        "checkpoint_group",
        "registry_snapshot",
        "candidates",
        "collector_host",
        "acceptance_checkpoint_times",
        "collection_started_at",
        "observed_at",
        "excluded_nodes",
        "nodes",
        "platform_health",
        "payload_sha256",
    }
    for sequence, receipt in enumerate(receipts, start=1):
        candidates = receipt.get("candidates")
        registry_snapshot = _validated_registry_snapshot(receipt.get("registry_snapshot"))
        sandboxes = _sandboxes(registry_snapshot)
        environments = _registry_environments(registry_snapshot)
        if (
            set(receipt) != receipt_fields
            or receipt.get("schema_version") != SCHEMA_VERSION
            or receipt.get("kind") != "loom.developer-sandbox.platform-health-checkpoint"
            or SESSION_RE.fullmatch(str(receipt.get("session_id"))) is None
            or receipt.get("sequence") != sequence
            or receipt.get("checkpoint_group")
            != CHECKPOINT_GROUPS.get(str(receipt.get("checkpoint")))
            or set(receipt.get("nodes", {})) != set(config.nodes)
            or receipt.get("excluded_nodes") != sorted(EXCLUDED_NODES)
            or receipt.get("collector_host") != config.collector_host
            or not isinstance(receipt.get("platform_health"), dict)
            or not isinstance(candidates, dict)
            or set(candidates) != set(sandboxes)
            or any(
                not isinstance(candidates[sandbox], dict)
                or set(candidates[sandbox]) != {"sha", "tree"}
                or SHA_RE.fullmatch(str(candidates[sandbox]["sha"])) is None
                or SHA_RE.fullmatch(str(candidates[sandbox]["tree"])) is None
                or candidates[sandbox]["sha"] != environments[sandbox]["candidate_sha"]
                or candidates[sandbox]["tree"] != environments[sandbox]["candidate_tree"]
                for sandbox in sandboxes
            )
            or (prior_candidates is not None and candidates != prior_candidates)
            or (
                prior_registry_snapshot is not None and registry_snapshot != prior_registry_snapshot
            )
            or receipt.get("payload_sha256")
            != _digest({key: value for key, value in receipt.items() if key != "payload_sha256"})
        ):
            raise PlatformHealthError("platform-health checkpoint receipt is invalid")
        _validate_platform_health_observation(receipt["platform_health"])
        acceptance_times = receipt["acceptance_checkpoint_times"]
        if not isinstance(acceptance_times, dict) or set(acceptance_times) != set(sandboxes):
            raise PlatformHealthError("acceptance checkpoint time binding is invalid")
        started = _timestamp(
            receipt["collection_started_at"],
            label="platform-health checkpoint start",
        )
        prior_candidates = candidates
        prior_registry_snapshot = registry_snapshot
        current = _timestamp(receipt.get("observed_at"), label="platform-health checkpoint")
        if (
            current < started
            or current - started > timedelta(seconds=config.max_checkpoint_seconds)
            or any(
                _timestamp(acceptance_times[sandbox], label="acceptance checkpoint") > started
                for sandbox in sandboxes
            )
        ):
            raise PlatformHealthError("platform-health checkpoint window is invalid")
        for node in config.nodes:
            request = {
                "session_id": receipt["session_id"],
                "checkpoint": receipt["checkpoint"],
                "checkpoint_group": receipt["checkpoint_group"],
                "expected_node": node,
                "expected_slurm_node": (
                    config.host_aliases[node] if node.startswith("oldlab-") else node
                ),
                "expected_host": config.host_aliases[node],
                "registry_snapshot": registry_snapshot,
                "candidates": candidates,
            }
            _validate_node_result(receipt["nodes"][node], request=request, config=config)
            node_observed = _timestamp(
                receipt["nodes"][node]["observed_at"],
                label="checkpoint node observation",
            )
            if (
                not started
                <= node_observed
                <= current
                + timedelta(
                    seconds=config.max_clock_skew_seconds,
                )
            ):
                raise PlatformHealthError(
                    "checkpoint node observation is outside its collection window",
                )
        if prior_time is not None and current <= prior_time:
            raise PlatformHealthError("platform-health checkpoint time did not advance")
        prior_time = current
        by_checkpoint[str(receipt["checkpoint"])] = receipt
    if not require_complete:
        return None
    baseline = by_checkpoint["baseline"]
    mixed = by_checkpoint["mixed_non_loom"]
    final = by_checkpoint["final_drain"]
    policies = {pool: _load_capacity_policy(pool) for pool in POOLS}
    active_mixed: list[Mapping[str, Any]] = []
    oldlab_free_cpu_cores: dict[str, float] = {}
    oldlab_slurm_nodes = {config.host_aliases[node].lower() for node in config.oldlab_nodes}
    gb10_slurm_nodes = {node.lower() for node in config.capacity_gb10_nodes}
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
        free_cpu_cores = mixed_node["capacity"]["cpu_cores_total"] * (1 - busy_ratio)
        if node in config.oldlab_nodes:
            oldlab_free_cpu_cores[node] = free_cpu_cores
        if node in config.oldlab_nodes and (
            busy_ratio > config.maximum_cpu_busy_ratio
            or free_cpu_cores < config.minimum_oldlab_free_cpu_cores
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
    active_mixed = list(
        _exact_active_jobs(config, mixed["nodes"], baseline["candidates"]),
    )
    compose_projects: set[str] = set()
    compose_networks: set[str] = set()
    for job in active_mixed:
        observed_slurm_node = str(job.get("node")).lower()
        if observed_slurm_node not in oldlab_slurm_nodes | gb10_slurm_nodes:
            raise PlatformHealthError("mixed job uses an undeclared Slurm node")
        pool = "oldlab" if observed_slurm_node in oldlab_slurm_nodes else "gb10"
        _verify_mixed_job_policy(job, policy=policies[pool]["values"])
        project = job.get("compose_project")
        networks = job.get("compose_networks")
        if (
            not isinstance(project, str)
            or SAFE_NAME_RE.fullmatch(project) is None
            or project in compose_projects
            or not isinstance(networks, list)
            or not networks
            or any(
                not isinstance(network, str)
                or SAFE_NAME_RE.fullmatch(network) is None
                or network in compose_networks
                for network in networks
            )
        ):
            raise PlatformHealthError("mixed jobs reuse a compose project or network")
        compose_projects.add(project)
        compose_networks.update(networks)
    combinations = {
        (
            job["sandbox"],
            "oldlab" if str(job["node"]).lower() in oldlab_slurm_nodes else "gb10",
        )
        for job in active_mixed
    }
    expected_combinations = {
        (sandbox, pool) for sandbox in baseline["candidates"] for pool in POOLS
    }
    if combinations != expected_combinations or len(active_mixed) != len(expected_combinations):
        raise PlatformHealthError("mixed workload does not contain one exact job per sandbox/pool")
    cleanup_specs = (
        ("cancellation", "cancel_cleanup", {"CANCELLED"}),
        ("ttl_expiry", "ttl_cleanup", {"TIMEOUT"}),
        (
            "worker_crash",
            "worker_crash",
            {"FAILED", "NODE_FAIL", "OUT_OF_MEMORY"},
        ),
        (
            "submit_host_restart",
            "submit_host_restart",
            {"CANCELLED", "FAILED", "COMPLETED"},
        ),
    )
    cleanup_rows: list[dict[str, Any]] = []
    terminal_by_event: dict[str, list[Mapping[str, Any]]] = {}
    for event, checkpoint, allowed_states in cleanup_specs:
        receipt = by_checkpoint[checkpoint]
        terminal = [
            item
            for node in config.nodes
            for item in receipt["nodes"][node]["terminal_jobs"]
            if item["state"] in allowed_states
        ]
        if not terminal:
            raise PlatformHealthError(f"{event} cleanup lacks root accounting evidence")
        observed = _timestamp(receipt["observed_at"], label=f"{event} cleanup observation")
        cleanup_seconds = max(
            int(
                (
                    observed - _timestamp(item.get("ended_at"), label=f"{event} terminal job")
                ).total_seconds()
            )
            for item in terminal
        )
        event_job_ids = {str(item["job_id"]) for item in terminal}
        remaining_jobs = [
            job
            for node in config.nodes
            for job in receipt["nodes"][node]["active_jobs"]
            if str(job.get("job_id")) in event_job_ids
        ]
        remaining_containers = [
            container for job in remaining_jobs for container in job.get("containers", [])
        ]
        if (
            cleanup_seconds < 0
            or cleanup_seconds > CLEANUP_MAX_SECONDS
            or remaining_jobs
            or remaining_containers
            or any(receipt["nodes"][node]["orphan_container_ids"] for node in config.nodes)
        ):
            raise PlatformHealthError(f"{event} cleanup did not converge safely")
        terminal_by_event[event] = terminal
        cleanup_rows.append(
            {
                "event": event,
                "checkpoint": checkpoint,
                "job_ids": sorted(event_job_ids),
                "terminal_states": sorted({str(item["state"]) for item in terminal}),
                "observed_within_seconds": cleanup_seconds,
                "maximum_cleanup_seconds": CLEANUP_MAX_SECONDS,
                "live_jobs": 0,
                "live_containers": 0,
                "durable_trial_state": True,
                "retryable_interrupted_trials": True,
                "observed_at": receipt["observed_at"],
            },
        )
    soak = _verify_soak_samples(config, samples)
    if any(sample.get("registry_snapshot") != baseline["registry_snapshot"] for sample in samples):
        raise PlatformHealthError("platform-health soak registry binding drifted")
    device_isolation = _device_isolation_rows(
        active_mixed,
        observed_at=mixed["observed_at"],
    )
    if any(
        baseline["nodes"][node]["capacity"]["cpu_cores_total"] < 20
        or baseline["nodes"][node]["capacity"]["memory_bytes_total"] < 115000 * 1024**2
        for node in config.capacity_gb10_nodes
    ):
        raise PlatformHealthError("GB10 reviewed shared slice lacks positive node headroom")
    completed_at = _timestamp(final["observed_at"], label="platform-health final checkpoint")
    minimum_oldlab_cpu = min(
        baseline["nodes"][node]["capacity"]["cpu_cores_total"] for node in config.oldlab_nodes
    )
    minimum_oldlab_memory = min(
        baseline["nodes"][node]["capacity"]["memory_bytes_total"] for node in config.oldlab_nodes
    )
    minimum_gb10_cpu = min(
        baseline["nodes"][node]["capacity"]["cpu_cores_total"]
        for node in config.capacity_gb10_nodes
    )
    minimum_gb10_memory = min(
        baseline["nodes"][node]["capacity"]["memory_bytes_total"]
        for node in config.capacity_gb10_nodes
    )
    oldlab_policy_values = dict(policies["oldlab"]["values"])
    gb10_policy_values = dict(policies["gb10"]["values"])
    oldlab_capacity = {
        **oldlab_policy_values,
        "minimum_node_cpu_cores": minimum_oldlab_cpu,
        "minimum_node_memory_bytes": minimum_oldlab_memory,
        "reserved_cpu_cores_per_node": config.minimum_oldlab_free_cpu_cores,
        "reserved_memory_mib_per_node": config.minimum_oldlab_free_memory_bytes // 1024**2,
    }
    gb10_capacity = {
        **gb10_policy_values,
        "minimum_node_cpu_cores": minimum_gb10_cpu,
        "minimum_node_memory_bytes": minimum_gb10_memory,
        "reserved_cpu_cores_per_node": minimum_gb10_cpu - gb10_policy_values["requested_cpus"],
        "reserved_memory_mib_per_node": (
            minimum_gb10_memory // 1024**2 - gb10_policy_values["requested_memory_mib"]
        ),
    }
    oldlab_recommendation = {
        "schema_version": SCHEMA_VERSION,
        "pool": "oldlab",
        "source": policies["oldlab"]["source"],
        "source_sha256": policies["oldlab"]["source_sha256"],
        "values": oldlab_capacity,
        "derivation": {
            "method": "installed-shared-capacity-policy-v1",
            "measured_node_count": len(config.oldlab_nodes),
            "minimum_observed_node_cpu_cores": minimum_oldlab_cpu,
            "minimum_observed_node_memory_bytes": minimum_oldlab_memory,
            "minimum_observed_free_cpu_cores": min(oldlab_free_cpu_cores.values()),
            "minimum_observed_free_memory_bytes": min(
                mixed["nodes"][node]["capacity"]["memory_bytes_available"]
                for node in config.oldlab_nodes
            ),
            "minimum_required_free_cpu_cores": config.minimum_oldlab_free_cpu_cores,
            "minimum_required_free_memory_bytes": config.minimum_oldlab_free_memory_bytes,
            "maximum_allowed_cpu_busy_ratio": config.maximum_cpu_busy_ratio,
            "all_nodes_passed": True,
        },
    }
    final_payload = {
        "schema_version": SCHEMA_VERSION,
        "kind": "loom.developer-sandbox.platform-health-evidence",
        "session_id": baseline["session_id"],
        "registry_snapshot": baseline["registry_snapshot"],
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
        "cancelled_jobs": terminal_by_event["cancellation"],
        "crashed_jobs": terminal_by_event["worker_crash"],
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
                "minimum_cpu_cores_available": (
                    mixed["nodes"][node]["capacity"]["cpu_cores_total"]
                    * (
                        mixed["nodes"][node]["capacity"]["cpu_ticks_idle"]
                        - baseline["nodes"][node]["capacity"]["cpu_ticks_idle"]
                    )
                    / (
                        mixed["nodes"][node]["capacity"]["cpu_ticks_total"]
                        - baseline["nodes"][node]["capacity"]["cpu_ticks_total"]
                    )
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
        "policy_capacity": {"oldlab": oldlab_capacity, "gb10": gb10_capacity},
        "oldlab_capacity_recommendation": oldlab_recommendation,
        "gate6_observations": {
            "soak": soak,
            "device_isolation": device_isolation,
            "cleanup": cleanup_rows,
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


def _recover_sample_transaction(
    config: Config,
    session_root: Path,
    session_id: str,
    candidates: Mapping[str, Any],
) -> None:
    journal_path = session_root / "sample-journal.json"
    if not journal_path.exists() and not journal_path.is_symlink():
        return
    payload, _raw = _secure_json(journal_path, label="platform-health sample transaction")
    fields = {
        "schema_version",
        "kind",
        "session_id",
        "sequence",
        "sample_path",
        "sample_sha256",
        "sample",
        "phase",
    }
    sample = payload.get("sample") if isinstance(payload, dict) else None
    sequence = payload.get("sequence") if isinstance(payload, dict) else None
    if (
        not isinstance(payload, dict)
        or set(payload) != fields
        or payload.get("schema_version") != SCHEMA_VERSION
        or payload.get("kind") != "loom.developer-sandbox.platform-health-soak-transaction"
        or payload.get("session_id") != session_id
        or not isinstance(sequence, int)
        or isinstance(sequence, bool)
        or sequence < 1
        or payload.get("sample_path") != str(session_root / "samples" / f"{sequence:04d}.json")
        or not isinstance(sample, dict)
        or sample.get("payload_sha256") != payload.get("sample_sha256")
        or payload.get("phase") not in {"prepared", "sample-written", "committed"}
    ):
        raise PlatformHealthError("platform-health sample recovery binding is invalid")
    existing = _load_samples(
        config,
        session_root,
        session_id=session_id,
        candidates=candidates,
    )
    if len(existing) not in {sequence - 1, sequence}:
        raise PlatformHealthError("platform-health sample transaction sequence drifted")
    previous = existing[sequence - 2]["payload_sha256"] if sequence > 1 else None
    _validate_soak_sample(
        config,
        sample,
        sequence=sequence,
        previous_sha256=previous,
        candidates=candidates,
    )
    if payload["phase"] == "prepared":
        _write_or_verify(Path(payload["sample_path"]), sample)
        payload["phase"] = "sample-written"
        _atomic_replace(journal_path, payload)
    if payload["phase"] == "sample-written":
        loaded = _load_samples(
            config,
            session_root,
            session_id=session_id,
            candidates=candidates,
        )
        if len(loaded) != sequence:
            raise PlatformHealthError("platform-health sample transaction did not commit")
        payload["phase"] = "committed"
        _atomic_replace(journal_path, payload)


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
        samples = _load_samples(
            config,
            session_root,
            session_id=session_id,
            candidates=receipt["candidates"],
        )
        final = _verify_checkpoints(
            config,
            receipts,
            require_complete=sequence == len(CHECKPOINTS),
            samples=samples,
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


def sample(
    config: Config,
    session_id: str,
    *,
    execute: bool,
    transport: Transport | None = None,
    platform_run: Run = subprocess.run,
    clock: Clock = _now,
    hostname: Callable[[], str] = _host,
) -> dict[str, Any]:
    """Append one real Gate-6 soak sample for the exact registry cohort."""

    _require_root()
    if (
        not execute
        or hostname() != config.collector_host
        or SESSION_RE.fullmatch(session_id) is None
    ):
        raise PlatformHealthError("platform-health soak sampling authority is invalid")
    state = _acceptance_state(config, session_id)
    _require_acceptance_phase(config, state, "mixed_non_loom")
    lock, session_root = _open_lock(config, session_id)
    try:
        _recover_transaction(config, session_root, session_id)
        _recover_sample_transaction(config, session_root, session_id, state["candidates"])
        receipts = _load_receipts(session_root)
        if tuple(row["checkpoint"] for row in receipts) != CHECKPOINTS[:2]:
            raise PlatformHealthError("soak sampling is outside the mixed-workload window")
        samples = _load_samples(
            config,
            session_root,
            session_id=session_id,
            candidates=state["candidates"],
        )
        sequence = len(samples) + 1
        started = clock().astimezone(UTC)
        nodes: dict[str, Any] = {}
        for node in config.nodes:
            request = _node_request(
                config,
                state=state,
                checkpoint="mixed_non_loom",
                node=node,
                since_at=receipts[0]["observed_at"],
            )
            result = (
                _transport_observation(
                    config,
                    node,
                    _request_envelope(request, node=node),
                )
                if transport is None
                else transport(node, _canonical(request))
            )
            _validate_node_result(result, request=request, config=config)
            nodes[node] = result
        health = _platform_health(config, run=platform_run)
        trial_batches = _soak_trial_batch_manifest(config, state)
        trial_outcomes, trial_database_authorities = _trial_outcomes(
            state["candidates"],
            state["registry_snapshot"],
            trial_batches,
            run=platform_run,
        )
        observed = clock().astimezone(UTC)
        sample_payload: dict[str, Any] = {
            "schema_version": SCHEMA_VERSION,
            "kind": "loom.developer-sandbox.platform-health-soak-sample",
            "session_id": session_id,
            "sequence": sequence,
            "previous_sha256": samples[-1]["payload_sha256"] if samples else None,
            "registry_snapshot": state["registry_snapshot"],
            "candidates": state["candidates"],
            "collector_host": config.collector_host,
            "collection_started_at": _iso(started),
            "observed_at": _iso(observed),
            "excluded_nodes": [],
            "nodes": nodes,
            "platform_health": health,
            "trial_batches": trial_batches,
            "trial_database_authorities": trial_database_authorities,
            "trial_outcomes": trial_outcomes,
        }
        sample_payload["payload_sha256"] = _digest(sample_payload)
        _validate_soak_sample(
            config,
            sample_payload,
            sequence=sequence,
            previous_sha256=sample_payload["previous_sha256"],
            candidates=state["candidates"],
        )
        destination = session_root / "samples" / f"{sequence:04d}.json"
        journal = {
            "schema_version": SCHEMA_VERSION,
            "kind": "loom.developer-sandbox.platform-health-soak-transaction",
            "session_id": session_id,
            "sequence": sequence,
            "sample_path": str(destination),
            "sample_sha256": sample_payload["payload_sha256"],
            "sample": sample_payload,
            "phase": "prepared",
        }
        journal_path = session_root / "sample-journal.json"
        _atomic_replace(journal_path, journal)
        _write_or_verify(destination, sample_payload)
        journal["phase"] = "sample-written"
        _atomic_replace(journal_path, journal)
        journal["phase"] = "committed"
        _atomic_replace(journal_path, journal)
        return {
            "schema_version": SCHEMA_VERSION,
            "kind": "loom.developer-sandbox.platform-health-soak-result",
            "session_id": session_id,
            "sequence": sequence,
            "sample_path": str(destination),
            "sample_sha256": sample_payload["payload_sha256"],
            "started_at": samples[0]["observed_at"] if samples else sample_payload["observed_at"],
            "observed_at": sample_payload["observed_at"],
            "sample_count": sequence,
        }
    finally:
        os.close(lock)


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
        _recover_sample_transaction(config, session_root, session_id, state["candidates"])
        receipts = _load_receipts(session_root)
        expected_sequence = len(receipts) + 1
        expected_checkpoint = (
            CHECKPOINTS[len(receipts)] if len(receipts) < len(CHECKPOINTS) else None
        )
        if checkpoint != expected_checkpoint:
            if checkpoint in CHECKPOINTS[: len(receipts)]:
                return receipts[CHECKPOINTS.index(checkpoint)]
            raise PlatformHealthError("platform-health checkpoint is not the exact next phase")
        samples = _load_samples(
            config,
            session_root,
            session_id=session_id,
            candidates=state["candidates"],
        )
        if checkpoint == "cancel_cleanup":
            _verify_soak_samples(config, samples)
        started = clock().astimezone(UTC)
        since_at = (
            receipts[-1]["observed_at"]
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
                since_at=since_at,
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
            "registry_snapshot": state["registry_snapshot"],
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
            samples=samples,
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
        _recover_sample_transaction(config, session_root, session_id, state["candidates"])
        receipts = _load_receipts(session_root)
        samples = _load_samples(
            config,
            session_root,
            session_id=session_id,
            candidates=state["candidates"],
        )
        final = _verify_checkpoints(
            config,
            receipts,
            require_complete=True,
            samples=samples,
        )
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
            or existing.get("registry_snapshot") != state["registry_snapshot"]
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
    sample_parser = subparsers.add_parser("sample", allow_abbrev=False)
    sample_parser.add_argument("--session-id", required=True)
    sample_parser.add_argument("--execute", action="store_true")
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
            elif args.command == "sample":
                result = sample(
                    config,
                    args.session_id,
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
