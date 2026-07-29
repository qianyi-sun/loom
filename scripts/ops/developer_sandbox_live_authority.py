#!/usr/bin/env python3
"""Produce root-owned, candidate-bound developer-sandbox overlap observations.

The ``collect`` command runs only on the fixed OLDLAB2 acceptance host.  It
combines the real shared-capacity adapter observation, sandbox lifecycle state,
Control Plane policy and active-job registry, fixed systemd readback, and one
controller-owned Slurm readback.  GB10 Slurm facts are obtained only through
the installed node-authority transport.

The ``observe-slurm-job`` command is an internal controller action.  It accepts
one closed canonical request on stdin and emits one sanitized canonical result;
the node authority is the only supported caller.
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
import urllib.error
import urllib.parse
import urllib.request
import uuid
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Final

SCHEMA_VERSION: Final = 1
REPO_ROOT: Final = Path(__file__).resolve().parents[2]
STATE_ROOT: Final = Path("/var/lib/loom-developer-sandbox-live-authority")
CAPACITY_ROOT: Final = Path("/var/lib/loom-shared-capacity/observations")
SANDBOX_ROOT: Final = Path("/srv/loom/developer-sandboxes")
ADAPTER_CONFIG_ROOT: Final = Path("/etc/loom/shared-capacity-adapters")
NODE_TRANSPORT: Final = Path("/usr/local/libexec/loom-developer-sandbox-node-transport")
LOCK: Final = STATE_ROOT / "authority.lock"
HIGH_WATER_ROOT: Final = STATE_ROOT / "high-water"
TRANSACTION_ROOT: Final = STATE_ROOT / "transactions"
OVERLAP_ROOT: Final = STATE_ROOT / "overlap"
COLLECT_HOST: Final = "trt-eai-oldlab-2"
SANDBOXES: Final = ("qianyi", "hongjian", "devansh")
POOLS: Final = ("oldlab", "gb10")
SOURCE_HOSTS: Final = {"oldlab": COLLECT_HOST, "gb10": "trt-gb10-1"}
SOURCE_NODES: Final = {"oldlab": "oldlab-2", "gb10": "trt-gb10-1"}
SOURCE_ALIASES: Final = {
    "oldlab": frozenset({"trt-eai-oldlab-2"}),
    "gb10": frozenset({"trt-gb10-1", "gx10-01c7"}),
}
SERVICE_USERS: Final = {
    "qianyi": "loom-sandbox-qianyi",
    "hongjian": "loom-sandbox-hongjian",
    "devansh": "loom-sandbox-devansh",
}
CONTROL_PLANE_URLS: Final = {
    "qianyi": "http://127.0.0.1:20080",
    "hongjian": "http://127.0.0.1:21080",
    "devansh": "http://127.0.0.1:22080",
}
EXPECTED_ADAPTER_FIELDS: Final = {
    "schema_version",
    "sandbox",
    "environment",
    "pool_name",
    "control_plane_url",
    "admin_secret_file",
    "handoff_path",
    "observation_path",
    "adapter_state_path",
    "sandbox_state_path",
    "runtime_attestation_root",
    "max_slots_bound",
    "timeout_seconds",
}
CAPACITY_FIELDS: Final = {
    "sandbox",
    "pool_name",
    "candidate_sha",
    "request_id",
    "lease_epoch",
    "capacity_lease_state",
    "observed_at",
    "observation_sequence",
    "pending_slots",
    "active_slots",
    "draining_slots",
    "terminal_slots",
    "payload_sha256",
}
SANDBOX_STATE_FIELDS: Final = {
    "schema_version",
    "sandbox",
    "compose_project",
    "candidate_sha",
    "candidate_tree",
    "source_repo",
    "updated_at",
}
COLLECTION_FIELDS: Final = {
    "schema_version",
    "kind",
    "collection_id",
    "candidate_tree",
    "job_id",
}
SLURM_REQUEST_FIELDS: Final = {
    "schema_version",
    "kind",
    "source_host",
    "sandbox",
    "pool",
    "candidate_sha",
    "candidate_tree",
    "job_id",
    "account",
    "user",
    "job_name",
    "node",
    "requested_cpus",
    "requested_memory_mib",
    "job_pids_max",
    "requested_gpus",
    "requested_gpu_tres",
}
SLURM_RESULT_FIELDS: Final = {
    "schema_version",
    "kind",
    "source_host",
    "sandbox",
    "pool",
    "candidate_sha",
    "candidate_tree",
    "job_id",
    "account",
    "user",
    "job_name",
    "node",
    "state",
    "allocation",
    "observed_at",
}
JOB_ID_RE: Final = re.compile(r"^[1-9][0-9]*(?:_[0-9]+)?$")
SHA_RE: Final = re.compile(r"^[0-9a-f]{40}$")
DIGEST_RE: Final = re.compile(r"^[0-9a-f]{64}$")
SAFE_JOB_RE: Final = re.compile(r"^[A-Za-z0-9_.-]{1,128}$")
MAX_FILE_BYTES: Final = 8 * 1024 * 1024
MAX_CAPACITY_AGE: Final = timedelta(seconds=120)
MAX_CLOCK_SKEW: Final = timedelta(seconds=5)
MAX_COLLECTION_SPAN: Final = timedelta(seconds=30)
REQUIRED_UID = 0
REQUIRED_GID = 0


class LiveAuthorityError(RuntimeError):
    """A bounded, secret-safe live-authority failure."""


@dataclass(frozen=True, slots=True)
class AdapterConfig:
    sandbox: str
    environment: str
    pool: str
    control_plane_url: str
    admin_secret_file: Path
    observation_path: Path
    sandbox_state_path: Path
    max_slots_bound: int
    timeout_seconds: float


Run = Callable[..., subprocess.CompletedProcess[str]]
HttpJson = Callable[..., dict[str, Any]]
Clock = Callable[[], datetime]
Transport = Callable[[str, bytes], dict[str, Any]]


def _canonical(value: Any) -> bytes:
    return (json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n").encode()


def _digest(value: Any) -> str:
    return hashlib.sha256(_canonical(value)).hexdigest()


def _host() -> str:
    return socket.gethostname().split(".", 1)[0].lower()


def _now() -> datetime:
    return datetime.now(UTC)


def _timestamp(value: object, *, label: str) -> datetime:
    if not isinstance(value, str):
        raise LiveAuthorityError(f"{label} timestamp is invalid")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise LiveAuthorityError(f"{label} timestamp is invalid") from exc
    if parsed.tzinfo is None:
        raise LiveAuthorityError(f"{label} timestamp is invalid")
    return parsed.astimezone(UTC)


def _iso(value: datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _require_root() -> None:
    if os.getuid() != REQUIRED_UID or os.geteuid() != REQUIRED_UID:
        raise LiveAuthorityError("live authority requires root")


def _read_secure_bytes(path: Path, *, label: str, mode: int = 0o600) -> bytes:
    if not path.is_absolute():
        raise LiveAuthorityError(f"{label} path is invalid")
    descriptor = os.open("/", os.O_RDONLY | os.O_DIRECTORY)
    try:
        for part in path.parts[1:-1]:
            child = os.open(
                part,
                os.O_RDONLY
                | os.O_DIRECTORY
                | getattr(os, "O_CLOEXEC", 0)
                | getattr(os, "O_NOFOLLOW", 0),
                dir_fd=descriptor,
            )
            os.close(descriptor)
            descriptor = child
            metadata = os.fstat(descriptor)
            if (
                not stat.S_ISDIR(metadata.st_mode)
                or metadata.st_uid != REQUIRED_UID
                or metadata.st_gid != REQUIRED_GID
                or stat.S_IMODE(metadata.st_mode) & 0o022
            ):
                raise LiveAuthorityError(f"{label} directory is unsafe")
        leaf = path.name
        before = os.stat(leaf, dir_fd=descriptor, follow_symlinks=False)
        file_descriptor = os.open(
            leaf,
            os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0),
            dir_fd=descriptor,
        )
        try:
            opened = os.fstat(file_descriptor)
            if (
                not stat.S_ISREG(opened.st_mode)
                or opened.st_uid != REQUIRED_UID
                or opened.st_gid != REQUIRED_GID
                or opened.st_nlink != 1
                or stat.S_IMODE(opened.st_mode) != mode
                or (opened.st_dev, opened.st_ino) != (before.st_dev, before.st_ino)
            ):
                raise LiveAuthorityError(f"{label} file is unsafe")
            chunks: list[bytes] = []
            remaining = MAX_FILE_BYTES + 1
            while remaining:
                chunk = os.read(file_descriptor, remaining)
                if not chunk:
                    break
                chunks.append(chunk)
                remaining -= len(chunk)
            raw = b"".join(chunks)
            if len(raw) > MAX_FILE_BYTES:
                raise LiveAuthorityError(f"{label} file exceeds its size bound")
            after = os.stat(leaf, dir_fd=descriptor, follow_symlinks=False)
            if (
                (after.st_dev, after.st_ino) != (opened.st_dev, opened.st_ino)
                or after.st_size != opened.st_size
                or after.st_mtime_ns != opened.st_mtime_ns
            ):
                raise LiveAuthorityError(f"{label} file changed during read")
            return raw
        finally:
            os.close(file_descriptor)
    except LiveAuthorityError:
        raise
    except OSError as exc:
        raise LiveAuthorityError(f"{label} file is unavailable") from exc
    finally:
        os.close(descriptor)


def _secure_json(path: Path, *, label: str) -> tuple[Any, bytes]:
    raw = _read_secure_bytes(path, label=label)
    try:
        payload = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise LiveAuthorityError(f"{label} file is invalid") from exc
    if raw != _canonical(payload):
        raise LiveAuthorityError(f"{label} file is not canonical")
    return payload, raw


def _load_adapter_config(sandbox: str, pool: str) -> AdapterConfig:
    path = ADAPTER_CONFIG_ROOT / f"{sandbox}-{pool}.toml"
    raw = _read_secure_bytes(path, label="adapter config")
    try:
        payload = tomllib.loads(raw.decode())
    except (UnicodeDecodeError, tomllib.TOMLDecodeError) as exc:
        raise LiveAuthorityError("adapter config is invalid") from exc
    expected = {
        "schema_version": SCHEMA_VERSION,
        "sandbox": sandbox,
        "environment": f"sandbox-{sandbox}",
        "pool_name": pool,
        "control_plane_url": CONTROL_PLANE_URLS[sandbox],
        "admin_secret_file": str(SANDBOX_ROOT / sandbox / "secrets/admin.toml"),
        "handoff_path": f"/var/lib/loom-shared-capacity/handoffs/current/{sandbox}-{pool}.json",
        "observation_path": str(CAPACITY_ROOT / f"{sandbox}-{pool}.json"),
        "adapter_state_path": f"/var/lib/loom-shared-capacity/adapters/{sandbox}-{pool}.json",
        "sandbox_state_path": str(SANDBOX_ROOT / sandbox / "sandbox-state.json"),
        "runtime_attestation_root": "/var/lib/loom-shared-capacity/runtime-attestations",
        "max_slots_bound": 20 if pool == "oldlab" else 120,
        "timeout_seconds": 10,
    }
    if set(payload) != EXPECTED_ADAPTER_FIELDS or payload != expected:
        raise LiveAuthorityError("adapter config differs from the fixed live authority")
    return AdapterConfig(
        sandbox=sandbox,
        environment=f"sandbox-{sandbox}",
        pool=pool,
        control_plane_url=CONTROL_PLANE_URLS[sandbox],
        admin_secret_file=SANDBOX_ROOT / sandbox / "secrets/admin.toml",
        observation_path=CAPACITY_ROOT / f"{sandbox}-{pool}.json",
        sandbox_state_path=SANDBOX_ROOT / sandbox / "sandbox-state.json",
        max_slots_bound=20 if pool == "oldlab" else 120,
        timeout_seconds=10.0,
    )


def _load_admin_token(path: Path) -> str:
    raw = _read_secure_bytes(path, label="admin secret")
    try:
        payload = tomllib.loads(raw.decode())
    except (UnicodeDecodeError, tomllib.TOMLDecodeError) as exc:
        raise LiveAuthorityError("admin secret file is invalid") from exc
    admin = payload.get("admin")
    token = admin.get("token") if isinstance(admin, dict) else None
    if not isinstance(token, str) or not token.strip():
        raise LiveAuthorityError("admin secret file is invalid")
    return token.strip()


def _http_json(
    *,
    base_url: str,
    token: str,
    path: str,
    timeout: float,
) -> dict[str, Any]:
    request = urllib.request.Request(
        base_url.rstrip("/") + path,
        headers={"Authorization": f"Bearer {token}"},
        method="GET",
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            raw = response.read(MAX_FILE_BYTES + 1)
    except (urllib.error.HTTPError, urllib.error.URLError, TimeoutError) as exc:
        raise LiveAuthorityError("Control Plane readback failed safely") from exc
    if len(raw) > MAX_FILE_BYTES:
        raise LiveAuthorityError("Control Plane readback exceeds its size bound")
    try:
        payload = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise LiveAuthorityError("Control Plane readback is invalid") from exc
    if not isinstance(payload, dict):
        raise LiveAuthorityError("Control Plane readback is invalid")
    return payload


def _capacity_observation(
    config: AdapterConfig,
    *,
    candidate_sha: str,
    now: datetime,
) -> tuple[dict[str, Any], bytes]:
    document, raw = _secure_json(config.observation_path, label="capacity observation")
    if not isinstance(document, list) or len(document) != 1 or not isinstance(document[0], dict):
        raise LiveAuthorityError("capacity observation must contain exactly one row")
    row = document[0]
    unsigned = dict(row)
    payload_digest = unsigned.pop("payload_sha256", None)
    counters = ("pending_slots", "active_slots", "draining_slots", "terminal_slots")
    try:
        uuid.UUID(str(row.get("request_id")))
    except (ValueError, AttributeError) as exc:
        raise LiveAuthorityError("capacity observation request identity is invalid") from exc
    if (
        set(row) != CAPACITY_FIELDS
        or row.get("sandbox") != config.sandbox
        or row.get("pool_name") != config.pool
        or row.get("candidate_sha") != candidate_sha
        or row.get("capacity_lease_state") != "active"
        or not isinstance(row.get("lease_epoch"), int)
        or isinstance(row.get("lease_epoch"), bool)
        or row["lease_epoch"] < 1
        or not isinstance(row.get("observation_sequence"), int)
        or isinstance(row.get("observation_sequence"), bool)
        or row["observation_sequence"] < 1
        or any(
            not isinstance(row.get(field), int) or isinstance(row[field], bool) or row[field] < 0
            for field in counters
        )
        or row["active_slots"] < 1
        or payload_digest
        != hashlib.sha256(
            json.dumps(unsigned, sort_keys=True, separators=(",", ":")).encode(),
        ).hexdigest()
    ):
        raise LiveAuthorityError("capacity observation binding is invalid")
    observed = _timestamp(row["observed_at"], label="capacity observation")
    if observed > now + MAX_CLOCK_SKEW or now - observed > MAX_CAPACITY_AGE:
        raise LiveAuthorityError("capacity observation is stale or future-dated")
    return row, raw


def _sandbox_state(
    config: AdapterConfig,
    *,
    candidate_sha: str,
    candidate_tree: str,
) -> tuple[dict[str, Any], bytes]:
    payload, raw = _secure_json(config.sandbox_state_path, label="sandbox lifecycle state")
    if (
        not isinstance(payload, dict)
        or set(payload) != SANDBOX_STATE_FIELDS
        or payload.get("schema_version") != SCHEMA_VERSION
        or payload.get("sandbox") != config.sandbox
        or payload.get("compose_project") != f"loom-sandbox-{config.sandbox}"
        or payload.get("candidate_sha") != candidate_sha
        or payload.get("candidate_tree") != candidate_tree
        or not isinstance(payload.get("source_repo"), str)
        or not Path(payload["source_repo"]).is_absolute()
    ):
        raise LiveAuthorityError("sandbox lifecycle state binding is invalid")
    _timestamp(payload["updated_at"], label="sandbox lifecycle state")
    return payload, raw


def _capacity_binding(policy: Mapping[str, Any], capacity: Mapping[str, Any]) -> Mapping[str, Any]:
    binding = policy.get("capacity_lease_state")
    actuator = policy.get("actuator_config")
    if (
        policy.get("environment") != f"sandbox-{capacity['sandbox']}"
        or policy.get("pool_name") != capacity["pool_name"]
        or policy.get("actuator") != "slurm"
        or policy.get("enabled") is not True
        or not isinstance(policy.get("max_slots"), int)
        or isinstance(policy.get("max_slots"), bool)
        or policy["max_slots"] < 1
        or not isinstance(actuator, dict)
        or actuator.get("shared_capacity_managed") is not True
        or actuator.get("candidate_sha") != capacity["candidate_sha"]
        or actuator.get("slurm_account") != f"loom-dev-{capacity['sandbox']}"
        or actuator.get("exclusive") is not False
        or not isinstance(binding, dict)
        or binding.get("schema_version") != SCHEMA_VERSION
        or binding.get("state") != "active"
        or binding.get("request_id") != capacity["request_id"]
        or binding.get("lease_epoch") != capacity["lease_epoch"]
        or binding.get("candidate_sha") != capacity["candidate_sha"]
        or binding.get("preemptible") is not True
    ):
        raise LiveAuthorityError("Control Plane capacity policy binding is invalid")
    return actuator


def _active_job(
    payload: Mapping[str, Any],
    *,
    config: AdapterConfig,
    candidate_sha: str,
    actuator: Mapping[str, Any],
    job_id: str,
) -> dict[str, Any]:
    jobs = payload.get("jobs")
    if not isinstance(jobs, list):
        raise LiveAuthorityError("Control Plane active-job registry is invalid")
    matches = [
        job
        for job in jobs
        if isinstance(job, dict)
        and job.get("environment") == config.environment
        and job.get("pool_name") == config.pool
        and job.get("sandbox_identity") == config.environment
        and job.get("candidate_sha") == candidate_sha
        and str(job.get("job_id")) == job_id
        and job.get("state") == "running"
        and job.get("slurm_state") == "RUNNING"
        and job.get("job_id") is not None
    ]
    if len(matches) != 1:
        raise LiveAuthorityError("Control Plane active job is unavailable or ambiguous")
    job = matches[0]
    node = job.get("nodelist")
    job_id = str(job.get("job_id"))
    allowed_nodes = actuator.get("allowed_nodes")
    requested_gpu_tres = job.get("requested_gpu_tres")
    requested_gpus = job.get("requested_gpus")
    if (
        JOB_ID_RE.fullmatch(job_id) is None
        or not isinstance(node, str)
        or not isinstance(allowed_nodes, list)
        or node not in allowed_nodes
        or (config.pool == "oldlab" and not node.startswith("trt-eai-oldlab-"))
        or (config.pool == "gb10" and not node.startswith("trt-gb10-"))
        or not isinstance(job.get("requested_cpus"), int)
        or isinstance(job.get("requested_cpus"), bool)
        or job["requested_cpus"] < 1
        or not isinstance(job.get("requested_memory_mib"), int)
        or isinstance(job.get("requested_memory_mib"), bool)
        or job["requested_memory_mib"] < 1
        or not isinstance(job.get("requested_concurrency"), int)
        or isinstance(job.get("requested_concurrency"), bool)
        or job["requested_concurrency"] < 1
        or not isinstance(requested_gpus, int)
        or isinstance(requested_gpus, bool)
        or requested_gpus < 0
        or (
            requested_gpu_tres is not None
            and (not isinstance(requested_gpu_tres, str) or not requested_gpu_tres)
        )
        or job.get("submission_error") is not None
    ):
        raise LiveAuthorityError("Control Plane active-job identity is invalid")
    expected_name = (
        f"loom-{config.environment}-{candidate_sha[:12]}-"
        f"{re.sub(r'[^A-Za-z0-9_.-]+', '-', node).strip('-') or 'worker'}"
    )[:128]
    expected_project = f"loom-{config.environment}-{candidate_sha[:12]}-{job_id}"
    if job.get("compose_project") != expected_project:
        raise LiveAuthorityError("Control Plane active-job compose identity is invalid")
    return {
        "job_id": job_id,
        "node": node,
        "job_name": expected_name,
        "requested_cpus": job["requested_cpus"],
        "requested_memory_mib": job["requested_memory_mib"],
        "requested_concurrency": job["requested_concurrency"],
        "requested_gpus": requested_gpus,
        "requested_gpu_tres": requested_gpu_tres or "",
    }


def _service_readback(
    sandbox: str,
    candidate_sha: str,
    candidate_tree: str,
    *,
    run: Run = subprocess.run,
    clock: Clock = _now,
) -> dict[str, Any]:
    unit = f"loom-developer-sandbox-{sandbox}.service"
    try:
        completed = run(
            (
                "/usr/bin/systemctl",
                "show",
                unit,
                "--no-pager",
                "--property=Id",
                "--property=LoadState",
                "--property=ActiveState",
                "--property=SubState",
            ),
            check=False,
            capture_output=True,
            text=True,
            timeout=10,
            env={"PATH": "/usr/sbin:/usr/bin:/sbin:/bin", "LANG": "C", "LC_ALL": "C"},
        )
    except (OSError, subprocess.SubprocessError) as exc:
        raise LiveAuthorityError("systemd service readback failed safely") from exc
    if completed.returncode != 0 or completed.stderr or len(completed.stdout) > 4096:
        raise LiveAuthorityError("systemd service readback failed safely")
    fields: dict[str, str] = {}
    for line in completed.stdout.splitlines():
        key, separator, value = line.partition("=")
        if not separator or key in fields:
            raise LiveAuthorityError("systemd service readback is malformed")
        fields[key] = value
    if fields != {
        "Id": unit,
        "LoadState": "loaded",
        "ActiveState": "active",
        "SubState": "running",
    }:
        raise LiveAuthorityError("sandbox service is not exactly active")
    return {
        "sandbox": sandbox,
        "candidate_sha": candidate_sha,
        "candidate_tree": candidate_tree,
        "unit": unit,
        "active_state": "active",
        "sub_state": "running",
        "observed_at": _iso(clock()),
    }


def _request_envelope(
    *,
    action: str,
    node: str,
    domain: str,
    sandbox: str,
    candidate_sha: str,
    authority_tree: str,
    payload_kind: str,
    payload: bytes,
) -> bytes:
    body: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "action": action,
        "node": node,
        "domain": domain,
        "sandbox": sandbox,
        "candidate_sha": candidate_sha,
        "candidate_tree": authority_tree,
        "payload_kind": payload_kind,
        "payload_sha256": hashlib.sha256(payload).hexdigest(),
        "payload_base64": base64.b64encode(payload).decode(),
        "prior_request_id": None,
    }
    body["request_id"] = hashlib.sha256(_canonical(body)).hexdigest()
    return _canonical(body)


def _transport_check(
    node: str,
    envelope: bytes,
    *,
    run: Callable[..., subprocess.CompletedProcess[bytes]] = subprocess.run,
) -> dict[str, Any]:
    try:
        completed = run(
            (str(NODE_TRANSPORT), "invoke", "--node", node, "--verb", "check"),
            input=envelope,
            check=False,
            capture_output=True,
            timeout=120,
            env={"PATH": "/usr/sbin:/usr/bin:/sbin:/bin", "LANG": "C", "LC_ALL": "C"},
        )
    except (OSError, subprocess.SubprocessError) as exc:
        raise LiveAuthorityError("node-authority transport failed safely") from exc
    if completed.returncode != 0 or completed.stderr or len(completed.stdout) > MAX_FILE_BYTES:
        raise LiveAuthorityError("node-authority transport failed safely")
    try:
        response = json.loads(completed.stdout)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise LiveAuthorityError("node-authority response is invalid") from exc
    request_id = json.loads(envelope)["request_id"]
    if (
        not isinstance(response, dict)
        or set(response) != {"schema_version", "request_id", "status", "result"}
        or response.get("schema_version") != SCHEMA_VERSION
        or response.get("request_id") != request_id
        or response.get("status") != "succeeded"
        or not isinstance(response.get("result"), dict)
    ):
        raise LiveAuthorityError("node-authority response binding is invalid")
    return dict(response["result"])


def _slurm_request(
    *,
    config: AdapterConfig,
    candidate_sha: str,
    candidate_tree: str,
    job: Mapping[str, Any],
    actuator: Mapping[str, Any],
) -> dict[str, Any]:
    pids = actuator.get("job_pids_max")
    if not isinstance(pids, int) or isinstance(pids, bool) or pids < 1:
        raise LiveAuthorityError("Control Plane job PID allocation is invalid")
    return {
        "schema_version": SCHEMA_VERSION,
        "kind": "loom.developer-sandbox.live-slurm-request",
        "source_host": SOURCE_HOSTS[config.pool],
        "sandbox": config.sandbox,
        "pool": config.pool,
        "candidate_sha": candidate_sha,
        "candidate_tree": candidate_tree,
        "job_id": job["job_id"],
        "account": f"loom-dev-{config.sandbox}",
        "user": SERVICE_USERS[config.sandbox],
        "job_name": job["job_name"],
        "node": job["node"],
        "requested_cpus": job["requested_cpus"],
        "requested_memory_mib": job["requested_memory_mib"],
        "job_pids_max": pids,
        "requested_gpus": job["requested_gpus"],
        "requested_gpu_tres": job["requested_gpu_tres"],
    }


def _slurm_readback(
    request: Mapping[str, Any],
    *,
    authority_tree: str,
    transport: Transport | None = None,
) -> dict[str, Any]:
    payload = _canonical(request)
    envelope = _request_envelope(
        action="observe-live-overlap-job",
        node=SOURCE_NODES[str(request["pool"])],
        domain=str(request["pool"]),
        sandbox=str(request["sandbox"]),
        candidate_sha=str(request["candidate_sha"]),
        authority_tree=authority_tree,
        payload_kind="live-overlap-job-json",
        payload=payload,
    )
    if request["pool"] == "oldlab":
        return observe_slurm_job(payload)
    invoke = _transport_check if transport is None else transport
    result = invoke(SOURCE_NODES["gb10"], envelope)
    if set(result) != SLURM_RESULT_FIELDS:
        raise LiveAuthorityError("remote Slurm readback has an invalid closed shape")
    return result


def _parse_scontrol(output: str) -> dict[str, str]:
    matches = list(re.finditer(r"(?:^|\s)([A-Za-z][A-Za-z0-9_]*)=", output))
    if not matches:
        raise LiveAuthorityError("Slurm job readback is malformed")
    values: dict[str, str] = {}
    for index, match in enumerate(matches):
        key = match.group(1)
        start = match.end()
        finish = matches[index + 1].start() if index + 1 < len(matches) else len(output)
        if key in values:
            raise LiveAuthorityError("Slurm job readback is ambiguous")
        values[key] = output[start:finish].strip()
    return values


def _memory_bytes(value: str) -> int:
    match = re.fullmatch(r"([1-9][0-9]*)([KMGTP]?)", value, re.IGNORECASE)
    if match is None:
        raise LiveAuthorityError("Slurm memory allocation is invalid")
    powers: dict[str, int] = {"": 0, "K": 1, "M": 2, "G": 3, "T": 4, "P": 5}
    power = powers[match.group(2).upper()]
    count: int = int(str(match.group(1)))
    return int(count * (1024**power))


def _gpu_count(tres: str) -> int:
    return sum(
        int(value) for value in re.findall(r"(?:^|,)gres/gpu(?:[^=,]*)=([0-9]+)(?:,|$)", tres)
    )


def _requested_gpu_count(tres: str) -> int:
    if not tres:
        return 0
    match = re.fullmatch(r"gpu(?::[A-Za-z0-9_.-]+)?:([1-9][0-9]*)", tres)
    if match is None:
        raise LiveAuthorityError("requested GPU TRES is invalid")
    return int(str(match.group(1)))


def observe_slurm_job(
    raw: bytes,
    *,
    run: Run = subprocess.run,
    clock: Clock = _now,
    hostname: Callable[[], str] = _host,
) -> dict[str, Any]:
    """Return one exact sanitized running-job readback on the fixed source host."""

    _require_root()
    try:
        request = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise LiveAuthorityError("live Slurm request is invalid") from exc
    if (
        not isinstance(request, dict)
        or set(request) != SLURM_REQUEST_FIELDS
        or raw != _canonical(request)
        or request.get("schema_version") != SCHEMA_VERSION
        or request.get("kind") != "loom.developer-sandbox.live-slurm-request"
        or request.get("sandbox") not in SANDBOXES
        or request.get("pool") not in POOLS
        or request.get("source_host") != SOURCE_HOSTS.get(str(request.get("pool")))
        or hostname() not in SOURCE_ALIASES[str(request["pool"])]
        or SHA_RE.fullmatch(str(request.get("candidate_sha"))) is None
        or SHA_RE.fullmatch(str(request.get("candidate_tree"))) is None
        or JOB_ID_RE.fullmatch(str(request.get("job_id"))) is None
        or request.get("account") != f"loom-dev-{request.get('sandbox')}"
        or request.get("user") != SERVICE_USERS.get(str(request.get("sandbox")))
        or SAFE_JOB_RE.fullmatch(str(request.get("job_name"))) is None
        or not isinstance(request.get("node"), str)
        or (
            request.get("pool") == "oldlab"
            and not str(request.get("node")).startswith("trt-eai-oldlab-")
        )
        or (request.get("pool") == "gb10" and not str(request.get("node")).startswith("trt-gb10-"))
        or any(
            not isinstance(request.get(field), int)
            or isinstance(request[field], bool)
            or request[field] < minimum
            for field, minimum in (
                ("requested_cpus", 1),
                ("requested_memory_mib", 1),
                ("job_pids_max", 1),
                ("requested_gpus", 0),
            )
        )
        or not isinstance(request.get("requested_gpu_tres"), str)
        or _requested_gpu_count(str(request.get("requested_gpu_tres")))
        != request.get("requested_gpus")
    ):
        raise LiveAuthorityError("live Slurm request binding is invalid")
    try:
        completed = run(
            (
                "/usr/bin/scontrol",
                "show",
                "job",
                "--oneliner",
                "--details",
                str(request["job_id"]),
            ),
            check=False,
            capture_output=True,
            text=True,
            timeout=15,
            env={"PATH": "/usr/sbin:/usr/bin:/sbin:/bin", "LANG": "C", "LC_ALL": "C"},
        )
    except (OSError, subprocess.SubprocessError) as exc:
        raise LiveAuthorityError("Slurm job readback failed safely") from exc
    if completed.returncode != 0 or completed.stderr or len(completed.stdout) > 65536:
        raise LiveAuthorityError("Slurm job readback failed safely")
    values = _parse_scontrol(completed.stdout.strip())
    alloc_tres = values.get("AllocTRES", "")
    comment = values.get("Comment", "")
    shared = values.get("Shared", values.get("OverSubscribe", ""))
    user = values.get("UserId", "").split("(", 1)[0]
    if (
        values.get("JobId") != request["job_id"]
        or values.get("JobName") != request["job_name"]
        or user != request["user"]
        or values.get("Account") != request["account"]
        or values.get("JobState") != "RUNNING"
        or values.get("NodeList") != request["node"]
        or values.get("NumNodes") != "1"
        or values.get("NumCPUs") != str(request["requested_cpus"])
        or _memory_bytes(values.get("MinMemoryNode", ""))
        != request["requested_memory_mib"] * 1024 * 1024
        or comment != f"loom-cgroup-v1:pids={request['job_pids_max']}"
        or shared not in {"OK", "YES", "USER", "1"}
        or _gpu_count(alloc_tres) != request["requested_gpus"]
    ):
        raise LiveAuthorityError("Slurm job readback does not match the exact request")
    return {
        "schema_version": SCHEMA_VERSION,
        "kind": "loom.developer-sandbox.live-slurm-observation",
        "source_host": request["source_host"],
        "sandbox": request["sandbox"],
        "pool": request["pool"],
        "candidate_sha": request["candidate_sha"],
        "candidate_tree": request["candidate_tree"],
        "job_id": request["job_id"],
        "account": request["account"],
        "user": request["user"],
        "job_name": request["job_name"],
        "node": request["node"],
        "state": "RUNNING",
        "allocation": {
            "cpu_cores": request["requested_cpus"],
            "memory_bytes": request["requested_memory_mib"] * 1024 * 1024,
            "pids": request["job_pids_max"],
            "gpu_count": request["requested_gpus"],
            "tres": alloc_tres,
            "exclusive": False,
        },
        "observed_at": _iso(clock()),
    }


def _ensure_directory(path: Path) -> None:
    path.mkdir(mode=0o700, parents=True, exist_ok=True)
    metadata = path.lstat()
    if (
        not stat.S_ISDIR(metadata.st_mode)
        or stat.S_ISLNK(metadata.st_mode)
        or metadata.st_uid != REQUIRED_UID
        or metadata.st_gid != REQUIRED_GID
        or stat.S_IMODE(metadata.st_mode) != 0o700
    ):
        raise LiveAuthorityError("live authority directory is unsafe")


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
    raw = _canonical(payload)
    try:
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
        existing = _read_secure_bytes(path, label="live authority receipt")
        if existing != raw:
            raise LiveAuthorityError("immutable live authority receipt conflicts") from None
        return
    try:
        os.write(descriptor, raw)
        os.fchmod(descriptor, 0o600)
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    _fsync_directory(path.parent)


def _open_lock() -> int:
    _ensure_directory(STATE_ROOT)
    for path in (HIGH_WATER_ROOT, TRANSACTION_ROOT, OVERLAP_ROOT):
        _ensure_directory(path)
    descriptor = os.open(
        LOCK,
        os.O_RDWR | os.O_CREAT | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0),
        0o600,
    )
    metadata = os.fstat(descriptor)
    if (
        not stat.S_ISREG(metadata.st_mode)
        or metadata.st_uid != REQUIRED_UID
        or metadata.st_gid != REQUIRED_GID
        or metadata.st_nlink != 1
        or stat.S_IMODE(metadata.st_mode) != 0o600
    ):
        os.close(descriptor)
        raise LiveAuthorityError("live authority lock is unsafe")
    fcntl.flock(descriptor, fcntl.LOCK_EX)
    return descriptor


def _load_optional(path: Path, *, label: str) -> dict[str, Any] | None:
    if not path.exists() and not path.is_symlink():
        return None
    payload, _raw = _secure_json(path, label=label)
    if not isinstance(payload, dict):
        raise LiveAuthorityError(f"{label} has an invalid closed shape")
    return payload


def _validate_collection(raw: bytes, candidate_sha: str) -> dict[str, Any]:
    try:
        payload = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise LiveAuthorityError("live authority collection request is invalid") from exc
    try:
        uuid.UUID(str(payload.get("collection_id")))
    except (AttributeError, ValueError) as exc:
        raise LiveAuthorityError("live authority collection identity is invalid") from exc
    if (
        not isinstance(payload, dict)
        or set(payload) != COLLECTION_FIELDS
        or raw != _canonical(payload)
        or payload.get("schema_version") != SCHEMA_VERSION
        or payload.get("kind") != "loom.developer-sandbox.live-overlap-collection"
        or SHA_RE.fullmatch(candidate_sha) is None
        or SHA_RE.fullmatch(str(payload.get("candidate_tree"))) is None
        or JOB_ID_RE.fullmatch(str(payload.get("job_id"))) is None
    ):
        raise LiveAuthorityError("live authority collection request binding is invalid")
    return payload


def _transaction_result(transaction: Mapping[str, Any]) -> dict[str, Any]:
    receipt_path = Path(str(transaction["receipt_path"]))
    receipt, raw = _secure_json(receipt_path, label="live authority receipt")
    if hashlib.sha256(raw).hexdigest() != transaction["receipt_sha256"]:
        raise LiveAuthorityError("live authority transaction receipt drifted")
    return {
        "schema_version": SCHEMA_VERSION,
        "kind": "loom.developer-sandbox.live-overlap-result",
        "path": str(receipt_path),
        "payload_sha256": transaction["receipt_sha256"],
        "job_id": receipt["job_readback"]["job_id"],
        "observation_sequence": receipt["capacity_sample"]["observation_sequence"],
        "observed_at": receipt["observed_at"],
    }


def collect(
    sandbox: str,
    pool: str,
    candidate_sha: str,
    authority_tree: str,
    raw_request: bytes,
    *,
    http_json: HttpJson = _http_json,
    service_run: Run = subprocess.run,
    transport: Transport | None = None,
    clock: Clock = _now,
    hostname: Callable[[], str] = _host,
) -> dict[str, Any]:
    """Collect and persist one immutable overlap observation."""

    _require_root()
    if (
        hostname() != COLLECT_HOST
        or sandbox not in SANDBOXES
        or pool not in POOLS
        or SHA_RE.fullmatch(candidate_sha) is None
        or SHA_RE.fullmatch(authority_tree) is None
    ):
        raise LiveAuthorityError("live authority collection host or identity is invalid")
    request = _validate_collection(raw_request, candidate_sha)
    candidate_tree = str(request["candidate_tree"])
    lock = _open_lock()
    try:
        transaction_path = TRANSACTION_ROOT / f"{request['collection_id']}.json"
        existing_transaction = _load_optional(
            transaction_path,
            label="live authority transaction",
        )
        if existing_transaction is not None:
            expected_fields = {
                "schema_version",
                "kind",
                "collection_id",
                "sandbox",
                "pool",
                "candidate_sha",
                "candidate_tree",
                "job_id",
                "receipt_path",
                "receipt_sha256",
                "receipt_payload",
                "high_water",
                "phase",
            }
            if (
                set(existing_transaction) != expected_fields
                or existing_transaction.get("schema_version") != SCHEMA_VERSION
                or existing_transaction.get("kind")
                != "loom.developer-sandbox.live-overlap-transaction"
                or existing_transaction.get("collection_id") != request["collection_id"]
                or existing_transaction.get("sandbox") != sandbox
                or existing_transaction.get("pool") != pool
                or existing_transaction.get("candidate_sha") != candidate_sha
                or existing_transaction.get("candidate_tree") != candidate_tree
                or existing_transaction.get("job_id") != request["job_id"]
                or existing_transaction.get("phase")
                not in {"prepared", "receipt-written", "committed"}
                or not isinstance(existing_transaction.get("receipt_payload"), dict)
                or _digest(existing_transaction["receipt_payload"])
                != existing_transaction.get("receipt_sha256")
            ):
                raise LiveAuthorityError("live authority transaction is invalid")
            if existing_transaction["phase"] == "prepared":
                _write_or_verify(
                    Path(str(existing_transaction["receipt_path"])),
                    existing_transaction["receipt_payload"],
                )
                existing_transaction["phase"] = "receipt-written"
                _atomic_replace(transaction_path, existing_transaction)
            if existing_transaction["phase"] == "receipt-written":
                _atomic_replace(
                    HIGH_WATER_ROOT / f"{sandbox}-{pool}.json",
                    existing_transaction["high_water"],
                )
                existing_transaction["phase"] = "committed"
                _atomic_replace(transaction_path, existing_transaction)
            return _transaction_result(existing_transaction)

        started_at = clock().astimezone(UTC)
        config = _load_adapter_config(sandbox, pool)
        capacity, capacity_raw = _capacity_observation(
            config,
            candidate_sha=candidate_sha,
            now=started_at,
        )
        _sandbox_state_payload, sandbox_state_raw = _sandbox_state(
            config,
            candidate_sha=candidate_sha,
            candidate_tree=candidate_tree,
        )
        token = _load_admin_token(config.admin_secret_file)
        environment = urllib.parse.quote(config.environment, safe="")
        encoded_pool = urllib.parse.quote(pool, safe="")
        policy = http_json(
            base_url=config.control_plane_url,
            token=token,
            path=f"/admin/worker-pool-autoscaler-policies/{environment}/{encoded_pool}",
            timeout=config.timeout_seconds,
        )
        actuator = _capacity_binding(policy, capacity)
        registry = http_json(
            base_url=config.control_plane_url,
            token=token,
            path="/admin/slurm-worker-jobs/status",
            timeout=config.timeout_seconds,
        )
        job = _active_job(
            registry,
            config=config,
            candidate_sha=candidate_sha,
            actuator=actuator,
            job_id=str(request["job_id"]),
        )
        slurm_request = _slurm_request(
            config=config,
            candidate_sha=candidate_sha,
            candidate_tree=candidate_tree,
            job=job,
            actuator=actuator,
        )
        job_readback = _slurm_readback(
            slurm_request,
            authority_tree=authority_tree,
            transport=transport,
        )
        service_readback = _service_readback(
            sandbox,
            candidate_sha,
            candidate_tree,
            run=service_run,
            clock=clock,
        )
        finished_at = clock().astimezone(UTC)
        if finished_at < started_at or finished_at - started_at > MAX_COLLECTION_SPAN:
            raise LiveAuthorityError("live authority collection exceeded its bounded span")
        if (
            _read_secure_bytes(config.observation_path, label="capacity observation")
            != capacity_raw
        ):
            raise LiveAuthorityError("capacity observation changed during collection")
        if (
            _read_secure_bytes(config.sandbox_state_path, label="sandbox lifecycle state")
            != sandbox_state_raw
        ):
            raise LiveAuthorityError("sandbox lifecycle state changed during collection")
        capacity_sample = {
            "phase": "multi_candidate_overlap",
            "observed_at": capacity["observed_at"],
            "sandbox": sandbox,
            "pool": pool,
            "candidate_sha": candidate_sha,
            "candidate_tree": candidate_tree,
            "job_id": job["job_id"],
            "account": f"loom-dev-{sandbox}",
            "user": SERVICE_USERS[sandbox],
            "job_name": job["job_name"],
            "node": job["node"],
            "allocation": job_readback["allocation"],
            "request_id": capacity["request_id"],
            "lease_epoch": capacity["lease_epoch"],
            "observation_sequence": capacity["observation_sequence"],
            "requested_slots": policy["max_slots"],
            "granted_slots": job["requested_concurrency"],
            "pending_slots": capacity["pending_slots"],
            "active_slots": capacity["active_slots"],
            "draining_slots": capacity["draining_slots"],
            "terminal_slots": capacity["terminal_slots"],
        }
        live_observation = {
            "schema_version": SCHEMA_VERSION,
            "kind": "loom.developer-sandbox.live-overlap-observation",
            "source_host": SOURCE_HOSTS[pool],
            "observed_at": _iso(finished_at),
            "sandbox": sandbox,
            "pool": pool,
            "candidate_sha": candidate_sha,
            "candidate_tree": candidate_tree,
            "capacity_observation_sha256": hashlib.sha256(capacity_raw).hexdigest(),
            "sandbox_state_sha256": hashlib.sha256(sandbox_state_raw).hexdigest(),
            "capacity_sample": capacity_sample,
            "job_readback": {
                key: value
                for key, value in job_readback.items()
                if key
                in {
                    "sandbox",
                    "pool",
                    "candidate_sha",
                    "candidate_tree",
                    "job_id",
                    "account",
                    "user",
                    "job_name",
                    "node",
                    "state",
                    "allocation",
                    "observed_at",
                }
            },
            "service_readback": service_readback,
        }
        receipt_path = OVERLAP_ROOT / pool / sandbox / candidate_sha / f"{job['job_id']}.json"
        receipt_digest = _digest(live_observation)
        high_water_path = HIGH_WATER_ROOT / f"{sandbox}-{pool}.json"
        prior = _load_optional(high_water_path, label="live authority high-water")
        if prior is not None:
            expected_prior = {
                "schema_version",
                "sandbox",
                "pool",
                "request_id",
                "lease_epoch",
                "observation_sequence",
                "observed_at",
                "receipt_path",
                "receipt_sha256",
            }
            if (
                set(prior) != expected_prior
                or prior.get("schema_version") != SCHEMA_VERSION
                or prior.get("sandbox") != sandbox
                or prior.get("pool") != pool
                or not isinstance(prior.get("observation_sequence"), int)
                or capacity["observation_sequence"] <= prior["observation_sequence"]
                or (
                    prior.get("request_id") == capacity["request_id"]
                    and capacity["lease_epoch"] < prior.get("lease_epoch", -1)
                )
                or _timestamp(prior.get("observed_at"), label="live authority high-water")
                >= finished_at
            ):
                raise LiveAuthorityError("live authority observation regressed or replayed")
        high_water = {
            "schema_version": SCHEMA_VERSION,
            "sandbox": sandbox,
            "pool": pool,
            "request_id": capacity["request_id"],
            "lease_epoch": capacity["lease_epoch"],
            "observation_sequence": capacity["observation_sequence"],
            "observed_at": live_observation["observed_at"],
            "receipt_path": str(receipt_path),
            "receipt_sha256": receipt_digest,
        }
        transaction = {
            "schema_version": SCHEMA_VERSION,
            "kind": "loom.developer-sandbox.live-overlap-transaction",
            "collection_id": request["collection_id"],
            "sandbox": sandbox,
            "pool": pool,
            "candidate_sha": candidate_sha,
            "candidate_tree": candidate_tree,
            "job_id": job["job_id"],
            "receipt_path": str(receipt_path),
            "receipt_sha256": receipt_digest,
            "receipt_payload": live_observation,
            "high_water": high_water,
            "phase": "prepared",
        }
        _atomic_replace(transaction_path, transaction)
        _write_or_verify(receipt_path, live_observation)
        transaction["phase"] = "receipt-written"
        _atomic_replace(transaction_path, transaction)
        _atomic_replace(high_water_path, high_water)
        transaction["phase"] = "committed"
        _atomic_replace(transaction_path, transaction)
        return _transaction_result(transaction)
    finally:
        os.close(lock)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__, allow_abbrev=False)
    subparsers = parser.add_subparsers(dest="command", required=True)
    collect_parser = subparsers.add_parser("collect", allow_abbrev=False)
    collect_parser.add_argument("--sandbox", choices=SANDBOXES, required=True)
    collect_parser.add_argument("--pool", choices=POOLS, required=True)
    collect_parser.add_argument("--candidate-sha", required=True)
    collect_parser.add_argument("--authority-tree", required=True)
    envelope_parser = subparsers.add_parser("collection-envelope", allow_abbrev=False)
    envelope_parser.add_argument("--sandbox", choices=SANDBOXES, required=True)
    envelope_parser.add_argument("--pool", choices=POOLS, required=True)
    envelope_parser.add_argument("--candidate-sha", required=True)
    envelope_parser.add_argument("--candidate-tree", required=True)
    envelope_parser.add_argument("--authority-tree", required=True)
    envelope_parser.add_argument("--collection-id", required=True)
    envelope_parser.add_argument("--job-id", required=True)
    subparsers.add_parser("observe-slurm-job", allow_abbrev=False)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(list(argv) if argv is not None else None)
    try:
        if args.command == "collection-envelope":
            collection = _canonical(
                {
                    "schema_version": SCHEMA_VERSION,
                    "kind": "loom.developer-sandbox.live-overlap-collection",
                    "collection_id": args.collection_id,
                    "candidate_tree": args.candidate_tree,
                    "job_id": args.job_id,
                },
            )
            _validate_collection(collection, args.candidate_sha)
            if SHA_RE.fullmatch(args.authority_tree) is None:
                raise LiveAuthorityError("node-authority tree is invalid")
            sys.stdout.buffer.write(
                _request_envelope(
                    action="collect-live-overlap",
                    node="oldlab-2",
                    domain=args.pool,
                    sandbox=args.sandbox,
                    candidate_sha=args.candidate_sha,
                    authority_tree=args.authority_tree,
                    payload_kind="live-overlap-collection-json",
                    payload=collection,
                ),
            )
            return 0
        raw = sys.stdin.buffer.read(MAX_FILE_BYTES + 1)
        if len(raw) > MAX_FILE_BYTES:
            raise LiveAuthorityError("live authority request exceeds its size bound")
        if args.command == "collect":
            result = collect(
                args.sandbox,
                args.pool,
                args.candidate_sha,
                args.authority_tree,
                raw,
            )
        else:
            result = observe_slurm_job(raw)
        sys.stdout.buffer.write(_canonical(result))
        return 0
    except LiveAuthorityError as exc:
        sys.stderr.write(f"error: {exc}\n")
        return 1
    except OSError:
        sys.stderr.write("error: live authority filesystem operation failed safely\n")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
