#!/usr/bin/env python3
"""Produce trusted staging pressure-reclaim acceptance receipts.

This authority is intentionally staging-only and pool-scoped.  It proves the
existing pressure endpoint and Slurm drain actor as one closed transaction:

* every active Loom job in the reviewed pool belongs to the registered
  acceptance session before pressure is posted;
* pressure fences an acceptance-owned claim;
* the session's Slurm allocations reach a terminal state and the interrupted
  acceptance trial is attributed as retryable production-capacity pressure;
* clearing pressure restores an acceptance-owned claim, which is immediately
  returned to queued state; and
* non-Loom jobs in the same partition have byte-for-byte identical Slurm
  observations before, during, and after the transaction.

Session registration is root-only.  The live ``run`` command accepts only a
session UUID and derives every artifact path beneath the fixed state root.
Transactions are journaled after every remote mutation and recover by rolling
forward.  Receipts are immutable, Ed25519-signed, candidate/session-bound, and
published behind a monotonic per-pool high-water record.

The ``observe-slurm`` command is the bounded submit-host action.  The node
authority exposes it through a fixed transport action; until that integration
is installed, the producer fails closed.
"""

from __future__ import annotations

import argparse
import base64
import ctypes
import fcntl
import hashlib
import json
import os
import re
import secrets
import socket
import stat
import subprocess
import sys
import time
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
KIND_SESSION: Final = "loom.staging-pressure-reclaim.session"
KIND_TRANSACTION: Final = "loom.staging-pressure-reclaim.transaction"
KIND_RECEIPT: Final = "loom.staging-pressure-reclaim.receipt"
KIND_OBSERVE_REQUEST: Final = "loom.staging-pressure-reclaim.observe-request"
KIND_OBSERVE_RESULT: Final = "loom.staging-pressure-reclaim.observe-result"
KIND_PUBLISHED: Final = "loom.staging-pressure-reclaim.published-receipt"
KIND_HIGH_WATER: Final = "loom.staging-pressure-reclaim.high-water"

CONFIG_PATH: Final = Path("/etc/loom/staging-pressure-reclaim-authority.toml")
STATE_ROOT: Final = Path("/var/lib/loom-staging-pressure-reclaim")
SESSION_ROOT: Final = STATE_ROOT / "sessions"
TRANSACTION_ROOT: Final = STATE_ROOT / "transactions"
RECEIPT_ROOT: Final = STATE_ROOT / "receipts"
HIGH_WATER_ROOT: Final = STATE_ROOT / "high-water"
LOCK_PATH: Final = STATE_ROOT / "authority.lock"
CURRENT_PATH: Final = STATE_ROOT / "current.json"
ADMIN_SECRET_FILE: Final = Path("/etc/loom/staging-pressure-reclaim/admin-token")
WORKER_SECRET_FILE: Final = Path(
    "/etc/loom/staging-pressure-reclaim/acceptance-worker-token"
)
NODE_TRANSPORT: Final = Path("/usr/local/libexec/loom-developer-sandbox-node-transport")
PUBLISHED_ROOT: Final = Path("/srv/loom/staging-shared/results/pressure-reclaim")
PUBLIC_KEY: Final = Path("/etc/loom/staging-pressure-reclaim/authority-public.pem")
PRIVATE_KEY: Final = Path("/etc/loom/staging-pressure-reclaim/authority-private.pem")
CONTROL_PLANE_URL: Final = "http://127.0.0.1:8080"

ENVIRONMENT: Final = "staging"
SOURCE_HOST: Final = "trt-eai-oldlab-1"
SUBMIT_HOST: Final = "trt-gb10-1"
POOL: Final = "gb10"
PARTITION: Final = "gb10"
TRANSPORT_ACTION: Final = "staging-pressure-reclaim-observe"
SESSION_MARKER_KEY: Final = "LOOM_STAGING_PRESSURE_SESSION_ID"
OWNERSHIP_MARKER_KEY: Final = "LOOM_STAGING_PRESSURE_OWNERSHIP"
OWNERSHIP_MARKER_VALUE: Final = "acceptance-owned"
STAGING_SLURM_USER: Final = "loom-staging-worker"
STAGING_SLURM_ACCOUNT: Final = "loom-staging"
STAGING_SLURM_QOS: Final = "loom-staging"
ACTIVE_JOB_STATES: Final = frozenset({"pending", "running"})
TERMINAL_JOB_STATES: Final = frozenset({"completed", "failed", "cancelled", "stale"})
ACTIVE_SLURM_STATES: Final = frozenset(
    {
        "PENDING",
        "RUNNING",
        "CONFIGURING",
        "COMPLETING",
        "SIGNALING",
        "STAGE_OUT",
        "STOPPED",
        "SUSPENDED",
        "RESIZING",
    },
)
SESSION_FIELDS: Final = frozenset(
    {
        "schema_version",
        "kind",
        "session_id",
        "acceptance_session_id",
        "environment",
        "pool",
        "candidate_sha",
        "candidate_tree",
        "created_at",
        "expires_at",
        "loom_jobs",
        "interrupted_trial",
        "claim_probe",
    },
)
JOB_FIELDS: Final = frozenset(
    {
        "registry_id",
        "job_id",
        "compose_project",
        "worker_id",
        "sandbox_identity",
        "slurm_user",
        "slurm_account",
        "slurm_qos",
        "job_name",
    },
)
TRIAL_FIELDS: Final = frozenset({"trial_id", "team_id", "task_id", "worker_id"})
CLAIM_FIELDS: Final = frozenset(
    {"trial_id", "team_id", "task_id", "worker_id", "caps"},
)
CONFIG_FIELDS: Final = frozenset(
    {
        "schema_version",
        "environment",
        "pool",
        "source_host",
        "submit_host",
        "partition",
        "control_plane_url",
        "admin_secret_file",
        "worker_secret_file",
        "node_transport",
        "published_root",
        "public_key",
        "private_key",
        "max_session_age_seconds",
        "poll_interval_seconds",
        "terminal_timeout_seconds",
        "retry_timeout_seconds",
        "http_timeout_seconds",
    },
)
SHA_RE: Final = re.compile(r"^[0-9a-f]{40}$")
DIGEST_RE: Final = re.compile(r"^[0-9a-f]{64}$")
JOB_ID_RE: Final = re.compile(r"^[1-9][0-9]*(?:_[0-9]+)?$")
SAFE_NAME_RE: Final = re.compile(r"^[A-Za-z0-9_.-]{1,128}$")
MAX_JSON_BYTES: Final = 4 * 1024 * 1024
MAX_OBSERVER_BYTES: Final = 2 * 1024 * 1024
MAX_SESSION_AGE: Final = timedelta(hours=2)
MAX_KEY_BYTES: Final = 16 * 1024
ED25519_SPKI_PREFIX: Final = bytes.fromhex("302a300506032b6570032100")


class AuthorityError(RuntimeError):
    """A secret-safe, fail-closed authority error."""


@dataclass(frozen=True, slots=True)
class Config:
    environment: str
    pool: str
    source_host: str
    submit_host: str
    partition: str
    control_plane_url: str
    admin_secret_file: Path
    worker_secret_file: Path
    node_transport: Path
    published_root: Path
    public_key: Path
    private_key: Path
    max_session_age_seconds: int
    poll_interval_seconds: float
    terminal_timeout_seconds: float
    retry_timeout_seconds: float
    http_timeout_seconds: float


@dataclass(frozen=True, slots=True)
class HttpResponse:
    status: int
    body: dict[str, Any] | None


HttpCall = Callable[..., HttpResponse]
ObserveCall = Callable[..., dict[str, Any]]
Clock = Callable[[], datetime]
Sleep = Callable[[float], None]
Run = Callable[..., subprocess.CompletedProcess[bytes]]


def _canonical(value: Any) -> bytes:
    return (json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n").encode()


def _digest(value: Any) -> str:
    return hashlib.sha256(_canonical(value)).hexdigest()


def _iso(value: datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _now() -> datetime:
    return datetime.now(UTC)


def _timestamp(value: object, *, label: str) -> datetime:
    if not isinstance(value, str):
        raise AuthorityError(f"{label} timestamp is invalid")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise AuthorityError(f"{label} timestamp is invalid") from exc
    if parsed.tzinfo is None:
        raise AuthorityError(f"{label} timestamp is invalid")
    return parsed.astimezone(UTC)


def _host() -> str:
    return socket.gethostname().split(".", 1)[0].lower()


def _require_root() -> None:
    if os.getuid() != 0 or os.geteuid() != 0:
        raise AuthorityError("staging pressure-reclaim authority requires root")


def _require_source_host(config: Config) -> None:
    if _host() != config.source_host:
        raise AuthorityError("authority must run on the fixed staging source host")


def _verify_trusted_parent_chain(path: Path, *, label: str) -> None:
    for parent in path.parents:
        if parent == Path("/"):
            break
        try:
            info = parent.lstat()
        except OSError as exc:
            raise AuthorityError(f"{label} trusted parent is unavailable") from exc
        if (
            not stat.S_ISDIR(info.st_mode)
            or stat.S_ISLNK(info.st_mode)
            or info.st_uid != 0
            or info.st_gid != 0
            or info.st_mode & 0o022
        ):
            raise AuthorityError(f"{label} trusted parent metadata is invalid")


def _prepare_private_directory(path: Path) -> None:
    if path.exists() and path.is_symlink():
        raise AuthorityError(f"private directory is a symlink: {path}")
    path.mkdir(mode=0o700, parents=True, exist_ok=True)
    info = path.stat()
    if (
        not stat.S_ISDIR(info.st_mode)
        or info.st_uid != 0
        or info.st_gid != 0
        or stat.S_IMODE(info.st_mode) != 0o700
    ):
        raise AuthorityError(f"private directory metadata is invalid: {path}")


def _prepare_state() -> None:
    for path in (
        STATE_ROOT,
        SESSION_ROOT,
        TRANSACTION_ROOT,
        RECEIPT_ROOT,
        HIGH_WATER_ROOT,
    ):
        _prepare_private_directory(path)
    if not LOCK_PATH.exists():
        descriptor = os.open(
            LOCK_PATH,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0),
            0o600,
        )
        os.close(descriptor)
    info = LOCK_PATH.stat(follow_symlinks=False)
    if (
        not stat.S_ISREG(info.st_mode)
        or info.st_uid != 0
        or info.st_gid != 0
        or stat.S_IMODE(info.st_mode) != 0o600
        or info.st_nlink != 1
    ):
        raise AuthorityError("authority lock metadata is invalid")


def _locked() -> Any:
    class _Lock:
        descriptor: int | None = None

        def __enter__(self) -> None:
            self.descriptor = os.open(
                LOCK_PATH,
                os.O_RDWR | getattr(os, "O_NOFOLLOW", 0),
            )
            fcntl.flock(self.descriptor, fcntl.LOCK_EX)

        def __exit__(self, *_args: object) -> None:
            assert self.descriptor is not None
            fcntl.flock(self.descriptor, fcntl.LOCK_UN)
            os.close(self.descriptor)

    return _Lock()


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _rename_noreplace(source: Path, target: Path) -> None:
    if target.exists() or target.is_symlink():
        raise FileExistsError(target)
    libc = ctypes.CDLL(None, use_errno=True)
    renameat2 = getattr(libc, "renameat2", None)
    if renameat2 is None:
        raise AuthorityError("kernel no-replace publication support is required")
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
        raise AuthorityError("key no-replace publication failed safely") from OSError(
            error,
            os.strerror(error),
        )


def _install_key_no_replace(path: Path, payload: bytes, *, mode: int) -> bool:
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
        raise AuthorityError("key staging failed safely") from exc
    try:
        try:
            remaining = memoryview(payload)
            while remaining:
                written = os.write(descriptor, remaining)
                if written <= 0:
                    raise AuthorityError("key write failed safely")
                remaining = remaining[written:]
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


def _read_secure_bytes(
    path: Path,
    *,
    max_bytes: int = MAX_JSON_BYTES,
    allowed_modes: frozenset[int] = frozenset({0o600, 0o644}),
    uid: int = 0,
    gid: int = 0,
) -> bytes:
    try:
        descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
    except OSError as exc:
        raise AuthorityError(f"secure file is unavailable: {path}") from exc
    try:
        initial = os.fstat(descriptor)
        if (
            not stat.S_ISREG(initial.st_mode)
            or initial.st_uid != uid
            or initial.st_gid != gid
            or stat.S_IMODE(initial.st_mode) not in allowed_modes
            or initial.st_nlink != 1
            or initial.st_size > max_bytes
        ):
            raise AuthorityError(f"secure file metadata is invalid: {path}")
        chunks: list[bytes] = []
        total = 0
        while True:
            chunk = os.read(descriptor, min(64 * 1024, max_bytes + 1 - total))
            if not chunk:
                break
            chunks.append(chunk)
            total += len(chunk)
            if total > max_bytes:
                raise AuthorityError(f"secure file read is invalid: {path}")
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
            raise AuthorityError(f"secure file read is invalid: {path}")
        return b"".join(chunks)
    finally:
        os.close(descriptor)


def _read_json(path: Path, *, expected_kind: str | None = None) -> dict[str, Any]:
    raw = _read_secure_bytes(path)
    try:
        value = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise AuthorityError(f"JSON artifact is invalid: {path}") from exc
    if not isinstance(value, dict) or raw != _canonical(value):
        raise AuthorityError(f"JSON artifact is not canonical: {path}")
    if expected_kind is not None and value.get("kind") != expected_kind:
        raise AuthorityError(f"JSON artifact kind is invalid: {path}")
    return value


def _atomic_replace(path: Path, payload: Mapping[str, Any]) -> None:
    encoded = _canonical(dict(payload))
    temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
    descriptor = os.open(
        temporary,
        os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0),
        0o600,
    )
    try:
        remaining = memoryview(encoded)
        while remaining:
            written = os.write(descriptor, remaining)
            if written <= 0:
                raise AuthorityError(f"artifact write failed: {path}")
            remaining = remaining[written:]
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    os.replace(temporary, path)
    _fsync_directory(path.parent)


def _write_once(path: Path, payload: Mapping[str, Any]) -> None:
    encoded = _canonical(dict(payload))
    try:
        descriptor = os.open(
            path,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0),
            0o600,
        )
    except FileExistsError:
        existing = _read_secure_bytes(path)
        if existing != encoded:
            raise AuthorityError(f"immutable artifact collision: {path}") from None
        return
    try:
        remaining = memoryview(encoded)
        while remaining:
            written = os.write(descriptor, remaining)
            if written <= 0:
                raise AuthorityError(f"artifact write failed: {path}")
            remaining = remaining[written:]
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    _fsync_directory(path.parent)


def _load_config(path: Path = CONFIG_PATH) -> Config:
    try:
        raw = tomllib.loads(_read_secure_bytes(path, max_bytes=64 * 1024).decode())
    except (UnicodeDecodeError, tomllib.TOMLDecodeError) as exc:
        raise AuthorityError("authority config is invalid") from exc
    if set(raw) != CONFIG_FIELDS or raw.get("schema_version") != SCHEMA_VERSION:
        raise AuthorityError("authority config schema is invalid")
    if (
        raw.get("environment") != ENVIRONMENT
        or raw.get("pool") != POOL
        or raw.get("source_host") != SOURCE_HOST
        or raw.get("submit_host") != SUBMIT_HOST
        or raw.get("partition") != PARTITION
        or raw.get("control_plane_url") != CONTROL_PLANE_URL
        or raw.get("admin_secret_file") != str(ADMIN_SECRET_FILE)
        or raw.get("worker_secret_file") != str(WORKER_SECRET_FILE)
        or raw.get("node_transport") != str(NODE_TRANSPORT)
        or raw.get("published_root") != str(PUBLISHED_ROOT)
        or raw.get("public_key") != str(PUBLIC_KEY)
        or raw.get("private_key") != str(PRIVATE_KEY)
    ):
        raise AuthorityError("authority config fixed staging identity or path is invalid")
    base_url = str(raw["control_plane_url"])
    parsed_url = urllib.parse.urlparse(base_url)
    if (
        parsed_url.scheme != "http"
        or parsed_url.hostname not in {"127.0.0.1", "localhost"}
        or parsed_url.username is not None
        or parsed_url.password is not None
        or parsed_url.query
        or parsed_url.fragment
    ):
        raise AuthorityError("control plane URL must be fixed loopback HTTP")
    integer_fields = (
        "max_session_age_seconds",
        "terminal_timeout_seconds",
        "retry_timeout_seconds",
    )
    if any(not isinstance(raw[field], int) or raw[field] <= 0 for field in integer_fields):
        raise AuthorityError("authority timeout configuration is invalid")
    float_fields = ("poll_interval_seconds", "http_timeout_seconds")
    if any(
        not isinstance(raw[field], (int, float)) or float(raw[field]) <= 0 for field in float_fields
    ):
        raise AuthorityError("authority interval configuration is invalid")
    if int(raw["max_session_age_seconds"]) > int(MAX_SESSION_AGE.total_seconds()):
        raise AuthorityError("session maximum age exceeds the compiled bound")
    return Config(
        environment=ENVIRONMENT,
        pool=POOL,
        source_host=SOURCE_HOST,
        submit_host=SUBMIT_HOST,
        partition=PARTITION,
        control_plane_url=base_url.rstrip("/"),
        admin_secret_file=Path(str(raw["admin_secret_file"])),
        worker_secret_file=Path(str(raw["worker_secret_file"])),
        node_transport=Path(str(raw["node_transport"])),
        published_root=Path(str(raw["published_root"])),
        public_key=Path(str(raw["public_key"])),
        private_key=Path(str(raw["private_key"])),
        max_session_age_seconds=int(raw["max_session_age_seconds"]),
        poll_interval_seconds=float(raw["poll_interval_seconds"]),
        terminal_timeout_seconds=float(raw["terminal_timeout_seconds"]),
        retry_timeout_seconds=float(raw["retry_timeout_seconds"]),
        http_timeout_seconds=float(raw["http_timeout_seconds"]),
    )


def _load_secret(path: Path, *, uid: int = 0, gid: int = 0) -> str:
    _verify_trusted_parent_chain(path, label="external secret")
    try:
        info = path.stat(follow_symlinks=False)
    except OSError as exc:
        raise AuthorityError("external secret prerequisite is unavailable") from exc
    if (
        not stat.S_ISREG(info.st_mode)
        or info.st_uid != uid
        or info.st_gid != gid
        or stat.S_IMODE(info.st_mode) != 0o600
        or info.st_nlink != 1
    ):
        raise AuthorityError("external secret prerequisite metadata is invalid")
    try:
        raw = _read_secure_bytes(
            path,
            max_bytes=64 * 1024,
            allowed_modes=frozenset({0o600}),
            uid=uid,
            gid=gid,
        )
    except OSError as exc:
        raise AuthorityError("external secret prerequisite is unavailable") from exc
    try:
        value = raw.decode().strip()
    except UnicodeDecodeError as exc:
        raise AuthorityError("secret file encoding is invalid") from exc
    if not value or any(character.isspace() for character in value):
        raise AuthorityError("external secret prerequisite content is invalid")
    return value


def _validate_uuid(value: object, *, label: str) -> str:
    if not isinstance(value, str):
        raise AuthorityError(f"{label} is invalid")
    try:
        parsed = uuid.UUID(value)
    except ValueError as exc:
        raise AuthorityError(f"{label} is invalid") from exc
    if str(parsed) != value:
        raise AuthorityError(f"{label} is not canonical")
    return value


def _validate_session(
    session: Mapping[str, Any],
    *,
    now: datetime,
    max_age_seconds: int,
) -> dict[str, Any]:
    value = dict(session)
    if set(value) != SESSION_FIELDS:
        raise AuthorityError("session fields are invalid")
    if (
        value.get("schema_version") != SCHEMA_VERSION
        or value.get("kind") != KIND_SESSION
        or value.get("environment") != ENVIRONMENT
        or value.get("pool") != POOL
    ):
        raise AuthorityError("session staging identity is invalid")
    _validate_uuid(value.get("session_id"), label="session_id")
    if (
        not isinstance(value.get("acceptance_session_id"), str)
        or re.fullmatch(r"[0-9a-f]{32}", str(value["acceptance_session_id"])) is None
    ):
        raise AuthorityError("acceptance session identity is invalid")
    if not isinstance(value.get("candidate_sha"), str) or not SHA_RE.fullmatch(
        str(value["candidate_sha"]),
    ):
        raise AuthorityError("session candidate SHA is invalid")
    if not isinstance(value.get("candidate_tree"), str) or not SHA_RE.fullmatch(
        str(value["candidate_tree"]),
    ):
        raise AuthorityError("session candidate tree is invalid")
    created = _timestamp(value.get("created_at"), label="created_at")
    expires = _timestamp(value.get("expires_at"), label="expires_at")
    if (
        created > now + timedelta(seconds=5)
        or expires <= now
        or expires <= created
        or expires - created > timedelta(seconds=max_age_seconds)
    ):
        raise AuthorityError("session validity window is invalid")
    jobs = value.get("loom_jobs")
    if not isinstance(jobs, list) or not jobs:
        raise AuthorityError("session must own at least one Loom job")
    registry_ids: set[str] = set()
    slurm_ids: set[str] = set()
    worker_ids: set[str] = set()
    for job in jobs:
        if not isinstance(job, dict) or set(job) != JOB_FIELDS:
            raise AuthorityError("session Loom job is invalid")
        registry_ids.add(_validate_uuid(job.get("registry_id"), label="registry_id"))
        worker_ids.add(_validate_uuid(job.get("worker_id"), label="worker_id"))
        job_id = job.get("job_id")
        if not isinstance(job_id, str) or not JOB_ID_RE.fullmatch(job_id):
            raise AuthorityError("session Slurm job ID is invalid")
        slurm_ids.add(job_id)
        for field in ("compose_project", "sandbox_identity", "job_name"):
            field_value = job.get(field)
            if not isinstance(field_value, str) or not SAFE_NAME_RE.fullmatch(field_value):
                raise AuthorityError(f"session {field} is invalid")
        if (
            job.get("slurm_user") != STAGING_SLURM_USER
            or job.get("slurm_account") != STAGING_SLURM_ACCOUNT
            or job.get("slurm_qos") != STAGING_SLURM_QOS
        ):
            raise AuthorityError("session Slurm identity is not the fixed staging identity")
    if len(registry_ids) != len(jobs) or len(slurm_ids) != len(jobs):
        raise AuthorityError("session Loom job identities are not unique")
    interrupted = value.get("interrupted_trial")
    if not isinstance(interrupted, dict) or set(interrupted) != TRIAL_FIELDS:
        raise AuthorityError("interrupted trial binding is invalid")
    claim = value.get("claim_probe")
    if not isinstance(claim, dict) or set(claim) != CLAIM_FIELDS:
        raise AuthorityError("claim probe binding is invalid")
    for trial, label in ((interrupted, "interrupted"), (claim, "claim probe")):
        _validate_uuid(trial.get("trial_id"), label=f"{label} trial_id")
        _validate_uuid(trial.get("team_id"), label=f"{label} team_id")
        task_id = trial.get("task_id")
        if not isinstance(task_id, str) or not SAFE_NAME_RE.fullmatch(task_id):
            raise AuthorityError(f"{label} task_id is invalid")
        _validate_uuid(trial.get("worker_id"), label=f"{label} worker_id")
    if interrupted["worker_id"] not in worker_ids:
        raise AuthorityError("interrupted trial worker is not owned by a session job")
    if interrupted["trial_id"] == claim["trial_id"]:
        raise AuthorityError("interrupted and claim probe trials must be distinct")
    claim_worker = _validate_uuid(claim.get("worker_id"), label="claim worker_id")
    if claim_worker in worker_ids:
        raise AuthorityError("claim probe worker must be distinct from drained workers")
    caps = claim.get("caps")
    if not isinstance(caps, list) or not caps or len(_canonical(caps)) > 32 * 1024:
        raise AuthorityError("claim probe capabilities are invalid")
    return value


def register_session(
    source: Path,
    *,
    config: Config,
    clock: Clock = _now,
) -> dict[str, Any]:
    """Register one root-reviewed immutable acceptance session."""
    _require_root()
    _require_source_host(config)
    _prepare_state()
    raw = _read_secure_bytes(source)
    try:
        parsed = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise AuthorityError("session source is invalid JSON") from exc
    if not isinstance(parsed, dict) or raw != _canonical(parsed):
        raise AuthorityError("session source must be canonical JSON")
    session = _validate_session(
        parsed,
        now=clock(),
        max_age_seconds=config.max_session_age_seconds,
    )
    path = SESSION_ROOT / f"{session['session_id']}.json"
    with _locked():
        _write_once(path, session)
    return {
        "status": "registered",
        "session_id": session["session_id"],
        "session_path": str(path),
        "session_sha256": _digest(session),
    }


def _default_http_call(
    *,
    method: str,
    base_url: str,
    token: str,
    path: str,
    timeout: float,
    body: Mapping[str, Any] | None = None,
) -> HttpResponse:
    data = _canonical(dict(body)) if body is not None else None
    headers = {"Authorization": f"Bearer {token}"}
    if data is not None:
        headers["Content-Type"] = "application/json"
    request = urllib.request.Request(
        base_url.rstrip("/") + path,
        method=method,
        data=data,
        headers=headers,
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            raw = response.read(MAX_JSON_BYTES + 1)
            status = int(response.status)
    except urllib.error.HTTPError as exc:
        exc.read(MAX_JSON_BYTES + 1)
        raise AuthorityError(f"{method} {path} failed HTTP {exc.code}") from exc
    except urllib.error.URLError as exc:
        raise AuthorityError(f"{method} {path} transport failed safely") from exc
    if len(raw) > MAX_JSON_BYTES:
        raise AuthorityError(f"{method} {path} response is too large")
    if status == 204:
        if raw:
            raise AuthorityError(f"{method} {path} returned a body with HTTP 204")
        return HttpResponse(status=status, body=None)
    try:
        parsed = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise AuthorityError(f"{method} {path} returned invalid JSON") from exc
    if not isinstance(parsed, dict):
        raise AuthorityError(f"{method} {path} returned non-object JSON")
    return HttpResponse(status=status, body=parsed)


def _http_object(
    call: HttpCall,
    *,
    method: str,
    config: Config,
    token: str,
    path: str,
    body: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    response = call(
        method=method,
        base_url=config.control_plane_url,
        token=token,
        path=path,
        timeout=config.http_timeout_seconds,
        body=body,
    )
    if response.status != 200 or response.body is None:
        raise AuthorityError(f"{method} {path} returned unexpected status")
    return response.body


def _trial(
    call: HttpCall,
    *,
    config: Config,
    token: str,
    binding: Mapping[str, Any],
) -> dict[str, Any]:
    trial_id = str(binding["trial_id"])
    result = _http_object(
        call,
        method="GET",
        config=config,
        token=token,
        path=f"/trials/{urllib.parse.quote(trial_id, safe='')}",
    )
    for key in ("id", "team_id", "task_id"):
        expected_key = "trial_id" if key == "id" else key
        if result.get(key) != binding.get(expected_key):
            raise AuthorityError("trial readback does not match session binding")
    return result


def _trial_evidence(trial: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "id": trial.get("id"),
        "team_id": trial.get("team_id"),
        "task_id": trial.get("task_id"),
        "state": trial.get("state"),
        "failure_reason": trial.get("failure_reason"),
        "attempt_count": trial.get("attempt_count"),
    }


def _job_evidence(row: Mapping[str, Any]) -> dict[str, Any]:
    environment = row.get("redacted_env")
    return {
        "registry_id": row.get("id"),
        "job_id": row.get("job_id"),
        "worker_id": row.get("worker_id"),
        "compose_project": row.get("compose_project"),
        "sandbox_identity": row.get("sandbox_identity"),
        "candidate_sha": row.get("candidate_sha"),
        "state": row.get("state"),
        "pending_reason": row.get("pending_reason"),
        "acceptance_owned": (
            isinstance(environment, dict)
            and environment.get(SESSION_MARKER_KEY) is not None
            and environment.get(OWNERSHIP_MARKER_KEY) == OWNERSHIP_MARKER_VALUE
        ),
    }


def _active_pool_jobs(
    registry: Mapping[str, Any], session: Mapping[str, Any]
) -> list[dict[str, Any]]:
    jobs = registry.get("jobs")
    if not isinstance(jobs, list):
        raise AuthorityError("Slurm registry response is invalid")
    active: list[dict[str, Any]] = []
    for row in jobs:
        if not isinstance(row, dict):
            raise AuthorityError("Slurm registry job is invalid")
        if (
            row.get("environment") == ENVIRONMENT
            and row.get("pool_name") == POOL
            and row.get("state") in ACTIVE_JOB_STATES
        ):
            active.append(row)
    expected_by_registry = {str(job["registry_id"]): job for job in session["loom_jobs"]}
    if {str(row.get("id")) for row in active} != set(expected_by_registry):
        raise AuthorityError(
            "pool contains a foreign or missing active Loom job; refusing pressure",
        )
    for row in active:
        expected = expected_by_registry[str(row["id"])]
        if (
            row.get("job_id") != expected["job_id"]
            or row.get("worker_id") != expected["worker_id"]
            or row.get("compose_project") != expected["compose_project"]
            or row.get("sandbox_identity") != expected["sandbox_identity"]
            or row.get("candidate_sha") != session["candidate_sha"]
        ):
            raise AuthorityError("active Loom job does not match session binding")
        env = row.get("redacted_env")
        if (
            not isinstance(env, dict)
            or env.get(SESSION_MARKER_KEY) != session["session_id"]
            or env.get(OWNERSHIP_MARKER_KEY) != OWNERSHIP_MARKER_VALUE
        ):
            raise AuthorityError("active Loom job lacks exact acceptance ownership markers")
    return [_job_evidence(row) for row in sorted(active, key=lambda row: str(row["job_id"]))]


def _terminal_owned_jobs(
    registry: Mapping[str, Any],
    session: Mapping[str, Any],
) -> list[dict[str, Any]]:
    jobs = registry.get("jobs")
    if not isinstance(jobs, list):
        raise AuthorityError("Slurm registry response is invalid")
    by_id = {str(row.get("id")): row for row in jobs if isinstance(row, dict)}
    results: list[dict[str, Any]] = []
    for expected in session["loom_jobs"]:
        row = by_id.get(str(expected["registry_id"]))
        if (
            row is None
            or row.get("job_id") != expected["job_id"]
            or row.get("state") not in TERMINAL_JOB_STATES
            or row.get("pending_reason")
            not in {
                "cancelled by prod-pressure reclaim",
                "released during prod-pressure reclaim",
            }
        ):
            raise AuthorityError("owned Loom job has not reached reclaim terminal state")
        results.append(row)
    foreign_active = [
        row
        for row in jobs
        if isinstance(row, dict)
        and row.get("environment") == ENVIRONMENT
        and row.get("pool_name") == POOL
        and row.get("state") in ACTIVE_JOB_STATES
        and str(row.get("id")) not in {str(job["registry_id"]) for job in session["loom_jobs"]}
    ]
    if foreign_active:
        raise AuthorityError("foreign Loom job appeared during pressure transaction")
    return [_job_evidence(row) for row in sorted(results, key=lambda row: str(row["job_id"]))]


def _observer_envelope(
    config: Config,
    session: Mapping[str, Any],
    *,
    phase: str,
) -> bytes:
    if phase not in {"before", "during", "after"}:
        raise AuthorityError("Slurm observation phase is invalid")
    payload = {
        "schema_version": SCHEMA_VERSION,
        "kind": KIND_OBSERVE_REQUEST,
        "source_host": config.source_host,
        "submit_host": config.submit_host,
        "environment": config.environment,
        "pool": config.pool,
        "partition": config.partition,
        "account": STAGING_SLURM_ACCOUNT,
        "qos": STAGING_SLURM_QOS,
        "phase": phase,
        "session_id": session["session_id"],
        "acceptance_session_id": session["acceptance_session_id"],
        "candidate_sha": session["candidate_sha"],
        "candidate_tree": session["candidate_tree"],
        "owned_jobs": [
            {
                "job_id": job["job_id"],
                "user": job["slurm_user"],
                "account": job["slurm_account"],
                "qos": job["slurm_qos"],
                "name": job["job_name"],
            }
            for job in sorted(session["loom_jobs"], key=lambda row: str(row["job_id"]))
        ],
    }
    payload_raw = _canonical(payload)
    envelope: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "action": TRANSPORT_ACTION,
        "node": config.submit_host,
        "domain": "gb10",
        "sandbox": "staging",
        "candidate_sha": session["candidate_sha"],
        "candidate_tree": session["candidate_tree"],
        "payload_kind": KIND_OBSERVE_REQUEST,
        "payload_sha256": hashlib.sha256(payload_raw).hexdigest(),
        "payload_base64": base64.b64encode(payload_raw).decode(),
        "prior_request_id": None,
    }
    envelope["request_id"] = hashlib.sha256(_canonical(envelope)).hexdigest()
    return _canonical(envelope)


def _default_observe(
    *,
    config: Config,
    session: Mapping[str, Any],
    phase: str,
    run: Run = subprocess.run,
) -> dict[str, Any]:
    envelope = _observer_envelope(config, session, phase=phase)
    try:
        completed = run(
            (
                str(config.node_transport),
                "invoke",
                "--node",
                config.submit_host,
                "--verb",
                "check",
            ),
            input=envelope,
            capture_output=True,
            check=False,
            timeout=120,
            env={"PATH": "/usr/sbin:/usr/bin:/sbin:/bin", "LANG": "C", "LC_ALL": "C"},
        )
    except (OSError, subprocess.SubprocessError) as exc:
        raise AuthorityError("Slurm observer transport failed safely") from exc
    if completed.returncode != 0 or completed.stderr or len(completed.stdout) > MAX_OBSERVER_BYTES:
        raise AuthorityError("Slurm observer transport failed safely")
    try:
        response = json.loads(completed.stdout)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise AuthorityError("Slurm observer transport returned invalid JSON") from exc
    request_id = json.loads(envelope)["request_id"]
    if (
        not isinstance(response, dict)
        or set(response) != {"schema_version", "request_id", "status", "result"}
        or response.get("schema_version") != SCHEMA_VERSION
        or response.get("request_id") != request_id
        or response.get("status") != "succeeded"
        or not isinstance(response.get("result"), dict)
    ):
        raise AuthorityError("Slurm observer transport binding is invalid")
    return dict(response["result"])


def _validate_observation(
    observation: Mapping[str, Any],
    *,
    session: Mapping[str, Any],
) -> dict[str, Any]:
    expected_fields = {
        "schema_version",
        "kind",
        "submit_host",
        "environment",
        "pool",
        "partition",
        "account",
        "qos",
        "phase",
        "session_id",
        "acceptance_session_id",
        "candidate_sha",
        "candidate_tree",
        "observed_at",
        "jobs",
        "snapshot_sha256",
    }
    value = dict(observation)
    if (
        set(value) != expected_fields
        or value.get("schema_version") != SCHEMA_VERSION
        or value.get("kind") != KIND_OBSERVE_RESULT
        or value.get("submit_host") != SUBMIT_HOST
        or value.get("environment") != ENVIRONMENT
        or value.get("pool") != POOL
        or value.get("partition") != PARTITION
        or value.get("account") != STAGING_SLURM_ACCOUNT
        or value.get("qos") != STAGING_SLURM_QOS
        or value.get("phase") not in {"before", "during", "after"}
        or value.get("session_id") != session["session_id"]
        or value.get("acceptance_session_id") != session["acceptance_session_id"]
        or value.get("candidate_sha") != session["candidate_sha"]
        or value.get("candidate_tree") != session["candidate_tree"]
    ):
        raise AuthorityError("Slurm observation binding is invalid")
    _timestamp(value.get("observed_at"), label="Slurm observed_at")
    jobs = value.get("jobs")
    if not isinstance(jobs, list):
        raise AuthorityError("Slurm observation jobs are invalid")
    seen: set[str] = set()
    for row in jobs:
        if not isinstance(row, dict) or set(row) != {
            "job_id",
            "user",
            "account",
            "qos",
            "state",
            "nodes",
            "name",
        }:
            raise AuthorityError("Slurm observation row is invalid")
        job_id = row.get("job_id")
        if not isinstance(job_id, str) or not JOB_ID_RE.fullmatch(job_id) or job_id in seen:
            raise AuthorityError("Slurm observation job ID is invalid")
        seen.add(job_id)
        if row.get("state") not in ACTIVE_SLURM_STATES:
            raise AuthorityError("Slurm observation contains a non-active state")
        for field in ("user", "account", "qos", "nodes", "name"):
            if not isinstance(row.get(field), str) or "\n" in str(row[field]):
                raise AuthorityError("Slurm observation text field is invalid")
    value["jobs"] = sorted(jobs, key=lambda row: str(row["job_id"]))
    unsigned = {key: item for key, item in value.items() if key != "snapshot_sha256"}
    if value.get("snapshot_sha256") != _digest(unsigned):
        raise AuthorityError("Slurm observation digest is invalid")
    return value


def _peer_snapshot(
    observation: Mapping[str, Any],
    *,
    session: Mapping[str, Any],
    require_owned: bool,
) -> list[dict[str, Any]]:
    owned_ids = {str(job["job_id"]) for job in session["loom_jobs"]}
    rows = list(observation["jobs"])
    observed_owned = {str(row["job_id"]) for row in rows if str(row["job_id"]) in owned_ids}
    if require_owned and observed_owned != owned_ids:
        raise AuthorityError("Slurm observer does not contain every owned Loom job")
    return [dict(row) for row in rows if str(row["job_id"]) not in owned_ids]


def observe_slurm(payload: Mapping[str, Any], *, run: Run = subprocess.run) -> dict[str, Any]:
    """Bounded submit-host observation used only through node authority."""
    if _host() != SUBMIT_HOST:
        raise AuthorityError("Slurm observer must run on the fixed submit host")
    expected = {
        "schema_version",
        "kind",
        "source_host",
        "submit_host",
        "environment",
        "pool",
        "partition",
        "account",
        "qos",
        "phase",
        "session_id",
        "acceptance_session_id",
        "candidate_sha",
        "candidate_tree",
        "owned_jobs",
    }
    if (
        set(payload) != expected
        or payload.get("schema_version") != SCHEMA_VERSION
        or payload.get("kind") != KIND_OBSERVE_REQUEST
        or payload.get("source_host") != SOURCE_HOST
        or payload.get("submit_host") != SUBMIT_HOST
        or payload.get("environment") != ENVIRONMENT
        or payload.get("pool") != POOL
        or payload.get("partition") != PARTITION
        or payload.get("account") != STAGING_SLURM_ACCOUNT
        or payload.get("qos") != STAGING_SLURM_QOS
        or payload.get("phase") not in {"before", "during", "after"}
    ):
        raise AuthorityError("Slurm observe request is invalid")
    _validate_uuid(payload.get("session_id"), label="session_id")
    if (
        not isinstance(payload.get("acceptance_session_id"), str)
        or re.fullmatch(r"[0-9a-f]{32}", str(payload["acceptance_session_id"])) is None
    ):
        raise AuthorityError("acceptance session identity is invalid")
    if not isinstance(payload.get("candidate_sha"), str) or not SHA_RE.fullmatch(
        str(payload["candidate_sha"]),
    ):
        raise AuthorityError("Slurm observe candidate SHA is invalid")
    if not isinstance(payload.get("candidate_tree"), str) or not SHA_RE.fullmatch(
        str(payload["candidate_tree"]),
    ):
        raise AuthorityError("Slurm observe candidate tree is invalid")
    owned_jobs = payload.get("owned_jobs")
    if not isinstance(owned_jobs, list) or not owned_jobs:
        raise AuthorityError("Slurm observe owned jobs are invalid")
    expected_owned: dict[str, dict[str, Any]] = {}
    for row in owned_jobs:
        if (
            not isinstance(row, dict)
            or set(row) != {"job_id", "user", "account", "qos", "name"}
            or not isinstance(row.get("job_id"), str)
            or JOB_ID_RE.fullmatch(str(row["job_id"])) is None
            or row.get("user") != STAGING_SLURM_USER
            or row.get("account") != STAGING_SLURM_ACCOUNT
            or row.get("qos") != STAGING_SLURM_QOS
            or not isinstance(row.get("name"), str)
            or SAFE_NAME_RE.fullmatch(str(row["name"])) is None
            or row["job_id"] in expected_owned
        ):
            raise AuthorityError("Slurm observe owned job identity is invalid")
        expected_owned[str(row["job_id"])] = row
    try:
        completed = run(
            (
                "/usr/bin/squeue",
                "--noheader",
                "--partition",
                PARTITION,
                "--states",
                ",".join(sorted(ACTIVE_SLURM_STATES)),
                "--format",
                "%i|%u|%a|%q|%T|%N|%j",
            ),
            check=False,
            capture_output=True,
            timeout=30,
            env={"PATH": "/usr/sbin:/usr/bin:/sbin:/bin", "LANG": "C", "LC_ALL": "C"},
        )
    except (OSError, subprocess.SubprocessError) as exc:
        raise AuthorityError("squeue observation failed safely") from exc
    if completed.returncode != 0 or completed.stderr or len(completed.stdout) > MAX_OBSERVER_BYTES:
        raise AuthorityError("squeue observation failed safely")
    try:
        text = completed.stdout.decode()
    except UnicodeDecodeError as exc:
        raise AuthorityError("squeue observation encoding is invalid") from exc
    jobs: list[dict[str, str]] = []
    for line in text.splitlines():
        fields = line.split("|")
        if len(fields) != 7:
            raise AuthorityError("squeue observation format is invalid")
        job_id, user, account, qos, state, nodes, name = (field.strip() for field in fields)
        if not JOB_ID_RE.fullmatch(job_id) or state not in ACTIVE_SLURM_STATES:
            raise AuthorityError("squeue observation row is invalid")
        jobs.append(
            {
                "job_id": job_id,
                "user": user,
                "account": account,
                "qos": qos,
                "state": state,
                "nodes": nodes,
                "name": name,
            },
        )
    if len({job["job_id"] for job in jobs}) != len(jobs):
        raise AuthorityError("squeue observation contains duplicate jobs")
    observed_owned = {job["job_id"]: job for job in jobs if job["job_id"] in expected_owned}
    if payload["phase"] == "before":
        if set(observed_owned) != set(expected_owned):
            raise AuthorityError("squeue observation is missing an owned staging job")
        for job_id, expected_identity in expected_owned.items():
            actual = observed_owned[job_id]
            if any(
                actual[field] != expected_identity[field]
                for field in ("user", "account", "qos", "name")
            ):
                raise AuthorityError("squeue owned job identity does not match")
    elif observed_owned:
        raise AuthorityError("owned staging job remained active after reclaim")
    result = {
        "schema_version": SCHEMA_VERSION,
        "kind": KIND_OBSERVE_RESULT,
        "submit_host": SUBMIT_HOST,
        "environment": ENVIRONMENT,
        "pool": POOL,
        "partition": PARTITION,
        "account": STAGING_SLURM_ACCOUNT,
        "qos": STAGING_SLURM_QOS,
        "phase": payload["phase"],
        "session_id": payload["session_id"],
        "acceptance_session_id": payload["acceptance_session_id"],
        "candidate_sha": payload["candidate_sha"],
        "candidate_tree": payload["candidate_tree"],
        "observed_at": _iso(_now()),
        "jobs": sorted(jobs, key=lambda row: row["job_id"]),
    }
    result["snapshot_sha256"] = _digest(result)
    return result


def _pressure(
    call: HttpCall,
    *,
    config: Config,
    token: str,
    session: Mapping[str, Any],
    active: bool,
) -> dict[str, Any]:
    body = {
        "prod_pending_count": 1 if active else 0,
        "prod_active_count": 0,
        "prod_capacity_shortfall": 1 if active else 0,
        "source": (
            f"staging-pressure-reclaim acceptance session {session['session_id']}"
            if active
            else f"staging-pressure-reclaim clear session {session['session_id']}"
        ),
        "preemptible": True,
        "grace_period_seconds": 0,
    }
    result = _http_object(
        call,
        method="POST",
        config=config,
        token=token,
        path=f"/admin/worker-pools/{ENVIRONMENT}/{POOL}/prod-pressure",
        body=body,
    )
    pressure = result.get("prod_pressure")
    if not isinstance(pressure, dict):
        raise AuthorityError("pressure endpoint result is invalid")
    if active:
        if pressure.get("has_pressure") is not True:
            raise AuthorityError("pressure endpoint did not activate pressure")
    elif pressure.get("has_pressure") is not False:
        raise AuthorityError("pressure endpoint did not clear pressure")
    return {
        "action": result.get("action"),
        "actuator": result.get("actuator"),
        "environment": result.get("environment"),
        "pool_name": result.get("pool_name"),
        "has_pressure": pressure.get("has_pressure"),
        "new_staging_claims_allowed": result.get("new_staging_claims_allowed"),
        "drain_intent_active": result.get("drain_intent_active"),
        "grace_action": (
            result.get("grace", {}).get("action") if isinstance(result.get("grace"), dict) else None
        ),
    }


def _claim(
    call: HttpCall,
    *,
    config: Config,
    token: str,
    claim: Mapping[str, Any],
) -> HttpResponse:
    return call(
        method="POST",
        base_url=config.control_plane_url,
        token=token,
        path="/trials/claim",
        timeout=config.http_timeout_seconds,
        body={"worker_id": claim["worker_id"], "caps": claim["caps"]},
    )


def _retry_claim_probe(
    call: HttpCall,
    *,
    config: Config,
    token: str,
    claim: Mapping[str, Any],
) -> dict[str, Any]:
    return _http_object(
        call,
        method="POST",
        config=config,
        token=token,
        path=f"/trials/{claim['trial_id']}/retry",
        body={
            "worker_id": claim["worker_id"],
            "failure_reason": "node_setup_health",
            "failure_message": ("staging pressure-reclaim acceptance claim returned to queue"),
            "retry_after_sec": 0,
        },
    )


def _journal_path(session_id: str) -> Path:
    return TRANSACTION_ROOT / f"{session_id}.json"


def _session_path(session_id: str) -> Path:
    return SESSION_ROOT / f"{session_id}.json"


def _receipt_path(session_id: str) -> Path:
    return RECEIPT_ROOT / f"{session_id}.json"


def _signature_path(session_id: str) -> Path:
    return RECEIPT_ROOT / f"{session_id}.sig"


def _write_journal(journal: dict[str, Any], phase: str, **updates: Any) -> None:
    journal.update(updates)
    journal["phase"] = phase
    journal["updated_at"] = _iso(_now())
    _atomic_replace(_journal_path(str(journal["session_id"])), journal)


def _load_or_create_journal(session: Mapping[str, Any]) -> dict[str, Any]:
    path = _journal_path(str(session["session_id"]))
    if path.exists():
        journal = _read_json(path, expected_kind=KIND_TRANSACTION)
        if (
            journal.get("session_id") != session["session_id"]
            or journal.get("session_sha256") != _digest(session)
            or journal.get("candidate_sha") != session["candidate_sha"]
            or journal.get("candidate_tree") != session["candidate_tree"]
            or journal.get("environment") != ENVIRONMENT
            or journal.get("pool") != POOL
        ):
            raise AuthorityError("existing transaction does not match session")
        return journal
    now = _now()
    journal = {
        "schema_version": SCHEMA_VERSION,
        "kind": KIND_TRANSACTION,
        "session_id": session["session_id"],
        "session_sha256": _digest(session),
        "candidate_sha": session["candidate_sha"],
        "candidate_tree": session["candidate_tree"],
        "environment": ENVIRONMENT,
        "pool": POOL,
        "phase": "registered",
        "created_at": _iso(now),
        "updated_at": _iso(now),
        "evidence": {},
    }
    _atomic_replace(path, journal)
    return journal


def _wait_until(
    probe: Callable[[], Any],
    *,
    timeout: float,
    interval: float,
    clock: Clock,
    sleep: Sleep,
    label: str,
) -> Any:
    deadline = clock() + timedelta(seconds=timeout)
    last_error: AuthorityError | None = None
    while clock() <= deadline:
        try:
            return probe()
        except AuthorityError as exc:
            last_error = exc
        sleep(interval)
    if last_error is not None:
        raise AuthorityError(f"{label} timed out: {last_error}") from last_error
    raise AuthorityError(f"{label} timed out")


def _sign_receipt(
    receipt: Mapping[str, Any],
    *,
    config: Config,
    run: Run = subprocess.run,
) -> bytes:
    try:
        completed = run(
            (
                "/usr/bin/openssl",
                "pkeyutl",
                "-sign",
                "-rawin",
                "-inkey",
                str(config.private_key),
            ),
            input=_canonical(dict(receipt)),
            capture_output=True,
            check=False,
            timeout=30,
            env={"PATH": "/usr/sbin:/usr/bin:/sbin:/bin", "LANG": "C", "LC_ALL": "C"},
        )
    except (OSError, subprocess.SubprocessError) as exc:
        raise AuthorityError("receipt signing failed safely") from exc
    if completed.returncode != 0 or completed.stderr or len(completed.stdout) != 64:
        raise AuthorityError("receipt signing failed safely")
    return completed.stdout


def _verify_signature_files(
    receipt_path: Path,
    signature_path: Path,
    *,
    config: Config,
    run: Run = subprocess.run,
) -> None:
    try:
        completed = run(
            (
                "/usr/bin/openssl",
                "pkeyutl",
                "-verify",
                "-rawin",
                "-pubin",
                "-inkey",
                str(config.public_key),
                "-sigfile",
                str(signature_path),
                "-in",
                str(receipt_path),
            ),
            capture_output=True,
            check=False,
            timeout=30,
            env={"PATH": "/usr/sbin:/usr/bin:/sbin:/bin", "LANG": "C", "LC_ALL": "C"},
        )
    except (OSError, subprocess.SubprocessError) as exc:
        raise AuthorityError("receipt signature verification failed safely") from exc
    if completed.returncode != 0:
        raise AuthorityError("receipt signature verification failed safely")


def _next_sequence(session: Mapping[str, Any]) -> int:
    path = HIGH_WATER_ROOT / f"{POOL}.json"
    if not path.exists():
        return 1
    high = _read_json(path, expected_kind=KIND_HIGH_WATER)
    if (
        high.get("schema_version") != SCHEMA_VERSION
        or high.get("environment") != ENVIRONMENT
        or high.get("pool") != POOL
        or not isinstance(high.get("sequence"), int)
        or int(high["sequence"]) < 1
    ):
        raise AuthorityError("pressure-reclaim high-water is invalid")
    if high.get("session_id") == session["session_id"]:
        return int(high["sequence"])
    prior_created = _timestamp(
        high.get("session_created_at"), label="high-water session_created_at"
    )
    current_created = _timestamp(session.get("created_at"), label="session created_at")
    if current_created <= prior_created:
        raise AuthorityError("session creation time does not advance pool high-water")
    return int(high["sequence"]) + 1


def _publish_receipt(
    session: Mapping[str, Any],
    evidence: Mapping[str, Any],
    *,
    config: Config,
    clock: Clock,
    sign_run: Run = subprocess.run,
) -> dict[str, Any]:
    sequence = _next_sequence(session)
    issued_at = clock()
    if issued_at >= _timestamp(session.get("expires_at"), label="session expires_at"):
        raise AuthorityError("session expired before receipt publication")
    receipt_path = _receipt_path(str(session["session_id"]))
    signature_path = _signature_path(str(session["session_id"]))
    if receipt_path.exists():
        receipt = _read_json(receipt_path, expected_kind=KIND_RECEIPT)
        if (
            receipt.get("sequence") != sequence
            or receipt.get("session_id") != session["session_id"]
            or receipt.get("session_sha256") != _digest(session)
            or receipt.get("candidate_sha") != session["candidate_sha"]
            or receipt.get("candidate_tree") != session["candidate_tree"]
            or receipt.get("evidence") != dict(evidence)
        ):
            raise AuthorityError("existing immutable receipt does not match transaction")
    else:
        receipt = {
            "schema_version": SCHEMA_VERSION,
            "kind": KIND_RECEIPT,
            "environment": ENVIRONMENT,
            "pool": POOL,
            "partition": PARTITION,
            "source_host": SOURCE_HOST,
            "submit_host": SUBMIT_HOST,
            "sequence": sequence,
            "session_id": session["session_id"],
            "acceptance_session_id": session["acceptance_session_id"],
            "session_sha256": _digest(session),
            "candidate_sha": session["candidate_sha"],
            "candidate_tree": session["candidate_tree"],
            "issued_at": _iso(issued_at),
            "evidence": dict(evidence),
        }
    signature = _sign_receipt(receipt, config=config, run=sign_run)
    _write_once(receipt_path, receipt)
    public_key_bytes = _read_secure_bytes(config.public_key, max_bytes=16 * 1024)
    signature_payload = {
        "schema_version": SCHEMA_VERSION,
        "kind": f"{KIND_RECEIPT}.signature",
        "session_id": session["session_id"],
        "receipt_sha256": _digest(receipt),
        "key_id": hashlib.sha256(public_key_bytes).hexdigest(),
        "signature_base64": base64.b64encode(signature).decode(),
        "signature_sha256": hashlib.sha256(signature).hexdigest(),
    }
    _write_once(signature_path, signature_payload)
    _prepare_private_directory(config.published_root)
    published_session_root = config.published_root / str(
        session["acceptance_session_id"],
    )
    _prepare_private_directory(published_session_root)
    published_path = published_session_root / f"{session['session_id']}.json"
    published = {
        "schema_version": SCHEMA_VERSION,
        "kind": KIND_PUBLISHED,
        "acceptance_session_id": session["acceptance_session_id"],
        "authority_session_id": session["session_id"],
        "candidate_sha": session["candidate_sha"],
        "candidate_tree": session["candidate_tree"],
        "source_host": SOURCE_HOST,
        "published_at": receipt["issued_at"],
        "receipt": receipt,
        "signature": signature_payload,
    }
    _write_once(published_path, published)
    high_water = {
        "schema_version": SCHEMA_VERSION,
        "kind": KIND_HIGH_WATER,
        "environment": ENVIRONMENT,
        "pool": POOL,
        "sequence": sequence,
        "session_id": session["session_id"],
        "session_created_at": session["created_at"],
        "acceptance_session_id": session["acceptance_session_id"],
        "candidate_sha": session["candidate_sha"],
        "candidate_tree": session["candidate_tree"],
        "receipt_path": str(receipt_path),
        "receipt_sha256": _digest(receipt),
        "updated_at": _iso(clock()),
    }
    _atomic_replace(HIGH_WATER_ROOT / f"{POOL}.json", high_water)
    _atomic_replace(CURRENT_PATH, high_water)
    return {
        "status": "committed",
        "sequence": sequence,
        "session_id": session["session_id"],
        "receipt_path": str(receipt_path),
        "signature_path": str(signature_path),
        "published_path": str(published_path),
        "receipt_sha256": _digest(receipt),
    }


def run_session(
    session_id: str,
    *,
    config: Config,
    http_call: HttpCall = _default_http_call,
    observe_call: ObserveCall = _default_observe,
    clock: Clock = _now,
    sleep: Sleep = time.sleep,
    sign_run: Run = subprocess.run,
) -> dict[str, Any]:
    """Run or roll forward one pressure-reclaim acceptance transaction."""
    _require_root()
    _require_source_host(config)
    _prepare_state()
    session_id = _validate_uuid(session_id, label="session_id")
    admin_token = _load_secret(config.admin_secret_file)
    worker_token = _load_secret(config.worker_secret_file)
    with _locked():
        session = _validate_session(
            _read_json(_session_path(session_id), expected_kind=KIND_SESSION),
            now=clock(),
            max_age_seconds=config.max_session_age_seconds,
        )
        journal = _load_or_create_journal(session)
        evidence = journal.get("evidence")
        if not isinstance(evidence, dict):
            raise AuthorityError("transaction evidence is invalid")
        phase = str(journal.get("phase"))
        if phase == "committed":
            return verify_receipt(session_id, config=config)

        if phase == "registered":
            registry = _http_object(
                http_call,
                method="GET",
                config=config,
                token=admin_token,
                path="/admin/slurm-worker-jobs/status",
            )
            active_jobs = _active_pool_jobs(registry, session)
            interrupted_before = _trial(
                http_call,
                config=config,
                token=admin_token,
                binding=session["interrupted_trial"],
            )
            claim_before = _trial(
                http_call,
                config=config,
                token=admin_token,
                binding=session["claim_probe"],
            )
            if interrupted_before.get("state") not in {"claimed", "running"}:
                raise AuthorityError("interrupted acceptance trial is not in-flight")
            if claim_before.get("state") != "queued":
                raise AuthorityError("claim probe acceptance trial is not queued")
            before = _validate_observation(
                observe_call(config=config, session=session, phase="before"),
                session=session,
            )
            peers = _peer_snapshot(before, session=session, require_owned=True)
            evidence.update(
                {
                    "registry_before": active_jobs,
                    "interrupted_trial_before": _trial_evidence(interrupted_before),
                    "claim_probe_before": _trial_evidence(claim_before),
                    "slurm_before": before,
                    "foreign_peer_snapshot": peers,
                },
            )
            _write_journal(journal, "preflight-proven", evidence=evidence)
            phase = "preflight-proven"

        if phase == "preflight-proven":
            pressure_on = _pressure(
                http_call,
                config=config,
                token=admin_token,
                session=session,
                active=True,
            )
            evidence["pressure_on"] = pressure_on
            _write_journal(journal, "pressure-posted", evidence=evidence)
            phase = "pressure-posted"

        if phase == "pressure-posted":
            fenced = _claim(
                http_call,
                config=config,
                token=worker_token,
                claim=session["claim_probe"],
            )
            if fenced.status != 204 or fenced.body is not None:
                raise AuthorityError("claim was not fenced under staging pressure")
            evidence["claim_fence"] = {"status": 204, "trial_id": None}
            _write_journal(journal, "claim-fenced", evidence=evidence)
            phase = "claim-fenced"

        if phase == "claim-fenced":

            def terminal_probe() -> tuple[list[dict[str, Any]], dict[str, Any]]:
                registry = _http_object(
                    http_call,
                    method="GET",
                    config=config,
                    token=admin_token,
                    path="/admin/slurm-worker-jobs/status",
                )
                jobs = _terminal_owned_jobs(registry, session)
                trial = _trial(
                    http_call,
                    config=config,
                    token=admin_token,
                    binding=session["interrupted_trial"],
                )
                if (
                    trial.get("state") != "queued"
                    or trial.get("failure_reason") != "prod_capacity_pressure"
                ):
                    raise AuthorityError(
                        "interrupted acceptance trial is not retryably attributed",
                    )
                return jobs, trial

            terminal_jobs, retryable_trial = _wait_until(
                terminal_probe,
                timeout=config.terminal_timeout_seconds,
                interval=config.poll_interval_seconds,
                clock=clock,
                sleep=sleep,
                label="terminal reclaim",
            )
            during = _validate_observation(
                observe_call(config=config, session=session, phase="during"),
                session=session,
            )
            during_peers = _peer_snapshot(during, session=session, require_owned=False)
            if during_peers != evidence["foreign_peer_snapshot"]:
                raise AuthorityError("non-Loom Slurm peer changed during reclaim")
            evidence.update(
                {
                    "registry_terminal": terminal_jobs,
                    "interrupted_trial_retryable": _trial_evidence(retryable_trial),
                    "slurm_during": during,
                },
            )
            _write_journal(journal, "terminal-proven", evidence=evidence)
            phase = "terminal-proven"

        if phase == "terminal-proven":
            pressure_off = _pressure(
                http_call,
                config=config,
                token=admin_token,
                session=session,
                active=False,
            )
            evidence["pressure_off"] = pressure_off
            _write_journal(journal, "pressure-cleared", evidence=evidence)
            phase = "pressure-cleared"

        if phase == "pressure-cleared":
            claim_state = _trial(
                http_call,
                config=config,
                token=admin_token,
                binding=session["claim_probe"],
            )
            if claim_state.get("state") == "queued":
                recovered = _claim(
                    http_call,
                    config=config,
                    token=worker_token,
                    claim=session["claim_probe"],
                )
                if (
                    recovered.status != 200
                    or recovered.body is None
                    or recovered.body.get("trial_id") != session["claim_probe"]["trial_id"]
                    or recovered.body.get("state") != "claimed"
                ):
                    raise AuthorityError("claim did not recover after pressure clear")
                evidence["claim_recovered"] = {
                    "trial_id": recovered.body["trial_id"],
                    "state": recovered.body["state"],
                }
            elif claim_state.get("state") != "claimed":
                raise AuthorityError("claim probe trial recovery state is invalid")
            else:
                evidence["claim_recovered"] = {
                    "trial_id": session["claim_probe"]["trial_id"],
                    "state": "claimed",
                }
            _write_journal(journal, "claim-recovered", evidence=evidence)
            phase = "claim-recovered"

        if phase == "claim-recovered":
            claim_state = _trial(
                http_call,
                config=config,
                token=admin_token,
                binding=session["claim_probe"],
            )
            if claim_state.get("state") == "claimed":
                retry_result = _retry_claim_probe(
                    http_call,
                    config=config,
                    token=worker_token,
                    claim=session["claim_probe"],
                )
                if retry_result.get("trial_id") != session["claim_probe"]["trial_id"]:
                    raise AuthorityError("claim probe retry result is invalid")
            elif claim_state.get("state") != "queued":
                raise AuthorityError("claim probe retry recovery state is invalid")
            queued = _wait_until(
                lambda: _trial(
                    http_call,
                    config=config,
                    token=admin_token,
                    binding=session["claim_probe"],
                ),
                timeout=config.retry_timeout_seconds,
                interval=config.poll_interval_seconds,
                clock=clock,
                sleep=sleep,
                label="claim probe requeue",
            )
            if queued.get("state") != "queued":
                raise AuthorityError("claim probe was not returned to queued state")
            evidence["claim_probe_requeued"] = _trial_evidence(queued)
            _write_journal(journal, "recovery-proven", evidence=evidence)
            phase = "recovery-proven"

        if phase == "recovery-proven":
            after = _validate_observation(
                observe_call(config=config, session=session, phase="after"),
                session=session,
            )
            after_peers = _peer_snapshot(after, session=session, require_owned=False)
            if after_peers != evidence["foreign_peer_snapshot"]:
                raise AuthorityError("non-Loom Slurm peer changed after reclaim")
            registry_after = _http_object(
                http_call,
                method="GET",
                config=config,
                token=admin_token,
                path="/admin/slurm-worker-jobs/status",
            )
            _terminal_owned_jobs(registry_after, session)
            evidence["slurm_after"] = after
            evidence["foreign_peer_zero_impact"] = True
            _write_journal(journal, "foreign-zero-impact", evidence=evidence)
            phase = "foreign-zero-impact"

        if phase == "foreign-zero-impact":
            result = _publish_receipt(
                session,
                evidence,
                config=config,
                clock=clock,
                sign_run=sign_run,
            )
            _write_journal(
                journal,
                "committed",
                evidence=evidence,
                receipt_sha256=result["receipt_sha256"],
                sequence=result["sequence"],
            )
            return result

        raise AuthorityError(f"transaction phase is unsupported: {phase}")


def verify_receipt(
    session_id: str,
    *,
    config: Config,
    verify_run: Run = subprocess.run,
) -> dict[str, Any]:
    _require_root()
    _prepare_state()
    session_id = _validate_uuid(session_id, label="session_id")
    session = _read_json(_session_path(session_id), expected_kind=KIND_SESSION)
    receipt_path = _receipt_path(session_id)
    signature_path = _signature_path(session_id)
    receipt = _read_json(receipt_path, expected_kind=KIND_RECEIPT)
    signature_artifact = _read_json(signature_path, expected_kind=f"{KIND_RECEIPT}.signature")
    if (
        set(receipt)
        != {
            "schema_version",
            "kind",
            "environment",
            "pool",
            "partition",
            "source_host",
            "submit_host",
            "sequence",
            "session_id",
            "acceptance_session_id",
            "session_sha256",
            "candidate_sha",
            "candidate_tree",
            "issued_at",
            "evidence",
        }
        or set(signature_artifact)
        != {
            "schema_version",
            "kind",
            "session_id",
            "receipt_sha256",
            "key_id",
            "signature_base64",
            "signature_sha256",
        }
        or receipt.get("session_id") != session_id
        or receipt.get("acceptance_session_id") != session.get("acceptance_session_id")
        or receipt.get("session_sha256") != _digest(session)
        or receipt.get("candidate_sha") != session.get("candidate_sha")
        or receipt.get("candidate_tree") != session.get("candidate_tree")
        or receipt.get("environment") != ENVIRONMENT
        or receipt.get("pool") != POOL
        or receipt.get("partition") != PARTITION
        or signature_artifact.get("session_id") != session_id
        or signature_artifact.get("receipt_sha256") != _digest(receipt)
        or signature_artifact.get("key_id")
        != hashlib.sha256(
            _read_secure_bytes(config.public_key, max_bytes=16 * 1024),
        ).hexdigest()
    ):
        raise AuthorityError("receipt binding is invalid")
    try:
        signature = base64.b64decode(
            str(signature_artifact["signature_base64"]),
            validate=True,
        )
    except (KeyError, ValueError) as exc:
        raise AuthorityError("receipt signature encoding is invalid") from exc
    if len(signature) != 64 or hashlib.sha256(signature).hexdigest() != signature_artifact.get(
        "signature_sha256"
    ):
        raise AuthorityError("receipt signature digest is invalid")
    # Materialize only the signature bytes in a private, transaction-owned file
    # because the persisted .sig artifact is canonical JSON rather than raw DER.
    raw_signature = RECEIPT_ROOT / f".{session_id}.verify.{uuid.uuid4().hex}.sig"
    try:
        descriptor = os.open(
            raw_signature,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0),
            0o600,
        )
        try:
            os.write(descriptor, signature)
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
        _verify_signature_files(
            receipt_path,
            raw_signature,
            config=config,
            run=verify_run,
        )
    finally:
        raw_signature.unlink(missing_ok=True)
    published_path = (
        config.published_root / str(session["acceptance_session_id"]) / f"{session_id}.json"
    )
    published = _read_json(published_path, expected_kind=KIND_PUBLISHED)
    if published != {
        "schema_version": SCHEMA_VERSION,
        "kind": KIND_PUBLISHED,
        "acceptance_session_id": session["acceptance_session_id"],
        "authority_session_id": session_id,
        "candidate_sha": session["candidate_sha"],
        "candidate_tree": session["candidate_tree"],
        "source_host": SOURCE_HOST,
        "published_at": receipt["issued_at"],
        "receipt": receipt,
        "signature": signature_artifact,
    }:
        raise AuthorityError("published receipt binding is invalid")
    return {
        "status": "verified",
        "session_id": session_id,
        "sequence": receipt["sequence"],
        "receipt_path": str(receipt_path),
        "published_path": str(published_path),
        "receipt_sha256": _digest(receipt),
    }


def verify_current(*, config: Config, verify_run: Run = subprocess.run) -> dict[str, Any]:
    _require_root()
    _prepare_state()
    current = _read_json(CURRENT_PATH, expected_kind=KIND_HIGH_WATER)
    if (
        current.get("environment") != ENVIRONMENT
        or current.get("pool") != POOL
        or current.get("receipt_path") != str(_receipt_path(str(current.get("session_id"))))
    ):
        raise AuthorityError("current receipt pointer is invalid")
    verified = verify_receipt(
        str(current["session_id"]),
        config=config,
        verify_run=verify_run,
    )
    if verified["sequence"] != current.get("sequence") or verified["receipt_sha256"] != current.get(
        "receipt_sha256"
    ):
        raise AuthorityError("current receipt high-water binding is invalid")
    return verified


def _openssl(
    run: Run,
    argv: tuple[str, ...],
    *,
    input_bytes: bytes | None = None,
    maximum: int = MAX_KEY_BYTES,
) -> bytes:
    try:
        completed = run(
            argv,
            input=input_bytes,
            capture_output=True,
            check=False,
            timeout=30,
            env={"PATH": "/usr/sbin:/usr/bin:/sbin:/bin", "LANG": "C", "LC_ALL": "C"},
        )
    except (OSError, subprocess.SubprocessError) as exc:
        raise AuthorityError("authority key operation failed safely") from exc
    if (
        completed.returncode != 0
        or completed.stderr
        or not completed.stdout
        or len(completed.stdout) > maximum
    ):
        raise AuthorityError("authority key operation failed safely")
    return completed.stdout


def _require_ed25519_der(payload: bytes) -> None:
    if len(payload) != len(ED25519_SPKI_PREFIX) + 32 or not payload.startswith(
        ED25519_SPKI_PREFIX
    ):
        raise AuthorityError("authority key pair must be Ed25519")


def _derived_public(config: Config, *, run: Run) -> tuple[bytes, bytes]:
    public_pem = _openssl(
        run,
        (
            "/usr/bin/openssl",
            "pkey",
            "-in",
            str(config.private_key),
            "-pubout",
        ),
    )
    public_der = _openssl(
        run,
        (
            "/usr/bin/openssl",
            "pkey",
            "-in",
            str(config.private_key),
            "-pubout",
            "-outform",
            "DER",
        ),
        maximum=4096,
    )
    _require_ed25519_der(public_der)
    return public_pem, public_der


def _installed_public_der(config: Config, *, run: Run) -> bytes:
    public_der = _openssl(
        run,
        (
            "/usr/bin/openssl",
            "pkey",
            "-pubin",
            "-in",
            str(config.public_key),
            "-pubout",
            "-outform",
            "DER",
        ),
        maximum=4096,
    )
    _require_ed25519_der(public_der)
    return public_der


def _verify_keypair(config: Config, *, run: Run) -> None:
    for path, label in (
        (config.private_key, "authority private key"),
        (config.public_key, "authority public key"),
    ):
        _verify_trusted_parent_chain(path, label=label)
        _read_secure_bytes(
            path,
            max_bytes=MAX_KEY_BYTES,
            allowed_modes=frozenset({0o600}),
        )
    _public_pem, derived_der = _derived_public(config, run=run)
    if _installed_public_der(config, run=run) != derived_der:
        raise AuthorityError("authority private/public key pair does not match")
    challenge = (
        b"loom-staging-pressure-reclaim-authority-key-readback-v1\0"
        + secrets.token_bytes(32)
    )
    signature = _openssl(
        run,
        (
            "/usr/bin/openssl",
            "pkeyutl",
            "-sign",
            "-rawin",
            "-inkey",
            str(config.private_key),
        ),
        input_bytes=challenge,
        maximum=64,
    )
    if len(signature) != 64:
        raise AuthorityError("authority key signing readback failed safely")
    signature_path = config.private_key.parent / (
        f".authority-key-readback-{os.getpid()}-{secrets.token_hex(8)}.sig"
    )
    descriptor = os.open(
        signature_path,
        os.O_WRONLY
        | os.O_CREAT
        | os.O_EXCL
        | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_NOFOLLOW", 0),
        0o600,
    )
    try:
        try:
            os.fchown(descriptor, 0, 0)
            remaining = memoryview(signature)
            while remaining:
                written = os.write(descriptor, remaining)
                if written <= 0:
                    raise AuthorityError("authority key readback staging failed safely")
                remaining = remaining[written:]
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
        _fsync_directory(signature_path.parent)
        verified = _openssl(
            run,
            (
                "/usr/bin/openssl",
                "pkeyutl",
                "-verify",
                "-rawin",
                "-pubin",
                "-inkey",
                str(config.public_key),
                "-sigfile",
                str(signature_path),
            ),
            input_bytes=challenge,
            maximum=256,
        )
        if verified.strip() not in {b"Signature Verified Successfully", b"Signature Verified"}:
            raise AuthorityError("authority key signing readback failed safely")
    finally:
        signature_path.unlink(missing_ok=True)
        _fsync_directory(signature_path.parent)


def _key_exists(path: Path, *, label: str) -> bool:
    try:
        path.lstat()
    except FileNotFoundError:
        return False
    except OSError as exc:
        raise AuthorityError(f"{label} is unavailable") from exc
    return True


def _converge_keypair(config: Config, *, run: Run) -> str:
    for path, label in (
        (config.private_key, "authority private key"),
        (config.public_key, "authority public key"),
    ):
        _verify_trusted_parent_chain(path, label=label)
    private_exists = _key_exists(config.private_key, label="authority private key")
    public_exists = _key_exists(config.public_key, label="authority public key")
    if public_exists and not private_exists:
        raise AuthorityError("authority public key exists without its private key")
    status = "existing-keypair-verified"
    if not private_exists:
        generated = _openssl(
            run,
            (
                "/usr/bin/openssl",
                "genpkey",
                "-algorithm",
                "Ed25519",
            ),
        )
        installed = _install_key_no_replace(
            config.private_key,
            generated,
            mode=0o600,
        )
        status = "bootstrapped" if installed else "existing-keypair-verified"
    _read_secure_bytes(
        config.private_key,
        max_bytes=MAX_KEY_BYTES,
        allowed_modes=frozenset({0o600}),
    )
    public_pem, _public_der = _derived_public(config, run=run)
    if not public_exists:
        installed = _install_key_no_replace(
            config.public_key,
            public_pem,
            mode=0o600,
        )
        if status != "bootstrapped" and installed:
            status = "private-key-roll-forward"
    _verify_keypair(config, run=run)
    return status


def _validate_external_secret_prerequisites(config: Config) -> None:
    _load_secret(config.admin_secret_file)
    _load_secret(config.worker_secret_file)


def check_authority(*, config: Config, run: Run = subprocess.run) -> dict[str, Any]:
    _require_root()
    _require_source_host(config)
    _validate_external_secret_prerequisites(config)
    _verify_keypair(config, run=run)
    return {
        "status": "verified",
        "keypair": "ed25519",
        "external_secret_prerequisites": "verified",
    }


def bootstrap(*, config: Config, execute: bool, run: Run = subprocess.run) -> dict[str, Any]:
    _require_root()
    _require_source_host(config)
    if not execute:
        return {
            "status": "planned",
            "state_root": str(STATE_ROOT),
            "public_key": str(config.public_key),
            "private_key": str(config.private_key),
            "external_secret_prerequisites": "required",
        }
    _validate_external_secret_prerequisites(config)
    _prepare_state()
    _prepare_private_directory(config.private_key.parent)
    status = _converge_keypair(config, run=run)
    return {
        "status": status,
        "keypair": "ed25519",
        "external_secret_prerequisites": "verified",
    }


def _stdin_object() -> dict[str, Any]:
    raw = sys.stdin.buffer.read(MAX_JSON_BYTES + 1)
    if len(raw) > MAX_JSON_BYTES:
        raise AuthorityError("stdin payload is too large")
    try:
        parsed = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise AuthorityError("stdin payload is invalid JSON") from exc
    if not isinstance(parsed, dict) or raw != _canonical(parsed):
        raise AuthorityError("stdin payload must be canonical JSON")
    return parsed


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(allow_abbrev=False)
    subparsers = parser.add_subparsers(dest="command", required=True)
    bootstrap_parser = subparsers.add_parser("bootstrap", allow_abbrev=False)
    bootstrap_parser.add_argument("--execute", action="store_true")
    subparsers.add_parser("check", allow_abbrev=False)
    register = subparsers.add_parser("register-session", allow_abbrev=False)
    register.add_argument("--source", required=True, type=Path)
    run = subparsers.add_parser("run", allow_abbrev=False)
    run.add_argument("--session-id", required=True)
    verify = subparsers.add_parser("verify", allow_abbrev=False)
    verify.add_argument("--session-id", required=True)
    subparsers.add_parser("verify-current", allow_abbrev=False)
    subparsers.add_parser("observe-slurm", allow_abbrev=False)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        if args.command == "observe-slurm":
            result = observe_slurm(_stdin_object())
        else:
            config = _load_config(CONFIG_PATH)
            if args.command == "bootstrap":
                result = bootstrap(config=config, execute=bool(args.execute))
            elif args.command == "check":
                result = check_authority(config=config)
            elif args.command == "register-session":
                result = register_session(args.source, config=config)
            elif args.command == "run":
                result = run_session(args.session_id, config=config)
            elif args.command == "verify":
                result = verify_receipt(args.session_id, config=config)
            elif args.command == "verify-current":
                result = verify_current(config=config)
            else:
                raise AuthorityError("unknown command")
    except AuthorityError as exc:
        print(f"staging-pressure-reclaim-authority: {exc}", file=sys.stderr)
        return 1
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
