"""Persistent fair-share capacity broker for disposable developer sandboxes.

The broker is intentionally independent from every sandbox Control Plane.  One
submit-host process owns the SQLite authority and emits candidate-bound grant
handoffs that a sandbox-specific adapter may apply to its local autoscaler
policy.  Sandboxes never mutate the shared budget directly.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sqlite3
import stat
import sys
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import asdict, dataclass
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from pathlib import Path
from typing import Any, ClassVar, cast
from uuid import uuid4

_SHA_RE = re.compile(r"[0-9a-f]{40}")
_SANDBOX_ID_RE = re.compile(r"[a-z][a-z0-9-]{0,47}")
_POOL_RE = re.compile(r"[a-z][a-z0-9-]{0,31}")
_IDEMPOTENCY_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:-]{0,127}")
_PURPOSE_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9 ._:/()#,+-]{0,199}")
_ADMISSION_FENCE_RE = re.compile(r"[0-9a-f]{32}")
_SECRET_HINT_RE = re.compile(
    r"(?:bearer\s+|password|private[_ -]?key|api[_ -]?key|secret|token|"
    r"\bsk-[A-Za-z0-9_-]{8,})",
    re.IGNORECASE,
)
_SCHEMA_VERSION = "2"
_PREVIOUS_SCHEMA_VERSION = "1"
_MAX_TTL_SECONDS = 24 * 60 * 60
_MAX_REQUEST_SLOTS = 10_000
_MAX_BUDGET_SLOTS = 100_000
_OBSERVATION_MAX_AGE = timedelta(seconds=60)
_OBSERVATION_MAX_FUTURE_SKEW = timedelta(seconds=30)
_OBSERVATION_LEASE_STATES = {"active", "retiring", "retired"}
_OBSERVATION_FIELDS = {
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
_ACCEPTANCE_META_KEY = "acceptance_capacity_contract"
_ENVIRONMENT_ADMISSION_FENCE_PREFIX = "capacity_environment_admission_fence:"
_ACCEPTANCE_SESSION_RE = re.compile(r"[0-9a-f]{32}")
_ACCEPTANCE_PHASE_RE = re.compile(r"[a-z][a-z0-9_]{0,63}")
_ACCEPTANCE_POOLS = ("gb10", "oldlab")


class BrokerError(ValueError):
    """A bounded, secret-free broker failure."""


class SandboxId(str):
    """Strict dynamic identifier for one registered sandbox runtime."""

    QIANYI: ClassVar[SandboxId]
    HONGJIAN: ClassVar[SandboxId]
    DEVANSH: ClassVar[SandboxId]

    def __new__(cls, value: str) -> SandboxId:
        if not isinstance(value, str) or _SANDBOX_ID_RE.fullmatch(value) is None:
            raise ValueError(
                "sandbox id must match ^[a-z][a-z0-9-]{0,47}$",
            )
        return str.__new__(cls, value)

    @property
    def value(self) -> str:
        return str(self)

    @property
    def environment(self) -> str:
        return f"sandbox-{self.value}"


class RequestState(StrEnum):
    PENDING = "pending"
    ACTIVE = "active"
    DRAINING = "draining"
    TERMINAL = "terminal"


@dataclass(frozen=True, slots=True)
class CapacityRequest:
    id: str
    sandbox: SandboxId
    candidate_sha: str
    pool: str
    min_slots: int
    target_slots: int
    ttl_seconds: int
    purpose: str
    preemptible: bool
    idempotency_key: str
    state: RequestState
    created_at: str
    expires_at: str
    cancel_requested: bool = False
    terminal_reason: str | None = None

    def public_dict(self) -> dict[str, object]:
        value = asdict(self)
        value["sandbox"] = self.sandbox.value
        value["state"] = self.state.value
        return value


@dataclass(frozen=True, slots=True)
class CapacityLease:
    request_id: str
    lease_epoch: int
    granted_slots: int
    pending_slots: int
    active_slots: int
    draining_slots: int
    terminal_slots: int
    state: RequestState
    last_observed_at: str | None
    last_observation_sequence: int | None
    last_observation_digest: str | None
    last_policy_lease_state: str | None
    updated_at: str

    @property
    def committed_slots(self) -> int:
        return max(
            self.granted_slots,
            self.pending_slots + self.active_slots + self.draining_slots,
        )

    def public_dict(self) -> dict[str, object]:
        value = asdict(self)
        value["state"] = self.state.value
        value["committed_slots"] = self.committed_slots
        return value


@dataclass(frozen=True, slots=True)
class LeaseObservation:
    sandbox: SandboxId
    pool_name: str
    candidate_sha: str
    request_id: str
    lease_epoch: int
    capacity_lease_state: str
    observed_at: str
    observation_sequence: int
    pending_slots: int
    active_slots: int
    draining_slots: int
    terminal_slots: int
    payload_sha256: str

    @classmethod
    def from_mapping(cls, value: Mapping[str, object]) -> LeaseObservation:
        if set(value) != _OBSERVATION_FIELDS:
            raise BrokerError("observation fields do not match the closed schema")
        try:
            sandbox = SandboxId(_exact_text(value.get("sandbox"), "sandbox"))
        except ValueError as exc:
            raise BrokerError("observation sandbox id is invalid") from exc
        pool_name = _exact_text(value.get("pool_name"), "pool_name")
        if _POOL_RE.fullmatch(pool_name) is None:
            raise BrokerError("observation pool_name is invalid")
        candidate_sha = _exact_text(value.get("candidate_sha"), "candidate_sha")
        if _SHA_RE.fullmatch(candidate_sha) is None:
            raise BrokerError("observation candidate_sha is invalid")
        request_id = _exact_text(value.get("request_id"), "request_id")
        capacity_lease_state = _exact_text(
            value.get("capacity_lease_state"),
            "capacity_lease_state",
        )
        if capacity_lease_state not in _OBSERVATION_LEASE_STATES:
            raise BrokerError("observation capacity_lease_state is invalid")
        observed_at = _exact_text(value.get("observed_at"), "observed_at")
        if _timestamp(_parse_timestamp(observed_at)) != observed_at:
            raise BrokerError("observation observed_at must be canonical UTC")
        observation_sequence = _nonnegative_int(
            value.get("observation_sequence"),
            "observation_sequence",
        )
        if observation_sequence < 1:
            raise BrokerError("observation_sequence must be positive")
        payload_sha256 = _exact_text(value.get("payload_sha256"), "payload_sha256")
        if re.fullmatch(r"[0-9a-f]{64}", payload_sha256) is None:
            raise BrokerError("observation payload_sha256 is invalid")
        unsigned = {key: value[key] for key in value if key != "payload_sha256"}
        expected_digest = hashlib.sha256(
            json.dumps(unsigned, sort_keys=True, separators=(",", ":")).encode(),
        ).hexdigest()
        if payload_sha256 != expected_digest:
            raise BrokerError("observation payload_sha256 does not match its payload")
        return cls(
            sandbox=sandbox,
            pool_name=pool_name,
            candidate_sha=candidate_sha,
            request_id=request_id,
            lease_epoch=_nonnegative_int(value.get("lease_epoch"), "lease_epoch"),
            capacity_lease_state=capacity_lease_state,
            observed_at=observed_at,
            observation_sequence=observation_sequence,
            pending_slots=_nonnegative_int(value.get("pending_slots"), "pending_slots"),
            active_slots=_nonnegative_int(value.get("active_slots"), "active_slots"),
            draining_slots=_nonnegative_int(value.get("draining_slots"), "draining_slots"),
            terminal_slots=_nonnegative_int(value.get("terminal_slots"), "terminal_slots"),
            payload_sha256=payload_sha256,
        )


def _lease_observation_digest(observation: LeaseObservation) -> str:
    unsigned = {
        "sandbox": observation.sandbox.value,
        "pool_name": observation.pool_name,
        "candidate_sha": observation.candidate_sha,
        "request_id": observation.request_id,
        "lease_epoch": observation.lease_epoch,
        "capacity_lease_state": observation.capacity_lease_state,
        "observed_at": observation.observed_at,
        "observation_sequence": observation.observation_sequence,
        "pending_slots": observation.pending_slots,
        "active_slots": observation.active_slots,
        "draining_slots": observation.draining_slots,
        "terminal_slots": observation.terminal_slots,
    }
    return hashlib.sha256(
        json.dumps(unsigned, sort_keys=True, separators=(",", ":")).encode(),
    ).hexdigest()


@dataclass(frozen=True, slots=True)
class AutoscalerGrantHandoff:
    schema_version: int
    request_id: str
    lease_epoch: int
    sandbox: str
    environment: str
    candidate_sha: str
    pool_name: str
    enabled: bool
    min_slots: int
    max_slots: int
    expires_at: str
    preemptible: bool

    def public_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class BrokerBudgets:
    global_slots: int
    pool_slots: Mapping[str, int]
    global_pending_slots: int
    pool_pending_slots: Mapping[str, int]

    def validate_for_pools(self, pools: Iterable[str]) -> None:
        if (
            isinstance(self.global_slots, bool)
            or not isinstance(self.global_slots, int)
            or isinstance(self.global_pending_slots, bool)
            or not isinstance(self.global_pending_slots, int)
            or not 0 <= self.global_slots <= _MAX_BUDGET_SLOTS
            or not 0 <= self.global_pending_slots <= _MAX_BUDGET_SLOTS
        ):
            raise BrokerError("global budgets must be bounded non-negative integers")
        for pool in pools:
            if pool not in self.pool_slots:
                raise BrokerError(f"missing slot budget for pool {pool}")
            if pool not in self.pool_pending_slots:
                raise BrokerError(f"missing pending-slot budget for pool {pool}")
            slot_budget = self.pool_slots[pool]
            pending_budget = self.pool_pending_slots[pool]
            if (
                isinstance(slot_budget, bool)
                or not isinstance(slot_budget, int)
                or isinstance(pending_budget, bool)
                or not isinstance(pending_budget, int)
                or not 0 <= slot_budget <= _MAX_BUDGET_SLOTS
                or not 0 <= pending_budget <= _MAX_BUDGET_SLOTS
            ):
                raise BrokerError(f"pool budgets must be bounded integers for {pool}")

    def public_dict(self) -> dict[str, object]:
        return {
            "global_slots": self.global_slots,
            "pool_slots": dict(sorted(self.pool_slots.items())),
            "global_pending_slots": self.global_pending_slots,
            "pool_pending_slots": dict(sorted(self.pool_pending_slots.items())),
        }


@dataclass(slots=True)
class _MutableRecord:
    request: CapacityRequest
    lease: CapacityLease
    last_granted_seq: int

    @property
    def eligible(self) -> bool:
        return not self.request.cancel_requested and self.request.state != RequestState.TERMINAL


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        raise BrokerError("clock must return a timezone-aware datetime")
    return value.astimezone(UTC)


def _timestamp(value: datetime) -> str:
    return _utc(value).isoformat().replace("+00:00", "Z")


def _parse_timestamp(value: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise BrokerError("stored broker timestamp is invalid") from exc
    return _utc(parsed)


def _exact_text(value: object, field: str) -> str:
    if (
        not isinstance(value, str)
        or not value
        or value != value.strip()
        or any(ord(character) < 32 or ord(character) == 127 for character in value)
    ):
        raise BrokerError(f"{field} must be exact non-empty text")
    return value


def _nonnegative_int(value: object, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise BrokerError(f"{field} must be a non-negative integer")
    return value


def _validate_request_input(
    *,
    sandbox: SandboxId | str,
    candidate_sha: str,
    pool: str,
    min_slots: int,
    target_slots: int,
    ttl_seconds: int,
    purpose: str,
    preemptible: bool,
    idempotency_key: str,
) -> tuple[SandboxId, str, str, str, str]:
    try:
        normalized_sandbox = sandbox if isinstance(sandbox, SandboxId) else SandboxId(str(sandbox))
    except ValueError as exc:
        raise BrokerError(
            "sandbox must match ^[a-z][a-z0-9-]{0,47}$",
        ) from exc
    if _SHA_RE.fullmatch(candidate_sha) is None:
        raise BrokerError("candidate_sha must be a full lowercase 40-hex commit")
    if _POOL_RE.fullmatch(pool) is None:
        raise BrokerError("pool must be a lowercase bounded identifier")
    if (
        isinstance(min_slots, bool)
        or not isinstance(min_slots, int)
        or isinstance(target_slots, bool)
        or not isinstance(target_slots, int)
        or min_slots < 0
        or target_slots <= 0
        or min_slots > target_slots
        or target_slots > _MAX_REQUEST_SLOTS
    ):
        raise BrokerError(
            "slot request must satisfy 0 <= min_slots <= target_slots <= 10000",
        )
    if (
        isinstance(ttl_seconds, bool)
        or not isinstance(ttl_seconds, int)
        or not 60 <= ttl_seconds <= _MAX_TTL_SECONDS
    ):
        raise BrokerError("ttl_seconds must be in 60..86400")
    if not isinstance(preemptible, bool):
        raise BrokerError("preemptible must be a boolean")
    purpose = _exact_text(purpose, "purpose")
    if _PURPOSE_RE.fullmatch(purpose) is None or _SECRET_HINT_RE.search(purpose):
        raise BrokerError("purpose must be bounded secret-free operator text")
    idempotency_key = _exact_text(idempotency_key, "idempotency_key")
    if _IDEMPOTENCY_RE.fullmatch(idempotency_key) is None or _SECRET_HINT_RE.search(
        idempotency_key
    ):
        raise BrokerError("idempotency_key has invalid or secret-like content")
    return normalized_sandbox, candidate_sha, pool, purpose, idempotency_key


def _validate_acceptance_contract(value: object) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise BrokerError("acceptance contract is invalid")
    required = {
        "schema_version",
        "admission_token",
        "session_id",
        "candidate_shas",
        "phases",
        "target_slots",
        "ttl_seconds",
        "pool_slot_budgets",
        "pool_pending_slot_budgets",
        "expires_at",
    }
    candidate_shas = value.get("candidate_shas")
    phases = value.get("phases")
    target_slots = value.get("target_slots")
    ttl_seconds = value.get("ttl_seconds")
    slot_budgets = value.get("pool_slot_budgets")
    pending_budgets = value.get("pool_pending_slot_budgets")
    if (
        set(value) != required
        or value.get("schema_version") != 1
        or _ADMISSION_FENCE_RE.fullmatch(str(value.get("admission_token"))) is None
        or _ACCEPTANCE_SESSION_RE.fullmatch(str(value.get("session_id"))) is None
        or not isinstance(candidate_shas, dict)
        or not isinstance(phases, list)
        or not phases
        or len(phases) != len(set(phases))
        or any(
            not isinstance(phase, str) or _ACCEPTANCE_PHASE_RE.fullmatch(phase) is None
            for phase in phases
        )
        or not isinstance(target_slots, dict)
        or set(target_slots) != set(_ACCEPTANCE_POOLS)
        or not isinstance(ttl_seconds, dict)
        or set(ttl_seconds) != set(phases)
        or not isinstance(slot_budgets, dict)
        or set(slot_budgets) != set(_ACCEPTANCE_POOLS)
        or not isinstance(pending_budgets, dict)
        or set(pending_budgets) != set(_ACCEPTANCE_POOLS)
    ):
        raise BrokerError("acceptance contract is invalid")
    try:
        acceptance_sandboxes = tuple(
            sorted(
                (
                    SandboxId(_exact_text(sandbox, "acceptance sandbox"))
                    for sandbox in candidate_shas
                ),
                key=lambda sandbox: sandbox.value,
            ),
        )
    except (BrokerError, ValueError) as exc:
        raise BrokerError("acceptance contract sandbox registry is invalid") from exc
    if (
        len(acceptance_sandboxes) < 2
        or {sandbox.value for sandbox in acceptance_sandboxes} != set(candidate_shas)
        or any(
            not isinstance(candidate_shas[sandbox.value], str)
            or _SHA_RE.fullmatch(str(candidate_shas[sandbox.value])) is None
            for sandbox in acceptance_sandboxes
        )
        or len(set(candidate_shas.values())) != len(acceptance_sandboxes)
    ):
        raise BrokerError("acceptance contract sandbox registry is invalid")
    for pool in _ACCEPTANCE_POOLS:
        target = target_slots[pool]
        slot_budget = slot_budgets[pool]
        pending_budget = pending_budgets[pool]
        if (
            isinstance(target, bool)
            or not isinstance(target, int)
            or target <= 0
            or isinstance(slot_budget, bool)
            or not isinstance(slot_budget, int)
            or isinstance(pending_budget, bool)
            or not isinstance(pending_budget, int)
            or pending_budget < 0
            or target * len(acceptance_sandboxes) > slot_budget
            or slot_budget > _MAX_BUDGET_SLOTS
            or pending_budget > _MAX_BUDGET_SLOTS
        ):
            raise BrokerError("acceptance contract budgets are invalid")
    for phase in phases:
        ttl = ttl_seconds[phase]
        if isinstance(ttl, bool) or not isinstance(ttl, int) or not 60 <= ttl <= _MAX_TTL_SECONDS:
            raise BrokerError("acceptance contract TTL is invalid")
    _parse_timestamp(str(value.get("expires_at")))
    return cast(
        dict[str, Any],
        json.loads(json.dumps(value, sort_keys=True, separators=(",", ":"))),
    )


def _acceptance_sandboxes(contract: Mapping[str, Any]) -> tuple[SandboxId, ...]:
    candidate_shas = contract.get("candidate_shas")
    if not isinstance(candidate_shas, dict):
        raise BrokerError("acceptance contract sandbox registry is invalid")
    try:
        sandboxes = tuple(
            sorted(
                (SandboxId(str(sandbox)) for sandbox in candidate_shas),
                key=lambda sandbox: sandbox.value,
            ),
        )
    except ValueError as exc:
        raise BrokerError("acceptance contract sandbox registry is invalid") from exc
    if len(sandboxes) < 2 or {sandbox.value for sandbox in sandboxes} != set(candidate_shas):
        raise BrokerError("acceptance contract sandbox registry is invalid")
    return sandboxes


class SharedCapacityBroker:
    """Single persistent authority for all developer-sandbox capacity."""

    def __init__(self, state_db: Path, *, clock: Any | None = None) -> None:
        if not state_db.is_absolute():
            raise BrokerError("state_db must be an absolute path")
        self.state_db = state_db
        self._clock = clock or (lambda: datetime.now(UTC))

    def initialize(self) -> None:
        self._prepare_authority()
        with self._connect() as connection:
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS broker_meta (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS capacity_requests (
                    id TEXT PRIMARY KEY,
                    sandbox TEXT NOT NULL,
                    candidate_sha TEXT NOT NULL,
                    pool TEXT NOT NULL,
                    min_slots INTEGER NOT NULL,
                    target_slots INTEGER NOT NULL,
                    ttl_seconds INTEGER NOT NULL,
                    purpose TEXT NOT NULL,
                    preemptible INTEGER NOT NULL,
                    idempotency_key TEXT NOT NULL UNIQUE,
                    state TEXT NOT NULL,
                    cancel_requested INTEGER NOT NULL DEFAULT 0,
                    terminal_reason TEXT,
                    created_at TEXT NOT NULL,
                    expires_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    last_granted_seq INTEGER NOT NULL DEFAULT 0
                );
                CREATE TABLE IF NOT EXISTS capacity_leases (
                    request_id TEXT PRIMARY KEY
                        REFERENCES capacity_requests(id) ON DELETE RESTRICT,
                    lease_epoch INTEGER NOT NULL DEFAULT 0,
                    granted_slots INTEGER NOT NULL DEFAULT 0,
                    pending_slots INTEGER NOT NULL DEFAULT 0,
                    active_slots INTEGER NOT NULL DEFAULT 0,
                    draining_slots INTEGER NOT NULL DEFAULT 0,
                    terminal_slots INTEGER NOT NULL DEFAULT 0,
                    state TEXT NOT NULL,
                    last_observed_at TEXT,
                    last_observation_sequence INTEGER,
                    last_observation_digest TEXT,
                    last_policy_lease_state TEXT,
                    updated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS capacity_audit_events (
                    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
                    request_id TEXT REFERENCES capacity_requests(id) ON DELETE RESTRICT,
                    event_type TEXT NOT NULL,
                    occurred_at TEXT NOT NULL,
                    details_json TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS capacity_budgets (
                    scope TEXT PRIMARY KEY,
                    slot_budget INTEGER NOT NULL,
                    pending_slot_budget INTEGER NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS capacity_requests_pool_state_idx
                    ON capacity_requests(pool, state);
                CREATE INDEX IF NOT EXISTS capacity_requests_sandbox_state_idx
                    ON capacity_requests(sandbox, state);
                """,
            )
            connection.execute("BEGIN IMMEDIATE")
            try:
                current = connection.execute(
                    "SELECT value FROM broker_meta WHERE key = 'schema_version'",
                ).fetchone()
                if current is None:
                    connection.execute(
                        "INSERT INTO broker_meta(key, value) VALUES('schema_version', ?)",
                        (_SCHEMA_VERSION,),
                    )
                elif current["value"] == _PREVIOUS_SCHEMA_VERSION:
                    columns = {
                        str(row["name"])
                        for row in connection.execute("PRAGMA table_info(capacity_leases)")
                    }
                    additions = (
                        ("last_observation_sequence", "INTEGER"),
                        ("last_observation_digest", "TEXT"),
                        ("last_policy_lease_state", "TEXT"),
                    )
                    for column, column_type in additions:
                        if column not in columns:
                            connection.execute(
                                f"ALTER TABLE capacity_leases ADD COLUMN {column} {column_type}",
                            )
                    connection.execute(
                        "UPDATE broker_meta SET value = ? WHERE key = 'schema_version'",
                        (_SCHEMA_VERSION,),
                    )
                elif current["value"] != _SCHEMA_VERSION:
                    raise BrokerError("broker state schema version is unsupported")
                connection.commit()
            except Exception:
                connection.rollback()
                raise

    def _connect(self) -> sqlite3.Connection:
        self._validate_authority_file(self.state_db, mode=0o600)
        existing_sidecars = {
            path for path in self._sqlite_sidecar_paths() if path.exists() or path.is_symlink()
        }
        for path in existing_sidecars:
            self._validate_authority_file(path, mode=0o600, missing_ok=True)
        connection = sqlite3.connect(self.state_db, timeout=30.0, isolation_level=None)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute("PRAGMA journal_mode = WAL")
        connection.execute("PRAGMA synchronous = FULL")
        for path in self._sqlite_sidecar_paths():
            if not (path.exists() or path.is_symlink()):
                continue
            if path not in existing_sidecars:
                try:
                    os.chmod(path, 0o600, follow_symlinks=False)
                except FileNotFoundError:
                    continue
                except OSError as exc:
                    connection.close()
                    raise BrokerError("broker SQLite sidecar authority is unsafe") from exc
            self._validate_authority_file(path, mode=0o600, missing_ok=True)
        return connection

    def _prepare_authority(self) -> None:
        parent = self.state_db.parent
        if parent.exists() or parent.is_symlink():
            self._validate_authority_directory(parent)
        else:
            try:
                parent.mkdir(mode=0o700, parents=True)
            except OSError as exc:
                raise BrokerError("broker authority directory could not be created") from exc
            self._validate_authority_directory(parent)

        if self.state_db.exists() or self.state_db.is_symlink():
            self._validate_authority_file(self.state_db, mode=0o600)
            return
        flags = (
            os.O_WRONLY
            | os.O_CREAT
            | os.O_EXCL
            | getattr(os, "O_CLOEXEC", 0)
            | getattr(os, "O_NOFOLLOW", 0)
        )
        try:
            descriptor = os.open(self.state_db, flags, 0o600)
        except FileExistsError:
            self._validate_authority_file(self.state_db, mode=0o600)
            return
        except OSError as exc:
            raise BrokerError("broker state database could not be created safely") from exc
        else:
            os.close(descriptor)
        self._validate_authority_file(self.state_db, mode=0o600)

    @staticmethod
    def _validate_authority_directory(path: Path) -> None:
        try:
            metadata = os.lstat(path)
        except OSError as exc:
            raise BrokerError("broker authority directory is unavailable") from exc
        if (
            stat.S_ISLNK(metadata.st_mode)
            or not stat.S_ISDIR(metadata.st_mode)
            or metadata.st_uid != os.geteuid()
            or stat.S_IMODE(metadata.st_mode) != 0o700
        ):
            raise BrokerError("broker authority directory must be owner-only mode 0700")

    @staticmethod
    def _validate_authority_file(
        path: Path,
        *,
        mode: int,
        missing_ok: bool = False,
    ) -> bool:
        try:
            metadata = os.lstat(path)
        except FileNotFoundError:
            if missing_ok:
                return False
            raise BrokerError("broker authority file is unavailable") from None
        except OSError as exc:
            raise BrokerError("broker authority file is unavailable") from exc
        if (
            stat.S_ISLNK(metadata.st_mode)
            or not stat.S_ISREG(metadata.st_mode)
            or metadata.st_nlink != 1
            or metadata.st_uid != os.geteuid()
            or stat.S_IMODE(metadata.st_mode) != mode
        ):
            raise BrokerError("broker authority file has unsafe owner, type, link count, or mode")
        return True

    def _sqlite_sidecar_paths(self) -> tuple[Path, ...]:
        return tuple(
            self.state_db.with_name(f"{self.state_db.name}{suffix}")
            for suffix in ("-wal", "-shm", "-journal")
        )

    def _transaction(self) -> sqlite3.Connection:
        connection = self._connect()
        connection.execute("BEGIN IMMEDIATE")
        return connection

    @staticmethod
    def _admission_fence(connection: sqlite3.Connection) -> str | None:
        row = connection.execute(
            "SELECT value FROM broker_meta WHERE key = 'capacity_admission_fence'",
        ).fetchone()
        if row is None:
            return None
        token = str(row["value"])
        if _ADMISSION_FENCE_RE.fullmatch(token) is None:
            raise BrokerError("capacity admission fence is invalid")
        return token

    def close_admission(self, token: str) -> None:
        """Persistently fence new requests for an exact activation transaction."""

        if _ADMISSION_FENCE_RE.fullmatch(token) is None:
            raise BrokerError("capacity admission fence token is invalid")
        self.initialize()
        connection = self._transaction()
        try:
            existing = self._admission_fence(connection)
            if existing is None:
                connection.execute(
                    """
                    INSERT INTO broker_meta(key, value)
                    VALUES('capacity_admission_fence', ?)
                    """,
                    (token,),
                )
            elif existing != token:
                raise BrokerError("capacity admission is fenced by another transaction")
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    def open_admission(self, token: str) -> None:
        """Idempotently release only the caller's persisted activation fence."""

        if _ADMISSION_FENCE_RE.fullmatch(token) is None:
            raise BrokerError("capacity admission fence token is invalid")
        self.initialize()
        connection = self._transaction()
        try:
            existing = self._admission_fence(connection)
            if existing is not None and existing != token:
                raise BrokerError("capacity admission fence belongs to another transaction")
            if existing == token:
                connection.execute(
                    "DELETE FROM broker_meta WHERE key = 'capacity_admission_fence'",
                )
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    @staticmethod
    def _environment_admission_fence(
        connection: sqlite3.Connection,
        sandbox: SandboxId,
    ) -> str | None:
        row = connection.execute(
            "SELECT value FROM broker_meta WHERE key = ?",
            (_ENVIRONMENT_ADMISSION_FENCE_PREFIX + sandbox.value,),
        ).fetchone()
        if row is None:
            return None
        token = str(row["value"])
        if _ADMISSION_FENCE_RE.fullmatch(token) is None:
            raise BrokerError("environment admission fence is invalid")
        return token

    def close_environment_admission(
        self,
        sandbox: SandboxId | str,
        token: str,
    ) -> None:
        """Fence only one environment while peer admission remains open."""

        selected = SandboxId(str(sandbox))
        if _ADMISSION_FENCE_RE.fullmatch(token) is None:
            raise BrokerError("environment admission fence token is invalid")
        self.initialize()
        connection = self._transaction()
        try:
            existing = self._environment_admission_fence(connection, selected)
            if existing is None:
                connection.execute(
                    "INSERT INTO broker_meta(key, value) VALUES(?, ?)",
                    (
                        _ENVIRONMENT_ADMISSION_FENCE_PREFIX + selected.value,
                        token,
                    ),
                )
            elif existing != token:
                raise BrokerError("environment admission is fenced by another transaction")
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    def open_environment_admission(
        self,
        sandbox: SandboxId | str,
        token: str,
    ) -> None:
        """Release only the exact environment fence owned by this token."""

        selected = SandboxId(str(sandbox))
        if _ADMISSION_FENCE_RE.fullmatch(token) is None:
            raise BrokerError("environment admission fence token is invalid")
        self.initialize()
        connection = self._transaction()
        try:
            existing = self._environment_admission_fence(connection, selected)
            if existing is not None and existing != token:
                raise BrokerError("environment admission fence belongs to another transaction")
            if existing == token:
                connection.execute(
                    "DELETE FROM broker_meta WHERE key = ?",
                    (_ENVIRONMENT_ADMISSION_FENCE_PREFIX + selected.value,),
                )
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    def rotate_environment_admission(
        self,
        sandbox: SandboxId | str,
        previous_token: str,
        token: str,
    ) -> None:
        """Atomically replace one exact environment fence without an open gap."""

        selected = SandboxId(str(sandbox))
        if (
            _ADMISSION_FENCE_RE.fullmatch(previous_token) is None
            or _ADMISSION_FENCE_RE.fullmatch(token) is None
        ):
            raise BrokerError("environment admission fence token is invalid")
        self.initialize()
        connection = self._transaction()
        try:
            existing = self._environment_admission_fence(connection, selected)
            if existing != previous_token:
                raise BrokerError("environment admission fence belongs to another transaction")
            connection.execute(
                "UPDATE broker_meta SET value = ? WHERE key = ?",
                (
                    token,
                    _ENVIRONMENT_ADMISSION_FENCE_PREFIX + selected.value,
                ),
            )
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    def environment_admission_fence(
        self,
        sandbox: SandboxId | str,
    ) -> str | None:
        selected = SandboxId(str(sandbox))
        self.initialize()
        connection = self._connect()
        try:
            return self._environment_admission_fence(connection, selected)
        finally:
            connection.close()

    @staticmethod
    def _acceptance_contract(
        connection: sqlite3.Connection,
    ) -> dict[str, Any] | None:
        row = connection.execute(
            "SELECT value FROM broker_meta WHERE key = ?",
            (_ACCEPTANCE_META_KEY,),
        ).fetchone()
        if row is None:
            return None
        try:
            value = json.loads(str(row["value"]))
        except json.JSONDecodeError as exc:
            raise BrokerError("stored acceptance contract is invalid") from exc
        contract = _validate_acceptance_contract(value)
        if str(row["value"]) != json.dumps(
            contract,
            sort_keys=True,
            separators=(",", ":"),
        ):
            raise BrokerError("stored acceptance contract is not canonical")
        return contract

    def open_acceptance_admission(
        self,
        *,
        token: str,
        contract: Mapping[str, Any],
    ) -> dict[str, Any]:
        """Persist one exact acceptance-only contract while general admission stays fenced."""

        validated = _validate_acceptance_contract(dict(contract))
        if validated["admission_token"] != token:
            raise BrokerError("acceptance contract token does not match its fence")
        now = _utc(self._clock())
        expires_at = _parse_timestamp(str(validated["expires_at"]))
        if expires_at <= now or expires_at > now + timedelta(seconds=_MAX_TTL_SECONDS):
            raise BrokerError("acceptance contract expiry is outside its fixed bound")
        self.initialize()
        connection = self._transaction()
        try:
            if self._admission_fence(connection) != token:
                raise BrokerError("acceptance contract requires the exact closed general fence")
            existing = self._acceptance_contract(connection)
            if existing is None:
                connection.execute(
                    "INSERT INTO broker_meta(key, value) VALUES(?, ?)",
                    (
                        _ACCEPTANCE_META_KEY,
                        json.dumps(validated, sort_keys=True, separators=(",", ":")),
                    ),
                )
            elif existing != validated:
                raise BrokerError("another acceptance contract is already active")
            connection.commit()
            return validated
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    @staticmethod
    def _acceptance_request_values(
        contract: Mapping[str, Any],
        *,
        phase: str,
        sandbox: SandboxId | str,
        pool: str,
    ) -> tuple[SandboxId, str, str, int, int, str]:
        try:
            normalized_sandbox = sandbox if isinstance(sandbox, SandboxId) else SandboxId(sandbox)
        except ValueError as exc:
            raise BrokerError("acceptance sandbox id is invalid") from exc
        candidate_shas = contract.get("candidate_shas")
        if not isinstance(candidate_shas, dict) or normalized_sandbox.value not in candidate_shas:
            raise BrokerError("acceptance sandbox is outside the session registry")
        candidate_sha = str(candidate_shas[normalized_sandbox.value])
        target_slots = int(contract["target_slots"][pool])
        ttl_seconds = int(contract["ttl_seconds"][phase])
        purpose = f"developer-sandbox-live-acceptance/{contract['session_id']}/{phase}"
        idempotency_key = (
            f"acceptance:{contract['session_id']}:{phase}:{normalized_sandbox.value}:{pool}"
        )
        _validate_request_input(
            sandbox=normalized_sandbox,
            candidate_sha=candidate_sha,
            pool=pool,
            min_slots=0,
            target_slots=target_slots,
            ttl_seconds=ttl_seconds,
            purpose=purpose,
            preemptible=True,
            idempotency_key=idempotency_key,
        )
        return (
            normalized_sandbox,
            candidate_sha,
            purpose,
            target_slots,
            ttl_seconds,
            idempotency_key,
        )

    @classmethod
    def _is_acceptance_owned_request(
        cls,
        request: CapacityRequest,
        contract: Mapping[str, Any],
    ) -> bool:
        if request.pool not in _ACCEPTANCE_POOLS:
            return False
        if request.sandbox not in _acceptance_sandboxes(contract):
            return False
        for phase in contract["phases"]:
            values = cls._acceptance_request_values(
                contract,
                phase=str(phase),
                sandbox=request.sandbox.value,
                pool=request.pool,
            )
            if (
                request.candidate_sha == values[1]
                and request.min_slots == 0
                and request.target_slots == values[3]
                and request.ttl_seconds == values[4]
                and request.purpose == values[2]
                and request.preemptible is True
                and request.idempotency_key == values[5]
            ):
                return True
        return False

    def acceptance_cohort_status(
        self,
        *,
        token: str,
        session_id: str,
        phase: str,
    ) -> list[dict[str, object]] | None:
        """Read one exact cohort without creating missing requests."""

        self.initialize()
        connection = self._transaction()
        try:
            if self._admission_fence(connection) != token:
                raise BrokerError("acceptance cohort requires the exact closed general fence")
            contract = self._acceptance_contract(connection)
            if (
                contract is None
                or contract["admission_token"] != token
                or contract["session_id"] != session_id
                or phase not in contract["phases"]
            ):
                raise BrokerError("acceptance cohort is outside the active contract")
            rows: list[tuple[CapacityRequest, CapacityLease]] = []
            missing = 0
            acceptance_sandboxes = _acceptance_sandboxes(contract)
            for normalized_sandbox in acceptance_sandboxes:
                sandbox = normalized_sandbox.value
                for pool in _ACCEPTANCE_POOLS:
                    values = self._acceptance_request_values(
                        contract,
                        phase=phase,
                        sandbox=sandbox,
                        pool=pool,
                    )
                    existing = connection.execute(
                        "SELECT id FROM capacity_requests WHERE idempotency_key = ?",
                        (values[-1],),
                    ).fetchone()
                    if existing is None:
                        missing += 1
                        continue
                    request, lease = _record_from_row(
                        self._joined_row(connection, existing["id"]),
                    )
                    if not self._is_acceptance_owned_request(request, contract):
                        raise BrokerError("acceptance cohort idempotency binding drifted")
                    rows.append((request, lease))
            expected_lane_count = len(acceptance_sandboxes) * len(_ACCEPTANCE_POOLS)
            if missing == expected_lane_count:
                connection.commit()
                return None
            if missing or len(rows) != expected_lane_count:
                raise BrokerError("acceptance cohort is partially materialized")
            rows.sort(key=lambda item: (item[0].sandbox.value, item[0].pool))
            connection.commit()
            return [self._public_record(request, lease) for request, lease in rows]
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    def retire_acceptance_requests(
        self,
        *,
        token: str,
        session_id: str,
    ) -> None:
        """Cancel only requests owned by the exact active acceptance contract."""

        now_text = _timestamp(_utc(self._clock()))
        self.initialize()
        connection = self._transaction()
        try:
            if self._admission_fence(connection) != token:
                raise BrokerError("acceptance retirement requires the exact closed general fence")
            contract = self._acceptance_contract(connection)
            if (
                contract is None
                or contract["admission_token"] != token
                or contract["session_id"] != session_id
            ):
                raise BrokerError("acceptance retirement is outside the active contract")
            prefix = f"developer-sandbox-live-acceptance/{session_id}/%"
            rows = connection.execute(
                "SELECT id FROM capacity_requests WHERE purpose LIKE ?",
                (prefix,),
            ).fetchall()
            for row in rows:
                request, lease = _record_from_row(
                    self._joined_row(connection, row["id"]),
                )
                if not self._is_acceptance_owned_request(request, contract):
                    raise BrokerError("acceptance retirement ownership drifted")
                if request.state == RequestState.TERMINAL:
                    continue
                new_epoch = lease.lease_epoch + (1 if lease.granted_slots != 0 else 0)
                connection.execute(
                    """
                    UPDATE capacity_requests
                    SET cancel_requested = 1, terminal_reason = ?, updated_at = ?
                    WHERE id = ?
                    """,
                    ("acceptance_phase_rotation", now_text, request.id),
                )
                connection.execute(
                    """
                    UPDATE capacity_leases
                    SET lease_epoch = ?, granted_slots = 0, updated_at = ?
                    WHERE request_id = ?
                    """,
                    (new_epoch, now_text, request.id),
                )
                self._audit(
                    connection,
                    request_id=request.id,
                    event_type="acceptance_rotation_requested",
                    occurred_at=now_text,
                    details={"session_id": session_id, "lease_epoch": new_epoch},
                )
            self._refresh_states(connection, now_text=now_text)
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    def request_acceptance_cohort(
        self,
        *,
        token: str,
        session_id: str,
        phase: str,
    ) -> list[dict[str, object]]:
        """Atomically create or read the session-registry cohort for one bounded phase."""

        if (
            _ADMISSION_FENCE_RE.fullmatch(token) is None
            or _ACCEPTANCE_SESSION_RE.fullmatch(session_id) is None
            or _ACCEPTANCE_PHASE_RE.fullmatch(phase) is None
        ):
            raise BrokerError("acceptance cohort identity is invalid")
        now = _utc(self._clock())
        now_text = _timestamp(now)
        self.initialize()
        connection = self._transaction()
        try:
            if self._admission_fence(connection) != token:
                raise BrokerError("acceptance cohort requires the exact closed general fence")
            contract = self._acceptance_contract(connection)
            if (
                contract is None
                or contract["admission_token"] != token
                or contract["session_id"] != session_id
                or phase not in contract["phases"]
            ):
                raise BrokerError("acceptance cohort is outside the active contract")
            rows: list[tuple[CapacityRequest, CapacityLease]] = []
            missing: list[tuple[str, str, tuple[SandboxId, str, str, int, int, str]]] = []
            acceptance_sandboxes = _acceptance_sandboxes(contract)
            for normalized_sandbox in acceptance_sandboxes:
                sandbox = normalized_sandbox.value
                for pool in _ACCEPTANCE_POOLS:
                    values = self._acceptance_request_values(
                        contract,
                        phase=phase,
                        sandbox=sandbox,
                        pool=pool,
                    )
                    idempotency_key = values[-1]
                    existing = connection.execute(
                        "SELECT id FROM capacity_requests WHERE idempotency_key = ?",
                        (idempotency_key,),
                    ).fetchone()
                    if existing is None:
                        missing.append((sandbox, pool, values))
                        continue
                    request, lease = _record_from_row(
                        self._joined_row(connection, existing["id"]),
                    )
                    expected = (
                        values[0],
                        values[1],
                        pool,
                        0,
                        values[3],
                        values[4],
                        values[2],
                        True,
                        values[5],
                    )
                    observed = (
                        request.sandbox,
                        request.candidate_sha,
                        request.pool,
                        request.min_slots,
                        request.target_slots,
                        request.ttl_seconds,
                        request.purpose,
                        request.preemptible,
                        request.idempotency_key,
                    )
                    if observed != expected:
                        raise BrokerError("acceptance cohort idempotency binding drifted")
                    rows.append((request, lease))
            if missing and rows:
                raise BrokerError("acceptance cohort is partially materialized")
            if missing and _parse_timestamp(str(contract["expires_at"])) <= now:
                raise BrokerError("acceptance contract expired before cohort creation")
            if missing:
                prefix = f"developer-sandbox-live-acceptance/{session_id}/%"
                for row in connection.execute(
                    """
                    SELECT id FROM capacity_requests
                    WHERE purpose LIKE ? AND state != ?
                    """,
                    (prefix, RequestState.TERMINAL.value),
                ).fetchall():
                    request, _lease = _record_from_row(
                        self._joined_row(connection, row["id"]),
                    )
                    if not self._is_acceptance_owned_request(request, contract):
                        raise BrokerError("acceptance cohort ownership drifted")
                    raise BrokerError("previous acceptance cohort is not fully retired")
            for sandbox, pool, values in missing:
                (
                    normalized_sandbox,
                    candidate_sha,
                    purpose,
                    target_slots,
                    ttl_seconds,
                    idempotency_key,
                ) = values
                request_id = str(uuid4())
                expires_at = _timestamp(now + timedelta(seconds=ttl_seconds))
                connection.execute(
                    """
                    INSERT INTO capacity_requests(
                        id, sandbox, candidate_sha, pool, min_slots, target_slots,
                        ttl_seconds, purpose, preemptible, idempotency_key, state,
                        created_at, expires_at, updated_at
                    ) VALUES(?, ?, ?, ?, 0, ?, ?, ?, 1, ?, ?, ?, ?, ?)
                    """,
                    (
                        request_id,
                        normalized_sandbox.value,
                        candidate_sha,
                        pool,
                        target_slots,
                        ttl_seconds,
                        purpose,
                        idempotency_key,
                        RequestState.PENDING.value,
                        now_text,
                        expires_at,
                        now_text,
                    ),
                )
                connection.execute(
                    """
                    INSERT INTO capacity_leases(request_id, state, updated_at)
                    VALUES(?, ?, ?)
                    """,
                    (request_id, RequestState.PENDING.value, now_text),
                )
                self._audit(
                    connection,
                    request_id=request_id,
                    event_type="acceptance_request_created",
                    occurred_at=now_text,
                    details={
                        "session_id": session_id,
                        "phase": phase,
                        "sandbox": sandbox,
                        "pool": pool,
                        "candidate_sha": candidate_sha,
                        "target_slots": target_slots,
                        "ttl_seconds": ttl_seconds,
                    },
                )
                rows.append(
                    _record_from_row(self._joined_row(connection, request_id)),
                )
            if len(rows) != len(acceptance_sandboxes) * len(_ACCEPTANCE_POOLS):
                raise BrokerError("acceptance cohort is not closed-world")
            rows.sort(key=lambda item: (item[0].sandbox.value, item[0].pool))
            connection.commit()
            return [self._public_record(request, lease) for request, lease in rows]
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    def cancel_acceptance_request(
        self,
        *,
        token: str,
        session_id: str,
        phase: str,
        sandbox: str,
        pool: str,
    ) -> dict[str, object]:
        """Cancel only the deterministic request owned by one acceptance lane."""

        now_text = _timestamp(_utc(self._clock()))
        self.initialize()
        connection = self._transaction()
        try:
            if self._admission_fence(connection) != token:
                raise BrokerError("acceptance cancel requires the exact closed general fence")
            contract = self._acceptance_contract(connection)
            try:
                normalized_sandbox = SandboxId(sandbox)
            except ValueError as exc:
                raise BrokerError(
                    "acceptance cancel is outside the active contract",
                ) from exc
            if (
                contract is None
                or contract["admission_token"] != token
                or contract["session_id"] != session_id
                or phase not in contract["phases"]
                or normalized_sandbox not in _acceptance_sandboxes(contract)
                or pool not in _ACCEPTANCE_POOLS
            ):
                raise BrokerError("acceptance cancel is outside the active contract")
            values = self._acceptance_request_values(
                contract,
                phase=phase,
                sandbox=normalized_sandbox,
                pool=pool,
            )
            row = connection.execute(
                "SELECT id FROM capacity_requests WHERE idempotency_key = ?",
                (values[-1],),
            ).fetchone()
            if row is None:
                raise BrokerError("acceptance-owned request is unavailable")
            request, lease = _record_from_row(self._joined_row(connection, row["id"]))
            if (
                request.sandbox != values[0]
                or request.candidate_sha != values[1]
                or request.pool != pool
                or request.purpose != values[2]
                or request.idempotency_key != values[5]
            ):
                raise BrokerError("acceptance cancel ownership drifted")
            if request.state != RequestState.TERMINAL:
                new_epoch = lease.lease_epoch + (1 if lease.granted_slots != 0 else 0)
                connection.execute(
                    """
                    UPDATE capacity_requests
                    SET cancel_requested = 1, terminal_reason = ?, updated_at = ?
                    WHERE id = ?
                    """,
                    ("acceptance_cancel_cleanup", now_text, request.id),
                )
                connection.execute(
                    """
                    UPDATE capacity_leases
                    SET lease_epoch = ?, granted_slots = 0, updated_at = ?
                    WHERE request_id = ?
                    """,
                    (new_epoch, now_text, request.id),
                )
                self._audit(
                    connection,
                    request_id=request.id,
                    event_type="acceptance_cancel_requested",
                    occurred_at=now_text,
                    details={
                        "session_id": session_id,
                        "phase": phase,
                        "sandbox": sandbox,
                        "pool": pool,
                        "lease_epoch": new_epoch,
                    },
                )
                self._refresh_states(connection, now_text=now_text)
                request, lease = _record_from_row(
                    self._joined_row(connection, request.id),
                )
            connection.commit()
            return self._public_record(request, lease)
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    def close_acceptance_admission(
        self,
        *,
        token: str,
        session_id: str,
    ) -> None:
        """Remove only a fully drained exact acceptance contract."""

        self.initialize()
        connection = self._transaction()
        try:
            if self._admission_fence(connection) != token:
                raise BrokerError("acceptance close requires the exact closed general fence")
            contract = self._acceptance_contract(connection)
            if contract is None:
                connection.commit()
                return
            if contract["admission_token"] != token or contract["session_id"] != session_id:
                raise BrokerError("acceptance close ownership drifted")
            purpose_prefix = f"developer-sandbox-live-acceptance/{session_id}/"
            rows = connection.execute(
                """
                SELECT r.id
                FROM capacity_requests r
                WHERE r.purpose LIKE ?
                """,
                (f"{purpose_prefix}%",),
            ).fetchall()
            for row in rows:
                request, lease = _record_from_row(
                    self._joined_row(connection, row["id"]),
                )
                if request.state != RequestState.TERMINAL or any(
                    value != 0
                    for value in (
                        lease.granted_slots,
                        lease.pending_slots,
                        lease.active_slots,
                        lease.draining_slots,
                        lease.committed_slots,
                    )
                ):
                    raise BrokerError("acceptance contract still owns live capacity")
            connection.execute(
                "DELETE FROM broker_meta WHERE key = ?",
                (_ACCEPTANCE_META_KEY,),
            )
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    def acceptance_contract(self) -> dict[str, Any] | None:
        self.initialize()
        with self._connect() as connection:
            return self._acceptance_contract(connection)

    def request_capacity(
        self,
        *,
        sandbox: SandboxId | str,
        candidate_sha: str,
        pool: str,
        min_slots: int,
        target_slots: int,
        ttl_seconds: int,
        purpose: str,
        preemptible: bool,
        idempotency_key: str,
    ) -> tuple[CapacityRequest, CapacityLease]:
        (
            normalized_sandbox,
            candidate_sha,
            pool,
            purpose,
            idempotency_key,
        ) = _validate_request_input(
            sandbox=sandbox,
            candidate_sha=candidate_sha,
            pool=pool,
            min_slots=min_slots,
            target_slots=target_slots,
            ttl_seconds=ttl_seconds,
            purpose=purpose,
            preemptible=preemptible,
            idempotency_key=idempotency_key,
        )
        now = _utc(self._clock())
        now_text = _timestamp(now)
        expires_at = _timestamp(now + timedelta(seconds=ttl_seconds))
        self.initialize()
        connection = self._transaction()
        try:
            existing = connection.execute(
                """
                SELECT r.id
                FROM capacity_requests r
                WHERE r.idempotency_key = ?
                """,
                (idempotency_key,),
            ).fetchone()
            if existing is not None:
                request, lease = _record_from_row(
                    self._joined_row(connection, existing["id"]),
                )
                expected = (
                    normalized_sandbox,
                    candidate_sha,
                    pool,
                    min_slots,
                    target_slots,
                    ttl_seconds,
                    purpose,
                    preemptible,
                )
                observed = (
                    request.sandbox,
                    request.candidate_sha,
                    request.pool,
                    request.min_slots,
                    request.target_slots,
                    request.ttl_seconds,
                    request.purpose,
                    request.preemptible,
                )
                if observed != expected:
                    raise BrokerError("idempotency_key is already bound to another request")
                connection.commit()
                return request, lease

            if self._admission_fence(connection) is not None:
                raise BrokerError("new capacity requests are fenced during runtime activation")
            if (
                self._environment_admission_fence(
                    connection,
                    normalized_sandbox,
                )
                is not None
            ):
                raise BrokerError("new capacity requests are fenced for this environment")

            request_id = str(uuid4())
            connection.execute(
                """
                INSERT INTO capacity_requests(
                    id, sandbox, candidate_sha, pool, min_slots, target_slots,
                    ttl_seconds, purpose, preemptible, idempotency_key, state,
                    created_at, expires_at, updated_at
                ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    request_id,
                    normalized_sandbox.value,
                    candidate_sha,
                    pool,
                    min_slots,
                    target_slots,
                    ttl_seconds,
                    purpose,
                    int(preemptible),
                    idempotency_key,
                    RequestState.PENDING.value,
                    now_text,
                    expires_at,
                    now_text,
                ),
            )
            connection.execute(
                """
                INSERT INTO capacity_leases(
                    request_id, state, updated_at
                ) VALUES(?, ?, ?)
                """,
                (request_id, RequestState.PENDING.value, now_text),
            )
            self._audit(
                connection,
                request_id=request_id,
                event_type="request_created",
                occurred_at=now_text,
                details={
                    "sandbox": normalized_sandbox.value,
                    "candidate_sha": candidate_sha,
                    "pool": pool,
                    "min_slots": min_slots,
                    "target_slots": target_slots,
                    "ttl_seconds": ttl_seconds,
                    "preemptible": preemptible,
                },
            )
            row = self._joined_row(connection, request_id)
            connection.commit()
            return _record_from_row(row)
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    def cancel(self, request_id: str, *, reason: str = "operator_cancelled") -> dict[str, object]:
        request_id = _exact_text(request_id, "request_id")
        reason = _exact_text(reason, "reason")
        if _SECRET_HINT_RE.search(reason):
            raise BrokerError("reason must be secret-free")
        now_text = _timestamp(_utc(self._clock()))
        self.initialize()
        connection = self._transaction()
        try:
            request, lease = _record_from_row(self._joined_row(connection, request_id))
            contract = self._acceptance_contract(connection)
            if contract is not None and self._is_acceptance_owned_request(request, contract):
                raise BrokerError(
                    "acceptance-owned request requires the exact acceptance cancel path",
                )
            if request.state == RequestState.TERMINAL:
                connection.commit()
                return self._public_record(request, lease)
            new_epoch = lease.lease_epoch + (1 if lease.granted_slots != 0 else 0)
            connection.execute(
                """
                UPDATE capacity_requests
                SET cancel_requested = 1, terminal_reason = ?, updated_at = ?
                WHERE id = ?
                """,
                (reason, now_text, request_id),
            )
            connection.execute(
                """
                UPDATE capacity_leases
                SET lease_epoch = ?, granted_slots = 0, updated_at = ?
                WHERE request_id = ?
                """,
                (new_epoch, now_text, request_id),
            )
            self._audit(
                connection,
                request_id=request_id,
                event_type="cancel_requested",
                occurred_at=now_text,
                details={"reason": reason, "lease_epoch": new_epoch},
            )
            self._refresh_states(connection, now_text=now_text)
            updated = _record_from_row(self._joined_row(connection, request_id))
            connection.commit()
            return self._public_record(*updated)
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    def reconcile(
        self,
        budgets: BrokerBudgets,
        *,
        observations: Sequence[LeaseObservation] = (),
    ) -> dict[str, object]:
        now = _utc(self._clock())
        now_text = _timestamp(now)
        self.initialize()
        connection = self._transaction()
        try:
            self._expire_requests(connection, now=now, now_text=now_text)
            for observation in observations:
                self._apply_observation(
                    connection,
                    observation,
                    now=now,
                    now_text=now_text,
                )
            records = self._records(connection)
            pools = {record.request.pool for record in records if record.eligible}
            budgets.validate_for_pools(pools)
            self._persist_budgets(connection, budgets, now_text=now_text)

            fair_targets = self._fair_targets(records, budgets)
            self._apply_reductions(
                connection,
                records,
                fair_targets,
                now_text=now_text,
            )
            records = self._records(connection)
            self._apply_safe_increases(
                connection,
                records,
                fair_targets,
                budgets,
                now_text=now_text,
            )
            self._refresh_states(connection, now_text=now_text)
            report = self._status_from_connection(connection)
            connection.commit()
            return report
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    def status(self, *, request_id: str | None = None) -> dict[str, object]:
        self.initialize()
        with self._connect() as connection:
            return self._status_from_connection(connection, request_id=request_id)

    def _status_from_connection(
        self,
        connection: sqlite3.Connection,
        *,
        request_id: str | None = None,
    ) -> dict[str, object]:
        records = self._records(connection, request_id=request_id)
        budget_rows = connection.execute(
            "SELECT * FROM capacity_budgets ORDER BY scope",
        ).fetchall()
        budgets: dict[str, object] = {
            "global_slots": 0,
            "global_pending_slots": 0,
            "pool_slots": {},
            "pool_pending_slots": {},
        }
        for row in budget_rows:
            if row["scope"] == "global":
                budgets["global_slots"] = row["slot_budget"]
                budgets["global_pending_slots"] = row["pending_slot_budget"]
            else:
                pool = row["scope"].removeprefix("pool:")
                assert isinstance(budgets["pool_slots"], dict)
                assert isinstance(budgets["pool_pending_slots"], dict)
                budgets["pool_slots"][pool] = row["slot_budget"]
                budgets["pool_pending_slots"][pool] = row["pending_slot_budget"]

        aggregate = {
            "requested_slots": sum(record.request.target_slots for record in records),
            "granted_slots": sum(record.lease.granted_slots for record in records),
            "active_slots": sum(record.lease.active_slots for record in records),
            "pending_slots": sum(record.lease.pending_slots for record in records),
            "draining_slots": sum(record.lease.draining_slots for record in records),
            "terminal_slots": sum(record.lease.terminal_slots for record in records),
            "committed_slots": sum(record.lease.committed_slots for record in records),
        }
        requests = [self._public_record(record.request, record.lease) for record in records]
        handoffs = [
            self._handoff(record).public_dict()
            for record in records
            if record.request.state != RequestState.TERMINAL or record.lease.lease_epoch > 0
        ]
        audit_rows = connection.execute(
            """
            SELECT sequence, request_id, event_type, occurred_at, details_json
            FROM capacity_audit_events
            ORDER BY sequence
            """
        ).fetchall()
        audit = [
            {
                "sequence": row["sequence"],
                "request_id": row["request_id"],
                "event_type": row["event_type"],
                "occurred_at": row["occurred_at"],
                "details": json.loads(row["details_json"]),
            }
            for row in audit_rows
            if request_id is None or row["request_id"] == request_id
        ]
        return {
            "schema_version": 1,
            "authority": "shared-capacity-broker",
            "budgets": budgets,
            "aggregate": aggregate,
            "requests": requests,
            "handoffs": handoffs,
            "audit": audit,
        }

    @staticmethod
    def _public_record(
        request: CapacityRequest,
        lease: CapacityLease,
    ) -> dict[str, object]:
        return {
            "request": request.public_dict(),
            "lease": lease.public_dict(),
        }

    @staticmethod
    def _handoff(record: _MutableRecord) -> AutoscalerGrantHandoff:
        request = record.request
        lease = record.lease
        return AutoscalerGrantHandoff(
            schema_version=1,
            request_id=request.id,
            lease_epoch=lease.lease_epoch,
            sandbox=request.sandbox.value,
            environment=request.sandbox.environment,
            candidate_sha=request.candidate_sha,
            pool_name=request.pool,
            enabled=lease.granted_slots > 0 and not request.cancel_requested,
            min_slots=0,
            max_slots=lease.granted_slots,
            expires_at=request.expires_at,
            preemptible=request.preemptible,
        )

    def _fair_targets(
        self,
        records: Sequence[_MutableRecord],
        budgets: BrokerBudgets,
    ) -> dict[str, int]:
        eligible = [record for record in records if record.eligible]
        targets = {record.request.id: 0 for record in records}
        sandbox_allocations = {record.request.sandbox: 0 for record in records}
        pool_allocations = {pool: 0 for pool in budgets.pool_slots}
        global_remaining = budgets.global_slots

        for phase in ("minimum", "target"):
            while global_remaining > 0:
                candidates = [
                    record
                    for record in eligible
                    if pool_allocations.get(record.request.pool, 0)
                    < budgets.pool_slots[record.request.pool]
                    and targets[record.request.id]
                    < (
                        record.request.min_slots
                        if phase == "minimum"
                        else record.request.target_slots
                    )
                ]
                if not candidates:
                    break
                record = min(
                    candidates,
                    key=lambda item: (
                        sandbox_allocations[item.request.sandbox],
                        item.last_granted_seq,
                        item.request.created_at,
                        item.request.sandbox.value,
                        item.request.pool,
                        item.request.id,
                    ),
                )
                targets[record.request.id] += 1
                sandbox_allocations[record.request.sandbox] += 1
                pool_allocations[record.request.pool] = (
                    pool_allocations.get(record.request.pool, 0) + 1
                )
                global_remaining -= 1
        return targets

    def _apply_reductions(
        self,
        connection: sqlite3.Connection,
        records: Sequence[_MutableRecord],
        fair_targets: Mapping[str, int],
        *,
        now_text: str,
    ) -> None:
        for record in records:
            target = fair_targets.get(record.request.id, 0)
            if record.lease.granted_slots <= target:
                continue
            epoch = record.lease.lease_epoch + 1
            previous = record.lease.granted_slots
            connection.execute(
                """
                UPDATE capacity_leases
                SET lease_epoch = ?, granted_slots = ?, updated_at = ?
                WHERE request_id = ?
                """,
                (epoch, target, now_text, record.request.id),
            )
            self._audit(
                connection,
                request_id=record.request.id,
                event_type="grant_reduced",
                occurred_at=now_text,
                details={
                    "previous_granted_slots": previous,
                    "granted_slots": target,
                    "lease_epoch": epoch,
                },
            )

    def _apply_safe_increases(
        self,
        connection: sqlite3.Connection,
        records: Sequence[_MutableRecord],
        fair_targets: Mapping[str, int],
        budgets: BrokerBudgets,
        *,
        now_text: str,
    ) -> None:
        committed_global = sum(record.lease.committed_slots for record in records)
        committed_by_pool = {
            pool: sum(
                record.lease.committed_slots for record in records if record.request.pool == pool
            )
            for pool in budgets.pool_slots
        }
        pending_global = sum(record.lease.pending_slots for record in records)
        pending_by_pool = {
            pool: sum(
                record.lease.pending_slots for record in records if record.request.pool == pool
            )
            for pool in budgets.pool_slots
        }
        deltas: dict[str, int] = {record.request.id: 0 for record in records}
        grants = {record.request.id: record.lease.granted_slots for record in records}
        last_seq = {record.request.id: record.last_granted_seq for record in records}
        next_sequence = max(last_seq.values(), default=0)
        sandbox_grants = {
            sandbox: sum(
                record.lease.granted_slots
                for record in records
                if record.request.sandbox == sandbox
            )
            for sandbox in {record.request.sandbox for record in records}
        }

        while True:
            candidates = [
                record
                for record in records
                if record.eligible
                and grants[record.request.id] < fair_targets.get(record.request.id, 0)
                and committed_global < budgets.global_slots
                and committed_by_pool.get(record.request.pool, 0)
                < budgets.pool_slots[record.request.pool]
                and pending_global < budgets.global_pending_slots
                and pending_by_pool.get(record.request.pool, 0)
                < budgets.pool_pending_slots[record.request.pool]
            ]
            if not candidates:
                break
            record = min(
                candidates,
                key=lambda item: (
                    sandbox_grants[item.request.sandbox],
                    last_seq[item.request.id],
                    item.request.created_at,
                    item.request.sandbox.value,
                    item.request.pool,
                    item.request.id,
                ),
            )
            request_id = record.request.id
            pool = record.request.pool
            grants[request_id] += 1
            deltas[request_id] += 1
            next_sequence += 1
            last_seq[request_id] = next_sequence
            committed_global += 1
            committed_by_pool[pool] = committed_by_pool.get(pool, 0) + 1
            pending_global += 1
            pending_by_pool[pool] = pending_by_pool.get(pool, 0) + 1
            sandbox_grants[record.request.sandbox] += 1

        for record in records:
            delta = deltas[record.request.id]
            if delta <= 0:
                continue
            epoch = record.lease.lease_epoch + 1
            connection.execute(
                """
                UPDATE capacity_leases
                SET lease_epoch = ?, granted_slots = ?, pending_slots = ?,
                    updated_at = ?
                WHERE request_id = ?
                """,
                (
                    epoch,
                    grants[record.request.id],
                    record.lease.pending_slots + delta,
                    now_text,
                    record.request.id,
                ),
            )
            connection.execute(
                """
                UPDATE capacity_requests
                SET last_granted_seq = ?, updated_at = ?
                WHERE id = ?
                """,
                (last_seq[record.request.id], now_text, record.request.id),
            )
            self._audit(
                connection,
                request_id=record.request.id,
                event_type="grant_increased",
                occurred_at=now_text,
                details={
                    "previous_granted_slots": record.lease.granted_slots,
                    "granted_slots": grants[record.request.id],
                    "lease_epoch": epoch,
                },
            )

    def _expire_requests(
        self,
        connection: sqlite3.Connection,
        *,
        now: datetime,
        now_text: str,
    ) -> None:
        rows = connection.execute(
            """
            SELECT id, expires_at
            FROM capacity_requests
            WHERE state != ? AND cancel_requested = 0
            """,
            (RequestState.TERMINAL.value,),
        ).fetchall()
        for row in rows:
            if _parse_timestamp(row["expires_at"]) > now:
                continue
            connection.execute(
                """
                UPDATE capacity_requests
                SET cancel_requested = 1, terminal_reason = 'ttl_expired',
                    updated_at = ?
                WHERE id = ?
                """,
                (now_text, row["id"]),
            )
            lease = connection.execute(
                "SELECT * FROM capacity_leases WHERE request_id = ?",
                (row["id"],),
            ).fetchone()
            assert lease is not None
            epoch = lease["lease_epoch"] + (1 if lease["granted_slots"] else 0)
            connection.execute(
                """
                UPDATE capacity_leases
                SET lease_epoch = ?, granted_slots = 0, updated_at = ?
                WHERE request_id = ?
                """,
                (epoch, now_text, row["id"]),
            )
            self._audit(
                connection,
                request_id=row["id"],
                event_type="ttl_expired",
                occurred_at=now_text,
                details={"lease_epoch": epoch},
            )

    def _apply_observation(
        self,
        connection: sqlite3.Connection,
        observation: LeaseObservation,
        *,
        now: datetime,
        now_text: str,
    ) -> None:
        request, lease = _record_from_row(
            self._joined_row(connection, observation.request_id),
        )
        if observation.payload_sha256 != _lease_observation_digest(observation):
            raise BrokerError("observation payload_sha256 does not match its payload")
        if (
            observation.sandbox != request.sandbox
            or observation.pool_name != request.pool
            or observation.candidate_sha != request.candidate_sha
        ):
            raise BrokerError(
                f"observation binding differs from request {observation.request_id}",
            )
        if observation.lease_epoch != lease.lease_epoch:
            raise BrokerError(
                f"observation lease_epoch is stale for request {observation.request_id}",
            )
        expected_lease_states = (
            {"active"}
            if lease.granted_slots > 0 and not request.cancel_requested
            else {"retiring", "retired"}
        )
        if observation.capacity_lease_state not in expected_lease_states:
            raise BrokerError(
                "observation policy lease state differs from the current grant",
            )
        observed_at = _parse_timestamp(observation.observed_at)
        if observed_at > now + _OBSERVATION_MAX_FUTURE_SKEW:
            raise BrokerError("observation timestamp is ahead of the broker clock")
        if now - observed_at > _OBSERVATION_MAX_AGE:
            raise BrokerError("observation is stale")
        if lease.last_observation_sequence is not None:
            if observation.observation_sequence < lease.last_observation_sequence:
                raise BrokerError("observation sequence regressed")
            if observation.observation_sequence == lease.last_observation_sequence:
                if observation.payload_sha256 != lease.last_observation_digest:
                    raise BrokerError("observation sequence was rebound to another payload")
                return
            if lease.last_observed_at is not None and observed_at < _parse_timestamp(
                lease.last_observed_at
            ):
                raise BrokerError("observation timestamp regressed")
        nonterminal = (
            observation.pending_slots + observation.active_slots + observation.draining_slots
        )
        allowed_nonterminal = max(lease.committed_slots, lease.granted_slots)
        if nonterminal > allowed_nonterminal:
            raise BrokerError(
                f"observation exceeds broker commitment for request {observation.request_id}",
            )
        if observation.terminal_slots < lease.terminal_slots:
            raise BrokerError(
                f"observation terminal_slots regressed for request {observation.request_id}",
            )
        pending = observation.pending_slots
        if not request.cancel_requested and lease.granted_slots > nonterminal:
            pending += lease.granted_slots - nonterminal
        connection.execute(
            """
            UPDATE capacity_leases
            SET pending_slots = ?, active_slots = ?, draining_slots = ?,
                terminal_slots = ?, last_observed_at = ?,
                last_observation_sequence = ?, last_observation_digest = ?,
                last_policy_lease_state = ?, updated_at = ?
            WHERE request_id = ?
            """,
            (
                pending,
                observation.active_slots,
                observation.draining_slots,
                observation.terminal_slots,
                observation.observed_at,
                observation.observation_sequence,
                observation.payload_sha256,
                observation.capacity_lease_state,
                now_text,
                observation.request_id,
            ),
        )
        self._audit(
            connection,
            request_id=observation.request_id,
            event_type="lease_observed",
            occurred_at=now_text,
            details={
                "lease_epoch": observation.lease_epoch,
                "sandbox": observation.sandbox.value,
                "pool_name": observation.pool_name,
                "candidate_sha": observation.candidate_sha,
                "capacity_lease_state": observation.capacity_lease_state,
                "observed_at": observation.observed_at,
                "observation_sequence": observation.observation_sequence,
                "payload_sha256": observation.payload_sha256,
                "pending_slots": pending,
                "active_slots": observation.active_slots,
                "draining_slots": observation.draining_slots,
                "terminal_slots": observation.terminal_slots,
            },
        )

    def _refresh_states(self, connection: sqlite3.Connection, *, now_text: str) -> None:
        for record in self._records(connection):
            request = record.request
            lease = record.lease
            nonterminal = lease.pending_slots + lease.active_slots + lease.draining_slots
            if request.cancel_requested and lease.granted_slots == 0 and nonterminal == 0:
                state = RequestState.TERMINAL
            elif request.cancel_requested or nonterminal > lease.granted_slots:
                state = RequestState.DRAINING
            elif lease.active_slots > 0:
                state = RequestState.ACTIVE
            else:
                state = RequestState.PENDING
            connection.execute(
                """
                UPDATE capacity_requests SET state = ?, updated_at = ? WHERE id = ?
                """,
                (state.value, now_text, request.id),
            )
            connection.execute(
                """
                UPDATE capacity_leases SET state = ?, updated_at = ? WHERE request_id = ?
                """,
                (state.value, now_text, request.id),
            )

    def _persist_budgets(
        self,
        connection: sqlite3.Connection,
        budgets: BrokerBudgets,
        *,
        now_text: str,
    ) -> None:
        connection.execute("DELETE FROM capacity_budgets")
        connection.execute(
            """
            INSERT INTO capacity_budgets(scope, slot_budget, pending_slot_budget, updated_at)
            VALUES('global', ?, ?, ?)
            """,
            (budgets.global_slots, budgets.global_pending_slots, now_text),
        )
        for pool in sorted(budgets.pool_slots):
            connection.execute(
                """
                INSERT INTO capacity_budgets(
                    scope, slot_budget, pending_slot_budget, updated_at
                ) VALUES(?, ?, ?, ?)
                """,
                (
                    f"pool:{pool}",
                    budgets.pool_slots[pool],
                    budgets.pool_pending_slots[pool],
                    now_text,
                ),
            )

    @staticmethod
    def _audit(
        connection: sqlite3.Connection,
        *,
        request_id: str,
        event_type: str,
        occurred_at: str,
        details: Mapping[str, object],
    ) -> None:
        connection.execute(
            """
            INSERT INTO capacity_audit_events(
                request_id, event_type, occurred_at, details_json
            ) VALUES(?, ?, ?, ?)
            """,
            (
                request_id,
                event_type,
                occurred_at,
                json.dumps(details, sort_keys=True, separators=(",", ":")),
            ),
        )

    @staticmethod
    def _joined_row(connection: sqlite3.Connection, request_id: str) -> sqlite3.Row:
        row = connection.execute(
            """
            SELECT r.*, l.lease_epoch, l.granted_slots, l.pending_slots,
                   l.active_slots, l.draining_slots, l.terminal_slots,
                   l.state AS lease_state, l.last_observed_at,
                   l.last_observation_sequence, l.last_observation_digest,
                   l.last_policy_lease_state,
                   l.updated_at AS lease_updated_at
            FROM capacity_requests r
            JOIN capacity_leases l ON l.request_id = r.id
            WHERE r.id = ?
            """,
            (request_id,),
        ).fetchone()
        if row is None:
            raise BrokerError(f"capacity request {request_id} was not found")
        return cast(sqlite3.Row, row)

    def _records(
        self,
        connection: sqlite3.Connection,
        *,
        request_id: str | None = None,
    ) -> list[_MutableRecord]:
        query = """
            SELECT r.*, l.lease_epoch, l.granted_slots, l.pending_slots,
                   l.active_slots, l.draining_slots, l.terminal_slots,
                   l.state AS lease_state, l.last_observed_at,
                   l.last_observation_sequence, l.last_observation_digest,
                   l.last_policy_lease_state,
                   l.updated_at AS lease_updated_at
            FROM capacity_requests r
            JOIN capacity_leases l ON l.request_id = r.id
        """
        parameters: tuple[object, ...] = ()
        if request_id is not None:
            query += " WHERE r.id = ?"
            parameters = (request_id,)
        query += " ORDER BY r.created_at, r.id"
        rows = connection.execute(query, parameters).fetchall()
        if request_id is not None and not rows:
            raise BrokerError(f"capacity request {request_id} was not found")
        return [
            _MutableRecord(
                request=_record_from_row(row)[0],
                lease=_record_from_row(row)[1],
                last_granted_seq=int(row["last_granted_seq"]),
            )
            for row in rows
        ]


def _record_from_row(row: sqlite3.Row) -> tuple[CapacityRequest, CapacityLease]:
    lease_state_key = "lease_state" if "lease_state" in row.keys() else "state"
    lease_updated_key = "lease_updated_at" if "lease_updated_at" in row.keys() else "updated_at"
    request = CapacityRequest(
        id=row["id"],
        sandbox=SandboxId(row["sandbox"]),
        candidate_sha=row["candidate_sha"],
        pool=row["pool"],
        min_slots=int(row["min_slots"]),
        target_slots=int(row["target_slots"]),
        ttl_seconds=int(row["ttl_seconds"]),
        purpose=row["purpose"],
        preemptible=bool(row["preemptible"]),
        idempotency_key=row["idempotency_key"],
        state=RequestState(row["state"]),
        created_at=row["created_at"],
        expires_at=row["expires_at"],
        cancel_requested=bool(row["cancel_requested"]),
        terminal_reason=row["terminal_reason"],
    )
    lease = CapacityLease(
        request_id=request.id,
        lease_epoch=int(row["lease_epoch"]),
        granted_slots=int(row["granted_slots"]),
        pending_slots=int(row["pending_slots"]),
        active_slots=int(row["active_slots"]),
        draining_slots=int(row["draining_slots"]),
        terminal_slots=int(row["terminal_slots"]),
        state=RequestState(row[lease_state_key]),
        last_observed_at=row["last_observed_at"],
        last_observation_sequence=(
            int(row["last_observation_sequence"])
            if row["last_observation_sequence"] is not None
            else None
        ),
        last_observation_digest=row["last_observation_digest"],
        last_policy_lease_state=row["last_policy_lease_state"],
        updated_at=row[lease_updated_key],
    )
    return request, lease


def _budget_entries(values: Sequence[str], field: str) -> dict[str, int]:
    budgets: dict[str, int] = {}
    for value in values:
        if "=" not in value:
            raise BrokerError(f"{field} entries must use POOL=SLOTS")
        pool, raw_slots = value.split("=", 1)
        if _POOL_RE.fullmatch(pool) is None:
            raise BrokerError(f"{field} contains an invalid pool")
        try:
            slots = int(raw_slots)
        except ValueError as exc:
            raise BrokerError(f"{field} slots must be integers") from exc
        if slots < 0:
            raise BrokerError(f"{field} slots must be non-negative")
        if pool in budgets:
            raise BrokerError(f"{field} contains duplicate pool {pool}")
        budgets[pool] = slots
    return budgets


def _load_observations(path: Path | None) -> tuple[LeaseObservation, ...]:
    if path is None:
        return ()
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise BrokerError("observation file is unavailable or invalid JSON") from exc
    if not isinstance(raw, list):
        raise BrokerError("observation file must contain a JSON array")
    observations: list[LeaseObservation] = []
    for item in raw:
        if not isinstance(item, dict):
            raise BrokerError("each observation must be a JSON object")
        observations.append(LeaseObservation.from_mapping(item))
    return tuple(observations)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="shared-capacity-broker",
        description="Manage candidate-bound developer-sandbox capacity leases.",
        allow_abbrev=False,
    )
    parser.add_argument(
        "--state-db",
        type=Path,
        default=Path("/var/lib/loom-shared-capacity/broker.sqlite3"),
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    request = subparsers.add_parser("request", allow_abbrev=False)
    request.add_argument("--sandbox", required=True)
    request.add_argument("--candidate-sha", required=True)
    request.add_argument("--pool", required=True)
    request.add_argument("--min-slots", type=int, required=True)
    request.add_argument("--target-slots", type=int, required=True)
    request.add_argument("--ttl-minutes", type=int, required=True)
    request.add_argument("--purpose", required=True)
    request.add_argument("--idempotency-key", required=True)
    preemptible = request.add_mutually_exclusive_group(required=True)
    preemptible.add_argument("--preemptible", dest="preemptible", action="store_true")
    preemptible.add_argument(
        "--non-preemptible",
        dest="preemptible",
        action="store_false",
    )

    status = subparsers.add_parser("status", allow_abbrev=False)
    status.add_argument("--request-id")

    cancel = subparsers.add_parser("cancel", allow_abbrev=False)
    cancel.add_argument("--request-id", required=True)
    cancel.add_argument("--reason", default="operator_cancelled")

    reconcile = subparsers.add_parser("reconcile", allow_abbrev=False)
    reconcile.add_argument("--global-budget", type=int, required=True)
    reconcile.add_argument("--pool-budget", action="append", default=[], required=True)
    reconcile.add_argument("--global-pending-budget", type=int)
    reconcile.add_argument("--pool-pending-budget", action="append", default=[])
    reconcile.add_argument("--observations-json", type=Path)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = _parser()
    args = parser.parse_args(list(argv) if argv is not None else None)
    try:
        broker = SharedCapacityBroker(args.state_db)
        if args.command == "request":
            request, lease = broker.request_capacity(
                sandbox=args.sandbox,
                candidate_sha=args.candidate_sha,
                pool=args.pool,
                min_slots=args.min_slots,
                target_slots=args.target_slots,
                ttl_seconds=args.ttl_minutes * 60,
                purpose=args.purpose,
                preemptible=args.preemptible,
                idempotency_key=args.idempotency_key,
            )
            document = SharedCapacityBroker._public_record(request, lease)
        elif args.command == "status":
            document = broker.status(request_id=args.request_id)
        elif args.command == "cancel":
            document = broker.cancel(args.request_id, reason=args.reason)
        elif args.command == "reconcile":
            pool_slots = _budget_entries(args.pool_budget, "--pool-budget")
            pending_values = _budget_entries(
                args.pool_pending_budget,
                "--pool-pending-budget",
            )
            pool_pending = pending_values or dict(pool_slots)
            if set(pool_pending) != set(pool_slots):
                raise BrokerError("pending-slot budgets must cover exactly the slot-budget pools")
            global_pending = (
                args.global_budget
                if args.global_pending_budget is None
                else args.global_pending_budget
            )
            document = broker.reconcile(
                BrokerBudgets(
                    global_slots=args.global_budget,
                    pool_slots=pool_slots,
                    global_pending_slots=global_pending,
                    pool_pending_slots=pool_pending,
                ),
                observations=_load_observations(args.observations_json),
            )
        else:  # pragma: no cover - argparse owns the command set
            raise BrokerError("unsupported broker command")
    except (BrokerError, OSError, sqlite3.Error):
        sys.stderr.write('{"error":"shared-capacity-broker-failed-safely"}\n')
        return 1
    json.dump(document, sys.stdout, sort_keys=True, separators=(",", ":"))
    sys.stdout.write("\n")
    return 0


__all__ = [
    "AutoscalerGrantHandoff",
    "BrokerBudgets",
    "BrokerError",
    "CapacityLease",
    "CapacityRequest",
    "LeaseObservation",
    "RequestState",
    "SandboxId",
    "SharedCapacityBroker",
    "main",
]
