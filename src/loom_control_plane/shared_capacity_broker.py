"""Persistent fair-share capacity broker for disposable developer sandboxes.

The broker is intentionally independent from every sandbox Control Plane.  One
submit-host process owns the SQLite authority and emits candidate-bound grant
handoffs that a sandbox-specific adapter may apply to its local autoscaler
policy.  Sandboxes never mutate the shared budget directly.
"""

from __future__ import annotations

import argparse
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
from typing import Any, cast
from uuid import uuid4

_SHA_RE = re.compile(r"[0-9a-f]{40}")
_POOL_RE = re.compile(r"[a-z][a-z0-9-]{0,31}")
_ENVIRONMENT_RE = re.compile(r"[a-z][a-z0-9-]{0,62}")
_IDEMPOTENCY_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:-]{0,127}")
_PURPOSE_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9 ._:/()#,+-]{0,199}")
_SECRET_HINT_RE = re.compile(
    r"(?:bearer\s+|password|private[_ -]?key|api[_ -]?key|secret|token|"
    r"\bsk-[A-Za-z0-9_-]{8,})",
    re.IGNORECASE,
)
_SCHEMA_VERSION = "2"
_MAX_TTL_SECONDS = 24 * 60 * 60
_MAX_REQUEST_SLOTS = 10_000
_MAX_BUDGET_SLOTS = 100_000


class BrokerError(ValueError):
    """A bounded, secret-free broker failure."""


class SandboxId(StrEnum):
    QIANYI = "qianyi"
    HONGJIAN = "hongjian"
    DEVANSH = "devansh"

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
    sandbox: str
    deployment_generation: int
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
        return {**asdict(self), "state": self.state.value}


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
    request_id: str
    lease_epoch: int
    pending_slots: int
    active_slots: int
    draining_slots: int
    terminal_slots: int

    @classmethod
    def from_mapping(cls, value: Mapping[str, object]) -> LeaseObservation:
        request_id = _exact_text(value.get("request_id"), "request_id")
        return cls(
            request_id=request_id,
            lease_epoch=_nonnegative_int(value.get("lease_epoch"), "lease_epoch"),
            pending_slots=_nonnegative_int(value.get("pending_slots"), "pending_slots"),
            active_slots=_nonnegative_int(value.get("active_slots"), "active_slots"),
            draining_slots=_nonnegative_int(value.get("draining_slots"), "draining_slots"),
            terminal_slots=_nonnegative_int(value.get("terminal_slots"), "terminal_slots"),
        )


@dataclass(frozen=True, slots=True)
class AutoscalerGrantHandoff:
    schema_version: int
    request_id: str
    lease_epoch: int
    sandbox: str
    environment: str
    deployment_generation: int
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


def _runtime_environment(identifier: str) -> str:
    """Map a legacy sandbox name or dynamic registry ID to runtime identity."""
    if identifier in {item.value for item in SandboxId}:
        return f"sandbox-{identifier}"
    return identifier


def _validate_request_input(
    *,
    sandbox: SandboxId | str,
    deployment_generation: int,
    candidate_sha: str,
    pool: str,
    min_slots: int,
    target_slots: int,
    ttl_seconds: int,
    purpose: str,
    preemptible: bool,
    idempotency_key: str,
) -> tuple[str, str, str, str, str]:
    normalized_sandbox = sandbox.value if isinstance(sandbox, SandboxId) else str(sandbox)
    if _ENVIRONMENT_RE.fullmatch(normalized_sandbox) is None:
        raise BrokerError("sandbox must be a lowercase bounded environment identifier")
    if (
        isinstance(deployment_generation, bool)
        or not isinstance(deployment_generation, int)
        or deployment_generation <= 0
    ):
        raise BrokerError("deployment_generation must be a positive integer")
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


class SharedCapacityBroker:
    """Single persistent authority for a dynamic development cohort.

    ``SandboxId`` remains a legacy-v1 input convenience, but the durable
    authority accepts any validated registry environment identifier.  Cohort
    membership is therefore data-driven rather than compiled into this module.
    """

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
                    deployment_generation INTEGER NOT NULL DEFAULT 1,
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
            current = connection.execute(
                "SELECT value FROM broker_meta WHERE key = 'schema_version'",
            ).fetchone()
            if current is None:
                connection.execute(
                    "INSERT INTO broker_meta(key, value) VALUES('schema_version', ?)",
                    (_SCHEMA_VERSION,),
                )
            elif current["value"] == "1":
                columns = {
                    str(row["name"])
                    for row in connection.execute(
                        "PRAGMA table_info(capacity_requests)",
                    ).fetchall()
                }
                if "deployment_generation" not in columns:
                    connection.execute(
                        "ALTER TABLE capacity_requests "
                        "ADD COLUMN deployment_generation INTEGER NOT NULL DEFAULT 1",
                    )
                connection.execute(
                    "UPDATE broker_meta SET value = ? WHERE key = 'schema_version'",
                    (_SCHEMA_VERSION,),
                )
            elif current["value"] != _SCHEMA_VERSION:
                raise BrokerError("broker state schema version is unsupported")

    def _connect(self) -> sqlite3.Connection:
        self._validate_authority_file(self.state_db, mode=0o600)
        existing_sidecars = {
            path for path in self._sqlite_sidecar_paths() if path.exists() or path.is_symlink()
        }
        for path in existing_sidecars:
            self._validate_authority_file(path, mode=0o600)
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
                except OSError as exc:
                    connection.close()
                    raise BrokerError("broker SQLite sidecar authority is unsafe") from exc
            self._validate_authority_file(path, mode=0o600)
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
    def _validate_authority_file(path: Path, *, mode: int) -> None:
        try:
            metadata = os.lstat(path)
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

    def _sqlite_sidecar_paths(self) -> tuple[Path, ...]:
        return tuple(
            self.state_db.with_name(f"{self.state_db.name}{suffix}")
            for suffix in ("-wal", "-shm", "-journal")
        )

    def _transaction(self) -> sqlite3.Connection:
        connection = self._connect()
        connection.execute("BEGIN IMMEDIATE")
        return connection

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
        deployment_generation: int = 1,
    ) -> tuple[CapacityRequest, CapacityLease]:
        (
            normalized_sandbox,
            candidate_sha,
            pool,
            purpose,
            idempotency_key,
        ) = _validate_request_input(
            sandbox=sandbox,
            deployment_generation=deployment_generation,
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
                    deployment_generation,
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
                    request.deployment_generation,
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

            request_id = str(uuid4())
            connection.execute(
                """
                INSERT INTO capacity_requests(
                    id, sandbox, deployment_generation, candidate_sha, pool,
                    min_slots, target_slots,
                    ttl_seconds, purpose, preemptible, idempotency_key, state,
                    created_at, expires_at, updated_at
                ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    request_id,
                    normalized_sandbox,
                    deployment_generation,
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
                    "sandbox": normalized_sandbox,
                    "deployment_generation": deployment_generation,
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

    def renew(self, request_id: str, *, ttl_seconds: int) -> dict[str, object]:
        """Extend one live request without changing its grant or lease epoch."""
        request_id = _exact_text(request_id, "request_id")
        if (
            isinstance(ttl_seconds, bool)
            or not isinstance(ttl_seconds, int)
            or not 60 <= ttl_seconds <= _MAX_TTL_SECONDS
        ):
            raise BrokerError(f"ttl_seconds must be in 60..{_MAX_TTL_SECONDS}")
        now = _utc(self._clock())
        now_text = _timestamp(now)
        expires_at = _timestamp(now + timedelta(seconds=ttl_seconds))
        self.initialize()
        connection = self._transaction()
        try:
            request, _ = _record_from_row(self._joined_row(connection, request_id))
            if request.cancel_requested or request.state == RequestState.TERMINAL:
                raise BrokerError("only a live non-cancelled capacity request can be renewed")
            connection.execute(
                """
                UPDATE capacity_requests
                SET ttl_seconds = ?, expires_at = ?, updated_at = ?
                WHERE id = ?
                """,
                (ttl_seconds, expires_at, now_text, request_id),
            )
            self._audit(
                connection,
                request_id=request_id,
                event_type="ttl_renewed",
                occurred_at=now_text,
                details={"expires_at": expires_at, "ttl_seconds": ttl_seconds},
            )
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
                self._apply_observation(connection, observation, now_text=now_text)
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
            sandbox=request.sandbox,
            environment=_runtime_environment(request.sandbox),
            deployment_generation=request.deployment_generation,
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
        sandbox_allocations = {record.request.sandbox: 0 for record in eligible}
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
                        item.request.sandbox,
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
                    item.request.sandbox,
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
        now_text: str,
    ) -> None:
        request, lease = _record_from_row(
            self._joined_row(connection, observation.request_id),
        )
        if observation.lease_epoch != lease.lease_epoch:
            raise BrokerError(
                f"observation lease_epoch is stale for request {observation.request_id}",
            )
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
                terminal_slots = ?, last_observed_at = ?, updated_at = ?
            WHERE request_id = ?
            """,
            (
                pending,
                observation.active_slots,
                observation.draining_slots,
                observation.terminal_slots,
                now_text,
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
        sandbox=str(row["sandbox"]),
        deployment_generation=int(row["deployment_generation"]),
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
    request.add_argument("--deployment-generation", type=int, default=1)
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
                deployment_generation=args.deployment_generation,
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
