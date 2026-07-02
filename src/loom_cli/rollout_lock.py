"""Durable operator lease for protected rollout mutation paths."""

from __future__ import annotations

import fcntl
import json
import os
import socket
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import TracebackType
from typing import Any, TextIO

DEFAULT_ROLLOUT_LOCK_TTL_SECONDS = 4 * 60 * 60
_HELD_LOCKS: dict[Path, dict[str, Any]] = {}


class RolloutLeaseError(RuntimeError):
    """Raised when a rollout mutation lease cannot be acquired."""

    def __init__(self, message: str, diagnostic: dict[str, Any]) -> None:
        super().__init__(message)
        self.diagnostic = diagnostic


def _safe_environment_name(environment: str) -> str:
    cleaned = "".join(
        char if char.isalnum() or char in {"-", "_", "."} else "-"
        for char in environment.strip()
    )
    return cleaned or "unknown"


def default_rollout_lock_dir() -> Path:
    configured = os.environ.get("LOOM_ROLLOUT_LOCK_DIR")
    if configured:
        return Path(configured)
    return Path.home() / ".loom" / "rollout-locks"


def rollout_owner_id(environment: str, provided: str | None = None) -> str:
    if provided and provided.strip():
        return provided.strip()
    return f"{environment}-{socket.gethostname()}-{os.getpid()}"


def _parse_json_text(raw: str) -> dict[str, Any] | None:
    if not raw.strip():
        return None
    try:
        loaded = json.loads(raw)
    except json.JSONDecodeError:
        return None
    return loaded if isinstance(loaded, dict) else None


def _read_json(path: Path) -> dict[str, Any] | None:
    try:
        return _parse_json_text(path.read_text(encoding="utf-8"))
    except OSError:
        return None


def _read_json_from_handle(handle: TextIO) -> dict[str, Any] | None:
    handle.seek(0)
    return _parse_json_text(handle.read())


def _write_json_to_handle(handle: TextIO, payload: dict[str, Any]) -> None:
    handle.seek(0)
    handle.truncate(0)
    handle.write(json.dumps(payload, indent=2, sort_keys=True) + "\n")
    handle.flush()
    os.fsync(handle.fileno())


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    tmp_path.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    os.replace(tmp_path, path)


@dataclass
class RolloutLease:
    """Acquired rollout lease held by an exclusive OS file lock."""

    lock_path: Path
    environment: str
    owner_id: str
    acquired_at: datetime
    expires_at: datetime
    _handle: TextIO
    evidence_path: Path | None = None
    stale_owner_id: str | None = None
    _released: bool = False

    def release(self, *, status: str = "released") -> None:
        if self._released:
            return
        released_at = datetime.now(UTC)
        try:
            active = _read_json_from_handle(self._handle)
            if active and active.get("owner_id") == self.owner_id:
                active["released_at"] = released_at.isoformat()
                active["release_status"] = status
                _write_json_to_handle(self._handle, active)
            if self.evidence_path is not None:
                _append_evidence_event(
                    self.evidence_path,
                    {
                        "event": "released",
                        "status": status,
                        "environment": self.environment,
                        "owner_id": self.owner_id,
                        "released_at": released_at.isoformat(),
                    },
                )
        finally:
            _HELD_LOCKS.pop(self.lock_path.resolve(), None)
            fcntl.flock(self._handle.fileno(), fcntl.LOCK_UN)
            self._handle.close()
            self._released = True

    def __enter__(self) -> RolloutLease:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        self.release(status="failed" if exc_type is not None else "released")


def _append_evidence_event(path: Path, event: dict[str, Any]) -> None:
    existing = _read_json(path) or {"schema_version": 1, "events": []}
    events = existing.get("events")
    if not isinstance(events, list):
        events = []
    events.append(event)
    existing["schema_version"] = 1
    existing["events"] = events
    _write_json(path, existing)


class RolloutLeaseManager:
    """Acquire per-environment rollout mutation leases."""

    def __init__(
        self,
        lock_dir: Path | str,
        *,
        now: Callable[[], datetime] | None = None,
    ) -> None:
        self.lock_dir = Path(lock_dir)
        self._now = now or (lambda: datetime.now(UTC))

    def lock_path_for(self, environment: str) -> Path:
        return self.lock_dir / f"{_safe_environment_name(environment)}.lock"

    def acquire(
        self,
        *,
        environment: str,
        owner_id: str,
        ttl_seconds: int,
        command: Sequence[str],
        evidence_path: Path | str | None = None,
        force: bool = False,
    ) -> RolloutLease:
        if ttl_seconds <= 0:
            raise ValueError("ttl_seconds must be positive")
        environment = environment.strip()
        if not environment:
            raise ValueError("environment must be a non-empty string")
        owner_id = owner_id.strip()
        if not owner_id:
            raise ValueError("owner_id must be a non-empty string")

        self.lock_dir.mkdir(parents=True, exist_ok=True)
        lock_path = self.lock_path_for(environment)
        lock_key = lock_path.resolve()
        evidence = Path(evidence_path) if evidence_path is not None else None
        handle = lock_path.open("a+", encoding="utf-8")
        lock_acquired = False
        try:
            if lock_key in _HELD_LOCKS:
                raise self._conflict(
                    environment=environment,
                    reason="active_rollout_lease",
                    previous=_HELD_LOCKS[lock_key],
                )
            try:
                fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                lock_acquired = True
            except BlockingIOError:
                active_record = _read_json_from_handle(handle) or {}
                raise self._conflict(
                    environment=environment,
                    reason="active_rollout_lease",
                    previous=active_record,
                ) from None

            previous = _read_json_from_handle(handle)
            stale_owner_id = self._stale_owner_id(previous)
            acquired_at = self._normalized_now()
            expires_at = acquired_at + timedelta(seconds=ttl_seconds)
            lease_payload: dict[str, Any] = {
                "schema_version": 1,
                "environment": environment,
                "owner_id": owner_id,
                "acquired_at": acquired_at.isoformat(),
                "expires_at": expires_at.isoformat(),
                "ttl_seconds": ttl_seconds,
                "hostname": socket.gethostname(),
                "pid": os.getpid(),
                "command": list(command),
                "forced": bool(force),
            }
            if stale_owner_id:
                lease_payload["replaced_stale_owner_id"] = stale_owner_id
            _write_json_to_handle(handle, lease_payload)

            if evidence is not None:
                event = {
                    "event": "acquired",
                    "environment": environment,
                    "owner_id": owner_id,
                    "acquired_at": acquired_at.isoformat(),
                    "expires_at": expires_at.isoformat(),
                    "lock_path": str(lock_path),
                    "forced": bool(force),
                }
                if stale_owner_id:
                    event["replaced_owner_id"] = stale_owner_id
                try:
                    _append_evidence_event(evidence, event)
                except OSError as exc:
                    handle.seek(0)
                    handle.truncate(0)
                    handle.flush()
                    os.fsync(handle.fileno())
                    _HELD_LOCKS.pop(lock_key, None)
                    fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
                    handle.close()
                    raise ValueError(
                        f"could not write rollout lock evidence {evidence}: {exc}",
                    ) from exc

            _HELD_LOCKS[lock_key] = dict(lease_payload)
            return RolloutLease(
                lock_path=lock_path,
                environment=environment,
                owner_id=owner_id,
                acquired_at=acquired_at,
                expires_at=expires_at,
                _handle=handle,
                evidence_path=evidence,
                stale_owner_id=stale_owner_id,
            )
        except Exception:
            if not handle.closed:
                if lock_acquired:
                    fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
                handle.close()
            raise

    def _normalized_now(self) -> datetime:
        now = self._now()
        if now.tzinfo is None:
            now = now.replace(tzinfo=UTC)
        return now.astimezone(UTC)

    def _stale_owner_id(self, previous: dict[str, Any] | None) -> str | None:
        if not previous:
            return None
        if previous.get("release_status") == "released":
            return None
        owner = previous.get("owner_id")
        return str(owner) if owner is not None else None

    def _conflict(
        self,
        *,
        environment: str,
        reason: str,
        previous: dict[str, Any],
    ) -> RolloutLeaseError:
        diagnostic = {
            "environment": environment,
            "reason": reason,
            "active_owner_id": previous.get("owner_id"),
            "active_acquired_at": previous.get("acquired_at"),
            "active_expires_at": previous.get("expires_at"),
            "active_hostname": previous.get("hostname"),
            "active_pid": previous.get("pid"),
            "active_command": previous.get("command"),
            "recovery": (
                "Wait for the active rollout to release the lease, or use "
                "--force-rollout-lock only after preserving evidence that the "
                "recorded owner is stale and no active process holds the lock."
            ),
        }
        owner = diagnostic["active_owner_id"] or "<unknown>"
        expires = diagnostic["active_expires_at"] or "<unknown>"
        message = (
            f"active rollout mutation lease for {environment!r} is held by "
            f"{owner} until {expires}; use --force-rollout-lock only after "
            "stale-owner evidence is preserved"
        )
        return RolloutLeaseError(message, diagnostic)
