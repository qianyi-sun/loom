"""Single-writer lease for the rollout reconciler (#1097 / #1085 phase 4 — increment 5).

The reconciler must be the *sole routine writer* of managed fields. This lease is
how: an actor acquires a time-bounded, environment-scoped lease before applying,
renews it while working, and releases it when done. Only one holder can hold an
unexpired lease at a time; a crashed holder's lease simply expires and the next
actor takes over.

Every acquisition mints a monotonically increasing **fencing token**. An actor
that lost the lease (it expired and someone else took it, bumping the token) is
detected as stale — the applier stamps its token and a writer holding an
out-of-date token is fenced out, so a paused-then-resumed old holder cannot apply
behind the current one.

File-backed and atomically written; pure w.r.t. time (the caller passes the
current instant and the expiry, so it is testable and deterministic). ISO-8601
timestamps must be zero-padded UTC (e.g. ``2026-07-30T20:00:00Z``) so lexical
comparison is chronological.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path


class LeaseError(RuntimeError):
    """Raised when the lease store is malformed or misused."""


class LeaseHeldError(LeaseError):
    """Raised when acquisition fails because another holder's lease is unexpired."""


@dataclass(frozen=True)
class Lease:
    holder: str
    fencing_token: int
    acquired_at: str
    expires_at: str

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": 1,
            "holder": self.holder,
            "fencing_token": self.fencing_token,
            "acquired_at": self.acquired_at,
            "expires_at": self.expires_at,
        }

    @classmethod
    def from_dict(cls, raw: dict[str, object]) -> Lease:
        if raw.get("schema_version") != 1:
            raise LeaseError("lease schema_version must be 1")
        try:
            holder = str(raw["holder"])
            fencing_token = raw["fencing_token"]
            acquired_at = str(raw["acquired_at"])
            expires_at = str(raw["expires_at"])
        except KeyError as exc:
            raise LeaseError(f"lease record missing key: {exc}") from exc
        if type(fencing_token) is not int or fencing_token < 1:
            raise LeaseError("lease fencing_token must be a positive integer")
        if not holder:
            raise LeaseError("lease holder must be non-empty")
        return cls(holder, fencing_token, acquired_at, expires_at)


class SingleWriterLease:
    """A file-backed, atomically-updated, fencing-token single-writer lease."""

    def __init__(self, path: Path, *, environment: str) -> None:
        self._path = path
        self._environment = environment

    @property
    def environment(self) -> str:
        return self._environment

    def read(self) -> Lease | None:
        try:
            raw_bytes = self._path.read_bytes()
        except FileNotFoundError:
            return None
        try:
            raw = json.loads(raw_bytes)
        except json.JSONDecodeError as exc:
            raise LeaseError(f"lease store is not valid JSON: {exc}") from exc
        if not isinstance(raw, dict):
            raise LeaseError("lease store must be a JSON object")
        if raw.get("environment") != self._environment:
            raise LeaseError(
                f"lease store environment {raw.get('environment')!r} "
                f"does not match {self._environment!r}"
            )
        return Lease.from_dict(raw)

    def acquire(self, holder: str, *, now: str, expires_at: str) -> Lease:
        """Acquire the lease if it is free or the current one has expired at `now`.

        Mints a fresh fencing token (current + 1). Raises `LeaseHeldError` if a
        different holder still has an unexpired lease. If the same holder re-acquires,
        the token still advances (treat as a fresh acquisition, not a renewal).
        """
        if not holder:
            raise LeaseError("lease holder must be non-empty")
        if expires_at <= now:
            raise LeaseError("lease expires_at must be after now")
        current = self.read()
        if current is not None and current.holder != holder and now < current.expires_at:
            raise LeaseHeldError(
                f"lease held by {current.holder!r} until {current.expires_at} (now {now})"
            )
        token = (current.fencing_token if current is not None else 0) + 1
        return self._write(Lease(holder, token, now, expires_at))

    def renew(self, holder: str, *, fencing_token: int, expires_at: str) -> Lease:
        """Extend the lease held by `holder` with `fencing_token`; keeps the token."""
        current = self.read()
        if current is None or current.holder != holder or current.fencing_token != fencing_token:
            raise LeaseError("cannot renew a lease not currently held with that token")
        if expires_at <= current.acquired_at:
            raise LeaseError("lease expires_at must be after acquired_at")
        return self._write(Lease(holder, fencing_token, current.acquired_at, expires_at))

    def release(self, holder: str, *, fencing_token: int) -> None:
        """Release the lease if held by `holder` with `fencing_token` (else no-op)."""
        current = self.read()
        if (
            current is not None
            and current.holder == holder
            and (current.fencing_token == fencing_token)
        ):
            self._path.unlink(missing_ok=True)

    def _write(self, lease: Lease) -> Lease:
        document = {**lease.to_dict(), "environment": self._environment}
        self._path.parent.mkdir(parents=True, exist_ok=True)
        payload = (json.dumps(document, indent=2, sort_keys=True) + "\n").encode("utf-8")
        tmp = self._path.with_name(f".{self._path.name}.tmp.{os.getpid()}")
        try:
            with open(tmp, "wb") as handle:
                handle.write(payload)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(tmp, self._path)
        finally:
            tmp.unlink(missing_ok=True)
        return lease
