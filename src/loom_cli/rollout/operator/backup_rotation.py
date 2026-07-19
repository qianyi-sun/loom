"""Single-source authority for bounded rollout backup payload rotation.

The protocol is deliberately storage independent.  Persistence adapters must
publish every returned state atomically before performing any returned delete
actions.  This keeps one known-good payload at steady state and never more than
one replacement candidate while it is being verified.
"""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping
from dataclasses import dataclass, replace
from datetime import datetime
from enum import StrEnum

from .backup_lease import BackupLease

_PAYLOAD_ID_RE = re.compile(r"^payload-[a-z0-9][a-z0-9-]{7,63}$")
_REQUEST_ID_RE = re.compile(r"^req-[a-z0-9][a-z0-9-]{7,63}$")


class BackupRotationError(RuntimeError):
    """Raised when a payload transition would weaken rollback authority."""


class BackupPayloadPhase(StrEnum):
    CREATING = "creating"
    MANIFEST_VERIFIED = "manifest_verified"
    RESTORE_VERIFIED = "restore_verified"
    ACTIVE = "active"
    FAILED = "failed"


@dataclass(frozen=True, slots=True)
class BackupPayloadRecord:
    """Compact authority record for one large backup payload."""

    payload_id: str
    request_id: str
    phase: BackupPayloadPhase
    created_at: datetime
    lease: BackupLease | None = None
    failure_code: str | None = None

    def __post_init__(self) -> None:
        if (
            _PAYLOAD_ID_RE.fullmatch(self.payload_id) is None
            or _REQUEST_ID_RE.fullmatch(self.request_id) is None
            or self.created_at.tzinfo is None
            or self.created_at.utcoffset() is None
        ):
            raise ValueError("backup payload identity is invalid")
        if self.lease is not None and self.lease.source_request_id != self.request_id:
            raise ValueError("backup payload lease belongs to another request")
        if (
            self.phase
            in {
                BackupPayloadPhase.MANIFEST_VERIFIED,
                BackupPayloadPhase.RESTORE_VERIFIED,
                BackupPayloadPhase.ACTIVE,
            }
            and self.lease is None
        ):
            raise ValueError("verified backup payload requires an exact lease")
        if self.phase is BackupPayloadPhase.FAILED:
            if not self.failure_code or self.lease is not None:
                raise ValueError("failed backup payload requires compact failure evidence only")
        elif self.failure_code is not None:
            raise ValueError("non-failed backup payload cannot carry a failure code")

    def to_dict(self) -> dict[str, object]:
        return {
            "created_at": self.created_at.isoformat(),
            "failure_code": self.failure_code,
            "lease": self.lease.to_dict() if self.lease is not None else None,
            "payload_id": self.payload_id,
            "phase": self.phase.value,
            "request_id": self.request_id,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, object]) -> BackupPayloadRecord:
        expected = {
            "created_at",
            "failure_code",
            "lease",
            "payload_id",
            "phase",
            "request_id",
        }
        if set(data) != expected or not all(
            isinstance(data[field], str)
            for field in ("created_at", "payload_id", "phase", "request_id")
        ):
            raise ValueError("backup payload record schema is invalid")
        if data["failure_code"] is not None and not isinstance(data["failure_code"], str):
            raise ValueError("backup payload record schema is invalid")
        if data["lease"] is not None and not isinstance(data["lease"], Mapping):
            raise ValueError("backup payload record schema is invalid")
        try:
            created_at = datetime.fromisoformat(data["created_at"])  # type: ignore[arg-type]
            phase = BackupPayloadPhase(data["phase"])  # type: ignore[arg-type]
        except ValueError as exc:
            raise ValueError("backup payload record phase or timestamp is invalid") from exc
        return cls(
            payload_id=data["payload_id"],  # type: ignore[arg-type]
            request_id=data["request_id"],  # type: ignore[arg-type]
            phase=phase,
            created_at=created_at,
            lease=(
                BackupLease.from_dict(data["lease"]) if isinstance(data["lease"], Mapping) else None
            ),
            failure_code=data["failure_code"],
        )


@dataclass(frozen=True, slots=True)
class BackupRotationState:
    """Crash-persisted active/candidate payload authority."""

    generation: int = 0
    active: BackupPayloadRecord | None = None
    candidate: BackupPayloadRecord | None = None

    def __post_init__(self) -> None:
        if self.generation < 0:
            raise ValueError("backup rotation generation must be non-negative")
        if self.active is not None and self.active.phase is not BackupPayloadPhase.ACTIVE:
            raise ValueError("active payload pointer must reference an active payload")
        if self.candidate is not None and self.candidate.phase is BackupPayloadPhase.ACTIVE:
            raise ValueError("candidate payload cannot already be active")
        if (
            self.active is not None
            and self.candidate is not None
            and self.active.payload_id == self.candidate.payload_id
        ):
            raise ValueError("active and candidate payloads must be distinct")

    @property
    def payload_count(self) -> int:
        return int(self.active is not None) + int(self.candidate is not None)

    @property
    def evidence_digest(self) -> str:
        return hashlib.sha256(
            json.dumps(self.to_dict(), sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()

    def to_dict(self) -> dict[str, object]:
        return {
            "active": self.active.to_dict() if self.active is not None else None,
            "candidate": self.candidate.to_dict() if self.candidate is not None else None,
            "generation": self.generation,
            "schema_version": 1,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, object]) -> BackupRotationState:
        expected = {"active", "candidate", "generation", "schema_version"}
        if (
            set(data) != expected
            or data["schema_version"] != 1
            or type(data["generation"]) is not int
            or any(
                data[field] is not None and not isinstance(data[field], Mapping)
                for field in ("active", "candidate")
            )
        ):
            raise ValueError("backup rotation state schema is invalid")
        return cls(
            generation=data["generation"],
            active=(
                BackupPayloadRecord.from_dict(data["active"])
                if isinstance(data["active"], Mapping)
                else None
            ),
            candidate=(
                BackupPayloadRecord.from_dict(data["candidate"])
                if isinstance(data["candidate"], Mapping)
                else None
            ),
        )


@dataclass(frozen=True, slots=True)
class BackupRotationResult:
    """State to publish plus payloads eligible for exact post-publish deletion."""

    state: BackupRotationState
    delete_payload_ids: tuple[str, ...] = ()


def begin_candidate(
    state: BackupRotationState,
    *,
    payload_id: str,
    request_id: str,
    created_at: datetime,
) -> BackupRotationResult:
    """Reserve the sole transient candidate without replacing the active lease."""
    if state.candidate is not None:
        raise BackupRotationError("a backup payload candidate is already reserved")
    candidate = BackupPayloadRecord(
        payload_id=payload_id,
        request_id=request_id,
        phase=BackupPayloadPhase.CREATING,
        created_at=created_at,
    )
    return BackupRotationResult(
        BackupRotationState(
            generation=state.generation + 1,
            active=state.active,
            candidate=candidate,
        )
    )


def record_manifest_verified(
    state: BackupRotationState,
    *,
    payload_id: str,
    lease: BackupLease,
) -> BackupRotationResult:
    candidate = _require_candidate(state, payload_id, BackupPayloadPhase.CREATING)
    if lease.source_request_id != candidate.request_id:
        raise BackupRotationError("candidate manifest belongs to another request")
    return _replace_candidate(
        state,
        replace(candidate, phase=BackupPayloadPhase.MANIFEST_VERIFIED, lease=lease),
    )


def record_restore_verified(
    state: BackupRotationState,
    *,
    payload_id: str,
) -> BackupRotationResult:
    candidate = _require_candidate(
        state,
        payload_id,
        BackupPayloadPhase.MANIFEST_VERIFIED,
    )
    return _replace_candidate(
        state,
        replace(candidate, phase=BackupPayloadPhase.RESTORE_VERIFIED),
    )


def fail_candidate(
    state: BackupRotationState,
    *,
    payload_id: str,
    failure_code: str,
    referenced_payload_ids: frozenset[str] = frozenset(),
) -> BackupRotationResult:
    """Seal compact evidence and schedule an unreferenced failed payload for deletion."""
    candidate = _require_candidate(state, payload_id)
    if not failure_code or failure_code != failure_code.strip() or len(failure_code) > 96:
        raise ValueError("failure_code must be normalized and bounded")
    failed = BackupPayloadRecord(
        payload_id=candidate.payload_id,
        request_id=candidate.request_id,
        phase=BackupPayloadPhase.FAILED,
        created_at=candidate.created_at,
        failure_code=failure_code,
    )
    if payload_id in referenced_payload_ids:
        return _replace_candidate(state, failed)
    return BackupRotationResult(
        BackupRotationState(
            generation=state.generation + 1,
            active=state.active,
            candidate=None,
        ),
        (payload_id,),
    )


def collect_failed_candidate(
    state: BackupRotationState,
    *,
    referenced_payload_ids: frozenset[str] = frozenset(),
) -> BackupRotationResult:
    candidate = state.candidate
    if candidate is None or candidate.phase is not BackupPayloadPhase.FAILED:
        return BackupRotationResult(state)
    if candidate.payload_id in referenced_payload_ids:
        return BackupRotationResult(state)
    return BackupRotationResult(
        BackupRotationState(
            generation=state.generation + 1,
            active=state.active,
            candidate=None,
        ),
        (candidate.payload_id,),
    )


def promote_candidate(
    state: BackupRotationState,
    *,
    payload_id: str,
    referenced_payload_ids: frozenset[str] = frozenset(),
) -> BackupRotationResult:
    """Atomically rotate only a fully restore-verified replacement payload."""
    candidate = _require_candidate(
        state,
        payload_id,
        BackupPayloadPhase.RESTORE_VERIFIED,
    )
    promoted = replace(candidate, phase=BackupPayloadPhase.ACTIVE)
    previous = state.active
    delete_ids: tuple[str, ...] = ()
    if previous is not None and previous.payload_id not in referenced_payload_ids:
        delete_ids = (previous.payload_id,)
    return BackupRotationResult(
        BackupRotationState(
            generation=state.generation + 1,
            active=promoted,
            candidate=None,
        ),
        delete_ids,
    )


def _require_candidate(
    state: BackupRotationState,
    payload_id: str,
    expected_phase: BackupPayloadPhase | None = None,
) -> BackupPayloadRecord:
    candidate = state.candidate
    if candidate is None or candidate.payload_id != payload_id:
        raise BackupRotationError("backup payload candidate identity does not match")
    if expected_phase is not None and candidate.phase is not expected_phase:
        raise BackupRotationError("backup payload candidate is in the wrong phase")
    return candidate


def _replace_candidate(
    state: BackupRotationState,
    candidate: BackupPayloadRecord,
) -> BackupRotationResult:
    return BackupRotationResult(
        BackupRotationState(
            generation=state.generation + 1,
            active=state.active,
            candidate=candidate,
        )
    )


__all__ = [
    "BackupPayloadPhase",
    "BackupPayloadRecord",
    "BackupRotationError",
    "BackupRotationResult",
    "BackupRotationState",
    "begin_candidate",
    "collect_failed_candidate",
    "fail_candidate",
    "promote_candidate",
    "record_manifest_verified",
    "record_restore_verified",
]
