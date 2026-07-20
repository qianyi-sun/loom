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
from datetime import UTC, datetime
from enum import StrEnum
from typing import cast

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
class BackupRetirementRecord:
    """Durable authority to delete one exact unreferenced large payload."""

    payload_id: str
    request_id: str
    bundle_name: str | None
    reason: str
    manifest_sha256: str | None = None

    def __post_init__(self) -> None:
        if (
            _PAYLOAD_ID_RE.fullmatch(self.payload_id) is None
            or _REQUEST_ID_RE.fullmatch(self.request_id) is None
            or self.reason not in {"failed", "superseded"}
        ):
            raise ValueError("backup retirement identity is invalid")
        if self.bundle_name is not None and (
            not self.bundle_name
            or "/" in self.bundle_name
            or self.bundle_name != self.bundle_name.strip()
        ):
            raise ValueError("backup retirement bundle identity is invalid")
        if self.manifest_sha256 is not None and (
            len(self.manifest_sha256) != 64
            or any(character not in "0123456789abcdef" for character in self.manifest_sha256)
        ):
            raise ValueError("backup retirement manifest digest is invalid")

    def to_dict(self) -> dict[str, str | None]:
        return {
            "bundle_name": self.bundle_name,
            "manifest_sha256": self.manifest_sha256,
            "payload_id": self.payload_id,
            "reason": self.reason,
            "request_id": self.request_id,
        }

    @classmethod
    def from_dict(
        cls,
        data: Mapping[str, object],
        *,
        legacy: bool = False,
    ) -> BackupRetirementRecord:
        expected = (
            {"payload_id", "reason", "request_id"}
            if legacy
            else {
                "bundle_name",
                "manifest_sha256",
                "payload_id",
                "reason",
                "request_id",
            }
        )
        if set(data) != expected or not all(
            isinstance(data[field], str) for field in ("payload_id", "reason", "request_id")
        ):
            raise ValueError("backup retirement record schema is invalid")
        if (
            not legacy
            and data["bundle_name"] is not None
            and not isinstance(data["bundle_name"], str)
        ):
            raise ValueError("backup retirement record schema is invalid")
        if (
            not legacy
            and data["manifest_sha256"] is not None
            and not isinstance(data["manifest_sha256"], str)
        ):
            raise ValueError("backup retirement record schema is invalid")
        return cls(
            bundle_name=(None if legacy else data["bundle_name"]),  # type: ignore[arg-type]
            manifest_sha256=cast(str | None, None if legacy else data["manifest_sha256"]),
            payload_id=data["payload_id"],  # type: ignore[arg-type]
            request_id=data["request_id"],  # type: ignore[arg-type]
            reason=data["reason"],  # type: ignore[arg-type]
        )


@dataclass(frozen=True, slots=True)
class BackupPayloadRecord:
    """Compact authority record for one large backup payload."""

    payload_id: str
    request_id: str
    bundle_name: str
    phase: BackupPayloadPhase
    created_at: datetime
    manifest_sha256: str | None = None
    lease: BackupLease | None = None
    failure_code: str | None = None

    def __post_init__(self) -> None:
        if (
            _PAYLOAD_ID_RE.fullmatch(self.payload_id) is None
            or _REQUEST_ID_RE.fullmatch(self.request_id) is None
            or not self.bundle_name
            or "/" in self.bundle_name
            or self.bundle_name != self.bundle_name.strip()
            or self.created_at.tzinfo is None
            or self.created_at.utcoffset() is None
        ):
            raise ValueError("backup payload identity is invalid")
        if self.lease is not None and self.lease.source_request_id != self.request_id:
            raise ValueError("backup payload lease belongs to another request")
        manifest_verified = self.phase in {
            BackupPayloadPhase.MANIFEST_VERIFIED,
            BackupPayloadPhase.RESTORE_VERIFIED,
            BackupPayloadPhase.ACTIVE,
        }
        restore_verified = self.phase in {
            BackupPayloadPhase.RESTORE_VERIFIED,
            BackupPayloadPhase.ACTIVE,
        }
        if manifest_verified != (
            isinstance(self.manifest_sha256, str)
            and len(self.manifest_sha256) == 64
            and all(character in "0123456789abcdef" for character in self.manifest_sha256)
        ):
            raise ValueError("manifest-verified payload requires an exact manifest digest")
        if restore_verified != (self.lease is not None):
            raise ValueError("restore-verified backup payload requires an exact lease")
        if self.lease is not None and self.lease.manifest_sha256 != self.manifest_sha256:
            raise ValueError("backup payload manifest and lease digests differ")
        if self.phase is BackupPayloadPhase.FAILED:
            if not self.failure_code or self.lease is not None or self.manifest_sha256 is not None:
                raise ValueError("failed backup payload requires compact failure evidence only")
        elif self.failure_code is not None:
            raise ValueError("non-failed backup payload cannot carry a failure code")

    def to_dict(self) -> dict[str, object]:
        return {
            "created_at": self.created_at.isoformat(),
            "bundle_name": self.bundle_name,
            "failure_code": self.failure_code,
            "lease": self.lease.to_dict() if self.lease is not None else None,
            "manifest_sha256": self.manifest_sha256,
            "payload_id": self.payload_id,
            "phase": self.phase.value,
            "request_id": self.request_id,
        }

    @classmethod
    def from_dict(
        cls,
        data: Mapping[str, object],
        *,
        legacy: bool = False,
    ) -> BackupPayloadRecord:
        expected = {
            "created_at",
            "failure_code",
            "lease",
            "manifest_sha256",
            "payload_id",
            "phase",
            "request_id",
        }
        if not legacy:
            expected.add("bundle_name")
        if set(data) != expected or not all(
            isinstance(data[field], str)
            for field in ("created_at", "payload_id", "phase", "request_id")
        ):
            raise ValueError("backup payload record schema is invalid")
        if data["failure_code"] is not None and not isinstance(data["failure_code"], str):
            raise ValueError("backup payload record schema is invalid")
        if data["manifest_sha256"] is not None and not isinstance(data["manifest_sha256"], str):
            raise ValueError("backup payload record schema is invalid")
        if data["lease"] is not None and not isinstance(data["lease"], Mapping):
            raise ValueError("backup payload record schema is invalid")
        try:
            created_at = datetime.fromisoformat(data["created_at"])  # type: ignore[arg-type]
            phase = BackupPayloadPhase(data["phase"])  # type: ignore[arg-type]
        except ValueError as exc:
            raise ValueError("backup payload record phase or timestamp is invalid") from exc
        bundle_name = (
            datetime.fromisoformat(data["created_at"])  # type: ignore[arg-type]
            .astimezone(UTC)
            .strftime("%Y%m%dT%H%M%SZ")
            + f"-{data['request_id']}"
            if legacy
            else data["bundle_name"]
        )
        return cls(
            payload_id=data["payload_id"],  # type: ignore[arg-type]
            request_id=data["request_id"],  # type: ignore[arg-type]
            bundle_name=bundle_name,  # type: ignore[arg-type]
            phase=phase,
            created_at=created_at,
            lease=(
                BackupLease.from_dict(data["lease"]) if isinstance(data["lease"], Mapping) else None
            ),
            manifest_sha256=data["manifest_sha256"],
            failure_code=data["failure_code"],
        )


@dataclass(frozen=True, slots=True)
class BackupRotationState:
    """Crash-persisted active/candidate payload authority."""

    generation: int = 0
    active: BackupPayloadRecord | None = None
    candidate: BackupPayloadRecord | None = None
    retirements: tuple[BackupRetirementRecord, ...] = ()

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
        payload_ids = [record.payload_id for record in self.retirements]
        if len(set(payload_ids)) != len(payload_ids) or any(
            payload_id
            in {
                None if self.active is None else self.active.payload_id,
                None if self.candidate is None else self.candidate.payload_id,
            }
            for payload_id in payload_ids
        ):
            raise ValueError("backup retirement queue identity is invalid")

    @property
    def payload_count(self) -> int:
        return (
            int(self.active is not None) + int(self.candidate is not None) + len(self.retirements)
        )

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
            "retirements": [record.to_dict() for record in self.retirements],
            "schema_version": 3,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, object]) -> BackupRotationState:
        schema_version = data.get("schema_version")
        expected = (
            {"active", "candidate", "generation", "schema_version"}
            if schema_version == 1
            else {"active", "candidate", "generation", "retirements", "schema_version"}
        )
        if (
            set(data) != expected
            or schema_version not in {1, 2, 3}
            or type(data["generation"]) is not int
            or any(
                data[field] is not None and not isinstance(data[field], Mapping)
                for field in ("active", "candidate")
            )
            or (
                schema_version in {2, 3}
                and (
                    not isinstance(data["retirements"], list)
                    or not all(isinstance(item, Mapping) for item in data["retirements"])
                )
            )
        ):
            raise ValueError("backup rotation state schema is invalid")
        return cls(
            generation=data["generation"],
            active=(
                BackupPayloadRecord.from_dict(data["active"], legacy=schema_version in {1, 2})
                if isinstance(data["active"], Mapping)
                else None
            ),
            candidate=(
                BackupPayloadRecord.from_dict(data["candidate"], legacy=schema_version in {1, 2})
                if isinstance(data["candidate"], Mapping)
                else None
            ),
            retirements=(
                tuple(
                    BackupRetirementRecord.from_dict(item, legacy=schema_version == 2)
                    for item in cast(list[Mapping[str, object]], data["retirements"])
                )
                if schema_version in {2, 3}
                else ()
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
    bundle_name: str,
    created_at: datetime,
) -> BackupRotationResult:
    """Reserve the sole transient candidate without replacing the active lease."""
    if state.candidate is not None:
        raise BackupRotationError("a backup payload candidate is already reserved")
    if state.retirements:
        raise BackupRotationError("backup payload retirement must complete before admission")
    candidate = BackupPayloadRecord(
        payload_id=payload_id,
        request_id=request_id,
        bundle_name=bundle_name,
        phase=BackupPayloadPhase.CREATING,
        created_at=created_at,
    )
    return BackupRotationResult(
        BackupRotationState(
            generation=state.generation + 1,
            active=state.active,
            candidate=candidate,
            retirements=state.retirements,
        )
    )


def record_manifest_verified(
    state: BackupRotationState,
    *,
    payload_id: str,
    manifest_sha256: str,
) -> BackupRotationResult:
    candidate = _require_candidate(state, payload_id, BackupPayloadPhase.CREATING)
    if len(manifest_sha256) != 64 or any(
        character not in "0123456789abcdef" for character in manifest_sha256
    ):
        raise ValueError("candidate manifest digest is invalid")
    return _replace_candidate(
        state,
        replace(
            candidate,
            phase=BackupPayloadPhase.MANIFEST_VERIFIED,
            manifest_sha256=manifest_sha256,
        ),
    )


def record_restore_verified(
    state: BackupRotationState,
    *,
    payload_id: str,
    lease: BackupLease,
) -> BackupRotationResult:
    candidate = _require_candidate(
        state,
        payload_id,
        BackupPayloadPhase.MANIFEST_VERIFIED,
    )
    if lease.source_request_id != candidate.request_id:
        raise BackupRotationError("candidate restore belongs to another request")
    if lease.manifest_sha256 != candidate.manifest_sha256:
        raise BackupRotationError("candidate restore manifest digest does not match")
    return _replace_candidate(
        state,
        replace(candidate, phase=BackupPayloadPhase.RESTORE_VERIFIED, lease=lease),
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
        bundle_name=candidate.bundle_name,
        phase=BackupPayloadPhase.FAILED,
        created_at=candidate.created_at,
        failure_code=failure_code,
    )
    if payload_id in referenced_payload_ids:
        return _replace_candidate(state, failed)
    retirement = BackupRetirementRecord(
        payload_id=failed.payload_id,
        request_id=failed.request_id,
        bundle_name=failed.bundle_name,
        reason="failed",
        manifest_sha256=candidate.manifest_sha256,
    )
    return BackupRotationResult(
        BackupRotationState(
            generation=state.generation + 1,
            active=state.active,
            candidate=None,
            retirements=(*state.retirements, retirement),
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
    retirement = BackupRetirementRecord(
        payload_id=candidate.payload_id,
        request_id=candidate.request_id,
        bundle_name=candidate.bundle_name,
        reason="failed",
        manifest_sha256=candidate.manifest_sha256,
    )
    return BackupRotationResult(
        BackupRotationState(
            generation=state.generation + 1,
            active=state.active,
            candidate=None,
            retirements=(*state.retirements, retirement),
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
    retirements = state.retirements
    if previous is not None:
        retirements = (
            *retirements,
            BackupRetirementRecord(
                payload_id=previous.payload_id,
                request_id=previous.request_id,
                bundle_name=previous.bundle_name,
                reason="superseded",
                manifest_sha256=previous.manifest_sha256,
            ),
        )
    if previous is not None and previous.payload_id not in referenced_payload_ids:
        delete_ids = (previous.payload_id,)
    return BackupRotationResult(
        BackupRotationState(
            generation=state.generation + 1,
            active=promoted,
            candidate=None,
            retirements=retirements,
        ),
        delete_ids,
    )


def acknowledge_retirement(
    state: BackupRotationState,
    *,
    payload_id: str,
) -> BackupRotationResult:
    """Remove one retirement record only after idempotent payload deletion."""
    matches = tuple(record for record in state.retirements if record.payload_id == payload_id)
    if len(matches) != 1:
        raise BackupRotationError("backup payload retirement identity does not match")
    return BackupRotationResult(
        BackupRotationState(
            generation=state.generation + 1,
            active=state.active,
            candidate=state.candidate,
            retirements=tuple(
                record for record in state.retirements if record.payload_id != payload_id
            ),
        )
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
            retirements=state.retirements,
        )
    )


__all__ = [
    "BackupPayloadPhase",
    "BackupPayloadRecord",
    "BackupRetirementRecord",
    "BackupRotationError",
    "BackupRotationResult",
    "BackupRotationState",
    "acknowledge_retirement",
    "begin_candidate",
    "collect_failed_candidate",
    "fail_candidate",
    "promote_candidate",
    "record_manifest_verified",
    "record_restore_verified",
]
