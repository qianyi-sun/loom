"""Immutable detached-backup job identity and cancellable state authority."""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime

from loom_cli.rollout.lifecycle_protocol import (
    LifecycleAction,
    LifecyclePhase,
    LifecycleState,
    transition_lifecycle,
)

_SAFE_ID_RE = re.compile(r"^[a-z0-9][a-z0-9-]{7,79}$")
_SHA_RE = re.compile(r"^[0-9a-f]{40}$")
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")


def _timestamp(value: datetime, field: str) -> str:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field} must be timezone-aware")
    return value.isoformat()


def _exact_keys(data: Mapping[str, object], expected: set[str], label: str) -> None:
    if set(data) != expected:
        raise ValueError(f"{label} fields do not match schema")


def _string(data: Mapping[str, object], key: str) -> str:
    value = data[key]
    if not isinstance(value, str):
        raise ValueError(f"{key} must be a string")
    return value


def _parse_timestamp(value: object, field: str) -> datetime:
    if not isinstance(value, str):
        raise ValueError(f"{field} must be an ISO timestamp")
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as exc:
        raise ValueError(f"{field} must be an ISO timestamp") from exc
    _timestamp(parsed, field)
    return parsed


@dataclass(frozen=True, slots=True)
class PreflightBackupJobEnvelope:
    """Immutable detached-backup input bound to a Tier 0-2 assessment."""

    job_id: str
    request_id: str
    payload_id: str
    candidate_sha: str
    candidate_tree: str
    preflight_assessment_sha256: str
    preflight_registry_sha256: str
    preflight_coverage_sha256: str
    mutation_epoch: int
    environment: str
    namespace: str
    bundle_name: str
    created_at: datetime

    def __post_init__(self) -> None:
        if any(
            _SAFE_ID_RE.fullmatch(value) is None
            for value in (self.job_id, self.request_id, self.payload_id)
        ):
            raise ValueError("preflight backup job identifier is invalid")
        if (
            _SHA_RE.fullmatch(self.candidate_sha) is None
            or _SHA_RE.fullmatch(self.candidate_tree) is None
            or _SHA256_RE.fullmatch(self.preflight_assessment_sha256) is None
            or _SHA256_RE.fullmatch(self.preflight_registry_sha256) is None
            or _SHA256_RE.fullmatch(self.preflight_coverage_sha256) is None
            or self.mutation_epoch < 0
            or self.environment != "staging"
            or not self.namespace
            or self.namespace != self.namespace.strip()
            or not self.bundle_name
            or "/" in self.bundle_name
            or self.bundle_name != self.bundle_name.strip()
        ):
            raise ValueError("preflight backup job binding is invalid")
        _timestamp(self.created_at, "created_at")

    def to_dict(self) -> dict[str, object]:
        return {
            "bundle_name": self.bundle_name,
            "candidate_sha": self.candidate_sha,
            "candidate_tree": self.candidate_tree,
            "created_at": _timestamp(self.created_at, "created_at"),
            "environment": self.environment,
            "job_id": self.job_id,
            "mutation_epoch": self.mutation_epoch,
            "namespace": self.namespace,
            "payload_id": self.payload_id,
            "preflight_assessment_sha256": self.preflight_assessment_sha256,
            "preflight_coverage_sha256": self.preflight_coverage_sha256,
            "preflight_registry_sha256": self.preflight_registry_sha256,
            "request_id": self.request_id,
            "schema_version": 1,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, object]) -> PreflightBackupJobEnvelope:
        expected = {
            "bundle_name",
            "candidate_sha",
            "candidate_tree",
            "created_at",
            "environment",
            "job_id",
            "mutation_epoch",
            "namespace",
            "payload_id",
            "preflight_assessment_sha256",
            "preflight_coverage_sha256",
            "preflight_registry_sha256",
            "request_id",
            "schema_version",
        }
        _exact_keys(data, expected, "preflight backup job envelope")
        if data["schema_version"] != 1 or type(data["mutation_epoch"]) is not int:
            raise ValueError("preflight backup job envelope version or epoch is invalid")
        return cls(
            job_id=_string(data, "job_id"),
            request_id=_string(data, "request_id"),
            payload_id=_string(data, "payload_id"),
            candidate_sha=_string(data, "candidate_sha"),
            candidate_tree=_string(data, "candidate_tree"),
            preflight_assessment_sha256=_string(data, "preflight_assessment_sha256"),
            preflight_registry_sha256=_string(data, "preflight_registry_sha256"),
            preflight_coverage_sha256=_string(data, "preflight_coverage_sha256"),
            mutation_epoch=data["mutation_epoch"],
            environment=_string(data, "environment"),
            namespace=_string(data, "namespace"),
            bundle_name=_string(data, "bundle_name"),
            created_at=_parse_timestamp(data["created_at"], "created_at"),
        )

    @property
    def evidence_digest(self) -> str:
        return hashlib.sha256(
            json.dumps(self.to_dict(), sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()


@dataclass(frozen=True, slots=True)
class BackupJobEnvelope:
    """Immutable input published before the long-running worker is dispatched."""

    job_id: str
    request_id: str
    payload_id: str
    candidate_sha: str
    candidate_tree: str
    preflight_attestation_sha256: str
    mutation_epoch: int
    environment: str
    namespace: str
    bundle_name: str
    created_at: datetime

    def __post_init__(self) -> None:
        if any(
            _SAFE_ID_RE.fullmatch(value) is None
            for value in (self.job_id, self.request_id, self.payload_id)
        ):
            raise ValueError("backup job identifier is invalid")
        if (
            _SHA_RE.fullmatch(self.candidate_sha) is None
            or _SHA_RE.fullmatch(self.candidate_tree) is None
            or _SHA256_RE.fullmatch(self.preflight_attestation_sha256) is None
            or self.mutation_epoch < 0
            or self.environment != "staging"
            or not self.namespace
            or self.namespace != self.namespace.strip()
            or not self.bundle_name
            or "/" in self.bundle_name
            or self.bundle_name != self.bundle_name.strip()
        ):
            raise ValueError("backup job binding is invalid")
        _timestamp(self.created_at, "created_at")

    def to_dict(self) -> dict[str, object]:
        return {
            "bundle_name": self.bundle_name,
            "candidate_sha": self.candidate_sha,
            "candidate_tree": self.candidate_tree,
            "created_at": _timestamp(self.created_at, "created_at"),
            "environment": self.environment,
            "job_id": self.job_id,
            "mutation_epoch": self.mutation_epoch,
            "namespace": self.namespace,
            "payload_id": self.payload_id,
            "preflight_attestation_sha256": self.preflight_attestation_sha256,
            "request_id": self.request_id,
            "schema_version": 1,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, object]) -> BackupJobEnvelope:
        expected = {
            "bundle_name",
            "candidate_sha",
            "candidate_tree",
            "created_at",
            "environment",
            "job_id",
            "mutation_epoch",
            "namespace",
            "payload_id",
            "preflight_attestation_sha256",
            "request_id",
            "schema_version",
        }
        _exact_keys(data, expected, "backup job envelope")
        if data["schema_version"] != 1 or type(data["mutation_epoch"]) is not int:
            raise ValueError("backup job envelope version or epoch is invalid")
        return cls(
            job_id=_string(data, "job_id"),
            request_id=_string(data, "request_id"),
            payload_id=_string(data, "payload_id"),
            candidate_sha=_string(data, "candidate_sha"),
            candidate_tree=_string(data, "candidate_tree"),
            preflight_attestation_sha256=_string(
                data,
                "preflight_attestation_sha256",
            ),
            mutation_epoch=data["mutation_epoch"],
            environment=_string(data, "environment"),
            namespace=_string(data, "namespace"),
            bundle_name=_string(data, "bundle_name"),
            created_at=_parse_timestamp(data["created_at"], "created_at"),
        )

    @property
    def evidence_digest(self) -> str:
        return hashlib.sha256(
            json.dumps(self.to_dict(), sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()


@dataclass(frozen=True, slots=True)
class BackupJobState:
    """Crash-durable state updated with compare-and-swap generation checks."""

    job_id: str
    request_id: str
    phase: LifecyclePhase = LifecyclePhase.BACKUP_PENDING
    sequence: int = 0
    updated_at: datetime | None = None
    manifest_sha256: str | None = None
    lease_digest: str | None = None
    preflight_attestation_sha256: str | None = None
    failure_code: str | None = None

    def __post_init__(self) -> None:
        if (
            _SAFE_ID_RE.fullmatch(self.job_id) is None
            or _SAFE_ID_RE.fullmatch(self.request_id) is None
            or self.sequence < 0
        ):
            raise ValueError("backup job state identity is invalid")
        if self.updated_at is not None:
            _timestamp(self.updated_at, "updated_at")
        verified = self.phase in {
            LifecyclePhase.BACKUP_VERIFIED,
            LifecyclePhase.LAUNCH_PENDING,
            LifecyclePhase.LAUNCH_RUNNING,
        }
        if verified != (
            _SHA256_RE.fullmatch(self.manifest_sha256 or "") is not None
            and _SHA256_RE.fullmatch(self.lease_digest or "") is not None
            and _SHA256_RE.fullmatch(self.preflight_attestation_sha256 or "") is not None
        ):
            raise ValueError("verified backup job state requires manifest, lease and attestation")
        if self.phase is LifecyclePhase.BACKUP_FAILED:
            if not self.failure_code:
                raise ValueError("failed backup job state requires a failure code")
        elif self.failure_code is not None:
            raise ValueError("non-failed backup job state cannot carry a failure code")

    def to_dict(self) -> dict[str, object]:
        return {
            "failure_code": self.failure_code,
            "job_id": self.job_id,
            "lease_digest": self.lease_digest,
            "manifest_sha256": self.manifest_sha256,
            "preflight_attestation_sha256": self.preflight_attestation_sha256,
            "phase": self.phase.value,
            "request_id": self.request_id,
            "schema_version": 1,
            "sequence": self.sequence,
            "updated_at": (
                _timestamp(self.updated_at, "updated_at") if self.updated_at is not None else None
            ),
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, object]) -> BackupJobState:
        expected = {
            "failure_code",
            "job_id",
            "lease_digest",
            "manifest_sha256",
            "preflight_attestation_sha256",
            "phase",
            "request_id",
            "schema_version",
            "sequence",
            "updated_at",
        }
        _exact_keys(data, expected, "backup job state")
        if data["schema_version"] != 1 or type(data["sequence"]) is not int:
            raise ValueError("backup job state version or sequence is invalid")
        try:
            phase = LifecyclePhase(_string(data, "phase"))
        except ValueError as exc:
            raise ValueError("backup job phase is invalid") from exc
        return cls(
            job_id=_string(data, "job_id"),
            request_id=_string(data, "request_id"),
            phase=phase,
            sequence=data["sequence"],
            updated_at=(
                _parse_timestamp(data["updated_at"], "updated_at")
                if data["updated_at"] is not None
                else None
            ),
            manifest_sha256=(
                _string(data, "manifest_sha256") if data["manifest_sha256"] is not None else None
            ),
            lease_digest=(
                _string(data, "lease_digest") if data["lease_digest"] is not None else None
            ),
            preflight_attestation_sha256=(
                _string(data, "preflight_attestation_sha256")
                if data["preflight_attestation_sha256"] is not None
                else None
            ),
            failure_code=(
                _string(data, "failure_code") if data["failure_code"] is not None else None
            ),
        )


def transition_backup_job(
    state: BackupJobState,
    action: LifecycleAction,
    *,
    updated_at: datetime,
    manifest_sha256: str | None = None,
    lease_digest: str | None = None,
    preflight_attestation_sha256: str | None = None,
    failure_code: str | None = None,
) -> BackupJobState:
    """Apply the shared lifecycle transition without inventing a second graph."""
    transitioned = transition_lifecycle(
        LifecycleState(
            request_id=state.request_id,
            phase=state.phase,
            sequence=state.sequence,
        ),
        action,
    )
    if action is LifecycleAction.VERIFY_BACKUP:
        if (
            _SHA256_RE.fullmatch(manifest_sha256 or "") is None
            or _SHA256_RE.fullmatch(lease_digest or "") is None
            or _SHA256_RE.fullmatch(preflight_attestation_sha256 or "") is None
        ):
            raise ValueError("backup verification requires manifest, lease and attestation digests")
    else:
        manifest_sha256 = state.manifest_sha256
        lease_digest = state.lease_digest
        preflight_attestation_sha256 = state.preflight_attestation_sha256
    if transitioned.phase is LifecyclePhase.BACKUP_FAILED:
        if not failure_code or failure_code != failure_code.strip() or len(failure_code) > 96:
            raise ValueError("backup failure code must be normalized and bounded")
    else:
        failure_code = None
    return BackupJobState(
        job_id=state.job_id,
        request_id=state.request_id,
        phase=transitioned.phase,
        sequence=transitioned.sequence,
        updated_at=updated_at,
        manifest_sha256=manifest_sha256,
        lease_digest=lease_digest,
        preflight_attestation_sha256=preflight_attestation_sha256,
        failure_code=failure_code,
    )


def validate_job_binding(
    envelope: BackupJobEnvelope | PreflightBackupJobEnvelope,
    state: BackupJobState,
) -> None:
    """Fail closed if mutable state was substituted across immutable jobs."""
    if envelope.job_id != state.job_id or envelope.request_id != state.request_id:
        raise ValueError("backup job state does not match immutable envelope")


__all__ = [
    "BackupJobEnvelope",
    "BackupJobState",
    "PreflightBackupJobEnvelope",
    "transition_backup_job",
    "validate_job_binding",
]
