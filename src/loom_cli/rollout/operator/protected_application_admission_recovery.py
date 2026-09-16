"""Non-secret recovery payload subordinate to an existing component intent.

This record is not credential recovery, maintenance authority, or an operation
journal of its own. Only ProtectedApplyJournal publishes it during active apply.
"""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping
from dataclasses import asdict, dataclass
from datetime import datetime

from loom.application_database_admission import (
    ApplicationDatabaseAdmissionTarget,
    ApplicationDatabaseCoordinationGuard,
    ApplicationDatabaseHandoffBackend,
)

MAX_HANDOFF_RECOVERIES = 16


def admission_record_digest(value: Mapping[str, object]) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


@dataclass(frozen=True, slots=True)
class ApplicationHandoffRecoveryIntent:
    """Write-ahead reopening intent, chained to the original or preceding receipt."""

    ordinal: int
    previous_record_digest: str

    def __post_init__(self) -> None:
        if (type(self.ordinal) is not int or not 1 <= self.ordinal <= MAX_HANDOFF_RECOVERIES
                or re.fullmatch(r"[0-9a-f]{64}", self.previous_record_digest) is None):
            raise ValueError("application handoff recovery intent is invalid")

    @property
    def digest(self) -> str:
        return admission_record_digest(self.to_dict())

    def to_dict(self) -> dict[str, object]:
        return {"schema_version": 1, **asdict(self)}

    @classmethod
    def from_dict(cls, value: Mapping[str, object]) -> ApplicationHandoffRecoveryIntent:
        _require_fields(value, {"ordinal", "previous_record_digest"})
        return cls(_integer(value, "ordinal"), _string(value, "previous_record_digest"))


@dataclass(frozen=True, slots=True)
class ApplicationHandoffReplacementReceipt:
    """Observed peer identity only; not proof of drain or permission to transfer."""

    recovery_intent_digest: str
    handoff_backend: ApplicationDatabaseHandoffBackend

    def __post_init__(self) -> None:
        if re.fullmatch(r"[0-9a-f]{64}", self.recovery_intent_digest) is None:
            raise ValueError("application handoff recovery receipt is invalid")

    @property
    def digest(self) -> str:
        return admission_record_digest(self.to_dict())

    def to_dict(self) -> dict[str, object]:
        return {"schema_version": 1, **asdict(self)}

    @classmethod
    def from_dict(cls, value: Mapping[str, object]) -> ApplicationHandoffReplacementReceipt:
        _require_fields(value, {"recovery_intent_digest", "handoff_backend"})
        return cls(_string(value, "recovery_intent_digest"), _backend(value["handoff_backend"]))


def _require_fields(value: Mapping[str, object], fields: set[str]) -> None:
    if (type(value.get("schema_version")) is not int or value["schema_version"] != 1
            or set(value) != {"schema_version", *fields}):
        raise ValueError("application handoff recovery fields are invalid")


def require_replacement_identity(
    original: ApplicationAdmissionRecoveryRecord,
    prior_backends: list[ApplicationDatabaseHandoffBackend],
    backend: ApplicationDatabaseHandoffBackend,
) -> None:
    ApplicationAdmissionRecoveryRecord(
        original.intent_digest, original.target, backend, original.coordination_guard,
    )
    if (datetime.fromisoformat(backend.server_started_at)
            != datetime.fromisoformat(original.handoff_backend.server_started_at)
            or any(backend.pid == prior.pid and datetime.fromisoformat(backend.started_at)
                   == datetime.fromisoformat(prior.started_at) for prior in prior_backends)):
        raise ValueError("application handoff replacement identity changed or reused")


@dataclass(frozen=True, slots=True)
class ApplicationAdmissionRecoveryRecord:
    intent_digest: str
    target: ApplicationDatabaseAdmissionTarget
    handoff_backend: ApplicationDatabaseHandoffBackend
    coordination_guard: ApplicationDatabaseCoordinationGuard | None = None

    def __post_init__(self) -> None:
        if (
            re.fullmatch(r"[0-9a-f]{64}", self.intent_digest) is None
            or self.target.system_identifier != self.handoff_backend.system_identifier
            or self.target.database_oid != self.handoff_backend.database_oid
        ):
            raise ValueError("application admission recovery identity is invalid")
        guard = self.coordination_guard
        if guard is not None and (
            guard.backend.system_identifier != self.target.system_identifier
            or guard.backend.database_oid != self.target.database_oid
            or guard.backend.pid == self.handoff_backend.pid
            or guard.role_oid in {self.target.owner_oid, self.target.successor_oid}
            or datetime.fromisoformat(guard.backend.server_started_at)
            != datetime.fromisoformat(self.handoff_backend.server_started_at)
        ):
            raise ValueError("application admission recovery coordination guard changed")

    def to_dict(self) -> dict[str, object]:
        value = asdict(self)
        if self.coordination_guard is None:
            value.pop("coordination_guard")
        return {"schema_version": 1 if self.coordination_guard is None else 2, **value}

    @classmethod
    def from_dict(cls, value: Mapping[str, object]) -> ApplicationAdmissionRecoveryRecord:
        version = value.get("schema_version")
        keys = {"schema_version", "intent_digest", "target", "handoff_backend"}
        if version == 2:
            keys.add("coordination_guard")
        if type(version) is not int or version not in {1, 2} or set(value) != keys:
            raise ValueError("application admission recovery fields are invalid")
        target = value["target"]
        backend = value["handoff_backend"]
        if (
            not isinstance(target, dict)
            or set(target) != set(ApplicationDatabaseAdmissionTarget.__dataclass_fields__)
            or not isinstance(backend, dict)
            or set(backend) != set(ApplicationDatabaseHandoffBackend.__dataclass_fields__)
        ):
            raise ValueError("application admission recovery target fields are invalid")
        guard = None
        if version == 2:
            raw = value["coordination_guard"]
            if (not isinstance(raw, dict)
                    or set(raw) != {"backend", "role_oid", "application_name"}):
                raise ValueError("application admission recovery guard fields are invalid")
            guard = ApplicationDatabaseCoordinationGuard(
                backend=_backend(raw["backend"]), role_oid=_integer(raw, "role_oid"),
                application_name=_string(raw, "application_name"),
            )
        return cls(
            intent_digest=_string(value, "intent_digest"),
            target=ApplicationDatabaseAdmissionTarget(
                system_identifier=_string(target, "system_identifier"),
                database=_string(target, "database"),
                database_oid=_integer(target, "database_oid"),
                owner_role=_string(target, "owner_role"),
                owner_oid=_integer(target, "owner_oid"),
                successor_role=_string(target, "successor_role"),
                successor_oid=_integer(target, "successor_oid"),
            ),
            handoff_backend=_backend(backend),
            coordination_guard=guard,
        )


def _backend(value: object) -> ApplicationDatabaseHandoffBackend:
    if not isinstance(value, dict) or set(value) != set(ApplicationDatabaseHandoffBackend.__dataclass_fields__):
        raise ValueError("application admission recovery backend fields are invalid")
    return ApplicationDatabaseHandoffBackend(
        pid=_integer(value, "pid"), started_at=_string(value, "started_at"),
        system_identifier=_string(value, "system_identifier"),
        server_started_at=_string(value, "server_started_at"), database_oid=_integer(value, "database_oid"),
    )


def _string(value: Mapping[str, object], key: str) -> str:
    item = value[key]
    if not isinstance(item, str):
        raise ValueError("application admission recovery string is invalid")
    return item


def _integer(value: Mapping[str, object], key: str) -> int:
    item = value[key]
    if type(item) is not int:
        raise ValueError("application admission recovery integer is invalid")
    return item
