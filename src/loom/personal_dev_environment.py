"""Durable authority records for personal development environments.

The records in this module intentionally contain no Kubernetes, Slurm, or
builder behavior.  They define the exact owner/candidate/policy request that
must be durably fenced before any of those external effects may start.
"""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass, field
from datetime import datetime
from typing import Literal
from uuid import UUID

from loom.dev_instance import PER_INSTANCE_CAP, InvalidDevInstanceNameError, validate_name

PersonalDevEnvironmentStatus = Literal[
    "provisioning",
    "ready",
    "updating",
    "activating",
    "deleting",
    "draining",
    "failed",
    "deleted",
]
PersonalDevOperationKind = Literal["create", "update", "capacity", "noop"]
PersonalDevOperationState = Literal[
    "requested",
    "running",
    "activating",
    "succeeded",
    "failed",
    "cancelling",
    "cancelled",
]

_DIGEST_RE = re.compile(r"[0-9a-f]{64}")


@dataclass(frozen=True, slots=True)
class PersonalDevLifecycleLimits:
    """Finite operator envelope for shared fixture and lifecycle admission."""

    global_live_instances: int = 16
    per_owner_live_instances: int = 2
    per_owner_aggregate_min_slots: int = 8
    per_owner_aggregate_max_slots: int = 16

    def __post_init__(self) -> None:
        if (
            type(self.global_live_instances) is not int
            or self.global_live_instances <= 0
            or type(self.per_owner_live_instances) is not int
            or self.per_owner_live_instances <= 0
        ):
            raise ValueError("personal-dev live-instance limits must be positive integers")
        if (
            type(self.per_owner_aggregate_min_slots) is not int
            or type(self.per_owner_aggregate_max_slots) is not int
            or not 0
            <= self.per_owner_aggregate_min_slots
            <= self.per_owner_aggregate_max_slots
        ):
            raise ValueError("personal-dev aggregate slot limits must be ordered integers")


@dataclass(frozen=True, slots=True)
class PersonalDevEnvironmentApplyRequest:
    """One authenticated compare-and-set request, before persistence."""

    name: str
    owner_user_id: UUID
    owner_team_id: UUID
    candidate_id: UUID
    candidate_sha: str
    min_slots: int
    max_slots: int
    expected_operation_epoch: int
    idempotency_key: UUID
    request_sha256: str = field(init=False)

    def __post_init__(self) -> None:
        try:
            validate_name(self.name)
        except InvalidDevInstanceNameError as exc:
            raise ValueError("personal-dev environment name is invalid") from exc
        if _DIGEST_RE.fullmatch(self.candidate_sha) is None:
            raise ValueError("personal-dev candidate SHA must be a lowercase SHA-256 digest")
        if type(self.min_slots) is not int or type(self.max_slots) is not int:
            raise ValueError("personal-dev slot limits must be integers")
        if not 0 <= self.min_slots <= self.max_slots <= PER_INSTANCE_CAP:
            raise ValueError(
                f"personal-dev slots must satisfy 0 <= min <= max <= {PER_INSTANCE_CAP}",
            )
        if (
            type(self.expected_operation_epoch) is not int
            or self.expected_operation_epoch < 0
        ):
            raise ValueError("expected_operation_epoch must be a nonnegative integer")
        payload = {
            "candidate_id": str(self.candidate_id),
            "candidate_sha": self.candidate_sha,
            "expected_operation_epoch": self.expected_operation_epoch,
            "max_slots": self.max_slots,
            "min_slots": self.min_slots,
            "name": self.name,
            "owner_team_id": str(self.owner_team_id),
            "owner_user_id": str(self.owner_user_id),
            "schema_version": 1,
        }
        canonical = json.dumps(
            payload,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
        ).encode("ascii")
        object.__setattr__(self, "request_sha256", hashlib.sha256(canonical).hexdigest())


@dataclass(frozen=True, slots=True)
class PersonalDevEnvironmentRecord:
    """Current committed projection for one personal environment subject."""

    name: str
    subject_id: UUID
    subject_incarnation: UUID
    owner_user_id: UUID
    owner_team_id: UUID
    min_slots: int
    max_slots: int
    status: PersonalDevEnvironmentStatus
    deployment_generation: int
    candidate_id: UUID | None
    candidate_sha: str
    operation_epoch: int
    operation_id: UUID
    operation_step: str
    created_at: datetime
    updated_at: datetime
    ready_at: datetime | None = None
    deleted_at: datetime | None = None
    failure_reason: str | None = None


@dataclass(frozen=True, slots=True)
class PersonalDevLifecycleOperationRecord:
    """The immutable request and mutable checkpoint for one logical apply."""

    id: UUID
    idempotency_key: UUID
    environment_name: str
    subject_id: UUID
    subject_incarnation: UUID
    owner_user_id: UUID
    owner_team_id: UUID
    operation_epoch: int
    expected_operation_epoch: int
    kind: PersonalDevOperationKind
    state: PersonalDevOperationState
    attempt_id: UUID
    attempt_sequence: int
    request_sha256: str
    candidate_id: UUID
    candidate_sha: str
    min_slots: int
    max_slots: int
    deployment_generation: int
    checkpoint: str
    created_at: datetime
    updated_at: datetime
    started_at: datetime | None = None
    finished_at: datetime | None = None
    failure_reason: str | None = None
    readiness_evidence_sha256: str | None = None
    activation_acknowledgement_sha256: str | None = None


@dataclass(frozen=True, slots=True)
class PersonalDevApplyReservation:
    environment: PersonalDevEnvironmentRecord
    operation: PersonalDevLifecycleOperationRecord
    acquired: bool
    requires_build_binding: bool


__all__ = [
    "PersonalDevApplyReservation",
    "PersonalDevEnvironmentApplyRequest",
    "PersonalDevEnvironmentRecord",
    "PersonalDevEnvironmentStatus",
    "PersonalDevLifecycleLimits",
    "PersonalDevLifecycleOperationRecord",
    "PersonalDevOperationKind",
    "PersonalDevOperationState",
]
