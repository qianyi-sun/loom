"""Strict, deterministic schema-v1 contracts for global capacity shadowing.

These models contain facts and diagnostics only.  They deliberately contain no
grant, permit, Slurm mutation, or capacity-release contract.
"""

from __future__ import annotations

import hashlib
import json
import re
from datetime import UTC, datetime
from typing import Annotated, Any, Literal, TypeVar, get_origin
from uuid import UUID

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    field_validator,
    model_validator,
)

SCHEMA_VERSION = 1
MAX_QUANTITY = (1 << 63) - 1
MAX_POOLS = 32
MAX_DOMAINS_PER_POOL = 128
MAX_NODES_PER_DOMAIN = 4_096
MAX_SUBJECTS = 10_000
MAX_SHAPES_PER_PROFILE = 256
MAX_DEMAND_BUCKETS_PER_REPORT = 2_048
MAX_ASSIGNMENTS_PER_REPORT = 10_000
MAX_FIXED_CLAIMS_PER_REPORT = 10_000
MAX_CONTRACT_BYTES = 8 * 1024 * 1024

_IDENTIFIER_PATTERN = r"^[a-z0-9][a-z0-9_.-]{0,127}$"
_RESOURCE_PATTERN = re.compile(r"^[a-z][a-z0-9_.-]{0,62}$")
_DIGEST_PATTERN = r"^[0-9a-f]{64}$"

Identifier = Annotated[str, Field(min_length=1, max_length=128, pattern=_IDENTIFIER_PATTERN)]
Digest = Annotated[str, Field(pattern=_DIGEST_PATTERN)]
Quantity = Annotated[int, Field(ge=0, le=MAX_QUANTITY)]
PositiveQuantity = Annotated[int, Field(gt=0, le=MAX_QUANTITY)]


class CapacityContractError(ValueError):
    """Raised when checked contract arithmetic or encoding is unsafe."""


class StrictV1Model(BaseModel):
    """Frozen base for every externally persisted schema-v1 document."""

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    schema_version: Literal[1] = 1

    @model_validator(mode="before")
    @classmethod
    def _json_values_to_strict_types(cls, value: Any) -> Any:
        """Decode canonical JSON containers/times without numeric coercion."""

        if not isinstance(value, dict):
            return value
        normalized = dict(value)
        for name, field in cls.model_fields.items():
            if get_origin(field.annotation) is tuple and isinstance(normalized.get(name), list):
                normalized[name] = tuple(normalized[name])
            if field.annotation is datetime and isinstance(normalized.get(name), str):
                timestamp = normalized[name]
                if timestamp.endswith("Z"):
                    timestamp = f"{timestamp[:-1]}+00:00"
                try:
                    normalized[name] = datetime.fromisoformat(timestamp)
                except ValueError:
                    pass
        return normalized


_ModelT = TypeVar("_ModelT", bound=StrictV1Model)


def checked_add(left: int, right: int) -> int:
    """Add two canonical quantities and reject type, sign, or uint63 overflow."""

    if type(left) is not int or type(right) is not int:
        raise CapacityContractError("capacity quantities must be integers")
    if left < 0 or right < 0:
        raise CapacityContractError("capacity quantities must be nonnegative")
    result = left + right
    if result > MAX_QUANTITY:
        raise CapacityContractError("capacity quantity overflow")
    return result


def checked_sum(values: tuple[int, ...]) -> int:
    result = 0
    for value in values:
        result = checked_add(result, value)
    return result


def _canonical_payload(model: StrictV1Model) -> dict[str, Any]:
    payload = model.model_dump(mode="json", exclude_none=False)
    if not isinstance(payload, dict):  # pragma: no cover - BaseModel guarantee
        raise CapacityContractError("contract payload must be an object")
    return payload


def canonical_bytes(model: StrictV1Model) -> bytes:
    """Encode one canonical contract without platform- or ordering-dependent bytes."""

    if not isinstance(model, StrictV1Model):
        raise CapacityContractError("canonical encoding requires a schema-v1 model")
    encoded = json.dumps(
        _canonical_payload(model),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("ascii")
    if len(encoded) > MAX_CONTRACT_BYTES:
        raise CapacityContractError("canonical contract exceeds maximum byte size")
    return encoded


def canonical_digest(model: StrictV1Model) -> str:
    return hashlib.sha256(canonical_bytes(model)).hexdigest()


def canonical_digest_excluding(
    model: StrictV1Model,
    *fields: str,
) -> str:
    """Digest a generation document while excluding its self-declared digest."""

    payload = _canonical_payload(model)
    for field in fields:
        payload.pop(field, None)
    encoded = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("ascii")
    if len(encoded) > MAX_CONTRACT_BYTES:
        raise CapacityContractError("canonical contract exceeds maximum byte size")
    return hashlib.sha256(encoded).hexdigest()


def _ensure_unique(values: tuple[Any, ...], attribute: str, label: str) -> None:
    identities = [getattr(value, attribute) for value in values]
    if len(identities) != len(set(identities)):
        raise ValueError(f"duplicate {label}")


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("timestamp must include an offset")
    return value.astimezone(UTC)


class ResourceVectorV1(StrictV1Model):
    slots: Quantity = 0
    cpu_millicores: Quantity = 0
    memory_bytes: Quantity = 0
    gpu_count: Quantity = 0
    generic: dict[str, Quantity] = Field(default_factory=dict)

    @field_validator("generic")
    @classmethod
    def _canonical_generic(cls, value: dict[str, int]) -> dict[str, int]:
        for key in value:
            if _RESOURCE_PATTERN.fullmatch(key) is None:
                raise ValueError(f"generic resource name is not canonical: {key!r}")
        return dict(sorted(value.items()))


def checked_add_vectors(left: ResourceVectorV1, right: ResourceVectorV1) -> ResourceVectorV1:
    keys = tuple(sorted(set(left.generic) | set(right.generic)))
    return ResourceVectorV1(
        slots=checked_add(left.slots, right.slots),
        cpu_millicores=checked_add(left.cpu_millicores, right.cpu_millicores),
        memory_bytes=checked_add(left.memory_bytes, right.memory_bytes),
        gpu_count=checked_add(left.gpu_count, right.gpu_count),
        generic={
            key: checked_add(left.generic.get(key, 0), right.generic.get(key, 0)) for key in keys
        },
    )


def checked_sum_vectors(values: tuple[ResourceVectorV1, ...]) -> ResourceVectorV1:
    result = ResourceVectorV1()
    for value in values:
        result = checked_add_vectors(result, value)
    return result


def vector_fits(required: ResourceVectorV1, available: ResourceVectorV1) -> bool:
    return (
        required.slots <= available.slots
        and required.cpu_millicores <= available.cpu_millicores
        and required.memory_bytes <= available.memory_bytes
        and required.gpu_count <= available.gpu_count
        and all(
            required.generic.get(key, 0) <= available.generic.get(key, 0)
            for key in required.generic
        )
    )


class NodeEnvelopeV1(StrictV1Model):
    node_id: Identifier
    allocatable: ResourceVectorV1
    features: tuple[Identifier, ...] = ()

    @field_validator("features")
    @classmethod
    def _features(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if len(value) != len(set(value)):
            raise ValueError("duplicate node feature")
        return tuple(sorted(value))


class ResourceDomainV1(StrictV1Model):
    domain_id: Identifier
    architecture: Literal["x86_64", "arm64"]
    partition: Identifier
    nodes: Annotated[
        tuple[NodeEnvelopeV1, ...], Field(min_length=1, max_length=MAX_NODES_PER_DOMAIN)
    ]
    topology_constraints: dict[Identifier, Identifier] = Field(default_factory=dict)

    @field_validator("nodes")
    @classmethod
    def _nodes(cls, value: tuple[NodeEnvelopeV1, ...]) -> tuple[NodeEnvelopeV1, ...]:
        _ensure_unique(value, "node_id", "node_id")
        return tuple(sorted(value, key=lambda item: item.node_id))

    @field_validator("topology_constraints")
    @classmethod
    def _topology(cls, value: dict[str, str]) -> dict[str, str]:
        return dict(sorted(value.items()))


class WorkerShapeV1(StrictV1Model):
    shape_id: Identifier
    concurrency_slots: PositiveQuantity
    total_resources: ResourceVectorV1
    node_resources: Annotated[
        tuple[ResourceVectorV1, ...],
        Field(min_length=1, max_length=MAX_NODES_PER_DOMAIN),
    ]
    compatible_domain_ids: Annotated[tuple[Identifier, ...], Field(min_length=1)]
    capabilities: Annotated[tuple[Identifier, ...], Field(min_length=1)]
    placement_constraints: dict[Identifier, Identifier] = Field(default_factory=dict)
    warm_approved: bool = False

    @field_validator("compatible_domain_ids", "capabilities")
    @classmethod
    def _unique_sorted_strings(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if len(value) != len(set(value)):
            raise ValueError("duplicate shape compatibility value")
        return tuple(sorted(value))

    @field_validator("placement_constraints")
    @classmethod
    def _constraints(cls, value: dict[str, str]) -> dict[str, str]:
        return dict(sorted(value.items()))

    @model_validator(mode="after")
    def _exact_node_sum(self) -> WorkerShapeV1:
        if checked_sum_vectors(self.node_resources) != self.total_resources:
            raise ValueError("node resources must sum exactly to total_resources")
        if self.total_resources.slots != self.concurrency_slots:
            raise ValueError("total resource slots must equal concurrency_slots")
        return self


class TierPolicyV1(StrictV1Model):
    tier_id: Literal["production", "staging", "development"]
    priority: Annotated[int, Field(ge=0, le=2)]
    max_slots: Quantity
    max_pending_slots: Quantity
    max_pending_jobs: Quantity


class AccountPolicyV1(StrictV1Model):
    account_id: Identifier
    kind: Literal["service", "owner", "owner_template"]
    owner_id: UUID | None = None
    min_reservation_slots: Quantity = 0
    max_slots: Quantity
    max_surge_slots: Quantity = 0
    max_pending_slots: Quantity
    max_pending_jobs: Quantity
    max_live_subjects: PositiveQuantity

    @model_validator(mode="after")
    def _limits(self) -> AccountPolicyV1:
        if self.min_reservation_slots > self.max_slots:
            raise ValueError("min reservation exceeds account max_slots")
        checked_add(self.max_slots, self.max_surge_slots)
        return self


class PoolManifestV1(StrictV1Model):
    pool_id: Identifier
    pool_generation: PositiveQuantity
    pool_digest: Digest
    controller: Identifier
    partition: Identifier
    association: Identifier
    protocol_generation: PositiveQuantity
    protocol_digest: Digest
    pool_reporter_incarnation: UUID
    resource_domains: Annotated[
        tuple[ResourceDomainV1, ...],
        Field(min_length=1, max_length=MAX_DOMAINS_PER_POOL),
    ]
    max_slots: Quantity
    max_pending_slots: Quantity
    max_pending_jobs: Quantity
    submission_rate_per_minute: Quantity
    health: Literal["eligible", "maintenance", "unhealthy"]

    @field_validator("resource_domains")
    @classmethod
    def _domains(cls, value: tuple[ResourceDomainV1, ...]) -> tuple[ResourceDomainV1, ...]:
        _ensure_unique(value, "domain_id", "domain_id")
        return tuple(sorted(value, key=lambda item: item.domain_id))

    @model_validator(mode="after")
    def _physical_nodes_are_unique(self) -> PoolManifestV1:
        node_ids = tuple(node.node_id for domain in self.resource_domains for node in domain.nodes)
        if len(node_ids) != len(set(node_ids)):
            raise ValueError("duplicate physical node_id across pool resource domains")
        return self


class FleetManifestV1(StrictV1Model):
    authority_incarnation: UUID
    fleet_generation: PositiveQuantity
    fleet_digest: Digest
    executable_new_capacity_ceiling: Literal[0] = 0
    tiers: Annotated[tuple[TierPolicyV1, ...], Field(min_length=3, max_length=3)]
    account_policies: Annotated[tuple[AccountPolicyV1, ...], Field(min_length=1)]
    pools: Annotated[tuple[PoolManifestV1, ...], Field(min_length=1, max_length=MAX_POOLS)]
    global_max_pending_slots: Quantity
    global_max_pending_jobs: Quantity
    global_submission_rate_per_minute: Quantity

    @field_validator("tiers")
    @classmethod
    def _tiers(cls, value: tuple[TierPolicyV1, ...]) -> tuple[TierPolicyV1, ...]:
        _ensure_unique(value, "tier_id", "tier_id")
        _ensure_unique(value, "priority", "tier priority")
        ordered = tuple(sorted(value, key=lambda item: item.priority))
        if tuple(item.tier_id for item in ordered) != (
            "production",
            "staging",
            "development",
        ):
            raise ValueError("tier priority must be production, staging, development")
        return ordered

    @field_validator("account_policies")
    @classmethod
    def _accounts(cls, value: tuple[AccountPolicyV1, ...]) -> tuple[AccountPolicyV1, ...]:
        _ensure_unique(value, "account_id", "account_id")
        return tuple(sorted(value, key=lambda item: item.account_id))

    @field_validator("pools")
    @classmethod
    def _pools(cls, value: tuple[PoolManifestV1, ...]) -> tuple[PoolManifestV1, ...]:
        _ensure_unique(value, "pool_id", "pool_id")
        return tuple(sorted(value, key=lambda item: item.pool_id))


class ProfileReferenceV1(StrictV1Model):
    pool_id: Identifier
    pool_generation: PositiveQuantity
    pool_digest: Digest
    profile_generation: PositiveQuantity
    profile_digest: Digest
    protocol_generation: PositiveQuantity
    protocol_digest: Digest
    eligible_resource_domains: Annotated[tuple[Identifier, ...], Field(min_length=1)]
    worker_shapes: Annotated[
        tuple[WorkerShapeV1, ...],
        Field(min_length=1, max_length=MAX_SHAPES_PER_PROFILE),
    ]

    @field_validator("eligible_resource_domains")
    @classmethod
    def _eligible_domains(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if len(value) != len(set(value)):
            raise ValueError("duplicate eligible resource domain")
        return tuple(sorted(value))

    @field_validator("worker_shapes")
    @classmethod
    def _shapes(cls, value: tuple[WorkerShapeV1, ...]) -> tuple[WorkerShapeV1, ...]:
        _ensure_unique(value, "shape_id", "shape_id")
        return tuple(sorted(value, key=lambda item: item.shape_id))

    @model_validator(mode="after")
    def _shape_domains_are_narrowed(self) -> ProfileReferenceV1:
        eligible = set(self.eligible_resource_domains)
        for item in self.worker_shapes:
            if not set(item.compatible_domain_ids) <= eligible:
                raise ValueError("shape references a domain outside the profile narrowing")
        if not any(item.concurrency_slots == 1 for item in self.worker_shapes):
            raise ValueError("worker profile requires a mandatory one-slot shape")
        return self


class SubjectConfigurationV1(StrictV1Model):
    subject_id: UUID
    subject_incarnation: UUID
    display_name: Identifier
    account_id: Identifier
    tier_id: Literal["production", "staging", "development"]
    min_slots: Quantity = 0
    max_slots: Quantity
    rollout_surge_slots: Quantity = 0
    max_pending_slots: Quantity
    max_pending_jobs: Quantity
    lifecycle_state: Literal["provisioning", "active", "draining", "disabled"]
    candidate_generation: PositiveQuantity
    deployment_generation: PositiveQuantity
    configuration_generation: PositiveQuantity
    demand_reporter_incarnation: UUID
    profiles: Annotated[tuple[ProfileReferenceV1, ...], Field(min_length=1, max_length=MAX_POOLS)]

    @field_validator("profiles")
    @classmethod
    def _profiles(cls, value: tuple[ProfileReferenceV1, ...]) -> tuple[ProfileReferenceV1, ...]:
        _ensure_unique(value, "pool_id", "profile pool_id")
        return tuple(sorted(value, key=lambda item: item.pool_id))

    @model_validator(mode="after")
    def _finite_limits(self) -> SubjectConfigurationV1:
        if self.min_slots > self.max_slots:
            raise ValueError("min_slots must not exceed max_slots")
        checked_add(self.max_slots, self.rollout_surge_slots)
        if self.tier_id == "development" and {item.pool_id for item in self.profiles} != {
            "gb10",
            "oldlab",
        }:
            raise ValueError("development subjects require gb10 and oldlab profiles")
        return self


class ConfigurationGenerationRefV1(StrictV1Model):
    scope: Literal["fleet", "subject"]
    generation: PositiveQuantity
    digest: Digest
    subject_id: UUID | None = None
    subject_incarnation: UUID | None = None

    @model_validator(mode="after")
    def _scope_binding(self) -> ConfigurationGenerationRefV1:
        bound = self.subject_id is not None or self.subject_incarnation is not None
        if self.scope == "fleet" and bound:
            raise ValueError("fleet generation cannot carry a subject binding")
        if self.scope == "subject" and (
            self.subject_id is None or self.subject_incarnation is None
        ):
            raise ValueError("subject generation requires an exact subject binding")
        return self


class ConfigurationSnapshotV1(StrictV1Model):
    configuration_epoch: Quantity
    fleet: ConfigurationGenerationRefV1
    subjects: Annotated[tuple[ConfigurationGenerationRefV1, ...], Field(max_length=MAX_SUBJECTS)]

    @field_validator("subjects")
    @classmethod
    def _subjects(
        cls, value: tuple[ConfigurationGenerationRefV1, ...]
    ) -> tuple[ConfigurationGenerationRefV1, ...]:
        if any(item.scope != "subject" for item in value):
            raise ValueError("configuration subject manifest contains a non-subject scope")
        _ensure_unique(value, "subject_id", "configuration subject_id")
        return tuple(sorted(value, key=lambda item: item.subject_id.hex if item.subject_id else ""))

    @model_validator(mode="after")
    def _fleet_scope(self) -> ConfigurationSnapshotV1:
        if self.fleet.scope != "fleet":
            raise ValueError("configuration fleet reference must have fleet scope")
        return self


class ConfigurationActivationV1(StrictV1Model):
    expected_configuration_epoch: Quantity
    fleet: ConfigurationGenerationRefV1
    subjects: Annotated[tuple[ConfigurationGenerationRefV1, ...], Field(max_length=MAX_SUBJECTS)]

    @field_validator("subjects")
    @classmethod
    def _subjects(
        cls, value: tuple[ConfigurationGenerationRefV1, ...]
    ) -> tuple[ConfigurationGenerationRefV1, ...]:
        _ensure_unique(value, "subject_id", "activation subject_id")
        return tuple(sorted(value, key=lambda item: item.subject_id.hex if item.subject_id else ""))


class DemandBucketV1(StrictV1Model):
    bucket_id: Identifier
    requested_slots: PositiveQuantity
    local_priority: Quantity
    oldest_submitted_at: datetime
    eligible_pool_ids: Annotated[tuple[Identifier, ...], Field(min_length=1, max_length=MAX_POOLS)]
    required_capabilities: tuple[Identifier, ...] = ()
    attempt_ids: Annotated[tuple[Identifier, ...], Field(min_length=1)]

    @field_validator("oldest_submitted_at")
    @classmethod
    def _time(cls, value: datetime) -> datetime:
        return _utc(value)

    @field_validator("eligible_pool_ids", "required_capabilities", "attempt_ids")
    @classmethod
    def _strings(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if len(value) != len(set(value)):
            raise ValueError("duplicate demand identity")
        return tuple(sorted(value))

    @model_validator(mode="after")
    def _slot_count(self) -> DemandBucketV1:
        if len(self.attempt_ids) != self.requested_slots:
            raise ValueError("demand bucket slots must equal distinct attempt count")
        return self


class CurrentAssignmentV1(StrictV1Model):
    attempt_id: Identifier
    pool_id: Identifier
    pool_generation: PositiveQuantity
    profile_id: Identifier
    profile_generation: PositiveQuantity
    profile_digest: Digest
    shape_id: Identifier
    allowance_epoch: PositiveQuantity
    local_priority: Quantity
    submitted_at: datetime

    @field_validator("submitted_at")
    @classmethod
    def _time(cls, value: datetime) -> datetime:
        return _utc(value)


class FixedClaimV1(StrictV1Model):
    claim_id: Identifier
    attempt_id: Identifier
    worker_identity: Identifier
    pool_id: Identifier
    pool_generation: PositiveQuantity
    profile_id: Identifier
    profile_generation: PositiveQuantity
    profile_digest: Digest
    shape_id: Identifier
    deployment_generation: PositiveQuantity
    concurrency_slots: PositiveQuantity
    resources: ResourceVectorV1
    state: Literal["pending", "live", "cancel-pending", "unknown", "quarantined"]


class DemandSnapshotV1(StrictV1Model):
    subject_id: UUID
    subject_incarnation: UUID
    configuration_generation: PositiveQuantity
    deployment_generation: PositiveQuantity
    reporter_incarnation: UUID
    sequence: PositiveQuantity
    source_observed_at: datetime
    pending_unassigned: Annotated[
        tuple[DemandBucketV1, ...], Field(max_length=MAX_DEMAND_BUCKETS_PER_REPORT)
    ]
    current_assignments: Annotated[
        tuple[CurrentAssignmentV1, ...], Field(max_length=MAX_ASSIGNMENTS_PER_REPORT)
    ]
    fixed_claims: Annotated[tuple[FixedClaimV1, ...], Field(max_length=MAX_FIXED_CLAIMS_PER_REPORT)]

    @field_validator("source_observed_at")
    @classmethod
    def _time(cls, value: datetime) -> datetime:
        return _utc(value)

    @field_validator("pending_unassigned")
    @classmethod
    def _buckets(cls, value: tuple[DemandBucketV1, ...]) -> tuple[DemandBucketV1, ...]:
        _ensure_unique(value, "bucket_id", "demand bucket_id")
        return tuple(sorted(value, key=lambda item: item.bucket_id))

    @field_validator("current_assignments")
    @classmethod
    def _assignments(
        cls, value: tuple[CurrentAssignmentV1, ...]
    ) -> tuple[CurrentAssignmentV1, ...]:
        _ensure_unique(value, "attempt_id", "assigned attempt_id")
        return tuple(sorted(value, key=lambda item: item.attempt_id))

    @field_validator("fixed_claims")
    @classmethod
    def _claims(cls, value: tuple[FixedClaimV1, ...]) -> tuple[FixedClaimV1, ...]:
        _ensure_unique(value, "claim_id", "fixed claim_id")
        _ensure_unique(value, "attempt_id", "fixed claim attempt_id")
        return tuple(sorted(value, key=lambda item: item.claim_id))

    @model_validator(mode="after")
    def _attempts_do_not_overlap(self) -> DemandSnapshotV1:
        pending = {attempt for bucket in self.pending_unassigned for attempt in bucket.attempt_ids}
        assigned = {item.attempt_id for item in self.current_assignments}
        claimed = {item.attempt_id for item in self.fixed_claims}
        if pending & assigned:
            raise ValueError("an attempt cannot be pending-unassigned and assigned")
        if pending & claimed:
            raise ValueError("an attempt cannot be pending-unassigned and claimed")
        return self


class ObservedCommitmentV1(StrictV1Model):
    kind: Literal["claim", "physical", "reserve"]
    commitment_id: Identifier
    physical_identity: Identifier
    attempt_id: Identifier | None = None
    concurrency_slots: PositiveQuantity | None = None
    subject_id: UUID
    subject_incarnation: UUID
    deployment_generation: PositiveQuantity
    pool_id: Identifier
    pool_generation: PositiveQuantity
    profile_id: Identifier
    profile_generation: PositiveQuantity
    profile_digest: Digest
    shape_id: Identifier | None = None
    resources: ResourceVectorV1
    state: Literal[
        "proposed",
        "accepted",
        "pending",
        "live",
        "draining",
        "cancel-pending",
        "submitting-unknown",
        "observed",
        "unknown",
        "quarantined",
    ]
    node_ids: tuple[Identifier, ...] = ()

    @field_validator("node_ids")
    @classmethod
    def _node_ids(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if len(value) != len(set(value)):
            raise ValueError("duplicate commitment node_id")
        return tuple(sorted(value))

    @model_validator(mode="after")
    def _kind_fields(self) -> ObservedCommitmentV1:
        if self.kind == "claim" and (self.attempt_id is None or self.concurrency_slots is None):
            raise ValueError("claim commitment requires attempt and concurrency slots")
        if self.kind != "claim" and (
            self.attempt_id is not None or self.concurrency_slots is not None
        ):
            raise ValueError("physical commitment cannot carry claim-only fields")
        return self


class PoolObservationV1(StrictV1Model):
    pool_id: Identifier
    pool_generation: PositiveQuantity
    reporter_incarnation: UUID
    sequence: PositiveQuantity
    source_observed_at: datetime
    health: Literal["eligible", "maintenance", "unhealthy", "unknown"]
    commitments: Annotated[
        tuple[ObservedCommitmentV1, ...], Field(max_length=MAX_FIXED_CLAIMS_PER_REPORT)
    ]

    @field_validator("source_observed_at")
    @classmethod
    def _time(cls, value: datetime) -> datetime:
        return _utc(value)

    @field_validator("commitments")
    @classmethod
    def _commitments(
        cls, value: tuple[ObservedCommitmentV1, ...]
    ) -> tuple[ObservedCommitmentV1, ...]:
        _ensure_unique(value, "commitment_id", "commitment_id")
        if any(item.kind != "physical" for item in value):
            raise ValueError("pool reporter commitments must be physical")
        return tuple(sorted(value, key=lambda item: item.commitment_id))


class InputFreshnessV1(StrictV1Model):
    state: Literal["valid", "stale", "missing", "invalid", "equivocal"]
    last_payload_digest: Digest | None = None
    database_received_at: datetime | None = None

    @field_validator("database_received_at", mode="before")
    @classmethod
    def _parse_time(cls, value: datetime | str | None) -> datetime | str | None:
        if isinstance(value, str):
            timestamp = f"{value[:-1]}+00:00" if value.endswith("Z") else value
            try:
                return datetime.fromisoformat(timestamp)
            except ValueError:
                return value
        return value

    @field_validator("database_received_at")
    @classmethod
    def _time(cls, value: datetime | None) -> datetime | None:
        return None if value is None else _utc(value)


class FairnessCursorV1(StrictV1Model):
    tier_id: Literal["production", "staging", "development"]
    phase: Literal["minimum", "demand"]
    account_id: Identifier | None = None
    subject_id: UUID | None = None

    @model_validator(mode="after")
    def _complete_scope(self) -> FairnessCursorV1:
        if self.account_id is None:
            raise ValueError("fairness cursor requires account_id")
        return self


class SubjectAllocationInputV1(StrictV1Model):
    configuration: SubjectConfigurationV1
    freshness: InputFreshnessV1
    last_demand: DemandSnapshotV1 | None = None


class PoolAllocationInputV1(StrictV1Model):
    configuration: PoolManifestV1
    freshness: InputFreshnessV1
    last_observation: PoolObservationV1 | None = None


class PackingShapeRequestV1(StrictV1Model):
    instance_id: Identifier
    shape: WorkerShapeV1


class PackingRequestV1(StrictV1Model):
    pool_id: Identifier
    domains: Annotated[tuple[ResourceDomainV1, ...], Field(min_length=1)]
    fixed_commitments: tuple[ObservedCommitmentV1, ...] = ()
    desired_shapes: tuple[PackingShapeRequestV1, ...] = ()

    @field_validator("domains")
    @classmethod
    def _domains(cls, value: tuple[ResourceDomainV1, ...]) -> tuple[ResourceDomainV1, ...]:
        _ensure_unique(value, "domain_id", "packing domain_id")
        return tuple(sorted(value, key=lambda item: item.domain_id))

    @field_validator("fixed_commitments")
    @classmethod
    def _fixed(cls, value: tuple[ObservedCommitmentV1, ...]) -> tuple[ObservedCommitmentV1, ...]:
        _ensure_unique(value, "commitment_id", "packing commitment_id")
        if any(item.kind == "claim" for item in value):
            raise ValueError("topology packing accepts only physical capacity")
        return tuple(sorted(value, key=lambda item: item.commitment_id))

    @field_validator("desired_shapes")
    @classmethod
    def _desired(
        cls, value: tuple[PackingShapeRequestV1, ...]
    ) -> tuple[PackingShapeRequestV1, ...]:
        _ensure_unique(value, "instance_id", "shape instance_id")
        return tuple(sorted(value, key=lambda item: item.instance_id))

    @model_validator(mode="after")
    def _physical_nodes_are_unique(self) -> PackingRequestV1:
        node_ids = tuple(node.node_id for domain in self.domains for node in domain.nodes)
        if len(node_ids) != len(set(node_ids)):
            raise ValueError("duplicate physical node_id across packing domains")
        return self


class PackingPlacementV1(StrictV1Model):
    instance_id: Identifier
    domain_id: Identifier
    node_ids: Annotated[tuple[Identifier, ...], Field(min_length=1)]


class NodeResidualV1(StrictV1Model):
    node_id: Identifier
    residual: ResourceVectorV1


class PackingWitnessV1(StrictV1Model):
    pool_id: Identifier
    placements: tuple[PackingPlacementV1, ...]
    residuals: tuple[NodeResidualV1, ...]
    charged_commitment_ids: tuple[Identifier, ...]
    over_limit_slots: Quantity = 0
    new_placement_allowed: bool = True
    blockers: tuple[Identifier, ...] = ()

    @model_validator(mode="after")
    def _complete_witness(self) -> PackingWitnessV1:
        _ensure_unique(self.placements, "instance_id", "packing placement instance")
        _ensure_unique(self.residuals, "node_id", "packing residual node")
        if len(self.charged_commitment_ids) != len(set(self.charged_commitment_ids)):
            raise ValueError("duplicate charged commitment")
        if len(self.blockers) != len(set(self.blockers)):
            raise ValueError("duplicate packing blocker")
        return self


class PlacementAllowanceV1(StrictV1Model):
    attempt_id: Identifier
    pool_id: Identifier
    shape_instance_id: Identifier


class ClaimSlotMatchV1(StrictV1Model):
    claim_id: Identifier
    physical_identity: Identifier
    slot_index: Quantity


class JointMatchingWitnessV1(StrictV1Model):
    matched_slots: Quantity
    attempt_ids: tuple[Identifier, ...]
    shape_instance_ids: tuple[Identifier, ...]

    @model_validator(mode="after")
    def _exact_matching(self) -> JointMatchingWitnessV1:
        if self.matched_slots != len(self.attempt_ids) or self.matched_slots != len(
            self.shape_instance_ids
        ):
            raise ValueError("matching witness count is inconsistent")
        if len(self.attempt_ids) != len(set(self.attempt_ids)):
            raise ValueError("matching witness duplicates an attempt")
        if len(self.shape_instance_ids) != len(set(self.shape_instance_ids)):
            raise ValueError("matching witness duplicates a physical slot")
        if self.attempt_ids != tuple(sorted(self.attempt_ids)):
            raise ValueError("matching witness attempts are not canonical")
        return self


class DesiredShapeCountV1(StrictV1Model):
    shape_id: Identifier
    count: Quantity


class RolloutSurgePairingV1(StrictV1Model):
    old_commitment_id: Identifier
    new_shape_instance_id: Identifier
    backed_slots: PositiveQuantity


class ShadowAllocationV1(StrictV1Model):
    subject_id: UUID
    subject_incarnation: UUID
    deployment_generation: PositiveQuantity
    pool_id: Identifier
    desired_slots: Quantity
    requested_slots: Quantity
    new_allowance_slots: Quantity
    retained_commitment_slots: Quantity
    desired_shapes: tuple[DesiredShapeCountV1, ...]
    protected_claim_slots: Quantity
    physical_committed_shape_slots: Quantity
    surge_pairings: tuple[RolloutSurgePairingV1, ...] = ()
    draining_shape_ids: tuple[Identifier, ...]
    placement_allowances: tuple[PlacementAllowanceV1, ...]
    claim_slot_matches: tuple[ClaimSlotMatchV1, ...]
    matching_witness: JointMatchingWitnessV1 | None
    blockers: tuple[Identifier, ...]
    executable: Literal[False] = False

    @model_validator(mode="after")
    def _internally_consistent(self) -> ShadowAllocationV1:
        if self.new_allowance_slots != len(self.placement_allowances):
            raise ValueError("new allowance count is inconsistent")
        _ensure_unique(self.desired_shapes, "shape_id", "desired shape_id")
        _ensure_unique(
            self.placement_allowances,
            "attempt_id",
            "placement allowance attempt_id",
        )
        if any(allowance.pool_id != self.pool_id for allowance in self.placement_allowances):
            raise ValueError("placement allowance is bound to a different pool")
        _ensure_unique(self.claim_slot_matches, "claim_id", "claim slot match")
        claim_slots = {
            (item.physical_identity, item.slot_index) for item in self.claim_slot_matches
        }
        if len(claim_slots) != len(self.claim_slot_matches):
            raise ValueError("claim matches duplicate one physical slot")
        _ensure_unique(
            self.surge_pairings,
            "new_shape_instance_id",
            "rollout surge new shape",
        )
        if len(self.draining_shape_ids) != len(set(self.draining_shape_ids)):
            raise ValueError("duplicate draining shape")
        if len(self.blockers) != len(set(self.blockers)):
            raise ValueError("duplicate allocation blocker")
        if self.matching_witness is not None:
            allowance_attempts = {item.attempt_id for item in self.placement_allowances}
            if not allowance_attempts <= set(self.matching_witness.attempt_ids):
                raise ValueError("allowance is absent from the joint matching witness")
            slot_by_attempt = dict(
                zip(
                    self.matching_witness.attempt_ids,
                    self.matching_witness.shape_instance_ids,
                    strict=True,
                )
            )
            if any(
                not slot_by_attempt[allowance.attempt_id].startswith(
                    f"{allowance.shape_instance_id}-slot-"
                )
                for allowance in self.placement_allowances
            ):
                raise ValueError("allowance shape disagrees with matching witness")
        elif self.placement_allowances:
            raise ValueError("placement allowances require a joint matching witness")
        return self


class RankedHypotheticalLaunchV1(StrictV1Model):
    rank: PositiveQuantity
    subject_id: UUID
    pool_id: Identifier
    shape_instance_id: Identifier
    rate_state: Literal["unavailable_package_1"] = "unavailable_package_1"


class ShadowEpochV1(StrictV1Model):
    configuration: ConfigurationSnapshotV1
    input_digest: Digest
    allocations: tuple[ShadowAllocationV1, ...]
    next_fairness_cursors: tuple[FairnessCursorV1, ...]
    hypothetical_launch_rank: tuple[RankedHypotheticalLaunchV1, ...]
    pool_witnesses: tuple[PackingWitnessV1, ...]
    blockers: tuple[Identifier, ...]
    executable_new_capacity_ceiling: Literal[0] = 0
    executable: Literal[False] = False

    @field_validator("pool_witnesses")
    @classmethod
    def _witnesses(cls, value: tuple[PackingWitnessV1, ...]) -> tuple[PackingWitnessV1, ...]:
        _ensure_unique(value, "pool_id", "packing witness pool_id")
        return tuple(sorted(value, key=lambda item: item.pool_id))

    @model_validator(mode="after")
    def _complete_epoch(self) -> ShadowEpochV1:
        allocation_keys = [
            (item.subject_id, item.subject_incarnation, item.pool_id) for item in self.allocations
        ]
        if len(allocation_keys) != len(set(allocation_keys)):
            raise ValueError("duplicate subject-pool shadow allocation")
        ranks = tuple(item.rank for item in self.hypothetical_launch_rank)
        if ranks != tuple(range(1, len(ranks) + 1)):
            raise ValueError("hypothetical launch ranks must be contiguous")
        launch_shapes = [item.shape_instance_id for item in self.hypothetical_launch_rank]
        if len(launch_shapes) != len(set(launch_shapes)):
            raise ValueError("duplicate hypothetical launch shape")
        cursor_keys = [
            (
                item.tier_id,
                item.phase,
                item.account_id if item.subject_id is not None else None,
            )
            for item in self.next_fairness_cursors
        ]
        if len(cursor_keys) != len(set(cursor_keys)):
            raise ValueError("duplicate fairness cursor scope")
        if len(self.blockers) != len(set(self.blockers)):
            raise ValueError("duplicate epoch blocker")
        return self


class AllocationInputV1(StrictV1Model):
    configuration: ConfigurationSnapshotV1
    fleet: FleetManifestV1
    subjects: Annotated[tuple[SubjectAllocationInputV1, ...], Field(max_length=MAX_SUBJECTS)]
    pools: Annotated[tuple[PoolAllocationInputV1, ...], Field(max_length=MAX_POOLS)]
    observed_commitments: tuple[ObservedCommitmentV1, ...] = ()
    fairness_cursors: tuple[FairnessCursorV1, ...] = ()
    existing_pending_slots: Quantity = 0
    existing_pending_jobs: Quantity = 0

    @property
    def observed_commitment_ids(self) -> tuple[str, ...]:
        return tuple(sorted(commitment.commitment_id for commitment in self.observed_commitments))

    @field_validator("subjects")
    @classmethod
    def _subjects(
        cls, value: tuple[SubjectAllocationInputV1, ...]
    ) -> tuple[SubjectAllocationInputV1, ...]:
        identities = [item.configuration.subject_id for item in value]
        if len(identities) != len(set(identities)):
            raise ValueError("duplicate allocation subject_id")
        return tuple(sorted(value, key=lambda item: item.configuration.subject_id.hex))

    @field_validator("pools")
    @classmethod
    def _pools(cls, value: tuple[PoolAllocationInputV1, ...]) -> tuple[PoolAllocationInputV1, ...]:
        identities = [item.configuration.pool_id for item in value]
        if len(identities) != len(set(identities)):
            raise ValueError("duplicate allocation pool_id")
        return tuple(sorted(value, key=lambda item: item.configuration.pool_id))

    @field_validator("observed_commitments")
    @classmethod
    def _commitments(
        cls, value: tuple[ObservedCommitmentV1, ...]
    ) -> tuple[ObservedCommitmentV1, ...]:
        keys = [(item.kind, item.commitment_id) for item in value]
        if len(keys) != len(set(keys)):
            raise ValueError("duplicate allocation commitment identity")
        return tuple(sorted(value, key=lambda item: (item.kind, item.commitment_id)))

    @field_validator("fairness_cursors")
    @classmethod
    def _cursors(cls, value: tuple[FairnessCursorV1, ...]) -> tuple[FairnessCursorV1, ...]:
        keys = [
            (
                item.tier_id,
                item.phase,
                item.account_id if item.subject_id is not None else None,
            )
            for item in value
        ]
        if len(keys) != len(set(keys)):
            raise ValueError("duplicate allocation fairness cursor scope")
        return tuple(
            sorted(
                value,
                key=lambda item: (
                    item.tier_id,
                    item.phase,
                    item.account_id or "",
                    item.subject_id.hex if item.subject_id else "",
                ),
            )
        )


__all__ = [
    "MAX_ASSIGNMENTS_PER_REPORT",
    "MAX_CONTRACT_BYTES",
    "MAX_DEMAND_BUCKETS_PER_REPORT",
    "MAX_DOMAINS_PER_POOL",
    "MAX_FIXED_CLAIMS_PER_REPORT",
    "MAX_NODES_PER_DOMAIN",
    "MAX_POOLS",
    "MAX_QUANTITY",
    "MAX_SHAPES_PER_PROFILE",
    "MAX_SUBJECTS",
    "SCHEMA_VERSION",
    "AccountPolicyV1",
    "AllocationInputV1",
    "CapacityContractError",
    "ClaimSlotMatchV1",
    "ConfigurationActivationV1",
    "ConfigurationGenerationRefV1",
    "ConfigurationSnapshotV1",
    "CurrentAssignmentV1",
    "DemandBucketV1",
    "DemandSnapshotV1",
    "DesiredShapeCountV1",
    "FairnessCursorV1",
    "FixedClaimV1",
    "FleetManifestV1",
    "InputFreshnessV1",
    "JointMatchingWitnessV1",
    "NodeEnvelopeV1",
    "NodeResidualV1",
    "ObservedCommitmentV1",
    "PackingPlacementV1",
    "PackingRequestV1",
    "PackingShapeRequestV1",
    "PackingWitnessV1",
    "PlacementAllowanceV1",
    "PoolAllocationInputV1",
    "PoolManifestV1",
    "PoolObservationV1",
    "ProfileReferenceV1",
    "RankedHypotheticalLaunchV1",
    "ResourceDomainV1",
    "ResourceVectorV1",
    "RolloutSurgePairingV1",
    "ShadowAllocationV1",
    "ShadowEpochV1",
    "StrictV1Model",
    "SubjectAllocationInputV1",
    "SubjectConfigurationV1",
    "TierPolicyV1",
    "WorkerShapeV1",
    "canonical_bytes",
    "canonical_digest",
    "canonical_digest_excluding",
    "checked_add",
    "checked_add_vectors",
    "checked_sum_vectors",
    "vector_fits",
]
