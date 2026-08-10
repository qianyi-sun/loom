"""ORM records for the independent global capacity management database."""

from __future__ import annotations

from datetime import datetime
from typing import Any
from uuid import UUID, uuid4

from sqlalchemy import (
    BigInteger,
    Boolean,
    CheckConstraint,
    ForeignKey,
    Integer,
    Text,
    UniqueConstraint,
    func,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB, TIMESTAMP
from sqlalchemy.dialects.postgresql import UUID as PgUUID  # noqa: N811
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    """Metadata root used only by the capacity management migration tree."""


class CapacityAuthorityState(Base):
    __tablename__ = "capacity_authority_state"
    __table_args__ = (
        CheckConstraint("singleton_id = 1", name="capacity_authority_singleton_check"),
        CheckConstraint("writer_epoch >= 0", name="capacity_authority_writer_epoch_check"),
        CheckConstraint("schema_version = 1", name="capacity_authority_schema_check"),
        CheckConstraint(
            "recovery_state = 'shadow'",
            name="capacity_authority_recovery_state_check",
        ),
        CheckConstraint(
            "executable_new_capacity_ceiling = 0",
            name="capacity_authority_shadow_only_check",
        ),
        CheckConstraint(
            "global_pending_slot_ceiling >= 0 AND global_pending_job_ceiling >= 0 "
            "AND global_submission_rate_ceiling >= 0",
            name="capacity_authority_limits_check",
        ),
    )

    singleton_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    authority_incarnation: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), nullable=False)
    writer_epoch: Mapped[int] = mapped_column(BigInteger, nullable=False, server_default=text("0"))
    schema_version: Mapped[int] = mapped_column(Integer, nullable=False, server_default=text("1"))
    recovery_state: Mapped[str] = mapped_column(
        Text, nullable=False, server_default=text("'shadow'")
    )
    increase_freeze: Mapped[bool] = mapped_column(
        Boolean, nullable=False, server_default=text("true")
    )
    increase_freeze_reason: Mapped[str | None] = mapped_column(Text)
    executable_new_capacity_ceiling: Mapped[int] = mapped_column(
        BigInteger, nullable=False, server_default=text("0")
    )
    global_pending_slot_ceiling: Mapped[int] = mapped_column(
        BigInteger, nullable=False, server_default=text("0")
    )
    global_pending_job_ceiling: Mapped[int] = mapped_column(
        BigInteger, nullable=False, server_default=text("0")
    )
    global_submission_rate_ceiling: Mapped[int] = mapped_column(
        BigInteger, nullable=False, server_default=text("0")
    )
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), nullable=False, server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), nullable=False, server_default=func.now()
    )


class CapacityConfigGeneration(Base):
    __tablename__ = "capacity_config_generations"
    __table_args__ = (
        CheckConstraint("scope IN ('fleet','subject')", name="capacity_config_scope_check"),
        CheckConstraint(
            "(scope = 'fleet' AND subject_id IS NULL AND subject_incarnation IS NULL) OR "
            "(scope = 'subject' AND subject_id IS NOT NULL "
            "AND subject_incarnation IS NOT NULL)",
            name="capacity_config_binding_check",
        ),
        CheckConstraint("scope_generation > 0", name="capacity_config_generation_check"),
        CheckConstraint("digest ~ '^[0-9a-f]{64}$'", name="capacity_config_digest_check"),
        CheckConstraint(
            "state IN ('proposed','active','retired')",
            name="capacity_config_state_check",
        ),
        UniqueConstraint(
            "scope",
            "subject_id",
            "subject_incarnation",
            "scope_generation",
            name="capacity_config_scope_generation_key",
            postgresql_nulls_not_distinct=True,
        ),
        UniqueConstraint("idempotency_key", name="capacity_config_idempotency_key"),
    )

    id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), primary_key=True, default=uuid4)
    scope: Mapped[str] = mapped_column(Text, nullable=False)
    subject_id: Mapped[UUID | None] = mapped_column(PgUUID(as_uuid=True))
    subject_incarnation: Mapped[UUID | None] = mapped_column(PgUUID(as_uuid=True))
    scope_generation: Mapped[int] = mapped_column(BigInteger, nullable=False)
    digest: Mapped[str] = mapped_column(Text, nullable=False)
    payload: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    state: Mapped[str] = mapped_column(Text, nullable=False, server_default=text("'proposed'"))
    actor: Mapped[str] = mapped_column(Text, nullable=False)
    idempotency_key: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), nullable=False, server_default=func.now()
    )


class CapacityConfigurationEpoch(Base):
    __tablename__ = "capacity_configuration_epochs"
    __table_args__ = (
        CheckConstraint("configuration_epoch > 0", name="capacity_configuration_epoch_check"),
        CheckConstraint(
            "fleet_generation > 0", name="capacity_configuration_fleet_generation_check"
        ),
        CheckConstraint(
            "fleet_digest ~ '^[0-9a-f]{64}$' "
            "AND canonical_digest ~ '^[0-9a-f]{64}$' "
            "AND activation_request_digest ~ '^[0-9a-f]{64}$'",
            name="capacity_configuration_digest_check",
        ),
    )

    configuration_epoch: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    fleet_generation: Mapped[int] = mapped_column(BigInteger, nullable=False)
    fleet_digest: Mapped[str] = mapped_column(Text, nullable=False)
    subject_generation_manifest: Mapped[list[dict[str, Any]]] = mapped_column(JSONB, nullable=False)
    canonical_digest: Mapped[str] = mapped_column(Text, nullable=False, unique=True)
    activation_idempotency_key: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True), nullable=False, unique=True
    )
    activation_actor: Mapped[str] = mapped_column(Text, nullable=False)
    activation_request_digest: Mapped[str] = mapped_column(Text, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), nullable=False, server_default=func.now()
    )


class CapacityTier(Base):
    __tablename__ = "capacity_tiers"
    __table_args__ = (
        CheckConstraint(
            "tier_id IN ('production','staging','development')",
            name="capacity_tier_name_check",
        ),
        CheckConstraint("priority BETWEEN 0 AND 2", name="capacity_tier_priority_check"),
        CheckConstraint(
            "max_slots >= 0 AND max_pending_slots >= 0 AND max_pending_jobs >= 0",
            name="capacity_tier_limits_check",
        ),
        UniqueConstraint("configuration_epoch", "tier_id", name="capacity_tier_epoch_id_key"),
        UniqueConstraint(
            "configuration_epoch",
            "priority",
            name="capacity_tier_epoch_priority_key",
        ),
    )

    id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), primary_key=True, default=uuid4)
    configuration_epoch: Mapped[int] = mapped_column(
        BigInteger,
        ForeignKey("capacity_configuration_epochs.configuration_epoch", ondelete="RESTRICT"),
        nullable=False,
    )
    tier_id: Mapped[str] = mapped_column(Text, nullable=False)
    priority: Mapped[int] = mapped_column(Integer, nullable=False)
    max_slots: Mapped[int] = mapped_column(BigInteger, nullable=False)
    resource_ceilings: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    max_pending_slots: Mapped[int] = mapped_column(BigInteger, nullable=False)
    max_pending_jobs: Mapped[int] = mapped_column(BigInteger, nullable=False)


class CapacityAccountPolicy(Base):
    __tablename__ = "capacity_account_policies"
    __table_args__ = (
        CheckConstraint(
            "kind IN ('service','owner','owner_template')",
            name="capacity_account_kind_check",
        ),
        CheckConstraint(
            "min_reservation_slots >= 0 AND max_slots >= min_reservation_slots "
            "AND max_surge_slots >= 0 AND max_pending_slots >= 0 "
            "AND max_pending_jobs >= 0 AND max_live_subjects > 0 "
            "AND max_builds >= 0 AND max_artifact_bytes >= 0",
            name="capacity_account_limits_check",
        ),
        UniqueConstraint(
            "configuration_epoch",
            "account_id",
            name="capacity_account_epoch_id_key",
        ),
    )

    id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), primary_key=True, default=uuid4)
    configuration_epoch: Mapped[int] = mapped_column(
        BigInteger,
        ForeignKey("capacity_configuration_epochs.configuration_epoch", ondelete="RESTRICT"),
        nullable=False,
    )
    account_id: Mapped[str] = mapped_column(Text, nullable=False)
    kind: Mapped[str] = mapped_column(Text, nullable=False)
    owner_id: Mapped[UUID | None] = mapped_column(PgUUID(as_uuid=True))
    min_reservation_slots: Mapped[int] = mapped_column(BigInteger, nullable=False)
    max_slots: Mapped[int] = mapped_column(BigInteger, nullable=False)
    max_surge_slots: Mapped[int] = mapped_column(BigInteger, nullable=False)
    max_pending_slots: Mapped[int] = mapped_column(BigInteger, nullable=False)
    max_pending_jobs: Mapped[int] = mapped_column(BigInteger, nullable=False)
    max_live_subjects: Mapped[int] = mapped_column(BigInteger, nullable=False)
    max_builds: Mapped[int] = mapped_column(BigInteger, nullable=False, server_default=text("0"))
    max_artifact_bytes: Mapped[int] = mapped_column(
        BigInteger, nullable=False, server_default=text("0")
    )
    payload: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)


class CapacityFairnessState(Base):
    __tablename__ = "capacity_fairness_state"
    __table_args__ = (
        CheckConstraint("mode = 'shadow'", name="capacity_fairness_state_shadow_only_check"),
        CheckConstraint(
            "scope IN ('tier_account','account_subject')",
            name="capacity_fairness_scope_check",
        ),
        CheckConstraint("phase IN ('minimum','demand')", name="capacity_fairness_phase_check"),
        CheckConstraint("last_shadow_epoch >= 0", name="capacity_fairness_epoch_check"),
        UniqueConstraint(
            "configuration_epoch",
            "scope",
            "phase",
            "tier_id",
            "account_id",
            "subject_id",
            name="capacity_fairness_scope_key",
        ),
    )

    id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), primary_key=True, default=uuid4)
    configuration_epoch: Mapped[int] = mapped_column(BigInteger, nullable=False)
    mode: Mapped[str] = mapped_column(Text, nullable=False, server_default=text("'shadow'"))
    scope: Mapped[str] = mapped_column(Text, nullable=False)
    phase: Mapped[str] = mapped_column(Text, nullable=False)
    tier_id: Mapped[str] = mapped_column(Text, nullable=False)
    account_id: Mapped[str | None] = mapped_column(Text)
    subject_id: Mapped[UUID | None] = mapped_column(PgUUID(as_uuid=True))
    cursor_id: Mapped[str | None] = mapped_column(Text)
    last_shadow_epoch: Mapped[int] = mapped_column(
        BigInteger, nullable=False, server_default=text("0")
    )


class CapacityPool(Base):
    __tablename__ = "capacity_pools"
    __table_args__ = (
        CheckConstraint("pool_generation > 0", name="capacity_pool_generation_check"),
        CheckConstraint(
            "pool_digest ~ '^[0-9a-f]{64}$' AND protocol_digest ~ '^[0-9a-f]{64}$'",
            name="capacity_pool_digest_check",
        ),
        CheckConstraint(
            "max_slots >= 0 AND max_pending_slots >= 0 "
            "AND max_pending_jobs >= 0 AND submission_rate_per_minute >= 0",
            name="capacity_pool_limits_check",
        ),
        UniqueConstraint(
            "configuration_epoch",
            "pool_id",
            "pool_generation",
            name="capacity_pool_generation_key",
        ),
    )

    id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), primary_key=True, default=uuid4)
    configuration_epoch: Mapped[int] = mapped_column(
        BigInteger,
        ForeignKey("capacity_configuration_epochs.configuration_epoch", ondelete="RESTRICT"),
        nullable=False,
    )
    pool_id: Mapped[str] = mapped_column(Text, nullable=False)
    pool_generation: Mapped[int] = mapped_column(BigInteger, nullable=False)
    pool_digest: Mapped[str] = mapped_column(Text, nullable=False)
    controller: Mapped[str] = mapped_column(Text, nullable=False)
    partition: Mapped[str] = mapped_column(Text, nullable=False)
    association: Mapped[str] = mapped_column(Text, nullable=False)
    protocol_generation: Mapped[int] = mapped_column(BigInteger, nullable=False)
    protocol_digest: Mapped[str] = mapped_column(Text, nullable=False)
    topology: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    envelope: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    health: Mapped[str] = mapped_column(Text, nullable=False)
    max_slots: Mapped[int] = mapped_column(BigInteger, nullable=False)
    max_pending_slots: Mapped[int] = mapped_column(BigInteger, nullable=False)
    max_pending_jobs: Mapped[int] = mapped_column(BigInteger, nullable=False)
    submission_rate_per_minute: Mapped[int] = mapped_column(BigInteger, nullable=False)


class CapacitySubject(Base):
    __tablename__ = "capacity_subjects"
    __table_args__ = (
        CheckConstraint(
            "tier_id IN ('production','staging','development')",
            name="capacity_subject_tier_check",
        ),
        CheckConstraint(
            "min_slots >= 0 AND max_slots >= min_slots AND rollout_surge_slots >= 0 "
            "AND max_pending_slots >= 0 AND max_pending_jobs >= 0",
            name="capacity_subject_limits_check",
        ),
        CheckConstraint(
            "candidate_generation > 0 AND deployment_generation > 0 "
            "AND configuration_generation > 0",
            name="capacity_subject_generations_check",
        ),
        UniqueConstraint(
            "configuration_epoch",
            "subject_id",
            "subject_incarnation",
            name="capacity_subject_epoch_binding_key",
        ),
    )

    id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), primary_key=True, default=uuid4)
    configuration_epoch: Mapped[int] = mapped_column(
        BigInteger,
        ForeignKey("capacity_configuration_epochs.configuration_epoch", ondelete="RESTRICT"),
        nullable=False,
    )
    subject_id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), nullable=False)
    subject_incarnation: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), nullable=False)
    display_name: Mapped[str] = mapped_column(Text, nullable=False)
    account_id: Mapped[str] = mapped_column(Text, nullable=False)
    tier_id: Mapped[str] = mapped_column(Text, nullable=False)
    min_slots: Mapped[int] = mapped_column(BigInteger, nullable=False, server_default=text("0"))
    max_slots: Mapped[int] = mapped_column(BigInteger, nullable=False)
    rollout_surge_slots: Mapped[int] = mapped_column(
        BigInteger, nullable=False, server_default=text("0")
    )
    max_pending_slots: Mapped[int] = mapped_column(BigInteger, nullable=False)
    max_pending_jobs: Mapped[int] = mapped_column(BigInteger, nullable=False)
    lifecycle_state: Mapped[str] = mapped_column(Text, nullable=False)
    candidate_generation: Mapped[int] = mapped_column(BigInteger, nullable=False)
    deployment_generation: Mapped[int] = mapped_column(BigInteger, nullable=False)
    configuration_generation: Mapped[int] = mapped_column(BigInteger, nullable=False)
    demand_reporter_incarnation: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), nullable=False)
    payload: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)


class CapacityCandidate(Base):
    __tablename__ = "capacity_candidates"
    __table_args__ = (
        CheckConstraint(
            "candidate_digest ~ '^[0-9a-f]{64}$'",
            name="capacity_candidate_digest_check",
        ),
        UniqueConstraint(
            "subject_id",
            "subject_incarnation",
            "candidate_generation",
            name="capacity_candidate_generation_key",
        ),
    )

    id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), primary_key=True, default=uuid4)
    subject_id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), nullable=False)
    subject_incarnation: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), nullable=False)
    candidate_generation: Mapped[int] = mapped_column(BigInteger, nullable=False)
    candidate_digest: Mapped[str] = mapped_column(Text, nullable=False)
    source_payload: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    artifact_payload: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    architecture_payload: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    launcher_payload: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    attestation_payload: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    protocol_payload: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)


class CapacityDeploymentGeneration(Base):
    __tablename__ = "capacity_deployment_generations"
    __table_args__ = (
        CheckConstraint("deployment_generation > 0", name="capacity_deployment_generation_check"),
        CheckConstraint(
            "candidate_digest ~ '^[0-9a-f]{64}$'",
            name="capacity_deployment_candidate_digest_check",
        ),
        UniqueConstraint(
            "subject_id",
            "subject_incarnation",
            "deployment_generation",
            name="capacity_deployment_binding_key",
        ),
    )

    id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), primary_key=True, default=uuid4)
    subject_id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), nullable=False)
    subject_incarnation: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), nullable=False)
    deployment_generation: Mapped[int] = mapped_column(BigInteger, nullable=False)
    candidate_digest: Mapped[str] = mapped_column(Text, nullable=False)
    required_profiles: Mapped[list[dict[str, Any]]] = mapped_column(JSONB, nullable=False)
    readiness_state: Mapped[str] = mapped_column(Text, nullable=False)
    lifecycle_state: Mapped[str] = mapped_column(Text, nullable=False)
    cutover_payload: Mapped[dict[str, Any] | None] = mapped_column(JSONB)


class CapacityWorkerProfile(Base):
    __tablename__ = "capacity_worker_profiles"
    __table_args__ = (
        CheckConstraint(
            "deployment_generation > 0 AND pool_generation > 0 AND profile_generation > 0",
            name="capacity_worker_profile_generations_check",
        ),
        CheckConstraint(
            "profile_digest ~ '^[0-9a-f]{64}$'",
            name="capacity_worker_profile_digest_check",
        ),
        UniqueConstraint(
            "subject_id",
            "subject_incarnation",
            "deployment_generation",
            "pool_id",
            "profile_generation",
            name="capacity_worker_profile_binding_key",
        ),
    )

    id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), primary_key=True, default=uuid4)
    subject_id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), nullable=False)
    subject_incarnation: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), nullable=False)
    deployment_generation: Mapped[int] = mapped_column(BigInteger, nullable=False)
    pool_id: Mapped[str] = mapped_column(Text, nullable=False)
    pool_generation: Mapped[int] = mapped_column(BigInteger, nullable=False)
    profile_generation: Mapped[int] = mapped_column(BigInteger, nullable=False)
    profile_digest: Mapped[str] = mapped_column(Text, nullable=False)
    shape_catalog: Mapped[list[dict[str, Any]]] = mapped_column(JSONB, nullable=False)
    narrowing_constraints: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)


class CapacityDemandReporter(Base):
    __tablename__ = "capacity_demand_reporters"
    __table_args__ = (
        CheckConstraint("high_water >= 0", name="capacity_demand_reporter_high_water_check"),
        CheckConstraint(
            "state IN ('current','fenced','equivocal')",
            name="capacity_demand_reporter_state_check",
        ),
        UniqueConstraint(
            "subject_id",
            "subject_incarnation",
            "reporter_incarnation",
            name="capacity_demand_reporter_binding_key",
        ),
    )

    id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), primary_key=True, default=uuid4)
    subject_id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), nullable=False)
    subject_incarnation: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), nullable=False)
    reporter_incarnation: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), nullable=False)
    configuration_generation: Mapped[int] = mapped_column(BigInteger, nullable=False)
    deployment_generation: Mapped[int] = mapped_column(BigInteger, nullable=False)
    high_water: Mapped[int] = mapped_column(BigInteger, nullable=False, server_default=text("0"))
    state: Mapped[str] = mapped_column(Text, nullable=False, server_default=text("'current'"))
    last_receipt_time: Mapped[datetime | None] = mapped_column(TIMESTAMP(timezone=True))
    last_digest: Mapped[str | None] = mapped_column(Text)


class CapacityDemandSnapshot(Base):
    __tablename__ = "capacity_demand_snapshots"
    __table_args__ = (
        CheckConstraint("sequence > 0", name="capacity_demand_snapshot_sequence_check"),
        CheckConstraint("digest ~ '^[0-9a-f]{64}$'", name="capacity_demand_snapshot_digest_check"),
        UniqueConstraint(
            "reporter_incarnation",
            "sequence",
            name="capacity_demand_snapshot_sequence_key",
        ),
    )

    id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), primary_key=True, default=uuid4)
    subject_id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), nullable=False)
    subject_incarnation: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), nullable=False)
    reporter_incarnation: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), nullable=False)
    sequence: Mapped[int] = mapped_column(BigInteger, nullable=False)
    digest: Mapped[str] = mapped_column(Text, nullable=False)
    payload: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    database_received_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), nullable=False, server_default=func.now()
    )
    validity: Mapped[str] = mapped_column(Text, nullable=False)
    acknowledged: Mapped[bool] = mapped_column(
        Boolean, nullable=False, server_default=text("false")
    )


class CapacityPoolReporter(Base):
    __tablename__ = "capacity_pool_reporters"
    __table_args__ = (
        CheckConstraint("high_water >= 0", name="capacity_pool_reporter_high_water_check"),
        CheckConstraint(
            "state IN ('current','fenced','equivocal')",
            name="capacity_pool_reporter_state_check",
        ),
        UniqueConstraint(
            "pool_id",
            "reporter_incarnation",
            name="capacity_pool_reporter_binding_key",
        ),
    )

    id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), primary_key=True, default=uuid4)
    pool_id: Mapped[str] = mapped_column(Text, nullable=False)
    reporter_incarnation: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), nullable=False)
    pool_generation: Mapped[int] = mapped_column(BigInteger, nullable=False)
    high_water: Mapped[int] = mapped_column(BigInteger, nullable=False, server_default=text("0"))
    state: Mapped[str] = mapped_column(Text, nullable=False, server_default=text("'current'"))
    last_receipt_time: Mapped[datetime | None] = mapped_column(TIMESTAMP(timezone=True))
    last_digest: Mapped[str | None] = mapped_column(Text)


class CapacityPoolObservation(Base):
    __tablename__ = "capacity_pool_observations"
    __table_args__ = (
        CheckConstraint("sequence > 0", name="capacity_pool_observation_sequence_check"),
        CheckConstraint(
            "digest ~ '^[0-9a-f]{64}$'",
            name="capacity_pool_observation_digest_check",
        ),
        UniqueConstraint(
            "reporter_incarnation",
            "sequence",
            name="capacity_pool_observation_sequence_key",
        ),
    )

    id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), primary_key=True, default=uuid4)
    pool_id: Mapped[str] = mapped_column(Text, nullable=False)
    reporter_incarnation: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), nullable=False)
    sequence: Mapped[int] = mapped_column(BigInteger, nullable=False)
    digest: Mapped[str] = mapped_column(Text, nullable=False)
    payload: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    database_received_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), nullable=False, server_default=func.now()
    )
    validity: Mapped[str] = mapped_column(Text, nullable=False)


class CapacityObservedCommitment(Base):
    __tablename__ = "capacity_observed_commitments"
    __table_args__ = (
        CheckConstraint("kind IN ('claim','physical')", name="capacity_commitment_kind_check"),
        CheckConstraint(
            "state IN ('proposed','accepted','pending','live','draining',"
            "'cancel-pending','submitting-unknown','observed','unknown','quarantined')",
            name="capacity_commitment_state_check",
        ),
        CheckConstraint(
            "profile_generation > 0 AND profile_digest ~ '^[0-9a-f]{64}$'",
            name="capacity_commitment_profile_binding_check",
        ),
        CheckConstraint(
            "(kind = 'claim' AND attempt_id IS NOT NULL AND concurrency_slots > 0) "
            "OR (kind = 'physical' AND attempt_id IS NULL AND concurrency_slots IS NULL)",
            name="capacity_commitment_kind_fields_check",
        ),
        UniqueConstraint(
            "kind",
            "commitment_identity",
            "source_incarnation",
            name="capacity_commitment_source_key",
        ),
    )

    id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), primary_key=True, default=uuid4)
    kind: Mapped[str] = mapped_column(Text, nullable=False)
    commitment_identity: Mapped[str] = mapped_column(Text, nullable=False)
    source_incarnation: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), nullable=False)
    subject_id: Mapped[UUID | None] = mapped_column(PgUUID(as_uuid=True))
    subject_incarnation: Mapped[UUID | None] = mapped_column(PgUUID(as_uuid=True))
    pool_id: Mapped[str] = mapped_column(Text, nullable=False)
    pool_generation: Mapped[int] = mapped_column(BigInteger, nullable=False)
    deployment_generation: Mapped[int | None] = mapped_column(BigInteger)
    profile_id: Mapped[str | None] = mapped_column(Text)
    profile_generation: Mapped[int] = mapped_column(BigInteger, nullable=False)
    profile_digest: Mapped[str] = mapped_column(Text, nullable=False)
    shape_id: Mapped[str | None] = mapped_column(Text)
    attempt_id: Mapped[str | None] = mapped_column(Text)
    concurrency_slots: Mapped[int | None] = mapped_column(BigInteger)
    binding_payload: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    resource_vector: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    state: Mapped[str] = mapped_column(Text, nullable=False)
    first_reporter_high_water: Mapped[int] = mapped_column(BigInteger, nullable=False)
    last_reporter_high_water: Mapped[int] = mapped_column(BigInteger, nullable=False)
    first_receipt_time: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), nullable=False)
    last_receipt_time: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), nullable=False)


class CapacityAllocationEpoch(Base):
    __tablename__ = "capacity_allocation_epochs"
    __table_args__ = (
        CheckConstraint("writer_epoch > 0", name="capacity_allocation_writer_epoch_check"),
        CheckConstraint(
            "configuration_epoch > 0",
            name="capacity_allocation_configuration_epoch_check",
        ),
        CheckConstraint(
            "input_digest ~ '^[0-9a-f]{64}$'",
            name="capacity_allocation_input_digest_check",
        ),
        CheckConstraint("status IN ('shadow','failed')", name="capacity_allocation_status_check"),
        CheckConstraint("executable = false", name="capacity_allocation_epoch_shadow_only_check"),
    )

    allocation_epoch: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    writer_epoch: Mapped[int] = mapped_column(BigInteger, nullable=False)
    configuration_epoch: Mapped[int] = mapped_column(BigInteger, nullable=False)
    input_digest: Mapped[str] = mapped_column(Text, nullable=False)
    status: Mapped[str] = mapped_column(Text, nullable=False)
    failure_reason: Mapped[str | None] = mapped_column(Text)
    complete_payload: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    executable: Mapped[bool] = mapped_column(Boolean, nullable=False, server_default=text("false"))
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), nullable=False, server_default=func.now()
    )
    committed_at: Mapped[datetime | None] = mapped_column(TIMESTAMP(timezone=True))


class CapacityAllocation(Base):
    __tablename__ = "capacity_allocations"
    __table_args__ = (
        CheckConstraint(
            "mode = 'shadow' AND executable = false",
            name="capacity_allocations_shadow_only_check",
        ),
        UniqueConstraint(
            "allocation_epoch",
            "subject_id",
            "pool_id",
            name="capacity_allocation_epoch_subject_pool_key",
        ),
    )

    id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), primary_key=True, default=uuid4)
    allocation_epoch: Mapped[int] = mapped_column(
        BigInteger,
        ForeignKey("capacity_allocation_epochs.allocation_epoch", ondelete="RESTRICT"),
        nullable=False,
    )
    subject_id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), nullable=False)
    subject_incarnation: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), nullable=False)
    deployment_generation: Mapped[int] = mapped_column(BigInteger, nullable=False)
    pool_id: Mapped[str] = mapped_column(Text, nullable=False)
    desired_shapes: Mapped[list[dict[str, Any]]] = mapped_column(JSONB, nullable=False)
    desired_resources: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    commitments: Mapped[list[dict[str, Any]]] = mapped_column(JSONB, nullable=False)
    drains: Mapped[list[dict[str, Any]]] = mapped_column(JSONB, nullable=False)
    allowances: Mapped[list[dict[str, Any]]] = mapped_column(JSONB, nullable=False)
    witness: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    mode: Mapped[str] = mapped_column(Text, nullable=False, server_default=text("'shadow'"))
    executable: Mapped[bool] = mapped_column(Boolean, nullable=False, server_default=text("false"))


class CapacityAuditEvent(Base):
    __tablename__ = "capacity_audit_events"
    __table_args__ = (
        CheckConstraint("actor_kind <> ''", name="capacity_audit_actor_kind_check"),
        CheckConstraint("event_kind <> ''", name="capacity_audit_event_kind_check"),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    actor_kind: Mapped[str] = mapped_column(Text, nullable=False)
    actor_id: Mapped[str] = mapped_column(Text, nullable=False)
    event_kind: Mapped[str] = mapped_column(Text, nullable=False)
    object_binding: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    detail: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), nullable=False, server_default=func.now()
    )


__all__ = [
    "Base",
    "CapacityAccountPolicy",
    "CapacityAllocation",
    "CapacityAllocationEpoch",
    "CapacityAuditEvent",
    "CapacityAuthorityState",
    "CapacityCandidate",
    "CapacityConfigGeneration",
    "CapacityConfigurationEpoch",
    "CapacityDemandReporter",
    "CapacityDemandSnapshot",
    "CapacityDeploymentGeneration",
    "CapacityFairnessState",
    "CapacityObservedCommitment",
    "CapacityPool",
    "CapacityPoolObservation",
    "CapacityPoolReporter",
    "CapacitySubject",
    "CapacityTier",
    "CapacityWorkerProfile",
]
