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
    ForeignKeyConstraint,
    Index,
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
            "(execution_state = 'shadow' AND execution_epoch = 0 "
            "AND execution_manifest_sha256 IS NULL "
            "AND executable_new_capacity_ceiling = 0) OR "
            "(execution_state = 'prepared' AND execution_epoch > 0 "
            "AND execution_manifest_sha256 IS NOT NULL "
            "AND execution_manifest_sha256 ~ '^[0-9a-f]{64}$' "
            "AND executable_new_capacity_ceiling = 0) OR "
            "(execution_state = 'active' AND execution_epoch > 0 "
            "AND execution_manifest_sha256 IS NOT NULL "
            "AND execution_manifest_sha256 ~ '^[0-9a-f]{64}$' "
            "AND executable_new_capacity_ceiling > 0) OR "
            "(execution_state = 'drain-only' AND execution_epoch > 0 "
            "AND execution_manifest_sha256 IS NOT NULL "
            "AND execution_manifest_sha256 ~ '^[0-9a-f]{64}$' "
            "AND executable_new_capacity_ceiling = 0)",
            name="capacity_authority_execution_check",
        ),
        CheckConstraint(
            "global_pending_slot_ceiling >= 0 AND global_pending_job_ceiling >= 0 "
            "AND global_submission_rate_ceiling BETWEEN 0 AND 9223372036854",
            name="capacity_authority_limits_check",
        ),
        ForeignKeyConstraint(
            (
                "authority_incarnation",
                "writer_epoch",
                "execution_epoch",
                "execution_manifest_sha256",
                "execution_state",
                "executable_new_capacity_ceiling",
            ),
            (
                "capacity_execution_epochs.authority_incarnation",
                "capacity_execution_epochs.current_writer_epoch",
                "capacity_execution_epochs.execution_epoch",
                "capacity_execution_epochs.execution_manifest_sha256",
                "capacity_execution_epochs.state",
                "capacity_execution_epochs.effective_ceiling",
            ),
            name="capacity_authority_execution_epoch_fkey",
            ondelete="RESTRICT",
            deferrable=True,
            initially="DEFERRED",
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
    execution_epoch: Mapped[int] = mapped_column(
        BigInteger, nullable=False, server_default=text("0")
    )
    execution_state: Mapped[str] = mapped_column(
        Text, nullable=False, server_default=text("'shadow'")
    )
    execution_manifest_sha256: Mapped[str | None] = mapped_column(Text)
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


class CapacityExecutionEpoch(Base):
    __tablename__ = "capacity_execution_epochs"
    __table_args__ = (
        CheckConstraint(
            "execution_epoch > 0 AND prepared_writer_epoch > 0 "
            "AND current_writer_epoch > 0 "
            "AND configuration_epoch > 0 AND fleet_generation > 0 "
            "AND oldlab_pool_generation > 0 AND gb10_pool_generation > 0 "
            "AND requested_ceiling = 1 AND effective_ceiling >= 0 "
            "AND effective_ceiling <= requested_ceiling "
            "AND requested_rate_per_minute > 0 AND effective_rate_per_minute >= 0 "
            "AND effective_rate_per_minute <= requested_rate_per_minute",
            name="capacity_execution_epoch_quantity_check",
        ),
        CheckConstraint(
            "fleet_digest ~ '^[0-9a-f]{64}$' "
            "AND execution_manifest_sha256 ~ '^[0-9a-f]{64}$' "
            "AND trusted_fleet_release_sha256 ~ '^[0-9a-f]{64}$' "
            "AND oldlab_signing_key_sha256 ~ '^[0-9a-f]{64}$' "
            "AND oldlab_local_authority_sha256 ~ '^[0-9a-f]{64}$' "
            "AND oldlab_controller_authority_sha256 ~ '^[0-9a-f]{64}$' "
            "AND gb10_signing_key_sha256 ~ '^[0-9a-f]{64}$' "
            "AND gb10_local_authority_sha256 ~ '^[0-9a-f]{64}$' "
            "AND gb10_controller_authority_sha256 ~ '^[0-9a-f]{64}$' "
            "AND environment_acknowledgements_sha256 ~ '^[0-9a-f]{64}$' "
            "AND legacy_writer_manifest_sha256 ~ '^[0-9a-f]{64}$' "
            "AND rollback_evidence_sha256 ~ '^[0-9a-f]{64}$' "
            "AND request_digest ~ '^[0-9a-f]{64}$' "
            "AND (activation_request_digest IS NULL OR "
            "activation_request_digest ~ '^[0-9a-f]{64}$')",
            name="capacity_execution_epoch_digest_check",
        ),
        CheckConstraint(
            "oldlab_pool_id = 'oldlab' AND gb10_pool_id = 'gb10' "
            "AND oldlab_executor_id <> gb10_executor_id "
            "AND oldlab_executor_incarnation <> gb10_executor_incarnation",
            name="capacity_execution_epoch_pool_check",
        ),
        CheckConstraint(
            "jsonb_typeof(manifest_payload) = 'object' "
            "AND octet_length(manifest_payload::text) <= 8388608",
            name="capacity_execution_epoch_manifest_check",
        ),
        CheckConstraint(
            "state IN ('prepared','active','drain-only','retired')",
            name="capacity_execution_epoch_state_check",
        ),
        CheckConstraint(
            "(state = 'prepared' AND effective_ceiling = 0 "
            "AND effective_rate_per_minute = 0 "
            "AND activation_actor IS NULL AND activation_idempotency_key IS NULL "
            "AND activation_request_digest IS NULL AND activated_at IS NULL "
            "AND drain_only_at IS NULL AND retired_at IS NULL) OR "
            "(state = 'active' AND effective_ceiling > 0 "
            "AND effective_rate_per_minute > 0 "
            "AND activation_actor IS NOT NULL AND activation_idempotency_key IS NOT NULL "
            "AND activation_request_digest IS NOT NULL AND activated_at IS NOT NULL "
            "AND drain_only_at IS NULL AND retired_at IS NULL) OR "
            "(state = 'drain-only' AND effective_ceiling = 0 "
            "AND effective_rate_per_minute = 0 "
            "AND activation_actor IS NOT NULL AND activation_idempotency_key IS NOT NULL "
            "AND activation_request_digest IS NOT NULL AND activated_at IS NOT NULL "
            "AND drain_only_at IS NOT NULL "
            "AND retired_at IS NULL) OR "
            "(state = 'retired' AND effective_ceiling = 0 "
            "AND effective_rate_per_minute = 0 AND retired_at IS NOT NULL)",
            name="capacity_execution_epoch_state_time_check",
        ),
        ForeignKeyConstraint(
            ("configuration_epoch", "oldlab_pool_id", "oldlab_pool_generation"),
            (
                "capacity_pools.configuration_epoch",
                "capacity_pools.pool_id",
                "capacity_pools.pool_generation",
            ),
            name="capacity_execution_epoch_oldlab_pool_fkey",
            ondelete="RESTRICT",
        ),
        ForeignKeyConstraint(
            ("configuration_epoch", "gb10_pool_id", "gb10_pool_generation"),
            (
                "capacity_pools.configuration_epoch",
                "capacity_pools.pool_id",
                "capacity_pools.pool_generation",
            ),
            name="capacity_execution_epoch_gb10_pool_fkey",
            ondelete="RESTRICT",
        ),
        UniqueConstraint(
            "execution_epoch",
            "execution_manifest_sha256",
            name="capacity_execution_epoch_manifest_key",
        ),
        UniqueConstraint(
            "activation_idempotency_key",
            name="capacity_execution_epoch_activation_idempotency_key",
        ),
        UniqueConstraint("idempotency_key", name="capacity_execution_epoch_idempotency_key"),
        UniqueConstraint(
            "authority_incarnation",
            "current_writer_epoch",
            "execution_epoch",
            "execution_manifest_sha256",
            "state",
            "effective_ceiling",
            name="capacity_execution_epoch_complete_authority_binding_key",
        ),
    )

    execution_epoch: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    authority_incarnation: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), nullable=False)
    prepared_writer_epoch: Mapped[int] = mapped_column(BigInteger, nullable=False)
    current_writer_epoch: Mapped[int] = mapped_column(BigInteger, nullable=False)
    configuration_epoch: Mapped[int] = mapped_column(
        BigInteger,
        ForeignKey("capacity_configuration_epochs.configuration_epoch", ondelete="RESTRICT"),
        nullable=False,
    )
    fleet_generation: Mapped[int] = mapped_column(BigInteger, nullable=False)
    fleet_digest: Mapped[str] = mapped_column(Text, nullable=False)
    execution_manifest_sha256: Mapped[str] = mapped_column(Text, nullable=False)
    manifest_payload: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    trusted_fleet_release_sha256: Mapped[str] = mapped_column(Text, nullable=False)
    oldlab_executor_id: Mapped[str] = mapped_column(Text, nullable=False)
    oldlab_executor_incarnation: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), nullable=False)
    oldlab_pool_id: Mapped[str] = mapped_column(Text, nullable=False)
    oldlab_pool_generation: Mapped[int] = mapped_column(BigInteger, nullable=False)
    oldlab_signing_key_sha256: Mapped[str] = mapped_column(Text, nullable=False)
    oldlab_local_authority_sha256: Mapped[str] = mapped_column(Text, nullable=False)
    oldlab_controller_authority_sha256: Mapped[str] = mapped_column(Text, nullable=False)
    gb10_executor_id: Mapped[str] = mapped_column(Text, nullable=False)
    gb10_executor_incarnation: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), nullable=False)
    gb10_pool_id: Mapped[str] = mapped_column(Text, nullable=False)
    gb10_pool_generation: Mapped[int] = mapped_column(BigInteger, nullable=False)
    gb10_signing_key_sha256: Mapped[str] = mapped_column(Text, nullable=False)
    gb10_local_authority_sha256: Mapped[str] = mapped_column(Text, nullable=False)
    gb10_controller_authority_sha256: Mapped[str] = mapped_column(Text, nullable=False)
    environment_acknowledgements_sha256: Mapped[str] = mapped_column(Text, nullable=False)
    legacy_writer_manifest_sha256: Mapped[str] = mapped_column(Text, nullable=False)
    rollback_evidence_sha256: Mapped[str] = mapped_column(Text, nullable=False)
    requested_ceiling: Mapped[int] = mapped_column(BigInteger, nullable=False)
    effective_ceiling: Mapped[int] = mapped_column(BigInteger, nullable=False)
    requested_rate_per_minute: Mapped[int] = mapped_column(BigInteger, nullable=False)
    effective_rate_per_minute: Mapped[int] = mapped_column(BigInteger, nullable=False)
    state: Mapped[str] = mapped_column(Text, nullable=False)
    actor: Mapped[str] = mapped_column(Text, nullable=False)
    idempotency_key: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), nullable=False)
    request_digest: Mapped[str] = mapped_column(Text, nullable=False)
    activation_actor: Mapped[str | None] = mapped_column(Text)
    activation_idempotency_key: Mapped[UUID | None] = mapped_column(PgUUID(as_uuid=True))
    activation_request_digest: Mapped[str | None] = mapped_column(Text)
    prepared_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), nullable=False, server_default=func.now()
    )
    activated_at: Mapped[datetime | None] = mapped_column(TIMESTAMP(timezone=True))
    drain_only_at: Mapped[datetime | None] = mapped_column(TIMESTAMP(timezone=True))
    retired_at: Mapped[datetime | None] = mapped_column(TIMESTAMP(timezone=True))


class CapacityExecutionExecutor(Base):
    __tablename__ = "capacity_execution_executors"
    __table_args__ = (
        CheckConstraint(
            "execution_epoch > 0 AND pool_generation > 0 AND pool_id IN ('gb10','oldlab')",
            name="capacity_execution_executor_binding_check",
        ),
        CheckConstraint(
            "execution_manifest_sha256 ~ '^[0-9a-f]{64}$' "
            "AND signing_key_sha256 ~ '^[0-9a-f]{64}$' "
            "AND local_authority_sha256 ~ '^[0-9a-f]{64}$' "
            "AND controller_authority_sha256 ~ '^[0-9a-f]{64}$' "
            "AND registration_digest ~ '^[0-9a-f]{64}$'",
            name="capacity_execution_executor_digest_check",
        ),
        ForeignKeyConstraint(
            ("execution_epoch", "execution_manifest_sha256"),
            (
                "capacity_execution_epochs.execution_epoch",
                "capacity_execution_epochs.execution_manifest_sha256",
            ),
            name="capacity_execution_executor_epoch_manifest_fkey",
            ondelete="RESTRICT",
        ),
        UniqueConstraint("execution_epoch", "pool_id", name="capacity_execution_executor_pool_key"),
        UniqueConstraint(
            "executor_incarnation", name="capacity_execution_executor_incarnation_key"
        ),
        UniqueConstraint("idempotency_key", name="capacity_execution_executor_idempotency_key"),
    )

    id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), primary_key=True, default=uuid4)
    execution_epoch: Mapped[int] = mapped_column(BigInteger, nullable=False)
    execution_manifest_sha256: Mapped[str] = mapped_column(Text, nullable=False)
    executor_id: Mapped[str] = mapped_column(Text, nullable=False)
    executor_incarnation: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), nullable=False)
    pool_id: Mapped[str] = mapped_column(Text, nullable=False)
    pool_generation: Mapped[int] = mapped_column(BigInteger, nullable=False)
    signing_key_id: Mapped[str] = mapped_column(Text, nullable=False)
    signing_key_sha256: Mapped[str] = mapped_column(Text, nullable=False)
    local_authority_sha256: Mapped[str] = mapped_column(Text, nullable=False)
    controller_authority_sha256: Mapped[str] = mapped_column(Text, nullable=False)
    actor: Mapped[str] = mapped_column(Text, nullable=False)
    idempotency_key: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), nullable=False)
    registration_digest: Mapped[str] = mapped_column(Text, nullable=False)
    registration_payload: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    registered_at: Mapped[datetime] = mapped_column(
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
            "AND max_pending_jobs >= 0 "
            "AND submission_rate_per_minute BETWEEN 0 AND 9223372036854 "
            "AND max_live_subjects > 0 "
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
    submission_rate_per_minute: Mapped[int] = mapped_column(
        BigInteger, nullable=False, server_default=text("0")
    )
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
            "AND max_pending_jobs >= 0 "
            "AND submission_rate_per_minute BETWEEN 0 AND 9223372036854",
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
            "AND max_pending_slots >= 0 AND max_pending_jobs >= 0 "
            "AND submission_rate_per_minute BETWEEN 0 AND 9223372036854",
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
    submission_rate_per_minute: Mapped[int] = mapped_column(
        BigInteger, nullable=False, server_default=text("0")
    )
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
        CheckConstraint(
            "(candidate_identity_algorithm = 'git-sha1' "
            "AND candidate_identity ~ '^[0-9a-f]{40}$') OR "
            "(candidate_identity_algorithm = 'source-sha256' "
            "AND candidate_identity ~ '^[0-9a-f]{64}$')",
            name="capacity_candidate_identity_check",
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
    candidate_identity_algorithm: Mapped[str] = mapped_column(Text, nullable=False)
    candidate_identity: Mapped[str] = mapped_column(Text, nullable=False)
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
        CheckConstraint(
            "token_sha256 IS NULL OR token_sha256 ~ '^[0-9a-f]{64}$'",
            name="capacity_demand_reporter_token_digest_check",
        ),
        UniqueConstraint(
            "token_sha256",
            name="capacity_demand_reporter_token_key",
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
    token_sha256: Mapped[str | None] = mapped_column(Text)


class CapacityDevelopmentProjection(Base):
    """Idempotency and evidence binding for one lifecycle-owned projection."""

    __tablename__ = "capacity_development_projections"
    __table_args__ = (
        CheckConstraint(
            "request_digest ~ '^[0-9a-f]{64}$' AND result_digest ~ '^[0-9a-f]{64}$'",
            name="capacity_development_projection_digest_check",
        ),
        CheckConstraint(
            "configuration_epoch > 0 AND configuration_generation > 0",
            name="capacity_development_projection_generation_check",
        ),
        UniqueConstraint("operation_id", name="capacity_development_projection_operation_key"),
        UniqueConstraint(
            "idempotency_key",
            name="capacity_development_projection_idempotency_key",
        ),
    )

    id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), primary_key=True, default=uuid4)
    operation_id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), nullable=False)
    idempotency_key: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), nullable=False)
    request_digest: Mapped[str] = mapped_column(Text, nullable=False)
    request_payload: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    subject_id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), nullable=False)
    subject_incarnation: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), nullable=False)
    configuration_generation: Mapped[int] = mapped_column(BigInteger, nullable=False)
    configuration_epoch: Mapped[int] = mapped_column(
        BigInteger,
        ForeignKey("capacity_configuration_epochs.configuration_epoch", ondelete="RESTRICT"),
        nullable=False,
    )
    result_digest: Mapped[str] = mapped_column(Text, nullable=False)
    result_payload: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    actor: Mapped[str] = mapped_column(Text, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), nullable=False, server_default=func.now()
    )


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
            "(profile_generation IS NULL AND profile_digest IS NULL) OR "
            "(profile_generation > 0 AND profile_digest ~ '^[0-9a-f]{64}$')",
            name="capacity_commitment_profile_binding_check",
        ),
        CheckConstraint(
            "(subject_id IS NOT NULL AND subject_incarnation IS NOT NULL "
            "AND deployment_generation > 0 AND profile_id IS NOT NULL "
            "AND profile_generation > 0 AND profile_digest IS NOT NULL) OR "
            "(subject_id IS NULL AND subject_incarnation IS NULL "
            "AND deployment_generation IS NULL AND profile_id IS NULL "
            "AND profile_generation IS NULL AND profile_digest IS NULL "
            "AND shape_id IS NULL AND kind = 'physical' "
            "AND state IN ('unknown','quarantined'))",
            name="capacity_commitment_attribution_check",
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
    profile_generation: Mapped[int | None] = mapped_column(BigInteger)
    profile_digest: Mapped[str | None] = mapped_column(Text)
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
        CheckConstraint(
            "status IN ('shadow','failed','executable')",
            name="capacity_allocation_status_check",
        ),
        CheckConstraint(
            "(status IN ('shadow','failed') AND executable = false "
            "AND execution_epoch IS NULL AND execution_manifest_sha256 IS NULL "
            "AND sealed = true AND allocation_count IS NULL) OR "
            "(status = 'executable' AND executable = true "
            "AND execution_epoch IS NOT NULL AND execution_manifest_sha256 IS NOT NULL "
            "AND allocation_count IS NOT NULL AND allocation_count >= 0 "
            "AND COALESCE(jsonb_typeof(complete_payload -> 'allocations') = 'array', false) "
            "AND COALESCE(jsonb_array_length(complete_payload -> 'allocations') "
            "= allocation_count, false))",
            name="capacity_allocation_epoch_mode_check",
        ),
        ForeignKeyConstraint(
            ("execution_epoch", "execution_manifest_sha256"),
            (
                "capacity_execution_epochs.execution_epoch",
                "capacity_execution_epochs.execution_manifest_sha256",
            ),
            name="capacity_allocation_epoch_execution_fkey",
            ondelete="RESTRICT",
        ),
        UniqueConstraint(
            "allocation_epoch",
            "execution_epoch",
            "execution_manifest_sha256",
            name="capacity_allocation_epoch_execution_binding_key",
        ),
    )

    allocation_epoch: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    writer_epoch: Mapped[int] = mapped_column(BigInteger, nullable=False)
    configuration_epoch: Mapped[int] = mapped_column(
        BigInteger,
        ForeignKey(
            "capacity_configuration_epochs.configuration_epoch",
            ondelete="RESTRICT",
        ),
        nullable=False,
    )
    input_digest: Mapped[str] = mapped_column(Text, nullable=False)
    status: Mapped[str] = mapped_column(Text, nullable=False)
    failure_reason: Mapped[str | None] = mapped_column(Text)
    complete_payload: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    executable: Mapped[bool] = mapped_column(Boolean, nullable=False, server_default=text("false"))
    execution_epoch: Mapped[int | None] = mapped_column(BigInteger)
    execution_manifest_sha256: Mapped[str | None] = mapped_column(Text)
    sealed: Mapped[bool] = mapped_column(Boolean, nullable=False, server_default=text("true"))
    allocation_count: Mapped[int | None] = mapped_column(BigInteger)
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), nullable=False, server_default=func.now()
    )
    committed_at: Mapped[datetime | None] = mapped_column(TIMESTAMP(timezone=True))


class CapacityAllocation(Base):
    __tablename__ = "capacity_allocations"
    __table_args__ = (
        CheckConstraint(
            "(mode = 'shadow' AND executable = false "
            "AND execution_epoch IS NULL AND execution_manifest_sha256 IS NULL) OR "
            "(mode = 'executable' AND executable = true "
            "AND execution_epoch IS NOT NULL AND execution_manifest_sha256 IS NOT NULL)",
            name="capacity_allocations_mode_check",
        ),
        ForeignKeyConstraint(
            ("allocation_epoch", "execution_epoch", "execution_manifest_sha256"),
            (
                "capacity_allocation_epochs.allocation_epoch",
                "capacity_allocation_epochs.execution_epoch",
                "capacity_allocation_epochs.execution_manifest_sha256",
            ),
            name="capacity_allocation_execution_binding_fkey",
            ondelete="RESTRICT",
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
    execution_epoch: Mapped[int | None] = mapped_column(BigInteger)
    execution_manifest_sha256: Mapped[str | None] = mapped_column(Text)


class CapacityExecutor(Base):
    __tablename__ = "capacity_executors"
    __table_args__ = (
        CheckConstraint(
            "pool_generation > 0 AND registered_writer_epoch > 0 "
            "AND command_high_water >= 0 AND heartbeat_high_water >= 0 "
            "AND journal_high_water >= 0 AND inventory_high_water >= 0",
            name="capacity_executor_epoch_check",
        ),
        CheckConstraint(
            "state IN ('dry-run','fenced','equivocal')",
            name="capacity_executor_state_check",
        ),
        CheckConstraint(
            "signing_key_sha256 ~ '^[0-9a-f]{64}$' "
            "AND local_authority_sha256 ~ '^[0-9a-f]{64}$' "
            "AND registration_digest ~ '^[0-9a-f]{64}$' "
            "AND ((command_high_water = 0 AND last_command_digest IS NULL) "
            "OR (command_high_water > 0 "
            "AND last_command_digest ~ '^[0-9a-f]{64}$')) "
            "AND ((heartbeat_high_water = 0 AND last_heartbeat_digest IS NULL) "
            "OR (heartbeat_high_water > 0 "
            "AND last_heartbeat_digest ~ '^[0-9a-f]{64}$')) "
            "AND ((journal_high_water = 0 AND (journal_digest IS NULL "
            "OR journal_digest = repeat('0', 64))) OR (journal_high_water > 0 "
            "AND journal_digest ~ '^[0-9a-f]{64}$' "
            "AND journal_digest <> repeat('0', 64))) "
            "AND ((inventory_high_water = 0 AND last_inventory_digest IS NULL) "
            "OR (inventory_high_water > 0 "
            "AND last_inventory_digest ~ '^[0-9a-f]{64}$'))",
            name="capacity_executor_digest_check",
        ),
        UniqueConstraint(
            "executor_incarnation",
            name="capacity_executor_incarnation_key",
        ),
        UniqueConstraint(
            "executor_id",
            "executor_incarnation",
            "pool_id",
            "pool_generation",
            name="capacity_executor_exact_binding_key",
        ),
        UniqueConstraint(
            "id",
            "executor_incarnation",
            "pool_id",
            "pool_generation",
            name="capacity_executor_observation_binding_key",
        ),
        UniqueConstraint(
            "registration_idempotency_key",
            name="capacity_executor_registration_idempotency_key",
        ),
        Index(
            "capacity_executor_one_current_per_pool_idx",
            "pool_id",
            unique=True,
            postgresql_where=text("state = 'dry-run'"),
        ),
    )

    id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), primary_key=True, default=uuid4)
    executor_id: Mapped[str] = mapped_column(Text, nullable=False)
    executor_incarnation: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), nullable=False)
    pool_id: Mapped[str] = mapped_column(Text, nullable=False)
    pool_generation: Mapped[int] = mapped_column(BigInteger, nullable=False)
    authority_incarnation: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), nullable=False)
    registered_writer_epoch: Mapped[int] = mapped_column(BigInteger, nullable=False)
    signing_key_id: Mapped[str] = mapped_column(Text, nullable=False)
    signing_key_sha256: Mapped[str] = mapped_column(Text, nullable=False)
    local_authority_sha256: Mapped[str] = mapped_column(Text, nullable=False)
    registration_actor: Mapped[str] = mapped_column(Text, nullable=False)
    registration_idempotency_key: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), nullable=False)
    registration_digest: Mapped[str] = mapped_column(Text, nullable=False)
    state: Mapped[str] = mapped_column(Text, nullable=False, server_default=text("'dry-run'"))
    command_high_water: Mapped[int] = mapped_column(
        BigInteger, nullable=False, server_default=text("0")
    )
    last_command_digest: Mapped[str | None] = mapped_column(Text)
    heartbeat_high_water: Mapped[int] = mapped_column(
        BigInteger, nullable=False, server_default=text("0")
    )
    last_heartbeat_digest: Mapped[str | None] = mapped_column(Text)
    journal_high_water: Mapped[int] = mapped_column(
        BigInteger, nullable=False, server_default=text("0")
    )
    journal_digest: Mapped[str | None] = mapped_column(Text)
    inventory_high_water: Mapped[int] = mapped_column(
        BigInteger, nullable=False, server_default=text("0")
    )
    last_inventory_digest: Mapped[str | None] = mapped_column(Text)
    lease_expires_at: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), nullable=False)
    last_heartbeat_at: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), nullable=False, server_default=func.now()
    )


class CapacityExecutorObservation(Base):
    __tablename__ = "capacity_executor_observations"
    __table_args__ = (
        CheckConstraint(
            "inventory_sequence > 0 AND journal_sequence >= 0",
            name="capacity_executor_observation_sequence_check",
        ),
        CheckConstraint(
            "inventory_digest ~ '^[0-9a-f]{64}$' AND journal_digest ~ '^[0-9a-f]{64}$'",
            name="capacity_executor_observation_digest_check",
        ),
        CheckConstraint(
            "authenticated_count >= 0 AND quarantined_count >= 0 AND foreign_count >= 0",
            name="capacity_executor_observation_count_check",
        ),
        CheckConstraint(
            "validity IN ('valid','quarantined')",
            name="capacity_executor_observation_validity_check",
        ),
        CheckConstraint(
            "executable = false",
            name="capacity_executor_observation_dry_run_only_check",
        ),
        UniqueConstraint(
            "executor_incarnation",
            "inventory_sequence",
            name="capacity_executor_observation_sequence_key",
        ),
        ForeignKeyConstraint(
            ("executor_row_id", "executor_incarnation", "pool_id", "pool_generation"),
            (
                "capacity_executors.id",
                "capacity_executors.executor_incarnation",
                "capacity_executors.pool_id",
                "capacity_executors.pool_generation",
            ),
            name="capacity_executor_observation_binding_fkey",
            ondelete="RESTRICT",
        ),
    )

    id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), primary_key=True, default=uuid4)
    executor_row_id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True),
        nullable=False,
    )
    executor_incarnation: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), nullable=False)
    pool_id: Mapped[str] = mapped_column(Text, nullable=False)
    pool_generation: Mapped[int] = mapped_column(BigInteger, nullable=False)
    inventory_sequence: Mapped[int] = mapped_column(BigInteger, nullable=False)
    inventory_digest: Mapped[str] = mapped_column(Text, nullable=False)
    journal_sequence: Mapped[int] = mapped_column(BigInteger, nullable=False)
    journal_digest: Mapped[str] = mapped_column(Text, nullable=False)
    authenticated_count: Mapped[int] = mapped_column(BigInteger, nullable=False)
    quarantined_count: Mapped[int] = mapped_column(BigInteger, nullable=False)
    foreign_count: Mapped[int] = mapped_column(BigInteger, nullable=False)
    payload: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    classification_payload: Mapped[list[dict[str, Any]]] = mapped_column(JSONB, nullable=False)
    validity: Mapped[str] = mapped_column(Text, nullable=False)
    executable: Mapped[bool] = mapped_column(Boolean, nullable=False, server_default=text("false"))
    database_received_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), nullable=False, server_default=func.now()
    )


class CapacityReservationTranche(Base):
    __tablename__ = "capacity_reservation_tranches"
    __table_args__ = (
        CheckConstraint(
            "writer_epoch > 0 AND configuration_epoch > 0 "
            "AND allocation_epoch > 0 AND pool_generation > 0 "
            "AND deployment_generation > 0 AND candidate_generation > 0",
            name="capacity_reservation_tranche_epoch_check",
        ),
        CheckConstraint(
            "proposal_digest ~ '^[0-9a-f]{64}$'",
            name="capacity_reservation_tranche_digest_check",
        ),
        CheckConstraint(
            "state IN ('proposed','accepted','closed')",
            name="capacity_reservation_tranche_state_check",
        ),
        CheckConstraint(
            "tier_id IN ('production','staging','development')",
            name="capacity_reservation_tranche_tier_check",
        ),
        CheckConstraint(
            "executable = false",
            name="capacity_reservation_tranche_dry_run_only_check",
        ),
        CheckConstraint(
            "(state = 'proposed' AND accepted_at IS NULL AND closed_at IS NULL) OR "
            "(state = 'accepted' AND accepted_at IS NOT NULL AND closed_at IS NULL) OR "
            "(state = 'closed' AND closed_at IS NOT NULL)",
            name="capacity_reservation_tranche_state_time_check",
        ),
        CheckConstraint(
            "(state <> 'closed' AND closure_reason IS NULL) OR "
            "(state = 'closed' AND closure_reason IN "
            "('proposal-expired','proposal-superseded','fully-released'))",
            name="capacity_reservation_tranche_closure_check",
        ),
        UniqueConstraint(
            "idempotency_key",
            name="capacity_reservation_tranche_idempotency_key",
        ),
        ForeignKeyConstraint(
            ("executor_id", "executor_incarnation", "pool_id", "pool_generation"),
            (
                "capacity_executors.executor_id",
                "capacity_executors.executor_incarnation",
                "capacity_executors.pool_id",
                "capacity_executors.pool_generation",
            ),
            name="capacity_reservation_tranche_executor_binding_fkey",
            ondelete="RESTRICT",
        ),
    )

    id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), primary_key=True)
    idempotency_key: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), nullable=False)
    authority_incarnation: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), nullable=False)
    writer_epoch: Mapped[int] = mapped_column(BigInteger, nullable=False)
    configuration_epoch: Mapped[int] = mapped_column(
        BigInteger,
        ForeignKey(
            "capacity_configuration_epochs.configuration_epoch",
            ondelete="RESTRICT",
        ),
        nullable=False,
    )
    allocation_epoch: Mapped[int] = mapped_column(
        BigInteger,
        ForeignKey("capacity_allocation_epochs.allocation_epoch", ondelete="RESTRICT"),
        nullable=False,
    )
    executor_id: Mapped[str] = mapped_column(Text, nullable=False)
    executor_incarnation: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), nullable=False)
    pool_id: Mapped[str] = mapped_column(Text, nullable=False)
    pool_generation: Mapped[int] = mapped_column(BigInteger, nullable=False)
    subject_id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), nullable=False)
    subject_incarnation: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), nullable=False)
    account_id: Mapped[str] = mapped_column(Text, nullable=False)
    tier_id: Mapped[str] = mapped_column(Text, nullable=False)
    candidate_generation: Mapped[int] = mapped_column(BigInteger, nullable=False)
    deployment_generation: Mapped[int] = mapped_column(BigInteger, nullable=False)
    proposal_digest: Mapped[str] = mapped_column(Text, nullable=False)
    state: Mapped[str] = mapped_column(Text, nullable=False, server_default=text("'proposed'"))
    executable: Mapped[bool] = mapped_column(Boolean, nullable=False, server_default=text("false"))
    expires_at: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), nullable=False)
    accepted_at: Mapped[datetime | None] = mapped_column(TIMESTAMP(timezone=True))
    closed_at: Mapped[datetime | None] = mapped_column(TIMESTAMP(timezone=True))
    closure_reason: Mapped[str | None] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), nullable=False, server_default=func.now()
    )


class CapacityReservationShape(Base):
    __tablename__ = "capacity_reservation_shapes"
    __table_args__ = (
        CheckConstraint(
            "profile_generation > 0 AND concurrency_slots > 0 "
            "AND rollout_surge_slots >= 0 "
            "AND rollout_surge_slots <= concurrency_slots",
            name="capacity_reservation_shape_quantity_check",
        ),
        CheckConstraint(
            "jsonb_typeof(resource_vector -> 'slots') = 'number' "
            "AND (resource_vector ->> 'slots')::bigint = concurrency_slots "
            "AND jsonb_typeof(node_ids) = 'array' "
            "AND jsonb_array_length(node_ids) > 0",
            name="capacity_reservation_shape_binding_check",
        ),
        CheckConstraint(
            "profile_digest ~ '^[0-9a-f]{64}$'",
            name="capacity_reservation_shape_digest_check",
        ),
        CheckConstraint(
            "state IN ('proposed','accepted','releasing','released')",
            name="capacity_reservation_shape_state_check",
        ),
        CheckConstraint(
            "(rollout_surge_slots = 0 AND old_shape_backing_id IS NULL) OR "
            "(rollout_surge_slots > 0 AND old_shape_backing_id IS NOT NULL)",
            name="capacity_reservation_shape_surge_check",
        ),
        CheckConstraint(
            "(state = 'released' AND release_evidence_digest IS NOT NULL "
            "AND released_at IS NOT NULL) OR "
            "(state <> 'released' AND release_evidence_digest IS NULL "
            "AND released_at IS NULL)",
            name="capacity_reservation_shape_release_check",
        ),
        UniqueConstraint("shape_instance_id", name="capacity_reservation_shape_identity_key"),
        UniqueConstraint("intent_id", name="capacity_reservation_shape_intent_key"),
        UniqueConstraint(
            "tranche_id",
            "shape_instance_id",
            "intent_id",
            name="capacity_reservation_shape_exact_binding_key",
        ),
    )

    id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), primary_key=True, default=uuid4)
    tranche_id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey("capacity_reservation_tranches.id", ondelete="RESTRICT"),
        nullable=False,
    )
    shape_instance_id: Mapped[str] = mapped_column(Text, nullable=False)
    intent_id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), nullable=False)
    shape_id: Mapped[str] = mapped_column(Text, nullable=False)
    profile_id: Mapped[str] = mapped_column(Text, nullable=False)
    profile_generation: Mapped[int] = mapped_column(BigInteger, nullable=False)
    profile_digest: Mapped[str] = mapped_column(Text, nullable=False)
    concurrency_slots: Mapped[int] = mapped_column(BigInteger, nullable=False)
    resource_vector: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    node_ids: Mapped[list[str]] = mapped_column(JSONB, nullable=False)
    rollout_surge_slots: Mapped[int] = mapped_column(
        BigInteger, nullable=False, server_default=text("0")
    )
    old_shape_backing_id: Mapped[str | None] = mapped_column(Text)
    state: Mapped[str] = mapped_column(Text, nullable=False, server_default=text("'proposed'"))
    release_evidence_digest: Mapped[str | None] = mapped_column(Text)
    released_at: Mapped[datetime | None] = mapped_column(TIMESTAMP(timezone=True))


class CapacitySubmissionIntent(Base):
    __tablename__ = "capacity_submission_intents"
    __table_args__ = (
        CheckConstraint(
            "state IN ('prepared','launch-ready','submitting-unknown','bound',"
            "'observed','terminal','closing','closed','quarantined')",
            name="capacity_submission_intent_state_check",
        ),
        CheckConstraint(
            "ownership_metadata_sha256 ~ '^[0-9a-f]{64}$'",
            name="capacity_submission_intent_digest_check",
        ),
        CheckConstraint(
            "executable = false",
            name="capacity_submission_intent_dry_run_only_check",
        ),
        CheckConstraint(
            "((bootstrap_registration_epoch IS NULL "
            "AND bootstrap_evidence_sha256 IS NULL AND launch_ready_at IS NULL) OR "
            "(bootstrap_registration_epoch > 0 "
            "AND bootstrap_evidence_sha256 ~ '^[0-9a-f]{64}$' "
            "AND launch_ready_at IS NOT NULL)) "
            "AND (state <> 'launch-ready' OR bootstrap_registration_epoch IS NOT NULL) "
            "AND (state <> 'prepared' OR bootstrap_registration_epoch IS NULL) "
            "AND (state NOT IN ('submitting-unknown','bound','observed','terminal') "
            "OR bootstrap_registration_epoch IS NOT NULL)",
            name="capacity_submission_intent_bootstrap_check",
        ),
        UniqueConstraint("shape_instance_id", name="capacity_submission_intent_shape_key"),
        UniqueConstraint(
            "id",
            "executor_id",
            "executor_incarnation",
            name="capacity_submission_intent_executor_binding_key",
        ),
        ForeignKeyConstraint(
            ("tranche_id", "shape_instance_id", "id"),
            (
                "capacity_reservation_shapes.tranche_id",
                "capacity_reservation_shapes.shape_instance_id",
                "capacity_reservation_shapes.intent_id",
            ),
            name="capacity_submission_intent_shape_binding_fkey",
            ondelete="RESTRICT",
        ),
    )

    id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), primary_key=True)
    tranche_id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True),
        nullable=False,
    )
    shape_instance_id: Mapped[str] = mapped_column(
        Text,
        nullable=False,
    )
    executor_id: Mapped[str] = mapped_column(Text, nullable=False)
    executor_incarnation: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), nullable=False)
    ownership_metadata_sha256: Mapped[str] = mapped_column(Text, nullable=False)
    state: Mapped[str] = mapped_column(Text, nullable=False, server_default=text("'prepared'"))
    executable: Mapped[bool] = mapped_column(Boolean, nullable=False, server_default=text("false"))
    bootstrap_registration_epoch: Mapped[int | None] = mapped_column(BigInteger)
    bootstrap_evidence_sha256: Mapped[str | None] = mapped_column(Text)
    launch_ready_at: Mapped[datetime | None] = mapped_column(TIMESTAMP(timezone=True))
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), nullable=False, server_default=func.now()
    )


class CapacityLaunchPermit(Base):
    __tablename__ = "capacity_launch_permits"
    __table_args__ = (
        CheckConstraint(
            "permit_epoch > 0 AND allocation_epoch > 0 "
            "AND configuration_epoch > 0 AND launch_rank > 0",
            name="capacity_launch_permit_epoch_check",
        ),
        CheckConstraint(
            "permit_digest ~ '^[0-9a-f]{64}$'",
            name="capacity_launch_permit_digest_check",
        ),
        CheckConstraint(
            "state IN ('current','superseded','consumed','revoked')",
            name="capacity_launch_permit_state_check",
        ),
        CheckConstraint(
            "executable = false",
            name="capacity_launch_permit_dry_run_only_check",
        ),
        CheckConstraint(
            "(state = 'consumed' AND dry_run_consumed_at IS NOT NULL) OR "
            "(state <> 'consumed' AND dry_run_consumed_at IS NULL)",
            name="capacity_launch_permit_consumption_check",
        ),
        UniqueConstraint("intent_id", "permit_epoch", name="capacity_launch_permit_epoch_key"),
        UniqueConstraint("idempotency_key", name="capacity_launch_permit_idempotency_key"),
        Index(
            "capacity_launch_permit_one_current_per_intent_idx",
            "intent_id",
            unique=True,
            postgresql_where=text("state = 'current'"),
        ),
        ForeignKeyConstraint(
            ("intent_id", "executor_id", "executor_incarnation"),
            (
                "capacity_submission_intents.id",
                "capacity_submission_intents.executor_id",
                "capacity_submission_intents.executor_incarnation",
            ),
            name="capacity_launch_permit_intent_binding_fkey",
            ondelete="RESTRICT",
        ),
    )

    id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), primary_key=True, default=uuid4)
    intent_id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True),
        nullable=False,
    )
    permit_epoch: Mapped[int] = mapped_column(BigInteger, nullable=False)
    allocation_epoch: Mapped[int] = mapped_column(
        BigInteger,
        ForeignKey("capacity_allocation_epochs.allocation_epoch", ondelete="RESTRICT"),
        nullable=False,
    )
    configuration_epoch: Mapped[int] = mapped_column(
        BigInteger,
        ForeignKey(
            "capacity_configuration_epochs.configuration_epoch",
            ondelete="RESTRICT",
        ),
        nullable=False,
    )
    executor_id: Mapped[str] = mapped_column(Text, nullable=False)
    executor_incarnation: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), nullable=False)
    launch_rank: Mapped[int] = mapped_column(BigInteger, nullable=False)
    permit_digest: Mapped[str] = mapped_column(Text, nullable=False)
    idempotency_key: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), nullable=False)
    state: Mapped[str] = mapped_column(Text, nullable=False, server_default=text("'current'"))
    executable: Mapped[bool] = mapped_column(Boolean, nullable=False, server_default=text("false"))
    expires_at: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), nullable=False)
    dry_run_consumed_at: Mapped[datetime | None] = mapped_column(TIMESTAMP(timezone=True))
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), nullable=False, server_default=func.now()
    )


class CapacityLaunchRateBucket(Base):
    __tablename__ = "capacity_launch_rate_buckets"
    __table_args__ = (
        CheckConstraint(
            "scope IN ('global','account','subject','pool')",
            name="capacity_launch_rate_bucket_scope_check",
        ),
        CheckConstraint(
            "configuration_epoch > 0 "
            "AND rate_per_minute BETWEEN 0 AND 9223372036854 "
            "AND capacity_microtokens = rate_per_minute * 1000000 "
            "AND available_microtokens >= 0 "
            "AND available_microtokens <= capacity_microtokens "
            "AND refill_remainder >= 0 AND refill_remainder < 60",
            name="capacity_launch_rate_bucket_quantity_check",
        ),
        CheckConstraint(
            "state = 'dry-run'",
            name="capacity_launch_rate_bucket_dry_run_only_check",
        ),
        UniqueConstraint(
            "configuration_epoch",
            "scope",
            "scope_identity",
            name="capacity_launch_rate_bucket_scope_key",
        ),
    )

    id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), primary_key=True, default=uuid4)
    configuration_epoch: Mapped[int] = mapped_column(
        BigInteger,
        ForeignKey(
            "capacity_configuration_epochs.configuration_epoch",
            ondelete="RESTRICT",
        ),
        nullable=False,
    )
    scope: Mapped[str] = mapped_column(Text, nullable=False)
    scope_identity: Mapped[str] = mapped_column(Text, nullable=False)
    rate_per_minute: Mapped[int] = mapped_column(BigInteger, nullable=False)
    capacity_microtokens: Mapped[int] = mapped_column(BigInteger, nullable=False)
    available_microtokens: Mapped[int] = mapped_column(BigInteger, nullable=False)
    refill_remainder: Mapped[int] = mapped_column(
        BigInteger, nullable=False, server_default=text("0")
    )
    last_refill_at: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), nullable=False)
    state: Mapped[str] = mapped_column(Text, nullable=False, server_default=text("'dry-run'"))


class CapacityReservationReleaseEvidence(Base):
    __tablename__ = "capacity_reservation_release_evidence"
    __table_args__ = (
        CheckConstraint(
            "command_sequence > 0 AND inventory_sequence > 0 "
            "AND protected_registration_epoch > 0 AND bootstrap_revoked = true",
            name="capacity_reservation_release_sequence_check",
        ),
        CheckConstraint(
            "terminal_kind IN ('unused','slurm-job','worker')",
            name="capacity_reservation_release_kind_check",
        ),
        CheckConstraint(
            "evidence_digest ~ '^[0-9a-f]{64}$' "
            "AND terminal_evidence_sha256 ~ '^[0-9a-f]{64}$' "
            "AND protected_release_sha256 ~ '^[0-9a-f]{64}$'",
            name="capacity_reservation_release_digest_check",
        ),
        UniqueConstraint("shape_instance_id", name="capacity_reservation_release_shape_key"),
        UniqueConstraint(
            "executor_incarnation",
            "terminal_kind",
            "terminal_identity",
            name="capacity_reservation_release_terminal_key",
        ),
        ForeignKeyConstraint(
            ("tranche_id", "shape_instance_id", "intent_id"),
            (
                "capacity_reservation_shapes.tranche_id",
                "capacity_reservation_shapes.shape_instance_id",
                "capacity_reservation_shapes.intent_id",
            ),
            name="capacity_release_evidence_shape_binding_fkey",
            ondelete="RESTRICT",
        ),
        ForeignKeyConstraint(
            ("intent_id", "executor_id", "executor_incarnation"),
            (
                "capacity_submission_intents.id",
                "capacity_submission_intents.executor_id",
                "capacity_submission_intents.executor_incarnation",
            ),
            name="capacity_release_evidence_intent_binding_fkey",
            ondelete="RESTRICT",
        ),
    )

    id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), primary_key=True, default=uuid4)
    tranche_id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True),
        nullable=False,
    )
    shape_instance_id: Mapped[str] = mapped_column(
        Text,
        nullable=False,
    )
    intent_id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True),
        nullable=False,
    )
    executor_id: Mapped[str] = mapped_column(Text, nullable=False)
    executor_incarnation: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), nullable=False)
    command_sequence: Mapped[int] = mapped_column(BigInteger, nullable=False)
    inventory_sequence: Mapped[int] = mapped_column(BigInteger, nullable=False)
    terminal_kind: Mapped[str] = mapped_column(Text, nullable=False)
    terminal_identity: Mapped[str] = mapped_column(Text, nullable=False)
    terminal_evidence_sha256: Mapped[str] = mapped_column(Text, nullable=False)
    protected_registration_epoch: Mapped[int] = mapped_column(BigInteger, nullable=False)
    bootstrap_revoked: Mapped[bool] = mapped_column(Boolean, nullable=False)
    protected_release_sha256: Mapped[str] = mapped_column(Text, nullable=False)
    evidence_digest: Mapped[str] = mapped_column(Text, nullable=False)
    received_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), nullable=False, server_default=func.now()
    )


class CapacityProtectedReleaseAcknowledgement(Base):
    __tablename__ = "capacity_protected_release_acknowledgements"
    __table_args__ = (
        CheckConstraint(
            "writer_epoch > 0 AND configuration_epoch > 0 "
            "AND allocation_epoch > 0 AND deployment_generation > 0 "
            "AND pool_generation > 0 AND bootstrap_registration_epoch >= 0 "
            "AND protected_registration_epoch > bootstrap_registration_epoch "
            "AND bootstrap_revoked = true",
            name="capacity_protected_release_epoch_check",
        ),
        CheckConstraint(
            "protected_release_sha256 ~ '^[0-9a-f]{64}$' "
            "AND acknowledgement_digest ~ '^[0-9a-f]{64}$'",
            name="capacity_protected_release_digest_check",
        ),
        CheckConstraint(
            "executable = false",
            name="capacity_protected_release_dry_run_only_check",
        ),
        UniqueConstraint(
            "idempotency_key",
            name="capacity_protected_release_idempotency_key",
        ),
        UniqueConstraint(
            "shape_instance_id",
            name="capacity_protected_release_shape_key",
        ),
        ForeignKeyConstraint(
            ("tranche_id", "shape_instance_id", "intent_id"),
            (
                "capacity_reservation_shapes.tranche_id",
                "capacity_reservation_shapes.shape_instance_id",
                "capacity_reservation_shapes.intent_id",
            ),
            name="capacity_protected_release_shape_binding_fkey",
            ondelete="RESTRICT",
        ),
        ForeignKeyConstraint(
            ("subject_id", "subject_incarnation", "reporter_incarnation"),
            (
                "capacity_demand_reporters.subject_id",
                "capacity_demand_reporters.subject_incarnation",
                "capacity_demand_reporters.reporter_incarnation",
            ),
            name="capacity_protected_release_reporter_binding_fkey",
            ondelete="RESTRICT",
        ),
    )

    id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), primary_key=True, default=uuid4)
    idempotency_key: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), nullable=False)
    authority_incarnation: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), nullable=False)
    writer_epoch: Mapped[int] = mapped_column(BigInteger, nullable=False)
    configuration_epoch: Mapped[int] = mapped_column(BigInteger, nullable=False)
    allocation_epoch: Mapped[int] = mapped_column(BigInteger, nullable=False)
    tranche_id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), nullable=False)
    shape_instance_id: Mapped[str] = mapped_column(Text, nullable=False)
    intent_id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), nullable=False)
    subject_id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), nullable=False)
    subject_incarnation: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), nullable=False)
    reporter_incarnation: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), nullable=False)
    deployment_generation: Mapped[int] = mapped_column(BigInteger, nullable=False)
    pool_id: Mapped[str] = mapped_column(Text, nullable=False)
    pool_generation: Mapped[int] = mapped_column(BigInteger, nullable=False)
    bootstrap_registration_epoch: Mapped[int] = mapped_column(BigInteger, nullable=False)
    protected_registration_epoch: Mapped[int] = mapped_column(BigInteger, nullable=False)
    bootstrap_revoked: Mapped[bool] = mapped_column(Boolean, nullable=False)
    protected_release_sha256: Mapped[str] = mapped_column(Text, nullable=False)
    acknowledgement_digest: Mapped[str] = mapped_column(Text, nullable=False)
    actor_id: Mapped[str] = mapped_column(Text, nullable=False)
    executable: Mapped[bool] = mapped_column(Boolean, nullable=False, server_default=text("false"))
    received_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), nullable=False, server_default=func.now()
    )


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
    "CapacityExecutionEpoch",
    "CapacityExecutionExecutor",
    "CapacityExecutor",
    "CapacityExecutorObservation",
    "CapacityFairnessState",
    "CapacityLaunchPermit",
    "CapacityLaunchRateBucket",
    "CapacityObservedCommitment",
    "CapacityPool",
    "CapacityPoolObservation",
    "CapacityPoolReporter",
    "CapacityProtectedReleaseAcknowledgement",
    "CapacityReservationReleaseEvidence",
    "CapacityReservationShape",
    "CapacityReservationTranche",
    "CapacitySubject",
    "CapacitySubmissionIntent",
    "CapacityTier",
    "CapacityWorkerProfile",
]
