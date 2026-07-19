"""SQLAlchemy ORM models for Loom's Postgres state (spec §4.7).

JSONB-typed columns hold Pydantic-serialized payloads; the ORM doesn't
validate the inner shape — that's the responsibility of the application code
that writes the row (which already validates against the Pydantic models).
"""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Any
from uuid import UUID, uuid4

from sqlalchemy import (
    ARRAY,
    BigInteger,
    Boolean,
    CheckConstraint,
    Float,
    ForeignKey,
    Index,
    Integer,
    LargeBinary,
    Numeric,
    String,
    Text,
    UniqueConstraint,
    func,
    text,
)
from sqlalchemy.dialects.postgresql import INET, JSONB, TIMESTAMP
from sqlalchemy.dialects.postgresql import (
    UUID as PgUUID,  # noqa: N811  (UUID is a type, not a constant)
)
from sqlalchemy.orm import Mapped, mapped_column

from loom.db.base import Base


class Team(Base):
    __tablename__ = "teams"
    id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), primary_key=True, default=uuid4)
    name: Mapped[str] = mapped_column(String, unique=True, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True),
        server_default=func.now(),
        nullable=False,
    )
    disabled_at: Mapped[datetime | None] = mapped_column(
        TIMESTAMP(timezone=True),
        nullable=True,
    )
    disabled_reason: Mapped[str | None] = mapped_column(Text, nullable=True)
    submissions_paused_at: Mapped[datetime | None] = mapped_column(
        TIMESTAMP(timezone=True),
        nullable=True,
    )
    submissions_paused_reason: Mapped[str | None] = mapped_column(
        Text,
        nullable=True,
    )


class TeamQuota(Base):
    __tablename__ = "team_quotas"
    team_id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey("teams.id"),
        primary_key=True,
    )
    fair_share_weight: Mapped[float] = mapped_column(Float, nullable=False, default=1.0)
    # Admin's ceiling: the scheduler stops re-claiming a trial once
    # `attempt_count >= max_attempts_ceiling`. Semantically distinct from
    # TrialConfig.retry.max_attempts (submitter's requested count). See #401.
    max_attempts_ceiling: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        default=3,
    )
    in_flight_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    # Legacy per-team SPDX metadata. This field is retained for API/backfill
    # compatibility; license values no longer gate task selection or submit.
    license_allowlist: Mapped[list[str]] = mapped_column(
        ARRAY(Text),
        nullable=False,
        server_default=text(
            "ARRAY['MIT', 'Apache-2.0', 'BSD-3-Clause', 'CC-BY-4.0']::text[]",
        ),
    )
    # TaskSet quota columns (#242 sub-plan 7). NULL means "use global
    # default from loom-schema.toml"; non-NULL overrides per-team.
    taskset_max_count: Mapped[int | None] = mapped_column(
        Integer,
        nullable=True,
    )
    taskset_max_storage_bytes: Mapped[int | None] = mapped_column(
        BigInteger,
        nullable=True,
    )
    # SSRF defense layer 3 opt-in (cluster-deploy.md §Secrets/SSRF).
    # When False (default for `loom cluster`), `POST /provider-connections`
    # rejects RFC1918 / IPv6 ULA / loopback / link-local IPs. When True
    # (default for `loom service` single-box mode, set at bootstrap),
    # RFC1918 + ULA are permitted; loopback + link-local stay rejected
    # unconditionally.
    allow_private_endpoints: Mapped[bool] = mapped_column(
        Boolean,
        nullable=False,
        server_default=text("false"),
    )


class StagingMutationEpoch(Base):
    """Monotonic authority for protected staging mutations."""

    __tablename__ = "staging_mutation_epochs"
    __table_args__ = (
        CheckConstraint(
            "environment = 'staging'",
            name="staging_mutation_epochs_env_check",
        ),
        CheckConstraint(
            "namespace <> ''",
            name="staging_mutation_epochs_namespace_check",
        ),
        CheckConstraint("epoch >= 0", name="staging_mutation_epochs_epoch_check"),
        CheckConstraint(
            "reason IN ('bootstrap','rollout_apply','lifecycle_gc','object_rewrite','rollback')",
            name="staging_mutation_epochs_reason_check",
        ),
        CheckConstraint(
            "(reason = 'bootstrap' AND request_id IS NULL AND evidence_sha256 IS NULL) OR "
            "(reason <> 'bootstrap' AND request_id ~ '^[a-z0-9][a-z0-9-]{7,79}$' "
            "AND evidence_sha256 ~ '^[0-9a-f]{64}$')",
            name="staging_mutation_epochs_evidence_check",
        ),
    )
    environment: Mapped[str] = mapped_column(Text, primary_key=True)
    namespace: Mapped[str] = mapped_column(Text, nullable=False)
    epoch: Mapped[int] = mapped_column(
        BigInteger,
        nullable=False,
        server_default=text("0"),
        default=0,
    )
    reason: Mapped[str] = mapped_column(
        Text,
        nullable=False,
        server_default=text("'bootstrap'"),
        default="bootstrap",
    )
    request_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    evidence_sha256: Mapped[str | None] = mapped_column(Text, nullable=True)
    updated_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True),
        nullable=False,
        server_default=func.now(),
    )


class StagingMutationEpochEvent(Base):
    """Append-only evidence for one successful protected mutation CAS."""

    __tablename__ = "staging_mutation_epoch_events"
    __table_args__ = (
        CheckConstraint(
            "environment = 'staging'",
            name="staging_mutation_epoch_events_env_check",
        ),
        CheckConstraint(
            "namespace <> '' AND epoch > 0",
            name="staging_mutation_epoch_events_identity_check",
        ),
        CheckConstraint(
            "mutation_class IN ('rollout_apply','lifecycle_gc','object_rewrite','rollback')",
            name="staging_mutation_epoch_events_class_check",
        ),
        CheckConstraint(
            "request_id ~ '^[a-z0-9][a-z0-9-]{7,79}$' AND evidence_sha256 ~ '^[0-9a-f]{64}$'",
            name="staging_mutation_epoch_events_evidence_check",
        ),
    )
    environment: Mapped[str] = mapped_column(Text, primary_key=True)
    namespace: Mapped[str] = mapped_column(Text, primary_key=True)
    epoch: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    mutation_class: Mapped[str] = mapped_column(Text, nullable=False)
    request_id: Mapped[str] = mapped_column(Text, nullable=False)
    evidence_sha256: Mapped[str] = mapped_column(Text, nullable=False)
    occurred_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True),
        nullable=False,
    )


class StagingLifecycleCapacity(Base):
    """Fresh exact object/filesystem capacity used by staging admission."""

    __tablename__ = "staging_lifecycle_capacity"
    __table_args__ = (
        CheckConstraint("environment = 'staging'", name="staging_lifecycle_capacity_env_check"),
        CheckConstraint(
            "namespace <> '' AND source = 'exact-object-inventory-v1'",
            name="staging_lifecycle_capacity_identity_check",
        ),
        CheckConstraint(
            "object_count >= 0 AND bytes_used >= 0",
            name="staging_lifecycle_capacity_counters_check",
        ),
        CheckConstraint(
            "disk_free_percent BETWEEN 0 AND 100 AND inode_free_percent BETWEEN 0 AND 100",
            name="staging_lifecycle_capacity_percent_check",
        ),
        CheckConstraint(
            "policy_sha256 ~ '^[0-9a-f]{64}$' AND "
            "evidence_sha256 ~ '^[0-9a-f]{64}$'",
            name="staging_lifecycle_capacity_digest_check",
        ),
    )
    environment: Mapped[str] = mapped_column(Text, primary_key=True)
    namespace: Mapped[str] = mapped_column(Text, nullable=False)
    object_count: Mapped[int] = mapped_column(BigInteger, nullable=False)
    bytes_used: Mapped[int] = mapped_column(BigInteger, nullable=False)
    disk_free_percent: Mapped[int] = mapped_column(Integer, nullable=False)
    inode_free_percent: Mapped[int] = mapped_column(Integer, nullable=False)
    policy_sha256: Mapped[str] = mapped_column(Text, nullable=False)
    evidence_sha256: Mapped[str] = mapped_column(Text, nullable=False)
    source: Mapped[str] = mapped_column(Text, nullable=False)
    observed_at: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), nullable=False)


class DataLifecycleAuthority(Base):
    """Typed environment/owner/retention authority for execution data."""

    __tablename__ = "data_lifecycle_authorities"
    __table_args__ = (
        CheckConstraint("environment <> ''", name="data_lifecycle_authorities_env_check"),
        CheckConstraint("namespace <> ''", name="data_lifecycle_authorities_namespace_check"),
        CheckConstraint(
            "data_class IN ('run','trial','event','artifact','benchmark','catalog','system')",
            name="data_lifecycle_authorities_class_check",
        ),
        CheckConstraint(
            "owner_kind IN ('batch','trial','artifact','benchmark','system')",
            name="data_lifecycle_authorities_owner_kind_check",
        ),
        CheckConstraint(
            "state IN ('active','deleting','quarantined')",
            name="data_lifecycle_authorities_state_check",
        ),
        CheckConstraint(
            "(pinned AND expires_at IS NULL) OR "
            "(NOT pinned AND expires_at IS NOT NULL AND expires_at > created_at)",
            name="data_lifecycle_authorities_retention_check",
        ),
        CheckConstraint(
            "data_class NOT IN ('catalog','system') OR pinned",
            name="data_lifecycle_authorities_pinned_class_check",
        ),
        UniqueConstraint(
            "environment",
            "namespace",
            "data_class",
            "owner_kind",
            "owner_id",
            name="data_lifecycle_authorities_owner_uidx",
        ),
        Index(
            "data_lifecycle_authorities_gc_idx",
            "environment",
            "namespace",
            "state",
            "expires_at",
        ),
    )
    id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True),
        primary_key=True,
        server_default=text("gen_random_uuid()"),
    )
    environment: Mapped[str] = mapped_column(Text, nullable=False)
    namespace: Mapped[str] = mapped_column(Text, nullable=False)
    team_id: Mapped[UUID | None] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey("teams.id", ondelete="RESTRICT"),
        nullable=True,
    )
    data_class: Mapped[str] = mapped_column(Text, nullable=False)
    owner_kind: Mapped[str] = mapped_column(Text, nullable=False)
    owner_id: Mapped[str] = mapped_column(Text, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True),
        nullable=False,
        server_default=func.now(),
    )
    expires_at: Mapped[datetime | None] = mapped_column(
        TIMESTAMP(timezone=True),
        nullable=True,
    )
    pinned: Mapped[bool] = mapped_column(
        Boolean,
        nullable=False,
        server_default=text("false"),
        default=False,
    )
    state: Mapped[str] = mapped_column(
        Text,
        nullable=False,
        server_default=text("'active'"),
        default="active",
    )
    deletion_token: Mapped[UUID | None] = mapped_column(PgUUID(as_uuid=True), nullable=True)
    lifecycle_metadata: Mapped[dict[str, Any]] = mapped_column(
        "metadata",
        JSONB,
        nullable=False,
        server_default=text("'{}'::jsonb"),
        default=dict,
    )


class DataLifecycleObject(Base):
    """Exact object-store identity owned by one lifecycle authority."""

    __tablename__ = "data_lifecycle_objects"
    __table_args__ = (
        CheckConstraint("environment <> ''", name="data_lifecycle_objects_env_check"),
        CheckConstraint("namespace <> ''", name="data_lifecycle_objects_namespace_check"),
        CheckConstraint(
            "bucket <> '' AND object_key <> ''",
            name="data_lifecycle_objects_key_check",
        ),
        CheckConstraint("size_bytes >= 0", name="data_lifecycle_objects_size_check"),
        CheckConstraint(
            "state IN ('active','delete_pending','deleted','quarantined')",
            name="data_lifecycle_objects_state_check",
        ),
        CheckConstraint(
            "(state = 'deleted') = (verified_deleted_at IS NOT NULL)",
            name="data_lifecycle_objects_deleted_check",
        ),
        Index("data_lifecycle_objects_authority_state_idx", "authority_id", "state"),
        Index(
            "data_lifecycle_objects_identity_uidx",
            "environment",
            "namespace",
            "bucket",
            "object_key",
            text("COALESCE(version_id, '')"),
            unique=True,
        ),
    )
    id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True),
        primary_key=True,
        server_default=text("gen_random_uuid()"),
    )
    authority_id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey("data_lifecycle_authorities.id", ondelete="CASCADE"),
        nullable=False,
    )
    environment: Mapped[str] = mapped_column(Text, nullable=False)
    namespace: Mapped[str] = mapped_column(Text, nullable=False)
    bucket: Mapped[str] = mapped_column(Text, nullable=False)
    object_key: Mapped[str] = mapped_column(Text, nullable=False)
    version_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    content_sha256: Mapped[str | None] = mapped_column(Text, nullable=True)
    size_bytes: Mapped[int] = mapped_column(BigInteger, nullable=False)
    created_at: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), nullable=False)
    state: Mapped[str] = mapped_column(
        Text,
        nullable=False,
        server_default=text("'active'"),
        default="active",
    )
    deletion_token: Mapped[UUID | None] = mapped_column(PgUUID(as_uuid=True), nullable=True)
    verified_deleted_at: Mapped[datetime | None] = mapped_column(
        TIMESTAMP(timezone=True),
        nullable=True,
    )


class DataLifecycleGcRun(Base):
    """Append-only top-level journal for one staging-only GC transaction."""

    __tablename__ = "data_lifecycle_gc_runs"
    __table_args__ = (
        CheckConstraint("environment = 'staging'", name="data_lifecycle_gc_runs_env_check"),
        CheckConstraint("namespace <> ''", name="data_lifecycle_gc_runs_namespace_check"),
        CheckConstraint(
            "mutation_epoch_before >= 0",
            name="data_lifecycle_gc_runs_epoch_before_check",
        ),
        CheckConstraint(
            "mutation_epoch_after IS NULL OR mutation_epoch_after > mutation_epoch_before",
            name="data_lifecycle_gc_runs_epoch_after_check",
        ),
        CheckConstraint(
            "state IN ('planned','applying','verifying','completed','failed')",
            name="data_lifecycle_gc_runs_state_check",
        ),
    )
    id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True),
        primary_key=True,
        server_default=text("gen_random_uuid()"),
    )
    environment: Mapped[str] = mapped_column(Text, nullable=False)
    namespace: Mapped[str] = mapped_column(Text, nullable=False)
    mutation_epoch_before: Mapped[int] = mapped_column(BigInteger, nullable=False)
    mutation_epoch_after: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    state: Mapped[str] = mapped_column(
        Text,
        nullable=False,
        server_default=text("'planned'"),
        default="planned",
    )
    dry_run: Mapped[bool] = mapped_column(Boolean, nullable=False)
    requested_by: Mapped[str] = mapped_column(Text, nullable=False)
    policy: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    inventory: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True),
        nullable=False,
        server_default=func.now(),
    )
    finished_at: Mapped[datetime | None] = mapped_column(
        TIMESTAMP(timezone=True),
        nullable=True,
    )
    failure_reason: Mapped[str | None] = mapped_column(Text, nullable=True)


class DataLifecycleGcItem(Base):
    """Retryable exact-object progress in a two-phase GC run."""

    __tablename__ = "data_lifecycle_gc_items"
    __table_args__ = (
        CheckConstraint(
            "state IN ('marked','object_deleted','verified','metadata_deleted','failed')",
            name="data_lifecycle_gc_items_state_check",
        ),
    )
    gc_run_id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey("data_lifecycle_gc_runs.id", ondelete="CASCADE"),
        primary_key=True,
    )
    object_id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True),
        primary_key=True,
    )
    deletion_token: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), nullable=False)
    state: Mapped[str] = mapped_column(
        Text,
        nullable=False,
        server_default=text("'marked'"),
        default="marked",
    )
    last_error: Mapped[str | None] = mapped_column(Text, nullable=True)
    updated_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True),
        nullable=False,
        server_default=func.now(),
    )


class User(Base):
    __tablename__ = "users"
    id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), primary_key=True, default=uuid4)
    email: Mapped[str | None] = mapped_column(Text, nullable=True)
    username: Mapped[str] = mapped_column(Text, nullable=False)
    username_normalized: Mapped[str] = mapped_column(Text, nullable=False, unique=True)
    display_name: Mapped[str | None] = mapped_column(Text, nullable=True)
    password_hash: Mapped[str | None] = mapped_column(Text, nullable=True)
    password_set_at: Mapped[datetime | None] = mapped_column(
        TIMESTAMP(timezone=True),
        nullable=True,
    )
    status: Mapped[str] = mapped_column(
        Text,
        nullable=False,
        server_default=text("'pending_setup'"),
        default="pending_setup",
    )
    disabled_at: Mapped[datetime | None] = mapped_column(
        TIMESTAMP(timezone=True),
        nullable=True,
    )
    is_platform_admin: Mapped[bool] = mapped_column(
        Boolean,
        nullable=False,
        server_default=text("false"),
        default=False,
    )
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True),
        server_default=func.now(),
        nullable=False,
    )
    last_login_at: Mapped[datetime | None] = mapped_column(
        TIMESTAMP(timezone=True),
        nullable=True,
    )


class TeamMembership(Base):
    __tablename__ = "team_memberships"
    __table_args__ = (
        CheckConstraint(
            "role IN ('owner', 'member', 'viewer')",
            name="team_memberships_role_check",
        ),
    )
    team_id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey("teams.id", ondelete="CASCADE"),
        primary_key=True,
    )
    user_id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey("users.id", ondelete="CASCADE"),
        primary_key=True,
    )
    role: Mapped[str] = mapped_column(Text, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True),
        server_default=func.now(),
        nullable=False,
    )


class TeamInvite(Base):
    __tablename__ = "team_invites"
    __table_args__ = (
        CheckConstraint(
            "role IN ('owner', 'member', 'viewer')",
            name="team_invites_role_check",
        ),
        CheckConstraint(
            "status IN ('pending', 'accepted', 'revoked', 'expired')",
            name="team_invites_status_check",
        ),
        CheckConstraint(
            "max_uses IS NULL OR max_uses > 0",
            name="team_invites_max_uses_positive_check",
        ),
        CheckConstraint(
            "accepted_uses >= 0",
            name="team_invites_accepted_uses_nonnegative_check",
        ),
    )

    id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), primary_key=True, default=uuid4)
    team_id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey("teams.id", ondelete="CASCADE"),
        nullable=False,
    )
    email: Mapped[str] = mapped_column(Text, nullable=False)
    allowed_domain: Mapped[str | None] = mapped_column(Text, nullable=True)
    role: Mapped[str] = mapped_column(Text, nullable=False)
    status: Mapped[str] = mapped_column(
        Text,
        nullable=False,
        server_default=text("'pending'"),
        default="pending",
    )
    code_hash: Mapped[bytes] = mapped_column(LargeBinary, nullable=False)
    code_prefix: Mapped[str] = mapped_column(Text, nullable=False)
    max_uses: Mapped[int | None] = mapped_column(Integer, nullable=True)
    accepted_uses: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    created_by_actor: Mapped[str] = mapped_column(Text, nullable=False)
    created_by_user_id: Mapped[UUID | None] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey("users.id", ondelete="SET NULL"),
        nullable=True,
    )
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True),
        server_default=func.now(),
        nullable=False,
    )
    expires_at: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), nullable=False)
    last_sent_at: Mapped[datetime | None] = mapped_column(TIMESTAMP(timezone=True), nullable=True)
    accepted_at: Mapped[datetime | None] = mapped_column(TIMESTAMP(timezone=True), nullable=True)
    revoked_at: Mapped[datetime | None] = mapped_column(TIMESTAMP(timezone=True), nullable=True)
    revoked_reason: Mapped[str | None] = mapped_column(Text, nullable=True)


class UserSession(Base):
    __tablename__ = "user_sessions"
    session_hash: Mapped[bytes] = mapped_column(LargeBinary, primary_key=True)
    user_id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey("users.id", ondelete="CASCADE"),
        nullable=False,
    )
    current_team_id: Mapped[UUID | None] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey("teams.id", ondelete="SET NULL"),
        nullable=True,
    )
    csrf_hash: Mapped[bytes] = mapped_column(LargeBinary, nullable=False)
    issued_at: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), nullable=False)
    expires_at: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), nullable=False)
    revoked_at: Mapped[datetime | None] = mapped_column(TIMESTAMP(timezone=True), nullable=True)
    last_seen_at: Mapped[datetime | None] = mapped_column(TIMESTAMP(timezone=True), nullable=True)


class LoginChallenge(Base):
    __tablename__ = "login_challenges"
    challenge_hash: Mapped[bytes] = mapped_column(LargeBinary, primary_key=True)
    user_id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey("users.id", ondelete="CASCADE"),
        nullable=False,
    )
    issued_at: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), nullable=False)
    expires_at: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), nullable=False)
    consumed_at: Mapped[datetime | None] = mapped_column(TIMESTAMP(timezone=True), nullable=True)
    source_ip_hash: Mapped[str | None] = mapped_column(Text, nullable=True)
    user_agent_hash: Mapped[str | None] = mapped_column(Text, nullable=True)


class PendingTeamRegistration(Base):
    __tablename__ = "pending_team_registrations"
    id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), primary_key=True, default=uuid4)
    name: Mapped[str] = mapped_column(Text, nullable=False)
    contact_email: Mapped[str] = mapped_column(Text, nullable=False)
    status: Mapped[str] = mapped_column(
        Text,
        nullable=False,
        server_default=text("'pending'"),
        default="pending",
    )
    requested_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True),
        server_default=func.now(),
        nullable=False,
    )
    reviewed_at: Mapped[datetime | None] = mapped_column(
        TIMESTAMP(timezone=True),
        nullable=True,
    )
    reviewed_by_actor: Mapped[str | None] = mapped_column(Text, nullable=True)
    approved_team_id: Mapped[UUID | None] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey("teams.id"),
        nullable=True,
    )
    source_ip_hash: Mapped[str | None] = mapped_column(Text, nullable=True)
    user_agent_hash: Mapped[str | None] = mapped_column(Text, nullable=True)
    request_metadata: Mapped[dict[str, Any]] = mapped_column(
        "metadata",
        JSONB,
        nullable=False,
        server_default=text("'{}'::jsonb"),
        default=dict,
    )


class UserRegistrationRequest(Base):
    __tablename__ = "user_registration_requests"
    __table_args__ = (
        CheckConstraint(
            "role IN ('owner', 'member', 'viewer')",
            name="user_registration_requests_role_check",
        ),
        CheckConstraint(
            "status IN ('pending', 'approved', 'rejected', 'expired')",
            name="user_registration_requests_status_check",
        ),
        Index(
            "user_registration_requests_active_username_uidx",
            "username_normalized",
            unique=True,
            postgresql_where=text("status IN ('pending', 'approved')"),
        ),
    )

    id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), primary_key=True, default=uuid4)
    username: Mapped[str] = mapped_column(Text, nullable=False)
    username_normalized: Mapped[str] = mapped_column(Text, nullable=False)
    team_id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey("teams.id", ondelete="CASCADE"),
        nullable=False,
    )
    role: Mapped[str] = mapped_column(Text, nullable=False, default="member")
    status: Mapped[str] = mapped_column(
        Text,
        nullable=False,
        server_default=text("'pending'"),
        default="pending",
    )
    requested_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True),
        server_default=func.now(),
        nullable=False,
    )
    reviewed_at: Mapped[datetime | None] = mapped_column(TIMESTAMP(timezone=True), nullable=True)
    reviewed_by_user_id: Mapped[UUID | None] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey("users.id", ondelete="SET NULL"),
        nullable=True,
    )
    reviewed_by_actor: Mapped[str | None] = mapped_column(Text, nullable=True)
    user_id: Mapped[UUID | None] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey("users.id", ondelete="SET NULL"),
        nullable=True,
    )
    setup_token_prefix: Mapped[str | None] = mapped_column(Text, nullable=True)
    rejection_reason: Mapped[str | None] = mapped_column(Text, nullable=True)
    source_ip_hash: Mapped[str | None] = mapped_column(Text, nullable=True)
    user_agent_hash: Mapped[str | None] = mapped_column(Text, nullable=True)
    request_metadata: Mapped[dict[str, Any]] = mapped_column(
        "metadata",
        JSONB,
        nullable=False,
        server_default=text("'{}'::jsonb"),
        default=dict,
    )


class AccountActionToken(Base):
    __tablename__ = "account_action_tokens"
    __table_args__ = (
        CheckConstraint(
            "purpose IN ('setup_password', 'reset_password')",
            name="account_action_tokens_purpose_check",
        ),
        Index("account_action_tokens_user_purpose_idx", "user_id", "purpose"),
        Index("account_action_tokens_prefix_idx", "token_prefix"),
    )

    token_hash: Mapped[bytes] = mapped_column(LargeBinary, primary_key=True)
    token_prefix: Mapped[str] = mapped_column(Text, nullable=False)
    purpose: Mapped[str] = mapped_column(Text, nullable=False)
    user_id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey("users.id", ondelete="CASCADE"),
        nullable=False,
    )
    registration_request_id: Mapped[UUID | None] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey("user_registration_requests.id", ondelete="SET NULL"),
        nullable=True,
    )
    password_reset_request_id: Mapped[UUID | None] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey("password_reset_requests.id", ondelete="SET NULL"),
        nullable=True,
    )
    created_by_user_id: Mapped[UUID | None] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey("users.id", ondelete="SET NULL"),
        nullable=True,
    )
    issued_at: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), nullable=False)
    expires_at: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), nullable=False)
    consumed_at: Mapped[datetime | None] = mapped_column(TIMESTAMP(timezone=True), nullable=True)
    revoked_at: Mapped[datetime | None] = mapped_column(TIMESTAMP(timezone=True), nullable=True)


class PasswordResetRequest(Base):
    __tablename__ = "password_reset_requests"
    __table_args__ = (
        CheckConstraint(
            "status IN ('pending', 'approved', 'rejected', 'expired')",
            name="password_reset_requests_status_check",
        ),
        Index(
            "password_reset_requests_active_username_uidx",
            "username_normalized",
            unique=True,
            postgresql_where=text("status IN ('pending', 'approved')"),
        ),
    )

    id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), primary_key=True, default=uuid4)
    username: Mapped[str] = mapped_column(Text, nullable=False)
    username_normalized: Mapped[str] = mapped_column(Text, nullable=False)
    user_id: Mapped[UUID | None] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey("users.id", ondelete="SET NULL"),
        nullable=True,
    )
    status: Mapped[str] = mapped_column(
        Text,
        nullable=False,
        server_default=text("'pending'"),
        default="pending",
    )
    requested_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True),
        server_default=func.now(),
        nullable=False,
    )
    reviewed_at: Mapped[datetime | None] = mapped_column(TIMESTAMP(timezone=True), nullable=True)
    reviewed_by_user_id: Mapped[UUID | None] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey("users.id", ondelete="SET NULL"),
        nullable=True,
    )
    reviewed_by_actor: Mapped[str | None] = mapped_column(Text, nullable=True)
    reset_token_prefix: Mapped[str | None] = mapped_column(Text, nullable=True)
    rejection_reason: Mapped[str | None] = mapped_column(Text, nullable=True)
    source_ip_hash: Mapped[str | None] = mapped_column(Text, nullable=True)
    user_agent_hash: Mapped[str | None] = mapped_column(Text, nullable=True)


class AdminAuditEvent(Base):
    __tablename__ = "admin_audit_events"
    id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), primary_key=True, default=uuid4)
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True),
        server_default=func.now(),
        nullable=False,
    )
    actor: Mapped[str] = mapped_column(Text, nullable=False)
    action: Mapped[str] = mapped_column(Text, nullable=False)
    target_type: Mapped[str] = mapped_column(Text, nullable=False)
    target_id: Mapped[str] = mapped_column(Text, nullable=False)
    request_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    source_ip_hash: Mapped[str | None] = mapped_column(Text, nullable=True)
    user_agent_hash: Mapped[str | None] = mapped_column(Text, nullable=True)
    event_metadata: Mapped[dict[str, Any]] = mapped_column(
        "metadata",
        JSONB,
        nullable=False,
        server_default=text("jsonb_build_object()"),
        default=dict,
    )


class Task(Base):
    __tablename__ = "tasks"
    __table_args__ = (
        CheckConstraint(
            "task_set_id IS NULL OR benchmark_id IS NULL",
            name="tasks_benchmark_or_taskset_check",
        ),
        Index("tasks_task_set_id_idx", "task_set_id"),
    )
    id: Mapped[str] = mapped_column(String, primary_key=True)
    checksum: Mapped[str] = mapped_column(String, nullable=False)
    config: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    source: Mapped[str | None] = mapped_column(String, nullable=True)
    # Per-task SPDX license tag (Plan 13). NULL on hand-authored tasks;
    # benchmark-imported tasks always carry it.
    license: Mapped[str | None] = mapped_column(Text, nullable=True)
    # Parent benchmark, NULL for hand-authored tasks.
    benchmark_id: Mapped[str | None] = mapped_column(
        Text,
        ForeignKey("benchmarks.id"),
        nullable=True,
    )
    # Parent user TaskSet (#242 sub-plan 3). NULL for benchmark/hand-authored.
    task_set_id: Mapped[str | None] = mapped_column(
        Text,
        ForeignKey("task_sets.id", ondelete="SET NULL"),
        nullable=True,
    )
    # PR-1 (benchmark series): open-ended key→value metadata. Adapters
    # populate from upstream (year/exam/difficulty/topic/…). The SPA
    # uses these for the tag filter UI; the backend exposes a
    # discovery endpoint that walks distinct values per benchmark.
    tags: Mapped[dict[str, str]] = mapped_column(
        JSONB,
        nullable=False,
        server_default="{}",
    )
    # Immutable upstream identity for benchmark-imported tasks. Legacy tasks
    # default to an empty object; profile-specific importers populate it.
    source_provenance: Mapped[dict[str, Any]] = mapped_column(
        JSONB, nullable=False, server_default=text("'{}'::jsonb"), default=dict,
    )
    registered_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True),
        server_default=func.now(),
        nullable=False,
    )


class Benchmark(Base):
    """One row per registered benchmark suite (Plan 13)."""

    __tablename__ = "benchmarks"
    id: Mapped[str] = mapped_column(Text, primary_key=True)
    display_name: Mapped[str] = mapped_column(Text, nullable=False)
    upstream_kind: Mapped[str] = mapped_column(Text, nullable=False)
    upstream_locator: Mapped[str] = mapped_column(Text, nullable=False)
    upstream_revision: Mapped[str] = mapped_column(Text, nullable=False)
    license_spdx: Mapped[str] = mapped_column(Text, nullable=False)
    license_url: Mapped[str] = mapped_column(Text, nullable=False)
    splits: Mapped[list[str]] = mapped_column(ARRAY(Text), nullable=False)
    # PR-1 (benchmark series): groups related benchmarks for the SPA's
    # multi-select dropdown. Convention: "aime", "swe-bench", …; NULL =
    # standalone, not part of a series. Disjoint variants (AIME by year)
    # are siblings under the same series so group-select unions cleanly.
    series: Mapped[str | None] = mapped_column(
        String(64),
        nullable=True,
    )
    # Physical profile state. Historical profiles remain readable but cannot
    # be selected for new execution.
    execution_state: Mapped[str] = mapped_column(
        Text,
        nullable=False,
        server_default=text("'runnable'"),
        default="runnable",
    )
    profile_provenance: Mapped[dict[str, Any]] = mapped_column(
        JSONB,
        nullable=False,
        server_default=text("'{}'::jsonb"),
        default=dict,
    )
    imported_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True),
        server_default=func.now(),
        nullable=False,
    )
    imported_by: Mapped[str | None] = mapped_column(Text, nullable=True)


class BenchmarkAlias(Base):
    """Public selector for a runnable immutable benchmark profile."""

    __tablename__ = "benchmark_aliases"

    alias: Mapped[str] = mapped_column(Text, primary_key=True)
    benchmark_id: Mapped[str] = mapped_column(
        Text,
        ForeignKey("benchmarks.id", ondelete="RESTRICT"),
        nullable=False,
    )
    activated_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True),
        server_default=func.now(),
        nullable=False,
    )


class TaskSet(Base):
    """Team-owned user TaskSet"""

    __tablename__ = "task_sets"
    __table_args__ = (
        CheckConstraint(
            "visibility IN ('private')",
            name="task_sets_visibility_check",
        ),
        CheckConstraint(
            "status IN ('materializing', 'ready', 'partial', 'failed', 'deleted')",
            name="task_sets_status_check",
        ),
        CheckConstraint(
            "cardinality(intents) > 0 AND "
            "intents <@ ARRAY['trajectory_generation', 'evaluation']::text[]",
            name="task_sets_intents_check",
        ),
        CheckConstraint(
            "slug <> '' AND slug = trim(slug) AND slug !~ '[./\\\\]'",
            name="task_sets_slug_check",
        ),
        CheckConstraint(
            "id = 'ts/' || owning_team_id::text || '/' || slug",
            name="task_sets_id_namespace_check",
        ),
        CheckConstraint(
            "task_count >= 0",
            name="task_sets_task_count_nonneg_check",
        ),
        UniqueConstraint("owning_team_id", "slug", name="task_sets_team_slug_uidx"),
        Index(
            "task_sets_team_visibility_status_idx",
            "owning_team_id",
            "visibility",
            "status",
        ),
        Index(
            "task_sets_evaluation_ready_idx",
            "evaluation_ready",
            postgresql_where=text("evaluation_ready = true"),
        ),
    )
    id: Mapped[str] = mapped_column(Text, primary_key=True)
    owning_team_id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey("teams.id"),
        nullable=False,
    )
    slug: Mapped[str] = mapped_column(Text, nullable=False)
    display_name: Mapped[str] = mapped_column(Text, nullable=False)
    visibility: Mapped[str] = mapped_column(
        Text,
        nullable=False,
        server_default=text("'private'"),
    )
    status: Mapped[str] = mapped_column(Text, nullable=False)
    status_reason: Mapped[str | None] = mapped_column(Text, nullable=True)
    intents: Mapped[list[str]] = mapped_column(ARRAY(Text), nullable=False)
    evaluation_ready: Mapped[bool] = mapped_column(
        Boolean,
        nullable=False,
        server_default=text("false"),
    )
    manifest_blob_uri: Mapped[str] = mapped_column(Text, nullable=False)
    task_count: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        server_default=text("0"),
    )
    soft_deleted_at: Mapped[datetime | None] = mapped_column(
        TIMESTAMP(timezone=True),
        nullable=True,
    )
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True),
        server_default=func.now(),
        nullable=False,
    )
    updated_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True),
        server_default=func.now(),
        nullable=False,
    )


class TaskSetManifest(Base):
    """Current manifest sidecar for a user TaskSet."""

    __tablename__ = "task_set_manifests"
    task_set_id: Mapped[str] = mapped_column(
        Text,
        ForeignKey("task_sets.id", ondelete="CASCADE"),
        primary_key=True,
    )
    schema_version: Mapped[int] = mapped_column(Integer, nullable=False)
    manifest: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    verifier_blob_uri: Mapped[str | None] = mapped_column(Text, nullable=True)
    transform_blob_uri: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True),
        server_default=func.now(),
        nullable=False,
    )
    updated_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True),
        server_default=func.now(),
        nullable=False,
    )


class TaskSetMaterializationJob(Base):
    """Queued materialization work for a user TaskSet (#242 sub-plan 2)."""

    __tablename__ = "task_set_materialization_jobs"
    __table_args__ = (
        CheckConstraint(
            "state IN ('queued', 'claimed', 'running', 'succeeded', 'failed', 'cancelled')",
            name="task_set_materialization_jobs_state_check",
        ),
        Index(
            "task_set_materialization_jobs_active_uidx",
            "task_set_id",
            unique=True,
            postgresql_where=text(
                "state IN ('queued', 'claimed', 'running')",
            ),
        ),
        Index(
            "task_set_materialization_jobs_queued_idx",
            "enqueued_at",
            postgresql_where=text("state = 'queued'"),
        ),
        Index(
            "task_set_materialization_jobs_active_heartbeat_idx",
            "lease_heartbeat_at",
            postgresql_where=text(
                "state IN ('claimed', 'running') AND lease_heartbeat_at IS NOT NULL",
            ),
        ),
    )
    id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True),
        primary_key=True,
        server_default=text("gen_random_uuid()"),
        default=uuid4,
    )
    task_set_id: Mapped[str] = mapped_column(
        Text,
        ForeignKey("task_sets.id", ondelete="CASCADE"),
        nullable=False,
    )
    owning_team_id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey("teams.id"),
        nullable=False,
    )
    state: Mapped[str] = mapped_column(Text, nullable=False)
    attempt_count: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        server_default=text("0"),
    )
    max_attempts: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        server_default=text("3"),
    )
    next_attempt_at: Mapped[datetime | None] = mapped_column(
        TIMESTAMP(timezone=True),
        nullable=True,
    )
    enqueued_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True),
        server_default=func.now(),
        nullable=False,
    )
    claimed_at: Mapped[datetime | None] = mapped_column(
        TIMESTAMP(timezone=True),
        nullable=True,
    )
    lease_epoch: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        server_default=text("0"),
    )
    lease_heartbeat_at: Mapped[datetime | None] = mapped_column(
        TIMESTAMP(timezone=True),
        nullable=True,
    )
    published_materialization_generation: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        server_default=text("0"),
    )
    started_at: Mapped[datetime | None] = mapped_column(
        TIMESTAMP(timezone=True),
        nullable=True,
    )
    finished_at: Mapped[datetime | None] = mapped_column(
        TIMESTAMP(timezone=True),
        nullable=True,
    )
    claimed_by: Mapped[str | None] = mapped_column(Text, nullable=True)
    failure_reason: Mapped[str | None] = mapped_column(Text, nullable=True)
    failure_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    error_summary: Mapped[list[Any]] = mapped_column(
        JSONB,
        nullable=False,
        server_default=text("'[]'::jsonb"),
    )
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True),
        server_default=func.now(),
        nullable=False,
    )
    updated_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True),
        server_default=func.now(),
        nullable=False,
    )


class TaskSetFenceCanaryAuthorization(Base):
    """One deployment-created, one-use authority for the #756 canary."""

    __tablename__ = "task_set_fence_canary_authorizations"
    __table_args__ = (
        CheckConstraint(
            "candidate_sha ~ '^[0-9a-f]{40}$'",
            name="task_set_fence_canary_authorizations_candidate_sha_check",
        ),
        CheckConstraint(
            "image_tag ~ '^staging(-[a-z0-9][a-z0-9_-]*)?-[0-9a-f]{7}$'",
            name="task_set_fence_canary_authorizations_image_tag_check",
        ),
        CheckConstraint(
            "expected_task_checksum ~ '^[0-9a-f]{64}$'",
            name="task_set_fence_canary_authorizations_checksum_check",
        ),
        CheckConstraint(
            "octet_length(nonce_digest) = 32",
            name="task_set_fence_canary_authorizations_nonce_digest_check",
        ),
        CheckConstraint(
            "(consumed_at IS NULL AND consumed_lease_epoch IS NULL) OR "
            "(consumed_at IS NOT NULL AND consumed_lease_epoch > 0)",
            name="task_set_fence_canary_authorizations_consumption_check",
        ),
        UniqueConstraint(
            "materialization_job_id",
            name="task_set_fence_canary_authorizations_job_uidx",
        ),
    )

    task_set_id: Mapped[str] = mapped_column(
        Text,
        ForeignKey("task_sets.id", ondelete="CASCADE"),
        primary_key=True,
    )
    materialization_job_id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey("task_set_materialization_jobs.id", ondelete="CASCADE"),
        nullable=False,
    )
    candidate_sha: Mapped[str] = mapped_column(String(40), nullable=False)
    image_tag: Mapped[str] = mapped_column(String(64), nullable=False)
    expected_task_checksum: Mapped[str] = mapped_column(String(64), nullable=False)
    nonce_digest: Mapped[bytes] = mapped_column(LargeBinary, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True),
        server_default=func.now(),
        nullable=False,
    )
    consumed_at: Mapped[datetime | None] = mapped_column(
        TIMESTAMP(timezone=True),
        nullable=True,
    )
    consumed_lease_epoch: Mapped[int | None] = mapped_column(Integer, nullable=True)


class TaskSetGenerationGcCursor(Base):
    """Scheduling-only progress for bounded live-generation reconciliation."""

    __tablename__ = "task_set_generation_gc_cursors"
    __table_args__ = (
        CheckConstraint(
            "name = 'live-generation-gc'",
            name="task_set_generation_gc_cursors_name_check",
        ),
        CheckConstraint(
            "next_sweep >= 0",
            name="task_set_generation_gc_cursors_next_sweep_nonneg_check",
        ),
    )

    name: Mapped[str] = mapped_column(Text, primary_key=True)
    next_sweep: Mapped[int] = mapped_column(
        BigInteger,
        nullable=False,
        server_default=text("0"),
    )


class Agent(Base):
    __tablename__ = "agents"
    name: Mapped[str] = mapped_column(String, primary_key=True)
    version: Mapped[str] = mapped_column(String, primary_key=True)
    mode: Mapped[str] = mapped_column(String, nullable=False)
    spec: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)


class Worker(Base):
    __tablename__ = "workers"
    __table_args__ = (
        CheckConstraint(
            "drain_state IN ('active', 'draining', 'drained')",
            name="workers_drain_state_check",
        ),
        Index("idx_workers_drain_state", "drain_state"),
    )
    id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), primary_key=True, default=uuid4)
    hostname: Mapped[str] = mapped_column(String, nullable=False)
    version: Mapped[str] = mapped_column(String, nullable=False)
    capabilities: Mapped[list[dict[str, Any]]] = mapped_column(JSONB, nullable=False)
    max_concurrent: Mapped[int] = mapped_column(Integer, nullable=False, server_default=text("1"))
    pool_name: Mapped[str] = mapped_column(Text, nullable=False, server_default=text("'default'"))
    drain_state: Mapped[str] = mapped_column(
        Text,
        nullable=False,
        server_default=text("'active'"),
        default="active",
    )
    drain_requested_at: Mapped[datetime | None] = mapped_column(
        TIMESTAMP(timezone=True),
        nullable=True,
    )
    drain_reason: Mapped[str | None] = mapped_column(Text, nullable=True)
    drain_owner: Mapped[str | None] = mapped_column(Text, nullable=True)
    registered_at: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), nullable=False)
    last_seen_at: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), nullable=False)
    status: Mapped[str] = mapped_column(String, nullable=False)


class SlurmWorkerJob(Base):
    __tablename__ = "slurm_worker_jobs"
    __table_args__ = (
        CheckConstraint(
            "state IN ('pending', 'running', 'completed', 'failed', 'cancelled', 'stale')",
            name="slurm_worker_jobs_state_check",
        ),
        CheckConstraint(
            "requested_cpus IS NULL OR requested_cpus > 0",
            name="slurm_worker_jobs_requested_cpus_positive_check",
        ),
        CheckConstraint(
            "requested_memory_mib IS NULL OR requested_memory_mib > 0",
            name="slurm_worker_jobs_requested_memory_positive_check",
        ),
        CheckConstraint(
            "requested_concurrency > 0",
            name="slurm_worker_jobs_requested_concurrency_positive_check",
        ),
        Index(
            "slurm_worker_jobs_job_id_uidx",
            "job_id",
            unique=True,
            postgresql_where=text("job_id IS NOT NULL"),
        ),
        Index(
            "slurm_worker_jobs_active_capacity_uidx",
            "environment",
            "pool_name",
            "nodelist",
            text("coalesce(requested_cpus, -1)"),
            text("coalesce(requested_memory_mib, -1)"),
            "requested_concurrency",
            unique=True,
            postgresql_where=text("state IN ('pending', 'running')"),
        ),
    )

    id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), primary_key=True, default=uuid4)
    environment: Mapped[str] = mapped_column(Text, nullable=False)
    pool_name: Mapped[str] = mapped_column(Text, nullable=False)
    nodelist: Mapped[str] = mapped_column(Text, nullable=False)
    requested_cpus: Mapped[int | None] = mapped_column(Integer, nullable=True)
    requested_memory_mib: Mapped[int | None] = mapped_column(Integer, nullable=True)
    requested_concurrency: Mapped[int] = mapped_column(Integer, nullable=False)
    job_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    slurm_state: Mapped[str | None] = mapped_column(Text, nullable=True)
    state: Mapped[str] = mapped_column(
        Text,
        nullable=False,
        server_default=text("'pending'"),
        default="pending",
    )
    pending_reason: Mapped[str | None] = mapped_column(Text, nullable=True)
    worker_id: Mapped[UUID | None] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey("workers.id", ondelete="SET NULL"),
        nullable=True,
    )
    redacted_env: Mapped[dict[str, str]] = mapped_column(
        JSONB,
        nullable=False,
        server_default=text("'{}'::jsonb"),
        default=dict,
    )
    submission_error: Mapped[str | None] = mapped_column(Text, nullable=True)
    submitted_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True),
        server_default=func.now(),
        nullable=False,
    )
    started_at: Mapped[datetime | None] = mapped_column(TIMESTAMP(timezone=True), nullable=True)
    finished_at: Mapped[datetime | None] = mapped_column(TIMESTAMP(timezone=True), nullable=True)
    last_reconciled_at: Mapped[datetime | None] = mapped_column(
        TIMESTAMP(timezone=True),
        nullable=True,
    )
    stale_at: Mapped[datetime | None] = mapped_column(TIMESTAMP(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True),
        server_default=func.now(),
        nullable=False,
    )
    updated_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True),
        server_default=func.now(),
        nullable=False,
    )


class GB10WorkerPoolDesiredState(Base):
    __tablename__ = "gb10_worker_pool_desired_states"
    __table_args__ = (
        CheckConstraint(
            "length(trim(environment)) > 0",
            name="gb10_worker_pool_desired_states_environment_nonempty_check",
        ),
        CheckConstraint(
            "length(trim(pool_name)) > 0",
            name="gb10_worker_pool_desired_states_pool_name_nonempty_check",
        ),
        CheckConstraint(
            "length(trim(image_tag)) > 0",
            name="gb10_worker_pool_desired_states_image_tag_nonempty_check",
        ),
        CheckConstraint(
            "max_concurrent > 0",
            name="gb10_worker_pool_desired_states_max_concurrent_positive_check",
        ),
        CheckConstraint(
            "length(trim(env_config_version)) > 0",
            name="gb10_worker_pool_desired_states_env_version_nonempty_check",
        ),
        UniqueConstraint(
            "environment",
            "pool_name",
            name="gb10_worker_pool_desired_states_environment_pool_uidx",
        ),
    )

    id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), primary_key=True, default=uuid4)
    environment: Mapped[str] = mapped_column(Text, nullable=False)
    pool_name: Mapped[str] = mapped_column(Text, nullable=False)
    image_tag: Mapped[str] = mapped_column(Text, nullable=False)
    max_concurrent: Mapped[int] = mapped_column(Integer, nullable=False)
    env_config_version: Mapped[str] = mapped_column(Text, nullable=False)
    source_git_commit: Mapped[str | None] = mapped_column(Text, nullable=True)
    rollout_policy: Mapped[dict[str, Any]] = mapped_column(
        JSONB,
        nullable=False,
        server_default=text("'{}'::jsonb"),
        default=dict,
    )
    env: Mapped[dict[str, str]] = mapped_column(
        JSONB,
        nullable=False,
        server_default=text("'{}'::jsonb"),
        default=dict,
    )
    target_slots: Mapped[int | None] = mapped_column(Integer, nullable=True)
    host_intents: Mapped[dict[str, str]] = mapped_column(
        JSONB,
        nullable=False,
        server_default=text("'{}'::jsonb"),
        default=dict,
    )
    force: Mapped[bool] = mapped_column(Boolean, nullable=False, server_default=text("false"))
    previous_image_tag: Mapped[str | None] = mapped_column(Text, nullable=True)
    previous_max_concurrent: Mapped[int | None] = mapped_column(Integer, nullable=True)
    previous_env_config_version: Mapped[str | None] = mapped_column(Text, nullable=True)
    previous_source_git_commit: Mapped[str | None] = mapped_column(Text, nullable=True)
    previous_env: Mapped[dict[str, str]] = mapped_column(
        JSONB,
        nullable=False,
        server_default=text("'{}'::jsonb"),
        default=dict,
    )
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True),
        server_default=func.now(),
        nullable=False,
    )
    updated_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True),
        server_default=func.now(),
        nullable=False,
    )


class GB10WorkerNodeStatus(Base):
    __tablename__ = "gb10_worker_node_statuses"
    __table_args__ = (
        CheckConstraint(
            "length(trim(environment)) > 0",
            name="gb10_worker_node_statuses_environment_nonempty_check",
        ),
        CheckConstraint(
            "length(trim(pool_name)) > 0",
            name="gb10_worker_node_statuses_pool_name_nonempty_check",
        ),
        CheckConstraint(
            "length(trim(hostname)) > 0",
            name="gb10_worker_node_statuses_hostname_nonempty_check",
        ),
        CheckConstraint(
            "current_max_concurrent IS NULL OR current_max_concurrent > 0",
            name="gb10_worker_node_statuses_current_max_positive_check",
        ),
        CheckConstraint(
            "desired_max_concurrent IS NULL OR desired_max_concurrent > 0",
            name="gb10_worker_node_statuses_desired_max_positive_check",
        ),
        CheckConstraint(
            "apply_state IN ('unknown', 'idle', 'applying', 'draining', 'stopped', 'applied', 'blocked', 'failed', 'rolled_back')",
            name="gb10_worker_node_statuses_apply_state_check",
        ),
        UniqueConstraint(
            "environment",
            "pool_name",
            "hostname",
            name="gb10_worker_node_statuses_environment_pool_host_uidx",
        ),
    )

    id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), primary_key=True, default=uuid4)
    environment: Mapped[str] = mapped_column(Text, nullable=False)
    pool_name: Mapped[str] = mapped_column(Text, nullable=False)
    hostname: Mapped[str] = mapped_column(Text, nullable=False)
    worker_id: Mapped[UUID | None] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey("workers.id", ondelete="SET NULL"),
        nullable=True,
    )
    current_image_tag: Mapped[str | None] = mapped_column(Text, nullable=True)
    current_max_concurrent: Mapped[int | None] = mapped_column(Integer, nullable=True)
    current_env_config_version: Mapped[str | None] = mapped_column(Text, nullable=True)
    desired_image_tag: Mapped[str | None] = mapped_column(Text, nullable=True)
    desired_max_concurrent: Mapped[int | None] = mapped_column(Integer, nullable=True)
    desired_env_config_version: Mapped[str | None] = mapped_column(Text, nullable=True)
    desired_source_git_commit: Mapped[str | None] = mapped_column(Text, nullable=True)
    desired_intent: Mapped[str | None] = mapped_column(Text, nullable=True)
    current_intent: Mapped[str | None] = mapped_column(Text, nullable=True)
    apply_state: Mapped[str] = mapped_column(
        Text,
        nullable=False,
        server_default=text("'unknown'"),
        default="unknown",
    )
    last_apply_result: Mapped[str | None] = mapped_column(Text, nullable=True)
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    agent_version: Mapped[str | None] = mapped_column(Text, nullable=True)
    compose_project_dir: Mapped[str | None] = mapped_column(Text, nullable=True)
    source_git_commit: Mapped[str | None] = mapped_column(Text, nullable=True)
    source_git_dirty: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    last_heartbeat_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True),
        server_default=func.now(),
        nullable=False,
    )
    last_apply_at: Mapped[datetime | None] = mapped_column(
        TIMESTAMP(timezone=True),
        nullable=True,
    )
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True),
        server_default=func.now(),
        nullable=False,
    )
    updated_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True),
        server_default=func.now(),
        nullable=False,
    )


class WorkerPoolAutoscalerPolicy(Base):
    __tablename__ = "worker_pool_autoscaler_policies"
    __table_args__ = (
        CheckConstraint(
            "length(trim(environment)) > 0",
            name="worker_pool_autoscaler_policies_environment_nonempty_check",
        ),
        CheckConstraint(
            "length(trim(pool_name)) > 0",
            name="worker_pool_autoscaler_policies_pool_name_nonempty_check",
        ),
        CheckConstraint(
            "actuator IN ('slurm', 'gb10')",
            name="worker_pool_autoscaler_policies_actuator_check",
        ),
        CheckConstraint(
            "min_slots >= 0",
            name="worker_pool_autoscaler_policies_min_slots_nonnegative_check",
        ),
        CheckConstraint(
            "max_slots >= min_slots",
            name="worker_pool_autoscaler_policies_max_slots_check",
        ),
        CheckConstraint(
            "scale_up_threshold_slots >= 0",
            name="worker_pool_autoscaler_policies_scale_up_threshold_check",
        ),
        CheckConstraint(
            "scale_down_idle_seconds >= 0",
            name="worker_pool_autoscaler_policies_scale_down_idle_check",
        ),
        CheckConstraint(
            "scale_up_cooldown_seconds >= 0",
            name="worker_pool_autoscaler_policies_scale_up_cooldown_check",
        ),
        CheckConstraint(
            "scale_down_cooldown_seconds >= 0",
            name="worker_pool_autoscaler_policies_scale_down_cooldown_check",
        ),
        CheckConstraint(
            "drain_timeout_seconds > 0",
            name="worker_pool_autoscaler_policies_drain_timeout_positive_check",
        ),
        UniqueConstraint(
            "environment",
            "pool_name",
            name="worker_pool_autoscaler_policies_environment_pool_uidx",
        ),
    )

    id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), primary_key=True, default=uuid4)
    environment: Mapped[str] = mapped_column(Text, nullable=False)
    pool_name: Mapped[str] = mapped_column(Text, nullable=False)
    actuator: Mapped[str] = mapped_column(Text, nullable=False)
    enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, server_default=text("false"))
    min_slots: Mapped[int] = mapped_column(Integer, nullable=False, server_default=text("0"))
    max_slots: Mapped[int] = mapped_column(Integer, nullable=False)
    scale_up_threshold_slots: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        server_default=text("1"),
    )
    scale_down_idle_seconds: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        server_default=text("600"),
    )
    scale_up_cooldown_seconds: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        server_default=text("60"),
    )
    scale_down_cooldown_seconds: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        server_default=text("300"),
    )
    drain_timeout_seconds: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        server_default=text("600"),
    )
    force: Mapped[bool] = mapped_column(Boolean, nullable=False, server_default=text("false"))
    disabled_reason: Mapped[str | None] = mapped_column(Text, nullable=True)
    actuator_config: Mapped[dict[str, Any]] = mapped_column(
        JSONB,
        nullable=False,
        server_default=text("'{}'::jsonb"),
        default=dict,
    )
    # Prod-pressure drain intent for this (environment, pool). The CP request
    # handler is the sole writer of this field; for actuator="slurm" the
    # external autoscaler actor reads it to scancel/release SlurmWorkerJobs, and
    # the scheduler claim path reads it to fence new claims. NULL = no active
    # prod-pressure drain. See #892.
    prod_pressure_state: Mapped[dict[str, Any] | None] = mapped_column(
        JSONB,
        nullable=True,
    )
    idle_since_at: Mapped[datetime | None] = mapped_column(
        TIMESTAMP(timezone=True),
        nullable=True,
    )
    last_decision: Mapped[str | None] = mapped_column(Text, nullable=True)
    last_decision_reason: Mapped[str | None] = mapped_column(Text, nullable=True)
    last_desired_slots: Mapped[int | None] = mapped_column(Integer, nullable=True)
    last_actual_slots: Mapped[int | None] = mapped_column(Integer, nullable=True)
    last_pending_slots: Mapped[int | None] = mapped_column(Integer, nullable=True)
    last_draining_slots: Mapped[int | None] = mapped_column(Integer, nullable=True)
    last_occupied_slots: Mapped[int | None] = mapped_column(Integer, nullable=True)
    last_queued_slots: Mapped[int | None] = mapped_column(Integer, nullable=True)
    last_blocked_reason: Mapped[str | None] = mapped_column(Text, nullable=True)
    last_blocked_details: Mapped[dict[str, Any] | None] = mapped_column(JSONB, nullable=True)
    last_error: Mapped[str | None] = mapped_column(Text, nullable=True)
    last_scale_up_at: Mapped[datetime | None] = mapped_column(
        TIMESTAMP(timezone=True),
        nullable=True,
    )
    last_scale_down_at: Mapped[datetime | None] = mapped_column(
        TIMESTAMP(timezone=True),
        nullable=True,
    )
    last_decision_at: Mapped[datetime | None] = mapped_column(
        TIMESTAMP(timezone=True),
        nullable=True,
    )
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True),
        server_default=func.now(),
        nullable=False,
    )
    updated_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True),
        server_default=func.now(),
        nullable=False,
    )


class Batch(Base):
    """One row per submitted batch (Plan 19 + Plan 28 rename).

    The runner fans out a batch's `task_filter` into N trial
    submissions and back-links each via `trials.batch_id`. State
    transitions: submitted → running → finished | cancelled.

    Renamed from `Campaign` (and table `campaigns`) in migration 0011
    so the SPA, docs, and code all share Harbor's vocabulary
    (Task / Trial / Batch / Benchmark) without per-layer translation.
    """

    __tablename__ = "batches"
    __table_args__ = (
        CheckConstraint(
            "budget_policy IN ('none', 'soft', 'hard')",
            name="batches_budget_policy_check",
        ),
        CheckConstraint(
            "budget_usd IS NULL OR budget_usd >= 0",
            name="batches_budget_usd_nonnegative_check",
        ),
    )
    id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True),
        primary_key=True,
        server_default=text("gen_random_uuid()"),
    )
    team_id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey("teams.id"),
        nullable=False,
    )
    name: Mapped[str] = mapped_column(Text, nullable=False)
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    task_filter: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    trial_config: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    state: Mapped[str] = mapped_column(
        Text,
        nullable=False,
        server_default=text("'submitted'"),
    )
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True),
        server_default=func.now(),
        nullable=False,
    )
    finished_at: Mapped[datetime | None] = mapped_column(
        TIMESTAMP(timezone=True),
        nullable=True,
    )
    created_by_token_prefix: Mapped[str] = mapped_column(Text, nullable=False)
    submitted_by_user_id: Mapped[UUID | None] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey("users.id", ondelete="SET NULL"),
        nullable=True,
    )
    usage_attributed_user_id: Mapped[UUID | None] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey("users.id", ondelete="SET NULL"),
        nullable=True,
    )
    usage_attributed_actor: Mapped[str | None] = mapped_column(Text, nullable=True)
    expected_trial_count: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        default=0,
    )
    # Plan 23: n-sampling. Runner submits n_per_task trials per matched
    # task; expected_trial_count = len(task_ids) * n_per_task.
    # When `combinations` is non-empty, this `n_per_task` is ignored —
    # each Combination carries its own n_per_task.
    n_per_task: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        server_default=text("1"),
        default=1,
    )
    # Plan 28 PR-3: backend selection at the batch level. Catalog
    # lives at `/api/v1/backends` (derived from worker capabilities).
    backend: Mapped[str] = mapped_column(
        Text,
        nullable=False,
        server_default=text("'docker'"),
        default="docker",
    )
    # Plan 28 PR-3: multi-(agent, model) combinations. Each entry is
    # `{agent_name, agent_model, n_per_task, label?}`. Empty list ⇒
    # single-combination behaviour (agent + model + n_per_task live
    # on trial_config / Batch.n_per_task as before).
    combinations: Mapped[list[dict[str, Any]]] = mapped_column(
        JSONB,
        nullable=False,
        server_default=text("'[]'::jsonb"),
        default=list,
    )
    # Issue #188: release canaries can request deterministic terminal
    # coverage on named worker pools. The batch runner emits one extra
    # pool-pinned coverage trial per entry; ordinary batches keep [].
    required_worker_pools: Mapped[list[str]] = mapped_column(
        JSONB,
        nullable=False,
        server_default=text("'[]'::jsonb"),
        default=list,
    )
    # Plan 28 PR-3: outcome separate from lifecycle `status`. NULL
    # until terminal. Computed by the batch_runner when transitioning
    # to a terminal lifecycle state. Values: succeeded /
    # partial_failed / all_failed / cancelled.
    result_status: Mapped[str | None] = mapped_column(
        Text,
        nullable=True,
    )
    # Fan-out failures happen before Control Plane accepts a child Trial.
    # Store them on the batch for retry suppression and user diagnostics.
    fanout_errors: Mapped[list[dict[str, Any]]] = mapped_column(
        JSONB,
        nullable=False,
        server_default=text("'[]'::jsonb"),
        default=list,
    )
    # Issue #298: failed-case reruns are represented as ordinary child
    # batches with exact trial coordinates to re-submit. The parent batch
    # remains immutable; service/UI detail views compute an effective rollup.
    rerun_of_batch_id: Mapped[UUID | None] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey("batches.id", ondelete="SET NULL"),
        nullable=True,
    )
    rerun_targets: Mapped[list[dict[str, Any]]] = mapped_column(
        JSONB,
        nullable=False,
        server_default=text("jsonb_build_array()"),
        default=list,
    )
    # cluster-deploy.md §Schema additions: per-batch provider override.
    # When set, the gateway uses this connection (decrypts its API key,
    # forwards to base_url) for every trial in this batch instead of
    # the platform default. NULL = use platform default. The Trial
    # row's same-named column overrides this if both are set (per-trial
    # specificity wins).
    provider_connection_id: Mapped[UUID | None] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey("provider_connections.id"),
        nullable=True,
    )
    provider_model_id: Mapped[str | None] = mapped_column(
        Text,
        nullable=True,
    )
    budget_usd: Mapped[Decimal | None] = mapped_column(
        Numeric(12, 6),
        nullable=True,
    )
    budget_policy: Mapped[str] = mapped_column(
        Text,
        nullable=False,
        server_default=text("'none'"),
        default="none",
    )
    budget_confirmed_at: Mapped[datetime | None] = mapped_column(
        TIMESTAMP(timezone=True),
        nullable=True,
    )
    pre_run_estimated_cost_usd: Mapped[Decimal | None] = mapped_column(
        Numeric(12, 6),
        nullable=True,
    )
    pre_run_cost_estimate_source: Mapped[str | None] = mapped_column(
        Text,
        nullable=True,
    )
    pre_run_cost_estimate_confidence: Mapped[str | None] = mapped_column(
        Text,
        nullable=True,
    )
    budget_diagnostics: Mapped[list[dict[str, Any]]] = mapped_column(
        JSONB,
        nullable=False,
        server_default=text("'[]'::jsonb"),
        default=list,
    )
    # Issue #336: completed-run sharing is org-visible by default.
    # Team/private keeps the run in the owner team's boundary; org +
    # shared makes safe metadata visible in the org-wide Run Library.
    visibility: Mapped[str] = mapped_column(
        Text,
        nullable=False,
        server_default=text("'org'"),
        default="org",
    )
    share_status: Mapped[str] = mapped_column(
        Text,
        nullable=False,
        server_default=text("'shared'"),
        default="shared",
    )
    source_provenance: Mapped[list[dict[str, Any]]] = mapped_column(
        JSONB,
        nullable=False,
        server_default=text("'[]'::jsonb"),
        default=list,
    )
    # #672 family-runs: resolved spec persisted at batch-accept time;
    # NULL for non-family-run batches. See `docs/architecture/family-runs.md`.
    family_run_spec: Mapped[dict[str, Any] | None] = mapped_column(
        JSONB,
        nullable=True,
    )
    # NULL keeps pre-profile batches on their original selector semantics.
    # Non-empty lists are an immutable task-selection snapshot for new batches.
    resolved_task_ids: Mapped[list[str] | None] = mapped_column(JSONB, nullable=True)
    lifecycle_authority_id: Mapped[UUID | None] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey("data_lifecycle_authorities.id", ondelete="RESTRICT"),
        nullable=True,
        index=True,
    )

    @property
    def failure_reason(self) -> str | None:
        return "fanout_submit_failed" if self.fanout_errors else None

    @property
    def failure_message(self) -> str | None:
        if not self.fanout_errors:
            return None
        first = self.fanout_errors[0]
        task_id = first.get("task_id", "unknown task")
        status_code = first.get("status_code", "unknown status")
        detail = first.get("detail") or first.get("message") or "no detail"
        if len(self.fanout_errors) == 1:
            return f"task {task_id} submit failed: HTTP {status_code}: {detail}"
        return (
            f"{len(self.fanout_errors)} trial submissions failed; "
            f"first: task {task_id} HTTP {status_code}: {detail}"
        )


class Trial(Base):
    __tablename__ = "trials"
    __table_args__ = (
        # #416 Slice 4: a terminal-successful trial must carry its
        # TrialResult. Writeback in `loom_worker.trial_runner` already
        # patches `result` before the `state='succeeded'` transition;
        # this constraint pins the invariant at the DB so any future
        # writeback regression fails fast at PATCH time instead of
        # producing rows the SPA/ATIF/#426 reward gate can't consume.
        CheckConstraint(
            "state != 'succeeded' OR result IS NOT NULL",
            name="trials_succeeded_has_result",
        ),
    )
    id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), primary_key=True, default=uuid4)
    team_id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True), ForeignKey("teams.id"), nullable=False
    )
    task_id: Mapped[str] = mapped_column(String, ForeignKey("tasks.id"), nullable=False)
    config: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    requires_caps: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    state: Mapped[str] = mapped_column(String, nullable=False)
    failure_reason: Mapped[str | None] = mapped_column(String, nullable=True)
    failure_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    submit_priority: Mapped[int] = mapped_column(Integer, nullable=False, default=100)
    submitted_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True),
        server_default=func.now(),
        nullable=False,
    )
    claimed_at: Mapped[datetime | None] = mapped_column(TIMESTAMP(timezone=True), nullable=True)
    pre_start_heartbeat_at: Mapped[datetime | None] = mapped_column(
        TIMESTAMP(timezone=True), nullable=True
    )
    started_at: Mapped[datetime | None] = mapped_column(TIMESTAMP(timezone=True), nullable=True)
    finished_at: Mapped[datetime | None] = mapped_column(TIMESTAMP(timezone=True), nullable=True)
    cancellation_requested_at: Mapped[datetime | None] = mapped_column(
        TIMESTAMP(timezone=True), nullable=True
    )
    cancellation_observed_at: Mapped[datetime | None] = mapped_column(
        TIMESTAMP(timezone=True), nullable=True
    )
    worker_id: Mapped[UUID | None] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey("workers.id"),
        nullable=True,
    )
    attempt_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    next_attempt_at: Mapped[datetime | None] = mapped_column(
        TIMESTAMP(timezone=True), nullable=True
    )
    result: Mapped[dict[str, Any] | None] = mapped_column(JSONB, nullable=True)
    trajectory_index: Mapped[dict[str, Any] | None] = mapped_column(JSONB, nullable=True)
    # Plan 19: batch back-link + idempotency key. batch_id is
    # NULL for hand-submitted trials. idempotency_key uniqueness is
    # enforced via a partial unique index (`trials_idempotency_key_uidx`).
    batch_id: Mapped[UUID | None] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey("batches.id", ondelete="SET NULL"),
        nullable=True,
    )
    idempotency_key: Mapped[str | None] = mapped_column(Text, nullable=True)
    # Plan 23: which sample within (batch_id, task_id) this trial is.
    # Pairs with batch.n_per_task to support n-sampling fan-out.
    # 0 for hand-submitted trials and the only sample of a 1-per-task
    # batch — preserves pre-migration semantics.
    sample_idx: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        server_default=text("0"),
        default=0,
    )
    # Plan 28 PR-3: which Combination this trial belongs to within
    # its parent Batch. 0 for single-combination batches (matches
    # the pre-migration behaviour exactly).
    combination_idx: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        server_default=text("0"),
        default=0,
    )
    # cluster-deploy.md §Schema additions: per-trial provider override.
    # When set, the gateway uses this connection's decrypted API key
    # + base_url instead of the platform default. NULL = inherit from
    # parent Batch (also nullable) or platform default. Wins over the
    # Batch.provider_connection_id when both are set (per-trial
    # specificity).
    #
    # No cascade rule: provider_connections never hard-deletes today
    # (team_id FK is ON DELETE RESTRICT per migration 0018 self-
    # review). Soft-delete on the parent leaves this FK valid;
    # historical trials retain attribution for billing/audit.
    provider_connection_id: Mapped[UUID | None] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey("provider_connections.id"),
        nullable=True,
    )
    provider_model_id: Mapped[str | None] = mapped_column(
        Text,
        nullable=True,
    )
    submitted_by_user_id: Mapped[UUID | None] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey("users.id", ondelete="SET NULL"),
        nullable=True,
    )
    usage_attributed_user_id: Mapped[UUID | None] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey("users.id", ondelete="SET NULL"),
        nullable=True,
    )
    usage_attributed_actor: Mapped[str | None] = mapped_column(Text, nullable=True)
    visibility: Mapped[str] = mapped_column(
        Text,
        nullable=False,
        server_default=text("'org'"),
        default="org",
    )
    share_status: Mapped[str] = mapped_column(
        Text,
        nullable=False,
        server_default=text("'shared'"),
        default="shared",
    )
    source_provenance: Mapped[list[dict[str, Any]]] = mapped_column(
        JSONB,
        nullable=False,
        server_default=text("'[]'::jsonb"),
        default=list,
    )
    # #672 family-runs: populated when the batch opts into family-run
    # mode. Groups this trial with siblings in the same family so the
    # scheduler predicate can serialise them. NULL for non-family trials.
    family_key: Mapped[str | None] = mapped_column(Text, nullable=True)
    lifecycle_authority_id: Mapped[UUID | None] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey("data_lifecycle_authorities.id", ondelete="RESTRICT"),
        nullable=True,
        index=True,
    )


class BatchFamilyState(Base):
    """Per-family progression state for family-run batches (#672).

    Composite PK (batch_id, family_key). One row per (batch, family)
    pair, seeded at batch-accept time. The scheduler consults
    ``task_sequence[current_index]`` to gate claim eligibility; the CP
    finalize hook advances the state after each trial terminates.
    """

    __tablename__ = "batch_family_state"

    batch_id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey("batches.id", ondelete="CASCADE"),
        primary_key=True,
    )
    family_key: Mapped[str] = mapped_column(Text, primary_key=True)
    task_sequence: Mapped[list[str]] = mapped_column(
        ARRAY(Text),
        nullable=False,
    )
    current_index: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        server_default=text("0"),
    )
    state: Mapped[str] = mapped_column(Text, nullable=False)
    state_uri: Mapped[str | None] = mapped_column(Text, nullable=True)
    attempt_count: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        server_default=text("0"),
    )
    next_attempt_at: Mapped[datetime | None] = mapped_column(
        TIMESTAMP(timezone=True),
        nullable=True,
    )
    last_error: Mapped[str | None] = mapped_column(Text, nullable=True)
    updated_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True),
        nullable=False,
        server_default=func.now(),
    )


class Artifact(Base):
    """Typed artifact registry entry for reusable run outputs."""

    __tablename__ = "artifacts"
    __table_args__ = (
        Index("artifacts_team_type_idx", "team_id", "artifact_type"),
        Index("artifacts_batch_idx", "batch_id"),
        Index("artifacts_trial_idx", "trial_id"),
        Index("artifacts_policy_idx", "visibility", "share_status", "safety_state"),
    )

    id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True),
        primary_key=True,
        server_default=text("gen_random_uuid()"),
    )
    artifact_type: Mapped[str] = mapped_column(Text, nullable=False)
    artifact_schema_version: Mapped[str] = mapped_column(
        Text,
        nullable=False,
        server_default=text("'1.0'"),
        default="1.0",
    )
    name: Mapped[str] = mapped_column(Text, nullable=False)
    team_id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey("teams.id"),
        nullable=False,
    )
    project_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    batch_id: Mapped[UUID | None] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey("batches.id", ondelete="SET NULL"),
        nullable=True,
    )
    trial_id: Mapped[UUID | None] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey("trials.id", ondelete="SET NULL"),
        nullable=True,
    )
    created_by: Mapped[dict[str, Any]] = mapped_column(
        JSONB,
        nullable=False,
        server_default=text("'{}'::jsonb"),
        default=dict,
    )
    content_hash: Mapped[str] = mapped_column(Text, nullable=False)
    storage: Mapped[dict[str, Any]] = mapped_column(
        JSONB,
        nullable=False,
        server_default=text("'{}'::jsonb"),
        default=dict,
    )
    visibility: Mapped[str] = mapped_column(
        Text,
        nullable=False,
        server_default=text("'team'"),
        default="team",
    )
    share_status: Mapped[str] = mapped_column(
        Text,
        nullable=False,
        server_default=text("'pending_scan'"),
        default="pending_scan",
    )
    redaction_state: Mapped[str] = mapped_column(
        Text,
        nullable=False,
        server_default=text("'pending'"),
        default="pending",
    )
    safety_state: Mapped[str] = mapped_column(
        Text,
        nullable=False,
        server_default=text("'unknown'"),
        default="unknown",
    )
    blocked_reason: Mapped[str | None] = mapped_column(Text, nullable=True)
    retention: Mapped[dict[str, Any]] = mapped_column(
        JSONB,
        nullable=False,
        server_default=text("'{}'::jsonb"),
        default=dict,
    )
    provenance: Mapped[dict[str, Any]] = mapped_column(
        JSONB,
        nullable=False,
        server_default=text("'{}'::jsonb"),
        default=dict,
    )
    artifact_metadata: Mapped[dict[str, Any]] = mapped_column(
        "metadata",
        JSONB,
        nullable=False,
        server_default=text("'{}'::jsonb"),
        default=dict,
    )
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True),
        server_default=func.now(),
        nullable=False,
    )
    lifecycle_authority_id: Mapped[UUID | None] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey("data_lifecycle_authorities.id", ondelete="RESTRICT"),
        nullable=True,
        index=True,
    )


class ArtifactLineageEdge(Base):
    """Direct artifact parent edge for clone, reuse, export, and audit."""

    __tablename__ = "artifact_lineage_edges"
    __table_args__ = (
        Index("artifact_lineage_child_idx", "child_artifact_id"),
        Index("artifact_lineage_parent_idx", "parent_artifact_id"),
        Index("artifact_lineage_relation_idx", "relation"),
    )

    id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True),
        primary_key=True,
        server_default=text("gen_random_uuid()"),
    )
    child_artifact_id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey("artifacts.id", ondelete="CASCADE"),
        nullable=False,
    )
    parent_artifact_id: Mapped[UUID | None] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey("artifacts.id", ondelete="SET NULL"),
        nullable=True,
    )
    relation: Mapped[str] = mapped_column(Text, nullable=False)
    edge_metadata: Mapped[dict[str, Any]] = mapped_column(
        "metadata",
        JSONB,
        nullable=False,
        server_default=text("'{}'::jsonb"),
        default=dict,
    )
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True),
        server_default=func.now(),
        nullable=False,
    )


class Token(Base):
    __tablename__ = "tokens"
    token_hash: Mapped[bytes] = mapped_column(LargeBinary, primary_key=True)
    name: Mapped[str | None] = mapped_column(Text, nullable=True)
    type: Mapped[str] = mapped_column(String, nullable=False)
    scopes: Mapped[list[str]] = mapped_column(ARRAY(String), nullable=False)
    team_id: Mapped[UUID | None] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey("teams.id"),
        nullable=True,
    )
    created_by_user_id: Mapped[UUID | None] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey("users.id", ondelete="SET NULL"),
        nullable=True,
    )
    created_by_actor: Mapped[str | None] = mapped_column(Text, nullable=True)
    issued_at: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), nullable=False)
    expires_at: Mapped[datetime | None] = mapped_column(TIMESTAMP(timezone=True), nullable=True)
    revoked_at: Mapped[datetime | None] = mapped_column(TIMESTAMP(timezone=True), nullable=True)
    last_used_at: Mapped[datetime | None] = mapped_column(TIMESTAMP(timezone=True), nullable=True)
    # Plan 13 A13.3: service layer + auth helpers debounce-update this
    # on each successful verify_bearer_token, so /teams/{id}/members can
    # surface last-active without each route writing per-request.
    last_seen_at: Mapped[datetime | None] = mapped_column(TIMESTAMP(timezone=True), nullable=True)


class RateCard(Base):
    __tablename__ = "rate_cards"
    id: Mapped[str] = mapped_column(String, primary_key=True)
    captured_at: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), nullable=False)
    table: Mapped[dict[str, Any]] = mapped_column("table", JSONB, nullable=False)


class LlmCall(Base):
    """One row per LLM call routed through the Gateway. Written by every
    dialect endpoint after the upstream provider returns. Read by the
    worker at trial finalize to project LLMCallEvents into the trial's
    trajectory JSONL before ATIF projection runs."""

    __tablename__ = "llm_calls"
    id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), primary_key=True, default=uuid4)
    team_id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), nullable=False)
    trial_id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), nullable=False)
    step_id: Mapped[str] = mapped_column(Text, nullable=False)
    model: Mapped[str] = mapped_column(Text, nullable=False)
    dialect: Mapped[str] = mapped_column(Text, nullable=False)
    input_tokens: Mapped[int] = mapped_column(Integer, nullable=False)
    output_tokens: Mapped[int] = mapped_column(Integer, nullable=False)
    provider_extras: Mapped[dict[str, Any]] = mapped_column(
        JSONB,
        nullable=False,
        default=dict,
    )
    request_params: Mapped[dict[str, Any] | None] = mapped_column(
        JSONB,
        nullable=True,
        default=None,
    )
    cost_usd: Mapped[Decimal] = mapped_column(Numeric(12, 6), nullable=False)
    rate_card_hash: Mapped[str] = mapped_column(Text, nullable=False)
    captured_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True),
        nullable=False,
        server_default=func.now(),
    )
    # #298 Slice B (migration 0029): which gateway-internal attempt
    # produced this successful row. 1 = first try (the historical
    # case and the server default). > 1 = the gateway retried N-1
    # transient failures before this attempt succeeded. Operators
    # query `MAX(attempt) GROUP BY trial_id` to find trials that
    # suffered retries without parsing logs.
    attempt: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        server_default=text("1"),
        default=1,
    )
    lifecycle_authority_id: Mapped[UUID | None] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey("data_lifecycle_authorities.id", ondelete="RESTRICT"),
        nullable=True,
        index=True,
    )


class TrialEvent(Base):
    """One row per typed trajectory event a trial emits (#5 Slice 3a).

    Writers append events here so Phase 2's SSE endpoint can flip
    from MinIO-poll-based to LISTEN/NOTIFY push, and so analytics /
    debug paths can query by kind/source without scanning the
    per-trial MinIO JSONL.

    `seq` is the event's logical sequence within its trial (matches
    the existing `_EventBase.seq` envelope field in
    `loom.models.trajectory`). UNIQUE (trial_id, seq) doubles as the
    idempotency key — workers INSERT ... ON CONFLICT DO NOTHING so
    a retry after a partial batch ack doesn't dupe.

    `payload` is the full typed event body (matching the Pydantic
    union); `kind` mirrors `payload.kind` so cursor reads can filter
    without unpacking JSONB; `source` records the emitting subsystem
    ('worker' | 'control-plane' | future 'scheduler' | etc.); and
    `schema_version` lets readers interpret payloads against the
    version they were emitted under.

    `created_at` is the DB insert time (NOT the worker's
    `emitted_at` which lives in payload) — used as a tie-breaker
    for ORDER BY when seq is ambiguous and for retention sweeps.
    """

    __tablename__ = "trial_events"
    __table_args__ = (
        CheckConstraint("seq >= 0", name="trial_events_seq_nonneg_check"),
        CheckConstraint(
            "schema_version >= 1",
            name="trial_events_schema_version_positive_check",
        ),
        UniqueConstraint(
            "trial_id",
            "seq",
            name="trial_events_trial_seq_uidx",
        ),
        Index(
            "trial_events_trial_created_at_idx",
            "trial_id",
            "created_at",
        ),
        Index("trial_events_kind_idx", "kind"),
    )
    id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True),
        primary_key=True,
        server_default=text("gen_random_uuid()"),
        default=uuid4,
    )
    trial_id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey("trials.id", ondelete="CASCADE"),
        nullable=False,
    )
    seq: Mapped[int] = mapped_column(BigInteger, nullable=False)
    kind: Mapped[str] = mapped_column(Text, nullable=False)
    source: Mapped[str] = mapped_column(Text, nullable=False)
    schema_version: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        server_default=text("1"),
        default=1,
    )
    payload: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True),
        server_default=func.now(),
        nullable=False,
    )
    lifecycle_authority_id: Mapped[UUID | None] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey("data_lifecycle_authorities.id", ondelete="RESTRICT"),
        nullable=True,
        index=True,
    )


class Secret(Base):
    """One row per encrypted secret value managed by the `local-encrypted`
    SecretStore impl (cluster-deploy.md §Secrets). The `ref` is an opaque
    string of the form "loom://<namespace>/<uuid>" that callers store
    as the encrypted_api_key_ref on the consuming row (e.g.,
    provider_connections). master_key_version is bumped by the rotation
    walker when LOOM_SECRET_STORE_MASTER_KEY changes; pre-rotation rows
    are re-encrypted in place inside the rotation transaction."""

    __tablename__ = "secrets"
    ref: Mapped[str] = mapped_column(Text, primary_key=True)
    ciphertext: Mapped[bytes] = mapped_column(LargeBinary, nullable=False)
    # 12-byte AES-GCM nonce.
    nonce: Mapped[bytes] = mapped_column(LargeBinary, nullable=False)
    master_key_version: Mapped[int] = mapped_column(Integer, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True),
        server_default=func.now(),
        nullable=False,
    )


class ProviderConnection(Base):
    """Per-team record for a user-supplied LLM provider endpoint
    (cluster-deploy.md §Schema additions). Soft-deleted via `deleted_at`
    so in-flight trials' FKs stay valid for billing/audit; the partial
    UNIQUE index on (team_id, display_name) WHERE deleted_at IS NULL
    lets the operator reuse a name after deletion.

    `resolved_egress_ips` is the union (with bounded 24h window) of
    IPs the upstream_host has resolved to recently; populated by the
    re-resolver background task (advisory-lock-protected leader). The
    egress proxy gates outbound traffic to `target_ip ∈
    resolved_egress_ips[connection_id]`.

    `encrypted_api_key_ref` is opaque to the application: for the
    local-encrypted impl it's "loom://..."; for k8s-secret it's
    "k8s://ns/name". SecretStore.dispatch routes by URL scheme.
    """

    __tablename__ = "provider_connections"
    id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True),
        primary_key=True,
        default=uuid4,
    )
    team_id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey("teams.id", ondelete="CASCADE"),
        nullable=False,
    )
    provider_type: Mapped[str] = mapped_column(Text, nullable=False)
    display_name: Mapped[str] = mapped_column(Text, nullable=False)
    base_url: Mapped[str] = mapped_column(Text, nullable=False)
    upstream_host: Mapped[str] = mapped_column(Text, nullable=False)
    resolved_egress_ips: Mapped[list[str]] = mapped_column(
        ARRAY(INET),
        nullable=False,
        server_default=text("ARRAY[]::inet[]"),
    )
    egress_ips_refreshed_at: Mapped[datetime | None] = mapped_column(
        TIMESTAMP(timezone=True),
        nullable=True,
    )
    egress_ips_min_ttl_seconds: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        server_default=text("300"),
    )
    encrypted_api_key_ref: Mapped[str] = mapped_column(Text, nullable=False)
    allowed_models: Mapped[list[str] | None] = mapped_column(
        ARRAY(Text),
        nullable=True,
    )
    status: Mapped[str] = mapped_column(
        Text,
        nullable=False,
        server_default=text("'pending'"),
    )
    last_validated_at: Mapped[datetime | None] = mapped_column(
        TIMESTAMP(timezone=True),
        nullable=True,
    )
    last_validation_error: Mapped[str | None] = mapped_column(Text, nullable=True)
    # #277 / responses-api-support-probe: cache whether this upstream
    # implements POST /v1/responses. NULL means "never probed" — the
    # gateway probes at first use and refreshes on a TTL. TRUE routes
    # native pass-through; FALSE dispatches into responses_chat_compat.
    responses_api_supported: Mapped[bool | None] = mapped_column(
        Boolean,
        nullable=True,
    )
    responses_api_probed_at: Mapped[datetime | None] = mapped_column(
        TIMESTAMP(timezone=True),
        nullable=True,
    )
    responses_api_probe_error: Mapped[str | None] = mapped_column(
        Text,
        nullable=True,
    )
    pricing_source: Mapped[str] = mapped_column(
        Text,
        nullable=False,
        server_default=text("'tokens-only'"),
    )
    pricing_data: Mapped[dict[str, Any] | None] = mapped_column(
        JSONB,
        nullable=True,
    )
    rate_card_provider: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_by: Mapped[str] = mapped_column(Text, nullable=False)
    deleted_at: Mapped[datetime | None] = mapped_column(
        TIMESTAMP(timezone=True),
        nullable=True,
    )
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True),
        server_default=func.now(),
        nullable=False,
    )
    # Maintained by the trigger created in migration 0018 — updates
    # automatically on every UPDATE. Don't set explicitly in app code.
    updated_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True),
        server_default=func.now(),
        nullable=False,
    )


class ProviderConnectionShare(Base):
    """Authorization for one target team to use another team's provider.

    The source connection and its encrypted secret remain owned by the
    provider team. Sharing grants use/list/read access only; mutation and
    rotation stay with the owner team or platform admins.
    """

    __tablename__ = "provider_connection_shares"
    __table_args__ = (
        Index(
            "provider_connection_shares_target_team_idx",
            "target_team_id",
        ),
    )
    provider_connection_id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey("provider_connections.id", ondelete="CASCADE"),
        primary_key=True,
    )
    target_team_id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey("teams.id", ondelete="CASCADE"),
        primary_key=True,
    )
    created_by_actor: Mapped[str] = mapped_column(Text, nullable=False)
    created_by_user_id: Mapped[UUID | None] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey("users.id", ondelete="SET NULL"),
        nullable=True,
    )
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True),
        server_default=func.now(),
        nullable=False,
    )


class ProviderModelCache(Base):
    """Per-connection enumeration of upstream models. Refreshed on a 1h
    TTL on read; not hard-deleted when a model disappears upstream —
    `upstream_present` flips to false (audit trail). Operator can
    override visibility independently via `visible` + `hidden_reason`.
    """

    __tablename__ = "provider_models_cache"
    __table_args__ = (
        CheckConstraint(
            "last_preflight_status IS NULL OR last_preflight_status IN ('valid', 'failed')",
            name="provider_models_cache_preflight_status_check",
        ),
    )
    provider_connection_id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey("provider_connections.id", ondelete="CASCADE"),
        primary_key=True,
    )
    model_id: Mapped[str] = mapped_column(Text, primary_key=True)
    family: Mapped[str | None] = mapped_column(Text, nullable=True)
    context_length: Mapped[int | None] = mapped_column(Integer, nullable=True)
    capabilities: Mapped[dict[str, Any]] = mapped_column(
        JSONB,
        nullable=False,
        server_default=text("'{}'::jsonb"),
    )
    visible: Mapped[bool] = mapped_column(
        Boolean,
        nullable=False,
        server_default=text("true"),
    )
    hidden_reason: Mapped[str | None] = mapped_column(Text, nullable=True)
    last_seen_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True),
        server_default=func.now(),
        nullable=False,
    )
    upstream_present: Mapped[bool] = mapped_column(
        Boolean,
        nullable=False,
        server_default=text("true"),
    )
    # Per-model entitlement probe. NULL means discovered/manual but not
    # yet preflighted. Failed entries can warn/block before submission.
    last_preflight_status: Mapped[str | None] = mapped_column(Text, nullable=True)
    last_preflight_at: Mapped[datetime | None] = mapped_column(
        TIMESTAMP(timezone=True),
        nullable=True,
    )
    last_preflight_http_status: Mapped[int | None] = mapped_column(
        Integer,
        nullable=True,
    )
    last_preflight_error_code: Mapped[str | None] = mapped_column(
        Text,
        nullable=True,
    )
    last_preflight_error_message: Mapped[str | None] = mapped_column(
        Text,
        nullable=True,
    )


class ActiveTrialCacheBuild(Base):
    """One row per in-progress cache build for a (task_image_digest,
    install_script) combination (#317 Phase 1).

    Workers claim a slot here before docker-building the layered
    image; subsequent workers see the slot and wait via cheap SELECT
    polling. Crash safety via `expires_at`: when a builder's process
    dies, its heartbeat task stops refreshing the TTL; within ~60s
    the slot expires and another worker can steal it via
    `INSERT ON CONFLICT WHERE expires_at < now()`.

    NOT a Postgres advisory lock — those tie up a CP connection for
    the build duration; with N waiters that exhausts the CP pool.
    This table uses short transactions only (claim, exists, refresh,
    release each are one statement).
    """

    __tablename__ = "active_trial_cache_builds"
    cache_key: Mapped[str] = mapped_column(Text, primary_key=True)
    builder_worker_id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True),
        nullable=False,
    )
    started_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True),
        nullable=False,
        server_default=func.now(),
    )
    expires_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True),
        nullable=False,
    )
