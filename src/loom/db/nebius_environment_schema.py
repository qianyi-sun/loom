"""Always-on management identities, never the legacy slot/fleet authority.

Registration and all three namespace reservations must be inserted in one
management transaction before provisioning. Destroy retains the row and claims;
only verified purge may release names. Child databases contain no management rows.
"""

from __future__ import annotations

from datetime import datetime
from uuid import UUID

from sqlalchemy import (
    BigInteger,
    CheckConstraint,
    ForeignKey,
    ForeignKeyConstraint,
    Index,
    Text,
    UniqueConstraint,
    func,
    text,
)
from sqlalchemy.dialects.postgresql import TIMESTAMP
from sqlalchemy.dialects.postgresql import UUID as PgUUID  # noqa: N811
from sqlalchemy.orm import Mapped, mapped_column

from loom.db.base import Base


class NebiusEnvironment(Base):
    __tablename__ = "nebius_environments"
    __table_args__ = (
        UniqueConstraint("incarnation", name="nebius_environment_incarnation_key"),
        UniqueConstraint("target_id", name="nebius_environment_target_key"),
        UniqueConstraint("environment_id", "cluster_id", name="nebius_environment_cluster_key"),
        Index("nebius_environment_slug_key", "slug", unique=True, postgresql_where=text("purged_at IS NULL")),
        Index("nebius_environment_host_key", "public_host", unique=True, postgresql_where=text("purged_at IS NULL")),
        Index("nebius_environment_owner_idx", "owner_user_id", "desired_state"),
        CheckConstraint(
            "(scope = 'personal' AND kind = 'development' AND owner_user_id IS NOT NULL "
            "AND slug NOT IN ('dev', 'staging', 'prod', 'shared')) OR "
            "(scope = 'shared' AND ((kind = 'development' AND slug = 'dev') OR "
            "(kind = 'staging' AND slug = 'staging') OR (kind = 'production' AND slug = 'prod')))",
            name="nebius_environment_scope_check",
        ),
        CheckConstraint("slug ~ '^[a-z0-9]([-a-z0-9]{0,52}[a-z0-9])?$'", name="nebius_environment_slug_check"),
        CheckConstraint("deployment_generation > 0", name="nebius_environment_generation_check"),
        CheckConstraint("desired_state IN ('active', 'suspended', 'destroyed')", name="nebius_environment_state_check"),
        CheckConstraint("binding_mode IN ('generated', 'imported')", name="nebius_environment_binding_check"),
        CheckConstraint("purged_at IS NULL OR desired_state = 'destroyed'", name="nebius_environment_purge_check"),
        CheckConstraint(
            "application_namespace <> execution_namespace AND application_namespace <> build_namespace "
            "AND build_namespace = execution_namespace || '-build'",
            name="nebius_environment_namespaces_check",
        ),
        CheckConstraint(
            "environment_id <> '00000000-0000-0000-0000-000000000000'::uuid AND "
            "incarnation <> '00000000-0000-0000-0000-000000000000'::uuid",
            name="nebius_environment_identity_check",
        ),
    )

    environment_id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), primary_key=True)
    incarnation: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), nullable=False)
    owner_user_id: Mapped[UUID | None] = mapped_column(PgUUID(as_uuid=True), ForeignKey("users.id", ondelete="RESTRICT"))
    owner_team_id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), ForeignKey("teams.id", ondelete="RESTRICT"), nullable=False)
    scope: Mapped[str] = mapped_column(Text, nullable=False)
    kind: Mapped[str] = mapped_column(Text, nullable=False)
    slug: Mapped[str] = mapped_column(Text, nullable=False)
    cluster_id: Mapped[str] = mapped_column(Text, nullable=False)
    physical_pool_id: Mapped[str] = mapped_column(Text, nullable=False)
    application_namespace: Mapped[str] = mapped_column(Text, nullable=False)
    execution_namespace: Mapped[str] = mapped_column(Text, nullable=False)
    build_namespace: Mapped[str] = mapped_column(Text, nullable=False)
    public_host: Mapped[str] = mapped_column(Text, nullable=False)
    target_id: Mapped[str] = mapped_column(Text, nullable=False)
    binding_mode: Mapped[str] = mapped_column(Text, nullable=False)
    candidate_id: Mapped[UUID | None] = mapped_column(PgUUID(as_uuid=True))
    deployment_generation: Mapped[int] = mapped_column(BigInteger, nullable=False)
    desired_state: Mapped[str] = mapped_column(Text, nullable=False)
    created_at: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), server_default=func.now(), nullable=False)
    purged_at: Mapped[datetime | None] = mapped_column(TIMESTAMP(timezone=True))


class NebiusEnvironmentNamespace(Base):
    """One global physical-name index across application, execution and build roles."""

    __tablename__ = "nebius_environment_namespaces"
    __table_args__ = (
        ForeignKeyConstraint(
            ["environment_id", "cluster_id"],
            ["nebius_environments.environment_id", "nebius_environments.cluster_id"],
            ondelete="RESTRICT", name="nebius_environment_namespace_owner_fk",
        ),
        UniqueConstraint("environment_id", "role", name="nebius_environment_namespace_role_key"),
        CheckConstraint("role IN ('application', 'execution', 'build')", name="nebius_environment_namespace_role_check"),
        CheckConstraint("namespace_name ~ '^[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?$'", name="nebius_environment_namespace_name_check"),
    )

    cluster_id: Mapped[str] = mapped_column(Text, primary_key=True)
    namespace_name: Mapped[str] = mapped_column(Text, primary_key=True)
    environment_id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), nullable=False)
    role: Mapped[str] = mapped_column(Text, nullable=False)
