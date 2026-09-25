"""Shared-data application identities, without legacy environment ownership.

Name claims are transactionally projected by migration-owned triggers. A caller
must insert only qualified registrations; these records do not authorize rollout.
"""
from __future__ import annotations

from datetime import datetime
from uuid import UUID

from sqlalchemy import BigInteger, CheckConstraint, ForeignKey, Index, Text, UniqueConstraint, func
from sqlalchemy.dialects.postgresql import TIMESTAMP
from sqlalchemy.dialects.postgresql import UUID as PgUUID  # noqa: N811
from sqlalchemy.orm import Mapped, mapped_column

from loom.db.base import Base


class NebiusApplication(Base):
    __tablename__ = "nebius_applications"
    __table_args__ = (
        UniqueConstraint("incarnation", name="nebius_application_incarnation_key"),
        Index("nebius_application_owner_idx", "owner_user_id", "desired_state"),
        CheckConstraint("deployment_generation > 0 AND access_generation > 0", name="nebius_application_generation_check"),
        CheckConstraint("desired_state IN ('active', 'suspended', 'destroyed')", name="nebius_application_state_check"),
        CheckConstraint("purged_at IS NULL OR desired_state = 'destroyed'", name="nebius_application_purge_check"),
        CheckConstraint("slug ~ '^[a-z0-9]([-a-z0-9]{0,52}[a-z0-9])?$' AND slug NOT IN ('dev','staging','prod','shared')",
                        name="nebius_application_slug_check"),
        CheckConstraint("application_namespace = 'loom-dev-' || slug", name="nebius_application_namespace_check"),
        CheckConstraint("cluster_id ~ '^[a-zA-Z0-9_-]{1,128}$'", name="nebius_application_cluster_check"),
        CheckConstraint("length(public_host) <= 253 AND public_host ~ '^[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?([.][a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?)+$'",
                        name="nebius_application_host_check"),
        CheckConstraint(" AND ".join(f"{name} <> '00000000-0000-0000-0000-000000000000'::uuid" for name in (
            "application_id", "incarnation", "owner_user_id", "owner_team_id", "data_environment_id", "release_id",
        )), name="nebius_application_identity_check"),
    )
    application_id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), primary_key=True)
    incarnation: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), nullable=False)
    owner_user_id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), ForeignKey("users.id", ondelete="RESTRICT"), nullable=False)
    owner_team_id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), ForeignKey("teams.id", ondelete="RESTRICT"), nullable=False)
    data_environment_id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), nullable=False)
    cluster_id: Mapped[str] = mapped_column(Text, nullable=False)
    slug: Mapped[str] = mapped_column(Text, nullable=False)
    application_namespace: Mapped[str] = mapped_column(Text, nullable=False)
    public_host: Mapped[str] = mapped_column(Text, nullable=False)
    release_id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), nullable=False)
    deployment_generation: Mapped[int] = mapped_column(BigInteger, nullable=False)
    access_generation: Mapped[int] = mapped_column(BigInteger, nullable=False)
    desired_state: Mapped[str] = mapped_column(Text, nullable=False)
    created_at: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), nullable=False, server_default=func.now())
    purged_at: Mapped[datetime | None] = mapped_column(TIMESTAMP(timezone=True))


class NebiusDeploymentNameClaim(Base):
    __tablename__ = "nebius_deployment_name_claims"
    __table_args__ = (
        CheckConstraint("num_nonnulls(environment_id, application_id) = 1", name="nebius_deployment_claim_owner_check"),
        CheckConstraint("(kind IN ('slug','host') AND scope = '') OR (kind = 'namespace' AND scope <> '')",
                        name="nebius_deployment_claim_kind_check"),
        CheckConstraint("name <> ''", name="nebius_deployment_claim_name_check"),
        Index("nebius_deployment_claim_environment_idx", "environment_id"),
        Index("nebius_deployment_claim_application_idx", "application_id"),
    )
    kind: Mapped[str] = mapped_column(Text, primary_key=True)
    scope: Mapped[str] = mapped_column(Text, primary_key=True)
    name: Mapped[str] = mapped_column(Text, primary_key=True)
    environment_id: Mapped[UUID | None] = mapped_column(PgUUID(as_uuid=True), ForeignKey("nebius_environments.environment_id", ondelete="CASCADE"))
    application_id: Mapped[UUID | None] = mapped_column(PgUUID(as_uuid=True), ForeignKey("nebius_applications.application_id", ondelete="CASCADE"))
