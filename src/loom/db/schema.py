"""SQLAlchemy ORM models for Loom's Postgres state (spec §4.7).

JSONB-typed columns hold Pydantic-serialized payloads; the ORM doesn't
validate the inner shape — that's the responsibility of the application code
that writes the row (which already validates against the Pydantic models).
"""

from __future__ import annotations

from datetime import datetime
from typing import Any
from uuid import UUID, uuid4

from sqlalchemy import (
    ARRAY,
    Float,
    ForeignKey,
    Integer,
    LargeBinary,
    String,
    func,
)
from sqlalchemy.dialects.postgresql import JSONB, TIMESTAMP
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
        TIMESTAMP(timezone=True), server_default=func.now(), nullable=False,
    )


class TeamQuota(Base):
    __tablename__ = "team_quotas"
    team_id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True), ForeignKey("teams.id"), primary_key=True,
    )
    fair_share_weight: Mapped[float] = mapped_column(Float, nullable=False, default=1.0)
    max_attempts: Mapped[int] = mapped_column(Integer, nullable=False, default=3)
    in_flight_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)


class Task(Base):
    __tablename__ = "tasks"
    id: Mapped[str] = mapped_column(String, primary_key=True)
    checksum: Mapped[str] = mapped_column(String, nullable=False)
    config: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    source: Mapped[str | None] = mapped_column(String, nullable=True)
    registered_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), server_default=func.now(), nullable=False,
    )


class Agent(Base):
    __tablename__ = "agents"
    name: Mapped[str] = mapped_column(String, primary_key=True)
    version: Mapped[str] = mapped_column(String, primary_key=True)
    mode: Mapped[str] = mapped_column(String, nullable=False)
    spec: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)


class Worker(Base):
    __tablename__ = "workers"
    id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), primary_key=True, default=uuid4)
    hostname: Mapped[str] = mapped_column(String, nullable=False)
    version: Mapped[str] = mapped_column(String, nullable=False)
    capabilities: Mapped[list[dict[str, Any]]] = mapped_column(JSONB, nullable=False)
    registered_at: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), nullable=False)
    last_seen_at: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), nullable=False)
    status: Mapped[str] = mapped_column(String, nullable=False)


class Trial(Base):
    __tablename__ = "trials"
    id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), primary_key=True, default=uuid4)
    team_id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), ForeignKey("teams.id"), nullable=False)
    task_id: Mapped[str] = mapped_column(String, ForeignKey("tasks.id"), nullable=False)
    config: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    requires_caps: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    state: Mapped[str] = mapped_column(String, nullable=False)
    failure_reason: Mapped[str | None] = mapped_column(String, nullable=True)
    submit_priority: Mapped[int] = mapped_column(Integer, nullable=False, default=100)
    submitted_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), server_default=func.now(), nullable=False,
    )
    claimed_at: Mapped[datetime | None] = mapped_column(TIMESTAMP(timezone=True), nullable=True)
    started_at: Mapped[datetime | None] = mapped_column(TIMESTAMP(timezone=True), nullable=True)
    finished_at: Mapped[datetime | None] = mapped_column(TIMESTAMP(timezone=True), nullable=True)
    cancellation_requested_at: Mapped[datetime | None] = mapped_column(TIMESTAMP(timezone=True), nullable=True)
    cancellation_observed_at: Mapped[datetime | None] = mapped_column(TIMESTAMP(timezone=True), nullable=True)
    worker_id: Mapped[UUID | None] = mapped_column(
        PgUUID(as_uuid=True), ForeignKey("workers.id"), nullable=True,
    )
    attempt_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    next_attempt_at: Mapped[datetime | None] = mapped_column(TIMESTAMP(timezone=True), nullable=True)
    result: Mapped[dict[str, Any] | None] = mapped_column(JSONB, nullable=True)
    trajectory_index: Mapped[dict[str, Any] | None] = mapped_column(JSONB, nullable=True)


class Token(Base):
    __tablename__ = "tokens"
    token_hash: Mapped[bytes] = mapped_column(LargeBinary, primary_key=True)
    type: Mapped[str] = mapped_column(String, nullable=False)
    scopes: Mapped[list[str]] = mapped_column(ARRAY(String), nullable=False)
    team_id: Mapped[UUID | None] = mapped_column(
        PgUUID(as_uuid=True), ForeignKey("teams.id"), nullable=True,
    )
    issued_at: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), nullable=False)
    expires_at: Mapped[datetime | None] = mapped_column(TIMESTAMP(timezone=True), nullable=True)
    revoked_at: Mapped[datetime | None] = mapped_column(TIMESTAMP(timezone=True), nullable=True)
    last_used_at: Mapped[datetime | None] = mapped_column(TIMESTAMP(timezone=True), nullable=True)


class RateCard(Base):
    __tablename__ = "rate_cards"
    id: Mapped[str] = mapped_column(String, primary_key=True)
    captured_at: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), nullable=False)
    table: Mapped[dict[str, Any]] = mapped_column("table", JSONB, nullable=False)
