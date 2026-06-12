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
    Float,
    ForeignKey,
    Integer,
    LargeBinary,
    Numeric,
    String,
    Text,
    func,
    text,
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
    # Per-team SPDX allowlist (Plan 13 A13.1). DB default ships
    # MIT / Apache-2.0 / BSD-3-Clause / CC-BY-4.0 so the v1 benchmark
    # slate passes without operator action.
    license_allowlist: Mapped[list[str]] = mapped_column(
        ARRAY(Text), nullable=False,
        server_default=text(
            "ARRAY['MIT', 'Apache-2.0', 'BSD-3-Clause', 'CC-BY-4.0']::text[]",
        ),
    )


class Task(Base):
    __tablename__ = "tasks"
    id: Mapped[str] = mapped_column(String, primary_key=True)
    checksum: Mapped[str] = mapped_column(String, nullable=False)
    config: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    source: Mapped[str | None] = mapped_column(String, nullable=True)
    # Per-task SPDX license tag (Plan 13). NULL on hand-authored tasks;
    # benchmark-imported tasks always carry it.
    license: Mapped[str | None] = mapped_column(Text, nullable=True)
    # Parent benchmark, NULL for hand-authored tasks.
    benchmark_id: Mapped[str | None] = mapped_column(
        Text, ForeignKey("benchmarks.id"), nullable=True,
    )
    registered_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), server_default=func.now(), nullable=False,
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
    imported_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), server_default=func.now(), nullable=False,
    )
    imported_by: Mapped[str | None] = mapped_column(Text, nullable=True)


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
    id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True), primary_key=True,
        server_default=text("gen_random_uuid()"),
    )
    team_id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True), ForeignKey("teams.id"), nullable=False,
    )
    name: Mapped[str] = mapped_column(Text, nullable=False)
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    task_filter: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    trial_config: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    state: Mapped[str] = mapped_column(
        Text, nullable=False, server_default=text("'submitted'"),
    )
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), server_default=func.now(), nullable=False,
    )
    finished_at: Mapped[datetime | None] = mapped_column(
        TIMESTAMP(timezone=True), nullable=True,
    )
    created_by_token_prefix: Mapped[str] = mapped_column(Text, nullable=False)
    expected_trial_count: Mapped[int] = mapped_column(
        Integer, nullable=False, default=0,
    )
    # Plan 23: n-sampling. Runner submits n_per_task trials per matched
    # task; expected_trial_count = len(task_ids) * n_per_task.
    n_per_task: Mapped[int] = mapped_column(
        Integer, nullable=False, server_default=text("1"), default=1,
    )


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
        Integer, nullable=False, server_default=text("0"), default=0,
    )


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
        JSONB, nullable=False, default=dict,
    )
    cost_usd: Mapped[Decimal] = mapped_column(Numeric(12, 6), nullable=False)
    rate_card_hash: Mapped[str] = mapped_column(Text, nullable=False)
    captured_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), nullable=False, server_default=func.now(),
    )
