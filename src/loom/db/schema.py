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


class Campaign(Base):
    """One row per submitted campaign (Plan 19).

    The runner fans out a campaign's `task_filter` into N trial
    submissions and back-links each via `trials.campaign_id`. State
    transitions: submitted → running → finished | cancelled.
    """
    __tablename__ = "campaigns"
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
    # Plan 22: when a Campaign was launched from a Workflow, the
    # workflow id is recorded for traceability. NULL for hand-submitted
    # campaigns.
    workflow_id: Mapped[UUID | None] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey("workflows.id"),
        nullable=True,
    )


class Workflow(Base):
    """One row per global saved-recipe (Plan 22).

    Workflows are admin-managed (`admin:workflows` scope for write
    routes); reads are open to any team user. Launching a Workflow
    creates a Campaign whose task_filter + trial_config are
    deep-copied from the workflow at launch time so subsequent edits
    don't retroactively change the historical run. Campaign.workflow_id
    is the back-link for traceability.

    All knobs are pinned for reproducibility: benchmark + agent +
    agent_version + model + backend + concurrency + task_filter +
    trial_config. No `*_latest` defaults — the workflow is an immutable
    contract once launched, mutable only by an admin re-saving it.
    """
    __tablename__ = "workflows"
    id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True), primary_key=True, default=uuid4,
    )
    name: Mapped[str] = mapped_column(Text, nullable=False)
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    benchmark_id: Mapped[str] = mapped_column(
        Text, ForeignKey("benchmarks.id"), nullable=False,
    )
    agent_name: Mapped[str] = mapped_column(Text, nullable=False)
    agent_version: Mapped[str] = mapped_column(Text, nullable=False)
    model_provider: Mapped[str] = mapped_column(Text, nullable=False)
    model_name: Mapped[str] = mapped_column(Text, nullable=False)
    backend: Mapped[str] = mapped_column(Text, nullable=False)
    concurrency: Mapped[int] = mapped_column(
        Integer, nullable=False, default=1,
    )
    task_filter: Mapped[dict[str, Any]] = mapped_column(
        JSONB, nullable=False, default=dict,
    )
    trial_config: Mapped[dict[str, Any]] = mapped_column(
        JSONB, nullable=False, default=dict,
    )
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), server_default=func.now(), nullable=False,
    )
    updated_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), server_default=func.now(), nullable=False,
    )
    created_by_token_prefix: Mapped[str] = mapped_column(
        Text, nullable=False,
    )
    deleted_at: Mapped[datetime | None] = mapped_column(
        TIMESTAMP(timezone=True), nullable=True,
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
    # Plan 19: campaign back-link + idempotency key. campaign_id is
    # NULL for hand-submitted trials. idempotency_key uniqueness is
    # enforced via a partial unique index (`trials_idempotency_key_uidx`).
    campaign_id: Mapped[UUID | None] = mapped_column(
        PgUUID(as_uuid=True),
        ForeignKey("campaigns.id", ondelete="SET NULL"),
        nullable=True,
    )
    idempotency_key: Mapped[str | None] = mapped_column(Text, nullable=True)


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
