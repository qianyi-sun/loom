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
    Boolean,
    CheckConstraint,
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
        TIMESTAMP(timezone=True), server_default=func.now(), nullable=False,
    )
    disabled_at: Mapped[datetime | None] = mapped_column(
        TIMESTAMP(timezone=True), nullable=True,
    )
    disabled_reason: Mapped[str | None] = mapped_column(Text, nullable=True)
    submissions_paused_at: Mapped[datetime | None] = mapped_column(
        TIMESTAMP(timezone=True), nullable=True,
    )
    submissions_paused_reason: Mapped[str | None] = mapped_column(
        Text, nullable=True,
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
    # SSRF defense layer 3 opt-in (cluster-deploy.md §Secrets/SSRF).
    # When False (default for `loom cluster`), `POST /provider-connections`
    # rejects RFC1918 / IPv6 ULA / loopback / link-local IPs. When True
    # (default for `loom service` single-box mode, set at bootstrap),
    # RFC1918 + ULA are permitted; loopback + link-local stay rejected
    # unconditionally.
    allow_private_endpoints: Mapped[bool] = mapped_column(
        Boolean, nullable=False, server_default=text("false"),
    )


class User(Base):
    __tablename__ = "users"
    id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), primary_key=True, default=uuid4)
    email: Mapped[str] = mapped_column(Text, nullable=False)
    display_name: Mapped[str | None] = mapped_column(Text, nullable=True)
    is_platform_admin: Mapped[bool] = mapped_column(
        Boolean, nullable=False, server_default=text("false"), default=False,
    )
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), server_default=func.now(), nullable=False,
    )
    last_login_at: Mapped[datetime | None] = mapped_column(
        TIMESTAMP(timezone=True), nullable=True,
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
        PgUUID(as_uuid=True), ForeignKey("teams.id", ondelete="CASCADE"), primary_key=True,
    )
    user_id: Mapped[UUID] = mapped_column(
        PgUUID(as_uuid=True), ForeignKey("users.id", ondelete="CASCADE"), primary_key=True,
    )
    role: Mapped[str] = mapped_column(Text, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), server_default=func.now(), nullable=False,
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
        PgUUID(as_uuid=True), ForeignKey("teams.id", ondelete="CASCADE"), nullable=False,
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
        PgUUID(as_uuid=True), ForeignKey("users.id", ondelete="SET NULL"), nullable=True,
    )
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), server_default=func.now(), nullable=False,
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
        PgUUID(as_uuid=True), ForeignKey("users.id", ondelete="CASCADE"), nullable=False,
    )
    current_team_id: Mapped[UUID | None] = mapped_column(
        PgUUID(as_uuid=True), ForeignKey("teams.id", ondelete="SET NULL"), nullable=True,
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
        PgUUID(as_uuid=True), ForeignKey("users.id", ondelete="CASCADE"), nullable=False,
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
        TIMESTAMP(timezone=True), server_default=func.now(), nullable=False,
    )
    reviewed_at: Mapped[datetime | None] = mapped_column(
        TIMESTAMP(timezone=True), nullable=True,
    )
    reviewed_by_actor: Mapped[str | None] = mapped_column(Text, nullable=True)
    approved_team_id: Mapped[UUID | None] = mapped_column(
        PgUUID(as_uuid=True), ForeignKey("teams.id"), nullable=True,
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


class AdminAuditEvent(Base):
    __tablename__ = "admin_audit_events"
    id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), primary_key=True, default=uuid4)
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), server_default=func.now(), nullable=False,
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
    # PR-1 (benchmark series): open-ended key→value metadata. Adapters
    # populate from upstream (year/exam/difficulty/topic/…). The SPA
    # uses these for the tag filter UI; the backend exposes a
    # discovery endpoint that walks distinct values per benchmark.
    tags: Mapped[dict[str, str]] = mapped_column(
        JSONB, nullable=False, server_default="{}",
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
    # PR-1 (benchmark series): groups related benchmarks for the SPA's
    # multi-select dropdown. Convention: "aime", "swe-bench", …; NULL =
    # standalone, not part of a series. Disjoint variants (AIME by year)
    # are siblings under the same series so group-select unions cleanly.
    series: Mapped[str | None] = mapped_column(
        String(64), nullable=True,
    )
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
    # When `combinations` is non-empty, this `n_per_task` is ignored —
    # each Combination carries its own n_per_task.
    n_per_task: Mapped[int] = mapped_column(
        Integer, nullable=False, server_default=text("1"), default=1,
    )
    # Plan 28 PR-3: backend selection at the batch level. Catalog
    # lives at `/api/v1/backends` (derived from worker capabilities).
    backend: Mapped[str] = mapped_column(
        Text, nullable=False, server_default=text("'docker'"), default="docker",
    )
    # Plan 28 PR-3: multi-(agent, model) combinations. Each entry is
    # `{agent_name, agent_model, n_per_task, label?}`. Empty list ⇒
    # single-combination behaviour (agent + model + n_per_task live
    # on trial_config / Batch.n_per_task as before).
    combinations: Mapped[list[dict[str, Any]]] = mapped_column(
        JSONB, nullable=False, server_default=text("'[]'::jsonb"),
        default=list,
    )
    # Plan 28 PR-3: outcome separate from lifecycle `status`. NULL
    # until terminal. Computed by the batch_runner when transitioning
    # to a terminal lifecycle state. Values: succeeded /
    # partial_failed / all_failed / cancelled.
    result_status: Mapped[str | None] = mapped_column(
        Text, nullable=True,
    )
    # Fan-out failures happen before Control Plane accepts a child Trial.
    # Store them on the batch for retry suppression and user diagnostics.
    fanout_errors: Mapped[list[dict[str, Any]]] = mapped_column(
        JSONB, nullable=False, server_default=text("'[]'::jsonb"), default=list,
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
        JSONB, nullable=False, server_default=text("jsonb_build_array()"), default=list,
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
        Text, nullable=True,
    )
    # Issue #336: completed-run sharing is org-visible by default.
    # Team/private keeps the run in the owner team's boundary; org +
    # shared makes safe metadata visible in the org-wide Run Library.
    visibility: Mapped[str] = mapped_column(
        Text, nullable=False, server_default=text("'org'"), default="org",
    )
    share_status: Mapped[str] = mapped_column(
        Text,
        nullable=False,
        server_default=text("'shared'"),
        default="shared",
    )
    source_provenance: Mapped[list[dict[str, Any]]] = mapped_column(
        JSONB, nullable=False, server_default=text("'[]'::jsonb"),
        default=list,
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
    id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), primary_key=True, default=uuid4)
    team_id: Mapped[UUID] = mapped_column(PgUUID(as_uuid=True), ForeignKey("teams.id"), nullable=False)
    task_id: Mapped[str] = mapped_column(String, ForeignKey("tasks.id"), nullable=False)
    config: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    requires_caps: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    state: Mapped[str] = mapped_column(String, nullable=False)
    failure_reason: Mapped[str | None] = mapped_column(String, nullable=True)
    failure_message: Mapped[str | None] = mapped_column(Text, nullable=True)
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
    # Plan 28 PR-3: which Combination this trial belongs to within
    # its parent Batch. 0 for single-combination batches (matches
    # the pre-migration behaviour exactly).
    combination_idx: Mapped[int] = mapped_column(
        Integer, nullable=False, server_default=text("0"), default=0,
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
        Text, nullable=True,
    )
    visibility: Mapped[str] = mapped_column(
        Text, nullable=False, server_default=text("'org'"), default="org",
    )
    share_status: Mapped[str] = mapped_column(
        Text,
        nullable=False,
        server_default=text("'shared'"),
        default="shared",
    )
    source_provenance: Mapped[list[dict[str, Any]]] = mapped_column(
        JSONB, nullable=False, server_default=text("'[]'::jsonb"),
        default=list,
    )


class Token(Base):
    __tablename__ = "tokens"
    token_hash: Mapped[bytes] = mapped_column(LargeBinary, primary_key=True)
    name: Mapped[str | None] = mapped_column(Text, nullable=True)
    type: Mapped[str] = mapped_column(String, nullable=False)
    scopes: Mapped[list[str]] = mapped_column(ARRAY(String), nullable=False)
    team_id: Mapped[UUID | None] = mapped_column(
        PgUUID(as_uuid=True), ForeignKey("teams.id"), nullable=True,
    )
    created_by_user_id: Mapped[UUID | None] = mapped_column(
        PgUUID(as_uuid=True), ForeignKey("users.id", ondelete="SET NULL"), nullable=True,
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
        JSONB, nullable=False, default=dict,
    )
    cost_usd: Mapped[Decimal] = mapped_column(Numeric(12, 6), nullable=False)
    rate_card_hash: Mapped[str] = mapped_column(Text, nullable=False)
    captured_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), nullable=False, server_default=func.now(),
    )
    # #298 Slice B (migration 0029): which gateway-internal attempt
    # produced this successful row. 1 = first try (the historical
    # case and the server default). > 1 = the gateway retried N-1
    # transient failures before this attempt succeeded. Operators
    # query `MAX(attempt) GROUP BY trial_id` to find trials that
    # suffered retries without parsing logs.
    attempt: Mapped[int] = mapped_column(
        Integer, nullable=False, server_default=text("1"), default=1,
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
        TIMESTAMP(timezone=True), server_default=func.now(), nullable=False,
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
        PgUUID(as_uuid=True), primary_key=True, default=uuid4,
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
        ARRAY(INET), nullable=False, server_default=text("ARRAY[]::inet[]"),
    )
    egress_ips_refreshed_at: Mapped[datetime | None] = mapped_column(
        TIMESTAMP(timezone=True), nullable=True,
    )
    egress_ips_min_ttl_seconds: Mapped[int] = mapped_column(
        Integer, nullable=False, server_default=text("300"),
    )
    encrypted_api_key_ref: Mapped[str] = mapped_column(Text, nullable=False)
    allowed_models: Mapped[list[str] | None] = mapped_column(
        ARRAY(Text), nullable=True,
    )
    status: Mapped[str] = mapped_column(
        Text, nullable=False, server_default=text("'pending'"),
    )
    last_validated_at: Mapped[datetime | None] = mapped_column(
        TIMESTAMP(timezone=True), nullable=True,
    )
    last_validation_error: Mapped[str | None] = mapped_column(Text, nullable=True)
    pricing_source: Mapped[str] = mapped_column(
        Text, nullable=False, server_default=text("'tokens-only'"),
    )
    pricing_data: Mapped[dict[str, Any] | None] = mapped_column(
        JSONB, nullable=True,
    )
    rate_card_provider: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_by: Mapped[str] = mapped_column(Text, nullable=False)
    deleted_at: Mapped[datetime | None] = mapped_column(
        TIMESTAMP(timezone=True), nullable=True,
    )
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), server_default=func.now(), nullable=False,
    )
    # Maintained by the trigger created in migration 0018 — updates
    # automatically on every UPDATE. Don't set explicitly in app code.
    updated_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), server_default=func.now(), nullable=False,
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
            "last_preflight_status IS NULL OR "
            "last_preflight_status IN ('valid', 'failed')",
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
        JSONB, nullable=False, server_default=text("'{}'::jsonb"),
    )
    visible: Mapped[bool] = mapped_column(
        Boolean, nullable=False, server_default=text("true"),
    )
    hidden_reason: Mapped[str | None] = mapped_column(Text, nullable=True)
    last_seen_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), server_default=func.now(), nullable=False,
    )
    upstream_present: Mapped[bool] = mapped_column(
        Boolean, nullable=False, server_default=text("true"),
    )
    # Per-model entitlement probe. NULL means discovered/manual but not
    # yet preflighted. Failed entries can warn/block before submission.
    last_preflight_status: Mapped[str | None] = mapped_column(Text, nullable=True)
    last_preflight_at: Mapped[datetime | None] = mapped_column(
        TIMESTAMP(timezone=True), nullable=True,
    )
    last_preflight_http_status: Mapped[int | None] = mapped_column(
        Integer, nullable=True,
    )
    last_preflight_error_code: Mapped[str | None] = mapped_column(
        Text, nullable=True,
    )
    last_preflight_error_message: Mapped[str | None] = mapped_column(
        Text, nullable=True,
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
        PgUUID(as_uuid=True), nullable=False,
    )
    started_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), nullable=False, server_default=func.now(),
    )
    expires_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), nullable=False,
    )
