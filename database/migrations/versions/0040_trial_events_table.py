"""trial_events: durable event log keyed by (trial_id, seq) — #5 Slice 3a

Revision ID: 0040
Revises: 0039
Create Date: 2026-06-25

#5 Slice 3a — adds the Postgres event table that Phase 2 needs to
flip the SSE endpoint from MinIO-poll-based to LISTEN/NOTIFY push,
and that future analytics paths can index against without scanning
the per-trial MinIO JSONL.

This migration just creates the table — there are no writers yet
(Slice 3b wires the worker batched dual-write) and no readers yet
(Slice 3c flips the /events endpoint). The table starts empty;
backfilling from existing MinIO trajectories is intentionally out
of scope (the historical record stays in MinIO as the audit-log
copy).

Schema:
- `id` UUID PK — gen_random_uuid default
- `trial_id` UUID FK trials.id ON DELETE CASCADE
- `seq` BIGINT — the event's logical sequence within its trial
- `kind` TEXT — discriminator (matches loom.models.trajectory.EventKind)
- `source` TEXT — origin subsystem ('worker' | 'control-plane' | …)
- `schema_version` INT — bump-only; payload shape is interpreted
  against this version
- `payload` JSONB — the full typed event body
- `created_at` TIMESTAMPTZ — DB-side insert time, NOT the worker's
  emitted_at (that lives in payload). Used for ORDER BY when seq is
  ambiguous and for retention sweeps.

Indexes:
- UNIQUE (trial_id, seq) — cursor-read fast path + idempotency for
  worker retries (writers INSERT ... ON CONFLICT DO NOTHING).
- (trial_id, created_at) — time-ordered queries within a trial.
- (kind) — future cross-trial analytics; cheap to ship now.
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "0040"
down_revision = "0039"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "trial_events",
        sa.Column(
            "id",
            postgresql.UUID(as_uuid=True),
            server_default=sa.text("gen_random_uuid()"),
            nullable=False,
        ),
        sa.Column(
            "trial_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("trials.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("seq", sa.BigInteger(), nullable=False),
        sa.Column("kind", sa.Text(), nullable=False),
        sa.Column("source", sa.Text(), nullable=False),
        sa.Column(
            "schema_version",
            sa.Integer(),
            nullable=False,
            server_default=sa.text("1"),
        ),
        sa.Column("payload", postgresql.JSONB(), nullable=False),
        sa.Column(
            "created_at",
            sa.TIMESTAMP(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.CheckConstraint("seq >= 0", name="trial_events_seq_nonneg_check"),
        sa.CheckConstraint(
            "schema_version >= 1",
            name="trial_events_schema_version_positive_check",
        ),
        sa.UniqueConstraint(
            "trial_id", "seq", name="trial_events_trial_seq_uidx",
        ),
    )
    op.create_index(
        "trial_events_trial_created_at_idx",
        "trial_events",
        ["trial_id", "created_at"],
    )
    op.create_index(
        "trial_events_kind_idx",
        "trial_events",
        ["kind"],
    )


def downgrade() -> None:
    op.drop_index("trial_events_kind_idx", table_name="trial_events")
    op.drop_index(
        "trial_events_trial_created_at_idx", table_name="trial_events",
    )
    op.drop_table("trial_events")
