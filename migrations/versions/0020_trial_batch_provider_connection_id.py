"""Trial.provider_connection_id + Batch.provider_connection_id + provider_model_id

Revision ID: 0020
Revises: 0019
Create Date: 2026-06-16

Adds the consumer-side FKs that let a Trial or Batch reference the
provider_connection it should use for LLM calls. Per the cluster-deploy
spec:

- `Trial.provider_connection_id` — UUID NULL FK; no cascade rule
  because the parent (provider_connections) never hard-deletes today
  (the team_id FK is ON DELETE RESTRICT, set in migration 0018 self-
  review). Soft-delete on the parent leaves this FK valid; eventual
  Phase 5 `loom admin providers purge` is the only path to hard-delete,
  and it explicitly rejects connections with referring trials.
- `Trial.provider_model_id` — TEXT NULL. Plain string; FK to
  `provider_models_cache.(provider_connection_id, model_id)` would
  add a composite-FK to a soft-deleted-aware table for marginal
  benefit (the cache row is auto-populated from upstream — Trial
  references a model name string, not a managed row).
- `Batch.provider_connection_id` + `Batch.provider_model_id` —
  same shape; carry the connection through batch fan-out into trial
  config.

Trial+Batch null = "use the platform default" (today's env-keyed
provider, preserved for backward compatibility). The gateway resolves:
prefer Trial value > Batch value > platform default.

Why no triggers / cascades / etc: this migration just adds the
nullable FK columns. The route layer that uses these columns is the
next PR's concern.
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "0020"
down_revision = "0019"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "trials",
        sa.Column(
            "provider_connection_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("provider_connections.id"),
            nullable=True,
        ),
    )
    op.add_column(
        "trials",
        sa.Column("provider_model_id", sa.Text(), nullable=True),
    )
    op.add_column(
        "batches",
        sa.Column(
            "provider_connection_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("provider_connections.id"),
            nullable=True,
        ),
    )
    op.add_column(
        "batches",
        sa.Column("provider_model_id", sa.Text(), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("batches", "provider_model_id")
    op.drop_column("batches", "provider_connection_id")
    op.drop_column("trials", "provider_model_id")
    op.drop_column("trials", "provider_connection_id")
