"""active_trial_cache_builds: coordinate per-(image, agent) builds (#317)

Revision ID: 0033
Revises: 0032
Create Date: 2026-06-22

Tracks which worker is currently building a (task_image_digest,
install_script) → cached layered image combination. Used by the
worker's `resolve_trial_image` to prevent thundering-herd duplicate
builds on cold-cluster cold-start.

Coordination shape:
- Atomic claim via INSERT ON CONFLICT, with TTL-based expired-slot
  stealing (`WHERE expires_at < now()` on the conflict UPDATE).
- Builder refreshes its slot's `expires_at` every 60s via heartbeat;
  on crash the heartbeat dies and the slot expires within 60s.
- Waiters poll with cheap SELECT; only re-attempt the full
  INSERT-ON-CONFLICT when the slot row disappears.

NOT a long-held lock: each operation is one short DB transaction.
Workers reach this table via 4 new CP HTTP routes under
`/api/v1/internal/trial-cache/*` (worker→CP boundary preserved;
workers don't get direct CP postgres sessions).
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "0033"
down_revision = "0032"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "active_trial_cache_builds",
        sa.Column("cache_key", sa.Text(), primary_key=True),
        sa.Column(
            "builder_worker_id",
            postgresql.UUID(as_uuid=True),
            nullable=False,
        ),
        sa.Column(
            "started_at",
            postgresql.TIMESTAMP(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column(
            "expires_at",
            postgresql.TIMESTAMP(timezone=True),
            nullable=False,
        ),
    )
    op.create_index(
        "active_trial_cache_builds_expires_idx",
        "active_trial_cache_builds",
        ["expires_at"],
    )


def downgrade() -> None:
    op.drop_index(
        "active_trial_cache_builds_expires_idx",
        table_name="active_trial_cache_builds",
    )
    op.drop_table("active_trial_cache_builds")
