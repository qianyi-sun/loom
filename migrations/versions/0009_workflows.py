"""workflows table — global saved recipes (admin-creates, all-teams-launch)

Revision ID: 0009
Revises: 0008
Create Date: 2026-06-10

A Workflow is a saved configuration: which benchmark to run, which
agent + agent-version + model + backend to use, and the
`task_filter` + `trial_config` payload that gets frozen into a
Campaign on launch. Workflows are GLOBAL (no team_id) — any team
can launch any workflow, but only admins can create / update /
delete them. The `admin:workflows` scope is added to the same row
the migration 0005 used for `admin:rate_cards`.

Launching a workflow creates a Campaign whose `task_filter` +
`trial_config` are deep-copied from the workflow at submit time, so
edits to the workflow after the launch don't retroactively change
the historical run. The Campaign also records `workflow_id` for
traceability + queryability.
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0009"
down_revision: str | None = "0008"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "workflows",
        sa.Column(
            "id", postgresql.UUID(as_uuid=True), primary_key=True,
            server_default=sa.text("gen_random_uuid()"),
        ),
        sa.Column("name", sa.Text(), nullable=False),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column(
            "benchmark_id", sa.Text(),
            sa.ForeignKey("benchmarks.id"), nullable=False,
        ),
        sa.Column("agent_name", sa.Text(), nullable=False),
        sa.Column("agent_version", sa.Text(), nullable=False),
        sa.Column("model_provider", sa.Text(), nullable=False),
        sa.Column("model_name", sa.Text(), nullable=False),
        sa.Column(
            "backend", sa.Text(), nullable=False,
            server_default=sa.text("'docker'"),
        ),
        sa.Column(
            "concurrency", sa.Integer(), nullable=False,
            server_default="1",
        ),
        sa.Column(
            "task_filter", postgresql.JSONB, nullable=False,
            server_default=sa.text("'{}'::jsonb"),
        ),
        sa.Column(
            "trial_config", postgresql.JSONB, nullable=False,
            server_default=sa.text("'{}'::jsonb"),
        ),
        sa.Column(
            "created_at", sa.TIMESTAMP(timezone=True),
            nullable=False, server_default=sa.text("now()"),
        ),
        sa.Column(
            "updated_at", sa.TIMESTAMP(timezone=True),
            nullable=False, server_default=sa.text("now()"),
        ),
        sa.Column("created_by_token_prefix", sa.Text(), nullable=False),
        sa.Column(
            "deleted_at", sa.TIMESTAMP(timezone=True), nullable=True,
        ),
    )
    # Partial unique on (name) where the row is active. Emitted as
    # a CREATE UNIQUE INDEX … WHERE … rather than a UniqueConstraint
    # because alembic/SQLAlchemy's table-level UniqueConstraint does
    # not accept `postgresql_where` — same pattern as migration 0007's
    # `trials_idempotency_key_uidx`. This index ALSO services the
    # dominant "list active workflows ordered by name" query, so no
    # separate non-unique partial index is needed.
    op.create_index(
        "workflows_name_unique",
        "workflows",
        ["name"],
        unique=True,
        postgresql_where=sa.text("deleted_at IS NULL"),
    )

    # Campaign back-reference: a campaign that was launched from a
    # workflow records the workflow_id for traceability. Nullable so
    # campaigns created via POST /campaigns directly aren't required
    # to carry a workflow.
    op.add_column(
        "campaigns",
        sa.Column(
            "workflow_id", postgresql.UUID(as_uuid=True),
            sa.ForeignKey("workflows.id"), nullable=True,
        ),
    )
    op.create_index(
        "campaigns_workflow_id_idx",
        "campaigns",
        ["workflow_id"],
        postgresql_where=sa.text("workflow_id IS NOT NULL"),
    )


def downgrade() -> None:
    op.drop_index("campaigns_workflow_id_idx", table_name="campaigns")
    op.drop_column("campaigns", "workflow_id")
    op.drop_index("workflows_name_unique", table_name="workflows")
    op.drop_table("workflows")
