"""User TaskSet foundation schema (#242 sub-plan 1).

Creates `task_sets` and `task_set_manifests` for team-owned user uploads.
Native `benchmarks` rows are untouched. Linking materialized tasks to
TaskSets (`tasks.task_set_id`) is deferred to the materialization slice.
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "0051"
down_revision = "0050"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "task_sets",
        sa.Column("id", sa.Text(), nullable=False),
        sa.Column("owning_team_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("slug", sa.Text(), nullable=False),
        sa.Column("display_name", sa.Text(), nullable=False),
        sa.Column(
            "visibility",
            sa.Text(),
            nullable=False,
            server_default=sa.text("'private'"),
        ),
        sa.Column("status", sa.Text(), nullable=False),
        sa.Column("status_reason", sa.Text(), nullable=True),
        sa.Column("intents", postgresql.ARRAY(sa.Text()), nullable=False),
        sa.Column(
            "evaluation_ready",
            sa.Boolean(),
            nullable=False,
            server_default=sa.text("false"),
        ),
        sa.Column("manifest_blob_uri", sa.Text(), nullable=False),
        sa.Column(
            "task_count",
            sa.Integer(),
            nullable=False,
            server_default=sa.text("0"),
        ),
        sa.Column("soft_deleted_at", postgresql.TIMESTAMP(timezone=True), nullable=True),
        sa.Column(
            "created_at",
            postgresql.TIMESTAMP(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            postgresql.TIMESTAMP(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(["owning_team_id"], ["teams.id"]),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_check_constraint(
        "task_sets_visibility_check",
        "task_sets",
        "visibility IN ('private')",
    )
    op.create_check_constraint(
        "task_sets_status_check",
        "task_sets",
        "status IN ('materializing', 'ready', 'partial', 'failed', 'deleted')",
    )
    op.create_check_constraint(
        "task_sets_intents_check",
        "task_sets",
        "cardinality(intents) > 0 AND "
        "intents <@ ARRAY['trajectory_generation', 'evaluation']::text[]",
    )
    op.create_check_constraint(
        "task_sets_slug_check",
        "task_sets",
        "slug <> '' AND slug = trim(slug) AND slug !~ '[./\\\\]'",
    )
    op.create_check_constraint(
        "task_sets_id_namespace_check",
        "task_sets",
        "id = 'ts/' || owning_team_id::text || '/' || slug",
    )
    op.create_check_constraint(
        "task_sets_task_count_nonneg_check",
        "task_sets",
        "task_count >= 0",
    )
    op.create_index(
        "task_sets_team_visibility_status_idx",
        "task_sets",
        ["owning_team_id", "visibility", "status"],
    )
    op.create_index(
        "task_sets_team_slug_uidx",
        "task_sets",
        ["owning_team_id", "slug"],
        unique=True,
    )
    op.create_index(
        "task_sets_evaluation_ready_idx",
        "task_sets",
        ["evaluation_ready"],
        postgresql_where=sa.text("evaluation_ready = true"),
    )

    op.create_table(
        "task_set_manifests",
        sa.Column("task_set_id", sa.Text(), nullable=False),
        sa.Column("schema_version", sa.Integer(), nullable=False),
        sa.Column(
            "manifest",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
        ),
        sa.Column("verifier_blob_uri", sa.Text(), nullable=True),
        sa.Column("transform_blob_uri", sa.Text(), nullable=True),
        sa.Column(
            "created_at",
            postgresql.TIMESTAMP(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            postgresql.TIMESTAMP(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(
            ["task_set_id"],
            ["task_sets.id"],
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("task_set_id"),
    )


def downgrade() -> None:
    op.drop_table("task_set_manifests")
    op.drop_table("task_sets")
