"""Add typed staging data-lifecycle authority and mutation epochs.

Revision ID: 0066
Revises: 0065
Create Date: 2026-07-19

The new tables are additive. Existing execution rows remain deliberately
unclassified until the staging inventory tool creates exact authorities; an
unclassified row or object is never deletion-eligible.
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "0066"
down_revision = "0065"
branch_labels = None
depends_on = None

_EXECUTION_TABLES = ("batches", "trials", "llm_calls", "trial_events", "artifacts")


def upgrade() -> None:
    op.create_table(
        "staging_mutation_epochs",
        sa.Column("environment", sa.Text(), primary_key=True),
        sa.Column("namespace", sa.Text(), nullable=False),
        sa.Column("epoch", sa.BigInteger(), nullable=False, server_default=sa.text("0")),
        sa.Column("reason", sa.Text(), nullable=False, server_default=sa.text("'bootstrap'")),
        sa.Column("request_id", sa.Text(), nullable=True),
        sa.Column("evidence_sha256", sa.Text(), nullable=True),
        sa.Column(
            "updated_at",
            postgresql.TIMESTAMP(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.CheckConstraint("environment = 'staging'", name="staging_mutation_epochs_env_check"),
        sa.CheckConstraint("namespace <> ''", name="staging_mutation_epochs_namespace_check"),
        sa.CheckConstraint("epoch >= 0", name="staging_mutation_epochs_epoch_check"),
        sa.CheckConstraint(
            "reason IN ('bootstrap','rollout_apply','lifecycle_gc','object_rewrite','rollback')",
            name="staging_mutation_epochs_reason_check",
        ),
        sa.CheckConstraint(
            "(reason = 'bootstrap' AND request_id IS NULL AND evidence_sha256 IS NULL) OR "
            "(reason <> 'bootstrap' AND request_id ~ '^[a-z0-9][a-z0-9-]{7,79}$' "
            "AND evidence_sha256 ~ '^[0-9a-f]{64}$')",
            name="staging_mutation_epochs_evidence_check",
        ),
    )
    op.create_table(
        "staging_mutation_epoch_events",
        sa.Column("environment", sa.Text(), nullable=False),
        sa.Column("namespace", sa.Text(), nullable=False),
        sa.Column("epoch", sa.BigInteger(), nullable=False),
        sa.Column("mutation_class", sa.Text(), nullable=False),
        sa.Column("request_id", sa.Text(), nullable=False),
        sa.Column("evidence_sha256", sa.Text(), nullable=False),
        sa.Column("occurred_at", postgresql.TIMESTAMP(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint(
            "environment",
            "namespace",
            "epoch",
            name="staging_mutation_epoch_events_pkey",
        ),
        sa.CheckConstraint(
            "environment = 'staging'",
            name="staging_mutation_epoch_events_env_check",
        ),
        sa.CheckConstraint(
            "namespace <> '' AND epoch > 0",
            name="staging_mutation_epoch_events_identity_check",
        ),
        sa.CheckConstraint(
            "mutation_class IN ('rollout_apply','lifecycle_gc','object_rewrite','rollback')",
            name="staging_mutation_epoch_events_class_check",
        ),
        sa.CheckConstraint(
            "request_id ~ '^[a-z0-9][a-z0-9-]{7,79}$' AND evidence_sha256 ~ '^[0-9a-f]{64}$'",
            name="staging_mutation_epoch_events_evidence_check",
        ),
    )
    op.create_table(
        "data_lifecycle_authorities",
        sa.Column(
            "id",
            postgresql.UUID(as_uuid=True),
            primary_key=True,
            server_default=sa.text("gen_random_uuid()"),
        ),
        sa.Column("environment", sa.Text(), nullable=False),
        sa.Column("namespace", sa.Text(), nullable=False),
        sa.Column(
            "team_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("teams.id", ondelete="RESTRICT"),
            nullable=True,
        ),
        sa.Column("data_class", sa.Text(), nullable=False),
        sa.Column("owner_kind", sa.Text(), nullable=False),
        sa.Column("owner_id", sa.Text(), nullable=False),
        sa.Column(
            "created_at",
            postgresql.TIMESTAMP(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.Column("expires_at", postgresql.TIMESTAMP(timezone=True), nullable=True),
        sa.Column("pinned", sa.Boolean(), nullable=False, server_default=sa.text("false")),
        sa.Column("state", sa.Text(), nullable=False, server_default=sa.text("'active'")),
        sa.Column("deletion_token", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column(
            "metadata", postgresql.JSONB(), nullable=False, server_default=sa.text("'{}'::jsonb")
        ),
        sa.CheckConstraint("environment <> ''", name="data_lifecycle_authorities_env_check"),
        sa.CheckConstraint("namespace <> ''", name="data_lifecycle_authorities_namespace_check"),
        sa.CheckConstraint(
            "data_class IN ('run','trial','event','artifact','benchmark','catalog','system')",
            name="data_lifecycle_authorities_class_check",
        ),
        sa.CheckConstraint(
            "owner_kind IN ('batch','trial','artifact','benchmark','system')",
            name="data_lifecycle_authorities_owner_kind_check",
        ),
        sa.CheckConstraint(
            "state IN ('active','deleting','quarantined')",
            name="data_lifecycle_authorities_state_check",
        ),
        sa.CheckConstraint(
            "(pinned AND expires_at IS NULL) OR "
            "(NOT pinned AND expires_at IS NOT NULL AND expires_at > created_at)",
            name="data_lifecycle_authorities_retention_check",
        ),
        sa.CheckConstraint(
            "data_class NOT IN ('catalog','system') OR pinned",
            name="data_lifecycle_authorities_pinned_class_check",
        ),
        sa.UniqueConstraint(
            "environment",
            "namespace",
            "data_class",
            "owner_kind",
            "owner_id",
            name="data_lifecycle_authorities_owner_uidx",
        ),
    )
    op.create_index(
        "data_lifecycle_authorities_gc_idx",
        "data_lifecycle_authorities",
        ["environment", "namespace", "state", "expires_at"],
    )
    op.create_table(
        "data_lifecycle_objects",
        sa.Column(
            "id",
            postgresql.UUID(as_uuid=True),
            primary_key=True,
            server_default=sa.text("gen_random_uuid()"),
        ),
        sa.Column(
            "authority_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("data_lifecycle_authorities.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("environment", sa.Text(), nullable=False),
        sa.Column("namespace", sa.Text(), nullable=False),
        sa.Column("bucket", sa.Text(), nullable=False),
        sa.Column("object_key", sa.Text(), nullable=False),
        sa.Column("version_id", sa.Text(), nullable=True),
        sa.Column("content_sha256", sa.Text(), nullable=True),
        sa.Column("size_bytes", sa.BigInteger(), nullable=False),
        sa.Column("created_at", postgresql.TIMESTAMP(timezone=True), nullable=False),
        sa.Column("state", sa.Text(), nullable=False, server_default=sa.text("'active'")),
        sa.Column("deletion_token", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("verified_deleted_at", postgresql.TIMESTAMP(timezone=True), nullable=True),
        sa.CheckConstraint("environment <> ''", name="data_lifecycle_objects_env_check"),
        sa.CheckConstraint("namespace <> ''", name="data_lifecycle_objects_namespace_check"),
        sa.CheckConstraint(
            "bucket <> '' AND object_key <> ''", name="data_lifecycle_objects_key_check"
        ),
        sa.CheckConstraint("size_bytes >= 0", name="data_lifecycle_objects_size_check"),
        sa.CheckConstraint(
            "state IN ('active','delete_pending','deleted','quarantined')",
            name="data_lifecycle_objects_state_check",
        ),
        sa.CheckConstraint(
            "(state = 'deleted') = (verified_deleted_at IS NOT NULL)",
            name="data_lifecycle_objects_deleted_check",
        ),
    )
    op.create_index(
        "data_lifecycle_objects_identity_uidx",
        "data_lifecycle_objects",
        ["environment", "namespace", "bucket", "object_key", sa.text("COALESCE(version_id, '')")],
        unique=True,
    )
    op.create_index(
        "data_lifecycle_objects_authority_state_idx",
        "data_lifecycle_objects",
        ["authority_id", "state"],
    )
    op.create_table(
        "data_lifecycle_gc_runs",
        sa.Column(
            "id",
            postgresql.UUID(as_uuid=True),
            primary_key=True,
            server_default=sa.text("gen_random_uuid()"),
        ),
        sa.Column("environment", sa.Text(), nullable=False),
        sa.Column("namespace", sa.Text(), nullable=False),
        sa.Column("mutation_epoch_before", sa.BigInteger(), nullable=False),
        sa.Column("mutation_epoch_after", sa.BigInteger(), nullable=True),
        sa.Column("state", sa.Text(), nullable=False, server_default=sa.text("'planned'")),
        sa.Column("dry_run", sa.Boolean(), nullable=False),
        sa.Column("requested_by", sa.Text(), nullable=False),
        sa.Column("policy", postgresql.JSONB(), nullable=False),
        sa.Column("inventory", postgresql.JSONB(), nullable=False),
        sa.Column(
            "created_at",
            postgresql.TIMESTAMP(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.Column("finished_at", postgresql.TIMESTAMP(timezone=True), nullable=True),
        sa.Column("failure_reason", sa.Text(), nullable=True),
        sa.CheckConstraint("environment = 'staging'", name="data_lifecycle_gc_runs_env_check"),
        sa.CheckConstraint("namespace <> ''", name="data_lifecycle_gc_runs_namespace_check"),
        sa.CheckConstraint(
            "mutation_epoch_before >= 0", name="data_lifecycle_gc_runs_epoch_before_check"
        ),
        sa.CheckConstraint(
            "mutation_epoch_after IS NULL OR mutation_epoch_after > mutation_epoch_before",
            name="data_lifecycle_gc_runs_epoch_after_check",
        ),
        sa.CheckConstraint(
            "state IN ('planned','applying','verifying','completed','failed')",
            name="data_lifecycle_gc_runs_state_check",
        ),
    )
    op.create_table(
        "data_lifecycle_gc_items",
        sa.Column(
            "gc_run_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("data_lifecycle_gc_runs.id", ondelete="CASCADE"),
            primary_key=True,
        ),
        sa.Column(
            "object_id",
            postgresql.UUID(as_uuid=True),
            primary_key=True,
        ),
        sa.Column("deletion_token", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("state", sa.Text(), nullable=False, server_default=sa.text("'marked'")),
        sa.Column("last_error", sa.Text(), nullable=True),
        sa.Column(
            "updated_at",
            postgresql.TIMESTAMP(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.CheckConstraint(
            "state IN ('marked','object_deleted','verified','metadata_deleted','failed')",
            name="data_lifecycle_gc_items_state_check",
        ),
    )
    for table_name in _EXECUTION_TABLES:
        op.add_column(
            table_name,
            sa.Column("lifecycle_authority_id", postgresql.UUID(as_uuid=True), nullable=True),
        )
        op.create_foreign_key(
            f"{table_name}_lifecycle_authority_id_fkey",
            table_name,
            "data_lifecycle_authorities",
            ["lifecycle_authority_id"],
            ["id"],
            ondelete="RESTRICT",
        )
        op.create_index(
            f"{table_name}_lifecycle_authority_idx",
            table_name,
            ["lifecycle_authority_id"],
        )


def downgrade() -> None:
    op.execute(
        """
        DO $$
        BEGIN
            IF EXISTS (SELECT 1 FROM data_lifecycle_authorities)
               OR EXISTS (SELECT 1 FROM data_lifecycle_gc_runs)
               OR EXISTS (SELECT 1 FROM staging_mutation_epochs)
               OR EXISTS (SELECT 1 FROM staging_mutation_epoch_events)
            THEN
                RAISE EXCEPTION 'cannot downgrade lifecycle authority while deployment data remains';
            END IF;
        END
        $$;
        """
    )
    for table_name in reversed(_EXECUTION_TABLES):
        op.drop_index(f"{table_name}_lifecycle_authority_idx", table_name=table_name)
        op.drop_constraint(
            f"{table_name}_lifecycle_authority_id_fkey",
            table_name,
            type_="foreignkey",
        )
        op.drop_column(table_name, "lifecycle_authority_id")
    op.drop_table("data_lifecycle_gc_items")
    op.drop_table("data_lifecycle_gc_runs")
    op.drop_index("data_lifecycle_objects_authority_state_idx", table_name="data_lifecycle_objects")
    op.drop_index("data_lifecycle_objects_identity_uidx", table_name="data_lifecycle_objects")
    op.drop_table("data_lifecycle_objects")
    op.drop_index("data_lifecycle_authorities_gc_idx", table_name="data_lifecycle_authorities")
    op.drop_table("data_lifecycle_authorities")
    op.drop_table("staging_mutation_epoch_events")
    op.drop_table("staging_mutation_epochs")
