"""Add lease-fenced personal-development artifact garbage collection.

Revision ID: 0086
Revises: 0085
Create Date: 2026-08-11
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "0086"
down_revision = "0085"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "personal_dev_candidates",
        sa.Column("source_generation_id", postgresql.UUID(as_uuid=True), nullable=True),
    )
    op.execute("UPDATE personal_dev_candidates SET source_generation_id = id")
    op.alter_column(
        "personal_dev_candidates",
        "source_generation_id",
        existing_type=postgresql.UUID(as_uuid=True),
        nullable=False,
    )
    op.create_check_constraint(
        "personal_dev_candidates_object_binding_check",
        "personal_dev_candidates",
        "object_bucket <> '' AND object_bucket = btrim(object_bucket) "
        "AND position('/' in object_bucket) = 0 AND "
        "((source_generation_id = id AND "
        "object_key = 'personal-dev/sources/' || owner_team_id::text || '/' || "
        "owner_user_id::text || '/' || candidate_sha || '/' || "
        "archive_sha256 || '.tar') OR "
        "object_key = 'personal-dev/sources/' || owner_team_id::text || '/' || "
        "owner_user_id::text || '/' || candidate_sha || '/' || "
        "source_generation_id::text || '/' || archive_sha256 || '.tar')",
    )
    op.add_column(
        "personal_dev_candidates",
        sa.Column("registry_prefix", sa.Text(), nullable=True),
    )
    op.create_check_constraint(
        "personal_dev_candidates_registry_prefix_check",
        "personal_dev_candidates",
        "registry_prefix IS NULL OR ("
        "registry_prefix ~ '^[A-Za-z0-9][A-Za-z0-9._:/-]{0,308}$' "
        "AND right(registry_prefix, 1) NOT IN ('/', ':') "
        "AND position('://' in registry_prefix) = 0 "
        "AND position('@' in registry_prefix) = 0)",
    )
    op.add_column(
        "personal_dev_candidates",
        sa.Column(
            "artifact_state",
            sa.String(length=16),
            nullable=False,
            server_default=sa.text("'retained'"),
        ),
    )
    op.add_column(
        "personal_dev_candidates",
        sa.Column(
            "artifact_gc_lease_epoch",
            sa.BigInteger(),
            nullable=False,
            server_default=sa.text("0"),
        ),
    )
    for column in (
        "artifact_gc_unreferenced_at",
        "artifact_gc_lease_expires_at",
        "artifact_collected_at",
    ):
        op.add_column(
            "personal_dev_candidates",
            sa.Column(column, sa.TIMESTAMP(timezone=True), nullable=True),
        )
    op.add_column(
        "personal_dev_candidates",
        sa.Column("artifact_gc_claimed_by", sa.String(length=128), nullable=True),
    )
    op.add_column(
        "personal_dev_candidates",
        sa.Column("artifact_gc_blocked_reason", sa.String(length=64), nullable=True),
    )
    op.add_column(
        "personal_dev_candidates",
        sa.Column("artifact_gc_manifest_json", postgresql.JSONB(), nullable=True),
    )
    op.add_column(
        "personal_dev_candidates",
        sa.Column("artifact_gc_manifest_sha256", sa.String(length=64), nullable=True),
    )
    op.create_check_constraint(
        "personal_dev_candidates_artifact_gc_check",
        "personal_dev_candidates",
        "artifact_gc_lease_epoch >= 0 "
        "AND (artifact_gc_blocked_reason IS NULL OR "
        "artifact_gc_blocked_reason IN ("
        "'manifest_authority_invalid', 'registry_authority_unavailable')) AND ("
        "(artifact_state = 'retained' "
        "AND artifact_gc_claimed_by IS NULL "
        "AND artifact_gc_lease_expires_at IS NULL "
        "AND artifact_gc_manifest_json IS NULL "
        "AND artifact_gc_manifest_sha256 IS NULL "
        "AND artifact_collected_at IS NULL) OR ("
        "artifact_state = 'collecting' "
        "AND artifact_gc_blocked_reason IS NULL "
        "AND artifact_gc_unreferenced_at IS NOT NULL "
        "AND artifact_gc_claimed_by IS NOT NULL "
        "AND artifact_gc_lease_expires_at IS NOT NULL "
        "AND artifact_gc_manifest_json IS NOT NULL "
        "AND jsonb_typeof(artifact_gc_manifest_json) = 'object' "
        "AND artifact_gc_manifest_sha256 ~ '^[0-9a-f]{64}$' "
        "AND artifact_collected_at IS NULL) OR ("
        "artifact_state = 'collected' "
        "AND artifact_gc_blocked_reason IS NULL "
        "AND artifact_gc_unreferenced_at IS NOT NULL "
        "AND artifact_gc_claimed_by IS NULL "
        "AND artifact_gc_lease_expires_at IS NULL "
        "AND artifact_gc_manifest_json IS NOT NULL "
        "AND jsonb_typeof(artifact_gc_manifest_json) = 'object' "
        "AND artifact_gc_manifest_sha256 ~ '^[0-9a-f]{64}$' "
        "AND artifact_collected_at IS NOT NULL))",
    )
    op.create_check_constraint(
        "personal_dev_candidates_artifact_manifest_binding_check",
        "personal_dev_candidates",
        "artifact_gc_manifest_json IS NULL OR (("
        "jsonb_typeof(artifact_gc_manifest_json) = 'object' "
        "AND artifact_gc_manifest_json->>'schema_version' = '1' "
        "AND artifact_gc_manifest_json->>'candidate_id' = id::text "
        "AND artifact_gc_manifest_json->>'owner_user_id' = owner_user_id::text "
        "AND artifact_gc_manifest_json->>'owner_team_id' = owner_team_id::text "
        "AND artifact_gc_manifest_json->>'candidate_sha' = candidate_sha "
        "AND artifact_gc_manifest_json->>'object_bucket' = object_bucket "
        "AND artifact_gc_manifest_json->>'source_generation_id' = "
        "source_generation_id::text "
        "AND artifact_gc_manifest_json->>'source_object_key' = object_key) IS TRUE)",
    )
    op.create_index(
        "personal_dev_candidates_artifact_gc_idx",
        "personal_dev_candidates",
        (
            "artifact_state",
            "artifact_gc_unreferenced_at",
            "artifact_gc_lease_expires_at",
            "id",
        ),
    )
    op.create_table(
        "personal_dev_candidate_artifact_collections",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("candidate_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("collection_sequence", sa.Integer(), nullable=False),
        sa.Column("collector_id", sa.String(length=128), nullable=False),
        sa.Column("gc_lease_epoch", sa.BigInteger(), nullable=False),
        sa.Column("manifest_json", postgresql.JSONB(), nullable=False),
        sa.Column("manifest_sha256", sa.String(length=64), nullable=False),
        sa.Column("unreferenced_at", sa.TIMESTAMP(timezone=True), nullable=False),
        sa.Column("collected_at", sa.TIMESTAMP(timezone=True), nullable=False),
        sa.CheckConstraint(
            "collection_sequence > 0 AND gc_lease_epoch > 0 "
            "AND collector_id <> '' AND collector_id = btrim(collector_id) "
            "AND manifest_sha256 ~ '^[0-9a-f]{64}$' "
            "AND jsonb_typeof(manifest_json) = 'object' "
            "AND ((manifest_json->>'schema_version' = '1' "
            "AND manifest_json->>'candidate_id' = candidate_id::text) IS TRUE) "
            "AND collected_at >= unreferenced_at",
            name="personal_dev_candidate_artifact_collections_check",
        ),
        sa.ForeignKeyConstraint(
            ["candidate_id"],
            ["personal_dev_candidates.id"],
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "candidate_id",
            "collection_sequence",
            name="personal_dev_candidate_artifact_collections_sequence_uidx",
        ),
    )
    op.create_index(
        "personal_dev_candidate_artifact_collections_candidate_idx",
        "personal_dev_candidate_artifact_collections",
        ("candidate_id", "collected_at", "id"),
    )
    op.execute(
        """
        CREATE FUNCTION reject_personal_dev_artifact_collection_mutation()
        RETURNS trigger
        LANGUAGE plpgsql
        AS $$
        BEGIN
            RAISE EXCEPTION
                'personal-dev artifact collection evidence is append-only';
        END
        $$
        """
    )
    op.execute(
        """
        CREATE TRIGGER personal_dev_artifact_collections_append_only
        BEFORE UPDATE OR DELETE ON personal_dev_candidate_artifact_collections
        FOR EACH ROW
        EXECUTE FUNCTION reject_personal_dev_artifact_collection_mutation()
        """
    )


def downgrade() -> None:
    op.execute(
        """
        DO $$
        BEGIN
            IF EXISTS (
                SELECT 1
                  FROM personal_dev_candidates
                 WHERE registry_prefix IS NOT NULL
                    OR artifact_state <> 'retained'
                    OR artifact_gc_lease_epoch <> 0
                    OR artifact_gc_unreferenced_at IS NOT NULL
            ) THEN
                RAISE EXCEPTION
                    'cannot downgrade 0086 after personal-dev artifact authority is used';
            END IF;
        END
        $$
        """
    )
    op.execute(
        "DROP TRIGGER personal_dev_artifact_collections_append_only "
        "ON personal_dev_candidate_artifact_collections"
    )
    op.execute("DROP FUNCTION reject_personal_dev_artifact_collection_mutation()")
    op.drop_index(
        "personal_dev_candidate_artifact_collections_candidate_idx",
        table_name="personal_dev_candidate_artifact_collections",
    )
    op.drop_table("personal_dev_candidate_artifact_collections")
    op.drop_index(
        "personal_dev_candidates_artifact_gc_idx",
        table_name="personal_dev_candidates",
    )
    op.drop_constraint(
        "personal_dev_candidates_artifact_gc_check",
        "personal_dev_candidates",
        type_="check",
    )
    op.drop_constraint(
        "personal_dev_candidates_artifact_manifest_binding_check",
        "personal_dev_candidates",
        type_="check",
    )
    op.drop_constraint(
        "personal_dev_candidates_object_binding_check",
        "personal_dev_candidates",
        type_="check",
    )
    op.drop_constraint(
        "personal_dev_candidates_registry_prefix_check",
        "personal_dev_candidates",
        type_="check",
    )
    for column in (
        "artifact_collected_at",
        "artifact_gc_manifest_sha256",
        "artifact_gc_manifest_json",
        "artifact_gc_lease_expires_at",
        "artifact_gc_claimed_by",
        "artifact_gc_blocked_reason",
        "artifact_gc_unreferenced_at",
        "artifact_gc_lease_epoch",
        "artifact_state",
        "registry_prefix",
        "source_generation_id",
    ):
        op.drop_column("personal_dev_candidates", column)
