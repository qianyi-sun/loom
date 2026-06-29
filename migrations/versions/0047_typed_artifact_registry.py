"""add typed artifact registry

Revision ID: 0047
Revises: 0046
Create Date: 2026-06-29
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "0047"
down_revision = "0046"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "artifacts",
        sa.Column(
            "id",
            postgresql.UUID(as_uuid=True),
            server_default=sa.text("gen_random_uuid()"),
            nullable=False,
        ),
        sa.Column("artifact_type", sa.Text(), nullable=False),
        sa.Column(
            "artifact_schema_version",
            sa.Text(),
            server_default=sa.text("'1.0'"),
            nullable=False,
        ),
        sa.Column("name", sa.Text(), nullable=False),
        sa.Column("team_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("project_id", sa.Text(), nullable=True),
        sa.Column("batch_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("trial_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column(
            "created_by",
            postgresql.JSONB(astext_type=sa.Text()),
            server_default=sa.text("'{}'::jsonb"),
            nullable=False,
        ),
        sa.Column("content_hash", sa.Text(), nullable=False),
        sa.Column(
            "storage",
            postgresql.JSONB(astext_type=sa.Text()),
            server_default=sa.text("'{}'::jsonb"),
            nullable=False,
        ),
        sa.Column(
            "visibility",
            sa.Text(),
            server_default=sa.text("'team'"),
            nullable=False,
        ),
        sa.Column(
            "share_status",
            sa.Text(),
            server_default=sa.text("'pending_scan'"),
            nullable=False,
        ),
        sa.Column(
            "redaction_state",
            sa.Text(),
            server_default=sa.text("'pending'"),
            nullable=False,
        ),
        sa.Column(
            "safety_state",
            sa.Text(),
            server_default=sa.text("'unknown'"),
            nullable=False,
        ),
        sa.Column("blocked_reason", sa.Text(), nullable=True),
        sa.Column(
            "retention",
            postgresql.JSONB(astext_type=sa.Text()),
            server_default=sa.text("'{}'::jsonb"),
            nullable=False,
        ),
        sa.Column(
            "provenance",
            postgresql.JSONB(astext_type=sa.Text()),
            server_default=sa.text("'{}'::jsonb"),
            nullable=False,
        ),
        sa.Column(
            "metadata",
            postgresql.JSONB(astext_type=sa.Text()),
            server_default=sa.text("'{}'::jsonb"),
            nullable=False,
        ),
        sa.Column(
            "created_at",
            postgresql.TIMESTAMP(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(["batch_id"], ["batches.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["team_id"], ["teams.id"]),
        sa.ForeignKeyConstraint(["trial_id"], ["trials.id"], ondelete="SET NULL"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("artifacts_team_type_idx", "artifacts", ["team_id", "artifact_type"])
    op.create_index("artifacts_batch_idx", "artifacts", ["batch_id"])
    op.create_index("artifacts_trial_idx", "artifacts", ["trial_id"])
    op.create_index(
        "artifacts_policy_idx",
        "artifacts",
        ["visibility", "share_status", "safety_state"],
    )

    op.create_table(
        "artifact_lineage_edges",
        sa.Column(
            "id",
            postgresql.UUID(as_uuid=True),
            server_default=sa.text("gen_random_uuid()"),
            nullable=False,
        ),
        sa.Column("child_artifact_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("parent_artifact_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("relation", sa.Text(), nullable=False),
        sa.Column(
            "metadata",
            postgresql.JSONB(astext_type=sa.Text()),
            server_default=sa.text("'{}'::jsonb"),
            nullable=False,
        ),
        sa.Column(
            "created_at",
            postgresql.TIMESTAMP(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(["child_artifact_id"], ["artifacts.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["parent_artifact_id"], ["artifacts.id"], ondelete="SET NULL"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "artifact_lineage_child_idx",
        "artifact_lineage_edges",
        ["child_artifact_id"],
    )
    op.create_index(
        "artifact_lineage_parent_idx",
        "artifact_lineage_edges",
        ["parent_artifact_id"],
    )
    op.create_index(
        "artifact_lineage_relation_idx",
        "artifact_lineage_edges",
        ["relation"],
    )

    _backfill_legacy_artifact_list()
    _backfill_trajectory_and_atif()


def downgrade() -> None:
    op.drop_index("artifact_lineage_relation_idx", table_name="artifact_lineage_edges")
    op.drop_index("artifact_lineage_parent_idx", table_name="artifact_lineage_edges")
    op.drop_index("artifact_lineage_child_idx", table_name="artifact_lineage_edges")
    op.drop_table("artifact_lineage_edges")
    op.drop_index("artifacts_policy_idx", table_name="artifacts")
    op.drop_index("artifacts_trial_idx", table_name="artifacts")
    op.drop_index("artifacts_batch_idx", table_name="artifacts")
    op.drop_index("artifacts_team_type_idx", table_name="artifacts")
    op.drop_table("artifacts")


def _backfill_legacy_artifact_list() -> None:
    op.execute(sa.text("""
        INSERT INTO artifacts (
            artifact_type,
            artifact_schema_version,
            name,
            team_id,
            batch_id,
            trial_id,
            created_by,
            content_hash,
            storage,
            visibility,
            share_status,
            redaction_state,
            safety_state,
            blocked_reason,
            retention,
            provenance,
            metadata,
            created_at
        )
        SELECT
            COALESCE(
                NULLIF(item->>'artifact_type', ''),
                CASE
                    WHEN lower(COALESCE(item->>'role', item->>'artifact_role', '')) IN
                        ('trajectory', 'trajectories') THEN 'trajectory'
                    WHEN lower(COALESCE(item->>'role', item->>'artifact_role', '')) IN
                        ('report', 'reports', 'atif') THEN 'atif_projection'
                    WHEN lower(COALESCE(item->>'role', item->>'artifact_role', '')) IN
                        ('raw', 'raw_diagnostic', 'raw_diagnostics', 'internal_diagnostics')
                        THEN 'debug_bundle'
                    WHEN lower(COALESCE(item->>'role', item->>'artifact_role', '')) IN
                        ('log', 'logs', 'diagnostic', 'diagnostics', 'logs_diagnostics')
                        THEN 'debug_bundle'
                    ELSE 'evidence_bundle'
                END
            ) AS artifact_type,
            '1.0',
            COALESCE(NULLIF(item->>'name', ''), regexp_replace(item->>'key', '^.*/', '')),
            t.team_id,
            t.batch_id,
            t.id,
            jsonb_build_object(
                'kind', 'system_backfill',
                'batch_id', t.batch_id,
                'trial_id', t.id
            ),
            COALESCE(NULLIF(item->>'content_hash', ''), 'pending:legacy-unhashed'),
            jsonb_build_object(
                'backend', 'object_store',
                'bucket', COALESCE(NULLIF(item->>'bucket', ''), 'artifacts'),
                'key', item->>'key',
                'media_type', COALESCE(NULLIF(item->>'media_type', ''), 'application/octet-stream'),
                'size_bytes',
                    CASE
                        WHEN COALESCE(item->>'size', '') ~ '^[0-9]+$'
                            THEN (item->>'size')::bigint
                        ELSE 0
                    END
            ),
            t.visibility,
            CASE
                WHEN item->>'share_status' IN ('pending_scan', 'shared', 'blocked')
                    THEN item->>'share_status'
                ELSE 'pending_scan'
            END,
            CASE
                WHEN item->>'share_status' = 'blocked' THEN 'blocked'
                WHEN item->>'share_status' = 'shared' THEN 'not_required'
                ELSE 'pending'
            END,
            CASE
                WHEN item->>'share_status' = 'blocked' THEN 'unsafe'
                WHEN item->>'share_status' = 'shared' THEN 'safe'
                ELSE 'unknown'
            END,
            NULLIF(item->>'blocked_reason', ''),
            jsonb_build_object(
                'class',
                CASE
                    WHEN lower(COALESCE(item->>'role', item->>'artifact_role', '')) LIKE '%raw%'
                        THEN 'owner_only_debug'
                    ELSE 'shared_reusable'
                END,
                'expires_at', NULL
            ),
            jsonb_build_object(
                'batch_id', t.batch_id,
                'trial_id', t.id,
                'source_trial_ids', jsonb_build_array(t.id),
                'relation', 'produced_from'
            ),
            jsonb_build_object(
                'legacy_role', COALESCE(item->>'role', item->>'artifact_role'),
                'step_name', item->>'step_name'
            ),
            COALESCE(t.finished_at, t.started_at, t.submitted_at)
        FROM trials t
        CROSS JOIN LATERAL jsonb_array_elements(t.trajectory_index->'artifacts') AS item
        WHERE jsonb_typeof(t.trajectory_index->'artifacts') = 'array'
          AND item ? 'key'
          AND item->>'key' <> ''
    """))


def _backfill_trajectory_and_atif() -> None:
    op.execute(sa.text("""
        INSERT INTO artifacts (
            artifact_type,
            artifact_schema_version,
            name,
            team_id,
            batch_id,
            trial_id,
            created_by,
            content_hash,
            storage,
            visibility,
            share_status,
            redaction_state,
            safety_state,
            blocked_reason,
            retention,
            provenance,
            metadata,
            created_at
        )
        SELECT
            v.artifact_type,
            '1.0',
            v.name,
            t.team_id,
            t.batch_id,
            t.id,
            jsonb_build_object(
                'kind', 'system_backfill',
                'batch_id', t.batch_id,
                'trial_id', t.id
            ),
            'pending:legacy-unhashed',
            jsonb_build_object(
                'backend', 'object_store',
                'bucket',
                    CASE
                        WHEN v.uri LIKE 's3://%' THEN split_part(substr(v.uri, 6), '/', 1)
                        ELSE 'trajectories'
                    END,
                'key',
                    CASE
                        WHEN v.uri LIKE 's3://%' THEN regexp_replace(substr(v.uri, 6), '^[^/]+/', '')
                        ELSE t.team_id::text || '/' || t.id::text || '/' || v.default_file
                    END,
                'media_type', v.media_type,
                'size_bytes', 0
            ),
            t.visibility,
            t.share_status,
            CASE WHEN t.share_status = 'shared' THEN 'not_required' ELSE 'pending' END,
            CASE WHEN t.share_status = 'shared' THEN 'safe' ELSE 'unknown' END,
            NULL,
            jsonb_build_object('class', 'release_evidence', 'expires_at', NULL),
            jsonb_build_object(
                'batch_id', t.batch_id,
                'trial_id', t.id,
                'source_trial_ids', jsonb_build_array(t.id),
                'relation', 'produced_from'
            ),
            '{}'::jsonb,
            COALESCE(t.finished_at, t.started_at, t.submitted_at)
        FROM trials t
        CROSS JOIN LATERAL (
            VALUES
                (
                    'trajectory',
                    'Trajectory events',
                    'events.jsonl',
                    'application/x-ndjson',
                    t.trajectory_index->>'trajectory_uri',
                    t.started_at IS NOT NULL
                ),
                (
                    'atif_projection',
                    'ATIF projection',
                    'atif.json',
                    'application/json',
                    t.trajectory_index->>'atif_uri',
                    t.finished_at IS NOT NULL
                )
        ) AS v(artifact_type, name, default_file, media_type, uri, should_backfill)
        WHERE t.trajectory_index IS NOT NULL
          AND v.should_backfill
    """))
