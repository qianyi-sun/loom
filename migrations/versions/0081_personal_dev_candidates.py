"""Add immutable personal-dev candidates and fenced build attempts.

Revision ID: 0081
Revises: 0080
Create Date: 2026-08-10
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "0081"
down_revision = "0080"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "personal_dev_candidates",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("owner_user_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("owner_team_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("candidate_sha", sa.String(length=64), nullable=False),
        sa.Column("source_sha256", sa.String(length=64), nullable=False),
        sa.Column("archive_sha256", sa.String(length=64), nullable=False),
        sa.Column("build_contract_sha256", sa.String(length=64), nullable=False),
        sa.Column("source_commit", sa.String(length=40), nullable=False),
        sa.Column("dirty", sa.Boolean(), nullable=False),
        sa.Column("manifest_json", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("object_bucket", sa.Text(), nullable=False),
        sa.Column("object_key", sa.Text(), nullable=False),
        sa.Column("archive_size_bytes", sa.BigInteger(), nullable=False),
        sa.Column("status", sa.Text(), server_default=sa.text("'uploaded'"), nullable=False),
        sa.Column("image_manifest_digest", sa.String(length=71), nullable=True),
        sa.Column(
            "publication_json",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=True,
        ),
        sa.Column("publication_sha256", sa.String(length=64), nullable=True),
        sa.Column("failure_reason", sa.String(length=256), nullable=True),
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
        sa.Column("ready_at", postgresql.TIMESTAMP(timezone=True), nullable=True),
        sa.CheckConstraint(
            "candidate_sha ~ '^[0-9a-f]{64}$' AND "
            "source_sha256 ~ '^[0-9a-f]{64}$' AND "
            "archive_sha256 ~ '^[0-9a-f]{64}$' AND "
            "build_contract_sha256 ~ '^[0-9a-f]{64}$'",
            name="personal_dev_candidates_digests_check",
        ),
        sa.CheckConstraint(
            "source_commit ~ '^[0-9a-f]{40}$'",
            name="personal_dev_candidates_source_commit_check",
        ),
        sa.CheckConstraint(
            "archive_size_bytes > 0",
            name="personal_dev_candidates_archive_size_check",
        ),
        sa.CheckConstraint(
            "status IN ('uploaded', 'queued', 'building', 'ready', 'failed')",
            name="personal_dev_candidates_status_check",
        ),
        sa.CheckConstraint(
            "(status IN ('uploaded', 'queued', 'building') "
            "AND image_manifest_digest IS NULL "
            "AND publication_json IS NULL AND publication_sha256 IS NULL "
            "AND failure_reason IS NULL AND ready_at IS NULL) OR "
            "(status = 'ready' AND image_manifest_digest IS NOT NULL "
            "AND image_manifest_digest ~ '^sha256:[0-9a-f]{64}$' "
            "AND publication_json IS NOT NULL "
            "AND publication_sha256 IS NOT NULL "
            "AND publication_sha256 ~ '^[0-9a-f]{64}$' "
            "AND failure_reason IS NULL AND ready_at IS NOT NULL) OR "
            "(status = 'failed' AND image_manifest_digest IS NULL "
            "AND publication_json IS NULL AND publication_sha256 IS NULL "
            "AND failure_reason IS NOT NULL AND ready_at IS NULL)",
            name="personal_dev_candidates_terminal_fields_check",
        ),
        sa.ForeignKeyConstraint(["owner_team_id"], ["teams.id"], ondelete="RESTRICT"),
        sa.ForeignKeyConstraint(["owner_user_id"], ["users.id"], ondelete="RESTRICT"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "owner_user_id",
            "owner_team_id",
            "source_sha256",
            "archive_sha256",
            "build_contract_sha256",
            name="personal_dev_candidates_owner_source_uidx",
        ),
    )
    op.create_index(
        "personal_dev_candidates_owner_created_idx",
        "personal_dev_candidates",
        ["owner_user_id", "created_at", "id"],
    )
    op.create_index(
        "personal_dev_candidates_status_created_idx",
        "personal_dev_candidates",
        ["status", "created_at", "id"],
    )

    op.create_table(
        "personal_dev_candidate_build_attempts",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("candidate_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("subject_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("subject_incarnation", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("operation_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("operation_epoch", sa.BigInteger(), nullable=False),
        sa.Column("attempt_sequence", sa.Integer(), server_default=sa.text("0"), nullable=False),
        sa.Column("state", sa.Text(), server_default=sa.text("'queued'"), nullable=False),
        sa.Column("lease_epoch", sa.BigInteger(), server_default=sa.text("0"), nullable=False),
        sa.Column("claimed_by", sa.String(length=128), nullable=True),
        sa.Column("lease_expires_at", postgresql.TIMESTAMP(timezone=True), nullable=True),
        sa.Column("failure_reason", sa.String(length=256), nullable=True),
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
        sa.Column("started_at", postgresql.TIMESTAMP(timezone=True), nullable=True),
        sa.Column("finished_at", postgresql.TIMESTAMP(timezone=True), nullable=True),
        sa.CheckConstraint(
            "attempt_sequence >= 0 AND operation_epoch > 0 AND lease_epoch >= 0",
            name="personal_dev_candidate_build_attempts_counters_check",
        ),
        sa.CheckConstraint(
            "state IN ('queued', 'claimed', 'running', 'succeeded', 'failed')",
            name="personal_dev_candidate_build_attempts_state_check",
        ),
        sa.CheckConstraint(
            "(state = 'queued' AND claimed_by IS NULL AND lease_expires_at IS NULL "
            "AND started_at IS NULL AND finished_at IS NULL AND failure_reason IS NULL) OR "
            "(state = 'claimed' AND claimed_by IS NOT NULL AND lease_expires_at IS NOT NULL "
            "AND started_at IS NULL AND finished_at IS NULL AND failure_reason IS NULL) OR "
            "(state = 'running' AND claimed_by IS NOT NULL AND lease_expires_at IS NOT NULL "
            "AND started_at IS NOT NULL AND finished_at IS NULL AND failure_reason IS NULL) OR "
            "(state = 'succeeded' AND claimed_by IS NOT NULL AND lease_expires_at IS NULL "
            "AND started_at IS NOT NULL AND finished_at IS NOT NULL AND failure_reason IS NULL) OR "
            "(state = 'failed' AND claimed_by IS NOT NULL AND lease_expires_at IS NULL "
            "AND started_at IS NOT NULL AND finished_at IS NOT NULL AND failure_reason IS NOT NULL)",
            name="personal_dev_candidate_build_attempts_state_fields_check",
        ),
        sa.ForeignKeyConstraint(
            ["candidate_id"],
            ["personal_dev_candidates.id"],
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "candidate_id",
            "attempt_sequence",
            name="personal_dev_candidate_build_attempts_sequence_uidx",
        ),
        sa.UniqueConstraint(
            "subject_id",
            "subject_incarnation",
            "operation_epoch",
            "attempt_sequence",
            name="personal_dev_candidate_build_attempts_operation_uidx",
        ),
    )
    op.create_index(
        "personal_dev_candidate_build_attempts_picker_idx",
        "personal_dev_candidate_build_attempts",
        ["state", "lease_expires_at", "created_at", "id"],
    )


def downgrade() -> None:
    op.drop_index(
        "personal_dev_candidate_build_attempts_picker_idx",
        table_name="personal_dev_candidate_build_attempts",
    )
    op.drop_table("personal_dev_candidate_build_attempts")
    op.drop_index(
        "personal_dev_candidates_status_created_idx",
        table_name="personal_dev_candidates",
    )
    op.drop_index(
        "personal_dev_candidates_owner_created_idx",
        table_name="personal_dev_candidates",
    )
    op.drop_table("personal_dev_candidates")
