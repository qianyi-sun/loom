from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "20260602_0005"
down_revision = "20260602_0004"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "artifact_chunks",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("run_id", sa.String(length=128), sa.ForeignKey("runs.run_id"), nullable=False),
        sa.Column("attempt_id", sa.String(length=160), sa.ForeignKey("run_attempts.attempt_id"), nullable=False),
        sa.Column(
            "artifact_id",
            sa.String(length=255),
            sa.ForeignKey("artifacts.artifact_id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("chunk_kind", sa.String(length=64), nullable=False),
        sa.Column("chunk_sequence", sa.Integer(), nullable=False),
        sa.Column("storage_key", sa.Text(), nullable=False),
        sa.Column("media_type", sa.String(length=255), nullable=False),
        sa.Column("size_bytes", sa.Integer(), nullable=False),
        sa.Column("sha256", sa.String(length=64), nullable=False),
        sa.Column("upload_status", sa.String(length=64), nullable=False),
        sa.Column("upload_error_reason", sa.Text(), nullable=True),
        sa.Column("metadata", sa.JSON(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.UniqueConstraint("artifact_id", "chunk_kind", "chunk_sequence", name="uq_artifact_chunk_sequence"),
    )
    op.create_index(
        "ix_artifact_chunks_run_attempt_kind_sequence",
        "artifact_chunks",
        ["run_id", "attempt_id", "chunk_kind", "chunk_sequence"],
    )
    op.create_index(
        "ix_artifact_chunks_upload_status_created",
        "artifact_chunks",
        ["upload_status", "created_at"],
    )


def downgrade() -> None:
    op.drop_index("ix_artifact_chunks_upload_status_created", table_name="artifact_chunks")
    op.drop_index("ix_artifact_chunks_run_attempt_kind_sequence", table_name="artifact_chunks")
    op.drop_table("artifact_chunks")
