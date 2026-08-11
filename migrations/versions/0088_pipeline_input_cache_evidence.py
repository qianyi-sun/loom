"""Add immutable Pipeline input materialization evidence.

Revision ID: 0088
Revises: 0087
Create Date: 2026-08-11
"""

from __future__ import annotations

from alembic import op

revision = "0088"
down_revision = "0087"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        "ALTER TABLE workers ADD COLUMN input_cache_capacity_bytes BIGINT NOT NULL DEFAULT 0"
    )
    op.execute(
        "ALTER TABLE workers ADD COLUMN input_cache_reserved_bytes BIGINT NOT NULL DEFAULT 0"
    )
    op.execute(
        "ALTER TABLE workers ADD COLUMN input_cache_ready_bytes BIGINT NOT NULL DEFAULT 0"
    )
    op.execute(
        "ALTER TABLE workers ADD CONSTRAINT workers_input_cache_capacity_check CHECK "
        "(input_cache_capacity_bytes >= 0 AND input_cache_reserved_bytes >= 0 "
        "AND input_cache_ready_bytes >= 0 AND input_cache_reserved_bytes <= input_cache_capacity_bytes "
        "AND input_cache_ready_bytes <= input_cache_capacity_bytes)"
    )
    op.execute(
        "ALTER TABLE execution_attempts ADD CONSTRAINT "
        "execution_attempts_worker_identity_uidx UNIQUE(id,worker_id)"
    )
    op.execute(
        """
        CREATE TABLE pipeline_input_materialization_evidence (
            execution_attempt_id UUID PRIMARY KEY,
            worker_id UUID NOT NULL REFERENCES workers(id) ON DELETE RESTRICT,
            lease_epoch BIGINT NOT NULL CHECK (lease_epoch >= 0),
            cache_expectation TEXT NOT NULL
                CHECK (cache_expectation IN ('cold_after_eviction','warm_reuse_only')),
            ordered_manifest_sha256s_json BYTEA NOT NULL,
            manifest_open_count BIGINT NOT NULL CHECK (manifest_open_count >= 0),
            file_open_count BIGINT NOT NULL CHECK (file_open_count >= 0),
            file_bytes BIGINT NOT NULL CHECK (file_bytes >= 0),
            archive_extraction_count BIGINT NOT NULL CHECK (archive_extraction_count >= 0),
            cas_rename_count BIGINT NOT NULL CHECK (cas_rename_count >= 0),
            input_view_sha256 TEXT NOT NULL,
            materialized_at TIMESTAMPTZ NOT NULL,
            evidence_json BYTEA NOT NULL,
            evidence_sha256 TEXT NOT NULL,
            CONSTRAINT pipeline_input_materialization_evidence_attempt_worker_fk
                FOREIGN KEY(execution_attempt_id,worker_id)
                REFERENCES execution_attempts(id,worker_id) ON DELETE CASCADE
        )
        """
    )


def downgrade() -> None:
    op.execute("DROP TABLE pipeline_input_materialization_evidence")
    op.execute(
        "ALTER TABLE execution_attempts DROP CONSTRAINT execution_attempts_worker_identity_uidx"
    )
    op.execute("ALTER TABLE workers DROP CONSTRAINT workers_input_cache_capacity_check")
    op.execute("ALTER TABLE workers DROP COLUMN input_cache_ready_bytes")
    op.execute("ALTER TABLE workers DROP COLUMN input_cache_reserved_bytes")
    op.execute("ALTER TABLE workers DROP COLUMN input_cache_capacity_bytes")
