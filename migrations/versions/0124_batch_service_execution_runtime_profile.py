"""Freeze automatic service-execution runtime profiles on batches.

Revision ID: 0124
Revises: 0123
"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "0124"
down_revision = "0123"
branch_labels = None
depends_on = None

_CONSTRAINT = "batches_service_execution_runtime_profile_check"
_RULE = (
    "service_execution_runtime_profile IS NULL OR ("
    "backend = 'nebius' "
    "AND jsonb_typeof(service_execution_runtime_profile) = 'object' "
    "AND service_execution_runtime_profile->>'schema_version' = "
    "'loom.service-execution-runtime-profile.v1' "
    "AND service_execution_runtime_profile->>'logical_pool_id' = 'nebius-cpu' "
    "AND service_execution_runtime_profile->>'candidate_sha' ~ '^[0-9a-f]{40}$'"
    ")"
)


def upgrade() -> None:
    op.add_column(
        "batches",
        sa.Column(
            "service_execution_runtime_profile",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=True,
        ),
    )
    op.create_check_constraint(_CONSTRAINT, "batches", _RULE)


def downgrade() -> None:
    op.drop_constraint(_CONSTRAINT, "batches", type_="check")
    op.drop_column("batches", "service_execution_runtime_profile")
