"""provider model preflight status (#315)

Revision ID: 0036
Revises: 0035
Create Date: 2026-06-23

Track model-level entitlement/preflight separately from provider model
discovery. NULL means not tested; valid/failed records the latest bounded
single-model probe.
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "0036"
down_revision = "0035"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "provider_models_cache",
        sa.Column("last_preflight_status", sa.Text(), nullable=True),
    )
    op.add_column(
        "provider_models_cache",
        sa.Column("last_preflight_at", sa.TIMESTAMP(timezone=True), nullable=True),
    )
    op.add_column(
        "provider_models_cache",
        sa.Column("last_preflight_http_status", sa.Integer(), nullable=True),
    )
    op.add_column(
        "provider_models_cache",
        sa.Column("last_preflight_error_code", sa.Text(), nullable=True),
    )
    op.add_column(
        "provider_models_cache",
        sa.Column("last_preflight_error_message", sa.Text(), nullable=True),
    )
    op.create_check_constraint(
        "provider_models_cache_preflight_status_check",
        "provider_models_cache",
        "last_preflight_status IS NULL OR "
        "last_preflight_status IN ('valid', 'failed')",
    )


def downgrade() -> None:
    op.drop_constraint(
        "provider_models_cache_preflight_status_check",
        "provider_models_cache",
        type_="check",
    )
    op.drop_column("provider_models_cache", "last_preflight_error_message")
    op.drop_column("provider_models_cache", "last_preflight_error_code")
    op.drop_column("provider_models_cache", "last_preflight_http_status")
    op.drop_column("provider_models_cache", "last_preflight_at")
    op.drop_column("provider_models_cache", "last_preflight_status")
