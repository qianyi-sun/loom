"""Cache per-connection Responses-API support so the gateway can dispatch
straight into `responses_chat_compat` for providers that don't implement
`POST /v1/responses` (yibuapi et al.). See
docs/architecture/responses-api.md.

Three nullable columns; no data migration; downgrade drops them.
"""
from __future__ import annotations

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "0048"
down_revision = "0047"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "provider_connections",
        sa.Column("responses_api_supported", sa.Boolean(), nullable=True),
    )
    op.add_column(
        "provider_connections",
        sa.Column(
            "responses_api_probed_at",
            sa.TIMESTAMP(timezone=True),
            nullable=True,
        ),
    )
    op.add_column(
        "provider_connections",
        sa.Column("responses_api_probe_error", sa.Text(), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("provider_connections", "responses_api_probe_error")
    op.drop_column("provider_connections", "responses_api_probed_at")
    op.drop_column("provider_connections", "responses_api_supported")
