"""ProviderConnection.rate_card_provider for facade cost attribution.

Revision ID: 0021
Revises: 0020
Create Date: 2026-06-16

Adds an explicit rate-card provider override to provider connections.
Facade-routed calls receive a raw model id such as ``gpt-4o`` and a
connection type such as ``openai-compatible``; the type is not enough to
distinguish OpenAI from Together, Fireworks, self-hosted vLLM, or other
OpenAI-compatible upstreams. Operators can set this field when a
connection should use an existing rate-card provider namespace.
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "0021"
down_revision = "0020"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "provider_connections",
        sa.Column("rate_card_provider", sa.Text(), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("provider_connections", "rate_card_provider")
