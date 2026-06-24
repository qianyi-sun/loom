"""key pending access requests by contact email

Revision ID: 0037
Revises: 0036
Create Date: 2026-06-24

Public-beta access requests now represent people asking to join a fixed
internal team. Requester names are labels only; active request uniqueness must
therefore use contact_email instead of the requested display name.
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "0037"
down_revision = "0036"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.drop_index(
        "pending_team_registrations_active_name_uidx",
        table_name="pending_team_registrations",
    )
    op.create_index(
        "pending_team_registrations_active_contact_email_uidx",
        "pending_team_registrations",
        [sa.text("lower(contact_email)")],
        unique=True,
        postgresql_where=sa.text("status IN ('pending', 'approved')"),
    )


def downgrade() -> None:
    op.drop_index(
        "pending_team_registrations_active_contact_email_uidx",
        table_name="pending_team_registrations",
    )
    op.create_index(
        "pending_team_registrations_active_name_uidx",
        "pending_team_registrations",
        [sa.text("lower(name)")],
        unique=True,
        postgresql_where=sa.text("status IN ('pending', 'approved')"),
    )
