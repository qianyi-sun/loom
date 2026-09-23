"""team_quotas.allow_private_endpoints — opt-in for on-prem provider IPs

Revision ID: 0019
Revises: 0018
Create Date: 2026-06-16

Adds a single boolean column to `team_quotas` that gates whether the
team's provider_connections may resolve to RFC1918 / ULA addresses.
Default false.

Per cluster-deploy.md §SSRF defense layer 3:
- `loom cluster` defaults this to false at bootstrap (multi-team
  trust model; private IPs are almost always SSRF targets).
- `loom service` (single-box) defaults to true at bootstrap (the
  one operator IS the host; local vLLM at http://localhost:8000 is
  the dominant use case).
- Loopback (127.0.0.0/8) and link-local are NEVER allowed even with
  the flag on — those are not legitimate provider hosts on any
  topology.

The toggle itself is set by `loom admin teams set` (Phase 2 future
PR) or by direct UPDATE today. Existing teams get DEFAULT false on
migration — preserves the safer behavior.

Why on team_quotas (not teams): team_quotas already exists per-team,
already gates team-policy concerns (license_allowlist, max_attempts).
Avoids creating a new table for one bool.
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "0019"
down_revision = "0018"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "team_quotas",
        sa.Column(
            "allow_private_endpoints",
            sa.Boolean(),
            nullable=False,
            server_default=sa.text("false"),
        ),
    )


def downgrade() -> None:
    op.drop_column("team_quotas", "allow_private_endpoints")
