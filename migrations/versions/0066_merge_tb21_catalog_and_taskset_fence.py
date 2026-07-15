"""Merge the TB2.1 catalog and TaskSet fence migration branches.

Revision ID: 0066
Revises: 0062_tb21_profile_catalog, 0065
Create Date: 2026-07-15
"""

from __future__ import annotations

revision = "0066"
down_revision = ("0062_tb21_profile_catalog", "0065")
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Join both already-applied schema branches without additional DDL."""


def downgrade() -> None:
    """Remove only the merge marker; branch migrations remain applied."""
