"""Merge the TB2.1 catalog branch into the current linear head.

Revision ID: 0068
Revises: 0062_tb21_profile_catalog, 0067
Create Date: 2026-07-15
"""

from __future__ import annotations

revision = "0068"
down_revision = ("0062_tb21_profile_catalog", "0067")
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Join both already-applied schema branches without additional DDL."""


def downgrade() -> None:
    """Remove only the merge marker; branch migrations remain applied."""
