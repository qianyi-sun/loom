"""Harden the trial in-flight trigger for protected callers.

Revision ID: 0126
Revises: 0125
Create Date: 2026-09-02
"""

from __future__ import annotations

from alembic import op

revision: str = "0126"
down_revision: str | None = "0125"
branch_labels: str | None = None
depends_on: str | None = None


_HARDENED_FUNCTION = """
CREATE OR REPLACE FUNCTION public.trials_inflight_delta() RETURNS TRIGGER AS $$
DECLARE
    was_active boolean := OLD.state IN ('claimed', 'running');
    is_active  boolean := NEW.state IN ('claimed', 'running');
BEGIN
    IF was_active AND NOT is_active THEN
        UPDATE public.team_quotas SET in_flight_count = in_flight_count - 1
         WHERE team_id = OLD.team_id;
    ELSIF is_active AND NOT was_active THEN
        UPDATE public.team_quotas SET in_flight_count = in_flight_count + 1
         WHERE team_id = NEW.team_id;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql SET search_path = pg_catalog;
"""


_LEGACY_FUNCTION = """
CREATE OR REPLACE FUNCTION public.trials_inflight_delta() RETURNS TRIGGER AS $$
DECLARE
    was_active boolean := OLD.state IN ('claimed', 'running');
    is_active  boolean := NEW.state IN ('claimed', 'running');
BEGIN
    IF was_active AND NOT is_active THEN
        UPDATE team_quotas SET in_flight_count = in_flight_count - 1
         WHERE team_id = OLD.team_id;
    ELSIF is_active AND NOT was_active THEN
        UPDATE team_quotas SET in_flight_count = in_flight_count + 1
         WHERE team_id = NEW.team_id;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
"""


def upgrade() -> None:
    op.execute(_HARDENED_FUNCTION)


def downgrade() -> None:
    op.execute(_LEGACY_FUNCTION)
